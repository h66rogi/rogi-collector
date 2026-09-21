package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5"
)

var ErrCollectionDisabled = errors.New("collection disabled")
var ErrOwner = errors.New("collection owner no longer valid")
var ErrGeneration = errors.New("journal generation mismatch")
var ErrCursorExpired = errors.New("donation cursor expired")
var ErrCursor = errors.New("explicit contiguous cursor required")
var ErrRevision = errors.New("recovery revision mismatch")
var ErrConflict = errors.New("idempotency conflict")
var ErrJournalFull = errors.New("journal payload limit reached")

type CollectionState struct {
	Channel        string
	Generation     string
	Current        uint64
	ExpiredThrough uint64
	Subscribed     bool
	Runtime        string
	StateAt        time.Time
	LastReceived   *time.Time
}
type OwnerGrant struct {
	Token      string    `json:"token"`
	Channel    string    `json:"channel"`
	Generation string    `json:"generation"`
	GrantedAt  time.Time `json:"grantedAt"`
	ValidUntil time.Time `json:"validUntil"`
}

func num(n uint64) string               { return strconv.FormatUint(n, 10) }
func parseNum(s string) (uint64, error) { return strconv.ParseUint(s, 10, 64) }
func (s *PgStore) EnsureCollection(ctx context.Context, channel string) error {
	if channel == "" {
		return nil
	}
	if !s.allowsCollection(model.PlatformSoop, channel) {
		return ErrCollectionDisabled
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO collector_channels(channel_id,generation) VALUES($1,$2) ON CONFLICT DO NOTHING`, channel, uuid.NewString())
	return err
}
func scanCollection(row pgx.Row) (CollectionState, error) {
	var c CollectionState
	var current, expired string
	err := row.Scan(&c.Channel, &c.Generation, &current, &expired, &c.Subscribed, &c.Runtime, &c.StateAt, &c.LastReceived)
	if err != nil {
		return c, err
	}
	c.Current, err = parseNum(current)
	if err != nil {
		return c, err
	}
	c.ExpiredThrough, err = parseNum(expired)
	return c, err
}

const collectionColumns = `channel_id,generation,next_offset::text,expired_through::text,subscribed,CASE WHEN runtime_state='connected' AND NOT EXISTS(SELECT 1 FROM collector_owner_grants g WHERE g.token=collector_channels.owner_token AND g.closed_at IS NULL AND g.valid_until>clock_timestamp()) THEN 'reconnecting' ELSE runtime_state END,state_at,last_received_at`

func (s *PgStore) CollectionState(ctx context.Context, channel string) (CollectionState, error) {
	return scanCollection(s.pool.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1`, channel))
}
func (s *PgStore) CollectionEnabled(ctx context.Context, channel string) (bool, error) {
	if !s.allowsCollection(model.PlatformSoop, channel) {
		return false, nil
	}
	var enabled bool
	err := s.pool.QueryRow(ctx, `SELECT subscribed AND NOT EXISTS(SELECT 1 FROM collection_opt_outs WHERE platform='soop' AND channel_id=$1 AND active) FROM collector_channels WHERE channel_id=$1`, channel).Scan(&enabled)
	return enabled, err
}
func (s *PgStore) SetCollectionState(ctx context.Context, channel, state string) error {
	_, err := s.pool.Exec(ctx, `UPDATE collector_channels SET runtime_state=$2,state_at=clock_timestamp() WHERE channel_id=$1`, channel, state)
	return err
}
func enabledTX(ctx context.Context, tx pgx.Tx, channel string) error {
	var enabled bool
	err := tx.QueryRow(ctx, `SELECT subscribed AND NOT EXISTS(SELECT 1 FROM collection_opt_outs WHERE platform='soop' AND channel_id=$1 AND active) FROM collector_channels WHERE channel_id=$1`, channel).Scan(&enabled)
	if err != nil {
		return err
	}
	if !enabled {
		return ErrCollectionDisabled
	}
	return nil
}
func (s *PgStore) AcquireCollection(ctx context.Context, channel, worker string) (OwnerGrant, error) {
	g := OwnerGrant{Channel: channel, Token: uuid.NewString()}
	if !s.allowsCollection(model.PlatformSoop, channel) {
		return g, ErrCollectionDisabled
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return g, err
	}
	defer tx.Rollback(ctx)
	var token *string
	err = tx.QueryRow(ctx, `SELECT generation,owner_token FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel).Scan(&g.Generation, &token)
	if err != nil {
		return g, err
	}
	if err = enabledTX(ctx, tx, channel); err != nil {
		return g, err
	}
	var assigned bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM live_channels WHERE platform='soop' AND channel_id=$1 AND worker_id=$2 AND status='live')`, channel, worker).Scan(&assigned)
	if err != nil {
		return g, err
	}
	if !assigned {
		return g, ErrOwner
	}
	if token != nil {
		var valid bool
		err = tx.QueryRow(ctx, `SELECT valid_until>clock_timestamp() AND closed_at IS NULL FROM collector_owner_grants WHERE token=$1`, *token).Scan(&valid)
		if err != nil {
			return g, err
		}
		if valid {
			return g, ErrOwner
		}
	}
	err = tx.QueryRow(ctx, `INSERT INTO collector_owner_grants(token,channel_id,worker_id,generation,granted_at,valid_until) VALUES($1,$2,$3,$4,clock_timestamp(),clock_timestamp()+interval '30 seconds') RETURNING granted_at,valid_until`, g.Token, channel, worker, g.Generation).Scan(&g.GrantedAt, &g.ValidUntil)
	if err != nil {
		return g, err
	}
	_, err = tx.Exec(ctx, `UPDATE collector_channels SET owner_token=$2,runtime_state='connecting',state_at=clock_timestamp() WHERE channel_id=$1`, channel, g.Token)
	if err != nil {
		return g, err
	}
	return g, tx.Commit(ctx)
}
func (s *PgStore) RenewCollection(ctx context.Context, g OwnerGrant) (time.Time, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx)
	var token *string
	var generation string
	if err = tx.QueryRow(ctx, `SELECT owner_token,generation FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, g.Channel).Scan(&token, &generation); err != nil {
		return time.Time{}, err
	}
	if token == nil || *token != g.Token || generation != g.Generation {
		return time.Time{}, ErrOwner
	}
	if err = enabledTX(ctx, tx, g.Channel); err != nil {
		return time.Time{}, err
	}
	var until time.Time
	err = tx.QueryRow(ctx, `UPDATE collector_owner_grants g SET valid_until=clock_timestamp()+interval '30 seconds' WHERE token=$1 AND closed_at IS NULL AND valid_until>clock_timestamp() AND EXISTS(SELECT 1 FROM live_channels l WHERE l.platform='soop' AND l.channel_id=g.channel_id AND l.worker_id=g.worker_id AND l.status='live') RETURNING valid_until`, g.Token).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return until, ErrOwner
	}
	if err != nil {
		return until, err
	}
	return until, tx.Commit(ctx)
}
func (s *PgStore) ReleaseCollection(ctx context.Context, g OwnerGrant) error {
	_, err := s.pool.Exec(ctx, `UPDATE collector_owner_grants SET closed_at=COALESCE(closed_at,clock_timestamp()) WHERE token=$1`, g.Token)
	return err
}

// AppendDonation accepts a previously fsynced observation with its original grant.
// Grant history permits older accepted spool records, never new writes by old owners.
func (s *PgStore) AppendDonation(ctx context.Context, g OwnerGrant, d model.Donation, acceptedAt time.Time) (model.Donation, error) {
	if err := d.Validate(); err != nil {
		return d, err
	}
	if acceptedAt.Before(time.Now().Add(-30 * 24 * time.Hour)) {
		return d, ErrCursorExpired
	}
	if d.ChannelID != g.Channel || !s.allowsCollection(model.PlatformSoop, d.ChannelID) {
		return d, ErrCollectionDisabled
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback(ctx)
	c, err := scanCollection(tx.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, d.ChannelID))
	if err != nil {
		return d, err
	}
	if g.Generation != c.Generation {
		return d, ErrGeneration
	}
	// The ID tombstone is kept even after payload retention; replay never recreates it.
	var old []byte
	err = tx.QueryRow(ctx, `SELECT payload FROM collector_donations WHERE event_id=$1`, d.EventID).Scan(&old)
	if err == nil {
		original := d
		if old == nil {
			return d, ErrCursorExpired
		}
		if json.Unmarshal(old, &d) != nil {
			return d, errors.New("journal payload invalid")
		}
		if d.ChannelID != original.ChannelID || d.ConnectionEpoch != original.ConnectionEpoch || d.Sequence != original.Sequence || d.Count != original.Count || d.DonorID != original.DonorID || d.Message != original.Message {
			return original, ErrConflict
		}
		return d, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return d, err
	}
	if err = enabledTX(ctx, tx, d.ChannelID); err != nil {
		return d, err
	}
	var valid bool
	err = tx.QueryRow(ctx, `SELECT channel_id=$2 AND generation=$3 AND $4>=granted_at AND $4<=valid_until AND (closed_at IS NULL OR $4<=closed_at) AND $4<=clock_timestamp()+interval '1 second' FROM collector_owner_grants WHERE token=$1`, g.Token, g.Channel, g.Generation, acceptedAt).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, ErrOwner
	}
	if err != nil {
		return d, err
	}
	if !valid {
		return d, ErrOwner
	}
	if c.Current == ^uint64(0) {
		return d, errors.New("journal offset exhausted")
	}
	d.Generation = c.Generation
	d.Offset = c.Current + 1
	// No raw hash dedup. A matching recent observation on another connection is
	// preserved as a separate event with review evidence.
	content := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s", d.DonorID, d.Count, d.Kind, d.Message)))
	key := hex.EncodeToString(content[:])
	d.Identity = "observation_only"
	if d.SourceID != nil {
		return d, errors.New("unverified SOOP source identity")
	}
	if !d.ConnectionStartedAt.IsZero() && d.ObservedAt.Sub(d.ConnectionStartedAt) < 10*time.Second {
		var related string
		matchErr := tx.QueryRow(ctx, `SELECT event_id FROM collector_donations WHERE channel_id=$1 AND content_key=$2 AND connection_epoch<>$3 AND observed_at BETWEEN $4::timestamptz-interval '30 seconds' AND $4 ORDER BY observed_at DESC LIMIT 1`, d.ChannelID, key, d.ConnectionEpoch, d.ObservedAt).Scan(&related)
		if matchErr == nil {
			d.Identity = "reconnect_ambiguous"
			d.Reasons = []string{"matching_observation_after_reconnect"}
			d.Related = []string{related}
		} else if !errors.Is(matchErr, pgx.ErrNoRows) {
			return d, matchErr
		}
	}
	payload, err := json.Marshal(d)
	if err != nil {
		return d, err
	}
	var payloadBytes int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(pg_column_size(payload)),0) FROM collector_donations WHERE channel_id=$1 AND payload IS NOT NULL`, d.ChannelID).Scan(&payloadBytes); err != nil {
		return d, err
	}
	if payloadBytes+int64(len(payload)) > 1<<30 {
		return d, ErrJournalFull
	}
	_, err = tx.Exec(ctx, `INSERT INTO collector_donations(event_id,channel_id,generation,channel_offset,content_key,connection_epoch,observed_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, d.EventID, d.ChannelID, d.Generation, num(d.Offset), key, d.ConnectionEpoch, d.ObservedAt, payload)
	if err != nil {
		return d, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO collector_outbox(event_id,channel_id) VALUES($1,$2)`, d.EventID, d.ChannelID)
	if err != nil {
		return d, err
	}
	_, err = tx.Exec(ctx, `UPDATE collector_channels SET next_offset=$2,last_received_at=$3 WHERE channel_id=$1`, d.ChannelID, num(d.Offset), d.ObservedAt)
	if err != nil {
		return d, err
	}
	return d, tx.Commit(ctx)
}
