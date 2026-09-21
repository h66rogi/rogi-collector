package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5"
)

type ConsumerProgress struct {
	Generation        string
	Baseline, Offered uint64
	Ack               *uint64
	Revision          uint64
}

func progressTX(ctx context.Context, tx pgx.Tx, consumer, channel string) (ConsumerProgress, error) {
	var p ConsumerProgress
	var base, offered, revision string
	var ack *string
	err := tx.QueryRow(ctx, `SELECT generation,baseline::text,offered_offset::text,ack_offset::text,recovery_revision::text FROM collector_consumers WHERE consumer_id=$1 AND channel_id=$2 FOR UPDATE`, consumer, channel).Scan(&p.Generation, &base, &offered, &ack, &revision)
	if err != nil {
		return p, err
	}
	p.Revision, err = parseNum(revision)
	if err != nil {
		return p, err
	}
	p.Baseline, err = parseNum(base)
	if err != nil {
		return p, err
	}
	p.Offered, err = parseNum(offered)
	if err != nil {
		return p, err
	}
	if ack != nil {
		v, e := parseNum(*ack)
		if e != nil {
			return p, e
		}
		p.Ack = &v
	}
	return p, nil
}
func (s *PgStore) ConsumerProgress(ctx context.Context, consumer, channel string) (ConsumerProgress, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ConsumerProgress{}, err
	}
	defer tx.Rollback(ctx)
	p, err := progressTX(ctx, tx, consumer, channel)
	return p, err
}

// ReadDonations records only contiguous offered ranges. An initial cursor is explicit.
func (s *PgStore) ReadDonations(ctx context.Context, consumer, channel, generation string, after, revision uint64, limit int) ([]model.Donation, CollectionState, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, CollectionState{}, err
	}
	defer tx.Rollback(ctx)
	c, err := scanCollection(tx.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel))
	if err != nil {
		return nil, c, err
	}
	if generation != c.Generation {
		return nil, c, ErrGeneration
	}
	if after < c.ExpiredThrough {
		return nil, c, ErrCursorExpired
	}
	if after > c.Current {
		return nil, c, ErrCursor
	}
	// Brand new consumers may start at the retained beginning, never silently skip.
	_, err = tx.Exec(ctx, `INSERT INTO collector_consumers(consumer_id,channel_id,generation,baseline,offered_offset) VALUES($1,$2,$3,$4,$4) ON CONFLICT DO NOTHING`, consumer, channel, generation, num(c.ExpiredThrough))
	if err != nil {
		return nil, c, err
	}
	p, err := progressTX(ctx, tx, consumer, channel)
	if err != nil {
		return nil, c, err
	}
	if p.Generation != generation {
		return nil, c, ErrGeneration
	}
	if p.Revision != revision {
		return nil, c, ErrRevision
	}
	if after < p.Baseline || after > p.Offered {
		return nil, c, ErrCursor
	}
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := tx.Query(ctx, `SELECT payload FROM collector_donations WHERE channel_id=$1 AND generation=$2 AND channel_offset>$3 AND payload IS NOT NULL ORDER BY channel_offset LIMIT $4`, channel, generation, num(after), limit)
	if err != nil {
		return nil, c, err
	}
	var result []model.Donation
	next := after
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		var d model.Donation
		if err = json.Unmarshal(raw, &d); err != nil {
			break
		}
		if d.Offset != next+1 {
			err = ErrCursorExpired
			break
		}
		next = d.Offset
		result = append(result, d)
	}
	rows.Close()
	if err != nil {
		return nil, c, err
	}
	if err = rows.Err(); err != nil {
		return nil, c, err
	}
	if next > p.Offered {
		_, err = tx.Exec(ctx, `UPDATE collector_consumers SET offered_offset=$3 WHERE consumer_id=$1 AND channel_id=$2`, consumer, channel, num(next))
		if err != nil {
			return nil, c, err
		}
	}
	return result, c, tx.Commit(ctx)
}
func (s *PgStore) AckDonations(ctx context.Context, consumer, channel, generation string, offset, revision uint64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	c, err := scanCollection(tx.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel))
	if err != nil {
		return err
	}
	if c.Generation != generation {
		return ErrGeneration
	}
	if offset < c.ExpiredThrough {
		return ErrCursorExpired
	}
	p, err := progressTX(ctx, tx, consumer, channel)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCursor
	}
	if err != nil {
		return err
	}
	if p.Generation != generation {
		return ErrGeneration
	}
	if p.Revision != revision {
		return ErrRevision
	}
	if offset <= p.Baseline || offset > p.Offered || (p.Ack != nil && offset < *p.Ack) {
		return ErrCursor
	}
	_, err = tx.Exec(ctx, `UPDATE collector_consumers SET ack_offset=$3 WHERE consumer_id=$1 AND channel_id=$2`, consumer, channel, num(offset))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func requestHash(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }
func requestResult(ctx context.Context, tx pgx.Tx, consumer, channel, op, key string, body []byte) ([]byte, error) {
	var hash string
	var result []byte
	err := tx.QueryRow(ctx, `SELECT body_hash,result FROM collector_requests WHERE consumer_id=$1 AND channel_id=$2 AND operation=$3 AND request_key=$4`, consumer, channel, op, key).Scan(&hash, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if hash != requestHash(body) {
		return nil, ErrConflict
	}
	return result, nil
}
func saveRequest(ctx context.Context, tx pgx.Tx, consumer, channel, op, key string, body, result []byte) error {
	_, err := tx.Exec(ctx, `INSERT INTO collector_requests(consumer_id,channel_id,operation,request_key,body_hash,result) VALUES($1,$2,$3,$4,$5,$6)`, consumer, channel, op, key, requestHash(body), result)
	return err
}
func (s *PgStore) SetSubscription(ctx context.Context, consumer, channel, key string, enabled bool) (bool, error) {
	if key == "" || len(key) > 200 {
		return false, ErrCursor
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	c, err := scanCollection(tx.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel))
	if err != nil {
		return false, err
	}
	body, _ := json.Marshal(enabled)
	saved, err := requestResult(ctx, tx, consumer, channel, "subscription", key, body)
	if err != nil {
		return false, err
	}
	if saved != nil {
		var old bool
		if json.Unmarshal(saved, &old) != nil {
			return false, ErrConflict
		}
		return old, tx.Commit(ctx)
	}
	if enabled {
		var opted bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM collection_opt_outs WHERE platform='soop' AND channel_id=$1 AND active)`, channel).Scan(&opted); err != nil {
			return false, err
		}
		if opted {
			return false, ErrCollectionDisabled
		}
	}
	_, err = tx.Exec(ctx, `UPDATE collector_channels SET subscribed=$2 WHERE channel_id=$1`, c.Channel, enabled)
	if err != nil {
		return false, err
	}
	if !enabled {
		_, err = tx.Exec(ctx, `UPDATE collector_owner_grants SET closed_at=COALESCE(closed_at,clock_timestamp()) WHERE channel_id=$1`, channel)
		if err != nil {
			return false, err
		}
	}
	if err = saveRequest(ctx, tx, consumer, channel, "subscription", key, body, body); err != nil {
		return false, err
	}
	return enabled, tx.Commit(ctx)
}

type RecoveryDecision struct {
	PreviousGeneration string `json:"previousGeneration"`
	NewGeneration      string `json:"newGeneration"`
	PreviousOffset     uint64 `json:"previousOffset,string"`
	ResumeFrom         uint64 `json:"resumeFrom,string"`
	Revision           uint64 `json:"revision,string"`
}

// ResolveRecovery persists the complete request (including reason/operator/loss
// ranges supplied by the authenticated handler) in its idempotency audit record.
func (s *PgStore) ResolveRecovery(ctx context.Context, consumer, channel, key, previous, next string, resume, expected uint64, request []byte) (RecoveryDecision, error) {
	var result RecoveryDecision
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	c, err := scanCollection(tx.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel))
	if err != nil {
		return result, err
	}
	saved, err := requestResult(ctx, tx, consumer, channel, "recovery", key, request)
	if err != nil {
		return result, err
	}
	if saved != nil {
		err = json.Unmarshal(saved, &result)
		return result, err
	}
	if next != c.Generation {
		return result, ErrGeneration
	}
	if resume < c.ExpiredThrough || resume > c.Current {
		return result, ErrCursor
	}
	p, err := progressTX(ctx, tx, consumer, channel)
	if errors.Is(err, pgx.ErrNoRows) {
		p = ConsumerProgress{Generation: previous}
		err = nil
	}
	if err != nil {
		return result, err
	}
	if p.Generation != previous {
		return result, ErrGeneration
	}
	if expected == ^uint64(0) {
		return result, ErrRevision
	}
	if expected != p.Revision {
		return result, ErrRevision
	}
	old := p.Baseline
	if p.Ack != nil {
		old = *p.Ack
	}
	if previous == next && resume < old {
		return result, ErrCursor
	}
	// Skipping retained events in the same generation requires an explicit
	// inclusive loss range in the audited request, not only a free-text reason.
	if previous == next && resume > old {
		var audit struct {
			Ranges []struct {
				Start, End struct {
					Generation string `json:"journalGeneration"`
					Offset     uint64 `json:"channelOffset,string"`
				}
			} `json:"unrecoveredRanges"`
		}
		if json.Unmarshal(request, &audit) != nil {
			return result, ErrCursor
		}
		covered := old
		for _, r := range audit.Ranges {
			if r.Start.Generation != previous || r.End.Generation != previous || r.Start.Offset > r.End.Offset {
				return result, ErrCursor
			}
			if r.Start.Offset > covered+1 {
				return result, ErrCursor
			}
			if r.End.Offset > covered {
				covered = r.End.Offset
			}
		}
		if covered != resume {
			return result, ErrCursor
		}
	}
	result = RecoveryDecision{PreviousGeneration: previous, NewGeneration: next, PreviousOffset: old, ResumeFrom: resume, Revision: expected + 1}
	_, err = tx.Exec(ctx, `INSERT INTO collector_consumers(consumer_id,channel_id,generation,baseline,offered_offset,recovery_revision) VALUES($1,$2,$3,$4,$4,$5) ON CONFLICT(consumer_id,channel_id) DO UPDATE SET generation=$3,baseline=$4,offered_offset=$4,ack_offset=NULL,recovery_revision=$5`, consumer, channel, next, num(resume), num(result.Revision))
	if err != nil {
		return result, err
	}
	payload, _ := json.Marshal(result)
	if err = saveRequest(ctx, tx, consumer, channel, "recovery", key, request, payload); err != nil {
		return result, err
	}
	// Retain the actual decision request, not only its hash.
	_, err = tx.Exec(ctx, `INSERT INTO collector_recovery_audit(consumer_id,channel_id,request,result) VALUES($1,$2,$3,$4)`, consumer, channel, request, payload)
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}
func (s *PgStore) DispatchDonationOutbox(ctx context.Context, notify func(context.Context, string) error) error {
	rows, err := s.pool.Query(ctx, `SELECT event_id,channel_id FROM collector_outbox ORDER BY created_at LIMIT 100`)
	if err != nil {
		return err
	}
	type row struct{ id, channel string }
	var pending []row
	for rows.Next() {
		var r row
		if err = rows.Scan(&r.id, &r.channel); err != nil {
			break
		}
		pending = append(pending, r)
	}
	rows.Close()
	if err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		return err
	}
	for _, r := range pending {
		if err = notify(ctx, r.channel); err != nil {
			return err
		}
		if _, err = s.pool.Exec(ctx, `DELETE FROM collector_outbox WHERE event_id=$1`, r.id); err != nil {
			return err
		}
	}
	return nil
}

// ExpireDonations drops only a prefix of payloads, retaining ID tombstones to
// prevent old spools from resurrecting consumed donations. Hard limits are explicit.
func (s *PgStore) ExpireDonations(ctx context.Context, channel string, minAge, maxAge time.Duration, maxPayloadBytes int64) error {
	if minAge < 0 || maxAge < minAge || maxPayloadBytes <= 0 {
		return errors.New("invalid retention policy")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	c, err := scanCollection(tx.QueryRow(ctx, `SELECT `+collectionColumns+` FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel))
	if err != nil {
		return err
	}
	var total int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(sum(pg_column_size(payload)),0) FROM collector_donations WHERE channel_id=$1 AND payload IS NOT NULL`, channel).Scan(&total)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT channel_offset::text,stored_at,pg_column_size(payload) FROM collector_donations WHERE channel_id=$1 AND generation=$2 AND payload IS NOT NULL ORDER BY channel_offset LIMIT 500`, channel, c.Generation)
	if err != nil {
		return err
	}
	type candidate struct {
		offset uint64
		at     time.Time
		size   int64
	}
	var candidates []candidate
	for rows.Next() {
		var v candidate
		var n string
		if err = rows.Scan(&n, &v.at, &v.size); err != nil {
			break
		}
		v.offset, err = parseNum(n)
		if err != nil {
			break
		}
		candidates = append(candidates, v)
	}
	rows.Close()
	if err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		return err
	}
	through := c.ExpiredThrough
	for _, v := range candidates {
		forced := time.Since(v.at) > maxAge || total > maxPayloadBytes
		if !forced {
			if time.Since(v.at) < minAge {
				break
			}
			var unacked bool
			err = tx.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM collector_consumers WHERE channel_id=$1) OR EXISTS(SELECT 1 FROM collector_consumers WHERE channel_id=$1 AND (generation<>$2 OR COALESCE(ack_offset,baseline)<$3))`, channel, c.Generation, num(v.offset)).Scan(&unacked)
			if err != nil {
				return err
			}
			if unacked {
				break
			}
		}
		through = v.offset
		total -= v.size
	}
	if through > c.ExpiredThrough {
		_, err = tx.Exec(ctx, `UPDATE collector_donations SET payload=NULL WHERE channel_id=$1 AND generation=$2 AND channel_offset<=$3`, channel, c.Generation, num(through))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE collector_channels SET expired_through=$2 WHERE channel_id=$1`, channel, num(through))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
