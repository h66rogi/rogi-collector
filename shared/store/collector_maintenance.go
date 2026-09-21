package store

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"time"
)

// MaintainCollection bounds durable payloads and recovery metadata. Tombstones
// outlive the maximum spool replay age; missing grants can never authorize replay.
func (s *PgStore) MaintainCollection(ctx context.Context, channel string) error {
	if err := s.ExpireDonations(ctx, channel, 7*24*time.Hour, 30*24*time.Hour, (1<<30)-(1<<20)); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT channel_id FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel); err != nil {
		return err
	}
	for _, q := range []string{
		`DELETE FROM collector_outbox WHERE channel_id=$1 AND created_at<clock_timestamp()-interval '90 days'`,
		`DELETE FROM collector_donations WHERE channel_id=$1 AND payload IS NULL AND stored_at<clock_timestamp()-interval '90 days' AND NOT EXISTS(SELECT 1 FROM collector_outbox o WHERE o.event_id=collector_donations.event_id)`,
		`DELETE FROM collector_owner_grants WHERE channel_id=$1 AND valid_until<clock_timestamp()-interval '90 days' AND token IS DISTINCT FROM (SELECT owner_token FROM collector_channels WHERE channel_id=$1)`,
		`DELETE FROM collector_requests WHERE channel_id=$1 AND created_at<clock_timestamp()-interval '90 days'`,
		`DELETE FROM collector_recovery_audit WHERE channel_id=$1 AND created_at<clock_timestamp()-interval '365 days'`,
	} {
		if _, err = tx.Exec(ctx, q, channel); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RotateJournal is an explicit restore operation, never called during startup.
// It invalidates old cursors/owners while retaining event IDs and observations.
func (s *PgStore) RotateJournal(ctx context.Context, channel, expected, reason string) (string, error) {
	if channel == "" || expected == "" || reason == "" {
		return "", errors.New("channel, expected generation and restore reason required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var current string
	if err = tx.QueryRow(ctx, `SELECT generation FROM collector_channels WHERE channel_id=$1 FOR UPDATE`, channel).Scan(&current); err != nil {
		return "", err
	}
	if current != expected {
		return "", ErrGeneration
	}
	next := uuid.NewString()
	if _, err = tx.Exec(ctx, `UPDATE collector_owner_grants SET closed_at=COALESCE(closed_at,clock_timestamp()) WHERE channel_id=$1`, channel); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `UPDATE collector_donations SET generation=$2,payload=CASE WHEN payload IS NULL THEN NULL ELSE jsonb_set(payload,'{journalGeneration}',to_jsonb($2::text)) END WHERE channel_id=$1 AND generation=$3`, channel, next, current); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `UPDATE collector_channels SET generation=$2,owner_token=NULL,runtime_state='recovery_required',state_at=clock_timestamp() WHERE channel_id=$1`, channel, next); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO collector_recovery_audit(consumer_id,channel_id,request,result) VALUES('collector_restore',$1,jsonb_build_object('reason',$2::text,'previousGeneration',$3::text),jsonb_build_object('newGeneration',$4::text))`, channel, reason, current, next); err != nil {
		return "", err
	}
	return next, tx.Commit(ctx)
}
func (s *PgStore) CollectionHeartbeat(ctx context.Context, g OwnerGrant, state string, last time.Time) error {
	var received *time.Time
	if !last.IsZero() {
		received = &last
	}
	_, err := s.pool.Exec(ctx, `UPDATE collector_channels SET runtime_state=$3,state_at=clock_timestamp(),last_received_at=CASE WHEN $4::timestamptz IS NULL THEN last_received_at ELSE GREATEST(last_received_at,$4) END WHERE channel_id=$1 AND owner_token=$2`, g.Channel, g.Token, state, received)
	return err
}
