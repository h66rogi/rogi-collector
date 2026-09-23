package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5"
)

// ArchiveChatRecord contains only the fields permitted in the public chat
// archive. In particular, the platform's raw user ID and packet are absent.
type ArchiveChatRecord struct {
	SpoolID       string         `json:"spoolId"`
	EventID       string         `json:"eventId"`
	Platform      model.Platform `json:"platform"`
	ChannelID     string         `json:"channelId"`
	ReceivedAt    time.Time      `json:"receivedAt"`
	UserIDVersion int16          `json:"userIdVersion"`
	PublicUserID  string         `json:"publicUserId"`
	DisplayName   string         `json:"displayName"`
	Message       string         `json:"message"`
}

func (r ArchiveChatRecord) Validate() error {
	if _, err := uuid.Parse(r.SpoolID); err != nil {
		return errors.New("invalid archive spool ID")
	}
	if r.EventID == "" || len(r.EventID) > 255 || r.Platform != model.PlatformSoop || r.ChannelID == "" || r.ReceivedAt.IsZero() || r.UserIDVersion < 1 || len(r.Message) > 65536 || len(r.DisplayName) > 2048 {
		return errors.New("invalid archive chat record")
	}
	decoded, err := hex.DecodeString(r.PublicUserID)
	if err != nil || len(decoded) != 32 {
		return errors.New("invalid public user ID")
	}
	return nil
}

// AppendArchiveChat stores one message transactionally. A session is selected
// using the receive time captured before local spool fsync, never the replay
// time. If the historical interval is absent or ambiguous, the message enters
// the private unassigned queue instead of being attributed to a guessed show.
// The return value reports whether a broadcast session was assigned.
func (s *PgStore) AppendArchiveChat(ctx context.Context, record ArchiveChatRecord) (bool, error) {
	if err := record.Validate(); err != nil {
		return false, err
	}
	if !s.allowsCollection(record.Platform, record.ChannelID) {
		return false, ErrCollectionDisabled
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// Match query/archive.archivePositionLock. Export snapshots serialize with
	// position allocation so a late commit cannot land inside an indexed range.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7266467100622)`); err != nil {
		return false, err
	}
	rows, err := tx.Query(ctx, `
		SELECT started_at, session_seq FROM broadcast_sessions
			WHERE platform=$1 AND channel_id=$2 AND started_observed_at <= $3 + INTERVAL '30 seconds'
			  AND started_at <= $3 + INTERVAL '30 seconds'
		  AND last_seen_at >= $3 - INTERVAL '10 minutes'
		  AND (ended_observed_at IS NULL OR ended_observed_at >= $3)
		ORDER BY started_at DESC LIMIT 2`, string(record.Platform), record.ChannelID, record.ReceivedAt)
	if err != nil {
		return false, err
	}
	type sourceSession struct {
		startedAt time.Time
		seq       int64
	}
	var matches []sourceSession
	for rows.Next() {
		var found sourceSession
		if err := rows.Scan(&found.startedAt, &found.seq); err != nil {
			rows.Close()
			return false, err
		}
		matches = append(matches, found)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	if len(matches) != 1 {
		reason := "no_open_session"
		if len(matches) > 1 {
			reason = "ambiguous_session"
		}
		_, err = tx.Exec(ctx, `INSERT INTO archive_unassigned_chat
			(spool_id,platform,channel_id,event_id,received_at,user_id_version,public_user_id,display_name,message,reason)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (spool_id) DO NOTHING`,
			record.SpoolID, string(record.Platform), record.ChannelID, record.EventID, record.ReceivedAt,
			record.UserIDVersion, record.PublicUserID, record.DisplayName, record.Message, reason)
		if err != nil {
			return false, err
		}
		return false, tx.Commit(ctx)
	}
	source := matches[0]
	_, err = tx.Exec(ctx, `INSERT INTO archive_sessions
		(session_id,platform,channel_id,source_started_at,source_session_seq)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT (platform,channel_id,source_started_at,source_session_seq) DO NOTHING`,
		uuid.NewString(), string(record.Platform), record.ChannelID, source.startedAt, source.seq)
	if err != nil {
		return false, err
	}
	var sessionID string
	err = tx.QueryRow(ctx, `SELECT session_id FROM archive_sessions
		WHERE platform=$1 AND channel_id=$2 AND source_started_at=$3 AND source_session_seq=$4`,
		string(record.Platform), record.ChannelID, source.startedAt, source.seq).Scan(&sessionID)
	if err != nil {
		return false, err
	}
	var position int64
	err = tx.QueryRow(ctx, `INSERT INTO archive_event_ids(session_id,event_id)
			VALUES($1,$2) ON CONFLICT (session_id,event_id) DO NOTHING RETURNING position`, sessionID, record.EventID).Scan(&position)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err := tx.Exec(ctx, `DELETE FROM archive_unassigned_chat WHERE spool_id=$1`, record.SpoolID); err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO archive_chat_hot
		(position,session_id,event_id,received_at,user_id_version,public_user_id,display_name,message)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, position, sessionID, record.EventID, record.ReceivedAt,
		record.UserIDVersion, record.PublicUserID, record.DisplayName, record.Message)
	if err != nil {
		return false, fmt.Errorf("insert hot archive row: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE archive_sessions SET recording_started_at=LEAST(COALESCE(recording_started_at,$2),$2)
		WHERE session_id=$1`, sessionID, record.ReceivedAt)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM archive_unassigned_chat WHERE spool_id=$1`, record.SpoolID); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// ReconcileUnassignedArchiveChats retries records that arrived before their
// broadcast session was committed. Unresolved records remain private and are
// retried later; no ambiguous record is assigned to a guessed broadcast.
func (s *PgStore) ReconcileUnassignedArchiveChats(ctx context.Context, limit int) (int, int, error) {
	if limit < 1 || limit > 1000 || s.collectionChannel == nil || *s.collectionChannel == "" {
		return 0, 0, errors.New("invalid archive reconciliation limit")
	}
	rows, err := s.pool.Query(ctx, `SELECT spool_id::text,event_id,platform,channel_id,received_at,
		user_id_version,public_user_id,display_name,message
		FROM archive_unassigned_chat WHERE platform='soop' AND channel_id=$1 AND next_retry_at<=clock_timestamp()
		ORDER BY next_retry_at,id LIMIT $2`, *s.collectionChannel, limit)
	if err != nil {
		return 0, 0, err
	}
	var records []ArchiveChatRecord
	for rows.Next() {
		var record ArchiveChatRecord
		if err := rows.Scan(&record.SpoolID, &record.EventID, &record.Platform, &record.ChannelID,
			&record.ReceivedAt, &record.UserIDVersion, &record.PublicUserID, &record.DisplayName, &record.Message); err != nil {
			rows.Close()
			return 0, 0, err
		}
		records = append(records, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, 0, err
	}
	assigned := 0
	for _, record := range records {
		matched, err := s.AppendArchiveChat(ctx, record)
		if err != nil {
			return len(records), assigned, err
		}
		if matched {
			assigned++
			continue
		}
		if _, err := s.pool.Exec(ctx, `UPDATE archive_unassigned_chat
			SET next_retry_at=clock_timestamp()+INTERVAL '1 minute' WHERE spool_id=$1`, record.SpoolID); err != nil {
			return len(records), assigned, err
		}
	}
	return len(records), assigned, nil
}

// RecordArchiveSpoolFailure stores a bounded, minute-bucketed signal that a
// received chat could not enter the durable archive spool.
func (s *PgStore) RecordArchiveSpoolFailure(ctx context.Context, platform model.Platform, channelID string, at time.Time) error {
	if at.IsZero() || !s.allowsCollection(platform, channelID) {
		return ErrCollectionDisabled
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO archive_quality_gaps
		(platform,channel_id,started_at,ended_at,reason)
		VALUES($1,$2,date_trunc('minute',$3::timestamptz AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',$3,'spool_save_failed')
		ON CONFLICT (platform,channel_id,started_at,reason)
		DO UPDATE SET ended_at=GREATEST(archive_quality_gaps.ended_at,EXCLUDED.ended_at)`,
		string(platform), channelID, at.UTC())
	return err
}
