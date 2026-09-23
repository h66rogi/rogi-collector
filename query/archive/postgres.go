package archive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Position allocation and export snapshots share this advisory lock with the
// worker's append transaction. A later committed chat cannot appear inside a
// segment position range that was already selected for upload.
const archivePositionLock int64 = 7266467100622

type PgArchive struct {
	pool    *pgxpool.Pool
	channel string
}

func NewPgArchive(pool *pgxpool.Pool, channel string) (*PgArchive, error) {
	if pool == nil || channel == "" {
		return nil, errors.New("archive database and channel required")
	}
	return &PgArchive{pool: pool, channel: channel}, nil
}

// NextBatch selects one segment candidate only after its first row has aged
// past cutoff. Subsequent rows are selected by position without an age filter
// so the segment never skips a newer row inside its indexed position range.
func (p *PgArchive) NextBatch(ctx context.Context, cutoff time.Time, limit int) ([]ChatRow, error) {
	if limit < 1 || limit > MaxSegmentRows {
		return nil, errors.New("invalid export batch limit")
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, archivePositionLock); err != nil {
		return nil, err
	}
	var sessionID string
	var first int64
	err = tx.QueryRow(ctx, `SELECT h.session_id::text,h.position
		FROM archive_chat_hot h JOIN archive_sessions a ON a.session_id=h.session_id
		WHERE a.platform='soop' AND a.channel_id=$1 AND h.received_at<$2
		AND NOT EXISTS(SELECT 1 FROM archive_segments s WHERE s.session_id=h.session_id AND h.position BETWEEN s.first_position AND s.last_position)
		ORDER BY h.position LIMIT 1`, p.channel, cutoff).Scan(&sessionID, &first)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}
	var nextIndexed *int64
	err = tx.QueryRow(ctx, `SELECT MIN(first_position) FROM archive_segments WHERE session_id=$1 AND first_position>$2`, sessionID, first).Scan(&nextIndexed)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT session_id::text,position,event_id,received_at,user_id_version,public_user_id,display_name,message
		FROM archive_chat_hot WHERE session_id=$1 AND position>=$2 AND ($3::bigint IS NULL OR position<$3)
		ORDER BY position LIMIT $4`, sessionID, first, nextIndexed, limit)
	if err != nil {
		return nil, err
	}
	var result []ChatRow
	for rows.Next() {
		var row ChatRow
		if err := rows.Scan(&row.SessionID, &row.Position, &row.EventID, &row.ReceivedAt, &row.UserIDVersion, &row.PublicUserID, &row.DisplayName, &row.Message); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("archive candidate vanished")
	}
	return result, tx.Commit(ctx)
}

// CommitSegment is called only after the exact object has been uploaded and
// read back. Index insertion and hot-row deletion are one PostgreSQL commit;
// the permanent event-ID index remains intact for replay deduplication.
func (p *PgArchive) CommitSegment(ctx context.Context, verified VerifiedSegment) error {
	segment, bucket := verified.segment, verified.bucket
	if bucket == "" {
		return errors.New("archive bucket required")
	}
	if _, err := Decode(segment); err != nil {
		return fmt.Errorf("unverified segment: %w", err)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, archivePositionLock); err != nil {
		return err
	}
	var owned bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM archive_sessions WHERE session_id=$1 AND platform='soop' AND channel_id=$2)`, segment.SessionID, p.channel).Scan(&owned); err != nil {
		return err
	}
	if !owned {
		return errors.New("archive session outside configured channel")
	}
	var existingKey, existingSHA, existingBucket string
	var existingCount int
	var existingFirst, existingLast int64
	err = tx.QueryRow(ctx, `SELECT object_key,sha256,object_bucket,message_count,first_position,last_position FROM archive_segments
		WHERE session_id=$1 AND NOT (last_position<$2 OR first_position>$3)
		LIMIT 1`, segment.SessionID, segment.FirstPosition, segment.LastPosition).Scan(&existingKey, &existingSHA, &existingBucket, &existingCount, &existingFirst, &existingLast)
	if err == nil {
		if existingKey == segment.ObjectKey() && existingSHA == segment.SHA256 && existingBucket == bucket && existingCount == segment.Count && existingFirst == segment.FirstPosition && existingLast == segment.LastPosition {
			return tx.Commit(ctx)
		}
		return errors.New("overlapping archive segment")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO archive_segments
		(session_id,first_position,last_position,message_count,object_bucket,object_key,sha256,byte_length)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, segment.SessionID, segment.FirstPosition, segment.LastPosition, segment.Count, bucket, segment.ObjectKey(), segment.SHA256, len(segment.Body))
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM archive_chat_hot WHERE session_id=$1 AND position BETWEEN $2 AND $3`, segment.SessionID, segment.FirstPosition, segment.LastPosition)
	if err != nil {
		return err
	}
	if int(tag.RowsAffected()) != segment.Count {
		return fmt.Errorf("archive hot row count changed: got %d want %d", tag.RowsAffected(), segment.Count)
	}
	return tx.Commit(ctx)
}
