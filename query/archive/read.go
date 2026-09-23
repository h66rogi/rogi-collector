package archive

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrSessionNotFound = errors.New("archive session not found")

type VerifiedGetter interface {
	GetVerified(context.Context, string, string) ([]byte, error)
	Bucket() string
}

type ChatPage struct {
	Rows         []ChatRow
	NextPosition *int64
	HasMore      bool
}

const maxObjectsPerPage = 4

// ReadChatsPage takes a repeatable-read database snapshot for the object index
// and hot rows. This prevents an atomic export commit between the two queries
// from hiding a chat that moved from PostgreSQL to object storage.
func (p *PgArchive) ReadChatsPage(ctx context.Context, sessionID string, after int64, limit int, objects VerifiedGetter) (ChatPage, error) {
	if _, err := uuid.Parse(sessionID); err != nil || after < 0 || limit < 1 || limit > 200 || objects == nil {
		return ChatPage{}, errors.New("invalid archive page request")
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ChatPage{}, err
	}
	defer tx.Rollback(ctx)
	var belongs bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM archive_sessions WHERE session_id=$1 AND platform='soop' AND channel_id=$2)`, sessionID, p.channel).Scan(&belongs); err != nil {
		return ChatPage{}, err
	}
	if !belongs {
		return ChatPage{}, ErrSessionNotFound
	}
	segmentRows, err := tx.Query(ctx, `SELECT first_position,last_position,message_count,object_bucket,object_key,sha256
		FROM archive_segments WHERE session_id=$1 AND last_position>$2 ORDER BY first_position LIMIT $3`, sessionID, after, maxObjectsPerPage+1)
	if err != nil {
		return ChatPage{}, err
	}
	type indexedSegment struct {
		first, last           int64
		count                 int
		bucket, key, checksum string
	}
	var segments []indexedSegment
	for segmentRows.Next() {
		var item indexedSegment
		if err := segmentRows.Scan(&item.first, &item.last, &item.count, &item.bucket, &item.key, &item.checksum); err != nil {
			segmentRows.Close()
			return ChatPage{}, err
		}
		segments = append(segments, item)
	}
	err = segmentRows.Err()
	segmentRows.Close()
	if err != nil {
		return ChatPage{}, err
	}
	var boundary *int64
	if len(segments) > maxObjectsPerPage {
		value := segments[maxObjectsPerPage].first
		boundary = &value
		segments = segments[:maxObjectsPerPage]
	}
	hotRows, err := tx.Query(ctx, `SELECT session_id::text,position,event_id,received_at,user_id_version,public_user_id,display_name,message
		FROM archive_chat_hot WHERE session_id=$1 AND position>$2 AND ($3::bigint IS NULL OR position<$3)
		ORDER BY position LIMIT $4`, sessionID, after, boundary, limit+1)
	if err != nil {
		return ChatPage{}, err
	}
	rows := make([]ChatRow, 0, limit+1)
	for hotRows.Next() {
		var row ChatRow
		if err := hotRows.Scan(&row.SessionID, &row.Position, &row.EventID, &row.ReceivedAt, &row.UserIDVersion, &row.PublicUserID, &row.DisplayName, &row.Message); err != nil {
			hotRows.Close()
			return ChatPage{}, err
		}
		rows = append(rows, row)
	}
	err = hotRows.Err()
	hotRows.Close()
	if err != nil {
		return ChatPage{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ChatPage{}, err
	}
	for _, item := range segments {
		segment := Segment{SessionID: sessionID, FirstPosition: item.first, LastPosition: item.last, Count: item.count, SHA256: item.checksum}
		if item.bucket != objects.Bucket() {
			return ChatPage{}, errors.New("archive bucket mismatch")
		}
		if item.key != segment.ObjectKey() {
			return ChatPage{}, errors.New("archive segment key mismatch")
		}
		body, err := objects.GetVerified(ctx, item.key, item.checksum)
		if err != nil {
			return ChatPage{}, fmt.Errorf("read archive object: %w", err)
		}
		segment.Body = body
		decoded, err := Decode(segment)
		if err != nil {
			return ChatPage{}, err
		}
		for _, row := range decoded {
			if row.Position > after {
				rows = append(rows, row)
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Position < rows[j].Position })
	for i := 1; i < len(rows); i++ {
		if rows[i-1].Position == rows[i].Position {
			return ChatPage{}, errors.New("archive position appears twice")
		}
	}
	hasMore := boundary != nil || len(rows) > limit
	if len(rows) > limit {
		rows = rows[:limit]
	}
	var next *int64
	if hasMore && len(rows) > 0 {
		value := rows[len(rows)-1].Position
		next = &value
	}
	return ChatPage{Rows: rows, NextPosition: next, HasMore: hasMore}, nil
}
