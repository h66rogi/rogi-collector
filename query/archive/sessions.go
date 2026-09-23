package archive

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Session struct {
	SessionID          string     `json:"sessionId"`
	StartedAt          time.Time  `json:"startedAt"`
	EndedAt            *time.Time `json:"endedAt"`
	Title              *string    `json:"title"`
	RecordingStartedAt *time.Time `json:"recordingStartedAt"`
	SourceGapDetected  bool       `json:"sourceGapDetected"`
	Complete           bool       `json:"complete"`
}

// ListSessions lists only broadcasts that entered the public archive. The
// current archive session is created on its first accepted chat, so a broadcast
// with no chat has no archive record and is not included here.
func (p *PgArchive) ListSessions(ctx context.Context, beforeAt *time.Time, beforeID string, limit int) ([]Session, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("invalid session page limit")
	}
	if beforeAt != nil {
		if _, err := uuid.Parse(beforeID); err != nil {
			return nil, errors.New("invalid session cursor")
		}
	} else if beforeID != "" {
		return nil, errors.New("invalid session cursor")
	}
	var cursorID any
	if beforeAt != nil {
		cursorID = beforeID
	}
	rows, err := p.pool.Query(ctx, `SELECT a.session_id::text,a.source_started_at,b.ended_at,b.title,a.recording_started_at,b.gap_detected
		FROM archive_sessions a JOIN broadcast_sessions b
		ON b.started_at=a.source_started_at AND b.platform=a.platform AND b.channel_id=a.channel_id AND b.session_seq=a.source_session_seq
		WHERE a.platform='soop' AND a.channel_id=$1
		AND ($2::timestamptz IS NULL OR (a.source_started_at,a.session_id)<($2,$3::uuid))
		ORDER BY a.source_started_at DESC,a.session_id DESC LIMIT $4`, p.channel, beforeAt, cursorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []Session
	for rows.Next() {
		var session Session
		if err := rows.Scan(&session.SessionID, &session.StartedAt, &session.EndedAt, &session.Title, &session.RecordingStartedAt, &session.SourceGapDetected); err != nil {
			return nil, err
		}
		// End-to-end completeness remains false until spool failure gaps and
		// object-store recovery have been validated in production.
		session.Complete = false
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}
