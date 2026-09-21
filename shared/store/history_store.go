package store

import (
	"context"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// queryExecer is satisfied by both pgx.Tx and pgxpool.Pool,
// allowing methods to fall back to the pool when tx is nil.
type queryExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BroadcastSession represents a broadcast ON/OFF event.
type BroadcastSession struct {
	StartedAt         time.Time
	Platform          model.Platform
	ChannelID         string
	SessionSeq        int64
	StreamerName      string
	Title             string
	Category          string
	StartedObservedAt time.Time
	EndedAt           *time.Time
	EndedObservedAt   *time.Time
	FirstSeenAt       time.Time
	LastSeenAt        time.Time
	PeakViewerCount   int
	InstanceID        string
	CloseReason       *string
	GapDetected       bool
}

// MetadataHistoryRow represents a metadata change event.
type MetadataHistoryRow struct {
	ValidFrom    time.Time
	Platform     model.Platform
	ChannelID    string
	SessionSeq   int64
	Title        string
	Category     string
	CategoryCode string
	Tags         []string
	ThumbnailURL string
	MetadataHash []byte
	ValidTo      *time.Time
	ObservedAt   time.Time
	InstanceID   string
}

// HistoryStore defines operations for broadcast history.
type HistoryStore interface {
	InsertSession(ctx context.Context, tx pgx.Tx, s *BroadcastSession) error
	CloseSession(ctx context.Context, tx pgx.Tx, platform model.Platform, channelID string, sessionSeq int64, endedAt, observedAt time.Time, reason string) error
	UpdateSessionLastSeen(ctx context.Context, tx pgx.Tx, platform model.Platform, channelID string, sessionSeq int64, lastSeenAt time.Time, peakViewerCount int) error
	MarkSessionGap(ctx context.Context, platform model.Platform, channelID string, sessionSeq int64) error
	ListOpenSessions(ctx context.Context) ([]BroadcastSession, error)
	CloseOrphanedSessions(ctx context.Context) (int, error)
	ListChannelBroadcastHistory(ctx context.Context, platform model.Platform, channelID string, days int, limit int) ([]BroadcastSession, int, error)
	// ListChannelBroadcastHistoryByRange queries broadcast sessions in the half-open
	// time range [from, to). Used by the channel-unified-calendar feature where a
	// caller needs sessions for a specific day or week. Both from and to are required
	// (callers must validate before calling). Limit is clamped to (0, 50] (default 20).
	ListChannelBroadcastHistoryByRange(ctx context.Context, platform model.Platform, channelID string, from, to time.Time, limit int) ([]BroadcastSession, int, error)

	InsertMetadata(ctx context.Context, tx pgx.Tx, m *MetadataHistoryRow) error
	CloseMetadata(ctx context.Context, tx pgx.Tx, platform model.Platform, channelID string, sessionSeq int64, validTo time.Time) error
	GetCurrentMetadataHash(ctx context.Context, platform model.Platform, channelID string, sessionSeq int64) ([]byte, error)
	GetChannelViewerCount(ctx context.Context, platform model.Platform, channelID string) (int, error)
}

// PgHistoryStore implements HistoryStore using PostgreSQL.
type PgHistoryStore struct {
	pool *pgxpool.Pool
}

func NewPgHistoryStore(pool *pgxpool.Pool) *PgHistoryStore {
	return &PgHistoryStore{pool: pool}
}

func (s *PgHistoryStore) executor(tx pgx.Tx) queryExecer {
	if tx != nil {
		return tx
	}
	return s.pool
}

func (s *PgHistoryStore) InsertSession(ctx context.Context, tx pgx.Tx, sess *BroadcastSession) error {
	_, err := s.executor(tx).Exec(ctx, `
		INSERT INTO broadcast_sessions
			(started_at, platform, channel_id, session_seq, streamer_name, title, category,
			 started_observed_at, first_seen_at, last_seen_at, peak_viewer_count, instance_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		sess.StartedAt, string(sess.Platform), sess.ChannelID, sess.SessionSeq,
		sess.StreamerName, sess.Title, sess.Category,
		sess.StartedObservedAt, sess.FirstSeenAt, sess.LastSeenAt,
		sess.PeakViewerCount, sess.InstanceID)
	return err
}

func (s *PgHistoryStore) CloseSession(ctx context.Context, tx pgx.Tx, platform model.Platform, channelID string, sessionSeq int64, endedAt, observedAt time.Time, reason string) error {
	_, err := s.executor(tx).Exec(ctx, `
		UPDATE broadcast_sessions
		SET ended_at = $1, ended_observed_at = $2, close_reason = $3
		WHERE platform = $4 AND channel_id = $5 AND session_seq = $6 AND ended_at IS NULL`,
		endedAt, observedAt, reason, string(platform), channelID, sessionSeq)
	return err
}

func (s *PgHistoryStore) UpdateSessionLastSeen(ctx context.Context, tx pgx.Tx, platform model.Platform, channelID string, sessionSeq int64, lastSeenAt time.Time, peakViewerCount int) error {
	_, err := s.executor(tx).Exec(ctx, `
		UPDATE broadcast_sessions
		SET last_seen_at = $1,
		    peak_viewer_count = GREATEST(peak_viewer_count, $2)
		WHERE platform = $3 AND channel_id = $4 AND session_seq = $5 AND ended_at IS NULL`,
		lastSeenAt, peakViewerCount, string(platform), channelID, sessionSeq)
	return err
}

func (s *PgHistoryStore) MarkSessionGap(ctx context.Context, platform model.Platform, channelID string, sessionSeq int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE broadcast_sessions
		SET gap_detected = true
		WHERE platform = $1 AND channel_id = $2 AND session_seq = $3 AND ended_at IS NULL`,
		string(platform), channelID, sessionSeq)
	return err
}

func (s *PgHistoryStore) ListOpenSessions(ctx context.Context) ([]BroadcastSession, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT started_at, platform, channel_id, session_seq, streamer_name, title, category,
		       started_observed_at, first_seen_at, last_seen_at, peak_viewer_count, instance_id, gap_detected
		FROM broadcast_sessions
		WHERE ended_at IS NULL
		ORDER BY platform, channel_id, session_seq DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []BroadcastSession
	for rows.Next() {
		var s BroadcastSession
		var platformStr string
		if err := rows.Scan(
			&s.StartedAt, &platformStr, &s.ChannelID, &s.SessionSeq,
			&s.StreamerName, &s.Title, &s.Category,
			&s.StartedObservedAt, &s.FirstSeenAt, &s.LastSeenAt,
			&s.PeakViewerCount, &s.InstanceID, &s.GapDetected,
		); err != nil {
			return nil, err
		}
		s.Platform = model.Platform(platformStr)
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

func (s *PgHistoryStore) CloseOrphanedSessions(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE broadcast_sessions bs
		SET ended_at = bs.last_seen_at,
		    ended_observed_at = NOW(),
		    close_reason = 'orphan_cleanup'
		WHERE bs.ended_at IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM live_channels lc
		      WHERE lc.platform = bs.platform
		        AND lc.channel_id = bs.channel_id
		        AND lc.status IN ('live', 'pending')
		  )`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PgHistoryStore) InsertMetadata(ctx context.Context, tx pgx.Tx, m *MetadataHistoryRow) error {
	tags := m.Tags
	if tags == nil {
		tags = []string{}
	}
	_, err := s.executor(tx).Exec(ctx, `
		INSERT INTO broadcast_metadata_history
			(valid_from, platform, channel_id, session_seq, title, category, category_code,
			 tags, thumbnail_url, metadata_hash, observed_at, instance_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		m.ValidFrom, string(m.Platform), m.ChannelID, m.SessionSeq,
		m.Title, m.Category, m.CategoryCode,
		tags, m.ThumbnailURL, m.MetadataHash,
		m.ObservedAt, m.InstanceID)
	return err
}

func (s *PgHistoryStore) CloseMetadata(ctx context.Context, tx pgx.Tx, platform model.Platform, channelID string, sessionSeq int64, validTo time.Time) error {
	_, err := s.executor(tx).Exec(ctx, `
		UPDATE broadcast_metadata_history
		SET valid_to = $1
		WHERE platform = $2 AND channel_id = $3 AND session_seq = $4 AND valid_to IS NULL`,
		validTo, string(platform), channelID, sessionSeq)
	return err
}

func (s *PgHistoryStore) GetCurrentMetadataHash(ctx context.Context, platform model.Platform, channelID string, sessionSeq int64) ([]byte, error) {
	var hash []byte
	err := s.pool.QueryRow(ctx, `
		SELECT metadata_hash FROM broadcast_metadata_history
		WHERE platform = $1 AND channel_id = $2 AND session_seq = $3 AND valid_to IS NULL
		ORDER BY valid_from DESC LIMIT 1`,
		string(platform), channelID, sessionSeq).Scan(&hash)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return hash, err
}

func (s *PgHistoryStore) GetChannelViewerCount(ctx context.Context, platform model.Platform, channelID string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT viewer_count FROM live_channels
		WHERE platform = $1 AND channel_id = $2`,
		string(platform), channelID).Scan(&count)
	return count, err
}

func (s *PgHistoryStore) ListChannelBroadcastHistory(ctx context.Context, platform model.Platform, channelID string, days int, limit int) ([]BroadcastSession, int, error) {
	if days <= 0 {
		days = 30
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	since := time.Now().AddDate(0, 0, -days)

	// Count total
	var total int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM broadcast_sessions
		WHERE platform = $1 AND channel_id = $2 AND started_at >= $3`,
		string(platform), channelID, since).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT started_at, platform, channel_id, session_seq, streamer_name, title, category,
		       started_observed_at, first_seen_at, last_seen_at, peak_viewer_count, instance_id,
		       ended_at, ended_observed_at, close_reason, gap_detected
		FROM broadcast_sessions
		WHERE platform = $1 AND channel_id = $2 AND started_at >= $3
		ORDER BY started_at DESC
		LIMIT $4`,
		string(platform), channelID, since, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var sessions []BroadcastSession
	for rows.Next() {
		var sess BroadcastSession
		var platformStr string
		if err := rows.Scan(
			&sess.StartedAt, &platformStr, &sess.ChannelID, &sess.SessionSeq,
			&sess.StreamerName, &sess.Title, &sess.Category,
			&sess.StartedObservedAt, &sess.FirstSeenAt, &sess.LastSeenAt,
			&sess.PeakViewerCount, &sess.InstanceID,
			&sess.EndedAt, &sess.EndedObservedAt, &sess.CloseReason, &sess.GapDetected,
		); err != nil {
			return nil, 0, err
		}
		sess.Platform = model.Platform(platformStr)
		sessions = append(sessions, sess)
	}
	return sessions, total, rows.Err()
}

// ListChannelBroadcastHistoryByRange returns broadcast sessions whose started_at
// falls in the half-open range [from, to). The from/to predicates are pushed to
// the partition key (broadcast_sessions is RANGE partitioned by started_at), so
// the planner prunes irrelevant monthly partitions instead of scanning all.
// Existing index broadcast_sessions_lookup_idx (platform, channel_id, session_seq,
// started_at DESC) is reused — no new index needed.
func (s *PgHistoryStore) ListChannelBroadcastHistoryByRange(ctx context.Context, platform model.Platform, channelID string, from, to time.Time, limit int) ([]BroadcastSession, int, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	// Count total in the range. Same predicates so partition prune applies.
	var total int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM broadcast_sessions
		WHERE platform = $1 AND channel_id = $2
		  AND started_at >= $3 AND started_at < $4`,
		string(platform), channelID, from, to).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT started_at, platform, channel_id, session_seq, streamer_name, title, category,
		       started_observed_at, first_seen_at, last_seen_at, peak_viewer_count, instance_id,
		       ended_at, ended_observed_at, close_reason, gap_detected
		FROM broadcast_sessions
		WHERE platform = $1 AND channel_id = $2
		  AND started_at >= $3 AND started_at < $4
		ORDER BY started_at DESC
		LIMIT $5`,
		string(platform), channelID, from, to, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var sessions []BroadcastSession
	for rows.Next() {
		var sess BroadcastSession
		var platformStr string
		if err := rows.Scan(
			&sess.StartedAt, &platformStr, &sess.ChannelID, &sess.SessionSeq,
			&sess.StreamerName, &sess.Title, &sess.Category,
			&sess.StartedObservedAt, &sess.FirstSeenAt, &sess.LastSeenAt,
			&sess.PeakViewerCount, &sess.InstanceID,
			&sess.EndedAt, &sess.EndedObservedAt, &sess.CloseReason, &sess.GapDetected,
		); err != nil {
			return nil, 0, err
		}
		sess.Platform = model.Platform(platformStr)
		sessions = append(sessions, sess)
	}
	return sessions, total, rows.Err()
}

var _ HistoryStore = (*PgHistoryStore)(nil)
