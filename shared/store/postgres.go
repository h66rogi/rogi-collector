package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ChannelStore defines operations for managing live channels.
type ChannelStore interface {
	UpsertLiveChannels(ctx context.Context, channels []model.LiveChannel) error
	MarkEndedChannels(ctx context.Context, platform model.Platform, activeChannelIDs []string) ([]string, error)
	ListPendingChannels(ctx context.Context, limit int) ([]model.LiveChannel, error)
	ListWorkerChannels(ctx context.Context, workerID string) ([]model.LiveChannel, error)
	AssignChannel(ctx context.Context, channelID int64, workerID string) error
	UnassignWorkerChannels(ctx context.Context, workerID string) error
	UnassignChannel(ctx context.Context, platform model.Platform, channelID string, workerID string) error
	UnassignDeadWorkerChannels(ctx context.Context) (int64, error)
	MarkChannelEnded(ctx context.Context, channelID int64) error
	GetChannel(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error)

	// Handoff / graceful-drain operations
	ReassignChannel(ctx context.Context, channelID int64, newWorkerID, oldWorkerID string) (bool, error)
	ListDrainingWorkerChannels(ctx context.Context) ([]model.LiveChannel, error)
	ListStuckHandoffs(ctx context.Context, threshold time.Duration) ([]model.LiveChannel, error)
	ListPendingHandoffs(ctx context.Context, drainingWorkerID string) ([]model.LiveChannel, error)

	// Capacity management
	CountAssignedByWorker(ctx context.Context) (map[string]int, error)

	// Cleanup
	ListEndedChannelsBefore(ctx context.Context, before time.Time, limit int) ([]model.LiveChannel, error)
	ListActiveChannelKeys(ctx context.Context) ([]string, error)
	ListAllKnownChannelKeys(ctx context.Context) ([]string, error)
}

// WorkerStore defines operations for managing workers.
type WorkerStore interface {
	RegisterWorker(ctx context.Context, worker model.Worker) error
	UpdateWorkerStatus(ctx context.Context, workerID string, status string) error
	ListAliveWorkers(ctx context.Context) ([]model.Worker, error)
	ListAllWorkers(ctx context.Context) ([]model.Worker, error)
	ListWorkersByStatus(ctx context.Context, status string) ([]model.Worker, error)
	DeleteWorker(ctx context.Context, workerID string) error
}

// PgStore implements ChannelStore and WorkerStore using PostgreSQL.
type PgStore struct {
	pool              *pgxpool.Pool
	collectionChannel *string
}

// NewPgStore creates a new PgStore with the given connection pool.
func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{pool: pool}
}

// SetCollectionChannel restricts this role to a configured SOOP channel; empty means disabled.
// Call once during startup, before sharing the store across goroutines.
func (s *PgStore) SetCollectionChannel(channel string) { s.collectionChannel = &channel }
func (s *PgStore) allowsCollection(platform model.Platform, channel string) bool {
	return s.collectionChannel == nil || (*s.collectionChannel != "" && platform == model.PlatformSoop && channel == *s.collectionChannel)
}

// UpsertLiveChannels inserts or updates channels in batch. If a channel was
// previously ended and is seen again, its status is reset to pending.
func (s *PgStore) UpsertLiveChannels(ctx context.Context, channels []model.LiveChannel) error {
	if len(channels) == 0 {
		return nil
	}

	// Deduplicate by (platform, channel_id) — PostgreSQL ON CONFLICT cannot
	// update the same row twice in a single statement.
	seen := make(map[string]int, len(channels))
	deduped := make([]model.LiveChannel, 0, len(channels))
	for _, ch := range channels {
		key := string(ch.Platform) + ":" + ch.ChannelID
		if idx, ok := seen[key]; ok {
			deduped[idx] = ch // keep last occurrence (higher viewer count wins)
			continue
		}
		seen[key] = len(deduped)
		deduped = append(deduped, ch)
	}

	var b strings.Builder
	args := make([]interface{}, 0, len(deduped)*4)

	b.WriteString(`INSERT INTO live_channels (platform, channel_id, streamer_name, viewer_count, status, last_seen_at)
VALUES `)

	for i, ch := range deduped {
		if i > 0 {
			b.WriteString(", ")
		}
		base := i * 4
		fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, 'pending', NOW())", base+1, base+2, base+3, base+4)
		args = append(args, string(ch.Platform), ch.ChannelID, ch.StreamerName, ch.ViewerCount)
	}

	b.WriteString(` ON CONFLICT (platform, channel_id) DO UPDATE SET
		streamer_name = EXCLUDED.streamer_name,
		viewer_count = EXCLUDED.viewer_count,
		last_seen_at = NOW(),
		status = CASE
			WHEN live_channels.status = 'ended' THEN 'pending'
			ELSE live_channels.status
		END,
		ended_at = CASE
			WHEN live_channels.status = 'ended' THEN NULL
			ELSE live_channels.ended_at
		END`)

	_, err := s.pool.Exec(ctx, b.String(), args...)
	return err
}

// MarkEndedChannels marks channels as ended if they are not in the active list
// for the given platform. Returns the channel_ids that were actually ended.
func (s *PgStore) MarkEndedChannels(ctx context.Context, platform model.Platform, activeChannelIDs []string) ([]string, error) {
	var rows pgx.Rows
	var err error

	if len(activeChannelIDs) == 0 {
		rows, err = s.pool.Query(ctx, `
			UPDATE live_channels
			SET status = 'ended', ended_at = NOW(), worker_id = NULL
			WHERE platform = $1 AND status != 'ended'
			RETURNING channel_id`,
			string(platform))
	} else {
		args := make([]interface{}, 0, len(activeChannelIDs)+1)
		args = append(args, string(platform))
		placeholders := make([]string, len(activeChannelIDs))
		for i, id := range activeChannelIDs {
			args = append(args, id)
			placeholders[i] = fmt.Sprintf("$%d", i+2)
		}
		query := fmt.Sprintf(`
			UPDATE live_channels
			SET status = 'ended', ended_at = NOW(), worker_id = NULL
			WHERE platform = $1 AND status != 'ended'
			AND channel_id NOT IN (%s)
			RETURNING channel_id`, strings.Join(placeholders, ", "))
		rows, err = s.pool.Query(ctx, query, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var endedIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		endedIDs = append(endedIDs, id)
	}
	return endedIDs, rows.Err()
}

// ListPendingChannels returns channels with status=pending and no assigned worker.
func (s *PgStore) ListPendingChannels(ctx context.Context, limit int) ([]model.LiveChannel, error) {
	if s.collectionChannel != nil {
		rows, err := s.pool.Query(ctx, `SELECT id,platform,channel_id,streamer_name,viewer_count,status,worker_id,discovered_at,last_seen_at,ended_at,handoff_from,handoff_status,handoff_started_at FROM live_channels WHERE status='pending' AND worker_id IS NULL AND platform='soop' AND channel_id=$1 AND EXISTS(SELECT 1 FROM collector_channels c WHERE c.channel_id=live_channels.channel_id AND c.subscribed) AND NOT EXISTS(SELECT 1 FROM collection_opt_outs co WHERE co.platform='soop' AND co.channel_id=live_channels.channel_id AND co.active) ORDER BY viewer_count DESC LIMIT $2`, *s.collectionChannel, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanChannelsWithHandoff(rows)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE status = 'pending' AND worker_id IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM collection_opt_outs co
		      WHERE co.active = TRUE
		        AND co.platform = live_channels.platform
		        AND co.channel_id = live_channels.channel_id
		  )
		ORDER BY viewer_count DESC
		LIMIT $1`, limit)
	if isUndefinedTableError(err) && s.collectionChannel == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT id, platform, channel_id, streamer_name, viewer_count, status,
			       worker_id, discovered_at, last_seen_at, ended_at,
			       handoff_from, handoff_status, handoff_started_at
			FROM live_channels
			WHERE status = 'pending' AND worker_id IS NULL
			ORDER BY viewer_count DESC
			LIMIT $1`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// ListWorkerChannels returns live channels currently assigned to a worker.
func (s *PgStore) ListWorkerChannels(ctx context.Context, workerID string) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE worker_id = $1 AND status = 'live'
		  AND NOT EXISTS (
		      SELECT 1 FROM collection_opt_outs co
		      WHERE co.active = TRUE
		        AND co.platform = live_channels.platform
		        AND co.channel_id = live_channels.channel_id
		  )
		ORDER BY viewer_count DESC, id ASC`, workerID)
	if isUndefinedTableError(err) {
		rows, err = s.pool.Query(ctx, `
			SELECT id, platform, channel_id, streamer_name, viewer_count, status,
			       worker_id, discovered_at, last_seen_at, ended_at,
			       handoff_from, handoff_status, handoff_started_at
			FROM live_channels
			WHERE worker_id = $1 AND status = 'live'
			ORDER BY viewer_count DESC, id ASC`, workerID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// AssignChannel assigns a channel to a worker and sets status to live.
// Stale handoff fields are cleared so a new assignment never inherits a
// previous handoff's dedup-mode flag.
func (s *PgStore) AssignChannel(ctx context.Context, channelID int64, workerID string) error {
	if s.collectionChannel != nil {
		ct, err := s.pool.Exec(ctx, `UPDATE live_channels SET worker_id=$1,status='live',handoff_from=NULL,handoff_status=NULL,handoff_started_at=NULL WHERE id=$2 AND platform='soop' AND channel_id=$3 AND $3<>''`, workerID, channelID, *s.collectionChannel)
		if err != nil {
			return err
		}
		if ct.RowsAffected() != 1 {
			return fmt.Errorf("collection scope denied")
		}
		return nil
	}

	_, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET worker_id = $1, status = 'live',
		    handoff_from = NULL, handoff_status = NULL, handoff_started_at = NULL
		WHERE id = $2`,
		workerID, channelID)
	return err
}

// UnassignWorkerChannels resets all channels assigned to a dead worker back to pending.
// Stale handoff fields are cleared so the next assignee starts in a clean state.
func (s *PgStore) UnassignWorkerChannels(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET worker_id = NULL, status = 'pending',
		    handoff_from = NULL, handoff_status = NULL, handoff_started_at = NULL
		WHERE worker_id = $1 AND status != 'ended'`,
		workerID)
	return err
}

// UnassignChannel resets a single channel back to pending so it can be reassigned.
// The workerID guard prevents stale events from unassigning channels already reassigned
// to a different worker.
func (s *PgStore) UnassignChannel(ctx context.Context, platform model.Platform, channelID string, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET worker_id = NULL, status = 'pending', handoff_status = NULL, handoff_from = NULL, handoff_started_at = NULL
		WHERE platform = $1 AND channel_id = $2 AND worker_id = $3 AND status != 'ended'`,
		string(platform), channelID, workerID)
	return err
}

// MarkChannelEnded marks a single channel as ended.
func (s *PgStore) MarkChannelEnded(ctx context.Context, channelID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET status = 'ended', ended_at = NOW(), worker_id = NULL
		WHERE id = $1`,
		channelID)
	return err
}

// GetChannel retrieves a channel by platform and channel_id.
func (s *PgStore) GetChannel(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error) {
	var ch model.LiveChannel
	err := s.pool.QueryRow(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE platform = $1 AND channel_id = $2`,
		string(platform), channelID,
	).Scan(
		&ch.ID, &ch.Platform, &ch.ChannelID, &ch.StreamerName,
		&ch.ViewerCount, &ch.Status, &ch.WorkerID,
		&ch.DiscoveredAt, &ch.LastSeenAt, &ch.EndedAt,
		&ch.HandoffFrom, &ch.HandoffStatus, &ch.HandoffStartedAt,
	)
	if err != nil {
		return nil, err
	}
	return &ch, nil
}

// RegisterWorker inserts or updates a worker record.
func (s *PgStore) RegisterWorker(ctx context.Context, worker model.Worker) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, status, max_capacity, registered_at, last_heartbeat)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			max_capacity = EXCLUDED.max_capacity,
			last_heartbeat = EXCLUDED.last_heartbeat`,
		worker.ID, worker.Status, worker.MaxCapacity,
		worker.RegisteredAt, worker.LastHeartbeat)
	return err
}

// UpdateWorkerStatus updates the status and last_heartbeat of a worker.
func (s *PgStore) UpdateWorkerStatus(ctx context.Context, workerID string, status string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE workers SET status = $1, last_heartbeat = NOW()
		WHERE id = $2`,
		status, workerID)
	return err
}

// ListAliveWorkers returns all workers with status=alive.
func (s *PgStore) ListAliveWorkers(ctx context.Context) ([]model.Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, status, max_capacity, registered_at, last_heartbeat
		FROM workers
		WHERE status = 'alive'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []model.Worker
	for rows.Next() {
		var w model.Worker
		if err := rows.Scan(&w.ID, &w.Status, &w.MaxCapacity, &w.RegisteredAt, &w.LastHeartbeat); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

// DeleteWorker removes a worker record.
func (s *PgStore) DeleteWorker(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE id = $1`, workerID)
	return err
}

// ---------------------------------------------------------------------------
// Admin queries (read-only, used by gRPC admin service)
// ---------------------------------------------------------------------------

// PlatformStats holds aggregated stats for a single platform.
type PlatformStats struct {
	Platform         string
	LiveCount        int32
	PendingCount     int32
	EndedCount       int32
	TotalViewerCount int64
}

// GetPlatformStats returns aggregated channel counts and viewer totals per platform.
func (s *PgStore) GetPlatformStats(ctx context.Context) ([]PlatformStats, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT platform,
			COALESCE(SUM(CASE WHEN status = 'live' THEN 1 ELSE 0 END), 0) AS live_count,
			COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_count,
			COALESCE(SUM(CASE WHEN status = 'ended' THEN 1 ELSE 0 END), 0) AS ended_count,
			COALESCE(SUM(CASE WHEN status = 'live' THEN viewer_count ELSE 0 END), 0) AS total_viewer_count
		FROM live_channels
		GROUP BY platform
		ORDER BY platform`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []PlatformStats
	for rows.Next() {
		var s PlatformStats
		if err := rows.Scan(&s.Platform, &s.LiveCount, &s.PendingCount, &s.EndedCount, &s.TotalViewerCount); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

// ListChannelsFiltered returns channels with optional platform/status filtering and pagination.
func (s *PgStore) ListChannelsFiltered(ctx context.Context, platform, status, sort string, page, size int) ([]model.LiveChannel, int, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}

	where := "WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if platform != "" {
		where += fmt.Sprintf(" AND platform = $%d", argIdx)
		args = append(args, platform)
		argIdx++
	}
	if status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	// Count total
	var total int
	countQuery := "SELECT COUNT(*) FROM live_channels " + where
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Order
	orderBy := "ORDER BY viewer_count DESC, id ASC"
	if sort == "discoveredAt" {
		orderBy = "ORDER BY discovered_at DESC, id ASC"
	}

	// Fetch page
	query := fmt.Sprintf(`
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels %s %s
		LIMIT $%d OFFSET $%d`, where, orderBy, argIdx, argIdx+1)
	args = append(args, size, (page-1)*size)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	channels, scanErr := scanChannelsWithHandoff(rows)
	if scanErr != nil {
		return nil, 0, scanErr
	}
	return channels, total, nil
}

// SearchLiveChannelsArgs is the multi-axis filter spec for SearchLiveChannels.
// All fields are optional. Empty/zero = no filter for that axis.
type SearchLiveChannelsArgs struct {
	Keyword              string   // ILIKE on title/category/tags
	Platform             string   // legacy single-platform, used only when Platforms empty
	Platforms            []string // multi-select platforms (OR)
	Category             string   // legacy single-category
	Categories           []string // multi-select categories (OR)
	Tag                  string   // legacy single-tag
	Tags                 []string // multi-select tags (overlap with row.tags)
	SortOrder            string   // viewer_desc (default) | viewer_asc | started_at_desc | started_at_asc
	MinViewerCount       int
	MaxViewerCount       int
	StartedWithinSeconds int
	Limit                int
	Offset               int
}

// SearchLiveChannels returns currently-live channels matching the multi-axis
// filter args. All filters are optional (empty/zero = no filter for that axis).
// Only live/pending channels seen within the last 10 minutes are considered.
// Returns total matching rows (independent of limit/offset) + the page slice.
func (s *PgStore) SearchLiveChannels(ctx context.Context, args SearchLiveChannelsArgs) ([]model.LiveChannel, int, error) {
	// Normalize: prefer multi-select arrays; fall back to legacy single-value fields.
	// CRITICAL: pgx maps Go nil slice to SQL NULL (not '{}'), and `cardinality(NULL)`
	// returns NULL — making the WHERE clause silently exclude all rows. Always pass
	// a non-nil empty slice; the SQL also uses COALESCE for defense-in-depth.
	platforms := args.Platforms
	if len(platforms) == 0 && args.Platform != "" {
		platforms = []string{args.Platform}
	}
	if platforms == nil {
		platforms = []string{}
	}
	categories := args.Categories
	if len(categories) == 0 && args.Category != "" {
		categories = []string{args.Category}
	}
	if categories == nil {
		categories = []string{}
	}
	tags := args.Tags
	if len(tags) == 0 && args.Tag != "" {
		tags = []string{args.Tag}
	}
	if tags == nil {
		tags = []string{}
	}

	orderBy := "viewer_count DESC, id ASC"
	switch args.SortOrder {
	case "viewer_asc":
		orderBy = "viewer_count ASC, id ASC"
	case "started_at_desc":
		orderBy = "started_at DESC NULLS LAST, id ASC"
	case "started_at_asc":
		orderBy = "started_at ASC NULLS LAST, id ASC"
	}

	// COALESCE(cardinality(...), 0) — guards against pgx-mapped NULL silently
	// excluding rows when the array param happens to be nil (defense-in-depth).
	const matchClause = `
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		  AND (COALESCE(cardinality($1::text[]), 0) = 0 OR platform = ANY($1::text[]))
		  AND ($2 = '' OR (
		    title ILIKE '%' || $2 || '%'
		    OR category ILIKE '%' || $2 || '%'
		    OR EXISTS (SELECT 1 FROM unnest(tags) t WHERE t ILIKE '%' || $2 || '%')
		  ))
		  AND (COALESCE(cardinality($3::text[]), 0) = 0 OR category = ANY($3::text[]))
		  AND (COALESCE(cardinality($4::text[]), 0) = 0 OR tags && $4::text[])
		  AND ($5 = 0 OR viewer_count >= $5)
		  AND ($6 = 0 OR viewer_count <= $6)
		  AND ($7 = 0 OR (started_at IS NOT NULL AND started_at > NOW() - make_interval(secs => $7)))`

	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM live_channels`+matchClause,
		platforms, args.Keyword, categories, tags,
		args.MinViewerCount, args.MaxViewerCount, args.StartedWithinSeconds,
	).Scan(&total); err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return nil, 0, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at,
		       title, category, thumbnail_url, started_at, tags
		FROM live_channels`+matchClause+`
		ORDER BY `+orderBy+`
		LIMIT $8 OFFSET $9`,
		platforms, args.Keyword, categories, tags,
		args.MinViewerCount, args.MaxViewerCount, args.StartedWithinSeconds,
		args.Limit, args.Offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var channels []model.LiveChannel
	for rows.Next() {
		var ch model.LiveChannel
		if err := rows.Scan(
			&ch.ID, &ch.Platform, &ch.ChannelID, &ch.StreamerName,
			&ch.ViewerCount, &ch.Status, &ch.WorkerID,
			&ch.DiscoveredAt, &ch.LastSeenAt, &ch.EndedAt,
			&ch.HandoffFrom, &ch.HandoffStatus, &ch.HandoffStartedAt,
			&ch.Title, &ch.Category, &ch.ThumbnailURL, &ch.BroadcastStartedAt,
			&ch.Tags,
		); err != nil {
			return nil, 0, err
		}
		channels = append(channels, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return channels, total, nil
}

// FacetEntry is a single (name, count) bucket returned by facet aggregation.
type FacetEntry struct {
	Name  string
	Count int
}

// GetLiveDiscoveryFacets returns the most popular categories and tags across
// channels currently live (status 'live'/'pending', last_seen within 10 min),
// optionally scoped to a single platform. Aggregates run in parallel.
// Used by the explore live board to render filter chips with no operator curation.
func (s *PgStore) GetLiveDiscoveryFacets(ctx context.Context, platform string, categoryLimit, tagLimit int) ([]FacetEntry, []FacetEntry, error) {
	type result struct {
		entries []FacetEntry
		err     error
	}
	categoriesCh := make(chan result, 1)
	tagsCh := make(chan result, 1)

	go func() {
		rows, err := s.pool.Query(ctx, `
			SELECT category AS name, COUNT(*)::int AS count
			FROM live_channels
			WHERE status IN ('live', 'pending')
			  AND last_seen_at > NOW() - INTERVAL '10 minutes'
			  AND ($1 = '' OR platform = $1)
			  AND category IS NOT NULL AND category <> ''
			GROUP BY category
			ORDER BY count DESC
			LIMIT $2`, platform, categoryLimit)
		if err != nil {
			categoriesCh <- result{nil, err}
			return
		}
		defer rows.Close()
		var out []FacetEntry
		for rows.Next() {
			var e FacetEntry
			if scanErr := rows.Scan(&e.Name, &e.Count); scanErr != nil {
				categoriesCh <- result{nil, scanErr}
				return
			}
			out = append(out, e)
		}
		categoriesCh <- result{out, rows.Err()}
	}()

	go func() {
		rows, err := s.pool.Query(ctx, `
			SELECT tag AS name, COUNT(*)::int AS count
			FROM live_channels, unnest(tags) AS tag
			WHERE status IN ('live', 'pending')
			  AND last_seen_at > NOW() - INTERVAL '10 minutes'
			  AND ($1 = '' OR platform = $1)
			  AND tag IS NOT NULL AND tag <> ''
			GROUP BY tag
			ORDER BY count DESC
			LIMIT $2`, platform, tagLimit)
		if err != nil {
			tagsCh <- result{nil, err}
			return
		}
		defer rows.Close()
		var out []FacetEntry
		for rows.Next() {
			var e FacetEntry
			if scanErr := rows.Scan(&e.Name, &e.Count); scanErr != nil {
				tagsCh <- result{nil, scanErr}
				return
			}
			out = append(out, e)
		}
		tagsCh <- result{out, rows.Err()}
	}()

	cat := <-categoriesCh
	tg := <-tagsCh
	if cat.err != nil {
		return nil, nil, cat.err
	}
	if tg.err != nil {
		return nil, nil, tg.err
	}
	return cat.entries, tg.entries, nil
}

// Explore and discovery queries.

// GetTrendingLiveChannels returns top-N channels by a "viewer + just-started boost" score.
// Channels broadcasting since within boostWithinSeconds get +50 score (configurable).
func (s *PgStore) GetTrendingLiveChannels(ctx context.Context, platform string, boostWithinSeconds, limit int) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at,
		       title, category, thumbnail_url, started_at, tags
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		  AND ($1 = '' OR platform = $1)
		ORDER BY (
		  viewer_count
		  + CASE WHEN started_at IS NOT NULL
		           AND $2 > 0
		           AND started_at > NOW() - make_interval(secs => $2)
		         THEN 50 ELSE 0 END
		) DESC, id ASC
		LIMIT $3`, platform, boostWithinSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLiveChannels(rows)
}

// GetJustStartedLiveChannels returns channels whose broadcast started within
// the last withinMinutes, ordered by viewer DESC.
func (s *PgStore) GetJustStartedLiveChannels(ctx context.Context, platform string, withinMinutes, limit int) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at,
		       title, category, thumbnail_url, started_at, tags
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		  AND ($1 = '' OR platform = $1)
		  AND started_at IS NOT NULL
		  AND started_at > NOW() - make_interval(mins => $2)
		ORDER BY viewer_count DESC, started_at DESC, id ASC
		LIMIT $3`, platform, withinMinutes, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLiveChannels(rows)
}

// GetFeaturedLiveChannels returns the top-N channels by viewer DESC. The intent is
// for an upstream API to overfetch and re-rank with registered-channel priority.
func (s *PgStore) GetFeaturedLiveChannels(ctx context.Context, platform string, limit int) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at,
		       title, category, thumbnail_url, started_at, tags
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		  AND ($1 = '' OR platform = $1)
		ORDER BY viewer_count DESC, id ASC
		LIMIT $2`, platform, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLiveChannels(rows)
}

// PlatformLiveSummary aggregates per-platform active count + top channels.
type PlatformLiveSummary struct {
	Platform    string
	ActiveCount int
	TopChannels []model.LiveChannel
}

// GetPlatformLiveSummary returns per-platform active count + top-K channels per platform.
// Implemented with a window function (ROW_NUMBER OVER PARTITION BY platform).
func (s *PgStore) GetPlatformLiveSummary(ctx context.Context, topPerPlatform int) ([]PlatformLiveSummary, error) {
	// Active count per platform.
	countRows, err := s.pool.Query(ctx, `
		SELECT platform, COUNT(*)::int AS c
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		GROUP BY platform`)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for countRows.Next() {
		var p string
		var c int
		if err := countRows.Scan(&p, &c); err != nil {
			countRows.Close()
			return nil, err
		}
		counts[p] = c
	}
	countRows.Close()
	if err := countRows.Err(); err != nil {
		return nil, err
	}

	// Top-K channels per platform via window function.
	topRows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at,
		       title, category, thumbnail_url, started_at, tags
		FROM (
		  SELECT *,
		         ROW_NUMBER() OVER (PARTITION BY platform ORDER BY viewer_count DESC, id ASC) AS rn
		  FROM live_channels
		  WHERE status IN ('live', 'pending')
		    AND last_seen_at > NOW() - INTERVAL '10 minutes'
		) t
		WHERE rn <= $1
		ORDER BY platform, rn`, topPerPlatform)
	if err != nil {
		return nil, err
	}
	defer topRows.Close()
	chans, err := scanLiveChannels(topRows)
	if err != nil {
		return nil, err
	}

	byPlatform := map[string][]model.LiveChannel{}
	for _, ch := range chans {
		p := string(ch.Platform)
		byPlatform[p] = append(byPlatform[p], ch)
	}

	out := make([]PlatformLiveSummary, 0, len(counts))
	for p, c := range counts {
		out = append(out, PlatformLiveSummary{
			Platform:    p,
			ActiveCount: c,
			TopChannels: byPlatform[p],
		})
	}
	return out, nil
}

// ExploreStats is the global counter snapshot used by explore hero.
type ExploreStats struct {
	ActiveLiveCount       int
	DistinctCategoryCount int
	DistinctTagCount      int
	TotalViewerCount      int
	PlatformCounts        map[string]int
}

// GetExploreStats returns global counters for the explore hero. Three queries run
// sequentially (cheap aggregates over the partial-index-friendly active subset).
func (s *PgStore) GetExploreStats(ctx context.Context) (*ExploreStats, error) {
	stats := &ExploreStats{PlatformCounts: map[string]int{}}

	// Aggregate over active live channels.
	if err := s.pool.QueryRow(ctx, `
		SELECT
		  COUNT(*)::int,
		  COUNT(DISTINCT NULLIF(category, ''))::int,
		  COALESCE(SUM(viewer_count), 0)::int
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'`).Scan(
		&stats.ActiveLiveCount,
		&stats.DistinctCategoryCount,
		&stats.TotalViewerCount,
	); err != nil {
		return nil, err
	}

	// Distinct tag count (unnest).
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT tag)::int
		FROM live_channels, unnest(tags) AS tag
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		  AND tag IS NOT NULL AND tag <> ''`).Scan(&stats.DistinctTagCount); err != nil {
		return nil, err
	}

	// Per-platform active counts.
	rows, err := s.pool.Query(ctx, `
		SELECT platform, COUNT(*)::int
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		GROUP BY platform`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p string
		var c int
		if err := rows.Scan(&p, &c); err != nil {
			return nil, err
		}
		stats.PlatformCounts[p] = c
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return stats, nil
}

// CategoryLiveBundle is one (category, channels) bucket.
type CategoryLiveBundle struct {
	Category    string
	ActiveCount int
	Channels    []model.LiveChannel
}

// GetCategoryLiveBundles returns the top-N most-active categories with K live
// channels per category. Single CTE-based query: pick top categories by count,
// then ROW_NUMBER over each partition for top-K channels.
func (s *PgStore) GetCategoryLiveBundles(ctx context.Context, platform string, categoryCount, channelsPerCategory int) ([]CategoryLiveBundle, error) {
	// Step 1: top categories by active count.
	catRows, err := s.pool.Query(ctx, `
		SELECT category, COUNT(*)::int AS c
		FROM live_channels
		WHERE status IN ('live', 'pending')
		  AND last_seen_at > NOW() - INTERVAL '10 minutes'
		  AND ($1 = '' OR platform = $1)
		  AND category IS NOT NULL AND category <> ''
		GROUP BY category
		ORDER BY c DESC
		LIMIT $2`, platform, categoryCount)
	if err != nil {
		return nil, err
	}
	type catEntry struct {
		name  string
		count int
	}
	var topCats []catEntry
	for catRows.Next() {
		var e catEntry
		if err := catRows.Scan(&e.name, &e.count); err != nil {
			catRows.Close()
			return nil, err
		}
		topCats = append(topCats, e)
	}
	catRows.Close()
	if err := catRows.Err(); err != nil {
		return nil, err
	}
	if len(topCats) == 0 {
		return nil, nil
	}

	// Step 2: top-K channels per top category.
	catNames := make([]string, len(topCats))
	for i, c := range topCats {
		catNames[i] = c.name
	}
	chRows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at,
		       title, category, thumbnail_url, started_at, tags
		FROM (
		  SELECT *,
		         ROW_NUMBER() OVER (PARTITION BY category ORDER BY viewer_count DESC, id ASC) AS rn
		  FROM live_channels
		  WHERE status IN ('live', 'pending')
		    AND last_seen_at > NOW() - INTERVAL '10 minutes'
		    AND ($1 = '' OR platform = $1)
		    AND category = ANY($2::text[])
		) t
		WHERE rn <= $3
		ORDER BY category, rn`, platform, catNames, channelsPerCategory)
	if err != nil {
		return nil, err
	}
	defer chRows.Close()
	chans, err := scanLiveChannels(chRows)
	if err != nil {
		return nil, err
	}

	byCategory := map[string][]model.LiveChannel{}
	for _, ch := range chans {
		if ch.Category == nil {
			continue
		}
		byCategory[*ch.Category] = append(byCategory[*ch.Category], ch)
	}

	out := make([]CategoryLiveBundle, 0, len(topCats))
	for _, c := range topCats {
		out = append(out, CategoryLiveBundle{
			Category:    c.name,
			ActiveCount: c.count,
			Channels:    byCategory[c.name],
		})
	}
	return out, nil
}

// scanLiveChannels reads model.LiveChannel rows from a query that selects all
// LiveChannel columns in the canonical order. Reduces duplication across the
// explore RPCs that share the same projection.
func scanLiveChannels(rows pgx.Rows) ([]model.LiveChannel, error) {
	var out []model.LiveChannel
	for rows.Next() {
		var ch model.LiveChannel
		if err := rows.Scan(
			&ch.ID, &ch.Platform, &ch.ChannelID, &ch.StreamerName,
			&ch.ViewerCount, &ch.Status, &ch.WorkerID,
			&ch.DiscoveredAt, &ch.LastSeenAt, &ch.EndedAt,
			&ch.HandoffFrom, &ch.HandoffStatus, &ch.HandoffStartedAt,
			&ch.Title, &ch.Category, &ch.ThumbnailURL, &ch.BroadcastStartedAt,
			&ch.Tags,
		); err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	return out, rows.Err()
}

// SearchBroadcastChannels returns channels whose latest broadcast metadata
// (title/category/tags) matches a keyword within the given lookback window.
// Each (platform, channel_id) appears at most once with the most recent metadata
// row by valid_from. Currently-live channels are surfaced first via is_live, and
// rows are sorted by GREATEST(current_viewer, peak_viewer) DESC.
//
// total_count uses a window function so we get total + page in a single query
// (broadcast_metadata_history is partitioned by valid_from, so the lookback
// window enables partition pruning — counting is cheap).
func (s *PgStore) SearchBroadcastChannels(
	ctx context.Context, keyword string, platform string,
	lookbackDays int, limit int, offset int,
) ([]model.BroadcastChannel, int, error) {
	const query = `
WITH matched AS (
    SELECT DISTINCT ON (bmh.platform, bmh.channel_id)
        bmh.platform,
        bmh.channel_id,
        bmh.session_seq,
        bmh.title,
        bmh.category,
        bmh.tags,
        bmh.thumbnail_url,
        bmh.valid_from
    FROM broadcast_metadata_history bmh
    WHERE bmh.valid_from > NOW() - ($3 * INTERVAL '1 day')
      AND ($1 = '' OR bmh.platform = $1)
      AND (
        bmh.title ILIKE '%' || $2 || '%'
        OR bmh.category ILIKE '%' || $2 || '%'
        OR EXISTS (SELECT 1 FROM unnest(bmh.tags) t WHERE t ILIKE '%' || $2 || '%')
      )
    ORDER BY bmh.platform, bmh.channel_id, bmh.valid_from DESC
)
SELECT
    m.platform,
    m.channel_id,
    COALESCE(s.streamer_name, '') AS streamer_name,
    m.title,
    m.category,
    m.tags,
    m.thumbnail_url,
    s.started_at,
    s.ended_at,
    COALESCE(s.peak_viewer_count, 0) AS peak_viewer_count,
    CASE WHEN lc.status IN ('live', 'pending')
              AND lc.last_seen_at > NOW() - INTERVAL '10 minutes' THEN true
         ELSE false
    END AS is_live,
    COALESCE(lc.viewer_count, 0) AS current_viewer_count,
    COUNT(*) OVER () AS total_count
FROM matched m
LEFT JOIN broadcast_sessions s
    ON s.platform = m.platform
   AND s.channel_id = m.channel_id
   AND s.session_seq = m.session_seq
   AND s.started_at > NOW() - ($3 * INTERVAL '1 day')
LEFT JOIN live_channels lc
    ON lc.platform = m.platform
   AND lc.channel_id = m.channel_id
ORDER BY
    is_live DESC,
    GREATEST(COALESCE(lc.viewer_count, 0), COALESCE(s.peak_viewer_count, 0)) DESC,
    m.platform,
    m.channel_id
LIMIT $4 OFFSET $5`

	rows, err := s.pool.Query(ctx, query, platform, keyword, lookbackDays, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var channels []model.BroadcastChannel
	var total int
	for rows.Next() {
		var ch model.BroadcastChannel
		if err := rows.Scan(
			&ch.Platform, &ch.ChannelID, &ch.StreamerName,
			&ch.Title, &ch.Category, &ch.Tags, &ch.ThumbnailURL,
			&ch.LastStartedAt, &ch.LastEndedAt, &ch.PeakViewerCount,
			&ch.IsLive, &ch.CurrentViewerCount,
			&total,
		); err != nil {
			return nil, 0, err
		}
		channels = append(channels, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return channels, total, nil
}

// GetLiveStatusesByChannelIDs returns channels that are currently live or pending
// for the given (platform, channel_id) pairs. A 10-minute stale-data defense is applied.
func (s *PgStore) GetLiveStatusesByChannelIDs(ctx context.Context, pairs []struct {
	Platform  string
	ChannelID string
}) ([]model.LiveChannel, error) {
	if len(pairs) == 0 {
		return nil, nil
	}

	var b strings.Builder
	args := make([]interface{}, 0, len(pairs)*2)

	b.WriteString(`SELECT id, platform, channel_id, streamer_name, viewer_count, status,
	       worker_id, discovered_at, last_seen_at, ended_at,
	       handoff_from, handoff_status, handoff_started_at,
	       title, category, thumbnail_url, started_at, tags
	FROM live_channels
	WHERE status IN ('live', 'pending')
	  AND last_seen_at > NOW() - INTERVAL '10 minutes'
	  AND (platform, channel_id) IN (`)

	for i, p := range pairs {
		if i > 0 {
			b.WriteString(", ")
		}
		base := i * 2
		fmt.Fprintf(&b, "($%d, $%d)", base+1, base+2)
		args = append(args, p.Platform, p.ChannelID)
	}
	b.WriteString(")")

	rows, err := s.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var channels []model.LiveChannel
	for rows.Next() {
		var ch model.LiveChannel
		if err := rows.Scan(
			&ch.ID, &ch.Platform, &ch.ChannelID, &ch.StreamerName,
			&ch.ViewerCount, &ch.Status, &ch.WorkerID,
			&ch.DiscoveredAt, &ch.LastSeenAt, &ch.EndedAt,
			&ch.HandoffFrom, &ch.HandoffStatus, &ch.HandoffStartedAt,
			&ch.Title, &ch.Category, &ch.ThumbnailURL, &ch.BroadcastStartedAt,
			&ch.Tags,
		); err != nil {
			return nil, err
		}
		channels = append(channels, ch)
	}
	return channels, rows.Err()
}

// ListAllWorkers returns all workers regardless of status.
func (s *PgStore) ListAllWorkers(ctx context.Context) ([]model.Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, status, max_capacity, registered_at, last_heartbeat
		FROM workers
		ORDER BY registered_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []model.Worker
	for rows.Next() {
		var w model.Worker
		if err := rows.Scan(&w.ID, &w.Status, &w.MaxCapacity, &w.RegisteredAt, &w.LastHeartbeat); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

// ---------------------------------------------------------------------------
// Handoff / graceful-drain queries
// ---------------------------------------------------------------------------

// ReassignChannel reassigns a channel from oldWorkerID to newWorkerID, setting handoff tracking fields.
// Only reassigns channels that are still live — ended channels are skipped.
// Returns true if a row was actually updated, false if the channel was already ended.
func (s *PgStore) ReassignChannel(ctx context.Context, channelID int64, newWorkerID, oldWorkerID string) (bool, error) {
	// The single-channel product closes the old connection before a new owner.
	// Use normal unassignment/reconciliation; never overlap drain connections.
	if s.collectionChannel != nil {
		return false, nil
	}

	ct, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET worker_id = $1, handoff_from = $2, handoff_status = 'pending', handoff_started_at = NOW()
		WHERE id = $3 AND status = 'live'`,
		newWorkerID, oldWorkerID, channelID)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() > 0, nil
}

// AckHandoff marks a channel's handoff as acknowledged by the new worker.
func (s *PgStore) AckHandoff(ctx context.Context, platform model.Platform, channelID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET handoff_status = 'acked'
		WHERE platform = $1 AND channel_id = $2 AND handoff_status = 'pending'`,
		string(platform), channelID)
	return err
}

// ListAckedHandoffs returns channels where handoff_from matches the given worker and handoff_status is 'acked'.
func (s *PgStore) ListAckedHandoffs(ctx context.Context, oldWorkerID string) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE handoff_from = $1 AND handoff_status = 'acked'`, oldWorkerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// ClearHandoff resets all handoff fields to NULL after a successful handoff.
func (s *PgStore) ClearHandoff(ctx context.Context, channelID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET handoff_from = NULL, handoff_status = NULL, handoff_started_at = NULL
		WHERE id = $1 AND handoff_status = 'acked'`, channelID)
	return err
}

// ListStuckHandoffs returns live channels with handoff_status='pending' stuck longer than threshold.
func (s *PgStore) ListStuckHandoffs(ctx context.Context, threshold time.Duration) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE handoff_status = 'pending' AND status = 'live'
		  AND handoff_started_at < NOW() - $1 * INTERVAL '1 second'`,
		int(threshold.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// ListDrainingWorkerChannels returns live channels assigned to draining workers without pending/acked handoff.
func (s *PgStore) ListDrainingWorkerChannels(ctx context.Context) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT lc.id, lc.platform, lc.channel_id, lc.streamer_name, lc.viewer_count, lc.status,
		       lc.worker_id, lc.discovered_at, lc.last_seen_at, lc.ended_at,
		       lc.handoff_from, lc.handoff_status, lc.handoff_started_at
		FROM live_channels lc
		JOIN workers w ON lc.worker_id = w.id
		WHERE w.status = 'draining' AND lc.status = 'live' AND lc.handoff_status IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// UnassignDeadWorkerChannels unassigns channels from workers with status='dead' only.
// It returns the number of channels reclaimed so callers can observe and alert on
// recovery after a partial worker shutdown.
func (s *PgStore) UnassignDeadWorkerChannels(ctx context.Context) (int64, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE live_channels
		SET worker_id = NULL, status = 'pending',
		    handoff_from = NULL, handoff_status = NULL, handoff_started_at = NULL
		WHERE worker_id IN (SELECT id FROM workers WHERE status = 'dead')
		  AND status = 'live'`)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// ListWorkersByStatus returns all workers with the given status.
func (s *PgStore) ListWorkersByStatus(ctx context.Context, status string) ([]model.Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, status, max_capacity, registered_at, last_heartbeat
		FROM workers WHERE status = $1`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workers []model.Worker
	for rows.Next() {
		var w model.Worker
		if err := rows.Scan(&w.ID, &w.Status, &w.MaxCapacity, &w.RegisteredAt, &w.LastHeartbeat); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

// ListPendingHandoffs returns channels with pending handoff from a specific draining worker.
func (s *PgStore) ListPendingHandoffs(ctx context.Context, drainingWorkerID string) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE handoff_from = $1 AND handoff_status = 'pending' AND status = 'live'`, drainingWorkerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// ListWorkerChannelsByPlatform returns live channels for a specific worker and platform.
func (s *PgStore) ListWorkerChannelsByPlatform(ctx context.Context, workerID string, platform model.Platform) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE worker_id = $1 AND platform = $2 AND status = 'live'
		ORDER BY viewer_count DESC, id ASC`, workerID, string(platform))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// CountAssignedByWorker returns the number of live channels assigned to each worker.
func (s *PgStore) CountAssignedByWorker(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, count(*) FROM live_channels
		WHERE worker_id IS NOT NULL AND status = 'live' AND ($1::text IS NULL OR (platform='soop' AND channel_id=$1))
		GROUP BY worker_id`, s.collectionChannel)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var workerID string
		var count int
		if err := rows.Scan(&workerID, &count); err != nil {
			return nil, err
		}
		result[workerID] = count
	}
	return result, rows.Err()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// scanChannelsWithHandoff scans rows including handoff columns.
func scanChannelsWithHandoff(rows pgx.Rows) ([]model.LiveChannel, error) {
	var channels []model.LiveChannel
	for rows.Next() {
		var ch model.LiveChannel
		if err := rows.Scan(
			&ch.ID, &ch.Platform, &ch.ChannelID, &ch.StreamerName,
			&ch.ViewerCount, &ch.Status, &ch.WorkerID,
			&ch.DiscoveredAt, &ch.LastSeenAt, &ch.EndedAt,
			&ch.HandoffFrom, &ch.HandoffStatus, &ch.HandoffStartedAt,
		); err != nil {
			return nil, err
		}
		channels = append(channels, ch)
	}
	return channels, rows.Err()
}

// ListEndedChannelsBefore returns channels that ended before the given time.
// Used by stream cleanup to find channels whose Redis streams can be deleted.
func (s *PgStore) ListEndedChannelsBefore(ctx context.Context, before time.Time, limit int) ([]model.LiveChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, platform, channel_id, streamer_name, viewer_count, status,
		       worker_id, discovered_at, last_seen_at, ended_at,
		       handoff_from, handoff_status, handoff_started_at
		FROM live_channels
		WHERE status = 'ended' AND ended_at < $1
		ORDER BY ended_at ASC
		LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChannelsWithHandoff(rows)
}

// ListAllKnownChannelKeys returns Redis stream keys for ALL channels in PG (any status).
// Used by Phase 2 orphan detection: only keys NOT in PG at all are truly orphaned.
func (s *PgStore) ListAllKnownChannelKeys(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT platform, channel_id FROM live_channels`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var platform, channelID string
		if err := rows.Scan(&platform, &channelID); err != nil {
			return nil, err
		}
		keys = append(keys, ChatStreamKey(platform, channelID))
	}
	return keys, rows.Err()
}

// ListActiveChannelKeys returns Redis stream keys for all live/pending channels.
func (s *PgStore) ListActiveChannelKeys(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT platform, channel_id FROM live_channels
		WHERE status IN ('pending', 'live')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var platform, channelID string
		if err := rows.Scan(&platform, &channelID); err != nil {
			return nil, err
		}
		keys = append(keys, ChatStreamKey(platform, channelID))
	}
	return keys, rows.Err()
}

// Compile-time assertions to verify PgStore satisfies both interfaces.
var (
	_ ChannelStore = (*PgStore)(nil)
	_ WorkerStore  = (*PgStore)(nil)
)

// ValidChannelStatuses lists all valid channel status values.
var ValidChannelStatuses = []string{
	model.ChannelStatusPending,
	model.ChannelStatusLive,
	model.ChannelStatusEnded,
}

// ValidWorkerStatuses lists all valid worker status values.
var ValidWorkerStatuses = []string{
	model.WorkerStatusAlive,
	model.WorkerStatusDraining,
	model.WorkerStatusDead,
}

// IsValidChannelStatus returns true if the given status is a known channel status.
func IsValidChannelStatus(status string) bool {
	for _, s := range ValidChannelStatuses {
		if s == status {
			return true
		}
	}
	return false
}

// IsValidWorkerStatus returns true if the given status is a known worker status.
func IsValidWorkerStatus(status string) bool {
	for _, s := range ValidWorkerStatuses {
		if s == status {
			return true
		}
	}
	return false
}

func applyPoolDefaults(config *pgxpool.Config) {
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
}

// NewPgPool creates a new pgxpool.Pool from a connection string.
// This is a convenience wrapper; callers can also create pools directly.
func NewPgPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}
	applyPoolDefaults(config)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}
	return pool, nil
}

// NewPgPoolWithRetry creates a pgxpool.Pool with retry on connection failure.
// Retries up to maxRetries times with retryInterval between attempts.
// Config parse errors fail immediately without retry.
// Uses Ping to verify actual connectivity since pgxpool.NewWithConfig
// establishes MinConns asynchronously and never returns connection errors.
func NewPgPoolWithRetry(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	const maxRetries = 3
	const retryInterval = 5 * time.Second
	const connectTimeout = 5 * time.Second

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}
	applyPoolDefaults(config)
	config.ConnConfig.ConnectTimeout = connectTimeout

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			lastErr = err
		} else {
			pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				if attempt > 1 {
					slog.Info("pg pool connected after retry", "attempt", attempt)
				}
				return pool, nil
			}
			pool.Close()
			lastErr = err
		}
		if attempt < maxRetries {
			slog.Warn("pg pool connection failed, retrying",
				"attempt", attempt,
				"max_retries", maxRetries,
				"error", lastErr,
			)
			select {
			case <-time.After(retryInterval):
			case <-ctx.Done():
				return nil, fmt.Errorf("pg pool retry cancelled: %w", ctx.Err())
			}
		}
	}
	return nil, fmt.Errorf("create pg pool after %d attempts: %w", maxRetries, lastErr)
}
