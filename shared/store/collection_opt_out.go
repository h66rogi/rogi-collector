package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type CollectionOptOutInput struct {
	Platform       model.Platform
	ChannelID      string
	StreamerName   *string
	RequesterEmail *string
	Reason         *string
}

type CreateCollectionOptOutBatchInput struct {
	Title         string
	RequestSource *string
	Reason        *string
	RequestedBy   *string
	CreatedBy     *string
	Items         []CollectionOptOutInput
}

type ListCollectionOptOutsArgs struct {
	Active   *bool
	Platform model.Platform
	Query    string
	Limit    int
	Offset   int
}

func normalizeCollectionOptOutInput(item CollectionOptOutInput) CollectionOptOutInput {
	item.Platform = model.Platform(strings.ToLower(strings.TrimSpace(string(item.Platform))))
	item.ChannelID = strings.TrimSpace(item.ChannelID)
	return item
}

func (s *PgStore) CreateCollectionOptOutBatch(ctx context.Context, input CreateCollectionOptOutBatchInput) (*model.CollectionOptOutBatch, []model.CollectionOptOut, error) {
	if len(input.Items) == 0 {
		return nil, nil, fmt.Errorf("items required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	var batch model.CollectionOptOutBatch
	if err := tx.QueryRow(ctx, `
		INSERT INTO collection_opt_out_batches (title, request_source, reason, requested_by, created_by)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, title, request_source, reason, requested_by, created_by, created_at, 0, 0`,
		input.Title, input.RequestSource, input.Reason, input.RequestedBy, input.CreatedBy,
	).Scan(
		&batch.ID, &batch.Title, &batch.RequestSource, &batch.Reason,
		&batch.RequestedBy, &batch.CreatedBy, &batch.CreatedAt,
		&batch.ItemCount, &batch.ActiveCount,
	); err != nil {
		return nil, nil, err
	}

	items := make([]model.CollectionOptOut, 0, len(input.Items))
	for _, raw := range input.Items {
		item := normalizeCollectionOptOutInput(raw)
		if !item.Platform.IsValid() {
			return nil, nil, fmt.Errorf("invalid platform: %s", item.Platform)
		}
		if item.ChannelID == "" {
			return nil, nil, fmt.Errorf("channel_id required")
		}

		var out model.CollectionOptOut
		err := tx.QueryRow(ctx, `
			INSERT INTO collection_opt_outs (
				batch_id, platform, channel_id, streamer_name, requester_email,
				reason, active, created_by, updated_at, revoked_at, revoked_by, revocation_reason
			)
			VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, NOW(), NULL, NULL, NULL)
			ON CONFLICT (platform, channel_id) WHERE active = TRUE
			DO UPDATE SET
				batch_id = EXCLUDED.batch_id,
				streamer_name = COALESCE(EXCLUDED.streamer_name, collection_opt_outs.streamer_name),
				requester_email = COALESCE(EXCLUDED.requester_email, collection_opt_outs.requester_email),
				reason = COALESCE(EXCLUDED.reason, collection_opt_outs.reason),
				created_by = EXCLUDED.created_by,
				updated_at = NOW()
			RETURNING id, batch_id, platform, channel_id, streamer_name, requester_email,
				reason, active, created_by, created_at, updated_at,
				revoked_at, revoked_by, revocation_reason`,
			batch.ID, string(item.Platform), item.ChannelID, item.StreamerName,
			item.RequesterEmail, item.Reason, input.CreatedBy,
		).Scan(
			&out.ID, &out.BatchID, &out.Platform, &out.ChannelID,
			&out.StreamerName, &out.RequesterEmail, &out.Reason,
			&out.Active, &out.CreatedBy, &out.CreatedAt, &out.UpdatedAt,
			&out.RevokedAt, &out.RevokedBy, &out.RevocationReason,
		)
		if err != nil {
			return nil, nil, err
		}
		items = append(items, out)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}

	batch.ItemCount = len(items)
	batch.ActiveCount = len(items)
	return &batch, items, nil
}

func (s *PgStore) ListCollectionOptOutBatches(ctx context.Context, limit, offset int) ([]model.CollectionOptOutBatch, int, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM collection_opt_out_batches`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT b.id, b.title, b.request_source, b.reason, b.requested_by, b.created_by, b.created_at,
		       COUNT(o.id)::int AS item_count,
		       COUNT(o.id) FILTER (WHERE o.active)::int AS active_count
		FROM collection_opt_out_batches b
		LEFT JOIN collection_opt_outs o ON o.batch_id = b.id
		GROUP BY b.id
		ORDER BY b.created_at DESC, b.id DESC
		LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	batches := []model.CollectionOptOutBatch{}
	for rows.Next() {
		var batch model.CollectionOptOutBatch
		if err := rows.Scan(
			&batch.ID, &batch.Title, &batch.RequestSource, &batch.Reason,
			&batch.RequestedBy, &batch.CreatedBy, &batch.CreatedAt,
			&batch.ItemCount, &batch.ActiveCount,
		); err != nil {
			return nil, 0, err
		}
		batches = append(batches, batch)
	}
	return batches, total, rows.Err()
}

func (s *PgStore) ListCollectionOptOuts(ctx context.Context, args ListCollectionOptOutsArgs) ([]model.CollectionOptOut, int, error) {
	if args.Limit <= 0 || args.Limit > 500 {
		args.Limit = 100
	}
	if args.Offset < 0 {
		args.Offset = 0
	}

	where, params := buildCollectionOptOutWhere(args)
	countQuery := "SELECT COUNT(*) FROM collection_opt_outs " + where

	var total int
	if err := s.pool.QueryRow(ctx, countQuery, params...).Scan(&total); err != nil {
		return nil, 0, err
	}

	params = append(params, args.Limit, args.Offset)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, batch_id, platform, channel_id, streamer_name, requester_email,
		       reason, active, created_by, created_at, updated_at,
		       revoked_at, revoked_by, revocation_reason
		FROM collection_opt_outs
		%s
		ORDER BY active DESC, updated_at DESC, id DESC
		LIMIT $%d OFFSET $%d`,
		where, len(params)-1, len(params),
	), params...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items, err := scanCollectionOptOuts(rows)
	if err != nil {
		return nil, 0, err
	}
	return items, total, rows.Err()
}

func buildCollectionOptOutWhere(args ListCollectionOptOutsArgs) (string, []interface{}) {
	var clauses []string
	var params []interface{}
	if args.Active != nil {
		params = append(params, *args.Active)
		clauses = append(clauses, fmt.Sprintf("active = $%d", len(params)))
	}
	if args.Platform != "" {
		params = append(params, string(args.Platform))
		clauses = append(clauses, fmt.Sprintf("platform = $%d", len(params)))
	}
	if strings.TrimSpace(args.Query) != "" {
		params = append(params, "%"+strings.ToLower(strings.TrimSpace(args.Query))+"%")
		clauses = append(clauses, fmt.Sprintf("(LOWER(channel_id) LIKE $%d OR LOWER(COALESCE(streamer_name, '')) LIKE $%d OR LOWER(COALESCE(requester_email, '')) LIKE $%d)", len(params), len(params), len(params)))
	}
	if len(clauses) == 0 {
		return "", params
	}
	return "WHERE " + strings.Join(clauses, " AND "), params
}

func (s *PgStore) ListActiveCollectionOptOuts(ctx context.Context) ([]model.CollectionOptOut, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, batch_id, platform, channel_id, streamer_name, requester_email,
		       reason, active, created_by, created_at, updated_at,
		       revoked_at, revoked_by, revocation_reason
		FROM collection_opt_outs
		WHERE active = TRUE
		ORDER BY platform ASC, channel_id ASC`)
	if err != nil {
		if isUndefinedTableError(err) {
			return []model.CollectionOptOut{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	return scanCollectionOptOuts(rows)
}

func (s *PgStore) IsCollectionOptedOut(ctx context.Context, platform model.Platform, channelID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM collection_opt_outs
			WHERE active = TRUE AND platform = $1 AND channel_id = $2
		)`,
		string(platform), channelID,
	).Scan(&exists)
	if isUndefinedTableError(err) {
		return false, nil
	}
	return exists, err
}

func (s *PgStore) RevokeCollectionOptOut(ctx context.Context, platform model.Platform, channelID string, revokedBy, reason *string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE collection_opt_outs
		SET active = FALSE,
		    revoked_at = NOW(),
		    revoked_by = $3,
		    revocation_reason = $4,
		    updated_at = NOW()
		WHERE active = TRUE AND platform = $1 AND channel_id = $2`,
		string(platform), channelID, revokedBy, reason,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *PgStore) EndCollectionOptedOutLiveChannel(ctx context.Context, platform model.Platform, channelID string) (*string, error) {
	var workerID *string
	err := s.pool.QueryRow(ctx, `
		WITH target AS (
			SELECT id, worker_id
			FROM live_channels
			WHERE platform = $1 AND channel_id = $2 AND status != 'ended'
		), updated AS (
			UPDATE live_channels lc
			SET status = 'ended', ended_at = COALESCE(lc.ended_at, NOW()), worker_id = NULL
			FROM target
			WHERE lc.id = target.id
			RETURNING target.worker_id
		)
		SELECT worker_id FROM updated`,
		string(platform), channelID,
	).Scan(&workerID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return workerID, err
}

func scanCollectionOptOuts(rows pgx.Rows) ([]model.CollectionOptOut, error) {
	items := []model.CollectionOptOut{}
	for rows.Next() {
		var item model.CollectionOptOut
		if err := rows.Scan(
			&item.ID, &item.BatchID, &item.Platform, &item.ChannelID,
			&item.StreamerName, &item.RequesterEmail, &item.Reason,
			&item.Active, &item.CreatedBy, &item.CreatedAt, &item.UpdatedAt,
			&item.RevokedAt, &item.RevokedBy, &item.RevocationReason,
		); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type CollectionOptOutSource interface {
	ListActiveCollectionOptOuts(ctx context.Context) ([]model.CollectionOptOut, error)
}

type CollectionOptOutCache struct {
	source CollectionOptOutSource
	logger interface {
		Warn(msg string, args ...any)
		Info(msg string, args ...any)
	}

	mu       sync.RWMutex
	items    map[model.Platform]map[string]model.CollectionOptOut
	interval time.Duration
}

func NewCollectionOptOutCache(source CollectionOptOutSource, interval time.Duration, logger interface {
	Warn(msg string, args ...any)
	Info(msg string, args ...any)
}) *CollectionOptOutCache {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &CollectionOptOutCache{
		source:   source,
		logger:   logger,
		items:    make(map[model.Platform]map[string]model.CollectionOptOut),
		interval: interval,
	}
}

func (c *CollectionOptOutCache) Refresh(ctx context.Context) error {
	items, err := c.source.ListActiveCollectionOptOuts(ctx)
	if err != nil {
		return err
	}
	next := make(map[model.Platform]map[string]model.CollectionOptOut)
	for _, item := range items {
		if next[item.Platform] == nil {
			next[item.Platform] = make(map[string]model.CollectionOptOut)
		}
		next[item.Platform][item.ChannelID] = item
	}
	c.mu.Lock()
	c.items = next
	c.mu.Unlock()
	if c.logger != nil {
		c.logger.Info("collection opt-out cache refreshed", "count", len(items))
	}
	return nil
}

func (c *CollectionOptOutCache) Start(ctx context.Context) {
	if err := c.Refresh(ctx); err != nil && c.logger != nil {
		c.logger.Warn("collection opt-out cache initial refresh failed", "error", err)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil && c.logger != nil {
				c.logger.Warn("collection opt-out cache refresh failed", "error", err)
			}
		}
	}
}

func (c *CollectionOptOutCache) IsOptedOut(platform model.Platform, channelID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if byChannel := c.items[platform]; byChannel != nil {
		_, ok := byChannel[channelID]
		return ok
	}
	return false
}

func isUndefinedTableError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}
