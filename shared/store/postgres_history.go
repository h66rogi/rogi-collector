package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// UpsertResult captures the RETURNING output from UpsertLiveChannelsWithHistory.
type UpsertResult struct {
	ID              int64
	Platform        model.Platform
	ChannelID       string
	SessionSeq      int64
	Status          string
	IsInsert        bool   // true if INSERT (new channel), false if UPDATE
	OldMetadataHash []byte // previous metadata_hash before this upsert
}

// UpsertLiveChannelsWithHistory is like UpsertLiveChannels but includes
// metadata fields and returns UpsertResult for history tracking.
// It does NOT modify the ChannelStore interface — only used by discover when HISTORY_ENABLED=true.
func (s *PgStore) UpsertLiveChannelsWithHistory(ctx context.Context, channels []model.LiveChannel) ([]UpsertResult, error) {
	if len(channels) == 0 {
		return nil, nil
	}

	// Deduplicate by (platform, channel_id)
	seen := make(map[string]int, len(channels))
	deduped := make([]model.LiveChannel, 0, len(channels))
	for _, ch := range channels {
		key := string(ch.Platform) + ":" + ch.ChannelID
		if idx, ok := seen[key]; ok {
			deduped[idx] = ch
			continue
		}
		seen[key] = len(deduped)
		deduped = append(deduped, ch)
	}

	var b strings.Builder
	args := make([]interface{}, 0, len(deduped)*10)

	b.WriteString(`INSERT INTO live_channels (platform, channel_id, streamer_name, viewer_count, title, category, category_code, tags, thumbnail_url, started_at, status, last_seen_at)
VALUES `)

	for i, ch := range deduped {
		if i > 0 {
			b.WriteString(", ")
		}
		base := i * 10
		fmt.Fprintf(&b, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, 'pending', NOW())",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10)
		tags := ch.Tags
		if tags == nil {
			tags = []string{}
		}
		args = append(args,
			string(ch.Platform), ch.ChannelID, ch.StreamerName, ch.ViewerCount,
			ch.Title, ch.Category, ch.CategoryCode, tags, ch.ThumbnailURL, ch.BroadcastStartedAt)
	}

	b.WriteString(` ON CONFLICT (platform, channel_id) DO UPDATE SET
        streamer_name = EXCLUDED.streamer_name,
        viewer_count = EXCLUDED.viewer_count,
        title = EXCLUDED.title,
        category = EXCLUDED.category,
        category_code = EXCLUDED.category_code,
        tags = EXCLUDED.tags,
        thumbnail_url = EXCLUDED.thumbnail_url,
        started_at = EXCLUDED.started_at,
        last_seen_at = NOW(),
        status = CASE
            WHEN live_channels.status = 'ended' THEN 'pending'
            ELSE live_channels.status
        END,
        ended_at = CASE
            WHEN live_channels.status = 'ended' THEN NULL
            ELSE live_channels.ended_at
        END,
        current_session_seq = CASE
            WHEN live_channels.status = 'ended'
            THEN live_channels.current_session_seq + 1
            ELSE live_channels.current_session_seq
        END
    RETURNING id, platform, channel_id, current_session_seq, status,
        (xmax = 0) AS is_insert, metadata_hash`)

	rows, err := s.pool.Query(ctx, b.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []UpsertResult
	for rows.Next() {
		var r UpsertResult
		var platformStr string
		if err := rows.Scan(&r.ID, &platformStr, &r.ChannelID, &r.SessionSeq, &r.Status, &r.IsInsert, &r.OldMetadataHash); err != nil {
			return nil, err
		}
		r.Platform = model.Platform(platformStr)
		results = append(results, r)
	}
	return results, rows.Err()
}
