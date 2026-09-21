package model

import "time"

// Channel status constants.
const (
	ChannelStatusPending = "pending"
	ChannelStatusLive    = "live"
	ChannelStatusEnded   = "ended"
)

// LiveChannel represents a live streaming channel discovered by the discover service.
type LiveChannel struct {
	ID           int64      `db:"id"`
	Platform     Platform   `db:"platform"`
	ChannelID    string     `db:"channel_id"`
	StreamerName string     `db:"streamer_name"`
	ViewerCount  int        `db:"viewer_count"`
	Status       string     `db:"status"`
	WorkerID     *string    `db:"worker_id"`
	DiscoveredAt time.Time  `db:"discovered_at"`
	LastSeenAt   time.Time  `db:"last_seen_at"`
	EndedAt      *time.Time `db:"ended_at"`

	// Handoff fields for graceful drain
	HandoffFrom      *string    `db:"handoff_from"`
	HandoffStatus    *string    `db:"handoff_status"`
	HandoffStartedAt *time.Time `db:"handoff_started_at"`

	// Broadcast history fields (added by 003_broadcast_history migration)
	CurrentSessionSeq  int64      `db:"current_session_seq"`
	Title              *string    `db:"title"`
	Category           *string    `db:"category"`
	CategoryCode       *string    `db:"category_code"`
	Tags               []string   `db:"tags"`
	ThumbnailURL       *string    `db:"thumbnail_url"`
	BroadcastStartedAt *time.Time `db:"started_at"`
	MetadataHash       []byte     `db:"metadata_hash"`
}

// BroadcastChannel is the result row of SearchBroadcastChannels: a channel that
// matched a keyword in its latest broadcast metadata, joined with the most recent
// broadcast_sessions row and (optionally) the current live_channels row.
//
// Used by the collection-page channel picker to surface channels that recently
// broadcast a given content even when not currently live.
type BroadcastChannel struct {
	Platform           Platform   `db:"platform"`
	ChannelID          string     `db:"channel_id"`
	StreamerName       string     `db:"streamer_name"`
	Title              string     `db:"title"`
	Category           *string    `db:"category"`
	Tags               []string   `db:"tags"`
	ThumbnailURL       *string    `db:"thumbnail_url"`
	LastStartedAt      *time.Time `db:"started_at"`
	LastEndedAt        *time.Time `db:"ended_at"`
	PeakViewerCount    int        `db:"peak_viewer_count"`
	IsLive             bool       `db:"is_live"`
	CurrentViewerCount int        `db:"current_viewer_count"`
}
