package discovery

import (
	"context"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// DiscoveredChannel represents a live channel found via platform API.
type DiscoveredChannel struct {
	ChannelID    string
	StreamerName string
	ViewerCount  int

	Title        string
	Category     string
	CategoryCode string
	Tags         []string
	StartedAt    *time.Time
	ThumbnailURL string
}

// DiscoveryResult wraps a list of discovered channels and indicates whether
// the result is partial (e.g. pagination failed mid-way). When Partial is
// true, callers should NOT mark missing channels as ended because the
// active list is incomplete.
type DiscoveryResult struct {
	Channels []DiscoveredChannel
	Partial  bool
}

// PlatformDiscovery defines the interface for discovering live channels
// on a streaming platform.
type PlatformDiscovery interface {
	// Platform returns which platform this discovery handles.
	Platform() model.Platform

	// FetchLiveChannels returns the list of currently live channels.
	FetchLiveChannels(ctx context.Context) (DiscoveryResult, error)

	// IsChannelLive checks whether a specific channel is currently live.
	IsChannelLive(ctx context.Context, channelID string) (bool, error)
}

// APIMetrics allows platform discovery implementations to record API-level
// metrics without importing the parent internal package.
type APIMetrics interface {
	// ObserveAPIDuration records the duration of a platform API request.
	ObserveAPIDuration(platform, endpoint string, duration time.Duration)
	// IncAPIRequest increments the API request counter.
	IncAPIRequest(platform, endpoint, status string)
}
