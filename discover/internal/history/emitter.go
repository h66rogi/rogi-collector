package history

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/h66rogi/rogi-collector/discover/internal/discovery"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

const maxIntervalAge = 5 * time.Minute

type channelState struct {
	sessionSeq   int64
	metadataHash []byte
	viewerCount  int
	lastInterval *viewerInterval
	missedPolls  int
}

type viewerInterval struct {
	validFrom   time.Time
	viewerCount uint32
	minCount    uint32
	maxCount    uint32
	sampleCount uint16
}

// Emitter tracks broadcast sessions, metadata changes, and viewer count
// intervals. It writes sessions/metadata to PostgreSQL (via HistoryStore)
// and viewer-count intervals to ClickHouse (via BatchWriter).
type Emitter struct {
	historyStore store.HistoryStore
	chWriter     *store.BatchWriter[store.ViewerCountRow]
	instanceID   string
	states       map[string]*channelState
	mu           sync.RWMutex
	logger       *slog.Logger
	metrics      *Metrics
}

// NewEmitter creates a new Emitter.
func NewEmitter(hs store.HistoryStore, chWriter *store.BatchWriter[store.ViewerCountRow], instanceID string, metrics *Metrics) *Emitter {
	return &Emitter{
		historyStore: hs,
		chWriter:     chWriter,
		instanceID:   instanceID,
		states:       make(map[string]*channelState),
		logger:       slog.Default(),
		metrics:      metrics,
	}
}

// SetCHWriter replaces the ClickHouse batch writer. Thread-safe.
// Called when CH reconnects after a startup failure.
func (e *Emitter) SetCHWriter(w *store.BatchWriter[store.ViewerCountRow]) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.chWriter = w
}

func stateKey(platform model.Platform, channelID string) string {
	return string(platform) + ":" + channelID
}

func computeMetadataHash(title, category string, tags []string) []byte {
	sorted := make([]string, len(tags))
	copy(sorted, tags)
	sort.Strings(sorted)
	input := fmt.Sprintf("%s|%s|%s", title, category, strings.Join(sorted, ","))
	h := sha256.Sum256([]byte(input))
	return h[:]
}

func boundedViewerCount(value int) uint32 {
	if value <= 0 {
		return 0
	}
	if int64(value) >= int64(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(value)
}

func nonNegativeSessionSeq(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

// ProcessDiscovered handles upsert results: creates sessions for new channels,
// detects metadata changes, tracks viewer count changes.
func (e *Emitter) ProcessDiscovered(ctx context.Context, platform model.Platform, results []store.UpsertResult, discovered map[string]discovery.DiscoveredChannel) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	for _, r := range results {
		key := stateKey(r.Platform, r.ChannelID)
		ch, ok := discovered[r.ChannelID]
		if !ok {
			continue
		}
		viewerCount := boundedViewerCount(ch.ViewerCount)

		if r.IsInsert || e.states[key] == nil {
			// New channel: create session + first metadata + first viewer interval
			hash := computeMetadataHash(ch.Title, ch.Category, ch.Tags)
			sessionStartedAt := now
			if ch.StartedAt != nil {
				sessionStartedAt = *ch.StartedAt
			}
			if err := e.historyStore.InsertSession(ctx, nil, &store.BroadcastSession{
				StartedAt: sessionStartedAt, Platform: platform, ChannelID: r.ChannelID,
				SessionSeq: r.SessionSeq, StreamerName: ch.StreamerName,
				Title: ch.Title, Category: ch.Category,
				StartedObservedAt: now, FirstSeenAt: now, LastSeenAt: now,
				PeakViewerCount: ch.ViewerCount, InstanceID: e.instanceID,
			}); err != nil {
				e.logger.Error("insert session failed", "error", err, "channel", r.ChannelID)
				continue
			}
			if err := e.historyStore.InsertMetadata(ctx, nil, &store.MetadataHistoryRow{
				ValidFrom: now, Platform: platform, ChannelID: r.ChannelID,
				SessionSeq: r.SessionSeq, Title: ch.Title, Category: ch.Category,
				CategoryCode: ch.CategoryCode, Tags: ch.Tags, ThumbnailURL: ch.ThumbnailURL,
				MetadataHash: hash, ObservedAt: now, InstanceID: e.instanceID,
			}); err != nil {
				e.logger.Error("insert metadata failed", "error", err, "channel", r.ChannelID)
				continue
			}
			e.states[key] = &channelState{
				sessionSeq: r.SessionSeq, metadataHash: hash,
				viewerCount: ch.ViewerCount, missedPolls: 0,
				lastInterval: &viewerInterval{
					validFrom: now, viewerCount: viewerCount,
					minCount: viewerCount, maxCount: viewerCount,
					sampleCount: 1,
				},
			}
			continue
		}

		// Existing channel
		state := e.states[key]
		state.missedPolls = 0

		// Metadata hash comparison
		newHash := computeMetadataHash(ch.Title, ch.Category, ch.Tags)
		if !bytes.Equal(newHash, state.metadataHash) {
			if err := e.historyStore.CloseMetadata(ctx, nil, platform, r.ChannelID, state.sessionSeq, now); err != nil {
				e.logger.Error("close metadata failed", "error", err, "channel", r.ChannelID)
				// Don't update state.metadataHash — next poll will retry
			} else if err := e.historyStore.InsertMetadata(ctx, nil, &store.MetadataHistoryRow{
				ValidFrom: now, Platform: platform, ChannelID: r.ChannelID,
				SessionSeq: state.sessionSeq, Title: ch.Title, Category: ch.Category,
				CategoryCode: ch.CategoryCode, Tags: ch.Tags, ThumbnailURL: ch.ThumbnailURL,
				MetadataHash: newHash, ObservedAt: now, InstanceID: e.instanceID,
			}); err != nil {
				e.logger.Error("insert metadata failed", "error", err, "channel", r.ChannelID)
			} else {
				state.metadataHash = newHash
			}
		}

		// Viewer count change detection
		if ch.ViewerCount != state.viewerCount {
			e.flushViewerInterval(platform, r.ChannelID, state, now)
			state.viewerCount = ch.ViewerCount
			state.lastInterval = &viewerInterval{
				validFrom: now, viewerCount: viewerCount,
				minCount: viewerCount, maxCount: viewerCount,
				sampleCount: 1,
			}
		} else if state.lastInterval != nil {
			// Force-flush intervals older than 5 minutes to avoid silent gaps
			if now.Sub(state.lastInterval.validFrom) > maxIntervalAge {
				e.flushViewerInterval(platform, r.ChannelID, state, now)
				state.lastInterval = &viewerInterval{
					validFrom: now, viewerCount: viewerCount,
					minCount: viewerCount, maxCount: viewerCount,
					sampleCount: 1,
				}
			} else {
				state.lastInterval.sampleCount++
				if viewerCount < state.lastInterval.minCount {
					state.lastInterval.minCount = viewerCount
				}
				if viewerCount > state.lastInterval.maxCount {
					state.lastInterval.maxCount = viewerCount
				}
			}
		}

		// Update session last_seen
		if err := e.historyStore.UpdateSessionLastSeen(ctx, nil, platform, r.ChannelID, state.sessionSeq, now, ch.ViewerCount); err != nil {
			e.logger.Error("update session last_seen failed", "error", err, "channel", r.ChannelID)
		}
	}
}

func (e *Emitter) flushViewerInterval(platform model.Platform, channelID string, state *channelState, validTo time.Time) {
	if state.lastInterval == nil || e.chWriter == nil {
		return
	}
	e.chWriter.Enqueue(store.ViewerCountRow{
		ValidFrom: state.lastInterval.validFrom, ValidTo: &validTo,
		Platform: string(platform), ChannelID: channelID,
		SessionSeq: nonNegativeSessionSeq(state.sessionSeq), ViewerCount: state.lastInterval.viewerCount,
		MinCount: state.lastInterval.minCount, MaxCount: state.lastInterval.maxCount,
		SampleCount: state.lastInterval.sampleCount, InstanceID: e.instanceID,
	})
	state.lastInterval = nil
}

// EndedChannel holds info needed to close a session after MarkEndedChannels succeeds.
type EndedChannel struct {
	Platform  model.Platform
	ChannelID string
	StateKey  string
}

// DetectEndedChannels returns augmented activeIDs and a list of channels to end.
// The caller must call CommitEnded() after MarkEndedChannels succeeds.
func (e *Emitter) DetectEndedChannels(platform model.Platform, activeIDs []string, isPartial bool) (augmentedActiveIDs []string, toEnd []EndedChannel) {
	e.mu.Lock()
	defer e.mu.Unlock()

	activeSet := make(map[string]bool, len(activeIDs))
	for _, id := range activeIDs {
		activeSet[id] = true
	}

	augmentedActiveIDs = make([]string, len(activeIDs))
	copy(augmentedActiveIDs, activeIDs)
	prefix := string(platform) + ":"

	for key, state := range e.states {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		channelID := strings.TrimPrefix(key, prefix)
		if activeSet[channelID] {
			state.missedPolls = 0
			continue
		}

		if isPartial {
			augmentedActiveIDs = append(augmentedActiveIDs, channelID)
			continue
		}

		state.missedPolls++
		if state.missedPolls < 2 {
			augmentedActiveIDs = append(augmentedActiveIDs, channelID)
			continue
		}

		// Grace expired: collect for later commit (don't close session yet)
		toEnd = append(toEnd, EndedChannel{
			Platform:  platform,
			ChannelID: channelID,
			StateKey:  key,
		})
	}

	return augmentedActiveIDs, toEnd
}

// ResetMissedPolls clears the missing-list counter for channels that were
// verified live by a platform-specific detail endpoint.
func (e *Emitter) ResetMissedPolls(platform model.Platform, channelIDs []string) {
	if len(channelIDs) == 0 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, channelID := range channelIDs {
		if state := e.states[stateKey(platform, channelID)]; state != nil {
			state.missedPolls = 0
		}
	}
}

// CommitEnded actually closes sessions and cleans up state.
// Call after MarkEndedChannels succeeds.
func (e *Emitter) CommitEnded(ctx context.Context, ended []EndedChannel) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	for _, ec := range ended {
		state, ok := e.states[ec.StateKey]
		if !ok {
			continue
		}
		if err := e.historyStore.CloseSession(ctx, nil, ec.Platform, ec.ChannelID, state.sessionSeq, now, now, "grace_timeout"); err != nil {
			e.logger.Error("close session failed", "error", err, "channel", ec.ChannelID)
			continue
		}
		e.flushViewerInterval(ec.Platform, ec.ChannelID, state, now)
		delete(e.states, ec.StateKey)
	}
}

// RecoverState loads open sessions from PG and rebuilds in-memory state.
// Must be called before polling starts (in onBecomeLeader).
func (e *Emitter) RecoverState(ctx context.Context) error {
	// Close orphaned sessions first: broadcast_sessions with ended_at IS NULL
	// but corresponding live_channels already ended or missing.
	if closed, err := e.historyStore.CloseOrphanedSessions(ctx); err != nil {
		return fmt.Errorf("close orphaned sessions: %w", err)
	} else if closed > 0 {
		e.logger.Info("closed orphaned sessions during recovery", "count", closed)
	}

	sessions, err := e.historyStore.ListOpenSessions(ctx)
	if err != nil {
		return fmt.Errorf("list open sessions: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.states = make(map[string]*channelState, len(sessions))
	now := time.Now()

	for _, s := range sessions {
		key := stateKey(s.Platform, s.ChannelID)

		// Duplicate session for the same channel: close older one.
		// ListOpenSessions returns session_seq DESC, so the first
		// session per channel is the newest.
		if _, exists := e.states[key]; exists {
			if err := e.historyStore.CloseSession(ctx, nil, s.Platform, s.ChannelID,
				s.SessionSeq, s.LastSeenAt, now, "duplicate_cleanup"); err != nil {
				e.logger.Error("close duplicate session failed", "error", err,
					"platform", s.Platform, "channel", s.ChannelID, "seq", s.SessionSeq)
			}
			continue
		}

		hash, _ := e.historyStore.GetCurrentMetadataHash(ctx, s.Platform, s.ChannelID, s.SessionSeq)

		viewerCount, err := e.historyStore.GetChannelViewerCount(ctx, s.Platform, s.ChannelID)
		if err != nil {
			viewerCount = s.PeakViewerCount // fallback
		}

		e.states[key] = &channelState{
			sessionSeq:   s.SessionSeq,
			metadataHash: hash,
			viewerCount:  viewerCount,
			lastInterval: nil,
			missedPolls:  0,
		}

		// Gap detection
		interval := 60 * time.Second
		if s.Platform == model.PlatformCime {
			interval = 30 * time.Second
		}
		if gap := now.Sub(s.LastSeenAt); gap > interval*3 {
			if err := e.historyStore.MarkSessionGap(ctx, s.Platform, s.ChannelID, s.SessionSeq); err != nil {
				e.logger.Warn("mark session gap failed", "platform", s.Platform,
					"channel", s.ChannelID, "seq", s.SessionSeq, "error", err)
			}
		}
	}

	return nil
}

// FlushAll drains all pending viewer intervals. Called on leadership loss.
func (e *Emitter) FlushAll() {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	for key, state := range e.states {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		e.flushViewerInterval(model.Platform(parts[0]), parts[1], state, now)
	}
}
