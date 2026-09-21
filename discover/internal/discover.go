package internal

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/h66rogi/rogi-collector/discover/internal/discovery"
	"github.com/h66rogi/rogi-collector/discover/internal/history"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

// Config holds discover service configuration values.
type Config struct {
	InstanceID     string
	LeaderElection LeaderElectionConfig
	// Allowlist limits discovery to specific channels per platform.
	// nil means no filtering (all channels processed).
	// Format: map[platform] → set of channel IDs.
	Allowlist map[model.Platform]map[string]bool
	// CycleTimeouts overrides the per-cycle fetch timeout for specific platforms.
	// Platforms not in this map use the default (45s).
	CycleTimeouts map[model.Platform]time.Duration
	// DiscoverIntervals overrides the poll interval for specific platforms.
	// Platforms not in this map use the built-in defaults (180s chzzk/soop, 30s cime).
	DiscoverIntervals map[model.Platform]time.Duration
}

// Discover orchestrates leader election and platform discovery loops.
type Discover struct {
	instanceID        string
	leader            *LeaderElection
	discoveries       []discovery.PlatformDiscovery
	pgChannel         store.ChannelStore
	redisStore        *store.RedisStore
	metrics           *DiscoverMetrics
	logger            *slog.Logger
	cancelLoops       context.CancelFunc
	wg                sync.WaitGroup
	emitter           *history.Emitter                         // broadcast history emitter (nil when HISTORY_ENABLED=false)
	pgStore           *store.PgStore                           // direct PgStore ref for UpsertLiveChannelsWithHistory
	chWriter          *store.BatchWriter[store.ViewerCountRow] // CH batch writer (nil when CH unavailable)
	chMu              sync.Mutex                               // guards chWriter
	notifier          *BackNotifier                            // channel-end callback (nil when not configured)
	allowlist         map[model.Platform]map[string]bool       // nil = no filtering (process all channels)
	collectionOptOuts *store.CollectionOptOutCache             // nil = collection opt-out filtering disabled
	cycleTimeouts     map[model.Platform]time.Duration
	discoverIntervals map[model.Platform]time.Duration
	triggerChs        []chan struct{} // one buffered(1) channel per discovery loop
	triggerMu         sync.Mutex
}

// NewDiscover wires together all discover sub-components.
func NewDiscover(
	cfg Config,
	pgChannel store.ChannelStore,
	redisStore *store.RedisStore,
	discoveries []discovery.PlatformDiscovery,
	metrics *DiscoverMetrics,
	logger *slog.Logger,
) *Discover {
	d := &Discover{
		instanceID:        cfg.InstanceID,
		discoveries:       discoveries,
		pgChannel:         pgChannel,
		redisStore:        redisStore,
		metrics:           metrics,
		logger:            logger.With("component", "discover"),
		allowlist:         cfg.Allowlist,
		cycleTimeouts:     cfg.CycleTimeouts,
		discoverIntervals: cfg.DiscoverIntervals,
	}

	d.leader = NewLeaderElection(
		cfg.InstanceID,
		"discover:leader",
		redisStore,
		cfg.LeaderElection,
		logger,
		d.onBecomeLeader,
		d.onLoseLeadership,
	)

	return d
}

// Run starts the leader election loop. It blocks until ctx is cancelled.
func (d *Discover) Run(ctx context.Context) {
	d.logger.Info("starting discover", "instanceID", d.instanceID)
	d.leader.Run(ctx)
}

// SetEmitter enables broadcast history tracking. When set, discoveryLoop
// uses UpsertLiveChannelsWithHistory and emits session/metadata/viewer events.
// chWriter may be nil when ClickHouse is unavailable; PG history still works.
func (d *Discover) SetEmitter(emitter *history.Emitter, pgStore *store.PgStore, chWriter *store.BatchWriter[store.ViewerCountRow]) {
	d.emitter = emitter
	d.pgStore = pgStore
	d.chWriter = chWriter
}

// SetCHWriter updates the ClickHouse batch writer at runtime.
// Called when CH reconnects after a startup failure.
func (d *Discover) SetCHWriter(w *store.BatchWriter[store.ViewerCountRow]) {
	d.chMu.Lock()
	d.chWriter = w
	d.chMu.Unlock()
	if d.emitter != nil {
		d.emitter.SetCHWriter(w)
	}
}

// SetNotifier enables upstream force-end callback calls on channel end.
func (d *Discover) SetNotifier(n *BackNotifier) {
	d.notifier = n
}

// SetCollectionOptOutCache enables dynamic platform-channel collection blocking.
func (d *Discover) SetCollectionOptOutCache(cache *store.CollectionOptOutCache) {
	d.collectionOptOuts = cache
}

// onBecomeLeader starts per-platform discovery loop goroutines.
// IMPORTANT: This callback runs inside the leader election Run() loop.
// It must return quickly so the renewal timer can fire before the lease expires.
// RecoverState and discovery loops are started in a background goroutine.
func (d *Discover) onBecomeLeader() {
	d.logger.Info("became leader, starting discovery loops")

	ctx, cancel := context.WithCancel(context.Background())
	d.cancelLoops = cancel

	// Start discovery loops asynchronously to avoid blocking the leader
	// election renewal timer. RecoverState does N PG queries per open session
	// and can easily exceed the 15s lease TTL.
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		if d.emitter != nil {
			// Retry RecoverState with backoff to avoid silent idle leader.
			for attempt := 1; ; attempt++ {
				if err := d.emitter.RecoverState(ctx); err != nil {
					if ctx.Err() != nil {
						d.logger.Info("RecoverState aborted (leadership lost)")
						return
					}
					delay := time.Duration(attempt) * 2 * time.Second
					if delay > 10*time.Second {
						delay = 10 * time.Second
					}
					d.logger.Error("history state recovery failed, retrying",
						"error", err, "attempt", attempt, "retryIn", delay)
					select {
					case <-ctx.Done():
						return
					case <-time.After(delay):
						continue
					}
				}
				break
			}
		}

		d.triggerMu.Lock()
		d.triggerChs = nil
		for _, disc := range d.discoveries {
			disc := disc
			platform := disc.Platform()

			if d.allowlist != nil && len(d.allowlist[platform]) == 0 {
				d.logger.Info("skipping platform (not in allowlist)", "platform", platform)
				continue
			}

			interval := d.discoverInterval(platform)
			triggerCh := make(chan struct{}, 1)
			d.triggerChs = append(d.triggerChs, triggerCh)
			d.wg.Add(1)
			go func() {
				defer d.wg.Done()
				d.discoveryLoop(ctx, disc, interval, triggerCh)
			}()
		}
		d.triggerMu.Unlock()
	}()
}

// TriggerDiscovery forces an immediate discovery cycle for all platforms.
func (d *Discover) TriggerDiscovery() {
	d.triggerMu.Lock()
	defer d.triggerMu.Unlock()
	for _, ch := range d.triggerChs {
		select {
		case ch <- struct{}{}:
		default: // already triggered
		}
	}
}

// TriggerDiscoveryForPlatform forces an immediate cycle for a single platform.
// Falls back to TriggerDiscovery if platform is empty or no matching loop exists.
func (d *Discover) TriggerDiscoveryForPlatform(platform model.Platform) {
	if platform == "" {
		d.TriggerDiscovery()
		return
	}
	d.triggerMu.Lock()
	defer d.triggerMu.Unlock()
	// triggerChs are aligned 1:1 with d.discoveries by index.
	for i, disc := range d.discoveries {
		if disc.Platform() != platform {
			continue
		}
		if i >= len(d.triggerChs) {
			break
		}
		select {
		case d.triggerChs[i] <- struct{}{}:
		default: // already triggered
		}
		return
	}
	d.logger.Warn("discovery: trigger requested for unknown platform", "platform", platform)
}

// SubscribeAdminCommands runs a Redis pub/sub loop that listens for
// DiscoverCommand messages and acts on them. Only the leader instance fires
// the underlying Trigger* methods; non-leader instances ignore commands so
// the cycle isn't fired multiple times. Blocks until ctx is cancelled.
func (d *Discover) SubscribeAdminCommands(ctx context.Context) {
	sub := d.redisStore.SubscribeDiscoverCommands(ctx)
	defer sub.Close()

	ch := sub.Channel()
	d.logger.Info("subscribed to discover admin command channel")

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var cmd store.DiscoverCommand
			if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
				d.logger.Error("invalid discover command", "error", err, "payload", msg.Payload)
				continue
			}
			if !d.leader.IsLeader() {
				// Followers acknowledge silently; leader will process.
				continue
			}
			switch cmd.Type {
			case store.DiscoverCommandRediscover:
				d.logger.Info("admin: rediscover triggered",
					"platform", cmd.Platform, "channel", cmd.ChannelID)
				d.TriggerDiscoveryForPlatform(model.Platform(cmd.Platform))
			default:
				d.logger.Warn("unknown discover command type", "type", cmd.Type)
			}
		}
	}
}

// onLoseLeadership cancels all loops and waits for goroutines to finish.
func (d *Discover) onLoseLeadership() {
	d.logger.Info("lost leadership, stopping discovery loops")
	if d.cancelLoops != nil {
		d.cancelLoops()
		d.cancelLoops = nil
	}
	d.wg.Wait()

	if d.emitter != nil {
		d.emitter.FlushAll()
	}
	d.chMu.Lock()
	w := d.chWriter
	d.chMu.Unlock()
	if w != nil {
		w.Flush()
	}
	d.logger.Info("all discovery loops stopped")
}

// cycleTimeout returns the per-cycle fetch timeout for a platform.
func (d *Discover) cycleTimeout(p model.Platform) time.Duration {
	if d.cycleTimeouts != nil {
		if t, ok := d.cycleTimeouts[p]; ok {
			return t
		}
	}
	return 45 * time.Second
}

// discoverInterval returns the poll interval for a platform, respecting overrides.
func (d *Discover) discoverInterval(p model.Platform) time.Duration {
	if d.discoverIntervals != nil {
		if t, ok := d.discoverIntervals[p]; ok {
			return t
		}
	}
	switch p {
	case model.PlatformChzzk, model.PlatformSoop:
		return 180 * time.Second
	case model.PlatformCime:
		return 120 * time.Second
	default:
		return 120 * time.Second
	}
}

// discoveryLoop polls a platform for live channels at the given interval.
// It also listens on triggerCh for on-demand discovery requests.
func (d *Discover) discoveryLoop(ctx context.Context, disc discovery.PlatformDiscovery, interval time.Duration, triggerCh <-chan struct{}) {
	platform := disc.Platform()
	platformStr := string(platform)
	logger := d.logger.With("platform", platform)

	var consecutiveFailures int
	if !d.runDiscoveryCycle(ctx, disc, platform, platformStr, logger, &consecutiveFailures) {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !d.runDiscoveryCycle(ctx, disc, platform, platformStr, logger, &consecutiveFailures) {
				return
			}
		case <-triggerCh:
			logger.Info("discovery: manual trigger received")
			if !d.runDiscoveryCycle(ctx, disc, platform, platformStr, logger, &consecutiveFailures) {
				return
			}
			ticker.Reset(interval)
		}
	}
}

func (d *Discover) runDiscoveryCycle(
	ctx context.Context,
	disc discovery.PlatformDiscovery,
	platform model.Platform,
	platformStr string,
	logger *slog.Logger,
	consecutiveFailures *int,
) bool {
	// Capture the fencing token at the start of the cycle.  Each destructive
	// step validates that this instance still holds the same lease.
	cycleToken := d.leader.FencingToken()
	if cycleToken == 0 {
		logger.Warn("discovery: skipped cycle, no valid lease")
		return true
	}

	requireLease := func(step string) bool {
		valid, err := d.redisStore.ValidateLeaderLease(ctx, "discover:leader", cycleToken)
		if err != nil {
			logger.Warn("discovery: lease validation failed, aborting cycle",
				"step", step, "error", err)
			return false
		}
		if !valid {
			logger.Warn("discovery: lease token mismatch, aborting cycle",
				"step", step, "expectedToken", cycleToken)
			return false
		}
		return true
	}

	cycleStart := time.Now()

	// Per-cycle timeout to prevent slow pagination from exceeding the poll interval.
	cycleCtx, cancel := context.WithTimeout(ctx, d.cycleTimeout(platform))
	defer cancel()
	result, err := disc.FetchLiveChannels(cycleCtx)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}

		*consecutiveFailures = *consecutiveFailures + 1
		if d.metrics != nil {
			d.metrics.CycleTotal.WithLabelValues(platformStr, "error").Inc()
			d.metrics.ConsecutiveFailures.WithLabelValues(platformStr).Set(float64(*consecutiveFailures))
			d.metrics.CycleDurationSeconds.WithLabelValues(platformStr).Observe(time.Since(cycleStart).Seconds())
		}
		logger.Error("discovery: fetch failed",
			"error", err,
			"consecutiveFailures", *consecutiveFailures)
		return true
	}

	*consecutiveFailures = 0
	channels := result.Channels
	if d.metrics != nil {
		d.metrics.ConsecutiveFailures.WithLabelValues(platformStr).Set(0)
		d.metrics.ChannelsFound.WithLabelValues(platformStr).Set(float64(len(channels)))
	}

	// Apply the optional channel allowlist filter.
	// Platforms not in the allowlist produce an empty channel list, which
	// causes MarkEndedChannels to clean up any previously-live channels.
	if d.allowlist != nil {
		allowed := d.allowlist[platform]
		before := len(channels)
		filtered := make([]discovery.DiscoveredChannel, 0, len(allowed))
		for _, ch := range channels {
			if allowed[ch.ChannelID] {
				filtered = append(filtered, ch)
			}
		}
		channels = filtered
		logger.Info("discovery: allowlist applied", "before", before, "after", len(channels))
	}

	if d.collectionOptOuts != nil {
		before := len(channels)
		filtered := make([]discovery.DiscoveredChannel, 0, len(channels))
		for _, ch := range channels {
			if !d.collectionOptOuts.IsOptedOut(platform, ch.ChannelID) {
				filtered = append(filtered, ch)
			}
		}
		channels = filtered
		if before != len(channels) {
			logger.Info("discovery: collection opt-out filter applied", "before", before, "after", len(channels))
		}
	}

	// Convert discovered channels to LiveChannel models for upsert.
	liveChannels := make([]model.LiveChannel, len(channels))
	activeIDs := make([]string, len(channels))
	for i, ch := range channels {
		liveChannels[i] = model.LiveChannel{
			Platform:           platform,
			ChannelID:          ch.ChannelID,
			StreamerName:       ch.StreamerName,
			ViewerCount:        ch.ViewerCount,
			Title:              &ch.Title,
			Category:           &ch.Category,
			CategoryCode:       &ch.CategoryCode,
			Tags:               ch.Tags,
			ThumbnailURL:       &ch.ThumbnailURL,
			BroadcastStartedAt: ch.StartedAt,
		}
		activeIDs[i] = ch.ChannelID
	}

	if d.emitter != nil && d.pgStore != nil {
		// History-aware upsert path.
		if !requireLease("upsert-history") {
			return true
		}
		results, err := d.pgStore.UpsertLiveChannelsWithHistory(ctx, liveChannels)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			logger.Error("discovery: upsert with history failed", "error", err)
			if d.metrics != nil {
				d.metrics.CycleTotal.WithLabelValues(platformStr, "error").Inc()
			}
			return true
		}

		discoveredMap := make(map[string]discovery.DiscoveredChannel, len(channels))
		for _, ch := range channels {
			discoveredMap[ch.ChannelID] = ch
		}

		d.emitter.ProcessDiscovered(ctx, platform, results, discoveredMap)

		if d.metrics != nil {
			d.metrics.ChannelsUpsertedTotal.WithLabelValues(platformStr).Add(float64(len(liveChannels)))
		}
	} else {
		// Original path (HISTORY_ENABLED=false).
		if !requireLease("upsert") {
			return true
		}
		if err := d.pgChannel.UpsertLiveChannels(ctx, liveChannels); err != nil {
			if ctx.Err() != nil {
				return false
			}
			logger.Error("discovery: upsert failed", "error", err)
			if d.metrics != nil {
				d.metrics.CycleTotal.WithLabelValues(platformStr, "error").Inc()
			}
			return true
		}
		if d.metrics != nil {
			d.metrics.ChannelsUpsertedTotal.WithLabelValues(platformStr).Add(float64(len(liveChannels)))
		}
	}

	// Skip MarkEndedChannels when the result is partial (e.g. Chzzk pagination
	// failed mid-way). Marking channels as ended from an incomplete active list
	// would incorrectly kill live connections.
	if result.Partial {
		if d.emitter != nil {
			d.emitter.DetectEndedChannels(platform, activeIDs, true)
		}
		logger.Warn("discovery: partial result, skipping MarkEndedChannels",
			"discovered", len(channels))
	} else {
		markEndedIDs := activeIDs
		var toEnd []history.EndedChannel
		if d.emitter != nil {
			markEndedIDs, toEnd = d.emitter.DetectEndedChannels(platform, activeIDs, false)
			markEndedIDs, toEnd = d.validateEndedCandidates(cycleCtx, disc, platform, markEndedIDs, toEnd, logger)
		}
		if !requireLease("mark-ended") {
			return true
		}
		endedIDs, err := d.pgChannel.MarkEndedChannels(ctx, platform, markEndedIDs)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			logger.Error("discovery: mark ended failed", "error", err)
			if d.metrics != nil {
				d.metrics.CycleTotal.WithLabelValues(platformStr, "error").Inc()
			}
			return true
		}
		if d.notifier != nil && len(endedIDs) > 0 {
			d.notifier.Notify(ctx, platform, endedIDs)
		}
		if d.emitter != nil && len(toEnd) > 0 {
			d.emitter.CommitEnded(ctx, toEnd)
		}
		if d.metrics != nil && len(endedIDs) > 0 {
			d.metrics.ChannelsEndedTotal.WithLabelValues(platformStr).Add(float64(len(endedIDs)))
		}
	}

	if d.metrics != nil {
		cycleStatus := "success"
		if result.Partial {
			cycleStatus = "partial"
		}
		d.metrics.CycleTotal.WithLabelValues(platformStr, cycleStatus).Inc()
		d.metrics.CycleDurationSeconds.WithLabelValues(platformStr).Observe(time.Since(cycleStart).Seconds())
	}

	logger.Info("discovery: cycle complete",
		"discovered", len(channels),
		"partial", result.Partial)

	return true
}

func (d *Discover) validateEndedCandidates(
	ctx context.Context,
	disc discovery.PlatformDiscovery,
	platform model.Platform,
	markEndedIDs []string,
	toEnd []history.EndedChannel,
	logger *slog.Logger,
) ([]string, []history.EndedChannel) {
	if len(toEnd) == 0 {
		return markEndedIDs, toEnd
	}

	confirmedEnded := make([]history.EndedChannel, 0, len(toEnd))
	verifiedLiveIDs := make([]string, 0)
	unknownIDs := make([]string, 0)

	for _, candidate := range toEnd {
		live, err := disc.IsChannelLive(ctx, candidate.ChannelID)
		if err != nil {
			markEndedIDs = append(markEndedIDs, candidate.ChannelID)
			unknownIDs = append(unknownIDs, candidate.ChannelID)
			continue
		}
		if live {
			markEndedIDs = append(markEndedIDs, candidate.ChannelID)
			verifiedLiveIDs = append(verifiedLiveIDs, candidate.ChannelID)
			continue
		}
		confirmedEnded = append(confirmedEnded, candidate)
	}

	if d.emitter != nil && len(verifiedLiveIDs) > 0 {
		d.emitter.ResetMissedPolls(platform, verifiedLiveIDs)
	}
	if len(verifiedLiveIDs) > 0 {
		logger.Warn("discovery: suppressing force-end for channels verified live",
			"count", len(verifiedLiveIDs), "channels", sampleIDs(verifiedLiveIDs, 10))
	}
	if len(unknownIDs) > 0 {
		logger.Warn("discovery: suppressing force-end for channels with inconclusive live checks",
			"count", len(unknownIDs), "channels", sampleIDs(unknownIDs, 10))
	}

	return markEndedIDs, confirmedEnded
}

func sampleIDs(ids []string, limit int) []string {
	if len(ids) <= limit {
		return ids
	}
	sample := make([]string, limit)
	copy(sample, ids[:limit])
	return sample
}

// ObserveAPIDuration records the duration of a platform API request.
// Implements discovery.APIMetrics.
func (m *DiscoverMetrics) ObserveAPIDuration(platform, endpoint string, duration time.Duration) {
	m.APIRequestDurationSeconds.WithLabelValues(platform, endpoint).Observe(duration.Seconds())
}

// IncAPIRequest increments the API request counter.
// Implements discovery.APIMetrics.
func (m *DiscoverMetrics) IncAPIRequest(platform, endpoint, status string) {
	m.APIRequestTotal.WithLabelValues(platform, endpoint, status).Inc()
}
