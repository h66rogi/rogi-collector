package internal

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

// Config holds coordinator configuration values.
type Config struct {
	InstanceID    string
	HealthTimeout time.Duration
	AssignBatch   int
	LoadThreshold float64 // max load ratio for accepting assignments (0 uses default 0.95)
}

// Coordinator orchestrates leader election, worker health checks,
// and channel assignment. Platform discovery is handled by the
// separate discover service.
type Coordinator struct {
	instanceID    string
	leader        *LeaderElection
	healthChecker *HealthChecker
	assigner      *Assigner
	pgChannel     store.ChannelStore
	pgWorker      store.WorkerStore
	redisStore    *store.RedisStore
	metrics       *CoordinatorMetrics
	logger        *slog.Logger
	cancelLoops   context.CancelFunc
	wg            sync.WaitGroup

	healthMu         sync.RWMutex
	lastHealthResult *HealthCheckResult

	debounceMu       sync.Mutex
	debounceMap      map[string]time.Time
	unassignCooldown map[string]time.Time // prevents assign→unassign oscillation
}

// NewCoordinator wires together all coordinator sub-components.
func NewCoordinator(
	cfg Config,
	pgChannel store.ChannelStore,
	pgWorker store.WorkerStore,
	redisStore *store.RedisStore,
	metrics *CoordinatorMetrics,
	logger *slog.Logger,
) *Coordinator {
	c := &Coordinator{
		instanceID:       cfg.InstanceID,
		pgChannel:        pgChannel,
		pgWorker:         pgWorker,
		redisStore:       redisStore,
		metrics:          metrics,
		logger:           logger.With("component", "coordinator"),
		debounceMap:      make(map[string]time.Time),
		unassignCooldown: make(map[string]time.Time),
	}

	c.healthChecker = NewHealthChecker(pgWorker, redisStore, cfg.HealthTimeout, logger)
	loadThreshold := cfg.LoadThreshold
	if loadThreshold <= 0 {
		loadThreshold = 0.95
	}
	c.assigner = NewAssigner(pgChannel, redisStore, metrics, cfg.AssignBatch, loadThreshold, logger)
	c.leader = NewLeaderElection(
		cfg.InstanceID,
		"coordinator:leader",
		redisStore,
		logger,
		c.onBecomeLeader,
		c.onLoseLeadership,
	)

	return c
}

// Run starts the leader election loop. It blocks until ctx is cancelled.
func (c *Coordinator) Run(ctx context.Context) {
	c.logger.Info("starting coordinator", "instanceID", c.instanceID)
	c.leader.Run(ctx)
}

// onBecomeLeader starts all coordinator goroutines.
func (c *Coordinator) onBecomeLeader() {
	c.logger.Info("became leader, starting loops")
	c.metrics.IsLeader.Set(1)

	// Clean up zombie channels assigned to dead workers immediately. The same
	// reconciliation also runs in every health-check cycle so a transient DB
	// outage during worker shutdown cannot strand channels until leadership
	// changes.
	c.cleanupZombieChannels()

	// Resume any pending handoffs from draining workers that the previous leader may have started.
	c.resumeDrainingHandoffs()

	// Reset debounce state from any prior leadership term.
	c.debounceMu.Lock()
	c.debounceMap = make(map[string]time.Time)
	c.debounceMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	c.cancelLoops = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.assignmentLoop(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.healthCheckLoop(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.eventListenerLoop(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.streamCleanupLoop(ctx)
	}()
}

// onLoseLeadership cancels all loops and waits for them to finish.
func (c *Coordinator) onLoseLeadership() {
	c.logger.Info("lost leadership, stopping loops")
	if c.cancelLoops != nil {
		c.cancelLoops()
	}
	c.wg.Wait()
	c.metrics.ResetLeaderGauges()
	c.logger.Info("all loops stopped")
}

// assignmentLoop reads the latest health result and assigns pending channels every 5 seconds.
func (c *Coordinator) assignmentLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			start := time.Now()

			c.healthMu.RLock()
			result := c.lastHealthResult
			c.healthMu.RUnlock()

			if result == nil {
				continue
			}

			// Augment worker loads with DB assigned count to prevent over-assignment.
			// Prometheus connections only reflects actual WebSocket count (capped at MAX_CONNECTIONS),
			// but DB may have more channels assigned. Use the higher value.
			dbCounts, dbErr := c.pgChannel.CountAssignedByWorker(ctx)
			if dbErr != nil {
				c.logger.Warn("failed to get DB assigned counts, using Prometheus only", "error", dbErr)
			} else {
				for id, load := range result.WorkerLoads {
					if dbCount, ok := dbCounts[id]; ok && dbCount > load.Connections {
						load.Connections = dbCount
						result.WorkerLoads[id] = load
					}
				}
			}

			if dbErr == nil {
				var totalAssigned int
				for _, count := range dbCounts {
					totalAssigned += count
				}
				c.metrics.ChannelsAssigned.Set(float64(totalAssigned))
			}

			assigned, err := c.assigner.AssignPending(ctx, result.WorkerLoads)
			c.metrics.AssignCycleDurationSeconds.Observe(time.Since(start).Seconds())

			if err != nil {
				c.logger.Error("assignment-loop: assign failed", "error", err)
				c.metrics.AssignErrorsTotal.WithLabelValues("assign_pending").Inc()
				continue
			}

			if assigned > 0 {
				c.logger.Info("assignment-loop: assigned channels", "count", assigned)
			}

			// Reassign channels from draining workers
			if n, err := c.assigner.ReassignDrainingChannels(ctx, result.WorkerLoads); err != nil {
				c.logger.Error("reassign draining channels failed", "error", err)
			} else if n > 0 {
				c.logger.Info("reassigned draining channels", "count", n)
			}

			// Retry stuck handoffs (pending > 30s)
			if n, err := c.assigner.RetryStuckHandoffs(ctx, result.WorkerLoads, 30*time.Second); err != nil {
				c.logger.Error("retry stuck handoffs failed", "error", err)
			} else if n > 0 {
				c.logger.Warn("retried stuck handoffs", "count", n)
			}
		}
	}
}

const unassignCooldownTTL = 10 * time.Minute

// handleUnassign resets a channel back to pending when a worker reports
// repeated connection failures. This prevents ghost channels from consuming
// capacity slots indefinitely.
// A 10-minute cooldown prevents assign→unassign oscillation.
func (c *Coordinator) handleUnassign(ctx context.Context, event store.CoordEvent) {
	platform := event.Platform
	channelID := event.ChannelID
	workerID := event.WorkerID
	key := string(platform) + ":" + channelID

	// Check cooldown to prevent oscillation
	c.debounceMu.Lock()
	if last, ok := c.unassignCooldown[key]; ok && time.Since(last) < unassignCooldownTTL {
		c.debounceMu.Unlock()
		c.logger.Debug("event-listener: unassign cooldown active",
			"platform", platform, "channelID", channelID)
		return
	}
	c.unassignCooldown[key] = time.Now()
	// Lazy eviction
	if len(c.unassignCooldown) > 1000 {
		now := time.Now()
		for k, t := range c.unassignCooldown {
			if now.Sub(t) > unassignCooldownTTL {
				delete(c.unassignCooldown, k)
			}
		}
	}
	c.debounceMu.Unlock()

	if err := c.pgChannel.UnassignChannel(ctx, platform, channelID, workerID); err != nil {
		c.logger.Error("event-listener: unassign channel failed",
			"platform", platform, "channelID", channelID, "error", err)
		return
	}

	c.logger.Info("event-listener: unassigned ghost channel",
		"platform", platform, "channelID", channelID, "workerID", workerID)
	c.metrics.ChannelsUnassignedTotal.WithLabelValues("connect_failed").Inc()

	// Tell the worker to disconnect so it clears local state
	cmd := store.WorkerCommand{
		Type:      store.WorkerCommandDisconnect,
		Platform:  platform,
		ChannelID: channelID,
	}
	if err := c.redisStore.PublishWorkerCommand(ctx, workerID, cmd); err != nil {
		c.logger.Error("event-listener: publish disconnect after unassign failed", "error", err)
	}
}

// healthCheckLoop runs health checks every 5 seconds, stores the result for
// assignmentLoop, and marks dead workers.
func (c *Coordinator) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			start := time.Now()

			// This reconciliation is deliberately independent from Redis-based
			// worker health checks. Once PostgreSQL recovers, it must repair any
			// already-dead worker assignments even if another dependency is still
			// unavailable.
			c.reconcileDeadWorkerChannels(ctx)

			result, err := c.healthChecker.Check(ctx)
			c.metrics.HealthcheckDurationSeconds.Observe(time.Since(start).Seconds())

			if err != nil {
				c.logger.Error("health-check-loop: check failed", "error", err)
				c.metrics.HealthcheckErrorsTotal.WithLabelValues("check").Inc()
				continue
			}

			c.healthMu.Lock()
			c.lastHealthResult = result
			c.healthMu.Unlock()

			c.metrics.AliveWorkers.Set(float64(len(result.AliveWorkers)))
			c.metrics.WorkerLoad.Reset()
			for workerID, load := range result.WorkerLoads {
				c.metrics.WorkerLoad.WithLabelValues(workerID).Set(load.Load())
			}

			if len(result.DeadWorkers) > 0 {
				unassigned, err := c.healthChecker.MarkDead(ctx, result.DeadWorkers, c.pgChannel)
				if err != nil {
					c.logger.Error("health-check-loop: mark dead failed", "error", err)
					c.metrics.HealthcheckErrorsTotal.WithLabelValues("mark_dead").Inc()
				} else {
					c.logger.Info("health-check-loop: marked dead workers",
						"deadWorkers", result.DeadWorkers,
						"unassigned", unassigned)
					c.metrics.DeadWorkersTotal.Add(float64(len(result.DeadWorkers)))
					c.metrics.ChannelsUnassignedTotal.WithLabelValues("worker_dead").Add(float64(unassigned))
				}
			}
		}
	}
}

// eventListenerLoop subscribes to coord:events and handles ws_closed events
// by checking PG channel status and sending reconnect or disconnect.
func (c *Coordinator) eventListenerLoop(ctx context.Context) {
	pubsub := c.redisStore.SubscribeCoordEvents(ctx)
	defer pubsub.Close()

	ch := pubsub.Channel()
	sem := make(chan struct{}, 32) // limit concurrent event handlers

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}

			var event store.CoordEvent
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				c.logger.Error("event-listener: unmarshal failed", "error", err)
				continue
			}

			select {
			case sem <- struct{}{}:
				go func() {
					defer func() { <-sem }()
					c.handleEvent(ctx, event)
				}()
			case <-ctx.Done():
				return
			}
		}
	}
}

// handleEvent processes a single coordinator event.
func (c *Coordinator) handleEvent(ctx context.Context, event store.CoordEvent) {
	switch event.Type {
	case store.CoordEventWSClosed:
		c.handleWSClosed(ctx, event)
	case store.CoordEventUnassign:
		c.handleUnassign(ctx, event)
	case store.CoordEventDrainStarted:
		c.logger.Info("received drain_started event", "worker", event.WorkerID)
		// Assignment loop will pick up draining worker channels on next cycle
	default:
		c.logger.Debug("event-listener: unhandled event type", "type", event.Type)
	}
}

const wsClosedDebounceTTL = 30 * time.Second

// shouldDebounce returns true if the same channel had a ws_closed event
// within the debounce window.
func (c *Coordinator) shouldDebounce(platform model.Platform, channelID string) bool {
	key := string(platform) + ":" + channelID
	now := time.Now()

	c.debounceMu.Lock()
	defer c.debounceMu.Unlock()

	if last, ok := c.debounceMap[key]; ok && now.Sub(last) < wsClosedDebounceTTL {
		return true
	}
	c.debounceMap[key] = now

	// Lazy eviction of stale entries to prevent unbounded growth.
	if len(c.debounceMap) > 500 {
		for k, t := range c.debounceMap {
			if now.Sub(t) > wsClosedDebounceTTL {
				delete(c.debounceMap, k)
			}
		}
	}

	return false
}

// handleWSClosed checks PG channel status and sends the appropriate
// worker command (reconnect or disconnect).
// Draining workers are skipped — their channels are handled by the drain protocol.
func (c *Coordinator) handleWSClosed(ctx context.Context, event store.CoordEvent) {
	platform := event.Platform
	channelID := event.ChannelID
	workerID := event.WorkerID

	if c.shouldDebounce(platform, channelID) {
		c.logger.Debug("event-listener: ws_closed debounced",
			"platform", platform, "channelID", channelID)
		return
	}

	// Skip reconnect for draining workers
	drainingWorkers, err := c.pgWorker.ListWorkersByStatus(ctx, model.WorkerStatusDraining)
	if err == nil {
		for _, w := range drainingWorkers {
			if w.ID == workerID {
				c.logger.Info("ignoring ws_closed from draining worker",
					"worker", workerID, "platform", platform, "channel", channelID)
				return
			}
		}
	}

	ch, err := c.pgChannel.GetChannel(ctx, platform, channelID)
	if err != nil {
		c.logger.Error("event-listener: GetChannel failed",
			"platform", platform, "channelID", channelID, "error", err)
		return
	}

	if ch.Status != model.ChannelStatusEnded {
		// Channel still live/pending, send reconnect (connect) command
		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandConnect,
			Platform:  platform,
			ChannelID: channelID,
		}
		if err := c.redisStore.PublishWorkerCommand(ctx, workerID, cmd); err != nil {
			c.logger.Error("event-listener: publish reconnect failed", "error", err)
		} else {
			c.logger.Info("event-listener: sent reconnect command",
				"platform", platform, "channelID", channelID, "workerID", workerID)
		}
	} else {
		// Channel ended, send disconnect command (discover service owns MarkChannelEnded)
		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandDisconnect,
			Platform:  platform,
			ChannelID: channelID,
		}
		if err := c.redisStore.PublishWorkerCommand(ctx, workerID, cmd); err != nil {
			c.logger.Error("event-listener: publish disconnect failed", "error", err)
		} else {
			c.logger.Info("event-listener: channel ended, sent disconnect",
				"platform", platform, "channelID", channelID)
		}

		// Delete the Redis stream only if the channel has been ended for > 30s
		// to avoid racing with rapid end/reopen cycles.
		if ch.EndedAt != nil && time.Since(*ch.EndedAt) > 30*time.Second {
			streamKey := store.ChatStreamKey(string(platform), channelID)
			if _, err := c.redisStore.DeleteChatStreams(ctx, []string{streamKey}); err != nil {
				c.logger.Warn("failed to delete ended channel stream", "key", streamKey, "error", err)
			} else {
				c.logger.Info("deleted ended channel stream", "key", streamKey)
			}
		}
	}
}

// streamCleanupLoop periodically deletes Redis streams for channels that ended long ago.
func (c *Coordinator) streamCleanupLoop(ctx context.Context) {
	// Run immediately on leader start, then every 10 minutes.
	c.runStreamCleanup(ctx)

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runStreamCleanup(ctx)
		}
	}
}

func (c *Coordinator) runStreamCleanup(ctx context.Context) {
	cleanupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Phase 1: Delete streams for channels ended > 1 hour ago (PG-driven)
	cutoff := time.Now().Add(-1 * time.Hour)
	channels, err := c.pgChannel.ListEndedChannelsBefore(cleanupCtx, cutoff, 5000)
	if err != nil {
		c.logger.Error("stream-cleanup: list ended channels failed", "error", err)
	} else if len(channels) > 0 {
		keys := make([]string, len(channels))
		for i, ch := range channels {
			keys[i] = store.ChatStreamKey(string(ch.Platform), ch.ChannelID)
		}
		deleted, err := c.redisStore.DeleteChatStreams(cleanupCtx, keys)
		if err != nil {
			c.logger.Error("stream-cleanup: delete ended streams failed", "error", err)
		} else if deleted > 0 {
			c.logger.Info("stream-cleanup: deleted ended channel streams",
				"candidates", len(keys), "deleted", deleted)
		}
	}

	// Phase 2: Scan Redis for truly orphaned streams (not in PG at all).
	// Streams for recently-ended channels are NOT deleted here — Phase 1 handles
	// those with the 1-hour grace period. Phase 2 only catches keys whose channel
	// no longer exists in PG (e.g. manually deleted rows, platform decommissions).
	redisKeys, err := c.redisStore.ScanChatStreamKeys(cleanupCtx)
	if err != nil {
		c.logger.Error("stream-cleanup: scan redis keys failed", "error", err)
		return
	}
	if len(redisKeys) == 0 {
		return
	}

	// Build lookup of ALL channels in PG (any status) to protect grace period.
	// Only keys completely absent from PG are truly orphaned.
	allKeys, err := c.pgChannel.ListAllKnownChannelKeys(cleanupCtx)
	if err != nil {
		c.logger.Error("stream-cleanup: list all known channel keys failed", "error", err)
		return
	}
	knownSet := make(map[string]struct{}, len(allKeys))
	for _, key := range allKeys {
		knownSet[key] = struct{}{}
	}

	// Find truly orphaned keys (in Redis but not in PG at all)
	var orphans []string
	for _, key := range redisKeys {
		if _, ok := knownSet[key]; !ok {
			orphans = append(orphans, key)
		}
	}

	if len(orphans) > 0 {
		deleted, err := c.redisStore.DeleteChatStreams(cleanupCtx, orphans)
		if err != nil {
			c.logger.Error("stream-cleanup: delete orphan streams failed", "error", err)
		} else {
			c.logger.Info("stream-cleanup: deleted orphan streams",
				"total_redis_keys", len(redisKeys), "orphans", len(orphans), "deleted", deleted)
		}
	}
}

// resumeDrainingHandoffs re-publishes connect commands for channels with pending
// handoffs from draining workers. This ensures handoffs are not lost during
// leader failover.
func (c *Coordinator) resumeDrainingHandoffs() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	drainingWorkers, err := c.pgWorker.ListWorkersByStatus(ctx, model.WorkerStatusDraining)
	if err != nil {
		c.logger.Error("failed to list draining workers on leader start", "error", err)
		return
	}

	for _, w := range drainingWorkers {
		pending, err := c.pgChannel.ListPendingHandoffs(ctx, w.ID)
		if err != nil {
			c.logger.Error("failed to list pending handoffs", "worker", w.ID, "error", err)
			continue
		}
		for _, ch := range pending {
			if ch.WorkerID == nil {
				continue
			}
			cmd := store.WorkerCommand{
				Type:      store.WorkerCommandConnect,
				Platform:  ch.Platform,
				ChannelID: ch.ChannelID,
			}
			if err := c.redisStore.PublishWorkerCommand(ctx, *ch.WorkerID, cmd); err != nil {
				c.logger.Warn("failed to resume pending handoff command",
					"worker", *ch.WorkerID, "channel", ch.ChannelID, "error", err)
			}
		}
		if len(pending) > 0 {
			c.logger.Info("resumed pending handoffs", "worker", w.ID, "count", len(pending))
		}
	}
}

// cleanupZombieChannels unassigns channels belonging to dead workers only.
// Draining workers are preserved — their channels are handled by the drain protocol.
func (c *Coordinator) cleanupZombieChannels() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.reconcileDeadWorkerChannels(ctx)
}

func (c *Coordinator) reconcileDeadWorkerChannels(ctx context.Context) {
	reclaimed, err := c.pgChannel.UnassignDeadWorkerChannels(ctx)
	if err != nil {
		c.logger.Error("dead-worker channel reconciliation failed", "error", err)
		c.metrics.HealthcheckErrorsTotal.WithLabelValues("dead_worker_reconcile").Inc()
		return
	}
	if reclaimed > 0 {
		c.logger.Warn("reclaimed channels from dead workers", "channels", reclaimed)
		c.metrics.ChannelsUnassignedTotal.WithLabelValues("dead_worker_reconcile").Add(float64(reclaimed))
	}
}
