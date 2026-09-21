package internal

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

// Assigner assigns pending channels to the least-loaded worker.
type Assigner struct {
	pgStore       store.ChannelStore
	redisStore    *store.RedisStore
	metrics       *CoordinatorMetrics
	batchSize     int
	loadThreshold float64
	logger        *slog.Logger
}

// NewAssigner creates a new Assigner.
func NewAssigner(
	pgStore store.ChannelStore,
	redisStore *store.RedisStore,
	metrics *CoordinatorMetrics,
	batchSize int,
	loadThreshold float64,
	logger *slog.Logger,
) *Assigner {
	return &Assigner{
		pgStore:       pgStore,
		redisStore:    redisStore,
		metrics:       metrics,
		batchSize:     batchSize,
		loadThreshold: loadThreshold,
		logger:        logger.With("component", "assigner"),
	}
}

// AssignPending lists pending channels and assigns each to the least-loaded
// worker. Returns the number of channels assigned.
func (a *Assigner) AssignPending(ctx context.Context, workerLoads map[string]model.WorkerLoad) (int, error) {
	if len(workerLoads) == 0 {
		return 0, nil
	}

	pending, err := a.pgStore.ListPendingChannels(ctx, a.batchSize)
	if err != nil {
		return 0, err
	}

	// Update pending channels gauge per platform.
	// Always set all known platforms to 0 first so the time series never
	// disappears from Prometheus (sum() of an empty metric returns no data,
	// which breaks the KEDA scaling query).
	if a.metrics != nil {
		for _, p := range []string{"chzzk", "soop", "cime"} {
			a.metrics.ChannelsPending.WithLabelValues(p).Set(0)
		}
		for _, ch := range pending {
			g, _ := a.metrics.ChannelsPending.GetMetricWithLabelValues(string(ch.Platform))
			g.Inc()
		}
	}

	assigned := 0
	for _, ch := range pending {
		workerID := selectWorker(workerLoads, a.loadThreshold)
		if workerID == "" {
			a.logger.Warn("no available worker for assignment, all overloaded")
			break
		}

		if err := a.pgStore.AssignChannel(ctx, ch.ID, workerID); err != nil {
			a.logger.Error("failed to assign channel", "channelID", ch.ChannelID, "workerID", workerID, "error", err)
			continue
		}

		// Send connect command to the worker
		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandConnect,
			Platform:  ch.Platform,
			ChannelID: ch.ChannelID,
		}
		if err := a.redisStore.PublishWorkerCommand(ctx, workerID, cmd); err != nil {
			a.logger.Error("failed to publish connect command", "channelID", ch.ChannelID, "workerID", workerID, "error", err)
			// Channel is already assigned in PG; the worker will pick it up on next heartbeat
		}

		a.logger.Info("assigned channel", "platform", ch.Platform, "channelID", ch.ChannelID, "workerID", workerID)
		a.metrics.ChannelsAssignedTotal.WithLabelValues(string(ch.Platform)).Inc()

		// Update the local copy of worker loads to reflect the new assignment
		if load, ok := workerLoads[workerID]; ok {
			load.Connections++
			workerLoads[workerID] = load
		}

		assigned++
	}

	return assigned, nil
}

// ReassignDrainingChannels reassigns channels from draining workers to healthy workers.
func (a *Assigner) ReassignDrainingChannels(ctx context.Context, workerLoads map[string]model.WorkerLoad) (int, error) {
	channels, err := a.pgStore.ListDrainingWorkerChannels(ctx)
	if err != nil {
		return 0, fmt.Errorf("list draining channels: %w", err)
	}
	if len(channels) == 0 {
		return 0, nil
	}

	assigned := 0
	for _, ch := range channels {
		if assigned >= a.batchSize {
			break
		}
		workerID := selectWorker(workerLoads, a.loadThreshold)
		if workerID == "" {
			a.logger.Warn("no available worker for drain reassignment")
			break
		}

		oldWorkerID := ""
		if ch.WorkerID != nil {
			oldWorkerID = *ch.WorkerID
		}

		updated, err := a.pgStore.ReassignChannel(ctx, ch.ID, workerID, oldWorkerID)
		if err != nil {
			a.logger.Error("failed to reassign draining channel",
				"channel", ch.ChannelID, "worker", workerID, "error", err)
			continue
		}
		if !updated {
			continue // channel already ended, skip connect command
		}

		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandConnect,
			Platform:  ch.Platform,
			ChannelID: ch.ChannelID,
		}
		if err := a.redisStore.PublishWorkerCommand(ctx, workerID, cmd); err != nil {
			a.logger.Error("failed to publish connect for drain",
				"channel", ch.ChannelID, "worker", workerID, "error", err)
		}

		// Update the local copy of worker loads to reflect the new assignment
		if load, ok := workerLoads[workerID]; ok {
			load.Connections++
			workerLoads[workerID] = load
		}

		assigned++
	}
	return assigned, nil
}

// RetryStuckHandoffs retries channels stuck in 'pending' handoff for longer than threshold.
func (a *Assigner) RetryStuckHandoffs(ctx context.Context, workerLoads map[string]model.WorkerLoad, threshold time.Duration) (int, error) {
	stuck, err := a.pgStore.ListStuckHandoffs(ctx, threshold)
	if err != nil {
		return 0, fmt.Errorf("list stuck handoffs: %w", err)
	}

	retried := 0
	for _, ch := range stuck {
		workerID := selectWorker(workerLoads, a.loadThreshold)
		if workerID == "" {
			break
		}

		oldWorkerID := ""
		if ch.HandoffFrom != nil {
			oldWorkerID = *ch.HandoffFrom
		}

		updated, err := a.pgStore.ReassignChannel(ctx, ch.ID, workerID, oldWorkerID)
		if err != nil {
			a.logger.Error("failed to retry stuck handoff",
				"channel", ch.ChannelID, "error", err)
			continue
		}
		if !updated {
			continue // channel already ended, skip connect command
		}

		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandConnect,
			Platform:  ch.Platform,
			ChannelID: ch.ChannelID,
		}
		if err := a.redisStore.PublishWorkerCommand(ctx, workerID, cmd); err != nil {
			a.logger.Warn("failed to publish retried handoff command",
				"worker", workerID, "channel", ch.ChannelID, "error", err)
		}

		// Update the local copy of worker loads to reflect the new assignment
		if load, ok := workerLoads[workerID]; ok {
			load.Connections++
			workerLoads[workerID] = load
		}

		retried++
	}
	return retried, nil
}

// selectWorker returns the worker ID with the lowest load that is below the
// given threshold. Returns empty string if all workers are overloaded or the
// map is empty.
func selectWorker(workerLoads map[string]model.WorkerLoad, threshold float64) string {
	var bestID string
	bestLoad := 1.0 // anything >= 1.0 means fully loaded

	for id, load := range workerLoads {
		l := load.Load()
		if l >= threshold {
			continue
		}
		if l < bestLoad {
			bestLoad = l
			bestID = id
		}
	}

	return bestID
}
