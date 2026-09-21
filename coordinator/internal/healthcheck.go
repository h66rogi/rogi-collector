package internal

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

// HealthChecker monitors worker health by cross-referencing PG alive
// workers with their Redis heartbeats.
type HealthChecker struct {
	pgStore    store.WorkerStore
	redisStore *store.RedisStore
	timeout    time.Duration // heartbeat timeout (default 30s)
	logger     *slog.Logger
}

// HealthCheckResult contains the outcome of a health check cycle.
type HealthCheckResult struct {
	AliveWorkers []string
	DeadWorkers  []string
	WorkerLoads  map[string]model.WorkerLoad
}

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(
	pgStore store.WorkerStore,
	redisStore *store.RedisStore,
	timeout time.Duration,
	logger *slog.Logger,
) *HealthChecker {
	return &HealthChecker{
		pgStore:    pgStore,
		redisStore: redisStore,
		timeout:    timeout,
		logger:     logger.With("component", "health-checker"),
	}
}

// Check lists alive workers from PG, checks their heartbeats in Redis via MGET,
// and returns alive/dead lists along with current load metrics.
func (hc *HealthChecker) Check(ctx context.Context) (*HealthCheckResult, error) {
	workers, err := hc.pgStore.ListAliveWorkers(ctx)
	if err != nil {
		return nil, err
	}

	workerIDs := make([]string, len(workers))
	for i, w := range workers {
		workerIDs[i] = w.ID
	}

	heartbeats, err := hc.redisStore.GetWorkerHeartbeats(ctx, workerIDs)
	if err != nil {
		return nil, err
	}

	result := &HealthCheckResult{
		WorkerLoads: make(map[string]model.WorkerLoad, len(heartbeats)),
	}

	for _, id := range workerIDs {
		load, ok := heartbeats[id]
		if !ok || load == nil {
			hc.logger.Warn("no heartbeat found for worker", "workerID", id)
			result.DeadWorkers = append(result.DeadWorkers, id)
			continue
		}
		result.AliveWorkers = append(result.AliveWorkers, id)
		result.WorkerLoads[id] = *load
	}

	return result, nil
}

// MarkDead marks dead workers in PG and unassigns their channels.
// Draining workers are skipped — they are handled by the drain protocol.
// Returns the total number of channels unassigned.
func (hc *HealthChecker) MarkDead(
	ctx context.Context,
	deadWorkers []string,
	channelStore store.ChannelStore,
) (int64, error) {
	drainingWorkers, _ := hc.pgStore.ListWorkersByStatus(ctx, model.WorkerStatusDraining)
	drainingSet := make(map[string]bool, len(drainingWorkers))
	for _, w := range drainingWorkers {
		drainingSet[w.ID] = true
	}

	var (
		totalUnassigned int64
		errs            []error
	)

	for _, workerID := range deadWorkers {
		if drainingSet[workerID] {
			hc.logger.Info("skipping mark-dead for draining worker", "worker", workerID)
			continue
		}

		// Mark worker as dead in PG
		if err := hc.pgStore.UpdateWorkerStatus(ctx, workerID, model.WorkerStatusDead); err != nil {
			hc.logger.Error("failed to mark worker dead", "workerID", workerID, "error", err)
			errs = append(errs, err)
			continue
		}

		// Unassign all channels from the dead worker
		if err := channelStore.UnassignWorkerChannels(ctx, workerID); err != nil {
			hc.logger.Error("failed to unassign channels", "workerID", workerID, "error", err)
			errs = append(errs, err)
			continue
		}

		hc.logger.Info("marked worker dead and unassigned channels", "workerID", workerID)
		totalUnassigned++
	}

	return totalUnassigned, errors.Join(errs...)
}
