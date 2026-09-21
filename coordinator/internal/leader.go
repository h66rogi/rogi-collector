package internal

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/h66rogi/rogi-collector/shared/store"
)

// LeaderElection manages Redis-based leader election using SET NX leases.
// Only one coordinator instance holds the leader lock at a time.
type LeaderElection struct {
	instanceID string
	leaderKey  string
	redisStore *store.RedisStore
	ttl        time.Duration // how long the lease lives (default 5s)
	isLeader   atomic.Bool
	logger     *slog.Logger
	onAcquire  func() // called when this instance becomes the leader
	onLose     func() // called when this instance loses the leadership
}

// NewLeaderElection creates a new LeaderElection.
func NewLeaderElection(
	instanceID string,
	leaderKey string,
	redisStore *store.RedisStore,
	logger *slog.Logger,
	onAcquire func(),
	onLose func(),
) *LeaderElection {
	return &LeaderElection{
		instanceID: instanceID,
		leaderKey:  leaderKey,
		redisStore: redisStore,
		ttl:        5 * time.Second,
		logger:     logger.With("component", "leader-election"),
		onAcquire:  onAcquire,
		onLose:     onLose,
	}
}

// IsLeader returns true if this instance currently holds the leader lock.
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader.Load()
}

// Run is the main loop for leader election. It blocks until ctx is cancelled.
// When in standby mode, it attempts to acquire the lock every 1s.
// When leader, it renews the lease every 1s.
func (le *LeaderElection) Run(ctx context.Context) {
	le.logger.Info("starting leader election loop", "instanceID", le.instanceID)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if le.IsLeader() {
				le.loseLeadership()
			}
			le.logger.Info("leader election loop stopped")
			return
		case <-ticker.C:
			if le.IsLeader() {
				le.tryRenew(ctx)
			} else {
				le.tryAcquire(ctx)
			}
		}
	}
}

// tryAcquire attempts to acquire the leader lock.
func (le *LeaderElection) tryAcquire(ctx context.Context) {
	ok, err := le.redisStore.TryAcquireLeader(ctx, le.leaderKey, le.instanceID, le.ttl)
	if err != nil {
		le.logger.Error("failed to acquire leader lock", "error", err)
		return
	}
	if ok {
		le.isLeader.Store(true)
		le.logger.Info("acquired leader lock", "instanceID", le.instanceID)
		if le.onAcquire != nil {
			le.onAcquire()
		}
	}
}

// tryRenew attempts to renew the leader lease.
func (le *LeaderElection) tryRenew(ctx context.Context) {
	ok, err := le.redisStore.RenewLeader(ctx, le.leaderKey, le.instanceID, le.ttl)
	if err != nil {
		le.logger.Error("failed to renew leader lock", "error", err)
		le.loseLeadership()
		return
	}
	if !ok {
		le.logger.Warn("leader lock lost (renewal failed)", "instanceID", le.instanceID)
		le.loseLeadership()
	}
}

// loseLeadership marks this instance as no longer the leader and calls onLose.
func (le *LeaderElection) loseLeadership() {
	le.isLeader.Store(false)
	le.logger.Info("lost leader lock", "instanceID", le.instanceID)
	if le.onLose != nil {
		le.onLose()
	}
}
