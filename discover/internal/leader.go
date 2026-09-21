package internal

import (
	"context"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/h66rogi/rogi-collector/shared/store"
)

// LeaderElection manages Redis-based leader election using SET NX leases.
// Only one discover instance holds the leader lock at a time.
type LeaderElection struct {
	instanceID string
	leaderKey  string
	redisStore *store.RedisStore
	config     LeaderElectionConfig
	isLeader   atomic.Bool
	logger     *slog.Logger
	onAcquire  func() // called when this instance becomes the leader
	onLose     func() // called when this instance loses the leadership

	leaseToken             int64
	leaseValidUntil        time.Time
	consecutiveRenewErrors int
	random                 *rand.Rand
}

// LeaderElectionConfig controls Redis lease timings.
type LeaderElectionConfig struct {
	TTL                       time.Duration
	RenewInterval             time.Duration
	AcquireInterval           time.Duration
	RenewJitter               time.Duration
	AcquireJitter             time.Duration
	RetryInterval             time.Duration
	OperationTimeout          time.Duration
	ExpiryGuard               time.Duration
	MaxConsecutiveRenewErrors int
}

const (
	defaultLeaderTTL                 = 15 * time.Second
	defaultLeaderRenewInterval       = 3 * time.Second
	defaultLeaderAcquireInterval     = 2 * time.Second
	defaultLeaderRenewJitter         = 750 * time.Millisecond
	defaultLeaderAcquireJitter       = 500 * time.Millisecond
	defaultLeaderRetryInterval       = 1 * time.Second
	defaultLeaderOperationTimeout    = 2 * time.Second
	defaultLeaderExpiryGuard         = 2 * time.Second
	defaultMaxConsecutiveRenewErrors = 3
	minLeaderLoopDelay               = 100 * time.Millisecond
)

// DefaultLeaderElectionConfig returns the discover leader-election defaults.
func DefaultLeaderElectionConfig() LeaderElectionConfig {
	return LeaderElectionConfig{
		TTL:                       defaultLeaderTTL,
		RenewInterval:             defaultLeaderRenewInterval,
		AcquireInterval:           defaultLeaderAcquireInterval,
		RenewJitter:               defaultLeaderRenewJitter,
		AcquireJitter:             defaultLeaderAcquireJitter,
		RetryInterval:             defaultLeaderRetryInterval,
		OperationTimeout:          defaultLeaderOperationTimeout,
		ExpiryGuard:               defaultLeaderExpiryGuard,
		MaxConsecutiveRenewErrors: defaultMaxConsecutiveRenewErrors,
	}
}

func (cfg LeaderElectionConfig) normalized() LeaderElectionConfig {
	defaults := DefaultLeaderElectionConfig()

	if cfg.TTL <= 0 {
		cfg.TTL = defaults.TTL
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = defaults.RenewInterval
	}
	if cfg.AcquireInterval <= 0 {
		cfg.AcquireInterval = defaults.AcquireInterval
	}
	if cfg.RenewJitter < 0 {
		cfg.RenewJitter = 0
	}
	if cfg.RenewJitter == 0 {
		cfg.RenewJitter = defaults.RenewJitter
	}
	if cfg.AcquireJitter < 0 {
		cfg.AcquireJitter = 0
	}
	if cfg.AcquireJitter == 0 {
		cfg.AcquireJitter = defaults.AcquireJitter
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = defaults.RetryInterval
	}
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = defaults.OperationTimeout
	}
	if cfg.ExpiryGuard <= 0 {
		cfg.ExpiryGuard = defaults.ExpiryGuard
	}
	if cfg.MaxConsecutiveRenewErrors <= 0 {
		cfg.MaxConsecutiveRenewErrors = defaults.MaxConsecutiveRenewErrors
	}

	if cfg.ExpiryGuard >= cfg.TTL {
		cfg.ExpiryGuard = cfg.TTL / 5
		if cfg.ExpiryGuard < time.Second {
			cfg.ExpiryGuard = time.Second
		}
	}

	return cfg
}

// NewLeaderElection creates a new LeaderElection.
func NewLeaderElection(
	instanceID string,
	leaderKey string,
	redisStore *store.RedisStore,
	config LeaderElectionConfig,
	logger *slog.Logger,
	onAcquire func(),
	onLose func(),
) *LeaderElection {
	config = config.normalized()
	return &LeaderElection{
		instanceID: instanceID,
		leaderKey:  leaderKey,
		redisStore: redisStore,
		config:     config,
		logger:     logger.With("component", "leader-election"),
		onAcquire:  onAcquire,
		onLose:     onLose,
		// #nosec G404 -- jitter does not generate credentials or security tokens.
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// IsLeader returns true if this instance currently holds the leader lock.
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader.Load()
}

// FencingToken returns the current fencing token, or 0 if not the leader.
// Callers should capture the token at the start of a destructive cycle and
// pass it to RequireCurrentLease before each write operation.
func (le *LeaderElection) FencingToken() int64 {
	if !le.isLeader.Load() {
		return 0
	}
	return le.leaseToken
}

// Run is the main loop for leader election. It blocks until ctx is cancelled.
// When in standby mode, it attempts to acquire the lock on a jittered interval.
// When leader, it renews before the lease nears local expiry.
func (le *LeaderElection) Run(ctx context.Context) {
	le.logger.Info("starting leader election loop",
		"instanceID", le.instanceID,
		"ttl", le.config.TTL,
		"renewInterval", le.config.RenewInterval,
		"acquireInterval", le.config.AcquireInterval,
		"operationTimeout", le.config.OperationTimeout,
		"maxConsecutiveRenewErrors", le.config.MaxConsecutiveRenewErrors)

	// Jitter on initial acquire to avoid all standby pods hitting Redis at the
	// same instant after a coordinated (re)start.
	initialDelay := le.withJitter(le.config.AcquireInterval, le.config.AcquireJitter)
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if le.IsLeader() {
				// Gracefully release the lease so the standby can acquire
				// immediately instead of waiting up to TTL.
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), le.config.OperationTimeout)
				if released, err := le.redisStore.ReleaseLeaderLease(releaseCtx, le.leaderKey, le.instanceID); err != nil {
					le.logger.Warn("failed to release leader lease on shutdown", "error", err)
				} else if released {
					le.logger.Info("released leader lease on shutdown")
				}
				releaseCancel()
				le.loseLeadership("context-cancelled")
			}
			le.logger.Info("leader election loop stopped")
			return
		case <-timer.C:
			var nextDelay time.Duration
			if le.IsLeader() {
				nextDelay = le.tryRenew(ctx)
			} else {
				nextDelay = le.tryAcquire(ctx)
			}
			timer.Reset(nextDelay)
		}
	}
}

// tryAcquire attempts to acquire the leader lock.
func (le *LeaderElection) tryAcquire(ctx context.Context) time.Duration {
	opCtx, cancel := le.redisOpContext(ctx)
	defer cancel()

	lease, err := le.redisStore.TryAcquireLeaderLease(opCtx, le.leaderKey, le.instanceID, le.config.TTL)
	if err != nil {
		le.logger.Error("failed to acquire leader lock", "error", err)
		return le.nextAcquireDelay()
	}
	if lease.Held {
		le.isLeader.Store(true)
		le.leaseToken = lease.Token
		le.leaseValidUntil = time.Now().Add(le.config.TTL)
		le.consecutiveRenewErrors = 0
		le.logger.Info("acquired leader lock",
			"instanceID", le.instanceID,
			"leaseToken", le.leaseToken,
			"leaseValidUntil", le.leaseValidUntil)
		if le.onAcquire != nil {
			le.onAcquire()
		}
		return le.nextRenewDelay(0, false)
	}
	return le.nextAcquireDelay()
}

// tryRenew attempts to renew the leader lease.
func (le *LeaderElection) tryRenew(ctx context.Context) time.Duration {
	startedAt := time.Now()
	opCtx, cancel := le.redisOpContext(ctx)
	defer cancel()

	lease, err := le.redisStore.RenewLeaderLease(opCtx, le.leaderKey, le.instanceID, le.config.TTL)
	if err != nil {
		le.consecutiveRenewErrors++
		remaining := time.Until(le.leaseValidUntil)
		if le.shouldKeepLeadership(remaining) {
			le.logger.Warn("leader renew failed; keeping leadership until local lease deadline",
				"error", err,
				"instanceID", le.instanceID,
				"leaseToken", le.leaseToken,
				"consecutiveRenewErrors", le.consecutiveRenewErrors,
				"remainingLease", remaining)
			return le.nextRenewDelay(time.Since(startedAt), true)
		}

		le.logger.Error("leader renew grace exhausted, conceding leadership",
			"error", err,
			"instanceID", le.instanceID,
			"leaseToken", le.leaseToken,
			"consecutiveRenewErrors", le.consecutiveRenewErrors,
			"remainingLease", remaining)
		le.loseLeadership("renew-error")
		return le.nextAcquireDelay()
	}
	if !lease.Held {
		le.logger.Warn("leader lock lost (renewal rejected)",
			"instanceID", le.instanceID,
			"leaseToken", le.leaseToken)
		le.loseLeadership("renew-rejected")
		return le.nextAcquireDelay()
	}

	le.leaseToken = lease.Token
	le.leaseValidUntil = startedAt.Add(le.config.TTL)
	le.consecutiveRenewErrors = 0

	return le.nextRenewDelay(time.Since(startedAt), false)
}

// loseLeadership marks this instance as no longer the leader and calls onLose.
func (le *LeaderElection) loseLeadership(reason string) {
	le.isLeader.Store(false)
	le.consecutiveRenewErrors = 0
	le.leaseValidUntil = time.Time{}
	leaseToken := le.leaseToken
	le.leaseToken = 0
	le.logger.Info("lost leader lock",
		"instanceID", le.instanceID,
		"leaseToken", leaseToken,
		"reason", reason)
	if le.onLose != nil {
		le.onLose()
	}
}

func (le *LeaderElection) shouldKeepLeadership(remainingLease time.Duration) bool {
	if remainingLease <= le.config.ExpiryGuard {
		return false
	}
	if le.consecutiveRenewErrors <= le.config.MaxConsecutiveRenewErrors {
		return true
	}
	return remainingLease > le.config.ExpiryGuard+le.config.RetryInterval
}

func (le *LeaderElection) redisOpContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, le.config.OperationTimeout)
}

func (le *LeaderElection) nextAcquireDelay() time.Duration {
	return le.withJitter(le.config.AcquireInterval, le.config.AcquireJitter)
}

func (le *LeaderElection) nextRenewDelay(lastRTT time.Duration, suspect bool) time.Duration {
	base := le.config.RenewInterval
	if suspect || lastRTT >= le.config.RenewInterval/2 {
		base = le.config.RetryInterval
	}
	return le.withJitter(base, le.config.RenewJitter)
}

func (le *LeaderElection) withJitter(base, jitter time.Duration) time.Duration {
	if base <= minLeaderLoopDelay {
		base = minLeaderLoopDelay
	}
	if jitter <= 0 {
		return base
	}

	delta := time.Duration(le.random.Int63n(int64(jitter)*2+1)) - jitter
	delay := base + delta
	if delay < minLeaderLoopDelay {
		return minLeaderLoopDelay
	}
	return delay
}
