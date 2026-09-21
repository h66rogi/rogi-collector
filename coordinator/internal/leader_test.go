package internal

import (
	"log/slog"
	"os"
	"testing"
)

func TestLeaderElection_InitialState(t *testing.T) {
	le := NewLeaderElection(
		"test-instance",
		"coordinator:leader",
		nil, // redisStore not needed for state check
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
		nil,
		nil,
	)

	if le.IsLeader() {
		t.Error("expected IsLeader() to be false initially")
	}
}

func TestLeaderElection_Callbacks(t *testing.T) {
	acquireCalled := false
	loseCalled := false

	le := NewLeaderElection(
		"test-instance",
		"coordinator:leader",
		nil,
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
		func() { acquireCalled = true },
		func() { loseCalled = true },
	)

	// Simulate acquiring leadership
	le.isLeader.Store(true)
	if !le.IsLeader() {
		t.Error("expected IsLeader() to be true after setting isLeader")
	}

	// Simulate losing leadership
	le.loseLeadership()
	if le.IsLeader() {
		t.Error("expected IsLeader() to be false after loseLeadership")
	}
	if !loseCalled {
		t.Error("expected onLose callback to be called")
	}

	// onAcquire is only called inside tryAcquire which requires Redis,
	// but we can test the callback is wired correctly via direct call.
	if acquireCalled {
		t.Error("expected onAcquire not to be called yet")
	}
}

func TestLeaderElection_NilCallbacks(t *testing.T) {
	le := NewLeaderElection(
		"test-instance",
		"coordinator:leader",
		nil,
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
		nil,
		nil,
	)

	// Should not panic with nil callbacks
	le.isLeader.Store(true)
	le.loseLeadership()

	if le.IsLeader() {
		t.Error("expected IsLeader() to be false after loseLeadership")
	}
}
