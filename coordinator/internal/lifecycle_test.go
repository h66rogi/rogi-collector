package internal

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

func newLifecycleRedisStore(t *testing.T) (*miniredis.Miniredis, *store.RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := store.NewRedisClient(mr.Addr())
	t.Cleanup(func() { _ = client.Close() })
	return mr, store.NewRedisStore(client)
}

func lifecycleLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewCoordinatorAndLeadershipLifecycle(t *testing.T) {
	_, redisStore := newLifecycleRedisStore(t)
	channelStore := &testChannelStore{}
	workerStore := &testWorkerStore{}

	_, metrics := NewMetricsRegistry()
	coord := NewCoordinator(Config{
		InstanceID:    "coord-1",
		HealthTimeout: 30 * time.Second,
		AssignBatch:   10,
	}, channelStore, workerStore, redisStore, metrics, lifecycleLogger())

	if coord == nil || coord.assigner == nil || coord.healthChecker == nil || coord.leader == nil {
		t.Fatal("expected coordinator subcomponents to be initialized")
	}

	coord.onBecomeLeader()
	time.Sleep(10 * time.Millisecond)
	coord.onLoseLeadership()
}

func TestLeaderElectionRunStopsOnContextCancel(t *testing.T) {
	_, redisStore := newLifecycleRedisStore(t)
	le := NewLeaderElection("leader-1", "coordinator:leader", redisStore, lifecycleLogger(), nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		le.Run(ctx)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("leader election run did not stop after context cancellation")
	}
}

func TestCoordinatorEventListenerLoopHandlesWSClosed(t *testing.T) {
	_, redisStore := newLifecycleRedisStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channelStore := &testChannelStore{
		getChannelResp: &model.LiveChannel{
			ID:        77,
			Platform:  model.PlatformChzzk,
			ChannelID: "channel-77",
			Status:    model.ChannelStatusEnded,
		},
	}

	c := &Coordinator{
		pgChannel:        channelStore,
		pgWorker:         &testWorkerStore{},
		redisStore:       redisStore,
		logger:           lifecycleLogger(),
		debounceMap:      make(map[string]time.Time),
		unassignCooldown: make(map[string]time.Time),
	}

	pubsub := redisStore.SubscribeWorkerCommands(ctx, "worker-77")
	defer pubsub.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.eventListenerLoop(ctx)
	}()

	// Wait for the event listener's Redis subscription to be established.
	time.Sleep(200 * time.Millisecond)

	if err := redisStore.PublishCoordEvent(ctx, store.CoordEvent{
		Type:      store.CoordEventWSClosed,
		Platform:  model.PlatformChzzk,
		ChannelID: "channel-77",
		WorkerID:  "worker-77",
	}); err != nil {
		t.Fatalf("PublishCoordEvent failed: %v", err)
	}

	select {
	case msg := <-pubsub.Channel():
		if msg.Payload == "" {
			t.Fatal("expected worker command payload")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for worker command from event listener")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("event listener loop did not stop")
	}
}
