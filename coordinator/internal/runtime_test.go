package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Compile-time assertion that testChannelStore satisfies ChannelStore.
var _ store.ChannelStore = (*testChannelStore)(nil)

type testChannelStore struct {
	mu sync.Mutex

	pending            []model.LiveChannel
	assignments        []string
	unassignedWorkers  []string
	upsertedBatches    [][]model.LiveChannel
	markedEndedBatches []struct {
		platform model.Platform
		active   []string
	}
	getChannelResp *model.LiveChannel
	markEndedIDs   []int64
	deadReclaimed  int64
	deadReclaimErr error
}

func (s *testChannelStore) UpsertLiveChannels(_ context.Context, channels []model.LiveChannel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]model.LiveChannel(nil), channels...)
	s.upsertedBatches = append(s.upsertedBatches, cp)
	return nil
}

func (s *testChannelStore) MarkEndedChannels(_ context.Context, platform model.Platform, activeChannelIDs []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]string(nil), activeChannelIDs...)
	s.markedEndedBatches = append(s.markedEndedBatches, struct {
		platform model.Platform
		active   []string
	}{platform: platform, active: cp})
	return nil, nil
}

func (s *testChannelStore) ListPendingChannels(_ context.Context, _ int) ([]model.LiveChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.LiveChannel(nil), s.pending...), nil
}

func (s *testChannelStore) ListWorkerChannels(_ context.Context, _ string) ([]model.LiveChannel, error) {
	return nil, nil
}

func (s *testChannelStore) AssignChannel(_ context.Context, channelID int64, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assignments = append(s.assignments, fmt.Sprintf("%d:%s", channelID, workerID))
	return nil
}

func (s *testChannelStore) UnassignWorkerChannels(_ context.Context, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unassignedWorkers = append(s.unassignedWorkers, workerID)
	return nil
}

func (s *testChannelStore) MarkChannelEnded(_ context.Context, channelID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markEndedIDs = append(s.markEndedIDs, channelID)
	return nil
}

func (s *testChannelStore) UnassignChannel(_ context.Context, _ model.Platform, _ string, _ string) error {
	return nil
}

func (s *testChannelStore) UnassignDeadWorkerChannels(_ context.Context) (int64, error) {
	return s.deadReclaimed, s.deadReclaimErr
}

func (s *testChannelStore) GetChannel(_ context.Context, _ model.Platform, _ string) (*model.LiveChannel, error) {
	if s.getChannelResp == nil {
		return nil, errors.New("channel not found")
	}
	cp := *s.getChannelResp
	return &cp, nil
}

func (s *testChannelStore) ReassignChannel(_ context.Context, _ int64, _, _ string) (bool, error) {
	return true, nil
}

func (s *testChannelStore) ListDrainingWorkerChannels(_ context.Context) ([]model.LiveChannel, error) {
	return nil, nil
}

func (s *testChannelStore) ListStuckHandoffs(_ context.Context, _ time.Duration) ([]model.LiveChannel, error) {
	return nil, nil
}

func (s *testChannelStore) ListPendingHandoffs(_ context.Context, _ string) ([]model.LiveChannel, error) {
	return nil, nil
}

type testWorkerStore struct {
	mu            sync.Mutex
	aliveWorkers  []model.Worker
	updatedStatus map[string]string
}

func (s *testWorkerStore) RegisterWorker(context.Context, model.Worker) error { return nil }
func (s *testWorkerStore) DeleteWorker(context.Context, string) error         { return nil }

func (s *testWorkerStore) UpdateWorkerStatus(_ context.Context, workerID string, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.updatedStatus == nil {
		s.updatedStatus = make(map[string]string)
	}
	s.updatedStatus[workerID] = status
	return nil
}

func (s *testWorkerStore) ListAliveWorkers(context.Context) ([]model.Worker, error) {
	return append([]model.Worker(nil), s.aliveWorkers...), nil
}

func (s *testWorkerStore) ListAllWorkers(context.Context) ([]model.Worker, error) {
	return append([]model.Worker(nil), s.aliveWorkers...), nil
}

func (s *testWorkerStore) ListWorkersByStatus(_ context.Context, status string) ([]model.Worker, error) {
	var result []model.Worker
	for _, w := range s.aliveWorkers {
		if w.Status == status {
			result = append(result, w)
		}
	}
	return result, nil
}

func newTestRedisStore(t *testing.T) (*miniredis.Miniredis, *store.RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := store.NewRedisClient(mr.Addr())
	t.Cleanup(func() { _ = client.Close() })
	return mr, store.NewRedisStore(client)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *CoordinatorMetrics {
	_, m := NewMetricsRegistry()
	return m
}

func waitForMessage[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
		var zero T
		return zero
	}
}

func TestAssignerAssignPendingPublishesCommands(t *testing.T) {
	_, redisStore := newTestRedisStore(t)
	ctx := context.Background()

	channelStore := &testChannelStore{
		pending: []model.LiveChannel{
			{ID: 1, Platform: model.PlatformChzzk, ChannelID: "ch1"},
			{ID: 2, Platform: model.PlatformChzzk, ChannelID: "ch2"},
		},
	}
	assigner := NewAssigner(channelStore, redisStore, testMetrics(), 10, 0.95, testLogger())

	pubsub := redisStore.SubscribeWorkerCommands(ctx, "w1")
	defer pubsub.Close()
	cmdCh := pubsub.Channel()

	loads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 0, MaxConn: 10, MsgPerSec: 0, MaxMsgPerSec: 100},
	}

	assigned, err := assigner.AssignPending(ctx, loads)
	if err != nil {
		t.Fatalf("AssignPending failed: %v", err)
	}
	if assigned != 2 {
		t.Fatalf("expected 2 assignments, got %d", assigned)
	}
	if len(channelStore.assignments) != 2 {
		t.Fatalf("expected 2 persisted assignments, got %d", len(channelStore.assignments))
	}

	got1 := waitForMessage(t, cmdCh)
	got2 := waitForMessage(t, cmdCh)
	if got1.Payload == got2.Payload {
		t.Fatalf("expected distinct connect commands, got identical payloads %q", got1.Payload)
	}
	if loads["w1"].Connections != 2 {
		t.Fatalf("expected worker load to be incremented to 2, got %d", loads["w1"].Connections)
	}
}

func TestHealthCheckerCheckAndMarkDead(t *testing.T) {
	_, redisStore := newTestRedisStore(t)
	ctx := context.Background()

	workerStore := &testWorkerStore{
		aliveWorkers: []model.Worker{
			{ID: "w1", Status: model.WorkerStatusAlive},
			{ID: "w2", Status: model.WorkerStatusAlive},
		},
	}
	channelStore := &testChannelStore{}
	hc := NewHealthChecker(workerStore, redisStore, 30*time.Second, testLogger())

	if err := redisStore.SendHeartbeat(ctx, "w1", model.WorkerLoad{
		WorkerID:     "w1",
		Connections:  3,
		MsgPerSec:    10,
		MaxConn:      10,
		MaxMsgPerSec: 100,
	}); err != nil {
		t.Fatalf("SendHeartbeat failed: %v", err)
	}

	result, err := hc.Check(ctx)
	if err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	if len(result.AliveWorkers) != 1 || result.AliveWorkers[0] != "w1" {
		t.Fatalf("unexpected alive workers: %+v", result.AliveWorkers)
	}
	if len(result.DeadWorkers) != 1 || result.DeadWorkers[0] != "w2" {
		t.Fatalf("unexpected dead workers: %+v", result.DeadWorkers)
	}

	unassigned, err := hc.MarkDead(ctx, result.DeadWorkers, channelStore)
	if err != nil {
		t.Fatalf("MarkDead failed: %v", err)
	}
	if unassigned != 1 {
		t.Fatalf("expected 1 unassigned worker result, got %d", unassigned)
	}
	if workerStore.updatedStatus["w2"] != model.WorkerStatusDead {
		t.Fatalf("worker status not updated: %+v", workerStore.updatedStatus)
	}
	if len(channelStore.unassignedWorkers) != 1 || channelStore.unassignedWorkers[0] != "w2" {
		t.Fatalf("unexpected unassigned workers: %+v", channelStore.unassignedWorkers)
	}
}

func TestCoordinatorReconcileDeadWorkerChannels(t *testing.T) {
	channelStore := &testChannelStore{deadReclaimed: 7}
	c := &Coordinator{
		pgChannel: channelStore,
		metrics:   testMetrics(),
		logger:    testLogger(),
	}

	c.reconcileDeadWorkerChannels(context.Background())

	metric := c.metrics.ChannelsUnassignedTotal.WithLabelValues("dead_worker_reconcile")
	if got := testutil.ToFloat64(metric); got != 7 {
		t.Fatalf("expected 7 reclaimed channel metrics, got %v", got)
	}
}

func TestLeaderElectionAcquireRenewAndLose(t *testing.T) {
	_, redisStore := newTestRedisStore(t)

	acquired := 0
	lost := 0
	le := NewLeaderElection("leader-1", "coordinator:leader", redisStore, testLogger(), func() { acquired++ }, func() { lost++ })

	le.tryAcquire(context.Background())
	if !le.IsLeader() {
		t.Fatal("expected leader to be acquired")
	}
	if acquired != 1 {
		t.Fatalf("expected onAcquire once, got %d", acquired)
	}

	le.tryRenew(context.Background())
	if !le.IsLeader() {
		t.Fatal("expected renewal to keep leadership")
	}

	if ok, err := redisStore.TryAcquireLeader(context.Background(), "coordinator:leader", "other", time.Second); err != nil || ok {
		t.Fatalf("expected lock to remain held by leader-1, ok=%v err=%v", ok, err)
	}

	le2 := NewLeaderElection("leader-2", "coordinator:leader", redisStore, testLogger(), nil, nil)
	le2.tryAcquire(context.Background())
	if le2.IsLeader() {
		t.Fatal("expected second leader not to acquire lock while held")
	}

	le.loseLeadership()
	if le.IsLeader() {
		t.Fatal("expected leadership to be cleared")
	}
	if lost != 1 {
		t.Fatalf("expected onLose once, got %d", lost)
	}
}

func TestCoordinatorHandleWSClosedReconnect(t *testing.T) {
	_, redisStore := newTestRedisStore(t)
	ctx := context.Background()

	channelStore := &testChannelStore{
		getChannelResp: &model.LiveChannel{
			ID:        1,
			Platform:  model.PlatformChzzk,
			ChannelID: "channel-1",
			Status:    model.ChannelStatusLive,
		},
	}

	c := &Coordinator{
		pgChannel:   channelStore,
		pgWorker:    &testWorkerStore{},
		redisStore:  redisStore,
		logger:      testLogger(),
		debounceMap: make(map[string]time.Time),
	}

	pubsub := redisStore.SubscribeWorkerCommands(ctx, "worker-1")
	defer pubsub.Close()

	c.handleWSClosed(ctx, store.CoordEvent{
		Type:      store.CoordEventWSClosed,
		Platform:  model.PlatformChzzk,
		ChannelID: "channel-1",
		WorkerID:  "worker-1",
	})

	msg := waitForMessage(t, pubsub.Channel())
	if want := `"type":"connect"`; !strings.Contains(msg.Payload, want) {
		t.Fatalf("expected reconnect command payload to contain %s, got %s", want, msg.Payload)
	}
}

func TestCoordinatorHandleWSClosedDisconnect(t *testing.T) {
	_, redisStore := newTestRedisStore(t)
	ctx := context.Background()

	channelStore := &testChannelStore{
		getChannelResp: &model.LiveChannel{
			ID:        99,
			Platform:  model.PlatformSoop,
			ChannelID: "bj-1",
			Status:    model.ChannelStatusEnded,
		},
	}

	c := &Coordinator{
		pgChannel:   channelStore,
		pgWorker:    &testWorkerStore{},
		redisStore:  redisStore,
		logger:      testLogger(),
		debounceMap: make(map[string]time.Time),
	}

	pubsub := redisStore.SubscribeWorkerCommands(ctx, "worker-2")
	defer pubsub.Close()
	cmdCh := pubsub.Channel()

	// Allow subscription to register before publishing.
	time.Sleep(50 * time.Millisecond)

	c.handleWSClosed(ctx, store.CoordEvent{
		Type:      store.CoordEventWSClosed,
		Platform:  model.PlatformSoop,
		ChannelID: "bj-1",
		WorkerID:  "worker-2",
	})

	msg := waitForMessage(t, cmdCh)
	if want := `"type":"disconnect"`; !strings.Contains(msg.Payload, want) {
		t.Fatalf("expected disconnect command payload to contain %s, got %s", want, msg.Payload)
	}
	// Verify MarkChannelEnded is NOT called (discover service owns this now)
	if len(channelStore.markEndedIDs) != 0 {
		t.Fatalf("expected no MarkChannelEnded calls, got %+v", channelStore.markEndedIDs)
	}
}

func TestCoordinatorHandleWSClosedDebounce(t *testing.T) {
	_, redisStore := newTestRedisStore(t)
	ctx := context.Background()

	channelStore := &testChannelStore{
		getChannelResp: &model.LiveChannel{
			ID:        1,
			Platform:  model.PlatformChzzk,
			ChannelID: "channel-1",
			Status:    model.ChannelStatusLive,
		},
	}

	c := &Coordinator{
		pgChannel:   channelStore,
		pgWorker:    &testWorkerStore{},
		redisStore:  redisStore,
		logger:      testLogger(),
		debounceMap: make(map[string]time.Time),
	}

	pubsub := redisStore.SubscribeWorkerCommands(ctx, "worker-1")
	defer pubsub.Close()
	cmdCh := pubsub.Channel()

	event := store.CoordEvent{
		Type:      store.CoordEventWSClosed,
		Platform:  model.PlatformChzzk,
		ChannelID: "channel-1",
		WorkerID:  "worker-1",
	}

	// First call should go through
	c.handleWSClosed(ctx, event)

	msg := waitForMessage(t, cmdCh)
	if want := `"type":"connect"`; !strings.Contains(msg.Payload, want) {
		t.Fatalf("expected reconnect command, got %s", msg.Payload)
	}

	// Second call within 30s should be debounced (no command published)
	c.handleWSClosed(ctx, event)

	select {
	case msg := <-cmdCh:
		t.Fatalf("expected no command for debounced call, got %s", msg.Payload)
	case <-time.After(200 * time.Millisecond):
		// expected: no message
	}
}

func (s *testChannelStore) CountAssignedByWorker(_ context.Context) (map[string]int, error) {
	return nil, nil
}

func (s *testChannelStore) ListEndedChannelsBefore(_ context.Context, _ time.Time, _ int) ([]model.LiveChannel, error) {
	return nil, nil
}

func (s *testChannelStore) ListActiveChannelKeys(_ context.Context) ([]string, error) {
	return nil, nil
}

func (s *testChannelStore) ListAllKnownChannelKeys(_ context.Context) ([]string, error) {
	return nil, nil
}
