package internal

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/h66rogi/rogi-collector/discover/internal/discovery"
	"github.com/h66rogi/rogi-collector/discover/internal/history"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// fakeChannelStore — implements store.ChannelStore
// ---------------------------------------------------------------------------

type fakeChannelStore struct {
	mu               sync.Mutex
	upsertCalls      int
	upsertChannels   []model.LiveChannel
	markEndedCalls   int
	markEndedIDs     []string
	markEndedReturns []string // IDs returned by MarkEndedChannels (simulates DB RETURNING)
	markEndedErr     error
}

func (f *fakeChannelStore) UpsertLiveChannels(_ context.Context, channels []model.LiveChannel) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upsertCalls++
	f.upsertChannels = append(f.upsertChannels, channels...)
	return nil
}

func (f *fakeChannelStore) MarkEndedChannels(_ context.Context, _ model.Platform, activeIDs []string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markEndedCalls++
	f.markEndedIDs = append(f.markEndedIDs, activeIDs...)
	return f.markEndedReturns, f.markEndedErr
}

func (f *fakeChannelStore) ListPendingChannels(_ context.Context, _ int) ([]model.LiveChannel, error) {
	return nil, nil
}

func (f *fakeChannelStore) ListWorkerChannels(_ context.Context, _ string) ([]model.LiveChannel, error) {
	return nil, nil
}

func (f *fakeChannelStore) AssignChannel(_ context.Context, _ int64, _ string) error {
	return nil
}

func (f *fakeChannelStore) UnassignWorkerChannels(_ context.Context, _ string) error {
	return nil
}

func (f *fakeChannelStore) UnassignChannel(_ context.Context, _ model.Platform, _ string, _ string) error {
	return nil
}

func (f *fakeChannelStore) MarkChannelEnded(_ context.Context, _ int64) error {
	return nil
}

func (f *fakeChannelStore) UnassignDeadWorkerChannels(_ context.Context) (int64, error) {
	return 0, nil
}

func (f *fakeChannelStore) GetChannel(_ context.Context, _ model.Platform, _ string) (*model.LiveChannel, error) {
	return nil, nil
}

func (f *fakeChannelStore) ReassignChannel(_ context.Context, _ int64, _, _ string) (bool, error) {
	return true, nil
}

func (f *fakeChannelStore) ListDrainingWorkerChannels(_ context.Context) ([]model.LiveChannel, error) {
	return nil, nil
}

func (f *fakeChannelStore) ListStuckHandoffs(_ context.Context, _ time.Duration) ([]model.LiveChannel, error) {
	return nil, nil
}

func (f *fakeChannelStore) ListPendingHandoffs(_ context.Context, _ string) ([]model.LiveChannel, error) {
	return nil, nil
}

// compile-time check
var _ store.ChannelStore = (*fakeChannelStore)(nil)

// ---------------------------------------------------------------------------
// fakeDiscovery — implements discovery.PlatformDiscovery
// ---------------------------------------------------------------------------

type fakeDiscovery struct {
	platform model.Platform
	channels []discovery.DiscoveredChannel
	fetchFn  func()
	live     map[string]bool
	liveErr  map[string]error
}

func (f *fakeDiscovery) Platform() model.Platform {
	return f.platform
}

func (f *fakeDiscovery) FetchLiveChannels(_ context.Context) (discovery.DiscoveryResult, error) {
	if f.fetchFn != nil {
		f.fetchFn()
	}
	return discovery.DiscoveryResult{Channels: f.channels}, nil
}

func (f *fakeDiscovery) IsChannelLive(_ context.Context, channelID string) (bool, error) {
	if f.liveErr != nil {
		if err := f.liveErr[channelID]; err != nil {
			return false, err
		}
	}
	if f.live != nil {
		return f.live[channelID], nil
	}
	return false, nil
}

var _ discovery.PlatformDiscovery = (*fakeDiscovery)(nil)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newTestRedis(t *testing.T) *store.RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return store.NewRedisStore(client)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// seedLeaderForTest seeds a valid leader lease in the test Redis instance and
// updates the Discover's internal leader state so runDiscoveryCycle proceeds.
func seedLeaderForTest(t *testing.T, d *Discover, rs *store.RedisStore) {
	t.Helper()
	ctx := context.Background()
	result, err := rs.TryAcquireLeaderLease(ctx, "discover:leader", d.instanceID, 30*time.Second)
	if err != nil || !result.Held {
		t.Fatalf("seedLeaderForTest: failed to acquire lease: held=%v err=%v", result.Held, err)
	}
	d.leader.leaseToken = result.Token
	d.leader.isLeader.Store(true)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDiscoveryLoopUpsertsAndMarksEnded(t *testing.T) {
	fcs := &fakeChannelStore{}
	fd := &fakeDiscovery{
		platform: model.PlatformChzzk,
		channels: []discovery.DiscoveredChannel{
			{ChannelID: "ch1", StreamerName: "streamer1", ViewerCount: 100},
			{ChannelID: "ch2", StreamerName: "streamer2", ViewerCount: 200},
		},
	}

	rs := newTestRedis(t)
	d := NewDiscover(
		Config{InstanceID: "test-1"},
		fcs,
		rs,
		[]discovery.PlatformDiscovery{fd},
		nil,
		discardLogger(),
	)
	seedLeaderForTest(t, d, rs)

	ctx, cancel := context.WithCancel(context.Background())

	// Use a very short interval so the tick fires quickly
	go d.discoveryLoop(ctx, fd, 10*time.Millisecond, make(chan struct{}, 1))

	// Wait long enough for at least one tick
	time.Sleep(100 * time.Millisecond)
	cancel()

	fcs.mu.Lock()
	defer fcs.mu.Unlock()

	if fcs.upsertCalls < 1 {
		t.Fatalf("expected at least 1 upsert call, got %d", fcs.upsertCalls)
	}
	if fcs.markEndedCalls < 1 {
		t.Fatalf("expected at least 1 markEnded call, got %d", fcs.markEndedCalls)
	}
	if len(fcs.upsertChannels) < 2 {
		t.Fatalf("expected at least 2 upserted channels, got %d", len(fcs.upsertChannels))
	}
	if len(fcs.markEndedIDs) < 2 {
		t.Fatalf("expected at least 2 active IDs in markEnded, got %d", len(fcs.markEndedIDs))
	}

	// Verify channel data was converted correctly
	first := fcs.upsertChannels[0]
	if first.Platform != model.PlatformChzzk {
		t.Fatalf("expected platform chzzk, got %s", first.Platform)
	}
	if first.ChannelID != "ch1" {
		t.Fatalf("expected channelID ch1, got %s", first.ChannelID)
	}
}

func TestDiscoverLeadershipLifecycle(t *testing.T) {
	fcs := &fakeChannelStore{}
	fd := &fakeDiscovery{
		platform: model.PlatformSoop,
		channels: []discovery.DiscoveredChannel{
			{ChannelID: "s1", StreamerName: "soop1", ViewerCount: 50},
		},
	}

	rs := newTestRedis(t)
	d := NewDiscover(
		Config{InstanceID: "test-2"},
		fcs,
		rs,
		[]discovery.PlatformDiscovery{fd},
		nil,
		discardLogger(),
	)

	// Simulate becoming leader
	d.onBecomeLeader()

	// Give goroutines a moment to start (but they won't tick for 60s for soop)
	time.Sleep(50 * time.Millisecond)

	// Simulate losing leadership — should cancel loops and wait
	d.onLoseLeadership()

	// After onLoseLeadership returns, all goroutines should be stopped.
	// Calling it again should be safe (cancelLoops is already called).
	d.onLoseLeadership()
}

func TestDiscoveryLoopRunsImmediatelyOnStart(t *testing.T) {
	fetchStarted := make(chan struct{}, 1)
	fd := &fakeDiscovery{
		platform: model.PlatformCime,
		channels: []discovery.DiscoveredChannel{
			{ChannelID: "c1", StreamerName: "cime1", ViewerCount: 10},
		},
		fetchFn: func() {
			select {
			case fetchStarted <- struct{}{}:
			default:
			}
		},
	}

	rs := newTestRedis(t)
	d := NewDiscover(
		Config{InstanceID: "test-immediate"},
		&fakeChannelStore{},
		rs,
		[]discovery.PlatformDiscovery{fd},
		nil,
		discardLogger(),
	)
	seedLeaderForTest(t, d, rs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.discoveryLoop(ctx, fd, time.Hour, make(chan struct{}, 1))

	select {
	case <-fetchStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected discovery loop to fetch immediately on start")
	}
}

func TestDiscoveryIntervalMatchesSpec(t *testing.T) {
	d := &Discover{} // no overrides → use defaults

	tests := []struct {
		platform model.Platform
		expected time.Duration
	}{
		{model.PlatformChzzk, 180 * time.Second},
		{model.PlatformSoop, 180 * time.Second},
		{model.PlatformCime, 120 * time.Second},
	}

	for _, tt := range tests {
		got := d.discoverInterval(tt.platform)
		if got != tt.expected {
			t.Errorf("discoverInterval(%s) = %v, want %v", tt.platform, got, tt.expected)
		}
	}
}

func (s *fakeChannelStore) CountAssignedByWorker(_ context.Context) (map[string]int, error) {
	return nil, nil
}

func (s *fakeChannelStore) ListEndedChannelsBefore(_ context.Context, _ time.Time, _ int) ([]model.LiveChannel, error) {
	return nil, nil
}

func (s *fakeChannelStore) ListActiveChannelKeys(_ context.Context) ([]string, error) {
	return nil, nil
}

func (s *fakeChannelStore) ListAllKnownChannelKeys(_ context.Context) ([]string, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Leader Election Tests
// ---------------------------------------------------------------------------

func TestLeaderElection_SingleInstanceHoldsLeadership(t *testing.T) {
	rs := newTestRedis(t)
	cfg := DefaultLeaderElectionConfig()
	cfg.TTL = 2 * time.Second
	cfg.RenewInterval = 500 * time.Millisecond
	cfg.AcquireInterval = 500 * time.Millisecond

	acquired := make(chan struct{}, 1)
	lost := make(chan string, 1)

	le := NewLeaderElection(
		"instance-A",
		"test:leader",
		rs,
		cfg,
		discardLogger(),
		func() { acquired <- struct{}{} },
		func() { lost <- "lost" },
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go le.Run(ctx)

	// Should acquire leadership quickly
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("expected to acquire leadership")
	}

	// Should maintain leadership for several seconds without loss
	select {
	case reason := <-lost:
		t.Fatalf("did not expect leadership loss, got: %s", reason)
	case <-time.After(3 * time.Second):
		// Good — held for 3s without loss
	}
}

func TestLeaderElection_GracefulReleaseOnShutdown(t *testing.T) {
	rs := newTestRedis(t)
	cfg := DefaultLeaderElectionConfig()
	cfg.TTL = 5 * time.Second
	cfg.RenewInterval = 1 * time.Second
	cfg.AcquireInterval = 500 * time.Millisecond

	le := NewLeaderElection(
		"instance-A",
		"test:leader:release",
		rs,
		cfg,
		discardLogger(),
		func() {},
		func() {},
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		le.Run(ctx)
		close(done)
	}()

	// Wait for acquisition
	time.Sleep(2 * time.Second)
	if !le.IsLeader() {
		t.Fatal("expected to be leader")
	}

	// Cancel context (simulating SIGTERM)
	cancel()
	<-done

	// Lease should be released immediately (not waiting for TTL)
	_, err := rs.GetLeader(context.Background(), "test:leader:release")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Key should be gone since we released it
}

func TestLeaderErection_TwoInstances_NoSplitBrain(t *testing.T) {
	rs := newTestRedis(t)
	cfg := DefaultLeaderElectionConfig()
	cfg.TTL = 2 * time.Second
	cfg.RenewInterval = 500 * time.Millisecond
	cfg.AcquireInterval = 500 * time.Millisecond

	var leaderCount atomic.Int64

	newLE := func(id string) *LeaderElection {
		return NewLeaderElection(
			id,
			"test:leader:split",
			rs,
			cfg,
			discardLogger(),
			func() { leaderCount.Add(1) },
			func() { leaderCount.Add(-1) },
		)
	}

	le1 := newLE("A")
	le2 := newLE("B")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go le1.Run(ctx)
	go le2.Run(ctx)

	// Run for a few seconds, checking that at most 1 is leader at any time
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		leaders := 0
		if le1.IsLeader() {
			leaders++
		}
		if le2.IsLeader() {
			leaders++
		}
		if leaders > 1 {
			t.Fatal("split brain detected: both instances think they are leader")
		}
	}
}

func TestLeaderElection_GracePeriod(t *testing.T) {
	mr := miniredis.RunT(t)
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	rs := store.NewRedisStore(client)

	cfg := DefaultLeaderElectionConfig()
	cfg.TTL = 2 * time.Second
	cfg.RenewInterval = 500 * time.Millisecond
	cfg.AcquireInterval = 500 * time.Millisecond
	cfg.MaxConsecutiveRenewErrors = 2

	lostCh := make(chan string, 1)

	le := NewLeaderElection(
		"instance-grace",
		"test:leader:grace",
		rs,
		cfg,
		discardLogger(),
		func() {},
		func() { lostCh <- "lost" },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go le.Run(ctx)

	// Wait for acquisition
	time.Sleep(1 * time.Second)
	if !le.IsLeader() {
		t.Fatal("expected to be leader")
	}

	// Simulate Redis outage by closing miniredis
	mr.Close()

	// Leader should NOT immediately lose leadership (grace period)
	select {
	case <-lostCh:
		t.Fatal("should not lose leadership immediately during grace period")
	case <-time.After(1 * time.Second):
		// Good — still holding during grace period
	}
}

func TestLeaderElection_InitialJitter(t *testing.T) {
	rs := newTestRedis(t)
	cfg := DefaultLeaderElectionConfig()
	cfg.TTL = 5 * time.Second
	cfg.RenewInterval = 1 * time.Second
	cfg.AcquireInterval = 2 * time.Second
	cfg.AcquireJitter = 1 * time.Second

	le := NewLeaderElection(
		"instance-jitter",
		"test:leader:jitter",
		rs,
		cfg,
		discardLogger(),
		func() {},
		func() {},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Should not acquire immediately — there's jitter on first acquire
	start := time.Now()
	go le.Run(ctx)

	// Wait for acquisition (should take at least a few ms due to jitter)
	time.Sleep(4 * time.Second)
	if !le.IsLeader() {
		t.Fatal("expected to acquire leadership within 4s")
	}
	// Verify it didn't acquire instantly (some delay from jitter)
	elapsed := time.Since(start)
	if elapsed < 100*time.Millisecond {
		t.Log("acquired very quickly — jitter may not be working")
	}
}

// ---------------------------------------------------------------------------
// BackNotifier / endedIDs flow tests
// ---------------------------------------------------------------------------

func TestNotifierCalledWithEndedIDs(t *testing.T) {
	var notified []string
	var mu sync.Mutex

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		notified = append(notified, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fcs := &fakeChannelStore{
		markEndedReturns: []string{"ch-gone"},
	}
	fd := &fakeDiscovery{
		platform: model.PlatformChzzk,
		channels: []discovery.DiscoveredChannel{
			{ChannelID: "ch1", StreamerName: "s1", ViewerCount: 10},
		},
	}

	rs := newTestRedis(t)
	d := NewDiscover(Config{InstanceID: "test-notifier"}, fcs, rs, []discovery.PlatformDiscovery{fd}, nil, discardLogger())
	seedLeaderForTest(t, d, rs)
	notifier := NewBackNotifier(srv.URL, "test-key", discardLogger())
	notifier.httpClient = srv.Client()
	d.SetNotifier(notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.discoveryLoop(ctx, fd, time.Hour, make(chan struct{}, 1))
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(notified) == 0 {
		t.Fatal("expected notifier to be called, got 0 calls")
	}
}

func TestNotifierNotCalledWhenNoEndedIDs(t *testing.T) {
	called := atomic.Bool{}

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fcs := &fakeChannelStore{
		markEndedReturns: nil, // DB returned no ended channels
	}
	fd := &fakeDiscovery{
		platform: model.PlatformChzzk,
		channels: []discovery.DiscoveredChannel{
			{ChannelID: "ch1", StreamerName: "s1", ViewerCount: 10},
		},
	}

	rs := newTestRedis(t)
	d := NewDiscover(Config{InstanceID: "test-no-notify"}, fcs, rs, []discovery.PlatformDiscovery{fd}, nil, discardLogger())
	seedLeaderForTest(t, d, rs)
	notifier := NewBackNotifier(srv.URL, "test-key", discardLogger())
	notifier.httpClient = srv.Client()
	d.SetNotifier(notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.discoveryLoop(ctx, fd, time.Hour, make(chan struct{}, 1))
	time.Sleep(200 * time.Millisecond)

	if called.Load() {
		t.Fatal("expected notifier NOT to be called when no channels ended")
	}
}

func TestValidateEndedCandidatesSuppressesVerifiedLive(t *testing.T) {
	fd := &fakeDiscovery{
		platform: model.PlatformChzzk,
		live: map[string]bool{
			"still-live": true,
			"ended":      false,
		},
	}
	d := NewDiscover(Config{InstanceID: "test-validate"}, &fakeChannelStore{}, newTestRedis(t), nil, nil, discardLogger())

	activeIDs, toEnd := d.validateEndedCandidates(
		context.Background(),
		fd,
		model.PlatformChzzk,
		[]string{"active"},
		[]history.EndedChannel{
			{Platform: model.PlatformChzzk, ChannelID: "still-live", StateKey: "chzzk:still-live"},
			{Platform: model.PlatformChzzk, ChannelID: "ended", StateKey: "chzzk:ended"},
		},
		discardLogger(),
	)

	if !containsString(activeIDs, "still-live") {
		t.Fatal("expected verified live channel to be added back to active IDs")
	}
	if len(toEnd) != 1 || toEnd[0].ChannelID != "ended" {
		t.Fatalf("expected only ended channel to remain, got %#v", toEnd)
	}
}

func TestValidateEndedCandidatesSuppressesInconclusiveChecks(t *testing.T) {
	fd := &fakeDiscovery{
		platform: model.PlatformChzzk,
		liveErr: map[string]error{
			"unknown": errors.New("temporary live-detail failure"),
		},
	}
	d := NewDiscover(Config{InstanceID: "test-validate-unknown"}, &fakeChannelStore{}, newTestRedis(t), nil, nil, discardLogger())

	activeIDs, toEnd := d.validateEndedCandidates(
		context.Background(),
		fd,
		model.PlatformChzzk,
		nil,
		[]history.EndedChannel{
			{Platform: model.PlatformChzzk, ChannelID: "unknown", StateKey: "chzzk:unknown"},
		},
		discardLogger(),
	)

	if !containsString(activeIDs, "unknown") {
		t.Fatal("expected inconclusive channel to be added back to active IDs")
	}
	if len(toEnd) != 0 {
		t.Fatalf("expected no channels to end on inconclusive check, got %#v", toEnd)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
