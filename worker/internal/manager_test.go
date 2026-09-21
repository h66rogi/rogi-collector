package internal

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/worker/internal/connector"
)

func TestConnKey(t *testing.T) {
	tests := []struct {
		platform model.Platform
		channel  string
		expected string
	}{
		{model.PlatformChzzk, "abc123", "chzzk:abc123"},
		{model.PlatformSoop, "xyz", "soop:xyz"},
		{model.PlatformCime, "channel1", "cime:channel1"},
	}

	for _, tt := range tests {
		got := connKey(tt.platform, tt.channel)
		if got != tt.expected {
			t.Errorf("connKey(%s, %s) = %s, want %s", tt.platform, tt.channel, got, tt.expected)
		}
	}
}

func TestConnectionCountEmpty(t *testing.T) {
	m := &Manager{
		conns: make(map[string]*activeConn),
	}
	if m.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections, got %d", m.ConnectionCount())
	}
}

func TestGetLoad(t *testing.T) {
	m := &Manager{
		workerID:     "worker-1",
		maxConn:      50,
		maxMsgPerSec: 1000,
		conns:        make(map[string]*activeConn),
		rateCounter:  NewRateCounter(),
	}

	load := m.GetLoad()
	if load.WorkerID != "worker-1" {
		t.Errorf("expected workerId worker-1, got %s", load.WorkerID)
	}
	if load.Connections != 0 {
		t.Errorf("expected 0 connections, got %d", load.Connections)
	}
	if load.MaxConn != 50 {
		t.Errorf("expected maxConn 50, got %d", load.MaxConn)
	}
	if load.MaxMsgPerSec != 1000 {
		t.Errorf("expected maxMsgPerSec 1000, got %f", load.MaxMsgPerSec)
	}
}

func TestDisconnectAllEmpty(t *testing.T) {
	_, connCancel := context.WithCancel(context.Background())
	m := &Manager{
		conns:      make(map[string]*activeConn),
		connCancel: connCancel,
		stopSnap:   make(chan struct{}),
	}
	// Should not panic.
	m.DisconnectAll()

	if m.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections after DisconnectAll, got %d", m.ConnectionCount())
	}
}

func TestReconcileConnectionsDisconnectsStale(t *testing.T) {
	m := &Manager{
		conns: map[string]*activeConn{
			"chzzk:abc123": {
				connector: &stubConnector{},
				cancel:    func() {},
				channel: model.LiveChannel{
					Platform:  model.PlatformChzzk,
					ChannelID: "abc123",
				},
			},
		},
	}

	if err := m.ReconcilePlatform(context.Background(), model.PlatformChzzk, nil); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if m.ConnectionCount() != 0 {
		t.Fatalf("expected stale connection to be removed, got %d active", m.ConnectionCount())
	}
}

type stubConnector struct{}

func (s *stubConnector) Connect(context.Context, model.LiveChannel) error { return nil }
func (s *stubConnector) Disconnect() error                                { return nil }
func (s *stubConnector) IsAlive() bool                                    { return true }
func (s *stubConnector) Messages() <-chan model.ChatMessage               { return nil }
func (s *stubConnector) Errors() <-chan error                             { return nil }

// slowConnector simulates a connector that takes time to connect and tracks
// concurrent connection attempts.
type slowConnector struct {
	stubConnector
	delay      time.Duration
	concurrent atomic.Int32
	maxSeen    atomic.Int32
}

func (s *slowConnector) Connect(ctx context.Context, _ model.LiveChannel) error {
	cur := s.concurrent.Add(1)
	for {
		old := s.maxSeen.Load()
		if cur <= old || s.maxSeen.CompareAndSwap(old, cur) {
			break
		}
	}
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		s.concurrent.Add(-1)
		return ctx.Err()
	}
	s.concurrent.Add(-1)
	return nil
}

// failConnector counts Connect calls and always returns an error.
type failConnector struct {
	stubConnector
	calls atomic.Int32
}

func (f *failConnector) Connect(context.Context, model.LiveChannel) error {
	f.calls.Add(1)
	return fmt.Errorf("connect failed")
}

// countConnector counts successful connects with configurable delay.
type countConnector struct {
	stubConnector
	counter *atomic.Int32
	delay   time.Duration
}

func (c *countConnector) Connect(ctx context.Context, _ model.LiveChannel) error {
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	c.counter.Add(1)
	return nil
}

func TestReconcileConnectionsPerPlatformConcurrency(t *testing.T) {
	slow := &slowConnector{delay: 50 * time.Millisecond}

	origFactory := connectorFactory
	connectorFactory = func(model.Platform) (connector.PlatformConnector, error) {
		return slow, nil
	}
	defer func() { connectorFactory = origFactory }()

	m := NewManager(context.Background(), "worker-1", 500, 5000, nil, nil, nil, nil)

	assigned := make([]model.LiveChannel, PerPlatformConcurrency+20)
	for i := range assigned {
		assigned[i] = model.LiveChannel{
			Platform:  model.PlatformChzzk,
			ChannelID: fmt.Sprintf("ch-%d", i),
		}
	}

	m.ReconcilePlatform(context.Background(), assigned[0].Platform, assigned)

	if max := int(slow.maxSeen.Load()); max > PerPlatformConcurrency {
		t.Errorf("max concurrent connects = %d, want <= %d", max, PerPlatformConcurrency)
	}
}

func TestReconcilePlatformIsolation(t *testing.T) {
	var chzzkDone, soopDone atomic.Int32

	origFactory := connectorFactory
	connectorFactory = func(p model.Platform) (connector.PlatformConnector, error) {
		switch p {
		case model.PlatformChzzk:
			return &countConnector{counter: &chzzkDone, delay: 1 * time.Millisecond}, nil
		case model.PlatformSoop:
			return &countConnector{counter: &soopDone, delay: 200 * time.Millisecond}, nil
		default:
			return &stubConnector{}, nil
		}
	}
	defer func() { connectorFactory = origFactory }()

	m := NewManager(context.Background(), "worker-1", 2000, 5000, nil, nil, nil, nil)

	var chzzkChannels, soopChannels []model.LiveChannel
	for i := 0; i < 20; i++ {
		chzzkChannels = append(chzzkChannels, model.LiveChannel{Platform: model.PlatformChzzk, ChannelID: fmt.Sprintf("chzzk-%d", i)})
		soopChannels = append(soopChannels, model.LiveChannel{Platform: model.PlatformSoop, ChannelID: fmt.Sprintf("soop-%d", i)})
	}

	// Run both platforms concurrently — like the real reconcileAssignedChannels.
	go m.ReconcilePlatform(context.Background(), model.PlatformChzzk, chzzkChannels)
	go m.ReconcilePlatform(context.Background(), model.PlatformSoop, soopChannels)

	// Chzzk (1ms connect + 10/s rate limit with burst 15) finishes well before
	// SOOP (200ms connect + 5/s rate limit). Allow up to 3s for Chzzk.
	time.Sleep(3 * time.Second)
	if chzzkDone.Load() != 20 {
		t.Errorf("chzzk connected = %d after 3s, want 20 (not blocked by slow SOOP)", chzzkDone.Load())
	}

	// SOOP: 200ms connect + 5/s rate limit = ~4s for 20 channels. Wait 7s total.
	time.Sleep(7 * time.Second)
	if soopDone.Load() != 20 {
		t.Errorf("soop connected = %d, want 20", soopDone.Load())
	}
}

// newManagerNoRateLimiter creates a Manager without rate limiters, suitable for
// tests that verify batch sizing / concurrency logic without rate-limit delays.
func newManagerNoRateLimiter(workerID string, maxConn int) *Manager {
	connCtx, connCancel := context.WithCancel(context.Background())
	rc := NewRateCounter()
	stopSnap := make(chan struct{})
	go rc.StartSnapshotter(time.Second, stopSnap)
	return &Manager{
		workerID:     workerID,
		maxConn:      maxConn,
		maxMsgPerSec: 5000,
		rateCounter:  rc,
		rateLimiters: nil, // no rate limiters
		connCtx:      connCtx,
		connCancel:   connCancel,
		stopSnap:     stopSnap,
		conns:        make(map[string]*activeConn),
	}
}

func TestReconcileConnectionsBatchLimitNormalMode(t *testing.T) {
	fc := &failConnector{}

	origFactory := connectorFactory
	connectorFactory = func(model.Platform) (connector.PlatformConnector, error) {
		return fc, nil
	}
	defer func() { connectorFactory = origFactory }()

	m := newManagerNoRateLimiter("worker-1", 2000)
	defer close(m.stopSnap)

	// Normal mode: <= 100 missing channels, should cap at ReconcileMaxBatch.
	// Use exactly 100 channels so burst mode (>100) does NOT activate.
	assigned := make([]model.LiveChannel, 100)
	for i := range assigned {
		assigned[i] = model.LiveChannel{
			Platform:  model.PlatformChzzk,
			ChannelID: fmt.Sprintf("ch-%d", i),
		}
	}

	m.ReconcilePlatform(context.Background(), model.PlatformChzzk, assigned)
	time.Sleep(100 * time.Millisecond)

	if attempted := int(fc.calls.Load()); attempted > ReconcileMaxBatch {
		t.Errorf("connect attempts = %d, want <= %d (normal mode)", attempted, ReconcileMaxBatch)
	}
	// All 100 should have been attempted since 100 < ReconcileMaxBatch.
	if attempted := int(fc.calls.Load()); attempted != 100 {
		t.Errorf("connect attempts = %d, want 100 (all channels)", attempted)
	}
}

func TestReconcileConnectionsBatchLimitBurstMode(t *testing.T) {
	fc := &failConnector{}

	origFactory := connectorFactory
	connectorFactory = func(model.Platform) (connector.PlatformConnector, error) {
		return fc, nil
	}
	defer func() { connectorFactory = origFactory }()

	m := newManagerNoRateLimiter("worker-1", 5000)
	defer close(m.stopSnap)

	// Burst mode: >100 missing channels, should cap at BurstReconcileMaxBatch.
	assigned := make([]model.LiveChannel, BurstReconcileMaxBatch+200)
	for i := range assigned {
		assigned[i] = model.LiveChannel{
			Platform:  model.PlatformChzzk,
			ChannelID: fmt.Sprintf("ch-%d", i),
		}
	}

	m.ReconcilePlatform(context.Background(), model.PlatformChzzk, assigned)
	time.Sleep(100 * time.Millisecond)

	if attempted := int(fc.calls.Load()); attempted > BurstReconcileMaxBatch {
		t.Errorf("connect attempts = %d, want <= %d (burst mode)", attempted, BurstReconcileMaxBatch)
	}
}

func TestClassifyConnectorError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ws_closed"},
		{"read_timeout", fmt.Errorf("soop ws read: soop ws read timeout after 3m0s: i/o timeout"), "zombie_reaped"},
		{"ws_ping_failed", fmt.Errorf("soop ws ping failed: write tcp: broken pipe"), "liveness_ping_failed"},
		{"app_ping_write_failed", fmt.Errorf("soop ping write failed: broken pipe"), "liveness_ping_failed"},
		{"handshake", fmt.Errorf("soop handshake failed for x: login rejected: ret=-1"), "handshake_failed"},
		{"login_rejected", fmt.Errorf("soop login rejected: ret=5"), "handshake_failed"},
		{"generic_read", fmt.Errorf("soop ws read: connection reset by peer"), "ws_closed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classifyConnectorError(c.err)
			if got != c.want {
				t.Errorf("classifyConnectorError(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}
}

// errorConnector emits a single error then returns.
type errorConnector struct {
	stubConnector
	errs           chan error
	msgs           chan model.ChatMessage
	disconnectHits atomic.Int32
	err            error
}

func newErrorConnector(err error) *errorConnector {
	c := &errorConnector{
		errs: make(chan error, 1),
		msgs: make(chan model.ChatMessage),
		err:  err,
	}
	c.errs <- err
	return c
}

func (c *errorConnector) Disconnect() error {
	c.disconnectHits.Add(1)
	return nil
}
func (c *errorConnector) Messages() <-chan model.ChatMessage { return c.msgs }
func (c *errorConnector) Errors() <-chan error               { return c.errs }

// TestHandleErrorsTearsDownConnector verifies that on connector error,
// handleErrors calls cancel() + Disconnect() BEFORE removing the map entry,
// so that forwardMessages and connector goroutines exit deterministically.
// This regression-guards a zombie-leak bug where connector errors removed the
// map entry but left the connector running and forwardMessages blocked.
func TestHandleErrorsTearsDownConnector(t *testing.T) {
	ec := newErrorConnector(fmt.Errorf("soop ws read: soop ws read timeout after 3m0s: i/o timeout"))

	var cancelHits atomic.Int32
	cancel := func() { cancelHits.Add(1) }

	ac := &activeConn{
		connector: ec,
		cancel:    cancel,
		channel: model.LiveChannel{
			Platform:  model.PlatformSoop,
			ChannelID: "test-ch",
		},
		connectedAt: time.Now().Add(-5 * time.Minute),
	}

	m := &Manager{
		workerID:   "worker-test",
		conns:      map[string]*activeConn{"soop:test-ch": ac},
		redisStore: nil,
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ctxCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// redisStore is nil; PublishCoordEvent will panic. We skip the publish
		// path by swallowing the panic — the test focuses on teardown behavior.
		defer func() { _ = recover() }()
		m.handleErrors(ctx, "soop:test-ch", ac)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleErrors did not return within timeout")
	}

	if got := ec.disconnectHits.Load(); got != 1 {
		t.Errorf("Disconnect() called %d times, want 1", got)
	}
	if got := cancelHits.Load(); got != 1 {
		t.Errorf("cancel() called %d times, want 1", got)
	}
	if _, still := m.conns["soop:test-ch"]; still {
		t.Error("conn still present in m.conns after handleErrors")
	}
}
