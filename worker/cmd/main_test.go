package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	workerinternal "github.com/h66rogi/rogi-collector/worker/internal"
)

type cmdChannelStore struct {
	channel *model.LiveChannel
}

func (s *cmdChannelStore) UpsertLiveChannels(context.Context, []model.LiveChannel) error { return nil }
func (s *cmdChannelStore) MarkEndedChannels(context.Context, model.Platform, []string) ([]string, error) {
	return nil, nil
}
func (s *cmdChannelStore) ListPendingChannels(context.Context, int) ([]model.LiveChannel, error) {
	return nil, nil
}
func (s *cmdChannelStore) ListWorkerChannels(context.Context, string) ([]model.LiveChannel, error) {
	return nil, nil
}
func (s *cmdChannelStore) AssignChannel(context.Context, int64, string) error   { return nil }
func (s *cmdChannelStore) UnassignWorkerChannels(context.Context, string) error { return nil }
func (s *cmdChannelStore) UnassignChannel(context.Context, model.Platform, string, string) error {
	return nil
}
func (s *cmdChannelStore) UnassignDeadWorkerChannels(context.Context) (int64, error) {
	return 0, nil
}
func (s *cmdChannelStore) MarkChannelEnded(context.Context, int64) error { return nil }
func (s *cmdChannelStore) GetChannel(context.Context, model.Platform, string) (*model.LiveChannel, error) {
	cp := *s.channel
	return &cp, nil
}
func (s *cmdChannelStore) ReassignChannel(context.Context, int64, string, string) (bool, error) {
	return true, nil
}
func (s *cmdChannelStore) ListDrainingWorkerChannels(context.Context) ([]model.LiveChannel, error) {
	return nil, nil
}
func (s *cmdChannelStore) ListStuckHandoffs(context.Context, time.Duration) ([]model.LiveChannel, error) {
	return nil, nil
}
func (s *cmdChannelStore) ListPendingHandoffs(context.Context, string) ([]model.LiveChannel, error) {
	return nil, nil
}

func TestEnvHelpers(t *testing.T) {
	t.Setenv("TEST_STR", "value")
	t.Setenv("TEST_INT", "42")
	t.Setenv("TEST_FLOAT", "3.14")

	if got := envOrDefault("TEST_STR", "fallback"); got != "value" {
		t.Fatalf("envOrDefault = %q", got)
	}
	if got := envOrDefault("MISSING_STR", "fallback"); got != "fallback" {
		t.Fatalf("missing envOrDefault = %q", got)
	}
	if got := envIntOrDefault("TEST_INT", 7); got != 42 {
		t.Fatalf("envIntOrDefault = %d", got)
	}
	if got := envIntOrDefault("MISSING_INT", 7); got != 7 {
		t.Fatalf("missing envIntOrDefault = %d", got)
	}
	if got := envFloatOrDefault("TEST_FLOAT", 1.5); got != 3.14 {
		t.Fatalf("envFloatOrDefault = %f", got)
	}
	if got := envFloatOrDefault("MISSING_FLOAT", 1.5); got != 1.5 {
		t.Fatalf("missing envFloatOrDefault = %f", got)
	}
}

func TestValidateSpoolPaths(t *testing.T) {
	tests := []struct {
		name, donation, chat string
		valid                bool
	}{
		{"sibling directories", "/srv/spool/donations", "/srv/spool/chat", true},
		{"same directory", "/srv/spool", "/srv/spool", false},
		{"chat inside donation spool", "/srv/spool", "/srv/spool/chat", false},
		{"donations inside chat spool", "/srv/spool/chat/donations", "/srv/spool/chat", false},
		{"relative path", "spool/donations", "/srv/spool/chat", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSpoolPaths(test.donation, test.chat)
			if (err == nil) != test.valid {
				t.Fatalf("validateSpoolPaths(%q, %q): %v", test.donation, test.chat, err)
			}
		})
	}
}

func TestConnectAssignedChannelSkipsUnassignedChannel(t *testing.T) {
	otherWorker := "worker-2"
	store := &cmdChannelStore{
		channel: &model.LiveChannel{
			Platform:  model.PlatformChzzk,
			ChannelID: "channel-1",
			WorkerID:  &otherWorker,
		},
	}
	mgr := workerinternal.NewManager(context.Background(), "worker-1", 10, 1000, nil, nil, nil, nil)

	if err := connectAssignedChannel(context.Background(), store, mgr, "worker-1", model.PlatformChzzk, "channel-1"); err != nil {
		t.Fatalf("connectAssignedChannel should skip unassigned channel without error: %v", err)
	}
}

func TestCommandDispatcherConcurrentConnects(t *testing.T) {
	var concurrent atomic.Int32
	var maxSeen atomic.Int32

	slowHandler := func(ctx context.Context, cs store.ChannelStore, mgr *workerinternal.Manager, wid string, cmd store.WorkerCommand) {
		cur := concurrent.Add(1)
		defer concurrent.Add(-1)
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := newCommandDispatcher(commandDispatcherMaxConcurrency)
	mgr := workerinternal.NewManager(context.Background(), "w-1", 10, 1000, nil, nil, nil, nil)

	// Start the drain goroutine that actually processes connect commands.
	go d.drainConnectQueue(ctx, nil, mgr, "w-1", slowHandler)

	// Dispatch more commands than the concurrency limit.
	total := commandDispatcherMaxConcurrency * 3
	for i := 0; i < total; i++ {
		cmd := store.WorkerCommand{Type: store.WorkerCommandConnect, Platform: "chzzk", ChannelID: fmt.Sprintf("ch-%d", i)}
		d.dispatch(ctx, nil, mgr, "w-1", cmd, slowHandler)
	}

	// Wait for all queued commands to be processed.
	time.Sleep(500 * time.Millisecond)

	if maxSeen.Load() > int32(commandDispatcherMaxConcurrency) {
		t.Errorf("max concurrent = %d, want <= %d", maxSeen.Load(), commandDispatcherMaxConcurrency)
	}
}

func TestCommandDispatcherDisconnectNotBlocked(t *testing.T) {
	var calls atomic.Int32
	handler := func(ctx context.Context, cs store.ChannelStore, mgr *workerinternal.Manager, wid string, cmd store.WorkerCommand) {
		calls.Add(1)
	}

	d := newCommandDispatcher(1) // only 1 concurrent connect slot
	mgr := workerinternal.NewManager(context.Background(), "w-1", 10, 1000, nil, nil, nil, nil)

	// Disconnect commands run inline — not limited by connect semaphore.
	for i := 0; i < 5; i++ {
		cmd := store.WorkerCommand{Type: store.WorkerCommandDisconnect, Platform: "chzzk", ChannelID: "ch"}
		d.dispatch(context.Background(), nil, mgr, "w-1", cmd, handler)
	}

	if calls.Load() != 5 {
		t.Errorf("expected 5 disconnect calls, got %d", calls.Load())
	}
}

func TestCommandDispatcherDispatchNeverBlocks(t *testing.T) {
	// dispatch must return immediately even when the connect queue has
	// bounded capacity and the drain goroutine is not running.
	d := newCommandDispatcher(1)
	mgr := workerinternal.NewManager(context.Background(), "w-1", 10, 1000, nil, nil, nil, nil)
	noop := func(context.Context, store.ChannelStore, *workerinternal.Manager, string, store.WorkerCommand) {}

	done := make(chan struct{})
	go func() {
		// Send more commands than the queue capacity can hold.
		for i := 0; i < 2000; i++ {
			cmd := store.WorkerCommand{Type: store.WorkerCommandConnect, Platform: "chzzk", ChannelID: "ch"}
			d.dispatch(context.Background(), nil, mgr, "w-1", cmd, noop)
		}
		close(done)
	}()

	select {
	case <-done:
		// OK — dispatch never blocked.
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch blocked — PubSub reader would stall")
	}
}

func TestCommandDispatcherOrderPerChannel(t *testing.T) {
	// Verify that connect and disconnect for the SAME channel are serialised.
	var order []string
	var mu sync.Mutex

	handler := func(ctx context.Context, cs store.ChannelStore, mgr *workerinternal.Manager, wid string, cmd store.WorkerCommand) {
		mu.Lock()
		order = append(order, string(cmd.Type))
		mu.Unlock()
		if cmd.Type == store.WorkerCommandConnect {
			time.Sleep(50 * time.Millisecond) // simulate slow connect
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := newCommandDispatcher(commandDispatcherMaxConcurrency)
	mgr := workerinternal.NewManager(context.Background(), "w-1", 10, 1000, nil, nil, nil, nil)
	go d.drainConnectQueue(ctx, nil, mgr, "w-1", handler)

	// Send connect then disconnect for the same channel.
	d.dispatch(ctx, nil, mgr, "w-1",
		store.WorkerCommand{Type: store.WorkerCommandConnect, Platform: "chzzk", ChannelID: "same-ch"}, handler)

	// Small delay to ensure the connect is picked up first.
	time.Sleep(10 * time.Millisecond)

	d.dispatch(ctx, nil, mgr, "w-1",
		store.WorkerCommand{Type: store.WorkerCommandDisconnect, Platform: "chzzk", ChannelID: "same-ch"}, handler)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 2 {
		t.Fatalf("expected 2 operations, got %d", len(order))
	}
	if order[0] != "connect" || order[1] != "disconnect" {
		t.Errorf("expected [connect, disconnect], got %v", order)
	}
}

func (s *cmdChannelStore) CountAssignedByWorker(_ context.Context) (map[string]int, error) {
	return nil, nil
}

func (s *cmdChannelStore) ListEndedChannelsBefore(_ context.Context, _ time.Time, _ int) ([]model.LiveChannel, error) {
	return nil, nil
}

func (s *cmdChannelStore) ListActiveChannelKeys(_ context.Context) ([]string, error) {
	return nil, nil
}

func (s *cmdChannelStore) ListAllKnownChannelKeys(_ context.Context) ([]string, error) {
	return nil, nil
}
