package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/h66rogi/rogi-collector/worker/internal/pipeline"
)

type asyncTestConnector struct {
	msgCh          chan model.ChatMessage
	errCh          chan error
	disconnectErr  error
	disconnectCall int
}

func (c *asyncTestConnector) Connect(context.Context, model.LiveChannel) error { return nil }
func (c *asyncTestConnector) Disconnect() error {
	c.disconnectCall++
	return c.disconnectErr
}
func (c *asyncTestConnector) IsAlive() bool                      { return true }
func (c *asyncTestConnector) Messages() <-chan model.ChatMessage { return c.msgCh }
func (c *asyncTestConnector) Errors() <-chan error               { return c.errCh }

func newWorkerRedisStore(t *testing.T) (*miniredis.Miniredis, *store.RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := store.NewRedisClient(mr.Addr())
	t.Cleanup(func() { _ = client.Close() })
	return mr, store.NewRedisStore(client)
}

func TestStartHeartbeatLoopStoresHeartbeat(t *testing.T) {
	_, redisStore := newWorkerRedisStore(t)
	mgr := NewManager(context.Background(), "worker-1", 20, 1000, &pipeline.Publisher{}, redisStore, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go mgr.StartHeartbeatLoop(ctx, 5*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	cancel()

	load, err := redisStore.GetWorkerHeartbeat(context.Background(), "worker-1")
	if err != nil {
		t.Fatalf("GetWorkerHeartbeat failed: %v", err)
	}
	if load == nil || load.WorkerID != "worker-1" {
		t.Fatalf("unexpected heartbeat load: %+v", load)
	}
}

func TestForwardMessagesPublishesToRedisStreams(t *testing.T) {
	mr, redisStore := newWorkerRedisStore(t)
	client := store.NewRedisClient(mr.Addr())
	t.Cleanup(func() { _ = client.Close() })

	pub := pipeline.NewPublisher(client, nil)
	mgr := NewManager(context.Background(), "worker-1", 20, 1000, pub, redisStore, nil, nil)

	conn := &asyncTestConnector{msgCh: make(chan model.ChatMessage, 1), errCh: make(chan error, 1)}
	ac := &activeConn{connector: conn}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.forwardMessages(ctx, "chzzk:test", ac)

	conn.msgCh <- model.ChatMessage{
		ID:        "m1",
		Type:      model.MessageTypeChat,
		Platform:  model.PlatformChzzk,
		ChannelID: "channel-1",
		Message:   "hello",
		Timestamp: time.Now(),
	}

	time.Sleep(20 * time.Millisecond)

	if got := client.XLen(context.Background(), "chat:chzzk:channel-1").Val(); got != 1 {
		t.Fatalf("expected channel stream length 1, got %d", got)
	}
	if got := client.XLen(context.Background(), "chat:firehose").Val(); got != 1 {
		t.Fatalf("expected firehose stream length 1, got %d", got)
	}
}

func TestHandleErrorsPublishesCoordEventAndRemovesConn(t *testing.T) {
	_, redisStore := newWorkerRedisStore(t)
	mgr := NewManager(context.Background(), "worker-1", 20, 1000, &pipeline.Publisher{}, redisStore, nil, nil)

	conn := &asyncTestConnector{msgCh: make(chan model.ChatMessage, 1), errCh: make(chan error, 1)}
	ac := &activeConn{
		connector: conn,
		channel: model.LiveChannel{
			Platform:  model.PlatformChzzk,
			ChannelID: "channel-1",
		},
	}
	mgr.conns["chzzk:channel-1"] = ac

	sub := redisStore.SubscribeCoordEvents(context.Background())
	defer sub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ac.cancel = cancel
	go mgr.handleErrors(ctx, "chzzk:channel-1", ac)

	conn.errCh <- errors.New("socket closed")

	select {
	case msg := <-sub.Channel():
		if msg.Payload == "" || !containsRuntime(msg.Payload, `"type":"ws_closed"`) {
			t.Fatalf("unexpected coord event payload: %s", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for coord event after connection context cancellation")
	}

	if mgr.ConnectionCount() != 0 {
		t.Fatalf("expected connection to be removed after error, got %d", mgr.ConnectionCount())
	}
}

func TestDisconnectHandlesConnectorError(t *testing.T) {
	conn := &asyncTestConnector{
		msgCh:         make(chan model.ChatMessage),
		errCh:         make(chan error),
		disconnectErr: fmt.Errorf("disconnect failed"),
	}
	mgr := &Manager{
		conns: map[string]*activeConn{
			"soop:abc": {
				connector: conn,
				cancel:    func() {},
				channel: model.LiveChannel{
					Platform:  model.PlatformSoop,
					ChannelID: "abc",
				},
			},
		},
	}

	if err := mgr.Disconnect(model.PlatformSoop, "abc"); err != nil {
		t.Fatalf("Disconnect should tolerate connector errors, got %v", err)
	}
	if conn.disconnectCall != 1 {
		t.Fatalf("expected connector disconnect to be called once, got %d", conn.disconnectCall)
	}
}

func containsRuntime(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
