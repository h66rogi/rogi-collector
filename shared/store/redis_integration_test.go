package store

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/redis/go-redis/v9"
)

func TestReadLatestProductChatReturnsNewestRowsInAscendingOrder(t *testing.T) {
	_, store := newRedisStoreForTest(t)
	ctx := context.Background()
	key := ChatStreamKey("soop", "h66rogi")
	if err := store.client.Set(ctx, key+":generation", "generation-1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 120; i++ {
		if err := store.client.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: strconv.Itoa(i) + "-0", Values: map[string]any{"type": "chat", "message": strconv.Itoa(i)}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := store.ReadLatestProductChat(ctx, "h66rogi")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Generation != "generation-1" || batch.Earliest != "1-0" || batch.Latest != "120-0" || len(batch.Messages) != 100 || batch.Messages[0].StreamID != "21-0" || batch.Messages[99].StreamID != "120-0" {
		t.Fatalf("unexpected latest batch: generation=%q earliest=%q latest=%q count=%d", batch.Generation, batch.Earliest, batch.Latest, len(batch.Messages))
	}
}

func newRedisStoreForTest(t *testing.T) (*miniredis.Miniredis, *RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := NewRedisClient(mr.Addr())
	t.Cleanup(func() { _ = client.Close() })
	return mr, NewRedisStore(client)
}

func TestRedisStoreLeaderElectionRoundTrip(t *testing.T) {
	_, store := newRedisStoreForTest(t)
	ctx := context.Background()

	const leaderKey = "coordinator:leader"

	ok, err := store.TryAcquireLeader(ctx, leaderKey, "leader-1", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("TryAcquireLeader failed: ok=%v err=%v", ok, err)
	}

	ok, err = store.TryAcquireLeader(ctx, leaderKey, "leader-2", 5*time.Second)
	if err != nil {
		t.Fatalf("second TryAcquireLeader returned error: %v", err)
	}
	if ok {
		t.Fatal("expected second leader acquisition to fail while lease exists")
	}

	leader, err := store.GetLeader(ctx, leaderKey)
	if err != nil {
		t.Fatalf("GetLeader failed: %v", err)
	}
	if leader != "leader-1" {
		t.Fatalf("expected leader-1, got %q", leader)
	}

	ok, err = store.RenewLeader(ctx, leaderKey, "leader-1", 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("RenewLeader failed: ok=%v err=%v", ok, err)
	}

	ok, err = store.RenewLeader(ctx, leaderKey, "other", 5*time.Second)
	if err != nil {
		t.Fatalf("RenewLeader(other) returned error: %v", err)
	}
	if ok {
		t.Fatal("expected renew with wrong instance to fail")
	}
}

func TestRedisStoreHeartbeatRoundTrip(t *testing.T) {
	_, store := newRedisStoreForTest(t)
	ctx := context.Background()

	load := model.WorkerLoad{
		WorkerID:     "worker-1",
		Connections:  7,
		MsgPerSec:    12.5,
		MaxConn:      100,
		MaxMsgPerSec: 1000,
	}
	if err := store.SendHeartbeat(ctx, "worker-1", load); err != nil {
		t.Fatalf("SendHeartbeat failed: %v", err)
	}

	got, err := store.GetWorkerHeartbeat(ctx, "worker-1")
	if err != nil {
		t.Fatalf("GetWorkerHeartbeat failed: %v", err)
	}
	if got == nil || got.WorkerID != "worker-1" || got.Connections != 7 {
		t.Fatalf("unexpected heartbeat load: %+v", got)
	}

	all, err := store.GetWorkerHeartbeats(ctx, []string{"worker-1", "worker-2"})
	if err != nil {
		t.Fatalf("GetWorkerHeartbeats failed: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 heartbeat in batch result, got %d", len(all))
	}
	if _, ok := all["worker-2"]; ok {
		t.Fatal("expected missing worker to be omitted from batch result")
	}
}

func TestRedisStorePublishAndSubscribeCoordEvents(t *testing.T) {
	_, store := newRedisStoreForTest(t)
	ctx := context.Background()

	pubsub := store.SubscribeCoordEvents(ctx)
	defer pubsub.Close()

	want := CoordEvent{
		Type:      CoordEventWSClosed,
		Platform:  model.PlatformChzzk,
		ChannelID: "ch-1",
		WorkerID:  "worker-1",
	}
	if err := store.PublishCoordEvent(ctx, want); err != nil {
		t.Fatalf("PublishCoordEvent failed: %v", err)
	}

	select {
	case msg := <-pubsub.Channel():
		if msg.Payload == "" {
			t.Fatal("expected non-empty coord event payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for coord event")
	}
}

func TestRedisStorePublishAndSubscribeWorkerCommands(t *testing.T) {
	_, store := newRedisStoreForTest(t)
	ctx := context.Background()

	pubsub := store.SubscribeWorkerCommands(ctx, "worker-7")
	defer pubsub.Close()

	if err := store.PublishWorkerCommand(ctx, "worker-7", WorkerCommand{
		Type:      WorkerCommandDisconnect,
		Platform:  model.PlatformSoop,
		ChannelID: "bj-7",
	}); err != nil {
		t.Fatalf("PublishWorkerCommand failed: %v", err)
	}

	select {
	case msg := <-pubsub.Channel():
		if msg.Payload == "" {
			t.Fatal("expected non-empty worker command payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker command")
	}
}

func TestNewRedisClientCreatesUsableClient(t *testing.T) {
	mr := miniredis.RunT(t)
	client := NewRedisClient(mr.Addr())
	defer client.Close()

	if err := client.Set(context.Background(), "ping", "pong", 0).Err(); err != nil {
		t.Fatalf("redis SET failed: %v", err)
	}
	if got, err := mr.Get("ping"); err != nil || got != "pong" {
		t.Fatalf("expected pong in miniredis, got %q", got)
	}
}
