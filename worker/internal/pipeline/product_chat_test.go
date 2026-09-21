package pipeline

import (
	"context"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"os"
	"testing"
	"time"
)

func TestProductChatRetentionAndGeneration(t *testing.T) {
	addr := os.Getenv("COLLECTOR_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("COLLECTOR_TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	rdb := store.NewRedisClient(addr)
	defer rdb.Close()
	pub := NewPublisher(rdb, nil)
	rs := store.NewRedisStore(rdb)
	channel := "fixture_pub_" + time.Now().Format("150405.000000000")
	key := store.ChatStreamKey("soop", channel)
	defer rdb.Del(ctx, key, key+":generation")
	msg := model.ChatMessage{ID: "fixture", Type: model.MessageTypeChat, Platform: model.PlatformSoop, ChannelID: channel, UserID: "fixture_donor", Message: "!이동 7", Timestamp: time.Now(), Raw: "never export raw"}
	if err := pub.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}
	first, err := rs.ReadProductChat(ctx, channel, "0-0")
	if err != nil || len(first.Messages) != 1 || first.Generation == "" {
		t.Fatal(first, err)
	}
	keys, err := rs.ScanChatStreamKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundStream := false
	for _, candidate := range keys {
		if candidate == key+":generation" {
			t.Fatal("cleanup would delete live generation metadata")
		}
		if candidate == key {
			foundStream = true
		}
	}
	if !foundStream {
		t.Fatal("stream missing from cleanup scan")
	}
	if _, found := first.Messages[0].Values["raw"]; found {
		t.Fatal("raw message escaped product buffer")
	}
	if err = pub.PublishWithDedup(ctx, msg); err != nil {
		t.Fatal(err)
	}
	second, err := rs.ReadProductChat(ctx, channel, "0-0")
	if err != nil || len(second.Messages) != 2 || second.Generation != first.Generation {
		t.Fatal("identical chat was silently deduplicated", err)
	}
	if err = rdb.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if err = pub.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}
	reset, err := rs.ReadProductChat(ctx, channel, "0-0")
	if err != nil || reset.Generation == first.Generation {
		t.Fatal("stream reset kept old generation", err)
	}
}
