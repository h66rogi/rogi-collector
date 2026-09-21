package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

func TestPublisherPublishWritesBothStreams(t *testing.T) {
	mr := miniredis.RunT(t)
	client := store.NewRedisClient(mr.Addr())
	defer client.Close()

	p := NewPublisher(client, nil)
	msg := model.ChatMessage{
		ID:        "msg-1",
		Type:      model.MessageTypeDonation,
		Platform:  model.PlatformChzzk,
		ChannelID: "channel-1",
		Message:   "hello",
		Timestamp: time.Now(),
		Amount:    1000,
		Currency:  "KRW",
		AmountKRW: 1000,
	}

	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if got := client.XLen(context.Background(), "chat:chzzk:channel-1").Val(); got != 1 {
		t.Fatalf("expected channel stream length 1, got %d", got)
	}
	if got := client.XLen(context.Background(), "chat:firehose").Val(); got != 1 {
		t.Fatalf("expected firehose stream length 1, got %d", got)
	}
	if p.Published() != 1 || p.Errors() != 0 {
		t.Fatalf("unexpected counters published=%d errors=%d", p.Published(), p.Errors())
	}
}

func TestPublisherPublishErrorIncrementsCounter(t *testing.T) {
	mr := miniredis.RunT(t)
	client := store.NewRedisClient(mr.Addr())
	p := NewPublisher(client, nil)
	client.Close()
	mr.Close()

	err := p.Publish(context.Background(), model.ChatMessage{
		ID:        "msg-2",
		Type:      model.MessageTypeChat,
		Platform:  model.PlatformCime,
		ChannelID: "channel-2",
		Message:   "fail",
		Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("expected Publish to fail after redis shutdown")
	}
	if p.Errors() != 1 {
		t.Fatalf("expected errors counter 1, got %d", p.Errors())
	}
}
