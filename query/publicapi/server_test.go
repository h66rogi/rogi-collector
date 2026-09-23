package publicapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeCollection struct{ state store.CollectionState }

func (f fakeCollection) CollectionState(context.Context, string) (store.CollectionState, error) {
	return f.state, nil
}
func (f fakeCollection) CollectionEnabled(context.Context, string) (bool, error) { return true, nil }

type fakeChat struct{ batch store.ChatBatch }

func (f fakeChat) ReadProductChat(context.Context, string, string) (store.ChatBatch, error) {
	return f.batch, nil
}
func (f fakeChat) ReadLatestProductChat(context.Context, string) (store.ChatBatch, error) {
	return f.batch, nil
}

func testServer(t *testing.T, chat store.ChatBatch, checker BroadcastChecker) *Server {
	t.Helper()
	s, err := New("h66rogi", fakeCollection{store.CollectionState{Runtime: "connected", StateAt: time.Now()}}, fakeChat{chat}, checker, slog.New(slog.NewTextHandler(io.Discard, nil)), 2)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCurrentSeparatesUnknownBroadcastFromCollectorHealth(t *testing.T) {
	var calls atomic.Int32
	checker := func(context.Context, string) (*pb.BroadcastStatus, error) {
		calls.Add(1)
		return &pb.BroadcastStatus{State: "cookie_required", CheckedAt: timestamppb.Now()}, nil
	}
	s := testServer(t, store.ChatBatch{}, checker)
	for range 2 {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/broadcasts/current", nil))
		if response.Code != 200 {
			t.Fatalf("status: %d", response.Code)
		}
		var value map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if value["live"] != nil || value["state"] != "cookie_required" {
			t.Fatalf("unexpected broadcast state: %#v", value)
		}
		collection := value["collection"].(map[string]any)
		if collection["active"] != true {
			t.Fatalf("collection status was conflated with broadcast: %#v", collection)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("expected cached diagnostic, got %d calls", calls.Load())
	}
}

func TestRecentRejectsExpiredGeneration(t *testing.T) {
	s := testServer(t, store.ChatBatch{Generation: "new", Earliest: "2-0"}, func(context.Context, string) (*pb.BroadcastStatus, error) { return nil, nil })
	response := httptest.NewRecorder()
	path := "/v1/chats/recent?cursor=" + encodeCursor(chatCursor{"old", "1-0"})
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "cursor_expired") {
		t.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
	}
}

func TestRecentWithoutCursorReturnsNewestMessages(t *testing.T) {
	batch := store.ChatBatch{Generation: "generation-1", Earliest: "1-0", Latest: "3-0"}
	for i, id := range []string{"1-0", "2-0", "3-0"} {
		batch.Messages = append(batch.Messages, store.StreamMessage{StreamID: id, Values: map[string]interface{}{
			"type": "chat", "id": id, "userId": "viewer", "nickname": "viewer",
			"timestamp": "2026-09-23T08:00:00Z", "message": string(rune('a' + i)),
		}})
	}
	s := testServer(t, batch, func(context.Context, string) (*pb.BroadcastStatus, error) { return nil, nil })
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/chats/recent?limit=2", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d %s", response.Code, response.Body.String())
	}
	var value struct {
		Messages []struct {
			EventID string `json:"eventId"`
		} `json:"messages"`
		NextCursor string `json:"nextCursor"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if len(value.Messages) != 2 || value.Messages[0].EventID != "2-0" || value.Messages[1].EventID != "3-0" || value.NextCursor != encodeCursor(chatCursor{"generation-1", "3-0"}) {
		t.Fatalf("unexpected recent messages: %#v", value)
	}
}

func TestPublicRequestBudgetLimitsPollingButKeepsHealthAvailable(t *testing.T) {
	s := testServer(t, store.ChatBatch{}, func(context.Context, string) (*pb.BroadcastStatus, error) { return nil, nil })
	s.rateMu.Lock()
	s.rateTokens = 0
	s.rateAt = time.Now()
	s.rateMu.Unlock()
	limited := httptest.NewRecorder()
	s.Handler().ServeHTTP(limited, httptest.NewRequest(http.MethodGet, "/v1/chats/recent", nil))
	if limited.Code != http.StatusTooManyRequests || limited.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected bounded polling, got %d", limited.Code)
	}
	health := httptest.NewRecorder()
	s.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health check was rate limited: %d", health.Code)
	}
}

func TestWebSocketReportsGapAfterGenerationChange(t *testing.T) {
	s := testServer(t, store.ChatBatch{Generation: "new", Earliest: "2-0", Latest: "2-0"}, func(context.Context, string) (*pb.BroadcastStatus, error) { return nil, nil })
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/chat/stream?cursor=" + encodeCursor(chatCursor{"old", "1-0"})
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	var hello, gap map[string]any
	if err := wsjson.Read(ctx, conn, &hello); err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Read(ctx, conn, &gap); err != nil {
		t.Fatal(err)
	}
	if hello["type"] != "hello" || gap["type"] != "chat.gap" || gap["reason"] != "cursor_expired" {
		t.Fatalf("unexpected events: %#v %#v", hello, gap)
	}
}

func TestWebSocketBroadcastStatusIncludesUpdatedTitle(t *testing.T) {
	var calls atomic.Int32
	checker := func(context.Context, string) (*pb.BroadcastStatus, error) {
		if calls.Add(1) == 1 {
			return &pb.BroadcastStatus{State: "live", BroadcastId: "broadcast-1", Title: "First title", CheckedAt: timestamppb.Now()}, nil
		}
		return &pb.BroadcastStatus{State: "live", BroadcastId: "broadcast-1", Title: "Updated title", CheckedAt: timestamppb.Now()}, nil
	}
	s := testServer(t, store.ChatBatch{}, checker)
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 13*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/chat/stream"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	var event map[string]any
	if err := wsjson.Read(ctx, conn, &event); err != nil {
		t.Fatal(err)
	}
	if event["type"] != "hello" || event["title"] != "First title" {
		t.Fatalf("unexpected handshake: %#v", event)
	}
	if err := wsjson.Read(ctx, conn, &event); err != nil {
		t.Fatal(err)
	}
	if event["type"] != "broadcast.status" || event["title"] != "Updated title" || event["broadcastId"] != "broadcast-1" {
		t.Fatalf("unexpected status change: %#v", event)
	}
}
