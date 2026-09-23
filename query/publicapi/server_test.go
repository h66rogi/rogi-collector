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
