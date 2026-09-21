package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/diagnostics"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
	"github.com/h66rogi/rogi-collector/worker/internal/connector"
)

type testChatConnector struct {
	messages chan model.ChatMessage
	errs     chan error
	closed   atomic.Int32
	connect  func(context.Context) error
}

func (c *testChatConnector) Connect(ctx context.Context, _ model.LiveChannel) error {
	if c.connect != nil {
		return c.connect(ctx)
	}
	return nil
}
func (c *testChatConnector) Disconnect() error                  { c.closed.Add(1); return nil }
func (c *testChatConnector) IsAlive() bool                      { return c.closed.Load() == 0 }
func (c *testChatConnector) Messages() <-chan model.ChatMessage { return c.messages }
func (c *testChatConnector) Errors() <-chan error               { return c.errs }

const testDiagnosticToken = "synthetic-chat-diagnostic-token-only"

func chatTestCall(m *ChatTestDiagnostics, action, id, channel string) (int, diagnostics.ChatTestStatus) {
	body, _ := json.Marshal(map[string]string{"action": action, "sessionId": id, "targetChannelId": channel})
	r := httptest.NewRequest("POST", "/diagnostics/chat-test", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+testDiagnosticToken)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	var s diagnostics.ChatTestStatus
	_ = json.Unmarshal(w.Body.Bytes(), &s)
	return w.Code, s
}
func waitChatTest(t *testing.T, m *ChatTestDiagnostics, predicate func(diagnostics.ChatTestStatus) bool) diagnostics.ChatTestStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, s := chatTestCall(m, "status", "", "")
		if predicate(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("chat test state timed out")
	return diagnostics.ChatTestStatus{}
}
func TestChatTestSessionIsBoundedIsolatedAndIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &testChatConnector{messages: make(chan model.ChatMessage, 100), errs: make(chan error, 1)}
	var starts atomic.Int32
	m := NewChatTestDiagnostics(ctx, testDiagnosticToken, func() connector.PlatformConnector { starts.Add(1); return c })
	id := uuid.NewString()
	code, first := chatTestCall(m, "start", id, "fixture-channel")
	if code != 200 || first.State != "connecting" {
		t.Fatal(code, first.State)
	}
	waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool {
		return s.State == "joined" && s.JoinedAt != nil && s.ReceivedCount == 0
	})
	if code, _ = chatTestCall(m, "start", id, "fixture-channel"); code != 200 || starts.Load() != 1 {
		t.Fatal("duplicate start created connection")
	}
	if code, _ = chatTestCall(m, "start", id, "different-channel"); code != 409 {
		t.Fatal("idempotency key changed target")
	}
	if code, _ = chatTestCall(m, "start", uuid.NewString(), "other-channel"); code != 409 {
		t.Fatal("parallel test accepted")
	}
	if code, _ = chatTestCall(m, "stop", uuid.NewString(), ""); code != 404 || c.closed.Load() != 0 {
		t.Fatal("stale stop affected active test")
	}
	c.messages <- model.ChatMessage{Type: model.MessageTypeDonation, ChannelID: "fixture-channel", Raw: "synthetic-private-packet"}
	c.messages <- model.ChatMessage{Type: model.MessageTypeChat, ChannelID: "wrong-channel", Message: "wrong scope"}
	for i := 0; i < 25; i++ {
		c.messages <- model.ChatMessage{Type: model.MessageTypeChat, ChannelID: "fixture-channel", Nickname: strings.Repeat("n", 90), Message: strings.Repeat("m", 1100), Raw: "synthetic-private-packet", UserID: "excluded-user-id"}
	}
	s := waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool { return s.ReceivedCount == 25 })
	if s.State != "receiving" || len(s.Messages) != 20 || s.Messages[0].Sequence != 6 || len(s.Messages[0].Message) != 1000 || len(s.Messages[0].DisplayName) != 80 {
		t.Fatal("bounded chat sample invalid")
	}
	encoded, _ := json.Marshal(s)
	if strings.Contains(string(encoded), "synthetic-private-packet") || strings.Contains(string(encoded), "excluded-user-id") {
		t.Fatal("raw/private fields leaked")
	}
	code, s = chatTestCall(m, "stop", id, "")
	if code != 200 || s.Active || s.State != "stopped" || c.closed.Load() != 1 {
		t.Fatal("stop did not close connection")
	}
	if code, _ = chatTestCall(m, "start", id, "fixture-channel"); code != 200 || starts.Load() != 1 {
		t.Fatal("retry restarted completed test")
	}
	if code, _ = chatTestCall(m, "start", uuid.NewString(), "fixture-channel"); code != 429 {
		t.Fatal("start throttle bypassed")
	}
}
func TestChatTestTimeoutRetentionAndCancellationDuringConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &testChatConnector{messages: make(chan model.ChatMessage), errs: make(chan error)}
	m := NewChatTestDiagnostics(ctx, testDiagnosticToken, func() connector.PlatformConnector { return c })
	m.lifetime = 30 * time.Millisecond
	m.retention = 30 * time.Millisecond
	chatTestCall(m, "start", uuid.NewString(), "fixture-channel")
	waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool { return s.State == "expired" && !s.Active })
	if c.closed.Load() != 1 {
		t.Fatal("deadline did not disconnect")
	}
	waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool { return s.State == "idle" && len(s.Messages) == 0 })
	c = &testChatConnector{messages: make(chan model.ChatMessage), errs: make(chan error), connect: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	m = NewChatTestDiagnostics(ctx, testDiagnosticToken, func() connector.PlatformConnector { return c })
	id := uuid.NewString()
	chatTestCall(m, "start", id, "fixture-channel")
	code, s := chatTestCall(m, "stop", id, "")
	if code != 200 || s.State != "stopped" || s.JoinedAt != nil || c.closed.Load() != 1 {
		t.Fatal("connecting test did not stop")
	}
}
func TestChatTestErrorsAndRejectedRequests(t *testing.T) {
	for _, tc := range []struct {
		err   error
		state string
	}{{soopauth.ErrOffline, "offline"}, {soopauth.ErrCookieUnavailable, "cookie_required"}, {soopauth.ErrLoginRequired, "auth_required"}, {errors.New("synthetic secret"), "failed"}} {
		t.Run(tc.state, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c := &testChatConnector{connect: func(context.Context) error { return tc.err }}
			m := NewChatTestDiagnostics(ctx, testDiagnosticToken, func() connector.PlatformConnector { return c })
			chatTestCall(m, "start", uuid.NewString(), "fixture-channel")
			s := waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool { return !s.Active })
			if s.State != tc.state || s.JoinedAt != nil {
				t.Fatal("false joined/offline classification")
			}
		})
	}
	m := NewChatTestDiagnostics(context.Background(), testDiagnosticToken, func() connector.PlatformConnector { t.Fatal("invalid request opened connection"); return nil })
	for _, tc := range []struct{ action, id, target string }{{"start", uuid.NewString(), "https://example.com"}, {"start", "", "fixture-channel"}, {"stop", "bad", ""}, {"unknown", "", ""}} {
		code, _ := chatTestCall(m, tc.action, tc.id, tc.target)
		if code != 400 {
			t.Fatal("invalid request accepted")
		}
	}
	for _, body := range []string{`{"action":"status"}{}`, `{"action":"status","unknown":true}`, strings.Repeat("x", 513)} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+testDiagnosticToken)
		w := httptest.NewRecorder()
		m.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal("invalid body accepted")
		}
	}
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"action":"status"}`)))
	if w.Code != 401 {
		t.Fatal("missing token accepted")
	}
}

func TestChatTestRespectsExistingOptOutPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var blocked atomic.Bool
	blocked.Store(true)
	c := &testChatConnector{messages: make(chan model.ChatMessage), errs: make(chan error)}
	var starts atomic.Int32
	m := NewChatTestDiagnostics(ctx, testDiagnosticToken, func() connector.PlatformConnector { starts.Add(1); return c })
	m.IsOptedOut = func(string) bool { return blocked.Load() }
	if code, _ := chatTestCall(m, "start", uuid.NewString(), "fixture-channel"); code != 403 || starts.Load() != 0 {
		t.Fatal("opted-out channel connected")
	}
	blocked.Store(false)
	chatTestCall(m, "start", uuid.NewString(), "fixture-channel")
	waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool { return s.State == "joined" })
	blocked.Store(true)
	s := waitChatTest(t, m, func(s diagnostics.ChatTestStatus) bool { return s.State == "blocked" })
	if s.Active || len(s.Messages) != 0 || c.closed.Load() != 1 {
		t.Fatal("opt-out did not stop test")
	}
}
