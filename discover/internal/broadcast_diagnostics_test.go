package internal

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type broadcastDoer func(*http.Request) (*http.Response, error)

func (f broadcastDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }
func TestBroadcastDiagnosticsAuthorizationValidationAndCache(t *testing.T) {
	token := strings.Repeat("fixture-", 5)
	var calls atomic.Int32
	client := broadcastDoer(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"CHANNEL":{"RESULT":0}}`))}, nil
	})
	h := NewBroadcastDiagnostics(token, client)
	call := func(body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/diagnostics/broadcast", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if call(`{"channelId":"fixture-channel"}`, "wrong").Code != 401 {
		t.Fatal("missing token accepted")
	}
	for _, body := range []string{`{"channelId":"https://example.com"}`, `{"channelId":"fixture-channel","extra":true}`, `{"channelId":"fixture-channel"}{}`, strings.Repeat("x", 513)} {
		if call(body, token).Code != 400 {
			t.Fatal("invalid body accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("rejected request reached player API")
	}
	first := call(`{"channelId":"fixture-channel"}`, token)
	if first.Code != 200 || !strings.Contains(first.Body.String(), `"cached":false`) {
		t.Fatal(first.Code, first.Body.String())
	}
	second := call(`{"channelId":"fixture-channel"}`, token)
	if second.Code != 200 || !strings.Contains(second.Body.String(), `"cached":true`) || calls.Load() != 1 {
		t.Fatal("cache not respected")
	}
	if second.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("browser caching enabled")
	}
	if call(`{"channelId":"other-channel"}`, token).Code != 429 || calls.Load() != 1 {
		t.Fatal("cross-channel rate limit bypassed")
	}
}
func TestBroadcastDiagnosticsSingleInflight(t *testing.T) {
	token := strings.Repeat("fixture-", 5)
	entered := make(chan struct{})
	release := make(chan struct{})
	client := broadcastDoer(func(*http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"CHANNEL":{"RESULT":0}}`))}, nil
	})
	h := NewBroadcastDiagnostics(token, client)
	call := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/", strings.NewReader(`{"channelId":"fixture-channel"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- call() }()
	<-entered
	second := call()
	close(release)
	first := <-done
	if second.Code != 429 || first.Code != 200 {
		t.Fatal("concurrent lookup was not bounded")
	}
}
