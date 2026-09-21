package connector

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoWithRetrySucceedsFirstAttempt(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return httpResponse(http.StatusOK, `{"ok":true}`), nil
	})}

	req, _ := http.NewRequest("GET", "https://example.com/test", nil)
	resp, err := DoWithRetry(context.Background(), client, req, DefaultRetryConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 call, got %d", calls.Load())
	}
}

func TestDoWithRetryRetriesOn502(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		n := calls.Add(1)
		if n <= 2 {
			return httpResponse(http.StatusBadGateway, "bad gateway"), nil
		}
		return httpResponse(http.StatusOK, `{"ok":true}`), nil
	})}

	req, _ := http.NewRequest("GET", "https://example.com/test", nil)
	cfg := HTTPRetryConfig{MaxRetries: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2}
	resp, err := DoWithRetry(context.Background(), client, req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls.Load() != 3 {
		t.Errorf("expected 3 calls (2 retries + success), got %d", calls.Load())
	}
}

func TestDoWithRetryRetriesOn429(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		n := calls.Add(1)
		if n <= 1 {
			return httpResponse(http.StatusTooManyRequests, "rate limited"), nil
		}
		return httpResponse(http.StatusOK, `{}`), nil
	})}

	req, _ := http.NewRequest("GET", "https://example.com/test", nil)
	cfg := HTTPRetryConfig{MaxRetries: 2, BaseDelay: 1 * time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2}
	resp, err := DoWithRetry(context.Background(), client, req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls.Load() != 2 {
		t.Errorf("expected 2 calls, got %d", calls.Load())
	}
}

func TestDoWithRetryExhaustsRetries(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return httpResponse(http.StatusBadGateway, "bad gateway"), nil
	})}

	req, _ := http.NewRequest("GET", "https://example.com/test", nil)
	cfg := HTTPRetryConfig{MaxRetries: 2, BaseDelay: 1 * time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2}
	_, err := DoWithRetry(context.Background(), client, req, cfg)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}

	retryErr, ok := err.(*RetryableHTTPError)
	if !ok {
		t.Fatalf("expected RetryableHTTPError, got %T: %v", err, err)
	}
	if retryErr.StatusCode != 502 {
		t.Errorf("expected status 502, got %d", retryErr.StatusCode)
	}
	if retryErr.Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", retryErr.Attempts)
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 total calls, got %d", calls.Load())
	}
}

func TestDoWithRetryDoesNotRetryOn404(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		return httpResponse(http.StatusNotFound, "not found"), nil
	})}

	req, _ := http.NewRequest("GET", "https://example.com/test", nil)
	cfg := HTTPRetryConfig{MaxRetries: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2}
	resp, err := DoWithRetry(context.Background(), client, req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if calls.Load() != 1 {
		t.Errorf("expected 1 call (no retry for 404), got %d", calls.Load())
	}
}

func TestDoWithRetryRespectsContextCancellation(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return httpResponse(http.StatusBadGateway, "bad gateway"), nil
	})}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	req, _ := http.NewRequestWithContext(ctx, "GET", "https://example.com/test", nil)
	cfg := HTTPRetryConfig{MaxRetries: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 1 * time.Second, Multiplier: 2}
	_, err := DoWithRetry(ctx, client, req, cfg)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestDoWithRetryWithPOSTBody(t *testing.T) {
	var bodies []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		bodies = append(bodies, string(b))
		if len(bodies) < 2 {
			return httpResponse(http.StatusBadGateway, "bad gateway"), nil
		}
		return httpResponse(http.StatusOK, `{}`), nil
	})}

	body := "key=value"
	req, _ := http.NewRequest("POST", "https://example.com/test", strings.NewReader(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}

	cfg := HTTPRetryConfig{MaxRetries: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2}
	resp, err := DoWithRetry(context.Background(), client, req, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if len(bodies) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(bodies))
	}
	// Both requests should have the same body.
	if bodies[0] != body || bodies[1] != body {
		t.Errorf("body mismatch: %v", bodies)
	}
}

func TestDoWithRetryRejectsUnsafeTarget(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DoWithRetry(context.Background(), http.DefaultClient, req, DefaultRetryConfig())
	if err == nil || !strings.Contains(err.Error(), "must use HTTPS") {
		t.Fatalf("expected unsafe target rejection, got %v", err)
	}
}

func TestDoWithRetryDoesNotFollowRedirects(t *testing.T) {
	targetHits := atomic.Int32{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, target.URL, http.StatusFound)
	}))
	defer source.Close()

	req, err := http.NewRequest(http.MethodGet, source.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := DoWithRetry(context.Background(), source.Client(), req, DefaultRetryConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusFound)
	}
	if got := targetHits.Load(); got != 0 {
		t.Fatalf("redirect target was requested %d times", got)
	}
}
