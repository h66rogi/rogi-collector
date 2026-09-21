package internal

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestServerHealthz(t *testing.T) {
	srv := NewServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	srv.handleHealthz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

func TestServerReadyz(t *testing.T) {
	srv := NewServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	rec := httptest.NewRecorder()
	srv.handleReadyz(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before ready, got %d", rec.Code)
	}

	srv.SetReady(true)

	rec = httptest.NewRecorder()
	srv.handleReadyz(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after ready, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body["status"] != "ready" {
		t.Fatalf("expected status=ready, got %q", body["status"])
	}
}

func TestServerStartAndShutdown(t *testing.T) {
	srv := NewServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	done := make(chan error, 1)
	go func() {
		done <- srv.Start()
	}()

	time.Sleep(20 * time.Millisecond)

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop after shutdown")
	}
}

func TestServerExtraHandler(t *testing.T) {
	srv := NewServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), map[string]http.Handler{
		"/metrics": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("worker_metric 1\n"))
		}),
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "worker_metric 1") {
		t.Fatalf("expected metrics body, got %q", rec.Body.String())
	}
}
