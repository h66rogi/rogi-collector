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

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestProbeServerHealthz(t *testing.T) {
	ps := NewProbeServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	ps.handleHealthz(rec, req)

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

func TestProbeServerReadyz(t *testing.T) {
	ps := NewProbeServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	rec := httptest.NewRecorder()
	ps.handleReadyz(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before ready, got %d", rec.Code)
	}

	ps.SetReady(true)

	rec = httptest.NewRecorder()
	ps.handleReadyz(rec, req)
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

func TestProbeServerStartAndShutdown(t *testing.T) {
	ps := NewProbeServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)

	done := make(chan error, 1)
	go func() {
		done <- ps.Start()
	}()

	time.Sleep(20 * time.Millisecond)

	if err := ps.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("probe server did not stop after shutdown")
	}
}

func TestProbeServerMetrics(t *testing.T) {
	registry, metrics := NewMetricsRegistry()
	metrics.Ready.Set(1)
	ps := NewProbeServer(":0", slog.New(slog.NewTextHandler(os.Stderr, nil)), map[string]http.Handler{
		"/metrics": promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	ps.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "meloming_chat_coordinator_ready") {
		t.Fatalf("expected coordinator ready metric in body, got %q", rec.Body.String())
	}
}
