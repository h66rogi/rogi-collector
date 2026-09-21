package internal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// ProbeServer exposes liveness/readiness endpoints for Kubernetes probes.
type ProbeServer struct {
	server *http.Server
	ready  atomic.Bool
	logger *slog.Logger
}

// NewProbeServer creates a probe server listening on the given address.
func NewProbeServer(addr string, logger *slog.Logger, extraHandlers map[string]http.Handler) *ProbeServer {
	ps := &ProbeServer{
		logger: logger.With("component", "probe-server"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", ps.handleHealthz)
	mux.HandleFunc("/readyz", ps.handleReadyz)
	for path, handler := range extraHandlers {
		mux.Handle(path, handler)
	}

	ps.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	return ps
}

// SetReady toggles readiness for /readyz responses.
func (ps *ProbeServer) SetReady(ready bool) {
	ps.ready.Store(ready)
}

// Start serves probe endpoints until Shutdown is called.
func (ps *ProbeServer) Start() error {
	ps.logger.Info("starting probe server", "addr", ps.server.Addr)
	err := ps.server.ListenAndServe()
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the probe server.
func (ps *ProbeServer) Shutdown(ctx context.Context) error {
	ps.SetReady(false)
	return ps.server.Shutdown(ctx)
}

func (ps *ProbeServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeProbeResponse(w, http.StatusOK, "ok")
}

func (ps *ProbeServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !ps.ready.Load() {
		writeProbeResponse(w, http.StatusServiceUnavailable, "not_ready")
		return
	}
	writeProbeResponse(w, http.StatusOK, "ready")
}

func writeProbeResponse(w http.ResponseWriter, status int, state string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": state})
}
