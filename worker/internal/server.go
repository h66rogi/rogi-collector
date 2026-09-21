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

// Server exposes liveness/readiness and optional extra HTTP handlers.
type Server struct {
	server *http.Server
	ready  atomic.Bool
	logger *slog.Logger
}

// NewServer creates a worker runtime server listening on the given address.
func NewServer(addr string, logger *slog.Logger, extraHandlers map[string]http.Handler) *Server {
	s := &Server{
		logger: logger.With("component", "runtime-server"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	for path, handler := range extraHandlers {
		mux.Handle(path, handler)
	}

	s.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	return s
}

// SetReady toggles readiness for /readyz.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// Start serves runtime endpoints until Shutdown is called.
func (s *Server) Start() error {
	s.logger.Info("starting runtime server", "addr", s.server.Addr)
	err := s.server.ListenAndServe()
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.SetReady(false)
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeServerResponse(w, http.StatusOK, "ok")
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeServerResponse(w, http.StatusServiceUnavailable, "not_ready")
		return
	}
	writeServerResponse(w, http.StatusOK, "ready")
}

func writeServerResponse(w http.ResponseWriter, status int, state string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": state})
}
