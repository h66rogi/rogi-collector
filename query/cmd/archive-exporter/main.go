package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/h66rogi/rogi-collector/query/archive"
	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"github.com/h66rogi/rogi-collector/shared/store"
)

const exportAge = 10 * time.Minute

func main() {
	if err := run(); err != nil {
		slog.Error("archive exporter stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if err := runtimeenv.Load(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	channel, err := collection.Channel(os.Getenv("CHANNEL_ALLOWLIST"))
	if err != nil {
		return err
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL required")
	}
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := store.NewPgPoolWithRetry(startCtx, databaseURL)
	cancel()
	if err != nil {
		return err
	}
	defer pool.Close()
	archiveDatabase, err := archive.NewPgArchive(pool, channel)
	if err != nil {
		return err
	}
	objects, err := archive.NewR2ObjectStore(
		os.Getenv("R2_ACCOUNT_ID"), os.Getenv("R2_ARCHIVE_BUCKET"),
		os.Getenv("R2_ACCESS_KEY_ID"), os.Getenv("R2_SECRET_ACCESS_KEY"),
	)
	if err != nil {
		return err
	}
	exporter, err := archive.NewExporter(archiveDatabase, objects, exportAge)
	if err != nil {
		return err
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var lastSuccess atomic.Int64
	var lastFailure atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if lastFailure.Load() || time.Since(time.Unix(0, lastSuccess.Load())) > time.Minute {
			http.Error(w, "archive exporter not ready", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "archive database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serverError := make(chan error, 1)
	go func() { serverError <- server.ListenAndServe() }()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(root, 90*time.Second)
		err := objects.Probe(ctx)
		moved := false
		if err == nil {
			moved, err = exporter.RunOnce(ctx)
		}
		cancel()
		if err != nil {
			lastFailure.Store(true)
			logger.Error("archive export failed", "error", err)
		} else {
			lastSuccess.Store(time.Now().UnixNano())
			lastFailure.Store(false)
			if moved {
				logger.Info("archive segment committed")
			}
		}
		select {
		case <-root.Done():
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(ctx)
		case err := <-serverError:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ticker.C:
		}
	}
}
