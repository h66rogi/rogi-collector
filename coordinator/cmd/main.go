package main

import (
	"context"
	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/h66rogi/rogi-collector/coordinator/internal"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if err := runtimeenv.Load(); err != nil {
		slog.Error("role configuration failed", "error", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Read environment variables
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "coordinator-1"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	// Create Redis client and store
	redisClient := store.NewRedisClient(redisAddr)
	redisStore := store.NewRedisStore(redisClient)

	// Create PG pool and store
	ctx := context.Background()
	pgPool, err := store.NewPgPoolWithRetry(ctx, databaseURL)
	if err != nil {
		logger.Error("failed to create PG pool", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	pgStore := store.NewPgStore(pgPool)
	productChannel, scopeErr := collection.Channel(os.Getenv("CHANNEL_ALLOWLIST"))
	if scopeErr != nil {
		slog.Error("invalid collection scope", "error", scopeErr)
		os.Exit(1)
	}
	pgStore.SetCollectionChannel(productChannel)
	if err := pgStore.EnsureCollection(ctx, productChannel); err != nil {
		slog.Error("collector schema not ready")
		os.Exit(1)
	}

	metricsRegistry, metrics := internal.NewMetricsRegistry()
	probeAddr := os.Getenv("PROBE_ADDR")
	if probeAddr == "" {
		probeAddr = "127.0.0.1:8080"
	}
	probeServer := internal.NewProbeServer(probeAddr, logger, map[string]http.Handler{
		"/metrics":   promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}),
		"/api/stats": internal.NewStatsHandler(pgStore),
	})
	go func() {
		if err := probeServer.Start(); err != nil {
			logger.Error("probe server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	// Create coordinator config
	var loadThreshold float64
	if v := os.Getenv("LOAD_THRESHOLD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsNaN(f) && f > 0 && f <= 1.0 {
			loadThreshold = f
		} else {
			slog.Warn("invalid LOAD_THRESHOLD, using default 0.95", "value", v)
		}
	}

	cfg := internal.Config{
		InstanceID:    instanceID,
		HealthTimeout: 30 * time.Second,
		AssignBatch:   500,
		LoadThreshold: loadThreshold,
	}

	// Create and run coordinator
	coord := internal.NewCoordinator(cfg, pgStore, pgStore, redisStore, metrics, logger)
	probeServer.SetReady(true)
	metrics.Ready.Set(1)

	// Handle SIGTERM/SIGINT for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		probeServer.SetReady(false)
		metrics.Ready.Set(0)
		metrics.ResetLeaderGauges()
		cancel()
	}()

	coord.Run(ctx)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := probeServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("probe server shutdown failed", "error", err)
	}
	logger.Info("coordinator stopped")
}

func grpcReflectionEnabled() bool {
	enabled, err := strconv.ParseBool(os.Getenv("GRPC_ENABLE_REFLECTION"))
	return err == nil && enabled
}
