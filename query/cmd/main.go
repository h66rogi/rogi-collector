package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/grpcauth"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/h66rogi/rogi-collector/query/internal"
	"github.com/h66rogi/rogi-collector/shared/store"
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
	ctx, stopMaintenance := context.WithCancel(context.Background())
	defer stopMaintenance()
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

	tlsConfig, err := grpcauth.ServerTLS(os.Getenv("COLLECTOR_TLS_CERT_FILE"), os.Getenv("COLLECTOR_TLS_KEY_FILE"), os.Getenv("COLLECTOR_CLIENT_CA_FILE"))
	if err != nil {
		logger.Error("collector TLS configuration invalid", "error", err)
		os.Exit(1)
	}
	consumer := os.Getenv("COLLECTOR_CONSUMER_ID")
	if consumer == "" {
		consumer = "rogimarble"
	}
	grpcSrv, err := internal.NewProductServer(pgStore, redisStore, 7443, tlsConfig, internal.ProductAccess{Consumer: consumer, Channel: productChannel, ReadURI: os.Getenv("COLLECTOR_READ_CERT_URI"), ManageURI: os.Getenv("COLLECTOR_MANAGE_CERT_URI"), RecoveryURI: os.Getenv("COLLECTOR_RECOVERY_CERT_URI"), BroadcastCheck: internal.DiscoverBroadcastChecker(os.Getenv("SOOP_DIAGNOSTIC_TOKEN"))}, logger)
	if err != nil {
		logger.Error("collector access configuration invalid")
		os.Exit(1)
	}

	go func() {
		if err := grpcSrv.Start(); err != nil {
			logger.Error("gRPC server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if productChannel != "" {
					maintenanceCtx, done := context.WithTimeout(ctx, 20*time.Second)
					if err := pgStore.MaintainCollection(maintenanceCtx, productChannel); err != nil {
						logger.Warn("collector retention delayed")
					}
					_ = redisStore.TrimProductChat(maintenanceCtx, productChannel)
					done()
				}
			}
		}
	}()
	// Health probe HTTP server. Loopback is the secure default; deployments that
	// need remote probes must opt in through PROBE_ADDR and restrict access.
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_ready"})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})

	probeAddr := os.Getenv("PROBE_ADDR")
	if probeAddr == "" {
		probeAddr = "127.0.0.1:8080"
	}
	httpServer := &http.Server{
		Addr:              probeAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		logger.Info("probe server listening")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("probe server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	ready.Store(true)
	logger.Info("query service ready")

	// Handle SIGTERM/SIGINT for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigCh
	logger.Info("received signal, shutting down", "signal", sig)
	ready.Store(false)

	stopMaintenance()
	grpcSrv.Stop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("probe server shutdown failed", "error", err)
	}
	logger.Info("query service stopped")
}

func adminAPIKey() string {
	if v := os.Getenv("CHAT_ADMIN_API_KEY"); v != "" {
		return v
	}
	return os.Getenv("INTERNAL_API_KEY")
}

func grpcReflectionEnabled() bool {
	enabled, err := strconv.ParseBool(os.Getenv("GRPC_ENABLE_REFLECTION"))
	return err == nil && enabled
}
