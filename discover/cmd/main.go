package main

import (
	"context"
	"crypto/subtle"
	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/h66rogi/rogi-collector/discover/internal"
	"github.com/h66rogi/rogi-collector/discover/internal/discovery"
	"github.com/h66rogi/rogi-collector/discover/internal/history"
	"github.com/h66rogi/rogi-collector/shared/model"
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

	metricsRegistry, metrics := internal.NewMetricsRegistry()
	discoverAdminAPIKey := os.Getenv("DISCOVER_ADMIN_API_KEY")

	// disc is set later; the closure captures the pointer.
	var disc *internal.Discover
	triggerHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		provided := r.Header.Get("X-Admin-Api-Key")
		if discoverAdminAPIKey == "" {
			http.Error(w, "admin api key not configured", http.StatusServiceUnavailable)
			return
		}
		if !constantTimeKeyMatch(provided, discoverAdminAPIKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		body := http.MaxBytesReader(w, r.Body, 0)
		if _, err := io.Copy(io.Discard, body); err != nil {
			http.Error(w, "request body not allowed", http.StatusRequestEntityTooLarge)
			return
		}
		if disc == nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		disc.TriggerDiscovery()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"triggered"}`))
		logger.Info("discovery triggered via HTTP")
	})

	probeServer := internal.NewProbeServer(envOrDefault("PROBE_ADDR", "127.0.0.1:8080"), logger, map[string]http.Handler{
		"/metrics":               promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}),
		"/trigger":               triggerHandler,
		"/diagnostics/broadcast": internal.NewBroadcastDiagnostics(os.Getenv("SOOP_DIAGNOSTIC_TOKEN"), soopauth.NewHTTPClient(os.Getenv("SOOP_COOKIE_FILE"))),
	})
	go func() {
		if err := probeServer.Start(); err != nil {
			logger.Error("probe server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "discover-1"
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

	redisClient := store.NewRedisClient(redisAddr)
	redisStore := store.NewRedisStore(redisClient)

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

	optOutRefreshInterval := durationFromEnv(logger, "COLLECTION_OPT_OUT_REFRESH_INTERVAL", 15*time.Second)
	optOutCache := store.NewCollectionOptOutCache(pgStore, optOutRefreshInterval, logger)

	// History tracking (optional)
	var emitter *history.Emitter
	var chWriter *store.BatchWriter[store.ViewerCountRow]
	var chNeedsReconnect bool
	var chAddr, chDB, chUser, chPassword string
	var historyMetrics *history.Metrics
	historyEnabled := os.Getenv("HISTORY_ENABLED") == "true"
	if !historyEnabled {
		// Not applicable — suppress DiscoverClickHouseUnavailable alert
		metrics.ClickHouseAvailable.Set(1)
	}
	if historyEnabled {
		chAddr = os.Getenv("CLICKHOUSE_ADDR")
		if chAddr == "" {
			chAddr = "localhost:9000"
		}
		chDB = os.Getenv("CLICKHOUSE_DATABASE")
		if chDB == "" {
			chDB = "default"
		}
		chUser = os.Getenv("CLICKHOUSE_USER")
		if chUser == "" {
			chUser = "collector"
		}
		chPassword = os.Getenv("CLICKHOUSE_PASSWORD")

		historyMetrics = history.NewMetrics(metricsRegistry)

		chStore, err := store.NewClickHouseStore(chAddr, chDB, chUser, chPassword)
		if err != nil {
			logger.Warn("ClickHouse unavailable, viewer count tracking disabled", "error", err)
			metrics.ClickHouseAvailable.Set(0)
			chNeedsReconnect = true
		}

		if chStore != nil {
			defer chStore.Close()
			chWriter = history.NewBatchWriter(chStore, 5000, 10*time.Second, logger, historyMetrics)
			metrics.ClickHouseAvailable.Set(1)
		}

		pgHistoryStore := store.NewPgHistoryStore(pgPool)
		emitter = history.NewEmitter(pgHistoryStore, chWriter, instanceID, historyMetrics)
		if chStore != nil {
			logger.Info("history tracking enabled")
		} else {
			logger.Info("history tracking enabled (PG only, ClickHouse unavailable)")
		}
	}

	// Preserve the existing discovery/leader loop, but only probe explicitly
	// approved SOOP IDs. The original all-platform enumeration is not a default.
	if strings.EqualFold(os.Getenv("BYPASS_ALLOWLIST"), "true") {
		logger.Error("BYPASS_ALLOWLIST is not supported by rogi-collector")
		os.Exit(1)
	}
	allowlist := parseAllowlist(os.Getenv("CHANNEL_ALLOWLIST"), logger)
	anonymousTest, err := collection.AnonymousTest(os.Getenv("SOOP_AUTH_MODE"))
	if err != nil {
		logger.Error("invalid SOOP auth mode")
		os.Exit(1)
	}
	soopDiscovery := discovery.NewSoopDiscovery(metrics, os.Getenv("SOOP_COOKIE_FILE"))
	if anonymousTest {
		soopDiscovery = discovery.NewSoopDiscovery(metrics)
	}
	registered := discovery.NewRegisteredDiscovery(soopDiscovery, allowlist[model.PlatformSoop])
	registered.Enabled = pgStore.CollectionEnabled
	registered.Observed = func(ctx context.Context, id string, live bool, err error) {
		if err != nil {
			_ = pgStore.SetCollectionState(ctx, id, collection.FailureState(err))
		} else if !live {
			_ = pgStore.SetCollectionState(ctx, id, "waiting")
		}
	}
	discoveries := []discovery.PlatformDiscovery{registered}

	chzzkCycleTimeout := durationFromEnv(logger, "CHZZK_CYCLE_TIMEOUT", 90*time.Second)
	discoverInterval := durationFromEnv(logger, "DISCOVER_INTERVAL", 0)

	discoverIntervals := map[model.Platform]time.Duration{}
	if discoverInterval > 0 {
		discoverIntervals[model.PlatformChzzk] = discoverInterval
		discoverIntervals[model.PlatformSoop] = discoverInterval
		discoverIntervals[model.PlatformCime] = discoverInterval
		logger.Info("discovery interval override", "interval", discoverInterval)
	}

	cfg := internal.Config{
		InstanceID:     instanceID,
		LeaderElection: leaderElectionConfigFromEnv(logger),
		Allowlist:      allowlist,
		CycleTimeouts: map[model.Platform]time.Duration{
			model.PlatformChzzk: chzzkCycleTimeout,
		},
		DiscoverIntervals: discoverIntervals,
	}

	disc = internal.NewDiscover(cfg, pgStore, redisStore, discoveries, metrics, logger)
	disc.SetCollectionOptOutCache(optOutCache)

	if emitter != nil {
		disc.SetEmitter(emitter, pgStore, chWriter)
	}

	if n := internal.NewBackNotifier(os.Getenv("END_CALLBACK_URL"), os.Getenv("END_CALLBACK_API_KEY"), logger); n != nil {
		disc.SetNotifier(n)
		logger.Info("channel-end callback enabled")
	}

	if chWriter != nil {
		defer chWriter.Close()
	}

	probeServer.SetReady(true)
	metrics.Ready.Set(1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go optOutCache.Start(ctx)

	// ClickHouse reconnection: retry every 60s until connected.
	// A buffered channel transfers ownership of reconnected resources to the
	// shutdown path without racing on a shared function variable.
	reconnectedCleanup := make(chan func(), 1)
	if chNeedsReconnect {
		go func() {
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					newStore, err := store.NewClickHouseStore(chAddr, chDB, chUser, chPassword)
					if err != nil {
						logger.Warn("ClickHouse reconnect failed", "error", err)
						continue
					}
					logger.Info("ClickHouse reconnected")
					newWriter := history.NewBatchWriter(newStore, 5000, 10*time.Second, logger, historyMetrics)
					disc.SetCHWriter(newWriter)
					metrics.ClickHouseAvailable.Set(1)
					cleanup := func() {
						newWriter.Close()
						if err := newStore.Close(); err != nil {
							logger.Warn("close reconnected ClickHouse store failed", "error", err)
						}
					}
					select {
					case reconnectedCleanup <- cleanup:
					case <-ctx.Done():
						cleanup()
					}
					return
				}
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		probeServer.SetReady(false)
		metrics.Ready.Set(0)
		cancel()
	}()

	go disc.SubscribeAdminCommands(ctx)
	disc.Run(ctx)
	select {
	case cleanup := <-reconnectedCleanup:
		cleanup()
	default:
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := probeServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("probe server shutdown failed", "error", err)
	}
	logger.Info("discover stopped")
}

func leaderElectionConfigFromEnv(logger *slog.Logger) internal.LeaderElectionConfig {
	cfg := internal.DefaultLeaderElectionConfig()

	cfg.TTL = durationFromEnv(logger, "DISCOVER_LEADER_TTL", cfg.TTL)
	cfg.RenewInterval = durationFromEnv(logger, "DISCOVER_LEADER_RENEW_INTERVAL", cfg.RenewInterval)
	cfg.AcquireInterval = durationFromEnv(logger, "DISCOVER_LEADER_ACQUIRE_INTERVAL", cfg.AcquireInterval)
	cfg.RenewJitter = durationFromEnv(logger, "DISCOVER_LEADER_RENEW_JITTER", cfg.RenewJitter)
	cfg.AcquireJitter = durationFromEnv(logger, "DISCOVER_LEADER_ACQUIRE_JITTER", cfg.AcquireJitter)
	cfg.RetryInterval = durationFromEnv(logger, "DISCOVER_LEADER_RETRY_INTERVAL", cfg.RetryInterval)
	cfg.OperationTimeout = durationFromEnv(logger, "DISCOVER_LEADER_OPERATION_TIMEOUT", cfg.OperationTimeout)
	cfg.ExpiryGuard = durationFromEnv(logger, "DISCOVER_LEADER_EXPIRY_GUARD", cfg.ExpiryGuard)
	cfg.MaxConsecutiveRenewErrors = intFromEnv(logger, "DISCOVER_LEADER_MAX_CONSECUTIVE_RENEW_ERRORS", cfg.MaxConsecutiveRenewErrors)

	return cfg
}

func durationFromEnv(logger *slog.Logger, key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		logger.Warn("invalid duration env, using default", "key", key, "value", raw, "default", fallback, "error", err)
		return fallback
	}
	return value
}

func floatFromEnv(logger *slog.Logger, key string, fallback float64) float64 {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		logger.Warn("invalid float env, using default", "key", key, "value", raw, "default", fallback, "error", err)
		return fallback
	}
	return value
}

func intFromEnv(logger *slog.Logger, key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		logger.Warn("invalid int env, using default", "key", key, "value", raw, "default", fallback, "error", err)
		return fallback
	}
	return value
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func constantTimeKeyMatch(provided, expected string) bool {
	return expected != "" && len(provided) == len(expected) && subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}

// parseAllowlist retains the original platform:channel parser. The product
// entrypoint first validates a single SOOP channel through collection.Channel.
// An empty result collects nothing; it never enables platform-wide discovery.
func parseAllowlist(raw string, logger *slog.Logger) map[model.Platform]map[string]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[model.Platform]map[string]bool{}
	}

	result := make(map[model.Platform]map[string]bool)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			logger.Warn("CHANNEL_ALLOWLIST: invalid entry, skipping")
			continue
		}
		platform := model.Platform(parts[0])
		if !platform.IsValid() {
			logger.Warn("CHANNEL_ALLOWLIST: unknown platform, skipping", "platform", parts[0])
			continue
		}
		if result[platform] == nil {
			result[platform] = make(map[string]bool)
		}
		result[platform][parts[1]] = true
	}

	if len(result) == 0 {
		return map[model.Platform]map[string]bool{}
	}

	for p, ids := range result {
		logger.Info("channel allowlist loaded", "platform", p, "channels", len(ids))
	}
	return result
}
