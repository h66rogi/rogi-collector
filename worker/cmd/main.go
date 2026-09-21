package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	workerinternal "github.com/h66rogi/rogi-collector/worker/internal"
	"github.com/h66rogi/rogi-collector/worker/internal/connector"
	"github.com/h66rogi/rogi-collector/worker/internal/pipeline"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
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

	// Read configuration from environment.
	workerID := envOrDefault("WORKER_ID", "worker-1")
	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	databaseURL := envOrDefault("DATABASE_URL", "postgres://localhost:5432/chatcollector?sslmode=disable")
	maxConn := envIntOrDefault("MAX_CONNECTIONS", 50)
	maxMsgPerSec := envFloatOrDefault("MAX_MSG_PER_SEC", 5000)
	soopRelayAddr := os.Getenv("SOOP_RELAY_ADDR")
	chzzkDCProxyURL := os.Getenv("CHZZK_DC_PROXY_URL")

	slog.Info("starting worker",
		"workerId", workerID,
		"maxConn", maxConn,
		"maxMsgPerSec", maxMsgPerSec,
		"soopRelayEnabled", soopRelayAddr != "",
		"chzzkProxyEnabled", chzzkDCProxyURL != "",
	)

	var draining atomic.Bool

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	heartbeatCtx, heartbeatCancel := context.WithCancel(rootCtx)
	defer heartbeatCancel()

	opsCtx, opsCancel := context.WithCancel(rootCtx)
	defer opsCancel()

	// Create Redis client.
	rdb := store.NewRedisClient(redisAddr)
	defer rdb.Close()

	// Create PostgreSQL pool.
	pgPool, err := store.NewPgPoolWithRetry(rootCtx, databaseURL)
	if err != nil {
		slog.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()

	redisStore := store.NewRedisStore(rdb)
	pgStore := store.NewPgStore(pgPool)
	productChannel, scopeErr := collection.Channel(os.Getenv("CHANNEL_ALLOWLIST"))
	if scopeErr != nil {
		slog.Error("invalid collection scope", "error", scopeErr)
		os.Exit(1)
	}
	pgStore.SetCollectionChannel(productChannel)

	optOutRefreshInterval := envDurationOrDefault("COLLECTION_OPT_OUT_REFRESH_INTERVAL", 15*time.Second)
	optOutCache := store.NewCollectionOptOutCache(pgStore, optOutRefreshInterval, logger)

	// Register worker in PostgreSQL.
	worker := model.Worker{
		ID:            workerID,
		Status:        model.WorkerStatusAlive,
		MaxCapacity:   maxConn,
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}
	if err := pgStore.RegisterWorker(rootCtx, worker); err != nil {
		slog.Error("failed to register worker", "error", err)
		os.Exit(1)
	}
	slog.Info("worker registered in PostgreSQL")

	// Create publisher and manager.
	metricsRegistry, workerMetrics := workerinternal.NewMetricsRegistry()
	pub := pipeline.NewPublisher(rdb, &pipeline.PublishMetrics{
		PublishedTotal:    workerMetrics.MessagesPublishedTotal,
		DedupSkippedTotal: workerMetrics.MessagesDedupSkippedTotal,
		ErrorsTotal:       workerMetrics.PublishErrorsTotal,
		DurationSeconds:   workerMetrics.PublishDurationSeconds,
	})

	// Configure connector with relay address and metrics now that workerMetrics is ready.
	anonymousTest, authErr := collection.AnonymousTest(os.Getenv("SOOP_AUTH_MODE"))
	if authErr != nil {
		slog.Error("invalid SOOP auth mode")
		os.Exit(1)
	}
	connector.SetConnectorConfig(connector.ConnectorConfig{
		SoopAnonymousTest:    anonymousTest,
		SoopRelayAddr:        soopRelayAddr,
		SoopCookieFile:       os.Getenv("SOOP_COOKIE_FILE"),
		CimeWSURL:            os.Getenv("CIME_WS_URL"),
		SoopChatHostSuffixes: splitAndTrimCSV(os.Getenv("SOOP_CHAT_HOST_SUFFIXES")),
		RelayMetrics: &connector.RelayMetrics{
			AttemptsTotal:   workerMetrics.RelayConnectionAttemptsTotal,
			DurationSeconds: workerMetrics.RelayConnectionDurationSeconds,
		},
		OnMessageDropped: func(platform string) {
			workerMetrics.MessagesDroppedTotal.WithLabelValues(platform).Inc()
		},
		ChzzkDCProxyURL: chzzkDCProxyURL,
	})

	mgr := workerinternal.NewManager(rootCtx, workerID, maxConn, maxMsgPerSec, pub, redisStore, workerMetrics, pgStore)
	mgr.SetCollectionOptOutCache(optOutCache)
	mgr.SetCollectionChannel(productChannel)
	if err := pgStore.EnsureCollection(rootCtx, productChannel); err != nil {
		slog.Error("collector schema not ready")
		os.Exit(1)
	}
	spool, err := pipeline.NewDonationSpool(os.Getenv("DONATION_SPOOL_DIR"), pgStore, 1<<30)
	if err != nil {
		slog.Error("donation spool unavailable", "error", err)
		os.Exit(1)
	}
	defer func() { rootCancel(); _ = spool.Close() }()
	mgr.EnableDonations(pgStore, spool)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				drainCtx, done := context.WithTimeout(rootCtx, 3*time.Second)
				err := spool.Drain(drainCtx)
				_ = pgStore.DispatchDonationOutbox(drainCtx, redisStore.NotifyDonations)
				done()
				if err != nil {
					_ = pgStore.SetCollectionState(rootCtx, productChannel, "storage_delayed")
				}
			}
		}
	}()

	var chatWriters []*store.BatchWriter[store.ChatMessageRow]

	// Optional: ClickHouse chat message storage
	if os.Getenv("CHAT_CH_ENABLED") == "true" {
		chAddr := os.Getenv("CLICKHOUSE_ADDR")
		if chAddr != "" {
			chStore, chErr := store.NewLazyClickHouseStore(
				chAddr,
				envOrDefault("CLICKHOUSE_DATABASE", "default"),
				envOrDefault("CLICKHOUSE_USER", "collector"),
				os.Getenv("CLICKHOUSE_PASSWORD"),
			)
			if chErr != nil {
				slog.Warn("ClickHouse configuration invalid, chat storage disabled", "error", chErr)
			} else {
				chatWriter := store.NewBatchWriter[store.ChatMessageRow](
					func(rows []store.ChatMessageRow) error {
						return chStore.BatchInsertChatMessages(context.Background(), rows)
					},
					5000,
					10*time.Second,
					func(r store.ChatMessageRow) time.Time { return r.Timestamp },
					logger,
					&store.BatchWriterMetrics{
						FlushTotal:   workerMetrics.ChatCHFlushTotal,
						BatchSize:    workerMetrics.ChatCHBatchSize,
						DroppedTotal: workerMetrics.ChatCHDroppedTotal,
						RetryTotal:   workerMetrics.ChatCHRetryTotal,
						Pending:      workerMetrics.ChatCHPending,
					},
				)
				mgr.SetChatWriter(chatWriter)
				chatWriters = append(chatWriters, chatWriter)
				slog.Info("ClickHouse chat storage enabled with retry")
			}
		}
	}

	// Optional secondary ClickHouse buffer. This writer never replaces the
	// authoritative writer and uses a bounded retry queue so a storage or
	// network outage cannot block collection or exhaust worker memory.
	if os.Getenv("CHAT_CH_BUFFER_ENABLED") == "true" {
		bufferAddr := os.Getenv("BUFFER_CLICKHOUSE_ADDR")
		if bufferAddr != "" {
			bufferStore, bufferErr := store.NewLazyClickHouseStore(
				bufferAddr,
				envOrDefault("BUFFER_CLICKHOUSE_DATABASE", "default"),
				envOrDefault("BUFFER_CLICKHOUSE_USER", "collector"),
				os.Getenv("BUFFER_CLICKHOUSE_PASSWORD"),
			)
			if bufferErr != nil {
				slog.Warn("secondary ClickHouse buffer configuration invalid; buffer disabled", "error", bufferErr)
			} else {
				bufferWriter := store.NewBoundedBatchWriter[store.ChatMessageRow](
					func(rows []store.ChatMessageRow) error {
						return bufferStore.BatchInsertChatMessages(context.Background(), rows)
					},
					5000,
					100000,
					10*time.Second,
					func(r store.ChatMessageRow) time.Time { return r.Timestamp },
					logger,
					&store.BatchWriterMetrics{
						FlushTotal:   workerMetrics.ChatCHBufferFlushTotal,
						BatchSize:    workerMetrics.ChatCHBufferBatchSize,
						DroppedTotal: workerMetrics.ChatCHBufferDroppedTotal,
						RetryTotal:   workerMetrics.ChatCHBufferRetryTotal,
						Pending:      workerMetrics.ChatCHBufferPending,
					},
				)
				mgr.SetChatBufferWriter(bufferWriter)
				chatWriters = append(chatWriters, bufferWriter)
				slog.Info("secondary ClickHouse buffer enabled", "max_pending", 100000)
			}
		}
	}
	defer func() {
		var closeWG sync.WaitGroup
		for _, writer := range chatWriters {
			closeWG.Add(1)
			go func(w *store.BatchWriter[store.ChatMessageRow]) {
				defer closeWG.Done()
				w.Close()
			}(writer)
		}
		closeWG.Wait()
	}()

	runtimeServer := workerinternal.NewServer(envOrDefault("PROBE_ADDR", "127.0.0.1:8080"), logger, map[string]http.Handler{
		"/metrics": promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}),
	})
	go func() {
		if err := runtimeServer.Start(); err != nil {
			slog.Error("runtime server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	// Start heartbeat loop (every 5 seconds) — uses heartbeatCtx so it
	// continues during drain, keeping the coordinator informed.
	go mgr.StartHeartbeatLoop(heartbeatCtx, 5*time.Second)
	go optOutCache.Start(opsCtx)

	// Ensure PG status stays alive (recovers from coordinator's MarkDead race).
	// Uses opsCtx and the draining flag so it stops overwriting status during drain.
	go ensureAlive(opsCtx, pgStore, workerID, &draining)

	// Periodically reconcile assigned channels from PostgreSQL to recover from Pub/Sub loss.
	go reconcileAssignedChannels(opsCtx, pgStore, mgr, workerID, 3*time.Second)

	// Subscribe to worker commands via Redis Pub/Sub.
	go subscribeCommands(opsCtx, redisStore, pgStore, mgr, workerID)
	runtimeServer.SetReady(true)
	workerMetrics.Ready.Set(1)

	// Graceful shutdown with drain protocol.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	slog.Info("received signal, starting drain", "signal", sig)

	// 1. Set draining flag (prevents ensureAlive from overwriting status).
	draining.Store(true)
	mgr.SetDraining()

	// 2. Mark the readiness probe unavailable before draining.
	runtimeServer.SetReady(false)
	workerMetrics.Ready.Set(0)

	// 3. Mark draining in PG (blocks new assignments immediately).
	if err := pgStore.UpdateWorkerStatus(context.Background(), workerID, model.WorkerStatusDraining); err != nil {
		slog.Error("failed to mark worker draining", "error", err)
	}

	// 4. Notify coordinator via Redis.
	if err := redisStore.PublishCoordEvent(context.Background(), store.CoordEvent{
		Type:     store.CoordEventDrainStarted,
		WorkerID: workerID,
	}); err != nil {
		slog.Warn("failed to publish drain-started event", "error", err)
	}

	// 5. Stop reconcile + command subscriber (heartbeat continues via heartbeatCtx).
	opsCancel()

	mgr.DisconnectAll()

	// 6. Wait for handoffs.
	drainTimeout := 135 * time.Second // Original handoff limit; the host/container stop deadline may be shorter.
	if err := waitForHandoffs(context.Background(), pgStore, mgr, workerID, drainTimeout); err != nil {
		slog.Warn("drain incomplete, force disconnecting", "error", err)
		mgr.DisconnectAll()
	}

	// 7. Shutdown HTTP server.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := runtimeServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("runtime server shutdown failed", "error", err)
	}

	// 8. Cleanup.
	if err := pgStore.UnassignWorkerChannels(context.Background(), workerID); err != nil {
		slog.Error("failed to unassign channels", "error", err)
	}
	if err := pgStore.UpdateWorkerStatus(context.Background(), workerID, model.WorkerStatusDead); err != nil {
		slog.Error("failed to mark worker dead", "error", err)
	}

	// 9. Stop heartbeat + root context.
	heartbeatCancel()
	rootCancel()

	slog.Info("worker shutdown complete")
}

// commandDispatcherMaxConcurrency limits how many connect commands run in
// parallel. This prevents thundering-herd when the coordinator publishes
// a large batch of connect commands.
const commandDispatcherMaxConcurrency = 50

// commandHandlerFunc is the signature for the function that processes a single command.
type commandHandlerFunc func(ctx context.Context, cs store.ChannelStore, mgr *workerinternal.Manager, workerID string, cmd store.WorkerCommand)

// commandDispatcher decouples the PubSub reader from blocking connect
// handlers. Connect commands are sent to an internal queue and drained by
// a fixed-size worker pool. Enqueueing a connect does not wait for the network.
// Other commands run inline and may wait for an in-flight handler on the same
// channel. The per-channel lock prevents concurrent handlers, but does not
// guarantee FIFO ordering between queued connects and inline commands.
type commandDispatcher struct {
	connectQueue chan store.WorkerCommand
	sem          chan struct{}

	// mu guards channelLocks.
	mu           sync.Mutex
	channelLocks map[string]*sync.Mutex
}

func newCommandDispatcher(maxConcurrency int) *commandDispatcher {
	return &commandDispatcher{
		connectQueue: make(chan store.WorkerCommand, 2000),
		sem:          make(chan struct{}, maxConcurrency),
		channelLocks: make(map[string]*sync.Mutex),
	}
}

// channelMu returns a per-channel mutex, creating one if needed.
func (d *commandDispatcher) channelMu(platform, channelID string) *sync.Mutex {
	key := string(platform) + ":" + channelID
	d.mu.Lock()
	defer d.mu.Unlock()
	if mu, ok := d.channelLocks[key]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	d.channelLocks[key] = mu
	return mu
}

// dispatch enqueues connects without waiting, dropping commands when the queue
// is full. Other commands run inline and may block on the channel lock or handler.
func (d *commandDispatcher) dispatch(
	ctx context.Context,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
	cmd store.WorkerCommand,
	handler commandHandlerFunc,
) {
	if cmd.Type == store.WorkerCommandConnect {
		select {
		case d.connectQueue <- cmd:
		default:
			slog.Warn("connect queue full, dropping command",
				"platform", cmd.Platform, "channel", cmd.ChannelID)
		}
		return
	}
	// Disconnect and reconnect commands run inline under the channel lock so
	// they are serialised with any in-flight connect for the same channel.
	chMu := d.channelMu(string(cmd.Platform), cmd.ChannelID)
	chMu.Lock()
	handler(ctx, channelStore, mgr, workerID, cmd)
	chMu.Unlock()
}

// drainConnectQueue processes connect commands from the queue with bounded
// concurrency. Must be run as a goroutine.
func (d *commandDispatcher) drainConnectQueue(
	ctx context.Context,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
	handler commandHandlerFunc,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd, ok := <-d.connectQueue:
			if !ok {
				return
			}
			select {
			case d.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(cmd store.WorkerCommand) {
				defer func() { <-d.sem }()
				chMu := d.channelMu(string(cmd.Platform), cmd.ChannelID)
				chMu.Lock()
				handler(ctx, channelStore, mgr, workerID, cmd)
				chMu.Unlock()
			}(cmd)
		}
	}
}

// subscribeCommands listens for commands from the coordinator and acts on them.
// It uses a larger Pub/Sub buffer (1000) and dispatches connect commands
// via an internal queue to avoid blocking the reader.
func subscribeCommands(
	ctx context.Context,
	redisStore *store.RedisStore,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
) {
	sub := redisStore.SubscribeWorkerCommands(ctx, workerID)
	defer sub.Close()

	ch := sub.Channel(redis.WithChannelSize(1000))
	dispatcher := newCommandDispatcher(commandDispatcherMaxConcurrency)
	go dispatcher.drainConnectQueue(ctx, channelStore, mgr, workerID, handleCommand)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var cmd store.WorkerCommand
			if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
				slog.Error("invalid worker command", "error", err)
				continue
			}
			dispatcher.dispatch(ctx, channelStore, mgr, workerID, cmd, handleCommand)
		}
	}
}

func handleCommand(
	ctx context.Context,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
	cmd store.WorkerCommand,
) {
	switch cmd.Type {
	case store.WorkerCommandConnect:
		slog.Info("received connect command", "platform", cmd.Platform, "channel", cmd.ChannelID)
		if err := connectAssignedChannel(ctx, channelStore, mgr, workerID, cmd.Platform, cmd.ChannelID); err != nil {
			slog.Error("connect failed", "platform", cmd.Platform, "channel", cmd.ChannelID, "error", err)
		}

	case store.WorkerCommandDisconnect:
		slog.Info("received disconnect command", "platform", cmd.Platform, "channel", cmd.ChannelID)
		if err := mgr.Disconnect(cmd.Platform, cmd.ChannelID); err != nil {
			slog.Error("disconnect failed", "platform", cmd.Platform, "channel", cmd.ChannelID, "error", err)
		}

	case store.WorkerCommandReconnect:
		slog.Info("received reconnect command", "platform", cmd.Platform, "channel", cmd.ChannelID)
		if err := mgr.Disconnect(cmd.Platform, cmd.ChannelID); err != nil {
			slog.Warn("reconnect: disconnect step failed (continuing)",
				"platform", cmd.Platform, "channel", cmd.ChannelID, "error", err)
		}
		if err := connectAssignedChannel(ctx, channelStore, mgr, workerID, cmd.Platform, cmd.ChannelID); err != nil {
			slog.Error("reconnect: connect step failed",
				"platform", cmd.Platform, "channel", cmd.ChannelID, "error", err)
		}

	default:
		slog.Warn("unknown command type", "type", cmd.Type)
	}
}

// reconcileAssignedChannels spawns a per-platform reconciliation goroutine.
// Each platform runs its own independent ticker loop so a slow/failing
// platform never blocks others.
func reconcileAssignedChannels(
	ctx context.Context,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
	interval time.Duration,
) {
	platforms := []model.Platform{model.PlatformChzzk, model.PlatformSoop, model.PlatformCime}

	var wg sync.WaitGroup
	for _, p := range platforms {
		wg.Add(1)
		go func(platform model.Platform) {
			defer wg.Done()
			reconcilePlatformLoop(ctx, channelStore, mgr, workerID, platform, interval)
		}(p)
	}
	wg.Wait()
}

func reconcilePlatformLoop(
	ctx context.Context,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
	platform model.Platform,
	interval time.Duration,
) {
	run := func() {
		channels, err := channelStore.ListWorkerChannels(ctx, workerID)
		if err != nil {
			slog.Error("reconcile: list channels failed", "platform", platform, "error", err)
			return
		}
		// Filter to this platform only.
		var platChannels []model.LiveChannel
		for _, ch := range channels {
			if ch.Platform == platform {
				platChannels = append(platChannels, ch)
			}
		}
		if err := mgr.ReconcilePlatform(ctx, platform, platChannels); err != nil {
			slog.Error("reconcile failed", "platform", platform, "error", err)
		}
	}

	run()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func connectAssignedChannel(
	ctx context.Context,
	channelStore store.ChannelStore,
	mgr *workerinternal.Manager,
	workerID string,
	platform model.Platform,
	channelID string,
) error {
	ch, err := channelStore.GetChannel(ctx, platform, channelID)
	if err != nil {
		return err
	}
	if ch.WorkerID == nil || *ch.WorkerID != workerID {
		slog.Warn("skipping connect for unassigned channel",
			"platform", platform,
			"channel", channelID,
			"workerId", workerID)
		return nil
	}
	return mgr.Connect(ctx, *ch)
}

// ensureAlive periodically re-asserts the worker's PG status as alive.
// This recovers from the race where the coordinator marks a freshly started
// worker as dead before its first Redis heartbeat arrives.
// When the draining flag is set, the tick is skipped to avoid overwriting
// the "draining" status in PostgreSQL.
func ensureAlive(ctx context.Context, pgStore *store.PgStore, workerID string, draining *atomic.Bool) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if draining.Load() {
				continue
			}
			if err := pgStore.UpdateWorkerStatus(ctx, workerID, model.WorkerStatusAlive); err != nil {
				slog.Warn("ensureAlive failed", "error", err)
			}
		}
	}
}

// waitForHandoffs polls for acknowledged handoffs from the coordinator. For
// each acked channel, the old worker disconnects it and clears the handoff
// record. Returns nil once all connections have been drained, or an error
// if the timeout expires with connections remaining.
func waitForHandoffs(ctx context.Context, pgStore *store.PgStore, mgr *workerinternal.Manager,
	workerID string, timeout time.Duration) error {

	deadline := time.After(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			acked, err := pgStore.ListAckedHandoffs(ctx, workerID)
			if err != nil {
				slog.Warn("waitForHandoffs: list acked failed", "error", err)
				continue
			}
			for _, ch := range acked {
				if err := mgr.Disconnect(ch.Platform, ch.ChannelID); err != nil {
					slog.Warn("waitForHandoffs: disconnect failed",
						"platform", ch.Platform, "channel", ch.ChannelID, "error", err)
				}
				if err := pgStore.ClearHandoff(ctx, ch.ID); err != nil {
					slog.Warn("waitForHandoffs: clear handoff failed",
						"platform", ch.Platform, "channel", ch.ChannelID, "error", err)
				}
			}
			remaining := mgr.ActiveCount()
			if remaining == 0 {
				slog.Info("drain complete: all channels handed off")
				return nil
			}
			if len(acked) > 0 {
				slog.Info("drain progress", "remaining", remaining, "acked_this_cycle", len(acked))
			}
		case <-deadline:
			remaining := mgr.ActiveCount()
			return fmt.Errorf("drain timeout: %d channels remaining", remaining)
		}
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitAndTrimCSV(raw string) []string {
	var values []string
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func envIntOrDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envFloatOrDefault(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
