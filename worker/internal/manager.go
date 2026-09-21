package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/h66rogi/rogi-collector/worker/internal/connector"
	"github.com/h66rogi/rogi-collector/worker/internal/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

// classifyConnectorError maps a connector error to a disconnect reason label
// used by DisconnectsTotal and ZombieReapedTotal metrics. Matches on error
// substrings because connectors return wrapped errors without typed sentinels.
func classifyConnectorError(err error) string {
	if err == nil {
		return "ws_closed"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "read timeout"):
		return "zombie_reaped"
	case strings.Contains(msg, "ws ping failed"), strings.Contains(msg, "ping write failed"):
		return "liveness_ping_failed"
	case strings.Contains(msg, "handshake"), strings.Contains(msg, "login rejected"), strings.Contains(msg, "join rejected"):
		return "handshake_failed"
	default:
		return "ws_closed"
	}
}

// isZombieReason reports whether a classified reason represents a silent-stall
// that was forcibly cleaned up (vs. a normal close/graceful disconnect).
func isZombieReason(reason string) bool {
	return reason == "zombie_reaped" || reason == "liveness_ping_failed"
}

const (
	// ConnectTimeout is the maximum time allowed for a single Connect call
	// (platform API + WebSocket dial). Must be short so failing connections
	// release their concurrency slot quickly.
	ConnectTimeout = 10 * time.Second

	// PerPlatformConcurrency is the max concurrent Connect calls PER PLATFORM
	// during reconciliation. Each platform gets its own pool so a failing
	// platform (e.g. SOOP timeout) cannot starve healthy ones (e.g. Chzzk).
	PerPlatformConcurrency = 30
	// BurstPerPlatformConcurrency is used when a large backlog (>100 missing
	// channels) is detected, allowing faster catch-up.
	BurstPerPlatformConcurrency = 60

	// ReconcileMaxBatch is the maximum number of new connections attempted per
	// reconciliation cycle per platform.
	ReconcileMaxBatch = 300
	// BurstReconcileMaxBatch allows a larger batch during burst catch-up.
	BurstReconcileMaxBatch = 1000

	// CoordEventPublishTimeout bounds coordinator notifications that must outlive
	// a connection context teardown.
	CoordEventPublishTimeout = 5 * time.Second
)

// connectorFactory is the function used to create platform connectors.
// It is a package variable so tests can override it.
var connectorFactory = connector.NewConnector

// connKey builds a unique key for a connection: "platform:channelId".
func connKey(platform model.Platform, channelID string) string {
	return fmt.Sprintf("%s:%s", platform, channelID)
}

// backoffEntry tracks persistent connect failures for a channel.
type backoffEntry struct {
	firstFail    time.Time
	lastFail     time.Time
	failures     int
	unassignSent bool
}

// activeConn tracks a single platform connection and its cancellation.
type activeConn struct {
	connector         connector.PlatformConnector
	cancel            context.CancelFunc
	channel           model.LiveChannel
	connectedAt       time.Time
	releaseCollection func()
}

// handoffAcker is the subset of store.PgStore used by the Manager to
// acknowledge handoff completions after a successful Connect.
type handoffAcker interface {
	AckHandoff(ctx context.Context, platform model.Platform, channelID string) error
	ClearHandoff(ctx context.Context, channelID int64) error
}

// Manager manages all active platform connections for this worker.
type Manager struct {
	collectionStore   *store.PgStore
	donationSpool     *pipeline.DonationSpool
	collectionChannel *string
	connecting        map[string]bool
	workerID          string
	maxConn           int
	maxMsgPerSec      float64
	publisher         *pipeline.Publisher
	redisStore        *store.RedisStore
	pgStore           handoffAcker
	metrics           *WorkerMetrics
	rateCounter       *RateCounter
	rateLimiters      map[model.Platform]*rate.Limiter
	stopSnap          chan struct{} // closed on shutdown to stop the snapshotter

	// connCtx is a long-lived context used as the parent for all per-connection
	// contexts. It is independent of the caller's reconcile/command context so
	// that cancelling opsCtx during drain does NOT kill existing connections.
	connCtx    context.Context
	connCancel context.CancelFunc

	// draining indicates the worker is shutting down. When true, ALL channels
	// use PublishWithDedup so the new worker can deduplicate overlapping messages.
	draining atomic.Bool

	chatWriter       *store.BatchWriter[store.ChatMessageRow]
	chatBufferWriter *store.BatchWriter[store.ChatMessageRow]
	optOuts          *store.CollectionOptOutCache

	mu      sync.RWMutex
	conns   map[string]*activeConn
	dialing int // number of Connect calls in progress (between check and insert)

	// connectBackoff tracks channels that recently failed to connect.
	// Entries are cleaned up when they expire (older than backoffDuration).
	connectBackoff  map[string]backoffEntry
	backoffMu       sync.Mutex
	backoffDuration time.Duration
}

// NewManager creates a new connection Manager. The provided ctx should be a
// long-lived context (e.g. rootCtx) so that per-connection contexts survive
// independently of the reconcile/command loop.
func NewManager(ctx context.Context, workerID string, maxConn int, maxMsgPerSec float64, publisher *pipeline.Publisher, redisStore *store.RedisStore, metrics *WorkerMetrics, pgStore handoffAcker) *Manager {
	connCtx, connCancel := context.WithCancel(ctx)
	rc := NewRateCounter()
	stopSnap := make(chan struct{})

	go rc.StartSnapshotter(time.Second, stopSnap)

	platforms := []string{
		string(model.PlatformSoop),
		string(model.PlatformChzzk),
		string(model.PlatformCime),
	}
	if metrics != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					for _, p := range platforms {
						metrics.MessagesPerSecond.WithLabelValues(p).Set(rc.RateForKey(p))
					}
				case <-stopSnap:
					return
				}
			}
		}()
	}

	return &Manager{
		workerID:     workerID,
		maxConn:      maxConn,
		maxMsgPerSec: maxMsgPerSec,
		publisher:    publisher,
		redisStore:   redisStore,
		pgStore:      pgStore,
		metrics:      metrics,
		rateCounter:  rc,
		rateLimiters: map[model.Platform]*rate.Limiter{
			model.PlatformChzzk: rate.NewLimiter(rate.Limit(10), 15),
			model.PlatformSoop:  rate.NewLimiter(rate.Limit(5), 10),
			model.PlatformCime:  rate.NewLimiter(rate.Limit(10), 15),
		},
		connCtx:         connCtx,
		connCancel:      connCancel,
		stopSnap:        stopSnap,
		conns:           make(map[string]*activeConn),
		connectBackoff:  make(map[string]backoffEntry),
		backoffDuration: 30 * time.Second,
	}
}

func (m *Manager) SetCollectionChannel(channel string) { m.collectionChannel = &channel }
func (m *Manager) permits(channel model.LiveChannel) bool {
	return m.collectionChannel == nil || (*m.collectionChannel != "" && channel.Platform == model.PlatformSoop && channel.ChannelID == *m.collectionChannel)
}

// Connect creates a connector for the given channel and starts forwarding messages.
func (m *Manager) Connect(ctx context.Context, channel model.LiveChannel) error {
	if !m.permits(channel) {
		return fmt.Errorf("collection scope denied")
	}
	if m.optOuts != nil && m.optOuts.IsOptedOut(channel.Platform, channel.ChannelID) {
		slog.Info("connect skipped: collection opted out",
			"platform", channel.Platform, "channel", channel.ChannelID)
		return nil
	}

	key := connKey(channel.Platform, channel.ChannelID)

	m.mu.Lock()
	if _, exists := m.conns[key]; exists || m.connecting[key] {
		m.mu.Unlock()
		return nil
	}
	if m.maxConn > 0 && len(m.conns)+m.dialing >= m.maxConn {
		m.mu.Unlock()
		return fmt.Errorf("max connections reached (%d)", m.maxConn)
	}
	if m.connecting == nil {
		m.connecting = make(map[string]bool)
	}
	m.connecting[key] = true
	m.dialing++
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.dialing--
		delete(m.connecting, key)
		m.mu.Unlock()
	}()

	conn, err := connectorFactory(channel.Platform)
	if err != nil {
		return fmt.Errorf("create connector: %w", err)
	}

	// Derive the per-connection context from m.connCtx (long-lived), NOT from
	// the caller's ctx. This prevents opsCancel() during drain from killing
	// existing connections — only individual Disconnect or DisconnectAll will
	// cancel them.
	connCtx, cancel := context.WithCancel(m.connCtx)
	release, err := m.prepareCollection(connCtx, cancel, channel, conn)
	if err != nil {
		cancel()
		return err
	}
	connected := false
	defer func() {
		if !connected {
			release()
		}
	}()

	// Enforce a timeout on the initial connection attempt without cancelling
	// the long-lived connCtx that readLoop/pingLoop goroutines depend on.
	// The connector receives connCtx so its background goroutines survive
	// after Connect returns.
	type dialResult struct{ err error }
	dialCh := make(chan dialResult, 1)
	go func() {
		dialCh <- dialResult{conn.Connect(connCtx, channel)}
	}()

	select {
	case r := <-dialCh:
		err = r.err
	case <-time.After(ConnectTimeout):
		cancel()
		if m.metrics != nil {
			m.metrics.ConnectionAttemptsTotal.WithLabelValues(string(channel.Platform), "error").Inc()
		}
		return fmt.Errorf("connect to %s: timeout after %v", key, ConnectTimeout)
	}
	if err != nil {
		if m.collectionStore != nil {
			_ = m.collectionStore.SetCollectionState(ctx, channel.ChannelID, collection.FailureState(err))
		}
		cancel()
		if m.metrics != nil {
			m.metrics.ConnectionAttemptsTotal.WithLabelValues(string(channel.Platform), "error").Inc()
		}
		return fmt.Errorf("connect to %s: %w", key, err)
	}

	if err := connCtx.Err(); err != nil {
		_ = conn.Disconnect()
		return err
	}
	ac := &activeConn{
		connector:         conn,
		cancel:            cancel,
		channel:           channel,
		connectedAt:       time.Now(),
		releaseCollection: release,
	}

	// Double-check after dialing in case another goroutine connected concurrently.
	m.mu.Lock()
	if _, exists := m.conns[key]; exists {
		m.mu.Unlock()
		cancel()
		if err := conn.Disconnect(); err != nil {
			slog.Debug("close duplicate connection failed", "platform", channel.Platform, "error", err)
		}
		return nil
	}
	connected = true
	m.conns[key] = ac
	m.mu.Unlock()

	if m.metrics != nil {
		m.metrics.ConnectionAttemptsTotal.WithLabelValues(string(channel.Platform), "success").Inc()
		m.metrics.ConnectionsActive.WithLabelValues(string(channel.Platform), m.workerID).Inc()
	}

	// Ack handoff if this connection is part of a drain handoff.
	// AckHandoff only updates rows with handoff_status='pending', so this is
	// a no-op for normal (non-handoff) connections.
	// ClearHandoff은 acked → handoff_from/status/started_at = NULL로 정리하여
	// 워커 재시작 후 ListWorkerChannels로 다시 로드되더라도 dedup 모드로
	// 다시 들어가지 않게 한다. (이전 워커가 죽어서 ClearHandoff를 못 부른 경우 대비)
	if m.pgStore != nil {
		if err := m.pgStore.AckHandoff(ctx, channel.Platform, channel.ChannelID); err != nil {
			slog.Warn("ack handoff failed (non-fatal)",
				"platform", channel.Platform, "channel", channel.ChannelID, "error", err)
		} else {
			if err := m.pgStore.ClearHandoff(ctx, ac.channel.ID); err != nil {
				slog.Warn("clear handoff failed (non-fatal)",
					"platform", channel.Platform, "channel", channel.ChannelID, "error", err)
			}
			if ac.channel.HandoffFrom != nil {
				// 메모리에서도 클리어하여 이번 커넥션의 forwardMessages가
				// 즉시 dedup 모드(PublishWithDedup)에서 빠져나오도록 한다.
				ac.channel.HandoffFrom = nil
				slog.Info("handoff cleared", "key", key)
			}
		}
	}

	go m.forwardMessages(connCtx, key, ac)
	go m.handleErrors(connCtx, key, ac)

	if m.collectionStore != nil {
		_ = m.collectionStore.SetCollectionState(ctx, channel.ChannelID, "connected")
	}
	slog.Info("connected", "key", key)
	return nil
}

// Disconnect closes a specific connection by platform and channel ID.
func (m *Manager) Disconnect(platform model.Platform, channelID string) error {
	key := connKey(platform, channelID)

	m.mu.Lock()
	ac, ok := m.conns[key]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no connection for %s", key)
	}
	delete(m.conns, key)
	m.mu.Unlock()

	if m.metrics != nil {
		m.metrics.ConnectionsActive.WithLabelValues(string(platform), m.workerID).Dec()
		m.metrics.DisconnectsTotal.WithLabelValues(string(platform), "graceful").Inc()
		if !ac.connectedAt.IsZero() {
			m.metrics.ConnectionDurationSeconds.WithLabelValues(string(platform)).Observe(time.Since(ac.connectedAt).Seconds())
		}
	}

	ac.cancel()
	if ac.releaseCollection != nil {
		ac.releaseCollection()
	}
	if err := ac.connector.Disconnect(); err != nil {
		slog.Warn("disconnect error", "key", key, "error", err)
	}
	slog.Info("disconnected", "key", key)
	return nil
}

// DisconnectAll gracefully closes all active connections.
func (m *Manager) DisconnectAll() {
	// Cancel the long-lived connection context so any goroutines still
	// selecting on connCtx.Done() are unblocked.
	m.connCancel()

	// Stop the snapshotter and rate-gauge goroutines.
	close(m.stopSnap)

	m.mu.Lock()
	snapshot := make(map[string]*activeConn, len(m.conns))
	for k, v := range m.conns {
		snapshot[k] = v
	}
	m.conns = make(map[string]*activeConn)
	m.mu.Unlock()

	for key, ac := range snapshot {
		platform := string(ac.channel.Platform)
		if m.metrics != nil {
			m.metrics.ConnectionsActive.WithLabelValues(platform, m.workerID).Dec()
			m.metrics.DisconnectsTotal.WithLabelValues(platform, "shutdown").Inc()
			if !ac.connectedAt.IsZero() {
				m.metrics.ConnectionDurationSeconds.WithLabelValues(platform).Observe(time.Since(ac.connectedAt).Seconds())
			}
		}
		ac.cancel()
		if ac.releaseCollection != nil {
			ac.releaseCollection()
		}
		if err := ac.connector.Disconnect(); err != nil {
			slog.Warn("disconnect error during shutdown", "key", key, "error", err)
		}
	}
	slog.Info("all connections disconnected", "count", len(snapshot))
}

// SetDraining marks the manager as draining. In drain mode, ALL messages are
// published with dedup so the new worker receiving handoffs can deduplicate
// overlapping messages during the transition period.
func (m *Manager) SetDraining() {
	m.draining.Store(true)
}

// SetChatWriter sets the optional ClickHouse chat message batch writer.
func (m *Manager) SetChatWriter(w *store.BatchWriter[store.ChatMessageRow]) {
	m.chatWriter = w
}

// SetChatBufferWriter sets the optional best-effort secondary buffer writer. It is
// independent of the authoritative writer and must always use a bounded queue.
func (m *Manager) SetChatBufferWriter(w *store.BatchWriter[store.ChatMessageRow]) {
	m.chatBufferWriter = w
}

// SetCollectionOptOutCache enables dynamic collection blocking for this worker.
func (m *Manager) SetCollectionOptOutCache(cache *store.CollectionOptOutCache) {
	m.optOuts = cache
}

// ConnectionCount returns the number of active connections.
func (m *Manager) ConnectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// ActiveCount returns the number of currently active connections.
// It is an alias for ConnectionCount used by the drain shutdown sequence.
func (m *Manager) ActiveCount() int {
	return m.ConnectionCount()
}

// ReconcilePlatform connects missing channels and disconnects stale ones for
// a SINGLE platform. Runs synchronously with bounded concurrency — the caller
// is responsible for running each platform in its own goroutine.
func (m *Manager) ReconcilePlatform(ctx context.Context, platform model.Platform, assigned []model.LiveChannel) error {
	var timer *prometheus.Timer
	if m.metrics != nil {
		timer = prometheus.NewTimer(m.metrics.ReconcileDurationSeconds.WithLabelValues(string(platform)))
	}
	defer func() {
		if timer != nil {
			timer.ObserveDuration()
		}
	}()

	// 자기 채널 중 handoff_status='acked' 상태로 stuck된 것을 자동 청소.
	// 정상 흐름에서는 Connect 시점에 ClearHandoff를 부르지만, 이전 워커가
	// drain 도중 죽거나 ClearHandoff 호출 전에 새 워커가 연결됐던 경우
	// DB에 acked 상태가 영구 잔존하여 워커 재시작 시 dedup 모드로 빠진다.
	// 여기서 한 번 더 청소하여 자기 회복(self-heal) 보장.
	if m.pgStore != nil {
		for i := range assigned {
			ch := &assigned[i]
			if ch.HandoffStatus != nil && *ch.HandoffStatus == "acked" {
				if err := m.pgStore.ClearHandoff(ctx, ch.ID); err != nil {
					slog.Warn("reconcile: clear stale handoff failed",
						"platform", ch.Platform, "channel", ch.ChannelID, "error", err)
				} else {
					slog.Info("reconcile: cleared stale handoff",
						"platform", ch.Platform, "channel", ch.ChannelID)
					ch.HandoffFrom = nil
					ch.HandoffStatus = nil
					ch.HandoffStartedAt = nil
				}
			}
		}
		// in-memory의 active connection 채널 정보도 갱신.
		m.mu.Lock()
		for _, ch := range assigned {
			key := connKey(ch.Platform, ch.ChannelID)
			if ac, ok := m.conns[key]; ok && ac.channel.HandoffFrom != nil && ch.HandoffFrom == nil {
				ac.channel.HandoffFrom = nil
				ac.channel.HandoffStatus = nil
				ac.channel.HandoffStartedAt = nil
			}
		}
		m.mu.Unlock()
	}

	desired := make(map[string]model.LiveChannel, len(assigned))
	for _, ch := range assigned {
		if !m.permits(ch) {
			continue
		}
		if m.optOuts != nil && m.optOuts.IsOptedOut(ch.Platform, ch.ChannelID) {
			continue
		}
		desired[connKey(ch.Platform, ch.ChannelID)] = ch
	}

	// Snapshot this platform's active connections.
	m.mu.RLock()
	var activeKeys []string
	for key := range m.conns {
		if m.conns[key].channel.Platform == platform {
			activeKeys = append(activeKeys, key)
		}
	}
	m.mu.RUnlock()

	// Clean up stale backoff entries (no activity for 10 minutes).
	now := time.Now()
	if m.connectBackoff != nil {
		m.backoffMu.Lock()
		for k, e := range m.connectBackoff {
			if now.Sub(e.lastFail) >= 10*time.Minute {
				delete(m.connectBackoff, k)
			}
		}
		m.backoffMu.Unlock()
	}

	// Find channels that need connecting, skipping ones still in backoff window.
	var missing []model.LiveChannel
	for key, ch := range desired {
		m.mu.RLock()
		_, connected := m.conns[key]
		m.mu.RUnlock()
		if connected {
			continue
		}
		if m.connectBackoff != nil {
			m.backoffMu.Lock()
			e, exists := m.connectBackoff[key]
			m.backoffMu.Unlock()
			if exists && now.Sub(e.lastFail) < m.backoffDuration {
				continue // still in backoff window, skip
			}
			// entry exists but backoff expired → allow retry (entry kept for failure tracking)
		}
		missing = append(missing, ch)
	}

	// Use burst parameters when a large backlog is detected.
	batchSize := ReconcileMaxBatch
	concurrency := PerPlatformConcurrency
	if len(missing) > 100 {
		batchSize = BurstReconcileMaxBatch
		concurrency = BurstPerPlatformConcurrency
	}

	// Cap to batchSize.
	toConnect := missing
	if len(toConnect) > batchSize {
		toConnect = toConnect[:batchSize]
	}

	if m.metrics != nil {
		m.metrics.ReconcileBatchSize.WithLabelValues(string(platform)).Set(float64(len(toConnect)))
	}

	// Connect with bounded concurrency — BLOCKS until this batch is done.
	sem := make(chan struct{}, concurrency)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, ch := range toConnect {
		select {
		case <-ctx.Done():
			goto wait
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(ch model.LiveChannel) {
			defer func() { <-sem; wg.Done() }()
			// Per-platform rate limiting to avoid hammering upstream APIs.
			if limiter, ok := m.rateLimiters[platform]; ok {
				if err := limiter.Wait(ctx); err != nil {
					return // context cancelled
				}
			}
			if err := m.Connect(ctx, ch); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				if m.connectBackoff != nil {
					key := connKey(ch.Platform, ch.ChannelID)
					m.backoffMu.Lock()
					entry := m.connectBackoff[key] // copy previous state
					entry.lastFail = time.Now()
					entry.failures++
					if entry.firstFail.IsZero() {
						entry.firstFail = entry.lastFail
					}
					m.connectBackoff[key] = entry
					m.backoffMu.Unlock()

					// After 3 failures, notify coordinator to attempt reconnect.
					if entry.failures == 3 {
						evt := store.CoordEvent{
							Type:      store.CoordEventWSClosed,
							Platform:  ch.Platform,
							ChannelID: ch.ChannelID,
							WorkerID:  m.workerID,
						}
						if err := m.redisStore.PublishCoordEvent(ctx, evt); err != nil {
							slog.Warn("publish websocket-closed event failed", "platform", ch.Platform, "error", err)
						}
					}
					// After 5 minutes of persistent failure, request unassign to free capacity.
					if !entry.unassignSent && entry.failures >= 3 && time.Since(entry.firstFail) > 5*time.Minute {
						slog.Warn("channel connect failing for 5+ minutes, requesting unassign",
							"platform", ch.Platform, "channelID", ch.ChannelID,
							"failures", entry.failures, "firstFail", entry.firstFail)
						evt := store.CoordEvent{
							Type:      store.CoordEventUnassign,
							Platform:  ch.Platform,
							ChannelID: ch.ChannelID,
							WorkerID:  m.workerID,
						}
						if err := m.redisStore.PublishCoordEvent(ctx, evt); err != nil {
							slog.Warn("publish unassign event failed", "platform", ch.Platform, "error", err)
						}
						m.backoffMu.Lock()
						entry.unassignSent = true
						m.connectBackoff[key] = entry
						m.backoffMu.Unlock()
					}
				}
			}
		}(ch)
	}
wait:
	wg.Wait()

	if len(toConnect) > 0 {
		slog.Info("reconcile batch complete",
			"platform", platform,
			"attempted", len(toConnect),
			"connected", len(toConnect)-len(errs),
			"errors", len(errs),
			"active", m.ConnectionCount())
	}

	// Disconnect stale connections for this platform.
	for _, key := range activeKeys {
		if _, ok := desired[key]; ok {
			continue
		}
		m.mu.RLock()
		ac, exists := m.conns[key]
		m.mu.RUnlock()
		if !exists {
			continue
		}
		if err := m.Disconnect(ac.channel.Platform, ac.channel.ChannelID); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// GetLoad returns the current WorkerLoad snapshot.
func (m *Manager) GetLoad() model.WorkerLoad {
	return model.WorkerLoad{
		WorkerID:     m.workerID,
		Connections:  m.ConnectionCount(),
		MsgPerSec:    m.rateCounter.Rate(),
		MaxConn:      m.maxConn,
		MaxMsgPerSec: m.maxMsgPerSec,
	}
}

// StartHeartbeatLoop sends periodic heartbeats to Redis. It blocks until
// the context is cancelled. An immediate heartbeat is sent before the ticker
// starts so the coordinator sees us before the first health check.
func (m *Manager) StartHeartbeatLoop(ctx context.Context, interval time.Duration) {
	// sendAndObserve sends a heartbeat and records metrics.
	sendAndObserve := func() {
		load := m.GetLoad()
		if m.metrics != nil {
			m.metrics.LoadRatio.Set(load.Load())
		}

		start := time.Now()
		if err := m.redisStore.SendHeartbeat(ctx, m.workerID, load); err != nil {
			slog.Error("heartbeat failed", "error", err)
		}
		if m.metrics != nil {
			m.metrics.HeartbeatDurationSeconds.Observe(time.Since(start).Seconds())
		}

		if length, err := m.redisStore.StreamLength(ctx, "chat:firehose"); err == nil {
			if m.metrics != nil {
				m.metrics.RedisStreamLength.WithLabelValues("firehose").Set(float64(length))
			}
		}
	}

	// Immediate heartbeat to prevent being marked dead before first tick.
	sendAndObserve()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendAndObserve()
		}
	}
}

// forwardMessages reads parsed messages from a connector and publishes them.
func (m *Manager) forwardMessages(ctx context.Context, key string, ac *activeConn) {
	platform := string(ac.channel.Platform)
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ac.connector.Messages():
			if !ok {
				return
			}
			if m.optOuts != nil && m.optOuts.IsOptedOut(ac.channel.Platform, ac.channel.ChannelID) {
				slog.Info("message dropped: collection opted out", "key", key)
				go func() {
					if err := m.Disconnect(ac.channel.Platform, ac.channel.ChannelID); err != nil {
						slog.Warn("opt-out disconnect failed", "key", key, "error", err)
					}
				}()
				return
			}
			m.rateCounter.AddForKey(platform, 1)
			if m.metrics != nil {
				m.metrics.MessagesReceivedTotal.WithLabelValues(platform, string(msg.Type)).Inc()
			}
			// Queue both storage sinks before the synchronous Redis publish. A
			// Redis outage must not prevent either ClickHouse path from retaining
			// a message that was already received from the platform connector.
			row := store.ChatMessageRowFrom(msg, m.workerID)
			if m.chatWriter != nil {
				m.chatWriter.Enqueue(row)
				if m.metrics != nil {
					m.metrics.ChatCHEnqueueTotal.WithLabelValues(platform).Inc()
				}
			}
			if m.chatBufferWriter != nil {
				m.chatBufferWriter.Enqueue(row)
				if m.metrics != nil {
					m.metrics.ChatCHBufferEnqueueTotal.WithLabelValues(platform).Inc()
				}
			}
			var err error
			if m.draining.Load() || (ac.channel.HandoffFrom != nil && *ac.channel.HandoffFrom != "") {
				err = m.publisher.PublishWithDedup(ctx, msg)
			} else {
				err = m.publisher.Publish(ctx, msg)
			}
			if err != nil {
				slog.Error("publish failed", "key", key, "error", err)
			}
		}
	}
}

// handleErrors reads errors from a connector and publishes a ws_closed CoordEvent.
// On error, it performs full teardown (cancel per-conn context + Disconnect the
// connector) before removing the map entry so that forwardMessages and any
// connector goroutines exit deterministically. Without this, a connector error
// leaks the forwardMessages goroutine, which blocks on the connector's Messages
// channel indefinitely.
func (m *Manager) handleErrors(ctx context.Context, key string, ac *activeConn) {
	platform := string(ac.channel.Platform)
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-ac.connector.Errors():
			if !ok {
				return
			}
			reason := classifyConnectorError(err)
			slog.Error("connector error", "key", key, "reason", reason, "error", err)

			// Full teardown BEFORE removing the map entry: cancel per-conn
			// context and close the WS. This unblocks forwardMessages (which
			// is selecting on Messages()) and any other connector goroutines.
			if ac.cancel != nil {
				ac.cancel()
				if ac.releaseCollection != nil {
					ac.releaseCollection()
				}
			}
			if disconnectErr := ac.connector.Disconnect(); disconnectErr != nil {
				slog.Warn("connector disconnect after error failed",
					"key", key, "error", disconnectErr)
			}

			// Remove from active connections.
			m.mu.Lock()
			delete(m.conns, key)
			m.mu.Unlock()

			if m.metrics != nil {
				m.metrics.ConnectionsActive.WithLabelValues(platform, m.workerID).Dec()
				m.metrics.DisconnectsTotal.WithLabelValues(platform, reason).Inc()
				if isZombieReason(reason) {
					m.metrics.ZombieReapedTotal.WithLabelValues(platform, reason).Inc()
				}
				if !ac.connectedAt.IsZero() {
					m.metrics.ConnectionDurationSeconds.WithLabelValues(platform).Observe(time.Since(ac.connectedAt).Seconds())
				}
			}

			// Notify coordinator that the WebSocket closed.
			event := store.CoordEvent{
				Type:      store.CoordEventWSClosed,
				Platform:  ac.channel.Platform,
				ChannelID: ac.channel.ChannelID,
				WorkerID:  m.workerID,
			}
			publishCtx, publishCancel := context.WithTimeout(context.WithoutCancel(ctx), CoordEventPublishTimeout)
			if pubErr := m.redisStore.PublishCoordEvent(publishCtx, event); pubErr != nil {
				slog.Error("failed to publish ws_closed event", "error", pubErr)
			}
			publishCancel()
			return
		}
	}
}
