package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// WorkerMetrics holds all Prometheus metrics for the chat worker.
// Metrics are updated by instrumented code in the manager, publisher,
// and connector layers rather than pulled at scrape time.
type WorkerMetrics struct {
	// Ready indicates whether the worker process is ready to accept assignments.
	Ready prometheus.Gauge

	// ConnectionsActive tracks the number of active connections per platform and worker.
	ConnectionsActive *prometheus.GaugeVec

	// ConnectionAttemptsTotal counts connection attempts by platform and result (success/failure).
	ConnectionAttemptsTotal *prometheus.CounterVec

	// ConnectionDurationSeconds observes how long connections stay alive, by platform.
	ConnectionDurationSeconds *prometheus.HistogramVec

	// DisconnectsTotal counts disconnections by platform and reason.
	// Reason values: graceful, shutdown, ws_closed, zombie_reaped,
	// liveness_ping_failed, handshake_failed.
	DisconnectsTotal *prometheus.CounterVec

	// ZombieReapedTotal counts silent/stalled connections forcefully reaped
	// by the worker, labelled by platform and specific cause. A subset of
	// DisconnectsTotal is surfaced separately to make stale connections visible.
	ZombieReapedTotal *prometheus.CounterVec

	// MessagesReceivedTotal counts incoming messages by platform and message type.
	MessagesReceivedTotal *prometheus.CounterVec

	// MessagesPublishedTotal counts messages published to Redis by platform.
	MessagesPublishedTotal *prometheus.CounterVec

	// MessagesDedupSkippedTotal counts messages dropped by PublishWithDedup
	// because the dedup key was already present (Lua SET NX returned 0).
	// Used to detect stuck handoff state and dedup-induced message loss.
	MessagesDedupSkippedTotal *prometheus.CounterVec

	// PublishErrorsTotal counts Redis publish errors by platform and error type.
	PublishErrorsTotal *prometheus.CounterVec

	// PublishDurationSeconds observes the latency of individual publish operations.
	PublishDurationSeconds prometheus.Histogram

	// MessagesPerSecond tracks the observed message rate per platform.
	MessagesPerSecond *prometheus.GaugeVec

	// ReconcileDurationSeconds observes how long reconciliation takes per platform.
	ReconcileDurationSeconds *prometheus.HistogramVec

	// ReconcileBatchSize tracks the number of channels reconciled per platform.
	ReconcileBatchSize *prometheus.GaugeVec

	// LoadRatio reports the current aggregate load ratio of this worker.
	LoadRatio prometheus.Gauge

	// HeartbeatDurationSeconds observes the latency of heartbeat operations.
	HeartbeatDurationSeconds prometheus.Histogram

	// RelayConnectionAttemptsTotal counts SOOP relay connection attempts by result.
	RelayConnectionAttemptsTotal *prometheus.CounterVec

	// RelayConnectionDurationSeconds observes SOOP relay connection latency.
	RelayConnectionDurationSeconds prometheus.Histogram

	// MessagesDroppedTotal counts messages silently dropped because the
	// connector's internal channel buffer was full, by platform.
	MessagesDroppedTotal *prometheus.CounterVec

	// RedisStreamLength tracks the length of Redis streams.
	RedisStreamLength *prometheus.GaugeVec

	// ChatCHFlushTotal counts ClickHouse batch flush results.
	ChatCHFlushTotal *prometheus.CounterVec

	// ChatCHBatchSize observes batch sizes when flushing to ClickHouse.
	ChatCHBatchSize prometheus.Histogram

	// ChatCHDroppedTotal counts chat messages dropped due to CH failures.
	ChatCHDroppedTotal prometheus.Counter

	// ChatCHRetryTotal counts failed ClickHouse flushes retained for retry.
	ChatCHRetryTotal prometheus.Counter

	// ChatCHPending tracks messages retained in memory until ClickHouse recovers.
	ChatCHPending prometheus.Gauge

	// ChatCHEnqueueTotal counts messages enqueued for ClickHouse by platform.
	ChatCHEnqueueTotal *prometheus.CounterVec

	ChatCHBufferFlushTotal      *prometheus.CounterVec
	ChatCHBufferBatchSize       prometheus.Histogram
	ChatCHBufferDroppedTotal    prometheus.Counter
	ChatCHBufferRetryTotal      prometheus.Counter
	ChatCHBufferPending         prometheus.Gauge
	ChatCHBufferEnqueueTotal    *prometheus.CounterVec
	ChatArchiveAcceptedTotal    prometheus.Counter
	ChatArchiveSaveErrorsTotal  prometheus.Counter
	ChatArchiveDrainErrorsTotal prometheus.Counter
	ChatArchiveBacklogFiles     prometheus.Gauge
	ChatArchiveBacklogBytes     prometheus.Gauge
}

// NewMetricsRegistry creates a Prometheus registry and returns it along with
// the full set of worker metrics. All metrics are registered with the registry.
func NewMetricsRegistry() (*prometheus.Registry, *WorkerMetrics) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &WorkerMetrics{
		Ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_ready",
			Help: "Whether the worker process is ready to accept assignments.",
		}),

		ConnectionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_connections_active",
			Help: "Number of active chat platform connections on this worker.",
		}, []string{"platform", "worker_id"}),

		ConnectionAttemptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_connection_attempts_total",
			Help: "Total number of connection attempts by platform and result.",
		}, []string{"platform", "result"}),

		ConnectionDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_connection_duration_seconds",
			Help:    "Duration of chat platform connections in seconds.",
			Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 14400},
		}, []string{"platform"}),

		DisconnectsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_disconnects_total",
			Help: "Total number of disconnections by platform and reason.",
		}, []string{"platform", "reason"}),

		ZombieReapedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_zombie_reaped_total",
			Help: "Total silent/stalled connections forcefully reaped by platform and cause.",
		}, []string{"platform", "cause"}),

		MessagesReceivedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_messages_received_total",
			Help: "Total number of chat messages received by platform and type.",
		}, []string{"platform", "type"}),

		MessagesPublishedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_messages_published_total",
			Help: "Total number of chat messages published to Redis by platform.",
		}, []string{"platform"}),

		MessagesDedupSkippedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_messages_dedup_skipped_total",
			Help: "Total chat messages skipped by PublishWithDedup because the dedup key already exists.",
		}, []string{"platform"}),

		PublishErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_publish_errors_total",
			Help: "Total number of Redis publish errors by platform and error type.",
		}, []string{"platform", "error_type"}),

		PublishDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_publish_duration_seconds",
			Help:    "Latency of individual publish operations in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		}),

		MessagesPerSecond: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_messages_per_second",
			Help: "Current observed message rate per platform.",
		}, []string{"platform"}),

		ReconcileDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_reconcile_duration_seconds",
			Help:    "Duration of reconciliation operations per platform in seconds.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30},
		}, []string{"platform"}),

		ReconcileBatchSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_reconcile_batch_size",
			Help: "Number of channels reconciled per platform.",
		}, []string{"platform"}),

		LoadRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_load_ratio",
			Help: "Current worker load ratio derived from active connections and message rate.",
		}),

		HeartbeatDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_heartbeat_duration_seconds",
			Help:    "Latency of heartbeat operations in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5},
		}),

		RelayConnectionAttemptsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_relay_connection_attempts_total",
			Help: "Total number of SOOP relay connection attempts by result.",
		}, []string{"result"}),

		RelayConnectionDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_relay_connection_duration_seconds",
			Help:    "Latency of SOOP relay connection establishment in seconds.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		}),

		MessagesDroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_messages_dropped_total",
			Help: "Total number of messages dropped because the connector buffer was full.",
		}, []string{"platform"}),

		RedisStreamLength: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_redis_stream_length",
			Help: "Length of Redis streams used by the worker.",
		}, []string{"stream"}),

		ChatCHFlushTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_batch_flush_total",
			Help: "Total ClickHouse batch flush operations by status.",
		}, []string{"status"}),

		ChatCHBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_ch_batch_size",
			Help:    "Number of messages per ClickHouse batch flush.",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
		}),

		ChatCHDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_dropped_total",
			Help: "Total chat messages dropped after the shutdown drain deadline.",
		}),

		ChatCHRetryTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_retry_total",
			Help: "Total failed ClickHouse batch flushes retained for retry.",
		}),

		ChatCHPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_ch_pending_messages",
			Help: "Number of chat messages retained in memory pending a ClickHouse flush.",
		}),

		ChatCHEnqueueTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_enqueue_total",
			Help: "Total messages enqueued for ClickHouse by platform.",
		}, []string{"platform"}),

		ChatCHBufferFlushTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_buffer_batch_flush_total",
			Help: "Total secondary ClickHouse buffer flush operations by status.",
		}, []string{"status"}),
		ChatCHBufferBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_worker_ch_buffer_batch_size",
			Help:    "Number of messages per secondary ClickHouse buffer flush.",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
		}),
		ChatCHBufferDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_buffer_dropped_total",
			Help: "Total secondary ClickHouse buffer messages dropped at the bounded queue or shutdown deadline.",
		}),
		ChatCHBufferRetryTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_buffer_retry_total",
			Help: "Total failed secondary ClickHouse buffer flushes retained for retry.",
		}),
		ChatCHBufferPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_worker_ch_buffer_pending_messages",
			Help: "Secondary ClickHouse buffer messages currently retained in memory.",
		}),
		ChatCHBufferEnqueueTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_worker_ch_buffer_enqueue_total",
			Help: "Total messages offered to the secondary ClickHouse buffer by platform.",
		}, []string{"platform"}),
		ChatArchiveAcceptedTotal:    prometheus.NewCounter(prometheus.CounterOpts{Name: "rogi_chat_archive_accepted_total", Help: "Chat messages fsynced to the local archive spool."}),
		ChatArchiveSaveErrorsTotal:  prometheus.NewCounter(prometheus.CounterOpts{Name: "rogi_chat_archive_save_errors_total", Help: "Chat messages that could not be accepted by the archive spool."}),
		ChatArchiveDrainErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{Name: "rogi_chat_archive_drain_errors_total", Help: "Failed archive spool replay attempts."}),
		ChatArchiveBacklogFiles:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "rogi_chat_archive_backlog_files", Help: "Durable chat records pending PostgreSQL acceptance."}),
		ChatArchiveBacklogBytes:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "rogi_chat_archive_backlog_bytes", Help: "Bytes of durable chat records pending PostgreSQL acceptance."}),
	}

	registry.MustRegister(
		m.Ready,
		m.ConnectionsActive,
		m.ConnectionAttemptsTotal,
		m.ConnectionDurationSeconds,
		m.DisconnectsTotal,
		m.ZombieReapedTotal,
		m.MessagesReceivedTotal,
		m.MessagesPublishedTotal,
		m.MessagesDedupSkippedTotal,
		m.PublishErrorsTotal,
		m.PublishDurationSeconds,
		m.MessagesPerSecond,
		m.ReconcileDurationSeconds,
		m.ReconcileBatchSize,
		m.LoadRatio,
		m.HeartbeatDurationSeconds,
		m.RelayConnectionAttemptsTotal,
		m.RelayConnectionDurationSeconds,
		m.MessagesDroppedTotal,
		m.RedisStreamLength,
		m.ChatCHFlushTotal,
		m.ChatCHBatchSize,
		m.ChatCHDroppedTotal,
		m.ChatCHRetryTotal,
		m.ChatCHPending,
		m.ChatCHEnqueueTotal,
		m.ChatCHBufferFlushTotal,
		m.ChatCHBufferBatchSize,
		m.ChatCHBufferDroppedTotal,
		m.ChatCHBufferRetryTotal,
		m.ChatCHBufferPending,
		m.ChatCHBufferEnqueueTotal,
		m.ChatArchiveAcceptedTotal,
		m.ChatArchiveSaveErrorsTotal,
		m.ChatArchiveDrainErrorsTotal,
		m.ChatArchiveBacklogFiles,
		m.ChatArchiveBacklogBytes,
	)

	return registry, m
}
