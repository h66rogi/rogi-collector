package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// DiscoverMetrics holds all Prometheus metrics for the discover service.
type DiscoverMetrics struct {
	// Ready indicates whether the discover process is ready to accept traffic.
	Ready prometheus.Gauge

	// CycleDurationSeconds observes how long a full discovery cycle takes per platform.
	CycleDurationSeconds *prometheus.HistogramVec

	// CycleTotal counts discovery cycles by platform and result (success/error).
	CycleTotal *prometheus.CounterVec

	// ChannelsFound tracks the number of live channels found per platform.
	ChannelsFound *prometheus.GaugeVec

	// ChannelsUpsertedTotal counts channels upserted per platform.
	ChannelsUpsertedTotal *prometheus.CounterVec

	// ChannelsEndedTotal counts channels marked as ended per platform.
	ChannelsEndedTotal *prometheus.CounterVec

	// APIRequestDurationSeconds observes latency of platform API calls.
	APIRequestDurationSeconds *prometheus.HistogramVec

	// APIRequestTotal counts platform API requests by platform, endpoint, and status.
	APIRequestTotal *prometheus.CounterVec

	// ConsecutiveFailures tracks the number of consecutive failures per platform.
	ConsecutiveFailures *prometheus.GaugeVec

	// ClickHouseAvailable indicates whether ClickHouse is connected (1) or not (0).
	ClickHouseAvailable prometheus.Gauge
}

// NewMetricsRegistry creates a Prometheus registry and returns it along with
// the full set of discover metrics. All metrics are registered with the registry.
func NewMetricsRegistry() (*prometheus.Registry, *DiscoverMetrics) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &DiscoverMetrics{
		Ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_discover_ready",
			Help: "Whether the discover process is ready to accept traffic.",
		}),

		CycleDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "meloming_chat_discover_cycle_duration_seconds",
			Help:    "Duration of a full discovery cycle per platform in seconds.",
			Buckets: []float64{1, 5, 10, 20, 30, 60, 120},
		}, []string{"platform"}),

		CycleTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_discover_cycle_total",
			Help: "Total number of discovery cycles by platform and result.",
		}, []string{"platform", "result"}),

		ChannelsFound: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_discover_channels_found",
			Help: "Number of live channels found in the last discovery cycle per platform.",
		}, []string{"platform"}),

		ChannelsUpsertedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_discover_channels_upserted_total",
			Help: "Total number of channels upserted per platform.",
		}, []string{"platform"}),

		ChannelsEndedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_discover_channels_ended_total",
			Help: "Total number of channels marked as ended per platform.",
		}, []string{"platform"}),

		APIRequestDurationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "meloming_chat_discover_api_request_duration_seconds",
			Help:    "Latency of platform API requests in seconds.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}, []string{"platform", "endpoint"}),

		APIRequestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_discover_api_request_total",
			Help: "Total number of platform API requests by platform, endpoint, and status.",
		}, []string{"platform", "endpoint", "status"}),

		ConsecutiveFailures: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_discover_consecutive_failures",
			Help: "Number of consecutive discovery failures per platform.",
		}, []string{"platform"}),

		ClickHouseAvailable: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_discover_clickhouse_available",
			Help: "Whether ClickHouse is connected and available for viewer count writes (1=yes, 0=no).",
		}),
	}

	registry.MustRegister(
		m.Ready,
		m.CycleDurationSeconds,
		m.CycleTotal,
		m.ChannelsFound,
		m.ChannelsUpsertedTotal,
		m.ChannelsEndedTotal,
		m.APIRequestDurationSeconds,
		m.APIRequestTotal,
		m.ConsecutiveFailures,
		m.ClickHouseAvailable,
	)

	return registry, m
}
