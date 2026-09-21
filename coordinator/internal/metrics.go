package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// CoordinatorMetrics holds all Prometheus metrics for the coordinator service.
type CoordinatorMetrics struct {
	// Ready indicates whether the coordinator process is ready to accept traffic.
	Ready prometheus.Gauge

	// IsLeader indicates whether this coordinator instance is currently the leader (1=leader, 0=standby).
	IsLeader prometheus.Gauge

	// AliveWorkers tracks the number of workers with valid heartbeats.
	AliveWorkers prometheus.Gauge

	// DeadWorkersTotal counts the total number of workers marked as dead.
	DeadWorkersTotal prometheus.Counter

	// ChannelsAssignedTotal counts channels assigned to workers, by platform.
	ChannelsAssignedTotal *prometheus.CounterVec

	// ChannelsUnassignedTotal counts channels unassigned from workers, by reason.
	ChannelsUnassignedTotal *prometheus.CounterVec

	// ChannelsPending tracks the number of pending channels per platform.
	ChannelsPending *prometheus.GaugeVec

	// ChannelsAssigned tracks the total number of live channels assigned to any worker.
	// Stable during rollouts, suitable for KEDA autoscaling.
	ChannelsAssigned prometheus.Gauge

	// AssignCycleDurationSeconds observes the duration of each assignment cycle.
	AssignCycleDurationSeconds prometheus.Histogram

	// HealthcheckDurationSeconds observes the duration of each health check cycle.
	HealthcheckDurationSeconds prometheus.Histogram

	// WorkerLoad tracks the load ratio of each worker.
	WorkerLoad *prometheus.GaugeVec

	// AssignErrorsTotal counts assignment errors by error type.
	AssignErrorsTotal *prometheus.CounterVec

	// HealthcheckErrorsTotal counts health check errors by error type.
	HealthcheckErrorsTotal *prometheus.CounterVec
}

var cycleBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2}

// NewMetricsRegistry creates a Prometheus registry and returns it along with
// the full set of coordinator metrics. All metrics are registered with the registry.
func NewMetricsRegistry() (*prometheus.Registry, *CoordinatorMetrics) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &CoordinatorMetrics{
		Ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_coordinator_ready",
			Help: "Whether the coordinator process is ready to accept traffic.",
		}),

		IsLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_coordinator_is_leader",
			Help: "Whether this coordinator instance is the leader (1=leader, 0=standby).",
		}),

		AliveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_coordinator_alive_workers",
			Help: "Number of workers with valid heartbeats.",
		}),

		DeadWorkersTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "meloming_chat_coordinator_dead_workers_total",
			Help: "Total number of workers marked as dead.",
		}),

		ChannelsAssignedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_coordinator_channels_assigned_total",
			Help: "Total number of channels assigned to workers by platform.",
		}, []string{"platform"}),

		ChannelsUnassignedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_coordinator_channels_unassigned_total",
			Help: "Total number of channels unassigned from workers by reason.",
		}, []string{"reason"}),

		ChannelsPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_coordinator_channels_pending",
			Help: "Number of pending channels per platform.",
		}, []string{"platform"}),

		ChannelsAssigned: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "meloming_chat_coordinator_channels_assigned",
			Help: "Total number of live channels assigned to any worker. Stable during rollouts.",
		}),

		AssignCycleDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_coordinator_assign_cycle_duration_seconds",
			Help:    "Duration of each assignment cycle in seconds.",
			Buckets: cycleBuckets,
		}),

		HealthcheckDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meloming_chat_coordinator_healthcheck_duration_seconds",
			Help:    "Duration of each health check cycle in seconds.",
			Buckets: cycleBuckets,
		}),

		WorkerLoad: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "meloming_chat_coordinator_worker_load",
			Help: "Current load ratio of each worker as seen by the coordinator.",
		}, []string{"worker_id"}),

		AssignErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_coordinator_assign_errors_total",
			Help: "Total number of assignment errors by error type.",
		}, []string{"error_type"}),

		HealthcheckErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meloming_chat_coordinator_healthcheck_errors_total",
			Help: "Total number of health check errors by error type.",
		}, []string{"error_type"}),
	}

	registry.MustRegister(
		m.Ready,
		m.IsLeader,
		m.AliveWorkers,
		m.DeadWorkersTotal,
		m.ChannelsAssignedTotal,
		m.ChannelsUnassignedTotal,
		m.ChannelsPending,
		m.ChannelsAssigned,
		m.AssignCycleDurationSeconds,
		m.HealthcheckDurationSeconds,
		m.WorkerLoad,
		m.AssignErrorsTotal,
		m.HealthcheckErrorsTotal,
	)

	return registry, m
}

// ResetLeaderGauges resets all gauges that are only meaningful when this instance
// is the leader. Called on leadership loss or shutdown.
func (m *CoordinatorMetrics) ResetLeaderGauges() {
	m.IsLeader.Set(0)
	m.AliveWorkers.Set(0)
	m.WorkerLoad.Reset()
	m.ChannelsPending.Reset()
	m.ChannelsAssigned.Set(0)
}
