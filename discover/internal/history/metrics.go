package history

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	SessionsCreatedTotal   *prometheus.CounterVec
	SessionsClosedTotal    *prometheus.CounterVec
	MetadataChangesTotal   *prometheus.CounterVec
	PGWriteDuration        *prometheus.HistogramVec
	ViewerIntervalsTotal   *prometheus.CounterVec
	CHBatchFlushTotal      *prometheus.CounterVec
	CHBatchSize            prometheus.Histogram
	CHWriteDuration        prometheus.Histogram
	CHBufferLength         prometheus.Gauge
	GraceSuppressionsTotal *prometheus.CounterVec
	RecoveryDuration       prometheus.Histogram
	RecoveredSessionsTotal prometheus.Counter
	GapDetectedTotal       *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		SessionsCreatedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_sessions_created_total",
		}, []string{"platform"}),
		SessionsClosedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_sessions_closed_total",
		}, []string{"platform", "close_reason"}),
		MetadataChangesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_metadata_changes_total",
		}, []string{"platform"}),
		PGWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "history_pg_write_duration_seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),
		ViewerIntervalsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_viewer_intervals_total",
		}, []string{"platform"}),
		CHBatchFlushTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_ch_batch_flush_total",
		}, []string{"status"}),
		CHBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "history_ch_batch_size",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
		}),
		CHWriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "history_ch_write_duration_seconds",
			Buckets: prometheus.DefBuckets,
		}),
		CHBufferLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "history_ch_buffer_length",
		}),
		GraceSuppressionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_grace_suppressions_total",
		}, []string{"platform"}),
		RecoveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "history_state_recovery_duration_seconds",
			Buckets: prometheus.DefBuckets,
		}),
		RecoveredSessionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "history_recovered_sessions_total",
		}),
		GapDetectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "history_gap_detected_total",
		}, []string{"platform"}),
	}

	reg.MustRegister(
		m.SessionsCreatedTotal, m.SessionsClosedTotal, m.MetadataChangesTotal,
		m.PGWriteDuration, m.ViewerIntervalsTotal, m.CHBatchFlushTotal,
		m.CHBatchSize, m.CHWriteDuration, m.CHBufferLength,
		m.GraceSuppressionsTotal, m.RecoveryDuration, m.RecoveredSessionsTotal,
		m.GapDetectedTotal,
	)
	return m
}
