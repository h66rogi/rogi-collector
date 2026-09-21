package store

import (
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type BatchWriterMetrics struct {
	FlushTotal   *prometheus.CounterVec // labels: "status" (success/error)
	BatchSize    prometheus.Histogram
	DroppedTotal prometheus.Counter
	RetryTotal   prometheus.Counter
	Pending      prometheus.Gauge
}

type BatchWriter[T any] struct {
	flushFn     func(rows []T) error
	buffer      []T
	inFlight    int
	mu          sync.Mutex
	flushMu     sync.Mutex
	maxBatch    int
	maxPending  int
	flushCh     chan struct{}
	done        chan struct{}
	stopped     chan struct{}
	closed      bool
	retryDelay  time.Duration
	nextRetryAt time.Time
	logger      *slog.Logger
	metrics     *BatchWriterMetrics
	extractTime func(T) time.Time
}

func NewBatchWriter[T any](
	flushFn func(rows []T) error,
	maxBatch int,
	flushInterval time.Duration,
	extractTime func(T) time.Time,
	logger *slog.Logger,
	metrics *BatchWriterMetrics,
) *BatchWriter[T] {
	return newBatchWriter(flushFn, maxBatch, 0, flushInterval, extractTime, logger, metrics)
}

// NewBoundedBatchWriter creates a writer whose in-memory retry queue is capped.
// Once the cap is reached, new rows are dropped and counted instead of allowing
// an optional secondary sink outage to exhaust worker memory.
func NewBoundedBatchWriter[T any](
	flushFn func(rows []T) error,
	maxBatch int,
	maxPending int,
	flushInterval time.Duration,
	extractTime func(T) time.Time,
	logger *slog.Logger,
	metrics *BatchWriterMetrics,
) *BatchWriter[T] {
	return newBatchWriter(flushFn, maxBatch, maxPending, flushInterval, extractTime, logger, metrics)
}

func newBatchWriter[T any](
	flushFn func(rows []T) error,
	maxBatch int,
	maxPending int,
	flushInterval time.Duration,
	extractTime func(T) time.Time,
	logger *slog.Logger,
	metrics *BatchWriterMetrics,
) *BatchWriter[T] {
	if logger == nil {
		logger = slog.Default()
	}
	w := &BatchWriter[T]{
		flushFn:     flushFn,
		maxBatch:    maxBatch,
		maxPending:  maxPending,
		flushCh:     make(chan struct{}, 1),
		done:        make(chan struct{}),
		stopped:     make(chan struct{}),
		logger:      logger,
		metrics:     metrics,
		extractTime: extractTime,
	}
	go w.loop(flushInterval)
	return w
}

func (w *BatchWriter[T]) Enqueue(row T) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	if w.maxPending > 0 && len(w.buffer)+w.inFlight >= w.maxPending {
		w.mu.Unlock()
		if w.metrics != nil && w.metrics.DroppedTotal != nil {
			w.metrics.DroppedTotal.Inc()
		}
		return
	}
	w.buffer = append(w.buffer, row)
	shouldFlush := len(w.buffer) >= w.maxBatch
	w.setPendingMetricLocked()
	w.mu.Unlock()

	if shouldFlush {
		select {
		case w.flushCh <- struct{}{}:
		default:
		}
	}
}

func (w *BatchWriter[T]) Flush() {
	_ = w.flush(false)
}

func (w *BatchWriter[T]) flush(force bool) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if !force && time.Now().Before(w.nextRetryAt) {
		return nil
	}

	w.mu.Lock()
	rows := w.buffer
	w.buffer = nil
	w.inFlight = len(rows)
	w.setPendingMetricLocked()
	w.mu.Unlock()

	if len(rows) == 0 {
		return nil
	}

	if err := w.flushFn(rows); err != nil {
		w.mu.Lock()
		retained := make([]T, 0, len(rows)+len(w.buffer))
		retained = append(retained, rows...)
		retained = append(retained, w.buffer...)
		w.buffer = retained
		w.inFlight = 0
		w.setPendingMetricLocked()
		w.mu.Unlock()

		if w.retryDelay == 0 {
			w.retryDelay = time.Second
		} else {
			w.retryDelay *= 2
			if w.retryDelay > 30*time.Second {
				w.retryDelay = 30 * time.Second
			}
		}
		w.nextRetryAt = time.Now().Add(w.retryDelay)

		oldest := w.extractTime(rows[0])
		newest := w.extractTime(rows[len(rows)-1])
		w.logger.Error("batch flush failed; retained for retry",
			"count", len(rows),
			"oldest", oldest.Format(time.RFC3339Nano),
			"newest", newest.Format(time.RFC3339Nano),
			"retry_after", w.retryDelay,
			"error", err,
		)
		if w.metrics != nil {
			if w.metrics.FlushTotal != nil {
				w.metrics.FlushTotal.WithLabelValues("error").Inc()
			}
			if w.metrics.RetryTotal != nil {
				w.metrics.RetryTotal.Inc()
			}
		}
		return err
	}

	w.retryDelay = 0
	w.nextRetryAt = time.Time{}
	w.mu.Lock()
	w.inFlight = 0
	w.setPendingMetricLocked()
	w.mu.Unlock()

	if w.metrics != nil {
		if w.metrics.FlushTotal != nil {
			w.metrics.FlushTotal.WithLabelValues("success").Inc()
		}
		if w.metrics.BatchSize != nil {
			w.metrics.BatchSize.Observe(float64(len(rows)))
		}
	}
	return nil
}

func (w *BatchWriter[T]) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()

	close(w.done)
	<-w.stopped

	deadline := time.Now().Add(60 * time.Second)
	for {
		w.mu.Lock()
		pending := len(w.buffer)
		w.mu.Unlock()
		if pending == 0 {
			return
		}

		if err := w.flush(true); err == nil {
			continue
		}
		if time.Now().After(deadline) {
			w.logger.Error("batch dropped after shutdown drain timeout", "count", pending)
			if w.metrics != nil && w.metrics.DroppedTotal != nil {
				w.metrics.DroppedTotal.Add(float64(pending))
			}
			return
		}

		delay := w.retryDelay
		if delay <= 0 {
			delay = time.Second
		}
		remaining := time.Until(deadline)
		if delay > remaining {
			delay = remaining
		}
		time.Sleep(delay)
	}
}

func (w *BatchWriter[T]) loop(flushInterval time.Duration) {
	defer close(w.stopped)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-w.flushCh:
			w.Flush()
		case <-ticker.C:
			w.Flush()
		}
	}
}

func (w *BatchWriter[T]) setPendingMetricLocked() {
	if w.metrics != nil && w.metrics.Pending != nil {
		w.metrics.Pending.Set(float64(len(w.buffer) + w.inFlight))
	}
}
