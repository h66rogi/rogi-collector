package store

import (
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type testRow struct {
	Value     int
	Timestamp time.Time
}

func extractTestRowTime(r testRow) time.Time {
	return r.Timestamp
}

func TestBatchWriter_FlushOnMaxBatch(t *testing.T) {
	maxBatch := 5
	var mu sync.Mutex
	var flushed []testRow

	flushFn := func(rows []testRow) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, rows...)
		return nil
	}

	w := NewBatchWriter(flushFn, maxBatch, 10*time.Second, extractTestRowTime, slog.Default(), nil)
	defer w.Close()

	now := time.Now()
	for i := 0; i < maxBatch; i++ {
		w.Enqueue(testRow{Value: i, Timestamp: now})
	}

	// Give the background goroutine a moment to process the flush signal
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	count := len(flushed)
	mu.Unlock()

	if count != maxBatch {
		t.Errorf("expected %d rows flushed, got %d", maxBatch, count)
	}
}

func TestBatchWriter_FlushOnClose(t *testing.T) {
	var mu sync.Mutex
	var flushed []testRow

	flushFn := func(rows []testRow) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, rows...)
		return nil
	}

	w := NewBatchWriter(flushFn, 100, 10*time.Second, extractTestRowTime, slog.Default(), nil)

	now := time.Now()
	w.Enqueue(testRow{Value: 1, Timestamp: now})

	w.Close()

	mu.Lock()
	count := len(flushed)
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 row flushed on close, got %d", count)
	}
}

func TestBatchWriter_RetainsAndRetriesAfterError(t *testing.T) {
	var mu sync.Mutex
	var flushed []testRow
	attempts := 0
	flushFn := func(rows []testRow) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts == 1 {
			return errors.New("simulated flush error")
		}
		flushed = append(flushed, rows...)
		return nil
	}

	w := NewBatchWriter(flushFn, 100, 10*time.Second, extractTestRowTime, slog.Default(), nil)

	now := time.Now()
	w.Enqueue(testRow{Value: 1, Timestamp: now})
	w.Enqueue(testRow{Value: 2, Timestamp: now})

	// The first flush fails, so the rows must remain pending.
	w.Flush()

	w.mu.Lock()
	bufLen := len(w.buffer)
	w.mu.Unlock()

	if bufLen != 2 {
		t.Fatalf("expected 2 retained rows after flush error, got %d", bufLen)
	}

	// A row arriving during the outage must remain ordered behind retained rows.
	w.Enqueue(testRow{Value: 3, Timestamp: now})

	// Close bypasses the retry timer and drains pending rows before shutdown.
	w.Close()

	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("expected 2 flush attempts, got %d", attempts)
	}
	if len(flushed) != 3 {
		t.Fatalf("expected 3 rows flushed after recovery, got %d", len(flushed))
	}
	for i, row := range flushed {
		if row.Value != i+1 {
			t.Errorf("row %d: expected value %d, got %d", i, i+1, row.Value)
		}
	}
}

func TestBatchWriter_FlushOnInterval(t *testing.T) {
	var mu sync.Mutex
	var flushed []testRow

	flushFn := func(rows []testRow) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, rows...)
		return nil
	}

	w := NewBatchWriter(flushFn, 100, 100*time.Millisecond, extractTestRowTime, slog.Default(), nil)
	defer w.Close()

	now := time.Now()
	w.Enqueue(testRow{Value: 42, Timestamp: now})

	// Wait longer than the flush interval
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	count := len(flushed)
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected 1 row flushed by interval, got %d", count)
	}
}
