package history

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/shared/store"
)

func newTestBatchWriter(maxBatch int, flushInterval time.Duration, fn func([]store.ViewerCountRow) error) *store.BatchWriter[store.ViewerCountRow] {
	return store.NewBatchWriter[store.ViewerCountRow](
		fn,
		maxBatch,
		flushInterval,
		func(r store.ViewerCountRow) time.Time { return r.ValidFrom },
		nil,
		nil,
	)
}

func TestBatchWriter_FlushOnMaxBatch(t *testing.T) {
	var mu sync.Mutex
	var flushed []store.ViewerCountRow
	mockFlush := func(rows []store.ViewerCountRow) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, rows...)
		return nil
	}

	w := newTestBatchWriter(2, time.Hour, mockFlush)
	defer w.Close()

	w.Enqueue(store.ViewerCountRow{ChannelID: "a", ViewerCount: 100})
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if len(flushed) != 0 {
		t.Errorf("should not flush yet, got %d", len(flushed))
	}
	mu.Unlock()

	w.Enqueue(store.ViewerCountRow{ChannelID: "b", ViewerCount: 200})
	time.Sleep(100 * time.Millisecond) // allow goroutine to process
	mu.Lock()
	defer mu.Unlock()
	if len(flushed) != 2 {
		t.Errorf("expected 2 flushed, got %d", len(flushed))
	}
}

func TestBatchWriter_FlushOnClose(t *testing.T) {
	var flushed []store.ViewerCountRow
	mockFlush := func(rows []store.ViewerCountRow) error {
		flushed = append(flushed, rows...)
		return nil
	}

	w := newTestBatchWriter(100, time.Hour, mockFlush)
	w.Enqueue(store.ViewerCountRow{ChannelID: "a", ViewerCount: 100})
	w.Close()

	if len(flushed) != 1 {
		t.Errorf("expected 1 flushed on close, got %d", len(flushed))
	}
}

func TestBatchWriter_RetainsOnError(t *testing.T) {
	var mu sync.Mutex
	callCount := 0
	mockFlush := func(rows []store.ViewerCountRow) error {
		mu.Lock()
		defer mu.Unlock()
		callCount++
		if callCount == 1 {
			return errors.New("connection refused")
		}
		return nil
	}

	w := newTestBatchWriter(100, time.Hour, mockFlush)

	w.Enqueue(store.ViewerCountRow{ChannelID: "a", ViewerCount: 100})

	// The first flush fails and the row is retained.
	w.Flush()
	// Close bypasses backoff and drains the retained row.
	w.Close()

	mu.Lock()
	defer mu.Unlock()
	if callCount != 2 {
		t.Errorf("expected 2 flush calls (failure then retry), got %d", callCount)
	}
}
