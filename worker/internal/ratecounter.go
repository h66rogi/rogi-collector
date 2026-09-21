package internal

import (
	"sync"
	"sync/atomic"
	"time"
)

// RateCounter tracks message rates using a snapshot-based approach.
// Call Add/AddForKey to count, then read rates via Rate/RateForKey.
// snapshot() must be called periodically (via StartSnapshotter) to compute rates.
type RateCounter struct {
	totalCount atomic.Int64
	totalPrev  int64
	totalRate  atomic.Value // stores float64

	mu       sync.RWMutex
	keys     map[string]*keyCounter
	lastSnap time.Time
}

type keyCounter struct {
	count atomic.Int64
	prev  int64
	rate  atomic.Value // stores float64
}

// NewRateCounter creates a RateCounter.
func NewRateCounter() *RateCounter {
	rc := &RateCounter{
		keys:     make(map[string]*keyCounter),
		lastSnap: time.Now(),
	}
	rc.totalRate.Store(float64(0))
	return rc
}

// Add increments the global message counter by n.
func (rc *RateCounter) Add(n int64) {
	rc.totalCount.Add(n)
}

// AddForKey increments both the global counter and the per-key counter.
func (rc *RateCounter) AddForKey(key string, n int64) {
	rc.totalCount.Add(n)
	rc.mu.RLock()
	kc, ok := rc.keys[key]
	rc.mu.RUnlock()
	if !ok {
		rc.mu.Lock()
		kc, ok = rc.keys[key]
		if !ok {
			kc = &keyCounter{}
			kc.rate.Store(float64(0))
			rc.keys[key] = kc
		}
		rc.mu.Unlock()
	}
	kc.count.Add(n)
}

// snapshot calculates rates from the delta since the last snapshot.
func (rc *RateCounter) snapshot() {
	now := time.Now()
	elapsed := now.Sub(rc.lastSnap).Seconds()
	if elapsed <= 0 {
		return
	}
	rc.lastSnap = now

	current := rc.totalCount.Load()
	delta := current - rc.totalPrev
	rc.totalPrev = current
	rc.totalRate.Store(float64(delta) / elapsed)

	rc.mu.RLock()
	defer rc.mu.RUnlock()
	for _, kc := range rc.keys {
		c := kc.count.Load()
		d := c - kc.prev
		kc.prev = c
		kc.rate.Store(float64(d) / elapsed)
	}
}

// Rate returns the current global message rate (messages per second).
func (rc *RateCounter) Rate() float64 {
	return rc.totalRate.Load().(float64)
}

// RateForKey returns the current message rate for a specific key.
func (rc *RateCounter) RateForKey(key string) float64 {
	rc.mu.RLock()
	kc, ok := rc.keys[key]
	rc.mu.RUnlock()
	if !ok {
		return 0
	}
	return kc.rate.Load().(float64)
}

// StartSnapshotter runs periodic snapshots in a loop until stop is closed.
func (rc *RateCounter) StartSnapshotter(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rc.snapshot()
		case <-stop:
			return
		}
	}
}
