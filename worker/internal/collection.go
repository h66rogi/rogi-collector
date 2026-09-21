package internal

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/h66rogi/rogi-collector/worker/internal/connector"
	"github.com/h66rogi/rogi-collector/worker/internal/pipeline"
)

// EnableDonations extends the existing Manager, never a second connection manager.
func (m *Manager) EnableDonations(pg *store.PgStore, spool *pipeline.DonationSpool) {
	m.collectionStore = pg
	m.donationSpool = spool
}
func (m *Manager) prepareCollection(ctx context.Context, stop context.CancelFunc, ch model.LiveChannel, conn connector.PlatformConnector) (func(), error) {
	if m.collectionStore == nil {
		return func() {}, nil
	}
	soop, ok := conn.(*connector.SoopConnector)
	if !ok {
		return nil, errors.New("unsupported product connector")
	}
	acquireStarted := time.Now()
	grant, err := m.collectionStore.AcquireCollection(ctx, ch.ChannelID, m.workerID)
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	// Keep a monotonic local deadline; wall-clock changes cannot extend authority.
	deadline := acquireStarted.Add(30 * time.Second)
	sessionCtx, cancel := context.WithCancel(ctx)
	soop.SetDonationSink(func(_ context.Context, d model.Donation) error {
		mu.Lock()
		g, until := grant, deadline
		mu.Unlock()
		if !time.Now().Before(until) {
			return store.ErrOwner
		}
		acceptCtx, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		if err := m.donationSpool.Save(acceptCtx, g, d, until); err != nil {
			_ = m.collectionStore.SetCollectionState(acceptCtx, ch.ChannelID, "storage_stopped")
			return err
		}
		return nil
	})
	go func() {
		timer := time.NewTicker(8 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case <-timer.C:
				updateCtx, done := context.WithTimeout(sessionCtx, 3*time.Second)
				mu.Lock()
				g := grant
				mu.Unlock()
				renewStarted := time.Now()
				until, err := m.collectionStore.RenewCollection(updateCtx, g)
				done()
				if err != nil {
					stop()
					_ = conn.Disconnect()
					_ = m.Disconnect(ch.Platform, ch.ChannelID)
					cancel()
					return
				}
				mu.Lock()
				grant.ValidUntil = until
				deadline = renewStarted.Add(30 * time.Second)
				mu.Unlock()
				state := "connecting"
				m.mu.RLock()
				_, connected := m.conns[connKey(ch.Platform, ch.ChannelID)]
				m.mu.RUnlock()
				if connected && soop.IsAlive() {
					state = "connected"
				}
				count, _, spoolErr := m.donationSpool.Backlog()
				if spoolErr != nil {
					state = "storage_stopped"
				} else if count > 0 {
					state = "storage_delayed"
				}
				_ = m.collectionStore.CollectionHeartbeat(sessionCtx, g, state, soop.LastReceived())
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			releaseCtx, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()
			mu.Lock()
			g := grant
			mu.Unlock()
			_ = m.collectionStore.ReleaseCollection(releaseCtx, g)
		})
	}, nil
}
