package history

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/discover/internal/discovery"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5"
)

// Ensure pgx is used (tests pass nil for tx).
var _ pgx.Tx = nil

// fakeHistoryStore implements store.HistoryStore for unit tests.
type fakeHistoryStore struct {
	sessions         []store.BroadcastSession
	metadata         []store.MetadataHistoryRow
	openSessions     []store.BroadcastSession
	hashByKey        map[string][]byte // "platform:channelID" -> hash
	viewerCountByKey map[string]int    // "platform:channelID" -> viewer_count
	closedSessions   int
	closedMeta       int
	gapMarked        int
	lastSeenCalls    int
	lastCloseReason  string
	orphansClosed    int
}

func newFakeHistoryStore() *fakeHistoryStore {
	return &fakeHistoryStore{
		hashByKey:        make(map[string][]byte),
		viewerCountByKey: make(map[string]int),
	}
}

func (f *fakeHistoryStore) InsertSession(_ context.Context, _ pgx.Tx, s *store.BroadcastSession) error {
	f.sessions = append(f.sessions, *s)
	return nil
}

func (f *fakeHistoryStore) CloseSession(_ context.Context, _ pgx.Tx, _ model.Platform, _ string, _ int64, _, _ time.Time, reason string) error {
	f.closedSessions++
	f.lastCloseReason = reason
	return nil
}

func (f *fakeHistoryStore) UpdateSessionLastSeen(_ context.Context, _ pgx.Tx, _ model.Platform, _ string, _ int64, _ time.Time, _ int) error {
	f.lastSeenCalls++
	return nil
}

func (f *fakeHistoryStore) MarkSessionGap(_ context.Context, _ model.Platform, _ string, _ int64) error {
	f.gapMarked++
	return nil
}

func (f *fakeHistoryStore) ListOpenSessions(_ context.Context) ([]store.BroadcastSession, error) {
	return f.openSessions, nil
}

func (f *fakeHistoryStore) CloseOrphanedSessions(_ context.Context) (int, error) {
	f.orphansClosed++
	return f.orphansClosed, nil
}

func (f *fakeHistoryStore) InsertMetadata(_ context.Context, _ pgx.Tx, m *store.MetadataHistoryRow) error {
	f.metadata = append(f.metadata, *m)
	return nil
}

func (f *fakeHistoryStore) CloseMetadata(_ context.Context, _ pgx.Tx, _ model.Platform, _ string, _ int64, _ time.Time) error {
	f.closedMeta++
	return nil
}

func (f *fakeHistoryStore) GetCurrentMetadataHash(_ context.Context, platform model.Platform, channelID string, sessionSeq int64) ([]byte, error) {
	key := string(platform) + ":" + channelID
	if h, ok := f.hashByKey[key]; ok {
		return h, nil
	}
	return nil, nil
}

func (f *fakeHistoryStore) GetChannelViewerCount(_ context.Context, platform model.Platform, channelID string) (int, error) {
	key := string(platform) + ":" + channelID
	if v, ok := f.viewerCountByKey[key]; ok {
		return v, nil
	}
	return 0, fmt.Errorf("channel not found")
}

func (f *fakeHistoryStore) ListChannelBroadcastHistory(_ context.Context, _ model.Platform, _ string, _ int, _ int) ([]store.BroadcastSession, int, error) {
	return nil, 0, nil
}

func (f *fakeHistoryStore) ListChannelBroadcastHistoryByRange(_ context.Context, _ model.Platform, _ string, _, _ time.Time, _ int) ([]store.BroadcastSession, int, error) {
	return nil, 0, nil
}

var _ store.HistoryStore = (*fakeHistoryStore)(nil)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestComputeMetadataHash(t *testing.T) {
	t.Run("same input produces same hash", func(t *testing.T) {
		h1 := computeMetadataHash("title", "category", []string{"a", "b"})
		h2 := computeMetadataHash("title", "category", []string{"a", "b"})
		if !bytes.Equal(h1, h2) {
			t.Fatalf("expected identical hashes for identical input")
		}
	})

	t.Run("different title produces different hash", func(t *testing.T) {
		h1 := computeMetadataHash("title1", "category", []string{"a"})
		h2 := computeMetadataHash("title2", "category", []string{"a"})
		if bytes.Equal(h1, h2) {
			t.Fatalf("expected different hashes for different titles")
		}
	})

	t.Run("tag order is irrelevant", func(t *testing.T) {
		h1 := computeMetadataHash("t", "c", []string{"b", "a", "c"})
		h2 := computeMetadataHash("t", "c", []string{"c", "a", "b"})
		if !bytes.Equal(h1, h2) {
			t.Fatalf("expected identical hashes regardless of tag order")
		}
	})

	t.Run("does not mutate input slice", func(t *testing.T) {
		tags := []string{"z", "a", "m"}
		computeMetadataHash("t", "c", tags)
		if tags[0] != "z" || tags[1] != "a" || tags[2] != "m" {
			t.Fatalf("computeMetadataHash mutated the input slice: %v", tags)
		}
	})

	t.Run("hash length is 32 bytes (SHA256)", func(t *testing.T) {
		h := computeMetadataHash("t", "c", nil)
		if len(h) != 32 {
			t.Fatalf("expected 32-byte hash, got %d", len(h))
		}
	})
}

func TestProcessDiscovered_NewChannel_CreatesSession(t *testing.T) {
	fs := newFakeHistoryStore()
	var flushed []store.ViewerCountRow
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		flushed = append(flushed, rows...)
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	results := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"ch1": {
			ChannelID: "ch1", StreamerName: "Streamer1", ViewerCount: 500,
			Title: "Hello", Category: "Gaming", CategoryCode: "game",
			Tags: []string{"tag1"}, ThumbnailURL: "http://thumb",
		},
	}

	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results, discovered)

	// Verify session was created
	if len(fs.sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(fs.sessions))
	}
	s := fs.sessions[0]
	if s.ChannelID != "ch1" || s.StreamerName != "Streamer1" || s.PeakViewerCount != 500 {
		t.Errorf("unexpected session: %+v", s)
	}

	// Verify metadata was created
	if len(fs.metadata) != 1 {
		t.Fatalf("expected 1 metadata row, got %d", len(fs.metadata))
	}
	m := fs.metadata[0]
	if m.Title != "Hello" || m.Category != "Gaming" {
		t.Errorf("unexpected metadata: %+v", m)
	}

	// Verify in-memory state exists
	em.mu.RLock()
	state, ok := em.states["chzzk:ch1"]
	em.mu.RUnlock()
	if !ok {
		t.Fatal("expected state for chzzk:ch1")
	}
	if state.sessionSeq != 1 || state.viewerCount != 500 {
		t.Errorf("unexpected state: sessionSeq=%d viewerCount=%d", state.sessionSeq, state.viewerCount)
	}
	if state.lastInterval == nil {
		t.Fatal("expected lastInterval to be set")
	}
	if state.lastInterval.sampleCount != 1 {
		t.Errorf("expected sampleCount=1, got %d", state.lastInterval.sampleCount)
	}
}

func TestProcessDiscovered_ExistingChannel_MetadataChange(t *testing.T) {
	fs := newFakeHistoryStore()
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	// First: create the channel
	results := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "Title1", Category: "Cat1", Tags: []string{"t1"}},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results, discovered)

	// Second: same channel, different title
	results2 := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: false},
	}
	discovered2 := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "Title2", Category: "Cat1", Tags: []string{"t1"}},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results2, discovered2)

	// Should have: 1 session, 2 metadata inserts, 1 metadata close
	if len(fs.sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(fs.sessions))
	}
	if len(fs.metadata) != 2 {
		t.Errorf("expected 2 metadata rows, got %d", len(fs.metadata))
	}
	if fs.closedMeta != 1 {
		t.Errorf("expected 1 closed metadata, got %d", fs.closedMeta)
	}

	// Verify new hash stored in state
	em.mu.RLock()
	state := em.states["chzzk:ch1"]
	em.mu.RUnlock()
	expectedHash := computeMetadataHash("Title2", "Cat1", []string{"t1"})
	if !bytes.Equal(state.metadataHash, expectedHash) {
		t.Error("state metadataHash not updated after change")
	}
}

func TestProcessDiscovered_ExistingChannel_ViewerCountChange(t *testing.T) {
	fs := newFakeHistoryStore()
	var flushed []store.ViewerCountRow
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		flushed = append(flushed, rows...)
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	// Create channel with viewer count 100
	results := []store.UpsertResult{
		{Platform: model.PlatformSoop, ChannelID: "s1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"s1": {ChannelID: "s1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformSoop, results, discovered)

	// Update with same viewer count -- should just increment sampleCount
	results2 := []store.UpsertResult{
		{Platform: model.PlatformSoop, ChannelID: "s1", SessionSeq: 1, IsInsert: false},
	}
	discovered2 := map[string]discovery.DiscoveredChannel{
		"s1": {ChannelID: "s1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformSoop, results2, discovered2)

	em.mu.RLock()
	state := em.states["soop:s1"]
	em.mu.RUnlock()
	if state.lastInterval.sampleCount != 2 {
		t.Errorf("expected sampleCount=2, got %d", state.lastInterval.sampleCount)
	}

	// Now change viewer count to 200
	discovered3 := map[string]discovery.DiscoveredChannel{
		"s1": {ChannelID: "s1", StreamerName: "S", ViewerCount: 200,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformSoop, results2, discovered3)

	// The old interval should have been enqueued to the batch writer
	w.Flush()
	if len(flushed) != 1 {
		t.Fatalf("expected 1 flushed interval, got %d", len(flushed))
	}
	row := flushed[0]
	if row.ViewerCount != 100 {
		t.Errorf("expected flushed viewerCount=100, got %d", row.ViewerCount)
	}
	if row.SampleCount != 2 {
		t.Errorf("expected flushed sampleCount=2, got %d", row.SampleCount)
	}
	if row.ValidTo == nil {
		t.Fatal("expected ValidTo to be set")
	}

	// State should now reflect viewer count 200
	em.mu.RLock()
	state = em.states["soop:s1"]
	em.mu.RUnlock()
	if state.viewerCount != 200 {
		t.Errorf("expected viewerCount=200, got %d", state.viewerCount)
	}
	if state.lastInterval.viewerCount != 200 {
		t.Errorf("expected new interval viewerCount=200, got %d", state.lastInterval.viewerCount)
	}
}

func TestDetectEndedChannels_GraceSuppression(t *testing.T) {
	fs := newFakeHistoryStore()
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	// Manually set up state for a channel
	em.mu.Lock()
	em.states["chzzk:ch1"] = &channelState{
		sessionSeq:  1,
		viewerCount: 100,
		missedPolls: 0,
	}
	em.mu.Unlock()

	// First miss (non-partial): should be grace-protected
	augmented, toEnd := em.DetectEndedChannels(model.PlatformChzzk, []string{}, false)

	// ch1 should be in the augmented list (grace-protected)
	found := false
	for _, id := range augmented {
		if id == "ch1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ch1 in augmented list during first miss (grace)")
	}
	if len(toEnd) != 0 {
		t.Errorf("expected 0 toEnd after first miss, got %d", len(toEnd))
	}
	if fs.closedSessions != 0 {
		t.Errorf("expected 0 closed sessions after first miss, got %d", fs.closedSessions)
	}

	// Second miss (non-partial): grace expired, should be in toEnd
	augmented2, toEnd2 := em.DetectEndedChannels(model.PlatformChzzk, []string{}, false)

	// ch1 should NOT be in augmented list
	for _, id := range augmented2 {
		if id == "ch1" {
			t.Fatal("ch1 should not be in augmented list after grace expired")
		}
	}
	// ch1 should be in toEnd list (but not yet closed)
	if len(toEnd2) != 1 {
		t.Fatalf("expected 1 toEnd after second miss, got %d", len(toEnd2))
	}
	if toEnd2[0].ChannelID != "ch1" {
		t.Errorf("expected toEnd channelID=ch1, got %s", toEnd2[0].ChannelID)
	}
	// Session should NOT be closed yet (before CommitEnded)
	if fs.closedSessions != 0 {
		t.Errorf("expected 0 closed sessions before CommitEnded, got %d", fs.closedSessions)
	}

	// State should still exist before CommitEnded
	em.mu.RLock()
	_, exists := em.states["chzzk:ch1"]
	em.mu.RUnlock()
	if !exists {
		t.Fatal("expected state to still exist before CommitEnded")
	}

	// Now commit the ended channels
	em.CommitEnded(context.Background(), toEnd2)

	if fs.closedSessions != 1 {
		t.Errorf("expected 1 closed session after CommitEnded, got %d", fs.closedSessions)
	}
	if fs.lastCloseReason != "grace_timeout" {
		t.Errorf("expected close_reason=grace_timeout, got %s", fs.lastCloseReason)
	}

	// State should be removed after CommitEnded
	em.mu.RLock()
	_, exists = em.states["chzzk:ch1"]
	em.mu.RUnlock()
	if exists {
		t.Fatal("expected state to be deleted after CommitEnded")
	}
}

func TestDetectEndedChannels_PartialSkip(t *testing.T) {
	fs := newFakeHistoryStore()
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	em.mu.Lock()
	em.states["chzzk:ch1"] = &channelState{
		sessionSeq:  1,
		viewerCount: 100,
		missedPolls: 0,
	}
	em.mu.Unlock()

	// Partial result: ch1 is not in activeIDs but partial=true
	augmented, toEnd := em.DetectEndedChannels(model.PlatformChzzk, []string{}, true)

	// ch1 should be augmented (preserved)
	found := false
	for _, id := range augmented {
		if id == "ch1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ch1 in augmented list during partial poll")
	}

	// No channels should be in toEnd
	if len(toEnd) != 0 {
		t.Errorf("expected 0 toEnd during partial, got %d", len(toEnd))
	}

	// missedPolls should NOT have incremented
	em.mu.RLock()
	state := em.states["chzzk:ch1"]
	em.mu.RUnlock()
	if state.missedPolls != 0 {
		t.Errorf("expected missedPolls=0 after partial, got %d", state.missedPolls)
	}

	// No sessions should be closed
	if fs.closedSessions != 0 {
		t.Errorf("expected 0 closed sessions during partial, got %d", fs.closedSessions)
	}
}

func TestRecoverState_LoadsOpenSessions(t *testing.T) {
	fs := newFakeHistoryStore()
	hash := computeMetadataHash("T", "C", []string{"a"})
	fs.openSessions = []store.BroadcastSession{
		{
			Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 5,
			PeakViewerCount: 300, LastSeenAt: time.Now().Add(-10 * time.Second),
		},
		{
			Platform: model.PlatformSoop, ChannelID: "s1", SessionSeq: 2,
			PeakViewerCount: 150, LastSeenAt: time.Now().Add(-10 * time.Second),
		},
	}
	fs.hashByKey["chzzk:ch1"] = hash
	// Set live_channels viewer counts (different from PeakViewerCount)
	fs.viewerCountByKey["chzzk:ch1"] = 250
	// s1 has no viewer count entry — should fall back to PeakViewerCount

	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)
	if err := em.RecoverState(context.Background()); err != nil {
		t.Fatalf("RecoverState failed: %v", err)
	}

	em.mu.RLock()
	defer em.mu.RUnlock()

	if len(em.states) != 2 {
		t.Fatalf("expected 2 recovered states, got %d", len(em.states))
	}

	st1 := em.states["chzzk:ch1"]
	if st1 == nil {
		t.Fatal("expected state for chzzk:ch1")
	}
	if st1.sessionSeq != 5 {
		t.Errorf("expected sessionSeq=5, got %d", st1.sessionSeq)
	}
	if !bytes.Equal(st1.metadataHash, hash) {
		t.Error("expected recovered metadataHash to match")
	}
	if st1.lastInterval != nil {
		t.Error("expected lastInterval to be nil after recovery")
	}
	// Should use live_channels viewer_count (250), not PeakViewerCount (300)
	if st1.viewerCount != 250 {
		t.Errorf("expected viewerCount=250 from live_channels, got %d", st1.viewerCount)
	}

	st2 := em.states["soop:s1"]
	if st2 == nil {
		t.Fatal("expected state for soop:s1")
	}
	if st2.sessionSeq != 2 {
		t.Errorf("expected sessionSeq=2, got %d", st2.sessionSeq)
	}
	// Should fall back to PeakViewerCount (150) since live_channels has no entry
	if st2.viewerCount != 150 {
		t.Errorf("expected viewerCount=150 (fallback), got %d", st2.viewerCount)
	}
}

func TestRecoverState_GapDetection(t *testing.T) {
	fs := newFakeHistoryStore()
	// Session with a large gap (> 3 * 60s = 180s)
	fs.openSessions = []store.BroadcastSession{
		{
			Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1,
			PeakViewerCount: 100, LastSeenAt: time.Now().Add(-5 * time.Minute),
		},
	}

	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)
	if err := em.RecoverState(context.Background()); err != nil {
		t.Fatalf("RecoverState failed: %v", err)
	}

	if fs.gapMarked != 1 {
		t.Errorf("expected 1 gap marked, got %d", fs.gapMarked)
	}
}

func TestRecoverState_ClosesOrphanedSessions(t *testing.T) {
	fs := newFakeHistoryStore()
	fs.openSessions = []store.BroadcastSession{
		{
			Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 3,
			PeakViewerCount: 100, LastSeenAt: time.Now().Add(-10 * time.Second),
		},
	}

	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)
	if err := em.RecoverState(context.Background()); err != nil {
		t.Fatalf("RecoverState failed: %v", err)
	}

	if fs.orphansClosed != 1 {
		t.Errorf("expected CloseOrphanedSessions called once, got %d", fs.orphansClosed)
	}
}

func TestRecoverState_ClosesDuplicateSessions(t *testing.T) {
	fs := newFakeHistoryStore()
	fs.openSessions = []store.BroadcastSession{
		{
			Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 7,
			PeakViewerCount: 200, LastSeenAt: time.Now().Add(-10 * time.Second),
		},
		{
			Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 5,
			PeakViewerCount: 100, LastSeenAt: time.Now().Add(-1 * time.Hour),
		},
	}

	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)
	if err := em.RecoverState(context.Background()); err != nil {
		t.Fatalf("RecoverState failed: %v", err)
	}

	em.mu.RLock()
	defer em.mu.RUnlock()

	if len(em.states) != 1 {
		t.Fatalf("expected 1 state (newest), got %d", len(em.states))
	}
	st := em.states["chzzk:ch1"]
	if st == nil {
		t.Fatal("expected state for chzzk:ch1")
	}
	if st.sessionSeq != 7 {
		t.Errorf("expected newest sessionSeq=7, got %d", st.sessionSeq)
	}

	if fs.closedSessions != 1 {
		t.Errorf("expected 1 closed duplicate session, got %d", fs.closedSessions)
	}
	if fs.lastCloseReason != "duplicate_cleanup" {
		t.Errorf("expected close_reason=duplicate_cleanup, got %s", fs.lastCloseReason)
	}
}

func TestFlushAll_DrainsPendingIntervals(t *testing.T) {
	fs := newFakeHistoryStore()
	var flushed []store.ViewerCountRow
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		flushed = append(flushed, rows...)
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)
	now := time.Now()

	em.mu.Lock()
	em.states["chzzk:ch1"] = &channelState{
		sessionSeq: 1, viewerCount: 100,
		lastInterval: &viewerInterval{
			validFrom: now, viewerCount: 100,
			minCount: 80, maxCount: 120, sampleCount: 5,
		},
	}
	em.states["soop:s1"] = &channelState{
		sessionSeq: 2, viewerCount: 200,
		lastInterval: &viewerInterval{
			validFrom: now, viewerCount: 200,
			minCount: 180, maxCount: 220, sampleCount: 3,
		},
	}
	// Channel with nil interval should be skipped
	em.states["chzzk:ch2"] = &channelState{
		sessionSeq: 3, viewerCount: 50,
		lastInterval: nil,
	}
	em.mu.Unlock()

	em.FlushAll()
	w.Flush()

	if len(flushed) != 2 {
		t.Fatalf("expected 2 flushed intervals, got %d", len(flushed))
	}

	// Sort flushed rows by channelID for deterministic assertion
	sort.Slice(flushed, func(i, j int) bool {
		return flushed[i].ChannelID < flushed[j].ChannelID
	})

	r1 := flushed[0]
	if r1.ChannelID != "ch1" || r1.ViewerCount != 100 || r1.SampleCount != 5 {
		t.Errorf("unexpected row 0: %+v", r1)
	}
	if r1.MinCount != 80 || r1.MaxCount != 120 {
		t.Errorf("unexpected min/max for row 0: min=%d max=%d", r1.MinCount, r1.MaxCount)
	}

	r2 := flushed[1]
	if r2.ChannelID != "s1" || r2.ViewerCount != 200 || r2.SampleCount != 3 {
		t.Errorf("unexpected row 1: %+v", r2)
	}

	// After FlushAll, all lastInterval should be nil
	em.mu.RLock()
	for key, state := range em.states {
		if state.lastInterval != nil {
			t.Errorf("expected nil lastInterval for %s after FlushAll", key)
		}
	}
	em.mu.RUnlock()
}

func TestProcessDiscovered_ForceFlushStaleInterval(t *testing.T) {
	fs := newFakeHistoryStore()
	var flushed []store.ViewerCountRow
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		flushed = append(flushed, rows...)
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	// Create channel with viewer count 100
	results := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results, discovered)

	// Manually backdate the interval's validFrom to 6 minutes ago
	em.mu.Lock()
	state := em.states["chzzk:ch1"]
	state.lastInterval.validFrom = time.Now().Add(-6 * time.Minute)
	state.lastInterval.sampleCount = 5
	em.mu.Unlock()

	// Process same viewer count — should trigger force-flush due to stale interval
	results2 := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: false},
	}
	discovered2 := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results2, discovered2)

	w.Flush()

	// The stale interval should have been flushed
	if len(flushed) != 1 {
		t.Fatalf("expected 1 flushed interval (force-flush), got %d", len(flushed))
	}
	if flushed[0].SampleCount != 5 {
		t.Errorf("expected flushed sampleCount=5, got %d", flushed[0].SampleCount)
	}

	// New interval should have been started with sampleCount=1
	em.mu.RLock()
	state = em.states["chzzk:ch1"]
	em.mu.RUnlock()
	if state.lastInterval == nil {
		t.Fatal("expected new lastInterval after force-flush")
	}
	if state.lastInterval.sampleCount != 1 {
		t.Errorf("expected new interval sampleCount=1, got %d", state.lastInterval.sampleCount)
	}
}

func TestProcessDiscovered_NoForceFlushFreshInterval(t *testing.T) {
	fs := newFakeHistoryStore()
	var flushed []store.ViewerCountRow
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		flushed = append(flushed, rows...)
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	// Create channel
	results := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results, discovered)

	// Process same viewer count immediately (interval is fresh, < 5 min)
	results2 := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: false},
	}
	discovered2 := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results2, discovered2)

	w.Flush()

	// No intervals should have been flushed (interval is still fresh)
	if len(flushed) != 0 {
		t.Errorf("expected 0 flushed intervals for fresh interval, got %d", len(flushed))
	}

	// sampleCount should have incremented
	em.mu.RLock()
	state := em.states["chzzk:ch1"]
	em.mu.RUnlock()
	if state.lastInterval.sampleCount != 2 {
		t.Errorf("expected sampleCount=2, got %d", state.lastInterval.sampleCount)
	}
}

func TestProcessDiscovered_UsesPlatformStartedAt(t *testing.T) {
	fs := newFakeHistoryStore()
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	platformTime := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	results := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil, StartedAt: &platformTime},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results, discovered)

	if len(fs.sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(fs.sessions))
	}
	s := fs.sessions[0]
	if !s.StartedAt.Equal(platformTime) {
		t.Errorf("expected StartedAt=%v (platform time), got %v", platformTime, s.StartedAt)
	}
	// StartedObservedAt should still be approximately now
	if time.Since(s.StartedObservedAt) > 5*time.Second {
		t.Errorf("expected StartedObservedAt to be ~now, got %v", s.StartedObservedAt)
	}
}

func TestProcessDiscovered_FallsBackToNowWhenNoStartedAt(t *testing.T) {
	fs := newFakeHistoryStore()
	w := newTestBatchWriter(100, time.Hour, func(rows []store.ViewerCountRow) error {
		return nil
	})
	defer w.Close()

	em := NewEmitter(fs, w, "inst-1", nil)

	results := []store.UpsertResult{
		{Platform: model.PlatformChzzk, ChannelID: "ch1", SessionSeq: 1, IsInsert: true},
	}
	discovered := map[string]discovery.DiscoveredChannel{
		"ch1": {ChannelID: "ch1", StreamerName: "S", ViewerCount: 100,
			Title: "T", Category: "C", Tags: nil, StartedAt: nil},
	}
	em.ProcessDiscovered(context.Background(), model.PlatformChzzk, results, discovered)

	if len(fs.sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(fs.sessions))
	}
	s := fs.sessions[0]
	// Without StartedAt, should use time.Now()
	if time.Since(s.StartedAt) > 5*time.Second {
		t.Errorf("expected StartedAt to be ~now when no platform time, got %v", s.StartedAt)
	}
}
