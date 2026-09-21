package store_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/h66rogi/rogi-collector/shared/testutil"
	"sync"
	"testing"
	"time"
)

func donation(g store.OwnerGrant, id int, epoch string) model.Donation {
	return model.Donation{EventID: fmt.Sprintf("%s-%d", epoch, id), ChannelID: g.Channel, Count: 33, Kind: "balloon", DonorID: "fixture_donor", ConnectionEpoch: epoch, Sequence: uint64(id), ObservedAt: time.Now().UTC(), ConnectionStartedAt: time.Now().UTC().Add(-time.Second)}
}
func TestCollectorJournalReplayAndRecovery(t *testing.T) {
	pg, pool, g := testutil.Collector(t)
	ctx := context.Background()
	const count = 24
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := pg.AppendDonation(ctx, g, donation(g, id, "epoch-a"), time.Now())
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, state, err := pg.ReadDonations(ctx, "fixture_consumer", g.Channel, g.Generation, 0, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != count || state.Current != count {
		t.Fatalf("count %d state %+v", len(rows), state)
	}
	for i, d := range rows {
		if d.Offset != uint64(i+1) {
			t.Fatal("non-contiguous offsets")
		}
		if d.Identity != "observation_only" {
			t.Fatal("identical real donations coalesced or marked ambiguous")
		}
	}
	replay, err := pg.AppendDonation(ctx, g, rows[0], time.Now())
	if err != nil || replay.EventID != rows[0].EventID || replay.Offset != 1 {
		t.Fatalf("replay %v %v", replay, err)
	}
	if err = pg.AckDonations(ctx, "fixture_consumer", g.Channel, g.Generation, count+1, 0); !errors.Is(err, store.ErrCursor) {
		t.Fatalf("accepted unoffered ACK: %v", err)
	}
	if err = pg.AckDonations(ctx, "fixture_consumer", g.Channel, g.Generation, count, 0); err != nil {
		t.Fatal(err)
	}
	if err = pg.AckDonations(ctx, "fixture_consumer", g.Channel, g.Generation, count, 0); err != nil {
		t.Fatal(err)
	}
	other, err := pg.AppendDonation(ctx, g, donation(g, 1, "epoch-b"), time.Now())
	if err != nil || other.Identity != "reconnect_ambiguous" || len(other.Related) != 1 {
		t.Fatalf("reconnect evidence %+v %v", other, err)
	}
	decision, err := pg.ResolveRecovery(ctx, "fixture_consumer", g.Channel, "decision-a", g.Generation, g.Generation, count+1, 0, []byte(fmt.Sprintf(`{"reason":"fixture","unrecoveredRanges":[{"Start":{"journalGeneration":%q,"channelOffset":"25"},"End":{"journalGeneration":%q,"channelOffset":"25"}}]}`, g.Generation, g.Generation)))
	if err != nil || decision.Revision != 1 {
		t.Fatal(decision, err)
	}
	same, err := pg.ResolveRecovery(ctx, "fixture_consumer", g.Channel, "decision-a", g.Generation, g.Generation, count+1, 0, []byte(fmt.Sprintf(`{"reason":"fixture","unrecoveredRanges":[{"Start":{"journalGeneration":%q,"channelOffset":"25"},"End":{"journalGeneration":%q,"channelOffset":"25"}}]}`, g.Generation, g.Generation)))
	if err != nil || same != decision {
		t.Fatal("recovery retry changed")
	}
	if _, _, err = pg.ReadDonations(ctx, "fixture_consumer", g.Channel, g.Generation, count, 0, 100); !errors.Is(err, store.ErrRevision) {
		t.Fatalf("old stream accepted: %v", err)
	}
	p, err := pg.ConsumerProgress(ctx, "fixture_consumer", g.Channel)
	if err != nil || p.Ack != nil || p.Baseline != count+1 {
		t.Fatalf("baseline became ACK: %+v %v", p, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE collector_donations SET stored_at=clock_timestamp()-interval '31 days'`); err != nil {
		t.Fatal(err)
	}
	if err = pg.ExpireDonations(ctx, g.Channel, time.Hour, 30*24*time.Hour, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, _, err = pg.ReadDonations(ctx, "fixture_consumer", g.Channel, g.Generation, 0, 1, 100); !errors.Is(err, store.ErrCursorExpired) {
		t.Fatalf("expired cursor not rejected: %v", err)
	}
	if _, err = pg.AppendDonation(ctx, g, rows[0], time.Now()); !errors.Is(err, store.ErrCursorExpired) {
		t.Fatal("tombstone resurrected", err)
	}
}
func TestCollectorOwnerAndHistoricalSpool(t *testing.T) {
	pg, _, g := testutil.Collector(t)
	ctx := context.Background()
	d := donation(g, 1, "old")
	accepted := time.Now()
	if _, err := pg.AcquireCollection(ctx, g.Channel, "fixture_worker"); !errors.Is(err, store.ErrOwner) {
		t.Fatal("overlapping owner accepted", err)
	}
	if err := pg.ReleaseCollection(ctx, g); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.AcquireCollection(ctx, g.Channel, "fixture_worker"); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.AppendDonation(ctx, g, d, accepted); err != nil {
		t.Fatal("historical accepted spool rejected", err)
	}
	if _, err := pg.AppendDonation(ctx, g, donation(g, 2, "old"), time.Now()); !errors.Is(err, store.ErrOwner) {
		t.Fatal("stale owner accepted", err)
	}
	if _, err := pg.SetSubscription(ctx, "fixture_consumer", g.Channel, "disable", false); err != nil {
		t.Fatal(err)
	}
	if enabled, err := pg.CollectionEnabled(ctx, g.Channel); err != nil || enabled {
		t.Fatal("subscription ineffective", err)
	}
	if _, err := pg.SetSubscription(ctx, "fixture_consumer", g.Channel, "disable", true); !errors.Is(err, store.ErrConflict) {
		t.Fatal("idempotency collision accepted", err)
	}
}

func TestCollectorRestoreChangesGenerationAndPreservesIDs(t *testing.T) {
	pg, _, g := testutil.Collector(t)
	ctx := context.Background()
	d := donation(g, 1, "restore")
	if _, err := pg.AppendDonation(ctx, g, d, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pg.ReadDonations(ctx, "fixture_consumer", g.Channel, g.Generation, 0, 0, 100); err != nil {
		t.Fatal(err)
	}
	next, err := pg.RotateJournal(ctx, g.Channel, g.Generation, "synthetic restore")
	if err != nil || next == g.Generation {
		t.Fatal(next, err)
	}
	if _, _, err = pg.ReadDonations(ctx, "fixture_consumer", g.Channel, g.Generation, 0, 0, 100); !errors.Is(err, store.ErrGeneration) {
		t.Fatal("old journal cursor accepted", err)
	}
	if _, err = pg.AppendDonation(ctx, g, donation(g, 2, "restore"), time.Now()); !errors.Is(err, store.ErrGeneration) {
		t.Fatal("old spool adopted automatically", err)
	}
	if _, err = pg.ResolveRecovery(ctx, "fixture_consumer", g.Channel, "restore-decision", g.Generation, next, 0, 0, []byte(`{"reason":"fixture"}`)); err != nil {
		t.Fatal(err)
	}
	rows, _, err := pg.ReadDonations(ctx, "fixture_consumer", g.Channel, next, 0, 1, 100)
	if err != nil || len(rows) != 1 || rows[0].EventID != d.EventID || rows[0].Generation != next {
		t.Fatal("restore replay changed identity", rows, err)
	}
}

func TestCollectorUint64DatabasePrecision(t *testing.T) {
	pg, pool, g := testutil.Collector(t)
	ctx := context.Background()
	const before = "18446744073709551614"
	const maximum = ^uint64(0)
	if _, err := pool.Exec(ctx, `UPDATE collector_channels SET next_offset=$1,expired_through=$1 WHERE channel_id=$2`, before, g.Channel); err != nil {
		t.Fatal(err)
	}
	d, err := pg.AppendDonation(ctx, g, donation(g, 1, "large-offset"), time.Now())
	if err != nil || d.Offset != maximum {
		t.Fatal("uint64 offset lost precision", d.Offset, err)
	}
	rows, _, err := pg.ReadDonations(ctx, "fixture_consumer", g.Channel, g.Generation, maximum-1, 0, 1)
	if err != nil || len(rows) != 1 || rows[0].Offset != maximum {
		t.Fatal("uint64 replay lost precision", err)
	}
	if err = pg.AckDonations(ctx, "fixture_consumer", g.Channel, g.Generation, maximum, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE collector_consumers SET recovery_revision=$1 WHERE consumer_id='fixture_consumer'`, "18446744073709551615"); err != nil {
		t.Fatal(err)
	}
	progress, err := pg.ConsumerProgress(ctx, "fixture_consumer", g.Channel)
	if err != nil || progress.Revision != maximum || progress.Ack == nil || *progress.Ack != maximum {
		t.Fatal("uint64 consumer progress lost precision", progress, err)
	}
}
