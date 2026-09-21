package pipeline

import (
	"context"
	"errors"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/h66rogi/rogi-collector/shared/testutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type journalFunc func(context.Context, store.OwnerGrant, model.Donation, time.Time) (model.Donation, error)

func (f journalFunc) AppendDonation(ctx context.Context, g store.OwnerGrant, d model.Donation, at time.Time) (model.Donation, error) {
	return f(ctx, g, d, at)
}
func sampleDonation() model.Donation {
	return model.Donation{EventID: "fixture-1", ChannelID: "fixture_channel", Count: 33, Kind: "balloon", DonorID: "fixture_donor", ConnectionEpoch: "fixture_epoch", Sequence: 1, ObservedAt: time.Now()}
}
func TestSpoolDurabilityAndFailureEvidence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	offline := journalFunc(func(_ context.Context, _ store.OwnerGrant, d model.Donation, _ time.Time) (model.Donation, error) {
		return d, errors.New("simulated database outage")
	})
	s, err := NewDonationSpool(dir, offline, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewDonationSpool(dir, offline, 1<<20); err == nil {
		t.Fatal("second writer accepted")
	}
	if err = s.Save(ctx, store.OwnerGrant{}, sampleDonation(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if count, _, _ := s.Backlog(); count != 1 {
		t.Fatal("not durably retained")
	}
	s.Close()
	s, err = NewDonationSpool(dir, offline, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	files, _, _ := s.files()
	if err = os.WriteFile(filepath.Join(dir, files[0]), []byte(`{"version":1,"checksum":"bad","record":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.Save(ctx, store.OwnerGrant{}, sampleDonation(), time.Now().Add(time.Minute)); err == nil {
		t.Fatal("corruption hidden as DB outage")
	}
}
func TestSpoolFullAndExpiredOwner(t *testing.T) {
	s, err := NewDonationSpool(t.TempDir(), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Save(context.Background(), store.OwnerGrant{}, sampleDonation(), time.Now().Add(time.Minute)); err == nil {
		t.Fatal("full spool accepted")
	}
	if err = s.Save(context.Background(), store.OwnerGrant{}, sampleDonation(), time.Now().Add(-time.Second)); !errors.Is(err, store.ErrOwner) {
		t.Fatal("expired owner accepted", err)
	}
}
func TestSpoolDatabaseCommitResponseLost(t *testing.T) {
	pg, _, g := testutil.Collector(t)
	ctx := context.Background()
	dir := t.TempDir()
	journal := journalFunc(func(ctx context.Context, g store.OwnerGrant, d model.Donation, at time.Time) (model.Donation, error) {
		saved, err := pg.AppendDonation(ctx, g, d, at)
		if err != nil {
			return saved, err
		}
		return saved, errors.New("response lost after commit")
	})
	s, err := NewDonationSpool(dir, journal, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Save(ctx, g, sampleDonation(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = NewDonationSpool(dir, pg, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := pg.CollectionState(ctx, g.Channel)
	if err != nil || c.Current != 1 {
		t.Fatal("spool replay duplicated event", c, err)
	}
	if count, _, err := s.Backlog(); count != 0 || err != nil {
		t.Fatal("replayed file retained", err)
	}
}
