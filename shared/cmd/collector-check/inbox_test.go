package main

import (
	"context"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"google.golang.org/grpc"
	"os"
	"path/filepath"
	"testing"
)

type probeClient struct {
	pb.CollectorServiceClient
	events []*pb.DonationEvent
	dir    string
	acked  uint64
	t      *testing.T
}

func (c *probeClient) ListDonations(_ context.Context, r *pb.ListDonationsRequest, _ ...grpc.CallOption) (*pb.ListDonationsResponse, error) {
	var events []*pb.DonationEvent
	for _, e := range c.events {
		if e.Cursor.ChannelOffset > r.AfterCursor.ChannelOffset {
			events = append(events, e)
		}
	}
	return &pb.ListDonationsResponse{Donations: events}, nil
}
func (c *probeClient) AckDonations(_ context.Context, r *pb.AckDonationsRequest, _ ...grpc.CallOption) (*pb.AckDonationsResponse, error) {
	if _, e := os.Stat(filepath.Join(c.dir, "cursor.json")); e != nil {
		c.t.Fatal("ACK before durable cursor")
	}
	for _, e := range c.events {
		if e.Cursor.ChannelOffset <= r.Cursor.ChannelOffset {
			if _, err := os.Stat(filepath.Join(c.dir, e.EventId+".json")); err != nil {
				c.t.Fatal("ACK before inbox")
			}
		}
	}
	c.acked = r.Cursor.ChannelOffset
	return &pb.AckDonationsResponse{}, nil
}
func TestProbeInboxReplayAndConflict(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CHECK_INBOX_DIR", dir)
	state := &pb.CollectionStatus{EarliestCursor: &pb.Cursor{JournalGeneration: "test-generation"}, RecoveryRevision: 1}
	client := &probeClient{dir: dir, t: t, events: []*pb.DonationEvent{{EventId: "synthetic-1", DonorId: "synthetic-donor", NativeBalloonCount: 33, Cursor: &pb.Cursor{JournalGeneration: "test-generation", ChannelOffset: 1}}}}
	out := map[string]any{}
	if e := donationCheck(context.Background(), client, "test-consumer", "test-channel", state, out); e != nil {
		t.Fatal(e)
	}
	if out["newInboxEvents"] != 1 || client.acked != 1 {
		t.Fatal(out)
	}
	t.Setenv("CHECK_REPLAY_FROM_START", "1")
	if e := donationCheck(context.Background(), client, "test-consumer", "test-channel", state, out); e != nil {
		t.Fatal(e)
	}
	if out["newInboxEvents"] != 0 || out["replayedInboxEvents"] != 1 {
		t.Fatal(out)
	}
	client.events[0].NativeBalloonCount = 34
	client.acked = 0
	if e := donationCheck(context.Background(), client, "test-consumer", "test-channel", state, out); e == nil {
		t.Fatal("accepted identity conflict")
	}
	if client.acked != 0 {
		t.Fatal("ACK after failed inbox")
	}
}
