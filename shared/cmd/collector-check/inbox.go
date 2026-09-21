package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"path/filepath"
	"strconv"
)

type checkpoint struct {
	Generation string `json:"generation"`
	Offset     uint64 `json:"offset,string"`
	Revision   uint64 `json:"revision,string"`
	Channel    string `json:"channel"`
}

func durableFile(path string, body []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(body); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func donationCheck(ctx context.Context, client pb.CollectorServiceClient, consumer, channel string, state *pb.CollectionStatus, out map[string]any) error {
	dir := os.Getenv("CHECK_INBOX_DIR")
	if !filepath.IsAbs(dir) {
		return errors.New("absolute isolated CHECK_INBOX_DIR required")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	if state.EarliestCursor == nil {
		return errors.New("retained cursor unavailable")
	}
	point := checkpoint{Generation: state.EarliestCursor.JournalGeneration, Offset: state.EarliestCursor.ChannelOffset, Revision: state.RecoveryRevision, Channel: channel}
	progress := filepath.Join(dir, "cursor.json")
	body, e := os.ReadFile(progress)
	if e == nil {
		if json.Unmarshal(body, &point) != nil || point.Generation != state.EarliestCursor.JournalGeneration || point.Revision != state.RecoveryRevision || point.Channel != channel {
			return errors.New("inbox requires explicit recovery")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if os.Getenv("CHECK_REPLAY_FROM_START") == "1" {
		point.Offset = state.EarliestCursor.ChannelOffset
	}
	after := &pb.Cursor{JournalGeneration: point.Generation, ChannelOffset: point.Offset}
	response, e := client.ListDonations(ctx, &pb.ListDonationsRequest{ConsumerId: consumer, ChannelId: channel, AfterCursor: after, RecoveryRevision: point.Revision, Limit: 100})
	if e != nil {
		return e
	}
	fresh, replayed := 0, 0
	for _, event := range response.Donations {
		if event.EventId == "" || filepath.Base(event.EventId) != event.EventId || event.DonorId == "" || event.NativeBalloonCount < 1 || event.NativeBalloonCount > 9007199254740991 || event.Cursor == nil || event.Cursor.JournalGeneration != point.Generation || event.Cursor.ChannelOffset != point.Offset+1 {
			return errors.New("invalid donation sequence or identity")
		}
		raw, e := protojson.Marshal(event)
		if e != nil {
			return e
		}
		path := filepath.Join(dir, event.EventId+".json")
		prior, e := os.ReadFile(path)
		if e == nil {
			if !bytes.Equal(prior, raw) {
				return errors.New("event identity payload conflict")
			}
			replayed++
		} else if errors.Is(e, os.ErrNotExist) {
			if e = durableFile(path, raw); e != nil {
				return e
			}
			fresh++
		} else {
			return e
		}
		point.Offset = event.Cursor.ChannelOffset
	}
	encoded, _ := json.Marshal(point)
	if e = durableFile(progress, encoded); e != nil {
		return e
	}
	if point.Offset > state.EarliestCursor.ChannelOffset {
		_, e = client.AckDonations(ctx, &pb.AckDonationsRequest{ConsumerId: consumer, ChannelId: channel, Cursor: &pb.Cursor{JournalGeneration: point.Generation, ChannelOffset: point.Offset}, RecoveryRevision: point.Revision})
		if e != nil {
			return e
		}
	}
	out["newInboxEvents"] = fresh
	out["replayedInboxEvents"] = replayed
	out["ackOffset"] = strconv.FormatUint(point.Offset, 10)
	return nil
}
