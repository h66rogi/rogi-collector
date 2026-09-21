package connector

import (
	"context"
	"github.com/h66rogi/rogi-collector/shared/model"
	"testing"
)

func TestDonationBypassesFullChatBufferAndPreservesIdenticalObservations(t *testing.T) {
	c := newTestSoopConnector()
	for len(c.msgCh) < cap(c.msgCh) {
		c.msgCh <- model.ChatMessage{}
	}
	var saved []model.Donation
	c.SetDonationSink(func(_ context.Context, d model.Donation) error { saved = append(saved, d); return nil })
	packet := soopPacket{Cmd: soopCmdSendBalloon, Fields: []string{"fixture_channel", "fixture_donor", "Fixture", "33"}}
	c.handlePacket(context.Background(), packet)
	c.handlePacket(context.Background(), packet)
	if len(saved) != 2 || saved[0].EventID == saved[1].EventID || saved[0].Count != 33 || saved[1].Count != 33 {
		t.Fatalf("donations lost or merged: %+v", saved)
	}
	c2 := newTestSoopConnector()
	if c2.epoch == c.epoch {
		t.Fatal("epoch reused")
	}
	packet.Fields[3] = "33.5"
	c.handlePacket(context.Background(), packet)
	if len(saved) != 2 {
		t.Fatal("fractional count truncated")
	}
	select {
	case <-c.errCh:
	default:
		t.Fatal("invalid donation not visible")
	}
}

func TestHandshakeRequiresRequestedJoinAcknowledgement(t *testing.T) {
	c := newTestSoopConnector()
	c.joinedCh = make(chan struct{}, 1)
	c.handlePacket(context.Background(), soopPacket{Cmd: soopCmdJoin, Ret: 0})
	select {
	case <-c.joinedCh:
		t.Fatal("unsolicited join accepted")
	default:
	}
	c.joinRequested = true
	c.handlePacket(context.Background(), soopPacket{Cmd: soopCmdJoin, Ret: 1})
	select {
	case <-c.joinedCh:
		t.Fatal("rejected join accepted")
	default:
	}
	c.handlePacket(context.Background(), soopPacket{Cmd: soopCmdJoin, Ret: 0})
	select {
	case <-c.joinedCh:
	default:
		t.Fatal("successful join not confirmed")
	}
}
