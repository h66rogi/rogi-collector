package connector

import (
	"context"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func newTestSoopConnector() *SoopConnector {
	c := NewSoopConnector("", nil)
	c.channel = model.LiveChannel{
		ChannelID:    "bj-test",
		StreamerName: "BJ Test",
	}
	return c
}

func TestParseSoopPackets(t *testing.T) {
	raw := append(buildSoopPacket(soopCmdChatMessage, "\fhello\fuser1\f\f\fnick1\f"), buildSoopPacket(soopCmdNotice, "\f공지\f")...)
	packets := parseSoopPackets(raw)
	if len(packets) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(packets))
	}
	if packets[0].Cmd != soopCmdChatMessage || packets[0].Fields[0] != "hello" {
		t.Fatalf("unexpected first packet: %+v", packets[0])
	}
	if packets[1].Cmd != soopCmdNotice || packets[1].Fields[0] != "공지" {
		t.Fatalf("unexpected second packet: %+v", packets[1])
	}
}

func TestSoopHandleChatPacket(t *testing.T) {
	c := newTestSoopConnector()
	packet := soopPacket{
		Cmd:    soopCmdChatMessage,
		Fields: []string{"안녕하세요", "user1", "", "", "", "닉네임", "256", "3"},
		Raw:    buildSoopPacket(soopCmdChatMessage, "\f안녕하세요\fuser1\f\f\f닉네임\f256\f3\f"),
	}

	c.handlePacket(context.Background(), packet)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeChat {
		t.Fatalf("expected chat type, got %s", msg.Type)
	}
	if msg.Nickname != "닉네임" || msg.UserID != "user1" || msg.Message != "안녕하세요" {
		t.Fatalf("unexpected message: %+v", msg)
	}
}

func TestSoopHandleDonationPacket(t *testing.T) {
	c := newTestSoopConnector()
	packet := soopPacket{
		Cmd:    soopCmdSendBalloon,
		Fields: []string{"bjid", "donor1", "후원자", "10"},
		Raw:    buildSoopPacket(soopCmdSendBalloon, "\fbjid\fdonor1\f후원자\f10\f"),
	}

	c.handlePacket(context.Background(), packet)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeDonation {
		t.Fatalf("expected donation type, got %s", msg.Type)
	}
	if msg.Amount != 10 || msg.Nickname != "후원자" {
		t.Fatalf("unexpected donation: %+v", msg)
	}
}

func TestSoopHandleMissionGiftPacket(t *testing.T) {
	c := newTestSoopConnector()
	payload := `{"type":"GIFT","chno":1001,"is_relay":false,"key":2001,"title":"합성 미션","image":"mission_gift_01","gift_count":33,"user_id":"synthetic-donor-1","user_nick":"예시후원자","bj_id":"synthetic-channel-1","bj_nick":"예시채널","top_fan":0,"fan_order":0,"uuid":"00000000-0000-4000-8000-000000000001"}`
	packet := soopPacket{
		Cmd:    soopCmdMissionGift,
		Fields: []string{payload},
		Raw:    buildSoopPacket(soopCmdMissionGift, "\f"+payload+"\f"),
	}

	c.handlePacket(context.Background(), packet)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeDonation {
		t.Fatalf("expected donation type, got %s", msg.Type)
	}
	if msg.UserID != "synthetic-donor-1" || msg.Nickname != "예시후원자" {
		t.Fatalf("unexpected mission gift donor: %+v", msg)
	}
	if msg.Amount != 33 || msg.AmountKRW != 3300 || msg.Currency != "SOOP_BALLOON" {
		t.Fatalf("unexpected mission gift amount: %+v", msg)
	}
	if msg.Message != "대결미션 별풍선 33개: 합성 미션" {
		t.Fatalf("unexpected mission gift message: %q", msg.Message)
	}
}

func TestSoopHandleChallengeMissionGiftPacket(t *testing.T) {
	c := newTestSoopConnector()
	payload := `{"type":"CHALLENGE_GIFT","chno":1002,"is_relay":false,"key":2002,"title":"합성 도전","image":"challenge_mission_gift_01","gift_count":1,"user_id":"synthetic-donor-2","user_nick":"예시후원자2","bj_id":"synthetic-channel-2","bj_nick":"예시채널2","uuid":"00000000-0000-4000-8000-000000000002"}`
	packet := soopPacket{
		Cmd:    soopCmdMissionGift,
		Fields: []string{payload},
		Raw:    buildSoopPacket(soopCmdMissionGift, "\f"+payload+"\f"),
	}

	c.handlePacket(context.Background(), packet)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeDonation {
		t.Fatalf("expected donation type, got %s", msg.Type)
	}
	if msg.UserID != "synthetic-donor-2" || msg.Nickname != "예시후원자2" {
		t.Fatalf("unexpected challenge mission gift donor: %+v", msg)
	}
	if msg.Amount != 1 || msg.AmountKRW != 100 || msg.Currency != "SOOP_BALLOON" {
		t.Fatalf("unexpected challenge mission gift amount: %+v", msg)
	}
	if msg.Message != "도전미션 별풍선 1개: 합성 도전" {
		t.Fatalf("unexpected challenge mission gift message: %q", msg.Message)
	}
}

func TestSoopHandleSystemPacket(t *testing.T) {
	c := newTestSoopConnector()
	packet := soopPacket{
		Cmd:    soopCmdNotice,
		Fields: []string{"공지입니다"},
		Raw:    buildSoopPacket(soopCmdNotice, "\f공지입니다\f"),
	}

	c.handlePacket(context.Background(), packet)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeSystem || msg.Message != "공지입니다" {
		t.Fatalf("unexpected system message: %+v", msg)
	}
}
