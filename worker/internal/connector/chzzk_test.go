package connector

import (
	"encoding/json"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// buildWSMessage constructs a raw Chzzk WebSocket frame from cmd and body items.
func buildWSMessage(cmd int, bodies []chzzkChatBody) []byte {
	bdy, _ := json.Marshal(bodies)
	msg := map[string]interface{}{
		"cmd": cmd,
		"bdy": json.RawMessage(bdy),
	}
	data, _ := json.Marshal(msg)
	return data
}

func newTestConnector() *ChzzkConnector {
	c := NewChzzkConnector("")
	c.channel = model.LiveChannel{
		ChannelID:    "test-channel-123",
		StreamerName: "TestStreamer",
	}
	return c
}

func TestParseChatMessage(t *testing.T) {
	c := newTestConnector()

	profile := json.RawMessage(`{"nickname":"테스터","userIdHash":"abc123"}`)
	bodies := []chzzkChatBody{
		{
			UID:         "abc123",
			Msg:         "안녕하세요!",
			MsgTypeCode: 1,
			Profile:     profile,
		},
	}
	raw := buildWSMessage(chzzkCmdChat, bodies)

	// Parse via the internal method.
	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	// parseChatMessages writes to the channel, so read from it.
	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Platform != model.PlatformChzzk {
			t.Errorf("expected platform chzzk, got %s", msg.Platform)
		}
		if msg.Type != model.MessageTypeChat {
			t.Errorf("expected type chat, got %s", msg.Type)
		}
		if msg.Nickname != "테스터" {
			t.Errorf("expected nickname 테스터, got %s", msg.Nickname)
		}
		if msg.UserID != "abc123" {
			t.Errorf("expected userId abc123, got %s", msg.UserID)
		}
		if msg.Message != "안녕하세요!" {
			t.Errorf("expected message 안녕하세요!, got %s", msg.Message)
		}
		if msg.ChannelID != "test-channel-123" {
			t.Errorf("expected channelId test-channel-123, got %s", msg.ChannelID)
		}
		if msg.StreamerName != "TestStreamer" {
			t.Errorf("expected streamerName TestStreamer, got %s", msg.StreamerName)
		}
		if msg.Amount != 0 {
			t.Errorf("expected amount 0, got %f", msg.Amount)
		}
	default:
		t.Fatal("expected a message on msgCh but got none")
	}
}

func TestParseDonationMessage(t *testing.T) {
	c := newTestConnector()

	profile := json.RawMessage(`{"nickname":"후원자","userIdHash":"donor456"}`)
	extras := json.RawMessage(`{"payAmount":6000,"isAnonymous":false}`)
	bodies := []chzzkChatBody{
		{
			UID:         "donor456",
			Msg:         "응원합니다!",
			MsgTypeCode: chzzkMsgTypeDonation,
			Profile:     profile,
			Extras:      extras,
		},
	}
	raw := buildWSMessage(chzzkCmdDonationChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Type != model.MessageTypeDonation {
			t.Errorf("expected type donation, got %s", msg.Type)
		}
		if msg.Amount != 6000 {
			t.Errorf("expected amount 6000, got %f", msg.Amount)
		}
		if msg.Currency != "CHZZK_CHEESE" {
			t.Errorf("expected currency CHZZK_CHEESE, got %s", msg.Currency)
		}
		if msg.AmountKRW != 6000 {
			t.Errorf("expected amountKRW 6000, got %d", msg.AmountKRW)
		}
		if msg.Nickname != "후원자" {
			t.Errorf("expected nickname 후원자, got %s", msg.Nickname)
		}
		if msg.Message != "응원합니다!" {
			t.Errorf("expected message 응원합니다!, got %s", msg.Message)
		}
	default:
		t.Fatal("expected a donation message but got none")
	}
}

func TestParseAnonymousDonation(t *testing.T) {
	c := newTestConnector()

	profile := json.RawMessage(`{"nickname":"실제닉네임","userIdHash":"anon789"}`)
	extras := json.RawMessage(`{"payAmount":10000,"isAnonymous":true}`)
	bodies := []chzzkChatBody{
		{
			UID:         "anon789",
			Msg:         "익명 후원",
			MsgTypeCode: chzzkMsgTypeDonation,
			Profile:     profile,
			Extras:      extras,
		},
	}
	raw := buildWSMessage(chzzkCmdDonationChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Nickname != "익명" {
			t.Errorf("expected anonymous nickname 익명, got %s", msg.Nickname)
		}
		if msg.Amount != 10000 {
			t.Errorf("expected amount 10000, got %f", msg.Amount)
		}
	default:
		t.Fatal("expected an anonymous donation message but got none")
	}
}

func TestParseMultipleMessagesInBody(t *testing.T) {
	c := newTestConnector()

	bodies := []chzzkChatBody{
		{
			UID:         "user1",
			Msg:         "first",
			MsgTypeCode: 1,
			Profile:     json.RawMessage(`{"nickname":"유저1","userIdHash":"user1"}`),
		},
		{
			UID:         "user2",
			Msg:         "second",
			MsgTypeCode: 1,
			Profile:     json.RawMessage(`{"nickname":"유저2","userIdHash":"user2"}`),
		},
	}
	raw := buildWSMessage(chzzkCmdChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	msg1 := <-c.msgCh
	msg2 := <-c.msgCh

	if msg1.Nickname != "유저1" {
		t.Errorf("expected first nickname 유저1, got %s", msg1.Nickname)
	}
	if msg2.Nickname != "유저2" {
		t.Errorf("expected second nickname 유저2, got %s", msg2.Nickname)
	}
}

func TestHandleRawMessageConnectReply(t *testing.T) {
	c := newTestConnector()

	// cmd 10100 should not produce any messages.
	msg := map[string]interface{}{
		"cmd": chzzkCmdConnectReply,
		"bdy": map[string]interface{}{},
	}
	data, _ := json.Marshal(msg)
	c.handleRawMessage(data)

	select {
	case <-c.msgCh:
		t.Fatal("did not expect a message for connect reply")
	default:
		// OK
	}
}

func TestHandleRawMessagePong(t *testing.T) {
	c := newTestConnector()

	msg := map[string]interface{}{
		"cmd": chzzkCmdPing,
	}
	data, _ := json.Marshal(msg)
	c.handleRawMessage(data)

	select {
	case <-c.msgCh:
		t.Fatal("did not expect a message for pong")
	default:
		// OK
	}
}

func TestParseMissingProfile(t *testing.T) {
	c := newTestConnector()

	bodies := []chzzkChatBody{
		{
			UID:         "noProfile",
			Msg:         "프로필없음",
			MsgTypeCode: 1,
			Profile:     nil, // empty profile
		},
	}
	raw := buildWSMessage(chzzkCmdChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Nickname != "?" {
			t.Errorf("expected fallback nickname ?, got %s", msg.Nickname)
		}
		if msg.UserID != "noProfile" {
			t.Errorf("expected userId from UID field, got %s", msg.UserID)
		}
	default:
		t.Fatal("expected a message but got none")
	}
}

func TestParseSystemMessage(t *testing.T) {
	c := newTestConnector()

	bodies := []chzzkChatBody{
		{
			UID:         "SYSTEM_MESSAGE",
			Msg:         "채팅 참여가 제한되었습니다.",
			MsgTypeCode: 1,
			Profile:     nil,
		},
	}
	raw := buildWSMessage(chzzkCmdChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Type != model.MessageTypeSystem {
			t.Errorf("expected type system, got %s", msg.Type)
		}
		if msg.Nickname != "system" {
			t.Errorf("expected nickname system, got %s", msg.Nickname)
		}
		if msg.UserID != "SYSTEM_MESSAGE" {
			t.Errorf("expected userId SYSTEM_MESSAGE, got %s", msg.UserID)
		}
		if msg.Message != "채팅 참여가 제한되었습니다." {
			t.Errorf("unexpected message: %s", msg.Message)
		}
	default:
		t.Fatal("expected a system message but got none")
	}
}

func TestParseAnonymousPresenceDropped(t *testing.T) {
	c := newTestConnector()

	bodies := []chzzkChatBody{
		{
			UID:         "anonymous",
			Msg:         "",
			MsgTypeCode: 1,
			Profile:     nil,
		},
	}
	raw := buildWSMessage(chzzkCmdChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		t.Fatalf("expected anonymous presence to be dropped, got msg: %+v", msg)
	default:
		// expected: nothing emitted
	}
}

func TestParseAnonymousDonationStillEmitted(t *testing.T) {
	c := newTestConnector()

	// uid="anonymous" with donation msgTypeCode must still be emitted (handled by donation branch).
	bodies := []chzzkChatBody{
		{
			UID:         "anonymous",
			Msg:         "감사합니다",
			MsgTypeCode: chzzkMsgTypeDonation,
			Profile:     nil,
			Extras:      json.RawMessage(`{"payAmount":1000,"isAnonymous":true}`),
		},
	}
	raw := buildWSMessage(chzzkCmdDonationChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Type != model.MessageTypeDonation {
			t.Errorf("expected type donation, got %s", msg.Type)
		}
		if msg.Nickname != "익명" {
			t.Errorf("expected nickname 익명, got %s", msg.Nickname)
		}
	default:
		t.Fatal("expected an anonymous donation message but got none")
	}
}

func TestConvertToMessageDonationZeroAmount(t *testing.T) {
	c := newTestConnector()

	// Donation msgTypeCode but no extras - should still be typed as donation.
	bodies := []chzzkChatBody{
		{
			UID:         "donor0",
			Msg:         "빈 후원",
			MsgTypeCode: chzzkMsgTypeDonation,
			Profile:     json.RawMessage(`{"nickname":"기부자"}`),
			Extras:      nil,
		},
	}
	raw := buildWSMessage(chzzkCmdDonationChat, bodies)

	var wsMsg chzzkWSMessage
	if err := json.Unmarshal(raw, &wsMsg); err != nil {
		t.Fatal(err)
	}

	c.parseChatMessages(wsMsg.Bdy)

	select {
	case msg := <-c.msgCh:
		if msg.Type != model.MessageTypeDonation {
			t.Errorf("expected donation type, got %s", msg.Type)
		}
		if msg.Amount != 0 {
			t.Errorf("expected amount 0, got %f", msg.Amount)
		}
	default:
		t.Fatal("expected a message but got none")
	}
}
