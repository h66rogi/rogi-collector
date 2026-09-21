package connector

import (
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func newTestCimeConnector() *CimeConnector {
	c := NewCimeConnector()
	c.channel = model.LiveChannel{
		ChannelID:    "cime-test",
		StreamerName: "Cime Test",
	}
	return c
}

func TestCimeHandleChatMessage(t *testing.T) {
	c := newTestCimeConnector()
	raw := []byte(`{"Type":"MESSAGE","Content":"반갑습니다","Sender":{"UserId":"244","Attributes":{"user":"{\"id\":\"244\",\"ch\":{\"id\":\"244\",\"na\":\"닉네임\",\"lv\":0},\"c\":\"D\",\"bg\":[{\"na\":\"배지\"}]}"}}}`)

	c.handleRawMessage(raw)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeChat {
		t.Fatalf("expected chat type, got %s", msg.Type)
	}
	if msg.Nickname != "닉네임" || msg.UserID != "244" || msg.Message != "반갑습니다" {
		t.Fatalf("unexpected message: %+v", msg)
	}
}

func TestCimeHandleDonationEvent(t *testing.T) {
	c := newTestCimeConnector()
	raw := []byte(`{"Type":"EVENT","EventName":"DONATION_CREATED","Attributes":{"extra":"{\"msg\":\"응원해요\",\"amt\":5000,\"anon\":false,\"prof\":{\"id\":12,\"name\":\"후원자\"}}"}}`)

	c.handleRawMessage(raw)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeDonation {
		t.Fatalf("expected donation type, got %s", msg.Type)
	}
	if msg.AmountKRW != 5000 || msg.Nickname != "후원자" || msg.Message != "응원해요" {
		t.Fatalf("unexpected donation: %+v", msg)
	}
}

func TestCimeHandleDonationAnon(t *testing.T) {
	c := newTestCimeConnector()
	raw := []byte(`{"Type":"EVENT","EventName":"DONATION_CREATED","Attributes":{"extra":"{\"msg\":\"\",\"amt\":1000,\"anon\":true,\"prof\":{\"id\":0,\"name\":\"\"}}"}}`)

	c.handleRawMessage(raw)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeDonation {
		t.Fatalf("expected donation type, got %s", msg.Type)
	}
	if msg.Nickname != "익명" {
		t.Fatalf("expected nickname 익명, got %s", msg.Nickname)
	}
}

func TestCimeHandleDonationEmptyName(t *testing.T) {
	c := newTestCimeConnector()
	// Non-anonymous but Prof.Name empty — still must not produce "?".
	raw := []byte(`{"Type":"EVENT","EventName":"DONATION_CREATED","Attributes":{"extra":"{\"msg\":\"감사\",\"amt\":2000,\"anon\":false,\"prof\":{\"id\":42,\"name\":\"\"}}"}}`)

	c.handleRawMessage(raw)

	msg := <-c.msgCh
	if msg.Nickname != "익명" {
		t.Fatalf("expected nickname 익명 (fallback for empty name), got %s", msg.Nickname)
	}
	if msg.UserID != "42" {
		t.Fatalf("expected userId 42, got %s", msg.UserID)
	}
}

func TestCimeHandleErrorMessage(t *testing.T) {
	c := newTestCimeConnector()
	raw := []byte(`{"Type":"ERROR","ErrorMessage":"token expired"}`)

	c.handleRawMessage(raw)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeSystem || msg.Message != "token expired" {
		t.Fatalf("unexpected system message: %+v", msg)
	}
}
