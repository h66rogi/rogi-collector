package model

import (
	"testing"
	"time"
)

func TestToStreamFields_ChatMessage(t *testing.T) {
	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	msg := ChatMessage{
		ID:           "msg-001",
		Type:         MessageTypeChat,
		Platform:     PlatformChzzk,
		ChannelID:    "ch-abc",
		StreamerName: "streamer1",
		UserID:       "user-1",
		Nickname:     "viewer1",
		Message:      "hello world",
		Timestamp:    ts,
		Raw:          `{"raw":"data"}`,
	}

	fields := msg.ToStreamFields()

	if fields["id"] != "msg-001" {
		t.Errorf("expected id=msg-001, got %v", fields["id"])
	}
	if fields["type"] != "chat" {
		t.Errorf("expected type=chat, got %v", fields["type"])
	}
	if fields["platform"] != "chzzk" {
		t.Errorf("expected platform=chzzk, got %v", fields["platform"])
	}
	if fields["channelId"] != "ch-abc" {
		t.Errorf("expected channelId=ch-abc, got %v", fields["channelId"])
	}
	if fields["streamerName"] != "streamer1" {
		t.Errorf("expected streamerName=streamer1, got %v", fields["streamerName"])
	}
	if fields["nickname"] != "viewer1" {
		t.Errorf("expected nickname=viewer1, got %v", fields["nickname"])
	}
	if fields["message"] != "hello world" {
		t.Errorf("expected message=hello world, got %v", fields["message"])
	}

	// Chat messages should NOT have donation fields
	if _, ok := fields["amount"]; ok {
		t.Error("chat message should not have amount field")
	}
	if _, ok := fields["currency"]; ok {
		t.Error("chat message should not have currency field")
	}
	if _, ok := fields["amountKRW"]; ok {
		t.Error("chat message should not have amountKRW field")
	}
}

func TestToStreamFields_DonationMessage(t *testing.T) {
	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	msg := ChatMessage{
		ID:           "msg-002",
		Type:         MessageTypeDonation,
		Platform:     PlatformSoop,
		ChannelID:    "ch-xyz",
		StreamerName: "streamer2",
		UserID:       "user-2",
		Nickname:     "donor1",
		Message:      "great stream!",
		Timestamp:    ts,
		Raw:          `{"raw":"donation"}`,
		Amount:       10000,
		Currency:     "KRW",
		AmountKRW:    10000,
	}

	fields := msg.ToStreamFields()

	if fields["type"] != "donation" {
		t.Errorf("expected type=donation, got %v", fields["type"])
	}
	if fields["amount"] != "10000.00" {
		t.Errorf("expected amount=10000.00, got %v", fields["amount"])
	}
	if fields["currency"] != "KRW" {
		t.Errorf("expected currency=KRW, got %v", fields["currency"])
	}
	if fields["amountKRW"] != "10000" {
		t.Errorf("expected amountKRW=10000, got %v", fields["amountKRW"])
	}
}
