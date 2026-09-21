package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestToStreamFields_EmotesOmittedWhenEmpty(t *testing.T) {
	msg := ChatMessage{
		ID:        "x",
		Type:      MessageTypeChat,
		Platform:  PlatformChzzk,
		ChannelID: "c",
		Message:   "hello",
		Timestamp: time.Now(),
	}
	fields := msg.ToStreamFields()
	if _, ok := fields["emotes"]; ok {
		t.Errorf("expected no `emotes` key when Emotes is empty, got %v", fields["emotes"])
	}
}

func TestToStreamFields_EmotesSerialized(t *testing.T) {
	msg := ChatMessage{
		ID:        "x",
		Type:      MessageTypeChat,
		Platform:  PlatformChzzk,
		ChannelID: "c",
		Message:   "{:d_67:}",
		Timestamp: time.Now(),
		Emotes: []EmoteToken{
			{Code: "d_67", Start: 0, End: 8, ImageURL: "https://e/x.png", Source: "chzzk:inline"},
		},
	}
	fields := msg.ToStreamFields()
	raw, ok := fields["emotes"].(string)
	if !ok || raw == "" {
		t.Fatalf("expected emotes JSON string, got %T %v", fields["emotes"], fields["emotes"])
	}
	var got []EmoteToken
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 1 || got[0].Code != "d_67" || got[0].ImageURL != "https://e/x.png" {
		t.Errorf("roundtrip mismatch: %#v", got)
	}
}
