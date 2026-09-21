package connector

import (
	"encoding/json"
	"testing"
	"time"
)

// buildExtras simulates the double-encoded extras string Chzzk sends:
// `"extras": "{\"emojis\":{\"example_1\":\"https://...\"}}"`.
func buildExtras(t *testing.T, payload map[string]any) json.RawMessage {
	t.Helper()
	inner, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal inner: %v", err)
	}
	encoded, err := json.Marshal(string(inner))
	if err != nil {
		t.Fatalf("marshal encoded: %v", err)
	}
	return encoded
}

func TestExtractChzzkEmotes_NoBody(t *testing.T) {
	if got := extractChzzkEmotes("", nil); got != nil {
		t.Errorf("expected nil for empty body, got %#v", got)
	}
}

func TestExtractChzzkEmotes_NoTokens(t *testing.T) {
	if got := extractChzzkEmotes("그냥 평범한 채팅", nil); got != nil {
		t.Errorf("expected nil when body has no tokens, got %#v", got)
	}
}

func TestExtractChzzkEmotes_SingleToken_WithMap(t *testing.T) {
	body := "{:example_1:}"
	emojis := map[string]string{
		"example_1": "https://example.invalid/emotes/example-1.png",
	}
	got := extractChzzkEmotes(body, emojis)
	if len(got) != 1 {
		t.Fatalf("expected 1 emote, got %d (%#v)", len(got), got)
	}
	if got[0].Code != "example_1" {
		t.Errorf("code: want example_1, got %q", got[0].Code)
	}
	if got[0].ImageURL != "https://example.invalid/emotes/example-1.png" {
		t.Errorf("imageURL: want set, got %q", got[0].ImageURL)
	}
	if got[0].Start != 0 || got[0].End != len(body) {
		t.Errorf("offsets: want [0,%d), got [%d,%d)", len(body), got[0].Start, got[0].End)
	}
	if got[0].Source != "chzzk:inline" {
		t.Errorf("source: want chzzk:inline, got %q", got[0].Source)
	}
}

func TestExtractChzzkEmotes_TokenWithoutMap(t *testing.T) {
	// Even when extras.emojis is missing, we still emit the token so the
	// renderer can decide on a fallback (e.g. catalog lookup).
	got := extractChzzkEmotes("{:example_1:}", nil)
	if len(got) != 1 || got[0].Code != "example_1" || got[0].ImageURL != "" {
		t.Errorf("expected token with empty URL, got %#v", got)
	}
}

func TestExtractChzzkEmotes_MultipleAndMixed(t *testing.T) {
	body := "안녕{:example_1:} 반가워 {:exampleWave:}{:example_1:}"
	emojis := map[string]string{
		"example_1":   "https://example.invalid/emotes/example-1.png",
		"exampleWave": "https://example.invalid/emotes/wave.png",
	}
	got := extractChzzkEmotes(body, emojis)
	if len(got) != 3 {
		t.Fatalf("expected 3 emotes, got %d (%#v)", len(got), got)
	}
	for i, e := range got {
		if e.Start >= e.End || body[e.Start:e.End][0] != '{' {
			t.Errorf("emote %d offsets wrong: %#v on body %q", i, e, body)
		}
	}
	codes := []string{got[0].Code, got[1].Code, got[2].Code}
	want := []string{"example_1", "exampleWave", "example_1"}
	for i := range codes {
		if codes[i] != want[i] {
			t.Errorf("code %d: want %s, got %s", i, want[i], codes[i])
		}
	}
}

func TestConvertToMessage_EmotesPropagated(t *testing.T) {
	c := newTestConnector()
	body := chzzkChatBody{
		UID:         "abc",
		Msg:         "헤이 {:example_2:}{:example_2:}",
		MsgTypeCode: 1,
		Profile:     json.RawMessage(`{"nickname":"u","userIdHash":"abc"}`),
		Extras: buildExtras(t, map[string]any{
			"emojis": map[string]string{"example_2": "https://example.invalid/emotes/example-2.png"},
		}),
	}
	raw, _ := json.Marshal(body)

	msg, ok := c.convertToMessage(body, raw, time.Now())
	if !ok {
		t.Fatal("convertToMessage dropped chat body")
	}
	if len(msg.Emotes) != 2 {
		t.Fatalf("want 2 emotes, got %d (%#v)", len(msg.Emotes), msg.Emotes)
	}
	for _, e := range msg.Emotes {
		if e.Code != "example_2" || e.ImageURL == "" {
			t.Errorf("unexpected emote: %#v", e)
		}
	}
}
