package connector

import (
	"encoding/json"
	"testing"
)

func TestExtractCimeEmotes_NoBody(t *testing.T) {
	if got := extractCimeEmotes("", map[string]string{":x:": "u"}); got != nil {
		t.Errorf("expected nil for empty body, got %#v", got)
	}
}

func TestExtractCimeEmotes_NoMap(t *testing.T) {
	if got := extractCimeEmotes("hello", nil); got != nil {
		t.Errorf("expected nil for nil map, got %#v", got)
	}
}

func TestExtractCimeEmotes_SingleAndDuplicate(t *testing.T) {
	body := "안녕 :EX-clap::EX-clap: 좋아요"
	emojis := map[string]string{
		":EX-clap:": "https://example.invalid/emotes/clap.webp",
	}
	got := extractCimeEmotes(body, emojis)
	if len(got) != 2 {
		t.Fatalf("expected 2 emotes, got %d (%#v)", len(got), got)
	}
	for _, e := range got {
		if e.Code != "EX-clap" {
			t.Errorf("code: want EX-clap, got %q", e.Code)
		}
		if e.ImageURL != "https://example.invalid/emotes/clap.webp" {
			t.Errorf("imageURL: want set, got %q", e.ImageURL)
		}
		if e.Source != "cime:inline" {
			t.Errorf("source: want cime:inline, got %q", e.Source)
		}
	}
	if got[0].Start >= got[1].Start {
		t.Errorf("expected ascending offsets, got %d then %d", got[0].Start, got[1].Start)
	}
}

func TestExtractCimeEmotes_MapWithoutColons_Ignored(t *testing.T) {
	// Defensive: if the upstream map ever omits the surrounding colons,
	// we skip the entry rather than scanning bare words.
	body := "raw token text"
	got := extractCimeEmotes(body, map[string]string{"text": "u"})
	if got != nil {
		t.Errorf("expected nil, got %#v", got)
	}
}

func TestParseCimeEmojiMap_RawObject(t *testing.T) {
	raw := json.RawMessage(`{":a-1:":"u1",":b_2:":"u2"}`)
	m := parseCimeEmojiMap(map[string]json.RawMessage{"emojis": raw})
	if len(m) != 2 || m[":a-1:"] != "u1" || m[":b_2:"] != "u2" {
		t.Errorf("unexpected map: %#v", m)
	}
}

func TestParseCimeEmojiMap_DoubleEncoded(t *testing.T) {
	inner := `{":a-1:":"u1"}`
	encoded, _ := json.Marshal(inner)
	m := parseCimeEmojiMap(map[string]json.RawMessage{"emojis": encoded})
	if len(m) != 1 || m[":a-1:"] != "u1" {
		t.Errorf("unexpected map: %#v", m)
	}
}

func TestParseCimeEmojiMap_Missing(t *testing.T) {
	if got := parseCimeEmojiMap(nil); got != nil {
		t.Errorf("nil attrs: %#v", got)
	}
	if got := parseCimeEmojiMap(map[string]json.RawMessage{}); got != nil {
		t.Errorf("empty attrs: %#v", got)
	}
}
