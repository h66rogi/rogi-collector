package publicapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/store"
)

func TestHistoryCursorIntegrityAndScope(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	sessionID := "606c9e5a-05ab-4b25-a5c2-ef03870a73a4"
	raw := signHistoryCursor(key, historyCursor{Kind: "chats", SessionID: sessionID, Position: 42})
	parsed, err := parseHistoryCursor(key, raw, "chats")
	if err != nil || parsed.SessionID != sessionID || parsed.Position != 42 {
		t.Fatalf("cursor did not roundtrip: %#v %v", parsed, err)
	}
	if _, err := parseHistoryCursor(key, raw, "broadcasts"); err == nil {
		t.Fatal("cursor reused across endpoints")
	}
	if _, err := parseHistoryCursor([]byte(strings.Repeat("x", 32)), raw, "chats"); err == nil {
		t.Fatal("cursor accepted with a different signing key")
	}
	parts := strings.Split(raw, ".")
	parts[0] = strings.Replace(parts[0], "A", "B", 1)
	if parts[0] == strings.Split(raw, ".")[0] {
		parts[0] = "A" + parts[0][1:]
	}
	if _, err := parseHistoryCursor(key, strings.Join(parts, "."), "chats"); err == nil {
		t.Fatal("tampered cursor accepted")
	}
}

func TestHistoryUnavailableUntilEnabled(t *testing.T) {
	s := testServer(t, store.ChatBatch{}, func(context.Context, string) (*pb.BroadcastStatus, error) { return nil, nil })
	for _, path := range []string{"/v1/broadcasts", "/v1/broadcasts/606c9e5a-05ab-4b25-a5c2-ef03870a73a4/chats"} {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "archive_not_ready") {
			t.Fatalf("%s: %d %s", path, response.Code, response.Body.String())
		}
	}
}

func TestHistoryCursorBounds(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	raw := signHistoryCursor(key, historyCursor{Kind: "broadcasts", SessionID: "606c9e5a-05ab-4b25-a5c2-ef03870a73a4", StartedAt: time.Now()})
	if _, err := parseHistoryCursor(key, raw, "broadcasts"); err != nil {
		t.Fatal(err)
	}
	if _, err := parseHistoryCursor(key, strings.Repeat("a", 1025), "broadcasts"); err == nil {
		t.Fatal("oversized cursor accepted")
	}
}
