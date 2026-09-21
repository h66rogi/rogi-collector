package soopauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type diagnosticDoer func(*http.Request) (*http.Response, error)

func (f diagnosticDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }
func TestBroadcastLookupFixedEndpointAndSanitizedStates(t *testing.T) {
	cases := []struct{ name, body, state string }{
		{"live", `{"CHANNEL":{"RESULT":1,"CHATNO":"123","TITLE":"Fixture live","BJNICK":"Fixture","BNO":1234,"FTK":"synthetic-private-ticket"}}`, "live"},
		{"offline", `{"CHANNEL":{"RESULT":0,"TITLE":"stale title"}}`, "offline"},
		{"auth", `{"CHANNEL":{"RESULT":-6}}`, "auth_required"},
		{"unknown", `{"CHANNEL":{"RESULT":-999}}`, "lookup_failed"},
		{"missing-room", `{"CHANNEL":{"RESULT":1}}`, "lookup_failed"},
		{"malformed", `not json synthetic-private-ticket`, "lookup_failed"},
		{"oversize", strings.Repeat("x", (1<<20)+1), "lookup_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := diagnosticDoer(func(r *http.Request) (*http.Response, error) {
				if r.Method != "POST" || r.URL.String() != "https://live.sooplive.com/afreeca/player_live_api.php?bjid=fixture-channel" {
					t.Fatal("unexpected endpoint")
				}
				if err := r.ParseForm(); err != nil {
					t.Fatal(err)
				}
				if r.PostForm.Get("bid") != "fixture-channel" || r.PostForm.Get("type") != "live" {
					t.Fatal("wrong lookup form")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			got, err := CheckBroadcast(context.Background(), client, "fixture-channel")
			if err != nil || got.State != tc.state {
				t.Fatalf("state %s error %v", got.State, err)
			}
			if got.CheckedAt.IsZero() {
				t.Fatal("missing observation time")
			}
			encoded, _ := json.Marshal(got)
			if strings.Contains(string(encoded), "synthetic-private-ticket") {
				t.Fatal("raw player ticket escaped")
			}
			if tc.state != "live" && (got.Title != "" || got.BroadcastID != "") {
				t.Fatal("stale broadcast details escaped")
			}
			if tc.state == "live" && (got.Title != "Fixture live" || got.BroadcastID != "1234") {
				t.Fatal("missing broadcast facts")
			}
		})
	}
}
func TestBroadcastLookupInvalidIDsAndCookieFailure(t *testing.T) {
	calls := 0
	client := diagnosticDoer(func(*http.Request) (*http.Response, error) { calls++; return nil, &urlError{ErrCookieUnavailable} })
	for _, id := range []string{"", "https://example.com", "../secret", strings.Repeat("x", 51), "a\nb"} {
		if _, err := CheckBroadcast(context.Background(), client, id); err == nil {
			t.Fatalf("accepted invalid ID %q", id)
		}
	}
	if calls != 0 {
		t.Fatal("invalid ID reached network")
	}
	got, err := CheckBroadcast(context.Background(), client, "fixture-channel")
	if err != nil || got.State != "cookie_required" {
		t.Fatal("cookie failure must not mean offline")
	}
	client = diagnosticDoer(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("synthetic private transport error")
	})
	got, err = CheckBroadcast(context.Background(), client, "fixture-channel")
	if err != nil || got.State != "lookup_failed" {
		t.Fatal("transport error must be sanitized")
	}
}

type urlError struct{ error }

func (e *urlError) Unwrap() error { return e.error }
