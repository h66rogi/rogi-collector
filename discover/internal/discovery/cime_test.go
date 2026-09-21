package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestCimeDiscoveryFetchLiveChannels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"bodyData":{"sections":[{"type":"EVENT_BANNER","items":[{"foo":"bar"}]},{"type":"LIVE","items":[{"curViewerCnt":10,"channel":{"slug":"slug-1","name":"채널1"}},{"curViewerCnt":20,"channel":{"slug":"slug-2","name":"채널2"}}]}]}}`))
	}))
	defer server.Close()

	d := newCimeDiscoveryForTest(server.Client(), server.URL, server.URL+"/@%s/live")
	result, err := d.FetchLiveChannels(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveChannels failed: %v", err)
	}
	if len(result.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(result.Channels))
	}
	if result.Partial {
		t.Fatal("expected Partial=false for complete fetch")
	}
}

func TestCimeFetchLiveChannels_ParsesMetadata(t *testing.T) {
	fixture, err := os.ReadFile("testdata/cime_live_list.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer srv.Close()

	disc := newCimeDiscoveryForTest(srv.Client(), srv.URL, srv.URL+"/@%s/live")
	result, err := disc.FetchLiveChannels(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveChannels: %v", err)
	}
	if len(result.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(result.Channels))
	}
	ch := result.Channels[0]
	if ch.Title != "Synthetic puzzle-game stream" {
		t.Errorf("Title = %q", ch.Title)
	}
	if ch.Category != "Puzzle Game" {
		t.Errorf("Category = %q", ch.Category)
	}
	if ch.CategoryCode != "puzzle" {
		t.Errorf("CategoryCode = %q", ch.CategoryCode)
	}
	if ch.ThumbnailURL != "https://example.invalid/cime/thumbnail.jpg" {
		t.Errorf("ThumbnailURL = %q", ch.ThumbnailURL)
	}
	if ch.StartedAt == nil {
		t.Fatal("StartedAt is nil")
	}
	if len(ch.Tags) != 2 || ch.Tags[0] != "virtual" {
		t.Errorf("Tags = %v", ch.Tags)
	}
	// Edge case: null category and tags
	ch2 := result.Channels[1]
	if ch2.Category != "" {
		t.Errorf("ch2.Category = %q, want empty", ch2.Category)
	}
	if len(ch2.Tags) != 0 {
		t.Errorf("ch2.Tags = %v, want empty", ch2.Tags)
	}
}

func TestCimeDiscoveryIsChannelLive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "@live/live") {
			w.Write([]byte(`{"bodyData":{"live":{"id":"1"}}}`))
			return
		}
		w.Write([]byte(`{"bodyData":{"live":null}}`))
	}))
	defer server.Close()

	d := newCimeDiscoveryForTest(server.Client(), server.URL, server.URL+"/@%s/live")

	live, err := d.IsChannelLive(context.Background(), "live")
	if err != nil || !live {
		t.Fatalf("expected live channel to be true, got live=%v err=%v", live, err)
	}

	live, err = d.IsChannelLive(context.Background(), "offline")
	if err != nil {
		t.Fatalf("offline IsChannelLive returned error: %v", err)
	}
	if live {
		t.Fatal("expected offline channel to be false")
	}
}
