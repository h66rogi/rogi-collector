package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestChzzkDiscoveryFetchLiveChannels(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/lives", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("liveId"); got == "" {
			w.Write([]byte(`{"content":{"data":[{"concurrentUserCount":100,"channel":{"channelId":"chan-1","channelName":"Alpha"}}],"page":{"next":{"concurrentUserCount":99,"liveId":10}}}}`))
			return
		}
		w.Write([]byte(`{"content":{"data":[{"concurrentUserCount":50,"channel":{"channelId":"chan-2","channelName":"Beta"}}],"page":{"next":null}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	d := newChzzkDiscoveryForTest(server.Client(), server.URL+"/lives", server.URL+"/channels/%s/live-detail")
	result, err := d.FetchLiveChannels(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveChannels failed: %v", err)
	}
	if len(result.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(result.Channels))
	}
	if result.Partial {
		t.Fatal("expected Partial=false for complete pagination")
	}
	if result.Channels[0].ChannelID != "chan-1" || result.Channels[1].ChannelID != "chan-2" {
		t.Fatalf("unexpected channels: %+v", result.Channels)
	}
}

func TestChzzkFetchLiveChannels_ParsesMetadata(t *testing.T) {
	fixture, err := os.ReadFile("testdata/chzzk_live_list.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer srv.Close()

	disc := newChzzkDiscoveryForTest(srv.Client(), srv.URL, srv.URL+"/%s")
	result, err := disc.FetchLiveChannels(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveChannels: %v", err)
	}
	if len(result.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(result.Channels))
	}
	ch := result.Channels[0]
	if ch.Title != "Synthetic strategy-game stream" {
		t.Errorf("Title = %q", ch.Title)
	}
	if ch.Category != "Strategy Game" {
		t.Errorf("Category = %q", ch.Category)
	}
	if ch.CategoryCode != "Strategy_Game" {
		t.Errorf("CategoryCode = %q", ch.CategoryCode)
	}
	if ch.ThumbnailURL != "https://example.invalid/chzzk/thumbnail.jpg" {
		t.Errorf("ThumbnailURL = %q", ch.ThumbnailURL)
	}
	if ch.StartedAt == nil {
		t.Fatal("StartedAt is nil")
	}
	// Edge case: empty/null fields
	ch2 := result.Channels[1]
	if ch2.Title != "" {
		t.Errorf("ch2.Title = %q, want empty", ch2.Title)
	}
	if ch2.StartedAt != nil {
		t.Errorf("ch2.StartedAt should be nil")
	}
}

func TestChzzkDiscoveryIsChannelLive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/open/live-detail") {
			w.Write([]byte(`{"content":{"status":"CLOSE"}}`))
			return
		}
		w.Write([]byte(`{"content":{"status":"OPEN"}}`))
	}))
	defer server.Close()

	d := newChzzkDiscoveryForTest(server.Client(), server.URL+"/lives", server.URL+"/%s/live-detail")

	live, err := d.IsChannelLive(context.Background(), "open")
	if err != nil || !live {
		t.Fatalf("expected open channel to be live, got live=%v err=%v", live, err)
	}

	live, err = d.IsChannelLive(context.Background(), "closed")
	if err != nil {
		t.Fatalf("closed IsChannelLive returned error: %v", err)
	}
	if live {
		t.Fatal("expected closed channel to be offline")
	}
}
