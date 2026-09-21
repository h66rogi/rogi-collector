package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSoopDiscoveryFetchLiveChannels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("pageNo") {
		case "1":
			w.Write([]byte(`{"total_cnt":2,"broad":[{"user_id":"bj1","user_nick":"닉1","current_view_cnt":"123"}]}`))
		case "2":
			w.Write([]byte(`{"total_cnt":2,"broad":[{"user_id":"bj2","user_nick":"닉2","current_view_cnt":"45"}]}`))
		default:
			w.Write([]byte(`{"total_cnt":2,"broad":[]}`))
		}
	}))
	defer server.Close()

	d := newSoopDiscoveryForTest(server.Client(), server.URL, server.URL+"?bjid=%s")
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
	if result.Channels[0].ViewerCount != 123 || result.Channels[1].ViewerCount != 45 {
		t.Fatalf("unexpected viewer counts: %+v", result.Channels)
	}
}

func TestSoopFetchLiveChannels_ParsesMetadata(t *testing.T) {
	fixture, err := os.ReadFile("testdata/soop_broad_list.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(fixture)
	}))
	defer srv.Close()

	disc := newSoopDiscoveryForTest(srv.Client(), srv.URL, srv.URL+"?bjid=%s")
	result, err := disc.FetchLiveChannels(context.Background())
	if err != nil {
		t.Fatalf("FetchLiveChannels: %v", err)
	}
	if len(result.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(result.Channels))
	}
	ch := result.Channels[0]
	if ch.Title != "Synthetic tournament stream" {
		t.Errorf("Title = %q", ch.Title)
	}
	if ch.Category != "Strategy Game" {
		t.Errorf("Category = %q", ch.Category)
	}
	if ch.CategoryCode != "synthetic-category" {
		t.Errorf("CategoryCode = %q", ch.CategoryCode)
	}
	if ch.ThumbnailURL != "https://example.invalid/soop/thumbnail.jpg" {
		t.Errorf("ThumbnailURL = %q", ch.ThumbnailURL)
	}
	if ch.StartedAt == nil {
		t.Fatal("StartedAt is nil")
	}
	if len(ch.Tags) != 3 {
		t.Errorf("Tags len = %d, want 3", len(ch.Tags))
	}
	// Edge case
	ch2 := result.Channels[1]
	if ch2.Title != "" {
		t.Errorf("ch2.Title = %q", ch2.Title)
	}
	if ch2.StartedAt != nil {
		t.Errorf("ch2.StartedAt should be nil")
	}
}

func TestSoopDiscoveryIsChannelLive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("bjid") == "live" {
			w.Write([]byte(`{"CHANNEL":{"CHATNO":"12345"}}`))
			return
		}
		w.Write([]byte(`{"CHANNEL":{"RESULT":0,"CHATNO":""}}`))
	}))
	defer server.Close()

	d := newSoopDiscoveryForTest(server.Client(), server.URL, server.URL+"?bjid=%s")

	live, err := d.IsChannelLive(context.Background(), "live")
	if err != nil || !live {
		t.Fatalf("expected live channel to be live, got live=%v err=%v", live, err)
	}

	live, err = d.IsChannelLive(context.Background(), "offline")
	if err != nil {
		t.Fatalf("offline IsChannelLive returned error: %v", err)
	}
	if live {
		t.Fatal("expected offline channel to be false")
	}
}
