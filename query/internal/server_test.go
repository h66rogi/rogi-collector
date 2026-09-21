package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	chatv1 "github.com/h66rogi/rogi-collector/proto/gen/go/meloming/chat/v1"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Mock store for testing
// ---------------------------------------------------------------------------

type mockQueryStore struct {
	// Legacy positional signature retained so existing test lambdas keep working.
	// The mock SearchLiveChannels method below unpacks the new args struct into
	// these positional params for the lambda.
	searchLiveChannelsFn      func(ctx context.Context, keyword, platform, category, tag, sort string, limit, offset int) ([]model.LiveChannel, int, error)
	searchBroadcastChannelsFn func(ctx context.Context, keyword string, platform string, lookbackDays int, limit int, offset int) ([]model.BroadcastChannel, int, error)
	getLiveDiscoveryFacetsFn  func(ctx context.Context, platform string, categoryLimit, tagLimit int) ([]store.FacetEntry, []store.FacetEntry, error)

	// Explore query mocks.
	getTrendingFn            func(ctx context.Context, platform string, boostWithinSeconds, limit int) ([]model.LiveChannel, error)
	getJustStartedFn         func(ctx context.Context, platform string, withinMinutes, limit int) ([]model.LiveChannel, error)
	getFeaturedFn            func(ctx context.Context, platform string, limit int) ([]model.LiveChannel, error)
	getPlatformLiveSummaryFn func(ctx context.Context, topPerPlatform int) ([]store.PlatformLiveSummary, error)
	getExploreStatsFn        func(ctx context.Context) (*store.ExploreStats, error)
	getCategoryLiveBundlesFn func(ctx context.Context, platform string, categoryCount, channelsPerCategory int) ([]store.CategoryLiveBundle, error)

	getChannelFn func(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error)
}

func (m *mockQueryStore) GetPlatformStats(ctx context.Context) ([]store.PlatformStats, error) {
	return nil, nil
}

func (m *mockQueryStore) ListChannelsFiltered(ctx context.Context, platform, status, sort string, page, size int) ([]model.LiveChannel, int, error) {
	return nil, 0, nil
}

func (m *mockQueryStore) ListAllWorkers(ctx context.Context) ([]model.Worker, error) {
	return nil, nil
}

func (m *mockQueryStore) GetLiveStatusesByChannelIDs(ctx context.Context, pairs []struct {
	Platform  string
	ChannelID string
}) ([]model.LiveChannel, error) {
	return nil, nil
}

// SearchLiveChannels accepts the new args struct, but unpacks legacy single-axis
// fields into the existing lambda signature so older tests continue to compile.
func (m *mockQueryStore) SearchLiveChannels(ctx context.Context, args store.SearchLiveChannelsArgs) ([]model.LiveChannel, int, error) {
	if m.searchLiveChannelsFn != nil {
		return m.searchLiveChannelsFn(ctx, args.Keyword, args.Platform, args.Category, args.Tag, args.SortOrder, args.Limit, args.Offset)
	}
	return nil, 0, nil
}

func (m *mockQueryStore) GetLiveDiscoveryFacets(ctx context.Context, platform string, categoryLimit, tagLimit int) ([]store.FacetEntry, []store.FacetEntry, error) {
	if m.getLiveDiscoveryFacetsFn != nil {
		return m.getLiveDiscoveryFacetsFn(ctx, platform, categoryLimit, tagLimit)
	}
	return nil, nil, nil
}

func (m *mockQueryStore) GetTrendingLiveChannels(ctx context.Context, platform string, boostWithinSeconds, limit int) ([]model.LiveChannel, error) {
	if m.getTrendingFn != nil {
		return m.getTrendingFn(ctx, platform, boostWithinSeconds, limit)
	}
	return nil, nil
}

func (m *mockQueryStore) GetJustStartedLiveChannels(ctx context.Context, platform string, withinMinutes, limit int) ([]model.LiveChannel, error) {
	if m.getJustStartedFn != nil {
		return m.getJustStartedFn(ctx, platform, withinMinutes, limit)
	}
	return nil, nil
}

func (m *mockQueryStore) GetFeaturedLiveChannels(ctx context.Context, platform string, limit int) ([]model.LiveChannel, error) {
	if m.getFeaturedFn != nil {
		return m.getFeaturedFn(ctx, platform, limit)
	}
	return nil, nil
}

func (m *mockQueryStore) GetPlatformLiveSummary(ctx context.Context, topPerPlatform int) ([]store.PlatformLiveSummary, error) {
	if m.getPlatformLiveSummaryFn != nil {
		return m.getPlatformLiveSummaryFn(ctx, topPerPlatform)
	}
	return nil, nil
}

func (m *mockQueryStore) GetExploreStats(ctx context.Context) (*store.ExploreStats, error) {
	if m.getExploreStatsFn != nil {
		return m.getExploreStatsFn(ctx)
	}
	return nil, nil
}

func (m *mockQueryStore) GetCategoryLiveBundles(ctx context.Context, platform string, categoryCount, channelsPerCategory int) ([]store.CategoryLiveBundle, error) {
	if m.getCategoryLiveBundlesFn != nil {
		return m.getCategoryLiveBundlesFn(ctx, platform, categoryCount, channelsPerCategory)
	}
	return nil, nil
}

func (m *mockQueryStore) SearchBroadcastChannels(ctx context.Context, keyword string, platform string, lookbackDays int, limit int, offset int) ([]model.BroadcastChannel, int, error) {
	if m.searchBroadcastChannelsFn != nil {
		return m.searchBroadcastChannelsFn(ctx, keyword, platform, lookbackDays, limit, offset)
	}
	return nil, 0, nil
}

func (m *mockQueryStore) GetChannel(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error) {
	if m.getChannelFn != nil {
		return m.getChannelFn(ctx, platform, channelID)
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Empty keywords are valid and return an unfiltered result.
// (chip-driven browsing). KeywordTooLong below still applies the 50-char cap.

func TestSearchLiveChannels_KeywordTooLong(t *testing.T) {
	s := &Server{}

	cases := []struct {
		name    string
		keyword string
	}{
		{"exactly 51 chars", strings.Repeat("a", 51)},
		{"100 chars", strings.Repeat("b", 100)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
				Keyword: tc.keyword,
			})
			if err == nil {
				t.Fatal("expected error for keyword too long, got nil")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected gRPC status error, got %v", err)
			}
			if st.Code() != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", st.Code())
			}
			if !strings.Contains(st.Message(), "max 50") {
				t.Fatalf("expected error message to mention max 50, got %q", st.Message())
			}
		})
	}
}

func TestSearchLiveChannels_KeywordAtBoundary(t *testing.T) {
	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			return nil, 0, nil
		},
	}
	s := &Server{
		pgStore: mock,
		logger:  slog.Default(),
	}

	// Exactly 50 chars should succeed (no validation error)
	_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Keyword: strings.Repeat("x", 50),
	})
	if err != nil {
		t.Fatalf("expected no error for 50 char keyword, got %v", err)
	}
}

func TestSearchLiveChannels_LimitClamping(t *testing.T) {
	cases := []struct {
		name          string
		inputLimit    int32
		expectedLimit int
	}{
		{"zero defaults to 10", 0, 10},
		{"negative defaults to 10", -5, 10},
		{"over 60 defaults to 10", 100, 10},
		{"exactly 61 defaults to 10", 61, 10},
		{"valid limit 1 passes through", 1, 1},
		{"valid limit 30 passes through", 30, 30},
		{"valid limit 60 passes through", 60, 60},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedLimit int
			mock := &mockQueryStore{
				searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
					capturedLimit = limit
					return nil, 0, nil
				},
			}
			s := &Server{
				pgStore: mock,
				logger:  slog.Default(),
			}

			_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
				Keyword: "test",
				Limit:   tc.inputLimit,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if capturedLimit != tc.expectedLimit {
				t.Fatalf("expected limit %d, got %d", tc.expectedLimit, capturedLimit)
			}
		})
	}
}

func TestSearchLiveChannels_PlatformNormalization(t *testing.T) {
	cases := []struct {
		name             string
		platform         *string
		expectedPlatform string
	}{
		{"nil platform passes empty string", nil, ""},
		{"CIME lowercased to cime", strPtr("CIME"), "cime"},
		{"Chzzk lowercased to chzzk", strPtr("Chzzk"), "chzzk"},
		{"SOOP lowercased to soop", strPtr("SOOP"), "soop"},
		{"already lowercase unchanged", strPtr("cime"), "cime"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedPlatform string
			mock := &mockQueryStore{
				searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
					capturedPlatform = platform
					return nil, 0, nil
				},
			}
			s := &Server{
				pgStore: mock,
				logger:  slog.Default(),
			}

			_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
				Keyword:  "test",
				Platform: tc.platform,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if capturedPlatform != tc.expectedPlatform {
				t.Fatalf("expected platform %q, got %q", tc.expectedPlatform, capturedPlatform)
			}
		})
	}
}

func TestSearchLiveChannels_ResponseMapping(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	title := "Live Stream Title"
	category := "Gaming"
	thumbnail := "https://example.com/thumb.jpg"

	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			return []model.LiveChannel{
				{
					Platform:           model.PlatformCime,
					ChannelID:          "ch-001",
					StreamerName:       "streamer1",
					ViewerCount:        1234,
					Title:              &title,
					Category:           &category,
					ThumbnailURL:       &thumbnail,
					BroadcastStartedAt: &now,
				},
				{
					Platform:     model.PlatformChzzk,
					ChannelID:    "ch-002",
					StreamerName: "streamer2",
					ViewerCount:  56,
					// nil optional fields
				},
			}, 2, nil
		},
	}
	s := &Server{
		pgStore: mock,
		logger:  slog.Default(),
	}

	resp, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Keyword: "stream",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Channels) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(resp.Channels))
	}
	if resp.Total != 2 {
		t.Errorf("expected total 2, got %d", resp.Total)
	}

	// Verify first channel (all optional fields set)
	ch1 := resp.Channels[0]
	if ch1.Platform != "cime" {
		t.Errorf("ch1 platform: expected %q, got %q", "cime", ch1.Platform)
	}
	if ch1.ChannelId != "ch-001" {
		t.Errorf("ch1 channel_id: expected %q, got %q", "ch-001", ch1.ChannelId)
	}
	if ch1.StreamerName != "streamer1" {
		t.Errorf("ch1 streamer_name: expected %q, got %q", "streamer1", ch1.StreamerName)
	}
	if ch1.ViewerCount != 1234 {
		t.Errorf("ch1 viewer_count: expected %d, got %d", 1234, ch1.ViewerCount)
	}
	if ch1.Title == nil || *ch1.Title != title {
		t.Errorf("ch1 title: expected %q, got %v", title, ch1.Title)
	}
	if ch1.Category == nil || *ch1.Category != category {
		t.Errorf("ch1 category: expected %q, got %v", category, ch1.Category)
	}
	if ch1.ThumbnailUrl == nil || *ch1.ThumbnailUrl != thumbnail {
		t.Errorf("ch1 thumbnail_url: expected %q, got %v", thumbnail, ch1.ThumbnailUrl)
	}
	expectedTime := now.Format(time.RFC3339)
	if ch1.BroadcastStartedAt == nil || *ch1.BroadcastStartedAt != expectedTime {
		t.Errorf("ch1 broadcast_started_at: expected %q, got %v", expectedTime, ch1.BroadcastStartedAt)
	}

	// Verify second channel (nil optional fields)
	ch2 := resp.Channels[1]
	if ch2.Platform != "chzzk" {
		t.Errorf("ch2 platform: expected %q, got %q", "chzzk", ch2.Platform)
	}
	if ch2.ChannelId != "ch-002" {
		t.Errorf("ch2 channel_id: expected %q, got %q", "ch-002", ch2.ChannelId)
	}
	if ch2.StreamerName != "streamer2" {
		t.Errorf("ch2 streamer_name: expected %q, got %q", "streamer2", ch2.StreamerName)
	}
	if ch2.ViewerCount != 56 {
		t.Errorf("ch2 viewer_count: expected %d, got %d", 56, ch2.ViewerCount)
	}
	if ch2.Title != nil {
		t.Errorf("ch2 title: expected nil, got %q", *ch2.Title)
	}
	if ch2.Category != nil {
		t.Errorf("ch2 category: expected nil, got %q", *ch2.Category)
	}
	if ch2.ThumbnailUrl != nil {
		t.Errorf("ch2 thumbnail_url: expected nil, got %q", *ch2.ThumbnailUrl)
	}
	if ch2.BroadcastStartedAt != nil {
		t.Errorf("ch2 broadcast_started_at: expected nil, got %q", *ch2.BroadcastStartedAt)
	}
}

func TestSearchLiveChannels_EmptyResult(t *testing.T) {
	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			return []model.LiveChannel{}, 0, nil
		},
	}
	s := &Server{
		pgStore: mock,
		logger:  slog.Default(),
	}

	resp, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Keyword: "nonexistent",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Channels) != 0 {
		t.Fatalf("expected 0 channels, got %d", len(resp.Channels))
	}
}

func TestSearchLiveChannels_StoreError(t *testing.T) {
	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			return nil, 0, fmt.Errorf("database connection failed")
		},
	}
	s := &Server{
		pgStore: mock,
		logger:  slog.Default(),
	}

	_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Keyword: "test",
	})
	if err == nil {
		t.Fatal("expected error when store fails, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal error code, got %v", st.Code())
	}
}

func TestSearchLiveChannels_OffsetPassthrough(t *testing.T) {
	cases := []struct {
		name           string
		inputOffset    int32
		expectedOffset int
	}{
		{"zero offset", 0, 0},
		{"negative offset clamps to zero", -10, 0},
		{"positive offset passes through", 60, 60},
		{"large offset passes through", 500, 500},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedOffset int
			mock := &mockQueryStore{
				searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
					capturedOffset = offset
					return nil, 0, nil
				},
			}
			s := &Server{
				pgStore: mock,
				logger:  slog.Default(),
			}

			_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
				Keyword: "test",
				Offset:  tc.inputOffset,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if capturedOffset != tc.expectedOffset {
				t.Fatalf("expected offset %d, got %d", tc.expectedOffset, capturedOffset)
			}
		})
	}
}

func TestSearchLiveChannels_TotalReflectsStoreReturn(t *testing.T) {
	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			// Page slice smaller than total — typical pagination scenario.
			return []model.LiveChannel{
				{Platform: model.PlatformCime, ChannelID: "ch-001", StreamerName: "s1", ViewerCount: 10},
			}, 137, nil
		},
	}
	s := &Server{
		pgStore: mock,
		logger:  slog.Default(),
	}

	resp, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Keyword: "syncroom",
		Limit:   1,
		Offset:  0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Total != 137 {
		t.Errorf("expected total 137, got %d", resp.Total)
	}
	if len(resp.Channels) != 1 {
		t.Errorf("expected 1 channel slice, got %d", len(resp.Channels))
	}
}

// ---------------------------------------------------------------------------
// Tests for filter-driven browsing.
// ---------------------------------------------------------------------------

func TestSearchLiveChannels_NoKeywordWithCategoryOK(t *testing.T) {
	var capturedKeyword, capturedCategory string
	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			capturedKeyword = keyword
			capturedCategory = categoryFilter
			return nil, 0, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}

	_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Category: "버추얼",
	})
	if err != nil {
		t.Fatalf("expected no error for empty keyword + category, got %v", err)
	}
	if capturedKeyword != "" {
		t.Errorf("expected empty keyword passed to store, got %q", capturedKeyword)
	}
	if capturedCategory != "버추얼" {
		t.Errorf("expected category=%q, got %q", "버추얼", capturedCategory)
	}
}

func TestSearchLiveChannels_TagFilterPropagated(t *testing.T) {
	var capturedTag string
	mock := &mockQueryStore{
		searchLiveChannelsFn: func(ctx context.Context, keyword, platform, categoryFilter, tagFilter, sort string, limit, offset int) ([]model.LiveChannel, int, error) {
			capturedTag = tagFilter
			return nil, 0, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}

	_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
		Tag: "싱크룸",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedTag != "싱크룸" {
		t.Errorf("expected tag=%q, got %q", "싱크룸", capturedTag)
	}
}

func TestSearchLiveChannels_SortValidation(t *testing.T) {
	s := &Server{pgStore: &mockQueryStore{}, logger: slog.Default()}

	cases := []struct {
		sort    string
		wantErr bool
	}{
		{"", false},
		{"viewer_desc", false},
		{"started_at_desc", false},
		{"random", true},
	}
	for _, tc := range cases {
		t.Run(tc.sort, func(t *testing.T) {
			_, err := s.SearchLiveChannels(context.Background(), &chatv1.SearchLiveChannelsRequest{
				Sort: tc.sort,
			})
			if tc.wantErr && err == nil {
				t.Fatal("expected InvalidArgument, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestGetLiveDiscoveryFacets_DefaultsAndClamping(t *testing.T) {
	cases := []struct {
		name              string
		categoryLimit     int32
		tagLimit          int32
		wantCategoryLimit int
		wantTagLimit      int
	}{
		{"zero -> defaults", 0, 0, 12, 24},
		{"in range passes through", 5, 30, 5, 30},
		{"oversize clamps", 999, 999, 50, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedCat, capturedTag int
			mock := &mockQueryStore{
				getLiveDiscoveryFacetsFn: func(ctx context.Context, platform string, categoryLimit, tagLimit int) ([]store.FacetEntry, []store.FacetEntry, error) {
					capturedCat = categoryLimit
					capturedTag = tagLimit
					return nil, nil, nil
				},
			}
			s := &Server{pgStore: mock, logger: slog.Default()}

			_, err := s.GetLiveDiscoveryFacets(context.Background(), &chatv1.GetLiveDiscoveryFacetsRequest{
				CategoryLimit: tc.categoryLimit,
				TagLimit:      tc.tagLimit,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if capturedCat != tc.wantCategoryLimit {
				t.Errorf("category_limit: expected %d, got %d", tc.wantCategoryLimit, capturedCat)
			}
			if capturedTag != tc.wantTagLimit {
				t.Errorf("tag_limit: expected %d, got %d", tc.wantTagLimit, capturedTag)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetBroadcastHistory: from/to range + days fallback
// ---------------------------------------------------------------------------

// mockHistoryStore implements historyQueryStore so tests can verify the handler
// dispatches to the right store method with the right arguments.
type mockHistoryStore struct {
	listByDaysFn  func(ctx context.Context, platform model.Platform, channelID string, days, limit int) ([]store.BroadcastSession, int, error)
	listByRangeFn func(ctx context.Context, platform model.Platform, channelID string, from, to time.Time, limit int) ([]store.BroadcastSession, int, error)

	listByDaysCalls  int
	listByRangeCalls int
}

func (m *mockHistoryStore) ListChannelBroadcastHistory(ctx context.Context, platform model.Platform, channelID string, days, limit int) ([]store.BroadcastSession, int, error) {
	m.listByDaysCalls++
	if m.listByDaysFn != nil {
		return m.listByDaysFn(ctx, platform, channelID, days, limit)
	}
	return nil, 0, nil
}

func (m *mockHistoryStore) ListChannelBroadcastHistoryByRange(ctx context.Context, platform model.Platform, channelID string, from, to time.Time, limit int) ([]store.BroadcastSession, int, error) {
	m.listByRangeCalls++
	if m.listByRangeFn != nil {
		return m.listByRangeFn(ctx, platform, channelID, from, to, limit)
	}
	return nil, 0, nil
}

var _ historyQueryStore = (*mockHistoryStore)(nil)

func TestGetBroadcastHistory_RequiredArgs(t *testing.T) {
	s := &Server{historyStore: &mockHistoryStore{}, logger: slog.Default()}

	cases := []struct {
		name      string
		platform  string
		channelID string
	}{
		{"both empty", "", ""},
		{"empty platform", "", "ch1"},
		{"empty channel", "chzzk", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.GetBroadcastHistory(context.Background(), &chatv1.GetBroadcastHistoryRequest{
				Platform:  tc.platform,
				ChannelId: tc.channelID,
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			st, _ := status.FromError(err)
			if st.Code() != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", st.Code())
			}
		})
	}
}

func TestGetBroadcastHistory_DaysFallback(t *testing.T) {
	var (
		gotDays  int
		gotLimit int
	)
	mock := &mockHistoryStore{
		listByDaysFn: func(_ context.Context, _ model.Platform, _ string, days, limit int) ([]store.BroadcastSession, int, error) {
			gotDays = days
			gotLimit = limit
			return nil, 0, nil
		},
	}
	s := &Server{historyStore: mock, logger: slog.Default()}

	_, err := s.GetBroadcastHistory(context.Background(), &chatv1.GetBroadcastHistoryRequest{
		Platform:  "chzzk",
		ChannelId: "ch1",
		Days:      7,
		Limit:     30,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.listByDaysCalls != 1 {
		t.Fatalf("expected listByDays once, got %d", mock.listByDaysCalls)
	}
	if mock.listByRangeCalls != 0 {
		t.Fatalf("expected listByRange zero, got %d", mock.listByRangeCalls)
	}
	if gotDays != 7 || gotLimit != 30 {
		t.Fatalf("unexpected days/limit dispatched: days=%d limit=%d", gotDays, gotLimit)
	}
}

func TestGetBroadcastHistory_FromToHappyPath(t *testing.T) {
	var (
		gotFrom  time.Time
		gotTo    time.Time
		gotLimit int
	)
	mock := &mockHistoryStore{
		listByRangeFn: func(_ context.Context, _ model.Platform, _ string, from, to time.Time, limit int) ([]store.BroadcastSession, int, error) {
			gotFrom = from
			gotTo = to
			gotLimit = limit
			return nil, 0, nil
		},
	}
	s := &Server{historyStore: mock, logger: slog.Default()}

	from := "2026-05-01T00:00:00+09:00"
	to := "2026-05-08T00:00:00+09:00"

	resp, err := s.GetBroadcastHistory(context.Background(), &chatv1.GetBroadcastHistoryRequest{
		Platform:  "chzzk",
		ChannelId: "ch1",
		Limit:     20,
		From:      &from,
		To:        &to,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil resp")
	}
	if mock.listByRangeCalls != 1 {
		t.Fatalf("expected listByRange once, got %d", mock.listByRangeCalls)
	}
	if mock.listByDaysCalls != 0 {
		t.Fatalf("expected listByDays zero, got %d", mock.listByDaysCalls)
	}
	wantFrom, _ := time.Parse(time.RFC3339, from)
	wantTo, _ := time.Parse(time.RFC3339, to)
	if !gotFrom.Equal(wantFrom) {
		t.Fatalf("from mismatch: got %v, want %v", gotFrom, wantFrom)
	}
	if !gotTo.Equal(wantTo) {
		t.Fatalf("to mismatch: got %v, want %v", gotTo, wantTo)
	}
	if gotLimit != 20 {
		t.Fatalf("limit mismatch: got %d", gotLimit)
	}
}

func TestGetBroadcastHistory_FromToTakesPrecedenceOverDays(t *testing.T) {
	mock := &mockHistoryStore{}
	s := &Server{historyStore: mock, logger: slog.Default()}

	from := "2026-05-01T00:00:00Z"
	to := "2026-05-02T00:00:00Z"

	_, err := s.GetBroadcastHistory(context.Background(), &chatv1.GetBroadcastHistoryRequest{
		Platform:  "chzzk",
		ChannelId: "ch1",
		Days:      30,
		From:      &from,
		To:        &to,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.listByRangeCalls != 1 || mock.listByDaysCalls != 0 {
		t.Fatalf("expected listByRange to win over days; range=%d days=%d", mock.listByRangeCalls, mock.listByDaysCalls)
	}
}

func TestGetBroadcastHistory_ValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		req  *chatv1.GetBroadcastHistoryRequest
		want codes.Code
	}{
		{
			name: "only from",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				From: strPtr("2026-05-01T00:00:00Z"),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "only to",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				To: strPtr("2026-05-02T00:00:00Z"),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "invalid from",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				From: strPtr("not-a-date"),
				To:   strPtr("2026-05-02T00:00:00Z"),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "invalid to",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				From: strPtr("2026-05-01T00:00:00Z"),
				To:   strPtr("not-a-date"),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "to equals from",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				From: strPtr("2026-05-01T00:00:00Z"),
				To:   strPtr("2026-05-01T00:00:00Z"),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "to before from",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				From: strPtr("2026-05-02T00:00:00Z"),
				To:   strPtr("2026-05-01T00:00:00Z"),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "range over 90 days",
			req: &chatv1.GetBroadcastHistoryRequest{
				Platform: "chzzk", ChannelId: "ch1",
				From: strPtr("2026-01-01T00:00:00Z"),
				To:   strPtr("2026-04-02T00:00:00Z"), // 91 days
			},
			want: codes.InvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockHistoryStore{}
			s := &Server{historyStore: mock, logger: slog.Default()}
			_, err := s.GetBroadcastHistory(context.Background(), tc.req)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected gRPC status error, got %v", err)
			}
			if st.Code() != tc.want {
				t.Fatalf("expected %v, got %v (msg: %s)", tc.want, st.Code(), st.Message())
			}
			// Validation must fail before any store call.
			if mock.listByRangeCalls != 0 || mock.listByDaysCalls != 0 {
				t.Fatalf("validation should not reach store; range=%d days=%d", mock.listByRangeCalls, mock.listByDaysCalls)
			}
		})
	}
}

func TestGetBroadcastHistory_RangeAtBoundary(t *testing.T) {
	// Exactly 90 days should pass.
	mock := &mockHistoryStore{}
	s := &Server{historyStore: mock, logger: slog.Default()}
	_, err := s.GetBroadcastHistory(context.Background(), &chatv1.GetBroadcastHistoryRequest{
		Platform:  "chzzk",
		ChannelId: "ch1",
		From:      strPtr("2026-01-01T00:00:00Z"),
		To:        strPtr("2026-04-01T00:00:00Z"), // exactly 90 days
	})
	if err != nil {
		t.Fatalf("expected 90 day range to pass, got %v", err)
	}
	if mock.listByRangeCalls != 1 {
		t.Fatalf("expected store to be called once, got %d", mock.listByRangeCalls)
	}
}

// ---------------------------------------------------------------------------
// Explore RPC tests.
// ---------------------------------------------------------------------------

func TestGetTrendingLiveChannels_ClampAndPropagation(t *testing.T) {
	cases := []struct {
		name          string
		inputLimit    int32
		inputBoost    int32
		inputPlatform *string
		wantLimit     int
		wantBoost     int
		wantPlatform  string
	}{
		{"defaults", 0, 0, nil, 12, 3600, ""},
		{"in-range passes through", 30, 1800, strPtr("CHZZK"), 30, 1800, "chzzk"},
		{"over 60 → clamps to 60", 999, 0, nil, 60, 3600, ""},
		{"under 1 → default", -5, 0, nil, 12, 3600, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPlatform string
			var gotBoost, gotLimit int
			mock := &mockQueryStore{
				getTrendingFn: func(ctx context.Context, p string, boost, limit int) ([]model.LiveChannel, error) {
					gotPlatform = p
					gotBoost = boost
					gotLimit = limit
					return nil, nil
				},
			}
			s := &Server{pgStore: mock, logger: slog.Default()}
			req := &chatv1.GetTrendingLiveChannelsRequest{
				Limit:              tc.inputLimit,
				BoostWithinSeconds: tc.inputBoost,
			}
			if tc.inputPlatform != nil {
				req.Platform = tc.inputPlatform
			}
			if _, err := s.GetTrendingLiveChannels(context.Background(), req); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotLimit != tc.wantLimit {
				t.Errorf("limit: got %d, want %d", gotLimit, tc.wantLimit)
			}
			if gotBoost != tc.wantBoost {
				t.Errorf("boost: got %d, want %d", gotBoost, tc.wantBoost)
			}
			if gotPlatform != tc.wantPlatform {
				t.Errorf("platform: got %q, want %q", gotPlatform, tc.wantPlatform)
			}
		})
	}
}

func TestGetTrendingLiveChannels_ResponseMapping(t *testing.T) {
	mock := &mockQueryStore{
		getTrendingFn: func(ctx context.Context, _ string, _, _ int) ([]model.LiveChannel, error) {
			title := "Live Title"
			return []model.LiveChannel{
				{
					Platform:     model.PlatformChzzk,
					ChannelID:    "ch1",
					StreamerName: "Streamer",
					ViewerCount:  500,
					Title:        &title,
				},
			}, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}
	resp, err := s.GetTrendingLiveChannels(context.Background(), &chatv1.GetTrendingLiveChannelsRequest{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(resp.Channels))
	}
	c := resp.Channels[0]
	if c.ChannelId != "ch1" || c.StreamerName != "Streamer" || c.ViewerCount != 500 {
		t.Errorf("response mapping mismatch: %+v", c)
	}
	if c.Title == nil || *c.Title != "Live Title" {
		t.Errorf("expected title 'Live Title', got %v", c.Title)
	}
}

func TestGetJustStartedLiveChannels_ClampAndDefaults(t *testing.T) {
	cases := []struct {
		name            string
		inputLimit      int32
		inputWithinMins int32
		wantLimit       int
		wantWithinMins  int
	}{
		{"defaults", 0, 0, 12, 30},
		{"oversize within_minutes clamps", 0, 9999, 12, 180},
		{"valid passes through", 5, 60, 5, 60},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotLimit, gotWithin int
			mock := &mockQueryStore{
				getJustStartedFn: func(ctx context.Context, p string, within, limit int) ([]model.LiveChannel, error) {
					gotLimit = limit
					gotWithin = within
					return nil, nil
				},
			}
			s := &Server{pgStore: mock, logger: slog.Default()}
			_, err := s.GetJustStartedLiveChannels(context.Background(), &chatv1.GetJustStartedLiveChannelsRequest{
				Limit:         tc.inputLimit,
				WithinMinutes: tc.inputWithinMins,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotLimit != tc.wantLimit {
				t.Errorf("limit: got %d, want %d", gotLimit, tc.wantLimit)
			}
			if gotWithin != tc.wantWithinMins {
				t.Errorf("within: got %d, want %d", gotWithin, tc.wantWithinMins)
			}
		})
	}
}

func TestGetFeaturedLiveChannels_ClampAndPlatformNormalization(t *testing.T) {
	var gotLimit int
	var gotPlatform string
	mock := &mockQueryStore{
		getFeaturedFn: func(ctx context.Context, p string, limit int) ([]model.LiveChannel, error) {
			gotPlatform = p
			gotLimit = limit
			return nil, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}
	_, err := s.GetFeaturedLiveChannels(context.Background(), &chatv1.GetFeaturedLiveChannelsRequest{
		Limit:    0,
		Platform: strPtr("SOOP"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotLimit != 24 {
		t.Errorf("expected default limit 24, got %d", gotLimit)
	}
	if gotPlatform != "soop" {
		t.Errorf("expected lowercase platform 'soop', got %q", gotPlatform)
	}
}

func TestGetPlatformLiveSummary_ClampAndResponseMapping(t *testing.T) {
	mock := &mockQueryStore{
		getPlatformLiveSummaryFn: func(ctx context.Context, topPer int) ([]store.PlatformLiveSummary, error) {
			if topPer != 6 {
				t.Errorf("expected default topPer 6, got %d", topPer)
			}
			return []store.PlatformLiveSummary{
				{
					Platform:    "chzzk",
					ActiveCount: 152,
					TopChannels: []model.LiveChannel{
						{Platform: model.PlatformChzzk, ChannelID: "c1", StreamerName: "S1", ViewerCount: 100},
					},
				},
			}, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}
	resp, err := s.GetPlatformLiveSummary(context.Background(), &chatv1.GetPlatformLiveSummaryRequest{TopPerPlatform: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Platforms) != 1 {
		t.Fatalf("expected 1 platform, got %d", len(resp.Platforms))
	}
	p := resp.Platforms[0]
	if p.Platform != "chzzk" || p.ActiveCount != 152 {
		t.Errorf("response mismatch: %+v", p)
	}
	if len(p.TopChannels) != 1 || p.TopChannels[0].ChannelId != "c1" {
		t.Errorf("top channels mismatch: %+v", p.TopChannels)
	}
}

func TestGetExploreStats_ResponseMapping(t *testing.T) {
	mock := &mockQueryStore{
		getExploreStatsFn: func(ctx context.Context) (*store.ExploreStats, error) {
			return &store.ExploreStats{
				ActiveLiveCount:       482,
				DistinctCategoryCount: 27,
				DistinctTagCount:      134,
				TotalViewerCount:      38291,
				PlatformCounts:        map[string]int{"chzzk": 220, "soop": 191},
			}, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}
	resp, err := s.GetExploreStats(context.Background(), &chatv1.GetExploreStatsRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ActiveLiveCount != 482 {
		t.Errorf("active: got %d, want 482", resp.ActiveLiveCount)
	}
	if resp.DistinctCategoryCount != 27 || resp.DistinctTagCount != 134 {
		t.Errorf("distinct counts mismatch: cat=%d, tag=%d", resp.DistinctCategoryCount, resp.DistinctTagCount)
	}
	if resp.TotalViewerCount != 38291 {
		t.Errorf("total viewer: got %d, want 38291", resp.TotalViewerCount)
	}
	if resp.PlatformCounts["chzzk"] != 220 || resp.PlatformCounts["soop"] != 191 {
		t.Errorf("platform_counts mismatch: %+v", resp.PlatformCounts)
	}
}

func TestGetExploreStats_StoreError(t *testing.T) {
	mock := &mockQueryStore{
		getExploreStatsFn: func(ctx context.Context) (*store.ExploreStats, error) {
			return nil, fmt.Errorf("db down")
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}
	_, err := s.GetExploreStats(context.Background(), &chatv1.GetExploreStatsRequest{})
	if err == nil {
		t.Fatal("expected error on store failure, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v", err)
	}
}

func TestGetCategoryLiveBundles_ClampAndMapping(t *testing.T) {
	var gotCatCount, gotChPerCat int
	mock := &mockQueryStore{
		getCategoryLiveBundlesFn: func(ctx context.Context, p string, catCount, chPerCat int) ([]store.CategoryLiveBundle, error) {
			gotCatCount = catCount
			gotChPerCat = chPerCat
			return []store.CategoryLiveBundle{
				{
					Category:    "버추얼",
					ActiveCount: 23,
					Channels: []model.LiveChannel{
						{Platform: model.PlatformChzzk, ChannelID: "ch1", StreamerName: "S1", ViewerCount: 50},
					},
				},
			}, nil
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}
	resp, err := s.GetCategoryLiveBundles(context.Background(), &chatv1.GetCategoryLiveBundlesRequest{
		CategoryCount:       0,
		ChannelsPerCategory: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotCatCount != 6 || gotChPerCat != 6 {
		t.Errorf("defaults: cat=%d, chPerCat=%d (want 6, 6)", gotCatCount, gotChPerCat)
	}
	if len(resp.Bundles) != 1 {
		t.Fatalf("expected 1 bundle, got %d", len(resp.Bundles))
	}
	b := resp.Bundles[0]
	if b.Category != "버추얼" || b.ActiveCount != 23 {
		t.Errorf("bundle mismatch: %+v", b)
	}
	if len(b.Channels) != 1 || b.Channels[0].StreamerName != "S1" {
		t.Errorf("channels mismatch: %+v", b.Channels)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string {
	return &s
}

// ---------------------------------------------------------------------------
// ForceReconnectChannel / RecoverChannel — graceful skip on ErrNoRows
// ---------------------------------------------------------------------------

func TestForceReconnectChannel_ErrNoRowsGracefulSkip(t *testing.T) {
	mock := &mockQueryStore{
		getChannelFn: func(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error) {
			return nil, pgx.ErrNoRows
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}

	resp, err := s.ForceReconnectChannel(context.Background(), &chatv1.ForceReconnectChannelRequest{
		Platform:  "chzzk",
		ChannelId: "abc123",
	})
	if err != nil {
		t.Fatalf("expected nil error on ErrNoRows, got %v", err)
	}
	if resp == nil || resp.Dispatched {
		t.Fatalf("expected Dispatched=false response, got %+v", resp)
	}
}

func TestForceReconnectChannel_OtherDBErrorReturnsUnavailable(t *testing.T) {
	mock := &mockQueryStore{
		getChannelFn: func(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}

	_, err := s.ForceReconnectChannel(context.Background(), &chatv1.ForceReconnectChannelRequest{
		Platform:  "chzzk",
		ChannelId: "abc123",
	})
	if err == nil {
		t.Fatal("expected error on connection failure, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
}

func TestRecoverChannel_ErrNoRowsGracefulSkip(t *testing.T) {
	mock := &mockQueryStore{
		getChannelFn: func(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error) {
			return nil, pgx.ErrNoRows
		},
	}
	// RediscoverClient nil → rediscover step skip. ch == nil → reconnect skip.
	s := &Server{pgStore: mock, logger: slog.Default()}

	resp, err := s.RecoverChannel(context.Background(), &chatv1.RecoverChannelRequest{
		Platform:  "chzzk",
		ChannelId: "abc123",
	})
	if err != nil {
		t.Fatalf("expected nil error on ErrNoRows, got %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if resp.ReconnectDispatched {
		t.Fatalf("expected ReconnectDispatched=false, got %+v", resp)
	}
}

func TestRecoverChannel_OtherDBErrorReturnsUnavailable(t *testing.T) {
	mock := &mockQueryStore{
		getChannelFn: func(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error) {
			return nil, fmt.Errorf("scan: type mismatch")
		},
	}
	s := &Server{pgStore: mock, logger: slog.Default()}

	_, err := s.RecoverChannel(context.Background(), &chatv1.RecoverChannelRequest{
		Platform:  "chzzk",
		ChannelId: "abc123",
	})
	if err == nil {
		t.Fatal("expected error on scan failure, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
}
