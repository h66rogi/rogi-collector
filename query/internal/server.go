package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	chatv1 "github.com/h66rogi/rogi-collector/proto/gen/go/meloming/chat/v1"
	commonv1 "github.com/h66rogi/rogi-collector/proto/gen/go/meloming/common/v1"
	"github.com/h66rogi/rogi-collector/shared/grpcauth"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// queryStore is the subset of store operations needed by the query server.
type queryStore interface {
	GetPlatformStats(ctx context.Context) ([]store.PlatformStats, error)
	ListChannelsFiltered(ctx context.Context, platform, status, sort string, page, size int) ([]model.LiveChannel, int, error)
	ListAllWorkers(ctx context.Context) ([]model.Worker, error)
	GetLiveStatusesByChannelIDs(ctx context.Context, pairs []struct {
		Platform  string
		ChannelID string
	}) ([]model.LiveChannel, error)
	SearchLiveChannels(ctx context.Context, args store.SearchLiveChannelsArgs) ([]model.LiveChannel, int, error)
	GetLiveDiscoveryFacets(ctx context.Context, platform string, categoryLimit, tagLimit int) ([]store.FacetEntry, []store.FacetEntry, error)
	GetTrendingLiveChannels(ctx context.Context, platform string, boostWithinSeconds, limit int) ([]model.LiveChannel, error)
	GetJustStartedLiveChannels(ctx context.Context, platform string, withinMinutes, limit int) ([]model.LiveChannel, error)
	GetFeaturedLiveChannels(ctx context.Context, platform string, limit int) ([]model.LiveChannel, error)
	GetPlatformLiveSummary(ctx context.Context, topPerPlatform int) ([]store.PlatformLiveSummary, error)
	GetExploreStats(ctx context.Context) (*store.ExploreStats, error)
	GetCategoryLiveBundles(ctx context.Context, platform string, categoryCount, channelsPerCategory int) ([]store.CategoryLiveBundle, error)
	SearchBroadcastChannels(ctx context.Context, keyword string, platform string, lookbackDays int, limit int, offset int) ([]model.BroadcastChannel, int, error)
	GetChannel(ctx context.Context, platform model.Platform, channelID string) (*model.LiveChannel, error)
}

// RediscoverClient triggers single-channel re-detection on the discover service.
// At runtime this is a thin transport wrapper; tests inject a stub.
type RediscoverClient interface {
	ForceRediscover(ctx context.Context, platform, channelID string) (RediscoverResult, error)
}

// RediscoverResult is the parsed response from the discover admin endpoint.
type RediscoverResult struct {
	Status string // resulting channel status: "pending" | "live" | "ended"
}

// historyQueryStore is the read-side subset of HistoryStore used by the query
// server. Defined as an interface so tests can inject a fake without spinning
// up a real Postgres pool.
type historyQueryStore interface {
	ListChannelBroadcastHistory(ctx context.Context, platform model.Platform, channelID string, days int, limit int) ([]store.BroadcastSession, int, error)
	ListChannelBroadcastHistoryByRange(ctx context.Context, platform model.Platform, channelID string, from, to time.Time, limit int) ([]store.BroadcastSession, int, error)
}

// Server wraps a gRPC server that implements ChatAdminService and ChatQueryService.
type Server struct {
	chatv1.UnimplementedChatAdminServiceServer
	chatv1.UnimplementedChatQueryServiceServer

	pgStore          queryStore
	redisStore       *store.RedisStore
	historyStore     historyQueryStore
	RediscoverClient RediscoverClient
	grpcServer       *grpc.Server
	port             int
	logger           *slog.Logger
}

// NewServer creates a new gRPC query server.
func NewServer(pgStore *store.PgStore, redisStore *store.RedisStore, historyStore *store.PgHistoryStore, rediscover RediscoverClient, port int, apiKey string, enableReflection bool, logger *slog.Logger) *Server {
	s := &Server{
		pgStore:          pgStore,
		redisStore:       redisStore,
		historyStore:     historyStore,
		RediscoverClient: rediscover,
		port:             port,
		logger:           logger.With("component", "grpc-server"),
	}

	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpcauth.UnaryServerInterceptor(apiKey)),
		grpc.ChainStreamInterceptor(grpcauth.StreamServerInterceptor(apiKey)),
		grpc.MaxRecvMsgSize(2<<20),
		grpc.MaxHeaderListSize(64<<10),
	)
	chatv1.RegisterChatAdminServiceServer(gs, s)
	chatv1.RegisterChatQueryServiceServer(gs, s)
	if enableReflection {
		reflection.Register(gs)
	}
	s.grpcServer = gs

	return s
}

// Start listens on the configured port. Blocks until Stop is called.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	s.logger.Info("gRPC server listening", "port", s.port)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	done := make(chan struct{})
	go func() { s.grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.grpcServer.Stop()
	}
}

// ---------------------------------------------------------------------------
// ChatAdminService RPC implementations (copied from coordinator)
// ---------------------------------------------------------------------------

func (s *Server) GetPlatformStats(ctx context.Context, _ *chatv1.GetPlatformStatsRequest) (*chatv1.GetPlatformStatsResponse, error) {
	stats, err := s.pgStore.GetPlatformStats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "get platform stats: %v", err)
	}

	resp := &chatv1.GetPlatformStatsResponse{
		Platforms: make([]*chatv1.PlatformStats, len(stats)),
	}
	for i, st := range stats {
		resp.Platforms[i] = &chatv1.PlatformStats{
			Platform:            st.Platform,
			LiveChannelCount:    int64(st.LiveCount),
			PendingChannelCount: int64(st.PendingCount),
			EndedChannelCount:   int64(st.EndedCount),
			TotalViewerCount:    st.TotalViewerCount,
		}
	}
	return resp, nil
}

func (s *Server) ListChannels(ctx context.Context, req *chatv1.ListChannelsRequest) (*chatv1.ListChannelsResponse, error) {
	var (
		platform, channelStatus, sort string
		page, size                    int
	)

	if req.Platform != nil {
		platform = *req.Platform
	}
	if req.Status != nil {
		channelStatus = *req.Status
	}
	if req.Sort != nil {
		sort = *req.Sort
	}
	if req.Pagination != nil {
		page = int(req.Pagination.Page)
		size = int(req.Pagination.Size)
	}

	channels, total, err := s.pgStore.ListChannelsFiltered(ctx, platform, channelStatus, sort, page, size)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list channels: %v", err)
	}

	resp := &chatv1.ListChannelsResponse{
		Channels: make([]*chatv1.LiveChannel, len(channels)),
		Pagination: &commonv1.PaginationResponse{
			Total: int64(total),
			Page:  int32(page),
			Size:  int32(size),
		},
	}
	for i, ch := range channels {
		lc := &chatv1.LiveChannel{
			Id:           ch.ID,
			Platform:     string(ch.Platform),
			ChannelId:    ch.ChannelID,
			StreamerName: ch.StreamerName,
			ViewerCount:  int64(ch.ViewerCount),
			Status:       ch.Status,
			DiscoveredAt: ch.DiscoveredAt.Format(time.RFC3339),
			LastSeenAt:   ch.LastSeenAt.Format(time.RFC3339),
		}
		if ch.WorkerID != nil {
			lc.WorkerId = ch.WorkerID
		}
		if ch.EndedAt != nil {
			t := ch.EndedAt.Format(time.RFC3339)
			lc.EndedAt = &t
		}
		resp.Channels[i] = lc
	}
	return resp, nil
}

func (s *Server) ListWorkers(ctx context.Context, _ *chatv1.ListWorkersRequest) (*chatv1.ListWorkersResponse, error) {
	workers, err := s.pgStore.ListAllWorkers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list workers: %v", err)
	}

	// Get heartbeats from Redis for active connection counts.
	workerIDs := make([]string, len(workers))
	for i, w := range workers {
		workerIDs[i] = w.ID
	}
	heartbeats, _ := s.redisStore.GetWorkerHeartbeats(ctx, workerIDs)

	resp := &chatv1.ListWorkersResponse{
		Workers: make([]*chatv1.Worker, len(workers)),
	}
	for i, w := range workers {
		wk := &chatv1.Worker{
			Id:            w.ID,
			Status:        w.Status,
			MaxCapacity:   int64(w.MaxCapacity),
			RegisteredAt:  w.RegisteredAt.Format(time.RFC3339),
			LastHeartbeat: w.LastHeartbeat.Format(time.RFC3339),
		}
		if hb, ok := heartbeats[w.ID]; ok && hb != nil {
			wk.ActiveConnections = int64(hb.Connections)
		}
		resp.Workers[i] = wk
	}
	return resp, nil
}

func (s *Server) GetChannelMessages(ctx context.Context, req *chatv1.GetChannelMessagesRequest) (*chatv1.GetChannelMessagesResponse, error) {
	count := int(req.Count)
	var beforeID string
	if req.BeforeId != nil {
		beforeID = *req.BeforeId
	}

	msgs, err := s.redisStore.ReadStreamMessages(ctx, req.Platform, req.ChannelId, count, beforeID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read stream messages: %v", err)
	}

	resp := &chatv1.GetChannelMessagesResponse{
		Messages: make([]*chatv1.ChatMessage, len(msgs)),
	}
	for i, msg := range msgs {
		resp.Messages[i] = streamMsgToProto(msg)
	}
	return resp, nil
}

func (s *Server) GetFirehoseMessages(ctx context.Context, req *chatv1.GetFirehoseMessagesRequest) (*chatv1.GetFirehoseMessagesResponse, error) {
	count := int(req.Count)
	var beforeID string
	if req.BeforeId != nil {
		beforeID = *req.BeforeId
	}

	msgs, err := s.redisStore.ReadFirehoseMessages(ctx, count, beforeID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read firehose messages: %v", err)
	}

	resp := &chatv1.GetFirehoseMessagesResponse{
		Messages: make([]*chatv1.ChatMessage, len(msgs)),
	}
	for i, msg := range msgs {
		resp.Messages[i] = streamMsgToProto(msg)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// ChatQueryService RPC implementations
// ---------------------------------------------------------------------------

func (s *Server) GetLiveStatuses(ctx context.Context, req *chatv1.GetLiveStatusesRequest) (*chatv1.GetLiveStatusesResponse, error) {
	if len(req.GetChannels()) > 500 {
		return nil, status.Error(codes.InvalidArgument, "too many channels (max 500)")
	}
	// Convert proto ChannelIdentifiers to store query pairs
	pairs := make([]struct {
		Platform  string
		ChannelID string
	}, len(req.GetChannels()))

	for i, ch := range req.GetChannels() {
		if ch == nil || utf8.RuneCountInString(ch.GetPlatform()) > 32 || utf8.RuneCountInString(ch.GetChannelId()) > 256 {
			return nil, status.Error(codes.InvalidArgument, "invalid channel identifier")
		}
		pairs[i] = struct {
			Platform  string
			ChannelID string
		}{
			Platform:  ch.GetPlatform(),
			ChannelID: ch.GetChannelId(),
		}
	}

	channels, err := s.pgStore.GetLiveStatusesByChannelIDs(ctx, pairs)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "get live statuses: %v", err)
	}

	resp := &chatv1.GetLiveStatusesResponse{
		Statuses: make([]*chatv1.LiveStatus, len(channels)),
	}
	for i, ch := range channels {
		ls := &chatv1.LiveStatus{
			Platform:     string(ch.Platform),
			ChannelId:    ch.ChannelID,
			StreamerName: ch.StreamerName,
			ViewerCount:  int64(ch.ViewerCount),
			Tags:         ch.Tags,
		}
		if ch.Title != nil {
			ls.Title = ch.Title
		}
		if ch.Category != nil {
			ls.Category = ch.Category
		}
		if ch.ThumbnailURL != nil {
			ls.ThumbnailUrl = ch.ThumbnailURL
		}
		if ch.BroadcastStartedAt != nil {
			t := ch.BroadcastStartedAt.Format(time.RFC3339)
			ls.BroadcastStartedAt = &t
		}
		resp.Statuses[i] = ls
	}
	return resp, nil
}

// maxBroadcastHistoryRangeDays caps the [from, to) span to mitigate accidental
// or malicious wide scans. Channel-unified-calendar callers fetch at most a
// week per request; 90 days leaves headroom for monthly views without being
// open-ended.
const maxBroadcastHistoryRangeDays = 90

func (s *Server) GetBroadcastHistory(ctx context.Context, req *chatv1.GetBroadcastHistoryRequest) (*chatv1.GetBroadcastHistoryResponse, error) {
	platform := req.GetPlatform()
	channelID := req.GetChannelId()
	if platform == "" || channelID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "platform and channel_id are required")
	}

	limit := int(req.GetLimit())

	// from/to take precedence over days when both are provided. Both must be set
	// together: passing only one is a contract violation (the half-open range is
	// not well-defined). Use the pointer fields directly (not GetFrom/GetTo) so we
	// can distinguish "unset" from "explicit empty string".
	hasFrom := req.From != nil
	hasTo := req.To != nil
	if hasFrom != hasTo {
		return nil, status.Errorf(codes.InvalidArgument,
			"from and to must be provided together (got from=%v, to=%v)", hasFrom, hasTo)
	}

	var (
		sessions []store.BroadcastSession
		total    int
		err      error
	)

	if hasFrom && hasTo {
		fromT, parseErr := time.Parse(time.RFC3339, *req.From)
		if parseErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid from (RFC 3339): %v", parseErr)
		}
		toT, parseErr := time.Parse(time.RFC3339, *req.To)
		if parseErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid to (RFC 3339): %v", parseErr)
		}
		if !toT.After(fromT) {
			return nil, status.Errorf(codes.InvalidArgument, "to must be strictly greater than from")
		}
		if toT.Sub(fromT) > time.Duration(maxBroadcastHistoryRangeDays)*24*time.Hour {
			return nil, status.Errorf(codes.InvalidArgument,
				"range too large: max %d days", maxBroadcastHistoryRangeDays)
		}

		sessions, total, err = s.historyStore.ListChannelBroadcastHistoryByRange(ctx, model.Platform(platform), channelID, fromT, toT, limit)
	} else {
		days := int(req.GetDays())
		sessions, total, err = s.historyStore.ListChannelBroadcastHistory(ctx, model.Platform(platform), channelID, days, limit)
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "list broadcast history: %v", err)
	}

	resp := &chatv1.GetBroadcastHistoryResponse{
		Items: make([]*chatv1.BroadcastHistoryItem, len(sessions)),
		Total: int64(total),
	}
	for i, sess := range sessions {
		item := &chatv1.BroadcastHistoryItem{
			Platform:        string(sess.Platform),
			ChannelId:       sess.ChannelID,
			StreamerName:    sess.StreamerName,
			Title:           sess.Title,
			Category:        sess.Category,
			StartedAt:       sess.StartedAt.Format(time.RFC3339),
			PeakViewerCount: int64(sess.PeakViewerCount),
		}
		if sess.EndedAt != nil {
			t := sess.EndedAt.Format(time.RFC3339)
			item.EndedAt = &t
			item.DurationMinutes = int64(sess.EndedAt.Sub(sess.StartedAt).Minutes())
		}
		resp.Items[i] = item
	}
	return resp, nil
}

func (s *Server) SearchLiveChannels(ctx context.Context, req *chatv1.SearchLiveChannelsRequest) (*chatv1.SearchLiveChannelsResponse, error) {
	// All filters are optional. Empty/zero = no filter on that axis.
	keyword := strings.TrimSpace(req.GetKeyword())
	if utf8.RuneCountInString(keyword) > 50 {
		return nil, status.Errorf(codes.InvalidArgument, "keyword too long (max 50 chars)")
	}

	category := strings.TrimSpace(req.GetCategory())
	if utf8.RuneCountInString(category) > 80 {
		return nil, status.Errorf(codes.InvalidArgument, "category too long (max 80 chars)")
	}

	tag := strings.TrimSpace(req.GetTag())
	if utf8.RuneCountInString(tag) > 80 {
		return nil, status.Errorf(codes.InvalidArgument, "tag too long (max 80 chars)")
	}

	// Normalize multi-select platform values to lowercase and trim other filters.
	platforms := normalizeStrings(req.GetPlatforms(), strings.ToLower)
	categories := normalizeStrings(req.GetCategories(), strings.TrimSpace)
	tagsIn := normalizeStrings(req.GetTagsIn(), strings.TrimSpace)
	if len(platforms) > 10 || len(categories) > 50 || len(tagsIn) > 50 {
		return nil, status.Error(codes.InvalidArgument, "too many filter values")
	}
	for _, c := range categories {
		if utf8.RuneCountInString(c) > 80 {
			return nil, status.Errorf(codes.InvalidArgument, "category too long (max 80 chars)")
		}
	}
	for _, t := range tagsIn {
		if utf8.RuneCountInString(t) > 80 {
			return nil, status.Errorf(codes.InvalidArgument, "tag too long (max 80 chars)")
		}
	}

	// Pick sort_order (new) preferring it over legacy sort.
	sortOrder := strings.TrimSpace(req.GetSortOrder())
	if sortOrder == "" {
		sortOrder = strings.TrimSpace(req.GetSort())
	}
	switch sortOrder {
	case "", "viewer_desc", "viewer_asc", "started_at_desc", "started_at_asc":
		// supported
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported sort_order: %s", sortOrder)
	}

	minViewer := int(req.GetMinViewerCount())
	if minViewer < 0 {
		minViewer = 0
	}
	maxViewer := int(req.GetMaxViewerCount())
	if maxViewer < 0 {
		maxViewer = 0
	}
	startedWithin := int(req.GetStartedWithinSeconds())
	if startedWithin < 0 {
		startedWithin = 0
	}

	limit := int(req.GetLimit())
	if limit <= 0 || limit > 60 {
		limit = 10
	}

	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}

	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	channels, total, err := s.pgStore.SearchLiveChannels(ctx, store.SearchLiveChannelsArgs{
		Keyword:              keyword,
		Platform:             platform,
		Platforms:            platforms,
		Category:             category,
		Categories:           categories,
		Tag:                  tag,
		Tags:                 tagsIn,
		SortOrder:            sortOrder,
		MinViewerCount:       minViewer,
		MaxViewerCount:       maxViewer,
		StartedWithinSeconds: startedWithin,
		Limit:                limit,
		Offset:               offset,
	})
	if err != nil {
		s.logger.Error("SearchLiveChannels store error", "err", err)
		return nil, status.Errorf(codes.Internal, "search live channels failed")
	}

	resp := &chatv1.SearchLiveChannelsResponse{
		Channels: make([]*chatv1.LiveStatus, len(channels)),
		Total:    int64(total),
	}
	for i, ch := range channels {
		ls := &chatv1.LiveStatus{
			Platform:     string(ch.Platform),
			ChannelId:    ch.ChannelID,
			StreamerName: ch.StreamerName,
			ViewerCount:  int64(ch.ViewerCount),
			Tags:         ch.Tags,
		}
		if ch.Title != nil {
			ls.Title = ch.Title
		}
		if ch.Category != nil {
			ls.Category = ch.Category
		}
		if ch.ThumbnailURL != nil {
			ls.ThumbnailUrl = ch.ThumbnailURL
		}
		if ch.BroadcastStartedAt != nil {
			t := ch.BroadcastStartedAt.Format(time.RFC3339)
			ls.BroadcastStartedAt = &t
		}
		resp.Channels[i] = ls
	}
	return resp, nil
}

func (s *Server) GetLiveDiscoveryFacets(ctx context.Context, req *chatv1.GetLiveDiscoveryFacetsRequest) (*chatv1.GetLiveDiscoveryFacetsResponse, error) {
	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	categoryLimit := int(req.GetCategoryLimit())
	if categoryLimit <= 0 {
		categoryLimit = 12
	}
	if categoryLimit > 50 {
		categoryLimit = 50
	}

	tagLimit := int(req.GetTagLimit())
	if tagLimit <= 0 {
		tagLimit = 24
	}
	if tagLimit > 100 {
		tagLimit = 100
	}

	categories, tags, err := s.pgStore.GetLiveDiscoveryFacets(ctx, platform, categoryLimit, tagLimit)
	if err != nil {
		s.logger.Error("GetLiveDiscoveryFacets store error", "err", err, "platform", platform)
		return nil, status.Errorf(codes.Internal, "get live discovery facets failed")
	}

	resp := &chatv1.GetLiveDiscoveryFacetsResponse{
		Categories: make([]*chatv1.FacetEntry, len(categories)),
		Tags:       make([]*chatv1.FacetEntry, len(tags)),
	}
	for i, c := range categories {
		resp.Categories[i] = &chatv1.FacetEntry{Name: c.Name, Count: int64(c.Count)}
	}
	for i, t := range tags {
		resp.Tags[i] = &chatv1.FacetEntry{Name: t.Name, Count: int64(t.Count)}
	}
	return resp, nil
}

// Explore and discovery handlers.

func (s *Server) GetTrendingLiveChannels(ctx context.Context, req *chatv1.GetTrendingLiveChannelsRequest) (*chatv1.GetTrendingLiveChannelsResponse, error) {
	limit := clampInt(int(req.GetLimit()), 1, 60, 12)
	boost := int(req.GetBoostWithinSeconds())
	if boost <= 0 {
		boost = 3600
	}
	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	channels, err := s.pgStore.GetTrendingLiveChannels(ctx, platform, boost, limit)
	if err != nil {
		s.logger.Error("GetTrendingLiveChannels store error", "err", err, "platform", platform)
		return nil, status.Errorf(codes.Internal, "get trending live channels failed")
	}
	return &chatv1.GetTrendingLiveChannelsResponse{Channels: liveChannelsToProto(channels)}, nil
}

func (s *Server) GetJustStartedLiveChannels(ctx context.Context, req *chatv1.GetJustStartedLiveChannelsRequest) (*chatv1.GetJustStartedLiveChannelsResponse, error) {
	withinMinutes := clampInt(int(req.GetWithinMinutes()), 1, 180, 30)
	limit := clampInt(int(req.GetLimit()), 1, 60, 12)
	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	channels, err := s.pgStore.GetJustStartedLiveChannels(ctx, platform, withinMinutes, limit)
	if err != nil {
		s.logger.Error("GetJustStartedLiveChannels store error", "err", err, "platform", platform)
		return nil, status.Errorf(codes.Internal, "get just-started live channels failed")
	}
	return &chatv1.GetJustStartedLiveChannelsResponse{Channels: liveChannelsToProto(channels)}, nil
}

func (s *Server) GetFeaturedLiveChannels(ctx context.Context, req *chatv1.GetFeaturedLiveChannelsRequest) (*chatv1.GetFeaturedLiveChannelsResponse, error) {
	limit := clampInt(int(req.GetLimit()), 1, 60, 24)
	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	channels, err := s.pgStore.GetFeaturedLiveChannels(ctx, platform, limit)
	if err != nil {
		s.logger.Error("GetFeaturedLiveChannels store error", "err", err, "platform", platform)
		return nil, status.Errorf(codes.Internal, "get featured live channels failed")
	}
	return &chatv1.GetFeaturedLiveChannelsResponse{Channels: liveChannelsToProto(channels)}, nil
}

func (s *Server) GetPlatformLiveSummary(ctx context.Context, req *chatv1.GetPlatformLiveSummaryRequest) (*chatv1.GetPlatformLiveSummaryResponse, error) {
	topPer := clampInt(int(req.GetTopPerPlatform()), 1, 20, 6)

	summaries, err := s.pgStore.GetPlatformLiveSummary(ctx, topPer)
	if err != nil {
		s.logger.Error("GetPlatformLiveSummary store error", "err", err)
		return nil, status.Errorf(codes.Internal, "get platform live summary failed")
	}

	resp := &chatv1.GetPlatformLiveSummaryResponse{
		Platforms: make([]*chatv1.PlatformLiveSummary, len(summaries)),
	}
	for i, s := range summaries {
		resp.Platforms[i] = &chatv1.PlatformLiveSummary{
			Platform:    s.Platform,
			ActiveCount: int64(s.ActiveCount),
			TopChannels: liveChannelsToProto(s.TopChannels),
		}
	}
	return resp, nil
}

func (s *Server) GetExploreStats(ctx context.Context, _ *chatv1.GetExploreStatsRequest) (*chatv1.GetExploreStatsResponse, error) {
	stats, err := s.pgStore.GetExploreStats(ctx)
	if err != nil {
		s.logger.Error("GetExploreStats store error", "err", err)
		return nil, status.Errorf(codes.Internal, "get explore stats failed")
	}

	resp := &chatv1.GetExploreStatsResponse{
		ActiveLiveCount:       int64(stats.ActiveLiveCount),
		DistinctCategoryCount: int64(stats.DistinctCategoryCount),
		DistinctTagCount:      int64(stats.DistinctTagCount),
		TotalViewerCount:      int64(stats.TotalViewerCount),
		PlatformCounts:        map[string]int64{},
	}
	for k, v := range stats.PlatformCounts {
		resp.PlatformCounts[k] = int64(v)
	}
	return resp, nil
}

func (s *Server) GetCategoryLiveBundles(ctx context.Context, req *chatv1.GetCategoryLiveBundlesRequest) (*chatv1.GetCategoryLiveBundlesResponse, error) {
	categoryCount := clampInt(int(req.GetCategoryCount()), 1, 20, 6)
	channelsPerCategory := clampInt(int(req.GetChannelsPerCategory()), 1, 20, 6)
	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	bundles, err := s.pgStore.GetCategoryLiveBundles(ctx, platform, categoryCount, channelsPerCategory)
	if err != nil {
		s.logger.Error("GetCategoryLiveBundles store error", "err", err, "platform", platform)
		return nil, status.Errorf(codes.Internal, "get category live bundles failed")
	}

	resp := &chatv1.GetCategoryLiveBundlesResponse{
		Bundles: make([]*chatv1.CategoryLiveBundle, len(bundles)),
	}
	for i, b := range bundles {
		resp.Bundles[i] = &chatv1.CategoryLiveBundle{
			Category:    b.Category,
			ActiveCount: int64(b.ActiveCount),
			Channels:    liveChannelsToProto(b.Channels),
		}
	}
	return resp, nil
}

// ===== helpers =====

func normalizeStrings(in []string, transform func(string) string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, v := range in {
		v = transform(v)
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func clampInt(v, lo, hi, def int) int {
	if v <= 0 {
		return def
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// liveChannelsToProto maps a slice of model.LiveChannel to chatv1.LiveStatus.
// Shared by all explore hub handlers.
func liveChannelsToProto(channels []model.LiveChannel) []*chatv1.LiveStatus {
	out := make([]*chatv1.LiveStatus, len(channels))
	for i, ch := range channels {
		ls := &chatv1.LiveStatus{
			Platform:     string(ch.Platform),
			ChannelId:    ch.ChannelID,
			StreamerName: ch.StreamerName,
			ViewerCount:  int64(ch.ViewerCount),
			Tags:         ch.Tags,
		}
		if ch.Title != nil {
			ls.Title = ch.Title
		}
		if ch.Category != nil {
			ls.Category = ch.Category
		}
		if ch.ThumbnailURL != nil {
			ls.ThumbnailUrl = ch.ThumbnailURL
		}
		if ch.BroadcastStartedAt != nil {
			t := ch.BroadcastStartedAt.Format(time.RFC3339)
			ls.BroadcastStartedAt = &t
		}
		out[i] = ls
	}
	return out
}

func (s *Server) SearchBroadcastChannels(ctx context.Context, req *chatv1.SearchBroadcastChannelsRequest) (*chatv1.SearchBroadcastChannelsResponse, error) {
	keyword := strings.TrimSpace(req.GetKeyword())
	if keyword == "" {
		return nil, status.Errorf(codes.InvalidArgument, "keyword must not be empty")
	}
	if utf8.RuneCountInString(keyword) > 50 {
		return nil, status.Errorf(codes.InvalidArgument, "keyword too long (max 50 chars)")
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}
	// 200 = broadcast_metadata_history partition 1-2개 스캔으로 안전하게 처리되는
	// 상한. 그 이상은 페이지네이션(offset) 사용.
	if limit > 200 {
		limit = 200
	}

	offset := int(req.GetOffset())
	if offset < 0 {
		offset = 0
	}

	// broadcast_metadata_history retention 은 180 일. 그 이상은 조회해도 데이터 없음.
	lookbackDays := int(req.GetLookbackDays())
	if lookbackDays <= 0 {
		lookbackDays = 30
	}
	if lookbackDays > 180 {
		lookbackDays = 180
	}

	platform := ""
	if req.Platform != nil {
		platform = strings.ToLower(req.GetPlatform())
	}

	channels, total, err := s.pgStore.SearchBroadcastChannels(ctx, keyword, platform, lookbackDays, limit, offset)
	if err != nil {
		s.logger.Error("SearchBroadcastChannels store error", "err", err, "keyword", keyword, "platform", platform, "lookback_days", lookbackDays)
		return nil, status.Errorf(codes.Internal, "search broadcast channels failed")
	}

	resp := &chatv1.SearchBroadcastChannelsResponse{
		Channels: make([]*chatv1.BroadcastChannel, len(channels)),
		Total:    int64(total),
	}
	for i, ch := range channels {
		bc := &chatv1.BroadcastChannel{
			Platform:        string(ch.Platform),
			ChannelId:       ch.ChannelID,
			StreamerName:    ch.StreamerName,
			Title:           ch.Title,
			Tags:            ch.Tags,
			PeakViewerCount: int64(ch.PeakViewerCount),
			IsLive:          ch.IsLive,
		}
		if ch.Category != nil {
			bc.Category = ch.Category
		}
		if ch.ThumbnailURL != nil {
			bc.ThumbnailUrl = ch.ThumbnailURL
		}
		if ch.LastStartedAt != nil {
			t := ch.LastStartedAt.Format(time.RFC3339)
			bc.LastStartedAt = &t
		}
		if ch.LastEndedAt != nil {
			t := ch.LastEndedAt.Format(time.RFC3339)
			bc.LastEndedAt = &t
		}
		if ch.IsLive {
			cv := int64(ch.CurrentViewerCount)
			bc.CurrentViewerCount = &cv
		}
		resp.Channels[i] = bc
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func streamMsgToProto(msg store.StreamMessage) *chatv1.ChatMessage {
	v := msg.Values
	cm := &chatv1.ChatMessage{
		StreamId:     msg.StreamID,
		Id:           strval(v, "id"),
		Type:         strval(v, "type"),
		Platform:     strval(v, "platform"),
		ChannelId:    strval(v, "channelId"),
		StreamerName: strval(v, "streamerName"),
		UserId:       strval(v, "userId"),
		Nickname:     strval(v, "nickname"),
		Message:      strval(v, "message"),
		Timestamp:    strval(v, "timestamp"),
		Currency:     strval(v, "currency"),
	}
	if amt, ok := v["amount"]; ok {
		if s, ok := amt.(string); ok {
			f, _ := strconv.ParseFloat(s, 64)
			cm.Amount = f
		}
	}
	if amt, ok := v["amountKRW"]; ok {
		if s, ok := amt.(string); ok {
			n, _ := strconv.ParseInt(s, 10, 64)
			cm.AmountKrw = n
		}
	}
	return cm
}

func strval(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// ChatAdminService Force RPCs
// ---------------------------------------------------------------------------

// ForceReconnectChannel publishes a WorkerCommandReconnect to the worker
// currently assigned to the channel. The worker tears down the existing chat
// connection and reconnects with a fresh chatChannelId/access-token. Use to
// recover silently stuck channels (e.g., chzzk session rotated mid-broadcast).
func (s *Server) ForceReconnectChannel(ctx context.Context, req *chatv1.ForceReconnectChannelRequest) (*chatv1.ForceReconnectChannelResponse, error) {
	platform := strings.TrimSpace(req.GetPlatform())
	channelID := strings.TrimSpace(req.GetChannelId())
	if platform == "" || channelID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "platform and channel_id are required")
	}

	ch, err := s.pgStore.GetChannel(ctx, model.Platform(platform), channelID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logger.Info("force-reconnect: channel not in live_channels", "platform", platform, "channel", channelID)
			return &chatv1.ForceReconnectChannelResponse{Dispatched: false}, nil
		}
		return nil, status.Errorf(codes.Unavailable, "get channel: %v", err)
	}
	if ch == nil || ch.WorkerID == nil || *ch.WorkerID == "" {
		s.logger.Info("force-reconnect: no assigned worker", "platform", platform, "channel", channelID)
		return &chatv1.ForceReconnectChannelResponse{Dispatched: false}, nil
	}

	cmd := store.WorkerCommand{
		Type:      store.WorkerCommandReconnect,
		Platform:  ch.Platform,
		ChannelID: channelID,
	}
	if err := s.redisStore.PublishWorkerCommand(ctx, *ch.WorkerID, cmd); err != nil {
		return nil, status.Errorf(codes.Unavailable, "publish reconnect: %v", err)
	}

	s.logger.Info("force-reconnect: dispatched",
		"platform", platform, "channel", channelID, "worker", *ch.WorkerID)
	wid := *ch.WorkerID
	return &chatv1.ForceReconnectChannelResponse{Dispatched: true, WorkerId: &wid}, nil
}

// ForceRediscoverChannel triggers the discover service to re-fetch
// platform-native live-detail for a single channel and update its DB state.
// Use when discover wrongly marked the channel offline.
func (s *Server) ForceRediscoverChannel(ctx context.Context, req *chatv1.ForceRediscoverChannelRequest) (*chatv1.ForceRediscoverChannelResponse, error) {
	platform := strings.TrimSpace(req.GetPlatform())
	channelID := strings.TrimSpace(req.GetChannelId())
	if platform == "" || channelID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "platform and channel_id are required")
	}
	if s.RediscoverClient == nil {
		return nil, status.Errorf(codes.Unimplemented, "discover client not configured")
	}

	res, err := s.RediscoverClient.ForceRediscover(ctx, platform, channelID)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "force rediscover: %v", err)
	}
	resp := &chatv1.ForceRediscoverChannelResponse{Dispatched: true}
	if res.Status != "" {
		st := res.Status
		resp.Status = &st
	}
	s.logger.Info("force-rediscover: dispatched",
		"platform", platform, "channel", channelID, "status", res.Status)
	return resp, nil
}

// RecoverChannel runs ForceRediscoverChannel followed by ForceReconnectChannel.
// A failure in rediscover is reported but does not abort the reconnect step —
// most stuck-chat cases (chzzk chatChannelId rotation) need only the reconnect,
// and rediscover can fail e.g. when discover is briefly unavailable. The
// reconnect step is still useful in that case.
func (s *Server) RecoverChannel(ctx context.Context, req *chatv1.RecoverChannelRequest) (*chatv1.RecoverChannelResponse, error) {
	platform := strings.TrimSpace(req.GetPlatform())
	channelID := strings.TrimSpace(req.GetChannelId())
	if platform == "" || channelID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "platform and channel_id are required")
	}

	resp := &chatv1.RecoverChannelResponse{}

	if s.RediscoverClient != nil {
		if res, err := s.RediscoverClient.ForceRediscover(ctx, platform, channelID); err != nil {
			s.logger.Warn("recover: rediscover step failed (continuing)",
				"platform", platform, "channel", channelID, "error", err)
		} else {
			resp.RediscoverDispatched = true
			if res.Status != "" {
				st := res.Status
				resp.Status = &st
			}
		}
	}

	ch, err := s.pgStore.GetChannel(ctx, model.Platform(platform), channelID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.Unavailable, "get channel: %v", err)
	}
	if ch != nil && ch.WorkerID != nil && *ch.WorkerID != "" {
		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandReconnect,
			Platform:  ch.Platform,
			ChannelID: channelID,
		}
		if err := s.redisStore.PublishWorkerCommand(ctx, *ch.WorkerID, cmd); err != nil {
			return nil, status.Errorf(codes.Unavailable, "publish reconnect: %v", err)
		}
		resp.ReconnectDispatched = true
		wid := *ch.WorkerID
		resp.WorkerId = &wid
	}

	s.logger.Info("recover: completed",
		"platform", platform, "channel", channelID,
		"rediscover", resp.RediscoverDispatched, "reconnect", resp.ReconnectDispatched)
	return resp, nil
}

// ---------------------------------------------------------------------------
// Redis-backed rediscover client
// ---------------------------------------------------------------------------

// RedisRediscoverClient publishes a DiscoverCommand to the shared Redis pub/sub
// channel that all discover instances subscribe to. The discover leader picks
// it up and triggers a fresh discovery cycle. Returns immediately after publish
// — the Status field is left empty since discover acts asynchronously.
type RedisRediscoverClient struct {
	Store  *store.RedisStore
	Logger *slog.Logger
}

func (c *RedisRediscoverClient) ForceRediscover(ctx context.Context, platform, channelID string) (RediscoverResult, error) {
	if c.Store == nil {
		return RediscoverResult{}, fmt.Errorf("redis store not configured")
	}
	cmd := store.DiscoverCommand{
		Type:      store.DiscoverCommandRediscover,
		Platform:  platform,
		ChannelID: channelID,
	}
	if err := c.Store.PublishDiscoverCommand(ctx, cmd); err != nil {
		return RediscoverResult{}, fmt.Errorf("publish discover command: %w", err)
	}
	return RediscoverResult{}, nil
}
