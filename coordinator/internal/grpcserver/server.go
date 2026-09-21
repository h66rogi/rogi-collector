package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	chatv1 "github.com/h66rogi/rogi-collector/proto/gen/go/meloming/chat/v1"
	commonv1 "github.com/h66rogi/rogi-collector/proto/gen/go/meloming/common/v1"
	"github.com/h66rogi/rogi-collector/shared/grpcauth"
	"github.com/h66rogi/rogi-collector/shared/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps a gRPC server that implements ChatAdminService.
type Server struct {
	chatv1.UnimplementedChatAdminServiceServer

	pgStore    *store.PgStore
	redisStore *store.RedisStore
	grpcServer *grpc.Server
	port       int
	logger     *slog.Logger
}

// New creates a new gRPC admin server.
func New(pgStore *store.PgStore, redisStore *store.RedisStore, port int, apiKey string, enableReflection bool, logger *slog.Logger) *Server {
	s := &Server{
		pgStore:    pgStore,
		redisStore: redisStore,
		port:       port,
		logger:     logger.With("component", "grpc-server"),
	}

	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpcauth.UnaryServerInterceptor(apiKey)),
		grpc.ChainStreamInterceptor(grpcauth.StreamServerInterceptor(apiKey)),
		grpc.MaxRecvMsgSize(2<<20),
		grpc.MaxHeaderListSize(64<<10),
	)
	chatv1.RegisterChatAdminServiceServer(gs, s)
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
	s.grpcServer.GracefulStop()
}

// ---------------------------------------------------------------------------
// RPC implementations
// ---------------------------------------------------------------------------

func (s *Server) GetPlatformStats(ctx context.Context, _ *chatv1.GetPlatformStatsRequest) (*chatv1.GetPlatformStatsResponse, error) {
	stats, err := s.pgStore.GetPlatformStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("get platform stats: %w", err)
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
		platform, status, sort string
		page, size             int
	)

	if req.Platform != nil {
		platform = *req.Platform
	}
	if req.Status != nil {
		status = *req.Status
	}
	if req.Sort != nil {
		sort = *req.Sort
	}
	if req.Pagination != nil {
		page = int(req.Pagination.Page)
		size = int(req.Pagination.Size)
	}

	channels, total, err := s.pgStore.ListChannelsFiltered(ctx, platform, status, sort, page, size)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
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
		return nil, fmt.Errorf("list workers: %w", err)
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
		return nil, fmt.Errorf("read stream messages: %w", err)
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
		return nil, fmt.Errorf("read firehose messages: %w", err)
	}

	resp := &chatv1.GetFirehoseMessagesResponse{
		Messages: make([]*chatv1.ChatMessage, len(msgs)),
	}
	for i, msg := range msgs {
		resp.Messages[i] = streamMsgToProto(msg)
	}
	return resp, nil
}

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
