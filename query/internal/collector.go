package internal

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"strings"
	"time"

	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProductAccess is deliberately one configured product and channel. Certificate
// URIs authorize scopes; request fields never establish caller identity.
type ProductAccess struct {
	Consumer, Channel, ReadURI, ManageURI, RecoveryURI string
	BroadcastCheck                                     func(context.Context, string) (*pb.BroadcastStatus, error)
}
type collectorService struct {
	pb.UnimplementedCollectorServiceServer
	pg     *store.PgStore
	redis  *store.RedisStore
	access ProductAccess
}

func NewProductServer(pg *store.PgStore, redis *store.RedisStore, port int, tlsConfig *tls.Config, access ProductAccess, logger *slog.Logger) (*Server, error) {
	if tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.MinVersion < tls.VersionTLS13 || access.Consumer == "" || access.ReadURI == "" {
		return nil, errors.New("collector mutual TLS and consumer identity required")
	}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.MaxRecvMsgSize(2<<20), grpc.MaxHeaderListSize(64<<10))
	pb.RegisterCollectorServiceServer(gs, &collectorService{pg: pg, redis: redis, access: access})
	return &Server{pgStore: pg, redisStore: redis, grpcServer: gs, port: port, logger: logger}, nil
}
func (s *collectorService) authorize(ctx context.Context, consumer, channel, scope string) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "client certificate required")
	}
	auth, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(auth.State.VerifiedChains) == 0 || len(auth.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "verified client certificate required")
	}
	required := s.access.ReadURI
	if scope == "subscription:manage" {
		required = s.access.ManageURI
	}
	if scope == "recovery:manage" {
		required = s.access.RecoveryURI
	}
	allowed := false
	for _, uri := range auth.State.PeerCertificates[0].URIs {
		if required != "" && uri.String() == required {
			allowed = true
		}
	}
	if !allowed || consumer != s.access.Consumer || channel == "" || channel != s.access.Channel {
		detail := &pb.ContractErrorDetail{Code: pb.ContractErrorCode_CONTRACT_ERROR_CODE_SCOPE_DENIED, ConsumerId: consumer, ChannelId: channel, RequiredScope: scope}
		st, _ := status.New(codes.PermissionDenied, "collector scope denied").WithDetails(detail)
		return st.Err()
	}
	return nil
}
func cursor(g string, n uint64) *pb.Cursor { return &pb.Cursor{JournalGeneration: g, ChannelOffset: n} }
func (s *collectorService) failure(ctx context.Context, err error, consumer, channel string, requested *pb.Cursor, revision uint64) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	code := codes.Unavailable
	message := "collector storage unavailable"
	detail := &pb.ContractErrorDetail{ConsumerId: consumer, ChannelId: channel, RequestedCursor: requested, ActualRecoveryRevision: revision}
	switch {
	case errors.Is(err, store.ErrGeneration):
		code = codes.FailedPrecondition
		message = "journal generation changed"
		detail.Code = pb.ContractErrorCode_CONTRACT_ERROR_CODE_GENERATION_MISMATCH
	case errors.Is(err, store.ErrCursorExpired):
		code = codes.FailedPrecondition
		message = "donation cursor expired"
		detail.Code = pb.ContractErrorCode_CONTRACT_ERROR_CODE_CURSOR_EXPIRED
	case errors.Is(err, store.ErrRevision):
		code = codes.FailedPrecondition
		message = "recovery decision changed"
		detail.Code = pb.ContractErrorCode_CONTRACT_ERROR_CODE_RECOVERY_REVISION_MISMATCH
	case errors.Is(err, store.ErrCursor):
		return status.Error(codes.InvalidArgument, "explicit contiguous cursor required")
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.AlreadyExists, "idempotency key conflicts with prior request")
	case errors.Is(err, store.ErrCollectionDisabled):
		return status.Error(codes.FailedPrecondition, "collection is disabled")
	}
	if code == codes.FailedPrecondition {
		if c, e := s.pg.CollectionState(ctx, channel); e == nil {
			detail.EarliestCursor = cursor(c.Generation, c.ExpiredThrough)
			detail.CurrentCursor = cursor(c.Generation, c.Current)
		}
		if p, e := s.pg.ConsumerProgress(ctx, consumer, channel); e == nil {
			detail.ExpectedRecoveryRevision = p.Revision
		}
		st, _ := status.New(code, message).WithDetails(detail)
		return st.Err()
	}
	return status.Error(code, message)
}
func donationEvent(d model.Donation) *pb.DonationEvent {
	identity := pb.IdentityStatus_IDENTITY_STATUS_OBSERVATION_ONLY
	if d.Identity == "reconnect_ambiguous" {
		identity = pb.IdentityStatus_IDENTITY_STATUS_RECONNECT_AMBIGUOUS
	}
	return &pb.DonationEvent{EventId: d.EventID, ChannelId: d.ChannelID, NativeBalloonCount: d.Count, DonationKind: d.Kind, SourceEventId: d.SourceID, BroadcastId: d.BroadcastID, DonorId: d.DonorID, ConnectionEpoch: d.ConnectionEpoch, ConnectionSequence: d.Sequence, Cursor: cursor(d.Generation, d.Offset), IdentityStatus: identity, QualityReasons: d.Reasons, RelatedObservationIds: d.Related, SchemaVersion: "v1", EventType: "donation", Platform: pb.Platform_PLATFORM_SOOP, PlatformChannelId: d.ChannelID, ObservedAt: timestamppb.New(d.ObservedAt), NativeCurrency: pb.NativeCurrency_NATIVE_CURRENCY_SOOP_BALLOON, DonorDisplayName: d.DisplayName, Message: d.Message}
}
func (s *collectorService) getStatus(ctx context.Context, consumer, channel string) (*pb.CollectionStatus, error) {
	c, err := s.pg.CollectionState(ctx, channel)
	if err != nil {
		return nil, s.failure(ctx, err, consumer, channel, nil, 0)
	}
	enabled, err := s.pg.CollectionEnabled(ctx, channel)
	if err != nil {
		return nil, s.failure(ctx, err, consumer, channel, nil, 0)
	}
	runtime := c.Runtime
	if (runtime == "connected" || runtime == "connecting") && time.Since(c.StateAt) > 35*time.Second {
		runtime = "reconnecting"
	}
	if !enabled {
		runtime = "disabled"
	}
	result := &pb.CollectionStatus{ChannelId: channel, Configured: true, CollectionActive: enabled && runtime == "connected", ProcessHealth: runtime, EarliestCursor: cursor(c.Generation, c.ExpiredThrough), CurrentCursor: cursor(c.Generation, c.Current)}
	if runtime != "connected" && runtime != "waiting" {
		result.QualityReasons = []string{runtime}
	}
	if c.LastReceived != nil {
		result.LastReceivedAt = timestamppb.New(*c.LastReceived)
	}
	p, err := s.pg.ConsumerProgress(ctx, consumer, channel)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, s.failure(ctx, err, consumer, channel, nil, 0)
	}
	result.RecoveryRevision = p.Revision
	return result, nil
}
func (s *collectorService) GetCollectionStatus(ctx context.Context, r *pb.GetCollectionStatusRequest) (*pb.CollectionStatus, error) {
	if err := s.authorize(ctx, r.ConsumerId, r.ChannelId, "events:read"); err != nil {
		return nil, err
	}
	return s.getStatus(ctx, r.ConsumerId, r.ChannelId)
}
func (s *collectorService) SetChannelSubscription(ctx context.Context, r *pb.SetChannelSubscriptionRequest) (*pb.SetChannelSubscriptionResponse, error) {
	if err := s.authorize(ctx, r.ConsumerId, r.ChannelId, "subscription:manage"); err != nil {
		return nil, err
	}
	_, err := s.pg.SetSubscription(ctx, r.ConsumerId, r.ChannelId, r.IdempotencyKey, r.Subscribed)
	if err != nil {
		return nil, s.failure(ctx, err, r.ConsumerId, r.ChannelId, nil, 0)
	}
	current, err := s.getStatus(ctx, r.ConsumerId, r.ChannelId)
	if err != nil {
		return nil, err
	}
	return &pb.SetChannelSubscriptionResponse{Status: current}, nil
}
func (s *collectorService) ListDonations(ctx context.Context, r *pb.ListDonationsRequest) (*pb.ListDonationsResponse, error) {
	if err := s.authorize(ctx, r.ConsumerId, r.ChannelId, "events:read"); err != nil {
		return nil, err
	}
	if r.AfterCursor == nil || r.AfterCursor.JournalGeneration == "" || r.Limit > 500 {
		return nil, status.Error(codes.InvalidArgument, "explicit cursor and limit at most 500 required")
	}
	ds, c, err := s.pg.ReadDonations(ctx, r.ConsumerId, r.ChannelId, r.AfterCursor.JournalGeneration, r.AfterCursor.ChannelOffset, r.RecoveryRevision, int(r.Limit))
	if err != nil {
		return nil, s.failure(ctx, err, r.ConsumerId, r.ChannelId, r.AfterCursor, r.RecoveryRevision)
	}
	result := &pb.ListDonationsResponse{EarliestCursor: cursor(c.Generation, c.ExpiredThrough), CurrentCursor: cursor(c.Generation, c.Current), RecoveryRevision: r.RecoveryRevision}
	for _, d := range ds {
		result.Donations = append(result.Donations, donationEvent(d))
	}
	return result, nil
}
func (s *collectorService) WatchDonations(r *pb.WatchDonationsRequest, stream grpc.ServerStreamingServer[pb.DonationEvent]) error {
	ctx := stream.Context()
	req := &pb.ListDonationsRequest{ConsumerId: r.ConsumerId, ChannelId: r.ChannelId, AfterCursor: r.AfterCursor, RecoveryRevision: r.RecoveryRevision, Limit: 100}
	for {
		result, err := s.ListDonations(ctx, req)
		if err != nil {
			return err
		}
		for _, d := range result.Donations {
			if err = stream.Send(d); err != nil {
				return err
			}
			req.AfterCursor = d.Cursor
		}
		if len(result.Donations) == 100 {
			continue
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
func (s *collectorService) AckDonations(ctx context.Context, r *pb.AckDonationsRequest) (*pb.AckDonationsResponse, error) {
	if err := s.authorize(ctx, r.ConsumerId, r.ChannelId, "events:read"); err != nil {
		return nil, err
	}
	if r.Cursor == nil || r.Cursor.JournalGeneration == "" {
		return nil, status.Error(codes.InvalidArgument, "explicit cursor required")
	}
	err := s.pg.AckDonations(ctx, r.ConsumerId, r.ChannelId, r.Cursor.JournalGeneration, r.Cursor.ChannelOffset, r.RecoveryRevision)
	if err != nil {
		return nil, s.failure(ctx, err, r.ConsumerId, r.ChannelId, r.Cursor, r.RecoveryRevision)
	}
	return &pb.AckDonationsResponse{AcceptedCursor: r.Cursor, RecoveryRevision: r.RecoveryRevision}, nil
}
func (s *collectorService) ResolveConsumerRecovery(ctx context.Context, r *pb.ResolveConsumerRecoveryRequest) (*pb.ResolveConsumerRecoveryResponse, error) {
	if err := s.authorize(ctx, r.ConsumerId, r.ChannelId, "recovery:manage"); err != nil {
		return nil, err
	}
	if r.ResumeFrom == nil || r.PreviousGeneration == "" || r.NewGeneration == "" || r.ResumeFrom.JournalGeneration != r.NewGeneration || strings.TrimSpace(r.Reason) == "" || strings.TrimSpace(r.OperatorId) == "" || r.IdempotencyKey == "" || len(r.IdempotencyKey) > 200 || len(r.Reason) > 2000 || len(r.OperatorId) > 200 || len(r.UnrecoveredRanges) > 100 {
		return nil, status.Error(codes.InvalidArgument, "complete recovery decision required")
	}
	for _, span := range r.UnrecoveredRanges {
		if span.Start == nil || span.End == nil || span.Start.JournalGeneration != r.PreviousGeneration || span.End.JournalGeneration != r.PreviousGeneration || span.Start.ChannelOffset > span.End.ChannelOffset {
			return nil, status.Error(codes.InvalidArgument, "invalid unrecovered range")
		}
	}
	body, err := protojson.Marshal(r)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid recovery request")
	}
	result, err := s.pg.ResolveRecovery(ctx, r.ConsumerId, r.ChannelId, r.IdempotencyKey, r.PreviousGeneration, r.NewGeneration, r.ResumeFrom.ChannelOffset, r.ExpectedRevision, body)
	if err != nil {
		return nil, s.failure(ctx, err, r.ConsumerId, r.ChannelId, r.ResumeFrom, r.ExpectedRevision)
	}
	return &pb.ResolveConsumerRecoveryResponse{PreviousGeneration: result.PreviousGeneration, NewGeneration: result.NewGeneration, PreviousCursor: cursor(result.PreviousGeneration, result.PreviousOffset), ResumeFrom: cursor(result.NewGeneration, result.ResumeFrom), RecoveryRevision: result.Revision, UnrecoveredRanges: r.UnrecoveredRanges, IdempotencyKey: r.IdempotencyKey}, nil
}
