package internal

import (
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (s *collectorService) WatchChat(r *pb.WatchChatRequest, stream grpc.ServerStreamingServer[pb.ChatEvent]) error {
	ctx := stream.Context()
	if err := s.authorize(ctx, r.ConsumerId, r.ChannelId, "events:read"); err != nil {
		return err
	}
	after, generation := "0-0", ""
	gap := false
	if r.AfterCursor != nil {
		after = r.AfterCursor.StreamId
		generation = r.AfterCursor.StreamGeneration
		if !store.ValidStreamID(after) || generation == "" {
			return status.Error(codes.InvalidArgument, "invalid chat cursor")
		}
	}
	for {
		batch, err := s.redis.ReadProductChat(ctx, r.ChannelId, after)
		if err != nil {
			return status.Error(codes.Unavailable, "chat buffer unavailable")
		}
		if batch.Generation == "" && len(batch.Messages) > 0 {
			return status.Error(codes.FailedPrecondition, "chat generation unavailable; explicit resubscription required")
		}
		if generation != "" && generation != batch.Generation {
			return status.Error(codes.FailedPrecondition, "chat stream reset; explicit resubscription required")
		}
		if generation == "" {
			generation = batch.Generation
		}
		if after != "0-0" && batch.Earliest != "" && store.StreamIDBefore(after, batch.Earliest) {
			gap = true
		}
		if batch.Latest != "" && store.StreamIDBefore(batch.Latest, after) {
			return status.Error(codes.InvalidArgument, "chat cursor beyond stream")
		}
		for _, msg := range batch.Messages {
			after = msg.StreamID
			value := func(k string) string { v, _ := msg.Values[k].(string); return v }
			if value("type") != "chat" || value("userId") == "" {
				continue
			}
			at, err := time.Parse(time.RFC3339Nano, value("timestamp"))
			if err != nil {
				gap = true
				continue
			}
			event := &pb.ChatEvent{EventId: value("id"), ChannelId: r.ChannelId, UserId: value("userId"), Message: value("message"), EmotesJson: value("emotes"), Cursor: &pb.ChatCursor{StreamGeneration: generation, StreamId: msg.StreamID, GapBefore: gap}, ObservedAt: timestamppb.New(at), SchemaVersion: "v1", Platform: pb.Platform_PLATFORM_SOOP, PlatformChannelId: r.ChannelId, UserDisplayName: value("nickname")}
			if err := stream.Send(event); err != nil {
				return err
			}
			gap = false
		}
		if len(batch.Messages) == 100 {
			continue
		}
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
