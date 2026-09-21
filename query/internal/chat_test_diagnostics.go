package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/diagnostics"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *collectorService) StartChatTest(ctx context.Context, req *pb.ChatTestRequest) (*pb.ChatTestStatus, error) {
	return s.chatTest(ctx, "start", req)
}
func (s *collectorService) GetChatTest(ctx context.Context, req *pb.ChatTestRequest) (*pb.ChatTestStatus, error) {
	return s.chatTest(ctx, "status", req)
}
func (s *collectorService) StopChatTest(ctx context.Context, req *pb.ChatTestRequest) (*pb.ChatTestStatus, error) {
	return s.chatTest(ctx, "stop", req)
}
func (s *collectorService) chatTest(ctx context.Context, action string, req *pb.ChatTestRequest) (*pb.ChatTestStatus, error) {
	if err := s.authorize(ctx, req.ConsumerId, req.ChannelId, "collection:read"); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(req.SessionId)
	if (req.SessionId != "" && (err != nil || id.String() != req.SessionId)) || (action != "status" && req.SessionId == "") || (action == "start" && !soopauth.ValidChannelID(req.TargetChannelId)) {
		return nil, status.Error(codes.InvalidArgument, "invalid chat test request")
	}
	if s.access.ChatTest == nil {
		return nil, status.Error(codes.Unavailable, "chat diagnostics unavailable")
	}
	return s.access.ChatTest(ctx, action, req.TargetChannelId, req.SessionId)
}
func WorkerChatTester(token string) func(context.Context, string, string, string) (*pb.ChatTestStatus, error) {
	if len(token) < 32 {
		return nil
	}
	client := &http.Client{Timeout: 9 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context, action, target, id string) (*pb.ChatTestStatus, error) {
		unavailable := func() (*pb.ChatTestStatus, error) {
			return nil, status.Error(codes.Unavailable, "chat diagnostics unavailable")
		}
		body, _ := json.Marshal(map[string]string{"action": action, "targetChannelId": target, "sessionId": id})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://worker:8080/diagnostics/chat-test", bytes.NewReader(body))
		if err != nil {
			return unavailable()
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return unavailable()
		}
		defer response.Body.Close()
		switch response.StatusCode {
		case 200:
		case 400:
			return nil, status.Error(codes.InvalidArgument, "invalid chat test request")
		case 403:
			return nil, status.Error(codes.PermissionDenied, "channel opted out of collection")
		case 404:
			return nil, status.Error(codes.NotFound, "chat test expired or replaced")
		case 409:
			return nil, status.Error(codes.AlreadyExists, "another chat test is active")
		case 429:
			return nil, status.Error(codes.ResourceExhausted, "chat tests are rate limited")
		default:
			return unavailable()
		}
		var info diagnostics.ChatTestStatus
		dec := json.NewDecoder(io.LimitReader(response.Body, 256<<10))
		if dec.Decode(&info) != nil || dec.Decode(new(any)) != io.EOF || len(info.Messages) > 20 {
			return unavailable()
		}
		switch info.State {
		case "idle", "connecting", "joined", "receiving", "stopping", "stopped", "expired", "disconnected", "offline", "cookie_required", "auth_required", "failed", "blocked":
		default:
			return unavailable()
		}
		if id != "" && info.SessionID != id || action == "start" && info.ChannelID != target {
			return unavailable()
		}
		timestamp := func(t *time.Time) *timestamppb.Timestamp {
			if t == nil {
				return nil
			}
			return timestamppb.New(*t)
		}
		result := &pb.ChatTestStatus{SessionId: info.SessionID, ChannelId: info.ChannelID, State: info.State, Active: info.Active, StartedAt: timestamp(info.StartedAt), ExpiresAt: timestamp(info.ExpiresAt), JoinedAt: timestamp(info.JoinedAt), LastReceivedAt: timestamp(info.LastReceivedAt), ReceivedCount: info.ReceivedCount}
		for _, msg := range info.Messages {
			if len([]rune(msg.Message)) > 1000 || len([]rune(msg.DisplayName)) > 80 {
				return unavailable()
			}
			result.Messages = append(result.Messages, &pb.ChatTestSample{Sequence: msg.Sequence, DisplayName: msg.DisplayName, Message: msg.Message, ReceivedAt: timestamppb.New(msg.ReceivedAt)})
		}
		return result, nil
	}
}
