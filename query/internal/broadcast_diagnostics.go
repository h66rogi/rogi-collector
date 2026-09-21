package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *collectorService) CheckBroadcast(ctx context.Context, req *pb.CheckBroadcastRequest) (*pb.BroadcastStatus, error) {
	if err := s.authorize(ctx, req.ConsumerId, req.ChannelId, "collection:read"); err != nil {
		return nil, err
	}
	if !soopauth.ValidChannelID(req.TargetChannelId) {
		return nil, status.Error(codes.InvalidArgument, "invalid broadcast channel ID")
	}
	if s.access.BroadcastCheck == nil {
		return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
	}
	return s.access.BroadcastCheck(ctx, req.TargetChannelId)
}

// DiscoverBroadcastChecker has a fixed internal destination. SOOP cookies stay
// mounted only in discover/worker; query relays sanitized broadcast facts.
func DiscoverBroadcastChecker(token string) func(context.Context, string) (*pb.BroadcastStatus, error) {
	if len(token) < 32 {
		return nil
	}
	client := &http.Client{Timeout: 9 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context, channel string) (*pb.BroadcastStatus, error) {
		body, _ := json.Marshal(map[string]string{"channelId": channel})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://discover:8080/diagnostics/broadcast", bytes.NewReader(body))
		if err != nil {
			return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
		}
		defer response.Body.Close()
		if response.StatusCode == http.StatusTooManyRequests {
			return nil, status.Error(codes.ResourceExhausted, "broadcast checks are rate limited")
		}
		if response.StatusCode != http.StatusOK {
			return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
		}
		var info soopauth.BroadcastInfo
		decoder := json.NewDecoder(io.LimitReader(response.Body, 8192))
		if decoder.Decode(&info) != nil || info.ChannelID != channel || info.CheckedAt.IsZero() {
			return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
		}
		switch info.State {
		case "live", "offline", "cookie_required", "auth_required", "lookup_failed":
		default:
			return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
		}
		checked := timestamppb.New(info.CheckedAt)
		if checked.CheckValid() != nil {
			return nil, status.Error(codes.Unavailable, "broadcast diagnostics unavailable")
		}
		return &pb.BroadcastStatus{ChannelId: info.ChannelID, State: info.State, Title: info.Title, DisplayName: info.DisplayName, BroadcastId: info.BroadcastID, CheckedAt: checked, Cached: info.Cached}, nil
	}
}
