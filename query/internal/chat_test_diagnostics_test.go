package internal

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestChatTestMTLSKeepsProductionScope(t *testing.T) {
	tlsServer, clientTLS := testCertificates(t)
	var calls atomic.Int32
	access := ProductAccess{Consumer: "fixture-consumer", Channel: "production-channel", ReadURI: "spiffe://fixture/reader", ChatTest: func(_ context.Context, action, target, id string) (*pb.ChatTestStatus, error) {
		calls.Add(1)
		return &pb.ChatTestStatus{SessionId: id, ChannelId: target, State: action}, nil
	}}
	srv, err := NewProductServer(nil, nil, 0, tlsServer, access, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.grpcServer.Serve(listener)
	t.Cleanup(srv.grpcServer.Stop)
	client := func(uri string) pb.CollectorServiceClient {
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS(uri))))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return pb.NewCollectorServiceClient(conn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader := client("spiffe://fixture/reader")
	request := &pb.ChatTestRequest{ConsumerId: "fixture-consumer", ChannelId: "production-channel", TargetChannelId: "test-channel", SessionId: uuid.NewString()}
	for _, call := range []func(context.Context, *pb.ChatTestRequest, ...grpc.CallOption) (*pb.ChatTestStatus, error){reader.StartChatTest, reader.GetChatTest, reader.StopChatTest} {
		got, err := call(ctx, request)
		if err != nil || got.ChannelId != "test-channel" {
			t.Fatal("test RPC failed", err)
		}
		bad := &pb.ChatTestRequest{ConsumerId: request.ConsumerId, ChannelId: "test-channel", SessionId: request.SessionId, TargetChannelId: request.TargetChannelId}
		if _, err = call(ctx, bad); status.Code(err) != codes.PermissionDenied {
			t.Fatal("test target replaced authorized scope", err)
		}
	}
	if _, err = client("spiffe://fixture/unknown").StartChatTest(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("unknown identity started chat test")
	}
	if _, err = reader.StartChatTest(ctx, &pb.ChatTestRequest{ConsumerId: request.ConsumerId, ChannelId: request.ChannelId, TargetChannelId: "https://example.com", SessionId: request.SessionId}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("URL accepted")
	}
	if calls.Load() != 3 {
		t.Fatal("rejected request reached worker")
	}
	if WorkerChatTester("") != nil {
		t.Fatal("missing token accepted")
	}
}
