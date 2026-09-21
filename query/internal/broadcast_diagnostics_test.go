package internal

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A real mTLS gRPC exchange with no storage attached proves that diagnostics
// retain production authorization without registering or ingesting the test ID.
func TestBroadcastDiagnosticsMTLSScope(t *testing.T) {
	tlsServer, clientTLS := testCertificates(t)
	var calls atomic.Int32
	access := ProductAccess{Consumer: "fixture-consumer", Channel: "production-channel", ReadURI: "spiffe://fixture/reader", BroadcastCheck: func(_ context.Context, id string) (*pb.BroadcastStatus, error) {
		calls.Add(1)
		return &pb.BroadcastStatus{ChannelId: id, State: "live", Title: "Fixture live", CheckedAt: timestamppb.Now()}, nil
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
	request := &pb.CheckBroadcastRequest{ConsumerId: "fixture-consumer", ChannelId: "production-channel", TargetChannelId: "test-channel"}
	got, err := reader.CheckBroadcast(ctx, request)
	if err != nil || got.GetChannelId() != "test-channel" || got.GetState() != "live" {
		t.Fatal("valid diagnostic failed", err)
	}
	for _, tc := range []struct {
		consumer, channel, target string
		code                      codes.Code
	}{
		{"other-consumer", "production-channel", "test-channel", codes.PermissionDenied},
		{"fixture-consumer", "test-channel", "test-channel", codes.PermissionDenied},
		{"fixture-consumer", "production-channel", "https://example.com", codes.InvalidArgument},
	} {
		_, err = reader.CheckBroadcast(ctx, &pb.CheckBroadcastRequest{ConsumerId: tc.consumer, ChannelId: tc.channel, TargetChannelId: tc.target})
		if status.Code(err) != tc.code {
			t.Fatal("scope/ID validation failed", err)
		}
	}
	_, err = client("spiffe://fixture/unknown").CheckBroadcast(ctx, request)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("unknown certificate gained lookup", err)
	}
	if calls.Load() != 1 {
		t.Fatal("unauthorized lookup reached platform")
	}
	if DiscoverBroadcastChecker("") != nil {
		t.Fatal("missing internal token must disable diagnostics")
	}
}
