package internal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/h66rogi/rogi-collector/shared/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"testing"
	"time"
)

func testCertificates(t *testing.T) (*tls.Config, func(string) *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Fixture CA"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	leaf := func(identity string, server bool) tls.Certificate {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, _ := url.Parse(identity)
		c := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "Fixture"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, URIs: []*url.URL{uri}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			c.DNSNames = []string{"collector.fixture"}
			c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}
		raw, err := x509.CreateCertificate(rand.Reader, c, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{raw}, PrivateKey: k}
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{leaf("spiffe://fixture/server", true)}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, func(uri string) *tls.Config {
		return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{leaf(uri, false)}, RootCAs: pool, ServerName: "collector.fixture"}
	}
}
func TestProductMTLSReplayAndScope(t *testing.T) {
	pg, _, g := testutil.Collector(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d := model.Donation{EventID: "fixture-event", ChannelID: g.Channel, Count: 33, Kind: "balloon", DonorID: "fixture_donor", ConnectionEpoch: "fixture_epoch", Sequence: 1, ObservedAt: time.Now()}
	if _, err := pg.AppendDonation(ctx, g, d, time.Now()); err != nil {
		t.Fatal(err)
	}
	tlsServer, clientTLS := testCertificates(t)
	srv, err := NewProductServer(pg, nil, 0, tlsServer, ProductAccess{Consumer: "rogimarble", Channel: g.Channel, ReadURI: "spiffe://fixture/reader", ManageURI: "spiffe://fixture/operator", RecoveryURI: "spiffe://fixture/operator"}, slog.Default())
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
	reader := client("spiffe://fixture/reader")
	state, err := reader.GetCollectionStatus(ctx, &pb.GetCollectionStatusRequest{ConsumerId: "rogimarble", ChannelId: g.Channel})
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentCursor.ChannelOffset != 1 {
		t.Fatal("journal status missing")
	}
	if _, err = reader.GetCollectionStatus(ctx, &pb.GetCollectionStatusRequest{ConsumerId: "other", ChannelId: g.Channel}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("request spoofed certificate identity", err)
	}
	if _, err = reader.SetChannelSubscription(ctx, &pb.SetChannelSubscriptionRequest{ConsumerId: "rogimarble", ChannelId: g.Channel, IdempotencyKey: "fixture"}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("reader gained management scope", err)
	}
	if _, err = client("spiffe://fixture/other").GetCollectionStatus(ctx, &pb.GetCollectionStatusRequest{ConsumerId: "rogimarble", ChannelId: g.Channel}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("unknown certificate URI authorized", err)
	}
	if _, err = reader.ListDonations(ctx, &pb.ListDonationsRequest{ConsumerId: "rogimarble", ChannelId: g.Channel}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("implicit latest accepted", err)
	}
	request := &pb.ListDonationsRequest{ConsumerId: "rogimarble", ChannelId: g.Channel, AfterCursor: state.EarliestCursor}
	listed, err := reader.ListDonations(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Donations) != 1 || listed.Donations[0].SchemaVersion != "v1" || listed.Donations[0].NativeBalloonCount != 33 {
		t.Fatalf("wire mapping invalid %+v", listed)
	}
	event := listed.Donations[0]
	for i := 0; i < 2; i++ {
		if _, err = reader.AckDonations(ctx, &pb.AckDonationsRequest{ConsumerId: "rogimarble", ChannelId: g.Channel, Cursor: event.Cursor}); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := reader.WatchDonations(ctx, &pb.WatchDonationsRequest{ConsumerId: "rogimarble", ChannelId: g.Channel, AfterCursor: state.EarliestCursor})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := stream.Recv()
	if err != nil || replayed.EventId != event.EventId {
		t.Fatal("watch and list differ", err)
	}
	operator := client("spiffe://fixture/operator")
	_, err = operator.ResolveConsumerRecovery(ctx, &pb.ResolveConsumerRecoveryRequest{ConsumerId: "rogimarble", ChannelId: g.Channel, PreviousGeneration: g.Generation, NewGeneration: g.Generation, ResumeFrom: event.Cursor, Reason: "Synthetic recovery", OperatorId: "fixture_operator", IdempotencyKey: "fixture_recovery"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("old stream survived recovery", err)
	}
}
func TestChatWatchRealRedis(t *testing.T) {
	addr := os.Getenv("COLLECTOR_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("COLLECTOR_TEST_REDIS_ADDR not set")
	}
	pg, _, g := testutil.Collector(t)
	rdb := store.NewRedisClient(addr)
	t.Cleanup(func() { rdb.Close() })
	rs := store.NewRedisStore(rdb)
	// Unique channel key prevents interference with other tests and never flushes Redis.
	channel := "fixture_chat_" + time.Now().Format("150405.000000000")
	key := store.ChatStreamKey("soop", channel)
	ctx := context.Background()
	t.Cleanup(func() { rdb.Del(ctx, key, key+":generation") })
	if err := rdb.Set(ctx, key+":generation", g.Generation, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Do(ctx, "XADD", key, "*", "id", "fixture-chat", "type", "chat", "userId", "fixture_donor", "nickname", "Fixture", "message", "!이동 7", "timestamp", time.Now().UTC().Format(time.RFC3339Nano)).Err(); err != nil {
		t.Fatal(err)
	}
	tlsServer, clientTLS := testCertificates(t)
	srv, err := NewProductServer(pg, rs, 0, tlsServer, ProductAccess{Consumer: "rogimarble", Channel: channel, ReadURI: "spiffe://fixture/reader"}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.grpcServer.Serve(listener)
	t.Cleanup(srv.grpcServer.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS("spiffe://fixture/reader"))))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := pb.NewCollectorServiceClient(conn).WatchChat(bounded, &pb.WatchChatRequest{ConsumerId: "rogimarble", ChannelId: channel})
	if err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv()
	if err != nil || event.UserId != "fixture_donor" || event.Message != "!이동 7" || event.Cursor.StreamGeneration != g.Generation {
		t.Fatalf("chat delivery: %v %v", event, err)
	}
	if err = rdb.Del(ctx, key, key+":generation").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("Redis reset hidden", err)
	}
}
