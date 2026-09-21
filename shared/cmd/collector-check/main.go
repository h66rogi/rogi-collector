// collector-check verifies the real mTLS API without printing viewer IDs/messages.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "collector check:", err)
		os.Exit(1)
	}
}
func run() error {
	if err := runtimeenv.Load(); err != nil {
		return err
	}
	pair, e := tls.LoadX509KeyPair(os.Getenv("CHECK_CERT_FILE"), os.Getenv("CHECK_KEY_FILE"))
	if e != nil {
		return fmt.Errorf("client certificate unavailable")
	}
	ca, e := os.ReadFile(os.Getenv("CHECK_CA_FILE"))
	if e != nil {
		return fmt.Errorf("CA unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return fmt.Errorf("CA invalid")
	}
	conn, e := grpc.NewClient(os.Getenv("CHECK_ADDRESS"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}, RootCAs: pool, ServerName: os.Getenv("CHECK_SERVER_NAME")})))
	if e != nil {
		return e
	}
	defer conn.Close()
	client := pb.NewCollectorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	consumer := os.Getenv("CHECK_CONSUMER")
	if consumer == "" {
		consumer = "rogimarble"
	}
	channel := os.Getenv("CHECK_CHANNEL")
	state, e := client.GetCollectionStatus(ctx, &pb.GetCollectionStatusRequest{ConsumerId: consumer, ChannelId: channel})
	if e != nil {
		return e
	}
	out := map[string]any{"state": state.ProcessHealth, "collectionActive": state.CollectionActive, "qualityReasons": state.QualityReasons}
	defer func() { _ = json.NewEncoder(os.Stdout).Encode(out) }()
	if len(os.Args) > 1 && os.Args[1] == "donations" {
		return donationCheck(ctx, client, consumer, channel, state, out)
	}
	if len(os.Args) > 1 && os.Args[1] == "chat" {
		started := time.Now().Add(-time.Second)
		stream, e := client.WatchChat(ctx, &pb.WatchChatRequest{ConsumerId: consumer, ChannelId: channel})
		if e != nil {
			return e
		}
		count := 0
		seen := map[string]bool{}
		wanted := 1
		if v := os.Getenv("CHECK_CHAT_COUNT"); v != "" {
			wanted, e = strconv.Atoi(v)
			if e != nil || wanted < 1 || wanted > 100 {
				return fmt.Errorf("invalid chat check count")
			}
		}
		for count < wanted {
			out["freshChatEvents"] = count
			event, e := stream.Recv()
			if e != nil {
				return e
			}
			if event.EventId == "" || event.UserId == "" || event.ObservedAt == nil || event.Cursor == nil || event.Cursor.StreamGeneration == "" {
				return fmt.Errorf("incomplete live chat event")
			}
			if event.ObservedAt.AsTime().Before(started) {
				continue
			}
			if seen[event.EventId] {
				return fmt.Errorf("duplicate chat event ID")
			}
			seen[event.EventId] = true
			count++
		}
		out["freshChatEvents"] = count
	}
	return nil
}
