package archive

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestArchiveExportPreservesDedupIndex(t *testing.T) {
	dsn := os.Getenv("COLLECTOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable PostgreSQL test database required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	channel := "archive-export-" + uuid.NewString()
	started := time.Now().UTC().Add(-time.Hour)
	_, err = pool.Exec(ctx, `INSERT INTO broadcast_sessions
		(started_at,platform,channel_id,session_seq,streamer_name,started_observed_at,first_seen_at,last_seen_at,instance_id)
		VALUES($1,'soop',$2,1,'synthetic',$1,$1,clock_timestamp(),'test')`, started, channel)
	if err != nil {
		t.Fatal(err)
	}
	writer := store.NewPgStore(pool)
	writer.SetCollectionChannel(channel)
	record := store.ArchiveChatRecord{
		SpoolID: uuid.NewString(), EventID: "synthetic-" + uuid.NewString(), Platform: model.PlatformSoop,
		ChannelID: channel, ReceivedAt: time.Now().UTC(), UserIDVersion: 1,
		PublicUserID: strings.Repeat("a", 64), DisplayName: "viewer", Message: "hello",
	}
	if assigned, err := writer.AppendArchiveChat(ctx, record); err != nil || !assigned {
		t.Fatalf("append: assigned=%v err=%v", assigned, err)
	}
	exporter, err := NewPgArchive(pool, channel)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := exporter.NextBatch(ctx, time.Now().Add(time.Minute), 1000)
	if err != nil || len(rows) != 1 {
		t.Fatalf("batch: %d rows, %v", len(rows), err)
	}
	segment, err := Build(rows)
	if err != nil {
		t.Fatal(err)
	}
	memory := &memoryS3{}
	objects, err := NewS3ObjectStore(memory, "disposable-bucket")
	if err != nil {
		t.Fatal(err)
	}
	verified, err := objects.PutVerified(ctx, segment)
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.CommitSegment(ctx, verified); err != nil {
		t.Fatal(err)
	}
	if err := exporter.CommitSegment(ctx, verified); err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
	rows, err = exporter.NextBatch(ctx, time.Now().Add(time.Minute), 1000)
	if err != nil || len(rows) != 0 {
		t.Fatalf("exported row reappeared: %d rows, %v", len(rows), err)
	}
	var hotCount, idCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM archive_chat_hot WHERE event_id=$1`, record.EventID).Scan(&hotCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM archive_event_ids WHERE event_id=$1`, record.EventID).Scan(&idCount); err != nil {
		t.Fatal(err)
	}
	if hotCount != 0 || idCount != 1 {
		t.Fatalf("unexpected retention: hot=%d dedup=%d", hotCount, idCount)
	}
	body, err := objects.GetVerified(ctx, segment.ObjectKey(), segment.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	segment.Body = body
	decoded, err := Decode(segment)
	if err != nil || len(decoded) != 1 || decoded[0].Message != "hello" {
		t.Fatalf("readback: %#v %v", decoded, err)
	}
	record.SpoolID, record.EventID = uuid.NewString(), "synthetic-"+uuid.NewString()
	record.Message = "newer"
	if assigned, err := writer.AppendArchiveChat(ctx, record); err != nil || !assigned {
		t.Fatalf("append newer: assigned=%v err=%v", assigned, err)
	}
	firstPage, err := exporter.ReadChatsPage(ctx, segment.SessionID, 0, 1, objects)
	if err != nil || len(firstPage.Rows) != 1 || firstPage.Rows[0].Message != "hello" || !firstPage.HasMore || firstPage.NextPosition == nil {
		t.Fatalf("first page: %#v %v", firstPage, err)
	}
	secondPage, err := exporter.ReadChatsPage(ctx, segment.SessionID, *firstPage.NextPosition, 1, objects)
	if err != nil || len(secondPage.Rows) != 1 || secondPage.Rows[0].Message != "newer" || secondPage.HasMore {
		t.Fatalf("second page: %#v %v", secondPage, err)
	}
}
