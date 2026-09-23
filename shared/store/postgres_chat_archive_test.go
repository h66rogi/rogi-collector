package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestArchiveChatSessionAndReplay(t *testing.T) {
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
	channel := "archive-test-" + uuid.NewString()
	store := NewPgStore(pool)
	store.SetCollectionChannel(channel)
	started := time.Now().UTC().Add(-time.Minute)
	_, err = pool.Exec(ctx, `INSERT INTO broadcast_sessions
		(started_at,platform,channel_id,session_seq,streamer_name,started_observed_at,first_seen_at,last_seen_at,instance_id)
		VALUES($1,'soop',$2,1,'synthetic',$1,$1,clock_timestamp(),'test')`, started, channel)
	if err != nil {
		t.Fatal(err)
	}
	record := ArchiveChatRecord{
		SpoolID: uuid.NewString(), EventID: "synthetic-" + uuid.NewString(), Platform: model.PlatformSoop,
		ChannelID: channel, ReceivedAt: time.Now().UTC(), UserIDVersion: 1,
		PublicUserID: strings.Repeat("a", 64), DisplayName: "synthetic", Message: "hello",
	}
	assigned, err := store.AppendArchiveChat(ctx, record)
	if err != nil || !assigned {
		t.Fatalf("first append: assigned=%v err=%v", assigned, err)
	}
	record.SpoolID = uuid.NewString()
	assigned, err = store.AppendArchiveChat(ctx, record)
	if err != nil || !assigned {
		t.Fatalf("replay: assigned=%v err=%v", assigned, err)
	}
	var hotCount, idCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM archive_chat_hot WHERE event_id=$1`, record.EventID).Scan(&hotCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM archive_event_ids WHERE event_id=$1`, record.EventID).Scan(&idCount); err != nil {
		t.Fatal(err)
	}
	if hotCount != 1 || idCount != 1 {
		t.Fatalf("replay duplicated archive rows: hot=%d id=%d", hotCount, idCount)
	}
	record.SpoolID, record.EventID = uuid.NewString(), "unassigned-event"
	record.ReceivedAt = time.Now().UTC().Add(time.Hour)
	assigned, err = store.AppendArchiveChat(ctx, record)
	if err != nil || assigned {
		t.Fatalf("unassigned append: assigned=%v err=%v", assigned, err)
	}
	assigned, err = store.AppendArchiveChat(ctx, record)
	if err != nil || assigned {
		t.Fatalf("unassigned replay: assigned=%v err=%v", assigned, err)
	}
	var unassignedCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM archive_unassigned_chat WHERE spool_id=$1`, record.SpoolID).Scan(&unassignedCount); err != nil {
		t.Fatal(err)
	}
	if unassignedCount != 1 {
		t.Fatalf("unassigned replay duplicated row: %d", unassignedCount)
	}
	secondStarted := record.ReceivedAt.Add(5 * time.Second)
	_, err = pool.Exec(ctx, `INSERT INTO broadcast_sessions
		(started_at,platform,channel_id,session_seq,streamer_name,started_observed_at,first_seen_at,last_seen_at,instance_id)
		VALUES($1,'soop',$2,2,'synthetic',$1,$1,$1,'test')`, secondStarted, channel)
	if err != nil {
		t.Fatal(err)
	}
	attempted, assignedCount, err := store.ReconcileUnassignedArchiveChats(ctx, 10)
	if err != nil || attempted != 1 || assignedCount != 1 {
		t.Fatalf("reconcile: attempted=%d assigned=%d err=%v", attempted, assignedCount, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM archive_unassigned_chat WHERE spool_id=$1`, record.SpoolID).Scan(&unassignedCount); err != nil {
		t.Fatal(err)
	}
	if unassignedCount != 0 {
		t.Fatalf("reconciled record remained unassigned: %d", unassignedCount)
	}
}
