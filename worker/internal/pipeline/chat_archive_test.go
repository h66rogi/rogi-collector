package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

type fakeArchiveJournal struct {
	fail    bool
	records []store.ArchiveChatRecord
}

func (f *fakeArchiveJournal) AppendArchiveChat(_ context.Context, record store.ArchiveChatRecord) (bool, error) {
	if f.fail {
		return false, errors.New("disposable database unavailable")
	}
	f.records = append(f.records, record)
	return true, nil
}

func TestChatArchiveSpoolKeepsSanitizedMessageAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	key := []byte(strings.Repeat("k", 32))
	journal := &fakeArchiveJournal{fail: true}
	spool, err := NewChatArchiveSpool(dir, key, journal, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	message := model.ChatMessage{
		ID: "event-1", Type: model.MessageTypeChat, Platform: model.PlatformSoop, ChannelID: "h66rogi",
		UserID: "private-platform-user", Nickname: "viewer", Message: "hello", Raw: "private-platform-packet",
	}
	if err := spool.Save(message, time.Now()); err != nil {
		t.Fatal(err)
	}
	if files, _ := spool.Backlog(); files != 1 {
		t.Fatalf("backlog=%d", files)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), message.UserID) || strings.Contains(string(body), message.Raw) {
			t.Fatal("spool leaked raw platform identity or packet")
		}
	}
	if err := spool.Drain(context.Background()); err == nil {
		t.Fatal("database failure did not retain record")
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	journal.fail = false
	spool, err = NewChatArchiveSpool(dir, key, journal, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	if err := spool.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if files, _ := spool.Backlog(); files != 0 {
		t.Fatalf("backlog after replay=%d", files)
	}
	if len(journal.records) != 1 || journal.records[0].PublicUserID == message.UserID || len(journal.records[0].PublicUserID) != 64 {
		t.Fatalf("unexpected replay record: %#v", journal.records)
	}
}
