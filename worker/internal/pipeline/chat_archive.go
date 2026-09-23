package pipeline

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"golang.org/x/sys/unix"
)

type ChatArchiveJournal interface {
	AppendArchiveChat(context.Context, store.ArchiveChatRecord) (bool, error)
}

type chatArchiveEnvelope struct {
	Version int                     `json:"version"`
	SHA256  string                  `json:"sha256"`
	Record  store.ArchiveChatRecord `json:"record"`
}

// ChatArchiveSpool fsyncs a sanitized chat record before the worker publishes
// that message to Redis. A single writer lock prevents duplicate local owners.
type ChatArchiveSpool struct {
	mu        sync.Mutex
	drainMu   sync.Mutex
	dir       string
	key       []byte
	maxBytes  int64
	usedBytes int64
	files     int
	journal   ChatArchiveJournal
	lock      *os.File
	broken    bool
}

func NewChatArchiveSpool(dir string, key []byte, journal ChatArchiveJournal, maxBytes int64) (*ChatArchiveSpool, error) {
	if !filepath.IsAbs(dir) || len(key) < 32 || journal == nil || maxBytes < 1<<20 {
		return nil, errors.New("absolute chat archive path, 32-byte key, journal and spool limit required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("chat archive spool directory permissions too broad")
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("chat archive spool already owned")
	}
	s := &ChatArchiveSpool{dir: dir, key: append([]byte(nil), key...), maxBytes: maxBytes, journal: journal, lock: lock}
	_, size, count, err := s.list()
	if err != nil {
		lock.Close()
		return nil, err
	}
	s.usedBytes, s.files = size, count
	return s, nil
}

func (s *ChatArchiveSpool) Close() error {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	_ = unix.Flock(int(s.lock.Fd()), unix.LOCK_UN)
	err := s.lock.Close()
	s.lock = nil
	for i := range s.key {
		s.key[i] = 0
	}
	return err
}

func (s *ChatArchiveSpool) syncDir() error {
	f, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (s *ChatArchiveSpool) list() ([]string, int64, int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, 0, 0, err
	}
	var names []string
	var size int64
	for _, entry := range entries {
		if entry.Name() == ".writer.lock" {
			continue
		}
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil, 0, 0, errors.New("unexpected or incomplete chat spool entry")
		}
		info, err := entry.Info()
		if err != nil {
			return nil, 0, 0, err
		}
		size += info.Size()
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, size, len(names), nil
}

func (s *ChatArchiveSpool) publicUserID(message model.ChatMessage) string {
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(message.Platform))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(message.ChannelID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(message.UserID))
	return hex.EncodeToString(mac.Sum(nil))
}

// Save only performs the durable local acceptance. Drain is run separately so
// a PostgreSQL outage does not hold up the real-time Redis publish path.
func (s *ChatArchiveSpool) Save(message model.ChatMessage, receivedAt time.Time) error {
	if message.Type != model.MessageTypeChat {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.broken {
		return errors.New("chat archive spool unavailable")
	}
	record := store.ArchiveChatRecord{
		SpoolID: uuid.NewString(), EventID: message.ID, Platform: message.Platform,
		ChannelID: message.ChannelID, ReceivedAt: receivedAt.UTC(), UserIDVersion: 1,
		PublicUserID: s.publicUserID(message), DisplayName: message.Nickname, Message: message.Message,
	}
	if err := record.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	body, err := json.Marshal(chatArchiveEnvelope{Version: 1, SHA256: hex.EncodeToString(sum[:]), Record: record})
	if err != nil {
		return err
	}
	if len(body) > 1<<20 || s.usedBytes+int64(len(body)) > s.maxBytes || s.files >= 100000 {
		return errors.New("chat archive spool full")
	}
	name := fmt.Sprintf("%020d-%s", receivedAt.UnixNano(), record.SpoolID)
	pending := filepath.Join(s.dir, name+".pending")
	file, err := os.OpenFile(pending, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(body)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		s.broken = true
		return err
	}
	if err = os.Rename(pending, filepath.Join(s.dir, name+".json")); err != nil {
		s.broken = true
		return err
	}
	if err = s.syncDir(); err != nil {
		s.broken = true
		return err
	}
	s.usedBytes += int64(len(body))
	s.files++
	return nil
}

func (s *ChatArchiveSpool) Drain(ctx context.Context) error {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	s.mu.Lock()
	if s.lock == nil || s.broken {
		s.mu.Unlock()
		return errors.New("chat archive spool unavailable")
	}
	names, _, _, err := s.list()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(s.dir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(body) > 1<<20 {
			return errors.New("oversized chat archive spool record")
		}
		var envelope chatArchiveEnvelope
		if err := json.Unmarshal(body, &envelope); err != nil || envelope.Version != 1 {
			return errors.New("corrupt chat archive spool record")
		}
		raw, err := json.Marshal(envelope.Record)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		if envelope.SHA256 != hex.EncodeToString(sum[:]) {
			return errors.New("chat archive spool checksum mismatch")
		}
		if err := envelope.Record.Validate(); err != nil {
			return err
		}
		if _, err := s.journal.AppendArchiveChat(ctx, envelope.Record); err != nil {
			return err
		}
		s.mu.Lock()
		if err := os.Remove(path); err != nil {
			s.broken = true
			s.mu.Unlock()
			return err
		}
		if err := s.syncDir(); err != nil {
			s.broken = true
			s.mu.Unlock()
			return err
		}
		s.files--
		s.usedBytes -= int64(len(body))
		s.mu.Unlock()
	}
	return nil
}

func (s *ChatArchiveSpool) Backlog() (int, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.files, s.usedBytes
}
