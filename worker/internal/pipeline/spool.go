package pipeline

import (
	"context"
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

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
	"golang.org/x/sys/unix"
)

type journalUnavailable struct{ error }

func (e journalUnavailable) Unwrap() error { return e.error }

type DonationJournal interface {
	AppendDonation(context.Context, store.OwnerGrant, model.Donation, time.Time) (model.Donation, error)
}
type spoolRecord struct {
	Grant      store.OwnerGrant `json:"grant"`
	Donation   model.Donation   `json:"donation"`
	AcceptedAt time.Time        `json:"acceptedAt"`
}
type spoolEnvelope struct {
	Version  int             `json:"version"`
	Checksum string          `json:"checksum"`
	Record   json.RawMessage `json:"record"`
}
type DonationSpool struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64
	maxFiles int
	journal  DonationJournal
	lock     *os.File
}

func NewDonationSpool(dir string, journal DonationJournal, maxBytes int64) (*DonationSpool, error) {
	if !filepath.IsAbs(dir) || maxBytes <= 0 {
		return nil, errors.New("absolute DONATION_SPOOL_DIR and positive spool limit required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("donation spool already owned")
	}
	return &DonationSpool{dir: dir, journal: journal, maxBytes: maxBytes, maxFiles: 10000, lock: f}, nil
}
func (s *DonationSpool) Close() error { s.mu.Lock(); defer s.mu.Unlock(); return s.lock.Close() }
func (s *DonationSpool) syncDir() error {
	f, e := os.Open(s.dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func (s *DonationSpool) files() ([]string, int64, error) {
	entries, e := os.ReadDir(s.dir)
	if e != nil {
		return nil, 0, e
	}
	var files []string
	var size int64
	for _, entry := range entries {
		if entry.Name() == ".writer.lock" {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, 0, errors.New("unexpected spool entry")
		}
		info, e := entry.Info()
		if e != nil {
			return nil, 0, e
		}
		size += info.Size()
		if strings.HasSuffix(entry.Name(), ".pending") {
			return nil, size, errors.New("incomplete spool record: recovery required")
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			return nil, size, errors.New("unknown spool record")
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	return files, size, nil
}

// Save fsyncs before returning success. A DB outage retains the file, not a RAM queue.
func (s *DonationSpool) Save(ctx context.Context, g store.OwnerGrant, d model.Donation, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := d.Validate(); err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return store.ErrOwner
	}
	files, size, err := s.files()
	if err != nil {
		return err
	}
	if len(files) >= s.maxFiles {
		return errors.New("donation spool full")
	}
	accepted := time.Now().UTC()
	raw, err := json.Marshal(spoolRecord{g, d, accepted})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	body, err := json.Marshal(spoolEnvelope{1, hex.EncodeToString(sum[:]), raw})
	if err != nil {
		return err
	}
	if size+int64(len(body)) > s.maxBytes {
		return errors.New("donation spool full")
	}
	name := fmt.Sprintf("%020d-%s", accepted.UnixNano(), d.EventID)
	if filepath.Base(name) != name {
		return errors.New("invalid event ID")
	}
	pending := filepath.Join(s.dir, name+".pending")
	f, err := os.OpenFile(pending, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	// A slow write must not become an accepted record after local authority expires.
	// Keep the pending file for explicit inspection rather than deleting evidence.
	if !time.Now().Before(deadline) {
		return store.ErrOwner
	}
	if err = os.Rename(pending, filepath.Join(s.dir, name+".json")); err != nil {
		return err
	}
	if err = s.syncDir(); err != nil {
		return err
	}
	err = s.drain(ctx)
	var unavailable journalUnavailable
	if errors.As(err, &unavailable) {
		return nil
	}
	return err
}
func (s *DonationSpool) Drain(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drain(ctx)
}
func (s *DonationSpool) drain(ctx context.Context) error {
	files, _, err := s.files()
	if err != nil {
		return err
	}
	for _, name := range files {
		if err = ctx.Err(); err != nil {
			return err
		}
		file := filepath.Join(s.dir, name)
		info, e := os.Stat(file)
		if e != nil {
			return e
		}
		if info.Size() > 1<<20 {
			return errors.New("spool record too large")
		}
		body, e := os.ReadFile(file)
		if e != nil {
			return e
		}
		var envelope spoolEnvelope
		if json.Unmarshal(body, &envelope) != nil || envelope.Version != 1 {
			return errors.New("corrupt spool record")
		}
		sum := sha256.Sum256(envelope.Record)
		if hex.EncodeToString(sum[:]) != envelope.Checksum {
			return errors.New("spool checksum mismatch")
		}
		var record spoolRecord
		if json.Unmarshal(envelope.Record, &record) != nil {
			return errors.New("corrupt spool payload")
		}
		if time.Since(record.AcceptedAt) > 30*24*time.Hour {
			return errors.New("old spool record: recovery required")
		}
		if _, e = s.journal.AppendDonation(ctx, record.Grant, record.Donation, record.AcceptedAt); e != nil {
			if errors.Is(e, store.ErrOwner) || errors.Is(e, store.ErrGeneration) || errors.Is(e, store.ErrCollectionDisabled) || errors.Is(e, store.ErrCursorExpired) || errors.Is(e, store.ErrConflict) {
				return e
			}
			return journalUnavailable{e}
		}
		if e = os.Remove(file); e != nil {
			return e
		}
		if e = s.syncDir(); e != nil {
			return e
		}
	}
	return nil
}
func (s *DonationSpool) Backlog() (int, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	files, size, err := s.files()
	return len(files), size, err
}
