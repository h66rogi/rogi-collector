package archive

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

const (
	MaxSegmentRows       = 1000
	MaxSegmentPlainBytes = 16 << 20
	MaxSegmentGzipBytes  = 16 << 20
)

// ChatRow is the public, sanitized data stored in one immutable S3 segment.
// The platform raw user ID and raw packet are deliberately absent.
type ChatRow struct {
	SessionID     string    `json:"sessionId"`
	Position      int64     `json:"position"`
	EventID       string    `json:"eventId"`
	ReceivedAt    time.Time `json:"receivedAt"`
	UserIDVersion int16     `json:"userIdVersion"`
	PublicUserID  string    `json:"publicUserId"`
	DisplayName   string    `json:"displayName"`
	Message       string    `json:"message"`
}

type Segment struct {
	Body          []byte
	SHA256        string
	SessionID     string
	FirstPosition int64
	LastPosition  int64
	Count         int
}

func (s Segment) ObjectKey() string {
	return fmt.Sprintf("v1/soop/h66rogi/%s/%d-%d-%s.jsonl.gz", s.SessionID, s.FirstPosition, s.LastPosition, s.SHA256)
}

// Build produces deterministic gzip NDJSON from a strictly ordered slice.
// A caller must reduce the batch size if a chat-heavy segment exceeds 16 MiB.
func Build(rows []ChatRow) (Segment, error) {
	if len(rows) == 0 || len(rows) > MaxSegmentRows {
		return Segment{}, errors.New("invalid segment row count")
	}
	sessionID := rows[0].SessionID
	if _, err := uuid.Parse(sessionID); err != nil || rows[0].Position <= 0 {
		return Segment{}, errors.New("invalid first archive row")
	}
	var plain bytes.Buffer
	previous := int64(0)
	for _, row := range rows {
		if row.SessionID != sessionID || row.Position <= previous || row.EventID == "" || row.ReceivedAt.IsZero() {
			return Segment{}, errors.New("unordered or invalid archive rows")
		}
		line, err := json.Marshal(row)
		if err != nil {
			return Segment{}, err
		}
		if plain.Len()+len(line)+1 > MaxSegmentPlainBytes {
			return Segment{}, errors.New("segment uncompressed size limit")
		}
		_, _ = plain.Write(line)
		_ = plain.WriteByte('\n')
		previous = row.Position
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	writer.Header.ModTime = time.Unix(0, 0)
	writer.Header.OS = 255
	if _, err := writer.Write(plain.Bytes()); err != nil {
		return Segment{}, err
	}
	if err := writer.Close(); err != nil {
		return Segment{}, err
	}
	if compressed.Len() > MaxSegmentGzipBytes {
		return Segment{}, errors.New("segment compressed size limit")
	}
	sum := sha256.Sum256(compressed.Bytes())
	return Segment{Body: compressed.Bytes(), SHA256: hex.EncodeToString(sum[:]), SessionID: sessionID, FirstPosition: rows[0].Position, LastPosition: previous, Count: len(rows)}, nil
}

// Decode verifies the exact checksum, bounded decompression, count and range
// before any row is returned to a public history request.
func Decode(segment Segment) ([]ChatRow, error) {
	if _, err := uuid.Parse(segment.SessionID); err != nil || len(segment.Body) == 0 || len(segment.Body) > MaxSegmentGzipBytes || segment.Count < 1 || segment.Count > MaxSegmentRows || segment.FirstPosition <= 0 || segment.LastPosition < segment.FirstPosition {
		return nil, errors.New("invalid segment metadata")
	}
	sum := sha256.Sum256(segment.Body)
	if hex.EncodeToString(sum[:]) != segment.SHA256 {
		return nil, errors.New("segment checksum mismatch")
	}
	reader, err := gzip.NewReader(bytes.NewReader(segment.Body))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	plain, err := io.ReadAll(io.LimitReader(reader, MaxSegmentPlainBytes+1))
	if err != nil {
		return nil, err
	}
	if len(plain) > MaxSegmentPlainBytes {
		return nil, errors.New("segment decompression limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(plain))
	rows := make([]ChatRow, 0, segment.Count)
	previous := int64(0)
	for {
		var row ChatRow
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode archive row: %w", err)
		}
		if row.SessionID != segment.SessionID || row.Position <= previous || row.EventID == "" || row.ReceivedAt.IsZero() {
			return nil, errors.New("segment row identity or order mismatch")
		}
		rows = append(rows, row)
		if len(rows) > segment.Count {
			return nil, errors.New("segment row count exceeded")
		}
		previous = row.Position
	}
	if len(rows) != segment.Count || rows[0].Position != segment.FirstPosition || previous != segment.LastPosition {
		return nil, errors.New("segment range or count mismatch")
	}
	return rows, nil
}
