package store

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// chOpenIntervalSentinel represents an open (not yet closed) viewer count interval.
// ClickHouse ReplacingMergeTree requires a non-nullable version column, so we use
// a far-future sentinel instead of NULL. The sentinel ensures open intervals "win"
// during deduplication (highest valid_to is kept).
var chOpenIntervalSentinel = time.Date(2099, 12, 31, 23, 59, 59, 999000000, time.UTC)

// ViewerCountRow represents a single viewer count interval for ClickHouse.
type ViewerCountRow struct {
	ValidFrom   time.Time
	ValidTo     *time.Time // nil means open interval (converted to sentinel on insert)
	Platform    string
	ChannelID   string
	SessionSeq  uint64
	ViewerCount uint32
	MinCount    uint32
	MaxCount    uint32
	SampleCount uint16
	InstanceID  string
}

// ClickHouseStore provides write access to ClickHouse.
type ClickHouseStore struct {
	conn driver.Conn
}

func NewClickHouseStore(addr, database, user, password string) (*ClickHouseStore, error) {
	return newClickHouseStore(addr, database, user, password, true)
}

// NewLazyClickHouseStore creates a store without requiring ClickHouse to be
// reachable during process startup. The driver's connection pool establishes
// and re-establishes connections when a batch is flushed, allowing the worker's
// retrying batch writer to recover from ClickHouse restarts and DNS outages.
func NewLazyClickHouseStore(addr, database, user, password string) (*ClickHouseStore, error) {
	return newClickHouseStore(addr, database, user, password, false)
}

func newClickHouseStore(addr, database, user, password string, ping bool) (*ClickHouseStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		ReadTimeout:     30 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 30 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	if ping {
		if err := conn.Ping(context.Background()); err != nil {
			return nil, err
		}
	}
	return &ClickHouseStore{conn: conn}, nil
}

// BatchInsertViewerCounts inserts multiple viewer count rows in a single batch.
func (s *ClickHouseStore) BatchInsertViewerCounts(ctx context.Context, rows []ViewerCountRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO viewer_count_history
			(valid_from, valid_to, platform, channel_id, session_seq,
			 viewer_count, min_count, max_count, sample_count, instance_id)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		validTo := chOpenIntervalSentinel
		if r.ValidTo != nil {
			validTo = *r.ValidTo
		}
		if err := batch.Append(
			r.ValidFrom, validTo, r.Platform, r.ChannelID, r.SessionSeq,
			r.ViewerCount, r.MinCount, r.MaxCount, r.SampleCount, r.InstanceID,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// BatchInsertChatMessages inserts multiple chat message rows in a single batch.
func (s *ClickHouseStore) BatchInsertChatMessages(ctx context.Context, rows []ChatMessageRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO chat_messages
			(message_id, timestamp, platform, channel_id, streamer_name,
			 user_id, nickname, message_type, message_text,
			 amount, currency, amount_krw, worker_id)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MessageID, r.Timestamp, r.Platform, r.ChannelID, r.StreamerName,
			r.UserID, r.Nickname, r.MessageType, r.MessageText,
			r.Amount, r.Currency, r.AmountKRW, r.WorkerID,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}
