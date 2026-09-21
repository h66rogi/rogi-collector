package store

import (
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
)

// ChatMessageRow represents a single chat message row for ClickHouse insertion.
type ChatMessageRow struct {
	MessageID    string
	Timestamp    time.Time
	Platform     string
	ChannelID    string
	StreamerName string
	UserID       string
	Nickname     string
	MessageType  string
	MessageText  string
	Amount       float32
	Currency     string
	AmountKRW    int64
	WorkerID     string
}

// ChatMessageRowFrom converts a model.ChatMessage to a ChatMessageRow.
// MessageID is derived from a content hash of the raw message, making it
// deterministic and idempotent regardless of the workerID.
func ChatMessageRowFrom(msg model.ChatMessage, workerID string) ChatMessageRow {
	hashInput := msg.Raw
	if hashInput == "" {
		// Fallback: use original ID if Raw is empty (e.g., backfill sources).
		// This avoids all empty-Raw messages for the same channel colliding.
		hashInput = msg.ID
	}
	hash := xxhash.Sum64String(hashInput)
	return ChatMessageRow{
		MessageID:    fmt.Sprintf("%s-%s-%016x", msg.Platform, msg.ChannelID, hash),
		Timestamp:    msg.Timestamp,
		Platform:     string(msg.Platform),
		ChannelID:    msg.ChannelID,
		StreamerName: msg.StreamerName,
		UserID:       msg.UserID,
		Nickname:     msg.Nickname,
		MessageType:  string(msg.Type),
		MessageText:  msg.Message,
		Amount:       float32(msg.Amount),
		Currency:     msg.Currency,
		AmountKRW:    msg.AmountKRW,
		WorkerID:     workerID,
	}
}
