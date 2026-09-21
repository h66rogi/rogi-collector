package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// MessageType represents the type of a chat message.
type MessageType string

const (
	MessageTypeChat         MessageType = "chat"
	MessageTypeDonation     MessageType = "donation"
	MessageTypeSubscription MessageType = "subscription"
	MessageTypeSystem       MessageType = "system"
)

// EmoteToken describes a platform emote referenced inline in Message.
// Start/End are byte offsets into Message (UTF-8). ImageURL may be empty
// when the platform did not provide an inline mapping (e.g. SOOP channel
// signature emotes whose URL is resolved from a per-channel catalog by
// the renderer).
type EmoteToken struct {
	Code     string `json:"code"`
	Start    int    `json:"start"`
	End      int    `json:"end"`
	ImageURL string `json:"imageUrl,omitempty"`
	Animated bool   `json:"animated,omitempty"`
	Source   string `json:"source,omitempty"`
}

// ChatMessage is the unified message format across all platforms.
type ChatMessage struct {
	ID           string       `json:"id"`
	Type         MessageType  `json:"type"`
	Platform     Platform     `json:"platform"`
	ChannelID    string       `json:"channelId"`
	StreamerName string       `json:"streamerName"`
	UserID       string       `json:"userId"`
	Nickname     string       `json:"nickname"`
	Message      string       `json:"message"`
	Timestamp    time.Time    `json:"timestamp"`
	Raw          string       `json:"raw"`
	Amount       float64      `json:"amount,omitempty"`
	Currency     string       `json:"currency,omitempty"`
	AmountKRW    int64        `json:"amountKRW,omitempty"`
	Emotes       []EmoteToken `json:"emotes,omitempty"`
}

// ToStreamFields returns a flat map suitable for Redis XADD.
func (m *ChatMessage) ToStreamFields() map[string]interface{} {
	fields := map[string]interface{}{
		"id":           m.ID,
		"type":         string(m.Type),
		"platform":     string(m.Platform),
		"channelId":    m.ChannelID,
		"streamerName": m.StreamerName,
		"userId":       m.UserID,
		"nickname":     m.Nickname,
		"message":      m.Message,
		"timestamp":    m.Timestamp.Format(time.RFC3339Nano),
		"raw":          m.Raw,
	}

	if m.Amount > 0 {
		fields["amount"] = fmt.Sprintf("%.2f", m.Amount)
		fields["currency"] = m.Currency
		fields["amountKRW"] = fmt.Sprintf("%d", m.AmountKRW)
	}

	if len(m.Emotes) > 0 {
		if b, err := json.Marshal(m.Emotes); err == nil {
			fields["emotes"] = string(b)
		}
	}

	return fields
}
