// Package diagnostics contains transient operator-test responses, never raw packets.
package diagnostics

import "time"

type ChatSample struct {
	Sequence    uint64    `json:"sequence"`
	DisplayName string    `json:"displayName"`
	Message     string    `json:"message"`
	ReceivedAt  time.Time `json:"receivedAt"`
}
type ChatTestStatus struct {
	SessionID      string       `json:"sessionId"`
	ChannelID      string       `json:"channelId"`
	State          string       `json:"state"`
	Active         bool         `json:"active"`
	StartedAt      *time.Time   `json:"startedAt"`
	ExpiresAt      *time.Time   `json:"expiresAt"`
	JoinedAt       *time.Time   `json:"joinedAt"`
	LastReceivedAt *time.Time   `json:"lastReceivedAt"`
	ReceivedCount  uint64       `json:"receivedCount"`
	Messages       []ChatSample `json:"messages"`
}
