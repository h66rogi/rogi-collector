package model

import (
	"errors"
	"time"
)

// Donation is the durable platform observation, with no game/session decisions.
type Donation struct {
	EventID             string    `json:"eventId"`
	ChannelID           string    `json:"channelId"`
	Count               int64     `json:"nativeBalloonCount"`
	Kind                string    `json:"donationKind"`
	DonorID             string    `json:"donorId"`
	DisplayName         string    `json:"displayName"`
	Message             string    `json:"message"`
	ConnectionEpoch     string    `json:"connectionEpoch"`
	Sequence            uint64    `json:"connectionSequence,string"`
	ObservedAt          time.Time `json:"observedAt"`
	ConnectionStartedAt time.Time `json:"connectionStartedAt"`
	SourceID            *string   `json:"sourceEventId,omitempty"`
	BroadcastID         *string   `json:"broadcastId,omitempty"`
	Identity            string    `json:"identityStatus"`
	Reasons             []string  `json:"qualityReasons,omitempty"`
	Related             []string  `json:"relatedObservationIds,omitempty"`
	Generation          string    `json:"journalGeneration"`
	Offset              uint64    `json:"channelOffset,string"`
}

func (d Donation) Validate() error {
	if len(d.EventID) > 256 || len(d.ChannelID) > 100 || len(d.DonorID) > 256 || len(d.DisplayName) > 4096 || len(d.Message) > 65536 || len(d.ConnectionEpoch) > 256 {
		return errors.New("donation observation too large")
	}
	if d.EventID == "" || d.ChannelID == "" || d.DonorID == "" || d.ConnectionEpoch == "" || d.Sequence == 0 || d.Count <= 0 || d.Count > 9007199254740991 || d.Kind != "balloon" || d.ObservedAt.IsZero() {
		return errors.New("invalid donation observation")
	}
	return nil
}
