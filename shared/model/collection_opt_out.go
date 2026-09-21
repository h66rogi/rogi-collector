package model

import "time"

// CollectionOptOut blocks future chat/ranking collection for a platform
// channel, including channels that are not registered in an upstream application.
type CollectionOptOut struct {
	ID               int64      `db:"id" json:"id"`
	BatchID          *int64     `db:"batch_id" json:"batchId"`
	Platform         Platform   `db:"platform" json:"platform"`
	ChannelID        string     `db:"channel_id" json:"channelId"`
	StreamerName     *string    `db:"streamer_name" json:"streamerName"`
	RequesterEmail   *string    `db:"requester_email" json:"requesterEmail"`
	Reason           *string    `db:"reason" json:"reason"`
	Active           bool       `db:"active" json:"active"`
	CreatedBy        *string    `db:"created_by" json:"createdBy"`
	CreatedAt        time.Time  `db:"created_at" json:"createdAt"`
	UpdatedAt        time.Time  `db:"updated_at" json:"updatedAt"`
	RevokedAt        *time.Time `db:"revoked_at" json:"revokedAt"`
	RevokedBy        *string    `db:"revoked_by" json:"revokedBy"`
	RevocationReason *string    `db:"revocation_reason" json:"revocationReason"`
}

type CollectionOptOutBatch struct {
	ID            int64     `db:"id" json:"id"`
	Title         string    `db:"title" json:"title"`
	RequestSource *string   `db:"request_source" json:"requestSource"`
	Reason        *string   `db:"reason" json:"reason"`
	RequestedBy   *string   `db:"requested_by" json:"requestedBy"`
	CreatedBy     *string   `db:"created_by" json:"createdBy"`
	CreatedAt     time.Time `db:"created_at" json:"createdAt"`
	ItemCount     int       `db:"item_count" json:"itemCount"`
	ActiveCount   int       `db:"active_count" json:"activeCount"`
}
