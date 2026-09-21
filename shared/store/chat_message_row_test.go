package store

import (
	"strings"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func baseChatMessage() model.ChatMessage {
	return model.ChatMessage{
		ID:           "msg-001",
		Type:         model.MessageTypeChat,
		Platform:     model.PlatformChzzk,
		ChannelID:    "channel-42",
		StreamerName: "streamer-name",
		UserID:       "user-99",
		Nickname:     "TestUser",
		Message:      "Hello, world!",
		Timestamp:    time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC),
		Raw:          `{"id":"msg-001","type":"chat","message":"Hello, world!"}`,
	}
}

func TestChatMessageRow_BasicConversion(t *testing.T) {
	msg := baseChatMessage()
	row := ChatMessageRowFrom(msg, "worker-1")

	if row.Platform != string(model.PlatformChzzk) {
		t.Errorf("Platform = %q, want %q", row.Platform, string(model.PlatformChzzk))
	}
	if row.ChannelID != msg.ChannelID {
		t.Errorf("ChannelID = %q, want %q", row.ChannelID, msg.ChannelID)
	}
	if row.StreamerName != msg.StreamerName {
		t.Errorf("StreamerName = %q, want %q", row.StreamerName, msg.StreamerName)
	}
	if row.UserID != msg.UserID {
		t.Errorf("UserID = %q, want %q", row.UserID, msg.UserID)
	}
	if row.Nickname != msg.Nickname {
		t.Errorf("Nickname = %q, want %q", row.Nickname, msg.Nickname)
	}
	if row.MessageType != string(model.MessageTypeChat) {
		t.Errorf("MessageType = %q, want %q", row.MessageType, string(model.MessageTypeChat))
	}
	if row.MessageText != msg.Message {
		t.Errorf("MessageText = %q, want %q", row.MessageText, msg.Message)
	}
	if !row.Timestamp.Equal(msg.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", row.Timestamp, msg.Timestamp)
	}
	if row.WorkerID != "worker-1" {
		t.Errorf("WorkerID = %q, want %q", row.WorkerID, "worker-1")
	}
}

func TestChatMessageRow_MessageIDFormat(t *testing.T) {
	msg := baseChatMessage()
	row := ChatMessageRowFrom(msg, "worker-1")

	// MessageID must start with "<platform>-<channelID>-"
	prefix := string(model.PlatformChzzk) + "-" + msg.ChannelID + "-"
	if !strings.HasPrefix(row.MessageID, prefix) {
		t.Errorf("MessageID %q does not start with prefix %q", row.MessageID, prefix)
	}

	// The hash suffix should be exactly 16 hex characters
	suffix := strings.TrimPrefix(row.MessageID, prefix)
	if len(suffix) != 16 {
		t.Errorf("MessageID hash suffix length = %d, want 16; suffix=%q", len(suffix), suffix)
	}
}

func TestChatMessageRow_Deterministic(t *testing.T) {
	msg := baseChatMessage()

	row1 := ChatMessageRowFrom(msg, "worker-1")
	row2 := ChatMessageRowFrom(msg, "worker-2")
	row3 := ChatMessageRowFrom(msg, "worker-1")

	if row1.MessageID != row2.MessageID {
		t.Errorf("MessageID differs across workers: %q vs %q", row1.MessageID, row2.MessageID)
	}
	if row1.MessageID != row3.MessageID {
		t.Errorf("MessageID is not deterministic: %q vs %q", row1.MessageID, row3.MessageID)
	}
}

func TestChatMessageRow_DifferentRawProducesDifferentID(t *testing.T) {
	msg1 := baseChatMessage()
	msg2 := baseChatMessage()
	msg2.Raw = `{"id":"msg-002","type":"chat","message":"Different raw"}`

	row1 := ChatMessageRowFrom(msg1, "worker-1")
	row2 := ChatMessageRowFrom(msg2, "worker-1")

	if row1.MessageID == row2.MessageID {
		t.Errorf("Different Raw produced same MessageID: %q", row1.MessageID)
	}
}

func TestChatMessageRow_DonationFields(t *testing.T) {
	msg := baseChatMessage()
	msg.Type = model.MessageTypeDonation
	msg.Amount = 5000.0
	msg.Currency = "KRW"
	msg.AmountKRW = 5000

	row := ChatMessageRowFrom(msg, "worker-1")

	if row.MessageType != string(model.MessageTypeDonation) {
		t.Errorf("MessageType = %q, want %q", row.MessageType, string(model.MessageTypeDonation))
	}
	if row.Amount != float32(5000.0) {
		t.Errorf("Amount = %v, want %v", row.Amount, float32(5000.0))
	}
	if row.Currency != "KRW" {
		t.Errorf("Currency = %q, want %q", row.Currency, "KRW")
	}
	if row.AmountKRW != 5000 {
		t.Errorf("AmountKRW = %d, want %d", row.AmountKRW, 5000)
	}
}

func TestChatMessageRow_AllMessageTypes(t *testing.T) {
	types := []model.MessageType{
		model.MessageTypeChat,
		model.MessageTypeDonation,
		model.MessageTypeSubscription,
		model.MessageTypeSystem,
	}

	for _, msgType := range types {
		t.Run(string(msgType), func(t *testing.T) {
			msg := baseChatMessage()
			msg.Type = msgType
			row := ChatMessageRowFrom(msg, "worker-1")
			if row.MessageType != string(msgType) {
				t.Errorf("MessageType = %q, want %q", row.MessageType, string(msgType))
			}
		})
	}
}

func TestChatMessageRow_ZeroAmountForNonDonation(t *testing.T) {
	nonDonationTypes := []model.MessageType{
		model.MessageTypeChat,
		model.MessageTypeSubscription,
		model.MessageTypeSystem,
	}

	for _, msgType := range nonDonationTypes {
		t.Run(string(msgType), func(t *testing.T) {
			msg := baseChatMessage()
			msg.Type = msgType
			// Amount is not set (zero value)
			row := ChatMessageRowFrom(msg, "worker-1")
			if row.Amount != 0 {
				t.Errorf("Amount = %v for %q, want 0", row.Amount, msgType)
			}
			if row.AmountKRW != 0 {
				t.Errorf("AmountKRW = %d for %q, want 0", row.AmountKRW, msgType)
			}
		})
	}
}

func TestChatMessageRow_AllPlatforms(t *testing.T) {
	platforms := []model.Platform{
		model.PlatformChzzk,
		model.PlatformSoop,
		model.PlatformCime,
	}

	for _, platform := range platforms {
		t.Run(string(platform), func(t *testing.T) {
			msg := baseChatMessage()
			msg.Platform = platform
			row := ChatMessageRowFrom(msg, "worker-1")
			if row.Platform != string(platform) {
				t.Errorf("Platform = %q, want %q", row.Platform, string(platform))
			}
			if !strings.HasPrefix(row.MessageID, string(platform)+"-") {
				t.Errorf("MessageID %q does not start with platform prefix %q", row.MessageID, string(platform)+"-")
			}
		})
	}
}
