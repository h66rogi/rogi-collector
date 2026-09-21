package pipeline

import (
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func TestPublisherCountersInitialZero(t *testing.T) {
	// Cannot test with a real Redis, but verify counters start at zero.
	// NewPublisher with a nil client is safe as long as we don't call Publish.
	p := &Publisher{}

	if p.Published() != 0 {
		t.Errorf("expected published=0, got %d", p.Published())
	}
	if p.Errors() != 0 {
		t.Errorf("expected errors=0, got %d", p.Errors())
	}
}

func TestChatStreamKey(t *testing.T) {
	tests := []struct {
		platform model.Platform
		channel  string
		expected string
	}{
		{model.PlatformChzzk, "ch123", "chat:chzzk:ch123"},
		{model.PlatformSoop, "abc", "chat:soop:abc"},
		{model.PlatformCime, "xyz", "chat:cime:xyz"},
	}

	for _, tt := range tests {
		key := chatStreamKey(tt.platform, tt.channel)
		if key != tt.expected {
			t.Errorf("chatStreamKey(%s, %s) = %s, want %s", tt.platform, tt.channel, key, tt.expected)
		}
	}
}
