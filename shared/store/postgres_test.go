package store

import (
	"testing"
)

// Compile-time interface checks - these fail at compile time if PgStore
// does not implement the interfaces.
var _ ChannelStore = (*PgStore)(nil)
var _ WorkerStore = (*PgStore)(nil)

func TestValidChannelStatuses(t *testing.T) {
	valid := []string{"pending", "live", "ended"}
	for _, s := range valid {
		if !IsValidChannelStatus(s) {
			t.Errorf("expected %q to be a valid channel status", s)
		}
	}

	invalid := []string{"unknown", "paused", ""}
	for _, s := range invalid {
		if IsValidChannelStatus(s) {
			t.Errorf("expected %q to be an invalid channel status", s)
		}
	}
}

func TestValidWorkerStatuses(t *testing.T) {
	valid := []string{"alive", "draining", "dead"}
	for _, s := range valid {
		if !IsValidWorkerStatus(s) {
			t.Errorf("expected %q to be a valid worker status", s)
		}
	}

	invalid := []string{"unknown", "sleeping", ""}
	for _, s := range invalid {
		if IsValidWorkerStatus(s) {
			t.Errorf("expected %q to be an invalid worker status", s)
		}
	}
}
