package main

import (
	"log/slog"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func TestAllowlistNeverDefaultsToAllChannels(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	for _, value := range []string{"", " ", "malformed", "unknown:synthetic-channel"} {
		got := parseAllowlist(value, logger)
		if got == nil || len(got) != 0 {
			t.Fatalf("%q expanded to unfiltered discovery", value)
		}
	}
	got := parseAllowlist("soop:synthetic-a,soop:synthetic-b", logger)
	if len(got[model.PlatformSoop]) != 2 {
		t.Fatal(got)
	}
}
