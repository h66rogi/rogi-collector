package main

import "testing"

func TestConstantTimeKeyMatch(t *testing.T) {
	t.Parallel()

	if !constantTimeKeyMatch("test-key", "test-key") {
		t.Fatal("matching keys should be accepted")
	}
	for _, pair := range [][2]string{{"wrong", "test-key"}, {"", "test-key"}, {"anything", ""}} {
		if constantTimeKeyMatch(pair[0], pair[1]) {
			t.Fatalf("unexpected match for provided=%q expected=%q", pair[0], pair[1])
		}
	}
}
