package soopauth

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestPlayerOfflineIsNotAuthFailure(t *testing.T) {
	for _, tc := range []struct {
		result, chat string
		live         bool
		err          error
	}{{"0", "", false, nil}, {"1", "fixture", true, nil}, {"-6", "", false, ErrLoginRequired}, {"-1", "", false, ErrPlayerUnknown}, {"1", "", false, ErrPlayerUnknown}, {"", "", false, ErrPlayerUnknown}} {
		live, err := LiveResult(json.RawMessage(tc.result), tc.chat)
		if live != tc.live || !errors.Is(err, tc.err) {
			t.Fatalf("result %q: live=%v error=%v", tc.result, live, err)
		}
	}
}
