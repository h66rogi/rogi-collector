// Package collection contains the single-channel product configuration, not game rules.
package collection

import (
	"errors"
	"regexp"
	"strings"
)

var channelID = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_-]{0,99}$`)

// Channel uses the existing allowlist input, but rejects wider configurations.
// An empty input explicitly disables collection.
func Channel(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 2 || parts[0] != "soop" || !channelID.MatchString(parts[1]) {
		return "", errors.New("CHANNEL_ALLOWLIST must contain exactly one soop channel")
	}
	return parts[1], nil
}

// Anonymous test mode is explicit and never a fallback from failed authentication.
func AnonymousTest(mode string) (bool, error) {
	switch mode {
	case "", "cookie":
		return false, nil
	case "anonymous-test":
		return true, nil
	default:
		return false, errors.New("SOOP_AUTH_MODE must be cookie or anonymous-test")
	}
}
