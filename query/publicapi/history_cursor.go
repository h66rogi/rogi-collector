package publicapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type historyCursor struct {
	Version   int       `json:"v"`
	Kind      string    `json:"k"`
	SessionID string    `json:"s,omitempty"`
	Position  int64     `json:"p,omitempty"`
	StartedAt time.Time `json:"t,omitempty"`
}

func signHistoryCursor(key []byte, value historyCursor) string {
	value.Version = 1
	body, _ := json.Marshal(value)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func parseHistoryCursor(key []byte, raw, kind string) (historyCursor, error) {
	var value historyCursor
	if raw == "" {
		return value, nil
	}
	if len(raw) > 1024 {
		return value, errors.New("history cursor too long")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return value, errors.New("invalid history cursor")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return value, errors.New("invalid history cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return value, errors.New("invalid history cursor")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), signature) {
		return value, errors.New("invalid history cursor signature")
	}
	if err := json.Unmarshal(body, &value); err != nil || value.Version != 1 || value.Kind != kind {
		return historyCursor{}, errors.New("invalid history cursor payload")
	}
	return value, nil
}
