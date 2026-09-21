package internal

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/h66rogi/rogi-collector/shared/soopauth"
)

// NewBroadcastDiagnostics reuses discover's cookie boundary for operator lookups.
// The bounded cache and one in-flight request protect the upstream player API.
func NewBroadcastDiagnostics(token string, client soopauth.HTTPDoer) http.Handler {
	var mu sync.Mutex
	cache := map[string]soopauth.BroadcastInfo{}
	var next time.Time
	busy := false
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		supplied := r.Header.Get("Authorization")
		if len(token) < 32 || subtle.ConstantTimeCompare([]byte(supplied), []byte("Bearer "+token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var input struct {
			ChannelID string `json:"channelId"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 512))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || !soopauth.ValidChannelID(input.ChannelID) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		now := time.Now()
		mu.Lock()
		if previous, ok := cache[input.ChannelID]; ok && now.Sub(previous.CheckedAt) < 10*time.Second {
			mu.Unlock()
			previous.Cached = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(previous)
			return
		}
		if busy || now.Before(next) {
			mu.Unlock()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		busy = true
		next = now.Add(time.Second)
		mu.Unlock()
		defer func() { mu.Lock(); busy = false; mu.Unlock() }()
		ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
		defer cancel()
		result, err := soopauth.CheckBroadcast(ctx, client, input.ChannelID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		if len(cache) >= 64 {
			clear(cache)
		}
		cache[input.ChannelID] = result
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
}
