package internal

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/diagnostics"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
	"github.com/h66rogi/rogi-collector/worker/internal/connector"
)

type chatTestSession struct {
	status diagnostics.ChatTestStatus
	cancel context.CancelFunc
	done   chan struct{}
}

// ChatTestDiagnostics uses the existing connector without a publisher, manager,
// donation sink or store. Only one bounded, ephemeral test connection is allowed.
type ChatTestDiagnostics struct {
	// Set once at startup to reuse the worker's existing opt-out policy.
	IsOptedOut                    func(string) bool
	mu                            sync.Mutex
	root                          context.Context
	token                         string
	factory                       func() connector.PlatformConnector
	current                       *chatTestSession
	cleanup                       *time.Timer
	nextStart                     time.Time
	lifetime, retention, cooldown time.Duration
}

func NewChatTestDiagnostics(ctx context.Context, token string, factory func() connector.PlatformConnector) *ChatTestDiagnostics {
	return &ChatTestDiagnostics{root: ctx, token: token, factory: factory, lifetime: 2 * time.Minute, retention: 5 * time.Minute, cooldown: 5 * time.Second}
}
func (m *ChatTestDiagnostics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if len(m.token) < 32 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+m.token)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var input struct {
		Action          string `json:"action"`
		TargetChannelID string `json:"targetChannelId"`
		SessionID       string `json:"sessionId"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 512))
	dec.DisallowUnknownFields()
	if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	validID := func(id string) bool { parsed, err := uuid.Parse(id); return err == nil && parsed.String() == id }
	if input.Action != "start" && input.Action != "status" && input.Action != "stop" || input.SessionID != "" && !validID(input.SessionID) || input.Action != "status" && input.SessionID == "" || input.Action == "start" && !soopauth.ValidChannelID(input.TargetChannelID) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	current := m.current
	if input.Action == "start" {
		if m.IsOptedOut != nil && m.IsOptedOut(input.TargetChannelID) {
			m.mu.Unlock()
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if m.root.Err() != nil {
			m.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if current != nil && current.status.SessionID == input.SessionID {
			if current.status.ChannelID != input.TargetChannelID {
				m.mu.Unlock()
				w.WriteHeader(http.StatusConflict)
				return
			}
		} else {
			if current != nil {
				select {
				case <-current.done:
				default:
					m.mu.Unlock()
					w.WriteHeader(http.StatusConflict)
					return
				}
			}
			now := time.Now().UTC()
			if now.Before(m.nextStart) {
				m.mu.Unlock()
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			expires := now.Add(m.lifetime)
			ctx, cancel := context.WithDeadline(m.root, expires)
			current = &chatTestSession{status: diagnostics.ChatTestStatus{SessionID: input.SessionID, ChannelID: input.TargetChannelID, State: "connecting", Active: true, StartedAt: &now, ExpiresAt: &expires, Messages: []diagnostics.ChatSample{}}, cancel: cancel, done: make(chan struct{})}
			if m.cleanup != nil {
				m.cleanup.Stop()
				m.cleanup = nil
			}
			m.current = current
			m.nextStart = now.Add(m.cooldown)
			go m.run(ctx, current)
		}
	} else if input.SessionID != "" && (current == nil || current.status.SessionID != input.SessionID) {
		m.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if input.Action == "stop" && current != nil && current.status.Active {
		current.status.State = "stopping"
		current.cancel()
		done := current.done
		m.mu.Unlock()
		select {
		case <-done:
		case <-r.Context().Done():
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		m.mu.Lock()
	}
	result := diagnostics.ChatTestStatus{State: "idle", Messages: []diagnostics.ChatSample{}}
	if current != nil {
		result = current.status
		result.Messages = append([]diagnostics.ChatSample{}, current.status.Messages...)
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
func (m *ChatTestDiagnostics) run(ctx context.Context, s *chatTestSession) {
	conn := m.factory()
	state := "disconnected"
	defer func() {
		// Disconnect completes before another test can take the single active slot.
		_ = conn.Disconnect()
		s.cancel()
		m.mu.Lock()
		s.status.Active = false
		s.status.State = state
		if m.root.Err() == nil {
			m.cleanup = time.AfterFunc(m.retention, func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.current == s {
					m.current = nil
					m.cleanup = nil
				}
			})
		} else if m.current == s {
			m.current = nil
		}
		close(s.done)
		m.mu.Unlock()
	}()
	if err := conn.Connect(ctx, model.LiveChannel{Platform: model.PlatformSoop, ChannelID: s.status.ChannelID}); err != nil {
		state = "failed"
		switch {
		case errors.Is(err, soopauth.ErrOffline):
			state = "offline"
		case errors.Is(err, soopauth.ErrCookieUnavailable):
			state = "cookie_required"
		case errors.Is(err, soopauth.ErrLoginRequired):
			state = "auth_required"
		}
		if ctx.Err() != nil {
			state = chatTestEndState(ctx)
		}
		return
	}
	m.mu.Lock()
	joined := time.Now().UTC()
	s.status.JoinedAt = &joined
	if ctx.Err() == nil {
		s.status.State = "joined"
	}
	m.mu.Unlock()
	policyTick := time.NewTicker(time.Second)
	defer policyTick.Stop()
	for {
		select {
		case <-policyTick.C:
			if m.IsOptedOut != nil && m.IsOptedOut(s.status.ChannelID) {
				m.mu.Lock()
				s.status.Messages = nil
				m.mu.Unlock()
				state = "blocked"
				return
			}
		case <-ctx.Done():
			state = chatTestEndState(ctx)
			return
		case <-conn.Errors():
			return
		case msg, ok := <-conn.Messages():
			if !ok {
				return
			}
			if msg.Type != model.MessageTypeChat || msg.ChannelID != s.status.ChannelID {
				continue
			}
			now := time.Now().UTC()
			m.mu.Lock()
			if ctx.Err() == nil {
				s.status.ReceivedCount++
				s.status.LastReceivedAt = &now
				s.status.State = "receiving"
				sample := diagnostics.ChatSample{Sequence: s.status.ReceivedCount, DisplayName: chatTestText(msg.Nickname, 80), Message: chatTestText(msg.Message, 1000), ReceivedAt: now}
				if len(s.status.Messages) == 20 {
					copy(s.status.Messages, s.status.Messages[1:])
					s.status.Messages = s.status.Messages[:19]
				}
				s.status.Messages = append(s.status.Messages, sample)
			}
			m.mu.Unlock()
		}
	}
}
func chatTestEndState(ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "expired"
	}
	return "stopped"
}
func chatTestText(text string, limit int) string {
	result := make([]rune, 0, limit)
	for _, r := range text {
		if unicode.IsControl(r) && r != '\n' {
			continue
		}
		result = append(result, r)
		if len(result) == limit {
			break
		}
	}
	return string(result)
}
