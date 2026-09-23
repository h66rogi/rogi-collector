// Package publicapi contains the anonymous, read-only HTTP surface. It is kept
// separate from the private collector gRPC service and has no write RPCs.
package publicapi

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	pb "github.com/h66rogi/rogi-collector/proto/gen/collector/v1"
	"github.com/h66rogi/rogi-collector/shared/store"
)

type CollectionReader interface {
	CollectionState(context.Context, string) (store.CollectionState, error)
	CollectionEnabled(context.Context, string) (bool, error)
}

type ChatReader interface {
	ReadProductChat(context.Context, string, string) (store.ChatBatch, error)
	ReadLatestProductChat(context.Context, string) (store.ChatBatch, error)
}

type BroadcastChecker func(context.Context, string) (*pb.BroadcastStatus, error)

type Server struct {
	channel         string
	collection      CollectionReader
	chat            ChatReader
	check           BroadcastChecker
	logger          *slog.Logger
	mu              sync.Mutex
	status          *pb.BroadcastStatus
	statusUntil     time.Time
	statusWait      chan struct{}
	connections     chan struct{}
	connectionMu    sync.Mutex
	connectionsByIP map[string]int
	history         *historyAccess
	historyReads    chan struct{}
	rateMu          sync.Mutex
	rateTokens      float64
	rateAt          time.Time
	readyMu         sync.Mutex
	readyUntil      time.Time
	readyStatus     int
	readyWait       chan struct{}
}

func New(channel string, collection CollectionReader, chat ChatReader, check BroadcastChecker, logger *slog.Logger, maxConnections int) (*Server, error) {
	if channel == "" || collection == nil || chat == nil || check == nil || logger == nil || maxConnections < 1 {
		return nil, errors.New("public API dependencies and positive connection limit required")
	}
	return &Server{channel: channel, collection: collection, chat: chat, check: check, logger: logger,
		connections: make(chan struct{}, maxConnections), connectionsByIP: make(map[string]int),
		historyReads: make(chan struct{}, 1),
		rateTokens:   60, rateAt: time.Now()}, nil
}

func clientIP(r *http.Request) string {
	if ip := net.ParseIP(r.Header.Get("CF-Connecting-IP")); ip != nil {
		return ip.String()
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ip.String()
		}
	}
	return "unknown"
}

func internalReadiness(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	return err == nil && net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}

// A single bounded token bucket protects the small public origin from
// accidental or abusive HTTP polling. WebSocket connections have a separate
// concurrent limit and consume only a token for their handshake.
func (s *Server) allowRequest() bool {
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	now := time.Now()
	s.rateTokens += now.Sub(s.rateAt).Seconds() * 20
	if s.rateTokens > 60 {
		s.rateTokens = 60
	}
	s.rateAt = now
	if s.rateTokens < 1 {
		return false
	}
	s.rateTokens--
	return true
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /ready", s.ready)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /v1/broadcasts/current", s.current)
	mux.HandleFunc("GET /v1/chats/recent", s.recent)
	mux.HandleFunc("GET /v1/chat/stream", s.stream)
	mux.HandleFunc("GET /v1/broadcasts", s.broadcasts)
	mux.HandleFunc("GET /v1/broadcasts/{sessionId}/chats", s.broadcastChats)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		w.Header().Set("Access-Control-Allow-Origin", "*")
		observed := &observedWriter{ResponseWriter: w, status: http.StatusOK}
		healthPath := r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/ready"
		if !(healthPath && internalReadiness(r)) && !s.allowRequest() {
			observed.Header().Set("Retry-After", "1")
			writeJSON(observed, http.StatusTooManyRequests, map[string]string{"code": "rate_limited"})
		} else {
			mux.ServeHTTP(observed, r)
		}
		pattern := r.Pattern
		if pattern == "" {
			pattern = "unmatched"
		}
		clientIP := net.ParseIP(r.Header.Get("CF-Connecting-IP"))
		if clientIP != nil {
			s.logger.Info("access", "method", r.Method, "route", pattern, "status", observed.status, "durationMs", time.Since(started).Milliseconds(), "clientIP", clientIP.String())
		} else {
			s.logger.Info("access", "method", r.Method, "route", pattern, "status", observed.status, "durationMs", time.Since(started).Milliseconds(), "remoteAddr", r.RemoteAddr)
		}
	})
}

type observedWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *observedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *observedWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.status, w.wrote = status, true
	w.ResponseWriter.WriteHeader(status)
}
func (w *observedWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}
func (w *observedWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (w *observedWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	conn, rw, err := h.Hijack()
	if err == nil {
		w.status, w.wrote = http.StatusSwitchingProtocols, true
	}
	return conn, rw, err
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func archiveUnavailable(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "archive_unavailable"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	for {
		s.readyMu.Lock()
		if time.Now().Before(s.readyUntil) {
			status := s.readyStatus
			s.readyMu.Unlock()
			s.writeReady(w, status)
			return
		}
		if wait := s.readyWait; wait != nil {
			s.readyMu.Unlock()
			select {
			case <-wait:
				continue
			case <-r.Context().Done():
				return
			}
		}
		wait := make(chan struct{})
		s.readyWait = wait
		s.readyMu.Unlock()
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		status := http.StatusOK
		if _, err := s.collection.CollectionState(ctx, s.channel); err != nil {
			status = http.StatusServiceUnavailable
		} else if _, err := s.chat.ReadProductChat(ctx, s.channel, "0-0"); err != nil {
			status = http.StatusServiceUnavailable
		}
		cancel()
		s.readyMu.Lock()
		s.readyStatus = status
		s.readyUntil = time.Now().Add(2 * time.Second)
		s.readyWait = nil
		close(wait)
		s.readyMu.Unlock()
		s.writeReady(w, status)
		return
	}
}

func (s *Server) writeReady(w http.ResponseWriter, status int) {
	if status == http.StatusOK {
		writeJSON(w, status, map[string]string{"status": "ready"})
	} else {
		writeJSON(w, status, map[string]string{"status": "not_ready"})
	}
}

// checkBroadcast shares a single 10-second cache and in-flight call. The
// upstream SOOP diagnostic has its own rate limit, so public requests cannot
// each trigger a platform lookup.
func (s *Server) checkBroadcast(ctx context.Context) (*pb.BroadcastStatus, error) {
	for {
		s.mu.Lock()
		if s.status != nil && time.Now().Before(s.statusUntil) {
			value := s.status
			s.mu.Unlock()
			return value, nil
		}
		if wait := s.statusWait; wait != nil {
			s.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		wait := make(chan struct{})
		s.statusWait = wait
		s.mu.Unlock()
		value, err := s.check(ctx, s.channel)
		s.mu.Lock()
		if err == nil && value != nil {
			s.status = value
			s.statusUntil = time.Now().Add(10 * time.Second)
		}
		s.statusWait = nil
		close(wait)
		s.mu.Unlock()
		return value, err
	}
}

func (s *Server) current(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	broadcast, err := s.checkBroadcast(ctx)
	if err != nil || broadcast == nil || broadcast.CheckedAt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "status_unavailable"})
		return
	}
	collection, err := s.collection.CollectionState(ctx, s.channel)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "collection_unavailable"})
		return
	}
	enabled, err := s.collection.CollectionEnabled(ctx, s.channel)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"code": "collection_unavailable"})
		return
	}
	var live *bool
	if broadcast.State == "live" || broadcast.State == "offline" {
		value := broadcast.State == "live"
		live = &value
	}
	var lastReceived *time.Time
	if collection.LastReceived != nil {
		lastReceived = collection.LastReceived
	}
	writeJSON(w, 200, map[string]any{
		"channelId": s.channel, "live": live, "state": broadcast.State,
		"checkedAt": broadcast.CheckedAt.AsTime(), "broadcastId": broadcast.BroadcastId,
		"title": broadcast.Title, "collection": map[string]any{
			"active": enabled && collection.Runtime == "connected" && time.Since(collection.StateAt) <= 35*time.Second,
			"state":  collection.Runtime, "lastReceivedAt": lastReceived,
		},
	})
}

type chatCursor struct {
	Generation string `json:"g"`
	ID         string `json:"i"`
}

func encodeCursor(value chatCursor) string {
	body, _ := json.Marshal(value)
	return base64.RawURLEncoding.EncodeToString(body)
}
func decodeCursor(raw string) (chatCursor, error) {
	var cursor chatCursor
	if raw == "" {
		return cursor, nil
	}
	if len(raw) > 512 {
		return cursor, errors.New("cursor too long")
	}
	body, err := base64.RawURLEncoding.DecodeString(raw)
	if err == nil {
		err = json.Unmarshal(body, &cursor)
	}
	if err != nil || cursor.Generation == "" || !store.ValidStreamID(cursor.ID) {
		return chatCursor{}, errors.New("invalid cursor")
	}
	return cursor, nil
}

func chatMessages(batch store.ChatBatch, after string) ([]map[string]any, string, bool) {
	events := make([]map[string]any, 0, len(batch.Messages))
	invalid := false
	for _, item := range batch.Messages {
		after = item.StreamID
		value := func(key string) string { v, _ := item.Values[key].(string); return v }
		if value("type") != "chat" || value("userId") == "" {
			continue
		}
		timestamp, err := time.Parse(time.RFC3339Nano, value("timestamp"))
		if err != nil {
			invalid = true
			continue
		}
		events = append(events, map[string]any{
			"type": "chat.message", "eventId": value("id"), "cursor": encodeCursor(chatCursor{batch.Generation, item.StreamID}),
			"receivedAt": timestamp, "user": map[string]string{"displayName": value("nickname")}, "message": value("message"),
		})
	}
	return events, after, invalid
}

func (s *Server) recent(w http.ResponseWriter, r *http.Request) {
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "invalid_cursor"})
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeJSON(w, 400, map[string]string{"code": "invalid_limit"})
			return
		}
	}
	after := cursor.ID
	if after == "" {
		after = "0-0"
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	var batch store.ChatBatch
	if cursor.ID == "" {
		batch, err = s.chat.ReadLatestProductChat(ctx, s.channel)
	} else {
		batch, err = s.chat.ReadProductChat(ctx, s.channel, after)
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"code": "chat_unavailable"})
		return
	}
	if batch.Generation == "" && len(batch.Messages) > 0 {
		writeJSON(w, 503, map[string]string{"code": "chat_generation_unavailable"})
		return
	}
	if cursor.Generation != "" && cursor.Generation != batch.Generation {
		writeJSON(w, 409, map[string]string{"code": "cursor_expired"})
		return
	}
	if cursor.ID != "" && batch.Earliest != "" && store.StreamIDBefore(cursor.ID, batch.Earliest) {
		writeJSON(w, 409, map[string]string{"code": "cursor_expired"})
		return
	}
	events, nextRaw, invalid := chatMessages(batch, after)
	if invalid {
		writeJSON(w, 503, map[string]string{"code": "chat_data_invalid"})
		return
	}
	if len(events) > limit {
		if cursor.ID == "" {
			events = events[len(events)-limit:]
		} else {
			events = events[:limit]
		}
	}
	var next any
	if len(events) > 0 {
		next = events[len(events)-1]["cursor"]
	} else if nextRaw != after && batch.Generation != "" {
		next = encodeCursor(chatCursor{batch.Generation, nextRaw})
	}
	writeJSON(w, 200, map[string]any{"messages": events, "nextCursor": next, "retention": "up to 24 hours and 10000 stream entries"})
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "invalid_cursor"})
		return
	}
	select {
	case s.connections <- struct{}{}:
		defer func() { <-s.connections }()
	default:
		writeJSON(w, 503, map[string]string{"code": "connection_limit"})
		return
	}
	ip := clientIP(r)
	s.connectionMu.Lock()
	if s.connectionsByIP[ip] >= 2 {
		s.connectionMu.Unlock()
		writeJSON(w, 503, map[string]string{"code": "connection_limit"})
		return
	}
	s.connectionsByIP[ip]++
	s.connectionMu.Unlock()
	defer func() {
		s.connectionMu.Lock()
		s.connectionsByIP[ip]--
		if s.connectionsByIP[ip] == 0 {
			delete(s.connectionsByIP, ip)
		}
		s.connectionMu.Unlock()
	}()
	// This endpoint is anonymous and read-only. Browser clients on other origins
	// need to connect; authentication will require a separate origin policy.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Hour)
	defer cancel()
	ctx = conn.CloseRead(ctx)
	after := cursor.ID
	if after == "" {
		after = "0-0"
	}
	initial, err := s.chat.ReadProductChat(ctx, s.channel, after)
	if err != nil {
		_ = wsjson.Write(ctx, conn, map[string]string{"type": "error", "code": "chat_unavailable"})
		return
	}
	if initial.Generation == "" && len(initial.Messages) > 0 {
		_ = wsjson.Write(ctx, conn, map[string]string{"type": "error", "code": "chat_generation_unavailable"})
		return
	}
	if cursor.ID == "" {
		after = initial.Latest
	}
	if after == "" {
		after = "0-0"
	}
	generation := initial.Generation
	broadcast, checkErr := s.checkBroadcast(ctx)
	broadcastState := "lookup_failed"
	broadcastID := ""
	broadcastTitle := ""
	if checkErr == nil && broadcast != nil {
		broadcastState = broadcast.State
		broadcastID = broadcast.BroadcastId
		broadcastTitle = broadcast.Title
	}
	var currentCursor, earliestCursor any
	if generation != "" && initial.Latest != "" {
		currentCursor = encodeCursor(chatCursor{generation, initial.Latest})
	}
	if generation != "" && initial.Earliest != "" {
		earliestCursor = encodeCursor(chatCursor{generation, initial.Earliest})
	}
	if err = wsjson.Write(ctx, conn, map[string]any{"type": "hello", "version": "v1", "serverTime": time.Now().UTC(), "broadcastState": broadcastState, "broadcastId": broadcastID, "title": broadcastTitle, "currentCursor": currentCursor, "earliestCursor": earliestCursor, "cursorRetention": "up to 24 hours and 10000 stream entries"}); err != nil {
		return
	}
	if cursor.ID != "" && (cursor.Generation != generation || (initial.Earliest != "" && store.StreamIDBefore(cursor.ID, initial.Earliest))) {
		if err = wsjson.Write(ctx, conn, map[string]string{"type": "chat.gap", "reason": "cursor_expired"}); err != nil {
			return
		}
		after = initial.Latest
		if after == "" {
			after = "0-0"
		}
	}
	lastHeartbeat := time.Now()
	lastStatus := time.Now()
	for {
		batch, readErr := s.chat.ReadProductChat(ctx, s.channel, after)
		if readErr != nil {
			_ = wsjson.Write(ctx, conn, map[string]string{"type": "error", "code": "chat_unavailable"})
			return
		}
		if batch.Generation == "" && len(batch.Messages) > 0 {
			_ = wsjson.Write(ctx, conn, map[string]string{"type": "error", "code": "chat_generation_unavailable"})
			return
		}
		if generation != "" && batch.Generation != generation {
			if wsjson.Write(ctx, conn, map[string]string{"type": "chat.gap", "reason": "stream_reset"}) != nil {
				return
			}
			generation = batch.Generation
			after = batch.Latest
			if after == "" {
				after = "0-0"
			}
		} else if after != "0-0" && batch.Earliest != "" && store.StreamIDBefore(after, batch.Earliest) {
			if wsjson.Write(ctx, conn, map[string]string{"type": "chat.gap", "reason": "cursor_expired"}) != nil {
				return
			}
			after = batch.Latest
			if after == "" {
				after = "0-0"
			}
		} else {
			if generation == "" {
				generation = batch.Generation
			}
			events, next, invalid := chatMessages(batch, after)
			if invalid && wsjson.Write(ctx, conn, map[string]string{"type": "chat.gap", "reason": "invalid_event"}) != nil {
				return
			}
			for _, event := range events {
				if wsjson.Write(ctx, conn, event) != nil {
					return
				}
			}
			after = next
		}
		if time.Since(lastHeartbeat) >= 20*time.Second {
			if wsjson.Write(ctx, conn, map[string]any{"type": "heartbeat", "serverTime": time.Now().UTC()}) != nil {
				return
			}
			lastHeartbeat = time.Now()
		}
		if time.Since(lastStatus) >= 10*time.Second {
			value, err := s.checkBroadcast(ctx)
			if err == nil && value != nil && value.CheckedAt != nil && (value.State != broadcastState || value.BroadcastId != broadcastID || value.Title != broadcastTitle) {
				if wsjson.Write(ctx, conn, map[string]any{"type": "broadcast.status", "state": value.State, "broadcastId": value.BroadcastId, "title": value.Title, "checkedAt": value.CheckedAt.AsTime()}) != nil {
					return
				}
				broadcastState = value.State
				broadcastID = value.BroadcastId
				broadcastTitle = value.Title
			}
			lastStatus = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
