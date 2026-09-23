package publicapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/query/archive"
)

type historyAccess struct {
	database  *archive.PgArchive
	objects   *archive.S3ObjectStore
	cursorKey []byte
}

// EnableHistory must be called only after the object-store upload, recovery,
// and gap-quality gates have passed. It is safe to configure before serving.
func (s *Server) EnableHistory(database *archive.PgArchive, objects *archive.S3ObjectStore, cursorKey []byte) error {
	if database == nil || objects == nil || len(cursorKey) < 32 {
		return errors.New("history database, object store and cursor key required")
	}
	s.mu.Lock()
	s.history = &historyAccess{database: database, objects: objects, cursorKey: append([]byte(nil), cursorKey...)}
	s.mu.Unlock()
	return nil
}

func (s *Server) historyAccess() *historyAccess {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.history
}

func pageLimit(raw string, defaultValue, maxValue int) (int, error) {
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maxValue {
		return 0, errors.New("invalid page limit")
	}
	return value, nil
}

func (s *Server) broadcasts(w http.ResponseWriter, r *http.Request) {
	history := s.historyAccess()
	if history == nil {
		archiveUnavailable(w, r)
		return
	}
	limit, err := pageLimit(r.URL.Query().Get("limit"), 20, 100)
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "invalid_limit"})
		return
	}
	cursor, err := parseHistoryCursor(history.cursorKey, r.URL.Query().Get("cursor"), "broadcasts")
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "invalid_cursor"})
		return
	}
	var before *time.Time
	if cursor.Version != 0 {
		if cursor.StartedAt.IsZero() || cursor.SessionID == "" || cursor.Position != 0 {
			writeJSON(w, 400, map[string]string{"code": "invalid_cursor"})
			return
		}
		before = &cursor.StartedAt
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := history.database.ListSessions(ctx, before, cursor.SessionID, limit+1)
	if err != nil {
		writeJSON(w, 503, map[string]string{"code": "archive_unavailable"})
		return
	}
	var next any
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		next = signHistoryCursor(history.cursorKey, historyCursor{Kind: "broadcasts", SessionID: last.SessionID, StartedAt: last.StartedAt})
	}
	if items == nil {
		items = []archive.Session{}
	}
	writeJSON(w, 200, map[string]any{"broadcasts": items, "nextCursor": next})
}

func (s *Server) broadcastChats(w http.ResponseWriter, r *http.Request) {
	history := s.historyAccess()
	if history == nil {
		archiveUnavailable(w, r)
		return
	}
	sessionID := r.PathValue("sessionId")
	if _, err := uuid.Parse(sessionID); err != nil {
		writeJSON(w, 404, map[string]string{"code": "broadcast_not_found"})
		return
	}
	limit, err := pageLimit(r.URL.Query().Get("limit"), 50, 200)
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "invalid_limit"})
		return
	}
	cursor, err := parseHistoryCursor(history.cursorKey, r.URL.Query().Get("cursor"), "chats")
	if err != nil || (cursor.Version != 0 && (cursor.SessionID != sessionID || cursor.Position <= 0 || !cursor.StartedAt.IsZero())) {
		writeJSON(w, 400, map[string]string{"code": "invalid_cursor"})
		return
	}
	select {
	case s.historyReads <- struct{}{}:
		defer func() { <-s.historyReads }()
	default:
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"code": "rate_limited"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	page, err := history.database.ReadChatsPage(ctx, sessionID, cursor.Position, limit, history.objects)
	if errors.Is(err, archive.ErrSessionNotFound) {
		writeJSON(w, 404, map[string]string{"code": "broadcast_not_found"})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"code": "archive_unavailable"})
		return
	}
	chats := make([]map[string]any, 0, len(page.Rows))
	for _, row := range page.Rows {
		chats = append(chats, map[string]any{
			"eventId": row.EventID, "sessionId": row.SessionID, "position": row.Position, "receivedAt": row.ReceivedAt,
			"user": map[string]any{"id": row.PublicUserID, "idVersion": row.UserIDVersion, "displayName": row.DisplayName}, "message": row.Message,
		})
	}
	var next any
	if page.NextPosition != nil {
		next = signHistoryCursor(history.cursorKey, historyCursor{Kind: "chats", SessionID: sessionID, Position: *page.NextPosition})
	}
	writeJSON(w, 200, map[string]any{"sessionId": sessionID, "chats": chats, "nextCursor": next, "complete": false})
}
