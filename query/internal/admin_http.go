package internal

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/store"
)

const (
	maxAdminRequestBytes = 1 << 20
	maxOptOutBatchItems  = 500
	maxAdminListLimit    = 500
	maxAdminOffset       = 1_000_000
	maxPlatformLength    = 32
	maxChannelIDLength   = 256
	maxTitleLength       = 200
	maxMetadataLength    = 1_000
	maxEmailLength       = 320
	maxActorLength       = 200
)

type collectionOptOutAdminHandler struct {
	pgStore    *store.PgStore
	redisStore *store.RedisStore
	apiKey     string
	logger     *slog.Logger
}

type collectionOptOutItemPayload struct {
	Platform       string  `json:"platform"`
	ChannelID      string  `json:"channelId"`
	StreamerName   *string `json:"streamerName,omitempty"`
	RequesterEmail *string `json:"requesterEmail,omitempty"`
	Reason         *string `json:"reason,omitempty"`
}

type createCollectionOptOutBatchRequest struct {
	Title         string                        `json:"title"`
	RequestSource *string                       `json:"requestSource,omitempty"`
	Reason        *string                       `json:"reason,omitempty"`
	RequestedBy   *string                       `json:"requestedBy,omitempty"`
	Items         []collectionOptOutItemPayload `json:"items"`
}

type revokeCollectionOptOutRequest struct {
	Platform  string  `json:"platform"`
	ChannelID string  `json:"channelId"`
	Reason    *string `json:"reason,omitempty"`
}

func RegisterCollectionOptOutAdminHandlers(mux *http.ServeMux, pgStore *store.PgStore, redisStore *store.RedisStore, apiKey string, logger *slog.Logger) {
	h := &collectionOptOutAdminHandler{
		pgStore:    pgStore,
		redisStore: redisStore,
		apiKey:     apiKey,
		logger:     logger.With("component", "collection-opt-out-admin"),
	}

	mux.HandleFunc("/admin/collection-opt-outs", h.handleOptOuts)
	mux.HandleFunc("/admin/collection-opt-outs/active", h.handleActiveOptOuts)
	mux.HandleFunc("/admin/collection-opt-outs/batches", h.handleBatches)
	mux.HandleFunc("/admin/collection-opt-outs/revoke", h.handleRevoke)
}

func (h *collectionOptOutAdminHandler) handleOptOuts(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	activeRaw := r.URL.Query().Get("active")
	var active *bool
	if activeRaw != "" {
		parsed, err := strconv.ParseBool(activeRaw)
		if err != nil {
			http.Error(w, "invalid active", http.StatusBadRequest)
			return
		}
		active = &parsed
	}

	items, total, err := h.pgStore.ListCollectionOptOuts(r.Context(), store.ListCollectionOptOutsArgs{
		Active:   active,
		Platform: normalizePlatform(r.URL.Query().Get("platform")),
		Query:    r.URL.Query().Get("q"),
		Limit:    intQuery(r, "limit", 100, 1, maxAdminListLimit),
		Offset:   intQuery(r, "offset", 0, 0, maxAdminOffset),
	})
	if err != nil {
		h.logger.Error("list opt-outs failed", "error", err)
		http.Error(w, "list opt-outs failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (h *collectionOptOutAdminHandler) handleActiveOptOuts(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	items, err := h.pgStore.ListActiveCollectionOptOuts(r.Context())
	if err != nil {
		h.logger.Error("list active opt-outs failed", "error", err)
		http.Error(w, "list active opt-outs failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

func (h *collectionOptOutAdminHandler) handleBatches(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		batches, total, err := h.pgStore.ListCollectionOptOutBatches(
			r.Context(),
			intQuery(r, "limit", 50, 1, maxAdminListLimit),
			intQuery(r, "offset", 0, 0, maxAdminOffset),
		)
		if err != nil {
			h.logger.Error("list batches failed", "error", err)
			http.Error(w, "list batches failed", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": batches, "total": total})
	case http.MethodPost:
		h.createBatch(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *collectionOptOutAdminHandler) createBatch(w http.ResponseWriter, r *http.Request) {
	var payload createCollectionOptOutBatchRequest
	if !decodeAdminJSON(w, r, &payload) {
		return
	}

	title := strings.TrimSpace(payload.Title)
	if title == "" || exceedsRuneLimit(title, maxTitleLength) {
		http.Error(w, "invalid title", http.StatusBadRequest)
		return
	}
	if len(payload.Items) == 0 || len(payload.Items) > maxOptOutBatchItems {
		http.Error(w, "items must contain between 1 and 500 entries", http.StatusBadRequest)
		return
	}
	if !validOptionalString(payload.RequestSource, maxMetadataLength) ||
		!validOptionalString(payload.Reason, maxMetadataLength) ||
		!validOptionalString(payload.RequestedBy, maxMetadataLength) {
		http.Error(w, "invalid metadata", http.StatusBadRequest)
		return
	}

	actor := strings.TrimSpace(r.Header.Get("X-Admin-Actor"))
	if exceedsRuneLimit(actor, maxActorLength) {
		http.Error(w, "invalid actor", http.StatusBadRequest)
		return
	}
	var createdBy *string
	if actor != "" {
		createdBy = &actor
	}

	items := make([]store.CollectionOptOutInput, 0, len(payload.Items))
	for _, raw := range payload.Items {
		platform := normalizePlatform(raw.Platform)
		channelID := strings.TrimSpace(raw.ChannelID)
		if !platform.IsValid() || exceedsRuneLimit(string(platform), maxPlatformLength) ||
			channelID == "" || exceedsRuneLimit(channelID, maxChannelIDLength) ||
			!validOptionalString(raw.StreamerName, maxMetadataLength) ||
			!validOptionalString(raw.RequesterEmail, maxEmailLength) ||
			!validOptionalString(raw.Reason, maxMetadataLength) {
			http.Error(w, "invalid item", http.StatusBadRequest)
			return
		}
		items = append(items, store.CollectionOptOutInput{
			Platform:       platform,
			ChannelID:      channelID,
			StreamerName:   cleanedStringPtr(raw.StreamerName),
			RequesterEmail: cleanedStringPtr(raw.RequesterEmail),
			Reason:         cleanedStringPtr(firstStringPtr(raw.Reason, payload.Reason)),
		})
	}
	batch, optOuts, err := h.pgStore.CreateCollectionOptOutBatch(r.Context(), store.CreateCollectionOptOutBatchInput{
		Title:         title,
		RequestSource: cleanedStringPtr(payload.RequestSource),
		Reason:        cleanedStringPtr(payload.Reason),
		RequestedBy:   cleanedStringPtr(payload.RequestedBy),
		CreatedBy:     createdBy,
		Items:         items,
	})
	if err != nil {
		h.logger.Error("create opt-out batch failed", "error", err)
		http.Error(w, "create opt-out batch failed", http.StatusInternalServerError)
		return
	}

	disconnected := h.endAndDisconnect(r, optOuts)
	writeJSON(w, http.StatusCreated, map[string]any{
		"batch":        batch,
		"items":        optOuts,
		"disconnected": disconnected,
	})
}

func (h *collectionOptOutAdminHandler) handleRevoke(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload revokeCollectionOptOutRequest
	if !decodeAdminJSON(w, r, &payload) {
		return
	}
	platform := normalizePlatform(payload.Platform)
	channelID := strings.TrimSpace(payload.ChannelID)
	if !platform.IsValid() || exceedsRuneLimit(string(platform), maxPlatformLength) ||
		channelID == "" || exceedsRuneLimit(channelID, maxChannelIDLength) ||
		!validOptionalString(payload.Reason, maxMetadataLength) {
		http.Error(w, "invalid target", http.StatusBadRequest)
		return
	}

	actor := strings.TrimSpace(r.Header.Get("X-Admin-Actor"))
	if exceedsRuneLimit(actor, maxActorLength) {
		http.Error(w, "invalid actor", http.StatusBadRequest)
		return
	}
	var revokedBy *string
	if actor != "" {
		revokedBy = &actor
	}
	rows, err := h.pgStore.RevokeCollectionOptOut(r.Context(), platform, channelID, revokedBy, cleanedStringPtr(payload.Reason))
	if err != nil {
		h.logger.Error("revoke opt-out failed", "error", err)
		http.Error(w, "revoke opt-out failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": rows > 0, "affected": rows})
}

func (h *collectionOptOutAdminHandler) endAndDisconnect(r *http.Request, items []model.CollectionOptOut) int {
	disconnected := 0
	for _, item := range items {
		workerID, err := h.pgStore.EndCollectionOptedOutLiveChannel(r.Context(), item.Platform, item.ChannelID)
		if err != nil {
			h.logger.Warn("end live channel failed",
				"platform", item.Platform, "channel", item.ChannelID, "error", err)
			continue
		}
		if h.redisStore != nil {
			if _, err := h.redisStore.DeleteChatStreams(r.Context(), []string{store.ChatStreamKey(string(item.Platform), item.ChannelID)}); err != nil {
				h.logger.Warn("delete opt-out stream failed",
					"platform", item.Platform, "channel", item.ChannelID, "error", err)
			}
		}
		if workerID == nil || *workerID == "" || h.redisStore == nil {
			continue
		}
		cmd := store.WorkerCommand{
			Type:      store.WorkerCommandDisconnect,
			Platform:  item.Platform,
			ChannelID: item.ChannelID,
		}
		if err := h.redisStore.PublishWorkerCommand(r.Context(), *workerID, cmd); err != nil {
			h.logger.Warn("publish opt-out disconnect failed",
				"platform", item.Platform, "channel", item.ChannelID, "worker", *workerID, "error", err)
			continue
		}
		disconnected++
	}
	return disconnected
}

func (h *collectionOptOutAdminHandler) authorize(w http.ResponseWriter, r *http.Request) bool {
	if h.apiKey == "" {
		http.Error(w, "admin api key not configured", http.StatusServiceUnavailable)
		return false
	}
	provided := r.Header.Get("X-Internal-Api-Key")
	if len(provided) != len(h.apiKey) || subtle.ConstantTimeCompare([]byte(provided), []byte(h.apiKey)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func normalizePlatform(value string) model.Platform {
	return model.Platform(strings.ToLower(strings.TrimSpace(value)))
}

func intQuery(r *http.Request, key string, fallback, minValue, maxValue int) int {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < minValue || parsed > maxValue {
		return fallback
	}
	return parsed
}

func decodeAdminJSON(w http.ResponseWriter, r *http.Request, dest any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return false
		}
		http.Error(w, "invalid json", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return false
	}
	return true
}

func validOptionalString(value *string, limit int) bool {
	return value == nil || !exceedsRuneLimit(strings.TrimSpace(*value), limit)
}

func exceedsRuneLimit(value string, limit int) bool {
	return utf8.RuneCountInString(value) > limit
}

func cleanedStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	cleaned := strings.TrimSpace(*value)
	if cleaned == "" {
		return nil
	}
	return &cleaned
}

func firstStringPtr(values ...*string) *string {
	for _, value := range values {
		if cleaned := cleanedStringPtr(value); cleaned != nil {
			return cleaned
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
