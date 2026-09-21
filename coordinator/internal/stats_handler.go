package internal

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/h66rogi/rogi-collector/shared/store"
)

type platformStatResponse struct {
	Platform            string `json:"platform"`
	LiveChannelCount    int32  `json:"liveChannelCount"`
	PendingChannelCount int32  `json:"pendingChannelCount"`
	EndedChannelCount   int32  `json:"endedChannelCount"`
	TotalViewerCount    int64  `json:"totalViewerCount"`
}

type statsResponse struct {
	Platforms         []platformStatResponse `json:"platforms"`
	TotalLiveChannels int32                  `json:"totalLiveChannels"`
	TotalViewers      int64                  `json:"totalViewers"`
	Timestamp         string                 `json:"timestamp"`
}

// NewStatsHandler returns an HTTP handler that serves platform live stats as JSON.
func NewStatsHandler(pgStore *store.PgStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		stats, err := pgStore.GetPlatformStats(r.Context())
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to get platform stats"})
			return
		}

		resp := statsResponse{
			Platforms: make([]platformStatResponse, 0, len(stats)),
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}

		for _, st := range stats {
			resp.Platforms = append(resp.Platforms, platformStatResponse{
				Platform:            st.Platform,
				LiveChannelCount:    st.LiveCount,
				PendingChannelCount: st.PendingCount,
				EndedChannelCount:   st.EndedCount,
				TotalViewerCount:    st.TotalViewerCount,
			})
			resp.TotalLiveChannels += st.LiveCount
			resp.TotalViewers += st.TotalViewerCount
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
