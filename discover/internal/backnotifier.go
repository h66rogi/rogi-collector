package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// BackNotifier sends optional channel-end callbacks to an operator-supplied URL.
type BackNotifier struct {
	targetURL  string
	apiKey     string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewBackNotifier creates a BackNotifier. It returns nil unless both a complete
// callback URL and API key are configured.
func NewBackNotifier(targetURL, apiKey string, logger *slog.Logger) *BackNotifier {
	if targetURL == "" || apiKey == "" {
		return nil
	}
	parsed, err := url.Parse(targetURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		logger.Warn("channel-end callback disabled: END_CALLBACK_URL must be an HTTPS URL without user info")
		return nil
	}
	return &BackNotifier{
		targetURL: targetURL,
		apiKey:    apiKey,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: logger,
	}
}

type channelEndNotification struct {
	Platform   string   `json:"platform"`
	ChannelIDs []string `json:"channelIds"`
}

// Notify sends a force-end request for the given ended channel IDs.
// Errors are logged but not propagated — webhook failure must not block discovery.
func (n *BackNotifier) Notify(ctx context.Context, platform model.Platform, endedChannelIDs []string) {
	if len(endedChannelIDs) == 0 {
		return
	}

	body, err := json.Marshal(channelEndNotification{
		Platform:   string(platform),
		ChannelIDs: endedChannelIDs,
	})
	if err != nil {
		n.logger.Error("backnotifier: failed to marshal request", "error", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.targetURL, bytes.NewReader(body))
	if err != nil {
		n.logger.Error("backnotifier: failed to create request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Api-Key", n.apiKey)

	resp, err := n.httpClient.Do(req)
	if err != nil {
		n.logger.Error("backnotifier: request failed", "platform", platform, "count", len(endedChannelIDs), "error", err)
		return
	}
	defer func() {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			n.logger.Warn("backnotifier: drain response failed", "error", err)
		}
		if err := resp.Body.Close(); err != nil {
			n.logger.Warn("backnotifier: close response failed", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		n.logger.Error("backnotifier: unexpected status",
			"platform", platform, "count", len(endedChannelIDs),
			"status", resp.StatusCode)
		return
	}

	n.logger.Info("backnotifier: force-end sent", "platform", platform, "count", len(endedChannelIDs))
}
