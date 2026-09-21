package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
)

const (
	chzzkDiscoveryLiveListURL   = "https://api.chzzk.naver.com/service/v1/lives"
	chzzkDiscoveryLiveDetailURL = "https://api.chzzk.naver.com/service/v3/channels/%s/live-detail"
	chzzkDiscoveryPageSize      = 50
	chzzkDiscoveryUserAgent     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

type chzzkPageCursor struct {
	ConcurrentUserCount int `json:"concurrentUserCount"`
	LiveID              int `json:"liveId"`
}

// ChzzkDiscovery discovers currently live Chzzk channels.
type ChzzkDiscovery struct {
	dcClient      *http.Client // datacenter proxy (or direct) — primary
	resClient     *http.Client // residential proxy — fallback, nil if not configured
	dcRatio       float64      // fraction of cycles using datacenter proxy [0.0, 1.0]
	liveListURL   string
	liveDetailURL string
	apiMetrics    APIMetrics
}

// NewChzzkDiscovery creates a Chzzk discovery client with optional dual-proxy support.
//
// resProxyURL: optional fallback proxy URL. Empty disables the fallback route.
// dcProxyURL:  optional primary proxy URL. Empty uses a direct connection.
// dcRatio:     fraction of discovery cycles routed through the datacenter proxy [0.0, 1.0].
//
// Returns an error if any non-empty proxy URL is invalid, to prevent silent degradation.
func NewChzzkDiscovery(apiMetrics APIMetrics, resProxyURL string, dcProxyURL string, dcRatio float64) (*ChzzkDiscovery, error) {
	// Residential proxy client (optional).
	var resClient *http.Client
	if resProxyURL != "" {
		parsed, err := url.Parse(resProxyURL)
		if err != nil || parsed.Host == "" {
			return nil, fmt.Errorf("invalid CHZZK_PROXY_URL %q: %w", resProxyURL, err)
		}
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = http.ProxyURL(parsed)
		resClient = &http.Client{Timeout: 15 * time.Second, Transport: t}
		slog.Info("chzzk discovery: primary proxy configured")
	}

	// Datacenter proxy client (primary; falls back to direct if dcProxyURL is empty).
	dcTransport := http.DefaultTransport.(*http.Transport).Clone()
	dcTimeout := 10 * time.Second
	if dcProxyURL != "" {
		parsed, err := url.Parse(dcProxyURL)
		if err != nil || parsed.Host == "" {
			return nil, fmt.Errorf("invalid CHZZK_DC_PROXY_URL %q: %w", dcProxyURL, err)
		}
		dcTransport.Proxy = http.ProxyURL(parsed)
		dcTimeout = 12 * time.Second
		slog.Info("chzzk discovery: secondary proxy configured")
	}
	dcClient := &http.Client{Timeout: dcTimeout, Transport: dcTransport}

	return &ChzzkDiscovery{
		dcClient:      dcClient,
		resClient:     resClient,
		dcRatio:       dcRatio,
		liveListURL:   chzzkDiscoveryLiveListURL,
		liveDetailURL: chzzkDiscoveryLiveDetailURL,
		apiMetrics:    apiMetrics,
	}, nil
}

func newChzzkDiscoveryForTest(client *http.Client, liveListURL, liveDetailURL string) *ChzzkDiscovery {
	return &ChzzkDiscovery{
		dcClient:      client,
		dcRatio:       1.0, // always use dcClient in tests
		liveListURL:   liveListURL,
		liveDetailURL: liveDetailURL,
	}
}

// selectClient picks an HTTP client for one full discovery cycle.
// Uses the datacenter client with probability dcRatio; residential otherwise.
func (d *ChzzkDiscovery) selectClient() *http.Client {
	// #nosec G404 -- this random choice only balances traffic between operator-configured clients.
	if d.resClient == nil || rand.Float64() < d.dcRatio {
		return d.dcClient
	}
	return d.resClient
}

// Platform returns PlatformChzzk.
func (d *ChzzkDiscovery) Platform() model.Platform {
	return model.PlatformChzzk
}

// FetchLiveChannels paginates through Chzzk's public live list.
// If a page fails mid-pagination, it retries the same cursor with the
// fallback proxy before giving up. Partial channels collected so far
// are returned with Partial=true so callers can avoid marking missing
// channels as ended.
func (d *ChzzkDiscovery) FetchLiveChannels(ctx context.Context) (DiscoveryResult, error) {
	primary := d.selectClient()
	var (
		all    []DiscoveredChannel
		cursor *chzzkPageCursor
	)

	for {
		page, next, err := d.fetchPage(ctx, cursor, primary)
		if err != nil {
			// Per-page failover: retry the same cursor with the alternate proxy.
			if fallback := d.fallbackClient(primary); fallback != nil {
				slog.Debug("chzzk: page failed, retrying with fallback proxy", "collected", len(all))
				page, next, err = d.fetchPage(ctx, cursor, fallback)
			}
			if err != nil {
				if len(all) > 0 {
					return DiscoveryResult{Channels: all, Partial: true}, nil
				}
				return DiscoveryResult{}, err
			}
		}
		all = append(all, page...)
		if next == nil || len(page) == 0 {
			return DiscoveryResult{Channels: all}, nil
		}
		cursor = next
	}
}

// fallbackClient returns the alternate proxy client for per-page failover.
func (d *ChzzkDiscovery) fallbackClient(primary *http.Client) *http.Client {
	if primary == d.dcClient && d.resClient != nil {
		return d.resClient
	}
	if primary == d.resClient {
		return d.dcClient
	}
	return nil
}

// IsChannelLive checks whether a channel is currently open.
func (d *ChzzkDiscovery) IsChannelLive(ctx context.Context, channelID string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(d.liveDetailURL, channelID), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", chzzkDiscoveryUserAgent)

	var respBody struct {
		Content struct {
			Status string `json:"status"`
		} `json:"content"`
	}
	if err := d.doJSON(req, &respBody, "live_detail", d.dcClient); err != nil {
		return false, err
	}
	return respBody.Content.Status == "OPEN", nil
}

func (d *ChzzkDiscovery) fetchPage(ctx context.Context, cursor *chzzkPageCursor, client *http.Client) ([]DiscoveredChannel, *chzzkPageCursor, error) {
	u, err := url.Parse(d.liveListURL)
	if err != nil {
		return nil, nil, err
	}
	q := u.Query()
	q.Set("size", fmt.Sprintf("%d", chzzkDiscoveryPageSize))
	q.Set("sortType", "POPULAR")
	if cursor != nil {
		q.Set("concurrentUserCount", fmt.Sprintf("%d", cursor.ConcurrentUserCount))
		q.Set("liveId", fmt.Sprintf("%d", cursor.LiveID))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", chzzkDiscoveryUserAgent)

	var respBody struct {
		Content struct {
			Data []struct {
				LiveID              int      `json:"liveId"`
				LiveTitle           string   `json:"liveTitle"`
				LiveImageURL        string   `json:"liveImageUrl"`
				ConcurrentUserCount int      `json:"concurrentUserCount"`
				OpenDate            string   `json:"openDate"`
				Tags                []string `json:"tags"`
				LiveCategory        string   `json:"liveCategory"`
				LiveCategoryValue   string   `json:"liveCategoryValue"`
				Channel             struct {
					ChannelID   string `json:"channelId"`
					ChannelName string `json:"channelName"`
				} `json:"channel"`
			} `json:"data"`
			Page struct {
				Next *chzzkPageCursor `json:"next"`
			} `json:"page"`
		} `json:"content"`
	}
	if err := d.doJSON(req, &respBody, "live_list", client); err != nil {
		return nil, nil, err
	}

	channels := make([]DiscoveredChannel, 0, len(respBody.Content.Data))
	for _, item := range respBody.Content.Data {
		if item.Channel.ChannelID == "" {
			continue
		}
		var startedAt *time.Time
		if item.OpenDate != "" {
			// Chzzk openDate is a local Asia/Seoul timestamp without an offset.
			kst := time.FixedZone("KST", 9*60*60)
			if t, err := time.ParseInLocation("2006-01-02 15:04:05", item.OpenDate, kst); err == nil {
				utc := t.UTC()
				startedAt = &utc
			}
		}
		channels = append(channels, DiscoveredChannel{
			ChannelID:    item.Channel.ChannelID,
			StreamerName: item.Channel.ChannelName,
			ViewerCount:  item.ConcurrentUserCount,
			Title:        item.LiveTitle,
			Category:     item.LiveCategoryValue,
			CategoryCode: item.LiveCategory,
			Tags:         item.Tags,
			StartedAt:    startedAt,
			ThumbnailURL: item.LiveImageURL,
		})
	}
	return channels, respBody.Content.Page.Next, nil
}

func (d *ChzzkDiscovery) doJSON(req *http.Request, dest any, endpoint string, client *http.Client) error {
	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		if d.apiMetrics != nil {
			d.apiMetrics.ObserveAPIDuration("chzzk", endpoint, duration)
			d.apiMetrics.IncAPIRequest("chzzk", endpoint, "error")
		}
		return err
	}
	defer resp.Body.Close()

	if d.apiMetrics != nil {
		d.apiMetrics.ObserveAPIDuration("chzzk", endpoint, duration)
		d.apiMetrics.IncAPIRequest("chzzk", endpoint, fmt.Sprintf("%d", resp.StatusCode))
	}

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("chzzk discovery request failed: %s", resp.Status)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("decode chzzk discovery response: %w", err)
	}
	return nil
}

var _ PlatformDiscovery = (*ChzzkDiscovery)(nil)
