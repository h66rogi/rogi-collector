package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
)

const (
	cimeDiscoveryListURL      = "https://ci.me/json/?sort=POPULAR&follow_only=false"
	cimeDiscoveryLiveCheckURL = "https://ci.me/json/@%s/live"
	cimeDiscoveryUserAgent    = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

// CimeDiscovery discovers currently live ci.me channels.
type CimeDiscovery struct {
	httpClient   *http.Client
	listURL      string
	liveCheckURL string
	apiMetrics   APIMetrics
}

// NewCimeDiscovery creates a ci.me discovery client.
func NewCimeDiscovery(apiMetrics APIMetrics) *CimeDiscovery {
	return &CimeDiscovery{
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		listURL:      cimeDiscoveryListURL,
		liveCheckURL: cimeDiscoveryLiveCheckURL,
		apiMetrics:   apiMetrics,
	}
}

func newCimeDiscoveryForTest(client *http.Client, listURL, liveCheckURL string) *CimeDiscovery {
	return &CimeDiscovery{
		httpClient:   client,
		listURL:      listURL,
		liveCheckURL: liveCheckURL,
	}
}

func (d *CimeDiscovery) Platform() model.Platform {
	return model.PlatformCime
}

func (d *CimeDiscovery) FetchLiveChannels(ctx context.Context) (DiscoveryResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.listURL, nil)
	if err != nil {
		return DiscoveryResult{}, err
	}
	req.Header.Set("User-Agent", cimeDiscoveryUserAgent)

	var respBody struct {
		BodyData struct {
			Sections []struct {
				Type  string `json:"type"`
				Items []struct {
					Title        string `json:"title"`
					OpenedAt     string `json:"openedAt"`
					ImageURL     string `json:"imageUrl"`
					CurViewerCnt int    `json:"curViewerCnt"`
					Channel      struct {
						Slug string `json:"slug"`
						Name string `json:"name"`
					} `json:"channel"`
					Category *struct {
						Name string `json:"name"`
						Code string `json:"code"`
					} `json:"category"`
					Tags []struct {
						Name string `json:"name"`
					} `json:"tags"`
				} `json:"items"`
			} `json:"sections"`
		} `json:"bodyData"`
	}
	if err := d.doJSON(req, &respBody, "live_list"); err != nil {
		return DiscoveryResult{}, err
	}

	var channels []DiscoveredChannel
	for _, section := range respBody.BodyData.Sections {
		if section.Type != "LIVE" {
			continue
		}
		for _, item := range section.Items {
			if item.Channel.Slug == "" {
				continue
			}

			var startedAt *time.Time
			if item.OpenedAt != "" {
				if t, err := time.Parse(time.RFC3339Nano, item.OpenedAt); err == nil {
					startedAt = &t
				}
			}

			var category, categoryCode string
			if item.Category != nil {
				category = item.Category.Name
				categoryCode = item.Category.Code
			}

			var tags []string
			for _, tag := range item.Tags {
				if tag.Name != "" {
					tags = append(tags, tag.Name)
				}
			}

			channels = append(channels, DiscoveredChannel{
				ChannelID:    item.Channel.Slug,
				StreamerName: item.Channel.Name,
				ViewerCount:  item.CurViewerCnt,
				Title:        item.Title,
				Category:     category,
				CategoryCode: categoryCode,
				Tags:         tags,
				StartedAt:    startedAt,
				ThumbnailURL: item.ImageURL,
			})
		}
	}
	return DiscoveryResult{Channels: channels}, nil
}

func (d *CimeDiscovery) IsChannelLive(ctx context.Context, channelID string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(d.liveCheckURL, channelID), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", cimeDiscoveryUserAgent)

	var respBody struct {
		BodyData struct {
			Live json.RawMessage `json:"live"`
		} `json:"bodyData"`
	}
	if err := d.doJSON(req, &respBody, "live_check"); err != nil {
		return false, err
	}
	return len(respBody.BodyData.Live) > 0 && string(respBody.BodyData.Live) != "null", nil
}

func (d *CimeDiscovery) doJSON(req *http.Request, dest any, endpoint string) error {
	start := time.Now()
	resp, err := d.httpClient.Do(req)
	duration := time.Since(start)

	if err != nil {
		if d.apiMetrics != nil {
			d.apiMetrics.ObserveAPIDuration("cime", endpoint, duration)
			d.apiMetrics.IncAPIRequest("cime", endpoint, "error")
		}
		return err
	}
	defer resp.Body.Close()

	if d.apiMetrics != nil {
		d.apiMetrics.ObserveAPIDuration("cime", endpoint, duration)
		d.apiMetrics.IncAPIRequest("cime", endpoint, fmt.Sprintf("%d", resp.StatusCode))
	}

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("cime discovery request failed: %s", resp.Status)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("decode cime discovery response: %w", err)
	}
	return nil
}

var _ PlatformDiscovery = (*CimeDiscovery)(nil)
