package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
)

const (
	soopDiscoveryListURL      = "https://live.sooplive.com/api/main_broad_list_api.php"
	soopDiscoveryLiveCheckURL = "https://live.sooplive.com/afreeca/player_live_api.php?bjid=%s"
	soopDiscoveryUserAgent    = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

// SoopDiscovery discovers currently live SOOP channels.
type SoopDiscovery struct {
	httpClient   *http.Client
	listURL      string
	liveCheckURL string
	apiMetrics   APIMetrics
}

// NewSoopDiscovery creates a SOOP discovery client.
func NewSoopDiscovery(apiMetrics APIMetrics, cookieFile ...string) *SoopDiscovery {
	client := &http.Client{Timeout: 10 * time.Second}
	if len(cookieFile) > 0 {
		client = soopauth.NewHTTPClient(cookieFile[0])
	}
	return &SoopDiscovery{
		httpClient:   client,
		listURL:      soopDiscoveryListURL,
		liveCheckURL: soopDiscoveryLiveCheckURL,
		apiMetrics:   apiMetrics,
	}
}

func newSoopDiscoveryForTest(client *http.Client, listURL, liveCheckURL string) *SoopDiscovery {
	return &SoopDiscovery{
		httpClient:   client,
		listURL:      listURL,
		liveCheckURL: liveCheckURL,
	}
}

func (d *SoopDiscovery) Platform() model.Platform {
	return model.PlatformSoop
}

func (d *SoopDiscovery) FetchLiveChannels(ctx context.Context) (DiscoveryResult, error) {
	var (
		all      []DiscoveredChannel
		pageNo   = 1
		totalCnt = -1
	)

	for {
		page, total, err := d.fetchPage(ctx, pageNo)
		if err != nil {
			return DiscoveryResult{}, err
		}
		if totalCnt < 0 {
			totalCnt = total
		}
		all = append(all, page...)
		if len(page) == 0 || (totalCnt >= 0 && len(all) >= totalCnt) {
			return DiscoveryResult{Channels: all}, nil
		}
		pageNo++
	}
}

func (d *SoopDiscovery) IsChannelLive(ctx context.Context, channelID string) (bool, error) {
	form := url.Values{
		"bid":         {channelID},
		"bno":         {"0"},
		"type":        {"live"},
		"player_type": {"html5"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf(d.liveCheckURL, channelID), strings.NewReader(form.Encode()))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", soopDiscoveryUserAgent)
	req.Header.Set("Referer", fmt.Sprintf("https://play.sooplive.com/%s", channelID))

	var respBody struct {
		Channel struct {
			Result json.RawMessage `json:"RESULT"`
			ChatNo string          `json:"CHATNO"`
		} `json:"CHANNEL"`
	}
	if err := d.doJSON(req, &respBody, "live_check"); err != nil {
		return false, err
	}
	return soopauth.LiveResult(respBody.Channel.Result, respBody.Channel.ChatNo)
}

func (d *SoopDiscovery) fetchPage(ctx context.Context, pageNo int) ([]DiscoveredChannel, int, error) {
	u, err := url.Parse(d.listURL)
	if err != nil {
		return nil, 0, err
	}
	q := u.Query()
	q.Set("selectType", "action")
	q.Set("selectValue", "all")
	q.Set("orderType", "view_cnt")
	q.Set("pageNo", strconv.Itoa(pageNo))
	q.Set("lang", "ko_KR")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", soopDiscoveryUserAgent)

	var respBody struct {
		TotalCnt json.Number `json:"total_cnt"`
		Broad    []struct {
			UserID         string   `json:"user_id"`
			UserNick       string   `json:"user_nick"`
			BroadTitle     string   `json:"broad_title"`
			BroadStart     string   `json:"broad_start"`
			BroadThumb     string   `json:"broad_thumb"`
			CurrentViewCnt string   `json:"current_view_cnt"`
			BroadCateNo    string   `json:"broad_cate_no"`
			CategoryName   string   `json:"category_name"`
			CategoryTags   []string `json:"category_tags"`
			HashTags       []string `json:"hash_tags"`
		} `json:"broad"`
	}
	if err := d.doJSON(req, &respBody, "broad_list"); err != nil {
		return nil, 0, err
	}

	channels := make([]DiscoveredChannel, 0, len(respBody.Broad))
	for _, item := range respBody.Broad {
		if item.UserID == "" {
			continue
		}
		viewerCount, _ := strconv.Atoi(item.CurrentViewCnt)

		var startedAt *time.Time
		if item.BroadStart != "" {
			kst := time.FixedZone("KST", 9*60*60)
			if t, err := time.ParseInLocation("2006-01-02 15:04:05", item.BroadStart, kst); err == nil {
				utc := t.UTC()
				startedAt = &utc
			}
		}

		var tags []string
		tags = append(tags, item.CategoryTags...)
		tags = append(tags, item.HashTags...)

		channels = append(channels, DiscoveredChannel{
			ChannelID:    item.UserID,
			StreamerName: item.UserNick,
			ViewerCount:  viewerCount,
			Title:        item.BroadTitle,
			Category:     item.CategoryName,
			CategoryCode: item.BroadCateNo,
			Tags:         tags,
			StartedAt:    startedAt,
			ThumbnailURL: item.BroadThumb,
		})
	}
	totalCnt, _ := respBody.TotalCnt.Int64()
	return channels, int(totalCnt), nil
}

func (d *SoopDiscovery) doJSON(req *http.Request, dest any, endpoint string) error {
	start := time.Now()
	resp, err := d.httpClient.Do(req)
	duration := time.Since(start)

	if err != nil {
		if d.apiMetrics != nil {
			d.apiMetrics.ObserveAPIDuration("soop", endpoint, duration)
			d.apiMetrics.IncAPIRequest("soop", endpoint, "error")
		}
		return err
	}
	defer resp.Body.Close()

	if d.apiMetrics != nil {
		d.apiMetrics.ObserveAPIDuration("soop", endpoint, duration)
		d.apiMetrics.IncAPIRequest("soop", endpoint, fmt.Sprintf("%d", resp.StatusCode))
	}

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return soopauth.ErrLoginRequired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("soop discovery request failed: %s", resp.Status)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("decode soop discovery response: %w", err)
	}
	return nil
}

var _ PlatformDiscovery = (*SoopDiscovery)(nil)
