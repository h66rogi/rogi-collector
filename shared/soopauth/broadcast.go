package soopauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var channelPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,50}$`)

func ValidChannelID(id string) bool { return channelPattern.MatchString(id) }

// BroadcastInfo is a sanitized, one-shot player lookup. It does not join a room,
// register a subscription, or return player tickets/cookies/raw protocol fields.
type BroadcastInfo struct {
	ChannelID   string    `json:"channelId"`
	State       string    `json:"state"`
	Title       string    `json:"title"`
	DisplayName string    `json:"displayName"`
	BroadcastID string    `json:"broadcastId"`
	CheckedAt   time.Time `json:"checkedAt"`
	Cached      bool      `json:"cached"`
}

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func CheckBroadcast(ctx context.Context, client HTTPDoer, channel string) (BroadcastInfo, error) {
	if !ValidChannelID(channel) {
		return BroadcastInfo{}, errors.New("invalid channel ID")
	}
	result := BroadcastInfo{ChannelID: channel, State: "lookup_failed", CheckedAt: time.Now().UTC()}
	form := url.Values{"bid": {channel}, "bno": {"0"}, "type": {"live"}, "player_type": {"html5"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://live.sooplive.com/afreeca/player_live_api.php?bjid="+url.QueryEscape(channel), strings.NewReader(form.Encode()))
	if err != nil {
		return result, nil
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Referer", "https://play.sooplive.com/"+channel)
	response, err := client.Do(req)
	if err != nil {
		if errors.Is(err, ErrCookieUnavailable) {
			result.State = "cookie_required"
		}
		return result, nil
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return result, nil
	}
	var payload struct {
		Channel struct {
			Result json.RawMessage `json:"RESULT"`
			ChatNo string          `json:"CHATNO"`
			Title  string          `json:"TITLE"`
			Name   string          `json:"BJNICK"`
			Number json.RawMessage `json:"BNO"`
		} `json:"CHANNEL"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return result, nil
	}
	live, err := LiveResult(payload.Channel.Result, payload.Channel.ChatNo)
	if errors.Is(err, ErrLoginRequired) {
		result.State = "auth_required"
		return result, nil
	}
	if err != nil {
		return result, nil
	}
	result.State = "offline"
	if live {
		result.State = "live"
		result.Title = boundedText(payload.Channel.Title, 300)
		result.DisplayName = boundedText(payload.Channel.Name, 80)
		number := strings.Trim(string(payload.Channel.Number), `"`)
		if matched, _ := regexp.MatchString(`^[0-9]{1,30}$`, number); matched {
			result.BroadcastID = number
		}
	}
	return result, nil
}
func boundedText(value string, limit int) string {
	value = strings.Map(func(r rune) rune {
		if r < ' ' || r == 127 {
			return -1
		}
		return r
	}, value)
	runes := []rune(value)
	if len(runes) > limit {
		runes = runes[:limit]
	}
	return string(runes)
}
