package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"nhooyr.io/websocket"
)

const (
	cimeTokenURL     = "https://ci.me/api/app/channels/%s/chat-token" // #nosec G101 -- public endpoint, not a credential
	cimeOrigin       = "https://ci.me"
	cimeUserAgent    = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
	cimePingInterval = 30 * time.Second
	// cimePingTimeout bounds how long we wait for a Pong before declaring
	// the socket dead while allowing routine network latency.
	cimePingTimeout = 5 * time.Second
	// cimeReadTimeout is a fallback per-read deadline. nhooyr's Read consumes
	// pong control frames internally and only returns on data frames, so the
	// deadline is NOT refreshed by pongs alone. The service may not push any
	// data frame on truly idle channels, so we keep this generously long
	// (5 min) to avoid false-positive zombie reaping — fast detection of
	// half-open sockets is delegated to conn.Ping in pingLoop.
	cimeReadTimeout = 5 * time.Minute
	cimeMsgBufSize  = 256
	cimeErrBufSize  = 8
)

type cimeTokenResponse struct {
	Data struct {
		Token string `json:"token"`
	} `json:"data"`
}

type cimeSender struct {
	UserID     string            `json:"UserId"`
	Attributes map[string]string `json:"Attributes"`
}

type cimeWSMessage struct {
	Type         string                     `json:"Type"`
	Content      string                     `json:"Content"`
	EventName    string                     `json:"EventName"`
	ErrorMessage string                     `json:"ErrorMessage"`
	Sender       *cimeSender                `json:"Sender"`
	Attributes   map[string]json.RawMessage `json:"Attributes"`
}

type cimeUserProfile struct {
	ID json.Number `json:"id"`
	Ch struct {
		ID json.Number `json:"id"`
		Na string      `json:"na"`
	} `json:"ch"`
	Bg []struct {
		Na string `json:"na"`
	} `json:"bg"`
}

type cimeDonationExtra struct {
	Msg  string `json:"msg"`
	Amt  int64  `json:"amt"`
	Anon bool   `json:"anon"`
	Prof struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"prof"`
}

// CimeConnector implements PlatformConnector for ci.me.
type CimeConnector struct {
	httpClient *http.Client
	wsURL      string

	mu      sync.Mutex
	conn    *websocket.Conn
	alive   bool
	cancel  context.CancelFunc
	channel model.LiveChannel
	msgCh   chan model.ChatMessage
	errCh   chan error
	seq     atomic.Int64
}

// NewCimeConnector creates a new ci.me connector. The websocket endpoint is
// operator-supplied because it is not a stable public API contract.
func NewCimeConnector(wsURLs ...string) *CimeConnector {
	connector := &CimeConnector{
		httpClient: &http.Client{Timeout: 10 * time.Second},
		msgCh:      make(chan model.ChatMessage, cimeMsgBufSize),
		errCh:      make(chan error, cimeErrBufSize),
	}
	if len(wsURLs) > 0 {
		connector.wsURL = strings.TrimSpace(wsURLs[0])
	}
	return connector
}

func (c *CimeConnector) Connect(ctx context.Context, channel model.LiveChannel) error {
	if c.wsURL == "" {
		return errors.New("CIME_WS_URL is required")
	}
	parsedWSURL, err := url.Parse(c.wsURL)
	if err != nil || parsedWSURL.Scheme != "wss" || parsedWSURL.Host == "" || parsedWSURL.User != nil {
		return errors.New("CIME_WS_URL must be a WSS URL without user info")
	}
	token, err := c.fetchChatToken(ctx, channel.ChannelID)
	if err != nil {
		return fmt.Errorf("cime fetch chat token: %w", err)
	}
	if token == "" {
		return fmt.Errorf("cime chat token empty for %s", channel.ChannelID)
	}

	wsCtx, cancel := context.WithCancel(ctx)
	conn, _, err := websocket.Dial(wsCtx, c.wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Origin":     {cimeOrigin},
			"User-Agent": {cimeUserAgent},
		},
		Subprotocols: []string{token},
	})
	if err != nil {
		cancel()
		return fmt.Errorf("cime ws dial: %w", err)
	}

	conn.SetReadLimit(1 << 20)

	c.mu.Lock()
	c.conn = conn
	c.alive = true
	c.cancel = cancel
	c.channel = channel
	c.mu.Unlock()

	go c.readLoop(wsCtx)
	go c.pingLoop(wsCtx)
	return nil
}

func (c *CimeConnector) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.alive {
		return nil
	}
	c.alive = false
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		return c.conn.Close(websocket.StatusNormalClosure, "disconnect")
	}
	return nil
}

func (c *CimeConnector) IsAlive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive
}

func (c *CimeConnector) Messages() <-chan model.ChatMessage {
	return c.msgCh
}

func (c *CimeConnector) Errors() <-chan error {
	return c.errCh
}

func (c *CimeConnector) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alive = false
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		if err := c.conn.Close(websocket.StatusGoingAway, "error"); err != nil {
			slog.Debug("cime websocket close failed", "error", err)
		}
	}
}

// pingLoop sends a WS-level Ping every cimePingInterval. AWS IVS replies
// with Pong (handled internally by nhooyr's Read), and the cimePingTimeout
// bounds how long we wait for it. Failure surfaces on errCh so manager can
// reap the connection — addressing a prior bug where ping failures returned
// silently and left readLoop blocking on a half-open socket.
func (c *CimeConnector) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(cimePingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			conn := c.conn
			alive := c.alive
			c.mu.Unlock()
			if !alive || conn == nil {
				return
			}
			pingCtx, cancel := context.WithTimeout(ctx, cimePingTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				select {
				case c.errCh <- fmt.Errorf("cime ws ping failed: %w", err):
				default:
				}
				return
			}
		}
	}
}

// readLoop reads messages with a per-read timeout context. Successful frames
// implicitly refresh the deadline by entering the next iteration with a
// fresh context; deadline-exceeded reads are wrapped so manager classifies
// them as zombie reaping.
func (c *CimeConnector) readLoop(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		c.alive = false
		c.mu.Unlock()
	}()

	for {
		readCtx, cancel := context.WithTimeout(ctx, cimeReadTimeout)
		_, data, err := c.conn.Read(readCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("cime ws read timeout after %s: %w", cimeReadTimeout, err)
			}
			select {
			case c.errCh <- fmt.Errorf("cime ws read: %w", err):
			default:
			}
			return
		}
		c.handleRawMessage(data)
	}
}

func (c *CimeConnector) handleRawMessage(data []byte) {
	var msg cimeWSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		// Non-JSON frame (e.g. IVS ping). Discard silently.
		return
	}

	switch msg.Type {
	case "MESSAGE":
		c.emitMessage(c.convertChatMessage(msg, data))
	case "EVENT":
		c.emitMessage(c.convertEventMessage(msg, data))
	case "ERROR":
		c.emitMessage(model.ChatMessage{
			ID:           fmt.Sprintf("cime-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
			Type:         model.MessageTypeSystem,
			Platform:     model.PlatformCime,
			ChannelID:    c.channel.ChannelID,
			StreamerName: c.channel.StreamerName,
			Nickname:     "system",
			Message:      msg.ErrorMessage,
			Timestamp:    time.Now(),
			Raw:          string(data),
		})
	default:
		c.emitMessage(model.ChatMessage{
			ID:           fmt.Sprintf("cime-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
			Type:         model.MessageTypeSystem,
			Platform:     model.PlatformCime,
			ChannelID:    c.channel.ChannelID,
			StreamerName: c.channel.StreamerName,
			Nickname:     "system",
			Message:      string(data),
			Timestamp:    time.Now(),
			Raw:          string(data),
		})
	}
}

func (c *CimeConnector) emitMessage(msg model.ChatMessage) {
	if msg.ID == "" {
		return
	}
	select {
	case c.msgCh <- msg:
	default:
		if cb := globalConfig.OnMessageDropped; cb != nil {
			cb("cime")
		}
	}
}

func (c *CimeConnector) convertChatMessage(msg cimeWSMessage, raw []byte) model.ChatMessage {
	nickname := "?"
	userID := ""
	if msg.Sender != nil {
		userID = msg.Sender.UserID
		nickname = userID
		if rawUser := msg.Sender.Attributes["user"]; rawUser != "" {
			var profile cimeUserProfile
			if err := json.Unmarshal([]byte(rawUser), &profile); err == nil {
				if profile.Ch.Na != "" {
					nickname = profile.Ch.Na
				}
			}
		}
	}
	if nickname == "" {
		nickname = "?"
	}

	return model.ChatMessage{
		ID:           fmt.Sprintf("cime-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
		Type:         model.MessageTypeChat,
		Platform:     model.PlatformCime,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		UserID:       userID,
		Nickname:     nickname,
		Message:      msg.Content,
		Timestamp:    time.Now(),
		Raw:          string(raw),
		Emotes:       extractCimeEmotes(msg.Content, parseCimeEmojiMap(msg.Attributes)),
	}
}

// parseCimeEmojiMap pulls the inline `emojis` attribute (a {":token:": url}
// map) out of the IVS chat event Attributes. ci.me ships this map alongside
// every chat event that references custom emotes, so the renderer doesn't
// need a separate emote-pack fetch.
func parseCimeEmojiMap(attrs map[string]json.RawMessage) map[string]string {
	rawEmojis, ok := attrs["emojis"]
	if !ok || len(rawEmojis) == 0 {
		return nil
	}
	parse := func(b []byte) map[string]string {
		var m map[string]string
		if err := json.Unmarshal(b, &m); err == nil {
			return m
		}
		return nil
	}
	// IVS attributes are sometimes double-encoded as a JSON string.
	if rawEmojis[0] == '"' {
		var encoded string
		if err := json.Unmarshal(rawEmojis, &encoded); err == nil && encoded != "" {
			return parse([]byte(encoded))
		}
		return nil
	}
	return parse(rawEmojis)
}

// extractCimeEmotes locates every occurrence of the ci.me-supplied emoji
// keys inside body. The emoji map keys are the full token form `:code:`
// (with colons), so we use them as the source of truth and avoid the
// false positives a bare `:word:` regex would produce against ordinary
// punctuation.
func extractCimeEmotes(body string, emojis map[string]string) []model.EmoteToken {
	if body == "" || len(emojis) == 0 {
		return nil
	}
	var tokens []model.EmoteToken
	for key, url := range emojis {
		if len(key) < 3 || !strings.HasPrefix(key, ":") || !strings.HasSuffix(key, ":") {
			continue
		}
		code := key[1 : len(key)-1]
		idx := 0
		for {
			rel := strings.Index(body[idx:], key)
			if rel < 0 {
				break
			}
			start := idx + rel
			end := start + len(key)
			tokens = append(tokens, model.EmoteToken{
				Code:     code,
				Start:    start,
				End:      end,
				ImageURL: url,
				Source:   "cime:inline",
			})
			idx = end
		}
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i].Start < tokens[j].Start })
	return tokens
}

func (c *CimeConnector) convertEventMessage(msg cimeWSMessage, raw []byte) model.ChatMessage {
	if strings.Contains(strings.ToUpper(msg.EventName), "DONATION") {
		var extra cimeDonationExtra
		if rawExtra, ok := msg.Attributes["extra"]; ok && len(rawExtra) > 0 {
			if len(rawExtra) > 0 && rawExtra[0] == '"' {
				var encoded string
				if err := json.Unmarshal(rawExtra, &encoded); err == nil {
					_ = json.Unmarshal([]byte(encoded), &extra)
				}
			} else {
				_ = json.Unmarshal(rawExtra, &extra)
			}
		}
		nickname := extra.Prof.Name
		if extra.Anon || nickname == "" {
			nickname = "익명"
		}
		return model.ChatMessage{
			ID:           fmt.Sprintf("cime-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
			Type:         model.MessageTypeDonation,
			Platform:     model.PlatformCime,
			ChannelID:    c.channel.ChannelID,
			StreamerName: c.channel.StreamerName,
			UserID:       fmt.Sprintf("%d", extra.Prof.ID),
			Nickname:     nickname,
			Message:      extra.Msg,
			Timestamp:    time.Now(),
			Raw:          string(raw),
			Amount:       float64(extra.Amt),
			Currency:     "CIME_BEAM",
			AmountKRW:    extra.Amt,
		}
	}

	return model.ChatMessage{
		ID:           fmt.Sprintf("cime-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
		Type:         model.MessageTypeSystem,
		Platform:     model.PlatformCime,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		Nickname:     "system",
		Message:      msg.EventName,
		Timestamp:    time.Now(),
		Raw:          string(raw),
	}
}

func (c *CimeConnector) fetchChatToken(ctx context.Context, channelID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf(cimeTokenURL, channelID), http.NoBody)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", cimeUserAgent)
	req.Header.Set("Origin", cimeOrigin)
	req.Header.Set("Content-Length", "0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("cime token api failed: %s", resp.Status)
	}

	var result cimeTokenResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse cime token response: %w", err)
	}
	return result.Data.Token, nil
}

var _ PlatformConnector = (*CimeConnector)(nil)
