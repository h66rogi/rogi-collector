package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"nhooyr.io/websocket"
)

const (
	chzzkWSURL          = "wss://kr-ss3.chat.naver.com/chat"
	chzzkOrigin         = "https://chzzk.naver.com"
	chzzkLiveDetailURL  = "https://api.chzzk.naver.com/service/v3/channels/%s/live-detail"
	chzzkAccessTokenURL = "https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId=%s&chatType=STREAMING" // #nosec G101 -- public endpoint, not a credential
	chzzkUserAgent      = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
	chzzkPingInterval   = 20 * time.Second
	// chzzkPingTimeout bounds how long we wait for a Pong (RFC 6455 control
	// frame) before declaring the socket dead. NAVER's chat server reliably
	// replies to WS-level Ping in sustained load tests, so conn.Ping is the
	// sole fast liveness
	// signal. We deliberately do NOT arm a per-Read deadline as a fallback —
	// NAVER does not push periodic data frames to idle chzzk rooms, so a
	// healthy idle channel can sit minutes without any returnable Read; both
	// read deadlines tripped false positives on such channels. With Ping/Pong
	// validated under sustained load, the readTimeout fallback is pure
	// risk with no upside.
	chzzkPingTimeout     = 5 * time.Second
	chzzkMsgBufSize      = 256
	chzzkErrBufSize      = 8
	chzzkCmdConnect      = 100
	chzzkCmdPing         = 10000
	chzzkCmdConnectReply = 10100
	chzzkCmdChat         = 93101
	chzzkCmdDonationChat = 93102
	chzzkMsgTypeDonation = 10
)

// chzzkChatInfo holds the transient identifiers needed to join a chat room.
type chzzkChatInfo struct {
	ChatChannelID string
	AccessToken   string
}

// ChzzkConnector implements PlatformConnector for Chzzk (NAVER).
// ChzzkConnector is single-use. Create a new instance for each connection.
type ChzzkConnector struct {
	httpClient *http.Client

	mu      sync.Mutex
	wmu     sync.Mutex // write-serialization for conn.Write
	conn    *websocket.Conn
	alive   bool
	cancel  context.CancelFunc
	channel model.LiveChannel
	msgCh   chan model.ChatMessage
	errCh   chan error
	seq     atomic.Int64
}

// NewChzzkConnector creates a new ChzzkConnector. dcProxyURL, when non-empty,
// routes the chzzk HTTP API calls (live-detail, access-token) through a
// configurable HTTP proxy when operators cannot use a direct route. Empty
// means direct HTTP. The WebSocket dial is always direct. Operators are
// responsible for ensuring that their access method complies with the
// platform's current terms and API policies.
func NewChzzkConnector(dcProxyURL string) *ChzzkConnector {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	if dcProxyURL != "" {
		parsed, err := url.Parse(dcProxyURL)
		if err != nil || parsed.Host == "" {
			slog.Error("chzzk: invalid CHZZK_DC_PROXY_URL, falling back to direct HTTP", "error", err, "url_len", len(dcProxyURL))
		} else {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.Proxy = http.ProxyURL(parsed)
			httpClient = &http.Client{Timeout: 12 * time.Second, Transport: t}
			slog.Info("chzzk: HTTP API calls routed through configured proxy")
		}
	}
	return &ChzzkConnector{
		httpClient: httpClient,
		msgCh:      make(chan model.ChatMessage, chzzkMsgBufSize),
		errCh:      make(chan error, chzzkErrBufSize),
	}
}

func (c *ChzzkConnector) Connect(ctx context.Context, channel model.LiveChannel) error {
	info, err := c.fetchChatInfo(ctx, channel.ChannelID)
	if err != nil {
		return fmt.Errorf("chzzk fetch chat info: %w", err)
	}

	wsCtx, cancel := context.WithCancel(ctx)

	conn, _, err := websocket.Dial(wsCtx, chzzkWSURL, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Origin":     {chzzkOrigin},
			"User-Agent": {chzzkUserAgent},
		},
	})
	if err != nil {
		cancel()
		return fmt.Errorf("chzzk ws dial: %w", err)
	}

	// Increase read limit for large donation messages.
	conn.SetReadLimit(1 << 20) // 1 MiB

	c.mu.Lock()
	c.conn = conn
	c.alive = true
	c.cancel = cancel
	c.channel = channel
	c.mu.Unlock()

	// Send connect command.
	if err := c.sendConnect(wsCtx, info); err != nil {
		c.closeConn()
		return fmt.Errorf("chzzk send connect: %w", err)
	}

	// Start background loops.
	go c.readLoop(wsCtx)
	go c.pingLoop(wsCtx)

	return nil
}

func (c *ChzzkConnector) Disconnect() error {
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

func (c *ChzzkConnector) IsAlive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive
}

func (c *ChzzkConnector) Messages() <-chan model.ChatMessage {
	return c.msgCh
}

func (c *ChzzkConnector) Errors() <-chan error {
	return c.errCh
}

// ---------- internal helpers ----------

func (c *ChzzkConnector) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alive = false
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		if err := c.conn.Close(websocket.StatusGoingAway, "error"); err != nil {
			slog.Debug("chzzk websocket close failed", "error", err)
		}
	}
}

// fetchChatInfo retrieves chatChannelId and accessToken from Chzzk APIs.
func (c *ChzzkConnector) fetchChatInfo(ctx context.Context, channelID string) (*chzzkChatInfo, error) {
	chatChannelID, err := c.fetchChatChannelID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	accessToken, err := c.fetchAccessToken(ctx, chatChannelID)
	if err != nil {
		return nil, err
	}
	return &chzzkChatInfo{
		ChatChannelID: chatChannelID,
		AccessToken:   accessToken,
	}, nil
}

func (c *ChzzkConnector) fetchChatChannelID(ctx context.Context, channelID string) (string, error) {
	reqURL := fmt.Sprintf(chzzkLiveDetailURL, channelID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", chzzkUserAgent)

	resp, err := DoWithRetry(ctx, c.httpClient, req, DefaultRetryConfig())
	if err != nil {
		return "", fmt.Errorf("live-detail request: %w", err)
	}
	defer resp.Body.Close()

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read live-detail body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("live-detail request failed: %s", resp.Status)
	}

	var result struct {
		Content struct {
			ChatChannelID string `json:"chatChannelId"`
			Status        string `json:"status"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse live-detail: %w", err)
	}
	if result.Content.ChatChannelID == "" {
		return "", fmt.Errorf("chatChannelId empty (status=%s)", result.Content.Status)
	}
	return result.Content.ChatChannelID, nil
}

func (c *ChzzkConnector) fetchAccessToken(ctx context.Context, chatChannelID string) (string, error) {
	reqURL := fmt.Sprintf(chzzkAccessTokenURL, chatChannelID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", chzzkUserAgent)
	req.Header.Set("Origin", chzzkOrigin)
	req.Header.Set("Referer", "https://chzzk.naver.com/")

	resp, err := DoWithRetry(ctx, c.httpClient, req, DefaultRetryConfig())
	if err != nil {
		return "", fmt.Errorf("access-token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read access-token body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("access-token request failed: %s", resp.Status)
	}

	var result struct {
		Content struct {
			AccessToken string `json:"accessToken"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse access-token: %w", err)
	}
	return result.Content.AccessToken, nil
}

// sendConnect sends the initial connection command on the WebSocket.
func (c *ChzzkConnector) sendConnect(ctx context.Context, info *chzzkChatInfo) error {
	cmd := map[string]interface{}{
		"ver":   "2",
		"cmd":   chzzkCmdConnect,
		"svcid": "game",
		"cid":   info.ChatChannelID,
		"bdy": map[string]interface{}{
			"uid":     nil,
			"devType": 2001,
			"accTkn":  info.AccessToken,
			"auth":    "READ",
		},
		"tid": 1,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

// pingLoop sends two keepalives every chzzkPingInterval:
//
//  1. App-level cmd=10000 (Chzzk protocol keepalive). NAVER does not reply
//     with a data frame, but write failure surfaces TCP-level death.
//  2. WS-level RFC 6455 Ping. The server replies reliably with a Pong in
//     sustained load tests. conn.Ping
//     blocks until pong arrives or the 5s pingCtx expires.
//
// Either failure pushes to errCh so manager.handleErrors can reap the
// half-open connection — addressing the prior bug where ping write
// failures only logged and returned silently, leaving readLoop to block
// forever. The two keepalives are complementary: the app-level write
// detects an unwritable socket immediately; the WS Ping detects a
// half-open socket where writes succeed (kernel buffers them) but no
// frames flow back.
func (c *ChzzkConnector) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(chzzkPingInterval)
	defer ticker.Stop()

	ping, _ := json.Marshal(map[string]interface{}{
		"ver": "2",
		"cmd": chzzkCmdPing,
	})

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

			// (1) App-level keepalive (Chzzk cmd=10000). NAVER does not ack
			// with a data frame, so write success alone means "TCP write
			// path still works"; that's the lower-bound liveness guarantee.
			c.wmu.Lock()
			err := conn.Write(ctx, websocket.MessageText, ping)
			c.wmu.Unlock()
			if err != nil {
				select {
				case c.errCh <- fmt.Errorf("chzzk ping write failed: %w", err):
				default:
				}
				return
			}

			// (2) WS-level Ping. NAVER replies reliably (verified) so this
			// is the primary fast half-open detector.
			pingCtx, pcancel := context.WithTimeout(ctx, chzzkPingTimeout)
			wsErr := conn.Ping(pingCtx)
			pcancel()
			if wsErr != nil {
				select {
				case c.errCh <- fmt.Errorf("chzzk ws ping failed: %w", wsErr):
				default:
				}
				return
			}
		}
	}
}

// readLoop reads messages from the WebSocket and dispatches them. There is
// NO per-read deadline by design — NAVER does not push periodic data frames
// to idle chzzk rooms, so any deadline (60s, 5min, …) ends up reaping
// healthy quiet channels as false-positive zombies. Half-open detection is
// done in pingLoop via conn.Ping; when that fails it pushes an error to
// errCh, manager.handleErrors calls Disconnect(), the parent ctx cancels,
// and this Read returns with ctx.Err() != nil and exits silently.
func (c *ChzzkConnector) readLoop(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		c.alive = false
		c.mu.Unlock()
	}()

	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			// Only report the error if the connector wasn't deliberately
			// torn down (Disconnect cancels ctx, which propagates here).
			if ctx.Err() == nil {
				select {
				case c.errCh <- fmt.Errorf("chzzk ws read: %w", err):
				default:
				}
			}
			return
		}
		c.handleRawMessage(data)
	}
}

// chzzkWSMessage is the top-level WebSocket message envelope.
type chzzkWSMessage struct {
	Cmd int             `json:"cmd"`
	Bdy json.RawMessage `json:"bdy"`
}

// chzzkChatBody represents one chat item from bdy array.
type chzzkChatBody struct {
	UID         string          `json:"uid"`
	Msg         string          `json:"msg"`
	MsgTypeCode int             `json:"msgTypeCode"`
	Profile     json.RawMessage `json:"profile"`
	Extras      json.RawMessage `json:"extras"`
}

// chzzkProfile is the parsed inner profile JSON.
type chzzkProfile struct {
	Nickname string `json:"nickname"`
	UserID   string `json:"userIdHash"`
}

// chzzkExtras contains donation metadata and inline emote mapping.
// Chzzk attaches a token→imageURL map alongside the message text for any
// emote referenced in the body, so the renderer does not need a separate
// emoji-pack fetch to display channel-custom or subscriber emotes.
type chzzkExtras struct {
	PayAmount   int               `json:"payAmount"`
	IsAnonymous bool              `json:"isAnonymous"`
	Emojis      map[string]string `json:"emojis"`
}

// chzzkEmoteTokenRe matches inline emote tokens of the form `{:emojiId:}`
// in CHZZK chat bodies. The id allows letters/digits/underscores/dashes
// (for example, `example_1` or `exampleWave`).
var chzzkEmoteTokenRe = regexp.MustCompile(`\{:([^:}]+):\}`)

// handleRawMessage parses one WebSocket frame and emits ChatMessages.
func (c *ChzzkConnector) handleRawMessage(data []byte) {
	var msg chzzkWSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		slog.Warn("chzzk unmarshal ws message", "error", err)
		return
	}

	switch msg.Cmd {
	case chzzkCmdConnectReply:
		slog.Info("chzzk connected to chat", "channel", c.channel.ChannelID)
	case chzzkCmdPing:
		// Pong - no action needed.
	case chzzkCmdChat, chzzkCmdDonationChat:
		c.parseChatMessages(msg.Bdy)
	}
}

// parseChatMessages parses cmd 93101/93102 body into ChatMessage(s).
func (c *ChzzkConnector) parseChatMessages(bdy json.RawMessage) {
	// bdy can be a single object or an array.
	var bodies []chzzkChatBody
	if err := json.Unmarshal(bdy, &bodies); err != nil {
		// Try single object.
		var single chzzkChatBody
		if err2 := json.Unmarshal(bdy, &single); err2 != nil {
			slog.Warn("chzzk parse chat body", "error", err2)
			return
		}
		bodies = []chzzkChatBody{single}
	}

	now := time.Now()
	for _, b := range bodies {
		// Re-marshal individual body so each message stores only its own raw data.
		perMsgRaw, _ := json.Marshal(b)
		chatMsg, ok := c.convertToMessage(b, perMsgRaw, now)
		if !ok {
			continue
		}
		select {
		case c.msgCh <- chatMsg:
		default:
			if cb := globalConfig.OnMessageDropped; cb != nil {
				cb("chzzk")
			}
		}
	}
}

// convertToMessage transforms a raw Chzzk chat body into a unified ChatMessage.
// Returns ok=false if the body should be dropped (e.g. anonymous presence with empty msg).
func (c *ChzzkConnector) convertToMessage(b chzzkChatBody, raw []byte, now time.Time) (model.ChatMessage, bool) {
	// Chzzk sends platform notices (e.g. "채팅 참여가 제한되었습니다") with uid="SYSTEM_MESSAGE"
	// over the same cmd=93101 frame as regular chat. Mark them as system so downstream
	// consumers (overlay, keyword-alert, ranking MV) can filter consistently.
	if b.UID == "SYSTEM_MESSAGE" {
		return model.ChatMessage{
			ID:           fmt.Sprintf("chzzk-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
			Type:         model.MessageTypeSystem,
			Platform:     model.PlatformChzzk,
			ChannelID:    c.channel.ChannelID,
			StreamerName: c.channel.StreamerName,
			UserID:       b.UID,
			Nickname:     "system",
			Message:      b.Msg,
			Timestamp:    now,
			Raw:          string(raw),
		}, true
	}

	// Anonymous presence events (uid="anonymous", non-donation, empty msg) are not chat.
	// Drop them — keyword-alert can't match an empty message and overlay shouldn't render "? : ".
	if b.UID == "anonymous" && b.MsgTypeCode != chzzkMsgTypeDonation && b.Msg == "" {
		return model.ChatMessage{}, false
	}

	// Parse profile. Chzzk sends profile as a double-encoded JSON string:
	// "profile": "{\"nickname\":\"...\",\"userIdHash\":\"...\"}"
	// First unmarshal extracts the inner JSON string, second parses it.
	var profile chzzkProfile
	if len(b.Profile) > 0 {
		var inner string
		if json.Unmarshal(b.Profile, &inner) == nil && inner != "" {
			_ = json.Unmarshal([]byte(inner), &profile)
		} else {
			_ = json.Unmarshal(b.Profile, &profile)
		}
	}

	nickname := profile.Nickname
	if nickname == "" {
		nickname = "?"
	}
	userID := profile.UserID
	if userID == "" {
		userID = b.UID
	}

	// Parse extras once: it carries donation metadata AND the inline
	// emote map. Both regular chat and donation frames may include emojis.
	var extras chzzkExtras
	if len(b.Extras) > 0 {
		var innerExtras string
		if json.Unmarshal(b.Extras, &innerExtras) == nil && innerExtras != "" {
			_ = json.Unmarshal([]byte(innerExtras), &extras)
		} else {
			_ = json.Unmarshal(b.Extras, &extras)
		}
	}

	msg := model.ChatMessage{
		ID:           fmt.Sprintf("chzzk-%s-%d", c.channel.ChannelID, c.seq.Add(1)),
		Type:         model.MessageTypeChat,
		Platform:     model.PlatformChzzk,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		UserID:       userID,
		Nickname:     nickname,
		Message:      b.Msg,
		Timestamp:    now,
		Raw:          string(raw),
		Emotes:       extractChzzkEmotes(b.Msg, extras.Emojis),
	}

	// Check for donation (msgTypeCode == 10).
	if b.MsgTypeCode == chzzkMsgTypeDonation {
		msg.Type = model.MessageTypeDonation
		msg.Amount = float64(extras.PayAmount)
		msg.Currency = "CHZZK_CHEESE"
		msg.AmountKRW = int64(extras.PayAmount)

		if extras.IsAnonymous {
			msg.Nickname = "익명"
		}
	}

	return msg, true
}

// extractChzzkEmotes scans body for `{:code:}` tokens and returns one
// EmoteToken per occurrence. The image URL is taken from the inline
// emojis map shipped on the chat frame; tokens missing from the map
// are still emitted so the renderer can decide whether to fall back to
// text or hit the channel emoji-pack catalog.
func extractChzzkEmotes(body string, emojis map[string]string) []model.EmoteToken {
	if body == "" {
		return nil
	}
	matches := chzzkEmoteTokenRe.FindAllStringSubmatchIndex(body, -1)
	if len(matches) == 0 {
		return nil
	}
	tokens := make([]model.EmoteToken, 0, len(matches))
	for _, m := range matches {
		// m: [matchStart, matchEnd, codeStart, codeEnd]
		code := body[m[2]:m[3]]
		tokens = append(tokens, model.EmoteToken{
			Code:     code,
			Start:    m[0],
			End:      m[1],
			ImageURL: emojis[code],
			Source:   "chzzk:inline",
		})
	}
	return tokens
}

// Compile-time interface check.
var _ PlatformConnector = (*ChzzkConnector)(nil)
