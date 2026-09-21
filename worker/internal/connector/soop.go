package connector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gorilla "github.com/gorilla/websocket"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/h66rogi/rogi-collector/shared/soopauth"
	"github.com/prometheus/client_golang/prometheus"
)

// soopUserIDSuffix strips the "(N)" tier suffix SOOP appends to user IDs
// for subscribers (e.g. "example-user(2)" → "example-user").
var soopUserIDSuffix = regexp.MustCompile(`\(\d+\)$`)

const (
	soopOrigin       = "https://play.sooplive.com"
	soopUserAgent    = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
	soopPlayerAPIURL = "https://live.sooplive.com/afreeca/player_live_api.php?bjid=%s"
	soopPingInterval = 60 * time.Second
	// soopReadTimeout is the per-read deadline. If no data (chat frame, app keepalive,
	// WS ping/pong) arrives within this window, readLoop returns a timeout error so
	// manager can reap the zombie half-open connection. Chosen as 3× ping interval to
	// tolerate scheduler jitter and occasional missed beats without false positives.
	soopReadTimeout    = 3 * soopPingInterval
	soopWSPingWriteTTL = 5 * time.Second
	soopMsgBufSize     = 256
	soopErrBufSize     = 8
	soopHeaderLen      = 14
	soopFieldSep       = "\f"
)

const (
	soopCmdKeepalive       = 0
	soopCmdLogin           = 1
	soopCmdJoin            = 2
	soopCmdChatMessage     = 5
	soopCmdNotice          = 10
	soopCmdSendBalloon     = 18
	soopCmdChocolate       = 37
	soopCmdSuperChat       = 41
	soopCmdFollowItem      = 91
	soopCmdFollowItemChain = 93
	soopCmdBJNotice        = 104
	soopCmdSendSubscribe   = 108
	soopCmdMissionGift     = 121
)

type soopChatInfo struct {
	Domain string
	Port   int
	ChatNo string
	FTK    string
	Title  string
}

type soopPacket struct {
	Cmd    int
	Ret    int
	Fields []string
	Raw    []byte
}

// RelayMetrics holds Prometheus metrics for the SOOP relay connection.
// It is populated from WorkerMetrics by the factory to avoid an import cycle
// between the connector and internal packages.
type RelayMetrics struct {
	AttemptsTotal   *prometheus.CounterVec
	DurationSeconds prometheus.Histogram
}

// SoopConnector implements PlatformConnector for SOOP.
type SoopConnector struct {
	donationSink        func(context.Context, model.Donation) error
	epoch               string
	startedAt           time.Time
	httpClient          *http.Client
	relayAddr           string // optional relay address (host:port) for proxying WebSocket via SNI
	metrics             *RelayMetrics
	allowedHostSuffixes []string

	joinRequested bool
	joined        bool
	lastReceived  atomic.Int64
	joinedCh      chan struct{} // signalled only after a successful join acknowledgement

	mu      sync.Mutex
	wmu     sync.Mutex // write-serialization for conn.WriteMessage
	conn    *gorilla.Conn
	alive   bool
	cancel  context.CancelFunc
	channel model.LiveChannel
	msgCh   chan model.ChatMessage
	errCh   chan error
	seq     atomic.Int64

	chatNo string
	ftk    string
}

// NewSoopConnector creates a new SOOP connector.
// relayAddr is optional; if non-empty, WebSocket connections are routed
// through the relay (for example, "relay.example.invalid:443") while preserving TLS SNI
// for the real SOOP chat hostname.
// metrics is optional; if non-nil, relay connection latency and attempt
// counts are recorded.
func NewSoopConnector(relayAddr string, metrics *RelayMetrics, allowedHostSuffixes ...string) *SoopConnector {
	connector := &SoopConnector{
		epoch: uuid.NewString(), startedAt: time.Now().UTC(),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		relayAddr:  relayAddr,
		metrics:    metrics,
		msgCh:      make(chan model.ChatMessage, soopMsgBufSize),
		errCh:      make(chan error, soopErrBufSize),
	}
	for _, suffix := range allowedHostSuffixes {
		suffix = strings.ToLower(strings.Trim(strings.TrimSpace(suffix), "."))
		if suffix != "" && net.ParseIP(suffix) == nil && suffix != "localhost" {
			connector.allowedHostSuffixes = append(connector.allowedHostSuffixes, suffix)
		}
	}
	return connector
}

// SetCookieFile configures authentication before Connect is called. Each API request reloads the snapshot.
func (c *SoopConnector) SetDonationSink(sink func(context.Context, model.Donation) error) {
	c.donationSink = sink
}

func (c *SoopConnector) SetCookieFile(path string) {
	c.httpClient = soopauth.NewHTTPClient(path)
}

func (c *SoopConnector) Connect(ctx context.Context, channel model.LiveChannel) error {
	info, err := c.fetchChannelInfo(ctx, channel.ChannelID)
	if err != nil {
		return fmt.Errorf("soop fetch channel info: %w", err)
	}
	if info.ChatNo == "" {
		return fmt.Errorf("soop chatno empty for %s", channel.ChannelID)
	}
	if !c.isAllowedChatHost(info.Domain) {
		return errors.New("soop chat host is not in SOOP_CHAT_HOST_SUFFIXES")
	}

	wsPort := info.Port + 1
	wsURL := fmt.Sprintf("wss://%s:%d/Websocket/%s", info.Domain, wsPort, channel.ChannelID)
	wsCtx, cancel := context.WithCancel(ctx)

	header := http.Header{
		"Origin":     {soopOrigin},
		"User-Agent": {soopUserAgent},
	}

	conn, err := c.dialWithFallback(wsCtx, wsURL, info.Domain, header)
	if err != nil {
		cancel()
		return fmt.Errorf("soop ws dial: %w", err)
	}

	conn.SetReadLimit(1 << 20)
	// Transport-level liveness: arm a read deadline so half-open sockets
	// (silent SOOP drop, NAT eviction, network partition) surface as a read
	// timeout instead of blocking ReadMessage forever. The deadline is
	// extended on each successful frame and on pong.
	if err := conn.SetReadDeadline(time.Now().Add(soopReadTimeout)); err != nil {
		cancel()
		_ = conn.Close()
		return fmt.Errorf("soop set initial read deadline: %w", err)
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(soopReadTimeout))
	})

	joinedCh := make(chan struct{}, 1)

	c.mu.Lock()
	c.conn = conn
	c.alive = true
	c.cancel = cancel
	c.channel = channel
	c.chatNo = info.ChatNo
	c.ftk = info.FTK
	c.joinedCh = joinedCh
	c.joinRequested = false
	c.joined = false
	c.mu.Unlock()

	// Start readLoop BEFORE sending login so we can receive the login ACK.
	go c.readLoop(wsCtx)
	go c.pingLoop(wsCtx)

	if err := c.writePacket(soopCmdLogin, soopFieldSep+soopFieldSep+soopFieldSep+"16"+soopFieldSep); err != nil {
		c.closeConn()
		return fmt.Errorf("soop send login: %w", err)
	}

	// Wait for the actual join acknowledgement before reporting success.
	select {
	case <-joinedCh:
		return nil
	case err := <-c.errCh:
		c.closeConn()
		return fmt.Errorf("soop handshake failed for %s: %w", channel.ChannelID, err)
	case <-time.After(10 * time.Second):
		c.closeConn()
		return fmt.Errorf("soop handshake timeout for %s", channel.ChannelID)
	case <-ctx.Done():
		c.closeConn()
		return ctx.Err()
	}
}

func (c *SoopConnector) isAllowedChatHost(host string) bool {
	host = strings.ToLower(strings.Trim(strings.TrimSpace(host), "."))
	if host == "" || net.ParseIP(host) != nil || host == "localhost" {
		return false
	}
	for _, suffix := range c.allowedHostSuffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

// dialWithFallback tries to connect via relay first. If the relay-backed
// WebSocket handshake fails, it falls back to a direct connection.
func (c *SoopConnector) dialWithFallback(ctx context.Context, wsURL, domain string, header http.Header) (*gorilla.Conn, error) {
	dialer := gorilla.Dialer{
		HandshakeTimeout: 15 * time.Second,
		Subprotocols:     []string{"chat"},
	}

	// Try relay first when configured.
	if c.relayAddr != "" {
		relayDialer := dialer
		relayAddr := c.relayAddr
		metrics := c.metrics
		relayDialer.NetDialContext = func(dCtx context.Context, network, _ string) (net.Conn, error) {
			start := time.Now()
			conn, dialErr := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(dCtx, network, relayAddr)
			if metrics != nil {
				metrics.DurationSeconds.Observe(time.Since(start).Seconds())
				if dialErr != nil {
					metrics.AttemptsTotal.WithLabelValues("error").Inc()
				} else {
					metrics.AttemptsTotal.WithLabelValues("success").Inc()
				}
			}
			return conn, dialErr
		}
		relayDialer.TLSClientConfig = &tls.Config{
			ServerName: domain,
		}

		conn, _, err := relayDialer.DialContext(ctx, wsURL, header)
		if err == nil {
			return conn, nil
		}
		// Relay failed — fall through to direct connection.
		slog.Warn("soop relay dial failed, falling back to direct", "error", err)
	}

	// Direct connection (no relay).
	conn, _, err := dialer.DialContext(ctx, wsURL, header)
	return conn, err
}

func (c *SoopConnector) Disconnect() error {
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
		// Serialize close-frame write through wmu to prevent concurrent write race
		// with pingLoop or other writers.
		c.wmu.Lock()
		_ = c.conn.WriteMessage(gorilla.CloseMessage,
			gorilla.FormatCloseMessage(gorilla.CloseNormalClosure, "disconnect"))
		c.wmu.Unlock()
		return c.conn.Close()
	}
	return nil
}

func (c *SoopConnector) IsAlive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive
}

func (c *SoopConnector) Messages() <-chan model.ChatMessage {
	return c.msgCh
}

func (c *SoopConnector) Errors() <-chan error {
	return c.errCh
}

func (c *SoopConnector) closeConn() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alive = false
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			slog.Debug("soop websocket close failed", "error", err)
		}
	}
}

func (c *SoopConnector) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(soopPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// WS-level ping. Expected pong refreshes read deadline via
			// SetPongHandler. Failure here means the socket is already dead.
			if err := c.writeWSPing(); err != nil {
				select {
				case c.errCh <- fmt.Errorf("soop ws ping failed: %w", err):
				default:
				}
				return
			}
			// App-level keepalive (SOOP protocol cmd=0). Kept for server-side
			// compatibility; some SOOP edges may only honor app keepalive.
			if err := c.writePacket(soopCmdKeepalive, soopFieldSep); err != nil {
				// Notify manager so the connection is cleaned up.
				select {
				case c.errCh <- fmt.Errorf("soop ping write failed: %w", err):
				default:
				}
				return
			}
		}
	}
}

// writeWSPing sends a WebSocket control-frame ping. Serialized via wmu to
// avoid interleaving with data writes from pingLoop/login handlers.
func (c *SoopConnector) writeWSPing() error {
	c.mu.Lock()
	conn := c.conn
	alive := c.alive
	c.mu.Unlock()
	if !alive || conn == nil {
		return fmt.Errorf("soop websocket not connected")
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return conn.WriteControl(gorilla.PingMessage, []byte("hb"), time.Now().Add(soopWSPingWriteTTL))
}

func (c *SoopConnector) readLoop(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		c.alive = false
		c.mu.Unlock()
	}()

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				// Classify read-deadline timeouts as zombie reaping so manager
				// can emit a distinct disconnect reason and metric.
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					err = fmt.Errorf("soop ws read timeout after %s: %w", soopReadTimeout, err)
				}
				select {
				case c.errCh <- fmt.Errorf("soop ws read: %w", err):
				default:
				}
			}
			return
		}
		// Successful frame: extend read deadline. Any frame (chat, donation,
		// notice, system) counts as liveness evidence.
		if err := c.conn.SetReadDeadline(time.Now().Add(soopReadTimeout)); err != nil {
			select {
			case c.errCh <- fmt.Errorf("soop refresh read deadline: %w", err):
			default:
			}
			return
		}
		c.handleFrame(ctx, data)
	}
}

func (c *SoopConnector) handleFrame(ctx context.Context, data []byte) {
	for _, packet := range parseSoopPackets(data) {
		c.handlePacket(ctx, packet)
	}
}

func (c *SoopConnector) handlePacket(ctx context.Context, packet soopPacket) {
	c.mu.Lock()
	waiting := c.joinedCh != nil && !c.joined
	c.mu.Unlock()
	if waiting && packet.Cmd != soopCmdLogin && packet.Cmd != soopCmdJoin {
		return
	}
	if packet.Cmd == soopCmdChatMessage || packet.Cmd == soopCmdSendBalloon {
		c.lastReceived.Store(time.Now().UnixNano())
	}

	switch packet.Cmd {
	case soopCmdLogin:
		if packet.Ret != 0 {
			select {
			case c.errCh <- fmt.Errorf("soop login rejected: ret=%d", packet.Ret):
			default:
			}
			return
		}
		joinBody := soopFieldSep + c.chatNo + soopFieldSep + c.ftk + soopFieldSep + "0" + soopFieldSep + soopFieldSep + soopFieldSep
		if err := c.writePacket(soopCmdJoin, joinBody); err != nil {
			select {
			case c.errCh <- fmt.Errorf("soop send join: %w", err):
			default:
			}
			return
		}
		c.mu.Lock()
		c.joinRequested = true
		c.mu.Unlock()
	case soopCmdJoin:
		c.mu.Lock()
		requested := c.joinRequested
		c.mu.Unlock()
		if !requested {
			return
		}
		if packet.Ret != 0 {
			select {
			case c.errCh <- fmt.Errorf("soop join rejected: ret=%d", packet.Ret):
			default:
			}
			return
		}
		c.mu.Lock()
		c.joined = true
		ch := c.joinedCh
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- struct{}{}:
			default:
			}
		}

	case soopCmdChatMessage:
		c.emitMessage(c.convertChatPacket(packet))
	case soopCmdSendBalloon, soopCmdChocolate, soopCmdSuperChat, soopCmdMissionGift:
		if c.donationSink == nil {
			c.emitMessage(c.convertDonationPacket(packet))
			return
		}
		if packet.Cmd != soopCmdSendBalloon {
			c.emitMessage(model.ChatMessage{ID: uuid.NewString(), Platform: model.PlatformSoop, ChannelID: c.channel.ChannelID, Type: model.MessageTypeSystem, Message: "unsupported_donation_kind", Timestamp: time.Now().UTC()})
			return
		}
		count, err := strconv.ParseInt(fieldAt(packet.Fields, 3), 10, 64)
		seq := uint64(c.seq.Add(1))
		donation := model.Donation{EventID: c.epoch + "-" + strconv.FormatUint(seq, 10), ChannelID: c.channel.ChannelID, Count: count, Kind: "balloon", DonorID: soopUserIDSuffix.ReplaceAllString(fieldAt(packet.Fields, 1), ""), DisplayName: fieldAt(packet.Fields, 2), Message: "", ConnectionEpoch: c.epoch, Sequence: seq, ObservedAt: time.Now().UTC(), ConnectionStartedAt: c.startedAt}
		if err == nil {
			err = donation.Validate()
		}
		if err == nil {
			err = c.donationSink(ctx, donation)
		}
		if err != nil {
			select {
			case c.errCh <- errors.New("donation acceptance failed; collection stopped"):
			default:
			}
			c.closeConn()
		}

	case soopCmdFollowItem, soopCmdFollowItemChain, soopCmdSendSubscribe:
		c.emitMessage(c.convertSubscriptionPacket(packet))
	case soopCmdNotice, soopCmdBJNotice:
		c.emitMessage(c.convertSystemPacket(packet))
	}
}

func (c *SoopConnector) emitMessage(msg model.ChatMessage) {
	if msg.ID == "" {
		return
	}
	select {
	case c.msgCh <- msg:
	default:
		if cb := globalConfig.OnMessageDropped; cb != nil {
			cb("soop")
		}
	}
}

func (c *SoopConnector) convertChatPacket(packet soopPacket) model.ChatMessage {
	fields := packet.Fields
	rawText := ""
	if len(fields) > 0 {
		rawText = strings.ReplaceAll(fields[0], "\r", "")
	}
	userID := soopUserIDSuffix.ReplaceAllString(fieldAt(fields, 1), "")
	nickname := fieldAt(fields, 5)
	if nickname == "" {
		nickname = userID
	}
	if nickname == "" {
		nickname = "?"
	}
	// Never infer user identity from a display name.

	return model.ChatMessage{
		ID:           fmt.Sprintf("soop-%s-%s-%d", c.channel.ChannelID, c.epoch, c.seq.Add(1)),
		Type:         model.MessageTypeChat,
		Platform:     model.PlatformSoop,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		UserID:       userID,
		Nickname:     nickname,
		Message:      rawText,
		Timestamp:    time.Now(),
		Raw:          string(packet.Raw),
	}
}

func (c *SoopConnector) convertDonationPacket(packet soopPacket) model.ChatMessage {
	fields := packet.Fields
	userID := fieldAt(fields, 1)
	nickname := fieldAt(fields, 2)
	amount := parseNumericField(fieldAt(fields, 3))
	message := donationMessageForCommand(packet.Cmd, fields)

	if packet.Cmd == soopCmdMissionGift {
		if gift, ok := parseMissionGiftPayload(fields); ok {
			userID = gift.UserID
			nickname = gift.UserNick
			amount = float64(gift.GiftCount)
			message = gift.Message()
		}
	}

	if nickname == "" {
		nickname = userID
	}
	if nickname == "" {
		nickname = "?"
	}

	return model.ChatMessage{
		ID:           fmt.Sprintf("soop-%s-%s-%d", c.channel.ChannelID, c.epoch, c.seq.Add(1)),
		Type:         model.MessageTypeDonation,
		Platform:     model.PlatformSoop,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		UserID:       soopUserIDSuffix.ReplaceAllString(userID, ""),
		Nickname:     nickname,
		Message:      message,
		Timestamp:    time.Now(),
		Raw:          string(packet.Raw),
		Amount:       amount,
		AmountKRW:    int64(amount * 100),
		Currency:     "SOOP_BALLOON",
	}
}

func (c *SoopConnector) convertSubscriptionPacket(packet soopPacket) model.ChatMessage {
	fields := packet.Fields
	nickname := fieldAt(fields, 0)
	if nickname == "" {
		nickname = fieldAt(fields, 1)
	}
	if nickname == "" {
		nickname = "?"
	}

	return model.ChatMessage{
		ID:           fmt.Sprintf("soop-%s-%s-%d", c.channel.ChannelID, c.epoch, c.seq.Add(1)),
		Type:         model.MessageTypeSubscription,
		Platform:     model.PlatformSoop,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		UserID:       soopUserIDSuffix.ReplaceAllString(fieldAt(fields, 1), ""),
		Nickname:     nickname,
		Message:      strings.Join(nonEmptyFields(fields[:min(3, len(fields))]), " "),
		Timestamp:    time.Now(),
		Raw:          string(packet.Raw),
	}
}

func (c *SoopConnector) convertSystemPacket(packet soopPacket) model.ChatMessage {
	message := fieldAt(packet.Fields, 0)
	if message == "" {
		return model.ChatMessage{}
	}

	return model.ChatMessage{
		ID:           fmt.Sprintf("soop-%s-%s-%d", c.channel.ChannelID, c.epoch, c.seq.Add(1)),
		Type:         model.MessageTypeSystem,
		Platform:     model.PlatformSoop,
		ChannelID:    c.channel.ChannelID,
		StreamerName: c.channel.StreamerName,
		Nickname:     "system",
		Message:      message,
		Timestamp:    time.Now(),
		Raw:          string(packet.Raw),
	}
}

func (c *SoopConnector) fetchChannelInfo(ctx context.Context, channelID string) (*soopChatInfo, error) {
	form := url.Values{
		"bid":         {channelID},
		"bno":         {"0"},
		"type":        {"live"},
		"player_type": {"html5"},
	}
	bodyStr := form.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf(soopPlayerAPIURL, channelID), strings.NewReader(bodyStr))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", soopUserAgent)
	req.Header.Set("Referer", fmt.Sprintf("https://play.sooplive.com/%s", channelID))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(bodyStr)), nil
	}

	resp, err := DoWithRetry(ctx, c.httpClient, req, DefaultRetryConfig())
	if err != nil {
		return nil, fmt.Errorf("soop player api: %w", err)
	}
	defer resp.Body.Close()

	body, err := readPlatformResponse(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return nil, soopauth.ErrLoginRequired
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("soop player api failed: %s", resp.Status)
	}

	var result struct {
		Channel struct {
			Result json.RawMessage `json:"RESULT"`
			Domain string          `json:"CHDOMAIN"`
			Port   json.Number     `json:"CHPT"`
			ChatNo string          `json:"CHATNO"`
			FTK    string          `json:"FTK"`
			Title  string          `json:"TITLE"`
		} `json:"CHANNEL"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse soop player api: %w", err)
	}

	live, stateErr := soopauth.LiveResult(result.Channel.Result, result.Channel.ChatNo)
	if stateErr != nil {
		return nil, stateErr
	}
	if !live {
		return nil, soopauth.ErrOffline
	}
	port, portErr := result.Channel.Port.Int64()
	if portErr != nil || port < 1 || port >= 65535 {
		return nil, errors.New("soop invalid chat port")
	}
	return &soopChatInfo{
		Domain: result.Channel.Domain,
		Port:   int(port),
		ChatNo: result.Channel.ChatNo,
		FTK:    result.Channel.FTK,
		Title:  result.Channel.Title,
	}, nil
}

func (c *SoopConnector) writePacket(cmd int, body string) error {
	c.mu.Lock()
	conn := c.conn
	alive := c.alive
	c.mu.Unlock()
	if !alive || conn == nil {
		return fmt.Errorf("soop websocket not connected")
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return conn.WriteMessage(gorilla.BinaryMessage, buildSoopPacket(cmd, body))
}

func buildSoopPacket(cmd int, body string) []byte {
	b := []byte(body)
	header := fmt.Sprintf("\x1b\t%04d%06d00", cmd, len(b))
	return append([]byte(header), b...)
}

func parseSoopPackets(raw []byte) []soopPacket {
	var packets []soopPacket
	for pos := 0; pos+soopHeaderLen <= len(raw); {
		if raw[pos] != 0x1B || raw[pos+1] != 0x09 {
			pos++
			continue
		}

		cmd, err1 := strconv.Atoi(string(raw[pos+2 : pos+6]))
		bodyLen, err2 := strconv.Atoi(string(raw[pos+6 : pos+12]))
		ret, err3 := strconv.Atoi(string(raw[pos+12 : pos+14]))
		if err1 != nil || err2 != nil || err3 != nil || pos+soopHeaderLen+bodyLen > len(raw) {
			pos++
			continue
		}

		frame := raw[pos : pos+soopHeaderLen+bodyLen]
		body := string(raw[pos+soopHeaderLen : pos+soopHeaderLen+bodyLen])
		body = strings.TrimPrefix(body, soopFieldSep)
		body = strings.TrimSuffix(body, soopFieldSep)

		var fields []string
		if body != "" {
			fields = strings.Split(body, soopFieldSep)
		}
		packets = append(packets, soopPacket{
			Cmd:    cmd,
			Ret:    ret,
			Fields: fields,
			Raw:    append([]byte(nil), frame...),
		})
		pos += soopHeaderLen + bodyLen
	}
	return packets
}

func donationMessageForCommand(cmd int, fields []string) string {
	switch cmd {
	case soopCmdSendBalloon:
		return fmt.Sprintf("별풍선 %s개", fieldAt(fields, 3))
	case soopCmdSuperChat:
		return fmt.Sprintf("슈퍼챗 %s", fieldAt(fields, 3))
	case soopCmdChocolate:
		return strings.Join(nonEmptyFields(fields[:min(4, len(fields))]), " ")
	case soopCmdMissionGift:
		if gift, ok := parseMissionGiftPayload(fields); ok {
			return gift.Message()
		}
		return strings.Join(nonEmptyFields(fields), " ")
	default:
		return strings.Join(nonEmptyFields(fields[:min(4, len(fields))]), " ")
	}
}

type soopMissionGiftPayload struct {
	Type      string
	UserID    string
	UserNick  string
	Title     string
	GiftCount int64
}

func (p soopMissionGiftPayload) Message() string {
	title := strings.TrimSpace(p.Title)
	if title == "" {
		return fmt.Sprintf("%s 별풍선 %d개", p.label(), p.GiftCount)
	}
	return fmt.Sprintf("%s 별풍선 %d개: %s", p.label(), p.GiftCount, title)
}

func (p soopMissionGiftPayload) label() string {
	switch strings.ToUpper(strings.TrimSpace(p.Type)) {
	case "CHALLENGE_GIFT":
		return "도전미션"
	case "GIFT":
		return "대결미션"
	default:
		return "미션"
	}
}

func parseMissionGiftPayload(fields []string) (soopMissionGiftPayload, bool) {
	raw := fieldAt(fields, 0)
	if raw == "" {
		return soopMissionGiftPayload{}, false
	}

	var payload struct {
		Type      string          `json:"type"`
		UserID    string          `json:"user_id"`
		UserNick  string          `json:"user_nick"`
		Title     string          `json:"title"`
		GiftCount json.RawMessage `json:"gift_count"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return soopMissionGiftPayload{}, false
	}
	switch strings.ToUpper(strings.TrimSpace(payload.Type)) {
	case "", "GIFT", "CHALLENGE_GIFT":
	default:
		return soopMissionGiftPayload{}, false
	}

	giftCount, ok := parseJSONInt(payload.GiftCount)
	if !ok {
		return soopMissionGiftPayload{}, false
	}

	return soopMissionGiftPayload{
		Type:      payload.Type,
		UserID:    payload.UserID,
		UserNick:  payload.UserNick,
		Title:     payload.Title,
		GiftCount: giftCount,
	}, true
}

func parseJSONInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		return v, err == nil
	}
	return 0, false
}

func parseNumericField(value string) float64 {
	n, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return n
}

func fieldAt(fields []string, idx int) string {
	if idx < 0 || idx >= len(fields) {
		return ""
	}
	return fields[idx]
}

func nonEmptyFields(fields []string) []string {
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			result = append(result, field)
		}
	}
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ PlatformConnector = (*SoopConnector)(nil)

func (c *SoopConnector) LastReceived() time.Time {
	n := c.lastReceived.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
