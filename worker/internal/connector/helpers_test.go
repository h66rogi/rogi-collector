package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func httpResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestNewConnectorFactory(t *testing.T) {
	tests := []struct {
		name string
		fn   func() error
	}{
		{
			name: "chzzk",
			fn: func() error {
				_, err := NewConnector("chzzk")
				return err
			},
		},
		{
			name: "soop",
			fn: func() error {
				_, err := NewConnector("soop")
				return err
			},
		},
		{
			name: "cime",
			fn: func() error {
				_, err := NewConnector("cime")
				return err
			},
		},
	}

	for _, tt := range tests {
		if err := tt.fn(); err != nil {
			t.Fatalf("%s connector creation failed: %v", tt.name, err)
		}
	}

	if _, err := NewConnector("unknown"); err == nil {
		t.Fatal("expected unknown platform to fail")
	}
}

func TestChzzkFetchChatInfo(t *testing.T) {
	c := NewChzzkConnector("")
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(req.URL.Path, "/live-detail"):
			return httpResponse(http.StatusOK, `{"content":{"chatChannelId":"chat-123","status":"OPEN"}}`), nil
		case strings.Contains(req.URL.Path, "/access-token"):
			return httpResponse(http.StatusOK, `{"content":{"accessToken":"token-abc"}}`), nil
		default:
			return nil, errors.New("unexpected path")
		}
	})}

	info, err := c.fetchChatInfo(context.Background(), "channel-1")
	if err != nil {
		t.Fatalf("fetchChatInfo failed: %v", err)
	}
	if info.ChatChannelID != "chat-123" || info.AccessToken != "token-abc" {
		t.Fatalf("unexpected chat info: %+v", info)
	}
}

func TestChzzkFetchChatChannelIDEmpty(t *testing.T) {
	c := NewChzzkConnector("")
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return httpResponse(http.StatusOK, `{"content":{"chatChannelId":"","status":"CLOSE"}}`), nil
	})}

	if _, err := c.fetchChatChannelID(context.Background(), "channel-1"); err == nil {
		t.Fatal("expected empty chatChannelId to fail")
	}
}

func TestChzzkParseChatMessagesSingleObjectFallback(t *testing.T) {
	c := newTestConnector()
	c.parseChatMessages(jsonRaw(`{"uid":"u1","msg":"hello","msgTypeCode":1,"profile":{"nickname":"nick","userIdHash":"u1"}}`))
	msg := <-c.msgCh
	if msg.Message != "hello" || msg.Nickname != "nick" {
		t.Fatalf("unexpected parsed message: %+v", msg)
	}
}

func TestChzzkHandleRawMessageInvalidJSON(t *testing.T) {
	c := newTestConnector()
	c.handleRawMessage([]byte("{not-json"))
	select {
	case <-c.msgCh:
		t.Fatal("did not expect message for invalid json")
	default:
	}
}

func TestSoopFetchChannelInfo(t *testing.T) {
	c := NewSoopConnector("", nil)
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return httpResponse(http.StatusOK, `{"CHANNEL":{"CHDOMAIN":"example.com","CHPT":3700,"CHATNO":"123","FTK":"ftk","TITLE":"title"}}`), nil
	})}

	info, err := c.fetchChannelInfo(context.Background(), "bj-1")
	if err != nil {
		t.Fatalf("fetchChannelInfo failed: %v", err)
	}
	if info.Domain != "example.com" || info.Port != 3700 || info.ChatNo != "123" || info.FTK != "ftk" {
		t.Fatalf("unexpected info: %+v", info)
	}
}

func TestSoopFetchChannelInfoBadResponse(t *testing.T) {
	c := NewSoopConnector("", nil)
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return httpResponse(http.StatusNotFound, `not found`), nil
	})}

	if _, err := c.fetchChannelInfo(context.Background(), "bj-1"); err == nil {
		t.Fatal("expected fetchChannelInfo to fail on non-2xx response")
	}
}

func TestSoopFetchChannelInfoRetries502(t *testing.T) {
	var calls int
	c := NewSoopConnector("", nil)
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls <= 1 {
			return httpResponse(http.StatusBadGateway, `bad gateway`), nil
		}
		return httpResponse(http.StatusOK, `{"CHANNEL":{"CHDOMAIN":"example.com","CHPT":3700,"CHATNO":"123","FTK":"ftk","TITLE":"title"}}`), nil
	})}

	info, err := c.fetchChannelInfo(context.Background(), "bj-1")
	if err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if info.ChatNo != "123" {
		t.Fatalf("unexpected info: %+v", info)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (1 retry + success), got %d", calls)
	}
}

func TestChzzkFetchChatInfoRetries502(t *testing.T) {
	var calls int
	c := NewChzzkConnector("")
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls <= 1 {
			return httpResponse(http.StatusBadGateway, `bad gateway`), nil
		}
		switch {
		case strings.Contains(req.URL.Path, "/live-detail"):
			return httpResponse(http.StatusOK, `{"content":{"chatChannelId":"chat-123","status":"OPEN"}}`), nil
		case strings.Contains(req.URL.Path, "/access-token"):
			return httpResponse(http.StatusOK, `{"content":{"accessToken":"token-abc"}}`), nil
		default:
			return nil, errors.New("unexpected path")
		}
	})}

	info, err := c.fetchChatInfo(context.Background(), "channel-1")
	if err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if info.ChatChannelID != "chat-123" {
		t.Fatalf("unexpected chat info: %+v", info)
	}
	if calls < 2 {
		t.Errorf("expected at least 2 calls (1 retry + success), got %d", calls)
	}
}

func TestSoopHelperFunctions(t *testing.T) {
	if got := donationMessageForCommand(soopCmdSuperChat, []string{"", "", "", "50"}); got != "슈퍼챗 50" {
		t.Fatalf("unexpected donation message: %q", got)
	}
	if got := parseNumericField("bad"); got != 0 {
		t.Fatalf("expected parseNumericField fallback 0, got %f", got)
	}
	if got := fieldAt([]string{"a"}, 1); got != "" {
		t.Fatalf("expected out-of-range field to be empty, got %q", got)
	}
	if got := min(2, 5); got != 2 {
		t.Fatalf("expected min(2,5)=2, got %d", got)
	}
	fields := nonEmptyFields([]string{" a ", "", "b"})
	if len(fields) != 2 || fields[0] != "a" || fields[1] != "b" {
		t.Fatalf("unexpected nonEmptyFields result: %+v", fields)
	}
}

func TestCimeFetchChatToken(t *testing.T) {
	c := NewCimeConnector()
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return httpResponse(http.StatusOK, `{"data":{"token":"token-123"}}`), nil
	})}

	token, err := c.fetchChatToken(context.Background(), "slug-1")
	if err != nil {
		t.Fatalf("fetchChatToken failed: %v", err)
	}
	if token != "token-123" {
		t.Fatalf("unexpected token %q", token)
	}
}

func TestCimeFetchChatTokenBadResponse(t *testing.T) {
	c := NewCimeConnector()
	c.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return httpResponse(http.StatusBadGateway, `bad gateway`), nil
	})}

	if _, err := c.fetchChatToken(context.Background(), "slug-1"); err == nil {
		t.Fatal("expected fetchChatToken to fail on non-2xx")
	}
}

func TestCimeHandleRawMessageUnknownTypeFallsBackToSystem(t *testing.T) {
	c := newTestCimeConnector()
	raw := []byte(`{"Type":"UNKNOWN","Content":"ignored"}`)
	c.handleRawMessage(raw)

	msg := <-c.msgCh
	if msg.Type != model.MessageTypeSystem {
		t.Fatalf("expected system fallback, got %+v", msg)
	}
}

func TestCimeConvertEventMessageWithObjectExtra(t *testing.T) {
	c := newTestCimeConnector()
	msg := c.convertEventMessage(cimeWSMessage{
		Type:      "EVENT",
		EventName: "DONATION_COMPLETED",
		Attributes: map[string]json.RawMessage{
			"extra": jsonRaw(`{"msg":"감사","amt":7000,"anon":true,"prof":{"id":44,"name":"실명"}}`),
		},
	}, []byte(`raw`))

	if msg.Type != model.MessageTypeDonation || msg.AmountKRW != 7000 || msg.Nickname != "익명" {
		t.Fatalf("unexpected donation conversion: %+v", msg)
	}
}

func jsonRaw(s string) []byte {
	return bytes.TrimSpace([]byte(s))
}
