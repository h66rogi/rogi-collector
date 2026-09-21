package discovery

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type cookieRoundTripper func(*http.Request) (*http.Response, error)

func (f cookieRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSoopDiscoveryReadsUpdatedCookieFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cookies.json")
	write := func(value string) {
		t.Helper()
		data := fmt.Sprintf(`{"version":1,"updated_at":%d,"cookies":[{"name":"AuthTicket","value":"%s","domain":".sooplive.com","path":"/","secure":true},{"name":"UserTicket","value":"uid%%3Dfixture-viewer","domain":".sooplive.com","path":"/","secure":true}]}`, time.Now().Unix(), value)
		if err := os.WriteFile(file, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	original := http.DefaultTransport
	http.DefaultTransport = cookieRoundTripper(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.Header.Get("Cookie"))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"CHANNEL":{"CHATNO":"synthetic-chat"}}`)), Request: r}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = original })
	d := NewSoopDiscovery(nil, file)
	for _, value := range []string{"first-synthetic", "second-synthetic"} {
		write(value)
		live, err := d.IsChannelLive(context.Background(), "fixture-channel")
		if err != nil || !live {
			t.Fatal("authenticated live check failed", err)
		}
		if !strings.Contains(seen[len(seen)-1], "AuthTicket="+value) {
			t.Fatal("latest cookie not sent")
		}
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if _, err := d.IsChannelLive(context.Background(), "fixture-channel"); err == nil {
		t.Fatal("missing cookie incorrectly became offline/success")
	}
	if len(seen) != 2 {
		t.Fatal("anonymous fallback occurred")
	}
}
