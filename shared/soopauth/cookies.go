// Package soopauth reads cookie snapshots produced by the cookie-auth component.
// Credentials and browser automation stay outside the collector processes.
package soopauth

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const maxSnapshotBytes = 256 * 1024
const maxSnapshotAge = 48 * time.Hour

var ErrCookieUnavailable = errors.New("soop cookie unavailable: refresh required")

type Cookie struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	Domain  string `json:"domain"`
	Path    string `json:"path"`
	Secure  bool   `json:"secure"`
	Expires int64  `json:"expires"`
}

type snapshot struct {
	Version   int      `json:"version"`
	UpdatedAt int64    `json:"updated_at"`
	Cookies   []Cookie `json:"cookies"`
}

// Header reloads after atomic replacement and respects domain, path and expiry.
// Never return raw snapshot/parser errors: they may contain cookie material.
func Header(file string, target *url.URL, now time.Time) (string, error) {
	if target == nil || target.Scheme != "https" || target.User != nil || !allowedHost(target.Hostname()) {
		return "", errors.New("soop cookie target rejected")
	}
	f, err := os.Open(file)
	if err != nil {
		return "", ErrCookieUnavailable
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxSnapshotBytes+1))
	if err != nil || len(data) > maxSnapshotBytes {
		return "", ErrCookieUnavailable
	}
	var state snapshot
	if json.Unmarshal(data, &state) != nil || state.Version != 1 || state.UpdatedAt <= 0 || len(state.Cookies) > 300 {
		return "", ErrCookieUnavailable
	}
	age := now.Sub(time.Unix(state.UpdatedAt, 0))
	if age < -time.Minute || age >= maxSnapshotAge {
		return "", ErrCookieUnavailable
	}
	host := strings.ToLower(target.Hostname())
	path := target.EscapedPath()
	if path == "" {
		path = "/"
	}
	var matched []Cookie
	auth, user := false, false
	for _, c := range state.Cookies {
		domain := strings.ToLower(c.Domain)
		base := strings.TrimPrefix(domain, ".")
		if !allowedHost(base) || !(host == base || (strings.HasPrefix(domain, ".") && strings.HasSuffix(host, "."+base))) {
			continue
		}
		if !strings.HasPrefix(c.Path, "/") || !pathMatches(path, c.Path) {
			continue
		}
		if c.Expires < 0 || (c.Expires != 0 && now.Unix() >= c.Expires) {
			continue
		}
		cookie := &http.Cookie{Name: c.Name, Value: c.Value, Path: c.Path, Secure: c.Secure}
		if c.Value == "" || cookie.Valid() != nil {
			return "", ErrCookieUnavailable
		}
		matched = append(matched, c)
		auth = auth || c.Name == "AuthTicket"
		user = user || c.Name == "UserTicket"
	}
	if !auth || !user {
		return "", ErrCookieUnavailable
	}
	sort.SliceStable(matched, func(i, j int) bool { return len(matched[i].Path) > len(matched[j].Path) })
	req := &http.Request{Header: make(http.Header)}
	for _, c := range matched {
		req.AddCookie(&http.Cookie{Name: c.Name, Value: c.Value})
	}
	return req.Header.Get("Cookie"), nil
}

func allowedHost(host string) bool {
	host = strings.ToLower(host)
	for _, root := range []string{"sooplive.co.kr", "sooplive.com"} {
		if host == root || strings.HasSuffix(host, "."+root) {
			return true
		}
	}
	return false
}
func pathMatches(path, cookiePath string) bool {
	return path == cookiePath || (strings.HasPrefix(path, cookiePath) && (strings.HasSuffix(cookiePath, "/") || path[len(cookiePath)] == '/'))
}

type fileTransport struct {
	file string
	base http.RoundTripper
}

func (t *fileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Authenticated requests are limited to the existing, fixed player API.
	// Cookies are never forwarded to platform-provided websocket/relay hosts.
	if req.URL.Scheme != "https" || req.URL.Host != "live.sooplive.com" || req.URL.Path != "/afreeca/player_live_api.php" {
		return nil, errors.New("soop authenticated endpoint rejected")
	}
	header, err := Header(t.file, req.URL, time.Now())
	if err != nil {
		return nil, err
	}
	clone := req.Clone(req.Context())
	clone.Header.Set("Cookie", header)
	return t.base.RoundTrip(clone)
}

func NewHTTPClient(file string) *http.Client {
	return &http.Client{Timeout: 10 * time.Second,
		Transport:     &fileTransport{file: file, base: http.DefaultTransport},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
}
