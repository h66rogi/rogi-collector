package soopauth

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func fixture(now time.Time) snapshot {
	return snapshot{Version: 1, UpdatedAt: now.Unix(), Cookies: []Cookie{
		{Name: "AuthTicket", Value: "synthetic-auth", Domain: ".sooplive.com", Path: "/", Secure: true},
		{Name: "UserTicket", Value: "uid%3Dfixture-viewer", Domain: ".sooplive.com", Path: "/", Secure: true},
	}}
}
func writeSnapshot(t *testing.T, file string, s snapshot) {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	temp := file + ".tmp"
	if err = os.WriteFile(temp, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(temp, file); err != nil {
		t.Fatal(err)
	}
}
func TestReloadDomainPathAndExpiry(t *testing.T) {
	now := time.Now()
	file := filepath.Join(t.TempDir(), "cookies.json")
	s := fixture(now)
	s.Cookies = append(s.Cookies,
		Cookie{Name: "other", Value: "x", Domain: ".sooplive.co.kr", Path: "/"},
		Cookie{Name: "hostonly", Value: "x", Domain: "sooplive.co.kr", Path: "/"},
		Cookie{Name: "wrongpath", Value: "x", Domain: ".sooplive.com", Path: "/afreeca/private"},
		Cookie{Name: "expired", Value: "x", Domain: ".sooplive.com", Path: "/", Expires: now.Add(-time.Second).Unix()},
		Cookie{Name: "scoped", Value: "yes", Domain: ".sooplive.com", Path: "/afreeca"})
	writeSnapshot(t, file, s)
	target, _ := url.Parse("https://live.sooplive.com/afreeca/player_live_api.php")
	header, err := Header(file, target, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"other=", "hostonly=", "wrongpath=", "expired="} {
		if strings.Contains(header, name) {
			t.Fatal("cookie escaped scope:", name)
		}
	}
	if !strings.HasPrefix(header, "scoped=yes;") {
		t.Fatal("longest path must be first")
	}
	s.Cookies[0].Value = "new-synthetic-auth"
	writeSnapshot(t, file, s)
	header, err = Header(file, target, now)
	if err != nil || !strings.Contains(header, "AuthTicket=new-synthetic-auth") {
		t.Fatal("atomic refresh not observed")
	}
}
func TestInvalidMissingStaleAndExpiredAuthFailClosed(t *testing.T) {
	now := time.Now()
	file := filepath.Join(t.TempDir(), "cookies.json")
	target, _ := url.Parse("https://live.sooplive.com/afreeca/player_live_api.php")
	if _, err := Header(file, target, now); err == nil {
		t.Fatal("missing accepted")
	}
	os.WriteFile(file, []byte(`{"cookies":"synthetic-private-content"}`), 0600)
	if _, err := Header(file, target, now); err == nil || strings.Contains(err.Error(), "synthetic-private-content") {
		t.Fatal("invalid snapshot handling")
	}
	s := fixture(now.Add(-49 * time.Hour))
	writeSnapshot(t, file, s)
	if _, err := Header(file, target, now); err == nil {
		t.Fatal("stale accepted")
	}
	s = fixture(now)
	s.Cookies[0].Expires = now.Add(-time.Second).Unix()
	writeSnapshot(t, file, s)
	if _, err := Header(file, target, now); err == nil {
		t.Fatal("expired auth accepted")
	}
	s = fixture(now)
	s.Cookies[0].Value = "inject\r\nHeader: x"
	writeSnapshot(t, file, s)
	if _, err := Header(file, target, now); err == nil {
		t.Fatal("header injection accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestTransportRestrictsEndpointAndStopsRedirect(t *testing.T) {
	now := time.Now()
	file := filepath.Join(t.TempDir(), "cookies.json")
	writeSnapshot(t, file, fixture(now))
	calls := 0
	client := NewHTTPClient(file)
	client.Transport.(*fileTransport).base = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if !strings.Contains(r.Header.Get("Cookie"), "AuthTicket=") {
			t.Error("no cookie")
		}
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"https://example.invalid/leak"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})
	resp, err := client.Get("https://live.sooplive.com/afreeca/player_live_api.php?bjid=fixture-channel")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls != 1 {
		t.Fatal("followed redirect")
	}
	for _, raw := range []string{"https://live.sooplive.com.evil.invalid/afreeca/player_live_api.php", "http://live.sooplive.com/afreeca/player_live_api.php", "https://live.sooplive.com/other", "https://relay.sooplive.com/Websocket/test"} {
		if _, err := client.Get(raw); err == nil {
			t.Fatal("unsupported target accepted")
		}
	}
	if calls != 1 {
		t.Fatal("credentials sent to untrusted target")
	}
}

// Verify the real Python writer and Go reader agree on the snapshot contract.
func TestPythonProducerToGoConsumer(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cookies.json")
	_, source, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(source), "..", "..", "cookie-auth", "service.py")
	program := `import importlib.util,sys
s=importlib.util.spec_from_file_location('cookie_service',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
def login():
 return [{'name':'AuthTicket','value':'synthetic-cross-language','domain':'.sooplive.com','path':'/','secure':True},{'name':'UserTicket','value':'uid%3Dfixture-viewer','domain':'.sooplive.com','path':'/','secure':True}]
m.CookieManager(sys.argv[2],'fixture-viewer',login).refresh()
`
	if output, err := exec.Command("python3", "-c", program, script, file).CombinedOutput(); err != nil {
		t.Fatalf("producer failed: %v %s", err, output)
	}
	target, _ := url.Parse("https://live.sooplive.com/afreeca/player_live_api.php")
	header, err := Header(file, target, time.Now())
	if err != nil || !strings.Contains(header, "AuthTicket=synthetic-cross-language") {
		t.Fatal("Python/Go snapshot contract failed")
	}
}
