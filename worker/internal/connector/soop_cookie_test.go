package connector

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSoopFactoryWiresCookieFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "cookies.json")
	data := fmt.Sprintf(`{"version":1,"updated_at":%d,"cookies":[{"name":"AuthTicket","value":"synthetic-worker-auth","domain":".sooplive.com","path":"/","secure":true},{"name":"UserTicket","value":"uid%%3Dfixture-viewer","domain":".sooplive.com","path":"/","secure":true}]}`, time.Now().Unix())
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	oldTransport, oldConfig := http.DefaultTransport, globalConfig
	t.Cleanup(func() { http.DefaultTransport = oldTransport; globalConfig = oldConfig })
	calls := 0
	http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if !strings.Contains(r.Header.Get("Cookie"), "AuthTicket=synthetic-worker-auth") {
			t.Error("factory did not wire cookie source")
		}
		return httpResponse(http.StatusOK, `{"CHANNEL":{"CHDOMAIN":"chat.sooplive.com","CHPT":3700,"CHATNO":"fixture-chat","FTK":"synthetic-ftk"}}`), nil
	})
	SetConnectorConfig(ConnectorConfig{SoopCookieFile: file})
	connector, err := NewConnector("soop")
	if err != nil {
		t.Fatal(err)
	}
	info, err := connector.(*SoopConnector).fetchChannelInfo(context.Background(), "fixture-channel")
	if err != nil || info.ChatNo != "fixture-chat" {
		t.Fatal("authenticated info failed", err)
	}
	if calls != 1 {
		t.Fatal("unexpected API calls")
	}
}
