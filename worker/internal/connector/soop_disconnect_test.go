package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gorilla "github.com/gorilla/websocket"
)

func TestSoopDisconnectClosesSocketAlreadyMarkedDead(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&gorilla.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		close(closed)
	}))
	defer server.Close()
	conn, _, err := gorilla.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewSoopConnector("", nil)
	c.conn = conn
	c.cancel = cancel
	c.alive = false
	if err = c.Disconnect(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("dead connector left socket open")
	}
	if ctx.Err() == nil {
		t.Fatal("connector child context not cancelled")
	}
	if err = c.Disconnect(); err != nil {
		t.Fatal("disconnect is not idempotent", err)
	}
}
