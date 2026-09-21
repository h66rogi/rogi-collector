package connector

import (
	"context"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func TestSoopChatHostAllowlist(t *testing.T) {
	t.Parallel()

	connector := NewSoopConnector("", nil, "chat.example.invalid")
	for _, host := range []string{"chat.example.invalid", "edge.chat.example.invalid"} {
		if !connector.isAllowedChatHost(host) {
			t.Fatalf("expected %q to be allowed", host)
		}
	}
	for _, host := range []string{"example.invalid", "chat.example.invalid.attacker.test", "127.0.0.1", "localhost", ""} {
		if connector.isAllowedChatHost(host) {
			t.Fatalf("expected %q to be rejected", host)
		}
	}
}

func TestCimeConnectorRejectsNonWSSURL(t *testing.T) {
	t.Parallel()

	connector := NewCimeConnector("http://127.0.0.1/chat")
	err := connector.Connect(context.Background(), model.LiveChannel{ChannelID: "synthetic-channel"})
	if err == nil {
		t.Fatal("expected non-WSS endpoint to be rejected")
	}
}
