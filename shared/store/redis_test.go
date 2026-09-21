package store

import (
	"encoding/json"
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func TestCoordEvent_JSONRoundTrip(t *testing.T) {
	original := CoordEvent{
		Type:      CoordEventAssign,
		Platform:  model.PlatformChzzk,
		ChannelID: "ch-123",
		WorkerID:  "worker-1",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded CoordEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("type: expected %q, got %q", original.Type, decoded.Type)
	}
	if decoded.Platform != original.Platform {
		t.Errorf("platform: expected %q, got %q", original.Platform, decoded.Platform)
	}
	if decoded.ChannelID != original.ChannelID {
		t.Errorf("channelId: expected %q, got %q", original.ChannelID, decoded.ChannelID)
	}
	if decoded.WorkerID != original.WorkerID {
		t.Errorf("workerId: expected %q, got %q", original.WorkerID, decoded.WorkerID)
	}
}

func TestWorkerCommand_JSONRoundTrip(t *testing.T) {
	original := WorkerCommand{
		Type:      WorkerCommandConnect,
		Platform:  model.PlatformSoop,
		ChannelID: "ch-456",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded WorkerCommand
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("type: expected %q, got %q", original.Type, decoded.Type)
	}
	if decoded.Platform != original.Platform {
		t.Errorf("platform: expected %q, got %q", original.Platform, decoded.Platform)
	}
	if decoded.ChannelID != original.ChannelID {
		t.Errorf("channelId: expected %q, got %q", original.ChannelID, decoded.ChannelID)
	}
}

func TestWorkerLoad_JSONRoundTrip(t *testing.T) {
	original := model.WorkerLoad{
		WorkerID:     "worker-1",
		Connections:  500,
		MsgPerSec:    1234.56,
		MaxConn:      2000,
		MaxMsgPerSec: 5000,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded model.WorkerLoad
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.WorkerID != original.WorkerID {
		t.Errorf("workerId: expected %q, got %q", original.WorkerID, decoded.WorkerID)
	}
	if decoded.Connections != original.Connections {
		t.Errorf("connections: expected %d, got %d", original.Connections, decoded.Connections)
	}
	if decoded.MsgPerSec != original.MsgPerSec {
		t.Errorf("msgPerSec: expected %f, got %f", original.MsgPerSec, decoded.MsgPerSec)
	}
	if decoded.MaxConn != original.MaxConn {
		t.Errorf("maxConn: expected %d, got %d", original.MaxConn, decoded.MaxConn)
	}
	if decoded.MaxMsgPerSec != original.MaxMsgPerSec {
		t.Errorf("maxMsgPerSec: expected %f, got %f", original.MaxMsgPerSec, decoded.MaxMsgPerSec)
	}
}
