package model

import (
	"math"
	"testing"
)

func TestWorkerLoad_ConnectionBottleneck(t *testing.T) {
	load := WorkerLoad{
		WorkerID:     "worker-1",
		Connections:  800,
		MsgPerSec:    100,
		MaxConn:      1000,
		MaxMsgPerSec: 5000,
	}

	got := load.Load()
	// connRatio = 800/1000 = 0.8, msgRatio = 100/5000 = 0.02
	expected := 0.8

	if math.Abs(got-expected) > 1e-9 {
		t.Errorf("expected load=%.4f (connection bottleneck), got %.4f", expected, got)
	}
}

func TestWorkerLoad_MsgRateBottleneck(t *testing.T) {
	load := WorkerLoad{
		WorkerID:     "worker-2",
		Connections:  100,
		MsgPerSec:    4500,
		MaxConn:      1000,
		MaxMsgPerSec: 5000,
	}

	got := load.Load()
	// connRatio = 100/1000 = 0.1, msgRatio = 4500/5000 = 0.9
	expected := 0.9

	if math.Abs(got-expected) > 1e-9 {
		t.Errorf("expected load=%.4f (msg rate bottleneck), got %.4f", expected, got)
	}
}

func TestWorkerLoad_ZeroCapacity(t *testing.T) {
	load := WorkerLoad{
		WorkerID:     "worker-3",
		Connections:  50,
		MsgPerSec:    100,
		MaxConn:      0,
		MaxMsgPerSec: 0,
	}

	got := load.Load()
	if got != 0 {
		t.Errorf("expected load=0 with zero capacity, got %.4f", got)
	}
}
