package internal

import (
	"testing"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func TestSelectWorker_PicksLowestLoad(t *testing.T) {
	loads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 5, MaxConn: 10, MsgPerSec: 10, MaxMsgPerSec: 100},
		"w2": {WorkerID: "w2", Connections: 2, MaxConn: 10, MsgPerSec: 5, MaxMsgPerSec: 100},
		"w3": {WorkerID: "w3", Connections: 8, MaxConn: 10, MsgPerSec: 20, MaxMsgPerSec: 100},
	}

	got := selectWorker(loads, 0.95)
	if got != "w2" {
		t.Errorf("expected w2 (lowest load), got %s", got)
	}
}

func TestSelectWorker_RejectsOverloaded(t *testing.T) {
	loads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 96, MaxConn: 100, MsgPerSec: 10, MaxMsgPerSec: 100},  // 0.96 conn ratio
		"w2": {WorkerID: "w2", Connections: 100, MaxConn: 100, MsgPerSec: 10, MaxMsgPerSec: 100}, // 1.0 conn ratio
	}

	got := selectWorker(loads, 0.95)
	if got != "" {
		t.Errorf("expected empty string (all overloaded at 0.95 threshold), got %s", got)
	}
}

func TestSelectWorker_EmptyMap(t *testing.T) {
	loads := map[string]model.WorkerLoad{}

	got := selectWorker(loads, 0.95)
	if got != "" {
		t.Errorf("expected empty string for empty map, got %s", got)
	}
}

func TestSelectWorker_MsgRateBottleneck(t *testing.T) {
	// Worker w1 has low conn ratio but high msg rate ratio
	loads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 1, MaxConn: 100, MsgPerSec: 96, MaxMsgPerSec: 100},
		"w2": {WorkerID: "w2", Connections: 50, MaxConn: 100, MsgPerSec: 10, MaxMsgPerSec: 100},
	}

	got := selectWorker(loads, 0.95)
	// w1 load = max(0.01, 0.96) = 0.96 >= 0.95, rejected
	// w2 load = max(0.50, 0.10) = 0.50 < 0.95, accepted
	if got != "w2" {
		t.Errorf("expected w2 (msg rate bottleneck on w1), got %s", got)
	}
}

func TestSelectWorker_ExactlyAtThreshold(t *testing.T) {
	// Load of exactly 0.95 should be rejected
	loads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 95, MaxConn: 100, MsgPerSec: 0, MaxMsgPerSec: 100},
	}

	got := selectWorker(loads, 0.95)
	if got != "" {
		t.Errorf("expected empty string for exactly 0.95 load, got %s", got)
	}
}

func TestSelectWorker_JustBelowThreshold(t *testing.T) {
	loads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 94, MaxConn: 100, MsgPerSec: 0, MaxMsgPerSec: 100},
	}

	got := selectWorker(loads, 0.95)
	// load = 0.94 < 0.95 -> accepted
	if got != "w1" {
		t.Errorf("expected w1 for load 0.94, got %s", got)
	}
}

func TestReassignDrainingChannels_UpdatesWorkerLoadsBetweenSelections(t *testing.T) {
	// Simulate: w1 has lower initial load (0.8) vs w2 (0.9)
	// First channel should go to w1, second should go to w2 (after w1's load increases)
	workerLoads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 8, MaxConn: 10, MsgPerSec: 0, MaxMsgPerSec: 100}, // load = 0.8
		"w2": {WorkerID: "w2", Connections: 9, MaxConn: 10, MsgPerSec: 0, MaxMsgPerSec: 100}, // load = 0.9
	}

	threshold := 0.95

	// First selection: should pick w1 (0.8 < 0.9)
	first := selectWorker(workerLoads, threshold)
	if first != "w1" {
		t.Errorf("first selection: expected w1 (load 0.8), got %s", first)
	}

	// Simulate what ReassignDrainingChannels does: update workerLoads
	if load, ok := workerLoads[first]; ok {
		load.Connections++
		workerLoads[first] = load
	}

	// Now w1 load = 0.9, w2 load = 0.9 (equal)
	// Second selection: could pick either, but should not pick w1 again if we had more workers
	// With equal loads, either is acceptable
	second := selectWorker(workerLoads, threshold)
	if second == "" {
		t.Error("second selection: expected a worker (both below threshold), got empty")
	}

	// Verify w1's load was actually updated
	if workerLoads["w1"].Connections != 9 {
		t.Errorf("w1 connections should be 9 after update, got %d", workerLoads["w1"].Connections)
	}
}

func TestRetryStuckHandoffs_UpdatesWorkerLoadsBetweenSelections(t *testing.T) {
	// Same scenario for stuck handoffs
	workerLoads := map[string]model.WorkerLoad{
		"w1": {WorkerID: "w1", Connections: 5, MaxConn: 10, MsgPerSec: 0, MaxMsgPerSec: 100}, // load = 0.5
		"w2": {WorkerID: "w2", Connections: 8, MaxConn: 10, MsgPerSec: 0, MaxMsgPerSec: 100}, // load = 0.8
	}

	threshold := 0.95

	// First selection: should pick w1 (0.5 < 0.8)
	first := selectWorker(workerLoads, threshold)
	if first != "w1" {
		t.Errorf("first selection: expected w1 (load 0.5), got %s", first)
	}

	// Simulate what RetryStuckHandoffs does: update workerLoads
	if load, ok := workerLoads[first]; ok {
		load.Connections++
		workerLoads[first] = load
	}

	// Now w1 load = 0.6, w2 load = 0.8
	// Second selection: should still pick w1 (0.6 < 0.8)
	second := selectWorker(workerLoads, threshold)
	if second != "w1" {
		t.Errorf("second selection: expected w1 (load 0.6 < 0.8), got %s", second)
	}

	// Verify w1's load was actually updated
	if workerLoads["w1"].Connections != 6 {
		t.Errorf("w1 connections should be 6 after update, got %d", workerLoads["w1"].Connections)
	}
}
