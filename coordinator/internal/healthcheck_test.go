package internal

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
)

func TestHealthChecker_NewHealthChecker(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	hc := NewHealthChecker(nil, nil, 30*time.Second, logger)

	if hc == nil {
		t.Fatal("expected non-nil HealthChecker")
	}
	if hc.timeout != 30*time.Second {
		t.Errorf("expected timeout 30s, got %s", hc.timeout)
	}
}

func TestHealthCheckResult_EmptyStruct(t *testing.T) {
	result := &HealthCheckResult{
		WorkerLoads: make(map[string]model.WorkerLoad),
	}

	if len(result.AliveWorkers) != 0 {
		t.Errorf("expected empty AliveWorkers, got %d", len(result.AliveWorkers))
	}
	if len(result.DeadWorkers) != 0 {
		t.Errorf("expected empty DeadWorkers, got %d", len(result.DeadWorkers))
	}
	if len(result.WorkerLoads) != 0 {
		t.Errorf("expected empty WorkerLoads, got %d", len(result.WorkerLoads))
	}
}
