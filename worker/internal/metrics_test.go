package internal

import (
	"testing"
)

func TestNewMetricsRegistryExportsWorkerMetrics(t *testing.T) {
	registry, m := NewMetricsRegistry()
	m.Ready.Set(1)

	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	names := make(map[string]struct{}, len(metricFamilies))
	for _, mf := range metricFamilies {
		names[mf.GetName()] = struct{}{}
	}

	// Only check scalar (non-Vec) metrics, which appear in Gather() output
	// without any label initialization. Vec metrics only appear after at
	// least one WithLabelValues call.
	for _, name := range []string{
		"meloming_chat_worker_ready",
		"meloming_chat_worker_publish_duration_seconds",
		"meloming_chat_worker_load_ratio",
		"meloming_chat_worker_heartbeat_duration_seconds",
		"meloming_chat_worker_relay_connection_duration_seconds",
	} {
		if _, ok := names[name]; !ok {
			t.Fatalf("expected metric %q to be registered", name)
		}
	}
}
