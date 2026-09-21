package grpcauth

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestAuthorized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		key    string
		pairs  []string
		wanted bool
	}{
		{name: "x-api-key", key: "test-key", pairs: []string{"x-api-key", "test-key"}, wanted: true},
		{name: "bearer", key: "test-key", pairs: []string{"authorization", "Bearer test-key"}, wanted: true},
		{name: "wrong key", key: "test-key", pairs: []string{"x-api-key", "wrong"}},
		{name: "empty configured key", pairs: []string{"x-api-key", "anything"}},
		{name: "missing metadata", key: "test-key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if len(tt.pairs) > 0 {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(tt.pairs...))
			}
			if got := authorized(ctx, tt.key); got != tt.wanted {
				t.Fatalf("authorized() = %v, want %v", got, tt.wanted)
			}
		})
	}
}
