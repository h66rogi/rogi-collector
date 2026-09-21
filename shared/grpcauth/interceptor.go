// Package grpcauth provides small authentication interceptors for internal
// gRPC services. Transport security remains the deployer's responsibility.
package grpcauth

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor requires an API key on every unary request.
func UnaryServerInterceptor(apiKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !authorized(ctx, apiKey) {
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor requires an API key on every streaming request.
func StreamServerInterceptor(apiKey string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !authorized(stream.Context(), apiKey) {
			return status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return handler(srv, stream)
	}
}

func authorized(ctx context.Context, apiKey string) bool {
	if apiKey == "" {
		return false
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	provided := first(md.Get("x-api-key"))
	if provided == "" {
		authorization := first(md.Get("authorization"))
		if len(authorization) > len("Bearer ") && strings.EqualFold(authorization[:len("Bearer ")], "Bearer ") {
			provided = strings.TrimSpace(authorization[len("Bearer "):])
		}
	}
	return len(provided) == len(apiKey) && subtle.ConstantTimeCompare([]byte(provided), []byte(apiKey)) == 1
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
