package connector

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// retryableStatusCodes are HTTP status codes that warrant a retry.
var retryableStatusCodes = map[int]bool{
	http.StatusBadGateway:          true, // 502
	http.StatusServiceUnavailable:  true, // 503
	http.StatusGatewayTimeout:      true, // 504
	http.StatusTooManyRequests:     true, // 429
	http.StatusRequestTimeout:      true, // 408
	http.StatusInternalServerError: true, // 500
}

// HTTPRetryConfig configures retry behaviour for HTTP requests.
type HTTPRetryConfig struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
}

// DefaultRetryConfig returns sensible defaults for platform API calls.
func DefaultRetryConfig() HTTPRetryConfig {
	return HTTPRetryConfig{
		MaxRetries: 2,
		BaseDelay:  200 * time.Millisecond,
		MaxDelay:   2 * time.Second,
		Multiplier: 2.0,
	}
}

// RetryableHTTPError is returned when all retries are exhausted.
type RetryableHTTPError struct {
	StatusCode int
	Attempts   int
}

func (e *RetryableHTTPError) Error() string {
	return fmt.Sprintf("HTTP %d %s after %d attempts", e.StatusCode, http.StatusText(e.StatusCode), e.Attempts)
}

// IsRetryableStatus returns true if the status code should be retried.
func IsRetryableStatus(statusCode int) bool {
	return retryableStatusCodes[statusCode]
}

// DoWithRetry executes an HTTP request with exponential backoff retry on
// retryable status codes. It returns the response only on success (2xx).
// The caller must close the response body.
func DoWithRetry(ctx context.Context, client *http.Client, req *http.Request, cfg HTTPRetryConfig) (*http.Response, error) {
	if client == nil {
		return nil, fmt.Errorf("HTTP client is required")
	}
	if err := validateRetryTarget(req); err != nil {
		return nil, err
	}

	// Platform requests must not carry credentials across redirects. A redirect
	// response is returned to the caller as a non-successful response instead.
	safeClient := *client
	safeClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	var lastResp *http.Response
	delay := cfg.BaseDelay

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			// Exponential backoff.
			delay = time.Duration(float64(delay) * cfg.Multiplier)
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}

		// Clone the request for retry (body must be re-readable).
		reqClone := req.Clone(ctx)
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("clone request body: %w", err)
			}
			reqClone.Body = body
		}

		// #nosec G704 -- validateRetryTarget restricts the scheme and production callers use fixed platform hosts.
		resp, err := safeClient.Do(reqClone)
		if err != nil {
			// Network error — retry.
			if attempt < cfg.MaxRetries {
				continue
			}
			return nil, err
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}

		// Non-retryable status — fail immediately.
		if !IsRetryableStatus(resp.StatusCode) {
			return resp, nil
		}

		// Retryable status — close body and try again.
		lastResp = resp
		if err := resp.Body.Close(); err != nil {
			return nil, fmt.Errorf("close retry response: %w", err)
		}
	}

	if lastResp != nil {
		return nil, &RetryableHTTPError{
			StatusCode: lastResp.StatusCode,
			Attempts:   cfg.MaxRetries + 1,
		}
	}
	return nil, fmt.Errorf("request failed after %d attempts", cfg.MaxRetries+1)
}

func validateRetryTarget(req *http.Request) error {
	if req == nil || req.URL == nil {
		return fmt.Errorf("HTTP request URL is required")
	}
	if req.URL.User != nil || req.URL.Hostname() == "" {
		return fmt.Errorf("HTTP request target must not contain user-info and must have a host")
	}
	if strings.EqualFold(req.URL.Scheme, "https") {
		return nil
	}
	if strings.EqualFold(req.URL.Scheme, "http") {
		host := req.URL.Hostname()
		ip := net.ParseIP(host)
		if strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback()) {
			return nil
		}
	}
	return fmt.Errorf("HTTP request target must use HTTPS")
}
