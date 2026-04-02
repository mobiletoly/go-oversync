package oversqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

// RetryPolicy configures bounded retry for transient sync I/O failures.
type RetryPolicy struct {
	Enabled        bool
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	JitterFraction float64
}

// RetryExhaustedError reports that the configured retry budget was exhausted for an operation.
type RetryExhaustedError struct {
	Operation string
	Attempts  int
	LastErr   error
}

// Error implements error.
func (e *RetryExhaustedError) Error() string {
	if e == nil {
		return "oversqlite retry policy exhausted"
	}
	if e.LastErr == nil {
		return fmt.Sprintf("oversqlite retry policy exhausted for %s after %d attempts", e.Operation, e.Attempts)
	}
	return fmt.Sprintf("oversqlite retry policy exhausted for %s after %d attempts: %v", e.Operation, e.Attempts, e.LastErr)
}

// Unwrap returns the last underlying retry failure.
func (e *RetryExhaustedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.LastErr
}

type retryHTTPError struct {
	Operation  string
	StatusCode int
	Message    string
}

func (e *retryHTTPError) Error() string {
	if e == nil {
		return "http request failed"
	}
	if strings.TrimSpace(e.Message) == "" {
		return fmt.Sprintf("%s request returned status %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("%s request returned status %d: %s", e.Operation, e.StatusCode, e.Message)
}

var defaultRetryPolicy = RetryPolicy{
	Enabled:        true,
	MaxAttempts:    3,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     time.Second,
	JitterFraction: 0.2,
}

type normalizedRetryPolicy struct {
	enabled        bool
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	jitterFraction float64
}

func (c *Client) normalizedRetryPolicy() normalizedRetryPolicy {
	if c == nil || c.config == nil || c.config.RetryPolicy == nil {
		return normalizeRetryPolicy(&defaultRetryPolicy)
	}
	return normalizeRetryPolicy(c.config.RetryPolicy)
}

func normalizeRetryPolicy(policy *RetryPolicy) normalizedRetryPolicy {
	if policy == nil {
		policy = &defaultRetryPolicy
	}
	if !policy.Enabled {
		return normalizedRetryPolicy{}
	}

	maxAttempts := policy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultRetryPolicy.MaxAttempts
	}

	initialBackoff := policy.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = defaultRetryPolicy.InitialBackoff
	}

	maxBackoff := policy.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultRetryPolicy.MaxBackoff
	}
	if maxBackoff < initialBackoff {
		maxBackoff = initialBackoff
	}

	jitter := policy.JitterFraction
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}

	return normalizedRetryPolicy{
		enabled:        true,
		maxAttempts:    maxAttempts,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		jitterFraction: jitter,
	}
}

func (c *Client) withRetry(ctx context.Context, operation string, fn func() error) error {
	policy := c.normalizedRetryPolicy()
	if !policy.enabled {
		return fn()
	}

	attempt := 0
	backoff := policy.initialBackoff
	for {
		attempt++
		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryableOperationError(ctx, err) {
			return err
		}
		if attempt >= policy.maxAttempts {
			return &RetryExhaustedError{
				Operation: operation,
				Attempts:  attempt,
				LastErr:   err,
			}
		}
		if err := waitRetryBackoff(ctx, jitterDuration(backoff, policy.jitterFraction)); err != nil {
			return err
		}
		backoff = nextRetryBackoff(backoff, policy.maxBackoff)
	}
}

func withRetryValue[T any](ctx context.Context, policy normalizedRetryPolicy, operation string, fn func() (T, error)) (T, error) {
	if !policy.enabled {
		return fn()
	}

	var zero T
	attempt := 0
	backoff := policy.initialBackoff
	for {
		attempt++
		value, err := fn()
		if err == nil {
			return value, nil
		}
		if !isRetryableOperationError(ctx, err) {
			return zero, err
		}
		if attempt >= policy.maxAttempts {
			return zero, &RetryExhaustedError{
				Operation: operation,
				Attempts:  attempt,
				LastErr:   err,
			}
		}
		if err := waitRetryBackoff(ctx, jitterDuration(backoff, policy.jitterFraction)); err != nil {
			return zero, err
		}
		backoff = nextRetryBackoff(backoff, policy.maxBackoff)
	}
}

func isRetryableOperationError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	var httpErr *retryHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}

	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}

	errorText := strings.ToLower(err.Error())
	if strings.Contains(errorText, "connection reset by peer") || strings.Contains(errorText, "broken pipe") {
		return true
	}
	return false
}

func waitRetryBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextRetryBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		return max
	}
	if current >= max {
		return max
	}
	next := current * 2
	if next > max || next < current {
		return max
	}
	return next
}

func jitterDuration(base time.Duration, fraction float64) time.Duration {
	if base <= 0 || fraction <= 0 {
		return base
	}
	span := float64(base) * fraction
	offset := (rand.Float64()*2 - 1) * span
	jittered := float64(base) + offset
	if jittered < 0 {
		jittered = 0
	}
	return time.Duration(math.Round(jittered))
}

type responsePayload struct {
	body       []byte
	statusCode int
}

func (c *Client) doAuthenticatedRequest(ctx context.Context, method, endpoint string, body []byte, contentType string) ([]byte, int, error) {
	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = strings.NewReader(string(body))
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(contentType) != "" {
		httpReq.Header.Set("Content-Type", contentType)
	}
	c.applyAuthenticatedSyncHeaders(httpReq, token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	responseBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, 0, readErr
	}
	return responseBody, resp.StatusCode, nil
}

func (c *Client) applyAuthenticatedSyncHeaders(httpReq *http.Request, token string) {
	if httpReq == nil {
		return
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	sourceID := strings.TrimSpace(c.sourceID)
	if sourceID != "" {
		httpReq.Header.Set(oversync.SourceIDHeader, sourceID)
	}
}

func (c *Client) doAuthenticatedRequestWithRetry(ctx context.Context, operation, method, endpoint string, body []byte, contentType string) ([]byte, int, error) {
	result, err := withRetryValue(ctx, c.normalizedRetryPolicy(), operation, func() (responsePayload, error) {
		responseBody, statusCode, err := c.doAuthenticatedRequest(ctx, method, endpoint, body, contentType)
		if err != nil {
			return responsePayload{}, err
		}
		if statusCode == http.StatusTooManyRequests || statusCode == http.StatusBadGateway || statusCode == http.StatusServiceUnavailable || statusCode == http.StatusGatewayTimeout {
			return responsePayload{}, &retryHTTPError{
				Operation:  operation,
				StatusCode: statusCode,
				Message:    decodeServerErrorBody(responseBody),
			}
		}
		return responsePayload{body: responseBody, statusCode: statusCode}, nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result.body, result.statusCode, nil
}
