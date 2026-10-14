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
	"strconv"
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

// SnapshotCapacityRetryPolicy configures elapsed-time retry for server snapshot
// admission capacity. It is intentionally independent of RetryPolicy so a busy
// server does not consume the generic transport-attempt budget.
type SnapshotCapacityRetryPolicy struct {
	Enabled        bool
	MaxWait        time.Duration
	FallbackDelay  time.Duration
	JitterFraction float64
}

// PushCapacityRetryPolicy configures elapsed-time retry for server history
// projection admission during push requests. It is independent of RetryPolicy
// so a busy server does not consume the generic transport-attempt budget.
type PushCapacityRetryPolicy struct {
	Enabled        bool
	MaxWait        time.Duration
	FallbackDelay  time.Duration
	JitterFraction float64
}

// PushCapacityRetryExhaustedError reports that push admission remained
// unavailable for the configured capacity-wait budget.
type PushCapacityRetryExhaustedError struct {
	Operation string
	ErrorCode string
	Waited    time.Duration
}

func (e *PushCapacityRetryExhaustedError) Error() string {
	if e == nil {
		return "oversqlite push capacity retry exhausted"
	}
	return fmt.Sprintf("oversqlite push capacity retry exhausted for %s (%s)", e.Operation, e.ErrorCode)
}

// RetryFailureClass is a redacted classification of the final retryable
// failure. It never contains response bodies, SQL details, tokens, or URLs.
type RetryFailureClass struct {
	Kind       string
	Operation  string
	HTTPStatus int
}

// SnapshotCapacityRetryExhaustedError reports that snapshot admission remained
// unavailable for the configured capacity-wait budget.
type SnapshotCapacityRetryExhaustedError struct {
	Operation string
	ErrorCode string
	Waited    time.Duration
}

func (e *SnapshotCapacityRetryExhaustedError) Error() string {
	if e == nil {
		return "oversqlite snapshot capacity retry exhausted"
	}
	return fmt.Sprintf("oversqlite snapshot capacity retry exhausted for %s", e.Operation)
}

// RetryExhaustedError reports that the configured retry budget was exhausted for an operation.
type RetryExhaustedError struct {
	Operation   string
	Attempts    int
	LastErr     error // Deprecated: retained as nil so remote or local error details cannot escape.
	LastFailure RetryFailureClass
}

// Error implements error.
func (e *RetryExhaustedError) Error() string {
	if e == nil {
		return "oversqlite retry policy exhausted"
	}
	base := fmt.Sprintf("oversqlite retry policy exhausted for %s after %d attempts", e.Operation, e.Attempts)
	if e.LastFailure.Kind == "http_status" && e.LastFailure.HTTPStatus > 0 {
		return fmt.Sprintf("%s; last failure was HTTP %d during %s", base, e.LastFailure.HTTPStatus, e.LastFailure.Operation)
	}
	if e.LastFailure.Kind != "" {
		if e.LastFailure.Operation != "" {
			return fmt.Sprintf("%s; last failure class was %s during %s", base, e.LastFailure.Kind, e.LastFailure.Operation)
		}
		return fmt.Sprintf("%s; last failure class was %s", base, e.LastFailure.Kind)
	}
	return base
}

// Unwrap deliberately does not expose the remote retry failure.
func (e *RetryExhaustedError) Unwrap() error {
	return nil
}

type retryHTTPError struct {
	Operation  string
	StatusCode int
	retryAfter time.Duration
}

func (e *retryHTTPError) Error() string {
	if e == nil {
		return "http request failed"
	}
	return fmt.Sprintf("%s request returned status %d", e.Operation, e.StatusCode)
}

var defaultRetryPolicy = RetryPolicy{
	Enabled:        true,
	MaxAttempts:    3,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     time.Second,
	JitterFraction: 0.2,
}

var defaultSnapshotCapacityRetryPolicy = SnapshotCapacityRetryPolicy{
	Enabled:        true,
	MaxWait:        30 * time.Second,
	FallbackDelay:  time.Second,
	JitterFraction: 1,
}

var defaultPushCapacityRetryPolicy = PushCapacityRetryPolicy{
	Enabled:        true,
	MaxWait:        30 * time.Second,
	FallbackDelay:  time.Second,
	JitterFraction: 1,
}

type normalizedRetryPolicy struct {
	enabled        bool
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	jitterFraction float64
}

type normalizedSnapshotCapacityRetryPolicy struct {
	enabled        bool
	maxWait        time.Duration
	fallbackDelay  time.Duration
	jitterFraction float64
}

type normalizedPushCapacityRetryPolicy struct {
	enabled        bool
	maxWait        time.Duration
	fallbackDelay  time.Duration
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

func (c *Client) normalizedSnapshotCapacityRetryPolicy() normalizedSnapshotCapacityRetryPolicy {
	if c == nil || c.config == nil || c.config.SnapshotCapacityRetryPolicy == nil {
		return normalizeSnapshotCapacityRetryPolicy(&defaultSnapshotCapacityRetryPolicy)
	}
	return normalizeSnapshotCapacityRetryPolicy(c.config.SnapshotCapacityRetryPolicy)
}

func normalizeSnapshotCapacityRetryPolicy(policy *SnapshotCapacityRetryPolicy) normalizedSnapshotCapacityRetryPolicy {
	if policy == nil {
		policy = &defaultSnapshotCapacityRetryPolicy
	}
	if !policy.Enabled {
		return normalizedSnapshotCapacityRetryPolicy{}
	}
	maxWait := policy.MaxWait
	if maxWait <= 0 {
		maxWait = defaultSnapshotCapacityRetryPolicy.MaxWait
	}
	fallbackDelay := policy.FallbackDelay
	if fallbackDelay <= 0 {
		fallbackDelay = defaultSnapshotCapacityRetryPolicy.FallbackDelay
	}
	jitter := policy.JitterFraction
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return normalizedSnapshotCapacityRetryPolicy{
		enabled:        true,
		maxWait:        maxWait,
		fallbackDelay:  fallbackDelay,
		jitterFraction: jitter,
	}
}

func (c *Client) normalizedPushCapacityRetryPolicy() normalizedPushCapacityRetryPolicy {
	if c == nil || c.config == nil || c.config.PushCapacityRetryPolicy == nil {
		return normalizePushCapacityRetryPolicy(&defaultPushCapacityRetryPolicy)
	}
	return normalizePushCapacityRetryPolicy(c.config.PushCapacityRetryPolicy)
}

func normalizePushCapacityRetryPolicy(policy *PushCapacityRetryPolicy) normalizedPushCapacityRetryPolicy {
	if policy == nil {
		policy = &defaultPushCapacityRetryPolicy
	}
	if !policy.Enabled {
		return normalizedPushCapacityRetryPolicy{}
	}
	maxWait := policy.MaxWait
	if maxWait <= 0 {
		maxWait = defaultPushCapacityRetryPolicy.MaxWait
	}
	fallbackDelay := policy.FallbackDelay
	if fallbackDelay <= 0 {
		fallbackDelay = defaultPushCapacityRetryPolicy.FallbackDelay
	}
	jitter := policy.JitterFraction
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return normalizedPushCapacityRetryPolicy{
		enabled: true, maxWait: maxWait, fallbackDelay: fallbackDelay, jitterFraction: jitter,
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
				Operation:   operation,
				Attempts:    attempt,
				LastFailure: classifyRetryFailure(err),
			}
		}
		delay := jitterDuration(backoff, policy.jitterFraction)
		var httpErr *retryHTTPError
		if errors.As(err, &httpErr) && httpErr.retryAfter > delay {
			delay = httpErr.retryAfter
		}
		if err := waitRetryBackoff(ctx, delay); err != nil {
			return zero, err
		}
		backoff = nextRetryBackoff(backoff, policy.maxBackoff)
	}
}

func classifyRetryFailure(err error) RetryFailureClass {
	var boundedErr *boundedHTTPFailureError
	if errors.As(err, &boundedErr) {
		return RetryFailureClass{Kind: string(boundedErr.stage), Operation: boundedErr.operation}
	}
	var httpErr *retryHTTPError
	if errors.As(err, &httpErr) {
		return RetryFailureClass{Kind: "http_status", Operation: httpErr.Operation, HTTPStatus: httpErr.StatusCode}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return RetryFailureClass{Kind: "network_timeout"}
	}
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return RetryFailureClass{Kind: "unexpected_eof"}
	case errors.Is(err, io.EOF):
		return RetryFailureClass{Kind: "eof"}
	case errors.Is(err, syscall.ECONNRESET):
		return RetryFailureClass{Kind: "connection_reset"}
	case errors.Is(err, syscall.EPIPE):
		return RetryFailureClass{Kind: "broken_pipe"}
	default:
		return RetryFailureClass{Kind: "retryable"}
	}
}

func isRetryableOperationError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	var boundedErr *boundedHTTPFailureError
	if errors.As(err, &boundedErr) {
		return boundedErr.retryable
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

func (c *Client) applyAuthenticatedSyncHeaders(httpReq *http.Request, token string) error {
	if httpReq == nil {
		return nil
	}
	sourceID := ""
	if c != nil {
		sourceID = c.sourceID
	}
	proof := oversync.SchemaGenerationProof{}
	if c != nil {
		proof = c.schemaGenerationProof
	}
	return applyAuthenticatedSyncHeadersWithSourceID(httpReq, token, sourceID, proof)
}

func applyAuthenticatedSyncHeadersWithSourceID(httpReq *http.Request, token string, sourceID string, proof oversync.SchemaGenerationProof) error {
	if httpReq == nil {
		return nil
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	if err := validateOptionalSourceID(sourceID); err != nil {
		return err
	}
	if sourceID != "" {
		httpReq.Header.Set(oversync.SourceIDHeader, sourceID)
	}
	if proof.ID < 1 {
		return fmt.Errorf("schema generation proof ID is invalid")
	}
	if !validSchemaGenerationFingerprint(proof.Fingerprint) {
		return fmt.Errorf("schema generation proof fingerprint is invalid")
	}
	httpReq.Header.Set(oversync.SchemaGenerationIDHeader, strconv.FormatInt(int64(proof.ID), 10))
	httpReq.Header.Set(oversync.SchemaGenerationFingerprintHeader, proof.Fingerprint)
	return nil
}
