package oversqlite

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestPullToStable_HistoryCapacityRetryAfter(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	var requests []time.Time
	var identity string
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		current := r.URL.String() + r.Header.Get(oversync.SourceIDHeader) + r.Header.Get(oversync.SchemaGenerationFingerprintHeader)
		if identity == "" {
			identity = current
		}
		require.Equal(t, identity, current)
		requests = append(requests, time.Now())
		if len(requests) < 3 {
			response := errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{Error: "history_projection_capacity_exceeded", Message: "private"})
			response.Header.Set("Retry-After", "1")
			return response, nil
		}
		return jsonResponse(oversync.PullResponse{Bundles: []oversync.Bundle{}}), nil
	})}
	_, err := client.PullToStable(context.Background())
	require.NoError(t, err)
	require.Len(t, requests, 3)
	for i := 1; i < len(requests); i++ {
		require.GreaterOrEqual(t, requests[i].Sub(requests[i-1]), time.Second)
	}
	requireLastBundleSeqSeen(t, client, context.Background(), 0)
}

func TestPullToStable_HistoryCapacityAttemptsRemainBounded(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		response := errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{Error: "history_projection_capacity_exceeded", Message: "private"})
		response.Header.Set("Retry-After", "1")
		return response, nil
	})}
	_, err := client.PullToStable(context.Background())
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 3, requests)
	require.Equal(t, 3, exhausted.Attempts)
	require.NotContains(t, err.Error(), "private")
	requireLastBundleSeqSeen(t, client, context.Background(), 0)
}

func TestPullToStable_HistoryCapacityWaitIsCancellable(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		response := errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{Error: "history_projection_capacity_exceeded", Message: "private"})
		response.Header.Set("Retry-After", "1")
		return response, nil
	})}
	_, err := client.PullToStable(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, requests)
	requireLastBundleSeqSeen(t, client, context.Background(), 0)
}

func TestRetryValue_TerminalAndDisabledPoliciesDoNotWait(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "terminal"}[enabled], func(t *testing.T) {
			calls := 0
			status := http.StatusServiceUnavailable
			if enabled {
				status = http.StatusUnprocessableEntity
			}
			failure := &retryHTTPError{Operation: "pull", StatusCode: status, retryAfter: time.Hour}
			_, err := withRetryValue(context.Background(), normalizedRetryPolicy{enabled: enabled, maxAttempts: 3}, "pull", func() (int, error) { calls++; return 0, failure })
			require.Same(t, failure, err)
			require.Equal(t, 1, calls)
		})
	}
}
