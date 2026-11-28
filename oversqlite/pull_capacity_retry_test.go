// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Pull capacity window (p0001 history-projection retry timing, amended 2026-11-14).

func historyCapacityRefusal(retryAfter string) *http.Response {
	response := errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{Error: "history_projection_capacity_exceeded", Message: "private"})
	if retryAfter != "" {
		response.Header.Set("Retry-After", retryAfter)
	}
	return response
}

func countRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(query).Scan(&n))
	return n
}

func TestPullToStable_HistoryCapacityRetriesPastAttemptLimitHonouringRetryAfter(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 1}
	var requests []time.Time
	var identity string
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		current := r.URL.String() + r.Header.Get(oversync.SourceIDHeader) + r.Header.Get(oversync.SchemaGenerationFingerprintHeader)
		if identity == "" {
			identity = current
		}
		require.Equal(t, identity, current)
		requests = append(requests, time.Now())
		if len(requests) <= 2 {
			return historyCapacityRefusal("1"), nil
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

// Sync pushes the local row, then its pull exhausts the window: the error is the
// generic retryable one and the local row and its committed upload survive.
func TestSync_HistoryCapacityWindowExhaustionKeepsLocalIntent(t *testing.T) {
	client, db := newUsersBundleClient(t)
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES ('user-1', 'Ada', 'ada@example.com')`)
	require.NoError(t, err)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	client.config.PullCapacityRetryPolicy = &PullCapacityRetryPolicy{Enabled: true, MaxWait: 900 * time.Millisecond, FallbackDelay: 200 * time.Millisecond}
	server := newMockPushServer(t)
	var pulls, pushes atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/pull" {
			pulls.Add(1)
			return historyCapacityRefusal(""), nil
		}
		if r.URL.Path == "/sync/push" {
			pushes.Add(1)
		}
		return server.RoundTrip(r)
	})}
	_, err = client.Sync(context.Background())
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, "pull_request", exhausted.Operation)
	require.Equal(t, int(pulls.Load()), exhausted.Attempts)
	require.Equal(t, RetryFailureClass{Kind: "http_status", Operation: "pull_request", HTTPStatus: http.StatusServiceUnavailable}, exhausted.LastFailure)
	// Fallback waits of 200 ms fit at most four times in 900 ms: more than the two
	// generic attempts, never more than the window holds.
	require.GreaterOrEqual(t, pulls.Load(), int64(3))
	require.LessOrEqual(t, pulls.Load(), int64(5))
	require.NotContains(t, err.Error(), "private")
	require.Equal(t, int64(1), pushes.Load())
	require.Equal(t, 1, countRows(t, db, `SELECT count(*) FROM users WHERE id = 'user-1' AND name = 'Ada' AND email = 'ada@example.com'`))
	require.Zero(t, countRows(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
}

func TestPullToStable_HistoryCapacityWindowWaitIsCancellable(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 1}
	client.config.PullCapacityRetryPolicy = &PullCapacityRetryPolicy{Enabled: true, MaxWait: time.Minute, FallbackDelay: 300 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var requests atomic.Int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if requests.Add(1) == 2 {
			// Cancel during the second wait; no third dispatch may follow.
			time.AfterFunc(100*time.Millisecond, cancel)
		}
		return historyCapacityRefusal(""), nil
	})}
	started := time.Now()
	_, err := client.PullToStable(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(2), requests.Load())
	require.Less(t, time.Since(started), 5*time.Second)
	requireLastBundleSeqSeen(t, client, context.Background(), 0)
}

func TestPullToStable_DisabledHistoryCapacityWindowKeepsGenericAttempts(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	client.config.PullCapacityRetryPolicy = &PullCapacityRetryPolicy{Enabled: false}
	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		return historyCapacityRefusal(""), nil
	})}
	_, err := client.PullToStable(context.Background())
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 3, requests)
	require.Equal(t, 3, exhausted.Attempts)
}

func TestPullCapacityRetryPolicy_DefaultsMatchPushWindow(t *testing.T) {
	client := &Client{config: &Config{}}
	require.Equal(t, client.normalizedPushCapacityRetryPolicy(), client.normalizedPullCapacityRetryPolicy())
	require.Equal(t, normalizedPushCapacityRetryPolicy{enabled: true, maxWait: 30 * time.Second, fallbackDelay: time.Second, jitterFraction: 1}, client.normalizedPullCapacityRetryPolicy())
}

// Capacity waits do not hand the generic attempt limit a fresh budget: two
// gateway failures exhaust two attempts even with a capacity refusal between them.
func TestPullToStable_HistoryCapacityWaitKeepsGenericAttemptCount(t *testing.T) {
	client, _ := newUsersBundleClient(t)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	client.config.PullCapacityRetryPolicy = &PullCapacityRetryPolicy{Enabled: true, MaxWait: time.Minute, FallbackDelay: 10 * time.Millisecond}
	requests := 0
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		switch requests {
		case 2:
			return historyCapacityRefusal(""), nil
		case 1, 3:
			return errorJSONResponse(http.StatusBadGateway, oversync.ErrorResponse{Error: "bad_gateway", Message: "private"}), nil
		default:
			return jsonResponse(oversync.PullResponse{Bundles: []oversync.Bundle{}}), nil
		}
	})}
	_, err := client.PullToStable(context.Background())
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 3, requests)
	require.Equal(t, http.StatusBadGateway, exhausted.LastFailure.HTTPStatus)
	requireLastBundleSeqSeen(t, client, context.Background(), 0)
}
