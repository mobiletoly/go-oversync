package oversqlite

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSnapshotCapacityRetry_UsesElapsedBudgetIndependentOfTransientAttempts(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 1}
	client.config.SnapshotCapacityRetryPolicy = &SnapshotCapacityRetryPolicy{
		Enabled: true, MaxWait: 100 * time.Millisecond, FallbackDelay: time.Millisecond,
	}
	var attempts atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return errorJSONResponse(http.StatusTooManyRequests, oversync.ErrorResponse{Error: "snapshot_build_capacity"}), nil
		}
		return snapshotFixtureResponse(t, client, r, "snapshot-1", 0, nil), nil
	})}

	_, err := client.createSnapshotSession(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), attempts.Load())
	diagnostics := client.SnapshotTransferDiagnostics()
	require.Equal(t, int64(1), diagnostics.CapacityResponses)
	require.Equal(t, int64(1), diagnostics.CapacityRetries)
	require.Positive(t, diagnostics.CapacityWait)
}

func TestSnapshotCapacityRetry_RejectsDelayOutsideBudget(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.config.SnapshotCapacityRetryPolicy = &SnapshotCapacityRetryPolicy{
		Enabled: true, MaxWait: 100 * time.Millisecond, FallbackDelay: time.Millisecond,
	}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		resp := errorJSONResponse(http.StatusTooManyRequests, oversync.ErrorResponse{Error: "snapshot_build_capacity"})
		resp.Header.Set("Retry-After", "1")
		return resp, nil
	})}

	_, err := client.createSnapshotSession(context.Background(), nil)
	var exhausted *SnapshotCapacityRetryExhaustedError
	require.True(t, errors.As(err, &exhausted))
	require.Equal(t, "snapshot_build_capacity", exhausted.ErrorCode)
}

func TestParseRetryAfterDeltaSeconds(t *testing.T) {
	require.Equal(t, 2*time.Second, parseRetryAfterDeltaSeconds("2"))
	for _, value := range []string{"", "0", "-1", "1.5", "Wed, 21 Oct 2015 07:28:00 GMT"} {
		require.Zero(t, parseRetryAfterDeltaSeconds(value), value)
	}
}
