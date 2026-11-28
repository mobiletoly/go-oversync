package oversqlite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type phase3TrackingBody struct {
	io.Reader
	closed bool
}

type phase3RetryableHostileError struct{}

func (phase3RetryableHostileError) Error() string {
	return "private-source-sentinel connection reset by peer"
}

func (phase3RetryableHostileError) Unwrap() error { return syscall.ECONNRESET }

func (b *phase3TrackingBody) Close() error {
	b.closed = true
	return nil
}

func TestPhase3RetryClassificationUsesTypedCausesOnly(t *testing.T) {
	require.False(t, isRetryableOperationError(context.Background(), errors.New("connection reset by peer")))
	require.False(t, isRetryableOperationError(context.Background(), errors.New("broken pipe")))
	require.False(t, isRetryableOperationError(context.Background(), &retryHTTPError{
		Operation:  "phase3",
		StatusCode: http.StatusBadRequest,
	}))

	require.True(t, isRetryableOperationError(context.Background(), syscall.ECONNRESET))
	require.True(t, isRetryableOperationError(context.Background(), syscall.EPIPE))
	require.True(t, isRetryableOperationError(context.Background(), io.ErrUnexpectedEOF))
}

func TestPhase3BufferedControlResponseIsBoundedAndClosed(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('phase3-row','Phase 3','phase3@example.com')`)
	require.NoError(t, err)
	snapshot, err := client.ensurePushOutboundSnapshot(context.Background())
	require.NoError(t, err)
	body := &phase3TrackingBody{Reader: strings.NewReader(strings.Repeat("x", int(client.discovery.Limits.MaxPushOutcomeDecodedBytes+1)))}
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       body,
		}, nil
	})}

	_, err = client.commitPushOutboundSnapshot(context.Background(), snapshot)
	var tooLarge *EncodedResponseBodyTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, "push", tooLarge.Operation)
	require.True(t, body.closed)
}

func TestAutomaticLoopsStopOnTerminalRepresentationErrorsAndPreserveState(t *testing.T) {
	for _, capacity := range []struct {
		name, code string
		status     int
	}{
		{"upload projection overflow", "bundle_projection_too_large", http.StatusUnprocessableEntity},
		{"upload outcome overflow", "push_outcome_too_large", http.StatusRequestEntityTooLarge},
	} {
		t.Run(capacity.name, func(t *testing.T) {
			client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			client.config.BackoffMin = time.Millisecond
			client.config.BackoffMax = 2 * time.Millisecond
			_, err := db.Exec(`INSERT INTO users(id,name,email) VALUES('terminal-row','Terminal','terminal@example.com')`)
			require.NoError(t, err)
			var requests atomic.Int64
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				require.Equal(t, "/sync/push", r.URL.Path)
				return errorJSONResponse(capacity.status, oversync.ErrorResponse{Error: capacity.code}), nil
			})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				client.uploaderLoop(ctx)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				cancel()
				<-done
				t.Fatal("uploader loop did not stop on intrinsic capacity failure")
			}
			require.EqualValues(t, 1, requests.Load())
			var frozenRows, nextSourceBundleID int64
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&frozenRows))
			require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)`).Scan(&nextSourceBundleID))
			require.Equal(t, int64(1), frozenRows)
			require.Equal(t, int64(1), nextSourceBundleID)
		})
	}

	t.Run("download committed bundle corruption", func(t *testing.T) {
		client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		client.config.BackoffMin = time.Millisecond
		client.config.BackoffMax = 2 * time.Millisecond
		var requests atomic.Int64
		client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "committed_bundle_unrepresentable"}), nil
		})}
		done := make(chan struct{})
		go func() {
			defer close(done)
			client.downloaderLoop(context.Background())
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("downloader loop did not stop on terminal committed-bundle corruption")
		}
		require.EqualValues(t, 1, requests.Load())
		checkpoint, err := client.LastBundleSeqSeen(context.Background())
		require.NoError(t, err)
		require.Zero(t, checkpoint)
	})
}

func TestAdmittedResponseRowLimitIsTypedAndTerminal(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.discovery.Limits.MaxRowDecodedBytes = 16
	err := client.validateAdmittedResponseRows("pull response", []oversync.PushRequestRow{{
		Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "row"}, Op: oversync.OpInsert,
		Payload: []byte(`{"id":"row","name":"Oversized","email":"oversized@example.com"}`),
	}})
	var oversized *AdmittedRowTooLargeError
	require.ErrorAs(t, err, &oversized)
	require.True(t, isTerminalProtocolRepresentationError(err))
}

func TestPhase3SourceRetiredPresenceDistinctions(t *testing.T) {
	tests := []struct {
		name string
		body string
		ok   bool
	}{
		{name: "absent", body: `{"error":"source_retired","message":"retired","source_id":"source-a"}`, ok: true},
		{name: "present null", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":null}`},
		{name: "present empty", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":""}`},
		{name: "valid same", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-a"}`, ok: true},
		{name: "valid different", body: `{"error":"source_retired","message":"retired","source_id":"source-a","replaced_by_source_id":"source-b"}`, ok: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, ok := decodeSourceRetiredResponse([]byte(testCase.body), "source-a")
			require.Equal(t, testCase.ok, ok)
		})
	}
}

func TestPhase3SnapshotRetirementMustNameFailedRequestSource(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.sourceID = "requested-source"
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return rawJSONResponse(
			http.StatusConflict,
			`{"error":"source_retired","message":"hostile","source_id":"different-source"}`,
		), nil
	})}

	_, err := client.createSnapshotSession(context.Background(), nil)
	require.EqualError(t, err, "invalid source_retired response")
	require.NotContains(t, err.Error(), "requested-source")
	require.NotContains(t, err.Error(), "different-source")
}

func TestPhase3RemoteErrorMessagesAndBodiesAreNotRetained(t *testing.T) {
	sentinel := "private-remote-sentinel"
	require.Equal(t, "invalid_error_response", decodeServerErrorBody([]byte(sentinel)))
	require.NotContains(t, decodeServerErrorBody([]byte(sentinel)), sentinel)

	body := []byte(`{"error":"history_pruned","message":"private-remote-sentinel"}`)
	require.Equal(t, "history_pruned", decodeServerErrorBody(body))
	decoded, ok := decodeServerErrorResponse(body)
	require.True(t, ok)
	require.Empty(t, decoded.Message)

	retired, ok := decodeSourceRetiredResponse(
		[]byte(`{"error":"source_retired","message":"private-remote-sentinel","source_id":"source-a"}`),
		"source-a",
	)
	require.True(t, ok)
	require.Empty(t, retired.Message)
}

func TestPhase3SnapshotSessionLimitErrorIsStructuredAndRedacted(t *testing.T) {
	const sentinel = "private-snapshot-limit-sentinel"
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return rawJSONResponse(
			http.StatusConflict,
			`{"error":"snapshot_session_limit_exceeded","message":"`+sentinel+`","dimension":"logical_byte_count","actual":257,"limit":256}`,
		), nil
	})}

	_, err := client.createSnapshotSession(context.Background(), nil)
	var limitErr *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "logical_byte_count", limitErr.Dimension)
	require.EqualValues(t, 257, limitErr.Actual)
	require.EqualValues(t, 256, limitErr.Limit)
	require.NotContains(t, err.Error(), sentinel)
	require.Equal(
		t, "snapshot_session_limit_exceeded",
		decodeServerErrorBody([]byte(`{"error":"snapshot_session_limit_exceeded","message":"`+sentinel+`"}`)),
	)
}

func TestPhase3SnapshotSessionLimitErrorRejectsMalformedDetails(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "unknown dimension",
			body: `{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"other","actual":2,"limit":1}`,
		},
		{
			name: "actual does not exceed limit",
			body: `{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"row_count","actual":1,"limit":1}`,
		},
		{
			name: "non-positive limit",
			body: `{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"row_byte_count","actual":1,"limit":0}`,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return rawJSONResponse(http.StatusConflict, testCase.body), nil
			})}

			_, err := client.createSnapshotSession(context.Background(), nil)
			require.EqualError(t, err, "invalid snapshot_session_limit_exceeded response")
			require.NotContains(t, err.Error(), "hostile")
		})
	}
}

func TestPhase3PublicRetryAndRecoveryErrorsDoNotRetainHostileCauses(t *testing.T) {
	const hostile = "private-source-sentinel connection reset by peer"
	policy := normalizedRetryPolicy{enabled: true, maxAttempts: 1}
	_, err := withRetryValue(context.Background(), policy, "phase3", func() (struct{}, error) {
		return struct{}{}, phase3RetryableHostileError{}
	})
	var retryErr *RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.NotContains(t, retryErr.Error(), hostile)
	require.Nil(t, retryErr.LastErr)
	require.Nil(t, errors.Unwrap(retryErr))

	blocked := &CheckpointRecoveryBlockedError{
		Reason: CheckpointRecoveryBlockedPushFailed,
		Cause:  errors.New(hostile),
	}
	require.NotContains(t, blocked.Error(), hostile)
	require.Nil(t, errors.Unwrap(blocked))
}
