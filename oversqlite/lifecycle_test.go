package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const lifecycleUsersDDL = `
	CREATE TABLE users (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		email TEXT NOT NULL
	)
`

func newLifecycleTestClientWithTransport(t *testing.T, transport rawRoundTripFunc) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(lifecycleUsersDDL)
	require.NoError(t, err)

	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	config.RetryPolicy = &RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		JitterFraction: 0,
	}
	config = testConfigWithLocalContract(t, db, config)

	client, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, config)
	require.NoError(t, err)
	client.sourceIDGenerator = func() string { return "device-a" }
	mustOpen(t, client, context.Background())
	client.HTTP = &http.Client{Transport: transport}

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })
	return client, db
}

func newLifecycleTestClientWithConfigAndTransport(t *testing.T, config *Config, transport rawRoundTripFunc) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(lifecycleUsersDDL)
	require.NoError(t, err)
	config = testConfigWithLocalContract(t, db, config)

	client, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, config)
	require.NoError(t, err)
	client.sourceIDGenerator = func() string { return "device-a" }
	mustOpen(t, client, context.Background())
	client.HTTP = &http.Client{Transport: transport}

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })
	return client, db
}

func newLifecycleTestClient(t *testing.T, connectResponse *oversync.ConnectResponse) (*Client, *sql.DB) {
	t.Helper()
	return newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			requireAuthenticatedSyncHeaders(t, r, "device-a")
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			requireAuthenticatedSyncHeaders(t, r, "device-a")
			return jsonResponse(connectResponse), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})
}

func requireLifecycleUserAttached(t *testing.T, client *Client) {
	t.Helper()

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
}

func requireDetachBlockedUnsyncedData(t *testing.T, client *Client) {
	t.Helper()

	detachResult, err := client.Detach(context.Background())
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detachResult.Outcome)
	require.EqualValues(t, 1, detachResult.PendingRowCount)
	require.Equal(t, "lifecycle-user", client.UserID)
}

func requireDetachBlocksWithSeededOutbox(t *testing.T, rec outboxBundleRecord) {
	t.Helper()

	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})
	requireLifecycleUserAttached(t, client)

	rec.SourceID = client.sourceID
	seedOutboxBundleForTest(t, db, rec)
	seedOutboxUserInsertForTest(t, db, rec.SourceBundleID, "1", "Ada", "ada@example.com")
	requireDetachBlockedUnsyncedData(t, client)
}

func TestConnect_RetryLaterSurfacesRetryAfter(t *testing.T) {
	client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution:     "retry_later",
		LeaseExpiresAt: "2030-01-01T00:00:00Z",
		RetryAfterSec:  7,
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusRetryLater, result.Status)
	require.Equal(t, 7*time.Second, result.RetryAfter)
}

func TestConnect_RetriesTransientHTTPFailureAndSucceeds(t *testing.T) {
	var connectRequests int32
	client, _ := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			if atomic.AddInt32(&connectRequests, 1) == 1 {
				return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
					Error:   "temporarily_unavailable",
					Message: "retry me",
				}), nil
			}
			return jsonResponse(&oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	require.Equal(t, AttachOutcomeStartedEmpty, result.Outcome)
	require.Equal(t, int32(2), atomic.LoadInt32(&connectRequests))
}

func TestConnect_DoesNotRetryUnauthorized(t *testing.T) {
	var connectRequests int32
	client, _ := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			atomic.AddInt32(&connectRequests, 1)
			return errorJSONResponse(http.StatusUnauthorized, oversync.ErrorResponse{
				Error:   "unauthorized",
				Message: "bad token",
			}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&connectRequests))
}

func TestConnect_InvalidInitializationLeaseIsNotPersisted(t *testing.T) {
	invalidInitializationID := " 22222222-2222-4222-8222-222222222222 "
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution:       "initialize_local",
		InitializationID: invalidInitializationID,
	})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "22222222")
	require.Empty(t, client.pendingInitializationID)
	require.Empty(t, client.UserID)
	require.Empty(t, requirePendingInitializationID(t, db))
}

func TestConnect_RetryExhaustionReturnsTypedError(t *testing.T) {
	var connectRequests int32
	client, _ := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			atomic.AddInt32(&connectRequests, 1)
			return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
				Error:   "temporarily_unavailable",
				Message: "retry me",
			}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	var retryErr *RetryExhaustedError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, "connect_request", retryErr.Operation)
	require.Equal(t, 3, retryErr.Attempts)
	require.Equal(t, int32(3), atomic.LoadInt32(&connectRequests))
}

func TestConnect_RetryPolicyCanBeDisabledExplicitly(t *testing.T) {
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	config.RetryPolicy = &RetryPolicy{Enabled: false}

	var connectRequests int32
	client, _ := newLifecycleTestClientWithConfigAndTransport(t, config, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			atomic.AddInt32(&connectRequests, 1)
			return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{
				Error:   "temporarily_unavailable",
				Message: "retry me",
			}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&connectRequests))
}

func TestConnect_NetworkFailureLeavesAnonymousStateIntact(t *testing.T) {
	const hostileText = "HOSTILE_CONNECT_TRANSPORT_SECRET"
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			return nil, errors.New(hostileText)
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)
	mustOpen(t, client, context.Background())

	_, err = client.Attach(context.Background(), "lifecycle-user")
	require.EqualError(t, err, "failed to send connect request: connect_request HTTP transport failed")
	require.NotContains(t, err.Error(), hostileText)
	require.Empty(t, client.UserID)

	var (
		bindingState string
		bindingScope string
		dirtyCount   int
		userCount    int
	)
	require.NoError(t, db.QueryRow(`
		SELECT binding_state, attached_user_id
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(&bindingState, &bindingScope))
	require.Equal(t, lifecycleBindingAnonymous, bindingState)
	require.Empty(t, bindingScope)
	kind, _, _, _, _ := requireOperationState(t, db)
	require.Equal(t, lifecycleTransitionNone, kind)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&userCount))
	require.Equal(t, 1, userCount)
}

func TestDetach_BlocksWhenAttachedDirtyRowsExist(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})
	requireLifecycleUserAttached(t, client)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	requireDetachBlockedUnsyncedData(t, client)
}

func TestDetach_BlocksWhenPreparedOutboxExists(t *testing.T) {
	requireDetachBlocksWithSeededOutbox(t, outboxBundleRecord{
		State:                outboxStatePrepared,
		SourceBundleID:       1,
		CanonicalRequestHash: "hash",
		RowCount:             1,
	})
}

func TestDetach_ReenablesDirtyRowCaptureForAnonymousWrites(t *testing.T) {
	ctx := context.Background()
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	detachResult := mustDetach(t, client, ctx)
	require.Equal(t, DetachOutcomeDetached, detachResult.Outcome)
	require.Empty(t, client.UserID)
	require.Equal(t, "device-a", client.sourceID)

	var applyMode int
	require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1`).Scan(&applyMode))
	require.Equal(t, 0, applyMode)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)

	var op string
	require.NoError(t, db.QueryRow(`SELECT op FROM _sync_dirty_rows WHERE table_name = 'users' AND key_json = ?`, rowKeyJSON("1")).Scan(&op))
	require.Equal(t, string(oversync.OpInsert), op)
}

func TestDetach_ReopenDoesNotDuplicateAnonymousWrites(t *testing.T) {
	ctx := context.Background()
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	detachResult := mustDetach(t, client, ctx)
	require.Equal(t, DetachOutcomeDetached, detachResult.Outcome)
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)

	require.NoError(t, client.Close())

	reopened, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	reopened.HTTP = client.HTTP
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	mustOpen(t, reopened, ctx)

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)

	var op string
	require.NoError(t, db.QueryRow(`SELECT op FROM _sync_dirty_rows WHERE table_name = 'users' AND key_json = ?`, rowKeyJSON("1")).Scan(&op))
	require.Equal(t, string(oversync.OpInsert), op)
}

func TestManagedTableGuards_BlockDirectWritesDuringRemoteReplace(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name       string
		seedRow    bool
		stmt       string
		args       []any
		verifyStmt string
		verifyArgs []any
		expected   int
	}{
		{
			name:       "insert",
			stmt:       `INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			args:       []any{"insert-1", "Ada", "ada@example.com"},
			verifyStmt: `SELECT COUNT(*) FROM users WHERE id = ?`,
			verifyArgs: []any{"insert-1"},
			expected:   0,
		},
		{
			name:       "update",
			seedRow:    true,
			stmt:       `UPDATE users SET name = ? WHERE id = ?`,
			args:       []any{"Updated", "seed-1"},
			verifyStmt: `SELECT COUNT(*) FROM users WHERE id = ? AND name = ?`,
			verifyArgs: []any{"seed-1", "Updated"},
			expected:   0,
		},
		{
			name:       "delete",
			seedRow:    true,
			stmt:       `DELETE FROM users WHERE id = ?`,
			args:       []any{"seed-1"},
			verifyStmt: `SELECT COUNT(*) FROM users WHERE id = ?`,
			verifyArgs: []any{"seed-1"},
			expected:   1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
				Resolution: "initialize_empty",
			})

			if tc.seedRow {
				setApplyModeForTest(t, db, true)
				_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "seed-1", "Seed", "seed@example.com")
				require.NoError(t, err)
				setApplyModeForTest(t, db, false)
				_, err = db.Exec(`DELETE FROM _sync_dirty_rows`)
				require.NoError(t, err)
			}

			setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindRemoteReplace, TargetUserID: "lifecycle-user"})

			_, err := db.ExecContext(ctx, tc.stmt, tc.args...)
			require.Error(t, err)
			require.Contains(t, err.Error(), transitionPendingTriggerError)

			var count int
			require.NoError(t, db.QueryRow(tc.verifyStmt, tc.verifyArgs...).Scan(&count))
			require.Equal(t, tc.expected, count)
		})
	}
}

func TestSourceInfo_ReportsManagedSourceWithPreparedOutbox(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})
	requireLifecycleUserAttached(t, client)
	seedOutboxBundleForTest(t, db, outboxBundleRecord{
		State:                outboxStatePrepared,
		SourceID:             client.sourceID,
		SourceBundleID:       1,
		CanonicalRequestHash: "hash",
		RowCount:             1,
	})
	seedOutboxUserInsertForTest(t, db, 1, "1", "Ada", "ada@example.com")

	info, err := client.SourceInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, client.sourceID, info.CurrentSourceID)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)
}

func TestSourceInfo_PreservesPendingLocalState(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	info, err := client.SourceInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, client.sourceID, info.CurrentSourceID)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
}

func TestDetach_ReattachPreservesNextSourceBundleIDOnSameSource(t *testing.T) {
	ctx := context.Background()
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	attachment, err := loadAttachmentState(ctx, db)
	require.NoError(t, err)
	require.NoError(t, updateSourceNextBundleID(ctx, db, attachment.CurrentSourceID, 9))

	mustDetach(t, client, ctx)
	require.NoError(t, client.Close())

	reopened, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	reopened.HTTP = client.HTTP
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	mustOpen(t, reopened, ctx)
	result, err = reopened.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, nextSourceBundleID, _ := requireBundleState(t, db)
	require.Equal(t, int64(9), nextSourceBundleID)
}

func TestOpen_DoesNotRequireCallerProvidedSourceID(t *testing.T) {
	ctx := context.Background()
	client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	err := client.open(ctx)
	require.NoError(t, err)

	_, err = client.Attach(ctx, "lifecycle-user")
	require.NoError(t, err)
}

func TestOpen_ReusesPersistedManagedSourceID(t *testing.T) {
	ctx := context.Background()
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	mustOpen(t, client, ctx)
	originalSourceID := client.sourceID
	require.NoError(t, client.Close())

	reopened, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	reopened.HTTP = client.HTTP
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	mustOpen(t, reopened, ctx)
	require.Equal(t, originalSourceID, reopened.sourceID)
}

func TestOpen_IsIdempotentForMatchingSource(t *testing.T) {
	client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	mustOpen(t, client, context.Background())
	originalSourceID := client.sourceID
	mustOpen(t, client, context.Background())
	require.Equal(t, originalSourceID, client.sourceID)
}

func TestOpen_CapturesAnonymousPreexistingRowsWithoutBlockingDetach(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	mustOpen(t, client, context.Background())

	status, err := client.PendingSyncStatus(context.Background())
	require.NoError(t, err)
	require.True(t, status.HasPendingSyncData)
	require.EqualValues(t, 1, status.PendingRowCount)
	require.False(t, status.BlocksDetach)
}

func TestConnect_FailsFastWhenLifecycleCapabilitiesAreMissing(t *testing.T) {
	client, _ := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
			Error:   "not_found",
			Message: "missing",
		}), nil
	})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	var unsupportedErr *AttachLifecycleUnsupportedError
	require.ErrorAs(t, err, &unsupportedErr)
}

func TestConnect_ReconnectAfterInitializationExpiredClearsStalePendingInitializationAndReissuesConnect(t *testing.T) {
	testReconnectAfterInitializationLeaseFailure(t, http.StatusGone, "initialization_expired")
}

func TestConnect_ReconnectAfterInitializationStaleClearsStalePendingInitializationAndReissuesConnect(t *testing.T) {
	testReconnectAfterInitializationLeaseFailure(t, http.StatusConflict, "initialization_stale")
}

func TestConnect_ReconnectAfterInitializationStalePreservesIntentWhenScopeInitialized(t *testing.T) {
	testReconnectAfterInitializationLeaseFailure(t, http.StatusConflict, "initialization_stale", "remote_authoritative")
}

func testReconnectAfterInitializationLeaseFailure(t *testing.T, statusCode int, errorCode string, secondResolutions ...string) {
	t.Helper()

	secondResolution := "initialize_local"
	secondToken := "33333333-3333-4333-8333-333333333333"
	expectedOutcome := AttachOutcomeSeededLocal
	if len(secondResolutions) > 0 {
		secondResolution = secondResolutions[0]
		secondToken = ""
		expectedOutcome = AttachOutcomeResumedAttached
	}
	var connectRequests int32
	recovery := newMockPushServer(t)
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			switch atomic.AddInt32(&connectRequests, 1) {
			case 1:
				return jsonResponse(&oversync.ConnectResponse{
					Resolution:       "initialize_local",
					InitializationID: "22222222-2222-4222-8222-222222222222",
					LeaseExpiresAt:   "2030-01-01T00:00:00Z",
				}), nil
			case 2:
				if secondResolution == "remote_authoritative" {
					return jsonResponse(&oversync.ConnectResponse{Resolution: secondResolution}), nil
				}
				return jsonResponse(&oversync.ConnectResponse{
					Resolution:       secondResolution,
					InitializationID: secondToken,
					LeaseExpiresAt:   "2030-01-01T00:00:00Z",
				}), nil
			default:
				return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{
					Error:   "unexpected_connect",
					Message: "unexpected connect attempt",
				}), nil
			}
		case "/sync/push-outcomes/resolve", "/sync/push-outcomes/ack":
			return recovery.RoundTrip(r)
		case "/sync/push":
			if atomic.LoadInt32(&connectRequests) > 1 {
				return recovery.RoundTrip(r)
			}
			return errorJSONResponse(statusCode, oversync.ErrorResponse{
				Error:   errorCode,
				Message: "seed lease is no longer valid",
			}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
				Error:   "not_found",
				Message: "unexpected path",
			}), nil
		}
	})

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "seed-1", "Seed", "seed@example.com")
	require.NoError(t, err)

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachOutcomeSeededLocal, result.Outcome)

	pendingInitializationID := requirePendingInitializationID(t, db)
	require.Equal(t, "22222222-2222-4222-8222-222222222222", pendingInitializationID)

	_, err = client.PushPending(context.Background())
	require.Error(t, err)
	// Admission errors are not terminal proof and cannot erase an uncertain F.
	require.Equal(t, "lifecycle-user", client.UserID)
	require.Equal(t, pendingInitializationID, client.pendingInitializationID)
	frozen, certificate, phase := frozenJournalBodies(t, db)
	require.NotEmpty(t, frozen)
	require.Empty(t, certificate)
	require.Equal(t, "frozen", phase)
	require.Equal(t, int32(1), atomic.LoadInt32(&connectRequests))
	// Resolve the original lease first, dispose + ack cancellation, then renew it.
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, recovery.resolveRequests, 1)
	require.Equal(t, pendingInitializationID, *recovery.resolveRequests[0].InitializationID)
	require.Len(t, recovery.ackRequests, 1)
	require.Equal(t, int32(1), atomic.LoadInt32(&connectRequests))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	reconnectResult, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, expectedOutcome, reconnectResult.Outcome)
	require.Equal(t, int32(2), atomic.LoadInt32(&connectRequests))
	require.Equal(t, secondToken, client.pendingInitializationID)
	require.Equal(t, client.pendingInitializationID, requirePendingInitializationID(t, db))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal`))
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, recovery.pushRequests, 1)
	require.EqualValues(t, 2, recovery.pushRequests[0].SourceBundleID)
	require.Equal(t, secondToken, recovery.pushRequests[0].InitializationID)
	require.Empty(t, client.pendingInitializationID)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM users WHERE id='seed-1' AND name='Seed'`))
}

func TestConnect_ResumesSameAttachedUserAfterProtocolGate(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	require.NoError(t, client.Close())

	var requestCount int32
	restarted, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	restarted.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requestCount, 1)
		return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
	})}
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })

	mustOpen(t, restarted, context.Background())

	resumeResult, err := restarted.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, resumeResult.Status)
	require.Equal(t, AttachOutcomeResumedAttached, resumeResult.Outcome)
	require.Zero(t, atomic.LoadInt32(&requestCount))
}

func TestOpen_AllowsPendingRemoteReplaceRecoveryState(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindRemoteReplace, TargetUserID: "lifecycle-user"})

	mustOpen(t, client, context.Background())
}

func TestConnect_RemoteAuthoritativePreservesUnregisteredLocalRows(t *testing.T) {
	snapshotRows := []oversync.SnapshotRow{{
		Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote-1"}, RowVersion: 7,
		Payload: json.RawMessage(`{"id":"remote-1","name":"Remote","email":"remote@example.com"}`),
	}}
	var client *Client
	var db *sql.DB
	client, db = newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			return jsonResponse(&oversync.ConnectResponse{Resolution: "remote_authoritative"}), nil
		case "/sync/snapshot-sessions":
			return snapshotFixtureResponse(t, client, r, "snapshot-remote-replace", 7, snapshotRows), nil
		case "/sync/snapshot-sessions/snapshot-remote-replace":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-remote-replace",
					SnapshotBundleSeq: 7,
					Rows:              snapshotRows,
					NextRowOrdinal:    1,
					HasMore:           false,
				}), nil
			case http.MethodDelete:
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
			}
		}
		return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
			Error:   "not_found",
			Message: "unexpected path",
		}), nil
	})

	_, err := db.Exec(`
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY NOT NULL,
			body TEXT NOT NULL
		)
	`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO legacy_docs (id, body) VALUES (?, ?)`, "legacy-1", "stale body")
	require.NoError(t, err)
	// This fresh-format fixture keeps application-owned, unregistered rows.
	// Dynamic historical registration/removal is no longer a snapshot path.

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachOutcomeUsedRemote, result.Outcome)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state WHERE table_name = 'legacy_docs'`).Scan(&count))
	require.Equal(t, 0, count)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "remote-1").Scan(&name))
	require.Equal(t, "Remote", name)
}

func TestConnect_PendingRemoteReplaceMismatchedScopeFailsClosed(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindRemoteReplace, TargetUserID: "other-user"})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	var conflictErr *AttachLocalStateConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.Contains(t, conflictErr.Reason, "pending remote_replace")
}

func TestConnect_PendingRemoteReplaceStartsFreshValidatedSessionAfterProtocolGate(t *testing.T) {
	snapshotRows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote-2"}, RowVersion: 5, Payload: mustJSONPayload(t, map[string]any{"id": "remote-2", "name": "Remote 2", "email": "remote2@example.com"})}}
	var client *Client
	var db *sql.DB
	client, db = newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/snapshot-sessions":
			return snapshotFixtureResponse(t, client, r, "snapshot-2", 12, snapshotRows), nil
		case "/sync/snapshot-sessions/snapshot-2":
			if r.Method == http.MethodDelete {
				return &http.Response{StatusCode: http.StatusNoContent, Header: http.Header{}, Body: http.NoBody}, nil
			}
			return jsonResponse(&oversync.SnapshotChunkResponse{
				SnapshotID: "snapshot-2", SnapshotBundleSeq: 12,
				Rows:           snapshotRows,
				NextRowOrdinal: 1,
			}), nil
		}
		return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{
			Error:   "unexpected_network",
			Message: "pending remote_replace recovery should use staged snapshot only",
		}), nil
	})

	setOperationStateForTest(t, db, &operationStateRecord{
		Kind:              operationKindRemoteReplace,
		TargetUserID:      "lifecycle-user",
		StagedSnapshotID:  "snapshot-1",
		SnapshotBundleSeq: 11,
		SnapshotRowCount:  1,
	})
	_, err := db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-1', 1, 'main', 'users', ?, 4, ?)
	`, rowKeyJSON("remote-1"), `{"id":"remote-1","name":"Remote","email":"remote@example.com"}`)
	require.NoError(t, err)

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	require.Equal(t, AttachOutcomeUsedRemote, result.Outcome)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "remote-2").Scan(&name))
	require.Equal(t, "Remote 2", name)

	var pendingTransition string
	require.NoError(t, db.QueryRow(`SELECT kind FROM _sync_operation_state WHERE singleton_key = 1`).Scan(&pendingTransition))
	require.Equal(t, "none", pendingTransition)
}

func TestConnect_RemoteAuthoritativeStagesAndReplacesAnonymousLocalRows(t *testing.T) {
	snapshotRows := []oversync.SnapshotRow{{
		Schema: "main", Table: "users", Key: oversync.SyncKey{"id": "remote-1"}, RowVersion: 7,
		Payload: mustJSONPayload(t, map[string]any{"id": "remote-1", "name": "Remote", "email": "remote@example.com"}),
	}}
	var client *Client
	var db *sql.DB
	client, db = newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			return jsonResponse(&oversync.ConnectResponse{Resolution: "remote_authoritative"}), nil
		case "/sync/snapshot-sessions":
			return snapshotFixtureResponse(t, client, r, "snapshot-remote", 9, snapshotRows), nil
		case "/sync/snapshot-sessions/snapshot-remote":
			return jsonResponse(&oversync.SnapshotChunkResponse{
				SnapshotID:        "snapshot-remote",
				SnapshotBundleSeq: 9,
				Rows:              snapshotRows,
				NextRowOrdinal:    1,
				HasMore:           false,
			}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "local-1", "Local", "local@example.com")
	require.NoError(t, err)
	mustOpen(t, client, context.Background())

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	require.Equal(t, AttachOutcomeUsedRemote, result.Outcome)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "local-1").Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "remote-1").Scan(&count))
	require.Equal(t, 1, count)
	require.Equal(t, "lifecycle-user", client.UserID)
	require.True(t, client.sessionConnected)
	attachment, err := loadAttachmentState(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, attachmentBindingAttached, attachment.BindingState)
	require.Equal(t, "lifecycle-user", attachment.AttachedUserID)
	operation, err := loadOperationState(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, operationKindNone, operation.Kind)
	require.Equal(t, "lifecycle-user", baselineText(t, db, `SELECT json_extract(server_provenance,'$.account_scope.account_id') FROM _sync_row_state WHERE table_name='users'`))
	require.Equal(t, "snapshot", baselineText(t, db, `SELECT json_extract(server_provenance,'$.origin') FROM _sync_row_state WHERE table_name='users'`))
	require.Equal(t, "9", baselineText(t, db, `SELECT json_extract(server_provenance,'$.bundle_sequence') FROM _sync_row_state WHERE table_name='users'`))
}

func TestConnect_RemoteAuthoritativeFailureDoesNotAdvancePublicIdentity(t *testing.T) {
	for _, testCase := range []struct {
		name                 string
		pendingRemoteReplace bool
		expectedConnectCalls int64
	}{
		{name: "new remote replace", expectedConnectCalls: 1},
		{name: "pending remote replace recovery", pendingRemoteReplace: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var connectCalls atomic.Int64
			client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/sync/generations/1":
					return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
				case "/sync/connect":
					connectCalls.Add(1)
					return jsonResponse(&oversync.ConnectResponse{Resolution: "remote_authoritative"}), nil
				case "/sync/snapshot-sessions":
					return errorJSONResponse(http.StatusBadRequest, oversync.ErrorResponse{Error: "snapshot_rejected"}), nil
				default:
					return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
				}
			})
			if testCase.pendingRemoteReplace {
				setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindRemoteReplace, TargetUserID: "lifecycle-user"})
			}

			_, err := client.Attach(context.Background(), "lifecycle-user")
			require.ErrorContains(t, err, "server returned status 400")
			require.Empty(t, client.UserID)
			require.False(t, client.sessionConnected)
			require.Equal(t, testCase.expectedConnectCalls, connectCalls.Load())

			attachment, loadErr := loadAttachmentState(context.Background(), db)
			require.NoError(t, loadErr)
			require.Equal(t, attachmentBindingAnonymous, attachment.BindingState)
			require.Empty(t, attachment.AttachedUserID)
			operation, loadErr := loadOperationState(context.Background(), db)
			require.NoError(t, loadErr)
			require.Equal(t, operationKindRemoteReplace, operation.Kind)
			require.Equal(t, "lifecycle-user", operation.TargetUserID)
		})
	}
}

func TestDetach_CancelsPendingRemoteReplace(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	setOperationStateForTest(t, db, &operationStateRecord{
		Kind:              operationKindRemoteReplace,
		TargetUserID:      "lifecycle-user",
		StagedSnapshotID:  "snapshot-1",
		SnapshotBundleSeq: 11,
		SnapshotRowCount:  1,
	})
	_, err := db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-1', 1, 'main', 'users', ?, 4, ?)
	`, rowKeyJSON("remote-1"), `{"id":"remote-1","name":"Remote","email":"remote@example.com"}`)
	require.NoError(t, err)

	mustDetach(t, client, context.Background())

	var (
		bindingState      string
		pendingTransition string
		stageCount        int
	)
	require.NoError(t, db.QueryRow(`SELECT binding_state FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&bindingState))
	require.NoError(t, db.QueryRow(`SELECT kind FROM _sync_operation_state WHERE singleton_key = 1`).Scan(&pendingTransition))
	require.Equal(t, lifecycleBindingAnonymous, bindingState)
	require.Equal(t, lifecycleTransitionNone, pendingTransition)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&stageCount))
	require.Equal(t, 0, stageCount)
}

func TestUninstallSync_RemovesMetadataAndTriggersButPreservesBusinessTables(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	require.ErrorContains(t, client.UninstallSync(context.Background()), "unresolved work")
	client.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.NoError(t, client.UninstallSync(context.Background()))

	for _, tableName := range syncMetadataTableNames {
		var count int
		require.NoError(t, db.QueryRow(`
			SELECT COUNT(*)
			FROM sqlite_master
			WHERE type = 'table' AND name = ?
		`, tableName).Scan(&count))
		require.Equalf(t, 0, count, "metadata table %s should be removed", tableName)
	}

	for _, triggerName := range []string{
		"trg_users_bi_guard",
		"trg_users_bu_guard",
		"trg_users_bd_guard",
		"trg_users_ai",
		"trg_users_au",
		"trg_users_ad",
	} {
		var count int
		require.NoError(t, db.QueryRow(`
			SELECT COUNT(*)
			FROM sqlite_master
			WHERE type = 'trigger' AND name = ?
		`, triggerName).Scan(&count))
		require.Equalf(t, 0, count, "trigger %s should be removed", triggerName)
	}

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "2", "Grace", "grace@example.com")
	require.NoError(t, err)

	var userCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&userCount))
	require.Equal(t, 2, userCount)

	err = client.open(context.Background())
	var closedErr *ClientClosedError
	require.ErrorAs(t, err, &closedErr)
}

func TestUninstallSync_BorrowedFixtureCanReinstallForLowerLevelCoverage(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	mustOpen(t, client, context.Background())
	require.NoError(t, client.UninstallSync(context.Background()))

	reinstalled, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	reinstalled.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})}
	t.Cleanup(func() { require.NoError(t, reinstalled.Close()) })

	for _, tableName := range syncMetadataTableNames {
		var count int
		require.NoError(t, db.QueryRow(`
			SELECT COUNT(*)
			FROM sqlite_master
			WHERE type = 'table' AND name = ?
		`, tableName).Scan(&count))
		require.Equalf(t, 1, count, "metadata table %s should be recreated", tableName)
	}

	mustOpen(t, reinstalled, context.Background())
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "3", "Linus", "linus@example.com")
	require.NoError(t, err)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
}
func TestProtocolVersionGateRejectsV0EmptyAndUnknown(t *testing.T) {
	for _, actual := range []string{"v0", "", hostileProtocolVersion} {
		t.Run(actual, func(t *testing.T) {
			err := requireSupportedProtocolVersion(actual)
			var mismatch *ProtocolVersionMismatchError
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, "v1", mismatch.Expected)
			require.Empty(t, mismatch.Actual)
			require.Equal(t, "oversqlite protocol version mismatch", mismatch.Error())
			require.NotContains(t, mismatch.Error(), hostileProtocolVersion)
		})
	}
}
