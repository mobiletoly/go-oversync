package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL
	)
`

func newLifecycleTestClientWithTransport(t *testing.T, transport roundTripFunc) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(lifecycleUsersDDL)
	require.NoError(t, err)

	config := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	config.RetryPolicy = &RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		JitterFraction: 0,
	}

	client, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, config)
	require.NoError(t, err)
	client.sourceIDGenerator = func() string { return "device-a" }
	openResult := mustOpen(t, client, context.Background())
	require.Equal(t, OpenStateReadyAnonymous, openResult.State)
	client.HTTP = &http.Client{Transport: transport}

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })
	return client, db
}

func newLifecycleTestClientWithConfigAndTransport(t *testing.T, config *Config, transport roundTripFunc) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(lifecycleUsersDDL)
	require.NoError(t, err)

	client, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
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
		case "/sync/capabilities":
			requireAuthenticatedSyncHeaders(t, r, "device-a")
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{
					"connect_lifecycle": true,
				},
			}), nil
		case "/sync/connect":
			requireAuthenticatedSyncHeaders(t, r, "device-a")
			return jsonResponse(connectResponse), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})
}

func TestConnect_RetryLaterSurfacesRetryAfter(t *testing.T) {
	client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution:    "retry_later",
		RetryAfterSec: 7,
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
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
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
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
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

func TestConnect_RetryExhaustionReturnsTypedError(t *testing.T) {
	var connectRequests int32
	client, _ := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
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
	config := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	config.RetryPolicy = &RetryPolicy{Enabled: false}

	var connectRequests int32
	client, _ := newLifecycleTestClientWithConfigAndTransport(t, config, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
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
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{
					"connect_lifecycle": true,
				},
			}), nil
		case "/sync/connect":
			return nil, fmt.Errorf("connect network failed")
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)
	mustOpen(t, client, context.Background())

	_, err = client.Attach(context.Background(), "lifecycle-user")
	require.ErrorContains(t, err, "connect network failed")
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

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	detachResult, err := client.Detach(context.Background())
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detachResult.Outcome)
	require.EqualValues(t, 1, detachResult.PendingRowCount)
	require.Equal(t, "lifecycle-user", client.UserID)
}

func TestDetach_BlocksWhenPreparedOutboxExists(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, err = db.Exec(`
		UPDATE _sync_outbox_bundle
		SET state = 'prepared', source_id = ?, source_bundle_id = 1, row_count = 1, canonical_request_hash = 'hash'
		WHERE singleton_key = 1
	`, client.sourceID)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_outbox_rows (
			source_bundle_id, row_ordinal, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, wire_payload
		) VALUES (1, 0, 'main', 'users', ?, ?, 'INSERT', 0, ?, ?)
	`, rowKeyJSON("1"), rowKeyJSON("1"), `{"id":"1","name":"Ada","email":"ada@example.com"}`, `{"id":"1","name":"Ada","email":"ada@example.com"}`)
	require.NoError(t, err)

	detachResult, err := client.Detach(context.Background())
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detachResult.Outcome)
	require.EqualValues(t, 1, detachResult.PendingRowCount)
	require.Equal(t, "lifecycle-user", client.UserID)
}

func TestDetach_BlocksWhenCommittedRemoteOutboxExists(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, err = db.Exec(`
		UPDATE _sync_outbox_bundle
		SET state = 'committed_remote',
			source_id = ?,
			source_bundle_id = 1,
			row_count = 1,
			canonical_request_hash = 'hash',
			remote_bundle_hash = 'remote-hash',
			remote_bundle_seq = 7
		WHERE singleton_key = 1
	`, client.sourceID)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_outbox_rows (
			source_bundle_id, row_ordinal, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, wire_payload
		) VALUES (1, 0, 'main', 'users', ?, ?, 'INSERT', 0, ?, ?)
	`, rowKeyJSON("1"), rowKeyJSON("1"), `{"id":"1","name":"Ada","email":"ada@example.com"}`, `{"id":"1","name":"Ada","email":"ada@example.com"}`)
	require.NoError(t, err)

	detachResult, err := client.Detach(context.Background())
	require.NoError(t, err)
	require.Equal(t, DetachOutcomeBlockedUnsyncedData, detachResult.Outcome)
	require.EqualValues(t, 1, detachResult.PendingRowCount)
	require.Equal(t, "lifecycle-user", client.UserID)
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

	reopened, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
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

func TestSourceInfo_ReportsManagedSourceWithCommittedRemoteOutbox(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	_, err = db.Exec(`
		UPDATE _sync_outbox_bundle
		SET state = 'committed_remote',
			source_id = ?,
			source_bundle_id = 1,
			row_count = 1,
			canonical_request_hash = 'hash',
			remote_bundle_hash = 'remote-hash',
			remote_bundle_seq = 7
		WHERE singleton_key = 1
	`, client.sourceID)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_outbox_rows (
			source_bundle_id, row_ordinal, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, wire_payload
		) VALUES (1, 0, 'main', 'users', ?, ?, 'INSERT', 0, ?, ?)
	`, rowKeyJSON("1"), rowKeyJSON("1"), `{"id":"1","name":"Ada","email":"ada@example.com"}`, `{"id":"1","name":"Ada","email":"ada@example.com"}`)
	require.NoError(t, err)

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

	reopened, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
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

	_, err := client.Open(ctx)
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

	reopened, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
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

func testReconnectAfterInitializationLeaseFailure(t *testing.T, statusCode int, errorCode string) {
	t.Helper()

	var connectRequests int32
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
		case "/sync/connect":
			switch atomic.AddInt32(&connectRequests, 1) {
			case 1:
				return jsonResponse(&oversync.ConnectResponse{
					Resolution:       "initialize_local",
					InitializationID: "init-1",
				}), nil
			case 2:
				return jsonResponse(&oversync.ConnectResponse{
					Resolution:       "initialize_local",
					InitializationID: "init-2",
				}), nil
			default:
				return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{
					Error:   "unexpected_connect",
					Message: "unexpected connect attempt",
				}), nil
			}
		case "/sync/push-sessions":
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
	require.Equal(t, "init-1", pendingInitializationID)

	_, err = client.PushPending(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), errorCode)
	require.Empty(t, client.UserID)
	require.Empty(t, client.pendingInitializationID)

	var (
		bindingState string
		bindingScope string
	)
	require.NoError(t, db.QueryRow(`
		SELECT binding_state, attached_user_id, pending_initialization_id
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(&bindingState, &bindingScope, &pendingInitializationID))
	require.Equal(t, lifecycleBindingAnonymous, bindingState)
	require.Empty(t, bindingScope)
	require.Empty(t, pendingInitializationID)

	reconnectResult, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachOutcomeSeededLocal, reconnectResult.Outcome)
	require.Equal(t, int32(2), atomic.LoadInt32(&connectRequests))
	require.Equal(t, "lifecycle-user", client.UserID)
	require.Equal(t, "init-2", client.pendingInitializationID)
	pendingInitializationID = requirePendingInitializationID(t, db)
	require.Equal(t, "init-2", pendingInitializationID)
}

func TestConnect_ResumesSameAttachedUserWithoutNetwork(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)

	require.NoError(t, client.Close())

	var requestCount int32
	restarted, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
	require.NoError(t, err)
	restarted.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requestCount, 1)
		return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{
			Error:   "unexpected_network",
			Message: "resume should not hit the network",
		}), nil
	})}
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })

	mustOpen(t, restarted, context.Background())

	resumeResult, err := restarted.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, resumeResult.Status)
	require.Equal(t, AttachOutcomeResumedAttached, resumeResult.Outcome)
	require.Equal(t, int32(0), atomic.LoadInt32(&requestCount))
}

func TestOpen_ReportsPendingRemoteReplaceRecovery(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	setOperationStateForTest(t, db, &operationStateRecord{Kind: operationKindRemoteReplace, TargetUserID: "lifecycle-user"})

	openResult := mustOpen(t, client, context.Background())
	require.Equal(t, OpenStateAttachRecoveryRequired, openResult.State)
	require.Equal(t, "lifecycle-user", openResult.TargetUserID)
}

func TestConnect_RemoteAuthoritativeReplaceClearsHistoricallyManagedTablesRemovedFromConfig(t *testing.T) {
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{"connect_lifecycle": true},
			}), nil
		case "/sync/connect":
			return jsonResponse(&oversync.ConnectResponse{Resolution: "remote_authoritative"}), nil
		case "/sync/snapshot-sessions":
			return jsonResponse(oversync.SnapshotSession{
				SnapshotID:        "snapshot-remote-replace",
				SnapshotBundleSeq: 7,
				RowCount:          1,
				ExpiresAt:         "2030-01-01T00:00:00Z",
			}), nil
		case "/sync/snapshot-sessions/snapshot-remote-replace":
			switch r.Method {
			case http.MethodGet:
				return jsonResponse(oversync.SnapshotChunkResponse{
					SnapshotID:        "snapshot-remote-replace",
					SnapshotBundleSeq: 7,
					Rows: []oversync.SnapshotRow{{
						Schema:     "main",
						Table:      "users",
						Key:        oversync.SyncKey{"id": "remote-1"},
						RowVersion: 7,
						Payload: json.RawMessage(`{
							"id":"remote-1",
							"name":"Remote",
							"email":"remote@example.com"
						}`),
					}},
					NextRowOrdinal: 1,
					HasMore:        false,
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
			id TEXT PRIMARY KEY,
			body TEXT NOT NULL
		)
	`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO legacy_docs (id, body) VALUES (?, ?)`, "legacy-1", "stale body")
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_managed_tables (schema_name, table_name)
		VALUES ('main', 'legacy_docs')
		ON CONFLICT(schema_name, table_name) DO NOTHING
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_row_state (schema_name, table_name, key_json, row_version, deleted)
		VALUES ('main', 'legacy_docs', ?, 9, 0)
	`, rowKeyJSON("legacy-1"))
	require.NoError(t, err)

	result, err := client.Attach(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, AttachOutcomeUsedRemote, result.Outcome)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 0, count)
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

func TestConnect_PendingRemoteReplaceFinalizesFromStagedSnapshotWithoutNetwork(t *testing.T) {
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
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
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "remote-1").Scan(&name))
	require.Equal(t, "Remote", name)

	var pendingTransition string
	require.NoError(t, db.QueryRow(`SELECT kind FROM _sync_operation_state WHERE singleton_key = 1`).Scan(&pendingTransition))
	require.Equal(t, "none", pendingTransition)
}

func TestConnect_RemoteAuthoritativeStagesAndReplacesAnonymousLocalRows(t *testing.T) {
	client, db := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{
					"connect_lifecycle": true,
				},
			}), nil
		case "/sync/connect":
			return jsonResponse(&oversync.ConnectResponse{Resolution: "remote_authoritative"}), nil
		case "/sync/snapshot-sessions":
			return jsonResponse(&oversync.SnapshotSession{
				SnapshotID:        "snapshot-remote",
				SnapshotBundleSeq: 9,
				RowCount:          1,
				ExpiresAt:         time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
			}), nil
		case "/sync/snapshot-sessions/snapshot-remote":
			return jsonResponse(&oversync.SnapshotChunkResponse{
				SnapshotID:        "snapshot-remote",
				SnapshotBundleSeq: 9,
				Rows: []oversync.SnapshotRow{{
					Schema:     "main",
					Table:      "users",
					Key:        oversync.SyncKey{"id": "remote-1"},
					RowVersion: 7,
					Payload:    mustJSONPayload(t, map[string]any{"id": "remote-1", "name": "Remote", "email": "remote@example.com"}),
				}},
				NextRowOrdinal: 1,
				HasMore:        false,
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

	_, err = client.Open(context.Background())
	var closedErr *ClientClosedError
	require.ErrorAs(t, err, &closedErr)
}

func TestUninstallSync_AllowsFreshReinstallOnSameDatabase(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	mustOpen(t, client, context.Background())
	require.NoError(t, client.UninstallSync(context.Background()))

	reinstalled, err := NewClient(db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}))
	require.NoError(t, err)
	reinstalled.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{
					"connect_lifecycle": true,
				},
			}), nil
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
