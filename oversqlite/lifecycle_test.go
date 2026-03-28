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
	require.NoError(t, client.Open(context.Background(), "device-a"))
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
	require.NoError(t, client.Open(context.Background(), "device-a"))
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
			return jsonResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{
					"connect_lifecycle": true,
				},
			}), nil
		case "/sync/connect":
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

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusRetryLater, result.Status)
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

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, result.Status)
	require.Equal(t, ConnectOutcomeStartedEmpty, result.Outcome)
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

	_, err := client.Connect(context.Background(), "lifecycle-user")
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

	_, err := client.Connect(context.Background(), "lifecycle-user")
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

	_, err := client.Connect(context.Background(), "lifecycle-user")
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
	require.NoError(t, client.Open(context.Background(), "device-a"))

	_, err = client.Connect(context.Background(), "lifecycle-user")
	require.ErrorContains(t, err, "connect network failed")
	require.Empty(t, client.UserID)

	var (
		bindingState      string
		bindingScope      string
		pendingTransition string
		dirtyCount        int
		userCount         int
	)
	require.NoError(t, db.QueryRow(`
		SELECT binding_state, binding_scope, pending_transition_kind
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`).Scan(&bindingState, &bindingScope, &pendingTransition))
	require.Equal(t, lifecycleBindingAnonymous, bindingState)
	require.Empty(t, bindingScope)
	require.Equal(t, lifecycleTransitionNone, pendingTransition)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&userCount))
	require.Equal(t, 1, userCount)
}

func TestSignOut_BlocksWhenAttachedDirtyRowsExist(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, result.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	err = client.SignOut(context.Background())
	var blockedErr *SignOutBlockedError
	require.ErrorAs(t, err, &blockedErr)
	require.Equal(t, 1, blockedErr.PendingRowCount)
	require.Equal(t, "lifecycle-user", client.UserID)
}

func TestResetForNewSource_ClearsPendingLocalState(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	_, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	require.NoError(t, client.ResetForNewSource(context.Background(), "device-b"))
	require.Equal(t, "device-b", client.SourceID)
	require.Empty(t, client.UserID)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)
}

func TestOpen_FailsClosedOnPersistedSourceMismatch(t *testing.T) {
	client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	require.NoError(t, client.Open(context.Background(), "device-a"))

	err := client.Open(context.Background(), "device-b")
	var mismatchErr *SourceMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, "device-a", mismatchErr.PersistedSourceID)
	require.Equal(t, "device-b", mismatchErr.RequestedSourceID)
}

func TestOpen_IsIdempotentForMatchingSource(t *testing.T) {
	client, _ := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	require.NoError(t, client.Open(context.Background(), "device-a"))
	require.NoError(t, client.Open(context.Background(), "device-a"))
}

func TestOpen_CapturesAnonymousPreexistingRowsWithoutBlockingSignOut(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "1", "Ada", "ada@example.com")
	require.NoError(t, err)

	require.NoError(t, client.Open(context.Background(), "device-a"))

	status, err := client.PendingSyncStatus(context.Background())
	require.NoError(t, err)
	require.True(t, status.HasPendingSyncData)
	require.Equal(t, 1, status.PendingRowCount)
	require.False(t, status.BlocksSignOut)
}

func TestConnect_FailsFastWhenLifecycleCapabilitiesAreMissing(t *testing.T) {
	client, _ := newLifecycleTestClientWithTransport(t, func(r *http.Request) (*http.Response, error) {
		return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
			Error:   "not_found",
			Message: "missing",
		}), nil
	})

	_, err := client.Connect(context.Background(), "lifecycle-user")
	var unsupportedErr *ConnectLifecycleUnsupportedError
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

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectOutcomeSeededLocal, result.Outcome)

	var pendingInitializationID string
	require.NoError(t, db.QueryRow(`SELECT pending_initialization_id FROM _sync_lifecycle_state WHERE singleton_key = 1`).Scan(&pendingInitializationID))
	require.Equal(t, "init-1", pendingInitializationID)

	err = client.PushPending(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), errorCode)
	require.Empty(t, client.UserID)
	require.Empty(t, client.pendingInitializationID)

	var (
		bindingState string
		bindingScope string
	)
	require.NoError(t, db.QueryRow(`
		SELECT binding_state, binding_scope, pending_initialization_id
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`).Scan(&bindingState, &bindingScope, &pendingInitializationID))
	require.Equal(t, lifecycleBindingAnonymous, bindingState)
	require.Empty(t, bindingScope)
	require.Empty(t, pendingInitializationID)

	reconnectResult, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectOutcomeSeededLocal, reconnectResult.Outcome)
	require.Equal(t, int32(2), atomic.LoadInt32(&connectRequests))
	require.Equal(t, "lifecycle-user", client.UserID)
	require.Equal(t, "init-2", client.pendingInitializationID)
	require.NoError(t, db.QueryRow(`SELECT pending_initialization_id FROM _sync_lifecycle_state WHERE singleton_key = 1`).Scan(&pendingInitializationID))
	require.Equal(t, "init-2", pendingInitializationID)
}

func TestConnect_ResumesSameAttachedUserWithoutNetwork(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{
		Resolution: "initialize_empty",
	})

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, result.Status)

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

	require.NoError(t, restarted.Open(context.Background(), "device-a"))

	resumeResult, err := restarted.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, resumeResult.Status)
	require.Equal(t, ConnectOutcomeResumedAttached, resumeResult.Outcome)
	require.Equal(t, int32(0), atomic.LoadInt32(&requestCount))
}

func TestOpen_ReportsPendingRemoteReplaceRecovery(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'remote_replace',
		    pending_target_scope = 'lifecycle-user',
		    pending_target_source_id = 'device-a'
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	err = client.Open(context.Background(), "device-a")
	var pendingErr *RemoteReplacePendingError
	require.ErrorAs(t, err, &pendingErr)
	require.Equal(t, "lifecycle-user", pendingErr.TargetUserID)
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

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectOutcomeUsedRemote, result.Outcome)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state WHERE table_name = 'legacy_docs'`).Scan(&count))
	require.Equal(t, 0, count)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "remote-1").Scan(&name))
	require.Equal(t, "Remote", name)
}

func TestOpen_ClearsStaleInterruptedSignOutMarkers(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)

	_, err = db.Exec(`
		UPDATE _sync_client_state
		SET apply_mode = 1
		WHERE user_id = 'lifecycle-user'
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'signout_reset',
		    pending_apply_cursor = 9,
		    runtime_bypass_active = 1
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	require.NoError(t, client.Open(context.Background(), "device-a"))

	var (
		applyMode         int
		bindingState      string
		bindingScope      string
		pendingTransition string
		pendingCursor     int64
		runtimeBypass     int
	)
	require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_client_state WHERE user_id = 'lifecycle-user'`).Scan(&applyMode))
	require.Equal(t, 0, applyMode)
	require.NoError(t, db.QueryRow(`
		SELECT binding_state, binding_scope, pending_transition_kind, pending_apply_cursor, runtime_bypass_active
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`).Scan(&bindingState, &bindingScope, &pendingTransition, &pendingCursor, &runtimeBypass))
	require.Equal(t, lifecycleBindingAttached, bindingState)
	require.Equal(t, "lifecycle-user", bindingScope)
	require.Equal(t, lifecycleTransitionNone, pendingTransition)
	require.EqualValues(t, 0, pendingCursor)
	require.Equal(t, 0, runtimeBypass)
}

func TestOpen_CancelsInterruptedSourceResetAndPreservesCurrentSource(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)

	_, err = db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'source_reset',
		    pending_target_source_id = 'device-b',
		    pending_apply_cursor = 3,
		    runtime_bypass_active = 1
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	require.NoError(t, client.Open(context.Background(), "device-a"))

	var (
		sourceID          string
		pendingTransition string
		pendingTarget     string
		pendingCursor     int64
		runtimeBypass     int
	)
	require.NoError(t, db.QueryRow(`
		SELECT source_id, pending_transition_kind, pending_target_source_id, pending_apply_cursor, runtime_bypass_active
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`).Scan(&sourceID, &pendingTransition, &pendingTarget, &pendingCursor, &runtimeBypass))
	require.Equal(t, "device-a", sourceID)
	require.Equal(t, lifecycleTransitionNone, pendingTransition)
	require.Empty(t, pendingTarget)
	require.EqualValues(t, 0, pendingCursor)
	require.Equal(t, 0, runtimeBypass)
}

func TestConnect_PendingRemoteReplaceMismatchedScopeFailsClosed(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'remote_replace',
		    pending_target_scope = 'other-user',
		    pending_target_source_id = 'device-a'
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	_, err = client.Connect(context.Background(), "lifecycle-user")
	var conflictErr *ConnectLocalStateConflictError
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

	_, err := db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'remote_replace',
		    pending_target_scope = 'lifecycle-user',
		    pending_target_source_id = 'device-a',
		    pending_staged_snapshot_id = 'snapshot-1',
		    pending_snapshot_bundle_seq = 11,
		    pending_snapshot_row_count = 1
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-1', 1, 'main', 'users', ?, 4, ?)
	`, rowKeyJSON("remote-1"), `{"id":"remote-1","name":"Remote","email":"remote@example.com"}`)
	require.NoError(t, err)

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, result.Status)
	require.Equal(t, ConnectOutcomeUsedRemote, result.Outcome)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, "remote-1").Scan(&name))
	require.Equal(t, "Remote", name)

	var pendingTransition string
	require.NoError(t, db.QueryRow(`SELECT pending_transition_kind FROM _sync_lifecycle_state WHERE singleton_key = 1`).Scan(&pendingTransition))
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
	require.NoError(t, client.Open(context.Background(), "device-a"))

	result, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)
	require.Equal(t, ConnectStatusConnected, result.Status)
	require.Equal(t, ConnectOutcomeUsedRemote, result.Outcome)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "local-1").Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, "remote-1").Scan(&count))
	require.Equal(t, 1, count)
}

func TestPushPending_FailsClosedWhileDestructiveTransitionIsMarked(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := client.Connect(context.Background(), "lifecycle-user")
	require.NoError(t, err)

	_, err = db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'signout_reset',
		    runtime_bypass_active = 1
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)

	err = client.PushPending(context.Background())
	var transitionErr *DestructiveTransitionInProgressError
	require.ErrorAs(t, err, &transitionErr)
	require.Equal(t, lifecycleTransitionSignOut, transitionErr.TransitionKind)
}

func TestSignOut_CancelsPendingRemoteReplace(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := db.Exec(`
		UPDATE _sync_lifecycle_state
		SET pending_transition_kind = 'remote_replace',
		    pending_target_scope = 'lifecycle-user',
		    pending_target_source_id = 'device-a',
		    pending_staged_snapshot_id = 'snapshot-1',
		    pending_snapshot_bundle_seq = 11,
		    pending_snapshot_row_count = 1
		WHERE singleton_key = 1
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-1', 1, 'main', 'users', ?, 4, ?)
	`, rowKeyJSON("remote-1"), `{"id":"remote-1","name":"Remote","email":"remote@example.com"}`)
	require.NoError(t, err)

	require.NoError(t, client.SignOut(context.Background()))

	var (
		bindingState      string
		pendingTransition string
		stageCount        int
		clientStateCount  int
	)
	require.NoError(t, db.QueryRow(`SELECT binding_state, pending_transition_kind FROM _sync_lifecycle_state WHERE singleton_key = 1`).Scan(&bindingState, &pendingTransition))
	require.Equal(t, lifecycleBindingAnonymous, bindingState)
	require.Equal(t, lifecycleTransitionNone, pendingTransition)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&stageCount))
	require.Equal(t, 0, stageCount)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_client_state`).Scan(&clientStateCount))
	require.Equal(t, 0, clientStateCount)
}

func TestUninstallSync_RemovesMetadataAndTriggersButPreservesBusinessTables(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	_, err := client.Connect(context.Background(), "lifecycle-user")
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

	for _, triggerName := range []string{"trg_users_ai", "trg_users_au", "trg_users_ad"} {
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

	err = client.Open(context.Background(), "device-a")
	var closedErr *ClientClosedError
	require.ErrorAs(t, err, &closedErr)
}

func TestUninstallSync_AllowsFreshReinstallOnSameDatabase(t *testing.T) {
	client, db := newLifecycleTestClient(t, &oversync.ConnectResponse{Resolution: "initialize_empty"})

	require.NoError(t, client.Open(context.Background(), "device-a"))
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

	require.NoError(t, reinstalled.Open(context.Background(), "device-b"))
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, "3", "Linus", "linus@example.com")
	require.NoError(t, err)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
}
