package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	_ "github.com/mattn/go-sqlite3"
	exampleserver "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_ConnectBeforeOpenReturnsTypedLifecycleError(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_before_open_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(usersDDL)
	require.NoError(t, err)

	userID := "e2e-connect-before-open-user-" + uuid.NewString()
	var client *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(userID, time.Hour)
	}
	client, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err = client.Attach(ctx, userID)
	var openErr *oversqlite.OpenRequiredError
	require.ErrorAs(t, err, &openErr)
	require.True(t, oversqlite.IsLifecyclePreconditionError(err))
}

func TestEndToEnd_SyncOperationsBeforeConnectReturnTypedLifecycleErrors(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_sync_before_connect_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-sync-before-connect-user-" + uuid.NewString()
	client, _ := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	var requestCount atomic.Int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestCount.Add(1)
		return nil, context.DeadlineExceeded
	})}

	operations := []struct {
		name string
		run  func() error
	}{
		{name: "PushPending", run: func() error { _, err := client.PushPending(ctx); return err }},
		{name: "PullToStable", run: func() error { _, err := client.PullToStable(ctx); return err }},
		{name: "Sync", run: func() error { _, err := client.Sync(ctx); return err }},
		{name: "Rebuild", run: func() error { _, err := client.Rebuild(ctx); return err }},
	}
	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var connectErr *oversqlite.AttachRequiredError
			require.ErrorAs(t, err, &connectErr)
			require.True(t, oversqlite.IsLifecyclePreconditionError(err))
		})
	}

	_, err := client.LastBundleSeqSeen(ctx)
	var connectErr *oversqlite.AttachRequiredError
	require.ErrorAs(t, err, &connectErr)
	require.True(t, oversqlite.IsLifecyclePreconditionError(err))
	require.Zero(t, requestCount.Load())
}

func TestEndToEnd_ConnectInitializeEmptyThenPushWorks(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_empty_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-empty-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)
	require.Equal(t, oversqlite.AttachOutcomeStartedEmpty, connectResult.Outcome)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Alpha", "alpha@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, client, ctx)

	require.Equal(t, "INITIALIZED", requireServerScopeState(t, server, userID))
}

func TestEndToEnd_ConnectRemoteAuthoritativeRebuildsExistingRemote(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_remote_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-remote-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectA, err := clientA.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectA.Status)

	rowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	connectB, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.AttachOutcomeUsedRemote, connectB.Outcome)

	var name string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Remote", name)
}

func TestEndToEnd_ConnectRemoteAuthoritativeReplacesAnonymousLocalRows(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_replace_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-replace-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectA, err := clientA.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectA.Status)

	remoteRowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, remoteRowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	localOnlyID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localOnlyID, "Local", "local@example.com")
	require.NoError(t, err)
	mustOpenE2E(t, clientB, ctx, "device-b")

	connectB, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.AttachOutcomeUsedRemote, connectB.Outcome)

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, localOnlyID).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, remoteRowID).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEndToEnd_ConnectInitializeLocalThenSeedRemote(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_local_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-local-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Seed", "seed@example.com")
	require.NoError(t, err)

	connectA, err := clientA.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectA.Status)
	require.Equal(t, oversqlite.AttachOutcomeSeededLocal, connectA.Outcome)

	mustPushPendingE2E(t, clientA, ctx)

	connectB, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.AttachOutcomeUsedRemote, connectB.Outcome)

	var name string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Seed", name)
}

func TestEndToEnd_ConnectRemoteAuthoritativeEmptyStillReplacesAnonymousLocalRows(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_empty_remote_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-empty-remote-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectA, err := clientA.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectA.Status)

	remoteRowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, remoteRowID, "Transient", "transient@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	_, err = dbA.Exec(`DELETE FROM users WHERE id = ?`, remoteRowID)
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	localOnlyID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localOnlyID, "Local", "local@example.com")
	require.NoError(t, err)
	mustOpenE2E(t, clientB, ctx, "device-b")

	connectB, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.AttachOutcomeUsedRemote, connectB.Outcome)

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 0, count)
}

func TestEndToEnd_ConnectResumesSameAttachedUserWithoutNetwork(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_resume_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-resume-user-" + uuid.NewString()
	dbClient, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := dbClient.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)
	require.NoError(t, dbClient.Close())

	var restarted *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(userID, time.Hour)
	}
	restarted, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	restarted.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })

	mustOpenE2E(t, restarted, ctx, "device-a")

	resumeResult, err := restarted.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, resumeResult.Status)
	require.Equal(t, oversqlite.AttachOutcomeResumedAttached, resumeResult.Outcome)
}

func TestEndToEnd_DetachBlocksWithAttachedDirtyRows(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_detach_blocked_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-detach-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Pending", "pending@example.com")
	require.NoError(t, err)

	detachResult, err := client.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.DetachOutcomeBlockedUnsyncedData, detachResult.Outcome)
	require.Greater(t, detachResult.PendingRowCount, int64(0))
}

func TestEndToEnd_ConnectNetworkFailureLeavesAnonymousLocalStateIntact(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_network_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-network-failure-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Offline", "offline@example.com")
	require.NoError(t, err)
	mustOpenE2E(t, client, ctx, "device-a")

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}

	_, err = client.Attach(ctx, userID)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	status, err := client.PendingSyncStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.HasPendingSyncData)
	require.EqualValues(t, 1, status.PendingRowCount)
	require.False(t, status.BlocksDetach)

	client.HTTP = &http.Client{Timeout: 120 * time.Second}
	result, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, result.Status)
	require.Equal(t, oversqlite.AttachOutcomeSeededLocal, result.Outcome)
}

func TestEndToEnd_ConnectRetryLaterSurfacesRetryAfterThenInitializeEmpty(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_retry_later_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServerWithConfig(t, schema, func(cfg *exampleserver.ServerConfig) {
		cfg.EnableEmptyFirstDeferral = true
		cfg.EmptyFirstDeferralDuration = 1200 * time.Millisecond
	})

	userID := "e2e-connect-retry-later-user-" + uuid.NewString()
	client, _ := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	first, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusRetryLater, first.Status)
	require.GreaterOrEqual(t, first.RetryAfter, time.Second)

	time.Sleep(1300 * time.Millisecond)

	second, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, second.Status)
	require.Equal(t, oversqlite.AttachOutcomeStartedEmpty, second.Outcome)
}

func TestEndToEnd_SyncThenDetachSuccessClearsAttachedLocalState(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_sync_then_signout_success_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-sync-then-signout-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Synced", "synced@example.com")
	require.NoError(t, err)

	result := mustSyncThenDetachE2E(t, client, ctx)
	require.True(t, result.IsSuccess())
	require.Empty(t, client.UserID)

	var remoteCount int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), rowID).Scan(&remoteCount))
	require.Equal(t, 1, remoteCount)

	var localCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 0, localCount)
}

func TestEndToEnd_SyncThenDetachFailureLeavesAttachedState(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_sync_then_signout_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-sync-then-signout-failure-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Pending", "pending@example.com")
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}

	_, err = client.SyncThenDetach(ctx)
	require.Error(t, err)
	require.Equal(t, userID, client.UserID)
}

func TestEndToEnd_DifferentUserCanConnectAfterSuccessfulDetach(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_different_user_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(usersDDL)
	require.NoError(t, err)

	currentUserID := "user-a-" + uuid.NewString()
	var client *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(currentUserID, time.Hour)
	}
	client, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	mustOpenE2E(t, client, ctx, "device-a")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })

	first, err := client.Attach(ctx, currentUserID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, first.Status)
	detachResult := mustDetachE2E(t, client, ctx)
	require.Equal(t, oversqlite.DetachOutcomeDetached, detachResult.Outcome)

	currentUserID = "user-b-" + uuid.NewString()
	second, err := client.Attach(ctx, currentUserID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, second.Status)
	require.Equal(t, currentUserID, client.UserID)
}

func TestEndToEnd_SameUserSameSourceCanDetachAttachAndPushAgain(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_same_source_reattach_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-same-source-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	rowOne := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowOne, "First", "first@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, client, ctx)
	require.Equal(t, int64(2), requireNextSourceBundleID(t, db))

	mustDetachE2E(t, client, ctx)
	mustOpenE2E(t, client, ctx, "device-a")

	connectResult, err = client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)
	require.Equal(t, int64(2), requireNextSourceBundleID(t, db))

	rowTwo := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowTwo, "Second", "second@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, client, ctx)

	require.Equal(t, int64(3), requireNextSourceBundleID(t, db))

	var remoteCount int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id IN ($1, $2)`, pgx.Identifier{schema}.Sanitize()), rowOne, rowTwo).Scan(&remoteCount))
	require.Equal(t, 2, remoteCount)
}

func TestEndToEnd_DetachedOfflineWritesSurviveReopenWithoutDuplicateCapture(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_detached_offline_writes_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-detached-offline-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	mustDetachE2E(t, client, ctx)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Offline", "offline@example.com")
	require.NoError(t, err)

	var dirtyCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'users'`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)

	mustOpenE2E(t, client, ctx, "device-a")
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'users'`).Scan(&dirtyCount))
	require.Equal(t, 1, dirtyCount)
}

func TestEndToEnd_OpenRestoresManagedSourceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_open_restore_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-open-restore-user-" + uuid.NewString()
	dbPath := filepath.Join(t.TempDir(), "managed-source.db")

	openPersistentClient := func() (*oversqlite.Client, *sql.DB) {
		db, err := sql.Open("sqlite3", dbPath)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		var tableCount int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'`).Scan(&tableCount))
		if tableCount == 0 {
			_, err = db.Exec(usersDDL)
			require.NoError(t, err)
		}

		tokenFn := func(ctx context.Context) (string, error) {
			return server.GenerateToken(userID, time.Hour)
		}
		client, err := oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
		require.NoError(t, err)
		return client, db
	}

	firstClient, firstDB := openPersistentClient()
	mustOpenE2E(t, firstClient, ctx, "")
	originalSourceID := requireCurrentSourceIDE2E(t, firstClient, ctx)
	require.NoError(t, firstClient.Close())
	require.NoError(t, firstDB.Close())

	restartedClient, restartedDB := openPersistentClient()
	defer restartedClient.Close()
	defer restartedDB.Close()
	mustOpenE2E(t, restartedClient, ctx, "")
	restartedSourceID := requireCurrentSourceIDE2E(t, restartedClient, ctx)
	require.Equal(t, originalSourceID, restartedSourceID)
}

func TestEndToEnd_DetachAfterOpenOverDurableAttachedStateSucceedsWithoutReattach(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_detach_after_open_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	currentUserID := "e2e-detach-after-open-user-a-" + uuid.NewString()
	nextUserID := "e2e-detach-after-open-user-b-" + uuid.NewString()
	dbPath := filepath.Join(t.TempDir(), "detach-after-open.db")

	openPersistentClient := func() (*oversqlite.Client, *sql.DB) {
		db, err := sql.Open("sqlite3", dbPath)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		var tableCount int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'`).Scan(&tableCount))
		if tableCount == 0 {
			_, err = db.Exec(usersDDL)
			require.NoError(t, err)
		}

		tokenFn := func(ctx context.Context) (string, error) {
			return server.GenerateToken(currentUserID, time.Hour)
		}
		client, err := oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
		require.NoError(t, err)
		return client, db
	}

	firstClient, firstDB := openPersistentClient()
	mustOpenE2E(t, firstClient, ctx, "")
	connectResult, err := firstClient.Attach(ctx, currentUserID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)
	require.NoError(t, firstClient.Close())
	require.NoError(t, firstDB.Close())

	restartedClient, restartedDB := openPersistentClient()
	defer restartedClient.Close()
	defer restartedDB.Close()
	mustOpenE2E(t, restartedClient, ctx, "")

	detachResult, err := restartedClient.Detach(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.DetachOutcomeDetached, detachResult.Outcome)

	currentUserID = nextUserID
	reconnectResult, err := restartedClient.Attach(ctx, currentUserID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, reconnectResult.Status)
	require.Equal(t, currentUserID, restartedClient.UserID)
}

func TestEndToEnd_UninstallSyncThenReinstallOpenAndConnectOnSameDatabase(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_uninstall_reinstall_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-uninstall-reinstall-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, client, ctx)

	require.NoError(t, client.UninstallSync(ctx))

	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, rowID)
	require.NoError(t, err)

	var reinstalled *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(userID, time.Hour)
	}
	reinstalled, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reinstalled.Close()) })

	mustOpenE2E(t, reinstalled, ctx, "device-a")

	reconnectResult, err := reinstalled.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, reconnectResult.Status)
	require.Equal(t, oversqlite.AttachOutcomeUsedRemote, reconnectResult.Outcome)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Remote", name)
}

func TestEndToEnd_ConcurrentFirstInitializerRaceHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_initializer_race_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-initializer-race-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "A", "a@example.com")
	require.NoError(t, err)
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "B", "b@example.com")
	require.NoError(t, err)
	mustOpenE2E(t, clientA, ctx, "device-a")
	mustOpenE2E(t, clientB, ctx, "device-b")

	start := make(chan struct{})
	results := make(chan oversqlite.AttachResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, tc := range []struct {
		userID string
		client *oversqlite.Client
	}{
		{userID: userID, client: clientA},
		{userID: userID, client: clientB},
	} {
		wg.Add(1)
		go func(client *oversqlite.Client, userID string) {
			defer wg.Done()
			<-start
			result, err := client.Attach(ctx, userID)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(tc.client, tc.userID)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var seededCount, retryCount int
	for result := range results {
		switch {
		case result.Status == oversqlite.AttachStatusConnected && result.Outcome == oversqlite.AttachOutcomeSeededLocal:
			seededCount++
		case result.Status == oversqlite.AttachStatusRetryLater:
			retryCount++
		}
	}
	require.Equal(t, 1, seededCount)
	require.Equal(t, 1, retryCount)
}

func TestEndToEnd_StaleInitializerLeaseExpiryRejectsSeedPush(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_initializer_expired_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServerWithConfig(t, schema, func(cfg *exampleserver.ServerConfig) {
		cfg.InitializationLeaseTTL = 200 * time.Millisecond
	})

	userID := "e2e-initializer-expired-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Seed", "seed@example.com")
	require.NoError(t, err)
	mustOpenE2E(t, client, ctx, "device-a")

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachOutcomeSeededLocal, connectResult.Outcome)

	time.Sleep(300 * time.Millisecond)

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "initialization_expired")
}

func TestEndToEnd_ExpiredInitializerReconnectCanSeedAgain(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_initializer_reconnect_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServerWithConfig(t, schema, func(cfg *exampleserver.ServerConfig) {
		cfg.InitializationLeaseTTL = 200 * time.Millisecond
	})

	userID := "e2e-initializer-reconnect-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowID := uuid.NewString()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Seed", "seed@example.com")
	require.NoError(t, err)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachOutcomeSeededLocal, connectResult.Outcome)

	time.Sleep(300 * time.Millisecond)

	_, err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "initialization_expired")

	pendingInitializationID := requirePendingInitializationID(t, db)
	require.Empty(t, pendingInitializationID)

	reconnectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachOutcomeStartedEmpty, reconnectResult.Outcome)

	mustPushPendingE2E(t, client, ctx)

	peer, peerDB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	peerConnect, err := peer.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachOutcomeUsedRemote, peerConnect.Outcome)

	var name string
	require.NoError(t, peerDB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Seed", name)
}

func TestEndToEnd_RebuildRequiredBlocksSyncUntilExplicitRebuild(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_rebuild_required_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-rebuild-required-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowOne := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowOne, "Remote One", "remote-one@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)
	mustRebuildE2E(t, clientB, ctx)

	rowTwo := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowTwo, "Remote Two", "remote-two@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	_, err = dbB.Exec(`UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1`)
	require.NoError(t, err)

	info := mustSourceInfoE2E(t, clientB, ctx)
	require.True(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)

	var requestCount atomic.Int64
	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestCount.Add(1)
		return nil, context.DeadlineExceeded
	})}

	for _, op := range []struct {
		name string
		run  func() error
	}{
		{name: "PushPending", run: func() error { _, err := clientB.PushPending(ctx); return err }},
		{name: "PullToStable", run: func() error { _, err := clientB.PullToStable(ctx); return err }},
		{name: "Sync", run: func() error { _, err := clientB.Sync(ctx); return err }},
	} {
		t.Run(op.name, func(t *testing.T) {
			err := op.run()
			var rebuildErr *oversqlite.RebuildRequiredError
			require.ErrorAs(t, err, &rebuildErr)
		})
	}
	require.Zero(t, requestCount.Load())

	clientB.HTTP = &http.Client{Transport: http.DefaultTransport}
	mustRebuildE2E(t, clientB, ctx)

	info = mustSourceInfoE2E(t, clientB, ctx)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users WHERE id IN (?, ?)`, rowOne, rowTwo).Scan(&count))
	require.Equal(t, 2, count)
}

func TestEndToEnd_SourceRecoveryPersistsAcrossRestartAndFailsClosedUntilRebuild(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_source_recovery_restart_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-source-recovery-restart-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	dbPath := filepath.Join(t.TempDir(), "source-recovery-restart.db")
	openPersistentClient := func() (*oversqlite.Client, *sql.DB) {
		db, err := sql.Open("sqlite3", dbPath)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		var tableCount int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'`).Scan(&tableCount))
		if tableCount == 0 {
			_, err = db.Exec(usersDDL)
			require.NoError(t, err)
		}

		tokenFn := func(ctx context.Context) (string, error) {
			return server.GenerateToken(userID, time.Hour)
		}
		client, err := oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
		require.NoError(t, err)
		return client, db
	}

	clientB, dbB := openPersistentClient()
	mustOpenE2E(t, clientB, ctx, "")
	connectResult, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	originalSourceID := requireCurrentSourceIDE2E(t, clientB, ctx)
	localPendingID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localPendingID, "Pending", "pending@example.com")
	require.NoError(t, err)

	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" {
			return errorJSONResponse(http.StatusConflict, map[string]any{
				"error":   "source_sequence_out_of_order",
				"message": "expected source_bundle_id 2",
			}), nil
		}
		return http.DefaultTransport.RoundTrip(r)
	})}

	_, err = clientB.PushPending(ctx)
	var recoveryErr *oversqlite.SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, oversqlite.SourceRecoverySequenceOutOfOrder, recoveryErr.Code)
	require.NoError(t, clientB.Close())
	require.NoError(t, dbB.Close())

	restartedClient, restartedDB := openPersistentClient()
	defer restartedClient.Close()
	defer restartedDB.Close()
	mustOpenE2E(t, restartedClient, ctx, "")

	info := mustSourceInfoE2E(t, restartedClient, ctx)
	require.True(t, info.RebuildRequired)
	require.True(t, info.SourceRecoveryRequired)
	require.Equal(t, oversqlite.SourceRecoverySequenceOutOfOrder, info.SourceRecoveryReason)

	restartedResult, err := restartedClient.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, restartedResult.Status)

	var requestCount atomic.Int64
	restartedClient.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestCount.Add(1)
		return nil, context.DeadlineExceeded
	})}

	for _, op := range []struct {
		name string
		run  func() error
	}{
		{name: "PushPending", run: func() error { _, err := restartedClient.PushPending(ctx); return err }},
		{name: "PullToStable", run: func() error { _, err := restartedClient.PullToStable(ctx); return err }},
		{name: "Sync", run: func() error { _, err := restartedClient.Sync(ctx); return err }},
	} {
		t.Run(op.name, func(t *testing.T) {
			err := op.run()
			var recoveryErr *oversqlite.SourceRecoveryRequiredError
			require.ErrorAs(t, err, &recoveryErr)
			require.Equal(t, oversqlite.SourceRecoverySequenceOutOfOrder, recoveryErr.Code)
		})
	}
	require.Zero(t, requestCount.Load())

	restartedClient.HTTP = &http.Client{Transport: http.DefaultTransport}
	mustRebuildE2E(t, restartedClient, ctx)

	info = mustSourceInfoE2E(t, restartedClient, ctx)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)
	require.NotEqual(t, originalSourceID, info.CurrentSourceID)

	var name string
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Remote", name)
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id = ?`, localPendingID).Scan(&name))
	require.Equal(t, "Pending", name)
}

func TestEndToEnd_SourceRecoveryRebuildRotatesManagedSourceAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_recover_restart_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-recover-restart-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	dbPath := filepath.Join(t.TempDir(), "recover-restart.db")
	openPersistentClient := func() (*oversqlite.Client, *sql.DB) {
		db, err := sql.Open("sqlite3", dbPath)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		var tableCount int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'`).Scan(&tableCount))
		if tableCount == 0 {
			_, err = db.Exec(usersDDL)
			require.NoError(t, err)
		}

		var client *oversqlite.Client
		tokenFn := func(ctx context.Context) (string, error) {
			return server.GenerateToken(userID, time.Hour)
		}
		client, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
		require.NoError(t, err)
		return client, db
	}

	clientB, dbB := openPersistentClient()
	mustOpenE2E(t, clientB, ctx, "device-b")
	connectResult, err := clientB.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	originalSourceID := requireCurrentSourceIDE2E(t, clientB, ctx)
	localPendingID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localPendingID, "Pending", "pending@example.com")
	require.NoError(t, err)

	baseTransport := http.DefaultTransport
	failCreate := true
	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if failCreate && r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions" {
			failCreate = false
			return errorJSONResponse(http.StatusConflict, map[string]any{
				"error":   "history_pruned",
				"message": "source tuple is below retained history floor",
			}), nil
		}
		return baseTransport.RoundTrip(r)
	})}

	_, err = clientB.PushPending(ctx)
	var recoveryErr *oversqlite.SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, oversqlite.SourceRecoveryHistoryPruned, recoveryErr.Code)

	info := mustSourceInfoE2E(t, clientB, ctx)
	require.True(t, info.RebuildRequired)
	require.True(t, info.SourceRecoveryRequired)
	require.Equal(t, oversqlite.SourceRecoveryHistoryPruned, info.SourceRecoveryReason)

	clientB.HTTP = &http.Client{Transport: baseTransport}
	mustRebuildE2E(t, clientB, ctx)
	info = mustSourceInfoE2E(t, clientB, ctx)
	rotatedSourceID := info.CurrentSourceID
	require.NotEqual(t, originalSourceID, rotatedSourceID)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)
	require.NoError(t, clientB.Close())
	require.NoError(t, dbB.Close())

	restartedClient, restartedDB := openPersistentClient()
	defer restartedClient.Close()
	defer restartedDB.Close()
	mustOpenE2E(t, restartedClient, ctx, "")
	require.Equal(t, rotatedSourceID, requireCurrentSourceIDE2E(t, restartedClient, ctx))

	restartedResult, err := restartedClient.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, restartedResult.Status)

	var name string
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Remote", name)
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id = ?`, localPendingID).Scan(&name))
	require.Equal(t, "Pending", name)
}

func TestEndToEnd_SourceRetirementIsVisibleOverHTTPAndReplacementBecomesActive(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_source_retired_http_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-source-retired-http-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	seedRowID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, seedRowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	mustPushPendingE2E(t, clientA, ctx)

	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	originalSourceID := requireCurrentSourceIDE2E(t, clientB, ctx)

	localPendingID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localPendingID, "Pending", "pending@example.com")
	require.NoError(t, err)

	baseTransport := http.DefaultTransport
	failCreate := true
	var replacementReq oversync.SnapshotSessionCreateRequest
	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch {
		case failCreate && r.Method == http.MethodPost && r.URL.Path == "/sync/push-sessions":
			failCreate = false
			return errorJSONResponse(http.StatusConflict, map[string]any{
				"error":   "history_pruned",
				"message": "source tuple is below retained history floor",
			}), nil
		case r.Method == http.MethodPost && r.URL.Path == "/sync/snapshot-sessions":
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			if len(body) > 0 {
				require.NoError(t, json.Unmarshal(body, &replacementReq))
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
			return baseTransport.RoundTrip(r)
		default:
			return baseTransport.RoundTrip(r)
		}
	})}

	_, err = clientB.PushPending(ctx)
	var recoveryErr *oversqlite.SourceRecoveryRequiredError
	require.ErrorAs(t, err, &recoveryErr)
	require.Equal(t, oversqlite.SourceRecoveryHistoryPruned, recoveryErr.Code)

	report, err := clientB.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.RemoteSyncOutcomeAppliedSnapshot, report.Outcome)

	info := mustSourceInfoE2E(t, clientB, ctx)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)
	require.NotEqual(t, originalSourceID, info.CurrentSourceID)

	require.NotNil(t, replacementReq.SourceReplacement)
	require.Equal(t, originalSourceID, replacementReq.SourceReplacement.PreviousSourceID)
	require.Equal(t, info.CurrentSourceID, replacementReq.SourceReplacement.NewSourceID)
	require.Equal(t, "history_pruned", replacementReq.SourceReplacement.Reason)

	userPK := lookupUserPKE2E(t, server, userID)
	var oldState, oldReplacedBy string
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT state, replaced_by_source_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, originalSourceID).Scan(&oldState, &oldReplacedBy))
	require.Equal(t, "retired", oldState)
	require.Equal(t, info.CurrentSourceID, oldReplacedBy)

	var retiredResp oversync.SourceRetiredResponse
	rawDoSyncJSON(t, server, http.MethodPost, "/sync/push-sessions", userID, originalSourceID, http.StatusConflict, oversync.PushSessionCreateRequest{
		SourceBundleID:  1,
		PlannedRowCount: 1,
	}, &retiredResp)
	require.Equal(t, "source_retired", retiredResp.Error)
	require.Equal(t, originalSourceID, retiredResp.SourceID)
	require.Equal(t, info.CurrentSourceID, retiredResp.ReplacedBySourceID)

	rotatedPendingID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rotatedPendingID, "Rotated", "rotated@example.com")
	require.NoError(t, err)
	pushReport, err := clientB.PushPending(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, pushReport.Outcome)

	var newState string
	var newMaxCommitted int64
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT state, max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, info.CurrentSourceID).Scan(&newState, &newMaxCommitted))
	require.Equal(t, "active", newState)
	require.Equal(t, int64(1), newMaxCommitted)
}
