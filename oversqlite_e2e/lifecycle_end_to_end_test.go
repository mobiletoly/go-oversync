package oversqlite_e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
		return server.GenerateToken(userID, client.SourceID, time.Hour)
	}
	client, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err = client.Connect(ctx, userID)
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
		{name: "PushPending", run: func() error { return client.PushPending(ctx) }},
		{name: "PullToStable", run: func() error { return client.PullToStable(ctx) }},
		{name: "Sync", run: func() error { return client.Sync(ctx) }},
		{name: "Hydrate", run: func() error { return client.Hydrate(ctx) }},
		{name: "Recover", run: func() error { return client.Recover(ctx) }},
	}
	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var connectErr *oversqlite.ConnectRequiredError
			require.ErrorAs(t, err, &connectErr)
			require.True(t, oversqlite.IsLifecyclePreconditionError(err))
		})
	}

	_, err := client.LastBundleSeqSeen(ctx)
	var connectErr *oversqlite.ConnectRequiredError
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

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)
	require.Equal(t, oversqlite.ConnectOutcomeStartedEmpty, connectResult.Outcome)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Alpha", "alpha@example.com")
	require.NoError(t, err)
	require.NoError(t, client.PushPending(ctx))

	var count int
	require.NoError(t, server.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.scope_state
		WHERE user_id = $1
		  AND state = 'INITIALIZED'
	`, userID).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEndToEnd_ConnectRemoteAuthoritativeHydratesExistingRemote(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_connect_remote_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-connect-remote-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectA, err := clientA.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectA.Status)

	rowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	connectB, err := clientB.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, connectB.Outcome)

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

	connectA, err := clientA.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectA.Status)

	remoteRowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, remoteRowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	localOnlyID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localOnlyID, "Local", "local@example.com")
	require.NoError(t, err)
	require.NoError(t, clientB.Open(ctx, "device-b"))

	connectB, err := clientB.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, connectB.Outcome)

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

	connectA, err := clientA.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectA.Status)
	require.Equal(t, oversqlite.ConnectOutcomeSeededLocal, connectA.Outcome)

	require.NoError(t, clientA.PushPending(ctx))

	connectB, err := clientB.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, connectB.Outcome)

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

	connectA, err := clientA.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectA.Status)

	remoteRowID := uuid.NewString()
	_, err = dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, remoteRowID, "Transient", "transient@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))
	_, err = dbA.Exec(`DELETE FROM users WHERE id = ?`, remoteRowID)
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	localOnlyID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, localOnlyID, "Local", "local@example.com")
	require.NoError(t, err)
	require.NoError(t, clientB.Open(ctx, "device-b"))

	connectB, err := clientB.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectB.Status)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, connectB.Outcome)

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

	connectResult, err := dbClient.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)
	require.NoError(t, dbClient.Close())

	var restarted *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(userID, restarted.SourceID, time.Hour)
	}
	restarted, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	restarted.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })

	require.NoError(t, restarted.Open(ctx, "device-a"))

	resumeResult, err := restarted.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, resumeResult.Status)
	require.Equal(t, oversqlite.ConnectOutcomeResumedAttached, resumeResult.Outcome)
}

func TestEndToEnd_SignOutBlocksWithAttachedDirtyRows(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_signout_blocked_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-signout-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Pending", "pending@example.com")
	require.NoError(t, err)

	err = client.SignOut(ctx)
	var blockedErr *oversqlite.SignOutBlockedError
	require.ErrorAs(t, err, &blockedErr)
	require.Greater(t, blockedErr.PendingRowCount, 0)
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
	require.NoError(t, client.Open(ctx, "device-a"))

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}

	_, err = client.Connect(ctx, userID)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))

	status, err := client.PendingSyncStatus(ctx)
	require.NoError(t, err)
	require.True(t, status.HasPendingSyncData)
	require.Equal(t, 1, status.PendingRowCount)
	require.False(t, status.BlocksSignOut)

	client.HTTP = &http.Client{Timeout: 120 * time.Second}
	result, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, result.Status)
	require.Equal(t, oversqlite.ConnectOutcomeSeededLocal, result.Outcome)
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

	first, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusRetryLater, first.Status)
	require.GreaterOrEqual(t, first.RetryAfter, time.Second)

	time.Sleep(1300 * time.Millisecond)

	second, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, second.Status)
	require.Equal(t, oversqlite.ConnectOutcomeStartedEmpty, second.Outcome)
}

func TestEndToEnd_SyncThenSignOutSuccessClearsAttachedLocalState(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_sync_then_signout_success_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-sync-then-signout-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Synced", "synced@example.com")
	require.NoError(t, err)

	require.NoError(t, client.SyncThenSignOut(ctx))
	require.Empty(t, client.UserID)

	var remoteCount int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), rowID).Scan(&remoteCount))
	require.Equal(t, 1, remoteCount)

	var localCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 0, localCount)
}

func TestEndToEnd_SyncThenSignOutFailureLeavesAttachedState(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_sync_then_signout_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-sync-then-signout-failure-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)

	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), "Pending", "pending@example.com")
	require.NoError(t, err)

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}

	err = client.SyncThenSignOut(ctx)
	require.Error(t, err)
	require.Equal(t, userID, client.UserID)
}

func TestEndToEnd_DifferentUserCanConnectAfterSuccessfulSignOut(t *testing.T) {
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
		return server.GenerateToken(currentUserID, client.SourceID, time.Hour)
	}
	client, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	require.NoError(t, client.Open(ctx, "device-a"))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })

	first, err := client.Connect(ctx, currentUserID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, first.Status)
	require.NoError(t, client.SignOut(ctx))

	currentUserID = "user-b-" + uuid.NewString()
	second, err := client.Connect(ctx, currentUserID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, second.Status)
	require.Equal(t, currentUserID, client.UserID)
}

func TestEndToEnd_OpenSourceMismatchThenResetForNewSourceRecovers(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_open_reset_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-open-reset-user-" + uuid.NewString()
	client, _ := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	require.NoError(t, client.Open(ctx, "device-a"))

	err := client.Open(ctx, "device-b")
	var mismatchErr *oversqlite.SourceMismatchError
	require.ErrorAs(t, err, &mismatchErr)

	require.NoError(t, client.ResetForNewSource(ctx, "device-b"))
	require.NoError(t, client.Open(ctx, "device-b"))
	require.Equal(t, "device-b", client.SourceID)
}

func TestEndToEnd_UninstallSyncThenReinstallOpenAndConnectOnSameDatabase(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_uninstall_reinstall_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-uninstall-reinstall-user-" + uuid.NewString()
	client, db := newSQLiteClientWithoutConnect(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)

	rowID := uuid.NewString()
	_, err = db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	require.NoError(t, client.PushPending(ctx))

	require.NoError(t, client.UninstallSync(ctx))

	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, rowID)
	require.NoError(t, err)

	var reinstalled *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(userID, reinstalled.SourceID, time.Hour)
	}
	reinstalled, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reinstalled.Close()) })

	require.NoError(t, reinstalled.Open(ctx, "device-a"))

	reconnectResult, err := reinstalled.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, reconnectResult.Status)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, reconnectResult.Outcome)

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
	require.NoError(t, clientA.Open(ctx, "device-a"))
	require.NoError(t, clientB.Open(ctx, "device-b"))

	start := make(chan struct{})
	results := make(chan oversqlite.ConnectResult, 2)
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
			result, err := client.Connect(ctx, userID)
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
		case result.Status == oversqlite.ConnectStatusConnected && result.Outcome == oversqlite.ConnectOutcomeSeededLocal:
			seededCount++
		case result.Status == oversqlite.ConnectStatusRetryLater:
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
	require.NoError(t, client.Open(ctx, "device-a"))

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectOutcomeSeededLocal, connectResult.Outcome)

	time.Sleep(300 * time.Millisecond)

	err = client.PushPending(ctx)
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

	connectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectOutcomeSeededLocal, connectResult.Outcome)

	time.Sleep(300 * time.Millisecond)

	err = client.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "initialization_expired")

	var pendingInitializationID string
	require.NoError(t, db.QueryRow(`SELECT pending_initialization_id FROM _sync_lifecycle_state WHERE singleton_key = 1`).Scan(&pendingInitializationID))
	require.Empty(t, pendingInitializationID)

	reconnectResult, err := client.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectOutcomeStartedEmpty, reconnectResult.Outcome)

	require.NoError(t, client.PushPending(ctx))

	peer, peerDB := newSQLiteClientWithoutConnect(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	peerConnect, err := peer.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectOutcomeUsedRemote, peerConnect.Outcome)

	var name string
	require.NoError(t, peerDB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Seed", name)
}

func TestEndToEnd_RecoverRotationSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_recover_restart_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-recover-restart-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowID, "Remote", "remote@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

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
			return server.GenerateToken(userID, client.SourceID, time.Hour)
		}
		client, err = oversqlite.NewClient(db, server.URL(), tokenFn, oversqlite.DefaultConfig(schema, syncTables("users")))
		require.NoError(t, err)
		return client, db
	}

	clientB, dbB := openPersistentClient()
	require.NoError(t, clientB.Open(ctx, "device-b"))
	connectResult, err := clientB.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)

	originalSourceID := clientB.SourceID
	require.NoError(t, clientB.Recover(ctx))
	rotatedSourceID := clientB.SourceID
	require.NotEqual(t, originalSourceID, rotatedSourceID)
	require.NoError(t, clientB.Close())
	require.NoError(t, dbB.Close())

	mismatchClient, mismatchDB := openPersistentClient()
	err = mismatchClient.Open(ctx, originalSourceID)
	var mismatchErr *oversqlite.SourceMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	require.NoError(t, mismatchClient.Close())
	require.NoError(t, mismatchDB.Close())

	restartedClient, restartedDB := openPersistentClient()
	defer restartedClient.Close()
	defer restartedDB.Close()
	require.NoError(t, restartedClient.Open(ctx, rotatedSourceID))

	restartedResult, err := restartedClient.Connect(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, restartedResult.Status)

	var name string
	require.NoError(t, restartedDB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID).Scan(&name))
	require.Equal(t, "Remote", name)
}
