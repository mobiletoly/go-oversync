package oversqlite_e2e

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_HydratePullAndServerCascadeOnCategories(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_server_cascade_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-cascade-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("categories")), categoriesDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("categories")), categoriesDDL)

	rootID := uuid.NewString()
	childID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO categories (id, name, parent_id) VALUES (?, ?, ?)`, rootID, "Root", nil)
	require.NoError(t, err)
	_, err = dbA.Exec(`INSERT INTO categories (id, name, parent_id) VALUES (?, ?, ?)`, childID, "Child", rootID)
	require.NoError(t, err)

	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 100,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.categories WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), rootID)
		return err
	}))

	require.NoError(t, clientB.PullToStable(ctx))

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM categories`).Scan(&count))
	require.Equal(t, 0, count)
}

func TestEndToEnd_PushThenOwnPushThenPullStillFetchesEarlierPeerBundle(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_push_gap_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-gap-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowAID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowAID, "From A", "from-a@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	rowBID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowBID, "From B", "from-b@example.com")
	require.NoError(t, err)
	require.NoError(t, clientB.PushPending(ctx))
	require.NoError(t, clientB.PullToStable(ctx))

	var nameA, nameB string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowAID).Scan(&nameA))
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowBID).Scan(&nameB))
	require.Equal(t, "From A", nameA)
	require.Equal(t, "From B", nameB)

	var lastBundleSeq int64
	require.NoError(t, dbB.QueryRow(`
		SELECT last_bundle_seq_seen
		FROM _sync_client_state
		WHERE user_id = ?
	`, clientB.UserID).Scan(&lastBundleSeq))
	require.Equal(t, int64(2), lastBundleSeq)
}

func TestEndToEnd_ThreeDevicesPushWithoutPullThenAllPullConverge(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_three_device_gap_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-three-gap-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientC, dbC := newSQLiteClient(t, server, userID, "device-c", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowAID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowAID, "From A", "from-a@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	rowBID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowBID, "From B", "from-b@example.com")
	require.NoError(t, err)
	require.NoError(t, clientB.PushPending(ctx))

	rowCID := uuid.NewString()
	_, err = dbC.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowCID, "From C", "from-c@example.com")
	require.NoError(t, err)
	require.NoError(t, clientC.PushPending(ctx))

	require.NoError(t, clientA.PullToStable(ctx))
	require.NoError(t, clientB.PullToStable(ctx))
	require.NoError(t, clientC.PullToStable(ctx))

	for _, db := range []*sql.DB{dbA, dbB, dbC} {
		var userCount int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&userCount))
		require.Equal(t, 3, userCount)
	}

	var name string
	require.NoError(t, dbA.QueryRow(`SELECT name FROM users WHERE id = ?`, rowCID).Scan(&name))
	require.Equal(t, "From C", name)
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowAID).Scan(&name))
	require.Equal(t, "From A", name)
	require.NoError(t, dbC.QueryRow(`SELECT name FROM users WHERE id = ?`, rowBID).Scan(&name))
	require.Equal(t, "From B", name)

	for _, client := range []*oversqlite.Client{clientA, clientB, clientC} {
		var lastBundleSeq int64
		require.NoError(t, client.DB.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = ?`, client.UserID).Scan(&lastBundleSeq))
		require.Equal(t, int64(3), lastBundleSeq)
	}
}

func TestEndToEnd_RestartAfterOwnPushBeforePullStillFetchesEarlierPeerBundle(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_restart_gap_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-restart-gap-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowAID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowAID, "From A", "from-a@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))

	rowBID := uuid.NewString()
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowBID, "From B", "from-b@example.com")
	require.NoError(t, err)
	require.NoError(t, clientB.PushPending(ctx))
	require.NoError(t, clientB.Close())

	restartedConfig := oversqlite.DefaultConfig(schema, syncTables("users"))
	restarted, err := oversqlite.NewClient(dbB, server.URL(), userID, clientB.SourceID, func(context.Context) (string, error) {
		return server.GenerateToken(userID, clientB.SourceID, time.Hour)
	}, restartedConfig)
	require.NoError(t, err)
	defer func() { require.NoError(t, restarted.Close()) }()
	require.NoError(t, restarted.Bootstrap(ctx, false))
	require.NoError(t, restarted.PullToStable(ctx))

	var nameA, nameB string
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowAID).Scan(&nameA))
	require.NoError(t, dbB.QueryRow(`SELECT name FROM users WHERE id = ?`, rowBID).Scan(&nameB))
	require.Equal(t, "From A", nameA)
	require.Equal(t, "From B", nameB)

	var lastBundleSeq int64
	require.NoError(t, dbB.QueryRow(`
		SELECT last_bundle_seq_seen
		FROM _sync_client_state
		WHERE user_id = ?
	`, restarted.UserID).Scan(&lastBundleSeq))
	require.Equal(t, int64(2), lastBundleSeq)
}

func TestEndToEnd_ExampleServerSchemaTimestampParity(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_example_schema_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-example-user-" + uuid.NewString()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	for _, stmt := range []string{usersDDL, postsDDL} {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}

	var client *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.GenerateToken(userID, client.SourceID, time.Hour)
	}
	client, err = oversqlite.NewClient(db, server.URL(), userID, "device-a", tokenFn, oversqlite.DefaultConfig(schema, syncTables("users", "posts")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, client.Bootstrap(ctx, false))

	userRowID := uuid.NewString()
	postRowID := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = db.Exec(`INSERT INTO users (id, name, email, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`, userRowID, "Ada", "ada@example.com", now, now)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO posts (id, title, content, author_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, postRowID, "Hello", "Post body", userRowID, now, now)
	require.NoError(t, err)

	require.NoError(t, client.PushPending(ctx))

	var gotCreatedAt, gotUpdatedAt string
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT created_at::text, updated_at::text FROM %s.users WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), userRowID).Scan(&gotCreatedAt, &gotUpdatedAt))
	require.NotEmpty(t, gotCreatedAt)
	require.NotEmpty(t, gotUpdatedAt)

	var postCount int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.posts WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), postRowID).Scan(&postCount))
	require.Equal(t, 1, postCount)
}

func TestEndToEnd_PushSessionCreateTransportRetryLeavesClientRecoverable(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_push_retry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, _ := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	rowUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowUserID, "Ada", "ada@example.com")
	require.NoError(t, err)

	baseTransport := http.DefaultTransport
	failFirstPush := true
	clientA.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/push-sessions" && failFirstPush {
			failFirstPush = false
			return nil, io.ErrUnexpectedEOF
		}
		return baseTransport.RoundTrip(r)
	})}

	err = clientA.PushPending(ctx)
	require.Error(t, err)

	var dirtyCount int
	require.NoError(t, dbA.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.Equal(t, 0, dirtyCount)

	clientA.HTTP = &http.Client{Transport: baseTransport}
	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))
}

func TestEndToEnd_StaleFollowerConvergesAfterChunkedPushAndPruneFallback(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_stale_follower_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-" + uuid.NewString()
	configA := oversqlite.DefaultConfig(schema, syncTables("users"))
	configA.UploadLimit = 2
	configB := oversqlite.DefaultConfig(schema, syncTables("users"))
	configB.SnapshotChunkRows = 2
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", configA, usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", configB, usersDDL)

	for i := 0; i < 5; i++ {
		_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			uuid.NewString(),
			fmt.Sprintf("User %d", i),
			fmt.Sprintf("user%d@example.com", i),
		)
		require.NoError(t, err)
	}

	clientA.ResetPushTransferDiagnostics()
	require.NoError(t, clientA.PushPending(ctx))
	pushStats := clientA.PushTransferDiagnostics()
	require.Greater(t, pushStats.ChunksUploaded, int64(1))

	stableBundleSeq, err := clientA.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Greater(t, stableBundleSeq, int64(0))

	staleFollowerSeq, err := clientB.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), staleFollowerSeq)

	_, err = server.Pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = $2 WHERE user_id = $1`, userID, stableBundleSeq)
	require.NoError(t, err)

	clientB.ResetSnapshotTransferDiagnostics()
	require.NoError(t, clientB.PullToStable(ctx))
	snapshotStats := clientB.SnapshotTransferDiagnostics()
	require.Greater(t, snapshotStats.ChunksFetched, int64(1))

	var localCount int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 5, localCount)

	lastSeen, err := clientB.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.Equal(t, stableBundleSeq, lastSeen)
}

func TestEndToEnd_ChunkedPushConflictPreservesWholeBundleSemantics(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_chunked_conflict_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-" + uuid.NewString()
	configB := oversqlite.DefaultConfig(schema, syncTables("users"))
	configB.UploadLimit = 2
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", configB, usersDDL)
	clientB.Resolver = &oversqlite.ClientWinsResolver{}

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Grace", "grace@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 201,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schema}.Sanitize()), conflictUserID, "Grace Server")
		return err
	}))

	extraUserOne := uuid.NewString()
	extraUserTwo := uuid.NewString()
	_, err = dbB.Exec(`UPDATE users SET name = ? WHERE id = ?`, "Grace Client", conflictUserID)
	require.NoError(t, err)
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, extraUserOne, "Extra One", "extra1@example.com")
	require.NoError(t, err)
	_, err = dbB.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, extraUserTwo, "Extra Two", "extra2@example.com")
	require.NoError(t, err)

	clientB.ResetPushTransferDiagnostics()
	require.NoError(t, clientB.PushPending(ctx))

	pushStats := clientB.PushTransferDiagnostics()
	require.Equal(t, int64(2), pushStats.SessionsCreated)
	require.Greater(t, pushStats.ChunksUploaded, int64(2))

	var dirtyCount, outboundCount, stagedCount int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&outboundCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 0, outboundCount)
	require.Equal(t, 0, stagedCount)

	var nextSourceBundleID int64
	require.NoError(t, dbB.QueryRow(`SELECT next_source_bundle_id FROM _sync_client_state WHERE user_id = ?`, userID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(2), nextSourceBundleID)

	var serverName string
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), conflictUserID).Scan(&serverName))
	require.Equal(t, "Grace Client", serverName)

	var extraCount int
	require.NoError(t, server.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id IN ($1, $2)`, pgx.Identifier{schema}.Sanitize()), extraUserOne, extraUserTwo).Scan(&extraCount))
	require.Equal(t, 2, extraCount)
}

func TestEndToEnd_PullRetryPruneFallbackAndRecover(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_pull_retry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)

	insertUser := func(id, name string) {
		_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, name, strings.ToLower(name)+"@example.com")
		require.NoError(t, err)
		require.NoError(t, clientA.PushPending(ctx))
	}

	userOne := uuid.NewString()
	insertUser(userOne, "Ada")
	require.NoError(t, clientB.Hydrate(ctx))

	userTwo := uuid.NewString()
	insertUser(userTwo, "Grace")

	baseTransport := http.DefaultTransport
	failFirstPull := true
	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/sync/pull" && failFirstPull {
			failFirstPull = false
			return nil, io.ErrUnexpectedEOF
		}
		return baseTransport.RoundTrip(r)
	})}

	err := clientB.PullToStable(ctx)
	require.Error(t, err)

	clientB.HTTP = &http.Client{Transport: baseTransport}
	require.NoError(t, clientB.PullToStable(ctx))

	var localCount int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 2, localCount)

	userThree := uuid.NewString()
	insertUser(userThree, "Linus")

	_, err = server.Pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = 3 WHERE user_id = $1`, userID)
	require.NoError(t, err)

	require.NoError(t, clientB.PullToStable(ctx))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 3, localCount)

	lastBundleSeq, err := clientB.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastBundleSeq, int64(3))

	oldSourceID := clientB.SourceID
	tx := mustBeginTx(t, dbB)
	_, err = tx.ExecContext(ctx, `UPDATE _sync_client_state SET apply_mode = 1 WHERE user_id = ?`, userID)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `DELETE FROM users`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE _sync_client_state SET apply_mode = 0 WHERE user_id = ?`, userID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	require.NoError(t, clientB.Recover(ctx))
	require.NotEqual(t, oldSourceID, clientB.SourceID)

	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 3, localCount)
}

func TestEndToEnd_LargeFKConnectedHydrateUsesChunkedSnapshot(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_chunked_hydrate_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-chunked-user-" + uuid.NewString()
	configB := oversqlite.DefaultConfig(schema, syncTables("users", "posts"))
	configB.SnapshotChunkRows = 10
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users", "posts")), usersDDL, postsDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", configB, usersDDL, postsDDL)

	for i := 0; i < 3; i++ {
		rowUserID := uuid.NewString()
		_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowUserID, fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
		for j := 0; j < 10; j++ {
			_, err = dbA.Exec(`INSERT INTO posts (id, title, content, author_id) VALUES (?, ?, ?, ?)`,
				uuid.NewString(),
				fmt.Sprintf("Post %d-%d", i, j),
				fmt.Sprintf("Content %d-%d", i, j),
				rowUserID,
			)
			require.NoError(t, err)
		}
	}
	require.NoError(t, clientA.PushPending(ctx))

	clientB.ResetSnapshotTransferDiagnostics()
	require.NoError(t, clientB.Hydrate(ctx))
	stats := clientB.SnapshotTransferDiagnostics()
	require.Greater(t, stats.SessionsCreated, int64(0))
	require.Greater(t, stats.ChunksFetched, int64(1))

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 3, count)
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM posts`).Scan(&count))
	require.Equal(t, 30, count)
}

func TestEndToEnd_HydrateRetryClearsStaleStageAndStartsNewSession(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_chunked_retry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-retry-user-" + uuid.NewString()
	configB := oversqlite.DefaultConfig(schema, syncTables("users"))
	configB.SnapshotChunkRows = 1
	clientA, dbA := newSQLiteClient(t, server, userID, "device-a", oversqlite.DefaultConfig(schema, syncTables("users")), usersDDL)
	clientB, dbB := newSQLiteClient(t, server, userID, "device-b", configB, usersDDL)

	for i := 0; i < 3; i++ {
		_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, uuid.NewString(), fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}
	require.NoError(t, clientA.PushPending(ctx))

	baseTransport := http.DefaultTransport
	chunkFetches := 0
	clientB.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sync/snapshot-sessions/") {
			chunkFetches++
			if chunkFetches > 1 {
				return nil, io.EOF
			}
		}
		return baseTransport.RoundTrip(r)
	})}

	err := clientB.Hydrate(ctx)
	require.Error(t, err)
	require.Greater(t, snapshotStageCount(t, dbB), 0)

	var count int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 0, count)

	clientB.HTTP = &http.Client{Timeout: 120 * time.Second}
	clientB.ResetSnapshotTransferDiagnostics()
	require.NoError(t, clientB.Hydrate(ctx))
	require.Equal(t, 0, snapshotStageCount(t, dbB))

	stats := clientB.SnapshotTransferDiagnostics()
	require.Greater(t, stats.SessionsCreated, int64(0))
	require.Greater(t, stats.ChunksFetched, int64(1))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 3, count)
}
