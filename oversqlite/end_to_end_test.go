package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	exampleserver "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type bundleEndToEndServer struct {
	pool      *pgxpool.Pool
	service   *oversync.SyncService
	jwt       *oversync.JWTAuth
	http      *httptest.Server
	schema    string
	logger    *slog.Logger
	baseURL   string
	appName   string
	jwtSecret string
}

func integrationTestDatabaseURL() string {
	if dbURL := os.Getenv("TEST_DATABASE_URL"); dbURL != "" {
		return dbURL
	}
	return "postgres://postgres:password@localhost:5432/clisync_test?sslmode=disable"
}

func newBundleEndToEndServer(t *testing.T, ctx context.Context, schema string, tables []oversync.RegisteredTable) *bundleEndToEndServer {
	t.Helper()

	pool, err := pgxpool.New(ctx, integrationTestDatabaseURL())
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
	require.NoError(t, resetBundleEndToEndSchema(ctx, pool, schema))

	service, err := oversync.NewRuntimeService(pool, &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "oversqlite-end-to-end",
		RegisteredTables:          tables,
	}, logger)
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(ctx))

	jwtAuth := oversync.NewJWTAuth("oversqlite-end-to-end-secret")
	handlers := oversync.NewHTTPSyncHandlers(service, logger)
	mux := http.NewServeMux()
	mux.Handle("POST /sync/push-sessions", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleCreatePushSession)))
	mux.Handle("POST /sync/push-sessions/{push_id}/chunks", jwtAuth.Middleware(http.HandlerFunc(handlers.HandlePushSessionChunk)))
	mux.Handle("POST /sync/push-sessions/{push_id}/commit", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleCommitPushSession)))
	mux.Handle("DELETE /sync/push-sessions/{push_id}", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleDeletePushSession)))
	mux.Handle("GET /sync/committed-bundles/{bundle_seq}/rows", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleGetCommittedBundleRows)))
	mux.Handle("GET /sync/pull", jwtAuth.Middleware(http.HandlerFunc(handlers.HandlePull)))
	mux.Handle("POST /sync/snapshot-sessions", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleCreateSnapshotSession)))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleGetSnapshotChunk)))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", jwtAuth.Middleware(http.HandlerFunc(handlers.HandleDeleteSnapshotSession)))

	httpServer := httptest.NewServer(mux)

	server := &bundleEndToEndServer{
		pool:      pool,
		service:   service,
		jwt:       jwtAuth,
		http:      httpServer,
		schema:    schema,
		logger:    logger,
		baseURL:   httpServer.URL,
		appName:   "oversqlite-end-to-end",
		jwtSecret: "oversqlite-end-to-end-secret",
	}

	t.Cleanup(func() {
		httpServer.Close()
		_ = service.Close(context.Background())
		_ = dropBundleEndToEndSchema(context.Background(), pool, schema)
		pool.Close()
	})

	return server
}

func resetBundleEndToEndSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	schemaIdent := pgx.Identifier{schema}.Sanitize()

	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaIdent)); err != nil {
		return fmt.Errorf("drop schema %s: %w", schema, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent)); err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.users: %w", schema, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.posts (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id UUID NOT NULL,
			CONSTRAINT posts_author_id_fkey
				FOREIGN KEY (author_id)
				REFERENCES %s.users(id)
				ON DELETE CASCADE
				DEFERRABLE INITIALLY DEFERRED
		)
	`, schemaIdent, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.posts: %w", schema, err)
	}
	return nil
}

func dropBundleEndToEndSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaIdent))
	return err
}

func newBundleHTTPClient(t *testing.T, server *bundleEndToEndServer, userID, sourceID string, tables []SyncTable, ddl ...string) (*Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	var client *Client
	tokenFn := func(ctx context.Context) (string, error) {
		return server.jwt.GenerateToken(userID, client.SourceID, time.Hour)
	}

	client, err = NewClient(db, server.baseURL, userID, sourceID, tokenFn, DefaultConfig(server.schema, tables))
	require.NoError(t, err)
	require.NoError(t, client.Bootstrap(context.Background(), false))

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })
	return client, db
}

func TestEndToEnd_PushHydratePullAndServerCascade(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_push_pull_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schema, Table: "posts", SyncKeyColumns: []string{"id"}},
	})

	tables := []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`
	postsDDL := `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", tables, usersDDL, postsDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", tables, usersDDL, postsDDL)

	rowUserID := uuid.NewString()
	rowPostID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, rowUserID, "Ada", "ada@example.com")
	require.NoError(t, err)
	_, err = dbA.Exec(`INSERT INTO posts (id, title, content, author_id) VALUES (?, ?, ?, ?)`, rowPostID, "Hello", "Post body", rowUserID)
	require.NoError(t, err)

	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	var userCount int
	var postCount int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&userCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM posts`).Scan(&postCount))
	require.Equal(t, 1, userCount)
	require.Equal(t, 1, postCount)

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.service.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 100,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.users WHERE id = $1`, pgx.Identifier{server.schema}.Sanitize()), rowUserID)
		return err
	}))

	require.NoError(t, clientB.PullToStable(ctx))

	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&userCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM posts`).Scan(&postCount))
	require.Equal(t, 0, userCount)
	require.Equal(t, 0, postCount)
}

func TestEndToEnd_ExampleServerSchemaTimestampParity(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_example_schema_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	ts, err := exampleserver.NewTestServer(&exampleserver.ServerConfig{
		DatabaseURL:    integrationTestDatabaseURL(),
		JWTSecret:      "oversqlite-example-schema-secret",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})),
		AppName:        "oversqlite-example-schema-e2e",
		BusinessSchema: schema,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ts.Close()
		_ = dropBundleEndToEndSchema(context.Background(), ts.Pool, schema)
	})

	tables := []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`
	postsDDL := `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`

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

	var client *Client
	tokenFn := func(ctx context.Context) (string, error) {
		return ts.JWTAuth.GenerateToken(userID, client.SourceID, time.Hour)
	}
	client, err = NewClient(db, ts.URL(), userID, "device-a", tokenFn, DefaultConfig(schema, tables))
	require.NoError(t, err)
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
	require.NoError(t, ts.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT created_at::text, updated_at::text FROM %s.users WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), userRowID).Scan(&gotCreatedAt, &gotUpdatedAt))
	require.NotEmpty(t, gotCreatedAt)
	require.NotEmpty(t, gotUpdatedAt)

	var postCount int
	require.NoError(t, ts.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.posts WHERE id = $1`, pgx.Identifier{schema}.Sanitize()), postRowID).Scan(&postCount))
	require.Equal(t, 1, postCount)
}

func TestEndToEnd_PushSessionCreateTransportRetryLeavesClientRecoverable(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_push_retry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)
	clientB, _ := newBundleHTTPClient(t, server, userID, "device-b", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)

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
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)
	clientA.config.UploadLimit = 2
	clientB.config.SnapshotChunkRows = 2

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

	_, err = server.pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = $2 WHERE user_id = $1`, userID, stableBundleSeq)
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
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)
	clientB.config.UploadLimit = 2

	conflictUserID := uuid.NewString()
	_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, conflictUserID, "Grace", "grace@example.com")
	require.NoError(t, err)
	require.NoError(t, clientA.PushPending(ctx))
	require.NoError(t, clientB.Hydrate(ctx))

	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.service.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 201,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{server.schema}.Sanitize()), conflictUserID, "Grace Server")
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
	err = clientB.PushPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "409")

	pushStats := clientB.PushTransferDiagnostics()
	require.Equal(t, int64(1), pushStats.SessionsCreated)
	require.Greater(t, pushStats.ChunksUploaded, int64(1))

	var dirtyCount, outboundCount, stagedCount int
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&outboundCount))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount))
	require.Equal(t, 0, dirtyCount)
	require.Equal(t, 3, outboundCount)
	require.Equal(t, 0, stagedCount)

	var nextSourceBundleID int64
	require.NoError(t, dbB.QueryRow(`SELECT next_source_bundle_id FROM _sync_client_state WHERE user_id = ?`, userID).Scan(&nextSourceBundleID))
	require.Equal(t, int64(1), nextSourceBundleID)

	var serverName string
	require.NoError(t, server.pool.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE id = $1`, pgx.Identifier{server.schema}.Sanitize()), conflictUserID).Scan(&serverName))
	require.Equal(t, "Grace Server", serverName)

	var extraCount int
	require.NoError(t, server.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id IN ($1, $2)`, pgx.Identifier{server.schema}.Sanitize()), extraUserOne, extraUserTwo).Scan(&extraCount))
	require.Equal(t, 0, extraCount)
}

func TestEndToEnd_PullRetryPruneFallbackAndRecover(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_pull_retry_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	userID := "e2e-user-" + uuid.NewString()
	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersDDL)

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

	_, err = server.pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = 3 WHERE user_id = $1`, userID)
	require.NoError(t, err)

	require.NoError(t, clientB.PullToStable(ctx))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 3, localCount)

	lastBundleSeq, err := clientB.LastBundleSeqSeen(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastBundleSeq, int64(3))

	oldSourceID := clientB.SourceID
	tx := mustBeginTx(t, dbB)
	require.NoError(t, clientB.setBundleApplyModeInTx(ctx, tx, true))
	_, err = tx.ExecContext(ctx, `DELETE FROM users`)
	require.NoError(t, err)
	require.NoError(t, clientB.setBundleApplyModeInTx(ctx, tx, false))
	require.NoError(t, tx.Commit())

	require.NoError(t, clientB.Recover(ctx))
	require.NotEqual(t, oldSourceID, clientB.SourceID)

	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&localCount))
	require.Equal(t, 3, localCount)
}

func TestEndToEnd_LargeFKConnectedHydrateUsesChunkedSnapshot(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_chunked_hydrate_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	userID := "e2e-chunked-user-" + uuid.NewString()
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schema, Table: "posts", SyncKeyColumns: []string{"id"}},
	})

	tables := []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`
	postsDDL := `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
		)
	`

	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", tables, usersDDL, postsDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", tables, usersDDL, postsDDL)
	clientB.config.SnapshotChunkRows = 10

	for i := 0; i < 3; i++ {
		userID := uuid.NewString()
		_, err := dbA.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, userID, fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
		for j := 0; j < 10; j++ {
			_, err = dbA.Exec(`INSERT INTO posts (id, title, content, author_id) VALUES (?, ?, ?, ?)`,
				uuid.NewString(),
				fmt.Sprintf("Post %d-%d", i, j),
				fmt.Sprintf("Content %d-%d", i, j),
				userID,
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
	userID := "e2e-retry-user-" + uuid.NewString()
	server := newBundleEndToEndServer(t, ctx, schema, []oversync.RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
	})

	tables := []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}
	usersDDL := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`

	clientA, dbA := newBundleHTTPClient(t, server, userID, "device-a", tables, usersDDL)
	clientB, dbB := newBundleHTTPClient(t, server, userID, "device-b", tables, usersDDL)
	clientB.config.SnapshotChunkRows = 1

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
