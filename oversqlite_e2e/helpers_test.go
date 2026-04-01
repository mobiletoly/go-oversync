package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	exampleserver "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const usersDDL = `
	CREATE TABLE users (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)
`

const postsDDL = `
	CREATE TABLE posts (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		content TEXT NOT NULL,
		author_id TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (author_id) REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
	)
`

const categoriesDDL = `
	CREATE TABLE categories (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
	)
`

const typedRowsDDL = `
	CREATE TABLE typed_rows (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		note TEXT NULL,
		count_value INTEGER NULL,
		enabled_flag INTEGER NOT NULL,
		rating REAL NULL,
		data BLOB NULL,
		created_at TEXT NULL
	)
`

func integrationTestDatabaseURL() string {
	if dbURL := os.Getenv("TEST_DATABASE_URL"); dbURL != "" {
		return dbURL
	}
	return "postgres://postgres:password@localhost:5432/clisync_oversqlite_e2e_test?sslmode=disable"
}

func newExampleServer(t *testing.T, schema string) *exampleserver.TestServer {
	t.Helper()
	return newExampleServerWithConfig(t, schema, func(*exampleserver.ServerConfig) {})
}

func newRetryingConfig(schema string, tables []oversqlite.SyncTable) *oversqlite.Config {
	config := oversqlite.DefaultConfig(schema, tables)
	config.RetryPolicy = &oversqlite.RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		JitterFraction: 0,
	}
	return config
}

func newNoRetryConfig(schema string, tables []oversqlite.SyncTable) *oversqlite.Config {
	config := oversqlite.DefaultConfig(schema, tables)
	config.RetryPolicy = &oversqlite.RetryPolicy{
		Enabled: false,
	}
	return config
}

func newExampleServerWithConfig(
	t *testing.T,
	schema string,
	configure func(*exampleserver.ServerConfig),
) *exampleserver.TestServer {
	t.Helper()

	databaseURL := integrationTestDatabaseURL()
	ensureSharedExampleDatabase(t, databaseURL)
	resetSharedExampleDatabase(t, databaseURL)

	cfg := &exampleserver.ServerConfig{
		DatabaseURL:    databaseURL,
		JWTSecret:      "oversqlite-end-to-end-secret",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})),
		AppName:        "oversqlite-end-to-end",
		BusinessSchema: schema,
	}
	if configure != nil {
		configure(cfg)
	}

	ts, err := exampleserver.NewTestServer(cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		ts.Close()
	})

	return ts
}

func ensureSharedExampleDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	adminURL, _, dbName := buildSharedDatabaseURLs(t, databaseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminConn, err := pgx.Connect(ctx, adminURL)
	require.NoError(t, err)
	defer adminConn.Close(ctx)

	var exists bool
	require.NoError(t, adminConn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists))
	if exists {
		return
	}

	_, err = adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize())
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P04" {
			require.NoError(t, err)
		}
	}
}

func resetSharedExampleDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public')
		  AND nspname NOT LIKE 'pg_toast%'
		  AND nspname NOT LIKE 'pg_temp_%'
		  AND nspname NOT LIKE 'pg_toast_temp_%'
		ORDER BY nspname
	`)
	require.NoError(t, err)
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		require.NoError(t, rows.Scan(&schema))
		schemas = append(schemas, schema)
	}
	require.NoError(t, rows.Err())

	for _, schema := range schemas {
		_, err := conn.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		require.NoError(t, err)
	}
}

func buildSharedDatabaseURLs(t *testing.T, databaseURL string) (adminURL string, resolvedDatabaseURL string, dbName string) {
	t.Helper()

	parsed, err := url.Parse(databaseURL)
	require.NoError(t, err)

	dbName = strings.TrimPrefix(parsed.Path, "/")
	require.NotEmpty(t, dbName)

	admin := *parsed
	admin.Path = "/postgres"
	adminURL = admin.String()

	return adminURL, parsed.String(), dbName
}

func newSQLiteClient(
	t *testing.T,
	ts *exampleserver.TestServer,
	userID string,
	sourceID string,
	config *oversqlite.Config,
	ddl ...string,
) (*oversqlite.Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	var client *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return ts.GenerateToken(userID, client.SourceID, time.Hour)
	}

	client, err = oversqlite.NewClient(db, ts.URL(), tokenFn, config)
	require.NoError(t, err)
	openResult, err := client.Open(context.Background(), sourceID)
	require.NoError(t, err)
	require.NotEmpty(t, openResult.State)
	connectResult, err := client.Attach(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })
	return client, db
}

func newSQLiteClientWithoutConnect(
	t *testing.T,
	ts *exampleserver.TestServer,
	userID string,
	sourceID string,
	config *oversqlite.Config,
	ddl ...string,
) (*oversqlite.Client, *sql.DB) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	var client *oversqlite.Client
	tokenFn := func(ctx context.Context) (string, error) {
		return ts.GenerateToken(userID, client.SourceID, time.Hour)
	}

	client, err = oversqlite.NewClient(db, ts.URL(), tokenFn, config)
	require.NoError(t, err)
	openResult, err := client.Open(context.Background(), sourceID)
	require.NoError(t, err)
	require.NotEmpty(t, openResult.State)

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	t.Cleanup(func() { _ = db.Close() })
	return client, db
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

type staticResolver struct {
	result oversqlite.MergeResult
}

func (r *staticResolver) Resolve(conflict oversqlite.ConflictContext) oversqlite.MergeResult {
	if r == nil || r.result == nil {
		return oversqlite.AcceptServer{}
	}
	return r.result
}

func rowKeyJSON(id string) string {
	raw, _ := json.Marshal(map[string]any{"id": id})
	return string(raw)
}

func mustJSONPayload(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return raw
}

func loadDirtyRowForKey(t *testing.T, db *sql.DB, keyJSON string) (string, int64, sql.NullString, bool) {
	t.Helper()

	var (
		op             string
		baseRowVersion int64
		payload        sql.NullString
	)
	err := db.QueryRow(`
		SELECT op, base_row_version, payload
		FROM _sync_dirty_rows
		WHERE table_name = 'users' AND key_json = ?
	`, keyJSON).Scan(&op, &baseRowVersion, &payload)
	if err == sql.ErrNoRows {
		return "", 0, sql.NullString{}, false
	}
	require.NoError(t, err)
	return op, baseRowVersion, payload, true
}

func requireServerScopeState(t *testing.T, server *exampleserver.TestServer, userID string) string {
	t.Helper()
	var state string
	require.NoError(t, server.Pool.QueryRow(context.Background(), `
		SELECT CASE ss.state_code
			WHEN 0 THEN 'UNINITIALIZED'
			WHEN 1 THEN 'INITIALIZING'
			WHEN 2 THEN 'INITIALIZED'
			ELSE 'UNKNOWN'
		END
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&state))
	return state
}

func requireServerScopeStateFields(t *testing.T, server *exampleserver.TestServer, userID string) (state string, initializationID sql.NullString, leaseExpiresAt sql.NullTime, initializedBySource sql.NullString) {
	t.Helper()
	require.NoError(t, server.Pool.QueryRow(context.Background(), `
		SELECT CASE ss.state_code
			WHEN 0 THEN 'UNINITIALIZED'
			WHEN 1 THEN 'INITIALIZING'
			WHEN 2 THEN 'INITIALIZED'
			ELSE 'UNKNOWN'
		END,
		ss.initialization_id::text,
		ss.lease_expires_at,
		ss.initialized_by_source_id
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&state, &initializationID, &leaseExpiresAt, &initializedBySource))
	return state, initializationID, leaseExpiresAt, initializedBySource
}

func requireServerScopeRowCount(t *testing.T, server *exampleserver.TestServer, userID string) int {
	t.Helper()
	var count int
	require.NoError(t, server.Pool.QueryRow(context.Background(), `
		SELECT COUNT(*)
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&count))
	return count
}

func snapshotStageCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&count))
	return count
}

func mustBeginTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	return tx
}

func jsonResponse(v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func errorJSONResponse(status int, v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func syncTables(names ...string) []oversqlite.SyncTable {
	tables := make([]oversqlite.SyncTable, 0, len(names))
	for _, name := range names {
		tables = append(tables, oversqlite.SyncTable{TableName: name, SyncKeyColumnName: "id"})
	}
	return tables
}

func registeredTables(schema string, names ...string) []oversync.RegisteredTable {
	tables := make([]oversync.RegisteredTable, 0, len(names))
	for _, name := range names {
		tables = append(tables, oversync.RegisteredTable{Schema: schema, Table: name, SyncKeyColumns: []string{"id"}})
	}
	return tables
}
