package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	return "postgres://postgres:password@localhost:5432/clisync_test?sslmode=disable"
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

	cfg := &exampleserver.ServerConfig{
		DatabaseURL:    integrationTestDatabaseURL(),
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
		require.NoError(t, dropSchema(context.Background(), ts.Pool, schema))
		ts.Close()
	})

	return ts
}

func dropSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if pool == nil {
		return nil
	}
	_, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, pgx.Identifier{schema}.Sanitize()))
	return err
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
	require.NoError(t, client.Open(context.Background(), sourceID))
	connectResult, err := client.Connect(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.ConnectStatusConnected, connectResult.Status)

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
	require.NoError(t, client.Open(context.Background(), sourceID))

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
