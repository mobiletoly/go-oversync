package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	exampleserver "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const usersDDL = `
	CREATE TABLE users (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)
`

const postsDDL = `
	CREATE TABLE posts (
		id TEXT PRIMARY KEY NOT NULL,
		title TEXT NOT NULL,
		content TEXT NOT NULL,
		author_id TEXT NULL,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (author_id) REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
	)
`

const categoriesDDL = `
	CREATE TABLE categories (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
	)
`

const typedRowsDDL = `
	CREATE TABLE typed_rows (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		note TEXT NULL,
	count_value INTEGER NULL,
	small_count INTEGER NULL,
	medium_count INTEGER NULL,
	exact_amount TEXT NULL,
	enabled_flag INTEGER NOT NULL,
	rating REAL NULL,
	float4_value REAL NULL,
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

func newExampleServer(t *testing.T, schema string, tables ...string) *exampleserver.TestServer {
	t.Helper()
	if len(tables) == 0 {
		tables = []string{"users"}
	}
	return newExampleServerWithConfig(t, schema, func(config *exampleserver.ServerConfig) {
		config.SchemaGenerations = exampleGenerationForTables(schema, tables)
	})
}

func exampleGenerationForTables(schema string, tables []string) []oversync.SchemaGeneration {
	selected := oversync.SchemaGeneration{ID: 1}
	for _, table := range exampleserver.SchemaGenerationsForBusinessSchema(schema)[0].Tables {
		for _, name := range tables {
			if name == table.Table {
				selected.Tables = append(selected.Tables, table)
			}
		}
	}
	return []oversync.SchemaGeneration{selected}
}

func defaultOversqliteConfig(schema string, tables []oversqlite.SyncTable) *oversqlite.Config {
	config := oversqlite.DefaultConfig(1, tables)
	config.DatabaseID, config.ServiceID = "e2e-client", schema
	return config
}

func newRetryingConfig(schema string, tables []oversqlite.SyncTable) *oversqlite.Config {
	config := defaultOversqliteConfig(schema, tables)
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
	config := defaultOversqliteConfig(schema, tables)
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

	databaseURL := createIsolatedExampleDatabase(t, integrationTestDatabaseURL())

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

	if cfg.SchemaGenerations == nil {
		cfg.SchemaGenerations = exampleGenerationForTables(schema, []string{"users"})
	}
	ts, err := exampleserver.NewTestServer(cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		ts.Close()
	})

	return ts
}

func createIsolatedExampleDatabase(t *testing.T, databaseURL string) string {
	t.Helper()

	adminURL, resolvedDatabaseURL, _ := buildSharedDatabaseURLs(t, databaseURL)
	dbName := "oversync_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	parsed, err := url.Parse(resolvedDatabaseURL)
	require.NoError(t, err)
	parsed.Path = "/" + dbName
	isolatedDatabaseURL := parsed.String()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminConn, err := pgx.Connect(ctx, adminURL)
	require.NoError(t, err)
	defer adminConn.Close(ctx)
	_, err = adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupConn, err := pgx.Connect(cleanupCtx, adminURL)
		if err != nil {
			t.Errorf("connect to drop isolated E2E database: %v", err)
			return
		}
		defer cleanupConn.Close(cleanupCtx)
		if _, err := cleanupConn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop isolated E2E database: %v", err)
		}
	})
	return isolatedDatabaseURL
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

	tokenFn := func(ctx context.Context) (string, error) {
		return ts.GenerateToken(userID, time.Hour)
	}
	client := openFreshSQLiteClient(t, filepath.Join(t.TempDir(), "client.sqlite"), ts.URL(), tokenFn, config, ddl...)
	_ = sourceID
	connectResult, err := client.Attach(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client, client.Database()
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

	tokenFn := func(ctx context.Context) (string, error) {
		return ts.GenerateToken(userID, time.Hour)
	}
	client := openFreshSQLiteClient(t, filepath.Join(t.TempDir(), "client.sqlite"), ts.URL(), tokenFn, config, ddl...)
	_ = sourceID

	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client, client.Database()
}

type freshDDLMigrator struct {
	ddl []string
}

func (m freshDDLMigrator) InstalledVersion(ctx context.Context, reader oversqlite.LocalMigrationReader) (int64, error) {
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m freshDDLMigrator) Migrate(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
	for _, statement := range m.ddl {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=1`)
	return err
}

func openFreshSQLiteClient(
	t *testing.T,
	path string,
	baseURL string,
	token func(context.Context) (string, error),
	config *oversqlite.Config,
	ddl ...string,
) *oversqlite.Client {
	t.Helper()
	config = readyTestConfig(config)
	plan := &oversqlite.UpgradePlan{
		PlanID: "test-fresh-v1", Mode: oversqlite.UpgradeModeFresh,
		DatabaseID: config.DatabaseID, ServiceID: config.ServiceID,
		TargetGenerationID: config.GenerationID, TargetVersion: 1,
		LocalOnlyTables: append([]string(nil), config.LocalOnlyTables...),
		Migrator:        freshDDLMigrator{ddl: append([]string(nil), ddl...)},
	}
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
		DatabasePath: path, Plan: plan, BaseURL: baseURL, Token: token,
		TargetClientConfig: config,
	})
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	return result.Ready
}

func openReadySQLiteClient(
	t *testing.T,
	path string,
	baseURL string,
	token func(context.Context) (string, error),
	config *oversqlite.Config,
) *oversqlite.Client {
	t.Helper()
	config = readyTestConfig(config)
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
		DatabasePath: path, BaseURL: baseURL, Token: token,
		TargetClientConfig: config,
	})
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	return result.Ready
}

func readyTestConfig(config *oversqlite.Config) *oversqlite.Config {
	copyValue := *config
	copyValue.Tables = append([]oversqlite.SyncTable(nil), config.Tables...)
	for index := range copyValue.Tables {
		copyValue.Tables[index].SyncKeyColumns = append([]string(nil), config.Tables[index].SyncKeyColumns...)
	}
	copyValue.LocalOnlyTables = append([]string(nil), config.LocalOnlyTables...)
	if copyValue.LocalVersion == 0 {
		copyValue.LocalVersion = 1
	}
	return &copyValue
}

func openPersistentSQLiteClient(
	t *testing.T,
	path string,
	baseURL string,
	token func(context.Context) (string, error),
	config *oversqlite.Config,
	ddl ...string,
) *oversqlite.Client {
	t.Helper()
	info, err := os.Stat(path)
	if err == nil && info.Size() > 0 {
		return openReadySQLiteClient(t, path, baseURL, token, config)
	}
	require.ErrorIs(t, err, os.ErrNotExist)
	return openFreshSQLiteClient(t, path, baseURL, token, config, ddl...)
}

func sqliteDatabasePath(t *testing.T, db *sql.DB) string {
	t.Helper()
	var sequence int
	var name, path string
	require.NoError(t, db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path))
	require.Equal(t, "main", name)
	require.NotEmpty(t, path)
	return path
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

func lookupUserPKE2E(t *testing.T, server *exampleserver.TestServer, userID string) int64 {
	t.Helper()
	var userPK int64
	require.NoError(t, server.Pool.QueryRow(context.Background(), `
		SELECT user_pk
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&userPK))
	return userPK
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
		table := oversqlite.SyncTable{TableName: name, SyncKeyColumnName: "id"}
		tables = append(tables, table)
	}
	return tables
}
