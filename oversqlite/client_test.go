package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestInitializeDatabase(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Test database initialization (now private function)
	err = initializeDatabase(db)
	require.NoError(t, err)

	// Verify sync metadata tables were created
	expectedTables := []string{
		"_sync_source_state",
		"_sync_attachment_state",
		"_sync_operation_state",
		"_sync_action_required",
		"_sync_apply_state",
		"_sync_outbox_bundle",
		"_sync_outbox_rows",
		"_sync_row_state",
		"_sync_dirty_rows",
	}
	for _, table := range expectedTables {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Table %s should exist", table)
	}

	// Test that WAL mode is enabled (in-memory databases use "memory" mode)
	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	require.NoError(t, err)
	// In-memory databases use "memory" mode instead of "wal"
	require.Contains(t, []string{"wal", "memory"}, journalMode)

	// Test that foreign keys are enabled
	var foreignKeys int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys)
	require.NoError(t, err)
	require.Equal(t, 1, foreignKeys)
}

func TestInitializeDatabase_ResetsStuckApplyModeAfterCrash(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE _sync_apply_state (
			singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
			apply_mode INTEGER NOT NULL DEFAULT 0
		)
	`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_apply_state(singleton_key, apply_mode)
		VALUES (1, 1)
	`)
	require.NoError(t, err)

	err = initializeDatabase(db)
	require.NoError(t, err)

	var applyMode int
	err = db.QueryRow(`SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1`).Scan(&applyMode)
	require.NoError(t, err)
	require.Equal(t, 0, applyMode)
}

func TestInitializeDatabase_PreservesUnknownSyncPrefixedTables(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE _sync_audit_log (
			id INTEGER PRIMARY KEY,
			event TEXT NOT NULL
		)
	`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO _sync_audit_log (id, event) VALUES (1, 'keep-me')`)
	require.NoError(t, err)

	require.NoError(t, initializeDatabase(db))

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sync_audit_log'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_audit_log`).Scan(&count))
	require.Equal(t, 1, count)

	var event string
	require.NoError(t, db.QueryRow(`SELECT event FROM _sync_audit_log WHERE id = 1`).Scan(&event))
	require.Equal(t, "keep-me", event)
}

func TestOpen_PersistsManagedSourceState(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	client, err := newTestClient(t, db, "http://localhost", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumns: []string{"id"}}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	client.sourceIDGenerator = func() string { return "device-a" }

	mustOpen(t, client, context.Background())

	var storedSourceID string
	var nextBundleID int64
	err = db.QueryRow(`
		SELECT source_id, next_source_bundle_id
		FROM _sync_source_state WHERE source_id = ?
	`, "device-a").Scan(&storedSourceID, &nextBundleID)
	require.NoError(t, err)
	require.Equal(t, "device-a", storedSourceID)
	require.Equal(t, int64(1), nextBundleID)
}

func TestConnect_PersistsConfiguredSchemaIdentity(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	client, err := newTestClient(t, db, "http://localhost", tokenFunc, admittedFixtureConfig("business", []SyncTable{
		{TableName: "users", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	attachTestClient(t, client, "test-user", "test-source")

	var schemaName string
	err = db.QueryRow(`SELECT schema_name FROM _sync_attachment_state WHERE singleton_key = 1`).Scan(&schemaName)
	require.NoError(t, err)
	require.Equal(t, "business", schemaName)
}

func requireBorrowedClientConfigError(
	t *testing.T,
	schemaName string,
	tables []SyncTable,
	ddl []string,
	expectedSubstrings ...string,
) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	for _, stmt := range ddl {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}

	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	_, err = newAdmittedFixtureClient(db, "http://localhost", tokenFunc, admittedFixtureConfig(schemaName, tables, testSchemaGenerationProof()))
	require.Error(t, err)
	for _, expected := range expectedSubstrings {
		require.Contains(t, err.Error(), expected)
	}
}

func TestBorrowedClient_RejectsUnsupportedIntegerSyncKeyColumn(t *testing.T) {
	requireBorrowedClientConfigError(t, "main", []SyncTable{
		{TableName: "users", SyncKeyColumns: []string{"id"}},
	}, []string{`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`}, "supports only TEXT keys and UUID-backed BLOB keys")
}

func TestBorrowedClient_RejectsNullableVisibleSyncKey(t *testing.T) {
	for _, declaredType := range []string{"TEXT", "BLOB"} {
		t.Run(declaredType, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE users (id " + declaredType + " PRIMARY KEY, name TEXT NOT NULL)")
			require.NoError(t, err)
			_, err = newAdmittedFixtureClient(db, "http://localhost", func(context.Context) (string, error) { return "token", nil }, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
			require.Error(t, err)
			require.Contains(t, err.Error(), "users")
			require.Contains(t, err.Error(), "id")
			require.Contains(t, err.Error(), "NOT NULL")
			require.Contains(t, err.Error(), "repair or recreate the local application database before sync initialization")
		})
	}
}

func TestBorrowedClient_NullableVisibleSyncKeyLeavesNoFreshRuntimeResidue(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	_, err = newAdmittedFixtureClient(db, "http://localhost", func(context.Context) (string, error) { return "token", nil }, config)
	require.Error(t, err)
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '_sync_%'`).Scan(&count))
	require.Zero(t, count)
	_, err = newAdmittedFixtureClient(db, "http://localhost", func(context.Context) (string, error) { return "token", nil }, config)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "already has an active oversqlite client")
}

func TestBorrowedClient_NullableVisibleSyncKeyRetryAfterMigration(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	token := func(context.Context) (string, error) { return "token", nil }
	_, err = newAdmittedFixtureClient(db, "http://localhost", token, config)
	require.Error(t, err)

	_, err = db.Exec(`
		CREATE TABLE users_h3 (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL);
		INSERT INTO users_h3 SELECT id, name FROM users;
		DROP TABLE users;
		ALTER TABLE users_h3 RENAME TO users;
	`)
	require.NoError(t, err)
	client, err := newTestClient(t, db, "http://localhost", token, config)
	require.NoError(t, err)
	config = client.config
	require.NoError(t, client.Close())
}

func TestBorrowedClient_NullableVisibleSyncKeyPreservesExistingRuntimeState(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "nullable-drift.sqlite")
	db, err := sql.Open("sqlite3", databasePath)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)`)
	require.NoError(t, err)
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	token := func(context.Context) (string, error) { return "token", nil }
	client, err := newTestClient(t, db, "http://localhost", token, config)
	require.NoError(t, err)
	require.NoError(t, client.Close())
	_, err = db.Exec(`INSERT INTO users(id, name) VALUES ('keep', 'Ada')`)
	require.NoError(t, err)
	before := localSchemaAndStateDigest(t, db)
	_, err = db.Exec(`PRAGMA writable_schema = ON`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE sqlite_schema SET sql = replace(sql, 'PRIMARY KEY NOT NULL', 'PRIMARY KEY') WHERE type = 'table' AND name = 'users'`)
	require.NoError(t, err)
	_, err = db.Exec(`PRAGMA writable_schema = OFF`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = sql.Open("sqlite3", databasePath)
	require.NoError(t, err)
	defer db.Close()
	_, err = newAdmittedFixtureClient(db, "http://localhost", token, config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NOT NULL")
	require.Equal(t, before, localSchemaAndStateDigest(t, db))
}

func localSchemaAndStateDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name`)
	require.NoError(t, err)
	defer rows.Close()
	var digest strings.Builder
	for rows.Next() {
		var objectType, name, tableName, sqlText string
		require.NoError(t, rows.Scan(&objectType, &name, &tableName, &sqlText))
		if objectType == "table" && name == "users" {
			continue
		}
		fmt.Fprintf(&digest, "%s|%s|%s|%s\n", objectType, name, tableName, sqlText)
	}
	require.NoError(t, rows.Err())
	var users, managed, dirty, sources int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&users))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_managed_tables`).Scan(&managed))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_attachment_state WHERE current_source_id <> ''`).Scan(&sources))
	fmt.Fprintf(&digest, "rows=%d managed=%d dirty=%d sources=%d", users, managed, dirty, sources)
	return digest.String()
}

func TestOpen_PreservesLocalRowsAndManagedSourceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY NOT NULL,
			body TEXT NOT NULL
		)
	`)

	seedSyncedUserRow(t, client, db, "stale", "Original", 7)
	_, err := db.Exec(`UPDATE users SET name='Stale',email='stale@example.com' WHERE id='stale'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_source_state SET next_source_bundle_id=5 WHERE source_id=?`, client.sourceID)
	require.NoError(t, err)
	frozen, err := client.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), frozen.SourceBundleID)
	require.Len(t, frozen.Rows, 1)
	// Preserve both the valid frozen request and a later captured edit on reopen.
	_, err = db.Exec(`UPDATE users SET email='newer@example.com' WHERE id='stale'`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_push_stage (
			bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		) VALUES (9, 0, 'main', 'users', ?, 'UPDATE', 8, ?)
	`, rowKeyJSON("stale"), `{"id":"stale","name":"Stage","email":"stage@example.com"}`)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES ('snapshot-stale', 1, 'main', 'users', ?, 8, ?)
	`, rowKeyJSON("stale"), `{"id":"stale","name":"Snapshot","email":"snapshot@example.com"}`)
	require.NoError(t, err)
	_, err = db.Exec(`
		UPDATE _sync_source_state
		SET next_source_bundle_id = 5
		WHERE source_id = ?
	`, client.sourceID)
	require.NoError(t, err)
	_, err = db.Exec(`
		UPDATE _sync_attachment_state
		SET last_bundle_seq_seen = 4, rebuild_required = 0
		WHERE singleton_key = 1
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
	seedLegacyDocBaseline(t, client, db)
	before := vectorState(t, client)
	require.NoError(t, client.Close())

	rebound, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rebound.Close()) })
	rebound.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, SchemaGeneration: oversync.SchemaGenerationCapabilities{ID: "1", Fingerprint: testSchemaGenerationProof().Fingerprint}, Limits: testWireLimits()}), nil
	})}
	mustOpen(t, rebound, ctx)
	result, err := rebound.Attach(ctx, client.UserID)
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	info, err := rebound.SourceInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, client.sourceID, rebound.sourceID)
	require.Equal(t, client.sourceID, info.CurrentSourceID)
	require.False(t, info.RebuildRequired)
	require.False(t, info.SourceRecoveryRequired)

	require.Equal(t, before, vectorState(t, rebound))
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state`).Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&count))
	require.Equal(t, 1, count)
	var outboxState string
	require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1`).Scan(&outboxState))
	require.Equal(t, outboxStatePrepared, outboxState)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_stage`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestDetach_ClearsHistoricallyManagedTablesRemovedFromCurrentConfig(t *testing.T) {
	ctx := context.Background()
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`, `
		CREATE TABLE legacy_docs (
			id TEXT PRIMARY KEY NOT NULL,
			body TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`INSERT INTO legacy_docs (id, body) VALUES (?, ?)`, "legacy-1", "stale body")
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO _sync_managed_tables (schema_name, table_name)
		VALUES ('main', 'legacy_docs')
		ON CONFLICT(schema_name, table_name) DO NOTHING
	`)
	require.NoError(t, err)
	seedLegacyDocBaseline(t, client, db)

	mustDetach(t, client, ctx)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM legacy_docs`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_row_state WHERE table_name = 'legacy_docs'`).Scan(&count))
	require.Equal(t, 0, count)
}

func TestCreateTriggersForTable(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Initialize database (for this test only)
	err = initializeDatabase(db)
	require.NoError(t, err)

	// Create a test table
	_, err = db.Exec(`
		CREATE TABLE test_table (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	require.NoError(t, err)

	// Create client with registered table (this will create triggers automatically)
	config := admittedFixtureConfig("business", []SyncTable{
		{TableName: "test_table", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof())

	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	client, err := newTestClient(t, db, "http://localhost", tokenFunc, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	// Verify triggers were created
	expectedTriggers := []string{
		"trg_test_table_bi_guard",
		"trg_test_table_bu_guard",
		"trg_test_table_bd_guard",
		"trg_test_table_ai",
		"trg_test_table_au",
		"trg_test_table_ad",
	}
	for _, trigger := range expectedTriggers {
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?", trigger).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Trigger %s should exist", trigger)
	}

	// Test that triggers work by inserting data
	mustOpen(t, client, context.Background())

	// Insert a record
	_, err = db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", "test-1", "Test Record", 42)
	require.NoError(t, err)

	// Verify dirty row was created
	var pendingCount int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = ?", "test_table").Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 1, pendingCount)

	// Local triggers only populate the dirty-row queue. Authoritative row state is recorded after bundle apply.

	// Test update operation
	_, err = db.Exec("UPDATE test_table SET name = ? WHERE id = ?", "Updated Record", "test-1")
	require.NoError(t, err)

	// Should still have 1 pending change (coalesced)
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = ?", "test_table").Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 1, pendingCount)

	// Verify operation is INSERT (preserved from original INSERT, not downgraded to UPDATE)
	var op string
	err = db.QueryRow("SELECT op FROM _sync_dirty_rows WHERE table_name = ? AND key_json = ?", "test_table", rowKeyJSON("test-1")).Scan(&op)
	require.NoError(t, err)
	require.Equal(t, "INSERT", op, "INSERT→UPDATE should preserve INSERT operation")

	// Test delete operation
	_, err = db.Exec("DELETE FROM test_table WHERE id = ?", "test-1")
	require.NoError(t, err)

	// INSERT→DELETE collapses to a no-op during push collection, not at trigger time.
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = ?", "test_table").Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 1, pendingCount, "INSERT→DELETE should remain queued until push collection removes the no-op")

	// The queued no-op is represented as a delete against the unsynced key.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = ? AND key_json = ?", "test_table", rowKeyJSON("test-1")).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "The queued delete should still be present until push collection clears it")

	err = db.QueryRow("SELECT op FROM _sync_dirty_rows WHERE table_name = ? AND key_json = ?", "test_table", rowKeyJSON("test-1")).Scan(&op)
	require.NoError(t, err)
	require.Equal(t, "DELETE", op, "The queued no-op should be represented as a delete intent")
}

func TestDefaultResolver_PreservesConflict(t *testing.T) {
	resolver := &DefaultResolver{}

	result := resolver.Resolve(ConflictContext{})
	require.IsType(t, DeferConflict{}, result)
}

func TestBorrowedClient(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)`)
	require.NoError(t, err)

	// Database initialization is handled by the borrowed fixture constructor

	// Create token function
	tokenFunc := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	// Create client
	config := admittedFixtureConfig("business", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	client, err := newTestClient(t, db, "http://localhost:8080", tokenFunc, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NotNil(t, client)
	require.Equal(t, db, client.db)
	require.Equal(t, "http://localhost:8080", client.BaseURL)
	require.Empty(t, client.UserID)
	require.Empty(t, client.sourceID)
	require.NotNil(t, client.Token)
	require.NotNil(t, client.Resolver)
	require.NotNil(t, client.HTTP)

	// Test token function
	token, err := client.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "test-token", token)
}

func TestBorrowedClient_AttachRequiresOpenToEstablishSourceID(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	client, err := newTestClient(t, db, "http://localhost", func(context.Context) (string, error) {
		return "test-token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumns: []string{"id"}}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err = client.Attach(context.Background(), "test-user")
	var openErr *OpenRequiredError
	require.ErrorAs(t, err, &openErr)
	require.True(t, IsLifecyclePreconditionError(err))
	require.Empty(t, client.sourceID)
	require.Empty(t, client.UserID)
}

func TestOpenAndAttachEstablishRuntimeIdentity(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	client, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "test-token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	client.sourceIDGenerator = func() string { return "device-a" }

	mustOpen(t, client, context.Background())
	require.Equal(t, "device-a", client.sourceID)
	require.Empty(t, client.UserID)

	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{
				ProtocolVersion:  requiredProtocolVersion,
				SchemaGeneration: oversync.SchemaGenerationCapabilities{ID: "1", Fingerprint: testSchemaGenerationProof().Fingerprint, Tables: []oversync.SchemaGenerationTableCapabilities{}},
				Limits:           testWireLimits(),
			}), nil
		case "/sync/connect":
			return jsonResponse(map[string]any{
				"resolution": "initialize_empty",
			}), nil
		default:
			return errorJSONResponse(404, map[string]string{"error": "not_found"}), nil
		}
	})}

	result, err := client.Attach(context.Background(), "test-user")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	require.Equal(t, "test-user", client.UserID)
	require.Equal(t, "device-a", client.sourceID)
}

func TestOperationsRequireAttachAfterOpen(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	client, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) {
		return "test-token", nil
	}, admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	mustOpen(t, client, context.Background())

	var requests int
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		return errorJSONResponse(http.StatusInternalServerError, map[string]string{"error": "unexpected_network"}), nil
	})}

	operations := []struct {
		name string
		run  func() error
	}{
		{name: "PushPending", run: func() error { _, err := client.PushPending(context.Background()); return err }},
		{name: "PullToStable", run: func() error { _, err := client.PullToStable(context.Background()); return err }},
		{name: "Sync", run: func() error { _, err := client.Sync(context.Background()); return err }},
		{name: "Rebuild", run: func() error { _, err := client.Rebuild(context.Background()); return err }},
	}

	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var connectErr *AttachRequiredError
			require.ErrorAs(t, err, &connectErr)
			require.True(t, IsLifecyclePreconditionError(err))
		})
	}

	_, err = client.LastBundleSeqSeen(context.Background())
	var connectErr *AttachRequiredError
	require.ErrorAs(t, err, &connectErr)
	require.True(t, IsLifecyclePreconditionError(err))

	require.Equal(t, 0, requests)
}

func TestLifecyclePreconditionErrorsResetSyncLoopBackoff(t *testing.T) {
	min := 10 * time.Millisecond
	max := 80 * time.Millisecond
	require.Equal(t, min, nextSyncLoopBackoffAfterError(&OpenRequiredError{Operation: "Attach(userID)"}, 40*time.Millisecond, min, max))
	require.Equal(t, min, nextSyncLoopBackoffAfterError(&AttachRequiredError{Operation: "PushPending()"}, 40*time.Millisecond, min, max))
	require.Equal(t, max, nextSyncLoopBackoffAfterError(context.DeadlineExceeded, 40*time.Millisecond, min, max))
}

func TestStartLoops_LogLifecycleMisuseWithoutTouchingNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	mustDetach(t, client, context.Background())
	mustOpen(t, client, context.Background())

	var requestCount atomic.Int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestCount.Add(1)
		return nil, context.DeadlineExceeded
	})}
	client.config.BackoffMin = time.Millisecond
	client.config.BackoffMax = 2 * time.Millisecond

	var logs lockedWatchLogBuffer
	client.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

	require.NoError(t, client.Start(ctx))
	time.Sleep(12 * time.Millisecond)
	cancel()
	time.Sleep(5 * time.Millisecond)

	require.Zero(t, requestCount.Load())
	logOutput := logs.String()
	require.Contains(t, logOutput, "upload loop blocked by lifecycle state")
	require.Contains(t, logOutput, "download loop blocked by lifecycle state")
	require.NotContains(t, strings.ToLower(logOutput), "deadline")
}

func TestBorrowedClient_FailsWhenManagedTablesAreNotFKClosed(t *testing.T) {
	requireBorrowedClientConfigError(t, "main", []SyncTable{
		{TableName: "posts", SyncKeyColumns: []string{"id"}},
	}, []string{`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`, `
		CREATE TABLE posts (
			id TEXT PRIMARY KEY NOT NULL,
			author_id TEXT NOT NULL REFERENCES users(id),
			title TEXT NOT NULL
		)
	`}, "not FK-closed", "posts.author_id -> users.id")
}

func TestBorrowedClient_RejectsSecondActiveClientForSameSQLiteDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.sqlite")

	db1, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })

	_, err = db1.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	client1, err := newOwnedTestClient(t, db1, "http://localhost", tokenFunc, admittedFixtureConfig("main", []SyncTable{
		{TableName: "users", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client1.Close()) })

	db2, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	_, err = newOwnedTestClient(t, db2, "http://localhost", tokenFunc, admittedFixtureConfig("main", []SyncTable{
		{TableName: "users", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof()))
	var ownedErr *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &ownedErr)
	require.Contains(t, ownedErr.Database, filepath.Base(dbPath))
}

func TestBorrowedClient_AllowsSecondClientAfterClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.sqlite")

	db1, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })

	_, err = db1.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	client1, err := newOwnedTestClient(t, db1, "http://localhost", tokenFunc, admittedFixtureConfig("main", []SyncTable{
		{TableName: "users", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof()))
	require.NoError(t, err)
	require.NoError(t, client1.Close())

	db2, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	client2, err := newOwnedTestClient(t, db2, "http://localhost", tokenFunc, admittedFixtureConfig("main", []SyncTable{
		{TableName: "users", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client2.Close()) })
}

func TestBorrowedClient_AllowsSelfReferencingManagedTable(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE categories (
			id TEXT PRIMARY KEY NOT NULL,
			parent_id TEXT REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED,
			name TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	client, err := newTestClient(t, db, "http://localhost", tokenFunc, admittedFixtureConfig("main", []SyncTable{
		{TableName: "categories", SyncKeyColumns: []string{"id"}},
	}, testSchemaGenerationProof()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NotNil(t, client)
}

func TestBorrowedClient_FailsWhenTableNameIsSchemaQualified(t *testing.T) {
	requireBorrowedClientConfigError(t, "business", []SyncTable{
		{TableName: "main.users", SyncKeyColumns: []string{"id"}},
	}, []string{`
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL
		)
	`}, "must not include a schema qualifier")
}

func TestBorrowedClient_FailsWhenCompositeSyncKeyColumnsConfigured(t *testing.T) {
	requireBorrowedClientConfigError(t, "main", []SyncTable{
		{TableName: "pairs", SyncKeyColumns: []string{"left_id", "right_id"}},
	}, []string{`
		CREATE TABLE pairs (
			left_id TEXT NOT NULL,
			right_id TEXT NOT NULL,
			PRIMARY KEY (left_id, right_id)
		)
	`}, "exactly one sync key column")
}

func TestBorrowedClient_FailsWhenManagedTablesContainCompositeFK(t *testing.T) {
	requireBorrowedClientConfigError(t, "main", []SyncTable{
		{TableName: "parents", SyncKeyColumns: []string{"id"}},
		{TableName: "children", SyncKeyColumns: []string{"id"}},
	}, []string{`
		CREATE TABLE parents (
			id TEXT PRIMARY KEY NOT NULL,
			code_a TEXT NOT NULL,
			code_b TEXT NOT NULL,
			UNIQUE (code_a, code_b)
		)
	`, `
		CREATE TABLE children (
			id TEXT PRIMARY KEY NOT NULL,
			parent_code_a TEXT NOT NULL,
			parent_code_b TEXT NOT NULL,
			FOREIGN KEY (parent_code_a, parent_code_b) REFERENCES parents(code_a, code_b)
		)
	`}, "unsupported composite foreign keys")
}

// Historical cleanup registrations can outlive the active wire generation.
// Seed a full typed observation from the retained physical row and bind its
// provenance to the fixture identity; do not install inactive capture triggers.
func seedLegacyDocBaseline(t *testing.T, _ *Client, db *sql.DB) {
	t.Helper()
	info, err := GetTableInfo(db, "legacy_docs")
	require.NoError(t, err)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.Exec(`INSERT INTO _sync_row_state
		(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known)
		SELECT 'main','legacy_docs',?,9,'live',`+buildJsonObjectExprHexAware(info, "legacy_docs")+`,`+
		baselineProvenanceSQL("'pull'", "9", "0")+`,1 FROM legacy_docs WHERE id='legacy-1'`, rowKeyJSON("legacy-1"))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}
