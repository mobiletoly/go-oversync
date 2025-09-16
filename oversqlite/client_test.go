package oversqlite

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
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
	expectedTables := []string{"_sync_client_info", "_sync_row_meta", "_sync_pending"}
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

func TestEnsureSourceID(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Initialize database (for this test only)
	err = initializeDatabase(db)
	require.NoError(t, err)

	userID := "test-user"

	// Test first call generates a source ID
	sourceID1, err := EnsureSourceID(db, userID)
	require.NoError(t, err)
	require.NotEmpty(t, sourceID1)

	// Test second call returns the same source ID
	sourceID2, err := EnsureSourceID(db, userID)
	require.NoError(t, err)
	require.Equal(t, sourceID1, sourceID2)

	// Test different user gets different source ID
	differentUserID := "different-user"
	sourceID3, err := EnsureSourceID(db, differentUserID)
	require.NoError(t, err)
	require.NotEqual(t, sourceID1, sourceID3)

	// Verify client info was created correctly
	var storedSourceID string
	var nextChangeID, lastServerSeq int64
	err = db.QueryRow(`
		SELECT source_id, next_change_id, last_server_seq_seen 
		FROM _sync_client_info WHERE user_id = ?
	`, userID).Scan(&storedSourceID, &nextChangeID, &lastServerSeq)
	require.NoError(t, err)
	require.Equal(t, sourceID1, storedSourceID)
	require.Equal(t, int64(1), nextChangeID)
	require.Equal(t, int64(0), lastServerSeq)
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
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	require.NoError(t, err)

	// Create client with registered table (this will create triggers automatically)
	config := DefaultConfig("business", []SyncTable{
		{TableName: "test_table"},
	})
	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	_, err = NewClient(db, "http://localhost", "test-user", "test-source", tokenFunc, config)
	require.NoError(t, err)

	// Verify triggers were created
	expectedTriggers := []string{
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
	userID := "test-user"
	_, err = EnsureSourceID(db, userID)
	require.NoError(t, err)

	// Insert a record
	_, err = db.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", "test-1", "Test Record", 42)
	require.NoError(t, err)

	// Verify pending change was created
	var pendingCount int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ?", "test_table").Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 1, pendingCount)

	// Verify row metadata was created
	var metaCount int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_row_meta WHERE table_name = ?", "test_table").Scan(&metaCount)
	require.NoError(t, err)
	require.Equal(t, 1, metaCount)

	// Test update operation
	_, err = db.Exec("UPDATE test_table SET name = ? WHERE id = ?", "Updated Record", "test-1")
	require.NoError(t, err)

	// Should still have 1 pending change (coalesced)
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ?", "test_table").Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 1, pendingCount)

	// Verify operation is INSERT (preserved from original INSERT, not downgraded to UPDATE)
	var op string
	err = db.QueryRow("SELECT op FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?", "test_table", "test-1").Scan(&op)
	require.NoError(t, err)
	require.Equal(t, "INSERT", op, "INSERT→UPDATE should preserve INSERT operation")

	// Test delete operation
	_, err = db.Exec("DELETE FROM test_table WHERE id = ?", "test-1")
	require.NoError(t, err)

	// Should now have 0 pending changes (INSERT→DELETE becomes no-op)
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ?", "test_table").Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 0, pendingCount, "INSERT→DELETE should result in no pending changes")

	// Verify no pending change exists for this record
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?", "test_table", "test-1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "No pending changes should exist after INSERT→DELETE")

	// Verify that metadata was cleaned up for never-synced INSERT→DELETE
	var deletedMetaCount int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_row_meta WHERE table_name = ? AND pk_uuid = ?", "test_table", "test-1").Scan(&deletedMetaCount)
	require.NoError(t, err)
	require.Equal(t, 0, deletedMetaCount, "Metadata should be cleaned up for never-synced INSERT→DELETE")
}

func TestDefaultResolver(t *testing.T) {
	resolver := &DefaultResolver{}

	serverData := []byte(`{"id": "test", "name": "Server Name", "version": 2}`)
	localData := []byte(`{"id": "test", "name": "Local Name", "version": 1}`)

	// Test that server wins by default
	merged, keepLocal, err := resolver.Merge("test_table", "test-id", serverData, localData)
	require.NoError(t, err)
	require.False(t, keepLocal)
	require.Equal(t, string(serverData), string(merged))
}

func TestNewClient(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Database initialization is now handled by NewClient

	// Create token function
	tokenFunc := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	// Create client
	config := DefaultConfig("business", []SyncTable{})
	client, err := NewClient(db, "http://localhost:8080", "test-user", "test-device", tokenFunc, config)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, db, client.DB)
	require.Equal(t, "http://localhost:8080", client.BaseURL)
	require.Equal(t, "test-user", client.UserID)
	require.Equal(t, "test-device", client.SourceID)
	require.NotNil(t, client.Token)
	require.NotNil(t, client.Resolver)
	require.NotNil(t, client.HTTP)

	// Test token function
	token, err := client.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "test-token", token)
}
