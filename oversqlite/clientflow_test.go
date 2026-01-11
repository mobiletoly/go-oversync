package oversqlite

import (
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestClientFlowExample tests the exact workflow described in docs/client_techreq/02_client_flow_example.md
// This test verifies that our oversqlite implementation correctly handles the documented client-server flow
func TestClientFlowExample(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Create users table matching the nethttp_server business entities
	_, err := harness.client1DB.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s','now')),
			updated_at INTEGER DEFAULT (strftime('%s','now'))
		)
	`)
	require.NoError(t, err)

	_, err = harness.client2DB.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s','now')),
			updated_at INTEGER DEFAULT (strftime('%s','now'))
		)
	`)
	require.NoError(t, err)

	// Triggers are now created automatically by NewClient for all registered tables

	// Step 0: Verify starting state
	t.Log("=== Step 0: Starting state ===")

	// Verify users table exists
	var tableCount int
	err = harness.client1DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableCount)
	require.NoError(t, err)
	require.Equal(t, 1, tableCount, "Users table should exist in client1")

	err = harness.client2DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableCount)
	require.NoError(t, err)
	require.Equal(t, 1, tableCount, "Users table should exist in client2")

	// Both clients should have empty tables and proper client_info
	var count int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.Equal(t, 0, count, "Phone users table should be empty")

	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.Equal(t, 0, count, "Laptop users table should be empty")

	// Verify client_info exists and has proper initial values
	var sourceID string
	var nextChangeID, lastServerSeq int64
	err = harness.client1DB.QueryRow(`
		SELECT source_id, next_change_id, last_server_seq_seen
		FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&sourceID, &nextChangeID, &lastServerSeq)
	require.NoError(t, err)
	require.NotEmpty(t, sourceID)
	require.Equal(t, int64(1), nextChangeID)
	require.Equal(t, int64(0), lastServerSeq)

	// Step 1: Phone creates a user (INSERT) - following the client flow pattern
	t.Log("=== Step 1: Phone creates a user (INSERT) ===")

	// Generate a UUID for the user (using proper UUID for nethttp_server compatibility)
	userID := uuid.New().String()
	t.Logf("Using user ID: %s", userID)

	// 1.1 Phone local write - using users table structure
	_, err = harness.client1DB.Exec(`
		INSERT INTO users(id, name, email, created_at, updated_at)
		VALUES (?, 'John Doe', 'john@phone.com', strftime('%s','now'), strftime('%s','now'))
	`, userID)
	require.NoError(t, err)

	// Verify triggers produced correct metadata
	var pendingCount, metaCount int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&metaCount)
	require.Equal(t, 1, pendingCount, "Should have pending change for user")
	require.Equal(t, 1, metaCount, "Should have row metadata for user")

	// Verify pending operation details
	var op string
	var baseVersion int64
	err = harness.client1DB.QueryRow(`
		SELECT op, base_version FROM _sync_pending
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&op, &baseVersion)
	require.NoError(t, err)
	require.Equal(t, "INSERT", op)
	require.Equal(t, int64(0), baseVersion)

	// 1.2 Phone → Server upload (simulated via UploadOnce)
	t.Log("=== Step 1.2: Phone uploads to server ===")

	// This simulates the HTTP upload described in the example
	err = harness.client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// 1.3 Verify upload result applied correctly
	t.Log("=== Step 1.3: Phone applies upload result ===")

	// Pending should be cleared
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 0, pendingCount, "Pending should be cleared after successful upload")

	// Row metadata should be updated with server version
	var serverVersion int64
	var deleted int
	err = harness.client1DB.QueryRow(`
		SELECT server_version, deleted FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&serverVersion, &deleted)
	require.NoError(t, err)
	require.Greater(t, serverVersion, int64(0), "Server version should be > 0 after upload")
	require.Equal(t, 0, deleted, "Row should not be marked as deleted")

	// Client info should be updated
	var currentWindowUntil int64
	err = harness.client1DB.QueryRow(`
		SELECT next_change_id, last_server_seq_seen, current_window_until
		FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&nextChangeID, &lastServerSeq, &currentWindowUntil)
	require.NoError(t, err)
	require.Equal(t, int64(2), nextChangeID, "Next change ID should be incremented")
	require.Equal(t, int64(0), lastServerSeq, "Last server seq should remain 0 before any download")
	require.Greater(t, currentWindowUntil, int64(0), "Upload watermark should be recorded")

	// Step 2: Laptop downloads the insert
	t.Log("=== Step 2: Laptop downloads the insert ===")

	// This simulates the HTTP download described in the example
	applied, nextAfter, err := harness.client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied, "Should have applied 1 change")
	require.Greater(t, nextAfter, int64(0), "Should have next_after value")

	// Verify laptop now has the user
	var name, email string
	var createdAt, updatedAt int64
	err = harness.client2DB.QueryRow(`
		SELECT name, email, created_at, updated_at FROM users WHERE id = ?
	`, userID).Scan(&name, &email, &createdAt, &updatedAt)
	require.NoError(t, err)
	require.Equal(t, "John Doe", name)
	require.Equal(t, "john@phone.com", email)
	require.Greater(t, createdAt, int64(0), "Should have created_at timestamp")
	require.Greater(t, updatedAt, int64(0), "Should have updated_at timestamp")

	// Verify laptop has row metadata
	err = harness.client2DB.QueryRow(`
		SELECT server_version, deleted FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&serverVersion, &deleted)
	require.NoError(t, err)
	require.Greater(t, serverVersion, int64(0), "Laptop should have server version")
	require.Equal(t, 0, deleted, "Row should not be marked as deleted")

	// Step 3: Both edit concurrently (conflict demo)
	t.Log("=== Step 3: Both edit concurrently (conflict demo) ===")

	// Store the current server version for conflict testing
	phoneServerVersion := serverVersion

	// 3.1 Phone updates user name - following the conflict scenario pattern
	t.Log("=== Step 3.1: Phone updates user name ===")
	_, err = harness.client1DB.Exec(`
		UPDATE users SET name='John Doe (phone)', updated_at=strftime('%s','now') WHERE id=?
	`, userID)
	require.NoError(t, err)

	// Verify pending change created
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 1, pendingCount, "Should have pending UPDATE for user")

	// Verify operation is UPDATE with correct base version
	err = harness.client1DB.QueryRow(`
		SELECT op, base_version FROM _sync_pending
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&op, &baseVersion)
	require.NoError(t, err)
	require.Equal(t, "UPDATE", op)
	require.Equal(t, phoneServerVersion, baseVersion, "Base version should match current server version")

	// Phone upload
	err = harness.client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Verify phone upload succeeded
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 0, pendingCount, "Phone pending should be cleared after successful upload")

	// Get new server version after phone update
	err = harness.client1DB.QueryRow(`
		SELECT server_version FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&serverVersion)
	require.NoError(t, err)
	require.Greater(t, serverVersion, phoneServerVersion, "Server version should be incremented after phone update")

	// 3.2 Laptop updates same row (now stale) - conflict scenario
	t.Log("=== Step 3.2: Laptop updates same row (conflict scenario) ===")
	_, err = harness.client2DB.Exec(`
		UPDATE users SET name='John Doe (laptop)', updated_at=strftime('%s','now') WHERE id=?
	`, userID)
	require.NoError(t, err)

	// Verify laptop has pending change with stale base version
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 1, pendingCount, "Laptop should have pending UPDATE for user")

	// Get laptop's base version (should be stale)
	var laptopBaseVersion int64
	err = harness.client2DB.QueryRow(`
		SELECT base_version FROM _sync_pending
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&laptopBaseVersion)
	require.NoError(t, err)
	require.Less(t, laptopBaseVersion, serverVersion, "Laptop base version should be stale")

	// Laptop upload (will conflict)
	err = harness.client2.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// After conflict resolution with server-wins strategy, pending should be cleared
	// and the server version should be applied locally
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 0, pendingCount, "Laptop pending should be cleared after conflict (server wins)")

	// Verify laptop now has the server version of the user
	err = harness.client2DB.QueryRow(`
		SELECT name, email, created_at, updated_at FROM users WHERE id = ?
	`, userID).Scan(&name, &email, &createdAt, &updatedAt)
	require.NoError(t, err)
	require.Equal(t, "John Doe (phone)", name, "Laptop should have server version (phone's name)")
	require.Equal(t, "john@phone.com", email)
	require.Greater(t, createdAt, int64(0), "Should have created_at timestamp")
	require.Greater(t, updatedAt, int64(0), "Should have updated_at timestamp")

	// Row metadata should be updated with server version
	err = harness.client2DB.QueryRow(`
		SELECT server_version, deleted FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&serverVersion, &deleted)
	require.NoError(t, err)
	require.Greater(t, serverVersion, phoneServerVersion, "Laptop should have latest server version")
	require.Equal(t, 0, deleted, "Row should not be marked as deleted")

	// Step 4: Phone deletes the user - testing delete operations
	t.Log("=== Step 4: Phone deletes the user ===")

	// Get current server version before delete
	err = harness.client1DB.QueryRow(`
		SELECT server_version FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&serverVersion)
	require.NoError(t, err)

	// Phone deletes the user
	_, err = harness.client1DB.Exec("DELETE FROM users WHERE id=?", userID)
	require.NoError(t, err)

	// Verify delete triggers worked
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 1, pendingCount, "Should have pending DELETE for user")

	// Verify operation is DELETE
	err = harness.client1DB.QueryRow(`
		SELECT op, base_version FROM _sync_pending
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&op, &baseVersion)
	require.NoError(t, err)
	require.Equal(t, "DELETE", op)
	require.Equal(t, serverVersion, baseVersion, "Base version should match current server version")

	// Phone upload delete
	err = harness.client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Verify delete upload succeeded
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&pendingCount)
	require.Equal(t, 0, pendingCount, "Phone pending should be cleared after delete upload")

	// Verify row metadata shows deletion
	err = harness.client1DB.QueryRow(`
		SELECT deleted FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&deleted)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "Row should be marked as deleted")

	// Laptop downloads the delete
	t.Log("=== Step 4: Laptop downloads the delete ===")
	applied, _, err = harness.client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Greater(t, applied, 0, "Should have applied changes (including delete)")

	// Verify laptop user is deleted
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
	require.Equal(t, 0, count, "Laptop user should be deleted")

	// Verify laptop row metadata shows deletion
	err = harness.client2DB.QueryRow(`
		SELECT deleted FROM _sync_row_meta
		WHERE table_name = 'users' AND pk_uuid = ?
	`, userID).Scan(&deleted)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "Laptop row should be marked as deleted")

	t.Log("=== Client Flow Example Test Completed Successfully ===")
	t.Log("✅ Verified complete client-server sync flow with conflicts and deletes")
}
