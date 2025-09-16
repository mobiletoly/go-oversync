package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// OversqliteTestHarness provides test utilities for oversqlite integration tests
type OversqliteTestHarness struct {
	t   *testing.T
	ctx context.Context

	// Server components
	testServer *server.TestServer
	serverURL  string

	// Client components
	client1DB *sql.DB
	client2DB *sql.DB
	client1   *Client
	client2   *Client

	// Auth
	userID    string
	device1ID string
	device2ID string

	logger *slog.Logger
}

// NewOversqliteTestHarness creates a new test harness for oversqlite integration tests
func NewOversqliteTestHarness(t *testing.T) *OversqliteTestHarness {
	ctx := context.Background()

	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Get database URL from environment
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"
	}

	// Create test server using nethttp_server setup
	serverConfig := &server.ServerConfig{
		DatabaseURL: databaseURL,
		JWTSecret:   "test-secret-key",
		Logger:      logger,
		AppName:     "oversqlite-integration-test",
	}

	testServer, err := server.NewTestServer(serverConfig)
	require.NoError(t, err)

	// Generate test identifiers
	userID := "test-user-" + uuid.New().String()[:8]
	device1ID := "device1-" + uuid.New().String()[:8]
	device2ID := "device2-" + uuid.New().String()[:8]

	harness := &OversqliteTestHarness{
		t:          t,
		ctx:        ctx,
		testServer: testServer,
		serverURL:  testServer.URL(),
		userID:     userID,
		device1ID:  device1ID,
		device2ID:  device2ID,
		logger:     logger,
	}

	// Initialize SQLite clients
	harness.initializeClients()

	return harness
}

// initializeClients creates and initializes the SQLite clients
func (h *OversqliteTestHarness) initializeClients() {
	// Create SQLite databases
	db1, err := sql.Open("sqlite3", ":memory:")
	require.NoError(h.t, err)
	h.client1DB = db1

	db2, err := sql.Open("sqlite3", ":memory:")
	require.NoError(h.t, err)
	h.client2DB = db2

	// Initialize databases manually for test harness (since we're not using NewClient here)
	err = initializeDatabase(h.client1DB)
	require.NoError(h.t, err)
	err = initializeDatabase(h.client2DB)
	require.NoError(h.t, err)

	// Create token functions
	token1Func := func(ctx context.Context) (string, error) {
		return h.testServer.GenerateToken(h.userID, h.device1ID, time.Hour)
	}
	token2Func := func(ctx context.Context) (string, error) {
		return h.testServer.GenerateToken(h.userID, h.device2ID, time.Hour)
	}

	// Create business tables first (before creating clients)
	h.createBusinessTables(h.client1DB)
	h.createBusinessTables(h.client2DB)

	// Create oversqlite clients with registered tables (this will create triggers automatically)
	config := DefaultConfig("business", []SyncTable{
		{TableName: "users"},
		{TableName: "posts"},
		{TableName: "files"},
		{TableName: "file_reviews"},
	})
	h.client1, err = NewClient(h.client1DB, h.serverURL, h.userID, h.device1ID, token1Func, config)
	require.NoError(h.t, err)
	h.client2, err = NewClient(h.client2DB, h.serverURL, h.userID, h.device2ID, token2Func, config)
	require.NoError(h.t, err)
}

// createBusinessTables creates the business tables (users and posts)
// Triggers are created automatically by NewClient
func (h *OversqliteTestHarness) createBusinessTables(db *sql.DB) {
	// Create users table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s','now')),
			updated_at INTEGER DEFAULT (strftime('%s','now'))
		)
	`)
	require.NoError(h.t, err)

	// Create posts table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s','now')),
			updated_at INTEGER DEFAULT (strftime('%s','now'))
		)
	`)
	require.NoError(h.t, err)

	// Create files table with BLOB primary key
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
			name TEXT NOT NULL,
			data BLOB
		)
	`)
	require.NoError(h.t, err)

	// Create file_reviews table with BLOB primary key and foreign key
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_reviews (
			id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
			file_id BLOB NOT NULL,
			review TEXT NOT NULL,
			FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
		)
	`)
	require.NoError(h.t, err)

	// Triggers are created automatically by NewClient for registered tables

	// Ensure source IDs for both clients
	_, err = EnsureSourceID(db, h.userID)
	require.NoError(h.t, err)
}

// insertTestData inserts test data into a client database
func (h *OversqliteTestHarness) insertTestData(db *sql.DB, userPrefix string) ([]string, []string) {
	var userIDs, postIDs []string

	// Insert users
	for i := 0; i < 3; i++ {
		userID := uuid.New().String()
		_, err := db.Exec(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`, userID, userPrefix+"-User-"+string(rune('A'+i)), userPrefix+"-user"+string(rune('a'+i))+"@example.com")
		require.NoError(h.t, err)
		userIDs = append(userIDs, userID)
	}

	// Insert posts
	for i := 0; i < 2; i++ {
		postID := uuid.New().String()
		_, err := db.Exec(`
			INSERT INTO posts (id, title, content, author_id) VALUES (?, ?, ?, ?)
		`, postID, userPrefix+"-Post-"+string(rune('1'+i)), "Content for "+userPrefix+" post "+string(rune('1'+i)), userIDs[i%len(userIDs)])
		require.NoError(h.t, err)
		postIDs = append(postIDs, postID)
	}

	return userIDs, postIDs
}

// getRowCounts returns the number of rows in users and posts tables
func (h *OversqliteTestHarness) getRowCounts(db *sql.DB) (int, int) {
	var userCount, postCount int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&postCount)
	return userCount, postCount
}

// getPendingCount returns the number of pending changes
func (h *OversqliteTestHarness) getPendingCount(db *sql.DB) int {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM _sync_pending").Scan(&count)
	return count
}

// Cleanup cleans up test resources
func (h *OversqliteTestHarness) Cleanup() {
	if h.client1DB != nil {
		h.client1DB.Close()
	}
	if h.client2DB != nil {
		h.client2DB.Close()
	}
	if h.testServer != nil {
		h.testServer.Close()
	}
}

// Test Functions

func TestOversqliteInitialization(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Test that databases are initialized correctly
	require.NotNil(t, harness.client1DB)
	require.NotNil(t, harness.client2DB)
	require.NotNil(t, harness.client1)
	require.NotNil(t, harness.client2)

	// Test that sync metadata tables exist
	tables := []string{"_sync_client_info", "_sync_row_meta", "_sync_pending"}
	for _, table := range tables {
		var count int
		err := harness.client1DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Table %s should exist", table)
	}

	// Test that business tables exist
	businessTables := []string{"users", "posts"}
	for _, table := range businessTables {
		var count int
		err := harness.client1DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Business table %s should exist", table)
	}

	// Test that triggers exist
	expectedTriggers := []string{
		"trg_users_ai", "trg_users_au", "trg_users_ad",
		"trg_posts_ai", "trg_posts_au", "trg_posts_ad",
	}
	for _, trigger := range expectedTriggers {
		var count int
		err := harness.client1DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?", trigger).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Trigger %s should exist", trigger)
	}
}

func TestOversqliteChangeTracking(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Initially no pending changes
	require.Equal(t, 0, harness.getPendingCount(harness.client1DB))

	// Insert test data
	userIDs, postIDs := harness.insertTestData(harness.client1DB, "Client1")
	require.Len(t, userIDs, 3)
	require.Len(t, postIDs, 2)

	// Should have pending changes now (3 users + 2 posts = 5 changes)
	require.Equal(t, 5, harness.getPendingCount(harness.client1DB))

	// Check row metadata was created
	var metaCount int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_row_meta").Scan(&metaCount)
	require.Equal(t, 5, metaCount)

	// Update a user
	_, err := harness.client1DB.Exec("UPDATE users SET name = ? WHERE id = ?", "Updated Name", userIDs[0])
	require.NoError(t, err)

	// Should still have 5 pending changes (update coalesces with insert)
	require.Equal(t, 5, harness.getPendingCount(harness.client1DB))

	// Delete a post
	_, err = harness.client1DB.Exec("DELETE FROM posts WHERE id = ?", postIDs[0])
	require.NoError(t, err)

	// Should now have 4 pending changes (INSERT→DELETE becomes no-op, canceling the INSERT)
	require.Equal(t, 4, harness.getPendingCount(harness.client1DB))

	// Verify that the INSERT→DELETE resulted in no pending change for this post
	var pendingCount int
	err = harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?", "posts", postIDs[0]).Scan(&pendingCount)
	require.NoError(t, err)
	require.Equal(t, 0, pendingCount, "INSERT→DELETE should result in no pending changes")

	// Verify that metadata was cleaned up for never-synced record
	var postMetaCount int
	err = harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_row_meta WHERE table_name = ? AND pk_uuid = ?", "posts", postIDs[0]).Scan(&postMetaCount)
	require.NoError(t, err)
	require.Equal(t, 0, postMetaCount, "Metadata should be cleaned up for never-synced INSERT→DELETE")
}

func TestOversqliteSerializeRow(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Insert test data
	userIDs, _ := harness.insertTestData(harness.client1DB, "Client1")

	// Test serializing a user row
	payload, err := harness.client1.SerializeRow(harness.ctx, "users", userIDs[0])
	require.NoError(t, err)
	require.NotNil(t, payload)

	// Parse the JSON to verify structure
	var userData map[string]interface{}
	err = json.Unmarshal(payload, &userData)
	require.NoError(t, err)
	require.Equal(t, userIDs[0], userData["id"])
	require.Equal(t, "Client1-User-A", userData["name"])
	require.Equal(t, "Client1-usera@example.com", userData["email"])

	// Test serializing non-existent row
	_, err = harness.client1.SerializeRow(harness.ctx, "users", "non-existent-id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "row not found")
}

func TestOversqliteSourceIDGeneration(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Test that source IDs are generated and persisted
	sourceID1, err := EnsureSourceID(harness.client1DB, harness.userID)
	require.NoError(t, err)
	require.NotEmpty(t, sourceID1)

	// Second call should return the same source ID
	sourceID2, err := EnsureSourceID(harness.client1DB, harness.userID)
	require.NoError(t, err)
	require.Equal(t, sourceID1, sourceID2)

	// Different user should get different source ID
	differentUserID := "different-user-" + uuid.New().String()[:8]
	sourceID3, err := EnsureSourceID(harness.client1DB, differentUserID)
	require.NoError(t, err)
	require.NotEqual(t, sourceID1, sourceID3)
}

func TestOversqliteClientInfo(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Check initial client info
	var sourceID string
	var nextChangeID, lastServerSeq int64
	err := harness.client1DB.QueryRow(`
		SELECT source_id, next_change_id, last_server_seq_seen
		FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&sourceID, &nextChangeID, &lastServerSeq)
	require.NoError(t, err)
	require.NotEmpty(t, sourceID)
	require.Equal(t, int64(1), nextChangeID)
	require.Equal(t, int64(0), lastServerSeq)
}

func TestOversqliteMultipleOperationsCoalescing(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Insert a user
	userID := uuid.New().String()
	_, err := harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User", "test@example.com")
	require.NoError(t, err)

	// Should have 1 pending change
	require.Equal(t, 1, harness.getPendingCount(harness.client1DB))

	// Update the same user multiple times
	_, err = harness.client1DB.Exec("UPDATE users SET name = ? WHERE id = ?", "Updated Name 1", userID)
	require.NoError(t, err)
	_, err = harness.client1DB.Exec("UPDATE users SET name = ? WHERE id = ?", "Updated Name 2", userID)
	require.NoError(t, err)
	_, err = harness.client1DB.Exec("UPDATE users SET email = ? WHERE id = ?", "updated@example.com", userID)
	require.NoError(t, err)

	// Should still have only 1 pending change (coalesced)
	require.Equal(t, 1, harness.getPendingCount(harness.client1DB))

	// Check that the operation is INSERT (preserved from original INSERT, not downgraded to UPDATE)
	var op string
	err = harness.client1DB.QueryRow("SELECT op FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?", "users", userID).Scan(&op)
	require.NoError(t, err)
	require.Equal(t, "INSERT", op, "INSERT→UPDATE should preserve INSERT operation")

	// Delete the user
	_, err = harness.client1DB.Exec("DELETE FROM users WHERE id = ?", userID)
	require.NoError(t, err)

	// Should now have 0 pending changes (INSERT→DELETE becomes no-op)
	require.Equal(t, 0, harness.getPendingCount(harness.client1DB))

	// Verify no pending change exists for this user
	var count int
	err = harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ? AND pk_uuid = ?", "users", userID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "INSERT→DELETE should result in no pending changes")
}

func TestOversqliteTriggerCreation(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Create a new test table
	_, err := harness.client1DB.Exec(`
		CREATE TABLE test_table (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	require.NoError(t, err)

	// Create a new client with the test table registered (this will create triggers automatically)
	config := DefaultConfig("business", []SyncTable{
		{TableName: "test_table"},
	})
	tokenFunc := func(ctx context.Context) (string, error) { return "test-token", nil }
	_, err = NewClient(harness.client1DB, harness.serverURL, harness.userID, harness.device1ID, tokenFunc, config)
	require.NoError(t, err)

	// Verify triggers were created
	expectedTriggers := []string{
		"trg_test_table_ai",
		"trg_test_table_au",
		"trg_test_table_ad",
	}
	for _, trigger := range expectedTriggers {
		var count int
		err = harness.client1DB.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?", trigger).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Trigger %s should exist", trigger)
	}

	// Test that triggers work
	testID := uuid.New().String()
	_, err = harness.client1DB.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", testID, "Test", 42)
	require.NoError(t, err)

	// Should have created pending change and row metadata
	var pendingCount, metaCount int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = ?", "test_table").Scan(&pendingCount)
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_row_meta WHERE table_name = ?", "test_table").Scan(&metaCount)
	require.Equal(t, 1, pendingCount)
	require.Equal(t, 1, metaCount)
}

// TestApplyModePreventsSyncLoops tests that apply_mode prevents sync triggers during server change application
func TestApplyModePreventsSyncLoops(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Create test table
	_, err := harness.client1DB.Exec(`
		CREATE TABLE test_table (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	require.NoError(t, err)

	// Create triggers for the test table
	syncTable := SyncTable{TableName: "test_table"}
	err = createTriggersForTable(harness.client1DB, syncTable)
	require.NoError(t, err)

	// Test 1: Normal mode (apply_mode = 0) should create sync entries
	t.Run("NormalModeCreatesSyncEntries", func(t *testing.T) {
		// Ensure apply_mode is 0
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 0")
		require.NoError(t, err)

		// Insert a record
		testID := uuid.New().String()
		_, err = harness.client1DB.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", testID, "Normal Mode", 100)
		require.NoError(t, err)

		// Should have created pending change and row metadata
		var pendingCount, metaCount int
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'test_table'").Scan(&pendingCount)
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_row_meta WHERE table_name = 'test_table'").Scan(&metaCount)
		require.Equal(t, 1, pendingCount, "Normal mode should create sync pending entry")
		require.Equal(t, 1, metaCount, "Normal mode should create row metadata")
	})

	// Test 2: Server apply mode (apply_mode = 1) should NOT create sync entries
	t.Run("ServerApplyModeSuppressesSyncEntries", func(t *testing.T) {
		// Clear existing sync data
		_, err = harness.client1DB.Exec("DELETE FROM _sync_pending")
		require.NoError(t, err)
		_, err = harness.client1DB.Exec("DELETE FROM _sync_row_meta")
		require.NoError(t, err)

		// Set apply_mode to 1 (server-apply mode)
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 1")
		require.NoError(t, err)

		// Insert a record (simulating server change application)
		testID := uuid.New().String()
		_, err = harness.client1DB.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", testID, "Server Apply Mode", 200)
		require.NoError(t, err)

		// Should NOT have created pending change (triggers suppressed)
		var pendingCount int
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'test_table'").Scan(&pendingCount)
		require.Equal(t, 0, pendingCount, "Server apply mode should suppress sync pending entries")

		// Reset apply_mode to 0
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 0")
		require.NoError(t, err)
	})

	// Test 3: Test all trigger types (INSERT, UPDATE, DELETE) with apply_mode
	t.Run("AllTriggerTypesSuppressed", func(t *testing.T) {
		// Clear existing sync data
		_, err = harness.client1DB.Exec("DELETE FROM _sync_pending")
		require.NoError(t, err)
		_, err = harness.client1DB.Exec("DELETE FROM _sync_row_meta")
		require.NoError(t, err)

		// Set apply_mode to 1
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 1")
		require.NoError(t, err)

		// Test INSERT trigger suppression
		testID := uuid.New().String()
		_, err = harness.client1DB.Exec("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", testID, "Test Insert", 300)
		require.NoError(t, err)

		// Test UPDATE trigger suppression
		_, err = harness.client1DB.Exec("UPDATE test_table SET name = ?, value = ? WHERE id = ?", "Updated Test", 400, testID)
		require.NoError(t, err)

		// Test DELETE trigger suppression
		_, err = harness.client1DB.Exec("DELETE FROM test_table WHERE id = ?", testID)
		require.NoError(t, err)

		// Should have NO pending changes (all triggers suppressed)
		var pendingCount int
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'test_table'").Scan(&pendingCount)
		require.Equal(t, 0, pendingCount, "All trigger types should be suppressed in server apply mode")

		// Reset apply_mode to 0
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 0")
		require.NoError(t, err)
	})
}

// TestDownloadBatchManagesApplyMode tests that downloadBatch properly manages apply_mode for entire batches
func TestDownloadBatchManagesApplyMode(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	// Create test table
	_, err := harness.client1DB.Exec(`
		CREATE TABLE test_table (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	require.NoError(t, err)

	// Create triggers for the test table
	syncTable := SyncTable{TableName: "test_table"}
	err = createTriggersForTable(harness.client1DB, syncTable)
	require.NoError(t, err)

	// Test that downloadBatch manages apply_mode correctly for batches
	t.Run("DownloadBatchManagesApplyModeForBatch", func(t *testing.T) {
		// Ensure apply_mode starts at 0
		var applyMode int
		harness.client1DB.QueryRow("SELECT apply_mode FROM _sync_client_info WHERE user_id = ?", harness.userID).Scan(&applyMode)
		require.Equal(t, 0, applyMode, "apply_mode should start at 0")

		// Test with empty batch (no changes) - apply_mode should remain 0
		applied, _, err := harness.client1.downloadBatch(harness.ctx, 10, false)
		require.NoError(t, err)
		require.Equal(t, 0, applied, "No changes should be applied for empty batch")

		// Verify apply_mode remains 0 for empty batch
		harness.client1DB.QueryRow("SELECT apply_mode FROM _sync_client_info WHERE user_id = ?", harness.userID).Scan(&applyMode)
		require.Equal(t, 0, applyMode, "apply_mode should remain 0 for empty batch")
	})

	// Test individual applyServerChange doesn't manage apply_mode
	t.Run("ApplyServerChangeDoesNotManageApplyMode", func(t *testing.T) {
		// Manually set apply_mode to 1 to simulate batch processing
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 1 WHERE user_id = ?", harness.userID)
		require.NoError(t, err)

		// Create a server change to apply
		testID := uuid.New().String()
		payload := map[string]interface{}{
			"id":    testID,
			"name":  "Server Change",
			"value": 500,
		}
		payloadJSON, err := json.Marshal(payload)
		require.NoError(t, err)

		serverChange := &oversync.ChangeDownloadResponse{
			ServerID:      1,
			SourceID:      "server",
			TableName:     "test_table",
			Op:            "INSERT",
			PK:            testID,
			Payload:       payloadJSON,
			ServerVersion: 1,
		}

		// Apply the server change
		err = harness.client1.applyServerChange(harness.ctx, serverChange)
		require.NoError(t, err)

		// Verify apply_mode is still 1 (not modified by applyServerChange)
		var applyMode int
		harness.client1DB.QueryRow("SELECT apply_mode FROM _sync_client_info WHERE user_id = ?", harness.userID).Scan(&applyMode)
		require.Equal(t, 1, applyMode, "apply_mode should remain 1 (not modified by applyServerChange)")

		// Verify the record was inserted
		var recordCount int
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM test_table WHERE id = ?", testID).Scan(&recordCount)
		require.Equal(t, 1, recordCount, "Server change should have been applied")

		// Verify NO sync entries were created (triggers were suppressed due to apply_mode = 1)
		var pendingCount int
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'test_table' AND pk_uuid = ?", testID).Scan(&pendingCount)
		require.Equal(t, 0, pendingCount, "Server change application should not create sync pending entries when apply_mode = 1")

		// Reset apply_mode to 0 for cleanup
		_, err = harness.client1DB.Exec("UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?", harness.userID)
		require.NoError(t, err)
	})
}

// TestMultiDeviceSyncScenario tests the exact scenario described by the user:
// - Sign in with user A database on device 1
// - Perform bootstrap/hydration
// - Sign in with user A database on device 2
// - Perform bootstrap/hydration
// - Add Person record to device 1, perform upload
// - Add Person record to device 2, perform upload
// - Perform sync/download device 1
// - Perform sync/download device 2
// - Make sure that both device 1 and device 2 have the same records
func TestMultiDeviceSyncScenario(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	logger := harness.logger

	// Use existing users table (already created by harness and registered on server)
	// No need to create additional tables since users table already exists

	// Use existing clients from harness (already configured with users and posts tables)
	client1 := harness.client1
	client2 := harness.client2

	// Step 1: Sign in with user A database on device 1 and perform bootstrap/hydration
	logger.Info("=== Step 1: Device 1 bootstrap/hydration ===")

	// Verify device 1 is properly initialized
	var device1SourceID string
	var device1NextChangeID, device1LastServerSeq int64
	err := harness.client1DB.QueryRow(`
		SELECT source_id, next_change_id, last_server_seq_seen
		FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&device1SourceID, &device1NextChangeID, &device1LastServerSeq)
	require.NoError(t, err)
	require.NotEmpty(t, device1SourceID)
	require.Equal(t, int64(1), device1NextChangeID)
	require.Equal(t, int64(0), device1LastServerSeq)

	// Perform initial download (bootstrap/hydration) - should get no data
	applied1, _, err := client1.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, applied1, "Device 1 should get no data during bootstrap")

	// Step 2: Sign in with user A database on device 2 and perform bootstrap/hydration
	logger.Info("=== Step 2: Device 2 bootstrap/hydration ===")

	// Verify device 2 is properly initialized
	var device2SourceID string
	var device2NextChangeID, device2LastServerSeq int64
	err = harness.client2DB.QueryRow(`
		SELECT source_id, next_change_id, last_server_seq_seen
		FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&device2SourceID, &device2NextChangeID, &device2LastServerSeq)
	require.NoError(t, err)
	require.NotEmpty(t, device2SourceID)
	require.Equal(t, int64(1), device2NextChangeID)
	require.Equal(t, int64(0), device2LastServerSeq)

	// Verify devices have different source IDs
	require.NotEqual(t, device1SourceID, device2SourceID, "Devices should have different source IDs")

	// Perform initial download (bootstrap/hydration) - should get no data
	applied2, _, err := client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, applied2, "Device 2 should get no data during bootstrap")

	// Step 3: Add Person record to device 1, perform upload
	logger.Info("=== Step 3: Device 1 creates user and uploads ===")

	person1ID := uuid.New().String()
	_, err = harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, person1ID, "John Doe (Device 1)", "john.device1@example.com")
	require.NoError(t, err)

	// Verify pending change was created
	var pendingCount int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users'").Scan(&pendingCount)
	require.Equal(t, 1, pendingCount, "Device 1 should have 1 pending change")

	// Upload the change
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Verify upload succeeded (pending cleared)
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users'").Scan(&pendingCount)
	require.Equal(t, 0, pendingCount, "Device 1 pending should be cleared after upload")

	// Step 4: Add Person record to device 2, perform upload
	logger.Info("=== Step 4: Device 2 creates user and uploads ===")

	person2ID := uuid.New().String()
	_, err = harness.client2DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, person2ID, "Jane Smith (Device 2)", "jane.device2@example.com")
	require.NoError(t, err)

	// Verify pending change was created
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users'").Scan(&pendingCount)
	require.Equal(t, 1, pendingCount, "Device 2 should have 1 pending change")

	// Upload the change
	err = client2.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Verify upload succeeded (pending cleared)
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM _sync_pending WHERE table_name = 'users'").Scan(&pendingCount)
	require.Equal(t, 0, pendingCount, "Device 2 pending should be cleared after upload")

	// Step 5: Perform download on device 1
	logger.Info("=== Step 5: Device 1 downloads changes ===")

	applied1, _, err = client1.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied1, "Device 1 should download 1 change (user from device 2)")

	// Step 6: Test the improved UploadOnce method with sync window strategy on device 2
	logger.Info("=== Step 6: Device 2 tests sync window strategy ===")

	// Check Device 2's sync state before sync
	var device2LastSeq int64
	harness.client2DB.QueryRow("SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?", harness.userID).Scan(&device2LastSeq)
	logger.Info("Device 2 sync state before sync", "last_server_seq_seen", device2LastSeq)

	// The improved UploadOnce method should use sync window strategy to catch Device 1's change
	// Even though Device 2 has no pending changes to upload, the sync window download should work
	err = client2.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Check if Device 2 now has both users (indicating sync window strategy worked)
	var device2UserCount int
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users").Scan(&device2UserCount)
	logger.Info("Device 2 user count after UploadOnce", "count", device2UserCount)

	// Check if the sync window strategy fixed the sync ordering bug
	if device2UserCount != 2 {
		t.Logf("❌ SYNC ORDERING BUG STILL EXISTS: Device 2 has %d users (expected 2)", device2UserCount)
		t.Logf("Device 2's last_server_seq_seen (%d) is ahead of Device 1's change", device2LastSeq)
		t.Logf("The sync window strategy in UploadOnce didn't fix the issue")

		// Let's verify Device 1's change is actually on the server by checking what Device 2 would get
		// if we reset its sync state (this proves the change exists on server)
		_, err = harness.client2DB.Exec("UPDATE _sync_client_info SET last_server_seq_seen = 0 WHERE user_id = ?", harness.userID)
		require.NoError(t, err)

		appliedWithReset, _, err := client2.DownloadOnce(harness.ctx, 100)
		require.NoError(t, err)

		t.Logf("✅ PROOF: When sync state reset, Device 2 gets %d changes", appliedWithReset)
		t.Logf("This confirms Device 1's change exists on server but is unreachable due to sync ordering")

		// Fail the test to highlight that the fix didn't work
		t.Fatalf("SYNC ORDERING BUG: Sync window strategy didn't fix the issue. Device 2 users: %d, Expected: 2", device2UserCount)
	}

	// If we get here, the sync window strategy successfully fixed the sync ordering bug
	logger.Info("✅ Sync window strategy successfully fixed sync ordering bug")
	require.Equal(t, 2, device2UserCount, "Device 2 should have 2 users after sync window strategy")

	// Step 7: Verify both devices have the same records
	logger.Info("=== Step 7: Verify both devices have same records ===")

	// Get all users from device 1
	rows1, err := harness.client1DB.Query("SELECT id, name, email FROM users ORDER BY name")
	require.NoError(t, err)
	defer rows1.Close()

	var device1Users []struct {
		ID    string
		Name  string
		Email string
	}
	for rows1.Next() {
		var user struct {
			ID    string
			Name  string
			Email string
		}
		err = rows1.Scan(&user.ID, &user.Name, &user.Email)
		require.NoError(t, err)
		device1Users = append(device1Users, user)
	}

	// Get all users from device 2
	rows2, err := harness.client2DB.Query("SELECT id, name, email FROM users ORDER BY name")
	require.NoError(t, err)
	defer rows2.Close()

	var device2Users []struct {
		ID    string
		Name  string
		Email string
	}
	for rows2.Next() {
		var user struct {
			ID    string
			Name  string
			Email string
		}
		err = rows2.Scan(&user.ID, &user.Name, &user.Email)
		require.NoError(t, err)
		device2Users = append(device2Users, user)
	}

	// Verify both devices have exactly 2 users
	require.Len(t, device1Users, 2, "Device 1 should have 2 users")
	require.Len(t, device2Users, 2, "Device 2 should have 2 users")

	// Verify the records are identical
	require.Equal(t, device1Users, device2Users, "Both devices should have identical user records")

	// Verify specific users exist on both devices
	userIDs := []string{person1ID, person2ID}
	for _, userID := range userIDs {
		var count1, count2 int
		harness.client1DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count1)
		harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count2)
		require.Equal(t, 1, count1, "Device 1 should have user %s", userID)
		require.Equal(t, 1, count2, "Device 2 should have user %s", userID)
	}

	// Verify row metadata is consistent
	for _, userID := range userIDs {
		var device1ServerVersion, device2ServerVersion int64
		err = harness.client1DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&device1ServerVersion)
		require.NoError(t, err)
		err = harness.client2DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&device2ServerVersion)
		require.NoError(t, err)
		require.Equal(t, device1ServerVersion, device2ServerVersion, "Both devices should have same server version for user %s", userID)
	}

	logger.Info("✅ Multi-device sync scenario completed successfully")
	logger.Info("✅ Both devices have identical records as expected")
}

// TestRecordResurrectionBug reproduces the record resurrection issue that occurs
// when a DELETE operation is followed by an INSERT operation with higher server versions,
// but the client processes them in the wrong order due to missing download optimization
func TestRecordResurrectionBug(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	logger := harness.logger
	client1 := harness.client1
	client2 := harness.client2

	logger.Info("=== Testing Record Resurrection Bug ===")

	// Step 1: Device 1 creates a user and syncs it
	logger.Info("Step 1: Device 1 creates user")
	userID := uuid.New().String()
	_, err := harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User", "test@example.com")
	require.NoError(t, err)

	// Upload from device 1
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Download to device 2
	applied, _, err := client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied, "Device 2 should receive the user")

	// Verify both devices have the user
	var count1, count2 int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count1)
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count2)
	require.Equal(t, 1, count1, "Device 1 should have the user")
	require.Equal(t, 1, count2, "Device 2 should have the user")

	// Step 2: Device 1 deletes the user and syncs
	logger.Info("Step 2: Device 1 deletes user")
	_, err = harness.client1DB.Exec("DELETE FROM users WHERE id = ?", userID)
	require.NoError(t, err)

	// Upload from device 1 (DELETE operation)
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Step 3: Device 1 re-inserts the user (same ID) and syncs
	logger.Info("Step 3: Device 1 re-inserts user with same ID")
	_, err = harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User Reinserted", "test-reinserted@example.com")
	require.NoError(t, err)

	// Upload from device 1 (INSERT operation with higher server version)
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Step 4: Device 2 downloads both DELETE and INSERT operations
	// This is where the bug occurs - without download optimization,
	// Device 2 might process DELETE after INSERT, causing record resurrection
	logger.Info("Step 4: Device 2 downloads DELETE and INSERT operations")
	applied, _, err = client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Greater(t, applied, 0, "Device 2 should receive operations")

	// Step 5: Verify final state - both devices should have the user
	// If the bug exists, Device 2 might not have the user (deleted by mistake)
	logger.Info("Step 5: Verifying final state")

	harness.client1DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count1)
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count2)

	logger.Info("Final counts", "device1", count1, "device2", count2)

	// Both devices should have the user (the reinserted version)
	require.Equal(t, 1, count1, "Device 1 should have the reinserted user")
	require.Equal(t, 1, count2, "Device 2 should have the reinserted user (bug: might be 0 due to record resurrection)")

	// Verify the user data is correct (should be the reinserted version)
	var name1, name2 string
	err = harness.client1DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name1)
	require.NoError(t, err)
	err = harness.client2DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name2)
	require.NoError(t, err)

	require.Equal(t, "Test User Reinserted", name1, "Device 1 should have reinserted user data")
	require.Equal(t, "Test User Reinserted", name2, "Device 2 should have reinserted user data")

	// Verify server versions are consistent
	var sv1, sv2 int64
	err = harness.client1DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&sv1)
	require.NoError(t, err)
	err = harness.client2DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&sv2)
	require.NoError(t, err)

	require.Equal(t, sv1, sv2, "Both devices should have same server version")
	require.Greater(t, sv1, int64(1), "Server version should be > 1 (after DELETE and INSERT)")

	logger.Info("✅ Record resurrection test completed")
}

// TestRecordResurrectionBugDetailed creates a more detailed test that specifically
// checks the order of operations and forces the problematic scenario
func TestRecordResurrectionBugDetailed(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	logger := harness.logger
	client1 := harness.client1
	client2 := harness.client2

	logger.Info("=== Testing Record Resurrection Bug (Detailed) ===")

	// Step 1: Device 1 creates a user and syncs it
	logger.Info("Step 1: Device 1 creates user")
	userID := uuid.New().String()
	_, err := harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User", "test@example.com")
	require.NoError(t, err)

	// Upload from device 1
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Download to device 2
	applied, _, err := client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied, "Device 2 should receive the user")

	// Get initial server version
	var initialServerVersion int64
	err = harness.client1DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&initialServerVersion)
	require.NoError(t, err)
	logger.Info("Initial server version", "version", initialServerVersion)

	// Step 2: Device 1 deletes the user and syncs
	logger.Info("Step 2: Device 1 deletes user")
	_, err = harness.client1DB.Exec("DELETE FROM users WHERE id = ?", userID)
	require.NoError(t, err)

	// Upload from device 1 (DELETE operation)
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Get DELETE server version
	var deleteServerVersion int64
	err = harness.client1DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&deleteServerVersion)
	require.NoError(t, err)
	logger.Info("DELETE server version", "version", deleteServerVersion)

	// Step 3: Device 1 re-inserts the user (same ID) and syncs
	logger.Info("Step 3: Device 1 re-inserts user with same ID")
	_, err = harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User Reinserted", "test-reinserted@example.com")
	require.NoError(t, err)

	// Upload from device 1 (INSERT operation with higher server version)
	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Get INSERT server version
	var insertServerVersion int64
	err = harness.client1DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&insertServerVersion)
	require.NoError(t, err)
	logger.Info("INSERT server version", "version", insertServerVersion)

	// Verify server versions are incremental
	require.Greater(t, deleteServerVersion, initialServerVersion, "DELETE should have higher server version than initial INSERT")
	require.Greater(t, insertServerVersion, deleteServerVersion, "Final INSERT should have higher server version than DELETE")

	// Step 4: Device 2 downloads both DELETE and INSERT operations
	logger.Info("Step 4: Device 2 downloads DELETE and INSERT operations")

	// Before download, check Device 2's current state
	var count2Before int
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count2Before)
	logger.Info("Device 2 count before download", "count", count2Before)

	applied, _, err = client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied, "With optimization, Device 2 applies only INSERT (DELETE is skipped)")

	// Step 5: Verify final state - both devices should have the user
	logger.Info("Step 5: Verifying final state")

	var count1, count2 int
	harness.client1DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count1)
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count2)

	logger.Info("Final counts", "device1", count1, "device2", count2)

	// Check server versions on both devices
	var sv1, sv2 int64
	err = harness.client1DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&sv1)
	require.NoError(t, err)
	err = harness.client2DB.QueryRow("SELECT server_version FROM _sync_row_meta WHERE table_name = 'users' AND pk_uuid = ?", userID).Scan(&sv2)
	require.NoError(t, err)

	logger.Info("Final server versions", "device1", sv1, "device2", sv2)

	// Both devices should have the user (the reinserted version)
	require.Equal(t, 1, count1, "Device 1 should have the reinserted user")
	require.Equal(t, 1, count2, "Device 2 should have the reinserted user (bug: might be 0 due to record resurrection)")

	// Verify the user data is correct (should be the reinserted version)
	var name1, name2 string
	err = harness.client1DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name1)
	require.NoError(t, err)
	err = harness.client2DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name2)
	require.NoError(t, err)

	require.Equal(t, "Test User Reinserted", name1, "Device 1 should have reinserted user data")
	require.Equal(t, "Test User Reinserted", name2, "Device 2 should have reinserted user data")

	// Verify server versions are consistent
	require.Equal(t, sv1, sv2, "Both devices should have same server version")
	require.Equal(t, insertServerVersion, sv1, "Final server version should match INSERT operation")

	logger.Info("✅ Detailed record resurrection test completed")
}

// TestServerResponseOrder tests the actual order of operations returned by the server
// to see if the server is returning DELETE and INSERT in the correct chronological order
func TestServerResponseOrder(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	logger := harness.logger
	client1 := harness.client1
	client2 := harness.client2

	logger.Info("=== Testing Server Response Order ===")

	// Step 1: Device 1 creates a user and syncs it
	userID := uuid.New().String()
	_, err := harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User", "test@example.com")
	require.NoError(t, err)

	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	applied, _, err := client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied)

	// Step 2: Device 1 deletes the user
	_, err = harness.client1DB.Exec("DELETE FROM users WHERE id = ?", userID)
	require.NoError(t, err)

	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Step 3: Device 1 re-inserts the user
	_, err = harness.client1DB.Exec(`
		INSERT INTO users (id, name, email) VALUES (?, ?, ?)
	`, userID, "Test User Reinserted", "test-reinserted@example.com")
	require.NoError(t, err)

	err = client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	// Step 4: Manually call the server download endpoint to inspect the response
	var lastServerSeq int64
	err = harness.client2DB.QueryRow(`
		SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&lastServerSeq)
	require.NoError(t, err)

	logger.Info("About to download from server", "after", lastServerSeq)

	// Use the client's sendDownloadRequest method to get raw server response
	downloadResponse, err := client2.sendDownloadRequest(harness.ctx, lastServerSeq, 100, false)
	require.NoError(t, err)

	logger.Info("Server response", "changes_count", len(downloadResponse.Changes))

	// Inspect each change in the response
	for i, change := range downloadResponse.Changes {
		if change.PK == userID {
			logger.Info("Server change",
				"index", i,
				"op", change.Op,
				"pk", change.PK,
				"server_version", change.ServerVersion,
				"table", change.TableName)
		}
	}

	// Verify that DELETE comes before INSERT in server version order
	var deleteVersion, insertVersion int64 = -1, -1
	for _, change := range downloadResponse.Changes {
		if change.PK == userID {
			if change.Op == "DELETE" {
				deleteVersion = change.ServerVersion
			} else if change.Op == "INSERT" {
				insertVersion = change.ServerVersion
			}
		}
	}

	require.NotEqual(t, int64(-1), deleteVersion, "Should find DELETE operation")
	require.NotEqual(t, int64(-1), insertVersion, "Should find INSERT operation")
	require.Less(t, deleteVersion, insertVersion, "DELETE should have lower server version than INSERT")

	// Now apply the changes and verify the result
	applied, _, err = client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, applied, "With optimization, should apply only INSERT (DELETE is skipped)")

	// Final verification
	var count2 int
	harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count2)
	require.Equal(t, 1, count2, "Device 2 should have the reinserted user")

	var name2 string
	err = harness.client2DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name2)
	require.NoError(t, err)
	require.Equal(t, "Test User Reinserted", name2, "Should have the reinserted user data")

	logger.Info("✅ Server response order test completed")
}

// TestDownloadOptimizationNeeded creates a scenario where the missing download optimization
// causes inefficient processing that could lead to issues in more complex scenarios
func TestDownloadOptimizationNeeded(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	logger := harness.logger
	client1 := harness.client1
	client2 := harness.client2

	logger.Info("=== Testing Download Optimization Need ===")

	// Create multiple users to simulate a batch scenario
	userIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		userIDs[i] = uuid.New().String()
		_, err := harness.client1DB.Exec(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`, userIDs[i], fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}

	// Upload and sync initial users
	err := client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	applied, _, err := client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 3, applied)

	// Now perform DELETE→INSERT sequence for each user
	for i, userID := range userIDs {
		// Delete the user
		_, err = harness.client1DB.Exec("DELETE FROM users WHERE id = ?", userID)
		require.NoError(t, err)

		// Upload DELETE
		err = client1.UploadOnce(harness.ctx)
		require.NoError(t, err)

		// Re-insert the user with updated data
		_, err = harness.client1DB.Exec(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`, userID, fmt.Sprintf("Updated User %d", i), fmt.Sprintf("updated-user%d@example.com", i))
		require.NoError(t, err)

		// Upload INSERT
		err = client1.UploadOnce(harness.ctx)
		require.NoError(t, err)
	}

	// Now Device 2 will download all the DELETE and INSERT operations
	// Without optimization, it will process 6 operations (3 DELETEs + 3 INSERTs)
	// With optimization, it should only process 3 operations (3 INSERTs, skipping DELETEs)

	var lastServerSeq int64
	err = harness.client2DB.QueryRow(`
		SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&lastServerSeq)
	require.NoError(t, err)

	// Get the raw server response to count operations
	downloadResponse, err := client2.sendDownloadRequest(harness.ctx, lastServerSeq, 100, false)
	require.NoError(t, err)

	// Count operations for our users
	deleteCount := 0
	insertCount := 0
	for _, change := range downloadResponse.Changes {
		for _, userID := range userIDs {
			if change.PK == userID {
				if change.Op == "DELETE" {
					deleteCount++
				} else if change.Op == "INSERT" {
					insertCount++
				}
			}
		}
	}

	logger.Info("Operations in server response", "deletes", deleteCount, "inserts", insertCount)

	// We should see 3 DELETEs and 3 INSERTs
	require.Equal(t, 3, deleteCount, "Should have 3 DELETE operations")
	require.Equal(t, 3, insertCount, "Should have 3 INSERT operations")

	// Apply the changes
	applied, _, err = client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)

	// Without optimization, this processes all 6 operations
	// With optimization, this would process only 3 operations (skipping superseded DELETEs)
	logger.Info("Operations applied by client", "applied", applied)
	require.Equal(t, 3, applied, "With optimization, client processes only 3 operations (skipping superseded DELETEs)")

	// Verify final state - all users should exist with updated data
	for i, userID := range userIDs {
		var count int
		harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
		require.Equal(t, 1, count, "User %d should exist", i)

		var name string
		err = harness.client2DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("Updated User %d", i), name, "User %d should have updated data", i)
	}

	logger.Info("✅ Download optimization test completed")
	logger.Info("📝 Note: This test shows that without optimization, the client processes 6 operations instead of the optimal 3")
}

// TestDownloadOptimizationWorking verifies that the download optimization is working correctly
// by skipping superseded DELETE operations
func TestDownloadOptimizationWorking(t *testing.T) {
	harness := NewOversqliteTestHarness(t)
	defer harness.Cleanup()

	logger := harness.logger
	client1 := harness.client1
	client2 := harness.client2

	logger.Info("=== Testing Download Optimization Working ===")

	// Create multiple users to simulate a batch scenario
	userIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		userIDs[i] = uuid.New().String()
		_, err := harness.client1DB.Exec(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`, userIDs[i], fmt.Sprintf("User %d", i), fmt.Sprintf("user%d@example.com", i))
		require.NoError(t, err)
	}

	// Upload and sync initial users
	err := client1.UploadOnce(harness.ctx)
	require.NoError(t, err)

	applied, _, err := client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 3, applied)

	// Now perform DELETE→INSERT sequence for each user
	for i, userID := range userIDs {
		// Delete the user
		_, err = harness.client1DB.Exec("DELETE FROM users WHERE id = ?", userID)
		require.NoError(t, err)

		// Upload DELETE
		err = client1.UploadOnce(harness.ctx)
		require.NoError(t, err)

		// Re-insert the user with updated data
		_, err = harness.client1DB.Exec(`
			INSERT INTO users (id, name, email) VALUES (?, ?, ?)
		`, userID, fmt.Sprintf("Updated User %d", i), fmt.Sprintf("updated-user%d@example.com", i))
		require.NoError(t, err)

		// Upload INSERT
		err = client1.UploadOnce(harness.ctx)
		require.NoError(t, err)
	}

	// Now Device 2 will download all the DELETE and INSERT operations
	// With optimization, it should only process 3 operations (3 INSERTs, skipping superseded DELETEs)

	var lastServerSeq int64
	err = harness.client2DB.QueryRow(`
		SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?
	`, harness.userID).Scan(&lastServerSeq)
	require.NoError(t, err)

	// Get the raw server response to count operations
	downloadResponse, err := client2.sendDownloadRequest(harness.ctx, lastServerSeq, 100, false)
	require.NoError(t, err)

	// Count operations for our users
	deleteCount := 0
	insertCount := 0
	for _, change := range downloadResponse.Changes {
		for _, userID := range userIDs {
			if change.PK == userID {
				if change.Op == "DELETE" {
					deleteCount++
				} else if change.Op == "INSERT" {
					insertCount++
				}
			}
		}
	}

	logger.Info("Operations in server response", "deletes", deleteCount, "inserts", insertCount)

	// We should see 3 DELETEs and 3 INSERTs in the server response
	require.Equal(t, 3, deleteCount, "Should have 3 DELETE operations in server response")
	require.Equal(t, 3, insertCount, "Should have 3 INSERT operations in server response")

	// Apply the changes
	applied, _, err = client2.DownloadOnce(harness.ctx, 100)
	require.NoError(t, err)

	// With optimization, this should process only 3 operations (skipping superseded DELETEs)
	logger.Info("Operations applied by client", "applied", applied)
	require.Equal(t, 3, applied, "With optimization, client should process only 3 operations (skipping superseded DELETEs)")

	// Verify final state - all users should exist with updated data
	for i, userID := range userIDs {
		var count int
		harness.client2DB.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
		require.Equal(t, 1, count, "User %d should exist", i)

		var name string
		err = harness.client2DB.QueryRow("SELECT name FROM users WHERE id = ?", userID).Scan(&name)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("Updated User %d", i), name, "User %d should have updated data", i)
	}

	logger.Info("✅ Download optimization working test completed")
	logger.Info("🎉 Optimization successfully reduced operations from 6 to 3!")
}
