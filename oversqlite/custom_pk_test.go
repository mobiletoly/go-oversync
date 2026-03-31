package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestCustomPrimaryKeyColumns tests that oversqlite works with custom primary key column names
func TestCustomPrimaryKeyColumns(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test tables with custom primary key columns
	_, err = db.Exec(`
		CREATE TABLE users (
			user_uuid TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE products (
			product_code TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}

	// Create test table with default "id" column
	_, err = db.Exec(`
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	// Create configuration with custom primary key columns
	config := DefaultConfig("test", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "user_uuid"},
		{TableName: "products", SyncKeyColumnName: "product_code"},
		{TableName: "posts", SyncKeyColumnName: "id"},
	})

	// Mock token function
	tokenFunc := func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	// Create client
	client, err := NewClient(db, "http://localhost:8080", tokenFunc, config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test that triggers were created successfully by checking if they exist
	var triggerCount int
	err = db.QueryRow(`
		SELECT COUNT(*) FROM sqlite_master 
		WHERE type='trigger' AND name LIKE 'trg_%'
	`).Scan(&triggerCount)
	if err != nil {
		t.Fatalf("Failed to count triggers: %v", err)
	}

	// Each managed table now gets 3 guard triggers plus 3 dirty-capture triggers.
	expectedTriggers := 18
	if triggerCount != expectedTriggers {
		t.Errorf("Expected %d triggers, got %d", expectedTriggers, triggerCount)
	}

	// Test primaryKeyColumnForTable helper method
	testCases := []struct {
		tableName  string
		expectedPK string
		expectErr  bool
	}{
		{"users", "user_uuid", false},
		{"products", "product_code", false},
		{"posts", "id", false},
		{"nonexistent", "", true},
	}

	for _, tc := range testCases {
		actualPK, err := client.primaryKeyColumnForTable(tc.tableName)
		if tc.expectErr {
			if err == nil {
				t.Errorf("expected error for table %s, got pk %s", tc.tableName, actualPK)
			}
			continue
		}
		if err != nil {
			t.Errorf("unexpected error for table %s: %v", tc.tableName, err)
			continue
		}
		if actualPK != tc.expectedPK {
			t.Errorf("For table %s, expected PK column %s, got %s", tc.tableName, tc.expectedPK, actualPK)
		}
	}
}

// TestNewClient_RequiresExplicitPrimaryKeyConfig verifies that setup fails without SyncKeyColumnName.
func TestNewClient_RequiresExplicitPrimaryKeyConfig(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with default "id" column
	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Use DefaultConfig with empty SyncKeyColumnName - this is now invalid.
	config := DefaultConfig("test", []SyncTable{
		{TableName: "users"},
	})

	// Verify the config was created correctly
	if len(config.Tables) != 1 {
		t.Fatalf("Expected 1 table in config, got %d", len(config.Tables))
	}

	syncTable := config.Tables[0]
	if syncTable.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", syncTable.TableName)
	}
	if syncTable.SyncKeyColumnName != "" {
		t.Errorf("Expected empty SyncKeyColumnName, got '%s'", syncTable.SyncKeyColumnName)
	}

	// Mock token function
	tokenFunc := func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	_, err = NewClient(db, "http://localhost:8080", tokenFunc, config)
	if err == nil {
		t.Fatal("expected client creation to fail when SyncKeyColumnName is omitted")
	}
}

func TestNewClient_FailsWhenSyncKeyColumnDoesNotExist(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	config := DefaultConfig("test", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "missing_pk"},
	})
	tokenFunc := func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}

	_, err = NewClient(db, "http://localhost:8080", tokenFunc, config)
	if err == nil {
		t.Fatal("expected client creation to fail when SyncKeyColumnName does not exist")
	}
	if !strings.Contains(err.Error(), "missing_pk") {
		t.Fatalf("expected error to mention missing_pk, got: %v", err)
	}
}

// TestTriggerGenerationWithCustomPK tests that triggers are generated correctly with custom primary keys
func TestTriggerGenerationWithCustomPK(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with custom primary key
	_, err = db.Exec(`
		CREATE TABLE test_table (
			custom_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Initialize database (create sync tables)
	if err := initializeDatabase(db); err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	// Create triggers for the table with custom primary key
	syncTable := SyncTable{
		TableName:         "test_table",
		SyncKeyColumnName: "custom_id",
	}

	err = createTriggersForTable(db, syncTable)
	if err != nil {
		t.Fatalf("Failed to create triggers: %v", err)
	}

	// Verify triggers were created
	var triggerNames []string
	rows, err := db.Query(`
		SELECT name FROM sqlite_master 
		WHERE type='trigger' AND tbl_name='test_table'
		ORDER BY name
	`)
	if err != nil {
		t.Fatalf("Failed to query triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Failed to scan trigger name: %v", err)
		}
		triggerNames = append(triggerNames, name)
	}

	expectedTriggers := []string{
		"trg_test_table_ad",       // DELETE trigger
		"trg_test_table_ai",       // INSERT trigger
		"trg_test_table_au",       // UPDATE trigger
		"trg_test_table_bd_guard", // BEFORE DELETE guard trigger
		"trg_test_table_bi_guard", // BEFORE INSERT guard trigger
		"trg_test_table_bu_guard", // BEFORE UPDATE guard trigger
	}

	if len(triggerNames) != len(expectedTriggers) {
		t.Errorf("Expected %d triggers, got %d: %v", len(expectedTriggers), len(triggerNames), triggerNames)
	}

	for i, expected := range expectedTriggers {
		if i >= len(triggerNames) || triggerNames[i] != expected {
			t.Errorf("Expected trigger %s, got %s", expected, triggerNames[i])
		}
	}
}

// TestTriggersWithCustomPKFunctionality tests that triggers work correctly with custom primary keys
func TestTriggersWithCustomPKFunctionality(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with custom primary key
	_, err = db.Exec(`
		CREATE TABLE test_items (
			item_code TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			quantity INTEGER DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Initialize database and create triggers
	if err := initializeDatabase(db); err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	syncTable := SyncTable{
		TableName:         "test_items",
		SyncKeyColumnName: "item_code",
	}

	err = createTriggersForTable(db, syncTable)
	if err != nil {
		t.Fatalf("Failed to create triggers: %v", err)
	}

	// Test INSERT trigger
	_, err = db.Exec(`
		INSERT INTO test_items (item_code, name, quantity)
		VALUES ('ITEM001', 'Test Item', 5)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test item: %v", err)
	}

	// Verify that dirty row was created
	var pendingCount int
	var op string
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(MAX(op), '') FROM _sync_dirty_rows
		WHERE table_name='test_items' AND key_json=?
	`, fmt.Sprintf(`{"item_code":"%s"}`, "ITEM001")).Scan(&pendingCount, &op)
	if err != nil {
		t.Fatalf("Failed to query pending changes: %v", err)
	}
	if pendingCount != 1 {
		t.Errorf("Expected 1 pending change, got %d", pendingCount)
	}
	if op != "INSERT" {
		t.Errorf("Expected INSERT operation, got %s", op)
	}

	// Test UPDATE trigger
	_, err = db.Exec(`
		UPDATE test_items SET quantity = 10 WHERE item_code = 'ITEM001'
	`)
	if err != nil {
		t.Fatalf("Failed to update test item: %v", err)
	}

	// Verify that pending change was updated (should still be INSERT since it was a new row)
	err = db.QueryRow(`
		SELECT COUNT(*), COALESCE(MAX(op), '') FROM _sync_dirty_rows
		WHERE table_name='test_items' AND key_json=?
	`, fmt.Sprintf(`{"item_code":"%s"}`, "ITEM001")).Scan(&pendingCount, &op)
	if err != nil {
		t.Fatalf("Failed to query pending changes after update: %v", err)
	}
	if pendingCount != 1 {
		t.Errorf("Expected 1 pending change after update, got %d", pendingCount)
	}
	if op != "INSERT" {
		t.Errorf("Expected INSERT operation after update (should preserve INSERT), got %s", op)
	}

	// Test DELETE trigger
	_, err = db.Exec(`
		DELETE FROM test_items WHERE item_code = 'ITEM001'
	`)
	if err != nil {
		t.Fatalf("Failed to delete test item: %v", err)
	}

	// INSERT→DELETE remains queued as a delete until push collection removes the no-op.
	err = db.QueryRow(`
		SELECT COUNT(*) FROM _sync_dirty_rows
		WHERE table_name='test_items' AND key_json=?
	`, fmt.Sprintf(`{"item_code":"%s"}`, "ITEM001")).Scan(&pendingCount)
	if err != nil {
		t.Fatalf("Failed to query pending changes after delete: %v", err)
	}
	if pendingCount != 1 {
		t.Errorf("Expected 1 queued delete after local delete, got %d", pendingCount)
	}

	err = db.QueryRow(`
		SELECT COALESCE(MAX(op), '') FROM _sync_dirty_rows
		WHERE table_name='test_items' AND key_json=?
	`, fmt.Sprintf(`{"item_code":"%s"}`, "ITEM001")).Scan(&op)
	if err != nil {
		t.Fatalf("Failed to query queued delete after local delete: %v", err)
	}
	if op != "DELETE" {
		t.Errorf("Expected queued delete after local delete, got %s", op)
	}
}

func TestNewClient_FailsWhenSyncKeyColumnUsesIntegerPrimaryKey(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	tokenFunc := func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}
	_, err = NewClient(db, "http://localhost:8080", tokenFunc, DefaultConfig("main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
	}))
	if err == nil {
		t.Fatal("expected integer sync key setup to fail")
	}
	if !strings.Contains(err.Error(), "supports only TEXT keys and UUID-backed BLOB keys") {
		t.Fatalf("expected integer sync key error, got: %v", err)
	}
}

func TestNewClient_FailsWhenSyncKeyColumnUsesBigIntPrimaryKey(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	tokenFunc := func(ctx context.Context) (string, error) {
		return "mock-token", nil
	}
	_, err = NewClient(db, "http://localhost:8080", tokenFunc, DefaultConfig("main", []SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
	}))
	if err == nil {
		t.Fatal("expected BIGINT sync key setup to fail")
	}
	if !strings.Contains(err.Error(), "supports only TEXT keys and UUID-backed BLOB keys") {
		t.Fatalf("expected BIGINT sync key error, got: %v", err)
	}
}

// TestTemplateBasedTriggerGeneration tests that the template-based trigger generation produces correct SQL
func TestTemplateBasedTriggerGeneration(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with custom primary key
	_, err = db.Exec(`
		CREATE TABLE test_products (
			product_code TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			price REAL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Initialize database and create triggers
	if err := initializeDatabase(db); err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	syncTable := SyncTable{
		TableName:         "test_products",
		SyncKeyColumnName: "product_code",
	}

	err = createTriggersForTable(db, syncTable)
	if err != nil {
		t.Fatalf("Failed to create triggers: %v", err)
	}

	// Verify that triggers were created and contain the correct primary key column references
	var triggerSQL string
	err = db.QueryRow(`
		SELECT sql FROM sqlite_master
		WHERE type='trigger' AND name='trg_test_products_ai'
	`).Scan(&triggerSQL)
	if err != nil {
		t.Fatalf("Failed to query INSERT trigger SQL: %v", err)
	}

	// Check that the trigger uses the custom primary key column
	if !strings.Contains(triggerSQL, "NEW.product_code") {
		t.Errorf("INSERT trigger should contain 'NEW.product_code', but got: %s", triggerSQL)
	}
	if strings.Contains(triggerSQL, "NEW.id") {
		t.Errorf("INSERT trigger should not contain 'NEW.id' when using custom primary key, but got: %s", triggerSQL)
	}

	// Check UPDATE trigger
	err = db.QueryRow(`
		SELECT sql FROM sqlite_master
		WHERE type='trigger' AND name='trg_test_products_au'
	`).Scan(&triggerSQL)
	if err != nil {
		t.Fatalf("Failed to query UPDATE trigger SQL: %v", err)
	}

	if !strings.Contains(triggerSQL, "NEW.product_code") {
		t.Errorf("UPDATE trigger should contain 'NEW.product_code', but got: %s", triggerSQL)
	}

	// Check DELETE trigger
	err = db.QueryRow(`
		SELECT sql FROM sqlite_master
		WHERE type='trigger' AND name='trg_test_products_ad'
	`).Scan(&triggerSQL)
	if err != nil {
		t.Fatalf("Failed to query DELETE trigger SQL: %v", err)
	}

	if !strings.Contains(triggerSQL, "OLD.product_code") {
		t.Errorf("DELETE trigger should contain 'OLD.product_code', but got: %s", triggerSQL)
	}
}
