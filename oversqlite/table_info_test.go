package oversqlite

import (
	"database/sql"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestColumnInfo_IsBlob tests the IsBlob method of ColumnInfo
func TestColumnInfo_IsBlob(t *testing.T) {
	tests := []struct {
		name         string
		declaredType string
		expected     bool
	}{
		{"BLOB type", "BLOB", true},
		{"blob lowercase", "blob", true},
		{"Blob mixed case", "Blob", true},
		{"BYTEA type", "BYTEA", false},
		{"TEXT type", "TEXT", false},
		{"INTEGER type", "INTEGER", false},
		{"REAL type", "REAL", false},
		{"VARCHAR with blob", "VARCHAR(255) blob", true},
		{"Empty type", "", false},
		{"NULL type", "NULL", false},
		{"Complex type with BLOB", "CUSTOM_BLOB_TYPE", true},
		{"Type ending with blob", "MYBLOB", true},
		{"Type starting with blob", "blobdata", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &ColumnInfo{
				DeclaredType: tt.declaredType,
			}
			result := col.IsBlob()
			if result != tt.expected {
				t.Errorf("IsBlob() = %v, expected %v for type %q", result, tt.expected, tt.declaredType)
			}
		})
	}
}

// TestNewTableInfoProvider tests the creation of a new TableInfoProvider
func TestNewTableInfoProvider(t *testing.T) {
	provider := NewTableInfoProvider()

	if provider == nil {
		t.Fatal("NewTableInfoProvider() returned nil")
	}

	if provider.cache == nil {
		t.Error("NewTableInfoProvider() did not initialize cache")
	}

	if len(provider.cache) != 0 {
		t.Error("NewTableInfoProvider() cache should be empty initially")
	}
}

// TestTableInfoProvider_ClearCache tests the cache clearing functionality
func TestTableInfoProvider_ClearCache(t *testing.T) {
	provider := NewTableInfoProvider()

	// Add some dummy data to cache
	provider.cache["test_table"] = &TableInfo{Table: "test_table"}

	if len(provider.cache) != 1 {
		t.Error("Cache should have 1 entry before clearing")
	}

	provider.ClearCache()

	if len(provider.cache) != 0 {
		t.Error("Cache should be empty after clearing")
	}
}

// TestTableInfoProvider_Get tests the Get method with various table structures
func TestTableInfoProvider_Get(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test tables with different column types
	testTables := []struct {
		name   string
		schema string
	}{
		{
			name: "simple_table",
			schema: `CREATE TABLE simple_table (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age INTEGER
			)`,
		},
		{
			name: "blob_table",
			schema: `CREATE TABLE blob_table (
				id BLOB PRIMARY KEY,
				data BLOB NOT NULL,
				metadata TEXT,
				size INTEGER DEFAULT 0
			)`,
		},
		{
			name: "mixed_table",
			schema: `CREATE TABLE mixed_table (
				uuid_id BLOB PRIMARY KEY,
				title TEXT NOT NULL,
				content BLOB,
				created_at INTEGER,
				is_active INTEGER DEFAULT 1
			)`,
		},
		{
			name: "no_pk_table",
			schema: `CREATE TABLE no_pk_table (
				name TEXT,
				value INTEGER
			)`,
		},
	}

	// Create all test tables
	for _, table := range testTables {
		_, err := db.Exec(table.schema)
		if err != nil {
			t.Fatalf("Failed to create table %s: %v", table.name, err)
		}
	}

	provider := NewTableInfoProvider()

	// Test simple_table
	t.Run("simple_table", func(t *testing.T) {
		info, err := provider.Get(db, "simple_table")
		if err != nil {
			t.Fatalf("Failed to get table info: %v", err)
		}

		if info.Table != "simple_table" {
			t.Errorf("Expected table name 'simple_table', got %q", info.Table)
		}

		if len(info.Columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(info.Columns))
		}

		// Check primary key
		if info.PrimaryKey == nil {
			t.Fatal("Expected primary key to be found")
		}
		if info.PrimaryKey.Name != "id" {
			t.Errorf("Expected primary key name 'id', got %q", info.PrimaryKey.Name)
		}
		if info.PrimaryKeyIsBlob {
			t.Error("Expected primary key to not be BLOB")
		}

		// Check column types
		expectedTypes := map[string]string{
			"id":   "INTEGER",
			"name": "TEXT",
			"age":  "INTEGER",
		}
		for colName, expectedType := range expectedTypes {
			if actualType, exists := info.TypesByNameLower[colName]; !exists {
				t.Errorf("Column %q not found in TypesByNameLower", colName)
			} else if actualType != expectedType {
				t.Errorf("Expected type %q for column %q, got %q", expectedType, colName, actualType)
			}
		}

		// Check BLOB detection
		for _, col := range info.Columns {
			if col.Name == "id" || col.Name == "name" || col.Name == "age" {
				if col.IsBlob() {
					t.Errorf("Column %q should not be detected as BLOB", col.Name)
				}
			}
		}
	})

	// Test blob_table
	t.Run("blob_table", func(t *testing.T) {
		info, err := provider.Get(db, "blob_table")
		if err != nil {
			t.Fatalf("Failed to get table info: %v", err)
		}

		if len(info.Columns) != 4 {
			t.Errorf("Expected 4 columns, got %d", len(info.Columns))
		}

		// Check primary key is BLOB
		if info.PrimaryKey == nil {
			t.Fatal("Expected primary key to be found")
		}
		if info.PrimaryKey.Name != "id" {
			t.Errorf("Expected primary key name 'id', got %q", info.PrimaryKey.Name)
		}
		if !info.PrimaryKeyIsBlob {
			t.Error("Expected primary key to be BLOB")
		}

		// Check BLOB columns
		blobColumns := []string{"id", "data"}
		nonBlobColumns := []string{"metadata", "size"}

		for _, col := range info.Columns {
			isExpectedBlob := false
			for _, blobCol := range blobColumns {
				if col.Name == blobCol {
					isExpectedBlob = true
					break
				}
			}

			if isExpectedBlob && !col.IsBlob() {
				t.Errorf("Column %q should be detected as BLOB", col.Name)
			}

			isExpectedNonBlob := false
			for _, nonBlobCol := range nonBlobColumns {
				if col.Name == nonBlobCol {
					isExpectedNonBlob = true
					break
				}
			}

			if isExpectedNonBlob && col.IsBlob() {
				t.Errorf("Column %q should not be detected as BLOB", col.Name)
			}
		}

		// Check default values
		var sizeColumn *ColumnInfo
		for _, col := range info.Columns {
			if col.Name == "size" {
				sizeColumn = &col
				break
			}
		}
		if sizeColumn == nil {
			t.Fatal("Size column not found")
		}
		if sizeColumn.DefaultValue == nil {
			t.Error("Expected size column to have default value")
		} else if *sizeColumn.DefaultValue != "0" {
			t.Errorf("Expected default value '0', got %q", *sizeColumn.DefaultValue)
		}
	})

	// Test no_pk_table
	t.Run("no_pk_table", func(t *testing.T) {
		info, err := provider.Get(db, "no_pk_table")
		if err != nil {
			t.Fatalf("Failed to get table info: %v", err)
		}

		if info.PrimaryKey != nil {
			t.Error("Expected no primary key to be found")
		}
		if info.PrimaryKeyIsBlob {
			t.Error("Expected PrimaryKeyIsBlob to be false when no primary key")
		}
	})
}

// TestTableInfoProvider_Caching tests that the provider properly caches results
func TestTableInfoProvider_Caching(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE cache_test (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	provider := NewTableInfoProvider()

	// First call should populate cache
	info1, err := provider.Get(db, "cache_test")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	// Check cache was populated
	if len(provider.cache) != 1 {
		t.Errorf("Expected cache to have 1 entry, got %d", len(provider.cache))
	}

	// Second call should use cache (same pointer)
	info2, err := provider.Get(db, "cache_test")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	// Should be the same object (from cache)
	if info1 != info2 {
		t.Error("Expected same object from cache, got different objects")
	}

	// Test case insensitive caching
	info3, err := provider.Get(db, "CACHE_TEST")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	if info1 != info3 {
		t.Error("Expected same object from cache for case-insensitive lookup")
	}
}

// TestTableInfoProvider_ConcurrentAccess tests concurrent access to the provider
func TestTableInfoProvider_ConcurrentAccess(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE concurrent_test (
		id INTEGER PRIMARY KEY,
		data BLOB,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	provider := NewTableInfoProvider()
	const numGoroutines = 10
	const numIterations = 5

	var wg sync.WaitGroup
	results := make([]*TableInfo, numGoroutines*numIterations)
	errors := make([]error, numGoroutines*numIterations)

	// Launch multiple goroutines that access the same table info
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				idx := goroutineID*numIterations + j
				info, err := provider.Get(db, "concurrent_test")
				results[idx] = info
				errors[idx] = err
			}
		}(i)
	}

	wg.Wait()

	// Check that all calls succeeded
	for i, err := range errors {
		if err != nil {
			t.Errorf("Goroutine call %d failed: %v", i, err)
		}
	}

	// Check that all results are the same object (from cache)
	firstResult := results[0]
	if firstResult == nil {
		t.Fatal("First result is nil")
	}

	for i, result := range results {
		if result != firstResult {
			t.Errorf("Result %d is different object than first result", i)
		}
	}

	// Verify cache has only one entry
	if len(provider.cache) != 1 {
		t.Errorf("Expected cache to have 1 entry, got %d", len(provider.cache))
	}
}

// TestTableInfoProvider_ErrorHandling tests error handling for invalid tables
func TestTableInfoProvider_ErrorHandling(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	provider := NewTableInfoProvider()

	// Test non-existent table (SQLite returns empty result set, not error)
	t.Run("non_existent_table", func(t *testing.T) {
		info, err := provider.Get(db, "non_existent_table")
		if err != nil {
			t.Errorf("Unexpected error for non-existent table: %v", err)
		}
		if info == nil {
			t.Error("Expected info object even for non-existent table")
		}
		// For non-existent tables, SQLite returns empty result set
		if info != nil && len(info.Columns) != 0 {
			t.Errorf("Expected 0 columns for non-existent table, got %d", len(info.Columns))
		}
		if info != nil && info.PrimaryKey != nil {
			t.Error("Expected no primary key for non-existent table")
		}
	})

	// Test with closed database
	t.Run("closed_database", func(t *testing.T) {
		closedDB, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		closedDB.Close()

		info, err := provider.Get(closedDB, "any_table")
		if err == nil {
			t.Error("Expected error for closed database")
		}
		if info != nil {
			t.Error("Expected nil info for closed database")
		}
	})
}

// TestGlobalTableInfoProvider tests the global convenience function
func TestGlobalTableInfoProvider(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE global_test (
		id BLOB PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test global function
	info, err := GetTableInfo(db, "global_test")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	if info.Table != "global_test" {
		t.Errorf("Expected table name 'global_test', got %q", info.Table)
	}

	if !info.PrimaryKeyIsBlob {
		t.Error("Expected primary key to be BLOB")
	}

	// Test that it uses the same global provider (caching works)
	info2, err := GetTableInfo(db, "global_test")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	if info != info2 {
		t.Error("Expected same object from global provider cache")
	}
}

// TestGlobalTableInfoCacheClear tests that the global cache clearing works correctly
func TestGlobalTableInfoCacheClear(t *testing.T) {
	// Clear the global cache first to ensure clean state
	ClearGlobalTableInfoCache()

	// Create first in-memory SQLite database
	db1, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open first database: %v", err)
	}
	defer db1.Close()

	// Create test table in first database
	_, err = db1.Exec(`CREATE TABLE cache_clear_test (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table in first database: %v", err)
	}

	// Get table info from first database (should populate cache)
	info1, err := GetTableInfo(db1, "cache_clear_test")
	if err != nil {
		t.Fatalf("Failed to get table info from first database: %v", err)
	}

	// Verify cache is populated
	if len(globalTableInfoProvider.cache) != 1 {
		t.Errorf("Expected cache to have 1 entry, got %d", len(globalTableInfoProvider.cache))
	}

	// Clear the global cache
	ClearGlobalTableInfoCache()

	// Verify cache is empty
	if len(globalTableInfoProvider.cache) != 0 {
		t.Errorf("Expected cache to be empty after clearing, got %d entries", len(globalTableInfoProvider.cache))
	}

	// Create second in-memory SQLite database with different schema
	db2, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open second database: %v", err)
	}
	defer db2.Close()

	// Create table with same name but different schema in second database
	_, err = db2.Exec(`CREATE TABLE cache_clear_test (
		id BLOB PRIMARY KEY,
		description TEXT,
		value INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table in second database: %v", err)
	}

	// Get table info from second database (should not use cached data from first database)
	info2, err := GetTableInfo(db2, "cache_clear_test")
	if err != nil {
		t.Fatalf("Failed to get table info from second database: %v", err)
	}

	// Verify that the table info is different (different schema)
	if len(info1.Columns) == len(info2.Columns) {
		// Check if primary key types are different
		if info1.PrimaryKeyIsBlob == info2.PrimaryKeyIsBlob {
			t.Error("Expected different table schemas, but got same primary key type")
		}
	}

	// Verify second database has BLOB primary key
	if !info2.PrimaryKeyIsBlob {
		t.Error("Expected second database to have BLOB primary key")
	}

	// Verify first database had INTEGER primary key
	if info1.PrimaryKeyIsBlob {
		t.Error("Expected first database to have INTEGER primary key")
	}
}

// TestTableInfo_ColumnNamesLower tests that column names are properly lowercased
func TestTableInfo_ColumnNamesLower(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table with mixed case column names
	_, err = db.Exec(`CREATE TABLE MixedCase (
		ID INTEGER PRIMARY KEY,
		UserName TEXT,
		EMAIL_ADDRESS TEXT,
		created_at INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	provider := NewTableInfoProvider()
	info, err := provider.Get(db, "MixedCase")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	expectedLowerNames := []string{"id", "username", "email_address", "created_at"}
	if len(info.ColumnNamesLower) != len(expectedLowerNames) {
		t.Errorf("Expected %d column names, got %d", len(expectedLowerNames), len(info.ColumnNamesLower))
	}

	for i, expected := range expectedLowerNames {
		if i >= len(info.ColumnNamesLower) {
			t.Errorf("Missing column name at index %d", i)
			continue
		}
		if info.ColumnNamesLower[i] != expected {
			t.Errorf("Expected column name %q at index %d, got %q", expected, i, info.ColumnNamesLower[i])
		}
	}

	// Test TypesByNameLower mapping
	expectedTypes := map[string]string{
		"id":            "INTEGER",
		"username":      "TEXT",
		"email_address": "TEXT",
		"created_at":    "INTEGER",
	}

	for colName, expectedType := range expectedTypes {
		if actualType, exists := info.TypesByNameLower[colName]; !exists {
			t.Errorf("Column %q not found in TypesByNameLower", colName)
		} else if actualType != expectedType {
			t.Errorf("Expected type %q for column %q, got %q", expectedType, colName, actualType)
		}
	}
}
