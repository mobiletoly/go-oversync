package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseVerifier provides direct PostgreSQL access for verification
type DatabaseVerifier struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewDatabaseVerifier creates a new database verifier
func NewDatabaseVerifier(databaseURL string, logger *slog.Logger) (*DatabaseVerifier, error) {
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DatabaseVerifier{
		pool:   pool,
		logger: logger,
	}, nil
}

// Close closes the database connection
func (v *DatabaseVerifier) Close() error {
	if v.pool != nil {
		v.pool.Close()
	}
	return nil
}

// CountRecords counts records in a specific table
func (v *DatabaseVerifier) CountRecords(ctx context.Context, tableName string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)

	err := v.pool.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records in %s: %w", tableName, err)
	}

	v.logger.Debug("Record count", "table", tableName, "count", count)
	return count, nil
}

// CountRows is a convenience wrapper over CountRecords.
func (v *DatabaseVerifier) CountRows(tableName string) (int, error) {
	return v.CountRecords(context.Background(), tableName)
}

// CountOrphanedReviews counts file reviews that don't have corresponding files
func (v *DatabaseVerifier) CountOrphanedReviews() (int, error) {
	var count int
	query := `
		SELECT COUNT(*)
		FROM business.file_reviews fr
		LEFT JOIN business.files f ON fr.file_id = f.id
		WHERE f.id IS NULL
	`

	err := v.pool.QueryRow(context.Background(), query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count orphaned reviews: %w", err)
	}

	v.logger.Debug("Orphaned reviews count", "count", count)
	return count, nil
}

// CountUserRecords counts records in a specific table for a specific user
func (v *DatabaseVerifier) CountUserRecords(ctx context.Context, tableName string, userID string) (int, error) {
	var count int

	// Query the authoritative bundle-era row state so we only count live rows that belong to this user.
	query := `
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_id = $1
		AND schema_name = $2
		AND table_name = $3
		AND deleted = FALSE`

	// Extract table name from full table name (e.g., "business.users" -> "users")
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid table name format: %s (expected schema.table)", tableName)
	}
	schemaName := parts[0]
	tableNameOnly := parts[1]

	err := v.pool.QueryRow(ctx, query, userID, schemaName, tableNameOnly).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count user records in %s for user %s: %w", tableName, userID, err)
	}

	v.logger.Debug("User record count", "table", tableName, "user_id", userID, "count", count)
	return count, nil
}

// CountActualBusinessRecords counts actual records in the business tables by querying sync metadata
func (v *DatabaseVerifier) CountActualBusinessRecords(ctx context.Context, tableName string, userID string) (int, error) {
	var count int

	// Extract table name from full table name (e.g., "business.users" -> "users")
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid table name format: %s (expected schema.table)", tableName)
	}
	schemaName := parts[0]
	tableNameOnly := parts[1]

	// Count records by joining business rows with authoritative bundle-era row ownership.
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT bt.id)
		FROM %s.%s bt
		INNER JOIN sync.row_state rs
			ON rs.key_json::jsonb ->> 'id' = bt.id::text
		WHERE rs.user_id = $1
		AND rs.schema_name = $2
		AND rs.table_name = $3
		AND rs.deleted = FALSE`, schemaName, tableNameOnly)

	err := v.pool.QueryRow(ctx, query, userID, schemaName, tableNameOnly).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count actual business records in %s for user %s: %w", tableName, userID, err)
	}

	v.logger.Debug("Actual business record count", "table", tableName, "user_id", userID, "count", count)
	return count, nil
}

// VerifyBusinessRecordsExist verifies that specific records exist in business tables for a user
func (v *DatabaseVerifier) VerifyBusinessRecordsExist(ctx context.Context, tableName string, userID string, recordIDs []string) error {
	if len(recordIDs) == 0 {
		return nil
	}

	// Extract table name from full table name (e.g., "business.users" -> "users")
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("invalid table name format: %s (expected schema.table)", tableName)
	}
	schemaName := parts[0]
	tableNameOnly := parts[1]

	// Check each record exists by joining with authoritative bundle-era row ownership.
	for _, recordID := range recordIDs {
		var exists bool
		query := fmt.Sprintf(`
			SELECT EXISTS(
				SELECT 1 FROM %s.%s bt
				INNER JOIN sync.row_state rs
					ON rs.key_json::jsonb ->> 'id' = bt.id::text
				WHERE rs.user_id = $1
				AND bt.id = $2
				AND rs.schema_name = $3
				AND rs.table_name = $4
				AND rs.deleted = FALSE
			)`, schemaName, tableNameOnly)

		err := v.pool.QueryRow(ctx, query, userID, recordID, schemaName, tableNameOnly).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check if record %s exists in %s: %w", recordID, tableName, err)
		}

		if !exists {
			return fmt.Errorf("record %s does not exist in %s for user %s", recordID, tableName, userID)
		}
	}

	v.logger.Debug("Business records verified", "table", tableName, "user_id", userID, "records_checked", len(recordIDs))
	return nil
}

// CountSyncChanges counts committed row effects for a specific user.
func (v *DatabaseVerifier) CountSyncChanges(ctx context.Context, userID string) (int, error) {
	var count int
	query := "SELECT COUNT(*) FROM sync.bundle_rows WHERE user_id = $1"

	err := v.pool.QueryRow(ctx, query, userID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count sync changes for user %s: %w", userID, err)
	}

	v.logger.Debug("Sync changes count", "user_id", userID, "count", count)
	return count, nil
}

// GetSyncMetadata retrieves sync metadata for a user
func (v *DatabaseVerifier) GetSyncMetadata(ctx context.Context, userID string) (*SyncMetadata, error) {
	metadata := &SyncMetadata{
		UserID: userID,
	}

	// Count committed row effects.
	changeCount, err := v.CountSyncChanges(ctx, userID)
	if err != nil {
		return nil, err
	}
	metadata.TotalChanges = changeCount

	// Get highest authoritative per-user bundle sequence.
	var maxSeq int64
	query := `
		SELECT COALESCE((SELECT next_bundle_seq - 1 FROM sync.user_state WHERE user_id = $1), 0)`
	err = v.pool.QueryRow(ctx, query, userID).Scan(&maxSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to get max bundle sequence for user %s: %w", userID, err)
	}
	metadata.HighestSequence = maxSeq

	// Count by operation type in bundle rows.
	opCounts := make(map[string]int)
	query = "SELECT op, COUNT(*) FROM sync.bundle_rows WHERE user_id = $1 GROUP BY op"
	rows, err := v.pool.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get operation counts for user %s: %w", userID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var op string
		var count int
		if err := rows.Scan(&op, &count); err != nil {
			return nil, fmt.Errorf("failed to scan operation count: %w", err)
		}
		opCounts[op] = count
	}

	metadata.OperationCounts = opCounts

	v.logger.Debug("Sync metadata",
		"user_id", userID,
		"total_changes", metadata.TotalChanges,
		"highest_sequence", metadata.HighestSequence,
		"operations", metadata.OperationCounts)

	return metadata, nil
}

// CountRecordsWithQuery executes a custom query and returns the count result
func (v *DatabaseVerifier) CountRecordsWithQuery(ctx context.Context, query string) (int, error) {
	var count int
	err := v.pool.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}
	return count, nil
}

// VerifyDataConsistency checks if client and server data are consistent
func (v *DatabaseVerifier) VerifyDataConsistency(ctx context.Context, userID string, expectedRecords map[string]int) error {
	v.logger.Info("🔍 Verifying data consistency", "user_id", userID)

	for tableName, expectedCount := range expectedRecords {
		actualCount, err := v.CountRecords(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to verify table %s: %w", tableName, err)
		}

		if actualCount < expectedCount {
			return fmt.Errorf("table %s: expected at least %d records, got %d",
				tableName, expectedCount, actualCount)
		}

		v.logger.Info("✅ Table verification passed",
			"table", tableName,
			"expected", expectedCount,
			"actual", actualCount)
	}

	return nil
}

// GetConflicts retrieves any conflict records
func (v *DatabaseVerifier) GetConflicts(ctx context.Context, userID string) ([]ConflictRecord, error) {
	// This would query for conflict records if they exist
	// For now, return empty slice
	return []ConflictRecord{}, nil
}

// SyncMetadata holds sync-related metadata for verification
type SyncMetadata struct {
	UserID          string         `json:"user_id"`
	TotalChanges    int            `json:"total_changes"`
	HighestSequence int64          `json:"highest_sequence"`
	OperationCounts map[string]int `json:"operation_counts"`
}

// ConflictRecord represents a conflict that occurred during sync
type ConflictRecord struct {
	TableName    string `json:"table_name"`
	PK           string `json:"pk"`
	ConflictType string `json:"conflict_type"`
	ResolvedAt   string `json:"resolved_at"`
}

// UserVerificationReport holds comprehensive verification results for a single user
type UserVerificationReport struct {
	UserID             string `json:"user_id"`
	ScenarioName       string `json:"scenario_name"`
	SQLiteDatabasePath string `json:"sqlite_database_path"`

	// PostgreSQL verification
	PostgreSQLResults PostgreSQLResults `json:"postgresql_results"`

	// SQLite verification
	SQLiteResults SQLiteResults `json:"sqlite_results"`

	// Overall status
	VerificationPassed bool     `json:"verification_passed"`
	Errors             []string `json:"errors"`
	Duration           string   `json:"duration"`
}

// PostgreSQLResults holds PostgreSQL verification results
type PostgreSQLResults struct {
	BusinessTableCounts map[string]int            `json:"business_table_counts"`
	SyncMetadataCounts  map[string]int            `json:"sync_metadata_counts"`
	TotalSyncChanges    int                       `json:"total_sync_changes"`
	OperationCounts     map[string]int            `json:"operation_counts"`
	HighestSequence     int64                     `json:"highest_sequence"`
	RecordSamples       map[string][]RecordSample `json:"record_samples"`
}

// SQLiteResults holds SQLite verification results
type SQLiteResults struct {
	TableCounts    map[string]int            `json:"table_counts"`
	SyncMetaCount  int                       `json:"sync_meta_count"`
	PendingChanges int                       `json:"pending_changes"`
	RecordSamples  map[string][]RecordSample `json:"record_samples"`
}

// RecordSample holds a sample record for verification
type RecordSample struct {
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	CreatedAt string                 `json:"created_at,omitempty"`
	UpdatedAt string                 `json:"updated_at,omitempty"`
}

// GeneratePostgreSQLReport generates detailed PostgreSQL verification results
func (v *DatabaseVerifier) GeneratePostgreSQLReport(ctx context.Context, userID string, expectedCounts map[string]int) (*PostgreSQLResults, error) {
	results := &PostgreSQLResults{
		BusinessTableCounts: make(map[string]int),
		SyncMetadataCounts:  make(map[string]int),
		OperationCounts:     make(map[string]int),
		RecordSamples:       make(map[string][]RecordSample),
	}

	// Get business table counts
	for tableName := range expectedCounts {
		actualCount, err := v.CountActualBusinessRecords(ctx, tableName, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to count business records in %s: %w", tableName, err)
		}
		results.BusinessTableCounts[tableName] = actualCount

		syncCount, err := v.CountUserRecords(ctx, tableName, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to count sync records in %s: %w", tableName, err)
		}
		results.SyncMetadataCounts[tableName] = syncCount
	}

	// Get sync metadata
	syncMetadata, err := v.GetSyncMetadata(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync metadata: %w", err)
	}

	results.TotalSyncChanges = syncMetadata.TotalChanges
	results.HighestSequence = syncMetadata.HighestSequence
	results.OperationCounts = syncMetadata.OperationCounts

	return results, nil
}

// ComprehensiveVerificationResult holds the results of comprehensive verification
type ComprehensiveVerificationResult struct {
	UserID              string         `json:"user_id"`
	BusinessTableCounts map[string]int `json:"business_table_counts"`
	SyncMetadataCounts  map[string]int `json:"sync_metadata_counts"`
	TotalSyncChanges    int            `json:"total_sync_changes"`
	OperationCounts     map[string]int `json:"operation_counts"`
	MissingRecords      []string       `json:"missing_records"`
	UnexpectedRecords   []string       `json:"unexpected_records"`
	VerificationPassed  bool           `json:"verification_passed"`
	Errors              []string       `json:"errors"`
}

// PerformComprehensiveVerification performs thorough verification of both business tables and sync metadata
func (v *DatabaseVerifier) PerformComprehensiveVerification(ctx context.Context, userID string, expectedData map[string][]string) (*ComprehensiveVerificationResult, error) {
	result := &ComprehensiveVerificationResult{
		UserID:              userID,
		BusinessTableCounts: make(map[string]int),
		SyncMetadataCounts:  make(map[string]int),
		OperationCounts:     make(map[string]int),
		MissingRecords:      []string{},
		UnexpectedRecords:   []string{},
		VerificationPassed:  true,
		Errors:              []string{},
	}

	v.logger.Info("🔍 Starting comprehensive verification", "user_id", userID)

	// Verify business table counts and record existence
	for tableName, expectedRecordIDs := range expectedData {
		// Count actual business records
		actualCount, err := v.CountActualBusinessRecords(ctx, tableName, userID)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to count business records in %s: %v", tableName, err))
			result.VerificationPassed = false
			continue
		}
		result.BusinessTableCounts[tableName] = actualCount

		// Count sync metadata records
		syncCount, err := v.CountUserRecords(ctx, tableName, userID)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to count sync records in %s: %v", tableName, err))
			result.VerificationPassed = false
			continue
		}
		result.SyncMetadataCounts[tableName] = syncCount

		// Verify expected count matches
		expectedCount := len(expectedRecordIDs)
		if actualCount != expectedCount {
			result.Errors = append(result.Errors, fmt.Sprintf("business table %s count mismatch: expected %d, got %d", tableName, expectedCount, actualCount))
			result.VerificationPassed = false
		}

		// Verify specific records exist
		if err := v.VerifyBusinessRecordsExist(ctx, tableName, userID, expectedRecordIDs); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("business record verification failed for %s: %v", tableName, err))
			result.VerificationPassed = false
		}

		v.logger.Info("📊 Table verification",
			"table", tableName,
			"business_count", actualCount,
			"sync_count", syncCount,
			"expected", expectedCount)
	}

	// Get sync metadata
	syncMetadata, err := v.GetSyncMetadata(ctx, userID)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to get sync metadata: %v", err))
		result.VerificationPassed = false
	} else {
		result.TotalSyncChanges = syncMetadata.TotalChanges
		result.OperationCounts = syncMetadata.OperationCounts
	}

	if result.VerificationPassed {
		v.logger.Info("✅ Comprehensive verification passed", "user_id", userID)
	} else {
		v.logger.Error("❌ Comprehensive verification failed", "user_id", userID, "errors", len(result.Errors))
	}

	return result, nil
}
