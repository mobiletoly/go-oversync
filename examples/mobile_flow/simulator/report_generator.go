package simulator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestReport holds the complete test report for all users
type TestReport struct {
	TestName        string                   `json:"test_name"`
	ScenarioName    string                   `json:"scenario_name"`
	TotalUsers      int                      `json:"total_users"`
	SuccessfulUsers int                      `json:"successful_users"`
	FailedUsers     int                      `json:"failed_users"`
	TotalDuration   string                   `json:"total_duration"`
	AverageUserTime string                   `json:"average_user_time"`
	GeneratedAt     string                   `json:"generated_at"`
	UserReports     []UserVerificationReport `json:"user_reports"`
	Summary         TestSummary              `json:"summary"`
}

// TestSummary holds aggregate statistics
type TestSummary struct {
	PostgreSQLTotals map[string]int `json:"postgresql_totals"`
	SQLiteTotals     map[string]int `json:"sqlite_totals"`
	TotalRecords     int            `json:"total_records"`
	TotalSyncChanges int            `json:"total_sync_changes"`
	DatabasePaths    []string       `json:"database_paths"`
	CommonErrors     []string       `json:"common_errors"`
}

// ReportGenerator generates comprehensive test reports
type ReportGenerator struct {
	verifier *DatabaseVerifier
}

// NewReportGenerator creates a new report generator
func NewReportGenerator(verifier *DatabaseVerifier) *ReportGenerator {
	return &ReportGenerator{
		verifier: verifier,
	}
}

// GenerateUserReport generates a comprehensive report for a single user
func (r *ReportGenerator) GenerateUserReport(ctx context.Context, userID, scenarioName, sqliteDBPath string, expectedCounts map[string]int, duration time.Duration) (*UserVerificationReport, error) {
	report := &UserVerificationReport{
		UserID:             userID,
		ScenarioName:       scenarioName,
		SQLiteDatabasePath: sqliteDBPath,
		VerificationPassed: true,
		Errors:             []string{},
		Duration:           duration.String(),
	}

	// Generate PostgreSQL report
	if r.verifier != nil {
		pgResults, err := r.verifier.GeneratePostgreSQLReport(ctx, userID, expectedCounts)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("PostgreSQL verification failed: %v", err))
			report.VerificationPassed = false
		} else {
			report.PostgreSQLResults = *pgResults
		}
	}

	// Generate SQLite report
	sqliteResults, err := r.generateSQLiteReport(sqliteDBPath, expectedCounts)
	if err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("SQLite verification failed: %v", err))
		report.VerificationPassed = false
	} else {
		report.SQLiteResults = *sqliteResults
	}

	return report, nil
}

// generateSQLiteReport generates SQLite verification results
func (r *ReportGenerator) generateSQLiteReport(dbPath string, expectedCounts map[string]int) (*SQLiteResults, error) {
	results := &SQLiteResults{
		TableCounts:   make(map[string]int),
		RecordSamples: make(map[string][]RecordSample),
	}

	// Check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("SQLite database not found: %s", dbPath)
	}

	// Open SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}
	defer db.Close()

	// Count records in each table
	for tableName := range expectedCounts {
		// Extract table name (remove schema prefix)
		parts := strings.Split(tableName, ".")
		tableNameOnly := parts[len(parts)-1]

		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableNameOnly)
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			return nil, fmt.Errorf("failed to count records in %s: %w", tableNameOnly, err)
		}
		results.TableCounts[tableNameOnly] = count

		// Get sample records
		samples, err := r.getSQLiteSampleRecords(db, tableNameOnly, 3)
		if err != nil {
			// Log warning but don't fail
			continue
		}
		results.RecordSamples[tableNameOnly] = samples
	}

	// Count sync metadata
	var syncMetaCount int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_row_meta").Scan(&syncMetaCount)
	if err != nil {
		// Sync metadata table might not exist in some scenarios
		syncMetaCount = 0
	}
	results.SyncMetaCount = syncMetaCount

	// Count pending changes
	var pendingCount int
	err = db.QueryRow("SELECT COUNT(*) FROM _sync_pending_changes").Scan(&pendingCount)
	if err != nil {
		// Pending changes table might not exist
		pendingCount = 0
	}
	results.PendingChanges = pendingCount

	return results, nil
}

// getSQLiteSampleRecords gets sample records from SQLite table
func (r *ReportGenerator) getSQLiteSampleRecords(db *sql.DB, tableName string, limit int) ([]RecordSample, error) {
	query := fmt.Sprintf("SELECT id, name FROM %s ORDER BY rowid DESC LIMIT ?", tableName)

	rows, err := db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query sample records: %w", err)
	}
	defer rows.Close()

	var samples []RecordSample
	for rows.Next() {
		var sample RecordSample
		var name string

		err := rows.Scan(&sample.ID, &name)
		if err != nil {
			return nil, fmt.Errorf("failed to scan sample record: %w", err)
		}

		sample.Data = map[string]interface{}{
			"name": name,
		}

		samples = append(samples, sample)
	}

	return samples, nil
}

// GenerateCompleteReport generates a complete test report for all users
func (r *ReportGenerator) GenerateCompleteReport(testName, scenarioName string, userReports []UserVerificationReport, totalDuration time.Duration) *TestReport {
	report := &TestReport{
		TestName:      testName,
		ScenarioName:  scenarioName,
		TotalUsers:    len(userReports),
		TotalDuration: totalDuration.String(),
		GeneratedAt:   time.Now().Format(time.RFC3339),
		UserReports:   userReports,
	}

	// Calculate statistics
	successfulUsers := 0
	var totalUserDuration time.Duration
	pgTotals := make(map[string]int)
	sqliteTotals := make(map[string]int)
	var dbPaths []string
	var commonErrors []string

	for _, userReport := range userReports {
		if userReport.VerificationPassed {
			successfulUsers++
		}

		// Parse user duration
		if userDuration, err := time.ParseDuration(userReport.Duration); err == nil {
			totalUserDuration += userDuration
		}

		// Aggregate PostgreSQL totals
		for table, count := range userReport.PostgreSQLResults.BusinessTableCounts {
			pgTotals[table] += count
		}

		// Aggregate SQLite totals
		for table, count := range userReport.SQLiteResults.TableCounts {
			sqliteTotals[table] += count
		}

		// Collect database paths
		if userReport.SQLiteDatabasePath != "" {
			dbPaths = append(dbPaths, userReport.SQLiteDatabasePath)
		}

		// Collect errors
		commonErrors = append(commonErrors, userReport.Errors...)
	}

	report.SuccessfulUsers = successfulUsers
	report.FailedUsers = report.TotalUsers - successfulUsers

	if report.TotalUsers > 0 {
		avgDuration := totalUserDuration / time.Duration(report.TotalUsers)
		report.AverageUserTime = avgDuration.String()
	}

	// Calculate total records
	totalRecords := 0
	for _, count := range pgTotals {
		totalRecords += count
	}

	report.Summary = TestSummary{
		PostgreSQLTotals: pgTotals,
		SQLiteTotals:     sqliteTotals,
		TotalRecords:     totalRecords,
		DatabasePaths:    dbPaths,
		CommonErrors:     commonErrors,
	}

	return report
}

// SaveReportToFile saves the test report to a JSON file
func (r *ReportGenerator) SaveReportToFile(report *TestReport, filename string) error {
	// Create reports directory if it doesn't exist
	reportsDir := "test_reports"
	if err := os.MkdirAll(reportsDir, 0755); err != nil {
		return fmt.Errorf("failed to create reports directory: %w", err)
	}

	// Generate filename with timestamp if not provided
	if filename == "" {
		timestamp := time.Now().Format("20060102_150405")
		filename = fmt.Sprintf("%s_%s_%dusers_%s.json",
			report.ScenarioName,
			report.TestName,
			report.TotalUsers,
			timestamp)
	}

	filePath := filepath.Join(reportsDir, filename)

	// Marshal to JSON with indentation
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report to JSON: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write report to file: %w", err)
	}

	fmt.Printf("📊 Test report saved to: %s\n", filePath)
	return nil
}
