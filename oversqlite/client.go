// Package oversqlite provides a SQLite-based client service for go-oversync
// single-user multi-device synchronization.
//
// This package implements the client-side service according to the technical
// specification in docs/client_techreq/01_client_sqlite.md
// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// Client manages SQLite database and two-way sync operations
type Client struct {
	DB       *sql.DB
	BaseURL  string
	Token    func(context.Context) (string, error) // returns JWT
	SourceID string
	UserID   string
	Resolver Resolver
	HTTP     *http.Client
	config   *Config
	logger   *slog.Logger
	writeMu  sync.Mutex // Serialize write operations to prevent SQLite locking issues

	// Pause switches (atomic): allow callers to suspend sync activity deterministically
	uploadPaused   int32
	downloadPaused int32
}

// SyncTable represents a table configuration for synchronization
type SyncTable struct {
	TableName         string // Table name (e.g., "users", "posts")
	SyncKeyColumnName string // Primary key column name (empty string defaults to "id")
}

// Config holds configuration for the SQLite sync client
type Config struct {
	Schema        string        // e.g., "app"
	Tables        []SyncTable   // Table configurations with optional custom primary key columns
	UploadLimit   int           // e.g., 200 per batch
	DownloadLimit int           // e.g., 1000
	BackoffMin    time.Duration // 1s
	BackoffMax    time.Duration // 60s
}

// Resolver interface for conflict resolution
type Resolver interface {
	// Merge returns merged JSON to store locally & attempt to upload as UPDATE,
	// or (nil, keepLocal bool=false) to accept server and drop local pending.
	Merge(table string, pk string, server json.RawMessage, local json.RawMessage) (merged json.RawMessage, keepLocal bool, err error)
}

// DefaultConfig returns a default configuration for the specified tables.
// Schema must be provided explicitly by the caller to avoid business-specific defaults.
// If SyncKeyColumnName is empty string, it defaults to "id".
func DefaultConfig(schema string, tables []SyncTable) *Config {
	return &Config{
		Schema:        schema,
		Tables:        tables,
		UploadLimit:   200,
		DownloadLimit: 1000,
		BackoffMin:    1 * time.Second,
		BackoffMax:    60 * time.Second,
	}
}

// PauseUploads suspends upload operations (UploadOnce and background loops respect this flag)
func (c *Client) PauseUploads() { atomic.StoreInt32(&c.uploadPaused, 1) }

// ResumeUploads resumes upload operations
func (c *Client) ResumeUploads() { atomic.StoreInt32(&c.uploadPaused, 0) }

// PauseDownloads suspends download operations (DownloadOnce/Hydrate and background loops respect this flag)
func (c *Client) PauseDownloads() { atomic.StoreInt32(&c.downloadPaused, 1) }

// ResumeDownloads resumes download operations
func (c *Client) ResumeDownloads() { atomic.StoreInt32(&c.downloadPaused, 0) }

// NewClient creates a new SQLite sync client with the specified configuration
// Automatically creates triggers for all tables specified in the config
func NewClient(db *sql.DB, baseURL, userID, sourceID string, tok func(ctx context.Context) (string, error), config *Config) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.Schema == "" {
		return nil, fmt.Errorf("config.Schema must be provided (no default schema)")
	}

	// Initialize database first (create sync tables and reset apply_mode)
	if err := initializeDatabase(db); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Clear global table info cache to prevent cached data from previous databases
	// from being used with this new database connection
	ClearGlobalTableInfoCache()

	client := &Client{
		DB:       db,
		BaseURL:  baseURL,
		Token:    tok,
		SourceID: sourceID,
		UserID:   userID,
		Resolver: &DefaultResolver{},
		HTTP:     &http.Client{Timeout: 120 * time.Second}, // Increased for large batch uploads
		config:   config,
		logger:   slog.Default(),
	}

	// Create triggers for all registered tables (after sync tables are created)
	for _, syncTable := range config.Tables {
		if err := createTriggersForTable(db, syncTable); err != nil {
			return nil, fmt.Errorf("failed to create triggers for table %s: %w", syncTable.TableName, err)
		}
	}

	return client, nil
}

// EnsureSourceID generates and persists a source ID if not already present
func EnsureSourceID(db *sql.DB, userID string) (string, error) {
	var sourceID string
	err := db.QueryRow(`SELECT source_id FROM _sync_client_info WHERE user_id = ?`, userID).Scan(&sourceID)
	if errors.Is(err, sql.ErrNoRows) {
		// Generate new source ID
		sourceID = uuid.New().String()
		_, err = db.Exec(`
			INSERT INTO _sync_client_info (user_id, source_id, next_change_id, last_server_seq_seen, apply_mode)
			VALUES (?, ?, 1, 0, 0)
		`, userID, sourceID)
		if err != nil {
			return "", fmt.Errorf("failed to insert client info: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("failed to query client info: %w", err)
	}
	return sourceID, nil
}

// initializeDatabase creates sync metadata tables according to the spec (private function)
func initializeDatabase(db *sql.DB) error {
	// Clear global table info cache to prevent cached data from previous databases
	// from being used with this new database connection
	ClearGlobalTableInfoCache()

	// Enable WAL mode and foreign keys
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Create sync metadata tables according to spec
	tables := []string{
		// Client/device info (one row)
		`CREATE TABLE IF NOT EXISTS _sync_client_info (
			user_id                TEXT NOT NULL,          -- from JWT.sub
			source_id              TEXT NOT NULL,          -- locally generated UUIDv4 (persisted)
			next_change_id         INTEGER NOT NULL DEFAULT 1,
			last_server_seq_seen   INTEGER NOT NULL DEFAULT 0,
			apply_mode             INTEGER NOT NULL DEFAULT 0,  -- 0=normal (queue to _sync_pending), 1=server-apply (suppress)
			current_window_until   INTEGER NOT NULL DEFAULT 0,  -- durable watermark for windowed downloads
			PRIMARY KEY (user_id)                               -- single signed-in user per DB file
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_row_meta (
			table_name     TEXT NOT NULL,
			pk_uuid        TEXT NOT NULL,
			server_version INTEGER NOT NULL DEFAULT 0,
			deleted        INTEGER NOT NULL DEFAULT 0,
			updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (table_name, pk_uuid)
		)`,

		// Pending queue (coalesced, one row per PK)
		`CREATE TABLE IF NOT EXISTS _sync_pending (
			table_name     TEXT NOT NULL,
			pk_uuid        TEXT NOT NULL,
			op             TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			base_version   INTEGER NOT NULL,
			payload        TEXT, -- JSON payload captured at time of change (NULL for DELETE)
			change_id      INTEGER, -- Original change ID for idempotency (assigned on first queue)
			queued_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (table_name, pk_uuid)
		)`,
	}

	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return fmt.Errorf("failed to create sync table: %w", err)
		}
	}

	// Reset apply_mode to 0 in case the app crashed while apply_mode was set to 1
	// This ensures sync triggers are not permanently suppressed after a crash
	_, err := db.Exec(`UPDATE _sync_client_info SET apply_mode = 0 WHERE apply_mode = 1`)
	if err != nil {
		// This is not a fatal error since the table might not exist yet or be empty
		// We'll just log it and continue
		fmt.Printf("Warning: failed to reset apply_mode during initialization: %v\n", err)
	}

	// (Migration to add change_id column is no longer needed.)

	return nil
}

// Start starts the uploader and downloader loops
func (c *Client) Start(ctx context.Context) error {
	// Start uploader goroutine
	go c.uploaderLoop(ctx)

	// Start downloader goroutine
	go c.downloaderLoop(ctx)

	return nil
}

// Stop stops the sync loops
func (c *Client) Stop(ctx context.Context) error {
	// Context cancellation will stop the loops
	return nil
}

// UploadOnce performs a single upload operation.
//
// It also performs a bounded post-upload download drain (include_self=false) to pick up any peer
// changes that arrived since the last download cursor, without ever fast-forwarding the cursor based
// on `highest_server_seq`. This avoids permanently skipping peer changes under high parallelism where
// server_id gaps can be large.
func (c *Client) UploadOnce(ctx context.Context) error {
	// Allow callers to pause uploads deterministically (prevents mid-creation drains)
	if atomic.LoadInt32(&c.uploadPaused) == 1 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Phase 1: upload local changes.
	if err := c.uploadBatch(ctx); err != nil {
		return err
	}

	// Phase 2: post-upload download drain (peer changes only).
	if atomic.LoadInt32(&c.downloadPaused) == 0 {
		maxPasses := 50
		for passes := 0; passes < maxPasses; passes++ {
			applied, _, err := c.downloadBatch(ctx, c.config.DownloadLimit, false)
			if err != nil {
				return fmt.Errorf("post-upload download drain failed: %w", err)
			}
			if applied == 0 {
				break
			}
		}
	}

	return nil
}

// DownloadOnce performs a single download operation
func (c *Client) DownloadOnce(ctx context.Context, limit int) (applied int, nextAfter int64, err error) {
	// Allow callers to pause downloads deterministically
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return 0, 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.downloadBatch(ctx, limit, false)
}

// DownloadOnceUntil performs a single download operation with a frozen upper bound (until)
func (c *Client) DownloadOnceUntil(ctx context.Context, limit int, until int64) (applied int, nextAfter int64, err error) {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return 0, 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// Get current cursor
	var lastServerSeq int64
	if err := c.DB.QueryRowContext(ctx, `SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?`, c.UserID).Scan(&lastServerSeq); err != nil {
		return 0, 0, fmt.Errorf("failed to get last server seq: %w", err)
	}
	// Request a windowed page
	downloadResponse, err := c.sendDownloadRequestWithUntil(ctx, lastServerSeq, limit, false, until)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to download changes: %w", err)
	}
	// If empty, persist next_after if advanced
	if len(downloadResponse.Changes) == 0 {
		if downloadResponse.NextAfter > lastServerSeq {
			tx, err := c.DB.BeginTx(ctx, nil)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to begin tx to persist cursor: %w", err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_info SET last_server_seq_seen = ? WHERE user_id = ?`, downloadResponse.NextAfter, c.UserID); err != nil {
				return 0, 0, fmt.Errorf("failed to update last_server_seq_seen: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return 0, 0, fmt.Errorf("failed to commit cursor persist tx: %w", err)
			}
		}
		return 0, downloadResponse.NextAfter, nil
	}
	// Apply page atomically (reuse existing logic via downloadBatchInTx-like sequence)
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
			_, _ = c.DB.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?`, c.UserID)
		}
	}()
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return 0, 0, fmt.Errorf("failed to enable deferred FK checks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 1 WHERE user_id = ?`, c.UserID); err != nil {
		return 0, 0, fmt.Errorf("failed to set apply_mode: %w", err)
	}
	applied = 0
	for i := range downloadResponse.Changes {
		ch := &downloadResponse.Changes[i]
		if ch.SourceID == c.SourceID {
			continue
		}
		if err := c.applyServerChangeInTx(ctx, tx, ch); err != nil {
			return applied, 0, fmt.Errorf("failed to apply server change: %w", err)
		}
		applied++
	}
	if downloadResponse.NextAfter > lastServerSeq {
		if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_info SET last_server_seq_seen = ? WHERE user_id = ?`, downloadResponse.NextAfter, c.UserID); err != nil {
			return applied, 0, fmt.Errorf("failed to update last_server_seq_seen: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?`, c.UserID); err != nil {
		return applied, 0, fmt.Errorf("failed to reset apply_mode: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return applied, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true
	return applied, downloadResponse.NextAfter, nil
}

// DownloadWithSelf performs a single download operation including own changes (for recovery)
func (c *Client) DownloadWithSelf(ctx context.Context, limit int) (applied int, nextAfter int64, err error) {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return 0, 0, nil
	}
	return c.downloadBatch(ctx, limit, true)
}

// Bootstrap performs initial setup for a new or restored client
// This includes creating client info and optionally performing hydration
func (c *Client) Bootstrap(ctx context.Context, performHydration bool) error {
	// Clear global table info cache to ensure fresh table information for this database
	ClearGlobalTableInfoCache()

	// Check if client info already exists
	var exists bool
	err := c.DB.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM _sync_client_info WHERE user_id = ? AND source_id = ?)
	`, c.UserID, c.SourceID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check client info existence: %w", err)
	}

	if !exists {
		// Create new client info
		_, err = c.DB.ExecContext(ctx, `
			INSERT INTO _sync_client_info (user_id, source_id, next_change_id, last_server_seq_seen, apply_mode)
			VALUES (?, ?, 1, 0, 0)
		`, c.UserID, c.SourceID)
		if err != nil {
			return fmt.Errorf("failed to create client info: %w", err)
		}
	}

	if performHydration {
		if atomic.LoadInt32(&c.downloadPaused) == 1 {
			return nil
		}
		return c.Hydrate(ctx)
	}

	return nil
}

// Hydrate downloads all user data from the server, optionally including own changes
// This is used for client recovery scenarios
func (c *Client) Hydrate(ctx context.Context) error {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.HydrateWithOptions(ctx, HydrationOptions{
		IncludeSelf: false,
		Limit:       1000,
	})
}

// HydrateWithSelf downloads all user data including own changes (for recovery)
func (c *Client) HydrateWithSelf(ctx context.Context) error {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.HydrateWithOptions(ctx, HydrationOptions{
		IncludeSelf: true,
		Limit:       1000,
	})
}

// HydrationOptions configures hydration behavior
type HydrationOptions struct {
	IncludeSelf bool // Include changes from the same source (for recovery)
	Limit       int  // Batch size for downloads
}

// HydrateWithOptions downloads all user data with specified options
func (c *Client) HydrateWithOptions(ctx context.Context, options HydrationOptions) error {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return nil
	}
	// Start transaction for atomic hydration
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin hydration transaction: %w", err)
	}
	defer tx.Rollback() // Safe to call even after commit

	// Defer foreign key constraint checks until end of transaction
	// This maintains data integrity while allowing out-of-order application
	_, err = tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`)
	if err != nil {
		return fmt.Errorf("failed to defer foreign keys: %w", err)
	}

	// Reset last server seq to 0 to download everything
	_, err = tx.ExecContext(ctx, `
		UPDATE _sync_client_info SET last_server_seq_seen = 0 WHERE user_id = ? AND source_id = ?
	`, c.UserID, c.SourceID)
	if err != nil {
		return fmt.Errorf("failed to reset server seq: %w", err)
	}

	totalApplied := 0
	for {
		var applied int

		applied, _, err = c.downloadBatchInTx(ctx, tx, options.Limit, options.IncludeSelf)
		if err != nil {
			return fmt.Errorf("hydration failed: %w", err)
		}

		totalApplied += applied

		// If we got fewer changes than the limit, we're done
		if applied < options.Limit {
			break
		}
	}

	// Commit the transaction - foreign key constraints will be checked at this point
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit hydration transaction: %w", err)
	}

	return nil
}

// SerializeRow loads a row by (table, pk) and serializes it to JSON payload for upload
// This follows the KMP approach for handling BLOB data
func (c *Client) SerializeRow(ctx context.Context, table, pk string) (json.RawMessage, error) {
	tableLc := strings.ToLower(table)

	// Get table information to determine primary key column and BLOB columns
	tableInfo, err := GetTableInfo(c.DB, tableLc)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	// Determine primary key column
	pkColumn := c.getPrimaryKeyColumn(table)

	// Convert PK value for query (handles BLOB primary keys)
	pkValue, err := c.convertPKForQuery(table, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	// Build query with correct primary key column
	query := fmt.Sprintf("SELECT * FROM \"%s\" WHERE \"%s\" = ?", tableLc, pkColumn)

	rows, err := c.DB.Query(query, pkValue)
	if err != nil {
		return nil, fmt.Errorf("failed to query row: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("row not found: table=%s pk=%s", table, pk)
	}

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Create slice of interface{} for scanning
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// Convert to map with BLOB-aware handling
	row := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]
		if val == nil {
			row[strings.ToLower(col)] = nil
			continue
		}

		// Handle BLOB data by hex encoding it (matching KMP approach)
		if b, ok := val.([]byte); ok {
			// Check if this column is a BLOB type
			isBlob := false
			for _, colInfo := range tableInfo.Columns {
				if strings.EqualFold(colInfo.Name, col) && colInfo.IsBlob() {
					isBlob = true
					break
				}
			}

			if isBlob {
				// Hex encode BLOB data (lowercase to match KMP)
				row[strings.ToLower(col)] = strings.ToLower(fmt.Sprintf("%x", b))
			} else {
				// Convert text data to string
				row[strings.ToLower(col)] = string(b)
			}
		} else {
			row[strings.ToLower(col)] = val
		}
	}

	// Convert to JSON
	jsonData, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal row to JSON: %w", err)
	}

	return json.RawMessage(jsonData), nil
}

// LocalDeletion represents a record that was deleted locally
type LocalDeletion struct {
	Table string
	ID    string
}

// captureLocalDeletions captures the current state of locally deleted records
// This is used to preserve deletions during sync window downloads
func (c *Client) captureLocalDeletions(ctx context.Context) ([]LocalDeletion, error) {
	var deletions []LocalDeletion

	// Query all pending DELETE operations from _sync_pending
	rows, err := c.DB.QueryContext(ctx, `
		SELECT table_name, pk_uuid
		FROM _sync_pending
		WHERE op = 'DELETE'
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending deletions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var deletion LocalDeletion
		err := rows.Scan(&deletion.Table, &deletion.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan deletion: %w", err)
		}
		deletions = append(deletions, deletion)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating deletions: %w", err)
	}

	return deletions, nil
}

// restoreLocalDeletions re-applies local deletions that may have been overwritten
// by sync window downloads, ensuring deleted records stay deleted
func (c *Client) restoreLocalDeletions(ctx context.Context, deletions []LocalDeletion) error {
	if len(deletions) == 0 {
		return nil // No deletions to restore
	}

	// Begin transaction to ensure atomicity
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, deletion := range deletions {
		pkColumn := c.getPrimaryKeyColumn(deletion.Table)
		// Use tx-aware table info lookups to avoid deadlocks when MaxOpenConns=1.
		pkValue, err := c.convertPKForQueryInTx(tx, deletion.Table, deletion.ID)
		if err != nil {
			return fmt.Errorf("failed to convert primary key for query: %w", err)
		}

		// Check if the record was restored by sync window download
		var exists bool
		err = tx.QueryRowContext(ctx,
			fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM \"%s\" WHERE \"%s\" = ?)", deletion.Table, pkColumn),
			pkValue).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check if record exists: %w", err)
		}

		if exists {
			// Check if there's a pending INSERT for this record (re-add case)
			var hasPendingInsert bool
			err := tx.QueryRowContext(ctx, `
				SELECT EXISTS(SELECT 1 FROM _sync_pending
				WHERE table_name = ? AND pk_uuid = ? AND op = 'INSERT')
			`, deletion.Table, deletion.ID).Scan(&hasPendingInsert)
			if err != nil {
				return fmt.Errorf("failed to check pending INSERT: %w", err)
			}

			if !hasPendingInsert {
				// Record was restored by sync window and not re-added locally - delete it again
				_, err = tx.ExecContext(ctx,
					fmt.Sprintf("DELETE FROM \"%s\" WHERE \"%s\" = ?", deletion.Table, pkColumn),
					pkValue)
				if err != nil {
					return fmt.Errorf("failed to restore deletion for %s.%s: %w", deletion.Table, deletion.ID, err)
				}
			}
			// If hasPendingInsert is true, skip deletion - the record was re-added locally
		}
	}

	return tx.Commit()
}
