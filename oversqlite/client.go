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
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// Client manages SQLite database and two-way sync operations
type Client struct {
	DB         *sql.DB
	BaseURL    string
	Token      func(context.Context) (string, error) // returns JWT
	SourceID   string
	UserID     string
	Resolver   Resolver
	HTTP       *http.Client
	config     *Config
	logger     *slog.Logger
	writeMu    sync.Mutex // Serialize write operations to prevent SQLite locking issues
	pkByTable  map[string]string
	keyByTable map[string][]string
	tableOrder map[string]int
	tableInfo  *TableInfoProvider
	closed     uint32

	// Pause switches (atomic): allow callers to suspend sync activity deterministically
	uploadPaused            int32
	downloadPaused          int32
	snapshotStats           snapshotTransferStats
	pushStats               pushTransferStats
	beforePushFreezeHook    func(context.Context) error
	beforePushCommitHook    func(context.Context) error
	beforePushReplayHook    func(context.Context) error
	dbOwnershipKey          string
	pendingInitializationID string
	sessionConnected        bool
}

// DatabaseAlreadyOwnedError indicates that another oversqlite client already owns the same SQLite DB.
type DatabaseAlreadyOwnedError struct {
	Database string
}

func (e *DatabaseAlreadyOwnedError) Error() string {
	if e == nil || strings.TrimSpace(e.Database) == "" {
		return "another oversqlite client is already active for this SQLite database"
	}
	return fmt.Sprintf("another oversqlite client is already active for SQLite database %s", e.Database)
}

// ClientClosedError indicates that Close() has already been called on the client.
type ClientClosedError struct{}

func (e *ClientClosedError) Error() string {
	return "oversqlite client has been closed"
}

type clientDBIdentity struct {
	Key   string
	Label string
}

type clientDBOwner struct {
	label string
	db    *sql.DB
}

var activeClientDBOwnership = struct {
	mu     sync.Mutex
	owners map[string]clientDBOwner
}{
	owners: make(map[string]clientDBOwner),
}

type snapshotTransferStats struct {
	sessionsCreated int64
	chunksFetched   int64
}

type SnapshotTransferStats struct {
	SessionsCreated int64
	ChunksFetched   int64
}

type pushTransferStats struct {
	sessionsCreated           int64
	chunksUploaded            int64
	committedBundleChunksRead int64
}

type PushTransferStats struct {
	SessionsCreated           int64
	ChunksUploaded            int64
	CommittedBundleChunksRead int64
}

var syncMetadataTableNames = []string{
	"_sync_client_state",
	"_sync_lifecycle_state",
	"_sync_row_state",
	"_sync_dirty_rows",
	"_sync_snapshot_stage",
	"_sync_push_outbound",
	"_sync_push_stage",
	"_sync_managed_tables",
}

func (c *Client) tryBeginSyncOperation() error {
	if c == nil {
		return nil
	}
	if atomic.LoadUint32(&c.closed) == 1 {
		return &ClientClosedError{}
	}
	if !c.writeMu.TryLock() {
		return &SyncOperationInProgressError{}
	}
	if atomic.LoadUint32(&c.closed) == 1 {
		c.writeMu.Unlock()
		return &ClientClosedError{}
	}
	return nil
}

func detectClientDBIdentity(db *sql.DB) (clientDBIdentity, error) {
	if db == nil {
		return clientDBIdentity{}, fmt.Errorf("db cannot be nil")
	}

	rows, err := db.Query(`PRAGMA database_list`)
	if err != nil {
		return clientDBIdentity{}, fmt.Errorf("failed to inspect SQLite database identity: %w", err)
	}
	defer rows.Close()

	var mainFile string
	for rows.Next() {
		var (
			seq  int
			name string
			file string
		)
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return clientDBIdentity{}, fmt.Errorf("failed to scan SQLite database identity: %w", err)
		}
		if name == "main" {
			mainFile = strings.TrimSpace(file)
			break
		}
	}
	if err := rows.Err(); err != nil {
		return clientDBIdentity{}, fmt.Errorf("failed to iterate SQLite database identity: %w", err)
	}

	if mainFile == "" {
		label := fmt.Sprintf("in-memory:%p", db)
		return clientDBIdentity{
			Key:   label,
			Label: label,
		}, nil
	}

	if !strings.HasPrefix(mainFile, "file:") {
		resolvedPath := mainFile
		if !filepath.IsAbs(resolvedPath) {
			if absPath, err := filepath.Abs(resolvedPath); err == nil {
				resolvedPath = absPath
			}
		}
		if evalPath, err := filepath.EvalSymlinks(resolvedPath); err == nil {
			resolvedPath = evalPath
		}
		mainFile = resolvedPath
	}

	return clientDBIdentity{
		Key:   "sqlite:" + mainFile,
		Label: mainFile,
	}, nil
}

func claimClientDBOwnership(db *sql.DB) (clientDBIdentity, error) {
	identity, err := detectClientDBIdentity(db)
	if err != nil {
		return clientDBIdentity{}, err
	}

	activeClientDBOwnership.mu.Lock()
	defer activeClientDBOwnership.mu.Unlock()

	if _, exists := activeClientDBOwnership.owners[identity.Key]; exists {
		return clientDBIdentity{}, &DatabaseAlreadyOwnedError{Database: identity.Label}
	}
	activeClientDBOwnership.owners[identity.Key] = clientDBOwner{
		label: identity.Label,
		db:    db,
	}
	return identity, nil
}

func releaseClientDBOwnership(key string) {
	if strings.TrimSpace(key) == "" {
		return
	}

	activeClientDBOwnership.mu.Lock()
	defer activeClientDBOwnership.mu.Unlock()
	delete(activeClientDBOwnership.owners, key)
}

func (c *Client) markClosedAndReleaseOwnershipLocked() {
	if c == nil {
		return
	}
	atomic.StoreUint32(&c.closed, 1)
	releaseClientDBOwnership(c.dbOwnershipKey)
	c.dbOwnershipKey = ""
}

// SyncTable represents a table configuration for synchronization
type SyncTable struct {
	TableName         string   // Table name (e.g., "users", "posts")
	SyncKeyColumnName string   // Single-column convenience field for the current supported envelope
	SyncKeyColumns    []string // Ordered sync key columns for the bundle-based runtime
}

// Config holds configuration for the SQLite sync client
type Config struct {
	Schema            string        // e.g., "app"
	Tables            []SyncTable   // Table configurations with explicit primary key columns
	UploadLimit       int           // e.g., 200 rows per push chunk
	DownloadLimit     int           // e.g., 1000
	SnapshotChunkRows int           // e.g., 1000
	BackoffMin        time.Duration // 1s
	BackoffMax        time.Duration // 60s
	RetryPolicy       *RetryPolicy  // nil => conservative built-in retry policy; explicit disabled policy turns retries off
}

// DefaultConfig returns a default configuration for the specified tables.
// Schema must be provided explicitly by the caller to avoid business-specific defaults.
// Each table must also declare SyncKeyColumnName explicitly.
func DefaultConfig(schema string, tables []SyncTable) *Config {
	return &Config{
		Schema:            schema,
		Tables:            tables,
		UploadLimit:       1000,
		DownloadLimit:     1000,
		SnapshotChunkRows: 1000,
		BackoffMin:        1 * time.Second,
		BackoffMax:        60 * time.Second,
	}
}

// PauseUploads suspends push operations and background sync loops.
func (c *Client) PauseUploads() { atomic.StoreInt32(&c.uploadPaused, 1) }

// ResumeUploads resumes upload operations
func (c *Client) ResumeUploads() { atomic.StoreInt32(&c.uploadPaused, 0) }

// PauseDownloads suspends pull, hydrate, recover, and background download-loop work.
func (c *Client) PauseDownloads() { atomic.StoreInt32(&c.downloadPaused, 1) }

// ResumeDownloads resumes download operations
func (c *Client) ResumeDownloads() { atomic.StoreInt32(&c.downloadPaused, 0) }

// SnapshotTransferDiagnostics returns counters for chunked hydrate/recover transport activity.
func (c *Client) SnapshotTransferDiagnostics() SnapshotTransferStats {
	if c == nil {
		return SnapshotTransferStats{}
	}
	return SnapshotTransferStats{
		SessionsCreated: atomic.LoadInt64(&c.snapshotStats.sessionsCreated),
		ChunksFetched:   atomic.LoadInt64(&c.snapshotStats.chunksFetched),
	}
}

// ResetSnapshotTransferDiagnostics clears the local chunked snapshot counters.
func (c *Client) ResetSnapshotTransferDiagnostics() {
	if c == nil {
		return
	}
	atomic.StoreInt64(&c.snapshotStats.sessionsCreated, 0)
	atomic.StoreInt64(&c.snapshotStats.chunksFetched, 0)
}

func (c *Client) PushTransferDiagnostics() PushTransferStats {
	if c == nil {
		return PushTransferStats{}
	}
	return PushTransferStats{
		SessionsCreated:           atomic.LoadInt64(&c.pushStats.sessionsCreated),
		ChunksUploaded:            atomic.LoadInt64(&c.pushStats.chunksUploaded),
		CommittedBundleChunksRead: atomic.LoadInt64(&c.pushStats.committedBundleChunksRead),
	}
}

func (c *Client) ResetPushTransferDiagnostics() {
	if c == nil {
		return
	}
	atomic.StoreInt64(&c.pushStats.sessionsCreated, 0)
	atomic.StoreInt64(&c.pushStats.chunksUploaded, 0)
	atomic.StoreInt64(&c.pushStats.committedBundleChunksRead, 0)
}

func (c *Client) SetBeforePushReplayHook(hook func(context.Context) error) {
	if c == nil {
		return
	}
	c.beforePushReplayHook = hook
}

func (c *Client) SetBeforePushFreezeHook(hook func(context.Context) error) {
	if c == nil {
		return
	}
	c.beforePushFreezeHook = hook
}

func (c *Client) SetBeforePushCommitHook(hook func(context.Context) error) {
	if c == nil {
		return
	}
	c.beforePushCommitHook = hook
}

// NewClient creates a new lifecycle-neutral SQLite sync client with the specified configuration.
// Automatically creates triggers for all tables specified in the config.
func NewClient(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config) (client *Client, err error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.Schema == "" {
		return nil, fmt.Errorf("config.Schema must be provided (no default schema)")
	}

	identity, err := claimClientDBOwnership(db)
	if err != nil {
		return nil, err
	}
	releaseOwnershipOnError := true
	defer func() {
		if releaseOwnershipOnError {
			releaseClientDBOwnership(identity.Key)
		}
	}()

	// Initialize database first (create sync tables and reset apply_mode)
	if err := initializeDatabase(db); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	if err := validateClientSchemaScope(db, config.Schema); err != nil {
		return nil, err
	}

	tableInfoProvider := NewTableInfoProvider()
	validatedTables, err := validateSyncTables(db, tableInfoProvider, config.Tables)
	if err != nil {
		return nil, err
	}

	client = &Client{
		DB:             db,
		BaseURL:        baseURL,
		Token:          tok,
		SourceID:       "",
		UserID:         "",
		Resolver:       &ServerWinsResolver{},
		HTTP:           &http.Client{Timeout: 120 * time.Second}, // Increased for large batch uploads
		config:         config,
		logger:         slog.Default(),
		pkByTable:      validatedTables.pkByTable,
		keyByTable:     validatedTables.keyByTable,
		tableOrder:     validatedTables.tableOrder,
		tableInfo:      tableInfoProvider,
		dbOwnershipKey: identity.Key,
	}

	// Create triggers for all registered tables (after sync tables are created)
	for _, syncTable := range config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if err := createTriggersForTableWithInfo(db, config.Schema, syncTable, validatedTables.tableInfoByKey[tableName]); err != nil {
			return nil, fmt.Errorf("failed to create triggers for table %s: %w", syncTable.TableName, err)
		}
	}
	if err := client.recordManagedSyncTables(); err != nil {
		return nil, err
	}

	releaseOwnershipOnError = false
	return client, nil
}

// Close releases the process-local ownership claim for this client.
// It does not close the underlying *sql.DB.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if atomic.LoadUint32(&c.closed) == 1 {
		return nil
	}
	c.markClosedAndReleaseOwnershipLocked()
	return nil
}

func (c *Client) recordManagedSyncTables() error {
	if c == nil || c.DB == nil || c.config == nil {
		return nil
	}
	schemaName := strings.TrimSpace(c.config.Schema)
	for _, syncTable := range c.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			continue
		}
		if _, err := c.DB.Exec(`
			INSERT INTO _sync_managed_tables (schema_name, table_name)
			VALUES (?, ?)
			ON CONFLICT(schema_name, table_name) DO NOTHING
		`, schemaName, tableName); err != nil {
			return fmt.Errorf("failed to record managed sync table %s: %w", tableName, err)
		}
	}
	return nil
}

func (c *Client) tableInfoProvider() *TableInfoProvider {
	if c.tableInfo == nil {
		c.tableInfo = NewTableInfoProvider()
	}
	return c.tableInfo
}

func (c *Client) getTableInfo(tableName string) (*TableInfo, error) {
	return c.tableInfoProvider().Get(c.DB, strings.ToLower(tableName))
}

func (c *Client) getTableInfoTx(tx *sql.Tx, tableName string) (*TableInfo, error) {
	return c.tableInfoProvider().Get(tx, strings.ToLower(tableName))
}

type validatedSyncTables struct {
	pkByTable      map[string]string
	keyByTable     map[string][]string
	tableOrder     map[string]int
	tableInfoByKey map[string]*TableInfo
}

func validateSyncTables(db *sql.DB, provider *TableInfoProvider, tables []SyncTable) (*validatedSyncTables, error) {
	if provider == nil {
		provider = NewTableInfoProvider()
	}
	pkByTable := make(map[string]string, len(tables))
	keyByTable := make(map[string][]string, len(tables))
	tableInfoByKey := make(map[string]*TableInfo, len(tables))
	managedTables := make(map[string]struct{}, len(tables))
	for _, syncTable := range tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			return nil, fmt.Errorf("sync table name must be provided")
		}
		if strings.Contains(tableName, ".") {
			return nil, fmt.Errorf("table %s must not include a schema qualifier; oversqlite supports exactly one config.Schema per local database", syncTable.TableName)
		}
		if _, exists := pkByTable[tableName]; exists {
			return nil, fmt.Errorf("duplicate sync table registration for %s", tableName)
		}
		managedTables[tableName] = struct{}{}

		keyColumns, err := normalizedSyncKeyColumns(syncTable)
		if err != nil {
			return nil, err
		}
		if len(keyColumns) != 1 {
			return nil, fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", syncTable.TableName)
		}

		tableInfo, err := provider.Get(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect table %s: %w", syncTable.TableName, err)
		}

		resolvedPK, err := configuredPrimaryKeyColumn(tableInfo, syncTable)
		if err != nil {
			return nil, err
		}

		pkByTable[tableName] = resolvedPK
		keyByTable[tableName] = append([]string(nil), keyColumns...)
		tableInfoByKey[tableName] = tableInfo
	}
	if err := validateManagedForeignKeyClosure(managedTables, tableInfoByKey); err != nil {
		return nil, err
	}
	tableOrder, err := computeManagedTableOrder(tables, tableInfoByKey)
	if err != nil {
		return nil, err
	}
	return &validatedSyncTables{
		pkByTable:      pkByTable,
		keyByTable:     keyByTable,
		tableOrder:     tableOrder,
		tableInfoByKey: tableInfoByKey,
	}, nil
}

func validateClientSchemaScope(db *sql.DB, schemaName string) error {
	schemaName = strings.TrimSpace(schemaName)
	if schemaName == "" {
		return fmt.Errorf("config.Schema must be provided (no default schema)")
	}

	rows, err := db.Query(`
			SELECT DISTINCT schema_name
			FROM (
				SELECT schema_name FROM _sync_client_state
				UNION ALL
				SELECT schema_name FROM _sync_row_state
				UNION ALL
				SELECT schema_name FROM _sync_dirty_rows
				UNION ALL
				SELECT schema_name FROM _sync_snapshot_stage
				UNION ALL
				SELECT schema_name FROM _sync_push_outbound
				UNION ALL
				SELECT schema_name FROM _sync_push_stage
			)
			WHERE TRIM(schema_name) <> ''
			ORDER BY schema_name
		`)
	if err != nil {
		return fmt.Errorf("failed to validate client schema scope: %w", err)
	}
	defer rows.Close()

	var seen []string
	for rows.Next() {
		var persisted string
		if err := rows.Scan(&persisted); err != nil {
			return fmt.Errorf("failed to scan client schema scope: %w", err)
		}
		seen = append(seen, persisted)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate client schema scope: %w", err)
	}

	if len(seen) == 0 {
		return nil
	}
	if len(seen) == 1 && seen[0] == schemaName {
		return nil
	}
	return fmt.Errorf("oversqlite supports exactly one configured schema per local database; existing sync state uses schema(s) %s while config.Schema=%s", strings.Join(seen, ", "), schemaName)
}

func normalizedSyncKeyColumns(syncTable SyncTable) ([]string, error) {
	if len(syncTable.SyncKeyColumns) == 0 {
		pkColumn := strings.TrimSpace(syncTable.SyncKeyColumnName)
		if pkColumn == "" {
			return nil, fmt.Errorf("table %s must declare SyncKeyColumnName or SyncKeyColumns explicitly", syncTable.TableName)
		}
		return []string{pkColumn}, nil
	}

	columns := make([]string, 0, len(syncTable.SyncKeyColumns))
	seen := make(map[string]struct{}, len(syncTable.SyncKeyColumns))
	for _, col := range syncTable.SyncKeyColumns {
		normalized := strings.TrimSpace(col)
		if normalized == "" {
			continue
		}
		key := strings.ToLower(normalized)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		columns = append(columns, normalized)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s must declare at least one non-empty sync key column", syncTable.TableName)
	}
	return columns, nil
}

func validateManagedForeignKeyClosure(managedTables map[string]struct{}, tableInfoByKey map[string]*TableInfo) error {
	for tableName := range managedTables {
		tableInfo := tableInfoByKey[tableName]
		if tableInfo == nil {
			return fmt.Errorf("missing table metadata for %s", tableName)
		}
		var unsupportedCompositeRefs []string
		var missingRefs []string
		for _, fk := range tableInfo.ForeignKeys {
			refKey := strings.ToLower(strings.TrimSpace(fk.RefTable))
			if refKey == "" {
				continue
			}
			if fk.Seq > 0 {
				unsupportedCompositeRefs = append(unsupportedCompositeRefs, fmt.Sprintf("%s -> %s", tableName, refKey))
				continue
			}
			if _, ok := managedTables[refKey]; ok {
				continue
			}

			missingRefs = append(missingRefs, fmt.Sprintf("%s.%s -> %s.%s", tableName, fk.FromCol, refKey, fk.ToCol))
		}

		if len(unsupportedCompositeRefs) > 0 {
			sort.Strings(unsupportedCompositeRefs)
			return fmt.Errorf("managed tables contain unsupported composite foreign keys: %s", strings.Join(unsupportedCompositeRefs, "; "))
		}
		if len(missingRefs) > 0 {
			sort.Strings(missingRefs)
			return fmt.Errorf("managed tables are not FK-closed: %s", strings.Join(missingRefs, "; "))
		}
	}

	return nil
}

func computeManagedTableOrder(tables []SyncTable, tableInfoByKey map[string]*TableInfo) (map[string]int, error) {
	orderByTable := make(map[string]int, len(tables))
	originalOrder := make(map[string]int, len(tables))
	managed := make(map[string]struct{}, len(tables))
	dependents := make(map[string]map[string]struct{}, len(tables))
	inDegree := make(map[string]int, len(tables))

	for i, table := range tables {
		name := strings.ToLower(strings.TrimSpace(table.TableName))
		originalOrder[name] = i
		managed[name] = struct{}{}
		if _, ok := dependents[name]; !ok {
			dependents[name] = make(map[string]struct{})
		}
	}

	for _, table := range tables {
		tableName := strings.ToLower(strings.TrimSpace(table.TableName))
		tableInfo := tableInfoByKey[tableName]
		if tableInfo == nil {
			return nil, fmt.Errorf("missing table metadata for %s", tableName)
		}
		for _, fk := range tableInfo.ForeignKeys {
			refName := strings.ToLower(strings.TrimSpace(fk.RefTable))
			if refName == "" || refName == tableName {
				continue
			}
			if _, ok := managed[refName]; !ok {
				continue
			}
			if _, ok := dependents[refName][tableName]; ok {
				continue
			}
			dependents[refName][tableName] = struct{}{}
			inDegree[tableName]++
		}
	}

	queue := make([]string, 0, len(tables))
	for _, table := range tables {
		name := strings.ToLower(strings.TrimSpace(table.TableName))
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}
	sort.Slice(queue, func(i, j int) bool {
		return originalOrder[queue[i]] < originalOrder[queue[j]]
	})

	ordered := make([]string, 0, len(tables))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		ordered = append(ordered, current)

		children := make([]string, 0, len(dependents[current]))
		for child := range dependents[current] {
			children = append(children, child)
		}
		sort.Slice(children, func(i, j int) bool {
			return originalOrder[children[i]] < originalOrder[children[j]]
		})

		for _, child := range children {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
		sort.Slice(queue, func(i, j int) bool {
			return originalOrder[queue[i]] < originalOrder[queue[j]]
		})
	}

	if len(ordered) < len(tables) {
		remaining := make([]string, 0, len(tables)-len(ordered))
		seen := make(map[string]struct{}, len(ordered))
		for _, tableName := range ordered {
			seen[tableName] = struct{}{}
		}
		for _, table := range tables {
			name := strings.ToLower(strings.TrimSpace(table.TableName))
			if _, ok := seen[name]; ok {
				continue
			}
			remaining = append(remaining, name)
		}
		sort.Slice(remaining, func(i, j int) bool {
			return originalOrder[remaining[i]] < originalOrder[remaining[j]]
		})
		ordered = append(ordered, remaining...)
	}

	for i, tableName := range ordered {
		orderByTable[tableName] = i
	}
	return orderByTable, nil
}

func (c *Client) primaryKeyColumnForTable(tableName string) (string, error) {
	pkColumn, ok := c.pkByTable[strings.ToLower(tableName)]
	if !ok {
		return "", fmt.Errorf("table %s is not configured for sync", tableName)
	}
	return pkColumn, nil
}

func (c *Client) syncKeyColumnsForTable(tableName string) ([]string, error) {
	keyColumns, ok := c.keyByTable[strings.ToLower(tableName)]
	if !ok || len(keyColumns) == 0 {
		return nil, fmt.Errorf("table %s is not configured for sync", tableName)
	}
	return append([]string(nil), keyColumns...), nil
}

// EnsureSourceID generates and persists a source ID if not already present
func EnsureSourceID(db *sql.DB, userID string) (string, error) {
	return ensureClientSourceID(db, userID, "", "")
}

func ensureClientSourceID(db *sql.DB, userID, preferredSourceID, schemaName string) (string, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", fmt.Errorf("userID must be provided")
	}

	sourceID := strings.TrimSpace(preferredSourceID)
	if sourceID == "" {
		sourceID = uuid.NewString()
	}

	schemaName = strings.TrimSpace(schemaName)
	if _, err := db.Exec(`
		INSERT INTO _sync_client_state (user_id, source_id, schema_name, next_source_bundle_id, last_bundle_seq_seen, apply_mode, rebuild_required)
		VALUES (?, ?, ?, 1, 0, 0, 0)
		ON CONFLICT(user_id) DO NOTHING
	`, userID, sourceID, schemaName); err != nil {
		return "", fmt.Errorf("failed to ensure client state: %w", err)
	}
	if schemaName != "" {
		if _, err := db.Exec(`
			UPDATE _sync_client_state
			SET schema_name = ?
			WHERE user_id = ? AND TRIM(schema_name) = ''
		`, schemaName, userID); err != nil {
			return "", fmt.Errorf("failed to persist client schema identity: %w", err)
		}
	}

	var persistedSourceID string
	var persistedSchema string
	if err := db.QueryRow(`SELECT source_id, schema_name FROM _sync_client_state WHERE user_id = ?`, userID).Scan(&persistedSourceID, &persistedSchema); err != nil {
		return "", fmt.Errorf("failed to load client source id: %w", err)
	}
	if schemaName != "" && strings.TrimSpace(persistedSchema) != schemaName {
		return "", fmt.Errorf("client state for user %s is bound to schema %s, not %s", userID, persistedSchema, schemaName)
	}
	return persistedSourceID, nil
}

// initializeDatabase creates sync metadata tables according to the spec (private function)
func initializeDatabase(db *sql.DB) error {
	// Enable WAL mode and foreign keys
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Create sync metadata tables according to spec
	tables := []string{
		`CREATE TABLE IF NOT EXISTS _sync_client_state (
			user_id               TEXT NOT NULL,
			source_id             TEXT NOT NULL,
			schema_name           TEXT NOT NULL DEFAULT '',
			next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
			last_bundle_seq_seen  INTEGER NOT NULL DEFAULT 0,
			apply_mode            INTEGER NOT NULL DEFAULT 0,
			rebuild_required      INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (user_id)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_lifecycle_state (
			singleton_key            INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
			source_id                TEXT NOT NULL DEFAULT '',
			binding_state            TEXT NOT NULL DEFAULT 'anonymous',
			binding_scope            TEXT NOT NULL DEFAULT '',
			pending_transition_kind  TEXT NOT NULL DEFAULT 'none',
			pending_target_scope     TEXT NOT NULL DEFAULT '',
			pending_target_source_id TEXT NOT NULL DEFAULT '',
			pending_staged_snapshot_id TEXT NOT NULL DEFAULT '',
			pending_snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
			pending_snapshot_row_count INTEGER NOT NULL DEFAULT 0,
			pending_apply_cursor     INTEGER NOT NULL DEFAULT 0,
			pending_initialization_id TEXT NOT NULL DEFAULT '',
			preexisting_capture_done INTEGER NOT NULL DEFAULT 0,
			runtime_bypass_active    INTEGER NOT NULL DEFAULT 0
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_row_state (
			schema_name TEXT NOT NULL,
			table_name  TEXT NOT NULL,
			key_json    TEXT NOT NULL,
			row_version INTEGER NOT NULL DEFAULT 0,
			deleted     INTEGER NOT NULL DEFAULT 0,
			updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (schema_name, table_name, key_json)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
			schema_name      TEXT NOT NULL,
			table_name       TEXT NOT NULL,
			key_json         TEXT NOT NULL,
			op               TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			base_row_version INTEGER NOT NULL DEFAULT 0,
			payload          TEXT,
			dirty_ordinal    INTEGER NOT NULL DEFAULT 0,
			updated_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (schema_name, table_name, key_json)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
			snapshot_id  TEXT NOT NULL,
			row_ordinal  INTEGER NOT NULL,
			schema_name  TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			key_json     TEXT NOT NULL,
			row_version  INTEGER NOT NULL,
			payload      TEXT NOT NULL,
			PRIMARY KEY (snapshot_id, row_ordinal)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_push_outbound (
			source_bundle_id INTEGER NOT NULL,
			row_ordinal      INTEGER NOT NULL,
			schema_name      TEXT NOT NULL,
			table_name       TEXT NOT NULL,
			key_json         TEXT NOT NULL,
			op               TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			base_row_version INTEGER NOT NULL DEFAULT 0,
			payload          TEXT,
			PRIMARY KEY (source_bundle_id, row_ordinal)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_push_stage (
			bundle_seq   INTEGER NOT NULL,
			row_ordinal  INTEGER NOT NULL,
			schema_name  TEXT NOT NULL,
			table_name   TEXT NOT NULL,
			key_json     TEXT NOT NULL,
			op           TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			row_version  INTEGER NOT NULL,
			payload      TEXT,
			PRIMARY KEY (bundle_seq, row_ordinal)
		)`,

		`CREATE TABLE IF NOT EXISTS _sync_managed_tables (
			schema_name TEXT NOT NULL,
			table_name  TEXT NOT NULL,
			PRIMARY KEY (schema_name, table_name)
		)`,
	}

	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return fmt.Errorf("failed to create sync table: %w", err)
		}
	}

	if _, err := db.Exec(`ALTER TABLE _sync_client_state ADD COLUMN rebuild_required INTEGER NOT NULL DEFAULT 0`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("failed to add rebuild_required column: %w", err)
	}
	if _, err := db.Exec(`
		INSERT INTO _sync_lifecycle_state (
			singleton_key, source_id, binding_state, binding_scope, pending_transition_kind, pending_target_scope, pending_target_source_id, pending_staged_snapshot_id, pending_snapshot_bundle_seq, pending_snapshot_row_count, pending_apply_cursor, pending_initialization_id, preexisting_capture_done, runtime_bypass_active
		) VALUES (1, '', 'anonymous', '', 'none', '', '', '', 0, 0, 0, '', 0, 0)
		ON CONFLICT(singleton_key) DO NOTHING
	`); err != nil {
		return fmt.Errorf("failed to initialize lifecycle state row: %w", err)
	}
	lifecycleColumnStatements := []string{
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN pending_target_scope TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN pending_target_source_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN pending_staged_snapshot_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN pending_snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN pending_snapshot_row_count INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN pending_apply_cursor INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE _sync_lifecycle_state ADD COLUMN runtime_bypass_active INTEGER NOT NULL DEFAULT 0`,
	}
	for _, stmt := range lifecycleColumnStatements {
		if _, err := db.Exec(stmt); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to evolve lifecycle state table: %w", err)
		}
	}

	// Reset apply_mode to 0 in case the app crashed while apply_mode was set to 1
	// This ensures sync triggers are not permanently suppressed after a crash
	if _, err := db.Exec(`UPDATE _sync_client_state SET apply_mode = 0 WHERE apply_mode = 1`); err != nil {
		fmt.Printf("Warning: failed to reset bundle apply_mode during initialization: %v\n", err)
	}

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

// PullToStable pulls complete committed bundles until the client reaches the server's frozen stable ceiling.
func (c *Client) PullToStable(ctx context.Context) error {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	return c.pullToStableLocked(ctx)
}

// Sync pushes local changes and then pulls to a stable committed-bundle ceiling.
func (c *Client) Sync(ctx context.Context) error {
	if atomic.LoadInt32(&c.uploadPaused) == 1 && atomic.LoadInt32(&c.downloadPaused) == 1 {
		return nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	if err := c.ensureConnectedSessionLocked(ctx, "Sync()"); err != nil {
		return err
	}
	if atomic.LoadInt32(&c.uploadPaused) == 0 {
		if err := c.pushPendingLocked(ctx, 0); err != nil {
			return err
		}
	}
	if atomic.LoadInt32(&c.downloadPaused) == 0 {
		if err := c.pullToStableLocked(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Hydrate rebuilds the managed tables from the current server snapshot.
func (c *Client) Hydrate(ctx context.Context) error {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		return nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	return c.hydrateLocked(ctx)
}

// SerializeRow loads a row by (table, pk) and serializes it to JSON payload for upload
// This follows the KMP approach for handling BLOB data
func (c *Client) SerializeRow(ctx context.Context, table, pk string) (json.RawMessage, error) {
	tableLc := strings.ToLower(table)

	// Get table information to determine primary key column and BLOB columns
	tableInfo, err := c.getTableInfo(tableLc)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	// Determine primary key column
	pkColumn, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return nil, err
	}

	// Convert PK value for query (handles BLOB primary keys)
	pkValue, err := c.convertPKForQuery(table, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	// Build query with correct primary key column
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", quoteIdent(tableLc), quoteIdent(pkColumn))

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

func (c *Client) serializeRowInTx(ctx context.Context, tx *sql.Tx, table, pk string) (json.RawMessage, error) {
	tableLc := strings.ToLower(table)

	tableInfo, err := c.getTableInfoTx(tx, tableLc)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s: %w", table, err)
	}

	pkColumn, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return nil, err
	}

	pkValue, err := c.convertPKForQueryInTx(tx, table, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to convert primary key for query: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", quoteIdent(tableLc), quoteIdent(pkColumn))
	rows, err := tx.QueryContext(ctx, query, pkValue)
	if err != nil {
		return nil, fmt.Errorf("failed to query row: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("row not found: table=%s pk=%s", table, pk)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	row := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]
		if val == nil {
			row[strings.ToLower(col)] = nil
			continue
		}

		if b, ok := val.([]byte); ok {
			isBlob := false
			for _, colInfo := range tableInfo.Columns {
				if strings.EqualFold(colInfo.Name, col) && colInfo.IsBlob() {
					isBlob = true
					break
				}
			}

			if isBlob {
				row[strings.ToLower(col)] = strings.ToLower(fmt.Sprintf("%x", b))
			} else {
				row[strings.ToLower(col)] = string(b)
			}
		} else {
			row[strings.ToLower(col)] = val
		}
	}

	jsonData, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal row to JSON: %w", err)
	}

	return json.RawMessage(jsonData), nil
}
