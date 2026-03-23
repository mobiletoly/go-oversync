// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type serviceLifecycleState string

const (
	serviceLifecycleRunning               serviceLifecycleState = "running"
	serviceLifecycleShuttingDown          serviceLifecycleState = "shutting_down"
	serviceLifecycleClosed                serviceLifecycleState = "closed"
	defaultMaxBundlesPerPull              int                   = 5000
	defaultPullBundlesPerRequest          int                   = 1000
	defaultRowsPerPushChunk               int                   = 1000
	defaultMaxRowsPerPushChunk            int                   = 5000
	defaultPushSessionTTL                 time.Duration         = 15 * time.Minute
	defaultRowsPerCommittedBundleChunk    int                   = 1000
	defaultMaxRowsPerCommittedBundleChunk int                   = 5000
	defaultMaxRowsPerSnapshotChunk        int                   = 5000
	defaultRowsPerSnapshotChunk           int                   = 1000
	defaultSnapshotSessionTTL             time.Duration         = 15 * time.Minute
)

var errServiceShuttingDown = errors.New("sync service is shutting down")

// RegisteredTable represents a table that is registered for sync operations
type RegisteredTable struct {
	Schema         string   `json:"schema"`                     // Schema name (e.g., "public", "crm", "business")
	Table          string   `json:"table"`                      // Table name (e.g., "users", "posts")
	SyncKeyColumns []string `json:"sync_key_columns,omitempty"` // Ordered sync key columns for the target bundle-based protocol
}

func (t RegisteredTable) normalizedSchema() string {
	schema := strings.ToLower(strings.TrimSpace(t.Schema))
	if schema == "" {
		return "public"
	}
	return schema
}

func (t RegisteredTable) normalizedTable() string {
	return strings.ToLower(strings.TrimSpace(t.Table))
}

func (t RegisteredTable) normalizedKey() string {
	return t.normalizedSchema() + "." + t.normalizedTable()
}

func (t RegisteredTable) normalizedSyncKeyColumns() []string {
	if len(t.SyncKeyColumns) == 0 {
		return nil
	}

	columns := make([]string, 0, len(t.SyncKeyColumns))
	seen := make(map[string]struct{}, len(t.SyncKeyColumns))
	for _, col := range t.SyncKeyColumns {
		normalized := strings.ToLower(strings.TrimSpace(col))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		columns = append(columns, normalized)
	}
	return columns
}

func (t RegisteredTable) effectiveSyncKeyColumns(primaryKeyColumn string) []string {
	columns := t.normalizedSyncKeyColumns()
	if len(columns) > 0 {
		return columns
	}
	if strings.TrimSpace(primaryKeyColumn) == "" {
		return nil
	}
	return []string{strings.ToLower(strings.TrimSpace(primaryKeyColumn))}
}

// SyncService provides the core synchronization functionality
// This is the main SDK component that developers integrate into their applications
type SyncService struct {
	pool               *pgxpool.Pool
	logger             *slog.Logger
	config             *ServiceConfig
	registeredTables   map[string]bool // Set of "schema.table" combinations allowed in sync operations
	columnTypesByTable map[string]map[string]string

	// Schema discovery snapshot for bootstrap validation and FK-safe bundle ordering.
	discoveredSchema *DiscoveredSchema

	// Runtime lifecycle tracking.
	mu          sync.RWMutex
	lifecycle   serviceLifecycleState
	inFlightOps int
	drainedCh   chan struct{}
}

// ServiceConfig holds configuration for the sync service
type ServiceConfig struct {
	MaxSupportedSchemaVersion int               // Current schema version to return
	AppName                   string            // Application name for connection tracking
	RegisteredTables          []RegisteredTable // Schema.table combinations allowed for sync (required)

	MaxRowsPerBundle  int // Maximum number of row effects allowed in one committed bundle (0 = unlimited)
	MaxBytesPerBundle int // Maximum JSON payload size allowed in one committed bundle (0 = unlimited)
	// Push-session chunking limits. Zero uses the runtime defaults.
	DefaultRowsPerPushChunk int
	MaxRowsPerPushChunk     int
	PushSessionTTL          time.Duration
	// Committed-bundle row fetch limits. Zero uses the runtime defaults.
	DefaultRowsPerCommittedBundleChunk int
	MaxRowsPerCommittedBundleChunk     int
	// Snapshot chunking limits. Zero uses the runtime defaults.
	DefaultRowsPerSnapshotChunk int
	MaxRowsPerSnapshotChunk     int
	SnapshotSessionTTL          time.Duration
	MaxRowsPerSnapshotSession   int64
	MaxBytesPerSnapshotSession  int64
	// UploadLockTimeout bounds lock waits inside upload transactions.
	// Zero disables SET LOCAL lock_timeout so lock waits are governed only by the request context,
	// which is the reliability-first default.
	UploadLockTimeout time.Duration

	// DependencyOverrides optionally adds explicit ordering constraints on top of discovered
	// DB FKs. Keys and values are "schema.table". Only affects ordering, not FK validation.
	DependencyOverrides map[string][]string

	// StageMetrics optionally records per-stage timings for sync hot paths.
	// The recorder is called synchronously; it must be fast and concurrency-safe.
	StageMetrics StageMetricsRecorder

	// LogStageTimings logs per-stage timings via the service logger at DEBUG.
	// Useful for profiling; keep disabled in production.
	LogStageTimings bool
}

func (s *SyncService) defaultRowsPerSnapshotChunk() int {
	if s != nil && s.config != nil && s.config.DefaultRowsPerSnapshotChunk > 0 {
		return s.config.DefaultRowsPerSnapshotChunk
	}
	return defaultRowsPerSnapshotChunk
}

func (s *SyncService) maxRowsPerSnapshotChunk() int {
	if s != nil && s.config != nil && s.config.MaxRowsPerSnapshotChunk > 0 {
		return s.config.MaxRowsPerSnapshotChunk
	}
	return defaultMaxRowsPerSnapshotChunk
}

func (s *SyncService) defaultRowsPerPushChunk() int {
	if s != nil && s.config != nil && s.config.DefaultRowsPerPushChunk > 0 {
		return s.config.DefaultRowsPerPushChunk
	}
	return defaultRowsPerPushChunk
}

func (s *SyncService) maxRowsPerPushChunk() int {
	if s != nil && s.config != nil && s.config.MaxRowsPerPushChunk > 0 {
		return s.config.MaxRowsPerPushChunk
	}
	return defaultMaxRowsPerPushChunk
}

func (s *SyncService) pushSessionTTL() time.Duration {
	if s != nil && s.config != nil && s.config.PushSessionTTL > 0 {
		return s.config.PushSessionTTL
	}
	return defaultPushSessionTTL
}

func (s *SyncService) defaultRowsPerCommittedBundleChunk() int {
	if s != nil && s.config != nil && s.config.DefaultRowsPerCommittedBundleChunk > 0 {
		return s.config.DefaultRowsPerCommittedBundleChunk
	}
	return defaultRowsPerCommittedBundleChunk
}

func (s *SyncService) maxRowsPerCommittedBundleChunk() int {
	if s != nil && s.config != nil && s.config.MaxRowsPerCommittedBundleChunk > 0 {
		return s.config.MaxRowsPerCommittedBundleChunk
	}
	return defaultMaxRowsPerCommittedBundleChunk
}

func (s *SyncService) snapshotSessionTTL() time.Duration {
	if s != nil && s.config != nil && s.config.SnapshotSessionTTL > 0 {
		return s.config.SnapshotSessionTTL
	}
	return defaultSnapshotSessionTTL
}

func (s *SyncService) maxRowsPerSnapshotSession() int64 {
	if s != nil && s.config != nil && s.config.MaxRowsPerSnapshotSession > 0 {
		return s.config.MaxRowsPerSnapshotSession
	}
	return 0
}

func (s *SyncService) maxBytesPerSnapshotSession() int64 {
	if s != nil && s.config != nil && s.config.MaxBytesPerSnapshotSession > 0 {
		return s.config.MaxBytesPerSnapshotSession
	}
	return 0
}

// NewRuntimeService creates a runtime-only sync service instance from an existing pool.
// It does not mutate database schema or discover runtime topology.
func NewRuntimeService(pool *pgxpool.Pool, config *ServiceConfig, logger *slog.Logger) (*SyncService, error) {
	if config == nil {
		config = &ServiceConfig{
			MaxSupportedSchemaVersion: 1,
			AppName:                   "go-oversync-app",
		}
	}
	if logger == nil {
		logger = slog.Default()
	}

	service := &SyncService{
		pool:               pool,
		logger:             logger,
		config:             config,
		registeredTables:   make(map[string]bool),
		columnTypesByTable: make(map[string]map[string]string),
		lifecycle:          serviceLifecycleRunning,
	}

	// Initialize registered tables set and handlers
	for _, regTable := range config.RegisteredTables {
		// Normalize keys to lowercase to match request validation normalization
		key := regTable.normalizedKey()
		service.registeredTables[key] = true
	}

	return service, nil
}

// Bootstrap initializes sync metadata and the runtime topology snapshot.
// Topology is prepared at bootstrap time and is restart-only for now; runtime schema changes are
// not re-discovered automatically by SyncService.
func (s *SyncService) Bootstrap(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.pool == nil {
		return fmt.Errorf("bootstrap requires a database pool")
	}
	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.initializeSchemaInTx(ctx, tx); err != nil {
			s.logger.Error("Failed to initialize database schema", "error", err)
			return err
		}
		s.logger.Debug("Database schema initialized successfully")
		return nil
	}); err != nil {
		return err
	}

	if err := s.normalizeAndValidateRegisteredSyncKeys(ctx); err != nil {
		return err
	}

	if err := s.discoverSchemaRelationships(ctx); err != nil {
		return fmt.Errorf("failed to discover schema relationships: %w", err)
	}
	if err := s.installRegisteredTableCaptureTriggers(ctx); err != nil {
		return fmt.Errorf("failed to install registered table capture triggers: %w", err)
	}
	return nil
}

func (s *SyncService) normalizeAndValidateRegisteredSyncKeys(ctx context.Context) error {
	if s == nil || s.pool == nil || s.config == nil || len(s.config.RegisteredTables) == 0 {
		return nil
	}

	type pkInfo struct {
		count int
		first string
	}

	type columnInfo struct {
		column string
		udt    string
	}

	schemas := make([]string, 0, len(s.config.RegisteredTables))
	tables := make([]string, 0, len(s.config.RegisteredTables))
	for _, tbl := range s.config.RegisteredTables {
		schemas = append(schemas, tbl.normalizedSchema())
		tables = append(tables, tbl.normalizedTable())
	}

	pkRows, err := s.pool.Query(ctx, `
WITH t AS (
  SELECT * FROM unnest(@schemas::text[], @tables::text[]) AS x(schema_name, table_name)
)
SELECT
  kcu.table_schema,
  kcu.table_name,
  lower(kcu.column_name) AS column_name,
  kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_schema = kcu.constraint_schema
 AND tc.constraint_name = kcu.constraint_name
JOIN t
  ON kcu.table_schema = t.schema_name
 AND kcu.table_name = t.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
ORDER BY kcu.table_schema, kcu.table_name, kcu.ordinal_position
`, pgx.NamedArgs{
		"schemas": schemas,
		"tables":  tables,
	})
	if err != nil {
		return fmt.Errorf("load registered table primary keys: %w", err)
	}
	defer pkRows.Close()

	primaryKeys := make(map[string]pkInfo, len(s.config.RegisteredTables))
	for pkRows.Next() {
		var schemaName, tableName, columnName string
		var ordinal int
		if err := pkRows.Scan(&schemaName, &tableName, &columnName, &ordinal); err != nil {
			return fmt.Errorf("scan registered table primary keys: %w", err)
		}

		tableKey := Key(schemaName, tableName)
		info := primaryKeys[tableKey]
		info.count++
		if ordinal == 1 {
			info.first = columnName
		}
		primaryKeys[tableKey] = info
	}
	if pkRows.Err() != nil {
		return fmt.Errorf("iterate registered table primary keys: %w", pkRows.Err())
	}

	colRows, err := s.pool.Query(ctx, `
WITH t AS (
  SELECT * FROM unnest(@schemas::text[], @tables::text[]) AS x(schema_name, table_name)
)
SELECT
  c.table_schema,
  c.table_name,
  lower(c.column_name) AS column_name,
  lower(c.udt_name) AS udt_name
FROM information_schema.columns c
JOIN t
  ON c.table_schema = t.schema_name
 AND c.table_name = t.table_name
`, pgx.NamedArgs{
		"schemas": schemas,
		"tables":  tables,
	})
	if err != nil {
		return fmt.Errorf("load registered table column types: %w", err)
	}
	defer colRows.Close()

	columnTypes := make(map[string]map[string]string, len(s.config.RegisteredTables))
	for colRows.Next() {
		var schemaName, tableName, columnName, udtName string
		if err := colRows.Scan(&schemaName, &tableName, &columnName, &udtName); err != nil {
			return fmt.Errorf("scan registered table column types: %w", err)
		}
		tableKey := Key(schemaName, tableName)
		cols := columnTypes[tableKey]
		if cols == nil {
			cols = make(map[string]string)
			columnTypes[tableKey] = cols
		}
		cols[columnName] = udtName
	}
	if colRows.Err() != nil {
		return fmt.Errorf("iterate registered table column types: %w", colRows.Err())
	}

	for i := range s.config.RegisteredTables {
		tbl := &s.config.RegisteredTables[i]
		tableKey := tbl.normalizedKey()
		pk := primaryKeys[tableKey]
		if pk.count != 1 || pk.first == "" {
			return unsupportedSchemaf("registered table %s must have a single-column primary key for the current server runtime", tableKey)
		}

		effective := tbl.effectiveSyncKeyColumns(pk.first)
		if len(effective) != 1 {
			return unsupportedSchemaf("registered table %s must resolve to exactly one sync key column for the current server runtime", tableKey)
		}
		if effective[0] != pk.first {
			return unsupportedSchemaf("registered table %s declares sync key column %s, but the current server runtime requires the table primary key %s", tableKey, effective[0], pk.first)
		}

		cols := columnTypes[tableKey]
		if cols == nil {
			return fmt.Errorf("registered table %s could not load column metadata for sync key validation", tableKey)
		}
		if cols[pk.first] != "uuid" {
			return unsupportedSchemaf("registered table %s uses unsupported sync key column type %s for %s; the current server runtime requires uuid", tableKey, cols[pk.first], pk.first)
		}

		tbl.SyncKeyColumns = []string{pk.first}
	}

	s.columnTypesByTable = make(map[string]map[string]string, len(columnTypes))
	for tableKey, cols := range columnTypes {
		cloned := make(map[string]string, len(cols))
		for columnName, udtName := range cols {
			cloned[columnName] = udtName
		}
		s.columnTypesByTable[tableKey] = cloned
	}

	return nil
}

// Close gracefully shuts down the sync service.
// It rejects new runtime operations, waits for in-flight work to drain, and is safe to call multiple times.
// Note: This does NOT close the database pool - the caller is responsible for pool lifecycle.
func (s *SyncService) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	switch s.lifecycle {
	case serviceLifecycleClosed:
		s.mu.Unlock()
		return nil
	case serviceLifecycleRunning:
		s.logger.Debug("Shutting down sync service")
		s.lifecycle = serviceLifecycleShuttingDown
		if s.inFlightOps == 0 {
			s.lifecycle = serviceLifecycleClosed
			s.mu.Unlock()
			s.logger.Debug("Sync service shutdown complete")
			return nil
		}
		if s.drainedCh == nil {
			s.drainedCh = make(chan struct{})
		}
	case serviceLifecycleShuttingDown:
		if s.inFlightOps == 0 {
			s.lifecycle = serviceLifecycleClosed
			s.mu.Unlock()
			return nil
		}
		if s.drainedCh == nil {
			s.drainedCh = make(chan struct{})
		}
	}
	drainedCh := s.drainedCh
	s.mu.Unlock()

	select {
	case <-drainedCh:
		s.logger.Debug("Sync service shutdown complete")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for in-flight operations to drain: %w", ctx.Err())
	}
}

// Pool returns the underlying database connection pool
// This allows advanced users to execute custom queries
func (s *SyncService) Pool() *pgxpool.Pool {
	return s.pool
}

// IsTableRegistered checks if a schema.table combination is registered for sync operations
func (s *SyncService) IsTableRegistered(schemaName, tableName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := schemaName + "." + tableName
	return s.registeredTables[key]
}

func (s *SyncService) beginOperation() (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.lifecycle {
	case serviceLifecycleShuttingDown:
		return nil, errServiceShuttingDown
	case serviceLifecycleClosed:
		return nil, errors.New("sync service has been closed")
	}

	s.inFlightOps++

	var once sync.Once
	return func() {
		once.Do(func() {
			s.finishOperation()
		})
	}, nil
}

func (s *SyncService) finishOperation() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inFlightOps > 0 {
		s.inFlightOps--
	}
	if s.inFlightOps == 0 && s.lifecycle == serviceLifecycleShuttingDown {
		s.lifecycle = serviceLifecycleClosed
		if s.drainedCh != nil {
			close(s.drainedCh)
			s.drainedCh = nil
		}
	}
}

func (s *SyncService) lifecycleSnapshot() (serviceLifecycleState, int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lifecycle, s.inFlightOps, s.lifecycle == serviceLifecycleRunning
}

type operationalInvariantSnapshot struct {
	UserStateRetentionFloorAheadCount int64
	LatestBundleSeqMax                int64
	RetainedBundleFloorMin            int64
	RetainedBundleFloorMax            int64
	RetainedBundleWindowMin           int64
	RetainedBundleWindowMax           int64
	HistoryPrunedErrorCount           int64
	AcceptedPushReplayCount           int64
	RejectedRegisteredWriteCount      int64
	CommittedBundleCount              int64
	CommittedBundleBytes              int64
}

func (s *SyncService) operationalInvariantStats(ctx context.Context) (*operationalInvariantSnapshot, error) {
	if s.pool == nil {
		return &operationalInvariantSnapshot{}, nil
	}

	var stats operationalInvariantSnapshot
	err := s.pool.QueryRow(ctx, `
		SELECT
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COUNT(*) FROM sync.user_state WHERE retained_bundle_floor > next_bundle_seq - 1)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MAX(GREATEST(next_bundle_seq - 1, 0)), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MIN(retained_bundle_floor), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MAX(retained_bundle_floor), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MIN(GREATEST((next_bundle_seq - 1) - retained_bundle_floor, 0)), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.user_state') IS NULL THEN 0
				ELSE (SELECT COALESCE(MAX(GREATEST((next_bundle_seq - 1) - retained_bundle_floor, 0)), 0) FROM sync.user_state)
			END,
			CASE
				WHEN to_regclass('sync.history_pruned_error_seq') IS NULL THEN 0
				ELSE (
					SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
					FROM sync.history_pruned_error_seq
				)
			END,
			CASE
				WHEN to_regclass('sync.accepted_push_replay_seq') IS NULL THEN 0
				ELSE (
					SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
					FROM sync.accepted_push_replay_seq
				)
			END,
			CASE
				WHEN to_regclass('sync.rejected_registered_write_seq') IS NULL THEN 0
				ELSE (
					SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
					FROM sync.rejected_registered_write_seq
				)
			END,
			CASE
				WHEN to_regclass('sync.bundle_log') IS NULL THEN 0
				ELSE (SELECT COUNT(*) FROM sync.bundle_log)
			END,
			CASE
				WHEN to_regclass('sync.bundle_log') IS NULL THEN 0
				ELSE (SELECT COALESCE(SUM(byte_count), 0) FROM sync.bundle_log)
			END
	`).Scan(
		&stats.UserStateRetentionFloorAheadCount,
		&stats.LatestBundleSeqMax,
		&stats.RetainedBundleFloorMin,
		&stats.RetainedBundleFloorMax,
		&stats.RetainedBundleWindowMin,
		&stats.RetainedBundleWindowMax,
		&stats.HistoryPrunedErrorCount,
		&stats.AcceptedPushReplayCount,
		&stats.RejectedRegisteredWriteCount,
		&stats.CommittedBundleCount,
		&stats.CommittedBundleBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("query operational invariant stats: %w", err)
	}
	return &stats, nil
}

// GetStatus returns the current service lifecycle and bundle-era operability snapshot.
func (s *SyncService) GetStatus(ctx context.Context) (*StatusResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	lifecycle, inFlightOps, accepting := s.lifecycleSnapshot()
	invariantStats, err := s.operationalInvariantStats(ctx)
	if err != nil {
		return nil, err
	}

	status := "healthy"
	switch lifecycle {
	case serviceLifecycleShuttingDown, serviceLifecycleClosed:
		status = "unhealthy"
	default:
		if invariantStats.UserStateRetentionFloorAheadCount > 0 {
			status = "unhealthy"
		}
	}

	caps := s.GetCapabilities()
	return &StatusResponse{
		Status:                            status,
		Version:                           caps.ProtocolVersion,
		AppName:                           caps.AppName,
		Lifecycle:                         string(lifecycle),
		AcceptingOperations:               accepting,
		InFlightOperations:                inFlightOps,
		RegisteredTables:                  caps.RegisteredTables,
		Features:                          caps.Features,
		UserStateRetentionFloorAheadCount: invariantStats.UserStateRetentionFloorAheadCount,
		LatestBundleSeqMax:                invariantStats.LatestBundleSeqMax,
		RetainedBundleFloorMin:            invariantStats.RetainedBundleFloorMin,
		RetainedBundleFloorMax:            invariantStats.RetainedBundleFloorMax,
		RetainedBundleWindowMin:           invariantStats.RetainedBundleWindowMin,
		RetainedBundleWindowMax:           invariantStats.RetainedBundleWindowMax,
		HistoryPrunedErrorCount:           invariantStats.HistoryPrunedErrorCount,
		AcceptedPushReplayCount:           invariantStats.AcceptedPushReplayCount,
		RejectedRegisteredWriteCount:      invariantStats.RejectedRegisteredWriteCount,
		CommittedBundleCount:              invariantStats.CommittedBundleCount,
		CommittedBundleBytes:              invariantStats.CommittedBundleBytes,
	}, nil
}

// GetSchemaVersion returns the current schema version
func (s *SyncService) GetSchemaVersion() int {
	return s.config.MaxSupportedSchemaVersion
}

// GetCapabilities returns the currently supported sync protocol surface.
func (s *SyncService) GetCapabilities() CapabilitiesResponse {
	features := map[string]bool{
		"bundle_pull":                         true,
		"push_session_chunking":               true,
		"committed_bundle_row_fetch":          true,
		"snapshot_chunking":                   true,
		"status_endpoint":                     true,
		"graceful_shutdown":                   true,
		"capabilities_endpoint":               true,
		"server_checkpoint_tracking":          false,
		"history_pruned_errors":               true,
		"bundle_push":                         true,
		"structured_sync_keys":                true,
		"registered_write_rejection_enforced": true,
		"accepted_push_replay_visibility":     true,
		"committed_bundle_visibility":         true,
		"retained_floor_visibility":           true,
		"retained_window_visibility":          true,
		"history_pruned_visibility":           true,
	}

	tables := make([]string, 0, len(s.config.RegisteredTables))
	specs := make([]RegisteredTableSpec, 0, len(s.config.RegisteredTables))
	for _, tbl := range s.config.RegisteredTables {
		tables = append(tables, tbl.normalizedKey())
		specs = append(specs, RegisteredTableSpec{
			Schema:         tbl.normalizedSchema(),
			Table:          tbl.normalizedTable(),
			SyncKeyColumns: tbl.normalizedSyncKeyColumns(),
		})
	}
	slices.Sort(tables)
	slices.SortFunc(specs, func(a, b RegisteredTableSpec) int {
		left := a.Schema + "." + a.Table
		right := b.Schema + "." + b.Table
		return strings.Compare(left, right)
	})

	return CapabilitiesResponse{
		ProtocolVersion:      SyncProtocolVersion,
		SchemaVersion:        s.GetSchemaVersion(),
		AppName:              s.config.AppName,
		RegisteredTables:     tables,
		RegisteredTableSpecs: specs,
		Features:             features,
		BundleLimits: &BundleCapabilitiesLimits{
			MaxRowsPerBundle:                   s.config.MaxRowsPerBundle,
			MaxBytesPerBundle:                  s.config.MaxBytesPerBundle,
			MaxBundlesPerPull:                  defaultMaxBundlesPerPull,
			DefaultRowsPerPushChunk:            s.defaultRowsPerPushChunk(),
			MaxRowsPerPushChunk:                s.maxRowsPerPushChunk(),
			PushSessionTTLSeconds:              int(s.pushSessionTTL().Seconds()),
			DefaultRowsPerCommittedBundleChunk: s.defaultRowsPerCommittedBundleChunk(),
			MaxRowsPerCommittedBundleChunk:     s.maxRowsPerCommittedBundleChunk(),
			DefaultRowsPerSnapshotChunk:        s.defaultRowsPerSnapshotChunk(),
			MaxRowsPerSnapshotChunk:            s.maxRowsPerSnapshotChunk(),
			SnapshotSessionTTLSeconds:          int(s.snapshotSessionTTL().Seconds()),
			MaxRowsPerSnapshotSession:          s.maxRowsPerSnapshotSession(),
			MaxBytesPerSnapshotSession:         s.maxBytesPerSnapshotSession(),
		},
	}
}

func (s *SyncService) uploadLockTimeoutMillis() (int64, bool) {
	if s == nil || s.config == nil || s.config.UploadLockTimeout <= 0 {
		return 0, false
	}
	ms := s.config.UploadLockTimeout.Milliseconds()
	if ms == 0 {
		ms = 1
	}
	return ms, true
}

func (s *SyncService) configureUploadTx(ctx context.Context, tx pgx.Tx) error {
	lockTimeoutMs, ok := s.uploadLockTimeoutMillis()
	if !ok {
		return nil
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockTimeoutMs)); err != nil {
		return fmt.Errorf("failed to set upload lock timeout: %w", err)
	}
	return nil
}
