// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaterializationHandler interface for materializing changes to business tables
type MaterializationHandler interface {
	// ApplyUpsert materializes an INSERT or UPDATE operation to the business table
	// Must be idempotent - safe to call multiple times for the same (schema, table, pk, server_version)
	ApplyUpsert(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID, payload []byte) error

	// ApplyDelete materializes a DELETE operation to the business table
	// Must be idempotent - safe to call multiple times for the same (schema, table, pk)
	ApplyDelete(ctx context.Context, tx pgx.Tx, schema, table string, pk uuid.UUID) error

	// ConvertReferenceKey converts a key value from the payload format to the database format
	// for foreign key validation. This is optional - return the original value and nil error
	// if no conversion is needed. This is useful when payload keys are encoded (e.g., base64)
	// but need to be decoded for database comparison.
	// Returns the converted value and nil if conversion was successful,
	// the original value and nil if no conversion is needed,
	// or any value and an error if conversion failed due to parsing errors.
	ConvertReferenceKey(fieldName string, payloadValue any) (any, error)
}

// RegisteredTable represents a table that is registered for sync operations
type RegisteredTable struct {
	Schema  string                 `json:"schema"` // Schema name (e.g., "public", "crm", "business")
	Table   string                 `json:"table"`  // Table name (e.g., "users", "posts")
	Handler MaterializationHandler `json:"-"`      // Optional handler for materializing changes to business tables
}

// SyncService provides the core synchronization functionality
// This is the main SDK component that developers integrate into their applications
type SyncService struct {
	pool             *pgxpool.Pool
	logger           *slog.Logger
	config           *ServiceConfig
	tableHandlers    map[string]MaterializationHandler // Two-way sync table handlers
	registeredTables map[string]bool                   // Set of "schema.table" combinations allowed in sync operations

	// Schema discovery for batch upload improvements
	discoveredSchema *DiscoveredSchema

	// Cleanup tracking
	mu     sync.RWMutex
	closed bool
}

// ServiceConfig holds configuration for the sync service
type ServiceConfig struct {
	MaxSupportedSchemaVersion int               // Current schema version to return
	AppName                   string            // Application name for connection tracking
	RegisteredTables          []RegisteredTable // Schema.table combinations allowed for sync (required)
	DisableAutoMigrateFKs     bool              // Whether to disable the automatic migration of FKs to deferrable

	MaxUploadBatchSize int // Maximum number of changes allowed in a single upload (0 = unlimited)
	MaxPayloadBytes    int // Maximum JSON payload size per change in bytes (0 = unlimited)
}

// NewSyncService creates a new sync service instance from an existing pool
// This is the main entry point for SDK users who already have a connection pool
func NewSyncService(pool *pgxpool.Pool, config *ServiceConfig, logger *slog.Logger) (*SyncService, error) {
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
		pool:             pool,
		logger:           logger,
		config:           config,
		tableHandlers:    make(map[string]MaterializationHandler),
		registeredTables: make(map[string]bool),
	}

	// Initialize registered tables set and handlers
	for _, regTable := range config.RegisteredTables {
		// Normalize keys to lowercase to match request validation normalization
		key := strings.ToLower(regTable.Schema) + "." + strings.ToLower(regTable.Table)
		service.registeredTables[key] = true

		// Register handler if provided
		if regTable.Handler != nil {
			service.tableHandlers[key] = regTable.Handler
			logger.Debug("Registered table handler from config", "schema", regTable.Schema, "table", regTable.Table, "key", key)
		}
	}

	// Initialize database schema and FK migration atomically
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		// Initialize schema within transaction
		if err := service.initializeSchemaInTx(ctx, tx); err != nil {
			logger.Error("Failed to initialize database schema", "error", err)
			return err
		}
		logger.Debug("Database schema initialized successfully")

		// Auto-migrate foreign keys to deferrable if not disabled
		if !config.DisableAutoMigrateFKs {
			if err := service.autoMigrateForeignKeysInTx(ctx, tx); err != nil {
				logger.Warn("Failed to auto-migrate foreign keys", "error", err)
				// Don't fail service creation - FK migration is optional
				// But log it as a warning since it's in a transaction
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize sync service: %w", err)
	}

	// Discover schema relationships for batch upload improvements.
	// Run this outside the initialization transaction to avoid pool self-deadlocks (e.g. max_conns=1)
	// and to reflect any committed FK migrations.
	if err := service.discoverSchemaRelationships(ctx); err != nil {
		return nil, fmt.Errorf("failed to discover schema relationships: %w", err)
	}

	return service, nil
}

// Close gracefully shuts down the sync service
// It's safe to call multiple times and will wait for ongoing operations to complete
// Note: This does NOT close the database pool - the caller is responsible for pool lifecycle
func (s *SyncService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil // Already closed
	}

	s.logger.Debug("Shutting down sync service")

	// Clear table handlers to prevent further use
	s.tableHandlers = nil

	s.closed = true
	s.logger.Debug("Sync service shutdown complete")
	return nil
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

// checkClosed returns an error if the service has been closed
func (s *SyncService) checkClosed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errors.New("sync service has been closed")
	}
	return nil
}

// ProcessUpload handles a batch upload request with improved ordering, FK precheck, and SAVEPOINTs
func (s *SyncService) ProcessUpload(ctx context.Context, userID, sourceID string, req *UploadRequest) (*UploadResponse, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}

	if len(req.Changes) == 0 {
		return &UploadResponse{
			Accepted:         true,
			HighestServerSeq: s.getUserHighestServerSeq(ctx, userID),
			Statuses:         []ChangeUploadStatus{},
		}, nil
	}

	// Enforce upload batch size limit (fail early with invalid.bad_payload per change)
	if s.config.MaxUploadBatchSize > 0 && len(req.Changes) > s.config.MaxUploadBatchSize {
		statuses := make([]ChangeUploadStatus, len(req.Changes))
		for i, ch := range req.Changes {
			msg := fmt.Errorf("batch too large: changes=%d limit=%d", len(req.Changes), s.config.MaxUploadBatchSize)
			statuses[i] = statusInvalidOther(ch.SourceChangeID, ReasonBatchTooLarge, msg)
		}
		// Entire batch is rejected to prevent clients from dropping pending changes.
		accepted := false

		return &UploadResponse{
			Accepted:         accepted,
			HighestServerSeq: s.getUserHighestServerSeq(ctx, userID),
			Statuses:         statuses,
		}, nil
	}

	var statuses []ChangeUploadStatus

	// Process changes in a transaction using pgx.BeginTxFunc at REPEATABLE READ
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadWrite}, func(tx pgx.Tx) error {
		// Defer FK checks to COMMIT (RI evaluated against this tx snapshot) and run the tx under REPEATABLE READ so cross-session parents committed later cannot "rescue" missing FKs in this batch.
		_, err := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED")
		if err != nil {
			return fmt.Errorf("failed to set constraints deferred: %w", err)
		}
		// Optional: bound lock wait times during stress
		_, _ = tx.Exec(ctx, "SET LOCAL lock_timeout = '3s'")

		// Prepare hot-path statements to reduce parse/plan overhead for per-item operations
		if err := s.prepareUploadStatements(ctx, tx); err != nil {
			return fmt.Errorf("failed to prepare statements: %w", err)
		}

		// Step 1: Split and order changes
		upserts, deletes := s.splitAndOrder(req.Changes)

		// Step 2: Convert primary keys using table handlers (skip invalid UUIDs, they'll be caught in validation)
		s.convertPrimaryKeysSkipInvalid(upserts)
		s.convertPrimaryKeysSkipInvalid(deletes)

		// Step 3: Build batch PK index for FK precheck
		inBatch := buildBatchPKIndex(upserts, deletes)

		// Step 4: Process upserts (parent-first)
		upsertStatuses, err := s.processUpserts(ctx, tx, userID, sourceID, upserts, inBatch)
		if err != nil {
			return fmt.Errorf("failed to process upserts: %w", err)
		}

		// Step 5: Process deletes (child-first)
		deleteStatuses, err := s.processDeletes(ctx, tx, userID, sourceID, deletes)
		if err != nil {
			return fmt.Errorf("failed to process deletes: %w", err)
		}

		// Step 6: Combine statuses in original order with safety guard
		statuses = make([]ChangeUploadStatus, len(req.Changes))
		statusMap := make(map[int64]ChangeUploadStatus)
		for _, status := range upsertStatuses {
			statusMap[status.SourceChangeID] = status
		}
		for _, status := range deleteStatuses {
			statusMap[status.SourceChangeID] = status
		}

		for i, change := range req.Changes {
			st, ok := statusMap[change.SourceChangeID]
			if !ok {
				st = statusInternalError(change.SourceChangeID, fmt.Errorf("missing status for scid"))
			}
			statuses[i] = st
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to process upload transaction: %w", err)
	}

	// Decide batch acceptance: mark false if any status signals unregistered table
	accepted := true
	for _, st := range statuses {
		if st.Status == StInvalid {
			if reason, ok := st.Invalid["reason"].(string); ok && reason == ReasonUnregisteredTable {
				accepted = false
				break
			}
		}
	}

	return &UploadResponse{
		Accepted:         accepted,
		HighestServerSeq: s.getUserHighestServerSeq(ctx, userID),
		Statuses:         statuses,
	}, nil
}

// convertPrimaryKeysSkipInvalid converts primary keys for all changes using uuid.Parse, skipping invalid ones
func (s *SyncService) convertPrimaryKeysSkipInvalid(changes []ChangeUpload) {
	for i := range changes {
		change := &changes[i]

		// Convert primary key using uuid.Parse directly, skip invalid ones (they'll be caught in validation)
		if convertedPK, err := uuid.Parse(change.PK); err == nil {
			// Update the change with the converted UUID string
			change.PK = convertedPK.String()
		}
		// Invalid UUIDs are left as-is and will be caught during validation
	}
}

// GetSchemaVersion returns the current schema version
func (s *SyncService) GetSchemaVersion() int {
	return s.config.MaxSupportedSchemaVersion
}
