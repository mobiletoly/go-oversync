// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// initializeSchema creates the required sync tables if they don't exist
func (s *SyncService) initializeSchema(ctx context.Context) error {
	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return s.initializeSchemaInTx(ctx, tx)
	}); err != nil {
		return err
	}

	// Run schema discovery outside the schema DDL transaction to avoid pool self-deadlocks
	// (e.g., when max_conns=1) and to observe a committed view of constraints.
	return s.discoverSchemaRelationships(ctx)
}

// initializeSchemaInTx creates the required sync tables within an existing transaction
func (s *SyncService) initializeSchemaInTx(ctx context.Context, tx pgx.Tx) error {
	migrations := []string{
		// Multi-schema sidecar sync schema based on 10_sidecar_with_schemas_v3

		// Create dedicated sync schema
		/*language=postgresql*/ `CREATE SCHEMA IF NOT EXISTS sync`,

		// 1) Per-row concurrency + lifecycle (user-scoped)
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.sync_row_meta (
			user_id        TEXT      NOT NULL,
			schema_name    TEXT      NOT NULL,
			table_name     TEXT      NOT NULL,
			pk_uuid        UUID      NOT NULL,
			server_version BIGINT    NOT NULL DEFAULT 0,
			deleted        BOOLEAN   NOT NULL DEFAULT FALSE,
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, schema_name, table_name, pk_uuid)
		)`,

		// 2) Current after-image (for snapshots/materialization) - user-scoped
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.sync_state (
			user_id      TEXT   NOT NULL,
			schema_name  TEXT   NOT NULL,
			table_name   TEXT   NOT NULL,
			pk_uuid      UUID   NOT NULL,
			payload      JSON   NOT NULL,
			PRIMARY KEY (user_id, schema_name, table_name, pk_uuid)
		)`,
		// Migrate payload to JSON (write-optimized) if previous schema used JSONB.
		/*language=postgresql*/ `DO $$
		BEGIN
		  IF EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'sync'
			  AND table_name = 'sync_state'
			  AND column_name = 'payload'
			  AND data_type = 'jsonb'
		  ) THEN
			ALTER TABLE sync.sync_state
			  ALTER COLUMN payload TYPE json
			  USING payload::json;
		  END IF;
		END $$;`,

		// 3) Distribution log (idempotency & download stream) - user-scoped
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.server_change_log (
			server_id        BIGSERIAL PRIMARY KEY,
			user_id          TEXT      NOT NULL,
			schema_name      TEXT      NOT NULL,
			table_name       TEXT      NOT NULL,
			op               TEXT      NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			pk_uuid          UUID      NOT NULL,
			payload          JSON,
			source_id        TEXT      NOT NULL,
			source_change_id BIGINT    NOT NULL,
			server_version   BIGINT    NOT NULL DEFAULT 0,
			ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (user_id, source_id, source_change_id),
			CONSTRAINT server_change_payload_by_op_chk
  			CHECK ((op = 'DELETE' AND payload IS NULL) OR (op IN ('INSERT','UPDATE') AND payload IS NOT NULL))
		)`,
		// Migrate payload to JSON (write-optimized) if previous schema used JSONB.
		/*language=postgresql*/ `DO $$
		BEGIN
		  IF EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'sync'
			  AND table_name = 'server_change_log'
			  AND column_name = 'payload'
			  AND data_type = 'jsonb'
		  ) THEN
			ALTER TABLE sync.server_change_log
			  ALTER COLUMN payload TYPE json
			  USING payload::json;
		  END IF;
		END $$;`,

		// Indexes for performance (user-scoped)
		// Drop redundant historical indexes (safe even if absent).
		// - server_id is already indexed by the PRIMARY KEY.
		// - scl_user_pk_seq_idx covers the (user_id,schema_name,table_name,pk_uuid) prefix.
		`DROP INDEX IF EXISTS sync.scl_seq_idx`,
		`DROP INDEX IF EXISTS sync.scl_triplet_idx`,
		`CREATE INDEX IF NOT EXISTS scl_user_seq_idx ON sync.server_change_log(user_id, server_id)`,                     // Optimizes per-user tail-follow downloads
		`CREATE INDEX IF NOT EXISTS scl_user_schema_seq_idx ON sync.server_change_log(user_id, schema_name, server_id)`, // Optimizes schema-filtered paging
		`CREATE INDEX IF NOT EXISTS scl_user_pk_seq_idx ON sync.server_change_log(user_id, schema_name, table_name, pk_uuid, server_id)`,
		`CREATE INDEX IF NOT EXISTS scl_user_delete_seq_idx ON sync.server_change_log(user_id, server_id) WHERE op='DELETE'`,

		// 4) Materializer failure log (for diagnostics and retries)
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.materialize_failures (
            id BIGSERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            schema_name TEXT NOT NULL,
            table_name  TEXT NOT NULL,
            pk_uuid UUID NOT NULL,
            attempted_version BIGINT NOT NULL,
            op TEXT NOT NULL,
            payload JSON,
            error TEXT NOT NULL,
            first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
            retry_count INT NOT NULL DEFAULT 0,
            UNIQUE (user_id, schema_name, table_name, pk_uuid, attempted_version)
        )`,
		// Migrate payload to JSON (write-optimized) if previous schema used JSONB.
		/*language=postgresql*/ `DO $$
		BEGIN
		  IF EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'sync'
			  AND table_name = 'materialize_failures'
			  AND column_name = 'payload'
			  AND data_type = 'jsonb'
		  ) THEN
			ALTER TABLE sync.materialize_failures
			  ALTER COLUMN payload TYPE json
			  USING payload::json;
		  END IF;
		END $$;`,
		`CREATE INDEX IF NOT EXISTS mf_user_table_idx ON sync.materialize_failures(user_id, schema_name, table_name)`,
	}

	// Run all migrations within the provided transaction
	for i, migration := range migrations {
		s.logger.Debug("Running sidecar migration", "step", i+1, "total", len(migrations))
		if _, err := tx.Exec(ctx, migration); err != nil {
			return fmt.Errorf("sidecar migration %d failed: %w", i+1, err)
		}
	}
	s.logger.Info("Sidecar schema initialized successfully", "migrations", len(migrations))

	return nil
}

// discoverSchemaRelationships analyzes registered tables and builds dependency graph
func (s *SyncService) discoverSchemaRelationships(ctx context.Context) error {
	if len(s.registeredTables) == 0 {
		s.logger.Info("No registered tables, skipping schema discovery")
		return nil
	}

	discovery := NewSchemaDiscovery(s.pool, s.logger)
	if s.config != nil && strings.TrimSpace(s.config.TenantScopeColumn) != "" {
		discovery.SetTenantScopeColumn(s.config.TenantScopeColumn)
	}
	discoveredSchema, err := discovery.DiscoverSchemaWithDependencyOverrides(ctx, s.registeredTables, s.config.DependencyOverrides)
	if err != nil {
		return fmt.Errorf("failed to discover schema relationships: %w", err)
	}

	if err := s.validateTenantScopeColumn(ctx, discoveredSchema); err != nil {
		return err
	}

	s.discoveredSchema = discoveredSchema
	s.logger.Info("Schema relationships discovered",
		"table_count", len(discoveredSchema.TableOrder),
		"fk_constraints", len(discoveredSchema.FKMap),
		"table_order", discoveredSchema.TableOrder)

	return nil
}

func (s *SyncService) validateTenantScopeColumn(ctx context.Context, discoveredSchema *DiscoveredSchema) error {
	if s.config == nil {
		return nil
	}
	if s.config.FKPrecheckMode == FKPrecheckDisabled {
		return nil
	}

	tenantCol := strings.ToLower(strings.TrimSpace(s.config.TenantScopeColumn))
	if tenantCol == "" {
		return nil
	}
	if !isValidColumnName(tenantCol) {
		return fmt.Errorf("TenantScopeColumn must match ^[a-z0-9_]+$, got %q", s.config.TenantScopeColumn)
	}

	// Validate tenant column exists on all registered tables and all FK parent tables referenced by precheck.
	toCheck := make(map[string]struct{}, len(s.registeredTables))
	for k := range s.registeredTables {
		toCheck[k] = struct{}{}
	}
	if discoveredSchema != nil {
		for _, fks := range discoveredSchema.FKMap {
			for _, fk := range fks {
				toCheck[Key(fk.RefSchema, fk.RefTable)] = struct{}{}
			}
		}
	}
	if len(toCheck) == 0 {
		return nil
	}

	schemas := make([]string, 0, len(toCheck))
	tables := make([]string, 0, len(toCheck))
	for st := range toCheck {
		parts := strings.SplitN(st, ".", 2)
		if len(parts) != 2 {
			continue
		}
		schemas = append(schemas, parts[0])
		tables = append(tables, parts[1])
	}
	if len(schemas) == 0 {
		return nil
	}

	query := `
		WITH t AS (
			SELECT * FROM unnest(@schemas::text[], @tables::text[]) AS x(schema_name, table_name)
		)
		SELECT t.schema_name, t.table_name
		FROM t
		LEFT JOIN information_schema.columns c
		  ON c.table_schema = t.schema_name
		 AND c.table_name = t.table_name
		 AND lower(c.column_name) = @col::text
		WHERE c.column_name IS NULL
		ORDER BY 1, 2`

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{
		"schemas": schemas,
		"tables":  tables,
		"col":     tenantCol,
	})
	if err != nil {
		return fmt.Errorf("failed to validate TenantScopeColumn %q: %w", tenantCol, err)
	}
	defer rows.Close()

	var missing []string
	for rows.Next() {
		var schemaName, tableName string
		if scanErr := rows.Scan(&schemaName, &tableName); scanErr != nil {
			return fmt.Errorf("failed to scan tenant scope column validation row: %w", scanErr)
		}
		missing = append(missing, Key(schemaName, tableName))
	}
	if rows.Err() != nil {
		return fmt.Errorf("tenant scope column validation failed: %w", rows.Err())
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("TenantScopeColumn=%q is configured, but the column is missing on: %s", tenantCol, strings.Join(missing, ", "))
	}

	return nil
}

// autoMigrateForeignKeys automatically migrates FKs to deferrable for registered tables
func (s *SyncService) autoMigrateForeignKeys(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return s.autoMigrateForeignKeysInTx(ctx, tx)
	})
}

// autoMigrateForeignKeysInTx automatically migrates FKs to deferrable within an existing transaction
func (s *SyncService) autoMigrateForeignKeysInTx(ctx context.Context, tx pgx.Tx) error {
	if len(s.config.RegisteredTables) == 0 {
		s.logger.Info("No registered tables, skipping FK migration")
		return nil
	}

	s.logger.Info("Auto-migrating foreign keys to deferrable",
		"tables", s.config.RegisteredTables)

	// Create FK manager with specific schema.table combinations
	fkManager := NewDeferrableFKManager(s.pool, s.logger, s.config.RegisteredTables)
	if err := fkManager.MigrateToDeferredInTx(ctx, tx); err != nil {
		return fmt.Errorf("failed to migrate FKs to deferrable: %w", err)
	}

	return nil
}
