// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const syncBootstrapLockKey int64 = 0x6f76657273796e63

const (
	syncSchemaProtocolLabel       = "v1"
	syncSchemaLayoutName          = "server_postgres_sync_v1"
	syncKeyKindUUIDCode     int16 = 1
	syncKeyKindTextCode     int16 = 2
)

type syncCatalogQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type expectedTableCatalogRow struct {
	TableID       int32
	SchemaName    string
	TableName     string
	SyncKeyColumn string
	SyncKeyKind   int16
}

func syncKeyKindCode(syncKeyType string) (int16, error) {
	switch syncKeyType {
	case syncKeyTypeUUID:
		return syncKeyKindUUIDCode, nil
	case syncKeyTypeText:
		return syncKeyKindTextCode, nil
	default:
		return 0, fmt.Errorf("unsupported sync key type %q", syncKeyType)
	}
}

func (s *SyncService) expectedTableCatalogRows() ([]expectedTableCatalogRow, error) {
	if s == nil || s.config == nil {
		return nil, nil
	}

	rows := make([]expectedTableCatalogRow, 0, len(s.registeredTableByID))
	for tableID := int32(1); tableID <= int32(len(s.registeredTableByID)); tableID++ {
		info, ok := s.registeredTableByID[tableID]
		if !ok {
			return nil, fmt.Errorf("missing registered table runtime info for table_id %d", tableID)
		}
		rows = append(rows, expectedTableCatalogRow{
			TableID:       tableID,
			SchemaName:    info.schemaName,
			TableName:     info.tableName,
			SyncKeyColumn: info.syncKeyColumn,
			SyncKeyKind:   info.syncKeyKind,
		})
	}

	return rows, nil
}

func syncSchemaObjectsExist(ctx context.Context, q syncCatalogQuerier) (bool, error) {
	var exists bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'sync'
		)
	`).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect sync schema objects: %w", err)
	}
	return exists, nil
}

func validateSyncMeta(ctx context.Context, q syncCatalogQuerier) error {
	rows, err := q.Query(ctx, `
		SELECT singleton_key, protocol_label, layout_name
		FROM sync.meta
		ORDER BY singleton_key DESC
	`)
	if err != nil {
		return unsupportedSchemaf("existing sync.meta layout is unsupported: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
		var (
			singletonKey bool
			protocol     string
			layoutName   string
		)
		if err := rows.Scan(&singletonKey, &protocol, &layoutName); err != nil {
			return unsupportedSchemaf("scan sync.meta row: %v", err)
		}
		if !singletonKey {
			return unsupportedSchemaf("sync.meta must contain singleton_key = true")
		}
		if protocol != syncSchemaProtocolLabel {
			return unsupportedSchemaf("sync.meta protocol_label = %q, expected %q", protocol, syncSchemaProtocolLabel)
		}
		if layoutName != syncSchemaLayoutName {
			return unsupportedSchemaf("sync.meta layout_name = %q, expected %q", layoutName, syncSchemaLayoutName)
		}
	}
	if err := rows.Err(); err != nil {
		return unsupportedSchemaf("iterate sync.meta rows: %v", err)
	}
	if rowCount != 1 {
		return unsupportedSchemaf("sync.meta must contain exactly one row, found %d", rowCount)
	}
	return nil
}

func validateSyncTableCatalog(ctx context.Context, q syncCatalogQuerier, expected []expectedTableCatalogRow) error {
	rows, err := q.Query(ctx, `
		SELECT table_id, schema_name, table_name, sync_key_column, sync_key_kind
		FROM sync.table_catalog
		ORDER BY table_id
	`)
	if err != nil {
		return unsupportedSchemaf("existing sync.table_catalog layout is unsupported: %v", err)
	}
	defer rows.Close()

	actual := make([]expectedTableCatalogRow, 0, len(expected))
	for rows.Next() {
		var row expectedTableCatalogRow
		if err := rows.Scan(&row.TableID, &row.SchemaName, &row.TableName, &row.SyncKeyColumn, &row.SyncKeyKind); err != nil {
			return unsupportedSchemaf("scan sync.table_catalog row: %v", err)
		}
		actual = append(actual, row)
	}
	if err := rows.Err(); err != nil {
		return unsupportedSchemaf("iterate sync.table_catalog rows: %v", err)
	}
	if len(actual) != len(expected) {
		return unsupportedSchemaf("sync.table_catalog row count = %d, expected %d", len(actual), len(expected))
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return unsupportedSchemaf(
				"sync.table_catalog row %d = (%d,%s,%s,%s,%d), expected (%d,%s,%s,%s,%d)",
				i,
				actual[i].TableID,
				actual[i].SchemaName,
				actual[i].TableName,
				actual[i].SyncKeyColumn,
				actual[i].SyncKeyKind,
				expected[i].TableID,
				expected[i].SchemaName,
				expected[i].TableName,
				expected[i].SyncKeyColumn,
				expected[i].SyncKeyKind,
			)
		}
	}
	return nil
}

func validateExistingSyncLayout(ctx context.Context, q syncCatalogQuerier, expectedCatalog []expectedTableCatalogRow) (bool, error) {
	var (
		metaExists         bool
		tableCatalogExists bool
	)
	if err := q.QueryRow(ctx, `
		SELECT
			to_regclass('sync.meta') IS NOT NULL,
			to_regclass('sync.table_catalog') IS NOT NULL
	`).Scan(&metaExists, &tableCatalogExists); err != nil {
		return false, fmt.Errorf("inspect sync schema marker tables: %w", err)
	}

	if !metaExists && !tableCatalogExists {
		exists, err := syncSchemaObjectsExist(ctx, q)
		if err != nil {
			return false, err
		}
		if !exists {
			return false, nil
		}
		return false, unsupportedSchemaf(
			"existing sync schema uses unsupported layout; drop and recreate the sync schema for layout %q",
			syncSchemaLayoutName,
		)
	}
	if !metaExists || !tableCatalogExists {
		return false, unsupportedSchemaf(
			"existing sync schema is missing required marker tables sync.meta and sync.table_catalog for layout %q",
			syncSchemaLayoutName,
		)
	}

	if err := validateSyncMeta(ctx, q); err != nil {
		return false, err
	}
	if err := validateSyncTableCatalog(ctx, q, expectedCatalog); err != nil {
		return false, err
	}
	return true, nil
}

func persistSyncLayoutMetadata(ctx context.Context, tx pgx.Tx, expectedCatalog []expectedTableCatalogRow) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.meta (singleton_key, protocol_label, layout_name)
		VALUES (TRUE, $1, $2)
	`, syncSchemaProtocolLabel, syncSchemaLayoutName); err != nil {
		return fmt.Errorf("insert sync.meta row: %w", err)
	}
	for _, row := range expectedCatalog {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.table_catalog (
				table_id, schema_name, table_name, sync_key_column, sync_key_kind
			)
			VALUES ($1, $2, $3, $4, $5)
		`, row.TableID, row.SchemaName, row.TableName, row.SyncKeyColumn, row.SyncKeyKind); err != nil {
			return fmt.Errorf("insert sync.table_catalog row %d: %w", row.TableID, err)
		}
	}
	return nil
}

// initializeSchemaInTx creates the required sync tables within an existing transaction
func (s *SyncService) initializeSchemaInTx(ctx context.Context, tx pgx.Tx) error {
	// Serialize sync DDL across concurrent service bootstraps sharing the same database.
	// `CREATE INDEX IF NOT EXISTS` is not sufficient to avoid deadlocks when multiple test
	// processes attempt the full migration list at the same time.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, syncBootstrapLockKey); err != nil {
		return fmt.Errorf("acquire sync bootstrap lock: %w", err)
	}
	expectedCatalog, err := s.expectedTableCatalogRows()
	if err != nil {
		return err
	}

	ready, err := validateExistingSyncLayout(ctx, tx, expectedCatalog)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	migrations := []string{
		// Create dedicated sync schema
		/*language=postgresql*/ `CREATE SCHEMA IF NOT EXISTS sync`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.meta (
			singleton_key BOOLEAN PRIMARY KEY CHECK (singleton_key),
			protocol_label TEXT NOT NULL DEFAULT 'v1',
			layout_name TEXT NOT NULL
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.table_catalog (
			table_id INTEGER PRIMARY KEY,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			sync_key_column TEXT NOT NULL,
			sync_key_kind SMALLINT NOT NULL,
			CONSTRAINT table_catalog_schema_table_key UNIQUE (schema_name, table_name),
			CONSTRAINT table_catalog_sync_key_kind_chk CHECK (sync_key_kind IN (1, 2))
		)`,

		// Current authoritative sync storage schema. These tables back the active server hot path.
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.user_state (
			user_pk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			user_id TEXT NOT NULL UNIQUE,
			next_bundle_seq BIGINT NOT NULL DEFAULT 1,
			retained_bundle_floor BIGINT NOT NULL DEFAULT 0,
			CONSTRAINT user_state_bundle_seq_chk CHECK (next_bundle_seq >= 1),
			CONSTRAINT user_state_retained_floor_chk CHECK (
				retained_bundle_floor >= 0
				AND retained_bundle_floor <= next_bundle_seq - 1
			)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.scope_state (
			user_pk BIGINT PRIMARY KEY REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			state_code SMALLINT NOT NULL,
			initializer_source_id TEXT,
			initialization_id UUID,
			lease_expires_at TIMESTAMPTZ,
			initialized_at TIMESTAMPTZ,
			initialized_by_source_id TEXT,
			CONSTRAINT scope_state_fields_chk CHECK (
				(
					state_code = 0
					AND initializer_source_id IS NULL
					AND initialization_id IS NULL
					AND lease_expires_at IS NULL
					AND initialized_at IS NULL
					AND initialized_by_source_id IS NULL
				) OR (
					state_code = 1
					AND initializer_source_id IS NOT NULL
					AND initialization_id IS NOT NULL
					AND lease_expires_at IS NOT NULL
					AND initialized_at IS NULL
					AND initialized_by_source_id IS NULL
				) OR (
					state_code = 2
					AND initializer_source_id IS NULL
					AND initialization_id IS NULL
					AND lease_expires_at IS NULL
					AND initialized_at IS NOT NULL
					AND initialized_by_source_id IS NOT NULL
				)
			),
			CONSTRAINT scope_state_code_chk CHECK (state_code IN (0, 1, 2))
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.source_state (
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			source_id TEXT NOT NULL,
			max_committed_source_bundle_id BIGINT NOT NULL,
			PRIMARY KEY (user_pk, source_id),
			CONSTRAINT source_state_max_committed_chk CHECK (max_committed_source_bundle_id >= 1)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.row_state (
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			table_id INTEGER NOT NULL REFERENCES sync.table_catalog(table_id),
			key_bytes BYTEA NOT NULL,
			bundle_seq BIGINT NOT NULL,
			deleted BOOLEAN NOT NULL DEFAULT FALSE,
			PRIMARY KEY (user_pk, table_id, key_bytes)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.bundle_capture_stage (
			capture_ordinal BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			txid BIGINT NOT NULL,
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			table_id INTEGER NOT NULL REFERENCES sync.table_catalog(table_id),
			op_code SMALLINT NOT NULL,
			key_bytes BYTEA NOT NULL,
			payload_db JSONB,
			CONSTRAINT bundle_capture_stage_op_code_chk CHECK (op_code IN (1, 2, 3)),
			CONSTRAINT bundle_capture_stage_payload_by_op_chk
				CHECK (
					(op_code = 3 AND payload_db IS NULL)
					OR (op_code IN (1, 2) AND payload_db IS NOT NULL)
				)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.bundle_log (
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			bundle_seq BIGINT NOT NULL,
			source_id TEXT NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			row_count BIGINT NOT NULL,
			byte_count BIGINT NOT NULL,
			bundle_hash BYTEA NOT NULL,
			committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_pk, bundle_seq),
			CONSTRAINT bundle_log_source_tuple_key UNIQUE (user_pk, source_id, source_bundle_id)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.bundle_rows (
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			bundle_seq BIGINT NOT NULL,
			row_ordinal BIGINT NOT NULL,
			table_id INTEGER NOT NULL REFERENCES sync.table_catalog(table_id),
			key_bytes BYTEA NOT NULL,
			op_code SMALLINT NOT NULL,
			payload_wire JSON,
			PRIMARY KEY (user_pk, bundle_seq, row_ordinal),
			CONSTRAINT bundle_rows_bundle_fk
				FOREIGN KEY (user_pk, bundle_seq) REFERENCES sync.bundle_log(user_pk, bundle_seq) ON DELETE CASCADE,
			CONSTRAINT bundle_rows_op_code_chk CHECK (op_code IN (1, 2, 3)),
			CONSTRAINT bundle_rows_payload_by_op_chk
				CHECK (
					(op_code = 3 AND payload_wire IS NULL)
					OR (op_code IN (1, 2) AND payload_wire IS NOT NULL)
				)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.push_sessions (
			push_id UUID PRIMARY KEY,
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			source_id TEXT NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			planned_row_count BIGINT NOT NULL,
			next_expected_row_ordinal BIGINT NOT NULL DEFAULT 0,
			initialization_id UUID,
			expires_at TIMESTAMPTZ NOT NULL,
			CONSTRAINT push_sessions_source_tuple_key UNIQUE (user_pk, source_id, source_bundle_id),
			CONSTRAINT push_sessions_row_count_chk CHECK (
				planned_row_count > 0
				AND next_expected_row_ordinal >= 0
				AND next_expected_row_ordinal <= planned_row_count
			)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.push_session_rows (
			push_id UUID NOT NULL REFERENCES sync.push_sessions(push_id) ON DELETE CASCADE,
			row_ordinal BIGINT NOT NULL,
			table_id INTEGER NOT NULL REFERENCES sync.table_catalog(table_id),
			key_bytes BYTEA NOT NULL,
			op_code SMALLINT NOT NULL,
			base_bundle_seq BIGINT NOT NULL,
			payload_apply JSON,
			PRIMARY KEY (push_id, row_ordinal),
			CONSTRAINT push_session_rows_op_code_chk CHECK (op_code IN (1, 2, 3)),
			CONSTRAINT push_session_rows_payload_by_op_chk
				CHECK (
					(op_code = 3 AND payload_apply IS NULL)
					OR (op_code IN (1, 2) AND payload_apply IS NOT NULL)
				)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.snapshot_sessions (
			snapshot_id UUID PRIMARY KEY,
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			snapshot_bundle_seq BIGINT NOT NULL,
			row_count BIGINT NOT NULL DEFAULT 0,
			byte_count BIGINT NOT NULL DEFAULT 0,
			expires_at TIMESTAMPTZ NOT NULL
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.snapshot_session_rows (
			snapshot_id UUID NOT NULL REFERENCES sync.snapshot_sessions(snapshot_id) ON DELETE CASCADE,
			row_ordinal BIGINT NOT NULL,
			table_id INTEGER NOT NULL REFERENCES sync.table_catalog(table_id),
			key_bytes BYTEA NOT NULL,
			bundle_seq BIGINT NOT NULL,
			payload_wire JSON NOT NULL,
			PRIMARY KEY (snapshot_id, row_ordinal),
			CONSTRAINT snapshot_session_rows_logical_row_key UNIQUE (snapshot_id, table_id, key_bytes)
		)`,
		`CREATE SEQUENCE IF NOT EXISTS sync.accepted_push_replay_seq`,
		`CREATE SEQUENCE IF NOT EXISTS sync.rejected_registered_write_seq`,
		`CREATE SEQUENCE IF NOT EXISTS sync.history_pruned_error_seq`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.enforce_registered_row_owner()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		DECLARE
			bundle_user_id TEXT;
		BEGIN
			bundle_user_id := NULLIF(current_setting('oversync.bundle_user_id', true), '');
			IF bundle_user_id IS NULL THEN
				PERFORM nextval('sync.rejected_registered_write_seq');
				RAISE EXCEPTION USING
					ERRCODE = 'P0001',
					MESSAGE = format(
						'write to registered table %I.%I requires oversync sync bundle context',
						TG_TABLE_SCHEMA,
						TG_TABLE_NAME
					);
			END IF;

			IF TG_OP = 'INSERT' THEN
				IF NEW._sync_scope_id IS NULL OR NEW._sync_scope_id = '' THEN
					NEW._sync_scope_id := bundle_user_id;
				ELSIF NEW._sync_scope_id <> bundle_user_id THEN
					RAISE EXCEPTION USING
						ERRCODE = 'P0001',
						MESSAGE = format(
							'scope mismatch on registered table %I.%I: actor %s cannot write owner %s',
							TG_TABLE_SCHEMA,
							TG_TABLE_NAME,
							bundle_user_id,
							NEW._sync_scope_id
						);
				END IF;
				RETURN NEW;
			END IF;

			IF TG_OP = 'UPDATE' THEN
				IF OLD._sync_scope_id <> bundle_user_id THEN
					RAISE EXCEPTION USING
						ERRCODE = 'P0001',
						MESSAGE = format(
							'scope mismatch on registered table %I.%I: actor %s cannot update owner %s',
							TG_TABLE_SCHEMA,
							TG_TABLE_NAME,
							bundle_user_id,
							OLD._sync_scope_id
						);
				END IF;
				IF NEW._sync_scope_id IS DISTINCT FROM OLD._sync_scope_id THEN
					RAISE EXCEPTION USING
						ERRCODE = 'P0001',
						MESSAGE = format(
							'scope mutation is not allowed on registered table %I.%I',
							TG_TABLE_SCHEMA,
							TG_TABLE_NAME
						);
				END IF;
				RETURN NEW;
			END IF;

			IF OLD._sync_scope_id <> bundle_user_id THEN
				RAISE EXCEPTION USING
					ERRCODE = 'P0001',
					MESSAGE = format(
						'scope mismatch on registered table %I.%I: actor %s cannot delete owner %s',
						TG_TABLE_SCHEMA,
						TG_TABLE_NAME,
						bundle_user_id,
						OLD._sync_scope_id
					);
			END IF;
			RETURN OLD;
		END;
		$$`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.capture_registered_row_change()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
			DECLARE
				bundle_user_id TEXT;
				bundle_user_pk_text TEXT;
				table_id_text TEXT;
				key_column TEXT;
				key_kind_text TEXT;
				bundle_user_pk BIGINT;
				table_id_value INTEGER;
				key_kind SMALLINT;
				old_key_text TEXT;
				new_key_text TEXT;
				old_key_bytes BYTEA;
				new_key_bytes BYTEA;
			BEGIN
				bundle_user_id := NULLIF(current_setting('oversync.bundle_user_id', true), '');
				IF bundle_user_id IS NULL THEN
				PERFORM nextval('sync.rejected_registered_write_seq');
				RAISE EXCEPTION USING
					ERRCODE = 'P0001',
					MESSAGE = format(
						'write to registered table %I.%I requires oversync sync bundle context',
						TG_TABLE_SCHEMA,
						TG_TABLE_NAME
						);
				END IF;
	
				bundle_user_pk_text := NULLIF(current_setting('oversync.bundle_user_pk', true), '');
				key_column := NULLIF(TG_ARGV[0], '');
				key_kind_text := NULLIF(TG_ARGV[1], '');
				table_id_text := NULLIF(TG_ARGV[2], '');
	
				IF bundle_user_pk_text IS NULL THEN
					RAISE EXCEPTION 'oversync bundle context is missing user_pk for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				IF key_column IS NULL THEN
					RAISE EXCEPTION 'oversync capture trigger is missing sync key column for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				IF key_kind_text IS NULL OR table_id_text IS NULL THEN
					RAISE EXCEPTION 'oversync capture trigger is missing compact metadata for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				bundle_user_pk := bundle_user_pk_text::bigint;
				table_id_value := table_id_text::integer;
				key_kind := key_kind_text::smallint;
	
				IF TG_OP = 'DELETE' THEN
					IF OLD._sync_scope_id <> bundle_user_id THEN
						RAISE EXCEPTION 'oversync capture scope mismatch for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
					END IF;
					old_key_text := to_jsonb(OLD)->>key_column;
					IF key_kind = 1 THEN
						old_key_bytes := uuid_send(old_key_text::uuid);
					ELSE
						old_key_bytes := convert_to(old_key_text, 'UTF8');
					END IF;
					INSERT INTO sync.bundle_capture_stage (
						txid, user_pk, table_id, op_code, key_bytes, payload_db
					)
					VALUES (
						txid_current(),
						bundle_user_pk,
						table_id_value,
						3,
						old_key_bytes,
						NULL
					);
					RETURN OLD;
			END IF;

				IF NEW._sync_scope_id <> bundle_user_id THEN
					RAISE EXCEPTION 'oversync capture scope mismatch for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				new_key_text := to_jsonb(NEW)->>key_column;
				IF key_kind = 1 THEN
					new_key_bytes := uuid_send(new_key_text::uuid);
				ELSE
					new_key_bytes := convert_to(new_key_text, 'UTF8');
				END IF;
				IF TG_OP = 'UPDATE' THEN
					IF OLD._sync_scope_id <> bundle_user_id THEN
						RAISE EXCEPTION 'oversync capture scope mismatch for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
					END IF;
					old_key_text := to_jsonb(OLD)->>key_column;
					IF key_kind = 1 THEN
						old_key_bytes := uuid_send(old_key_text::uuid);
					ELSE
						old_key_bytes := convert_to(old_key_text, 'UTF8');
					END IF;
					IF old_key_bytes IS DISTINCT FROM new_key_bytes THEN
						INSERT INTO sync.bundle_capture_stage (
							txid, user_pk, table_id, op_code, key_bytes, payload_db
						)
						VALUES (
							txid_current(),
							bundle_user_pk,
							table_id_value,
							3,
							old_key_bytes,
							NULL
						);
						INSERT INTO sync.bundle_capture_stage (
							txid, user_pk, table_id, op_code, key_bytes, payload_db
						)
						VALUES (
							txid_current(),
							bundle_user_pk,
							table_id_value,
							1,
							new_key_bytes,
							to_jsonb(NEW) - '_sync_scope_id'
						);
						RETURN NEW;
				END IF;
			END IF;
	
				INSERT INTO sync.bundle_capture_stage (
					txid, user_pk, table_id, op_code, key_bytes, payload_db
				)
				VALUES (
					txid_current(),
					bundle_user_pk,
					table_id_value,
					CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE 2 END,
					new_key_bytes,
					to_jsonb(NEW) - '_sync_scope_id'
				);

			RETURN NEW;
		END;
		$$`,
	}

	// Run all migrations within the provided transaction
	for i, migration := range migrations {
		s.logger.Debug("Running sync migration", "step", i+1, "total", len(migrations))
		if _, err := tx.Exec(ctx, migration); err != nil {
			return fmt.Errorf("sync migration %d failed: %w", i+1, err)
		}
	}
	if err := persistSyncLayoutMetadata(ctx, tx, expectedCatalog); err != nil {
		return err
	}

	bootstrapIndexes := []string{
		`CREATE INDEX IF NOT EXISTS bcs_tx_user_ordinal_idx ON sync.bundle_capture_stage(txid, user_pk, capture_ordinal)`,
		`CREATE INDEX IF NOT EXISTS rs_user_live_snapshot_idx ON sync.row_state(user_pk, table_id, key_bytes) WHERE deleted = FALSE`,
		`CREATE INDEX IF NOT EXISTS ps_expires_at_idx ON sync.push_sessions(expires_at)`,
		`CREATE INDEX IF NOT EXISTS ss_expires_at_idx ON sync.snapshot_sessions(expires_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ssr_snapshot_table_key_idx ON sync.snapshot_session_rows(snapshot_id, table_id, key_bytes)`,
	}
	for _, stmt := range bootstrapIndexes {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("bundle bootstrap index creation failed: %w", err)
		}
	}
	s.logger.Info("Sync schema initialized successfully", "migrations", len(migrations))

	return nil
}

// discoverSchemaRelationships analyzes registered tables and builds dependency graph
func (s *SyncService) discoverSchemaRelationships(ctx context.Context) error {
	if len(s.registeredTables) == 0 {
		s.logger.Info("No registered tables, skipping schema discovery")
		return nil
	}

	discovery := NewSchemaDiscovery(s.pool, s.logger)
	discoveredSchema, err := discovery.DiscoverSchemaWithDependencyOverrides(ctx, s.registeredTables, s.registeredTableInfo, s.config.DependencyOverrides)
	if err != nil {
		return fmt.Errorf("failed to discover schema relationships: %w", err)
	}

	s.discoveredSchema = discoveredSchema
	s.logger.Info("Schema relationships discovered",
		"table_count", len(discoveredSchema.TableOrder),
		"dependency_entries", len(discoveredSchema.Dependencies),
		"cycle_count", len(discoveredSchema.Cycles),
		"table_order", discoveredSchema.TableOrder)

	return nil
}
