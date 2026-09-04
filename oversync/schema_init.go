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

const syncBootstrapLockKey int64 = 0x6f76657273796e63

// Kept as an internal compatibility seam. Existing-layout attachment deliberately does not call
// this helper because attachment takes no managed data locks.
var _ = lockExistingManagedLayoutRelations

const (
	syncSchemaProtocolLabel       = "v1"
	syncSchemaLayoutName          = "server_postgres_sync_catalog_v1"
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

type unsupportedRegisteredRelationPersistence struct {
	tableKey    string
	persistence string
}

func registeredRelationPersistenceName(persistence string) string {
	switch persistence {
	case "p":
		return "PERMANENT"
	case "u":
		return "UNLOGGED"
	case "t":
		return "TEMPORARY"
	default:
		return "UNKNOWN"
	}
}

func validateRegisteredTablePersistence(
	ctx context.Context,
	q syncCatalogQuerier,
	registeredTables []RegisteredTable,
) error {
	if len(registeredTables) == 0 {
		return nil
	}

	schemas := make([]string, 0, len(registeredTables))
	tables := make([]string, 0, len(registeredTables))
	for _, table := range registeredTables {
		schemas = append(schemas, table.normalizedSchema())
		tables = append(tables, table.normalizedTable())
	}

	rows, err := q.Query(ctx, `
WITH registered AS (
  SELECT DISTINCT schema_name, table_name
  FROM unnest(@schemas::text[], @tables::text[]) AS requested(schema_name, table_name)
)
SELECT
  namespace.nspname,
  relation.relname,
  relation.relpersistence::text
FROM registered
JOIN pg_namespace AS namespace
  ON namespace.nspname = registered.schema_name
JOIN pg_class AS relation
  ON relation.relnamespace = namespace.oid
 AND relation.relname = registered.table_name
WHERE relation.relkind IN ('r', 'p')
ORDER BY namespace.nspname, relation.relname
`, pgx.NamedArgs{
		"schemas": schemas,
		"tables":  tables,
	})
	if err != nil {
		return fmt.Errorf("inspect registered table persistence: %w", err)
	}
	defer rows.Close()

	var unsupported []unsupportedRegisteredRelationPersistence
	for rows.Next() {
		var schemaName, tableName, persistence string
		if err := rows.Scan(&schemaName, &tableName, &persistence); err != nil {
			return fmt.Errorf("scan registered table persistence: %w", err)
		}
		if persistence == "p" {
			continue
		}
		unsupported = append(unsupported, unsupportedRegisteredRelationPersistence{
			tableKey:    Key(schemaName, tableName),
			persistence: persistence,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate registered table persistence: %w", err)
	}
	if len(unsupported) == 0 {
		return nil
	}

	sort.Slice(unsupported, func(i, j int) bool {
		if unsupported[i].tableKey == unsupported[j].tableKey {
			return unsupported[i].persistence < unsupported[j].persistence
		}
		return unsupported[i].tableKey < unsupported[j].tableKey
	})
	details := make([]string, 0, len(unsupported))
	for _, relation := range unsupported {
		details = append(details, fmt.Sprintf(
			"%s (%s, relpersistence=%q)",
			relation.tableKey,
			registeredRelationPersistenceName(relation.persistence),
			relation.persistence,
		))
	}
	return unsupportedSchemaf(
		"registered tables must use permanent logged PostgreSQL storage (relpersistence=\"p\"); unsupported relations: %s",
		strings.Join(details, "; "),
	)
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

	tableIDs := make([]int32, 0, len(s.registeredTableByID))
	for tableID := range s.registeredTableByID {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Slice(tableIDs, func(i, j int) bool { return tableIDs[i] < tableIDs[j] })

	rows := make([]expectedTableCatalogRow, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		info := s.registeredTableByID[tableID]
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
			UNION ALL
			SELECT 1
			FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_collation c
			JOIN pg_namespace n ON n.oid = c.collnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_conversion c
			JOIN pg_namespace n ON n.oid = c.connamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_operator o
			JOIN pg_namespace n ON n.oid = o.oprnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_opclass o
			JOIN pg_namespace n ON n.oid = o.opcnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_opfamily o
			JOIN pg_namespace n ON n.oid = o.opfnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_ts_config o
			JOIN pg_namespace n ON n.oid = o.cfgnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_ts_dict o
			JOIN pg_namespace n ON n.oid = o.dictnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_ts_parser o
			JOIN pg_namespace n ON n.oid = o.prsnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_ts_template o
			JOIN pg_namespace n ON n.oid = o.tmplnamespace
			WHERE n.nspname = 'sync'
			UNION ALL
			SELECT 1
			FROM pg_statistic_ext o
			JOIN pg_namespace n ON n.oid = o.stxnamespace
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
		return fmt.Errorf("load sync.meta marker: %w", err)
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
			return fmt.Errorf("scan sync.meta row: %w", err)
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
		return fmt.Errorf("iterate sync.meta rows: %w", err)
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
		return fmt.Errorf("load sync.table_catalog marker: %w", err)
	}
	defer rows.Close()

	actual := make([]expectedTableCatalogRow, 0, len(expected))
	for rows.Next() {
		var row expectedTableCatalogRow
		if err := rows.Scan(&row.TableID, &row.SchemaName, &row.TableName, &row.SyncKeyColumn, &row.SyncKeyKind); err != nil {
			return fmt.Errorf("scan sync.table_catalog row: %w", err)
		}
		actual = append(actual, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sync.table_catalog rows: %w", err)
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
			"existing sync schema uses an unsupported layout for %q",
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

// createFreshSyncLayoutInTx creates managed schema objects after Bootstrap has
// classified the database as fresh and locked and revalidated every registered
// table declaration. Marker rows and registered-table triggers are deliberately
// persisted by Bootstrap after semantic self-validation.
func (s *SyncService) createFreshSyncLayoutInTx(ctx context.Context, tx pgx.Tx) error {
	migrations := []string{
		// Create dedicated sync schema
		/*language=postgresql*/ `CREATE SCHEMA IF NOT EXISTS sync`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.meta (
			singleton_key BOOLEAN PRIMARY KEY,
			protocol_label TEXT COLLATE "C" NOT NULL,
			layout_name TEXT COLLATE "C" NOT NULL,
			catalog_revision BIGINT NOT NULL,
			next_table_id BIGINT NOT NULL,
			catalog_fingerprint TEXT COLLATE "C" NOT NULL,
			CONSTRAINT meta_singleton_key_chk CHECK (singleton_key),
			CONSTRAINT meta_protocol_label_chk CHECK (protocol_label = 'v1'),
			CONSTRAINT meta_layout_name_chk CHECK (layout_name = 'server_postgres_sync_catalog_v1'),
			CONSTRAINT meta_catalog_revision_chk CHECK (catalog_revision >= 1),
			CONSTRAINT meta_next_table_id_chk CHECK (next_table_id BETWEEN 1 AND 2147483648),
			CONSTRAINT meta_catalog_fingerprint_chk CHECK (catalog_fingerprint ~ '^[0-9a-f]{64}$')
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.table_catalog (
			table_id INTEGER PRIMARY KEY,
			schema_name TEXT COLLATE "C" NOT NULL,
			table_name TEXT COLLATE "C" NOT NULL,
			sync_key_column TEXT COLLATE "C" NOT NULL,
			sync_key_kind SMALLINT NOT NULL,
			state_code SMALLINT NOT NULL,
			introduced_revision BIGINT NOT NULL,
			retired_revision BIGINT,
			CONSTRAINT table_catalog_table_id_chk CHECK (table_id > 0),
			CONSTRAINT table_catalog_schema_name_chk CHECK (
				octet_length(schema_name) BETWEEN 1 AND 63
				AND schema_name ~ '^[a-z0-9_]+$'
			),
			CONSTRAINT table_catalog_table_name_chk CHECK (
				octet_length(table_name) BETWEEN 1 AND 63
				AND table_name ~ '^[a-z0-9_]+$'
			),
			CONSTRAINT table_catalog_sync_key_column_chk CHECK (
				octet_length(sync_key_column) BETWEEN 1 AND 63
				AND sync_key_column ~ '^[a-z0-9_]+$'
				AND sync_key_column <> '_sync_scope_id'
			),
			CONSTRAINT table_catalog_schema_table_key UNIQUE (schema_name, table_name),
			CONSTRAINT table_catalog_sync_key_kind_chk CHECK (sync_key_kind IN (1, 2)),
			CONSTRAINT table_catalog_state_code_chk CHECK (state_code IN (1, 2)),
			CONSTRAINT table_catalog_introduced_revision_chk CHECK (introduced_revision >= 1),
			CONSTRAINT table_catalog_state_revision_chk CHECK (
				(state_code = 1 AND retired_revision IS NULL)
				OR
				(state_code = 2 AND retired_revision > introduced_revision)
			)
		)`,

		// Current authoritative sync storage schema. These tables back the active server hot path.
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.user_state (
			user_pk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			user_id TEXT COLLATE "C" NOT NULL UNIQUE,
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
			initializer_source_id TEXT COLLATE "C",
			initialization_id UUID,
			lease_expires_at TIMESTAMPTZ,
			initialized_at TIMESTAMPTZ,
			initialized_by_source_id TEXT COLLATE "C",
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
			source_id TEXT COLLATE "C" NOT NULL,
			state TEXT NOT NULL,
			max_committed_source_bundle_id BIGINT NOT NULL DEFAULT 0,
			replaced_by_source_id TEXT COLLATE "C" NOT NULL DEFAULT '',
			retirement_reason TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (user_pk, source_id),
			CONSTRAINT source_state_state_chk CHECK (state IN ('active', 'reserved', 'retired')),
			CONSTRAINT source_state_max_committed_chk CHECK (max_committed_source_bundle_id >= 0),
			CONSTRAINT source_state_active_chk CHECK (
				(state <> 'active')
				OR (replaced_by_source_id = '' AND retirement_reason = '')
			),
			CONSTRAINT source_state_reserved_chk CHECK (
				(state <> 'reserved')
				OR (replaced_by_source_id = '' AND retirement_reason = '' AND max_committed_source_bundle_id = 0)
			),
			CONSTRAINT source_state_retired_chk CHECK (
				(state <> 'retired')
				OR (replaced_by_source_id <> '' AND retirement_reason <> '')
			)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.source_generation_bindings (
			user_pk BIGINT NOT NULL,
			source_id TEXT COLLATE "C" NOT NULL,
			schema_generation_id INTEGER NOT NULL,
			schema_generation_fingerprint TEXT COLLATE "C" NOT NULL,
			PRIMARY KEY (user_pk, source_id),
			CONSTRAINT source_generation_bindings_source_fk
				FOREIGN KEY (user_pk, source_id)
				REFERENCES sync.source_state(user_pk, source_id)
				ON DELETE CASCADE,
			CONSTRAINT source_generation_bindings_id_chk
				CHECK (schema_generation_id BETWEEN 1 AND 2147483647),
			CONSTRAINT source_generation_bindings_fingerprint_chk
				CHECK (schema_generation_fingerprint ~ '^[0-9a-f]{64}$'),
			CONSTRAINT source_generation_bindings_session_key
				UNIQUE (user_pk, source_id, schema_generation_id, schema_generation_fingerprint)
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
			source_id TEXT COLLATE "C" NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			planned_row_count BIGINT,
			row_count BIGINT NOT NULL,
			byte_count BIGINT NOT NULL,
			bundle_hash BYTEA NOT NULL,
			canonical_request_hash TEXT COLLATE "C" NOT NULL,
			committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_pk, bundle_seq),
			CONSTRAINT bundle_log_source_tuple_key UNIQUE (user_pk, source_id, source_bundle_id),
			CONSTRAINT bundle_log_planned_row_count_chk CHECK (planned_row_count IS NULL OR planned_row_count > 0)
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
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.snapshot_sessions (
			snapshot_id UUID PRIMARY KEY,
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			source_id TEXT COLLATE "C" NOT NULL,
			schema_generation_id INTEGER NOT NULL,
			schema_generation_fingerprint TEXT COLLATE "C" NOT NULL,
			snapshot_bundle_seq BIGINT NOT NULL,
			row_count BIGINT NOT NULL DEFAULT 0,
			byte_count BIGINT NOT NULL DEFAULT 0,
			expires_at TIMESTAMPTZ NOT NULL,
			CONSTRAINT snapshot_sessions_generation_id_chk CHECK (schema_generation_id BETWEEN 1 AND 2147483647),
			CONSTRAINT snapshot_sessions_generation_fingerprint_chk CHECK (schema_generation_fingerprint ~ '^[0-9a-f]{64}$'),
			CONSTRAINT snapshot_sessions_source_generation_fk
				FOREIGN KEY (user_pk, source_id, schema_generation_id, schema_generation_fingerprint)
				REFERENCES sync.source_generation_bindings (user_pk, source_id, schema_generation_id, schema_generation_fingerprint)
				ON DELETE CASCADE
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.snapshot_session_rows (
			snapshot_id UUID NOT NULL REFERENCES sync.snapshot_sessions(snapshot_id) ON DELETE CASCADE,
			row_ordinal BIGINT NOT NULL,
			table_id INTEGER NOT NULL REFERENCES sync.table_catalog(table_id),
			key_bytes BYTEA NOT NULL,
			bundle_seq BIGINT NOT NULL,
			payload_wire JSON NOT NULL,
			wire_byte_count BIGINT NOT NULL,
			PRIMARY KEY (snapshot_id, row_ordinal),
			CONSTRAINT snapshot_session_rows_logical_row_key UNIQUE (snapshot_id, table_id, key_bytes)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.scope_write_receipts (
			user_pk BIGINT NOT NULL REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
			operation_id UUID NOT NULL,
			operation_hash BYTEA NOT NULL,
			required_effect_tables_hash BYTEA NOT NULL,
			forbidden_effect_tables_hash BYTEA NOT NULL,
			writer_id TEXT COLLATE "C" NOT NULL,
			explicit_source_bundle_id BIGINT,
			operation_valid_until TIMESTAMPTZ NOT NULL,
			receipt_state TEXT COLLATE "C" NOT NULL,
			committed_source_bundle_id BIGINT,
			committed_bundle_seq BIGINT,
			committed_row_count BIGINT,
			committed_bundle_hash BYTEA,
			committed_canonical_request_hash TEXT COLLATE "C",
			auto_initialized BOOLEAN,
			created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
			completed_at TIMESTAMPTZ,
			receipt_expires_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (user_pk, operation_id),
			CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'::uuid),
			CHECK (writer_id <> ''),
			CHECK (writer_id ~ '^[!-~]+$'),
			CHECK (octet_length(operation_hash) = 32),
			CHECK (octet_length(required_effect_tables_hash) = 32),
			CHECK (octet_length(forbidden_effect_tables_hash) = 32),
			CHECK (explicit_source_bundle_id IS NULL OR explicit_source_bundle_id > 0),
			CHECK (isfinite(operation_valid_until) AND isfinite(receipt_expires_at)),
			CHECK (operation_valid_until >= '0001-01-01 00:00:00+00'::timestamptz
			       AND receipt_expires_at < '10000-01-01 00:00:00+00'::timestamptz),
			CHECK (receipt_state IN ('in_progress','completed')),
			CHECK (
				(receipt_state = 'in_progress'
				 AND committed_source_bundle_id IS NULL
				 AND committed_bundle_seq IS NULL
				 AND committed_row_count IS NULL
				 AND committed_bundle_hash IS NULL
				 AND committed_canonical_request_hash IS NULL
				 AND auto_initialized IS NULL
				 AND completed_at IS NULL
				 AND receipt_expires_at = operation_valid_until + interval '24 hours')
				OR
				(receipt_state = 'completed'
				 AND committed_source_bundle_id IS NOT NULL
				 AND committed_source_bundle_id > 0
				 AND committed_bundle_seq IS NOT NULL
				 AND committed_bundle_seq > 0
				 AND committed_row_count IS NOT NULL
				 AND committed_row_count > 0
				 AND committed_bundle_hash IS NOT NULL
				 AND octet_length(committed_bundle_hash) = 32
				 AND committed_canonical_request_hash IS NOT NULL
				 AND auto_initialized IS NOT NULL
				 AND completed_at IS NOT NULL
				 AND receipt_expires_at = GREATEST(operation_valid_until, completed_at) + interval '24 hours')
			),
			CHECK (receipt_state <> 'completed'
			       OR explicit_source_bundle_id IS NULL
			       OR committed_source_bundle_id = explicit_source_bundle_id),
			CHECK (committed_canonical_request_hash IS NULL
			       OR committed_canonical_request_hash = ''
			       OR committed_canonical_request_hash ~ '^[0-9a-f]{64}$')
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.server_source_reservations (
			source_id TEXT COLLATE "C" PRIMARY KEY,
			ever_used BOOLEAN NOT NULL DEFAULT FALSE,
			CHECK (octet_length(source_id) BETWEEN 1 AND 256),
			CHECK (source_id ~ '^[!-~]+$')
		)`,
		`CREATE SEQUENCE IF NOT EXISTS sync.accepted_push_replay_seq`,
		`CREATE SEQUENCE IF NOT EXISTS sync.rejected_registered_write_seq`,
		`CREATE SEQUENCE IF NOT EXISTS sync.history_pruned_error_seq`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.reject_registered_table_truncate()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		DECLARE
			registered_schema TEXT;
			registered_table TEXT;
		BEGIN
			registered_schema := COALESCE(NULLIF(TG_ARGV[0], ''), TG_TABLE_SCHEMA);
			registered_table := COALESCE(NULLIF(TG_ARGV[1], ''), TG_TABLE_NAME);
			RAISE EXCEPTION USING
				ERRCODE = '55000',
				MESSAGE = format(
					'TRUNCATE is not allowed on registered table %I.%I',
					registered_schema,
					registered_table
				),
				DETAIL = 'Oversync cannot capture TRUNCATE as row-level bundle events.',
				HINT = 'Stop all server and client processes and recreate PostgreSQL and every client database for an administrative reset.';
			RETURN NULL;
		END;
		$$`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.guard_source_generation_binding()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.user_pk IS NOT DISTINCT FROM OLD.user_pk
				AND NEW.source_id IS NOT DISTINCT FROM OLD.source_id
				AND NEW.schema_generation_id IS NOT DISTINCT FROM OLD.schema_generation_id
				AND NEW.schema_generation_fingerprint IS NOT DISTINCT FROM OLD.schema_generation_fingerprint THEN
				RETURN NEW;
			END IF;
			RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source generation binding is immutable';
		END;
		$$`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.finalize_scope_write_receipt()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		DECLARE
			current_state TEXT;
		BEGIN
			SELECT receipt_state
			INTO current_state
			FROM sync.scope_write_receipts
			WHERE user_pk = NEW.user_pk
			  AND operation_id = NEW.operation_id;

			IF current_state IS DISTINCT FROM 'completed' THEN
				RAISE EXCEPTION USING
					ERRCODE = '55000',
					MESSAGE = 'scope write receipt cannot commit before completion';
			END IF;
			RETURN NULL;
		END;
		$$`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.guard_server_source_reservation()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF TG_OP = 'INSERT' THEN
				PERFORM pg_advisory_xact_lock(8031718527699873379);
				LOCK TABLE sync.source_state IN SHARE ROW EXCLUSIVE MODE;
				IF NEW.ever_used THEN
					RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'new server source reservation must start unused';
				END IF;
				IF EXISTS (
					SELECT 1
					FROM sync.source_state
					WHERE source_id COLLATE "C" = NEW.source_id COLLATE "C"
					   OR replaced_by_source_id COLLATE "C" = NEW.source_id COLLATE "C"
				) THEN
					RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'client source history prevents server source reservation';
				END IF;
				RETURN NEW;
			END IF;

			IF TG_OP = 'UPDATE' THEN
				IF NEW.source_id IS DISTINCT FROM OLD.source_id THEN
					RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'server source reservation identity is immutable';
				END IF;
				IF OLD.ever_used OR NOT NEW.ever_used THEN
					RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'server source reservation permits only first-use publication';
				END IF;
				RETURN NEW;
			END IF;

			PERFORM pg_advisory_xact_lock(8031718527699873379);
			LOCK TABLE sync.source_state, sync.scope_write_receipts IN SHARE ROW EXCLUSIVE MODE;
			IF OLD.ever_used THEN
				RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'used server source reservation cannot be removed';
			END IF;
			IF EXISTS (
				SELECT 1
				FROM sync.source_state
				WHERE source_id COLLATE "C" = OLD.source_id COLLATE "C"
				   OR replaced_by_source_id COLLATE "C" = OLD.source_id COLLATE "C"
			)
			   OR EXISTS (SELECT 1 FROM sync.scope_write_receipts WHERE writer_id COLLATE "C" = OLD.source_id COLLATE "C") THEN
				RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'referenced server source reservation cannot be removed';
			END IF;
			RETURN OLD;
		END;
		$$`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.guard_table_catalog_identity()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF TG_OP = 'DELETE' THEN
				RAISE EXCEPTION USING
					ERRCODE = '55000',
					MESSAGE = format('registered table catalog row %s cannot be deleted', OLD.table_id);
			END IF;

			IF NEW.table_id IS DISTINCT FROM OLD.table_id
			   OR NEW.schema_name IS DISTINCT FROM OLD.schema_name
			   OR NEW.table_name IS DISTINCT FROM OLD.table_name
			   OR NEW.sync_key_column IS DISTINCT FROM OLD.sync_key_column
			   OR NEW.sync_key_kind IS DISTINCT FROM OLD.sync_key_kind
			   OR NEW.introduced_revision IS DISTINCT FROM OLD.introduced_revision THEN
				RAISE EXCEPTION USING
					ERRCODE = '55000',
					MESSAGE = format('registered table catalog identity is immutable for table_id %s', OLD.table_id);
			END IF;

			IF OLD.state_code = 2 AND NEW IS DISTINCT FROM OLD THEN
				RAISE EXCEPTION USING
					ERRCODE = '55000',
					MESSAGE = format('retired registered table catalog row %s is immutable', OLD.table_id);
			END IF;

			IF NEW IS NOT DISTINCT FROM OLD THEN
				RETURN NEW;
			END IF;

			IF OLD.state_code = 1
			   AND OLD.retired_revision IS NULL
			   AND NEW.state_code = 2
			   AND NEW.retired_revision > OLD.introduced_revision THEN
				RETURN NEW;
			END IF;

			RAISE EXCEPTION USING
				ERRCODE = '55000',
				MESSAGE = format('invalid registered table catalog state transition for table_id %s', OLD.table_id);
		END;
		$$`,
	}
	identityFunctionMigrations := []string{
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
				ELSIF NEW._sync_scope_id COLLATE "C" <> bundle_user_id COLLATE "C" THEN
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
				IF OLD._sync_scope_id IS NULL THEN
					RAISE EXCEPTION 'nullable old scope identity on registered table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				IF NEW._sync_scope_id IS NULL THEN
					RAISE EXCEPTION 'nullable new scope identity on registered table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				IF OLD._sync_scope_id COLLATE "C" <> bundle_user_id COLLATE "C" THEN
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

			IF OLD._sync_scope_id IS NULL THEN
				RAISE EXCEPTION 'nullable old scope identity on registered table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
			END IF;
			IF OLD._sync_scope_id COLLATE "C" <> bundle_user_id COLLATE "C" THEN
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
				new_payload JSONB;
				json_column TEXT;
				json_text TEXT;
				argument_index INTEGER;
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
					IF OLD._sync_scope_id IS NULL THEN
						RAISE EXCEPTION 'nullable old scope identity on registered table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
					END IF;
					IF OLD._sync_scope_id COLLATE "C" <> bundle_user_id COLLATE "C" THEN
						RAISE EXCEPTION 'oversync capture scope mismatch for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
					END IF;
					old_key_text := to_jsonb(OLD)->>key_column;
					IF old_key_text IS NULL THEN
						RAISE EXCEPTION 'nullable old sync key identity on registered table %.% column %', TG_TABLE_SCHEMA, TG_TABLE_NAME, key_column;
					END IF;
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

				IF NEW._sync_scope_id IS NULL THEN
					RAISE EXCEPTION 'nullable new scope identity on registered table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				IF NEW._sync_scope_id COLLATE "C" <> bundle_user_id COLLATE "C" THEN
					RAISE EXCEPTION 'oversync capture scope mismatch for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
				END IF;
				new_key_text := to_jsonb(NEW)->>key_column;
				IF new_key_text IS NULL THEN
					RAISE EXCEPTION 'nullable new sync key identity on registered table %.% column %', TG_TABLE_SCHEMA, TG_TABLE_NAME, key_column;
				END IF;
				IF key_kind = 1 THEN
					new_key_bytes := uuid_send(new_key_text::uuid);
				ELSE
					new_key_bytes := convert_to(new_key_text, 'UTF8');
				END IF;
				new_payload := to_jsonb(NEW) - '_sync_scope_id';
				IF TG_NARGS > 3 THEN
					FOR argument_index IN 3..TG_NARGS - 1 LOOP
						json_column := TG_ARGV[argument_index];
						EXECUTE format('SELECT (($1).%I)::text', json_column)
							INTO json_text
							USING NEW;
						new_payload := jsonb_set(
							new_payload,
							ARRAY[json_column],
							COALESCE(to_jsonb(json_text), 'null'::jsonb),
							false
						);
					END LOOP;
				END IF;
				IF TG_OP = 'UPDATE' THEN
					IF OLD._sync_scope_id IS NULL THEN
						RAISE EXCEPTION 'nullable old scope identity on registered table %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
					END IF;
					IF OLD._sync_scope_id COLLATE "C" <> bundle_user_id COLLATE "C" THEN
						RAISE EXCEPTION 'oversync capture scope mismatch for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
					END IF;
					old_key_text := to_jsonb(OLD)->>key_column;
					IF old_key_text IS NULL THEN
						RAISE EXCEPTION 'nullable old sync key identity on registered table %.% column %', TG_TABLE_SCHEMA, TG_TABLE_NAME, key_column;
					END IF;
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
							new_payload
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
					new_payload
				);

			RETURN NEW;
		END;
		$$`,
	}
	migrations = append(migrations, identityFunctionMigrations...)

	// Run all migrations within the provided transaction
	for i, migration := range migrations {
		s.logger.Debug("Running sync migration", "step", i+1, "total", len(migrations))
		if _, err := tx.Exec(ctx, migration); err != nil {
			return fmt.Errorf("sync migration %d failed: %w", i+1, err)
		}
	}

	bootstrapIndexes := []string{
		`CREATE INDEX IF NOT EXISTS bcs_tx_user_ordinal_idx ON sync.bundle_capture_stage(txid, user_pk, capture_ordinal)`,
		`CREATE INDEX IF NOT EXISTS rs_user_live_snapshot_idx ON sync.row_state(user_pk, table_id, key_bytes) WHERE deleted = FALSE`,
		`CREATE INDEX IF NOT EXISTS ss_expires_at_idx ON sync.snapshot_sessions(expires_at, snapshot_id)`,
		`CREATE INDEX IF NOT EXISTS ss_source_generation_idx ON sync.snapshot_sessions(user_pk, source_id, schema_generation_id, schema_generation_fingerprint)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ssr_snapshot_table_key_idx ON sync.snapshot_session_rows(snapshot_id, table_id, key_bytes)`,
		`CREATE INDEX IF NOT EXISTS scope_write_receipts_cleanup_idx ON sync.scope_write_receipts(receipt_expires_at, user_pk, operation_id) WHERE receipt_state = 'completed'`,
	}
	for _, stmt := range bootstrapIndexes {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("bundle bootstrap index creation failed: %w", err)
		}
	}
	managedTriggers := []string{
		`CREATE CONSTRAINT TRIGGER scope_write_receipts_finalizer AFTER INSERT OR UPDATE ON sync.scope_write_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION sync.finalize_scope_write_receipt()`,
		`CREATE TRIGGER server_source_reservations_guard BEFORE INSERT OR UPDATE OR DELETE ON sync.server_source_reservations FOR EACH ROW EXECUTE FUNCTION sync.guard_server_source_reservation()`,
		`CREATE TRIGGER table_catalog_identity_guard BEFORE UPDATE OR DELETE ON sync.table_catalog FOR EACH ROW EXECUTE FUNCTION sync.guard_table_catalog_identity()`,
		`CREATE TRIGGER source_generation_bindings_identity_guard BEFORE UPDATE ON sync.source_generation_bindings FOR EACH ROW EXECUTE FUNCTION sync.guard_source_generation_binding()`,
	}
	for _, stmt := range managedTriggers {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("scope write managed trigger creation failed: %w", err)
		}
	}
	s.logger.Info("Sync schema initialized successfully", "migrations", len(migrations))

	return nil
}

// discoverSchemaRelationships analyzes registered tables and builds dependency graph
func (s *SyncService) discoverSchemaRelationships(ctx context.Context, q syncCatalogQuerier) error {
	if len(s.registeredTables) == 0 {
		s.logger.Info("No registered tables, skipping schema discovery")
		return nil
	}

	discovery := newSchemaDiscoveryWithQuerier(q, s.logger)
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
