// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const syncBootstrapLockKey int64 = 0x6f76657273796e63

const scaleRedesignSchemaReadySQL = `
SELECT
	to_regclass('sync.user_state') IS NOT NULL
	AND to_regclass('sync.bundle_capture_stage') IS NOT NULL
	AND to_regclass('sync.row_state') IS NOT NULL
	AND to_regclass('sync.bundle_log') IS NOT NULL
	AND to_regclass('sync.bundle_rows') IS NOT NULL
	AND to_regclass('sync.applied_pushes') IS NOT NULL
	AND to_regclass('sync.snapshot_sessions') IS NOT NULL
	AND to_regclass('sync.snapshot_session_rows') IS NOT NULL
	AND to_regclass('sync.accepted_push_replay_seq') IS NOT NULL
	AND to_regclass('sync.rejected_registered_write_seq') IS NOT NULL
	AND to_regclass('sync.history_pruned_error_seq') IS NOT NULL
	AND to_regclass('sync.cl_user_schema_seq_idx') IS NOT NULL
	AND to_regclass('sync.bcs_tx_user_ordinal_idx') IS NOT NULL
	AND to_regclass('sync.rs_user_table_bundle_idx') IS NOT NULL
	AND to_regclass('sync.rs_user_live_snapshot_idx') IS NOT NULL
	AND to_regclass('sync.bl_user_committed_idx') IS NOT NULL
	AND to_regclass('sync.br_user_bundle_ordinal_idx') IS NOT NULL
	AND to_regclass('sync.br_user_bundle_key_idx') IS NOT NULL
	AND to_regclass('sync.ss_expires_at_idx') IS NOT NULL
	AND to_regclass('sync.ssr_snapshot_row_ordinal_idx') IS NOT NULL
	AND to_regprocedure('sync.capture_registered_row_change()') IS NOT NULL
`

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
	// Serialize sync DDL across concurrent service bootstraps sharing the same database.
	// `CREATE INDEX IF NOT EXISTS` is not sufficient to avoid deadlocks when multiple test
	// processes attempt the full migration list at the same time.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, syncBootstrapLockKey); err != nil {
		return fmt.Errorf("acquire sync bootstrap lock: %w", err)
	}

	migrations := []string{
		// Create dedicated sync schema
		/*language=postgresql*/ `CREATE SCHEMA IF NOT EXISTS sync`,

		// Current authoritative sync storage schema. These tables back the active server hot path.
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.user_state (
			user_id TEXT PRIMARY KEY,
			next_bundle_seq BIGINT NOT NULL DEFAULT 1,
			retained_bundle_floor BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.row_state (
			user_id TEXT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			key_json TEXT NOT NULL,
			row_version BIGINT NOT NULL DEFAULT 0,
			deleted BOOLEAN NOT NULL DEFAULT FALSE,
			bundle_seq BIGINT NOT NULL DEFAULT 0,
			payload_hash BYTEA,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, schema_name, table_name, key_json)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.bundle_capture_stage (
			capture_id BIGSERIAL PRIMARY KEY,
			txid BIGINT NOT NULL,
			user_id TEXT NOT NULL,
			source_id TEXT NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			key_json TEXT NOT NULL,
			payload JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			CONSTRAINT bundle_capture_stage_payload_by_op_chk
				CHECK ((op = 'DELETE' AND payload IS NULL) OR (op IN ('INSERT','UPDATE') AND payload IS NOT NULL))
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.bundle_log (
			user_id TEXT NOT NULL,
			bundle_seq BIGINT NOT NULL,
			source_id TEXT NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			row_count INT NOT NULL DEFAULT 0,
			byte_count BIGINT NOT NULL DEFAULT 0,
			bundle_hash TEXT NOT NULL DEFAULT '',
			committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, bundle_seq)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.bundle_rows (
			user_id TEXT NOT NULL,
			bundle_seq BIGINT NOT NULL,
			row_ordinal INT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			key_json TEXT NOT NULL,
			op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			row_version BIGINT NOT NULL,
			payload JSONB,
			PRIMARY KEY (user_id, bundle_seq, row_ordinal),
			CONSTRAINT bundle_rows_payload_by_op_chk
				CHECK ((op = 'DELETE' AND payload IS NULL) OR (op IN ('INSERT','UPDATE') AND payload IS NOT NULL))
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.applied_pushes (
			user_id TEXT NOT NULL,
			source_id TEXT NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			bundle_seq BIGINT NOT NULL,
			row_count BIGINT NOT NULL DEFAULT 0,
			bundle_hash TEXT NOT NULL DEFAULT '',
			committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, source_id, source_bundle_id)
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.push_sessions (
			push_id UUID PRIMARY KEY,
			user_id TEXT NOT NULL,
			source_id TEXT NOT NULL,
			source_bundle_id BIGINT NOT NULL,
			planned_row_count BIGINT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('staging', 'committed')),
			bundle_seq BIGINT,
			row_count BIGINT,
			bundle_hash TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			expires_at TIMESTAMPTZ NOT NULL
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.push_session_rows (
			push_id UUID NOT NULL REFERENCES sync.push_sessions(push_id) ON DELETE CASCADE,
			row_ordinal BIGINT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			key_json TEXT NOT NULL,
			op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
			base_row_version BIGINT NOT NULL,
			payload JSONB,
			PRIMARY KEY (push_id, row_ordinal),
			CONSTRAINT push_session_rows_payload_by_op_chk
				CHECK ((op = 'DELETE' AND payload IS NULL) OR (op IN ('INSERT','UPDATE') AND payload IS NOT NULL))
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.snapshot_sessions (
			snapshot_id UUID PRIMARY KEY,
			user_id TEXT NOT NULL,
			snapshot_bundle_seq BIGINT NOT NULL,
			row_count BIGINT NOT NULL DEFAULT 0,
			byte_count BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			expires_at TIMESTAMPTZ NOT NULL
		)`,
		/*language=postgresql*/ `CREATE TABLE IF NOT EXISTS sync.snapshot_session_rows (
			snapshot_id UUID NOT NULL REFERENCES sync.snapshot_sessions(snapshot_id) ON DELETE CASCADE,
			row_ordinal BIGINT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			key_json TEXT NOT NULL,
			row_version BIGINT NOT NULL,
			payload JSONB NOT NULL,
			PRIMARY KEY (snapshot_id, row_ordinal)
		)`,
		`CREATE SEQUENCE IF NOT EXISTS sync.accepted_push_replay_seq`,
		`CREATE SEQUENCE IF NOT EXISTS sync.rejected_registered_write_seq`,
		`CREATE SEQUENCE IF NOT EXISTS sync.history_pruned_error_seq`,
		/*language=postgresql*/ `CREATE OR REPLACE FUNCTION sync.capture_registered_row_change()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		DECLARE
			bundle_user_id TEXT;
			bundle_source_id TEXT;
			bundle_source_bundle_id_text TEXT;
			key_column TEXT;
			old_key JSONB;
			new_key JSONB;
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

			bundle_source_id := NULLIF(current_setting('oversync.bundle_source_id', true), '');
			bundle_source_bundle_id_text := NULLIF(current_setting('oversync.bundle_source_bundle_id', true), '');
			key_column := NULLIF(TG_ARGV[0], '');

			IF bundle_source_id IS NULL OR bundle_source_bundle_id_text IS NULL THEN
				RAISE EXCEPTION 'oversync bundle context is missing source metadata for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
			END IF;
			IF key_column IS NULL THEN
				RAISE EXCEPTION 'oversync capture trigger is missing sync key column for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
			END IF;

			IF TG_OP = 'DELETE' THEN
				old_key := jsonb_build_object(key_column, to_jsonb(OLD)->key_column);
				INSERT INTO sync.bundle_capture_stage (
					txid, user_id, source_id, source_bundle_id, schema_name, table_name, op, key_json, payload
				)
				VALUES (
					txid_current(),
					bundle_user_id,
					bundle_source_id,
					bundle_source_bundle_id_text::bigint,
					TG_TABLE_SCHEMA,
					TG_TABLE_NAME,
					'DELETE',
					old_key::text,
					NULL
				);
				RETURN OLD;
			END IF;

			new_key := jsonb_build_object(key_column, to_jsonb(NEW)->key_column);
			IF TG_OP = 'UPDATE' THEN
				old_key := jsonb_build_object(key_column, to_jsonb(OLD)->key_column);
				IF old_key IS DISTINCT FROM new_key THEN
					INSERT INTO sync.bundle_capture_stage (
						txid, user_id, source_id, source_bundle_id, schema_name, table_name, op, key_json, payload
					)
					VALUES (
						txid_current(),
						bundle_user_id,
						bundle_source_id,
						bundle_source_bundle_id_text::bigint,
						TG_TABLE_SCHEMA,
						TG_TABLE_NAME,
						'DELETE',
						old_key::text,
						NULL
					);
					INSERT INTO sync.bundle_capture_stage (
						txid, user_id, source_id, source_bundle_id, schema_name, table_name, op, key_json, payload
					)
					VALUES (
						txid_current(),
						bundle_user_id,
						bundle_source_id,
						bundle_source_bundle_id_text::bigint,
						TG_TABLE_SCHEMA,
						TG_TABLE_NAME,
						'INSERT',
						new_key::text,
						to_jsonb(NEW)
					);
					RETURN NEW;
				END IF;
			END IF;

			INSERT INTO sync.bundle_capture_stage (
				txid, user_id, source_id, source_bundle_id, schema_name, table_name, op, key_json, payload
			)
			VALUES (
				txid_current(),
				bundle_user_id,
				bundle_source_id,
				bundle_source_bundle_id_text::bigint,
				TG_TABLE_SCHEMA,
				TG_TABLE_NAME,
				TG_OP,
				new_key::text,
				to_jsonb(NEW)
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

	bootstrapIndexes := []string{
		`CREATE INDEX IF NOT EXISTS bcs_tx_user_ordinal_idx ON sync.bundle_capture_stage(txid, user_id, capture_id)`,
		`CREATE INDEX IF NOT EXISTS rs_user_table_bundle_idx ON sync.row_state(user_id, schema_name, table_name, bundle_seq DESC)`,
		`CREATE INDEX IF NOT EXISTS rs_user_live_snapshot_idx ON sync.row_state(user_id, schema_name, table_name, key_json, bundle_seq) WHERE deleted = FALSE`,
		`CREATE INDEX IF NOT EXISTS bl_user_committed_idx ON sync.bundle_log(user_id, committed_at DESC, bundle_seq DESC)`,
		`CREATE INDEX IF NOT EXISTS br_user_bundle_ordinal_idx ON sync.bundle_rows(user_id, bundle_seq, row_ordinal)`,
		`CREATE INDEX IF NOT EXISTS br_user_bundle_key_idx ON sync.bundle_rows(user_id, bundle_seq, schema_name, table_name, key_json)`,
		`CREATE INDEX IF NOT EXISTS ps_expires_at_idx ON sync.push_sessions(expires_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ps_active_tuple_idx ON sync.push_sessions(user_id, source_id, source_bundle_id) WHERE status = 'staging'`,
		`CREATE INDEX IF NOT EXISTS psr_push_row_ordinal_idx ON sync.push_session_rows(push_id, row_ordinal)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ap_user_bundle_seq_idx ON sync.applied_pushes(user_id, bundle_seq)`,
		`CREATE INDEX IF NOT EXISTS ss_expires_at_idx ON sync.snapshot_sessions(expires_at)`,
		`CREATE INDEX IF NOT EXISTS ssr_snapshot_row_ordinal_idx ON sync.snapshot_session_rows(snapshot_id, row_ordinal)`,
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
	discoveredSchema, err := discovery.DiscoverSchemaWithDependencyOverrides(ctx, s.registeredTables, s.config.DependencyOverrides)
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
