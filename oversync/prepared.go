// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Statement names for hot-path operations
const (
	stmtGateInsertUpsert = "s_gate_insert_upsert"
	stmtGateInsertDelete = "s_gate_insert_delete"
	stmtEnsureMeta       = "s_ensure_meta"
	stmtVersionGateUp    = "s_version_gate_upsert"
	stmtVersionGateDel   = "s_version_gate_delete"
	stmtUpsertState      = "s_upsert_state"
	stmtDeleteState      = "s_delete_state"
)

// prepareUploadStatements prepares frequently used statements in the current transaction connection.
// pgx caches prepared statements per connection.
func (s *SyncService) prepareUploadStatements(ctx context.Context, tx pgx.Tx) error {
	// Use IF NOT EXISTS-like behavior: pgx will treat preparing the same name with identical SQL as a no-op.

	// s_gate_insert_upsert: idempotency gate for INSERT/UPDATE with payload; ON CONFLICT avoids duplicates.
	if _, err := tx.Prepare(ctx, stmtGateInsertUpsert, `
        INSERT INTO sync.server_change_log
            (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
        VALUES ($1, $2, $3, $4, $5::uuid, $6::jsonb, $7, $8, $9)
        ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING`); err != nil {
		return err
	}
	// s_gate_insert_delete: idempotency gate for DELETE (payload is NULL); ON CONFLICT avoids duplicates.
	if _, err := tx.Prepare(ctx, stmtGateInsertDelete, `
        INSERT INTO sync.server_change_log
            (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
        VALUES ($1, $2, $3, $4, $5::uuid, NULL, $6, $7, $8)
        ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING`); err != nil {
		return err
	}
	// s_ensure_meta: create sync_row_meta for brand-new rows (sv=0) if missing; idempotent upsert.
	if _, err := tx.Prepare(ctx, stmtEnsureMeta, `
        INSERT INTO sync.sync_row_meta (user_id, schema_name, table_name, pk_uuid)
        VALUES ($1, $2, $3, $4::uuid)
        ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO NOTHING`); err != nil {
		return err
	}
	// s_version_gate_upsert: optimistic concurrency for upserts; increments version when server_version matches.
	if _, err := tx.Prepare(ctx, stmtVersionGateUp, `
        UPDATE sync.sync_row_meta
        SET server_version = server_version + 1,
            deleted = FALSE,
            updated_at = now()
        WHERE user_id = $1 AND schema_name = $2 AND table_name = $3
          AND pk_uuid = $4::uuid
          AND server_version = $5
        RETURNING server_version`); err != nil {
		return err
	}
	// s_version_gate_delete: optimistic concurrency for deletes; marks deleted when server_version matches.
	if _, err := tx.Prepare(ctx, stmtVersionGateDel, `
        UPDATE sync.sync_row_meta
        SET server_version = server_version + 1,
            deleted = TRUE,
            updated_at = now()
        WHERE user_id = $1 AND schema_name = $2 AND table_name = $3
          AND pk_uuid = $4::uuid
          AND server_version = $5
        RETURNING server_version`); err != nil {
		return err
	}
	// s_upsert_state: upsert after-image payload into sync_state; idempotent via ON CONFLICT.
	if _, err := tx.Prepare(ctx, stmtUpsertState, `
        INSERT INTO sync.sync_state (user_id, schema_name, table_name, pk_uuid, payload)
        VALUES ($1, $2, $3, $4::uuid, $5::jsonb)
        ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO UPDATE
        SET payload = EXCLUDED.payload`); err != nil {
		return err
	}
	// s_delete_state: delete after-image from sync_state after a successful delete version gate.
	if _, err := tx.Prepare(ctx, stmtDeleteState, `
        DELETE FROM sync.sync_state
        WHERE user_id = $1 AND schema_name = $2 AND table_name = $3 AND pk_uuid = $4::uuid`); err != nil {
		return err
	}
	return nil
}
