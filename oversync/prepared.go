// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Statement names for hot-path operations
const (
	stmtApplyUpsert = "s_apply_upsert"
	stmtApplyDelete = "s_apply_delete"
)

// prepareUploadStatements prepares frequently used statements in the current transaction connection.
// pgx caches prepared statements per connection.
func (s *SyncService) prepareUploadStatements(ctx context.Context, tx pgx.Tx) error {
	// Use IF NOT EXISTS-like behavior: pgx will treat preparing the same name with identical SQL as a no-op.

	// s_apply_upsert: single-statement apply for INSERT/UPDATE, including idempotency gate,
	// optimistic version gate, sync_state upsert, and server_change_log insertion.
	//
	// NOTE: Idempotency is enforced via an insert-first gate into server_change_log using the
	// unique (user_id, source_id, source_change_id) constraint. Concurrent duplicates block on
	// the unique index until the first tx commits/rolls back, then become idempotent.
	//
	// server_change_log is inserted first as the gate; if the change is not actually applied
	// (e.g. version conflict), we delete the gate row in the same statement.
	//
	// Returns:
	//   code:
	//     0 = idempotent (triplet already exists)
	//     1 = applied
	//     2 = conflict (row exists but version mismatch)
	//     3 = internal error (missing server row for non-zero base version)
	//   new_server_version: (base_version + 1) when applied, else 0
	if _, err := tx.Prepare(ctx, stmtApplyUpsert, `
WITH gate AS MATERIALIZED (
  INSERT INTO sync.server_change_log
      (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
  VALUES ($1, $2, $3, $4, $5::uuid, $6::jsonb, $7, $8::bigint, ($9 + 1))
  ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING
  RETURNING 1
),
meta_upd AS (
  UPDATE sync.sync_row_meta
  SET server_version = server_version + 1,
      deleted = FALSE,
      updated_at = now()
  WHERE user_id = $1
    AND schema_name = $2
    AND table_name = $3
    AND pk_uuid = $5::uuid
    AND server_version = $9
    AND EXISTS (SELECT 1 FROM gate)
  RETURNING server_version
),
meta_ins AS (
  INSERT INTO sync.sync_row_meta (user_id, schema_name, table_name, pk_uuid, server_version, deleted, updated_at)
  SELECT $1, $2, $3, $5::uuid, ($9 + 1), FALSE, now()
  WHERE $9 = 0
    AND EXISTS (SELECT 1 FROM gate)
    AND NOT EXISTS (
      SELECT 1
      FROM sync.sync_row_meta
      WHERE user_id = $1 AND schema_name = $2 AND table_name = $3 AND pk_uuid = $5::uuid
    )
  ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO NOTHING
  RETURNING server_version
),
applied AS (
  SELECT server_version AS new_server_version FROM meta_upd
  UNION ALL
  SELECT server_version AS new_server_version FROM meta_ins
),
state_upsert AS (
  INSERT INTO sync.sync_state (user_id, schema_name, table_name, pk_uuid, payload)
  SELECT $1, $2, $3, $5::uuid, $6::jsonb
  WHERE EXISTS (SELECT 1 FROM applied)
  ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO UPDATE
  SET payload = EXCLUDED.payload
  RETURNING 1
),
gate_cleanup AS (
  DELETE FROM sync.server_change_log
  WHERE EXISTS (SELECT 1 FROM gate)
    AND NOT EXISTS (SELECT 1 FROM applied)
    AND user_id = $1
    AND source_id = $7
    AND source_change_id = $8::bigint
  RETURNING 1
)
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM gate) THEN 0
    WHEN EXISTS (SELECT 1 FROM applied) THEN 1
    WHEN $9 = 0 THEN 2
    WHEN EXISTS (
      SELECT 1 FROM sync.sync_row_meta
      WHERE user_id = $1 AND schema_name = $2 AND table_name = $3 AND pk_uuid = $5::uuid
    ) THEN 2
    ELSE 3
  END AS code,
  COALESCE((SELECT new_server_version FROM applied LIMIT 1), 0) AS new_server_version
`); err != nil {
		return err
	}

	// s_apply_delete: single-statement apply for DELETE, including idempotency gate,
	// optimistic version gate, sync_state delete, and server_change_log insertion.
	//
	// NOTE: Idempotency is enforced via an insert-first gate into server_change_log using the
	// unique (user_id, source_id, source_change_id) constraint. Concurrent duplicates block on
	// the unique index until the first tx commits/rolls back, then become idempotent.
	//
	// server_change_log is inserted first as the gate; if the change is not actually applied
	// (e.g. version conflict), we delete the gate row in the same statement.
	//
	// Returns:
	//   code:
	//     0 = idempotent (triplet already exists)
	//     1 = applied
	//     2 = conflict (row exists but version mismatch)
	//     3 = idempotent (row does not exist / already deleted)
	//   new_server_version: (base_version + 1) when applied, else 0
	if _, err := tx.Prepare(ctx, stmtApplyDelete, `
WITH gate AS MATERIALIZED (
  INSERT INTO sync.server_change_log
      (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
  VALUES ($1, $2, $3, $4, $5::uuid, NULL, $6, $7::bigint, ($8 + 1))
  ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING
  RETURNING 1
),
meta_upd AS (
  UPDATE sync.sync_row_meta
  SET server_version = server_version + 1,
      deleted = TRUE,
      updated_at = now()
  WHERE user_id = $1
    AND schema_name = $2
    AND table_name = $3
    AND pk_uuid = $5::uuid
    AND server_version = $8
    AND EXISTS (SELECT 1 FROM gate)
  RETURNING server_version
),
state_delete AS (
  DELETE FROM sync.sync_state
  WHERE user_id = $1
    AND schema_name = $2
    AND table_name = $3
    AND pk_uuid = $5::uuid
    AND EXISTS (SELECT 1 FROM meta_upd)
  RETURNING 1
),
gate_cleanup AS (
  DELETE FROM sync.server_change_log
  WHERE EXISTS (SELECT 1 FROM gate)
    AND NOT EXISTS (SELECT 1 FROM meta_upd)
    AND user_id = $1
    AND source_id = $6
    AND source_change_id = $7::bigint
  RETURNING 1
)
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM gate) THEN 0
    WHEN EXISTS (SELECT 1 FROM meta_upd) THEN 1
    WHEN EXISTS (
      SELECT 1 FROM sync.sync_row_meta
      WHERE user_id = $1 AND schema_name = $2 AND table_name = $3 AND pk_uuid = $5::uuid
    ) THEN 2
    ELSE 3
  END AS code,
  COALESCE((SELECT server_version FROM meta_upd LIMIT 1), 0) AS new_server_version
`); err != nil {
		return err
	}
	return nil
}
