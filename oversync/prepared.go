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
	// Returns:
	//   code:
	//     0 = idempotent (triplet already exists)
	//     1 = applied
	//     2 = conflict (row exists but version mismatch)
	//     3 = internal error (missing server row for non-zero base version)
	//   new_server_version: (base_version + 1) when applied, else 0
	if _, err := tx.Prepare(ctx, stmtApplyUpsert, `
WITH gate AS (
  INSERT INTO sync.server_change_log
      (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
  VALUES ($1, $2, $3, $4, $5::uuid, $6::jsonb, $7, $8, ($9 + 1))
  ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING
  RETURNING 1
),
ensure_meta AS (
  INSERT INTO sync.sync_row_meta (user_id, schema_name, table_name, pk_uuid)
  SELECT $1, $2, $3, $5::uuid
  WHERE $9 = 0 AND EXISTS (SELECT 1 FROM gate)
  ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO NOTHING
),
vg AS (
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
  RETURNING 1
),
state_upsert AS (
  INSERT INTO sync.sync_state (user_id, schema_name, table_name, pk_uuid, payload)
  SELECT $1, $2, $3, $5::uuid, $6::jsonb
  FROM vg
  ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO UPDATE
  SET payload = EXCLUDED.payload
),
cleanup AS (
  DELETE FROM sync.server_change_log
  WHERE user_id = $1
    AND source_id = $7
    AND source_change_id = $8
    AND EXISTS (SELECT 1 FROM gate)
    AND NOT EXISTS (SELECT 1 FROM vg)
)
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM gate) THEN 0
    WHEN EXISTS (SELECT 1 FROM vg) THEN 1
    WHEN EXISTS (
      SELECT 1
      FROM sync.sync_row_meta
      WHERE user_id = $1 AND schema_name = $2 AND table_name = $3 AND pk_uuid = $5::uuid
    ) THEN 2
    ELSE 3
  END AS code,
  CASE WHEN EXISTS (SELECT 1 FROM vg) THEN ($9 + 1) ELSE 0 END AS new_server_version
`); err != nil {
		return err
	}

	// s_apply_delete: single-statement apply for DELETE, including idempotency gate,
	// optimistic version gate, sync_state delete, and server_change_log insertion.
	//
	// Returns:
	//   code:
	//     0 = idempotent (triplet already exists)
	//     1 = applied
	//     2 = conflict (row exists but version mismatch)
	//     3 = idempotent (row does not exist / already deleted)
	//   new_server_version: (base_version + 1) when applied, else 0
	if _, err := tx.Prepare(ctx, stmtApplyDelete, `
WITH gate AS (
  INSERT INTO sync.server_change_log
      (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
  VALUES ($1, $2, $3, $4, $5::uuid, NULL, $6, $7, ($8 + 1))
  ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING
  RETURNING 1
),
vg AS (
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
  RETURNING 1
),
state_delete AS (
  DELETE FROM sync.sync_state
  WHERE user_id = $1
    AND schema_name = $2
    AND table_name = $3
    AND pk_uuid = $5::uuid
    AND EXISTS (SELECT 1 FROM vg)
),
cleanup AS (
  DELETE FROM sync.server_change_log
  WHERE user_id = $1
    AND source_id = $6
    AND source_change_id = $7
    AND EXISTS (SELECT 1 FROM gate)
    AND NOT EXISTS (SELECT 1 FROM vg)
)
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM gate) THEN 0
    WHEN EXISTS (SELECT 1 FROM vg) THEN 1
    WHEN EXISTS (
      SELECT 1
      FROM sync.sync_row_meta
      WHERE user_id = $1 AND schema_name = $2 AND table_name = $3 AND pk_uuid = $5::uuid
    ) THEN 2
    ELSE 3
  END AS code,
  CASE WHEN EXISTS (SELECT 1 FROM vg) THEN ($8 + 1) ELSE 0 END AS new_server_version
`); err != nil {
		return err
	}
	return nil
}
