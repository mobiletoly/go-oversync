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

	stmtApplyUpsertBatch = "s_apply_upsert_batch"
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
  VALUES ($1, $2, $3, $4, $5::uuid, $6::json, $7, $8::bigint, ($9 + 1))
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
  SELECT $1, $2, $3, $5::uuid, $6::json
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

	// s_apply_upsert_batch: set-based apply for INSERT/UPDATE using array inputs to reduce
	// per-row round trips. Returns one result row per input row in ordinality order.
	if _, err := tx.Prepare(ctx, stmtApplyUpsertBatch, `
WITH inp AS (
  SELECT
    u.schema_name,
    u.table_name,
    u.op,
    u.pk_uuid::uuid AS pk_uuid,
    u.payload::json AS payload,
    u.source_change_id,
    u.base_version,
    u.ord
  FROM unnest(
    $3::text[],
    $4::text[],
    $5::text[],
    $6::text[],
    $7::text[],
    $8::bigint[],
    $9::bigint[]
  ) WITH ORDINALITY AS u(schema_name, table_name, op, pk_uuid, payload, source_change_id, base_version, ord)
),
gate AS MATERIALIZED (
  INSERT INTO sync.server_change_log
      (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
  SELECT $1, i.schema_name, i.table_name, i.op, i.pk_uuid, i.payload, $2, i.source_change_id, (i.base_version + 1)
  FROM inp i
  ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING
  RETURNING source_change_id
),
gate_rows AS (
  SELECT i.ord, i.source_change_id
  FROM inp i
  JOIN gate g ON g.source_change_id = i.source_change_id
),
meta_upd AS (
  UPDATE sync.sync_row_meta m
  SET server_version = m.server_version + 1,
      deleted = FALSE,
      updated_at = now()
  FROM inp i
  JOIN gate g ON g.source_change_id = i.source_change_id
  WHERE m.user_id = $1
    AND m.schema_name = i.schema_name
    AND m.table_name = i.table_name
    AND m.pk_uuid = i.pk_uuid
    AND m.server_version = i.base_version
  RETURNING i.ord, m.server_version AS new_server_version
),
meta_ins AS (
  INSERT INTO sync.sync_row_meta (user_id, schema_name, table_name, pk_uuid, server_version, deleted, updated_at)
  SELECT $1, i.schema_name, i.table_name, i.pk_uuid, (i.base_version + 1), FALSE, now()
  FROM inp i
  JOIN gate g ON g.source_change_id = i.source_change_id
  WHERE i.base_version = 0
  ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO NOTHING
  RETURNING schema_name, table_name, pk_uuid, server_version
),
meta_ins_mapped AS (
  SELECT i.ord, mi.server_version AS new_server_version
  FROM inp i
  JOIN gate g ON g.source_change_id = i.source_change_id
  JOIN meta_ins mi
    ON mi.schema_name = i.schema_name
   AND mi.table_name = i.table_name
   AND mi.pk_uuid = i.pk_uuid
  WHERE i.base_version = 0
),
applied AS (
  SELECT ord, new_server_version FROM meta_upd
  UNION ALL
  SELECT ord, new_server_version FROM meta_ins_mapped
),
state_upsert AS (
  INSERT INTO sync.sync_state (user_id, schema_name, table_name, pk_uuid, payload)
  SELECT $1, i.schema_name, i.table_name, i.pk_uuid, i.payload
  FROM inp i
  JOIN applied a ON a.ord = i.ord
  ON CONFLICT (user_id, schema_name, table_name, pk_uuid) DO UPDATE
  SET payload = EXCLUDED.payload
  RETURNING 1
),
gate_cleanup AS (
  DELETE FROM sync.server_change_log l
  USING gate_rows gr
  LEFT JOIN applied a ON a.ord = gr.ord
  WHERE a.ord IS NULL
    AND l.user_id = $1
    AND l.source_id = $2
    AND l.source_change_id = gr.source_change_id
  RETURNING 1
)
SELECT
  CASE
    WHEN gr.ord IS NULL THEN 0
    WHEN a.ord IS NOT NULL THEN 1
    WHEN inp.base_version = 0 THEN 2
    WHEN EXISTS (
      SELECT 1 FROM sync.sync_row_meta m
      WHERE m.user_id = $1 AND m.schema_name = inp.schema_name AND m.table_name = inp.table_name AND m.pk_uuid = inp.pk_uuid
    ) THEN 2
    ELSE 3
  END AS code,
  COALESCE(a.new_server_version, 0) AS new_server_version
FROM inp
LEFT JOIN gate_rows gr ON gr.ord = inp.ord
LEFT JOIN applied a ON a.ord = inp.ord
ORDER BY inp.ord
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
