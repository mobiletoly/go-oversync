// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) clearSnapshotStage(ctx context.Context) error {
	if _, err := c.db.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return fmt.Errorf("failed to clear snapshot staging: %w", err)
	}
	return nil
}

func (c *Client) stageSnapshotChunk(ctx context.Context, chunk *oversync.SnapshotChunkResponse, afterRowOrdinal int64) error {
	return c.stageSnapshotChunkWithFinalizer(ctx, chunk, afterRowOrdinal, nil)
}

func (c *Client) stageSnapshotChunkWithFinalizer(ctx context.Context, chunk *oversync.SnapshotChunkResponse, afterRowOrdinal int64, finalize func(context.Context, *immediateTx) error) error {
	return c.completeWork(ctx, "snapshot chunk stage", func(ctx context.Context) error {
		return c.stageSnapshotChunkWithFinalizerSlice(ctx, chunk, afterRowOrdinal, finalize)
	})
}

func (c *Client) stageSnapshotChunkWithFinalizerSlice(
	ctx context.Context,
	chunk *oversync.SnapshotChunkResponse,
	afterRowOrdinal int64,
	finalize func(context.Context, *immediateTx) error,
) (resultErr error) {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin snapshot stage transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "snapshot stage transaction")

	for idx, row := range chunk.Rows {
		if err := c.validateSnapshotRowForSession(&row, chunk.SnapshotBundleSeq); err != nil {
			return fmt.Errorf("invalid snapshot row %d: %w", idx, err)
		}
		keyJSON, _, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		if err != nil {
			return err
		}
		rowOrdinal, err := checkedAddInt64(afterRowOrdinal, int64(idx)+1)
		if err != nil {
			return fmt.Errorf("snapshot stage row ordinal overflow: %w", err)
		}
		unit := applyUnit("snapshot-stage", chunk.SnapshotBundleSeq, chunk.SnapshotID)
		charge, err := c.stageWorkspaceCharge(unit, int64(len(row.Payload)), chunk.SnapshotID, row.Schema, row.Table, keyJSON)
		if err != nil {
			return err
		}
		if err := c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, charge); err != nil {
			return err
		}
		if err := c.reserveSyncWork(ctx, unit, 8); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			INSERT INTO _sync_snapshot_stage (
				snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
			)
			SELECT ?, ?, ?, ?, ?, ?, ?
			WHERE NOT EXISTS (
				SELECT 1
				FROM _sync_snapshot_stage
				WHERE snapshot_id = ?
				  AND schema_name = ?
				  AND table_name = ?
				  AND key_json = ?
			)
		`,
			chunk.SnapshotID, rowOrdinal, row.Schema, row.Table, keyJSON, row.RowVersion, string(row.Payload),
			chunk.SnapshotID, row.Schema, row.Table, keyJSON,
		)
		if err != nil {
			return fmt.Errorf("failed to stage snapshot row %d for %s.%s: %w", rowOrdinal, row.Schema, row.Table, err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to inspect staged snapshot row %d for %s.%s: %w", rowOrdinal, row.Schema, row.Table, err)
		}
		if inserted != 1 {
			return fmt.Errorf("duplicate snapshot row identity for %s.%s at row %d", row.Schema, row.Table, rowOrdinal)
		}
	}

	if finalize != nil {
		if err := finalize(ctx, tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit snapshot stage transaction: %w", err)
	}
	return nil
}

func (c *Client) ensureFreshRotatedSourceInTx(ctx context.Context, tx sqliteTransaction, sourceID string) error {
	if err := validateSourceID(sourceID); err != nil {
		return fmt.Errorf("newSourceID must be provided for rotated rebuild")
	}
	if err := ensureSourceState(ctx, tx, sourceID); err != nil {
		return err
	}
	state, err := loadSourceState(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("missing source state for rotated source")
	}
	if state.NextSourceBundleID != 1 || state.ReplacedBySourceID != "" {
		return fmt.Errorf("rotated rebuild requires a fresh source id; reserved source is already in use")
	}
	return nil
}

func (c *Client) retargetPreparedOutboxInTx(ctx context.Context, tx sqliteTransaction, sourceID string, sourceBundleID int64) error {
	if sourceBundleID <= 0 {
		return fmt.Errorf("sourceBundleID must be positive when preserving prepared outbox")
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if outbox.State != outboxStatePrepared {
		return fmt.Errorf("cannot preserve outbox during source recovery when outbox state is %q", outbox.State)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_outbox_rows SET source_bundle_id = ? WHERE source_bundle_id = ?`, sourceBundleID, outbox.SourceBundleID); err != nil {
		return fmt.Errorf("failed to retarget preserved outbox rows: %w", err)
	}
	outbox.SourceID = sourceID
	outbox.SourceBundleID = sourceBundleID
	if err := persistOutboxBundle(ctx, tx, outbox); err != nil {
		return err
	}
	return nil
}

func (c *Client) reapplyPreparedOutboxIntentLocallyInTx(ctx context.Context, stmtCache *txStmtCache, tx sqliteTransaction) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT table_name, key_json, op, local_payload
		FROM _sync_outbox_rows
		ORDER BY row_ordinal
	`)
	if err != nil {
		return fmt.Errorf("failed to query preserved outbox rows for local replay: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tableName    string
			keyJSON      string
			op           string
			localPayload sql.NullString
		)
		if err := rows.Scan(&tableName, &keyJSON, &op, &localPayload); err != nil {
			return fmt.Errorf("failed to scan preserved outbox row for local replay: %w", err)
		}

		switch op {
		case oversync.OpDelete:
			localPK, _, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
			if err != nil {
				return err
			}
			pkColumn, err := c.primaryKeyColumnForTable(tableName)
			if err != nil {
				return err
			}
			pkValue, err := c.convertPKForQueryInTx(tx, tableName, localPK)
			if err != nil {
				return fmt.Errorf("failed to convert preserved outbox delete key for %s: %w", tableName, err)
			}
			query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(tableName), quoteIdent(pkColumn))
			if _, err := stmtCache.ExecContext(ctx, query, pkValue); err != nil {
				return fmt.Errorf("failed to replay preserved outbox delete for %s: %w", tableName, err)
			}
		case oversync.OpInsert, oversync.OpUpdate:
			if !localPayload.Valid {
				return fmt.Errorf("preserved outbox row for %s is missing local_payload", tableName)
			}
			payload, err := jcs.DecodeObject([]byte(localPayload.String))
			if err != nil {
				return fmt.Errorf("failed to decode preserved outbox payload for %s: %w", tableName, err)
			}
			if err := c.upsertLocalRowInTxUsing(ctx, stmtCache, tx, tableName, payload); err != nil {
				return fmt.Errorf("failed to replay preserved outbox upsert for %s: %w", tableName, err)
			}
		default:
			return fmt.Errorf("unsupported preserved outbox op %q", op)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate preserved outbox rows for local replay: %w", err)
	}
	return nil
}
