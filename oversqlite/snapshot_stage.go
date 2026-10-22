// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"fmt"

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
