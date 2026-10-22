// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

type snapshotApplyOptions struct {
	RotateSource              bool
	NewSourceID               string
	ReplacementReason         string
	ClearSourceRecovery       bool
	RequireFreshRotatedSource bool
	FinalizeRemoteReplace     bool
	RemoteTargetUserID        string
	PinnedGuard               *snapshotApplyGuard
	UpgradeFinalizer          func(context.Context, *immediateTx) error
}

func (c *Client) applyStagedSnapshotLocked(ctx context.Context, session *oversync.SnapshotSession, options snapshotApplyOptions) error {
	return c.completeWork(ctx, "snapshot apply", func(ctx context.Context) error { return c.applyStagedSnapshotLockedSlice(ctx, session, options) })
}

func (c *Client) applyStagedSnapshotLockedSlice(ctx context.Context, session *oversync.SnapshotSession, options snapshotApplyOptions) (resultErr error) {
	ctx = withUniqueApplyContext(ctx)
	uniqueInvocation(ctx).snapshot = true
	guard, err := c.resolveSnapshotApplyGuardLocked(ctx, options)
	if err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin staged snapshot apply transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "staged snapshot apply transaction")
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	if err := c.validateFinalSnapshotApplyGate(ctx, tx, guard, session); err != nil {
		return err
	}
	if err := c.validateStagedSnapshotForApply(ctx, tx, session); err != nil {
		return err
	}
	snapshotSource := c.sourceID
	if options.RotateSource {
		snapshotSource = options.NewSourceID
	}
	if !options.FinalizeRemoteReplace {
		floor, err := c.snapshotFloor(ctx, tx)
		if err != nil {
			return err
		}
		if session.SnapshotBundleSeq < floor {
			return fmt.Errorf("snapshot boundary precedes installed observation floor")
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for staged snapshot apply: %w", err)
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	if !options.FinalizeRemoteReplace && (attachment.BindingState != attachmentBindingAttached || strings.TrimSpace(attachment.AttachedUserID) == "") {
		return &AttachRequiredError{Operation: "Rebuild()"}
	}
	if err := c.prepareSnapshotApply(ctx, tx, session); err != nil {
		return commitApplyProgress(ctx, tx, err)
	}
	// Preparation is auxiliary and may span slices. Rehash once the complete
	// plan is ready, on this final writer, before any business DML.
	if err := c.verifySnapshotStageHash(ctx, tx, session, snapshotSource); err != nil {
		return commitApplyProgress(ctx, tx, err)
	}
	if err := c.validateSnapshotPreservation(ctx, tx); err != nil {
		return commitApplyProgress(ctx, tx, err)
	}
	if err := c.readyAutomaticRecovery(ctx, tx); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if err := c.installUniqueFinalState(ctx, tx, stmtCache); err != nil {
		return err
	}

	if err := c.installStagedSnapshotRowsInTx(ctx, stmtCache, tx, session); err != nil {
		return err
	}
	if c.afterSnapshotRowsHook != nil {
		c.afterSnapshotRowsHook()
	}
	if err := c.installSnapshotAbsenceBaselines(ctx, tx, session); err != nil {
		return err
	}
	if err := c.completeSnapshotDesiredState(ctx, tx, stmtCache, session); err != nil {
		return err
	}
	if err := c.completeAutomaticRecovery(ctx, tx, session.SnapshotBundleSeq); err != nil {
		return err
	}
	targetSourceID, err := c.finalizeSnapshotApplyStateInTx(ctx, tx, attachment, guard, session, options)
	if err != nil {
		return err
	}
	if err := deleteSnapshotApplyPages(ctx, tx, session.SnapshotID); err != nil {
		return fmt.Errorf("failed to clear snapshot staging after apply: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := stmtCache.Close(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit staged snapshot apply: %w", err)
	}
	if options.RotateSource || options.FinalizeRemoteReplace {
		c.sourceID = targetSourceID
	}
	return nil
}

func (c *Client) resolveSnapshotApplyGuardLocked(ctx context.Context, options snapshotApplyOptions) (*snapshotApplyGuard, error) {
	if options.PinnedGuard != nil {
		return options.PinnedGuard, nil
	}
	if !options.FinalizeRemoteReplace {
		attachment, err := loadAttachmentState(ctx, c.db)
		if err != nil {
			return nil, err
		}
		if attachment.BindingState != attachmentBindingAttached || strings.TrimSpace(attachment.AttachedUserID) == "" {
			return nil, &AttachRequiredError{Operation: "Rebuild()"}
		}
	}
	return c.pinSnapshotApplyGuard(ctx, options)
}

func (c *Client) installStagedSnapshotRowsInTx(ctx context.Context, stmtCache *txStmtCache, tx sqliteTransaction, session *oversync.SnapshotSession) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT schema_name, table_name, key_json, row_version, payload
		FROM _sync_snapshot_stage
		WHERE snapshot_id = ?
		ORDER BY row_ordinal
	`, session.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to query staged snapshot rows: %w", err)
	}
	defer rows.Close()

	var stagedRowCount int64
	for rows.Next() {
		var (
			schemaName string
			tableName  string
			keyJSON    string
			rowVersion int64
			payload    string
		)
		if err := rows.Scan(&schemaName, &tableName, &keyJSON, &rowVersion, &payload); err != nil {
			return fmt.Errorf("failed to scan staged snapshot row: %w", err)
		}
		atomicRecordMax(&c.snapshotStats.maxLiveStagedApplyRows, 1)
		atomicRecordMax(&c.snapshotStats.maxLiveStagedApplyText, int64(len(schemaName)+len(tableName)+len(keyJSON)+len(payload)))

		localPK, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return err
		}
		bundleRow := oversync.BundleRow{
			Schema:     schemaName,
			Table:      tableName,
			Key:        wireKey,
			Op:         oversync.OpInsert,
			RowVersion: rowVersion,
			Payload:    json.RawMessage(payload),
		}
		atomicRecordMax(&c.snapshotStats.maxAppliedInMemoryRows, 1)
		if err := c.applyBundleRowAuthoritativelyInTxUsing(ctx, stmtCache, tx, &bundleRow, localPK, baselineObservation{Origin: "snapshot", BundleSeq: &session.SnapshotBundleSeq}); err != nil {
			return fmt.Errorf("failed to apply staged snapshot row for %s.%s: %w", schemaName, tableName, err)
		}
		stagedRowCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate staged snapshot rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to close staged snapshot rows: %w", err)
	}
	if stagedRowCount != session.RowCount {
		return fmt.Errorf("staged snapshot row count %d does not match expected row_count %d", stagedRowCount, session.RowCount)
	}
	return nil
}

func (c *Client) finalizeSnapshotApplyStateInTx(ctx context.Context, tx *immediateTx, attachment *attachmentStateRecord, guard *snapshotApplyGuard, session *oversync.SnapshotSession, options snapshotApplyOptions) (string, error) {
	targetSourceID := c.sourceID
	if options.FinalizeRemoteReplace {
		targetSourceID = attachment.CurrentSourceID
		if targetSourceID == "" {
			targetSourceID = c.sourceID
		}
		if err := ensureSourceState(ctx, tx, targetSourceID); err != nil {
			return "", err
		}
		attachment.BindingState = attachmentBindingAttached
		attachment.AttachedUserID = strings.TrimSpace(options.RemoteTargetUserID)
		attachment.SchemaName = c.config.schema
		attachment.PendingInitializationID = ""
		if identity, identityErr := loadUpgradeIdentity(ctx, tx); identityErr != nil {
			return "", identityErr
		} else if identity != nil {
			if identity.UserID != "" && identity.UserID != attachment.AttachedUserID {
				return "", fmt.Errorf("installed upgrade identity belongs to another user")
			}
			identity.UserID = attachment.AttachedUserID
			if err := persistUpgradeIdentity(ctx, tx, *identity); err != nil {
				return "", err
			}
		}
	}
	if options.RotateSource {
		targetSourceID = options.NewSourceID
		if err := validateSourceID(targetSourceID); err != nil {
			return "", err
		}
		if options.RequireFreshRotatedSource {
			if err := c.ensureFreshRotatedSourceInTx(ctx, tx, targetSourceID); err != nil {
				return "", err
			}
		} else if err := ensureSourceState(ctx, tx, targetSourceID); err != nil {
			return "", err
		}
		if c.sourceID != "" && c.sourceID != targetSourceID {
			if err := markSourceReplaced(ctx, tx, c.sourceID, targetSourceID); err != nil {
				return "", err
			}
		}
	}
	attachment.CurrentSourceID = targetSourceID
	attachment.LastBundleSeqSeen = session.SnapshotBundleSeq
	attachment.RebuildRequired = false
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return "", fmt.Errorf("failed to persist snapshot bundle checkpoint: %w", err)
	}
	if options.FinalizeRemoteReplace {
		if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
			return "", err
		}
	} else if options.ClearSourceRecovery {
		if err := c.clearSourceRecoveryRequiredInTx(ctx, tx); err != nil {
			return "", err
		}
	} else {
		operation, err := loadOperationState(ctx, tx)
		if err != nil {
			return "", err
		}
		if operation.Kind == operationKindNone {
			if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
				return "", err
			}
		}
	}
	if options.UpgradeFinalizer != nil {
		if err := options.UpgradeFinalizer(ctx, tx); err != nil {
			return "", err
		}
	}
	return targetSourceID, nil
}
