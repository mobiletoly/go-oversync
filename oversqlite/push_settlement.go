// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

type pushSettlementDecision struct {
	skipApply      bool
	requeueOp      string
	requeuePayload sql.NullString
	needsRequeue   bool
	desiredExists  bool
	desiredPayload sql.NullString
	desiredWire    sql.NullString
}

func (c *Client) applyStagedPushBundleLocked(ctx context.Context, uploaded []dirtyRowCapture, committed *committedPushBundle) (resultErr error) {
	if committed == nil {
		return fmt.Errorf("committed push bundle metadata is required")
	}
	if committed.SourceBundleID == int64(^uint64(0)>>1) {
		return fmt.Errorf("committed source_bundle_id cannot advance without overflow")
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin staged push replay transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "staged push replay transaction")
	stmtCache := newTxStmtCache(tx)
	defer func() { resultErr = errors.Join(resultErr, stmtCache.closeAndRelease(ctx)) }()
	ctx, execution, err := c.configureStatementCache(ctx, tx, stmtCache)
	if err != nil {
		return err
	}

	if _, err := execution.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for staged push replay: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, execution, true); err != nil {
		return err
	}
	stagedRows, effects, err := c.loadValidatedStagedPushEffectsInTx(ctx, execution, uploaded, committed)
	if err != nil {
		return err
	}
	decisions, err := c.buildPushSettlementPlanInTx(ctx, execution, uploaded, stagedRows, effects)
	if err != nil {
		return err
	}
	unit := applyUnit("push", committed.BundleSeq, committed.BundleHash)
	if err := c.prepareApplySurvivors(ctx, execution, unit, len(stagedRows), func(index int) (applySurvivor, error) {
		row := stagedRows[index]
		decision := decisions[pushSettlementKey(row.SchemaName, row.TableName, row.KeyJSON)]
		if decision.skipApply {
			return applySurvivor{table: row.TableName, pk: row.LocalPK, payload: decision.desiredPayload}, nil
		}
		return applySurvivor{table: row.TableName, pk: row.LocalPK, payload: row.Payload, wire: true}, nil
	}, "settlement"); err != nil {
		if cleanup := stmtCache.closeAndRelease(ctx); cleanup != nil {
			return errors.Join(err, cleanup)
		}
		return commitApplyProgress(ctx, tx, err)
	}
	if err := c.applyPushSettlementEffectsInTx(ctx, stmtCache, execution, stagedRows, decisions, committed.BundleSeq); err != nil {
		return err
	}
	if err := c.restorePushSettlementIntentInTx(ctx, stmtCache, execution, stagedRows, decisions, committed.BundleSeq); err != nil {
		return err
	}
	if err := c.restoreTriggerAffectedDirtyIntentInTx(ctx, stmtCache, execution); err != nil {
		return err
	}
	if err := c.verifyPushSettlementFinalStateInTx(ctx, execution, stagedRows, decisions); err != nil {
		return err
	}
	if _, err := c.restoreApplySurvivors(ctx, execution, stmtCache, true); err != nil {
		return err
	}
	if _, err := c.restorePrimarySettlementIntent(ctx, execution, stmtCache, true); err != nil {
		return err
	}
	if err := finishApplySurvivors(ctx, execution); err != nil {
		return err
	}
	if err := c.finalizeStagedPushBundleInTx(ctx, execution, committed); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, execution, false); err != nil {
		return err
	}
	if err := stmtCache.closeAndRelease(ctx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit staged push replay transaction: %w", err)
	}
	return nil
}

func (c *Client) restoreTriggerAffectedDirtyIntentInTx(ctx context.Context, stmtCache *txStmtCache, tx sqliteTransaction) error {
	for pass := 0; pass < 64; pass++ {
		changed, err := c.restoreApplySurvivors(ctx, tx, stmtCache, false)
		if err != nil {
			return err
		}
		primaryChanged, err := c.restorePrimarySettlementIntent(ctx, tx, stmtCache, false)
		if err != nil {
			return err
		}
		changed = changed || primaryChanged
		if !changed {
			return nil
		}
	}
	return fmt.Errorf("preserved desired state was changed by cross-row effects after bounded restoration")
}

func (c *Client) loadValidatedStagedPushEffectsInTx(ctx context.Context, tx sqliteTransaction, uploaded []dirtyRowCapture, committed *committedPushBundle) ([]stagedPushBundleRow, map[string]stagedPushBundleRow, error) {
	if scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope); scope != nil {
		var stage workspaceCharge
		if err := scanTransactionRow(ctx, tx, `SELECT push_stage_entries,push_stage_bytes FROM _sync_operation_state WHERE singleton_key=1`, nil, &stage.entries, &stage.bytes); err != nil {
			return nil, nil, err
		}
		entries, err := c.workspaceScale(scope.unit, stage.entries, 32)
		if err != nil {
			return nil, nil, err
		}
		bytes, err := c.workspaceScale(scope.unit, stage.bytes, 64)
		if err != nil {
			return nil, nil, err
		}
		charge, err := c.addWorkspaceCharge(scope.unit, scope.base, workspaceCharge{entries, bytes})
		if err != nil {
			return nil, nil, err
		}
		if err = c.admitPushBufferCopies(ctx, tx, charge); err != nil {
			return nil, nil, err
		}
	}
	if _, err := c.validateAuthoritativePushAgainstOutbox(ctx, tx, committed); err != nil {
		return nil, nil, err
	}
	_, validatedStageCount, completeStage, err := c.inspectReusablePushStageInTx(ctx, tx, committed)
	if err != nil {
		return nil, nil, fmt.Errorf("final committed effect stream validation failed: %w", err)
	}
	if !completeStage || validatedStageCount != committed.RowCount {
		return nil, nil, fmt.Errorf("final committed effect stream is incomplete: staged row count %d does not match expected row_count %d", validatedStageCount, committed.RowCount)
	}
	stagedRows, err := c.loadStagedPushBundleRowsInTx(ctx, tx, committed.BundleSeq)
	if err != nil {
		return nil, nil, err
	}
	if int64(len(stagedRows)) != committed.RowCount {
		return nil, nil, fmt.Errorf("staged push replay row count %d does not match expected row_count %d", len(stagedRows), committed.RowCount)
	}
	if err := verifyCommittedEffectCoverage(uploaded, stagedRows); err != nil {
		return nil, nil, err
	}
	effects := make(map[string]stagedPushBundleRow, len(stagedRows))
	for _, effect := range stagedRows {
		effects[pushSettlementKey(effect.SchemaName, effect.TableName, effect.KeyJSON)] = effect
		currentState, err := c.loadStructuredRowStateInTx(ctx, tx, effect.SchemaName, effect.TableName, effect.KeyJSON)
		if err != nil {
			return nil, nil, err
		}
		if currentState.Exists && currentState.RowVersion > effect.RowVersion {
			return nil, nil, fmt.Errorf("committed effect for %s.%s would lower row version from %d to %d", effect.SchemaName, effect.TableName, currentState.RowVersion, effect.RowVersion)
		}
	}
	return stagedRows, effects, nil
}

func (c *Client) buildPushSettlementPlanInTx(ctx context.Context, tx sqliteTransaction, uploaded []dirtyRowCapture, stagedRows []stagedPushBundleRow, effects map[string]stagedPushBundleRow) (map[string]pushSettlementDecision, error) {
	decisions := make(map[string]pushSettlementDecision, len(uploaded))
	held := workspaceCharge{}
	for _, uploadedRow := range uploaded {
		decisionKey := pushSettlementKey(uploadedRow.SchemaName, uploadedRow.TableName, uploadedRow.KeyJSON)
		effect, ok := effects[decisionKey]
		if !ok {
			return nil, fmt.Errorf("committed effect stream does not cover every frozen outbox key")
		}
		if err := c.admitSettlementDecision(ctx, tx, uploadedRow.SchemaName, uploadedRow.TableName, uploadedRow.KeyJSON, uploadedRow.LocalPK, &held); err != nil {
			return nil, err
		}
		currentDirty, err := c.loadDirtyUploadStateInTx(ctx, tx, uploadedRow.SchemaName, uploadedRow.TableName, uploadedRow.KeyJSON)
		if err != nil {
			return nil, err
		}
		livePayload, liveExists, err := c.serializeExistingRowInTx(ctx, tx, uploadedRow.TableName, uploadedRow.LocalPK)
		if err != nil {
			return nil, err
		}
		liveMatches, err := c.livePayloadMatchesUploadedIntent(uploadedRow, livePayload, liveExists)
		if err != nil {
			return nil, err
		}
		if !currentDirty.Exists && liveMatches {
			decisions[decisionKey] = pushSettlementDecision{}
			continue
		}
		decision, err := c.settlementDecisionForNewerState(effect, livePayload, liveExists)
		if err != nil {
			return nil, fmt.Errorf("failed to compare newer local intent with committed effect for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
		}
		decisions[decisionKey] = decision
	}
	for _, effect := range stagedRows {
		decisionKey := pushSettlementKey(effect.SchemaName, effect.TableName, effect.KeyJSON)
		if _, uploadedEffect := decisions[decisionKey]; uploadedEffect {
			continue
		}
		if err := c.admitSettlementDecision(ctx, tx, effect.SchemaName, effect.TableName, effect.KeyJSON, effect.LocalPK, &held); err != nil {
			return nil, err
		}
		currentDirty, err := c.loadDirtyUploadStateInTx(ctx, tx, effect.SchemaName, effect.TableName, effect.KeyJSON)
		if err != nil {
			return nil, err
		}
		if !currentDirty.Exists {
			continue
		}
		livePayload, liveExists, err := c.serializeExistingRowInTx(ctx, tx, effect.TableName, effect.LocalPK)
		if err != nil {
			return nil, err
		}
		decision, err := c.settlementDecisionForNewerState(effect, livePayload, liveExists)
		if err != nil {
			return nil, fmt.Errorf("failed to compare newer local intent with extra committed effect for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
		}
		decisions[decisionKey] = decision
	}
	return decisions, nil
}

func (c *Client) applyPushSettlementEffectsInTx(ctx context.Context, stmtCache *txStmtCache, tx sqliteTransaction, stagedRows []stagedPushBundleRow, decisions map[string]pushSettlementDecision, bundleSeq int64) error {
	for _, row := range stagedRows {
		bundleRow := oversync.BundleRow{
			Schema:     row.SchemaName,
			Table:      row.TableName,
			Key:        row.WireKey,
			Op:         row.Op,
			RowVersion: row.RowVersion,
		}
		if row.Payload.Valid {
			bundleRow.Payload = json.RawMessage(row.Payload.String)
		}
		rowDecision, hasDecision := decisions[pushSettlementKey(row.SchemaName, row.TableName, row.KeyJSON)]
		if !hasDecision || !rowDecision.skipApply {
			if err := c.applyBundleRowAuthoritativelyInTxUsing(ctx, stmtCache, tx, &bundleRow, row.LocalPK, baselineObservation{Origin: "settlement", BundleSeq: &bundleSeq}); err != nil {
				return err
			}
			if hasDecision {
				if _, err := stmtCache.ExecContext(ctx, `
				DELETE FROM _sync_dirty_rows
				WHERE schema_name = ? AND table_name = ? AND key_json = ?
			`, row.SchemaName, row.TableName, row.KeyJSON); err != nil {
					return fmt.Errorf("failed to clear applied dirty row for %s.%s %s: %w", row.SchemaName, row.TableName, row.KeyJSON, err)
				}
			}
		}
	}
	return nil
}

func (c *Client) restorePushSettlementIntentInTx(ctx context.Context, stmtCache *txStmtCache, tx sqliteTransaction, stagedRows []stagedPushBundleRow, decisions map[string]pushSettlementDecision, bundleSeq int64) error {
	// Authoritative effects can run ordinary application triggers. Restore every
	// newer desired state only after all non-preserved effects have run, then
	// persist its authoritative base metadata and dirty disposition.
	for _, row := range stagedRows {
		rowDecision, hasDecision := decisions[pushSettlementKey(row.SchemaName, row.TableName, row.KeyJSON)]
		if !hasDecision || !rowDecision.skipApply {
			continue
		}
		kind := "live"
		if row.Op == oversync.OpDelete {
			kind = "deleted"
		}
		if err := c.installAuthoritativeBaseline(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, kind, row.RowVersion, []byte(row.Payload.String), baselineObservation{Origin: "settlement", BundleSeq: &bundleSeq}); err != nil {
			return err
		}
		if rowDecision.needsRequeue {
			if err := c.rebaseDirtyIntentInTx(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, rowDecision.requeueOp, row.RowVersion, rowDecision.requeuePayload); err != nil {
				return err
			}
		} else if _, err := stmtCache.ExecContext(ctx, `
			DELETE FROM _sync_dirty_rows
			WHERE schema_name = ? AND table_name = ? AND key_json = ?
		`, row.SchemaName, row.TableName, row.KeyJSON); err != nil {
			return fmt.Errorf("failed to clear satisfied dirty row for %s.%s %s: %w", row.SchemaName, row.TableName, row.KeyJSON, err)
		}
	}
	return nil
}

func (c *Client) verifyPushSettlementFinalStateInTx(ctx context.Context, tx sqliteTransaction, stagedRows []stagedPushBundleRow, decisions map[string]pushSettlementDecision) error {
	for _, row := range stagedRows {
		rowDecision, hasDecision := decisions[pushSettlementKey(row.SchemaName, row.TableName, row.KeyJSON)]
		if hasDecision && rowDecision.skipApply {
			if err := c.verifyPushSettlementDesiredStateInTx(ctx, tx, row, rowDecision); err != nil {
				return err
			}
			continue
		}
		if err := c.verifyPushSettlementAuthoritativeStateInTx(ctx, tx, row); err != nil {
			return err
		}
	}
	if err := validateForeignKeyIntegrity(ctx, tx); err != nil {
		return fmt.Errorf("staged push replay would violate local foreign keys: %w", err)
	}
	return nil
}

func (c *Client) finalizeStagedPushBundleInTx(ctx context.Context, tx sqliteTransaction, committed *committedPushBundle) error {
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	if committed.BundleSeq == attachment.LastBundleSeqSeen+1 {
		attachment.LastBundleSeqSeen = committed.BundleSeq
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	if err := updateSourceNextBundleID(ctx, tx, committed.SourceID, committed.SourceBundleID+1); err != nil {
		return err
	}
	if _, err := deleteSettlementPages(ctx, tx, "_sync_outbox_rows", "source_bundle_id", committed.SourceBundleID); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot for source_bundle_id %d: %w", committed.SourceBundleID, err)
	}
	if err := clearOutboxBundle(ctx, tx); err != nil {
		return err
	}
	if _, err := deleteSettlementPages(ctx, tx, "_sync_push_stage", "bundle_seq", committed.BundleSeq); err != nil {
		return fmt.Errorf("failed to clear staged committed bundle %d: %w", committed.BundleSeq, err)
	}
	return nil
}

func pushSettlementKey(schemaName, tableName, keyJSON string) string {
	return schemaName + "\x00" + tableName + "\x00" + keyJSON
}

// Each page is one primary reference. Measure encoded bytes before fetching its
// strings; a single oversized row is admitted separately from the soft page cap.
// The writer and full apply guard keep these references immutable until commit.
const settlementIntentTail = ` FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1`

func (c *Client) nextSettlementIntent(ctx context.Context, tx sqliteTransaction, p *applyPlan, cursor [3]string) ([3]string, applySurvivor, int64, error) {
	var key [3]string
	var row applySurvivor
	var size int64
	tail := settlementIntentTail
	if p.kind == "conflict" {
		tail = ` FROM (
 SELECT r.schema_name,r.table_name,r.key_json,CASE WHEN d.key_json IS NOT NULL THEN d.payload ELSE o.local_payload END AS payload
 FROM (SELECT * FROM (
 SELECT schema_name,table_name,key_json FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1)
 UNION ALL SELECT * FROM (
 SELECT schema_name,table_name,key_json FROM _sync_outbox_rows WHERE source_bundle_id=(SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key=1)
 AND (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1)
 ORDER BY schema_name,table_name,key_json LIMIT 1) r
 LEFT JOIN _sync_dirty_rows d ON (d.schema_name,d.table_name,d.key_json)=(r.schema_name,r.table_name,r.key_json)
 LEFT JOIN _sync_outbox_rows o ON o.source_bundle_id=(SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key=1)
 AND (o.schema_name,o.table_name,o.key_json)=(r.schema_name,r.table_name,r.key_json))`

	}
	args := []any{cursor[0], cursor[1], cursor[2]}
	if p.kind == "conflict" {
		args = append(args, cursor[0], cursor[1], cursor[2])
	}
	err := scanTransactionRow(ctx, tx, `SELECT 64+6*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))`+tail, args, &size)
	if err != nil {
		return key, row, 0, err
	}
	if err = c.checkApplyWorkspace(p, 8, 8*size); err != nil {
		return key, row, 0, err
	}
	err = scanTransactionRow(ctx, tx, `SELECT schema_name,table_name,key_json,payload`+tail, args, &key[0], &key[1], &key[2], &row.payload)
	if err != nil {
		return key, row, 0, err
	}
	c.a3Stats.primaryReads++
	c.a3Stats.primaryBytes += size
	c.a3Stats.indexedVisits += 2
	row.table = key[1]
	row.pk, _, err = c.decodeDirtyKeyForPush(tx, row.table, key[2])
	return key, row, size, err
}

func (c *Client) prepareSettlementIntent(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	for !p.preservationComplete {
		if err := c.reserveSyncWork(ctx, p.unit, 32); err != nil {
			return err
		}
		if err := admitApplyPlanCheckpoint(ctx, tx); err != nil {
			return err
		}
		key, desired, size, err := c.nextSettlementIntent(ctx, tx, p, p.preserve)
		if err == sql.ErrNoRows {
			p.preservationComplete = true
			return finishApplyPreparationStep(ctx, tx, p, nil)
		}
		if err != nil {
			return finishApplyPreparationStep(ctx, tx, p, err)
		}
		if err = c.preserveSettlementException(ctx, tx, p, desired, size); err != nil {
			return finishApplyPreparationStep(ctx, tx, p, err)
		}
		// All 64 allowed comparisons/restores, final verification, and keyset
		// metadata visits are prepaid before the atomic mutation starts.
		if p.atomicWork > int64(^uint64(0)>>1)-1024 {
			return &SyncWorkspaceLimitError{Unit: p.unit, Dimension: "atomic_apply_work", Limit: c.config.MaxSyncWorkUnitsPerInvocation, Used: c.syncWorkUsed, Reserved: int64(^uint64(0) >> 1)}
		}
		p.atomicWork += 1024
		p.preserve = key
		if err = finishApplyPreparationStep(ctx, tx, p, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) preserveSettlementException(ctx context.Context, tx sqliteTransaction, p *applyPlan, desired applySurvivor, size int64) error {
	c.a3Stats.duplicateChecks++
	var saved int
	err := scanTransactionRow(ctx, tx, `SELECT 1 FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, []any{desired.table, desired.pk}, &saved)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	live, err := c.captureApplySurvivorWithHeld(ctx, tx, p, desired.table, desired.pk, 8, 8*size)
	if err != nil {
		return err
	}
	equal, err := canonicalPayloadEqual(live.payload, desired.payload)
	if err != nil || equal {
		return err
	}
	live.role = "exception"
	return c.putApplySurvivor(ctx, tx, p, live)
}

func (c *Client) atomicApplyWork(p *applyPlan, roots int64) (int64, error) {
	work := p.atomicWork
	terms := [][2]int64{{roots, 16}, {p.entries, 528}, {p.edgeEntries, 2}, {1, 8}}
	if p.kind == "settlement" || p.kind == "conflict" || p.kind == "snapshot" {
		terms = [][2]int64{{roots, 32}, {p.entries, 1024}, {p.edgeEntries, 4}, {1, 64}}
	}
	for _, term := range terms {
		if term[0] < 0 || term[0] > (int64(^uint64(0)>>1)-work)/term[1] {
			return 0, &SyncWorkspaceLimitError{Unit: p.unit, Dimension: "atomic_apply_work", Limit: c.config.MaxSyncWorkUnitsPerInvocation, Used: c.syncWorkUsed, Reserved: int64(^uint64(0) >> 1)}
		}
		work += term[0] * term[1]
	}
	return work, nil
}

func (c *Client) restorePrimarySettlementIntent(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, verifyOnly bool) (bool, error) {
	c.a3Stats.restorationPasses++
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return false, err
	}
	changed := false
	var cursor [3]string
	for {
		if err = ctx.Err(); err != nil {
			return false, err
		}
		key, desired, size, e := c.nextSettlementIntent(ctx, tx, p, cursor)
		if e == sql.ErrNoRows {
			return changed, nil
		}
		if e != nil {
			return false, e
		}
		cursor = key
		c.a3Stats.duplicateChecks++
		var saved int
		e = scanTransactionRow(ctx, tx, `SELECT 1 FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, []any{desired.table, desired.pk}, &saved)
		if e == nil {
			continue
		}
		if e != sql.ErrNoRows {
			return false, e
		}
		matches, e := c.applySurvivorMatches(ctx, tx, p, desired, size)
		if e != nil {
			return false, e
		}
		if matches {
			continue
		}
		if verifyOnly {
			return false, fmt.Errorf("primary settlement intent final state mismatch")
		}
		if e = c.restoreApplySurvivor(ctx, tx, cache, desired); e != nil {
			return false, e
		}
		changed = true
	}
}

func (c *Client) settlementDecisionForNewerState(effect stagedPushBundleRow, livePayload json.RawMessage, liveExists bool) (pushSettlementDecision, error) {
	decision := pushSettlementDecision{
		skipApply: true, needsRequeue: true, desiredExists: liveExists,
		desiredPayload: sql.NullString{String: string(livePayload), Valid: liveExists},
	}
	var wireLivePayload json.RawMessage
	if liveExists {
		var err error
		wireLivePayload, err = c.processPayloadForUpload(effect.TableName, string(livePayload))
		if err != nil {
			return pushSettlementDecision{}, err
		}
		decision.desiredWire = sql.NullString{String: string(wireLivePayload), Valid: true}
	}
	targetLive := effect.Op != oversync.OpDelete
	switch {
	case liveExists && targetLive:
		decision.requeueOp, decision.requeuePayload = oversync.OpUpdate, sql.NullString{String: string(livePayload), Valid: true}
	case liveExists && !targetLive:
		decision.requeueOp, decision.requeuePayload = oversync.OpInsert, sql.NullString{String: string(livePayload), Valid: true}
	case !liveExists && targetLive:
		decision.requeueOp = oversync.OpDelete
	default:
		decision.requeueOp = oversync.OpDelete
	}
	return decision, nil
}

func (c *Client) restorePushSettlementDesiredStateInTx(ctx context.Context, execer execContexter, tx tableInfoQueryer, effect stagedPushBundleRow, decision pushSettlementDecision) error {
	if decision.desiredExists {
		if !decision.desiredWire.Valid {
			return fmt.Errorf("preserved desired state for %s.%s %s is missing its frozen wire payload", effect.SchemaName, effect.TableName, effect.KeyJSON)
		}
		payload, err := jcs.DecodeObject([]byte(decision.desiredWire.String))
		if err != nil {
			return fmt.Errorf("failed to decode preserved desired state for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
		}
		if err := c.upsertRowInTxUsing(ctx, execer, tx, effect.TableName, payload); err != nil {
			return fmt.Errorf("failed to restore preserved desired state for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
		}
		return nil
	}

	pkColumn, err := c.primaryKeyColumnForTable(effect.TableName)
	if err != nil {
		return err
	}
	pkValue, err := c.convertPKForQueryInTx(tx, effect.TableName, effect.LocalPK)
	if err != nil {
		return fmt.Errorf("failed to convert preserved absent key for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(effect.TableName), quoteIdent(pkColumn))
	if _, err := execer.ExecContext(ctx, query, pkValue); err != nil {
		return fmt.Errorf("failed to restore preserved absence for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
	}
	return nil
}

func (c *Client) verifyPushSettlementDesiredStateInTx(ctx context.Context, tx sqliteTransaction, effect stagedPushBundleRow, decision pushSettlementDecision) error {
	matches, err := c.pushSettlementDesiredStateMatchesInTx(ctx, tx, effect, decision)
	if err != nil {
		return err
	}
	if !matches {
		return fmt.Errorf("preserved desired state for %s.%s %s was changed by cross-row effects", effect.SchemaName, effect.TableName, effect.KeyJSON)
	}
	return nil
}

func (c *Client) verifyPushSettlementAuthoritativeStateInTx(ctx context.Context, tx sqliteTransaction, effect stagedPushBundleRow) error {
	livePayload, liveExists, err := c.serializeExistingRowInTx(ctx, tx, effect.TableName, effect.LocalPK)
	if err != nil {
		return err
	}
	targetExists := effect.Op != oversync.OpDelete
	if liveExists != targetExists {
		return fmt.Errorf("authoritative target for %s.%s %s was changed by cross-row effects", effect.SchemaName, effect.TableName, effect.KeyJSON)
	}
	if !targetExists {
		return nil
	}
	wirePayload, err := c.processPayloadForUpload(effect.TableName, string(livePayload))
	if err != nil {
		return fmt.Errorf("failed to encode authoritative target for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
	}
	normalizedTargetPayload, err := c.normalizeWirePayloadForSQLiteRoundTrip(effect.TableName, effect.Payload.String)
	if err != nil {
		return fmt.Errorf("failed to normalize authoritative target for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
	}
	equal, err := canonicalPayloadEqual(
		sql.NullString{String: string(wirePayload), Valid: true},
		sql.NullString{String: string(normalizedTargetPayload), Valid: true},
	)
	if err != nil {
		return fmt.Errorf("failed to compare authoritative target for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
	}
	if !equal {
		return fmt.Errorf("authoritative target for %s.%s %s was changed by cross-row effects", effect.SchemaName, effect.TableName, effect.KeyJSON)
	}
	return nil
}

func (c *Client) pushSettlementDesiredStateMatchesInTx(ctx context.Context, tx sqliteTransaction, effect stagedPushBundleRow, decision pushSettlementDecision) (bool, error) {
	livePayload, liveExists, err := c.serializeExistingRowInTx(ctx, tx, effect.TableName, effect.LocalPK)
	if err != nil {
		return false, err
	}
	if liveExists != decision.desiredExists {
		return false, nil
	}
	if !liveExists {
		return true, nil
	}
	equal, err := canonicalPayloadEqual(
		sql.NullString{String: string(livePayload), Valid: true},
		decision.desiredPayload,
	)
	if err != nil {
		return false, fmt.Errorf("failed to compare preserved desired state for %s.%s %s: %w", effect.SchemaName, effect.TableName, effect.KeyJSON, err)
	}
	return equal, nil
}

func verifyCommittedEffectCoverage(uploaded []dirtyRowCapture, effects []stagedPushBundleRow) error {
	identity := func(schema, table string, key oversync.SyncKey) (string, error) {
		wire, err := jcs.Marshal(key)
		if err != nil {
			return "", err
		}
		return schema + "\x00" + table + "\x00" + string(wire), nil
	}
	seen := make(map[string]struct{}, len(effects))
	for _, effect := range effects {
		id, err := identity(effect.SchemaName, effect.TableName, effect.WireKey)
		if err != nil {
			return fmt.Errorf("committed effect key is invalid: %w", err)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("committed effect stream contains a duplicate key")
		}
		seen[id] = struct{}{}
	}
	for _, request := range uploaded {
		id, err := identity(request.SchemaName, request.TableName, request.WireKey)
		if err != nil {
			return fmt.Errorf("frozen outbox key is invalid: %w", err)
		}
		if _, exists := seen[id]; !exists {
			return fmt.Errorf("committed effect stream does not cover every frozen outbox key")
		}
	}
	return nil
}

func (c *Client) loadStagedPushBundleRowsInTx(ctx context.Context, tx sqliteTransaction, bundleSeq int64) ([]stagedPushBundleRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT schema_name, table_name, key_json, op, row_version, payload
		FROM _sync_push_stage
		WHERE bundle_seq = ?
		ORDER BY row_ordinal
	`, bundleSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to query staged push rows: %w", err)
	}
	defer rows.Close()

	stagedRows := make([]stagedPushBundleRow, 0)
	for rows.Next() {
		var row stagedPushBundleRow
		if err := rows.Scan(&row.SchemaName, &row.TableName, &row.KeyJSON, &row.Op, &row.RowVersion, &row.Payload); err != nil {
			return nil, fmt.Errorf("failed to scan staged push row: %w", err)
		}
		localPK, wireKey, err := c.decodeDirtyKeyForPush(tx, row.TableName, row.KeyJSON)
		if err != nil {
			return nil, err
		}
		row.LocalPK = localPK
		row.WireKey = wireKey
		stagedRows = append(stagedRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate staged push rows: %w", err)
	}
	return stagedRows, nil
}

func (c *Client) livePayloadMatchesUploadedIntent(uploaded dirtyRowCapture, livePayload json.RawMessage, liveExists bool) (bool, error) {
	prepared := preparedUploadChange{
		Table:        uploaded.TableName,
		Op:           uploaded.Op,
		LocalPK:      uploaded.LocalPK,
		LocalPayload: uploaded.LocalPayload,
	}
	return liveMatchesUploadedIntent(prepared, livePayload, liveExists)
}

// Decisions may retain newer wide local state unrelated to the response size.
// Measure scalar local/L lengths and reserve all simultaneous decision copies.
func (c *Client) admitSettlementDecision(ctx context.Context, tx sqliteTransaction, schema, table, key, pk string, held *workspaceCharge) error {
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil {
		return nil
	}
	if err := c.reserveSyncWork(ctx, scope.unit, 16); err != nil {
		return err
	}
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return err
	}
	physical, err := c.convertPKForQueryInTx(tx, table, pk)
	if err != nil {
		return err
	}
	var bytes int64
	err = scanTransactionRow(ctx, tx, `SELECT COALESCE((SELECT `+livePayloadByteBound(info, "live_row")+` FROM `+quoteIdent(table)+` live_row WHERE `+quoteIdent(info.PrimaryKey.Name)+`=?),0)+COALESCE((SELECT length(CAST(payload AS BLOB)) FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?),0)`, []any{physical, schema, table, key}, &bytes)
	if err != nil {
		return err
	}
	bytes, err = c.workspaceScale(scope.unit, bytes, 64)
	if err != nil {
		return err
	}
	*held, err = c.addWorkspaceCharge(scope.unit, *held, workspaceCharge{32, bytes})
	if err != nil {
		return err
	}
	charge, err := c.addWorkspaceCharge(scope.unit, scope.base, *held)
	if err != nil {
		return err
	}
	// Keep the pre-decision base stable; held already includes prior decisions.
	return c.resizeWorkspaceInTx(ctx, tx, scope.token, charge)
}

// Ready preflight pays for all finalization work; each page has <=128 rows.
// Cancellation rolls back the complete final transaction and retains input.
func deleteSettlementPages(ctx context.Context, tx sqliteTransaction, table, column string, identity int64) (sql.Result, error) {
	var last sql.Result
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE rowid IN (SELECT rowid FROM `+table+` WHERE `+column+`=? ORDER BY row_ordinal LIMIT 128)`, identity)
		if err != nil {
			return nil, err
		}
		last = result
		count, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if count < 128 {
			return last, nil
		}
	}
}
