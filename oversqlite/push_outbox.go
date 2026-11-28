// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mobiletoly/go-oversync/oversync"
)

type dirtyRowCapture struct {
	SchemaName     string
	TableName      string
	KeyJSON        string
	LocalPK        string
	WireKey        oversync.SyncKey
	Op             string
	BaseRowVersion int64
	LocalPayload   sql.NullString
	BasePayload    sql.NullString
	BaseKind       string
	BaseProvenance string
	DirtyOrdinal   int64
}

type dirtyUploadState struct {
	Exists         bool
	Op             string
	Payload        sql.NullString
	BaseRowVersion int64
	CurrentOrdinal int64
}

type pushOutboundSnapshot struct {
	Fresh          bool
	conflict       *PendingConflict
	decision       MergeResult
	automatic      bool
	SourceBundleID int64
	Rows           []dirtyRowCapture
}

func (c *Client) ensurePushOutboundSnapshot(ctx context.Context) (*pushOutboundSnapshot, error) {
	return completeWorkPhase(ctx, c, "outbox freeze", c.ensurePushOutboundSnapshotSlice)
}

func (c *Client) ensurePushOutboundSnapshotSlice(ctx context.Context) (*pushOutboundSnapshot, error) {
	snapshot, err := c.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot != nil {
		return snapshot, nil
	}

	plan, err := c.preparePushPlan(ctx)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		if replay, e := c.loadPushOutboundSnapshot(ctx); e != nil || replay != nil {
			return replay, e
		}
		return &pushOutboundSnapshot{}, nil
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, err
	}
	currentOutbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())
	}
	if currentOutbox.State != outboxStateNone {
		if err := tx.Rollback(); err != nil {
			return nil, err
		}
		return c.loadPushOutboundSnapshot(ctx)
	}
	guard, _, _, err := c.pushGuard(ctx, tx, plan)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())
	}
	if guard != plan.Guard {
		return nil, errors.Join(&SyncWorkLimitError{Unit: plan.Unit, Limit: c.config.MaxSyncWorkUnitsPerInvocation, Used: c.syncWorkUsed}, tx.Rollback())
	}
	defer tx.Rollback()
	defer c.releasePushTableInfoScope(tx)

	frozenSnapshot, err := c.freezePushOutboundSnapshotInTx(ctx, tx, plan)
	if err != nil {
		return nil, err
	}
	if frozenSnapshot == nil || len(frozenSnapshot.Rows) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit no-op dirty-row pruning: %w", err)
		}
		return &pushOutboundSnapshot{}, nil
	}

	if err := c.runBeforePushFreezeHook(ctx); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit outbound push snapshot: %w", err)
	}
	return frozenSnapshot, nil
}

func (c *Client) loadPushOutboundSnapshot(ctx context.Context) (*pushOutboundSnapshot, error) {
	return completeWorkPhase(ctx, c, "outbox read", c.loadPushOutboundSnapshotSlice)
}

func (c *Client) loadPushOutboundSnapshotSlice(ctx context.Context) (*pushOutboundSnapshot, error) {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, fmt.Errorf("failed to begin outbound snapshot read transaction: %w", err)
	}
	defer tx.Rollback()

	outboxBundle, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, err
	}
	if outboxBundle.State == outboxStateNone {
		return nil, nil
	}
	if err := c.measureOutboxBufferCopies(ctx, tx); err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT source_bundle_id, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, row_ordinal, base_kind, base_payload, base_provenance
		FROM _sync_outbox_rows
		ORDER BY row_ordinal
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query outbound push snapshot: %w", err)
	}
	defer rows.Close()

	var snapshot *pushOutboundSnapshot
	for rows.Next() {
		var (
			sourceBundleID int64
			capture        dirtyRowCapture
			wireKeyJSON    string
		)
		if err := rows.Scan(
			&sourceBundleID,
			&capture.SchemaName,
			&capture.TableName,
			&capture.KeyJSON,
			&wireKeyJSON,
			&capture.Op,
			&capture.BaseRowVersion,
			&capture.LocalPayload,
			&capture.DirtyOrdinal,
			&capture.BaseKind, &capture.BasePayload, &capture.BaseProvenance,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbound push snapshot row: %w", err)
		}
		if snapshot == nil {
			snapshot = &pushOutboundSnapshot{SourceBundleID: sourceBundleID}
		} else if snapshot.SourceBundleID != sourceBundleID {
			return nil, fmt.Errorf("outbound push snapshot contains multiple source_bundle_id values (%d and %d)", snapshot.SourceBundleID, sourceBundleID)
		}
		capture.LocalPK, _, err = c.decodeDirtyKeyForPush(tx, capture.TableName, capture.KeyJSON)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(wireKeyJSON), &capture.WireKey); err != nil {
			return nil, fmt.Errorf("failed to decode stored wire key for %s.%s: %w", capture.SchemaName, capture.TableName, err)
		}
		if err := c.validateHistoricalBase(ctx, tx, capture.TableName, capture.BaseRowVersion, historicalBase{capture.BaseKind, capture.BasePayload, capture.BaseProvenance}); err != nil {
			return nil, err
		}
		snapshot.Rows = append(snapshot.Rows, capture)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate outbound push snapshot rows: %w", err)
	}
	if snapshot == nil {
		return nil, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit outbound push snapshot read transaction: %w", err)
	}
	return snapshot, nil
}

func (c *Client) measureOutboxBufferCopies(ctx context.Context, tx sqliteTransaction) error {
	if ctx.Value(workspaceBufferKey{}) == nil {
		return nil
	}
	var cursor int64 = -1
	charge := workspaceCharge{64, 256 * 1024}
	for {
		if err := c.reserveSyncWork(ctx, applyUnit("outbox-copies", 0, ""), 8); err != nil {
			return err
		}
		var ordinal, size int64
		err := tx.QueryRowContext(ctx, `SELECT row_ordinal,2048+64*(length(CAST(COALESCE(local_payload,'') AS BLOB))+
            length(CAST(COALESCE(wire_payload,'') AS BLOB))+length(CAST(COALESCE(base_payload,'') AS BLOB))+
            length(CAST(base_provenance AS BLOB))+length(CAST(key_json AS BLOB))+length(CAST(wire_key_json AS BLOB)))
            FROM _sync_outbox_rows WHERE row_ordinal>? ORDER BY row_ordinal LIMIT 1`, cursor).Scan(&ordinal, &size)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return err
		}
		charge, err = c.addWorkspaceCharge(applyUnit("outbox-copies", 0, ""), charge, workspaceCharge{32, size})
		if err != nil {
			return err
		}
		cursor = ordinal
	}
	return c.admitPushBufferCopies(ctx, tx, charge)
}

func (c *Client) buildPushRequestRows(rows []dirtyRowCapture) ([]oversync.PushRequestRow, error) {
	pushRows := make([]oversync.PushRequestRow, 0, len(rows))
	for _, dirty := range rows {
		row := oversync.PushRequestRow{
			Schema:         dirty.SchemaName,
			Table:          dirty.TableName,
			Key:            dirty.WireKey,
			Op:             dirty.Op,
			BaseRowVersion: dirty.BaseRowVersion,
		}
		if dirty.Op != oversync.OpDelete {
			payload, err := c.processPayloadForUpload(dirty.TableName, dirty.LocalPayload.String)
			if err != nil {
				return nil, fmt.Errorf("failed to process dirty payload for %s.%s: %w", dirty.TableName, dirty.LocalPK, err)
			}
			row.Payload = payload
		}
		pushRows = append(pushRows, row)
	}
	return pushRows, nil
}

// PushRowExceedsAdmittedLimitError leaves the dirty row untouched because its
// compact logical representation exceeds the selected generation contract.
type PushRowExceedsAdmittedLimitError struct{ Limit int64 }

func (e *PushRowExceedsAdmittedLimitError) Error() string {
	return fmt.Sprintf("push_row_exceeds_admitted_limit: max_row_decoded_bytes=%d", e.Limit)
}

// InvalidAdmittedContractError identifies a discovery contract that cannot
// carry even a single otherwise-valid compact request envelope.
type InvalidAdmittedContractError struct{ Message string }

func (e *InvalidAdmittedContractError) Error() string {
	return "invalid_admitted_contract: " + e.Message
}

// PushDependencyGroupExceedsAdmittedLimitError leaves the entire group dirty.
// No outbox or source sequence has been consumed when it is returned.
type PushDependencyGroupExceedsAdmittedLimitError struct {
	Dimension                                           string
	Limit, Actual, ComponentRows, ComponentDecodedBytes int64
	DiagnosticID                                        string
	Sample                                              []PushDependencyRecordRef
	HasMore                                             bool
	Progress                                            SyncProgress
}

func (e *PushDependencyGroupExceedsAdmittedLimitError) Error() string {
	return fmt.Sprintf("push_dependency_group_exceeds_admitted_limit: dimension=%s limit=%d actual=%d", e.Dimension, e.Limit, e.Actual)
}

func (c *Client) compactPushRequestBody(sourceBundleID int64, initializationID string, rows []dirtyRowCapture) ([]byte, error) {
	if c == nil || c.discovery == nil {
		return nil, &InvalidAdmittedContractError{Message: "admitted discovery is unavailable"}
	}
	logical, err := c.buildPushRequestRows(rows)
	if err != nil {
		return nil, err
	}
	hash, err := computeGenerationPinnedPushRequestHash(c.schemaGenerationProof, logical)
	if err != nil {
		return nil, err
	}
	groups, err := oversync.CompactGroupsForRows(c.discovery.SchemaGeneration, logical)
	if err != nil {
		return nil, err
	}
	return json.Marshal(oversync.CompactPushRequest{SourceBundleID: sourceBundleID, CanonicalRequestHash: hash, InitializationID: initializationID, Rows: groups})
}

func (c *Client) freezePushOutboundSnapshotInTx(ctx context.Context, tx sqliteTransaction, plan *pushPlan) (*pushOutboundSnapshot, error) {
	sourceBundleID := plan.SourceBundleID
	dirtyRows, err := c.selectedPushCaptures(ctx, tx, plan)
	if err != nil {
		return nil, err
	}
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	if len(dirtyRows) == 0 {
		return &pushOutboundSnapshot{}, nil
	}

	pushRequestRows, err := c.buildPushRequestRows(dirtyRows)
	if err != nil {
		return nil, err
	}
	canonicalRequestHash, err := computeGenerationPinnedPushRequestHash(c.schemaGenerationProof, pushRequestRows)
	if err != nil {
		return nil, err
	}
	body, err := c.compactPushRequestBody(sourceBundleID, c.pendingInitializationID, dirtyRows)
	if err != nil {
		return nil, err
	}
	if err := c.freezeUploadJournalInTx(ctx, tx, &outboxBundleRecord{SourceID: c.sourceID, SourceBundleID: sourceBundleID, CanonicalRequestHash: canonicalRequestHash, InitializationID: c.pendingInitializationID}, body); err != nil {
		return nil, err
	}
	if err := persistOutboxBundle(ctx, tx, &outboxBundleRecord{
		State:                outboxStatePrepared,
		SourceID:             c.sourceID,
		SourceBundleID:       sourceBundleID,
		CanonicalRequestHash: canonicalRequestHash,
		RowCount:             int64(len(dirtyRows)),
		InitializationID:     c.pendingInitializationID,
	}); err != nil {
		return nil, err
	}
	for idx, row := range dirtyRows {
		wireKeyJSON, err := json.Marshal(row.WireKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encode wire key for frozen outbox row %d: %w", idx, err)
		}
		wirePayload := sql.NullString{}
		if pushRequestRows[idx].Op != oversync.OpDelete {
			wirePayload = sql.NullString{String: string(pushRequestRows[idx].Payload), Valid: true}
		}
		if _, err := stmtCache.ExecContext(ctx, `
				INSERT INTO _sync_outbox_rows (
					source_bundle_id, row_ordinal, schema_name, table_name, key_json, wire_key_json, op, base_row_version, local_payload, wire_payload, base_kind, base_payload, base_provenance
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, sourceBundleID, idx, row.SchemaName, row.TableName, row.KeyJSON, string(wireKeyJSON), row.Op, row.BaseRowVersion, nullStringValue(row.LocalPayload), nullStringValue(wirePayload), row.BaseKind, nullStringValue(row.BasePayload), row.BaseProvenance); err != nil {
			return nil, fmt.Errorf("failed to freeze outbound push row %d for %s.%s: %w", idx, row.SchemaName, row.TableName, err)
		}
		if _, err := stmtCache.ExecContext(ctx, `
				DELETE FROM _sync_dirty_rows
				WHERE schema_name = ? AND table_name = ? AND key_json = ?
			`, row.SchemaName, row.TableName, row.KeyJSON); err != nil {
			return nil, fmt.Errorf("failed to move dirty row %s.%s %s into outbound snapshot: %w", row.SchemaName, row.TableName, row.KeyJSON, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE _sync_push_plan SET phase='clear' WHERE singleton_key=1`); err != nil {
		return nil, err
	}
	return &pushOutboundSnapshot{
		Fresh:          true,
		SourceBundleID: sourceBundleID,
		Rows:           dirtyRows,
	}, nil
}

func (c *Client) loadDirtyUploadStateInTx(ctx context.Context, tx sqliteTransaction, schemaName, tableName, keyJSON string) (dirtyUploadState, error) {
	var state dirtyUploadState
	err := tx.QueryRowContext(ctx, `
			SELECT op, payload, base_row_version, dirty_ordinal
			FROM _sync_dirty_rows
			WHERE schema_name = ? AND table_name = ? AND key_json = ?
		`, schemaName, tableName, keyJSON).Scan(&state.Op, &state.Payload, &state.BaseRowVersion, &state.CurrentOrdinal)
	if err == sql.ErrNoRows {
		return dirtyUploadState{}, nil
	}
	if err != nil {
		return dirtyUploadState{}, fmt.Errorf("failed to load dirty upload state for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	if err := c.validateBaselineProjection(ctx, tx, schemaName, tableName, keyJSON, "desired", state.Payload); err != nil {
		return dirtyUploadState{}, err
	}
	state.Exists = true
	return state, nil
}

func dirtyMatchesUploadedIntent(current dirtyUploadState, uploaded dirtyRowCapture) (bool, error) {
	if !current.Exists || current.Op != uploaded.Op {
		return false, nil
	}
	return canonicalPayloadEqual(current.Payload, uploaded.LocalPayload)
}

func (c *Client) requeueDirtyIntentInTx(ctx context.Context, tx sqliteTransaction, schemaName, tableName, keyJSON, op string, baseRowVersion int64, payload sql.NullString, base historicalBase) error {
	if err := c.validateHistoricalBase(ctx, tx, tableName, baseRowVersion, base); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
		VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?,
			COALESCE(
				(SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?),
				(SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
			),
			strftime('%Y-%m-%dT%H:%M:%fZ','now')
		)
		ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
			op = excluded.op,
			base_row_version = excluded.base_row_version,
			payload = excluded.payload, base_kind=excluded.base_kind, base_payload=excluded.base_payload, base_provenance=excluded.base_provenance,
			updated_at = excluded.updated_at
	`, schemaName, tableName, keyJSON, op, baseRowVersion, nullStringValue(payload), base.Kind, nullStringValue(base.Payload), base.Provenance, schemaName, tableName, keyJSON); err != nil {
		return fmt.Errorf("failed to requeue dirty intent for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	return nil
}

// Rebase is reserved for an explicit authoritative transition or unpublished adoption.
func (c *Client) rebaseDirtyIntentInTx(ctx context.Context, tx sqliteTransaction, schema, table, key, op string, version int64, payload sql.NullString) error {
	base, err := c.loadHistoricalBase(ctx, tx, schema, table, key, version)
	if err != nil {
		return err
	}
	return c.requeueDirtyIntentInTx(ctx, tx, schema, table, key, op, version, payload, base)
}
