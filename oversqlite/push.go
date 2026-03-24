// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

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
	SourceBundleID int64
	Rows           []dirtyRowCapture
}

type committedPushBundle struct {
	BundleSeq      int64
	SourceID       string
	SourceBundleID int64
	RowCount       int64
	BundleHash     string
}

type stagedPushBundleRow struct {
	SchemaName string
	TableName  string
	KeyJSON    string
	LocalPK    string
	WireKey    oversync.SyncKey
	Op         string
	RowVersion int64
	Payload    sql.NullString
}

func (c *Client) PushPending(ctx context.Context) error {
	if atomicLoadPaused(&c.uploadPaused) {
		return nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	return c.pushPendingLocked(ctx, 0)
}

func atomicLoadPaused(v *int32) bool {
	return v != nil && atomic.LoadInt32(v) == 1
}

func (c *Client) pushPendingLocked(ctx context.Context, conflictRetryCount int) error {
	rebuildRequired, err := c.rebuildRequired(ctx)
	if err != nil {
		return err
	}
	if rebuildRequired {
		return &RebuildRequiredError{}
	}

	snapshot, err := c.ensurePushOutboundSnapshot(ctx)
	if err != nil {
		return err
	}
	if len(snapshot.Rows) == 0 {
		return nil
	}

	if err := c.clearPushStage(ctx); err != nil {
		return err
	}

	committed, err := c.commitPushOutboundSnapshot(ctx, snapshot)
	if err != nil {
		var conflictErr *PushConflictError
		if errors.As(err, &conflictErr) {
			if err := c.resolvePushConflict(ctx, snapshot, conflictErr); err != nil {
				return err
			}
			remainingDirtyCount, err := c.pendingChangeCount(ctx)
			if err != nil {
				return err
			}
			if remainingDirtyCount > 0 {
				if conflictRetryCount >= maxPushConflictAutoRetries {
					return &PushConflictRetryExhaustedError{
						RetryCount:          maxPushConflictAutoRetries,
						RemainingDirtyCount: remainingDirtyCount,
					}
				}
				return c.pushPendingLocked(ctx, conflictRetryCount+1)
			}
			return nil
		}
		return err
	}
	if err := c.fetchCommittedPushBundle(ctx, committed); err != nil {
		return err
	}
	if err := c.runBeforePushReplayHook(ctx); err != nil {
		return err
	}
	return c.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed)
}

func (c *Client) ensurePushOutboundSnapshot(ctx context.Context) (*pushOutboundSnapshot, error) {
	snapshot, err := c.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot != nil {
		return snapshot, nil
	}

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin outbound snapshot transaction: %w", err)
	}
	defer tx.Rollback()

	frozenSnapshot, err := c.freezePushOutboundSnapshotInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if frozenSnapshot == nil || len(frozenSnapshot.Rows) == 0 {
		return &pushOutboundSnapshot{}, nil
	}

	if err := c.runBeforePushFreezeHook(ctx); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit outbound push snapshot: %w", err)
	}
	return frozenSnapshot, nil
}

func (c *Client) loadPushOutboundSnapshot(ctx context.Context) (*pushOutboundSnapshot, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin outbound snapshot read transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT source_bundle_id, schema_name, table_name, key_json, op, base_row_version, payload, row_ordinal
		FROM _sync_push_outbound
		ORDER BY source_bundle_id, row_ordinal
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
		)
		if err := rows.Scan(
			&sourceBundleID,
			&capture.SchemaName,
			&capture.TableName,
			&capture.KeyJSON,
			&capture.Op,
			&capture.BaseRowVersion,
			&capture.LocalPayload,
			&capture.DirtyOrdinal,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbound push snapshot row: %w", err)
		}
		if snapshot == nil {
			snapshot = &pushOutboundSnapshot{SourceBundleID: sourceBundleID}
		} else if snapshot.SourceBundleID != sourceBundleID {
			return nil, fmt.Errorf("outbound push snapshot contains multiple source_bundle_id values (%d and %d)", snapshot.SourceBundleID, sourceBundleID)
		}
		localPK, wireKey, err := c.decodeDirtyKeyForPush(tx, capture.TableName, capture.KeyJSON)
		if err != nil {
			return nil, err
		}
		capture.LocalPK = localPK
		capture.WireKey = wireKey
		snapshot.Rows = append(snapshot.Rows, capture)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate outbound push snapshot rows: %w", err)
	}
	if snapshot == nil {
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit outbound push snapshot read transaction: %w", err)
	}
	return snapshot, nil
}

func (c *Client) commitPushOutboundSnapshot(ctx context.Context, snapshot *pushOutboundSnapshot) (*committedPushBundle, error) {
	if snapshot == nil || len(snapshot.Rows) == 0 {
		return nil, fmt.Errorf("outbound push snapshot is empty")
	}

	sessionResp, err := c.createPushSession(ctx, snapshot.SourceBundleID, int64(len(snapshot.Rows)))
	if err != nil {
		return nil, err
	}
	switch sessionResp.Status {
	case "already_committed":
		return committedPushBundleFromCreateResponse(sessionResp)
	case "staging":
	default:
		return nil, fmt.Errorf("unexpected push session status %q", sessionResp.Status)
	}

	pushID := strings.TrimSpace(sessionResp.PushID)
	if pushID == "" {
		return nil, fmt.Errorf("push session response missing push_id")
	}
	defer c.deletePushSessionBestEffort(context.Background(), pushID)

	chunkSize := c.pushChunkRows()
	for start := 0; start < len(snapshot.Rows); start += chunkSize {
		end := start + chunkSize
		if end > len(snapshot.Rows) {
			end = len(snapshot.Rows)
		}
		chunkRows, err := c.buildPushRequestRows(snapshot.Rows[start:end])
		if err != nil {
			return nil, err
		}
		chunkResp, err := c.uploadPushChunk(ctx, pushID, &oversync.PushSessionChunkRequest{
			StartRowOrdinal: int64(start),
			Rows:            chunkRows,
		})
		if err != nil {
			return nil, err
		}
		expectedNext := int64(end)
		if chunkResp.NextExpectedRowOrdinal != expectedNext {
			return nil, fmt.Errorf("push chunk response next_expected_row_ordinal %d does not match expected %d", chunkResp.NextExpectedRowOrdinal, expectedNext)
		}
	}

	if err := c.runBeforePushCommitHook(ctx); err != nil {
		return nil, err
	}
	commitResp, err := c.commitPushSession(ctx, pushID)
	if err != nil {
		return nil, err
	}
	return committedPushBundleFromCommitResponse(commitResp)
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

func (c *Client) fetchCommittedPushBundle(ctx context.Context, committed *committedPushBundle) error {
	if committed == nil {
		return fmt.Errorf("committed push bundle metadata is required")
	}
	if err := c.clearPushStage(ctx); err != nil {
		return err
	}

	var (
		afterRowOrdinal *int64
		stagedCount     int64
	)
	for {
		chunk, err := c.fetchCommittedBundleChunk(ctx, committed.BundleSeq, afterRowOrdinal, c.committedBundleChunkRows())
		if err != nil {
			return err
		}
		if err := validateCommittedBundleRowsResponse(chunk, committed, afterRowOrdinal); err != nil {
			return err
		}
		if err := c.stageCommittedBundleChunk(ctx, chunk, afterRowOrdinal); err != nil {
			return err
		}
		stagedCount += int64(len(chunk.Rows))
		if !chunk.HasMore {
			break
		}
		next := chunk.NextRowOrdinal
		afterRowOrdinal = &next
	}

	if stagedCount != committed.RowCount {
		return fmt.Errorf("staged committed bundle row count %d does not match expected row_count %d", stagedCount, committed.RowCount)
	}
	bundleHash, err := c.computeStagedPushBundleHash(ctx, committed.BundleSeq)
	if err != nil {
		return err
	}
	if bundleHash != committed.BundleHash {
		return fmt.Errorf("staged committed bundle hash %s does not match expected %s", bundleHash, committed.BundleHash)
	}
	return nil
}

func (c *Client) stageCommittedBundleChunk(ctx context.Context, chunk *oversync.CommittedBundleRowsResponse, afterRowOrdinal *int64) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin committed bundle stage transaction: %w", err)
	}
	defer tx.Rollback()
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	startOrdinal := int64(0)
	if afterRowOrdinal != nil {
		startOrdinal = *afterRowOrdinal + 1
	}
	for idx, row := range chunk.Rows {
		if row.Schema != c.config.Schema {
			return fmt.Errorf("committed bundle row schema %s does not match client schema %s", row.Schema, c.config.Schema)
		}
		keyJSON, _, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		if err != nil {
			return err
		}
		rowOrdinal := startOrdinal + int64(idx)
		if _, err := stmtCache.ExecContext(ctx, `
				INSERT INTO _sync_push_stage (
					bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, chunk.BundleSeq, rowOrdinal, row.Schema, row.Table, keyJSON, row.Op, row.RowVersion, nullStringValue(sql.NullString{
			String: string(row.Payload),
			Valid:  row.Op != oversync.OpDelete,
		})); err != nil {
			return fmt.Errorf("failed to stage committed bundle row %d for %s.%s: %w", rowOrdinal, row.Schema, row.Table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit committed bundle stage transaction: %w", err)
	}
	return nil
}

func (c *Client) clearPushStage(ctx context.Context) error {
	if _, err := c.DB.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("failed to clear push staging: %w", err)
	}
	return nil
}

func (c *Client) computeStagedPushBundleHash(ctx context.Context, bundleSeq int64) (string, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin staged bundle hash transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		FROM _sync_push_stage
		WHERE bundle_seq = ?
		ORDER BY row_ordinal
	`, bundleSeq)
	if err != nil {
		return "", fmt.Errorf("failed to query staged bundle rows for hash: %w", err)
	}
	defer rows.Close()

	logicalRows := make([]map[string]any, 0)
	for rows.Next() {
		var (
			rowOrdinal int64
			schemaName string
			tableName  string
			keyJSON    string
			op         string
			rowVersion int64
			payload    sql.NullString
		)
		if err := rows.Scan(&rowOrdinal, &schemaName, &tableName, &keyJSON, &op, &rowVersion, &payload); err != nil {
			return "", fmt.Errorf("failed to scan staged bundle row for hash: %w", err)
		}

		_, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return "", err
		}
		payloadValue := any(nil)
		if op != oversync.OpDelete && payload.Valid {
			if err := json.Unmarshal([]byte(payload.String), &payloadValue); err != nil {
				return "", fmt.Errorf("failed to decode staged bundle payload for hash %s.%s row %d: %w", schemaName, tableName, rowOrdinal, err)
			}
		}
		logicalRows = append(logicalRows, map[string]any{
			"row_ordinal": rowOrdinal,
			"schema":      schemaName,
			"table":       tableName,
			"key":         wireKey,
			"op":          op,
			"row_version": rowVersion,
			"payload":     payloadValue,
		})
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("failed to iterate staged bundle rows for hash: %w", err)
	}

	raw, err := json.Marshal(logicalRows)
	if err != nil {
		return "", fmt.Errorf("failed to marshal staged logical bundle rows: %w", err)
	}
	canonical, err := canonicalizeJSONBytes(raw)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize staged logical bundle rows: %w", err)
	}
	sum := sha256.Sum256(canonical)
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit staged bundle hash transaction: %w", err)
	}
	return hex.EncodeToString(sum[:]), nil
}

func (c *Client) applyStagedPushBundleLocked(ctx context.Context, uploaded []dirtyRowCapture, committed *committedPushBundle) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin staged push replay transaction: %w", err)
	}
	defer tx.Rollback()
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for staged push replay: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}

	stagedRows, err := c.loadStagedPushBundleRowsInTx(ctx, tx, committed.BundleSeq)
	if err != nil {
		return err
	}
	if int64(len(stagedRows)) != committed.RowCount {
		return fmt.Errorf("staged push replay row count %d does not match expected row_count %d", len(stagedRows), committed.RowCount)
	}

	type decision struct {
		skipApply      bool
		requeueOp      string
		requeuePayload sql.NullString
		needsRequeue   bool
	}

	decisions := make(map[string]decision, len(uploaded))
	for _, uploadedRow := range uploaded {
		currentDirty, err := c.loadDirtyUploadStateInTx(ctx, tx, uploadedRow.SchemaName, uploadedRow.TableName, uploadedRow.KeyJSON)
		if err != nil {
			return err
		}
		livePayload, liveExists, err := c.serializeExistingRowInTx(ctx, tx, uploadedRow.TableName, uploadedRow.LocalPK)
		if err != nil {
			return err
		}
		dirtyMatches, err := dirtyMatchesUploadedIntent(currentDirty, uploadedRow)
		if err != nil {
			return err
		}
		liveMatches, err := livePayloadMatchesUploadedIntent(uploadedRow, livePayload, liveExists)
		if err != nil {
			return err
		}
		prepared := preparedUploadChange{
			Table:        uploadedRow.TableName,
			Op:           uploadedRow.Op,
			LocalPK:      uploadedRow.LocalPK,
			LocalPayload: uploadedRow.LocalPayload,
		}
		requeueOp, requeuePayload, _, needsRequeue := reconcileAppliedUploadState(prepared, currentDirty.Exists, dirtyMatches, livePayload, liveExists, liveMatches)
		decisions[uploadedRow.SchemaName+"\x00"+uploadedRow.TableName+"\x00"+uploadedRow.KeyJSON] = decision{
			skipApply:      needsRequeue,
			requeueOp:      requeueOp,
			requeuePayload: requeuePayload,
			needsRequeue:   needsRequeue,
		}
	}

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
		rowDecision, hasDecision := decisions[row.SchemaName+"\x00"+row.TableName+"\x00"+row.KeyJSON]
		if !hasDecision || !rowDecision.skipApply {
			if err := c.applyBundleRowAuthoritativelyInTxUsing(ctx, stmtCache, tx, &bundleRow, row.LocalPK); err != nil {
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
			continue
		}

		if err := c.updateRowMetaInTxUsing(ctx, stmtCache, tx, row.LocalPK, row.TableName, row.RowVersion, row.Op == oversync.OpDelete); err != nil {
			return err
		}
		if rowDecision.needsRequeue {
			if err := c.requeueDirtyIntentInTx(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, rowDecision.requeueOp, row.RowVersion, rowDecision.requeuePayload); err != nil {
				return err
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE _sync_client_state
		SET last_bundle_seq_seen = CASE
				WHEN last_bundle_seq_seen + 1 = ? THEN ?
				ELSE last_bundle_seq_seen
			END,
			next_source_bundle_id = CASE
				WHEN next_source_bundle_id <= ? THEN ?
				ELSE next_source_bundle_id
			END
		WHERE user_id = ?
	`, committed.BundleSeq, committed.BundleSeq, committed.SourceBundleID, committed.SourceBundleID+1, c.UserID); err != nil {
		return fmt.Errorf("failed to update client bundle checkpoint after staged push replay: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_outbound WHERE source_bundle_id = ?`, committed.SourceBundleID); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot for source_bundle_id %d: %w", committed.SourceBundleID, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage WHERE bundle_seq = ?`, committed.BundleSeq); err != nil {
		return fmt.Errorf("failed to clear staged committed bundle %d: %w", committed.BundleSeq, err)
	}

	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit staged push replay transaction: %w", err)
	}
	return nil
}

func (c *Client) loadStagedPushBundleRowsInTx(ctx context.Context, tx *sql.Tx, bundleSeq int64) ([]stagedPushBundleRow, error) {
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

func (c *Client) collectDirtyRowsForPushInTx(ctx context.Context, tx *sql.Tx) (sourceBundleID int64, baseBundleSeq int64, dirtyRows []dirtyRowCapture, noOps []string, err error) {
	if err := tx.QueryRowContext(ctx, `
		SELECT next_source_bundle_id, last_bundle_seq_seen
		FROM _sync_client_state
		WHERE user_id = ?
	`, c.UserID).Scan(&sourceBundleID, &baseBundleSeq); err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to load client bundle state: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
			SELECT
				d.schema_name,
				d.table_name,
				d.key_json,
				d.base_row_version,
				d.payload,
				d.dirty_ordinal,
				CASE WHEN rs.key_json IS NULL THEN 0 ELSE 1 END AS state_exists,
				COALESCE(rs.deleted, 0) AS state_deleted
			FROM _sync_dirty_rows AS d
			LEFT JOIN _sync_row_state AS rs
				ON rs.schema_name = d.schema_name
				AND rs.table_name = d.table_name
				AND rs.key_json = d.key_json
			ORDER BY d.dirty_ordinal, d.table_name, d.key_json
		`)
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to query dirty rows: %w", err)
	}
	defer rows.Close()

	type noOpKey struct {
		schemaName string
		tableName  string
		keyJSON    string
	}
	type dirtySnapshotRow struct {
		schemaName     string
		tableName      string
		keyJSON        string
		baseRowVersion int64
		payload        sql.NullString
		dirtyOrdinal   int64
		stateExists    bool
		stateDeleted   bool
	}
	snapshotRows := make([]dirtySnapshotRow, 0)
	noOpKeys := make([]noOpKey, 0)

	for rows.Next() {
		var (
			schemaName      string
			tableName       string
			keyJSON         string
			baseRowVersion  int64
			payload         sql.NullString
			dirtyOrdinal    int64
			stateExistsInt  int
			stateDeletedInt int
		)
		if err := rows.Scan(
			&schemaName,
			&tableName,
			&keyJSON,
			&baseRowVersion,
			&payload,
			&dirtyOrdinal,
			&stateExistsInt,
			&stateDeletedInt,
		); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("failed to scan dirty row: %w", err)
		}
		snapshotRows = append(snapshotRows, dirtySnapshotRow{
			schemaName:     schemaName,
			tableName:      tableName,
			keyJSON:        keyJSON,
			baseRowVersion: baseRowVersion,
			payload:        payload,
			dirtyOrdinal:   dirtyOrdinal,
			stateExists:    stateExistsInt == 1,
			stateDeleted:   stateDeletedInt == 1,
		})
	}
	if err := rows.Err(); err != nil {
		return 0, 0, nil, nil, fmt.Errorf("failed to iterate dirty rows: %w", err)
	}
	rows.Close()

	for _, snapshot := range snapshotRows {
		schemaName := snapshot.schemaName
		tableName := snapshot.tableName
		keyJSON := snapshot.keyJSON
		localPK, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return 0, 0, nil, nil, err
		}

		if !snapshot.payload.Valid && (!snapshot.stateExists || snapshot.stateDeleted) {
			noOpKeys = append(noOpKeys, noOpKey{schemaName: schemaName, tableName: tableName, keyJSON: keyJSON})
			continue
		}

		dirty := dirtyRowCapture{
			SchemaName:     schemaName,
			TableName:      tableName,
			KeyJSON:        keyJSON,
			LocalPK:        localPK,
			WireKey:        wireKey,
			BaseRowVersion: snapshot.baseRowVersion,
			LocalPayload:   snapshot.payload,
			DirtyOrdinal:   snapshot.dirtyOrdinal,
		}
		if snapshot.payload.Valid {
			if snapshot.stateExists && !snapshot.stateDeleted {
				dirty.Op = oversync.OpUpdate
			} else {
				dirty.Op = oversync.OpInsert
			}
		} else {
			dirty.Op = oversync.OpDelete
		}
		dirtyRows = append(dirtyRows, dirty)
	}

	sort.SliceStable(dirtyRows, func(i, j int) bool {
		opRank := func(op string) int {
			if op == oversync.OpDelete {
				return 1
			}
			return 0
		}
		if opRank(dirtyRows[i].Op) != opRank(dirtyRows[j].Op) {
			return opRank(dirtyRows[i].Op) < opRank(dirtyRows[j].Op)
		}
		leftOrder := c.tableOrder[strings.ToLower(dirtyRows[i].TableName)]
		rightOrder := c.tableOrder[strings.ToLower(dirtyRows[j].TableName)]
		if dirtyRows[i].Op == oversync.OpDelete {
			if leftOrder != rightOrder {
				return leftOrder > rightOrder
			}
		} else if leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		return dirtyRows[i].DirtyOrdinal < dirtyRows[j].DirtyOrdinal
	})
	noOps = make([]string, 0, len(noOpKeys))
	for _, noOp := range noOpKeys {
		noOps = append(noOps, noOp.schemaName+"\x00"+noOp.tableName+"\x00"+noOp.keyJSON)
	}
	return sourceBundleID, baseBundleSeq, dirtyRows, noOps, nil
}

func (c *Client) freezePushOutboundSnapshotInTx(ctx context.Context, tx *sql.Tx) (*pushOutboundSnapshot, error) {
	sourceBundleID, _, dirtyRows, noOpKeys, err := c.collectDirtyRowsForPushInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	for _, noOpKey := range noOpKeys {
		parts := strings.SplitN(noOpKey, "\x00", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid no-op dirty row key encoding")
		}
		if _, err := stmtCache.ExecContext(ctx, `
				DELETE FROM _sync_dirty_rows
				WHERE schema_name = ? AND table_name = ? AND key_json = ?
			`, parts[0], parts[1], parts[2]); err != nil {
			return nil, fmt.Errorf("failed to clear no-op dirty row %s.%s %s: %w", parts[0], parts[1], parts[2], err)
		}
	}

	if len(dirtyRows) == 0 {
		return &pushOutboundSnapshot{}, nil
	}

	for idx, row := range dirtyRows {
		if _, err := stmtCache.ExecContext(ctx, `
				INSERT INTO _sync_push_outbound (
					source_bundle_id, row_ordinal, schema_name, table_name, key_json, op, base_row_version, payload
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, sourceBundleID, idx, row.SchemaName, row.TableName, row.KeyJSON, row.Op, row.BaseRowVersion, nullStringValue(row.LocalPayload)); err != nil {
			return nil, fmt.Errorf("failed to freeze outbound push row %d for %s.%s: %w", idx, row.SchemaName, row.TableName, err)
		}
		if _, err := stmtCache.ExecContext(ctx, `
				DELETE FROM _sync_dirty_rows
				WHERE schema_name = ? AND table_name = ? AND key_json = ?
			`, row.SchemaName, row.TableName, row.KeyJSON); err != nil {
			return nil, fmt.Errorf("failed to move dirty row %s.%s %s into outbound snapshot: %w", row.SchemaName, row.TableName, row.KeyJSON, err)
		}
	}

	return &pushOutboundSnapshot{
		SourceBundleID: sourceBundleID,
		Rows:           dirtyRows,
	}, nil
}

func (c *Client) setBundleApplyModeInTx(ctx context.Context, tx *sql.Tx, enabled bool) error {
	value := 0
	if enabled {
		value = 1
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_state SET apply_mode = ? WHERE user_id = ?`, value, c.UserID); err != nil {
		return fmt.Errorf("failed to update _sync_client_state apply_mode: %w", err)
	}
	return nil
}

func (c *Client) decodeDirtyKeyForPush(tx *sql.Tx, tableName, keyJSON string) (string, oversync.SyncKey, error) {
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil {
		return "", nil, err
	}
	if len(keyColumns) != 1 {
		return "", nil, fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(keyJSON), &raw); err != nil {
		return "", nil, fmt.Errorf("failed to decode dirty key for %s: %w", tableName, err)
	}
	rawValue, ok := raw[strings.ToLower(keyColumns[0])]
	if !ok {
		rawValue, ok = raw[keyColumns[0]]
	}
	if !ok {
		return "", nil, fmt.Errorf("dirty key for %s is missing %s", tableName, keyColumns[0])
	}
	pkValue, ok := rawValue.(string)
	if !ok {
		return "", nil, fmt.Errorf("dirty key %s.%s must be a string", tableName, keyColumns[0])
	}
	wirePK, err := c.normalizePKForServerInTx(tx, tableName, pkValue)
	if err != nil {
		return "", nil, fmt.Errorf("failed to normalize key for push %s.%s: %w", tableName, pkValue, err)
	}
	return pkValue, oversync.SyncKey{strings.ToLower(keyColumns[0]): wirePK}, nil
}

func (c *Client) bundleRowKeyToLocalKey(tx *sql.Tx, tableName string, key oversync.SyncKey) (string, string, error) {
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil {
		return "", "", err
	}
	if len(keyColumns) != 1 {
		return "", "", fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
	}

	value, ok := key[strings.ToLower(keyColumns[0])]
	if !ok {
		value, ok = key[keyColumns[0]]
	}
	if !ok {
		return "", "", fmt.Errorf("bundle row key for %s is missing %s", tableName, keyColumns[0])
	}
	wirePK, ok := value.(string)
	if !ok {
		return "", "", fmt.Errorf("bundle row key for %s must be a string", tableName)
	}
	localPK := wirePK
	isBlobPK, err := c.isPrimaryKeyBlobInTx(tx, tableName)
	if err != nil {
		return "", "", fmt.Errorf("failed to inspect bundle row key for %s: %w", tableName, err)
	}
	if isBlobPK {
		uuidBytes, err := decodeCanonicalWireUUIDBytes(wirePK)
		if err != nil {
			return "", "", fmt.Errorf("failed to decode canonical wire bundle row key for %s: %w", tableName, err)
		}
		localPK = hex.EncodeToString(uuidBytes)
	}
	keyJSONBytes, err := json.Marshal(map[string]any{strings.ToLower(keyColumns[0]): localPK})
	if err != nil {
		return "", "", fmt.Errorf("failed to encode local key json for %s: %w", tableName, err)
	}
	return string(keyJSONBytes), localPK, nil
}

type structuredRowState struct {
	Exists     bool
	RowVersion int64
	Deleted    bool
}

func (c *Client) loadStructuredRowStateInTx(ctx context.Context, tx *sql.Tx, schemaName, tableName, keyJSON string) (*structuredRowState, error) {
	state := &structuredRowState{}
	var deletedInt int
	err := tx.QueryRowContext(ctx, `
			SELECT row_version, deleted
			FROM _sync_row_state
			WHERE schema_name = ? AND table_name = ? AND key_json = ?
		`, schemaName, tableName, keyJSON).Scan(&state.RowVersion, &deletedInt)
	if err == sql.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load structured row state for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	state.Exists = true
	state.Deleted = deletedInt == 1
	return state, nil
}

func (c *Client) loadDirtyUploadStateInTx(ctx context.Context, tx *sql.Tx, schemaName, tableName, keyJSON string) (dirtyUploadState, error) {
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
	state.Exists = true
	return state, nil
}

func dirtyMatchesUploadedIntent(current dirtyUploadState, uploaded dirtyRowCapture) (bool, error) {
	if !current.Exists || current.Op != uploaded.Op {
		return false, nil
	}
	return canonicalPayloadEqual(current.Payload, uploaded.LocalPayload)
}

func livePayloadMatchesUploadedIntent(uploaded dirtyRowCapture, livePayload json.RawMessage, liveExists bool) (bool, error) {
	prepared := preparedUploadChange{
		Table:        uploaded.TableName,
		Op:           uploaded.Op,
		LocalPK:      uploaded.LocalPK,
		LocalPayload: uploaded.LocalPayload,
	}
	return liveMatchesUploadedIntent(prepared, livePayload, liveExists)
}

func (c *Client) requeueDirtyIntentInTx(ctx context.Context, tx *sql.Tx, schemaName, tableName, keyJSON, op string, baseRowVersion int64, payload sql.NullString) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
		VALUES (
			?, ?, ?, ?, ?, ?,
			COALESCE(
				(SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?),
				(SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
			),
			strftime('%Y-%m-%dT%H:%M:%fZ','now')
		)
		ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
			op = excluded.op,
			base_row_version = excluded.base_row_version,
			payload = excluded.payload,
			updated_at = excluded.updated_at
	`, schemaName, tableName, keyJSON, op, baseRowVersion, nullStringValue(payload), schemaName, tableName, keyJSON); err != nil {
		return fmt.Errorf("failed to requeue dirty intent for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	return nil
}

func (c *Client) applyBundleRowAuthoritativelyInTx(ctx context.Context, tx *sql.Tx, row *oversync.BundleRow, localPK string) error {
	return c.applyBundleRowAuthoritativelyInTxUsing(ctx, tx, tx, row, localPK)
}

func (c *Client) applyBundleRowAuthoritativelyInTxUsing(ctx context.Context, execer execContexter, tx *sql.Tx, row *oversync.BundleRow, localPK string) error {
	switch row.Op {
	case oversync.OpInsert, oversync.OpUpdate:
		var payload map[string]any
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			return fmt.Errorf("failed to decode bundle row payload for %s.%s: %w", row.Schema, row.Table, err)
		}
		if err := c.upsertRowInTxUsing(ctx, execer, tx, row.Table, payload); err != nil {
			return fmt.Errorf("failed to apply authoritative upsert for %s.%s: %w", row.Schema, row.Table, err)
		}
	case oversync.OpDelete:
		pkColumn, err := c.primaryKeyColumnForTable(row.Table)
		if err != nil {
			return err
		}
		pkValue, err := c.convertPKForQueryInTx(tx, row.Table, localPK)
		if err != nil {
			return fmt.Errorf("failed to convert local key for delete: %w", err)
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(row.Table), quoteIdent(pkColumn))
		if _, err := execer.ExecContext(ctx, query, pkValue); err != nil {
			return fmt.Errorf("failed to apply authoritative delete for %s.%s: %w", row.Schema, row.Table, err)
		}
	default:
		return fmt.Errorf("unsupported bundle row op %q", row.Op)
	}

	if err := c.updateRowMetaInTxUsing(ctx, execer, tx, localPK, row.Table, row.RowVersion, row.Op == oversync.OpDelete); err != nil {
		return err
	}
	return nil
}

func (c *Client) pushChunkRows() int {
	if c != nil && c.config != nil && c.config.UploadLimit > 0 {
		return c.config.UploadLimit
	}
	return 1000
}

func (c *Client) committedBundleChunkRows() int {
	if c != nil && c.config != nil && c.config.DownloadLimit > 0 {
		return c.config.DownloadLimit
	}
	return 1000
}

func (c *Client) createPushSession(ctx context.Context, sourceBundleID, plannedRowCount int64) (*oversync.PushSessionCreateResponse, error) {
	reqBody, err := json.Marshal(&oversync.PushSessionCreateRequest{
		SourceID:        c.SourceID,
		SourceBundleID:  sourceBundleID,
		PlannedRowCount: plannedRowCount,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal push session request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, buildPushSessionCreateURL(c.BaseURL), strings.NewReader(string(reqBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create push session request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send push session request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if conflictErr := decodePushConflictError(resp.StatusCode, body); conflictErr != nil {
			return nil, conflictErr
		}
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, decodeServerErrorBody(body))
	}

	var session oversync.PushSessionCreateResponse
	if err := json.Unmarshal(body, &session); err != nil {
		return nil, fmt.Errorf("failed to decode push session response: %w", err)
	}
	if err := validatePushSessionCreateResponse(&session, sourceBundleID, plannedRowCount, c.SourceID); err != nil {
		return nil, err
	}
	if session.Status == "staging" {
		atomic.AddInt64(&c.pushStats.sessionsCreated, 1)
	}
	return &session, nil
}

func (c *Client) uploadPushChunk(ctx context.Context, pushID string, req *oversync.PushSessionChunkRequest) (*oversync.PushSessionChunkResponse, error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal push chunk request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, buildPushSessionChunkURL(c.BaseURL, pushID), strings.NewReader(string(reqBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create push chunk request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send push chunk request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if conflictErr := decodePushConflictError(resp.StatusCode, body); conflictErr != nil {
			return nil, conflictErr
		}
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, decodeServerErrorBody(body))
	}

	var chunkResp oversync.PushSessionChunkResponse
	if err := json.Unmarshal(body, &chunkResp); err != nil {
		return nil, fmt.Errorf("failed to decode push chunk response: %w", err)
	}
	if strings.TrimSpace(chunkResp.PushID) != pushID {
		return nil, fmt.Errorf("push chunk response push_id %q does not match requested %q", chunkResp.PushID, pushID)
	}
	atomic.AddInt64(&c.pushStats.chunksUploaded, 1)
	return &chunkResp, nil
}

func (c *Client) commitPushSession(ctx context.Context, pushID string) (*oversync.PushSessionCommitResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, buildPushSessionCommitURL(c.BaseURL, pushID), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create push commit request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send push commit request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if conflictErr := decodePushConflictError(resp.StatusCode, body); conflictErr != nil {
			return nil, conflictErr
		}
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, decodeServerErrorBody(body))
	}

	var commitResp oversync.PushSessionCommitResponse
	if err := json.Unmarshal(body, &commitResp); err != nil {
		return nil, fmt.Errorf("failed to decode push commit response: %w", err)
	}
	if err := validatePushSessionCommitResponse(&commitResp, c.SourceID); err != nil {
		return nil, err
	}
	return &commitResp, nil
}

func (c *Client) fetchCommittedBundleChunk(ctx context.Context, bundleSeq int64, afterRowOrdinal *int64, maxRows int) (*oversync.CommittedBundleRowsResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, buildCommittedBundleRowsURL(c.BaseURL, bundleSeq, afterRowOrdinal, maxRows), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create committed bundle chunk request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send committed bundle chunk request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, decodeServerErrorBody(body))
	}

	var chunk oversync.CommittedBundleRowsResponse
	if err := json.Unmarshal(body, &chunk); err != nil {
		return nil, fmt.Errorf("failed to decode committed bundle chunk response: %w", err)
	}
	atomic.AddInt64(&c.pushStats.committedBundleChunksRead, 1)
	return &chunk, nil
}

func (c *Client) runBeforePushReplayHook(ctx context.Context) error {
	if c == nil || c.beforePushReplayHook == nil {
		return nil
	}
	hook := c.beforePushReplayHook
	c.beforePushReplayHook = nil
	return hook(ctx)
}

func (c *Client) runBeforePushCommitHook(ctx context.Context) error {
	if c == nil || c.beforePushCommitHook == nil {
		return nil
	}
	hook := c.beforePushCommitHook
	c.beforePushCommitHook = nil
	return hook(ctx)
}

func (c *Client) runBeforePushFreezeHook(ctx context.Context) error {
	if c == nil || c.beforePushFreezeHook == nil {
		return nil
	}
	hook := c.beforePushFreezeHook
	c.beforePushFreezeHook = nil
	return hook(ctx)
}

func (c *Client) deletePushSessionBestEffort(ctx context.Context, pushID string) {
	if strings.TrimSpace(pushID) == "" {
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, buildPushSessionDeleteURL(c.BaseURL, pushID), nil)
	if err != nil {
		return
	}
	token, err := c.Token(ctx)
	if err != nil {
		return
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func committedPushBundleFromCreateResponse(resp *oversync.PushSessionCreateResponse) (*committedPushBundle, error) {
	if resp == nil {
		return nil, fmt.Errorf("push session response missing body")
	}
	if resp.BundleSeq <= 0 {
		return nil, fmt.Errorf("push session already_committed response missing bundle_seq")
	}
	if strings.TrimSpace(resp.SourceID) == "" {
		return nil, fmt.Errorf("push session already_committed response missing source_id")
	}
	if resp.SourceBundleID <= 0 {
		return nil, fmt.Errorf("push session already_committed response missing source_bundle_id")
	}
	if resp.RowCount <= 0 {
		return nil, fmt.Errorf("push session already_committed response missing row_count")
	}
	if strings.TrimSpace(resp.BundleHash) == "" {
		return nil, fmt.Errorf("push session already_committed response missing bundle_hash")
	}
	return &committedPushBundle{
		BundleSeq:      resp.BundleSeq,
		SourceID:       resp.SourceID,
		SourceBundleID: resp.SourceBundleID,
		RowCount:       resp.RowCount,
		BundleHash:     resp.BundleHash,
	}, nil
}

func committedPushBundleFromCommitResponse(resp *oversync.PushSessionCommitResponse) (*committedPushBundle, error) {
	if resp == nil {
		return nil, fmt.Errorf("push commit response missing body")
	}
	if resp.BundleSeq <= 0 {
		return nil, fmt.Errorf("push commit response bundle_seq %d must be positive", resp.BundleSeq)
	}
	if strings.TrimSpace(resp.SourceID) == "" {
		return nil, fmt.Errorf("push commit response source_id must be non-empty")
	}
	if resp.SourceBundleID <= 0 {
		return nil, fmt.Errorf("push commit response source_bundle_id %d must be positive", resp.SourceBundleID)
	}
	if resp.RowCount <= 0 {
		return nil, fmt.Errorf("push commit response row_count %d must be positive", resp.RowCount)
	}
	if strings.TrimSpace(resp.BundleHash) == "" {
		return nil, fmt.Errorf("push commit response bundle_hash must be non-empty")
	}
	return &committedPushBundle{
		BundleSeq:      resp.BundleSeq,
		SourceID:       resp.SourceID,
		SourceBundleID: resp.SourceBundleID,
		RowCount:       resp.RowCount,
		BundleHash:     resp.BundleHash,
	}, nil
}

func validatePushSessionCreateResponse(resp *oversync.PushSessionCreateResponse, sourceBundleID, plannedRowCount int64, sourceID string) error {
	if resp == nil {
		return fmt.Errorf("push session response missing body")
	}
	switch resp.Status {
	case "staging":
		if strings.TrimSpace(resp.PushID) == "" {
			return fmt.Errorf("push session response missing push_id")
		}
		if resp.PlannedRowCount != plannedRowCount {
			return fmt.Errorf("push session response planned_row_count %d does not match requested %d", resp.PlannedRowCount, plannedRowCount)
		}
		if resp.NextExpectedRowOrdinal != 0 {
			return fmt.Errorf("push session response next_expected_row_ordinal %d must be 0", resp.NextExpectedRowOrdinal)
		}
	case "already_committed":
		if resp.SourceBundleID != sourceBundleID {
			return fmt.Errorf("push session already_committed response source_bundle_id %d does not match requested %d", resp.SourceBundleID, sourceBundleID)
		}
		if resp.SourceID != sourceID {
			return fmt.Errorf("push session already_committed response source_id %q does not match client %q", resp.SourceID, sourceID)
		}
		if _, err := committedPushBundleFromCreateResponse(resp); err != nil {
			return err
		}
	default:
		return fmt.Errorf("push session response returned unsupported status %q", resp.Status)
	}
	return nil
}

func validatePushSessionCommitResponse(resp *oversync.PushSessionCommitResponse, sourceID string) error {
	committed, err := committedPushBundleFromCommitResponse(resp)
	if err != nil {
		return err
	}
	if committed.SourceID != sourceID {
		return fmt.Errorf("push commit response source_id %q does not match client %q", committed.SourceID, sourceID)
	}
	return nil
}

func validateCommittedBundleRowsResponse(resp *oversync.CommittedBundleRowsResponse, committed *committedPushBundle, afterRowOrdinal *int64) error {
	if resp == nil {
		return fmt.Errorf("committed bundle chunk response missing body")
	}
	if committed == nil {
		return fmt.Errorf("committed bundle metadata is required")
	}
	if resp.BundleSeq != committed.BundleSeq {
		return fmt.Errorf("committed bundle chunk response bundle_seq %d does not match expected %d", resp.BundleSeq, committed.BundleSeq)
	}
	if resp.SourceID != committed.SourceID {
		return fmt.Errorf("committed bundle chunk response source_id %q does not match expected %q", resp.SourceID, committed.SourceID)
	}
	if resp.SourceBundleID != committed.SourceBundleID {
		return fmt.Errorf("committed bundle chunk response source_bundle_id %d does not match expected %d", resp.SourceBundleID, committed.SourceBundleID)
	}
	if resp.RowCount != committed.RowCount {
		return fmt.Errorf("committed bundle chunk response row_count %d does not match expected %d", resp.RowCount, committed.RowCount)
	}
	if resp.BundleHash != committed.BundleHash {
		return fmt.Errorf("committed bundle chunk response bundle_hash %q does not match expected %q", resp.BundleHash, committed.BundleHash)
	}

	logicalAfter := int64(-1)
	if afterRowOrdinal != nil {
		logicalAfter = *afterRowOrdinal
	}
	expectedNext := logicalAfter
	if len(resp.Rows) > 0 {
		expectedNext = logicalAfter + int64(len(resp.Rows))
	}
	if resp.NextRowOrdinal != expectedNext {
		return fmt.Errorf("committed bundle chunk response next_row_ordinal %d does not match expected %d", resp.NextRowOrdinal, expectedNext)
	}
	if resp.HasMore && len(resp.Rows) == 0 {
		return fmt.Errorf("committed bundle chunk response with has_more=true must include at least one row")
	}
	for idx := range resp.Rows {
		if err := validateBundleRow(&resp.Rows[idx]); err != nil {
			return fmt.Errorf("invalid committed bundle row %d: %w", idx, err)
		}
	}
	return nil
}

func canonicalizeJSONBytes(raw []byte) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

func buildPushSessionCreateURL(base string) string {
	return fmt.Sprintf("%s/sync/push-sessions", base)
}

func buildPushSessionChunkURL(base, pushID string) string {
	return fmt.Sprintf("%s/sync/push-sessions/%s/chunks", base, url.PathEscape(pushID))
}

func buildPushSessionCommitURL(base, pushID string) string {
	return fmt.Sprintf("%s/sync/push-sessions/%s/commit", base, url.PathEscape(pushID))
}

func buildPushSessionDeleteURL(base, pushID string) string {
	return fmt.Sprintf("%s/sync/push-sessions/%s", base, url.PathEscape(pushID))
}

func buildCommittedBundleRowsURL(base string, bundleSeq int64, afterRowOrdinal *int64, maxRows int) string {
	values := url.Values{}
	if afterRowOrdinal != nil {
		values.Set("after_row_ordinal", strconv.FormatInt(*afterRowOrdinal, 10))
	}
	if maxRows > 0 {
		values.Set("max_rows", strconv.Itoa(maxRows))
	}
	return fmt.Sprintf("%s/sync/committed-bundles/%d/rows?%s", base, bundleSeq, values.Encode())
}
