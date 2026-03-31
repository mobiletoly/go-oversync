package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

type conflictResolutionPlan struct {
	localRowAction localRowAction
	rowStateAction rowStateAction
	requeueIntent  *resolvedDirtyIntent
}

type localRowAction interface {
	isLocalRowAction()
}

type localRowUpsert struct {
	payload map[string]any
}

func (localRowUpsert) isLocalRowAction() {}

type localRowDelete struct{}

func (localRowDelete) isLocalRowAction() {}

type rowStateAction interface {
	isRowStateAction()
}

type rowStateUpsert struct {
	rowVersion int64
	deleted    bool
}

func (rowStateUpsert) isRowStateAction() {}

type rowStateDelete struct{}

func (rowStateDelete) isRowStateAction() {}

type retryPayloadSource int

const (
	retryPayloadSourceNone retryPayloadSource = iota
	retryPayloadSourceLocalRow
)

type resolvedDirtyIntent struct {
	op             string
	baseRowVersion int64
	payloadSource  retryPayloadSource
}

func (c *Client) resolvePushConflict(ctx context.Context, snapshot *pushOutboundSnapshot, conflictErr *PushConflictError) error {
	if snapshot == nil || conflictErr == nil || conflictErr.Conflict() == nil {
		return conflictErr
	}

	conflict := conflictErr.Conflict()
	conflictingRow, err := findConflictingSnapshotRow(snapshot, conflict)
	if err != nil {
		return err
	}
	if conflictingRow == nil {
		return conflictErr
	}

	contextPayload := json.RawMessage(nil)
	if conflictingRow.LocalPayload.Valid {
		contextPayload = json.RawMessage(conflictingRow.LocalPayload.String)
	}
	conflictContext := ConflictContext{
		Schema:           conflict.Schema,
		Table:            conflict.Table,
		Key:              conflict.Key,
		LocalOp:          conflictingRow.Op,
		LocalPayload:     contextPayload,
		BaseRowVersion:   conflict.BaseRowVersion,
		ServerRowVersion: conflict.ServerRowVersion,
		ServerRowDeleted: conflict.ServerRowDeleted,
		ServerRowPayload: normalizeOptionalJSON(conflict.ServerRow),
	}

	var result MergeResult = AcceptServer{}
	if c != nil && c.Resolver != nil {
		result = c.Resolver.Resolve(conflictContext)
	}

	switch typed := result.(type) {
	case AcceptServer:
		return c.resolvePushConflictAcceptServer(ctx, snapshot, conflictingRow, conflict)
	case KeepLocal:
		return c.resolvePushConflictKeepLocal(ctx, snapshot, conflictingRow, conflictContext)
	case KeepMerged:
		return c.resolvePushConflictKeepMerged(ctx, snapshot, conflictingRow, conflictContext, typed)
	case nil:
		return c.throwInvalidConflictResolution(ctx, snapshot, conflictContext, nil,
			fmt.Sprintf("resolver returned nil result for %s.%s", conflictContext.Schema, conflictContext.Table))
	default:
		return c.throwInvalidConflictResolution(ctx, snapshot, conflictContext, typed,
			fmt.Sprintf("resolver returned unsupported result type %T for %s.%s", typed, conflictContext.Schema, conflictContext.Table))
	}
}

func findConflictingSnapshotRow(snapshot *pushOutboundSnapshot, conflict *oversync.PushConflictDetails) (*dirtyRowCapture, error) {
	if snapshot == nil || conflict == nil {
		return nil, nil
	}
	for idx := range snapshot.Rows {
		row := &snapshot.Rows[idx]
		if row.SchemaName != conflict.Schema || row.TableName != conflict.Table {
			continue
		}
		matches, err := syncKeysEqual(row.WireKey, conflict.Key)
		if err != nil {
			return nil, err
		}
		if matches {
			return row, nil
		}
	}
	return nil, nil
}

func (c *Client) resolvePushConflictAcceptServer(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, conflict *oversync.PushConflictDetails) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin structured conflict resolution transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys during conflict resolution: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}

	serverRowPayload := normalizeOptionalJSON(conflict.ServerRow)
	isMissingServerRow := len(serverRowPayload) == 0 && !conflict.ServerRowDeleted
	switch {
	case isMissingServerRow:
		if err := c.deleteLocalRowInTx(ctx, tx, conflict.Table, conflictingRow.LocalPK); err != nil {
			return err
		}
		if err := c.deleteStructuredRowStateInTx(ctx, tx, conflict.Schema, conflict.Table, conflictingRow.KeyJSON); err != nil {
			return err
		}
	case conflict.ServerRowDeleted:
		if err := c.applyBundleRowAuthoritativelyInTx(ctx, tx, &oversync.BundleRow{
			Schema:     conflict.Schema,
			Table:      conflict.Table,
			Key:        conflict.Key,
			Op:         oversync.OpDelete,
			RowVersion: conflict.ServerRowVersion,
		}, conflictingRow.LocalPK); err != nil {
			return err
		}
	default:
		if err := c.applyBundleRowAuthoritativelyInTx(ctx, tx, &oversync.BundleRow{
			Schema:     conflict.Schema,
			Table:      conflict.Table,
			Key:        conflict.Key,
			Op:         oversync.OpUpdate,
			RowVersion: conflict.ServerRowVersion,
			Payload:    serverRowPayload,
		}, conflictingRow.LocalPK); err != nil {
			return err
		}
	}

	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, conflictingRow); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit AcceptServer conflict resolution: %w", err)
	}
	return nil
}

func (c *Client) resolvePushConflictKeepLocal(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, conflict ConflictContext) error {
	switch conflict.LocalOp {
	case oversync.OpInsert:
		localPayload, err := parseStoredPayloadObject(conflictingRow.LocalPayload, conflict.Schema, conflict.Table, conflict.LocalOp)
		if err != nil {
			return err
		}
		switch {
		case len(conflict.ServerRowPayload) > 0:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowUpsert{payload: localPayload},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false},
				requeueIntent: &resolvedDirtyIntent{
					op:             oversync.OpUpdate,
					baseRowVersion: conflict.ServerRowVersion,
					payloadSource:  retryPayloadSourceLocalRow,
				},
			})
		case conflict.ServerRowDeleted:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowUpsert{payload: localPayload},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: true},
				requeueIntent: &resolvedDirtyIntent{
					op:             oversync.OpInsert,
					baseRowVersion: conflict.ServerRowVersion,
					payloadSource:  retryPayloadSourceLocalRow,
				},
			})
		default:
			return fmt.Errorf("KeepLocal INSERT conflict for %s.%s must include live row or tombstone", conflict.Schema, conflict.Table)
		}

	case oversync.OpUpdate:
		if len(conflict.ServerRowPayload) == 0 {
			return c.throwInvalidConflictResolution(ctx, snapshot, conflict, KeepLocal{},
				fmt.Sprintf("KeepLocal is invalid for stale UPDATE on %s.%s; authoritative row is deleted or missing", conflict.Schema, conflict.Table))
		}
		localPayload, err := parseStoredPayloadObject(conflictingRow.LocalPayload, conflict.Schema, conflict.Table, conflict.LocalOp)
		if err != nil {
			return err
		}
		return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
			localRowAction: localRowUpsert{payload: localPayload},
			rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false},
			requeueIntent: &resolvedDirtyIntent{
				op:             oversync.OpUpdate,
				baseRowVersion: conflict.ServerRowVersion,
				payloadSource:  retryPayloadSourceLocalRow,
			},
		})

	case oversync.OpDelete:
		switch {
		case len(conflict.ServerRowPayload) == 0 && !conflict.ServerRowDeleted:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowDelete{},
				rowStateAction: rowStateDelete{},
			})
		case conflict.ServerRowDeleted:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowDelete{},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: true},
			})
		default:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowDelete{},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false},
				requeueIntent: &resolvedDirtyIntent{
					op:             oversync.OpDelete,
					baseRowVersion: conflict.ServerRowVersion,
					payloadSource:  retryPayloadSourceNone,
				},
			})
		}
	default:
		return fmt.Errorf("unsupported local conflict op %q", conflict.LocalOp)
	}
}

func (c *Client) resolvePushConflictKeepMerged(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, conflict ConflictContext, result KeepMerged) error {
	if conflict.LocalOp == oversync.OpDelete {
		return c.throwInvalidConflictResolution(ctx, snapshot, conflict, result,
			fmt.Sprintf("KeepMerged is invalid for DELETE conflict on %s.%s", conflict.Schema, conflict.Table))
	}
	if conflict.LocalOp == oversync.OpUpdate && len(conflict.ServerRowPayload) == 0 {
		return c.throwInvalidConflictResolution(ctx, snapshot, conflict, result,
			fmt.Sprintf("KeepMerged is invalid for stale UPDATE on %s.%s; authoritative row is deleted or missing", conflict.Schema, conflict.Table))
	}

	mergedPayload, err := c.validateMergedPayloadOrError(conflict, result)
	if err != nil {
		return c.throwInvalidConflictResolution(ctx, snapshot, conflict, result, err.Error())
	}

	switch conflict.LocalOp {
	case oversync.OpInsert:
		switch {
		case len(conflict.ServerRowPayload) > 0:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowUpsert{payload: mergedPayload},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false},
				requeueIntent: &resolvedDirtyIntent{
					op:             oversync.OpUpdate,
					baseRowVersion: conflict.ServerRowVersion,
					payloadSource:  retryPayloadSourceLocalRow,
				},
			})
		case conflict.ServerRowDeleted:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowUpsert{payload: mergedPayload},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: true},
				requeueIntent: &resolvedDirtyIntent{
					op:             oversync.OpInsert,
					baseRowVersion: conflict.ServerRowVersion,
					payloadSource:  retryPayloadSourceLocalRow,
				},
			})
		default:
			return fmt.Errorf("KeepMerged INSERT conflict for %s.%s must include live row or tombstone", conflict.Schema, conflict.Table)
		}
	case oversync.OpUpdate:
		return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
			localRowAction: localRowUpsert{payload: mergedPayload},
			rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false},
			requeueIntent: &resolvedDirtyIntent{
				op:             oversync.OpUpdate,
				baseRowVersion: conflict.ServerRowVersion,
				payloadSource:  retryPayloadSourceLocalRow,
			},
		})
	default:
		return fmt.Errorf("unsupported local conflict op %q", conflict.LocalOp)
	}
}

func (c *Client) applyConflictResolutionPlan(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, tableName string, plan conflictResolutionPlan) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin conflict resolution plan transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys during conflict plan application: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}

	switch action := plan.localRowAction.(type) {
	case localRowUpsert:
		if err := c.upsertRowInTx(ctx, tx, tableName, action.payload); err != nil {
			return err
		}
	case localRowDelete:
		if err := c.deleteLocalRowInTx(ctx, tx, tableName, conflictingRow.LocalPK); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported local row action %T", action)
	}

	switch action := plan.rowStateAction.(type) {
	case rowStateUpsert:
		if err := c.updateStructuredRowStateInTx(ctx, tx, c.config.Schema, tableName, conflictingRow.KeyJSON, action.rowVersion, action.deleted); err != nil {
			return err
		}
	case rowStateDelete:
		if err := c.deleteStructuredRowStateInTx(ctx, tx, c.config.Schema, tableName, conflictingRow.KeyJSON); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported row state action %T", action)
	}

	var normalizedLocalPayload sql.NullString
	switch plan.localRowAction.(type) {
	case localRowUpsert:
		payload, exists, err := c.serializeExistingRowInTx(ctx, tx, tableName, conflictingRow.LocalPK)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("resolved payload for %s.%s must exist before requeue", conflictingRow.SchemaName, conflictingRow.TableName)
		}
		normalizedLocalPayload = sql.NullString{String: string(payload), Valid: true}
	case localRowDelete:
	}

	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, conflictingRow); err != nil {
		return err
	}
	if plan.requeueIntent != nil {
		payload := sql.NullString{}
		if plan.requeueIntent.payloadSource == retryPayloadSourceLocalRow {
			payload = normalizedLocalPayload
		}
		if err := c.requeueDirtyIntentInTx(ctx, tx, conflictingRow.SchemaName, conflictingRow.TableName, conflictingRow.KeyJSON, plan.requeueIntent.op, plan.requeueIntent.baseRowVersion, payload); err != nil {
			return err
		}
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit conflict resolution plan: %w", err)
	}
	return nil
}

func (c *Client) validateMergedPayloadOrError(conflict ConflictContext, result KeepMerged) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal(result.MergedPayload, &payload); err != nil {
		return nil, fmt.Errorf("KeepMerged for %s.%s must provide a JSON object payload", conflict.Schema, conflict.Table)
	}
	if payload == nil {
		return nil, fmt.Errorf("KeepMerged for %s.%s must provide a JSON object payload", conflict.Schema, conflict.Table)
	}

	tableInfo, err := c.getTableInfo(conflict.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect table %s for KeepMerged validation: %w", conflict.Table, err)
	}
	expectedColumns := make(map[string]struct{}, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		expectedColumns[strings.ToLower(col.Name)] = struct{}{}
	}
	if len(payload) != len(expectedColumns) {
		return nil, fmt.Errorf("KeepMerged for %s.%s must include exactly every table column", conflict.Schema, conflict.Table)
	}
	for key := range payload {
		if _, ok := expectedColumns[strings.ToLower(key)]; !ok {
			return nil, fmt.Errorf("KeepMerged for %s.%s must include exactly every table column", conflict.Schema, conflict.Table)
		}
	}
	return payload, nil
}

func (c *Client) throwInvalidConflictResolution(ctx context.Context, snapshot *pushOutboundSnapshot, conflict ConflictContext, result MergeResult, message string) error {
	if err := c.restoreOutboundSnapshotToDirtyRows(ctx, snapshot); err != nil {
		return err
	}
	return &InvalidConflictResolutionError{
		Conflict: conflict,
		Result:   result,
		Message:  message,
	}
}

func (c *Client) restoreOutboundSnapshotToDirtyRows(ctx context.Context, snapshot *pushOutboundSnapshot) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin outbound snapshot restore transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys during outbound restore: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, nil); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit outbound snapshot restore: %w", err)
	}
	return nil
}

func (c *Client) requeueSnapshotRowsInTx(ctx context.Context, tx *sql.Tx, snapshot *pushOutboundSnapshot, skipRow *dirtyRowCapture) error {
	if snapshot == nil {
		return nil
	}
	for idx := range snapshot.Rows {
		row := &snapshot.Rows[idx]
		if skipRow != nil && row.SchemaName == skipRow.SchemaName && row.TableName == skipRow.TableName && row.KeyJSON == skipRow.KeyJSON {
			continue
		}
		if err := c.requeueDirtyIntentInTx(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, row.Op, row.BaseRowVersion, row.LocalPayload); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) deletePushOutboundSnapshotInTx(ctx context.Context, tx *sql.Tx, sourceBundleID int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_outbox_rows WHERE source_bundle_id = ?`, sourceBundleID); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot for source_bundle_id %d: %w", sourceBundleID, err)
	}
	if err := clearOutboxBundle(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (c *Client) deleteStructuredRowStateInTx(ctx context.Context, tx *sql.Tx, schemaName, tableName, keyJSON string) error {
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM _sync_row_state
		WHERE schema_name = ? AND table_name = ? AND key_json = ?
	`, schemaName, tableName, keyJSON); err != nil {
		return fmt.Errorf("failed to delete structured row state for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	return nil
}

func (c *Client) deleteLocalRowInTx(ctx context.Context, tx *sql.Tx, tableName, localPK string) error {
	pkColumn, err := c.primaryKeyColumnForTable(tableName)
	if err != nil {
		return err
	}
	pkValue, err := c.convertPKForQueryInTx(tx, tableName, localPK)
	if err != nil {
		return fmt.Errorf("failed to convert local key for delete: %w", err)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(tableName), quoteIdent(pkColumn))
	if _, err := tx.ExecContext(ctx, query, pkValue); err != nil {
		return fmt.Errorf("failed to delete local row %s.%s: %w", tableName, localPK, err)
	}
	return nil
}

func parseStoredPayloadObject(payload sql.NullString, schemaName, tableName, op string) (map[string]any, error) {
	if !payload.Valid {
		return nil, fmt.Errorf("local %s conflict is missing payload for %s.%s", op, schemaName, tableName)
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(payload.String), &value); err != nil {
		return nil, fmt.Errorf("local %s conflict payload for %s.%s must be a JSON object: %w", op, schemaName, tableName, err)
	}
	if value == nil {
		return nil, fmt.Errorf("local %s conflict payload for %s.%s must be a JSON object", op, schemaName, tableName)
	}
	return value, nil
}
