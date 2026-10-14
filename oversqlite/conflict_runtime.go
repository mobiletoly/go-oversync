package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
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
	payload     map[string]any
	storedLocal bool
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
	payload    json.RawMessage
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
	if err := c.validateAuthoritativeConflictRow(ctx, conflict, conflictingRow); err != nil {
		return errMalformedConflictEvidence
	}
	conflictContext, err := c.trustedConflictContext(ctx, conflictingRow, conflict)
	if err != nil {
		return err
	}

	// The upgrade-only deferral already has a durable journal. Ordinary sync
	// records its definitive rejection before invoking an application resolver.
	if _, ok := c.Resolver.(deferUpgradeResolver); ok {
		return conflictErr
	}
	pending, err := c.preserveConflict(ctx, snapshot, conflictErr, conflictContext)
	if err != nil {
		return err
	}
	snapshot.conflict = pending
	snapshot.automatic = true
	return c.applyExplicitConflictResult(ctx, snapshot, conflictingRow, conflictContext, conflictErr, invokeConflictResolver(c.Resolver, conflictContext))
}

func (c *Client) trustedConflictContext(ctx context.Context, frozen *dirtyRowCapture, conflict *oversync.PushConflictDetails) (ConflictContext, error) {
	if frozen == nil || conflict == nil {
		return ConflictContext{}, fmt.Errorf("conflict evidence is incomplete")
	}
	if err := c.validateHistoricalBase(ctx, c.db, frozen.TableName, frozen.BaseRowVersion, historicalBase{frozen.BaseKind, frozen.BasePayload, frozen.BaseProvenance}); err != nil {
		return ConflictContext{}, err
	}
	localPayload := json.RawMessage(nil)
	if frozen.Op != oversync.OpDelete && frozen.LocalPayload.Valid {
		var err error
		localPayload, err = c.processPayloadForUpload(conflict.Table, frozen.LocalPayload.String)
		if err != nil {
			return ConflictContext{}, err
		}
	}
	return ConflictContext{
		Schema: conflict.Schema, Table: conflict.Table, Key: conflict.Key,
		LocalOp: frozen.Op, LocalPayload: localPayload,
		BaseRowVersion: conflict.BaseRowVersion, ServerRowVersion: conflict.ServerRowVersion,
		ServerRowDeleted: conflict.ServerRowDeleted,
		ServerRowPayload: normalizeOptionalJSON(conflict.ServerRow),
	}, nil
}

func (c *Client) validateAuthoritativeConflictRow(ctx context.Context, conflict *oversync.PushConflictDetails, frozen *dirtyRowCapture) error {
	if conflict == nil || frozen == nil {
		return fmt.Errorf("authoritative conflict identity is missing")
	}
	absent := conflict.ServerRowVersion == 0 && !conflict.ServerRowDeleted
	if !absent {
		if conflict.ServerRowVersion < frozen.BaseRowVersion {
			return fmt.Errorf("authoritative conflict version predates frozen base version")
		}
		var currentVersion int64
		err := c.db.QueryRowContext(ctx, `SELECT row_version FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, frozen.SchemaName, frozen.TableName, frozen.KeyJSON).Scan(&currentVersion)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil && conflict.ServerRowVersion < currentVersion {
			return fmt.Errorf("authoritative conflict version predates local authoritative state")
		}
	}
	if conflict.ServerRowDeleted || absent {
		return nil
	}
	row := &oversync.BundleRow{
		Schema:     conflict.Schema,
		Table:      conflict.Table,
		Key:        conflict.Key,
		Op:         oversync.OpUpdate,
		RowVersion: conflict.ServerRowVersion,
		Payload:    normalizeOptionalJSON(conflict.ServerRow),
	}
	if err := c.validateBundleRowForBundle(row, conflict.ServerRowVersion); err != nil {
		return err
	}
	payload, err := jcs.DecodeObjectLossless(row.Payload)
	if err != nil {
		return err
	}
	tableInfo, err := c.getTableInfo(strings.ToLower(conflict.Table))
	if err != nil {
		return err
	}
	if len(payload) != len(tableInfo.Columns) {
		return fmt.Errorf("authoritative conflict row must contain exactly every table column")
	}
	for _, col := range tableInfo.Columns {
		name := strings.ToLower(col.Name)
		value, ok := payload[name]
		if !ok {
			return fmt.Errorf("authoritative conflict row is missing column %s", name)
		}
		if err := c.validateAuthoritativeConflictColumn(conflict.Table, col, value); err != nil {
			return fmt.Errorf("authoritative conflict row column %s: %w", name, err)
		}
	}
	return nil
}

func (c *Client) validateAuthoritativeConflictColumn(table string, col ColumnInfo, value any) error {
	if c.localGeneration == nil {
		return fmt.Errorf("local contract is unavailable")
	}
	_, err := c.sqliteValueWithDeclaredCodec(table, col, value)
	return err
}

func (c *Client) applyExplicitConflictResult(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, conflictContext ConflictContext, conflictErr *PushConflictError, result MergeResult) error {
	snapshot.decision = result
	switch typed := result.(type) {
	case deferUpgradeConflictResolution:
		return conflictErr
	case DeferConflict, nil:
		return &ConflictNeedsResolutionError{Pending: *snapshot.conflict}
	case AcceptServer:
		return c.resolvePushConflictAcceptServer(ctx, snapshot, conflictingRow, conflictErr.Conflict())
	case KeepLocal:
		return c.resolvePushConflictKeepLocal(ctx, snapshot, conflictingRow, conflictContext)
	case KeepMerged:
		return c.resolvePushConflictKeepMerged(ctx, snapshot, conflictingRow, conflictContext, typed)
	default:
		return c.throwInvalidConflictResolution(ctx, snapshot, conflictContext, typed, "unsupported conflict decision")
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
			if row.Op != conflict.Op || row.BaseRowVersion != conflict.BaseRowVersion || !coherentConflict(conflict) {
				return nil, errMalformedConflictEvidence
			}
			return row, nil
		}
	}
	return nil, nil
}

func (c *Client) resolvePushConflictAcceptServer(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, conflict *oversync.PushConflictDetails) error {
	return c.completeWork(ctx, "accept conflict", func(ctx context.Context) error {
		return c.resolvePushConflictAcceptServerSlice(ctx, snapshot, conflictingRow, conflict)
	})
}

func (c *Client) resolvePushConflictAcceptServerSlice(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, conflict *oversync.PushConflictDetails) (resultErr error) {
	ctx = context.WithValue(ctx, applyInputSequenceKey{}, conflict.ServerRowVersion)
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin structured conflict resolution transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "AcceptServer conflict resolution")
	if err := c.guardConflictDecision(ctx, tx, snapshot.conflict); err != nil {
		return err
	}

	desired := applySurvivor{table: conflictingRow.TableName, pk: conflictingRow.LocalPK, role: "intended", wire: true,
		payload: sql.NullString{String: string(normalizeOptionalJSON(conflict.ServerRow)), Valid: !conflict.ServerRowDeleted && len(normalizeOptionalJSON(conflict.ServerRow)) > 0}}
	newer, err := c.loadDirtyUploadStateInTx(ctx, tx, conflictingRow.SchemaName, conflictingRow.TableName, conflictingRow.KeyJSON)
	if err != nil {
		return err
	}
	if newer.Exists {
		p := &applyPlan{unit: applyUnit("conflict-input", 0, conflictingRow.KeyJSON), workspaceContext: ctx, workspaceQuery: tx}
		desired, err = c.captureApplySurvivor(ctx, tx, p, conflictingRow.TableName, conflictingRow.LocalPK)
		if err != nil {
			return err
		}
	}
	if err := c.prepareConflictApply(ctx, tx, snapshot, conflictingRow, desired); err != nil {
		return commitApplyProgress(ctx, tx, err)
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys during conflict resolution: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}

	if err := c.applyAcceptedConflictRow(ctx, tx, conflictingRow, conflict); err != nil {
		return err
	}

	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, conflictingRow); err != nil {
		return err
	}
	if err := c.finishConflictApply(ctx, tx); err != nil {
		return err
	}
	if err := validateForeignKeyIntegrity(ctx, tx); err != nil {
		return fmt.Errorf("AcceptServer resolution would violate local foreign keys: %w", err)
	}
	if err := recordConflictDecision(ctx, tx, snapshot.conflict, snapshot.decision); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
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
				localRowAction: localRowUpsert{payload: localPayload, storedLocal: true},
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false, payload: conflict.ServerRowPayload},
				requeueIntent: &resolvedDirtyIntent{
					op:             oversync.OpUpdate,
					baseRowVersion: conflict.ServerRowVersion,
					payloadSource:  retryPayloadSourceLocalRow,
				},
			})
		case conflict.ServerRowDeleted:
			return c.applyConflictResolutionPlan(ctx, snapshot, conflictingRow, conflict.Table, conflictResolutionPlan{
				localRowAction: localRowUpsert{payload: localPayload, storedLocal: true},
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
			localRowAction: localRowUpsert{payload: localPayload, storedLocal: true},
			rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false, payload: conflict.ServerRowPayload},
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
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false, payload: conflict.ServerRowPayload},
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
				rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false, payload: conflict.ServerRowPayload},
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
			rowStateAction: rowStateUpsert{rowVersion: conflict.ServerRowVersion, deleted: false, payload: conflict.ServerRowPayload},
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
	return c.completeWork(ctx, "resolve conflict", func(ctx context.Context) error {
		return c.applyConflictResolutionPlanSlice(ctx, snapshot, conflictingRow, tableName, plan)
	})
}

func (c *Client) applyConflictResolutionPlanSlice(ctx context.Context, snapshot *pushOutboundSnapshot, conflictingRow *dirtyRowCapture, tableName string, plan conflictResolutionPlan) (resultErr error) {
	if observed, ok := plan.rowStateAction.(rowStateUpsert); ok {
		ctx = context.WithValue(ctx, applyInputSequenceKey{}, observed.rowVersion)
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin conflict resolution plan transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "conflict resolution plan")
	if err := c.guardConflictDecision(ctx, tx, snapshot.conflict); err != nil {
		return err
	}

	newer, err := c.loadDirtyUploadStateInTx(ctx, tx, conflictingRow.SchemaName, conflictingRow.TableName, conflictingRow.KeyJSON)
	if err != nil {
		return err
	}
	if newer.Exists {
		return &StaleConflictResolutionError{}
	}

	desired, err := c.conflictActionSurvivor(ctx, tx, conflictingRow, plan.localRowAction)
	if err != nil {
		return err
	}
	if err := c.prepareConflictApply(ctx, tx, snapshot, conflictingRow, desired); err != nil {
		return commitApplyProgress(ctx, tx, err)
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys during conflict plan application: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}

	normalizedLocalPayload, err := c.applyConflictLocalAction(ctx, tx, tableName, conflictingRow, plan.localRowAction)
	if err != nil {
		return err
	}
	if err := c.applyConflictStateAction(ctx, tx, tableName, conflictingRow.KeyJSON, plan.rowStateAction); err != nil {
		return err
	}
	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, conflictingRow); err != nil {
		return err
	}
	if plan.requeueIntent != nil {
		payload := sql.NullString{}
		if plan.requeueIntent.payloadSource == retryPayloadSourceLocalRow {
			payload = normalizedLocalPayload
		}
		if err := c.rebaseDirtyIntentInTx(ctx, tx, conflictingRow.SchemaName, conflictingRow.TableName, conflictingRow.KeyJSON, plan.requeueIntent.op, plan.requeueIntent.baseRowVersion, payload); err != nil {
			return err
		}
	}
	if err := c.finishConflictApply(ctx, tx); err != nil {
		return err
	}
	if err := validateForeignKeyIntegrity(ctx, tx); err != nil {
		return fmt.Errorf("conflict resolution would violate local foreign keys: %w", err)
	}
	if err := recordConflictDecision(ctx, tx, snapshot.conflict, snapshot.decision); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit conflict resolution plan: %w", err)
	}
	return nil
}

func (c *Client) validateMergedPayloadOrError(conflict ConflictContext, result KeepMerged) (map[string]any, error) {
	payload, err := jcs.DecodeObject(result.MergedPayload)
	if err != nil {
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
	mergedKey := make(oversync.SyncKey, len(conflict.Key))
	for column := range conflict.Key {
		mergedKey[column] = payload[column]
	}
	same, err := syncKeysEqual(mergedKey, conflict.Key)
	if err != nil || !same {
		return nil, fmt.Errorf("KeepMerged cannot change the conflicting key")
	}
	return payload, nil
}

func (c *Client) throwInvalidConflictResolution(ctx context.Context, snapshot *pushOutboundSnapshot, conflict ConflictContext, result MergeResult, message string) error {
	invalid := &InvalidConflictResolutionError{
		Conflict: conflict,
		Result:   result,
		Message:  message,
	}
	if snapshot == nil || !snapshot.automatic {
		return invalid
	}
	if err := c.restoreOutboundSnapshotToDirtyRows(ctx, snapshot); err != nil {
		return errors.Join(invalid, fmt.Errorf("failed to restore invalid automatic conflict decision: %w", err))
	}
	return invalid
}

func (c *Client) restoreOutboundSnapshotToDirtyRows(ctx context.Context, snapshot *pushOutboundSnapshot) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin outbound snapshot restore transaction: %w", err)
	}
	defer tx.Rollback()
	if err := c.guardConflictDecision(ctx, tx, snapshot.conflict); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys during outbound restore: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, nil); err != nil {
		return err
	}
	if err := recordConflictDecision(ctx, tx, snapshot.conflict, snapshot.decision); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, snapshot.SourceBundleID); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit outbound snapshot restore: %w", err)
	}
	return nil
}

func (c *Client) requeueSnapshotRowsInTx(ctx context.Context, tx sqliteTransaction, snapshot *pushOutboundSnapshot, skipRow *dirtyRowCapture) error {
	if snapshot == nil {
		return nil
	}
	for idx := range snapshot.Rows {
		row := &snapshot.Rows[idx]
		if skipRow != nil && row.SchemaName == skipRow.SchemaName && row.TableName == skipRow.TableName && row.KeyJSON == skipRow.KeyJSON {
			continue
		}
		current, err := c.loadDirtyUploadStateInTx(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON)
		if err != nil {
			return err
		}
		op, payload := row.Op, row.LocalPayload
		if current.Exists {
			op, payload = current.Op, current.Payload
		}
		base := historicalBase{Kind: row.BaseKind, Payload: row.BasePayload, Provenance: row.BaseProvenance}
		if err := c.requeueDirtyIntentInTx(ctx, tx, row.SchemaName, row.TableName, row.KeyJSON, op, row.BaseRowVersion, payload, base); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) deletePushOutboundSnapshotInTx(ctx context.Context, tx sqliteTransaction, sourceBundleID int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_outbox_rows WHERE source_bundle_id = ?`, sourceBundleID); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot for source_bundle_id %d: %w", sourceBundleID, err)
	}
	if err := clearOutboxBundle(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (c *Client) deleteStructuredRowStateInTx(ctx context.Context, tx sqliteTransaction, schemaName, tableName, keyJSON string) error {
	return c.installAuthoritativeBaseline(ctx, tx, schemaName, tableName, keyJSON, "absent", 0, nil, baselineObservation{Origin: "explicit_conflict"})
}

func (c *Client) deleteLocalRowInTx(ctx context.Context, tx sqliteTransaction, tableName, localPK string) error {
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
	value, err := jcs.DecodeObject([]byte(payload.String))
	if err != nil {
		return nil, fmt.Errorf("local %s conflict payload for %s.%s must be a JSON object: %w", op, schemaName, tableName, err)
	}
	if value == nil {
		return nil, fmt.Errorf("local %s conflict payload for %s.%s must be a JSON object", op, schemaName, tableName)
	}
	return value, nil
}

func (c *Client) applyConflictLocalAction(ctx context.Context, tx sqliteTransaction, tableName string, conflictingRow *dirtyRowCapture, localAction localRowAction) (sql.NullString, error) {
	switch action := localAction.(type) {
	case localRowUpsert:
		// Local decoding mutates its map. Capture complete L before that decode
		// and before application triggers can alter the installed business row.
		encoded, err := json.Marshal(action.payload)
		if err != nil {
			return sql.NullString{}, err
		}
		captured := string(encoded)
		if !action.storedLocal {
			captured, err = c.captureAuthoritativePayload(ctx, tx, tableName, encoded)
			if err != nil {
				return sql.NullString{}, err
			}
		}
		if action.storedLocal {
			err = c.upsertLocalRowInTxUsing(ctx, tx, tx, tableName, action.payload)
		} else {
			err = c.upsertRowInTx(ctx, tx, tableName, action.payload)
		}
		return sql.NullString{String: captured, Valid: err == nil}, err
	case localRowDelete:
		return sql.NullString{}, c.deleteLocalRowInTx(ctx, tx, tableName, conflictingRow.LocalPK)
	default:
		return sql.NullString{}, fmt.Errorf("unsupported local row action %T", localAction)
	}
}

func (c *Client) applyConflictStateAction(ctx context.Context, tx sqliteTransaction, tableName, keyJSON string, stateAction rowStateAction) error {
	switch action := stateAction.(type) {
	case rowStateUpsert:
		kind := "live"
		if action.deleted {
			kind = "deleted"
		}
		if err := c.installAuthoritativeBaseline(ctx, tx, c.schemaForTable(tableName), tableName, keyJSON, kind, action.rowVersion, action.payload, baselineObservation{Origin: "explicit_conflict"}); err != nil {
			return err
		}
	case rowStateDelete:
		if err := c.deleteStructuredRowStateInTx(ctx, tx, c.schemaForTable(tableName), tableName, keyJSON); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported row state action %T", action)
	}

	return nil
}

// Keep a newer business edit intact while resolving only its older frozen request.
// A newer delete needs a live authoritative base in both metadata representations.
func (c *Client) applyAcceptedConflictRow(ctx context.Context, tx sqliteTransaction, conflictingRow *dirtyRowCapture, conflict *oversync.PushConflictDetails) error {
	newer, err := c.loadDirtyUploadStateInTx(ctx, tx, conflictingRow.SchemaName, conflictingRow.TableName, conflictingRow.KeyJSON)
	if err != nil {
		return err
	}
	if newer.Exists {
		kind := "live"
		if conflict.ServerRowDeleted {
			kind = "deleted"
		} else if conflict.ServerRowVersion == 0 {
			kind = "absent"
		}
		if err := c.installAuthoritativeBaseline(ctx, tx, conflict.Schema, conflict.Table, conflictingRow.KeyJSON, kind, conflict.ServerRowVersion, conflict.ServerRow, baselineObservation{Origin: "explicit_conflict"}); err != nil {
			return err
		}
		op := newer.Op
		if op != oversync.OpDelete {
			if kind == "live" {
				op = oversync.OpUpdate
			} else {
				op = oversync.OpInsert
			}
		}
		return c.rebaseDirtyIntentInTx(ctx, tx, conflict.Schema, conflict.Table, conflictingRow.KeyJSON, op, conflict.ServerRowVersion, newer.Payload)
	}
	if !newer.Exists {
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

		return c.verifyAcceptedConflictRow(ctx, tx, conflictingRow, conflict)
	}

	return nil
}

// Check the stored row after application triggers, before acknowledging the decision.
func (c *Client) verifyAcceptedConflictRow(ctx context.Context, tx sqliteTransaction, row *dirtyRowCapture, conflict *oversync.PushConflictDetails) error {
	payload, exists, err := c.serializeExistingRowInTx(ctx, tx, conflict.Table, row.LocalPK)
	if err != nil {
		return err
	}
	expected := normalizeOptionalJSON(conflict.ServerRow)
	live := !conflict.ServerRowDeleted && len(expected) > 0
	if exists != live {
		return fmt.Errorf("AcceptServer final row does not match authoritative presence")
	}
	if !live {
		return nil
	}
	actual, err := c.processPayloadForUpload(conflict.Table, string(payload))
	if err != nil {
		return err
	}
	normalized, err := c.normalizeWirePayloadForSQLiteRoundTrip(conflict.Table, string(expected))
	if err != nil {
		return err
	}
	equal, err := canonicalPayloadEqual(sql.NullString{String: string(actual), Valid: true}, sql.NullString{String: string(normalized), Valid: true})
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("AcceptServer final row does not match authoritative payload")
	}
	return nil
}
