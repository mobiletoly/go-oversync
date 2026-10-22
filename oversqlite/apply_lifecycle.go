package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

type applyInputSequenceKey struct{}

func (c *Client) lifecycleAtomicWork(ctx context.Context, tx sqliteTransaction, p *applyPlan, work int64) (int64, error) {
	add := func(count, weight int64) error {
		if count < 0 || weight <= 0 {
			return fmt.Errorf("invalid atomic work estimate")
		}
		work = saturatingWorkAdd(work, saturatingWorkMultiply(count, weight))
		return nil
	}
	var maxFKs int64
	for _, table := range c.config.Tables {
		info, err := c.getTableInfoTx(tx, strings.ToLower(table.TableName))
		if err != nil {
			return 0, err
		}
		maxFKs = max(maxFKs, int64(len(info.ForeignKeys)))
	}
	if err := add(p.entries, 16*(maxFKs+int64(len(c.config.Tables)))); err != nil {
		return 0, err
	}
	return work, nil
}

func (c *Client) prepareConflictApply(ctx context.Context, tx sqliteTransaction, snapshot *pushOutboundSnapshot, selected *dirtyRowCapture, desired applySurvivor) error {
	h := sha256.New()
	encoder := json.NewEncoder(h)
	if err := encoder.Encode([]any{snapshot.SourceBundleID, fmt.Sprintf("%T", snapshot.decision), selected.SchemaName, selected.TableName, selected.KeyJSON, desired.payload, desired.wire}); err != nil {
		return err
	}
	unit := applyUnit("conflict", snapshot.SourceBundleID, fmt.Sprintf("%x", h.Sum(nil)))
	return c.prepareApplySurvivors(ctx, tx, unit, 1, func(int) (applySurvivor, error) { return desired, nil }, "conflict")
}

func (c *Client) conflictActionSurvivor(ctx context.Context, tx sqliteTransaction, selected *dirtyRowCapture, action localRowAction) (applySurvivor, error) {
	row := applySurvivor{table: selected.TableName, pk: selected.LocalPK, role: "intended"}
	upsert, ok := action.(localRowUpsert)
	if !ok {
		return row, nil
	}
	size, err := conflictPayloadBound(upsert.payload, 0)
	if err != nil {
		return row, err
	}
	unit := applyUnit("conflict-input", 0, selected.KeyJSON)
	bytes, err := c.workspaceScale(unit, size, 16)
	if err != nil {
		return row, err
	}
	scope, _ := ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
	if scope == nil {
		return row, fmt.Errorf("conflict buffer reservation is missing")
	}
	charge, err := c.addWorkspaceCharge(unit, scope.base, workspaceCharge{32, bytes})
	if err != nil {
		return row, err
	}
	if err := c.admitPushBufferCopies(ctx, tx, charge); err != nil {
		return row, err
	}
	encoded, err := json.Marshal(upsert.payload)
	row.payload = sql.NullString{String: string(encoded), Valid: true}
	row.wire = !upsert.storedLocal
	return row, err
}

// Admitted payloads are flat wire objects or two-level typed local captures.
// Bound all escaping and independent encodings before JSON allocates its buffer.
func conflictPayloadBound(value any, depth int) (int64, error) {
	if depth > 3 {
		return 0, fmt.Errorf("invalid conflict payload nesting")
	}
	switch v := value.(type) {
	case map[string]any:
		total := int64(2)
		for key, child := range v {
			size, err := conflictPayloadBound(child, depth+1)
			if err != nil {
				return 0, err
			}
			size, err = checkedAddInt64(size, 6*int64(len(key))+4)
			if err != nil {
				return 0, err
			}
			total, err = checkedAddInt64(total, size)
			if err != nil {
				return 0, err
			}
		}
		return total, nil
	case string:
		return 6*int64(len(v)) + 2, nil
	case nil, bool, json.Number, float64:
		return 64, nil
	default:
		return 0, fmt.Errorf("invalid conflict payload value")
	}
}

func (c *Client) finishConflictApply(ctx context.Context, tx sqliteTransaction) error {
	cache := newTxStmtCache(tx)
	defer cache.Close()
	if err := c.restoreTriggerAffectedDirtyIntentInTx(ctx, cache, tx); err != nil {
		return err
	}
	if _, err := c.restoreApplySurvivors(ctx, tx, cache, true); err != nil {
		return err
	}
	if _, err := c.restorePrimarySettlementIntent(ctx, tx, cache, true); err != nil {
		return err
	}
	if err := validateForeignKeyIntegrity(ctx, tx); err != nil {
		return err
	}
	return finishApplySurvivors(ctx, tx)
}

// Snapshot roots describe the complete final managed state. Existing rows absent
// from S are explicit deletions; surviving identities participate in the same
// dependency and uniqueness plan as ordinary final-state application.
func (c *Client) prepareSnapshotApply(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession) error {
	if err := requireNoUnresolvedUpload(ctx, tx); err != nil {
		return err
	}
	absentQuery, err := c.snapshotAbsentRowsQuery(tx, session.SnapshotID)
	if err != nil {
		return err
	}
	var absent int64
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM ("+absentQuery+")").Scan(&absent); err != nil {
		return err
	}
	roots, err := checkedAddInt64(session.RowCount, absent)
	if err != nil || roots > int64(int(^uint(0)>>1)) {
		return fmt.Errorf("snapshot apply root count overflow")
	}
	unit := applyUnit("snapshot", session.SnapshotBundleSeq, fmt.Sprintf("%s/%d/%d", session.SnapshotID, session.RowCount, session.ByteCount))
	return c.prepareApplySurvivors(ctx, tx, unit, int(roots), func(index int) (applySurvivor, error) {
		if int64(index) < session.RowCount {
			return c.snapshotStageSurvivor(ctx, tx, session.SnapshotID, int64(index)+1)
		}
		var row applySurvivor
		row.role = "intended"
		cursorTable, cursorKey := "", ""
		if int64(index) > session.RowCount {
			if err := tx.QueryRowContext(ctx, `SELECT table_name,local_pk FROM _sync_apply_rows WHERE ordinal=?`, index).Scan(&cursorTable, &cursorKey); err != nil {
				return row, err
			}
		}
		err := tx.QueryRowContext(ctx, "SELECT table_name,local_pk FROM ("+absentQuery+") WHERE (table_name,local_pk)>(?,?) ORDER BY table_name,local_pk LIMIT 1", cursorTable, cursorKey).Scan(&row.table, &row.pk)
		return row, err
	}, "snapshot")
}

func (c *Client) snapshotStageSurvivor(ctx context.Context, tx sqliteTransaction, id string, ordinal int64) (applySurvivor, error) {
	var table, key, payload string
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT charge FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?`, id, ordinal).Scan(&size); err != nil {
		return applySurvivor{}, err
	}
	p, err := loadApplyPlan(ctx, tx)
	if err != nil {
		return applySurvivor{}, err
	}
	if err := c.checkApplyWorkspace(p, 8, 8*size); err != nil {
		return applySurvivor{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT table_name,key_json,payload FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?`, id, ordinal).Scan(&table, &key, &payload); err != nil {
		return applySurvivor{}, err
	}
	pk, _, err := c.decodeDirtyKeyForPush(tx, table, key)
	return applySurvivor{table: table, pk: pk, payload: sql.NullString{String: payload, Valid: true}, wire: true, role: "intended"}, err
}

// No FK exception is relabeled. A blocker is derived from an admitted desired
// payload and concrete selected parent disposition before any business write.
func (c *Client) validateApplyDependencies(ctx context.Context, tx sqliteTransaction, p *applyPlan) error {
	var pending *FKReconciliationRequiredError
	for ordinal := int64(1); ordinal <= p.entries; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, _, err := c.loadApplySurvivor(ctx, tx, p, ordinal)
		if err != nil {
			return err
		}
		err = c.validateApplyRowDependencies(ctx, tx, p, row)
		var blocker *FKReconciliationRequiredError
		if err != nil && !errors.As(err, &blocker) {
			return err
		}
		if blocker == nil {
			continue
		}
		if pending == nil {
			if err := c.checkApplyWorkspace(p, 8, 16*4096); err != nil {
				return err
			}
			p.diagnosticCharge = workspaceCharge{8, 16 * 4096}
			pending = blocker
		} else {
			pending.HasMore = true
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if pending != nil {
		return pending
	}
	return nil
}

func (c *Client) validateApplyRowDependencies(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor) error {
	if !row.payload.Valid {
		if p.kind != "snapshot" {
			unknown, err := c.unknownIncomingPushCoverage(ctx, tx, &pushNode{table: row.table})
			if err != nil {
				return err
			}
			if unknown {
				return c.applyPending(ctx, tx, p, row, "relationship_unknown")
			}
		}
		return nil
	}
	payload, err := jcs.DecodeObject([]byte(row.payload.String))
	if err != nil {
		return err
	}
	if !row.wire {
		if err := c.validateAndUnwrapCapturedPayload(row.table, payload); err != nil {
			return err
		}
	}
	info, err := c.getTableInfoTx(tx, row.table)
	if err != nil {
		return err
	}
	for _, fk := range info.ForeignKeys {
		parent, err := c.desiredApplyParent(row, payload, info, fk)
		if err != nil {
			return err
		}
		if parent == "" && payload[strings.ToLower(fk.FromCol)] == nil {
			continue
		}
		if err := c.validateApplyParent(ctx, tx, p, row, strings.ToLower(fk.RefTable), parent); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) desiredApplyParent(row applySurvivor, payload map[string]any, info *TableInfo, fk ForeignKeyInfo) (string, error) {
	column := strings.ToLower(fk.FromCol)
	raw, ok := payload[column]
	if !ok {
		return "", fmt.Errorf("apply payload missing admitted FK column")
	}
	if raw == nil {
		return "", nil
	}
	var value any
	var err error
	if row.wire {
		for _, col := range info.Columns {
			if strings.EqualFold(col.Name, column) {
				value, err = c.sqliteValueWithDeclaredCodec(row.table, col, raw)
			}
		}
	} else {
		codec, _, _ := c.declaredColumn(row.table, column)
		value, err = sqliteValueFromLocalCodec(codec, raw)
	}
	if err != nil {
		return "", err
	}
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return hex.EncodeToString(v), nil
	default:
		return "", fmt.Errorf("apply FK has unsupported admitted identity type")
	}
}

func (c *Client) validateApplyParent(ctx context.Context, tx sqliteTransaction, p *applyPlan, child applySurvivor, table, pk string) error {
	var present bool
	err := tx.QueryRowContext(ctx, `SELECT payload IS NOT NULL FROM _sync_apply_rows WHERE table_name=? AND local_pk=?`, table, pk).Scan(&present)
	if err == nil && present {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil || p.kind == "snapshot" {
		return c.applyPending(ctx, tx, p, child, "dependency_unavailable")
	}
	key, err := c.syncKeyJSONForPKInTx(tx, table, pk)
	if err != nil {
		return err
	}
	var kind string
	var version int64
	var known bool
	err = tx.QueryRowContext(ctx, `SELECT server_kind,relationships_known,row_version FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, c.schemaForTable(table), table, key).Scan(&kind, &known, &version)
	if errors.Is(err, sql.ErrNoRows) {
		value, e := c.convertPKForQueryInTx(tx, table, pk)
		if e != nil {
			return e
		}
		var exists bool
		e = tx.QueryRowContext(ctx, `SELECT 1 FROM `+quoteIdent(table)+` WHERE `+quoteIdent(c.pkByTable[table])+`=?`, value).Scan(&exists)
		if e == nil {
			return fmt.Errorf("mandatory parent authoritative metadata is missing")
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		return c.applyPending(ctx, tx, p, child, "dependency_unavailable")
	}
	if err != nil {
		return err
	}
	if kind == "unknown" && !known {
		return c.applyPending(ctx, tx, p, child, "relationship_unknown")
	}
	if kind != "live" {
		if input, ok := ctx.Value(applyInputSequenceKey{}).(int64); ok && version > input {
			return c.applyPending(ctx, tx, p, child, "historical_repair_required")
		}
		return c.applyPending(ctx, tx, p, child, "dependency_unavailable")
	}
	value, err := c.convertPKForQueryInTx(tx, table, pk)
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM `+quoteIdent(table)+` WHERE `+quoteIdent(c.pkByTable[table])+`=?`, value).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return c.applyPending(ctx, tx, p, child, "historical_repair_required")
	}
	return err
}

func (c *Client) applyPending(ctx context.Context, tx sqliteTransaction, p *applyPlan, row applySurvivor, detail string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := c.syncKeyJSONForPKInTx(tx, row.table, row.pk)
	if err != nil {
		return err
	}
	_, wireKey, err := c.decodeDirtyKeyForPush(tx, row.table, key)
	if err != nil {
		return err
	}
	ref := PushDependencyRecordRef{Schema: c.schemaForTable(row.table), Table: row.table, Key: wireKey}
	bound := int64(6*(len(ref.Schema)+len(ref.Table)) + 128)
	for key, value := range ref.Key {
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("invalid canonical pending key")
		}
		bound += int64(6*(len(key)+len(text)) + 8)
	}
	if err := c.checkApplyWorkspace(p, 4, 4*bound); err != nil {
		return err
	}
	encoded, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if err := c.checkApplyWorkspace(p, 4, 4*int64(len(encoded))); err != nil {
		return err
	}
	result := &FKReconciliationRequiredError{Detail: detail, Unit: p.guard}
	if len(encoded)+2 <= 4096 {
		result.Sample = []PushDependencyRecordRef{ref}
	} else {
		result.HasMore = true
	}
	return result
}

func (c *Client) verifySnapshotFinalState(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession) error {
	cache := newTxStmtCache(tx)
	defer cache.Close()
	if _, err := c.restoreApplySurvivors(ctx, tx, cache, true); err != nil {
		return err
	}
	for ordinal := int64(1); ordinal <= session.RowCount; ordinal++ {
		if err := c.verifySnapshotAuthoritativeRow(ctx, tx, session, ordinal); err != nil {
			return err
		}
	}
	for _, table := range c.config.Tables {
		if err := c.verifySnapshotTableScope(ctx, tx, session, table.TableName); err != nil {
			return err
		}
	}
	return validateForeignKeyIntegrity(ctx, tx)
}

func deleteSnapshotApplyPages(ctx context.Context, tx sqliteTransaction, id string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage WHERE rowid IN (SELECT rowid FROM _sync_snapshot_stage WHERE snapshot_id=? ORDER BY row_ordinal LIMIT 128)`, id)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil || count == 0 {
			return err
		}
	}
}

func (c *Client) verifySnapshotAuthoritativeRow(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession, ordinal int64) error {
	row, err := c.snapshotStageSurvivor(ctx, tx, session.SnapshotID, ordinal)
	if err != nil {
		return err
	}
	key, err := c.syncKeyJSONForPKInTx(tx, row.table, row.pk)
	if err != nil {
		return err
	}
	expected, err := c.captureAuthoritativePayload(ctx, tx, row.table, []byte(row.payload.String))
	if err != nil {
		return err
	}
	var actual sql.NullString
	var version, declared int64
	var kind string
	if err := tx.QueryRowContext(ctx, `SELECT row_version,server_kind,server_payload FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, c.schemaForTable(row.table), row.table, key).Scan(&version, &kind, &actual); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT row_version FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?`, session.SnapshotID, ordinal).Scan(&declared); err != nil {
		return err
	}
	equal, err := canonicalPayloadEqual(actual, sql.NullString{String: expected, Valid: true})
	if err != nil {
		return err
	}
	if err := c.validateBaselineProjection(ctx, tx, c.schemaForTable(row.table), row.table, key, "server", sql.NullString{}); err != nil {
		return err
	}
	var provenanceOK bool
	if err := tx.QueryRowContext(ctx, `SELECT json_extract(server_provenance,'$.origin')='snapshot' AND json_extract(server_provenance,'$.authoritative_version')=? AND json_extract(server_provenance,'$.bundle_sequence')=? FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, declared, session.SnapshotBundleSeq, c.schemaForTable(row.table), row.table, key).Scan(&provenanceOK); err != nil {
		return err
	}
	if !provenanceOK {
		return fmt.Errorf("snapshot authoritative provenance differs from S")
	}

	if !equal || version != declared || kind != "live" {
		return fmt.Errorf("snapshot authoritative metadata does not match staged snapshot")
	}
	return nil
}
func (c *Client) verifySnapshotTableScope(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession, table string) error {
	var desired, live, authoritative, staged int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM _sync_apply_rows WHERE table_name=? AND payload IS NOT NULL`, table).Scan(&desired); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM `+quoteIdent(table)).Scan(&live); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM _sync_row_state WHERE table_name=? AND server_kind='live'`, table).Scan(&authoritative); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM _sync_snapshot_stage WHERE snapshot_id=? AND table_name=?`, session.SnapshotID, table).Scan(&staged); err != nil {
		return err
	}
	if live != desired || authoritative != staged {
		return fmt.Errorf("snapshot final scope count does not match intended installation")
	}
	return nil
}
func (c *Client) completeSnapshotDesiredState(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, session *oversync.SnapshotSession) error {
	if err := c.restoreTriggerAffectedDirtyIntentInTx(ctx, cache, tx); err != nil {
		return err
	}
	if err := c.verifySnapshotFinalState(ctx, tx, session); err != nil {
		return err
	}
	return finishApplySurvivors(ctx, tx)
}
