package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

type applyRowIDKey struct{}

// Each HTTP attempt retains the authority of its owning snapshot workflow.
// Retries must not turn a previously clean admission into permission to send.
type snapshotDispatchAdmissionKey struct{}
type snapshotDispatchAdmission struct {
	guard              *snapshotApplyGuard
	upgrade            *upgradeJournalRecord
	replacement        *upgradeJournalRecord
	replacementContext string
}

func (c *Client) validateSnapshotRequestAdmission(ctx context.Context) error {
	admission, ok := ctx.Value(snapshotDispatchAdmissionKey{}).(snapshotDispatchAdmission)
	if !ok {
		return nil // Low-level wire consumers do not own a database workflow.
	}
	if admission.upgrade != nil {
		return c.validateUpgradeSnapshotDispatch(ctx, admission.upgrade)
	}
	if admission.replacement != nil {
		current, err := c.snapshotReplacementContext(ctx, admission.replacement)
		if err != nil {
			return err
		}
		if current != admission.replacementContext {
			return fmt.Errorf("snapshot replacement context changed")
		}
		return nil
	}
	return c.validateSnapshotDispatch(ctx, admission.guard)
}

// U5 intentionally removed source counters. U6 admits only its exact existing
// replacement journal and local context, without inventing counters or claiming
// the confirmed-snapshot proof required by the application engine at U7/U8.
func (c *Client) snapshotReplacementContext(ctx context.Context, expected *upgradeJournalRecord) (string, error) {
	var result string
	err := c.completeWork(ctx, "snapshot replacement dispatch", func(ctx context.Context) error {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		result, err = c.readSnapshotReplacementContext(ctx, tx, expected)
		if err != nil {
			return err
		}
		return tx.Commit()
	})
	return result, err
}

func (c *Client) readSnapshotReplacementContext(ctx context.Context, tx sqliteTransaction, expected *upgradeJournalRecord) (string, error) {
	charge, err := c.snapshotReplacementWorkspaceCharge(ctx, tx)
	if err != nil {
		return "", err
	}
	if err := c.checkAggregateWorkspace(ctx, tx, "snapshot-replacement-dispatch", nil, nil, charge); err != nil {
		return "", err
	}
	if err := c.reserveReadySyncWork(ctx, "snapshot-replacement-dispatch", saturatingWorkAdd(128, charge.bytes)); err != nil {
		return "", err
	}
	if err := requireNoUnresolvedUpload(ctx, tx); err != nil {
		return "", err
	}
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil {
		return "", err
	}
	if current == nil || *current != *expected || current.Phase != UpgradePhaseReattachPending ||
		!current.DrainComplete || !current.MigrationComplete || current.ReplacementConfirmed {
		return "", fmt.Errorf("snapshot replacement journal changed")
	}
	guard, err := loadSnapshotGuardState(ctx, tx, snapshotModeClean)
	if err != nil {
		return "", err
	}
	if err := validateSnapshotGuardMode(guard); err != nil {
		return "", err
	}
	identity, err := loadRetainedUpgradeContractIdentity(ctx, tx)
	if err != nil || identity == nil {
		return "", fmt.Errorf("snapshot replacement identity missing: %w", err)
	}
	var revision, baseline, sources int64
	if err := tx.QueryRowContext(ctx, `SELECT revision,baseline_revision,EXISTS(SELECT 1 FROM _sync_source_state) FROM _sync_conflict_state`).Scan(&revision, &baseline, &sources); err != nil {
		return "", err
	}
	encoded, err := json.Marshal([]any{c.BaseURL, current, guard.lifecycle, guard.outbox, revision, baseline, sources,
		identity.databaseID, identity.serviceID, identity.generation, identity.localVersion, identity.physicalShape, identity.fingerprint,
		c.schemaGenerationProof, c.config.Tables, c.config.MaxSyncWorkspaceEntries, c.config.MaxSyncWorkspaceBytes, c.config.MaxSyncWorkUnitsPerInvocation, c.discovery.Limits})
	if err != nil {
		return "", err
	}
	return c.snapshotSchemaGuard(ctx, tx, fmt.Sprintf("%x", sha256.Sum256(encoded)))
}

// The target registration can exceed the retained old-generation identity.
// Measure each simultaneous descriptor/JSON copy before reading the journal.
func (c *Client) snapshotReplacementWorkspaceCharge(ctx context.Context, tx sqliteTransaction) (workspaceCharge, error) {
	charge, err := c.pendingUpgradeProofWorkspaceCharge(ctx, tx, "snapshot-replacement-dispatch")
	if err != nil {
		return workspaceCharge{}, err
	}
	contextCharge, err := c.applyGuardWorkspaceCharge(ctx, tx, "snapshot-replacement-dispatch")
	if err != nil {
		return workspaceCharge{}, err
	}
	charge, err = c.addWorkspaceCharge("snapshot-replacement-dispatch", charge, contextCharge)
	if err != nil {
		return workspaceCharge{}, err
	}
	charge, err = c.addWorkspaceCharge("snapshot-replacement-dispatch", charge, workspaceCharge{4, 512 + 16*int64(len(c.BaseURL))})
	if err != nil {
		return workspaceCharge{}, err
	}
	for _, table := range c.config.Tables {
		bytes := saturatingWorkAdd(int64(len(table.TableName)), int64(len(table.SyncKeyColumnName)))
		for _, key := range table.SyncKeyColumns {
			bytes = saturatingWorkAdd(bytes, int64(len(key)))
		}
		descriptor := workspaceCharge{4 + int64(len(table.SyncKeyColumns)), saturatingWorkAdd(512, saturatingWorkMultiply(bytes, 16))}
		charge, err = c.addWorkspaceCharge("snapshot-replacement-dispatch", charge, descriptor)
		if err != nil {
			return workspaceCharge{}, err
		}
	}
	return charge, nil
}

func (c *Client) snapshotContextGuard(ctx context.Context, tx sqliteTransaction) (string, error) {
	if err := c.checkAggregateWorkspace(ctx, tx, "snapshot-context", nil, nil, workspaceCharge{4, 512 + 16*int64(len(c.BaseURL))}); err != nil {
		return "", err
	}
	guard, err := c.applyGuard(ctx, tx, "snapshot-context")
	if err != nil {
		return "", err
	}
	guard, err = c.snapshotSchemaGuard(ctx, tx, guard)
	if err != nil {
		return "", err
	}
	var modePin any
	if pin, ok := ctx.Value(automaticRecoveryKey{}).(automaticRecoveryPin); ok {
		modePin = []any{pin.id, pin.revision}
	}
	encoded, err := json.Marshal([]any{c.BaseURL, guard, modePin})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

// TEMP schema_version is connection-local and resets on reopen. Bind actual
// TEMP definitions so an empty schema has a stable identity across lifetimes.
func (c *Client) snapshotSchemaGuard(ctx context.Context, tx sqliteTransaction, guard string) (string, error) {
	var main, entries, bytes int64
	if err := tx.QueryRowContext(ctx, `PRAGMA main.schema_version`).Scan(&main); err != nil {
		return "", err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))+length(CAST(type AS BLOB))+length(CAST(tbl_name AS BLOB))+length(CAST(COALESCE(sql,'') AS BLOB))),0) FROM sqlite_temp_schema`).Scan(&entries, &bytes); err != nil {
		return "", err
	}
	work := saturatingWorkAdd(saturatingWorkMultiply(entries, 128), saturatingWorkMultiply(bytes, 2))
	if err := c.reserveReadySyncWork(ctx, "snapshot-temp-schema", work); err != nil {
		return "", err
	}
	if err := c.checkAggregateWorkspace(ctx, tx, "snapshot-temp-schema", nil, nil, workspaceCharge{entries + 4, saturatingWorkAdd(4096, saturatingWorkMultiply(bytes, 16))}); err != nil {
		return "", err
	}
	rows, err := tx.QueryContext(ctx, `SELECT name,type,tbl_name,COALESCE(sql,'') FROM sqlite_temp_schema ORDER BY name,type`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	h := sha256.New()
	encoder := json.NewEncoder(h)
	if err := encoder.Encode([]any{guard, main}); err != nil {
		return "", err
	}
	for rows.Next() {
		var definition [4]string
		if err := rows.Scan(&definition[0], &definition[1], &definition[2], &definition[3]); err != nil {
			return "", err
		}
		if err := encoder.Encode(definition); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (c *Client) inspectSnapshotCapabilities(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p := &applyPlan{kind: "snapshot", unit: "snapshot-inspection", workspaceContext: ctx, workspaceQuery: tx}
	for _, table := range c.config.Tables {
		p.cursor = sql.NullString{}
		for {
			index, err := c.nextApplyUniqueIndex(ctx, tx, p, applySurvivor{table: strings.ToLower(table.TableName)})
			if err != nil {
				return err
			}
			if index == nil {
				break
			}
			p.cursor = sql.NullString{String: index.name, Valid: true}
		}
	}
	return tx.Commit()
}

// A visible SQLite rowid is part of local identity when an SCC must be vacated.
// Prefer an unshadowed alias; WITHOUT ROWID tables have no such identity.
func (c *Client) observableRowIDColumn(ctx context.Context, q tableInfoQueryer, table string) (string, error) {
	rows, err := q.Query(`SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?`, table)
	if err != nil {
		return "", err
	}
	var without bool
	if !rows.Next() {
		rows.Close()
		return "", fmt.Errorf("snapshot table disappeared: %s", table)
	}
	err = rows.Scan(&without)
	err = errors.Join(err, rows.Close())
	if err != nil || without {
		return "", err
	}
	info, err := c.getTableInfoTx(q, table)
	if err != nil {
		return "", err
	}
	for _, alias := range []string{"rowid", "_rowid_", "oid"} {
		shadowed := false
		for _, column := range info.Columns {
			if strings.EqualFold(column.Name, alias) {
				shadowed = true
			}
		}
		if !shadowed {
			return alias, nil
		}
	}
	return "", nil
}

func (c *Client) captureApplyRowID(ctx context.Context, tx sqliteTransaction, table, key string) (sql.NullInt64, error) {
	var result sql.NullInt64
	alias, err := c.observableRowIDColumn(ctx, tx, table)
	if err != nil || alias == "" {
		return result, err
	}
	value, err := c.convertPKForQueryInTx(tx, table, key)
	if err != nil {
		return result, err
	}
	err = scanTransactionRow(ctx, tx, `SELECT `+quoteIdent(alias)+` FROM `+quoteIdent(table)+` WHERE `+quoteIdent(c.pkByTable[table])+`=?`, []any{value}, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return sql.NullInt64{}, nil
	}
	return result, err
}

func (c *Client) snapshotAbsentRowsQuery(tx sqliteTransaction, id string) (string, error) {
	parts := make([]string, 0, len(c.config.Tables))
	for _, table := range c.config.Tables {
		name := strings.ToLower(table.TableName)
		info, err := c.getTableInfoTx(tx, name)
		if err != nil {
			return "", err
		}
		pk := c.pkByTable[name]
		key := applyKeyExpression(pk, info.PrimaryKeyIsBlob)
		// The indexed candidate uses the capture owner's canonical encoding;
		// membership additionally compares the exact decoded typed identity.
		path := quoteSQLLiteral(`$."` + strings.ToLower(pk) + `"`)
		parts = append(parts, `SELECT `+quoteSQLLiteral(name)+` AS table_name,`+key+` AS local_pk FROM `+quoteIdent(name)+` b WHERE NOT EXISTS(SELECT 1 FROM _sync_snapshot_stage s WHERE s.snapshot_id=`+quoteSQLLiteral(id)+` AND s.schema_name=`+quoteSQLLiteral(c.schemaForTable(name))+` AND s.table_name=`+quoteSQLLiteral(name)+` AND s.key_json=json_object(`+quoteSQLLiteral(strings.ToLower(pk))+`,`+key+`) AND json_extract(s.key_json,`+path+`) COLLATE BINARY=`+key+` COLLATE BINARY)`)
	}
	if len(parts) == 0 {
		return `SELECT '' AS table_name,'' AS local_pk WHERE 0`, nil
	}
	return strings.Join(parts, " UNION ALL "), nil
}

func quoteSQLLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

// Absence at T is not a tombstone version. Reuse the authoritative baseline
// owner for both prior observations and identities removed by this delta.
func (c *Client) installSnapshotAbsenceBaselines(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession) error {
	_, err := tx.ExecContext(ctx, `UPDATE _sync_row_state AS a SET row_version=0,server_kind='absent',server_payload=NULL,relationships_known=1,server_provenance=`+baselineProvenanceSQL("'snapshot'", "0", "?")+` WHERE NOT EXISTS(SELECT 1 FROM _sync_snapshot_stage s WHERE s.snapshot_id=? AND (s.schema_name,s.table_name,s.key_json)=(a.schema_name,a.table_name,a.key_json))`, session.SnapshotBundleSeq, session.SnapshotID)
	if err != nil {
		return err
	}
	var last int64
	for {
		var ordinal int64
		var table, pk string
		err := tx.QueryRowContext(ctx, `SELECT ordinal,table_name,local_pk FROM _sync_apply_rows WHERE ordinal>? AND payload IS NULL ORDER BY ordinal LIMIT 1`, last).Scan(&ordinal, &table, &pk)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		key, err := c.syncKeyJSONForPKInTx(tx, table, pk)
		if err != nil {
			return err
		}
		if err := c.installAuthoritativeBaseline(ctx, tx, c.schemaForTable(table), table, key, "absent", 0, nil, baselineObservation{Origin: "snapshot", BundleSeq: &session.SnapshotBundleSeq}); err != nil {
			return err
		}
		last = ordinal
	}
}

// This is the existing operation owner's transfer checkpoint, not another
// recovery coordinator. Only auxiliary rows commit before the final delta.
type snapshotTransfer struct {
	Session  oversync.SnapshotSession `json:"session"`
	Source   string                   `json:"source"`
	Guard    string                   `json:"guard"`
	Rows     int64                    `json:"rows"`
	Bytes    int64                    `json:"bytes"`
	Complete bool                     `json:"complete"`
}

func (c *Client) retainUpgradeSnapshot(ctx context.Context, tx sqliteTransaction, rec *upgradeJournalRecord, session *oversync.SnapshotSession) error {
	floor, err := c.snapshotFloor(ctx, tx)
	if err != nil {
		return err
	}
	if session.SnapshotBundleSeq < floor {
		return fmt.Errorf("upgrade snapshot boundary precedes installed observation floor")
	}
	guard, err := c.snapshotContextGuard(ctx, tx)
	if err != nil {
		return err
	}
	return c.persistSnapshotTransfer(ctx, tx, &snapshotTransfer{Session: *session, Source: rec.NewSourceID, Guard: guard})
}

func (c *Client) upgradeSnapshotDescriptor(ctx context.Context, q sqliteTransaction, rec *upgradeJournalRecord) (*oversync.SnapshotSession, error) {
	op, err := loadOperationState(ctx, q)
	if err != nil {
		return nil, err
	}
	var state snapshotTransfer
	if err := json.Unmarshal([]byte(op.SnapshotTransfer), &state); err != nil {
		return nil, fmt.Errorf("upgrade snapshot seal missing: %w", err)
	}
	s := state.Session
	if s.SnapshotID != rec.SnapshotID || s.SnapshotBundleSeq != rec.SnapshotBundleSeq || s.RowCount != rec.SnapshotRowCount || s.ByteCount != rec.SnapshotByteCount || state.Source != rec.NewSourceID {
		return nil, fmt.Errorf("upgrade snapshot seal differs from progress journal")
	}
	if err := validateSnapshotSession(&s); err != nil {
		return nil, err
	}
	guard, err := c.snapshotContextGuard(ctx, q)
	if err != nil {
		return nil, err
	}
	if state.Guard != guard {
		return nil, fmt.Errorf("upgrade snapshot authority or local context changed")
	}
	floor, err := c.snapshotFloor(ctx, q)
	if err != nil {
		return nil, err
	}
	if s.SnapshotBundleSeq < floor {
		return nil, fmt.Errorf("upgrade snapshot boundary precedes installed observation floor")
	}
	return &s, nil
}

func (c *Client) snapshotFloor(ctx context.Context, q queryRower) (int64, error) {
	var observations int64
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM _sync_row_state`).Scan(&observations); err != nil {
		return 0, err
	}
	if err := c.reserveReadySyncWork(ctx, "snapshot-observation-floor", saturatingWorkAdd(32, saturatingWorkMultiply(observations, 3))); err != nil {
		return 0, err
	}
	var floor int64
	err := q.QueryRowContext(ctx, `SELECT max(v) FROM (SELECT last_bundle_seq_seen v FROM _sync_attachment_state UNION ALL SELECT row_version FROM _sync_row_state UNION ALL SELECT COALESCE(json_extract(server_provenance,'$.bundle_sequence'),0) FROM _sync_row_state)`).Scan(&floor)
	if err != nil {
		return 0, err
	}
	// A blocked authoritative unit can be newer than every installed row.
	var blocked int64
	err = q.QueryRowContext(ctx, `SELECT CASE WHEN json_extract(NULLIF(recovery_state,''),'$.authority_hash')=? THEN COALESCE(json_extract(NULLIF(recovery_state,''),'$.observed_boundary'),0) ELSE 0 END FROM _sync_operation_state WHERE singleton_key=1`, c.recoveryAuthorityHash()).Scan(&blocked)
	return max(floor, blocked), err
}

func (c *Client) persistSnapshotTransfer(ctx context.Context, tx sqliteTransaction, state *snapshotTransfer) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	unit := applyUnit("snapshot-transfer", state.Session.SnapshotBundleSeq, state.Session.SnapshotID)
	if err := c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, workspaceCharge{8, 4096 + 16*int64(len(encoded))}); err != nil {
		return err
	}
	op, err := loadOperationState(ctx, tx)
	if err != nil {
		return err
	}
	op.StagedSnapshotID, op.SnapshotBundleSeq, op.SnapshotRowCount = state.Session.SnapshotID, state.Session.SnapshotBundleSeq, state.Session.RowCount
	op.SnapshotTransfer = string(encoded)
	return persistOperationState(ctx, tx, op)
}

func (c *Client) loadSnapshotTransfer(ctx context.Context, guard *snapshotApplyGuard, source string) (*snapshotTransfer, error) {
	op, err := loadOperationState(ctx, c.db)
	if err != nil || op.SnapshotTransfer == "" {
		return nil, err
	}
	var state snapshotTransfer
	if err := json.Unmarshal([]byte(op.SnapshotTransfer), &state); err != nil {
		return nil, &snapshotEvidenceError{"corrupt transfer"}
	}
	if err := validateSnapshotSession(&state.Session); err != nil {
		return nil, &snapshotEvidenceError{"invalid retained session"}
	}
	if state.Rows < 0 || state.Bytes < 0 || state.Rows > state.Session.RowCount || state.Bytes > state.Session.ByteCount || state.Complete && (state.Rows != state.Session.RowCount || state.Bytes != state.Session.ByteCount) {
		return nil, &snapshotEvidenceError{"invalid transfer checkpoint"}
	}
	if state.Source != source || state.Guard != guard.context {
		if ctx.Value(automaticRecoveryKey{}) != nil {
			if err := chargeAutomaticRecoveryReplan(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	floor, err := c.snapshotFloor(ctx, c.db)
	if err != nil {
		return nil, err
	}
	if state.Session.SnapshotBundleSeq < floor {
		if ctx.Value(automaticRecoveryKey{}) != nil {
			if err := chargeAutomaticRecoveryReplan(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	return &state, nil
}

func (c *Client) stageOperationSnapshot(ctx context.Context, options snapshotApplyOptions, limits snapshotNegotiation) (*oversync.SnapshotSession, snapshotTransferTotals, error) {
	guard := options.PinnedGuard
	if guard == nil {
		return nil, snapshotTransferTotals{}, fmt.Errorf("snapshot transfer has no context guard")
	}
	ctx = context.WithValue(ctx, snapshotDispatchAdmissionKey{}, snapshotDispatchAdmission{guard: guard})
	source := c.sourceID
	if options.RotateSource {
		source = options.NewSourceID
	}
	state, err := completeWorkPhase(ctx, c, "snapshot retained transfer", func(ctx context.Context) (*snapshotTransfer, error) {
		return c.loadSnapshotTransfer(ctx, guard, source)
	})
	if err != nil {
		return nil, snapshotTransferTotals{}, err
	}
	replacementConfirmed := state != nil && options.RotateSource
	for replacements := 0; ; replacements++ {
		if state == nil {
			state, err = c.createOperationSnapshot(ctx, options, guard, source, replacementConfirmed)
			if err != nil {
				return nil, snapshotTransferTotals{}, err
			}
			replacementConfirmed = options.RotateSource
		}
		err = c.downloadOperationSnapshot(ctx, state, guard, limits)
		if isRestartableSnapshotSessionError(err) && replacements == 0 && !state.Complete {
			if i := invocationFromContext(ctx); i != nil {
				if i.snapshotReplacements >= 1 {
					return nil, snapshotTransferTotals{}, err
				}
				i.snapshotReplacements++
			}
			state = nil
			continue
		}
		if err != nil {
			return nil, snapshotTransferTotals{}, err
		}
		return &state.Session, snapshotTransferTotals{state.Rows, state.Bytes}, nil
	}
}

func (c *Client) createOperationSnapshot(ctx context.Context, options snapshotApplyOptions, guard *snapshotApplyGuard, source string, replacementConfirmed bool) (*snapshotTransfer, error) {
	if err := c.validateSnapshotDispatch(ctx, guard); err != nil {
		return nil, err
	}
	request, err := snapshotSessionCreateRequestFromOptions(c.sourceID, options)
	if err != nil {
		return nil, err
	}
	requestSource := c.sourceID
	if replacementConfirmed {
		// The durable receipt already establishes the replacement. Replaying
		// that tuple could return its expired session; fetch a fresh baseline
		// under the established source without another rotation.
		request, requestSource = nil, source
	}
	floor, err := completeWorkPhase(ctx, c, "snapshot floor", func(ctx context.Context) (int64, error) { return c.snapshotFloor(ctx, c.db) })
	if err != nil {
		return nil, err
	}
	if request == nil {
		request = &oversync.SnapshotSessionCreateRequest{Recovery: &oversync.SnapshotRecovery{MinimumBundleSeq: floor}}
	}
	// Remote attach is a different lifecycle, with no previously installed
	// account baseline. It uses ordinary source-bound snapshot metadata.
	if options.FinalizeRemoteReplace {
		request = nil
		floor = 0
	}
	session, err := c.createSnapshotSessionAsSource(ctx, request, requestSource)
	if err != nil {
		return nil, err
	}
	if session.SnapshotBundleSeq < floor {
		return nil, &snapshotEvidenceError{"snapshot boundary precedes installed observations"}
	}
	state := &snapshotTransfer{Session: *session, Source: source, Guard: guard.context}
	err = c.completeWork(ctx, "snapshot publish", func(ctx context.Context) (resultErr error) {
		tx, err := beginImmediateTx(ctx, c.db)
		if err != nil {
			return err
		}
		defer tx.rollbackOnReturn(&resultErr, "snapshot publication")
		if err := c.validateFinalSnapshotApplyGate(ctx, tx, guard, session); err != nil {
			return err
		}
		var obsolete int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM _sync_snapshot_stage`).Scan(&obsolete); err != nil {
			return err
		}
		if err := c.reserveReadySyncWork(ctx, "snapshot-stage-replacement", saturatingWorkMultiply(obsolete, 128)); err != nil {
			return err
		}
		if err := deleteAllSnapshotPages(ctx, tx); err != nil {
			return err
		}
		if err := c.persistSnapshotTransfer(ctx, tx, state); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	return state, err
}

func deleteAllSnapshotPages(ctx context.Context, tx sqliteTransaction) error {
	for {
		result, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage WHERE rowid IN (SELECT rowid FROM _sync_snapshot_stage ORDER BY snapshot_id,row_ordinal LIMIT 128)`)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil || n == 0 {
			return err
		}
	}
}

func (c *Client) downloadOperationSnapshot(ctx context.Context, state *snapshotTransfer, guard *snapshotApplyGuard, limits snapshotNegotiation) error {
	for !state.Complete {
		if err := c.validateSnapshotDispatch(ctx, guard); err != nil {
			return err
		}
		chunk, _, err := c.fetchSnapshotChunkAsSource(ctx, state.Session.SnapshotID, state.Session.SnapshotBundleSeq, state.Rows, limits.maxRows, limits.maxBytes, state.Source)
		if err != nil {
			return err
		}
		next, err := c.publishSnapshotTransferPage(ctx, state, guard, chunk)
		if err != nil {
			return err
		}
		*state = next
	}
	// A verified complete stage is locally owned and remains usable after TTL.
	// Rehash on every resumed lifetime so corrupt rows cannot inherit its marker.
	return c.completeWork(ctx, "snapshot seal verification", func(ctx context.Context) error {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := c.validateFinalSnapshotApplyGate(ctx, tx, guard, &state.Session); err != nil {
			return err
		}
		if err := c.verifySnapshotStageHash(ctx, tx, &state.Session, state.Source); err != nil {
			return err
		}
		return tx.Commit()
	})
}

// Every resumable transfer boundary revalidates the entire pinned context
// before another request. The read ends before network I/O begins.
func (c *Client) validateSnapshotDispatch(ctx context.Context, guard *snapshotApplyGuard) error {
	return c.completeWork(ctx, "snapshot dispatch admission", func(ctx context.Context) error {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := c.validateFinalSnapshotApplyGate(ctx, tx, guard, nil); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (c *Client) validateUpgradeSnapshotDispatch(ctx context.Context, rec *upgradeJournalRecord) error {
	return c.completeWork(ctx, "upgrade snapshot dispatch admission", func(ctx context.Context) error {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := requireNoUnresolvedUpload(ctx, tx); err != nil {
			return err
		}
		if _, err := c.upgradeSnapshotDescriptor(ctx, tx, rec); err != nil {
			return err
		}
		return tx.Commit()
	})
}

// A page and its progress/complete marker publish in one auxiliary writer.
func (c *Client) publishSnapshotTransferPage(ctx context.Context, state *snapshotTransfer, guard *snapshotApplyGuard, chunk *oversync.SnapshotChunkResponse) (snapshotTransfer, error) {
	next := *state
	var err error
	next.Rows, err = checkedAddInt64(state.Rows, int64(len(chunk.Rows)))
	if err != nil {
		return next, err
	}
	next.Bytes, err = checkedAddInt64(state.Bytes, chunk.ByteCount)
	if err != nil {
		return next, err
	}
	if next.Rows > state.Session.RowCount || next.Bytes > state.Session.ByteCount {
		return next, &snapshotEvidenceError{"snapshot page exceeds sealed totals"}
	}
	if !chunk.HasMore && (next.Rows != state.Session.RowCount || next.Bytes != state.Session.ByteCount) {
		return next, &snapshotEvidenceError{"snapshot final totals differ from sealed metadata"}
	}
	if chunk.HasMore && (next.Rows == state.Session.RowCount || next.Bytes == state.Session.ByteCount) {
		return next, &snapshotEvidenceError{"snapshot has_more after sealed totals"}
	}
	err = c.stageSnapshotChunkWithFinalizer(ctx, chunk, state.Rows, func(ctx context.Context, tx *immediateTx) error {
		if err := c.validateFinalSnapshotApplyGate(ctx, tx, guard, &state.Session); err != nil {
			return err
		}
		if !chunk.HasMore {
			if err := c.verifySnapshotStageHash(ctx, tx, &next.Session, next.Source); err != nil {
				return err
			}
			next.Complete = true
		}
		return c.persistSnapshotTransfer(ctx, tx, &next)
	})
	return next, err
}

func (c *Client) verifySnapshotStageHash(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession, source string) error {
	if c.discovery == nil {
		return fmt.Errorf("snapshot hash requires admitted generation")
	}
	var rows, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(charge),0) FROM _sync_snapshot_stage WHERE snapshot_id=?`, session.SnapshotID).Scan(&rows, &bytes); err != nil {
		return err
	}
	if rows != session.RowCount {
		return &snapshotEvidenceError{"stage membership mismatch"}
	}
	work := saturatingWorkAdd(saturatingWorkMultiply(rows, 256), saturatingWorkMultiply(bytes, 2))
	if err := c.reserveReadySyncWork(ctx, "snapshot-hash", work); err != nil {
		return err
	}
	generation := c.discovery.SchemaGeneration
	h, err := oversync.NewSnapshotHashWriter(source, generation, session.SnapshotID, session.SnapshotBundleSeq, session.Recovery)
	if err != nil {
		return err
	}
	for ordinal := int64(1); ordinal <= session.RowCount; ordinal++ {
		if err := c.hashSnapshotStageRow(ctx, tx, session, ordinal, h); err != nil {
			return err
		}
	}
	actual, err := h.Sum(session.RowCount, session.ByteCount)
	if err != nil {
		return &snapshotEvidenceError{"sealed logical totals mismatch"}
	}
	if actual != session.SnapshotHash {
		return &snapshotEvidenceError{"hash mismatch"}
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM _sync_snapshot_stage WHERE snapshot_id=?`, session.SnapshotID).Scan(&rows); err != nil {
		return err
	}
	if rows != session.RowCount {
		return &snapshotEvidenceError{"stage membership mismatch"}
	}
	return nil
}

// Read and hash one admitted staged row; payload copies never span rows.
func (c *Client) hashSnapshotStageRow(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession, ordinal int64, h *oversync.SnapshotHashWriter) error {
	generation := c.discovery.SchemaGeneration
	var size, actual int64
	err := tx.QueryRowContext(ctx, `SELECT charge,length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB))+length(CAST(payload AS BLOB)) FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?`, session.SnapshotID, ordinal).Scan(&size, &actual)
	if errors.Is(err, sql.ErrNoRows) {
		return &snapshotEvidenceError{"missing staged row"}
	}
	if err != nil {
		return err
	}
	if actual > size {
		return &snapshotEvidenceError{"staged charge mismatch"}
	}
	if err := c.checkAggregateWorkspace(ctx, tx, "snapshot-hash", nil, nil, workspaceCharge{16, 4096 + 16*size}); err != nil {
		return err
	}
	var schema, table, key, payload string
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT schema_name,table_name,key_json,row_version,payload FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?`, session.SnapshotID, ordinal).Scan(&schema, &table, &key, &version, &payload); err != nil {
		return err
	}

	found := -1
	for index, descriptor := range generation.Tables {
		if descriptor.Schema == schema && descriptor.Table == table {
			found = index
			break
		}
	}
	if found < 0 {
		return &snapshotEvidenceError{"snapshot table outside admitted generation"}
	}
	values, err := c.snapshotHashKeyValues(tx, table, key, generation.Tables[found].SyncKeyColumns)
	if err != nil {
		return err
	}
	if version > session.SnapshotBundleSeq {
		return &snapshotEvidenceError{"snapshot row newer than sealed boundary"}
	}
	if err := h.Add(found, values, version, json.RawMessage(payload)); err != nil {
		return &snapshotEvidenceError{"invalid staged row"}
	}
	return nil
}

// Decode stored evidence with pure validation; metadata/SQLite errors retain
// their own classification instead of becoming a recovery trigger.
func (c *Client) snapshotHashKeyValues(tx tableInfoQueryer, table, key string, columns []string) ([]any, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(key), &fields) != nil || len(columns) != 1 || len(fields) != 1 {
		return nil, &snapshotEvidenceError{"invalid staged key"}
	}
	var local string
	if json.Unmarshal(fields[strings.ToLower(columns[0])], &local) != nil {
		return nil, &snapshotEvidenceError{"invalid staged key"}
	}
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return nil, err
	}
	if info.PrimaryKeyIsBlob {
		raw, err := decodeUUIDBytesFromString(local)
		if err != nil {
			return nil, &snapshotEvidenceError{"invalid staged UUID key"}
		}
		local = fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
	}
	return []any{local}, nil
}
