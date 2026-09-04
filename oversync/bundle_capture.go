// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

const (
	registeredTableCaptureTriggerName   = "oversync_bundle_capture_row"
	registeredTableOwnerGuardTrigger    = "oversync_bundle_owner_guard"
	registeredTableTruncateGuardTrigger = "oversync_registered_truncate_guard"
)

type registeredTableTriggerTarget struct {
	schemaName string
	tableName  string
}

// BundleSource identifies one server-side committed bundle source.
type BundleSource struct {
	SourceID             string
	SourceBundleID       int64
	PlannedRowCount      *int64
	CanonicalRequestHash string
}

type capturedBundleEvent struct {
	ordinal  int64
	userPK   int64
	tableID  int32
	opCode   int16
	keyBytes []byte
	payload  []byte
}

type normalizedBundleRow struct {
	firstOrdinal int64
	tableID      int32
	keyBytes     []byte
	opCode       int16
	payloadDB    []byte
}

type committedBundleStorageRow struct {
	tableID     int32
	keyBytes    []byte
	opCode      int16
	payloadWire []byte
}

type bundleAccumulator struct {
	firstOrdinal int64
	firstOpCode  int16
	lastOpCode   int16
	lastPayload  []byte
}

func quoteSQLLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func (s *SyncService) installRegisteredTableCaptureTriggersInTx(ctx context.Context, tx pgx.Tx) error {
	if s == nil {
		return nil
	}
	return s.installRegisteredTableCaptureTriggersForTablesInTx(ctx, tx, s.activeRegisteredTableDeclarations())
}

func (s *SyncService) installRegisteredTableCaptureTriggersForTablesInTx(
	ctx context.Context,
	tx pgx.Tx,
	tables []RegisteredTable,
) error {
	for tableIndex, table := range tables {
		keyColumns := table.normalizedSyncKeyColumns()
		if len(keyColumns) != 1 {
			return fmt.Errorf("registered table %s requires exactly one sync key column to install capture trigger", table.normalizedKey())
		}
		info, ok := s.registeredTableInfo[table.normalizedKey()]
		if !ok {
			return fmt.Errorf("registered table %s is missing runtime metadata for trigger installation", table.normalizedKey())
		}

		tableIdent := pgx.Identifier{table.normalizedSchema(), table.normalizedTable()}.Sanitize()
		truncateTargets, err := loadRegisteredTableTriggerTargets(ctx, tx, table)
		if err != nil {
			return err
		}
		for targetIndex, target := range truncateTargets {
			targetIdent := pgx.Identifier{target.schemaName, target.tableName}.Sanitize()
			if err := rejectReservedRegisteredTrigger(ctx, tx, target.schemaName, target.tableName, registeredTableTruncateGuardTrigger); err != nil {
				return err
			}
			stmt := fmt.Sprintf(
				`CREATE TRIGGER %s BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION sync.reject_registered_table_truncate(%s, %s)`,
				registeredTableTruncateGuardTrigger,
				targetIdent,
				quoteSQLLiteral(table.normalizedSchema()),
				quoteSQLLiteral(table.normalizedTable()),
			)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("create truncate guard trigger for %s on %s.%s: %w", table.normalizedKey(), target.schemaName, target.tableName, err)
			}
			if err := s.runRegisteredTableCatalogMutationHook("after_truncate_trigger", targetIndex); err != nil {
				return err
			}
		}

		if err := rejectReservedRegisteredTrigger(ctx, tx, table.Schema, table.Table, registeredTableOwnerGuardTrigger, registeredTableCaptureTriggerName); err != nil {
			return err
		}

		stmt := fmt.Sprintf(
			`CREATE TRIGGER %s BEFORE INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.enforce_registered_row_owner()`,
			registeredTableOwnerGuardTrigger,
			tableIdent,
		)
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create owner guard trigger for %s: %w", table.normalizedKey(), err)
		}
		if err := s.runRegisteredTableCatalogMutationHook("after_owner_trigger", tableIndex); err != nil {
			return err
		}

		triggerArguments := []string{
			quoteSQLLiteral(keyColumns[0]),
			quoteSQLLiteral(strconv.Itoa(int(info.syncKeyKind))),
			quoteSQLLiteral(strconv.Itoa(int(info.tableID))),
		}
		for _, columnName := range databaseJSONColumnNames(s.columnTypesForTable(table.normalizedSchema(), table.normalizedTable())) {
			triggerArguments = append(triggerArguments, quoteSQLLiteral(columnName))
		}
		stmt = fmt.Sprintf(
			`CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.capture_registered_row_change(%s)`,
			registeredTableCaptureTriggerName,
			tableIdent,
			strings.Join(triggerArguments, ", "),
		)
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create capture trigger for %s: %w", table.normalizedKey(), err)
		}
		if err := s.runRegisteredTableCatalogMutationHook("after_capture_trigger", tableIndex); err != nil {
			return err
		}
	}
	return nil
}

func validateNoReservedRegisteredTriggersForTables(
	ctx context.Context,
	tx pgx.Tx,
	tables []RegisteredTable,
) error {
	for _, table := range tables {
		targets, err := loadRegisteredTableTriggerTargets(ctx, tx, table)
		if err != nil {
			return err
		}
		for _, target := range targets {
			if err := rejectReservedRegisteredTrigger(ctx, tx, target.schemaName, target.tableName, registeredTableTruncateGuardTrigger); err != nil {
				return err
			}
		}
		if err := rejectReservedRegisteredTrigger(ctx, tx, table.Schema, table.Table, registeredTableOwnerGuardTrigger, registeredTableCaptureTriggerName); err != nil {
			return err
		}
	}
	return nil
}

func rejectReservedRegisteredTrigger(
	ctx context.Context,
	tx pgx.Tx,
	schemaName string,
	tableName string,
	triggerNames ...string,
) error {
	var triggerName string
	err := tx.QueryRow(ctx, `
		SELECT trigger.tgname
		FROM pg_trigger AS trigger
		JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1
		  AND relation.relname = $2
		  AND trigger.tgname = ANY($3::text[])
		  AND NOT trigger.tgisinternal
		ORDER BY trigger.tgname
		LIMIT 1
	`, schemaName, tableName, triggerNames).Scan(&triggerName)
	if err == nil {
		return unsupportedSchemaf(
			"registered table %s.%s already uses reserved managed trigger name %s",
			schemaName,
			tableName,
			triggerName,
		)
	}
	if err != pgx.ErrNoRows {
		return fmt.Errorf("inspect reserved managed triggers on %s.%s: %w", schemaName, tableName, err)
	}
	return nil
}

func loadRegisteredTableTriggerTargets(ctx context.Context, tx pgx.Tx, table RegisteredTable) ([]registeredTableTriggerTarget, error) {
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE relation_tree AS (
			SELECT relation.oid
			FROM pg_class AS relation
			JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relname = $2

			UNION ALL

			SELECT inheritance.inhrelid
			FROM pg_inherits AS inheritance
			JOIN relation_tree AS parent ON parent.oid = inheritance.inhparent
		)
		SELECT namespace.nspname, relation.relname
		FROM relation_tree AS tree
		JOIN pg_class AS relation ON relation.oid = tree.oid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		ORDER BY namespace.nspname, relation.relname
	`, table.normalizedSchema(), table.normalizedTable())
	if err != nil {
		return nil, fmt.Errorf("discover truncate guard targets for %s: %w", table.normalizedKey(), err)
	}
	defer rows.Close()

	targets := make([]registeredTableTriggerTarget, 0, 1)
	for rows.Next() {
		var target registeredTableTriggerTarget
		if err := rows.Scan(&target.schemaName, &target.tableName); err != nil {
			return nil, fmt.Errorf("scan truncate guard target for %s: %w", table.normalizedKey(), err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate truncate guard targets for %s: %w", table.normalizedKey(), err)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("registered table %s is missing while installing truncate guard", table.normalizedKey())
	}
	return targets, nil
}

func reserveUserBundleSeq(ctx context.Context, tx pgx.Tx, userPK int64) (int64, error) {
	var bundleSeq int64
	if err := tx.QueryRow(ctx, `
		UPDATE sync.user_state
		SET next_bundle_seq = next_bundle_seq + 1
		WHERE user_pk = $1
		RETURNING next_bundle_seq - 1
	`, userPK).Scan(&bundleSeq); err != nil {
		return 0, fmt.Errorf("reserve user bundle_seq: %w", err)
	}
	return bundleSeq, nil
}

func (s *SyncService) WithinSyncBundle(
	ctx context.Context,
	actor Actor,
	source BundleSource,
	opts RetryableBundleWriteOptions,
	fn RetryableDatabaseWrite,
) (err error) {
	if err := rejectRetryableCallbackContext(ctx, "WithinSyncBundle"); err != nil {
		return err
	}
	if err := s.validateClientActor(actor, false); err != nil {
		return err
	}
	if err := sourceid.Validate(source.SourceID); err != nil {
		return fmt.Errorf("bundle source_id is invalid: %w", err)
	}
	if s.isReservedServerSourceID(source.SourceID) {
		return fmt.Errorf("bundle source_id is invalid: %w", sourceid.ErrInvalid)
	}
	if source.SourceBundleID <= 0 {
		return &RetryableWriteInvalidError{Message: "bundle source_bundle_id must be > 0"}
	}
	if err := validateCanonicalRequestHash(source.CanonicalRequestHash); err != nil {
		return err
	}
	if fn == nil {
		return &RetryableWriteInvalidError{Message: "bundle callback is required"}
	}
	writeOpts := RetryableWriteOptions{
		OperationID:         withinSyncBundleOperationID(actor.UserID, source.SourceID, source.SourceBundleID),
		OperationHash:       opts.OperationHash,
		OperationValidUntil: opts.OperationValidUntil,
	}
	if err := validateRetryableWriteOptions(writeOpts); err != nil {
		return err
	}
	emptyEffects, err := s.canonicalizeEffectSet(nil)
	if err != nil {
		return err
	}

	done, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return err
	}
	defer releaseConn()

	_, err = runRetryableWriteTransaction(ctx, conn, func(tx pgx.Tx) (struct{}, error) {
		return struct{}{}, s.withinSyncBundleAttempt(ctx, tx, actor, source, writeOpts, emptyEffects, fn)
	})
	return err
}

func (s *SyncService) withinSyncBundleAttempt(
	ctx context.Context,
	tx pgx.Tx,
	actor Actor,
	source BundleSource,
	opts RetryableWriteOptions,
	emptyEffects canonicalEffectSet,
	fn RetryableDatabaseWrite,
) error {
	if err := ensureScopeStateExistsWithExec(ctx, tx, actor.UserID); err != nil {
		return err
	}
	userPK, err := lookupUserPK(ctx, tx, actor.UserID)
	if err != nil {
		return err
	}
	explicitSourceBundleID := source.SourceBundleID
	decision, err := acquireScopeWriteReceipt(ctx, tx, scopeWriteReceiptSpec{
		userPK:                 userPK,
		operationID:            opts.OperationID,
		operationHash:          opts.OperationHash,
		requiredEffectsHash:    emptyEffects.hash,
		forbiddenEffectsHash:   emptyEffects.hash,
		writerID:               source.SourceID,
		explicitSourceBundleID: &explicitSourceBundleID,
		operationValidUntil:    opts.OperationValidUntil,
	})
	if err != nil {
		return err
	}
	if decision.replay != nil {
		if decision.replay.Bundle.CanonicalRequestHash != source.CanonicalRequestHash {
			return &OperationReplayChangedError{}
		}
		return nil
	}
	scopeState, err := loadScopeStateForUpdate(ctx, tx, actor.UserID)
	if err != nil {
		return err
	}
	scopeState, err = expireInitializationLeaseIfNeeded(ctx, tx, scopeState)
	if err != nil {
		return err
	}
	switch scopeState.State {
	case scopeStateInitialized:
	case scopeStateInitializing:
		return &ScopeInitializingError{UserID: actor.UserID, LeaseExpiresAt: scopeState.LeaseExpiresAt}
	default:
		return &ScopeUninitializedError{UserID: actor.UserID}
	}
	if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
		return fmt.Errorf("defer bundle constraints: %w", err)
	}
	expectedSourceBundleID, _, err := loadNextExpectedSourceBundleIDForUpdate(ctx, tx, scopeState.UserPK, actor.UserID, source.SourceID)
	if err != nil {
		return err
	}
	switch {
	case source.SourceBundleID < expectedSourceBundleID:
		return &OperationHistoryUnavailableError{}
	case source.SourceBundleID > expectedSourceBundleID:
		return &SourceSequenceOutOfOrderError{
			UserID:   actor.UserID,
			SourceID: source.SourceID,
			Expected: expectedSourceBundleID,
			Actual:   source.SourceBundleID,
		}
	}
	if err := setBundleTxContext(ctx, tx, bundleTxContext{
		UserID:         actor.UserID,
		UserPK:         scopeState.UserPK,
		SourceID:       source.SourceID,
		SourceBundleID: source.SourceBundleID,
	}); err != nil {
		return err
	}

	callbackCtx, capability, err := newRestrictedDatabaseWriteTx(ctx, tx, actor.UserID)
	if err != nil {
		return err
	}
	if err := fn(callbackCtx, capability); err != nil {
		return err
	}
	if err := finishDeferredBundleEffects(ctx, tx); err != nil {
		return err
	}
	bundle, err := s.finalizeCapturedBundle(ctx, tx, actor, scopeState.UserPK, source)
	if err != nil {
		return err
	}
	if bundle == nil {
		return &ScopeWriteNoCapturedChangesError{ScopeID: actor.UserID, WriterID: source.SourceID}
	}
	if err := activateSourceState(ctx, tx, scopeState.UserPK, actor.UserID, source.SourceID, source.SourceBundleID); err != nil {
		return err
	}
	if err := s.applyRetentionPolicyForUser(ctx, tx, scopeState.UserPK); err != nil {
		return err
	}
	result := &ScopeWriteResult{Bundle: committedBundleRef(bundle)}
	if err := completeScopeWriteReceipt(ctx, tx, scopeState.UserPK, opts.OperationID, result); err != nil {
		return err
	}
	return nil
}

// finishDeferredBundleEffects forces application-owned deferred constraint
// triggers to run while the transaction can still capture, validate, and bound
// every registered-table effect they produce.
func finishDeferredBundleEffects(ctx context.Context, tx pgx.Tx) error {
	rows, err := tx.Query(ctx, `
		SELECT n.nspname, c.conname
		FROM pg_catalog.pg_constraint AS c
		JOIN pg_catalog.pg_namespace AS n ON n.oid=c.connamespace
		WHERE c.condeferrable
		  AND n.nspname <> 'sync'
		ORDER BY n.nspname, c.conname
	`)
	if err != nil {
		return fmt.Errorf("list deferred application constraints: %w", err)
	}
	var constraints []string
	for rows.Next() {
		var schemaName, constraintName string
		if err := rows.Scan(&schemaName, &constraintName); err != nil {
			rows.Close()
			return fmt.Errorf("scan deferred application constraint: %w", err)
		}
		constraints = append(constraints, pgx.Identifier{schemaName, constraintName}.Sanitize())
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate deferred application constraints: %w", err)
	}
	rows.Close()
	if len(constraints) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `SET CONSTRAINTS `+strings.Join(constraints, ",")+` IMMEDIATE`); err != nil {
		return fmt.Errorf("finish deferred bundle effects: %w", err)
	}
	return nil
}

func (s *SyncService) finalizeCapturedBundle(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, source BundleSource) (*Bundle, error) {
	return s.finalizeCapturedBundleWithLimit(ctx, tx, actor, userPK, source, 0)
}

func (s *SyncService) finalizeCapturedBundleWithLimit(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, source BundleSource, byteLimit int) (*Bundle, error) {
	if byteLimit == 0 {
		byteLimit = s.config.MaxBytesPerBundle
	}

	var txid int64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txid); err != nil {
		return nil, fmt.Errorf("read current txid for bundle capture: %w", err)
	}

	hasRows, err := hasNormalizedCapturedBundleRows(ctx, tx, txid, userPK)
	if err != nil {
		return nil, err
	}
	if !hasRows {
		if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_capture_stage WHERE txid = $1 AND user_pk = $2`, txid, userPK); err != nil {
			return nil, fmt.Errorf("clear empty captured bundle stage rows: %w", err)
		}
		return nil, nil
	}

	bundleSeq, err := reserveUserBundleSeq(ctx, tx, userPK)
	if err != nil {
		return nil, err
	}
	queryRows, err := s.queryNormalizedCapturedBundleRows(ctx, tx, txid, userPK)
	if err != nil {
		return nil, err
	}
	defer queryRows.Close()
	if !queryRows.Next() {
		if err := queryRows.Err(); err != nil {
			return nil, fmt.Errorf("iterate normalized captured bundle rows: %w", err)
		}
		return nil, projectionError(GenerationProjectionIntegrityError, "capture", "captured bundle changed during finalization")
	}

	storageRows := make([]committedBundleStorageRow, 0, min(s.config.MaxRowsPerBundle, 1024))
	bundleRows := make([]BundleRow, 0, min(s.config.MaxRowsPerBundle, 1024))
	hasher := protocolhash.NewCommittedBundleHasher()
	for {
		if len(bundleRows) >= s.config.MaxRowsPerBundle {
			return nil, projectionError(BundleLimitExceeded, "capture", "committed bundle exceeds configured limits")
		}
		var row normalizedBundleRow
		var firstOpCode int16
		if err := queryRows.Scan(&row.firstOrdinal, &row.tableID, &row.keyBytes, &firstOpCode, &row.opCode, &row.payloadDB); err != nil {
			return nil, fmt.Errorf("scan normalized captured bundle row: %w", err)
		}
		if row.opCode == opCodeDelete {
			row.payloadDB = nil
		} else if firstOpCode == opCodeInsert {
			row.opCode = opCodeInsert
		} else {
			row.opCode = opCodeUpdate
		}
		info, err := s.tableInfoForID(row.tableID)
		if err != nil {
			return nil, err
		}
		key, err := wireSyncKeyFromBytes(info, row.keyBytes)
		if err != nil {
			return nil, fmt.Errorf("decode bundle row key for table_id %d: %w", row.tableID, err)
		}
		op, err := opStringFromCode(row.opCode)
		if err != nil {
			return nil, err
		}
		bundleRow := BundleRow{
			Schema:     info.schemaName,
			Table:      info.tableName,
			Key:        key,
			Op:         op,
			RowVersion: bundleSeq,
		}
		var payloadWire []byte
		if row.opCode != opCodeDelete {
			payloadWire, err = s.canonicalizeWirePayload(info.schemaName, info.tableName, row.payloadDB)
			if err != nil {
				return nil, fmt.Errorf("canonicalize bundle row payload for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			bundleRow.Payload = payloadWire
		}
		if err := hasher.Add(protocolhash.BundleRow{
			Schema: bundleRow.Schema, Table: bundleRow.Table, Key: bundleRow.Key,
			Op: bundleRow.Op, RowVersion: bundleRow.RowVersion, Payload: bundleRow.Payload,
		}); err != nil {
			return nil, fmt.Errorf("compute committed bundle hash: %w", err)
		}
		if hasher.CurrentByteCount() > int64(byteLimit) {
			return nil, projectionError(BundleLimitExceeded, "capture", "committed bundle exceeds configured limits")
		}
		storageRows = append(storageRows, committedBundleStorageRow{
			tableID:     row.tableID,
			keyBytes:    append([]byte(nil), row.keyBytes...),
			opCode:      row.opCode,
			payloadWire: append([]byte(nil), payloadWire...),
		})
		bundleRows = append(bundleRows, bundleRow)
		if !queryRows.Next() {
			break
		}
	}
	if err := queryRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate normalized captured bundle rows: %w", err)
	}
	queryRows.Close()

	bundleHashText, byteCount, err := hasher.Sum()
	if err != nil {
		return nil, fmt.Errorf("compute committed bundle hash: %w", err)
	}
	bundleHash, err := hex.DecodeString(bundleHashText)
	if err != nil {
		return nil, fmt.Errorf("decode committed bundle hash: %w", err)
	}
	candidate := &Bundle{
		BundleSeq:            bundleSeq,
		SourceID:             source.SourceID,
		SourceBundleID:       source.SourceBundleID,
		RowCount:             int64(len(bundleRows)),
		ByteCount:            byteCount,
		BundleHash:           renderBundleHash(bundleHash),
		CanonicalRequestHash: source.CanonicalRequestHash,
		Rows:                 bundleRows,
	}
	if err := s.validateFutureBundleRepresentations(candidate); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_pk, bundle_seq, source_id, source_bundle_id, planned_row_count, row_count, byte_count, bundle_hash, canonical_request_hash, committed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
	`, userPK, bundleSeq, source.SourceID, source.SourceBundleID, source.PlannedRowCount, len(bundleRows), byteCount, bundleHash, source.CanonicalRequestHash); err != nil {
		return nil, fmt.Errorf("insert bundle_log row: %w", err)
	}
	if err := s.emitBundleChangeNotify(ctx, tx, bundleChangeNotification{
		UserPK:         userPK,
		BundleSeq:      bundleSeq,
		SourceID:       source.SourceID,
		SourceBundleID: source.SourceBundleID,
	}); err != nil {
		return nil, err
	}

	if err := persistCommittedBundleRows(ctx, tx, userPK, bundleSeq, storageRows); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_capture_stage WHERE txid = $1 AND user_pk = $2`, txid, userPK); err != nil {
		return nil, fmt.Errorf("delete captured bundle stage rows: %w", err)
	}
	return candidate, nil
}

func hasNormalizedCapturedBundleRows(ctx context.Context, tx pgx.Tx, txid, userPK int64) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		WITH normalized AS (
			SELECT
				first_value(op_code) OVER complete AS first_op_code,
				last_value(op_code) OVER complete AS last_op_code,
				row_number() OVER (PARTITION BY table_id, key_bytes ORDER BY capture_ordinal DESC) AS reverse_ordinal
			FROM sync.bundle_capture_stage
			WHERE txid = $1 AND user_pk = $2
			WINDOW complete AS (
				PARTITION BY table_id, key_bytes
				ORDER BY capture_ordinal
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			)
		)
		SELECT EXISTS (
			SELECT 1 FROM normalized
			WHERE reverse_ordinal = 1
			  AND NOT (first_op_code = $3 AND last_op_code = $4)
		)
	`, txid, userPK, opCodeInsert, opCodeDelete).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check normalized captured bundle rows: %w", err)
	}
	return exists, nil
}

func (s *SyncService) queryNormalizedCapturedBundleRows(ctx context.Context, tx pgx.Tx, txid, userPK int64) (pgx.Rows, error) {
	tableIDs := make([]int, 0, len(s.registeredTableByID))
	physicalLimitByTableID := make(map[int32]int64, len(s.registeredTableByID))
	for tableID, info := range s.registeredTableByID {
		columnTypes := s.columnTypesForTable(info.schemaName, info.tableName)
		tableLimit, limitErr := physicalDatabasePayloadByteLimitForColumnTypes(int64(s.config.MaxBytesPerBundle), columnTypes)
		if limitErr != nil {
			return nil, limitErr
		}
		tableIDs = append(tableIDs, int(tableID))
		physicalLimitByTableID[tableID] = tableLimit
	}
	sort.Ints(tableIDs)
	var physicalLimitCase strings.Builder
	if len(tableIDs) == 0 {
		physicalLimitCase.WriteByte('0')
	} else {
		physicalLimitCase.WriteString("CASE table_id")
		for _, rawTableID := range tableIDs {
			tableID := int32(rawTableID)
			fmt.Fprintf(&physicalLimitCase, " WHEN %d THEN %d", tableID, physicalLimitByTableID[tableID])
		}
		physicalLimitCase.WriteString(" ELSE 0 END")
	}
	var oversized bool
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM sync.bundle_capture_stage
			WHERE txid = $1
			  AND user_pk = $2
			  AND (
				octet_length(key_bytes) > $3
				OR COALESCE(octet_length(payload_db::text), 0) > (%s)
			  )
		)`, physicalLimitCase.String()), txid, userPK, s.config.MaxBytesPerBundle).Scan(&oversized); err != nil {
		return nil, fmt.Errorf("check captured bundle input limits: %w", err)
	}
	if oversized {
		return nil, projectionError(BundleLimitExceeded, "capture", "committed bundle exceeds configured limits")
	}
	queryRows, err := tx.Query(ctx, `
		WITH normalized AS (
			SELECT
				first_value(capture_ordinal) OVER complete AS first_ordinal,
				table_id,
				key_bytes,
				first_value(op_code) OVER complete AS first_op_code,
				last_value(op_code) OVER complete AS last_op_code,
				last_value(payload_db) OVER complete AS last_payload_db,
				row_number() OVER (
					PARTITION BY table_id, key_bytes ORDER BY capture_ordinal DESC
				) AS reverse_ordinal
			FROM sync.bundle_capture_stage
			WHERE txid = $1 AND user_pk = $2
			WINDOW complete AS (
				PARTITION BY table_id, key_bytes
				ORDER BY capture_ordinal
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			)
		)
		SELECT first_ordinal, table_id, key_bytes, first_op_code, last_op_code, last_payload_db
		FROM normalized
		WHERE reverse_ordinal = 1
		  AND NOT (first_op_code = $3 AND last_op_code = $4)
		ORDER BY first_ordinal, table_id, key_bytes
		LIMIT $5
	`, txid, userPK, opCodeInsert, opCodeDelete, s.config.MaxRowsPerBundle+1)
	if err != nil {
		return nil, fmt.Errorf("query normalized captured bundle rows: %w", err)
	}
	return queryRows, nil
}

func renderBundleHash(bundleHash []byte) string {
	return hex.EncodeToString(bundleHash)
}

func computeCommittedBundleHash(rows []BundleRow) ([]byte, int64, error) {
	logicalRows := make([]protocolhash.BundleRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.BundleRow{
			Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
			RowVersion: row.RowVersion, Payload: row.Payload,
		})
	}
	hash, byteCount, err := protocolhash.CommittedBundle(logicalRows)
	if err != nil {
		return nil, 0, err
	}
	decoded, err := hex.DecodeString(hash)
	if err != nil {
		return nil, 0, fmt.Errorf("decode committed bundle hash: %w", err)
	}
	return decoded, byteCount, nil
}

func loadCapturedBundleEvents(ctx context.Context, tx pgx.Tx, txid int64, userPK int64) ([]capturedBundleEvent, error) {
	rows, err := tx.Query(ctx, `
		SELECT capture_ordinal, user_pk, table_id, op_code, key_bytes, payload_db
		FROM sync.bundle_capture_stage
		WHERE txid = $1
		  AND user_pk = $2
		ORDER BY capture_ordinal
	`, txid, userPK)
	if err != nil {
		return nil, fmt.Errorf("query captured bundle stage rows: %w", err)
	}
	defer rows.Close()

	events := make([]capturedBundleEvent, 0)
	for rows.Next() {
		var event capturedBundleEvent
		var payload []byte
		if err := rows.Scan(&event.ordinal, &event.userPK, &event.tableID, &event.opCode, &event.keyBytes, &payload); err != nil {
			return nil, fmt.Errorf("scan captured bundle stage row: %w", err)
		}
		if payload != nil {
			// Preserve PostgreSQL's exact number tokens until schema-typed BIGINT
			// and NUMERIC fields have been converted to wire strings.
			event.payload = append([]byte(nil), payload...)
		}
		event.keyBytes = append([]byte(nil), event.keyBytes...)
		events = append(events, event)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate captured bundle stage rows: %w", rows.Err())
	}
	return events, nil
}

func normalizeCapturedBundleEvents(events []capturedBundleEvent) []normalizedBundleRow {
	accumulators := make(map[string]*bundleAccumulator, len(events))
	for _, event := range events {
		key := string(appendInt32BigEndian(append([]byte(nil), event.keyBytes...), event.tableID))
		acc := accumulators[key]
		if acc == nil {
			acc = &bundleAccumulator{
				firstOrdinal: event.ordinal,
				firstOpCode:  event.opCode,
			}
			accumulators[key] = acc
		}
		acc.lastOpCode = event.opCode
		if event.payload != nil {
			acc.lastPayload = append(acc.lastPayload[:0], event.payload...)
		} else {
			acc.lastPayload = nil
		}
	}

	rows := make([]normalizedBundleRow, 0, len(accumulators))
	for _, event := range events {
		key := string(appendInt32BigEndian(append([]byte(nil), event.keyBytes...), event.tableID))
		acc, ok := accumulators[key]
		if !ok {
			continue
		}
		delete(accumulators, key)

		row := normalizedBundleRow{
			firstOrdinal: acc.firstOrdinal,
			tableID:      event.tableID,
			keyBytes:     append([]byte(nil), event.keyBytes...),
		}

		switch acc.lastOpCode {
		case opCodeDelete:
			if acc.firstOpCode == opCodeInsert {
				continue
			}
			row.opCode = opCodeDelete
		default:
			if acc.firstOpCode == opCodeInsert {
				row.opCode = opCodeInsert
			} else {
				row.opCode = opCodeUpdate
			}
			row.payloadDB = append([]byte(nil), acc.lastPayload...)
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].firstOrdinal == rows[j].firstOrdinal {
			if rows[i].tableID == rows[j].tableID {
				return bytes.Compare(rows[i].keyBytes, rows[j].keyBytes) < 0
			}
			return rows[i].tableID < rows[j].tableID
		}
		return rows[i].firstOrdinal < rows[j].firstOrdinal
	})
	return rows
}

func persistCommittedBundleRows(ctx context.Context, tx pgx.Tx, userPK, bundleSeq int64, rows []committedBundleStorageRow) error {
	rowOrdinals := make([]int64, len(rows))
	tableIDs := make([]int32, len(rows))
	keyBytes := make([][]byte, len(rows))
	opCodes := make([]int16, len(rows))
	hasPayload := make([]bool, len(rows))
	payloadTexts := make([]string, len(rows))
	deletedFlags := make([]bool, len(rows))

	for i, row := range rows {
		rowOrdinals[i] = int64(i + 1)
		tableIDs[i] = row.tableID
		keyBytes[i] = append([]byte(nil), row.keyBytes...)
		opCodes[i] = row.opCode
		deletedFlags[i] = row.opCode == opCodeDelete
		if row.opCode != opCodeDelete {
			hasPayload[i] = true
			payloadTexts[i] = string(row.payloadWire)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_rows (
			user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire
		)
		SELECT
			$1,
			$2,
			rows.row_ordinal,
			rows.table_id,
			rows.key_bytes,
			rows.op_code,
			CASE WHEN rows.has_payload THEN rows.payload_text::json ELSE NULL END
		FROM unnest(
			$3::int8[],
			$4::int4[],
			$5::bytea[],
			$6::int2[],
			$7::bool[],
			$8::text[]
		) AS rows(row_ordinal, table_id, key_bytes, op_code, has_payload, payload_text)
	`, userPK, bundleSeq, rowOrdinals, tableIDs, keyBytes, opCodes, hasPayload, payloadTexts); err != nil {
		return fmt.Errorf("bulk insert bundle_rows: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.row_state (
			user_pk, table_id, key_bytes, bundle_seq, deleted
		)
		SELECT
			$1,
			rows.table_id,
			rows.key_bytes,
			$2,
			rows.deleted
		FROM unnest(
			$3::int4[],
			$4::bytea[],
			$5::bool[]
		) AS rows(table_id, key_bytes, deleted)
		ON CONFLICT (user_pk, table_id, key_bytes) DO UPDATE
		SET bundle_seq = EXCLUDED.bundle_seq,
			deleted = EXCLUDED.deleted
	`, userPK, bundleSeq, tableIDs, keyBytes, deletedFlags); err != nil {
		return fmt.Errorf("bulk upsert row_state: %w", err)
	}

	return nil
}
