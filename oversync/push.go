// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

type PushValidationError struct {
	Message string
}

func (e *PushValidationError) Error() string {
	return e.Message
}

type PushConflictError struct {
	Message                  string
	Conflict                 *PushConflictDetails
	historyProjectionRelease func()
}

func (e *PushConflictError) Error() string {
	return e.Message
}

type pushPreparedRow struct {
	schema                  string
	table                   string
	tableID                 int32
	op                      string
	keyColumn               string
	keyType                 string
	keyString               string
	keyValue                any
	keyBytes                []byte
	baseRowVersion          int64
	payload                 []byte
	requestPayload          []byte
	payloadColumns          []string
	jsonPayloadTypes        map[string]string
	requiresIndividualApply bool
	inputOrder              int
}

type rowStateSnapshot struct {
	rowVersion int64
	deleted    bool
}

type indexedRowStateSnapshot struct {
	rowStateSnapshot
	found bool
}

func computeCanonicalPushRequestHash(rows []PushRequestRow) (string, error) {
	logicalRows := make([]protocolhash.PushRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.PushRow{
			Schema:         strings.ToLower(strings.TrimSpace(row.Schema)),
			Table:          strings.ToLower(strings.TrimSpace(row.Table)),
			Key:            row.Key,
			Op:             strings.ToUpper(strings.TrimSpace(row.Op)),
			BaseRowVersion: row.BaseRowVersion,
			Payload:        row.Payload,
		})
	}
	hash, _, err := protocolhash.PushRequest(logicalRows)
	return hash, err
}

func computeGenerationPinnedPushRequestHash(proof SchemaGenerationProof, rows []PushRequestRow) (string, error) {
	logicalRows := make([]protocolhash.PushRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.PushRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, BaseRowVersion: row.BaseRowVersion, Payload: row.Payload})
	}
	hash, _, err := protocolhash.GenerationPinnedPushRequest(canonicalSchemaGenerationID(proof.ID), proof.Fingerprint, logicalRows)
	if err != nil {
		return "", fmt.Errorf("failed to compute generation-pinned push request hash: %w", err)
	}
	return hash, nil
}

// OneShotPushRequest is the complete, bounded logical request behind the
// compact POST /push envelope.  It deliberately contains no staging or
// continuation state: a source bundle is either committed atomically or not
// committed at all.
type OneShotPushRequest struct {
	SourceBundleID       int64
	CanonicalRequestHash string
	InitializationID     string
	Rows                 []PushRequestRow
}

// OneShotPushResponse is the authoritative committed effect of a one-request
// push. Replays return the same generation-projected bundle as the original
// commit.
type OneShotPushResponse struct {
	Bundle                   *Bundle
	historyProjectionRelease func()
}

// PushOneShot validates, applies, captures, and commits exactly one complete
// source bundle in a single transaction. It is also the idempotency boundary:
// an already committed source tuple is returned only when its row count and
// generation-pinned request hash match the complete request.
func (s *SyncService) PushOneShot(ctx context.Context, actor Actor, proof SchemaGenerationProof, req *OneShotPushRequest) (_ *OneShotPushResponse, err error) {
	defer func() {
		if err != nil {
			err = sanitizePushError(s.classifyApplicationWriteError(err))
		}
	}()
	if err := rejectRetryableCallbackContext(ctx, "PushOneShot"); err != nil {
		return nil, err
	}
	if err := s.validateClientActor(actor, true); err != nil {
		return nil, err
	}
	if err := validateSchemaGenerationProof(proof); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, &PushInvalidError{Message: "push request is required"}
	}
	if req.SourceBundleID < 1 {
		return nil, &PushInvalidError{Message: "source_bundle_id must be positive"}
	}
	if len(req.Rows) == 0 {
		return nil, &PushInvalidError{Message: "rows must not be empty"}
	}
	if len(req.Rows) > s.config.MaxRowsPerBundle {
		return nil, projectionError(BundleLimitExceeded, "push", "push request exceeds configured bundle row limits")
	}

	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	releaseOperation := done
	defer releaseHistoryProjection(&releaseOperation)
	view, err := s.resolveRequestGeneration(ctx, actor, proof)
	if err != nil {
		return nil, err
	}
	selection, err := selectionFromResolvedGeneration(proof, view)
	if err != nil {
		return nil, err
	}
	preparedRows, err := s.preparePushRowsForGeneration(selection, req.Rows)
	if err != nil {
		return nil, err
	}
	recomputedRequestHash, err := computeGenerationPinnedPushRequestHash(proof, req.Rows)
	if err != nil {
		return nil, err
	}
	if recomputedRequestHash != req.CanonicalRequestHash {
		return nil, &PushInvalidError{Message: "canonical_request_hash does not match push rows"}
	}
	for _, row := range req.Rows {
		rowBytes, sizeErr := ExpandedRowDecodedBytes(row)
		if sizeErr != nil {
			return nil, &PushInvalidError{Message: sizeErr.Error()}
		}
		if rowBytes > s.maxBytesPerSnapshotRow() {
			return nil, &PushInvalidError{Message: "push row exceeds max_row_decoded_bytes"}
		}
	}
	requestBytes, err := compactPushRequestDecodedBytes(generationDescriptor(view.contract), *req)
	if err != nil {
		return nil, &PushInvalidError{Message: err.Error()}
	}
	if requestBytes > int64(s.config.MaxBytesPerBundle)+historyEnvelopeAllowance {
		return nil, &PushInvalidError{Message: "push request exceeds max_push_decoded_bytes"}
	}

	var releaseProjection func()
	defer releaseHistoryProjection(&releaseProjection)
	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var resp *OneShotPushResponse
	err = runRetryableOwnedTransaction(ctx, conn, func(tx pgx.Tx) error {
		releaseHistoryProjection(&releaseProjection)
		resp = nil
		if err := s.configureUploadTx(ctx, tx); err != nil {
			return err
		}
		var bindingUserPK int64
		if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1`, actor.UserID).Scan(&bindingUserPK); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return sourceGenerationError(SourceGenerationUnbound, proof.ID, 0, "", "source has not connected")
			}
			return fmt.Errorf("load source binding user: %w", err)
		}
		if _, err := requireExactSourceGenerationBindingStateInTx(ctx, tx, bindingUserPK, actor.SourceID, proof, false); err != nil {
			return err
		}
		releaseProjection, err = s.acquireHistoryProjection(ctx)
		if err != nil {
			return err
		}
		replay, err := s.replayOneShotPush(ctx, tx, bindingUserPK, actor, selection, req)
		if err != nil {
			return err
		}
		if replay != nil {
			resp = replay
			return nil
		}
		releaseHistoryProjection(&releaseProjection)

		// Only a genuinely new source tuple enters mutable admission. Locking,
		// current lifecycle checks, lease expiry, and sequence mutation all stay
		// below the committed-history gate.
		if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1 FOR UPDATE`, actor.UserID).Scan(&bindingUserPK); err != nil {
			return fmt.Errorf("lock source binding user: %w", err)
		}
		sourceState, err := requireExactSourceGenerationBindingStateInTx(ctx, tx, bindingUserPK, actor.SourceID, proof, true)
		if err != nil {
			return err
		}
		if req.SourceBundleID <= sourceState.MaxCommittedSourceBundleID {
			return &SourceTupleHistoryPrunedError{UserID: actor.UserID, SourceID: actor.SourceID, SourceBundleID: req.SourceBundleID, MaxCommittedSourceBundleIDHint: sourceState.MaxCommittedSourceBundleID}
		}
		if sourceState.State == sourceStateRetired {
			return &SourceRetiredError{UserID: actor.UserID, SourceID: actor.SourceID, ReplacedBySourceID: sourceState.ReplacedBySourceID}
		}
		if err := ensureScopeStateExistsWithExec(ctx, tx, actor.UserID); err != nil {
			return err
		}
		scopeState, err := loadScopeStateForUpdate(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}
		scopeState, err = expireInitializationLeaseIfNeeded(ctx, tx, scopeState)
		if err != nil {
			return err
		}

		initializationID := ""
		switch scopeState.State {
		case scopeStateUninitialized:
			if req.InitializationID != "" {
				return &InitializationExpiredError{Message: "initialization lease is no longer active"}
			}
			return &ScopeUninitializedError{UserID: actor.UserID}
		case scopeStateInitializing:
			if req.InitializationID == "" {
				return &PushInvalidError{Message: "initialization_id is required while the scope is initializing"}
			}
			refreshUntil := time.Now().UTC().Add(s.initializationLeaseTTL())
			leasedState, err := requireActiveInitializationLease(ctx, tx, actor.UserID, actor.SourceID, req.InitializationID, refreshUntil)
			if err != nil {
				return err
			}
			initializationID = leasedState.InitializationID
		case scopeStateInitialized:
			if req.InitializationID != "" {
				return &PushInvalidError{Message: "initialization_id must be absent once the scope is initialized"}
			}
		default:
			return fmt.Errorf("unexpected scope state %q", scopeState.State)
		}
		releaseProjection, err = s.acquireHistoryProjection(ctx)
		if err != nil {
			return err
		}

		expectedSourceBundleID := sourceState.MaxCommittedSourceBundleID + 1
		switch {
		case req.SourceBundleID < expectedSourceBundleID:
			return &SourceTupleHistoryPrunedError{UserID: actor.UserID, SourceID: actor.SourceID, SourceBundleID: req.SourceBundleID, MaxCommittedSourceBundleIDHint: sourceState.MaxCommittedSourceBundleID}
		case req.SourceBundleID > expectedSourceBundleID:
			return &SourceSequenceOutOfOrderError{UserID: actor.UserID, SourceID: actor.SourceID, Expected: expectedSourceBundleID, Actual: req.SourceBundleID}
		}

		if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
			return fmt.Errorf("defer push constraints: %w", err)
		}
		if initializationID != "" {
			if err := ensureUserStateBaselineWithExec(ctx, tx, actor.UserID); err != nil {
				return err
			}
		} else if err := ensureUserStatePresent(ctx, tx, actor.UserID); err != nil {
			return err
		}
		if err := preflightPushPayloadDestinations(ctx, tx, preparedRows); err != nil {
			return err
		}
		if err := setBundleTxContext(ctx, tx, bundleTxContext{UserID: actor.UserID, UserPK: scopeState.UserPK, SourceID: actor.SourceID, SourceBundleID: req.SourceBundleID, InitializationID: initializationID}); err != nil {
			return err
		}
		rowStates, err := loadRowStateSnapshots(ctx, tx, scopeState.UserPK, preparedRows)
		if err != nil {
			return err
		}
		for i, row := range preparedRows {
			var state *rowStateSnapshot
			if rowStates[i].found {
				state = &rowStates[i].rowStateSnapshot
			}
			if err := s.validatePushRowConflictForGeneration(ctx, tx, selection, actor.UserID, row, state); err != nil {
				return err
			}
		}
		for _, group := range batchPreparedPushRows(preparedRows) {
			if err := s.applyPreparedPushRowBatchForGeneration(ctx, tx, selection, actor.UserID, group); err != nil {
				return err
			}
		}
		if err := finishDeferredBundleEffects(ctx, tx); err != nil {
			return err
		}
		plannedRowCount := int64(len(preparedRows))
		bundle, err := s.finalizeCapturedBundle(ctx, tx, actor, scopeState.UserPK, BundleSource{SourceID: actor.SourceID, SourceBundleID: req.SourceBundleID, PlannedRowCount: &plannedRowCount, CanonicalRequestHash: req.CanonicalRequestHash})
		if err != nil {
			return err
		}
		if bundle == nil {
			return &PushEffectCoverageError{}
		}
		projected, err := s.projectCommittedBundle(selection, bundle)
		if err != nil {
			return err
		}
		if err := requireCompletePushEffectCoverage(preparedRows, projected); err != nil {
			return err
		}
		if projected.RowCount <= 0 {
			return &PushEffectCoverageError{}
		}
		pushBytes, err := compactPushResponseDecodedBytes(generationDescriptor(view.contract), projected)
		if err != nil {
			return projectionError(BundleProjectionTooLarge, "push", "authoritative push response cannot be represented")
		}
		pullBytes, err := compactPullResponseDecodedBytes(generationDescriptor(view.contract), projected.BundleSeq, []Bundle{*projected}, false)
		if err != nil || pushBytes > int64(s.config.MaxBytesPerBundle)+historyEnvelopeAllowance || pullBytes > s.config.MaxBytesPerPullResponse {
			return projectionError(BundleProjectionTooLarge, "push", "authoritative bundle cannot be represented within admitted response limits")
		}
		if err := activateSourceState(ctx, tx, scopeState.UserPK, scopeState.UserID, actor.SourceID, req.SourceBundleID); err != nil {
			return err
		}
		if err := s.applyRetentionPolicyForUser(ctx, tx, scopeState.UserPK); err != nil {
			return err
		}
		if initializationID != "" {
			if err := transitionScopeToInitialized(ctx, tx, actor.UserID, actor.SourceID); err != nil {
				return err
			}
		}
		resp = &OneShotPushResponse{Bundle: projected}
		return nil
	})
	if err != nil {
		var conflictErr *PushConflictError
		if errors.As(err, &conflictErr) {
			transferHistoryProjectionForHTTP(ctx, &conflictErr.historyProjectionRelease, &releaseProjection, &releaseOperation)
		}
		return nil, err
	}
	transferHistoryProjectionForHTTP(ctx, &resp.historyProjectionRelease, &releaseProjection, &releaseOperation)
	return resp, nil
}

func (s *SyncService) replayOneShotPush(ctx context.Context, tx pgx.Tx, userPK int64, actor Actor, selection generationSelection, req *OneShotPushRequest) (*OneShotPushResponse, error) {
	meta, err := loadCommittedPushMetadataBySourceTuple(ctx, tx, userPK, actor.SourceID, req.SourceBundleID)
	if err != nil || meta == nil {
		return nil, err
	}
	if meta.CanonicalRequestHash != req.CanonicalRequestHash {
		return nil, &PushReplayMismatchError{}
	}
	if meta.PlannedRowCount == nil || *meta.PlannedRowCount != int64(len(req.Rows)) {
		return nil, &CommittedPushStateError{Code: CommittedPushStateCorrupt, Message: "committed source tuple row count is inconsistent"}
	}
	canonical, err := s.loadCommittedBundle(ctx, tx, actor.UserID, meta.BundleSeq)
	if err != nil {
		return nil, err
	}
	projected, err := s.projectCommittedBundle(selection, canonical)
	if err != nil {
		return nil, err
	}
	if projected.RowCount <= 0 {
		return nil, projectionError(GenerationProjectionIntegrityError, "push_replay", "committed push projection is empty")
	}
	return &OneShotPushResponse{Bundle: projected}, nil
}

func (s *SyncService) preparePushRowsPreservingInput(rows []PushRequestRow) ([]pushPreparedRow, error) {
	return s.preparePushRowsWithOptions(rows, true)
}

func (s *SyncService) preparePushRowsWithOptions(rows []PushRequestRow, preserveInputOrder bool) ([]pushPreparedRow, error) {
	prepared := make([]pushPreparedRow, 0, len(rows))
	seenTargets := make(map[string]struct{}, len(rows))

	for i, row := range rows {
		schemaName := strings.ToLower(strings.TrimSpace(row.Schema))
		if schemaName == "" {
			schemaName = "public"
		}
		tableName := strings.ToLower(strings.TrimSpace(row.Table))
		op := strings.ToUpper(strings.TrimSpace(row.Op))

		if !isValidSchemaName(schemaName) {
			return nil, &PushValidationError{Message: fmt.Sprintf("invalid schema name %q", row.Schema)}
		}
		if !isValidTableName(tableName) {
			return nil, &PushValidationError{Message: fmt.Sprintf("invalid table name %q", row.Table)}
		}
		if !s.IsTableRegistered(schemaName, tableName) {
			return nil, &PushValidationError{Message: fmt.Sprintf("table %s.%s is not registered", schemaName, tableName)}
		}
		switch op {
		case OpInsert, OpUpdate, OpDelete:
		default:
			return nil, &PushValidationError{Message: fmt.Sprintf("invalid op %q", row.Op)}
		}

		normalizedKey, err := s.normalizeVisibleSyncKey(schemaName, tableName, row.Key)
		if err != nil {
			return nil, err
		}
		tableID, err := s.tableIDForTable(schemaName, tableName)
		if err != nil {
			return nil, err
		}

		targetKey := string(appendInt32BigEndian(append([]byte(nil), normalizedKey.keyBytes...), tableID))
		if _, exists := seenTargets[targetKey]; exists {
			return nil, &PushValidationError{Message: fmt.Sprintf("duplicate target row in push request: %s.%s %s", schemaName, tableName, normalizedKey.keyString)}
		}
		seenTargets[targetKey] = struct{}{}

		var payload []byte
		var requestPayload []byte
		var payloadColumns []string
		var jsonPayloadTypes map[string]string
		var requiresIndividualApply bool
		if op == OpDelete {
			if len(row.Payload) != 0 {
				return nil, &PushValidationError{Message: fmt.Sprintf("DELETE for %s.%s must not include payload", schemaName, tableName)}
			}
		} else {
			if len(row.Payload) == 0 {
				return nil, &PushValidationError{Message: fmt.Sprintf("%s for %s.%s requires payload", op, schemaName, tableName)}
			}

			payloadObj, err := jcs.DecodeObject(row.Payload)
			if err != nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s must be a JSON object", schemaName, tableName)}
			}
			if _, ok := payloadObj[syncScopeColumnName]; ok {
				return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s must not include hidden scope column %q", schemaName, tableName, syncScopeColumnName)}
			}
			requestPayload, err = jcs.Canonicalize(row.Payload)
			if err != nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("canonicalize request payload for %s.%s: %v", schemaName, tableName, err)}
			}
			if err := normalizePayloadVisibleSyncKey(payloadObj, normalizedKey, schemaName, tableName); err != nil {
				return nil, err
			}

			for col := range payloadObj {
				normalizedColumn := strings.ToLower(col)
				if !isValidColumnName(normalizedColumn) {
					return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s contains invalid column %q", schemaName, tableName, col)}
				}
				columnTypes := s.columnTypesForTable(schemaName, tableName)
				if _, ok := columnTypes[normalizedColumn]; !ok {
					return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s contains unknown column %q", schemaName, tableName, col)}
				}
				payloadColumns = append(payloadColumns, normalizedColumn)
				columnType := columnTypes[normalizedColumn]
				if columnType == "json" || columnType == "jsonb" {
					if jsonPayloadTypes == nil {
						jsonPayloadTypes = make(map[string]string)
					}
					jsonPayloadTypes[strings.ToLower(col)] = columnType
					requiresIndividualApply = true
				}
			}
			if err := s.normalizePushPayloadFields(schemaName, tableName, payloadObj); err != nil {
				return nil, err
			}
			sort.Strings(payloadColumns)

			payloadRaw, err := jcs.Marshal(payloadObj)
			if err != nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("marshal payload for %s.%s: %v", schemaName, tableName, err)}
			}
			payload, err = canonicalJSON(payloadRaw)
			if err != nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("canonicalize payload for %s.%s: %v", schemaName, tableName, err)}
			}
		}

		prepared = append(prepared, pushPreparedRow{
			schema:                  schemaName,
			table:                   tableName,
			tableID:                 tableID,
			op:                      op,
			keyColumn:               normalizedKey.column,
			keyType:                 normalizedKey.keyType,
			keyString:               normalizedKey.keyString,
			keyValue:                normalizedKey.dbValue,
			keyBytes:                append([]byte(nil), normalizedKey.keyBytes...),
			baseRowVersion:          row.BaseRowVersion,
			payload:                 payload,
			requestPayload:          requestPayload,
			payloadColumns:          payloadColumns,
			jsonPayloadTypes:        jsonPayloadTypes,
			requiresIndividualApply: requiresIndividualApply,
			inputOrder:              i,
		})
	}

	if !preserveInputOrder {
		sort.SliceStable(prepared, func(i, j int) bool {
			opRank := func(op string) int {
				if op == OpDelete {
					return 1
				}
				return 0
			}
			if opRank(prepared[i].op) != opRank(prepared[j].op) {
				return opRank(prepared[i].op) < opRank(prepared[j].op)
			}

			orderI := s.tableOrderIndex(prepared[i].schema, prepared[i].table)
			orderJ := s.tableOrderIndex(prepared[j].schema, prepared[j].table)
			if prepared[i].op == OpDelete {
				if orderI != orderJ {
					return orderI > orderJ
				}
			} else if orderI != orderJ {
				return orderI < orderJ
			}
			return prepared[i].inputOrder < prepared[j].inputOrder
		})
	}

	return prepared, nil
}

func (s *SyncService) tableOrderIndex(schemaName, tableName string) int {
	if s == nil || s.discoveredSchema == nil {
		return 0
	}
	if idx, ok := s.discoveredSchema.OrderIdx[Key(schemaName, tableName)]; ok {
		return idx
	}
	return 0
}

func loadRowStateSnapshots(ctx context.Context, tx pgx.Tx, userPK int64, rows []pushPreparedRow) ([]indexedRowStateSnapshot, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	tableIDs := make([]int32, len(rows))
	keyBytes := make([][]byte, len(rows))
	for i, row := range rows {
		tableIDs[i] = row.tableID
		keyBytes[i] = append([]byte(nil), row.keyBytes...)
	}

	queryRows, err := tx.Query(ctx, `
		WITH requested AS (
			SELECT ord::bigint, table_id, key_bytes
			FROM unnest($2::int4[], $3::bytea[]) WITH ORDINALITY
				AS req(table_id, key_bytes, ord)
		)
		SELECT req.ord, rs.bundle_seq, rs.deleted
		FROM requested AS req
		LEFT JOIN sync.row_state AS rs
		  ON rs.user_pk = $1
		 AND rs.table_id = req.table_id
		 AND rs.key_bytes = req.key_bytes
		ORDER BY req.ord
	`, userPK, tableIDs, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("bulk load row state snapshots: %w", err)
	}
	defer queryRows.Close()

	states := make([]indexedRowStateSnapshot, len(rows))
	for queryRows.Next() {
		var (
			ord        int64
			rowVersion *int64
			deleted    *bool
		)
		if err := queryRows.Scan(&ord, &rowVersion, &deleted); err != nil {
			return nil, fmt.Errorf("scan bulk row state snapshot: %w", err)
		}
		idx := int(ord) - 1
		if idx < 0 || idx >= len(states) {
			return nil, fmt.Errorf("bulk row state snapshot returned invalid ordinal %d", ord)
		}
		if rowVersion == nil || deleted == nil {
			continue
		}
		states[idx] = indexedRowStateSnapshot{
			rowStateSnapshot: rowStateSnapshot{
				rowVersion: *rowVersion,
				deleted:    *deleted,
			},
			found: true,
		}
	}
	if queryRows.Err() != nil {
		return nil, fmt.Errorf("iterate bulk row state snapshots: %w", queryRows.Err())
	}
	return states, nil
}

func (s *SyncService) pushConflictError(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow, state *rowStateSnapshot, table GenerationTableContract, _ string) error {
	var (
		serverRowVersion int64
		serverRowDeleted bool
		serverRow        json.RawMessage
	)
	if state != nil {
		serverRowVersion = state.rowVersion
		serverRowDeleted = state.deleted
	}
	if state != nil && !state.deleted {
		var err error
		serverRow, err = s.loadCurrentServerRowPayload(ctx, tx, ownerUserID, row, table)
		if err != nil {
			return err
		}
	}
	return &PushConflictError{
		Message: "push conflict",
		Conflict: &PushConflictDetails{
			Schema:           row.schema,
			Table:            row.table,
			Key:              SyncKey{row.keyColumn: row.keyString},
			Op:               row.op,
			BaseRowVersion:   row.baseRowVersion,
			ServerRowVersion: serverRowVersion,
			ServerRowDeleted: serverRowDeleted,
			ServerRow:        serverRow,
		},
	}
}

func (s *SyncService) loadCurrentServerRowPayload(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow, table GenerationTableContract) (json.RawMessage, error) {
	tableIdent := pgx.Identifier{row.schema, row.table}.Sanitize()
	keyColumnIdent := pgx.Identifier{row.keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
	tableColumnTypes := s.columnTypesForTable(row.schema, row.table)
	visibleColumnTypes := make(map[string]string, len(table.Columns))
	for _, column := range table.Columns {
		columnName := strings.ToLower(column.Name)
		visibleColumnTypes[columnName] = tableColumnTypes[columnName]
	}
	payloadExpression := databaseVisibleWirePayloadExpression("src", table.Columns, visibleColumnTypes)
	maxPhysicalPayloadBytes, err := physicalDatabasePayloadByteLimitForColumnTypes(int64(s.config.MaxBytesPerBundle), visibleColumnTypes)
	if err != nil {
		return nil, err
	}

	var (
		payload          []byte
		payloadByteCount int64
	)
	err = tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT
			CASE WHEN octet_length(bounded.payload_db::text) <= $3 THEN bounded.payload_db END,
			octet_length(bounded.payload_db::text)
		FROM %s AS src
		CROSS JOIN LATERAL (SELECT %s AS payload_db) AS bounded
		WHERE %s = $1
		  AND %s = $2
	`, tableIdent, payloadExpression, keyColumnIdent, ownerColumnIdent), row.keyValue, ownerUserID, maxPhysicalPayloadBytes).Scan(&payload, &payloadByteCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict row is unavailable")
		}
		return nil, fmt.Errorf("load current server row payload for %s.%s %s: %w", row.schema, row.table, row.keyString, err)
	}
	if s.uploadConnHooks != nil && s.uploadConnHooks.afterConflictRowScan != nil {
		s.uploadConnHooks.afterConflictRowScan(conflictRowScanObservation{
			ReportedPayloadByteCount: payloadByteCount,
			PayloadMaterialized:      payload != nil,
			MaterializedPayloadBytes: len(payload),
		})
	}
	if payloadByteCount < 0 || payloadByteCount > maxPhysicalPayloadBytes {
		return nil, projectionError(BundleLimitExceeded, "conflict", "conflict data exceeds configured bundle limit")
	}
	if payload == nil {
		return nil, projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict row is invalid")
	}

	canonicalPayload, err := s.canonicalizeWirePayload(row.schema, row.table, payload)
	if err != nil {
		return nil, projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict row is invalid")
	}
	return canonicalPayload, nil
}

func (s *SyncService) validatePushRowConflict(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow, state *rowStateSnapshot, table GenerationTableContract) error {
	switch row.op {
	case OpInsert:
		if state == nil {
			if row.baseRowVersion != 0 {
				return s.pushConflictError(ctx, tx, ownerUserID, row, nil, table, "")
			}
			return nil
		}
		if row.baseRowVersion != state.rowVersion {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, table, "")
		}
		if !state.deleted {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, table, "")
		}
	case OpUpdate, OpDelete:
		if state == nil || state.deleted {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, table, "")
		}
		if row.baseRowVersion != state.rowVersion {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, table, "")
		}
	}
	return nil
}

func applyPreparedPushRow(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow) error {
	tableIdent := pgx.Identifier{row.schema, row.table}.Sanitize()
	keyColumnIdent := pgx.Identifier{row.keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()

	switch row.op {
	case OpDelete:
		stmt := fmt.Sprintf(`DELETE FROM %s WHERE %s = $1 AND %s = $2`, tableIdent, keyColumnIdent, ownerColumnIdent)
		if _, err := tx.Exec(ctx, stmt, row.keyValue, ownerUserID); err != nil {
			return fmt.Errorf("delete %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		return nil
	case OpInsert:
		payloadWithOwner, err := injectOwnerUserIDPayload(row.payload, ownerUserID)
		if err != nil {
			return fmt.Errorf("inject owner into insert payload for %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		columnList := quotedInsertColumnList(row.payloadColumns)
		selectList := pushInsertSelectList(row)
		stmt := fmt.Sprintf(`
			INSERT INTO %s (%s)
			SELECT %s
			FROM jsonb_populate_record(NULL::%s, $1::jsonb) AS populated
		`, tableIdent, columnList, selectList, tableIdent)
		if _, err := tx.Exec(ctx, stmt, payloadWithOwner); err != nil {
			return fmt.Errorf("insert %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		return nil
	case OpUpdate:
		setClauses := make([]string, 0, len(row.payloadColumns))
		for _, col := range row.payloadColumns {
			colIdent := pgx.Identifier{col}.Sanitize()
			columnType := row.jsonPayloadTypes[col]
			if columnType == "json" || columnType == "jsonb" {
				setClauses = append(setClauses, fmt.Sprintf("%s = ($1::jsonb ->> %s)::%s", colIdent, quoteSQLLiteral(col), columnType))
			} else {
				setClauses = append(setClauses, fmt.Sprintf("%s = src.%s", colIdent, colIdent))
			}
		}
		stmt := fmt.Sprintf(`
			UPDATE %s AS dst
			SET %s
			FROM (SELECT * FROM jsonb_populate_record(NULL::%s, $1::jsonb)) AS src
			WHERE dst.%s = $2
			  AND dst.%s = $3
		`, tableIdent, strings.Join(setClauses, ", "), tableIdent, keyColumnIdent, ownerColumnIdent)
		if _, err := tx.Exec(ctx, stmt, row.payload, row.keyValue, ownerUserID); err != nil {
			return fmt.Errorf("update %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		return nil
	default:
		return &PushValidationError{Message: fmt.Sprintf("unsupported op %q", row.op)}
	}
}

func batchPreparedPushRows(rows []pushPreparedRow) [][]pushPreparedRow {
	if len(rows) == 0 {
		return nil
	}

	batches := make([][]pushPreparedRow, 0, len(rows))
	start := 0
	for i := 1; i <= len(rows); i++ {
		if i < len(rows) && canBatchPreparedPushRows(rows[i-1], rows[i]) {
			continue
		}
		batches = append(batches, rows[start:i])
		start = i
	}
	return batches
}

func canBatchPreparedPushRows(a, b pushPreparedRow) bool {
	return !a.requiresIndividualApply &&
		!b.requiresIndividualApply &&
		a.schema == b.schema &&
		a.table == b.table &&
		a.op == b.op &&
		a.keyType == b.keyType &&
		a.keyColumn == b.keyColumn &&
		sameStrings(a.payloadColumns, b.payloadColumns)
}

func pushInsertSelectList(row pushPreparedRow) string {
	selectExpressions := make([]string, 0, len(row.payloadColumns)+1)
	ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
	selectExpressions = append(selectExpressions, "populated."+ownerIdent)
	for _, columnName := range row.payloadColumns {
		columnIdent := pgx.Identifier{columnName}.Sanitize()
		columnType := row.jsonPayloadTypes[columnName]
		if columnType == "json" || columnType == "jsonb" {
			selectExpressions = append(selectExpressions, fmt.Sprintf("($1::jsonb ->> %s)::%s", quoteSQLLiteral(columnName), columnType))
			continue
		}
		selectExpressions = append(selectExpressions, "populated."+columnIdent)
	}
	return strings.Join(selectExpressions, ", ")
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func applyPreparedPushRowBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) == 1 {
		return applyPreparedPushRow(ctx, tx, ownerUserID, rows[0])
	}

	switch rows[0].op {
	case OpDelete:
		return applyPreparedDeleteBatch(ctx, tx, ownerUserID, rows)
	case OpInsert:
		return applyPreparedInsertBatch(ctx, tx, ownerUserID, rows)
	case OpUpdate:
		return applyPreparedUpdateBatch(ctx, tx, ownerUserID, rows)
	default:
		return &PushValidationError{Message: fmt.Sprintf("unsupported op %q", rows[0].op)}
	}
}

func applyPreparedDeleteBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	for _, row := range rows {
		if err := applyPreparedPushRow(ctx, tx, ownerUserID, row); err != nil {
			return fmt.Errorf("delete batch %s.%s (%d rows): %w", rows[0].schema, rows[0].table, len(rows), err)
		}
	}
	return nil
}

func applyPreparedInsertBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	tableIdent := pgx.Identifier{rows[0].schema, rows[0].table}.Sanitize()
	columnList := quotedInsertColumnList(rows[0].payloadColumns)
	payloadArray, err := marshalPayloadJSONArray(rows, ownerUserID)
	if err != nil {
		return fmt.Errorf("marshal insert batch payload for %s.%s: %w", rows[0].schema, rows[0].table, err)
	}

	stmt := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s
		FROM jsonb_populate_recordset(NULL::%s, $1::jsonb)
	`, tableIdent, columnList, columnList, tableIdent)
	if _, err := tx.Exec(ctx, stmt, payloadArray); err != nil {
		return fmt.Errorf("insert batch %s.%s (%d rows): %w", rows[0].schema, rows[0].table, len(rows), err)
	}
	return nil
}

func applyPreparedUpdateBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	tableIdent := pgx.Identifier{rows[0].schema, rows[0].table}.Sanitize()
	keyColumnIdent := pgx.Identifier{rows[0].keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
	payloadArray, err := marshalPayloadJSONArray(rows, "")
	if err != nil {
		return fmt.Errorf("marshal update batch payload for %s.%s: %w", rows[0].schema, rows[0].table, err)
	}

	setClauses := make([]string, 0, len(rows[0].payloadColumns))
	for _, col := range rows[0].payloadColumns {
		colIdent := pgx.Identifier{col}.Sanitize()
		setClauses = append(setClauses, fmt.Sprintf("%s = src.%s", colIdent, colIdent))
	}
	stmt := fmt.Sprintf(`
		UPDATE %s AS dst
		SET %s
		FROM jsonb_populate_recordset(NULL::%s, $1::jsonb) AS src
		WHERE dst.%s = src.%s
		  AND dst.%s = $2
	`, tableIdent, strings.Join(setClauses, ", "), tableIdent, keyColumnIdent, keyColumnIdent, ownerColumnIdent)
	if _, err := tx.Exec(ctx, stmt, payloadArray, ownerUserID); err != nil {
		return fmt.Errorf("update batch %s.%s (%d rows): %w", rows[0].schema, rows[0].table, len(rows), err)
	}
	return nil
}

func marshalPayloadJSONArray(rows []pushPreparedRow, ownerUserID string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, row := range rows {
		if i > 0 {
			buf.WriteByte(',')
		}
		payload := row.payload
		if ownerUserID != "" {
			var err error
			payload, err = injectOwnerUserIDPayload(row.payload, ownerUserID)
			if err != nil {
				return nil, err
			}
		}
		buf.Write(payload)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func quotedColumnList(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, col := range columns {
		quoted = append(quoted, pgx.Identifier{col}.Sanitize())
	}
	return strings.Join(quoted, ", ")
}

func quotedInsertColumnList(columns []string) string {
	insertColumns := make([]string, 0, len(columns)+1)
	insertColumns = append(insertColumns, syncScopeColumnName)
	insertColumns = append(insertColumns, columns...)
	return quotedColumnList(insertColumns)
}

func (s *SyncService) loadCommittedBundle(ctx context.Context, tx pgx.Tx, userID string, bundleSeq int64) (*Bundle, error) {
	var bundle Bundle
	physicalPayloadLimit, err := physicalStoredWirePayloadByteLimit(int64(s.config.MaxBytesPerBundle))
	if err != nil {
		return nil, fmt.Errorf("derive retained history payload transfer limit: %w", err)
	}
	userPK, err := lookupUserPK(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	var bundleHash []byte
	if err := tx.QueryRow(ctx, `
		SELECT bundle_seq, source_id, source_bundle_id, row_count, byte_count, bundle_hash, canonical_request_hash
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq = $2
	`, userPK, bundleSeq).Scan(&bundle.BundleSeq, &bundle.SourceID, &bundle.SourceBundleID, &bundle.RowCount, &bundle.ByteCount, &bundleHash, &bundle.CanonicalRequestHash); err != nil {
		return nil, fmt.Errorf("load bundle_log %d: %w", bundleSeq, err)
	}
	if err := sourceid.Validate(bundle.SourceID); err != nil {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "retained bundle source identity is invalid")
	}
	bundle.BundleHash = renderBundleHash(bundleHash)
	if bundle.RowCount > int64(s.config.MaxRowsPerBundle) || bundle.ByteCount > int64(s.config.MaxBytesPerBundle) {
		return nil, projectionError(BundleLimitExceeded, "history", "retained bundle exceeds configured limits")
	}
	if bundle.BundleSeq <= 0 || bundle.SourceBundleID <= 0 || bundle.RowCount <= 0 || bundle.ByteCount < 2 || !protocolhash.IsCanonicalHash(bundle.BundleHash) {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "retained bundle metadata is invalid")
	}

	rows, err := tx.Query(ctx, `
		SELECT
			row_ordinal,
			table_id,
			CASE WHEN octet_length(key_bytes) <= $3 THEN key_bytes END,
			octet_length(key_bytes),
			op_code,
			CASE WHEN payload_wire IS NULL OR octet_length(payload_wire::text) <= $4 THEN payload_wire END,
			COALESCE(octet_length(payload_wire::text), 0)
		FROM sync.bundle_rows
		WHERE user_pk = $1
		  AND bundle_seq = $2
		ORDER BY row_ordinal
		LIMIT $5
	`, userPK, bundleSeq, s.config.MaxBytesPerBundle, physicalPayloadLimit, s.config.MaxRowsPerBundle+1)
	if err != nil {
		return nil, fmt.Errorf("query bundle_rows %d: %w", bundleSeq, err)
	}
	defer rows.Close()

	bundle.Rows = make([]BundleRow, 0, int(bundle.RowCount))
	hasher := protocolhash.NewCommittedBundleHasher()
	for rows.Next() {
		var row BundleRow
		var (
			rowOrdinal       int64
			tableID          int32
			keyBytes         []byte
			keyByteCount     int64
			opCode           int16
			payloadWire      []byte
			payloadByteCount int64
		)
		if err := rows.Scan(&rowOrdinal, &tableID, &keyBytes, &keyByteCount, &opCode, &payloadWire, &payloadByteCount); err != nil {
			return nil, fmt.Errorf("scan bundle_rows %d: %w", bundleSeq, err)
		}
		if len(bundle.Rows) >= s.config.MaxRowsPerBundle || keyByteCount < 0 || keyByteCount > int64(s.config.MaxBytesPerBundle) || payloadByteCount < 0 || payloadByteCount > physicalPayloadLimit {
			return nil, projectionError(BundleLimitExceeded, "history", "retained bundle exceeds configured limits")
		}
		if rowOrdinal != int64(len(bundle.Rows)+1) {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle row ordinals are invalid")
		}
		info, err := s.tableInfoForID(tableID)
		if err != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle table identity is invalid")
		}
		row.Schema = info.schemaName
		row.Table = info.tableName
		row.Key, err = wireSyncKeyFromBytes(info, keyBytes)
		if err != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle row key is invalid")
		}
		row.Op, err = opStringFromCode(opCode)
		if err != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle row operation is invalid")
		}
		row.RowVersion = bundleSeq
		row.Payload = payloadWire
		if err := hasher.Add(protocolhash.BundleRow{
			Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
			RowVersion: row.RowVersion, Payload: row.Payload,
		}); err != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle row framing is invalid")
		}
		if hasher.CurrentByteCount() > int64(s.config.MaxBytesPerBundle) {
			return nil, projectionError(BundleLimitExceeded, "history", "retained bundle exceeds configured limits")
		}
		if hasher.CurrentByteCount() > bundle.ByteCount {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle byte count is invalid")
		}
		bundle.Rows = append(bundle.Rows, row)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate bundle_rows %d: %w", bundleSeq, rows.Err())
	}
	if int64(len(bundle.Rows)) != bundle.RowCount {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle row count is invalid")
	}
	hash, byteCount, err := hasher.Sum()
	if err != nil || byteCount != bundle.ByteCount || hash != bundle.BundleHash {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle integrity check failed")
	}
	return &bundle, nil
}
