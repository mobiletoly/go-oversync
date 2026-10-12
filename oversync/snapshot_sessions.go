package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

type SnapshotSessionNotFoundError struct {
	SnapshotID string
}

func (e *SnapshotSessionNotFoundError) Error() string {
	return fmt.Sprintf("snapshot session %s was not found", e.SnapshotID)
}

type SnapshotSessionExpiredError struct {
	SnapshotID string
}

func (e *SnapshotSessionExpiredError) Error() string {
	return fmt.Sprintf("snapshot session %s has expired; start a new snapshot session", e.SnapshotID)
}

type SnapshotChunkInvalidError struct {
	Message string
}

func (e *SnapshotChunkInvalidError) Error() string {
	return e.Message
}

type SnapshotSessionForbiddenError struct {
	SnapshotID string
}

func (e *SnapshotSessionForbiddenError) Error() string {
	return fmt.Sprintf("snapshot session %s does not belong to the authenticated user", e.SnapshotID)
}

type SnapshotSessionLimitExceededError struct {
	Dimension string
	Actual    int64
	Limit     int64
}

type SnapshotCapacityError struct{ Operation string }

func (e *SnapshotCapacityError) Error() string {
	return "snapshot " + e.Operation + " capacity is exhausted"
}

type SnapshotChunkTooSmallError struct{ RequiredDecodedBodyBytes int64 }

func (e *SnapshotChunkTooSmallError) Error() string {
	return fmt.Sprintf("max_bytes is too small for the next snapshot row; required_decoded_body_bytes=%d", e.RequiredDecodedBodyBytes)
}

func (e *SnapshotSessionLimitExceededError) Error() string {
	return fmt.Sprintf("snapshot session %s %d exceeds limit %d", e.Dimension, e.Actual, e.Limit)
}

type snapshotMaterializedRow struct {
	rowOrdinal    int64
	tableID       int32
	keyBytes      []byte
	bundleSeq     int64
	payloadWire   []byte
	wireByteCount int64
}

type snapshotPayloadScanObservation struct {
	ReportedKeyByteCount     int64
	ReportedPayloadByteCount int64
	KeyMaterialized          bool
	PayloadMaterialized      bool
	MaterializedKeyBytes     int
	MaterializedPayloadBytes int
}

func (s *SyncService) CreateSnapshotSession(ctx context.Context, actor Actor, proof SchemaGenerationProof) (_ *SnapshotSession, err error) {
	return s.CreateSnapshotSessionWithRequest(ctx, actor, proof, nil)
}

func (s *SyncService) CreateSnapshotSessionWithRequest(ctx context.Context, actor Actor, proof SchemaGenerationProof, req *SnapshotSessionCreateRequest) (_ *SnapshotSession, err error) {
	if err := rejectRetryableCallbackContext(ctx, "CreateSnapshotSession"); err != nil {
		return nil, err
	}
	if err := s.validateClientActor(actor, true); err != nil {
		return nil, err
	}
	if err := validateSchemaGenerationProof(proof); err != nil {
		return nil, err
	}
	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	view, err := s.resolveRequestGeneration(ctx, actor, proof)
	if err != nil {
		return nil, err
	}
	if req != nil && req.SourceReplacement != nil {
		if _, err := s.validateSnapshotSourceReplacement(actor, req.SourceReplacement); err != nil {
			return nil, err
		}
	}
	if req == nil || req.SourceReplacement == nil {
		if _, err := s.requireExactSourceGenerationBinding(ctx, actor, proof); err != nil {
			return nil, err
		}
	} else if err := s.validateSnapshotReplacementBinding(ctx, actor, proof, req.SourceReplacement); err != nil {
		return nil, err
	}
	selection, err := selectionFromResolvedGeneration(proof, view)
	if err != nil {
		return nil, err
	}
	startedAt := time.Now()
	if !s.tryAcquireSnapshotBuild() {
		s.logger.Info("Snapshot build rejected", "user_id", actor.UserID, "outcome", "rejected", "duration", time.Since(startedAt), "admission_outcome", "rejected", "active_build_count_high_water", s.snapshotMetrics.buildHighWater.Load())
		return nil, &SnapshotCapacityError{Operation: "build"}
	}
	defer s.releaseSnapshotBuild()
	var snapshotID string
	var batchHighWater snapshotBatchHighWater
	defer func() {
		if err != nil {
			s.logger.Info("Snapshot build failed", "user_id", actor.UserID, "snapshot_id", snapshotID, "outcome", "failed", "materialization_batch_count", batchHighWater.batches, "batch_rows_high_water", batchHighWater.rows, "batch_bytes_high_water", batchHighWater.bytes, "duration", time.Since(startedAt), "admission_outcome", "admitted", "active_build_count_high_water", s.snapshotMetrics.buildHighWater.Load(), "error", err)
		}
	}()
	if s.snapshotHooks != nil && s.snapshotHooks.afterBuildPermit != nil {
		if err := s.snapshotHooks.afterBuildPermit(ctx); err != nil {
			return nil, err
		}
	}

	var resp *SnapshotSession
	err = withOriginTransaction(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		if err := requireScopeInitializedQuerier(ctx, tx, actor.UserID); err != nil {
			return err
		}

		retainedState, err := lockSnapshotRetainedHistoryState(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}
		if retainedState == nil {
			return fmt.Errorf("missing retained history state for %q", actor.UserID)
		}
		if req == nil || req.SourceReplacement == nil {
			if err := requireExactSourceGenerationBindingInTx(ctx, tx, retainedState.UserPK, actor.UserID, actor.SourceID, proof); err != nil {
				return err
			}
		}
		effectiveSourceID, err := s.applySnapshotSourceReplacementInTx(ctx, tx, actor, retainedState.UserPK, proof, req)
		if err != nil {
			return err
		}

		snapshotBundleSeq := retainedState.highestBundleSeq()
		snapshotID = uuid.NewString()
		var expiresAt time.Time
		if err := tx.QueryRow(ctx, `
			INSERT INTO sync.snapshot_sessions (
				snapshot_id, user_pk, source_id, schema_generation_id, schema_generation_fingerprint, snapshot_bundle_seq, row_count, byte_count, expires_at
			) VALUES ($1::uuid, $2, $3, $4, $5, $6, 0, 0,
				transaction_timestamp() + $7::bigint * interval '1 microsecond')
			RETURNING expires_at
		`, snapshotID, retainedState.UserPK, effectiveSourceID, proof.ID, proof.Fingerprint, snapshotBundleSeq, s.snapshotSessionTTL().Microseconds()).Scan(&expiresAt); err != nil {
			return fmt.Errorf("insert snapshot session: %w", err)
		}
		if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotFence != nil {
			if err := s.snapshotHooks.afterSnapshotFence(ctx); err != nil {
				return err
			}
		}
		rowCount, byteCount, materializationHighWater, err := s.materializeSnapshotRowsForGeneration(ctx, tx, selection, snapshotID, actor.UserID, retainedState.UserPK, snapshotBundleSeq)
		batchHighWater = materializationHighWater
		observeAtomicHighWater(&s.snapshotMetrics.materializationBatchRowsHighWater, int64(batchHighWater.rows))
		observeAtomicHighWater(&s.snapshotMetrics.materializationBatchBytesHighWater, batchHighWater.bytes)
		if err != nil {
			return err
		}
		if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotFinalize != nil {
			if err := s.snapshotHooks.beforeSnapshotFinalize(ctx); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE sync.snapshot_sessions SET row_count=$2, byte_count=$3 WHERE snapshot_id=$1::uuid`, snapshotID, rowCount, byteCount); err != nil {
			return fmt.Errorf("finalize snapshot session counts: %w", err)
		}

		resp = &SnapshotSession{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			RowCount:          rowCount,
			ByteCount:         byteCount,
			ExpiresAt:         expiresAt.Format(time.RFC3339Nano),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotCommit != nil {
		if err := s.snapshotHooks.afterSnapshotCommit(ctx); err != nil {
			return nil, err
		}
	}
	s.logger.Info("Snapshot build completed", "user_id", actor.UserID, "snapshot_id", resp.SnapshotID, "outcome", "completed", "row_count", resp.RowCount, "byte_count", resp.ByteCount, "materialization_batch_count", batchHighWater.batches, "batch_rows_high_water", batchHighWater.rows, "batch_bytes_high_water", batchHighWater.bytes, "duration", time.Since(startedAt), "admission_outcome", "admitted", "active_build_count_high_water", s.snapshotMetrics.buildHighWater.Load())
	s.requestSnapshotCleanup("post_create")
	return resp, nil
}

func lockSnapshotRetainedHistoryState(ctx context.Context, tx pgx.Tx, userID string) (*retainedHistoryState, error) {
	state, err := scanRetainedHistoryState(tx.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
		FOR UPDATE
	`, userID))
	if err != nil {
		return nil, fmt.Errorf("lock retained history state for snapshot %q: %w", userID, err)
	}
	return state, nil
}

func tryAcquireSnapshotPermit(permits chan struct{}) bool {
	select {
	case permits <- struct{}{}:
		return true
	default:
		return false
	}
}
func releaseSnapshotPermit(permits chan struct{}) { <-permits }

func (s *SyncService) validateSnapshotSourceReplacement(actor Actor, replacement *SnapshotSourceReplacement) (*SnapshotSourceReplacement, error) {
	if replacement == nil {
		return nil, nil
	}
	clean := &SnapshotSourceReplacement{
		PreviousSourceID: replacement.PreviousSourceID,
		NewSourceID:      replacement.NewSourceID,
		Reason:           replacement.Reason,
	}
	if err := sourceid.Validate(clean.PreviousSourceID); err != nil {
		return nil, &SnapshotSessionInvalidError{Message: "previous_source_id is invalid"}
	}
	if err := sourceid.Validate(clean.NewSourceID); err != nil {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id is invalid"}
	}
	if s.isReservedServerSourceID(clean.PreviousSourceID) || s.isReservedServerSourceID(clean.NewSourceID) {
		return nil, &SnapshotSessionInvalidError{Message: "source replacement is invalid"}
	}
	if clean.PreviousSourceID != actor.SourceID {
		return nil, &SnapshotSessionInvalidError{Message: "previous_source_id must match authenticated source_id"}
	}
	if clean.NewSourceID == clean.PreviousSourceID {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id must differ from previous_source_id"}
	}
	switch clean.Reason {
	case "history_pruned", "source_sequence_out_of_order", "source_sequence_changed", "source_retired", "schema_generation_upgrade":
	default:
		return nil, &SnapshotSessionInvalidError{Message: "source replacement reason is unsupported"}
	}
	return clean, nil
}

func validateSnapshotSourceReplacement(actor Actor, replacement *SnapshotSourceReplacement) (*SnapshotSourceReplacement, error) {
	return (*SyncService)(nil).validateSnapshotSourceReplacement(actor, replacement)
}

func (s *SyncService) validateSnapshotReplacementBinding(ctx context.Context, actor Actor, proof SchemaGenerationProof, replacement *SnapshotSourceReplacement) error {
	clean, err := s.validateSnapshotSourceReplacement(actor, replacement)
	if err != nil {
		return err
	}
	state, err := s.loadActorSourceGenerationState(ctx, actor, proof.ID)
	if err != nil {
		return err
	}
	if state == nil {
		return sourceGenerationError(SourceGenerationUnbound, proof.ID, 0, "", "source has not connected")
	}
	if err := validateStoredSourceGeneration(state, proof.ID); err != nil {
		return err
	}
	if proof.ID < state.generationID {
		return sourceGenerationError(SchemaGenerationDowngradeForbidden, proof.ID, state.generationID, "", "schema generation downgrade is forbidden")
	}
	if clean.Reason == "schema_generation_upgrade" {
		if proof.ID <= state.generationID {
			return sourceGenerationError(SourceGenerationConflict, proof.ID, state.generationID, "", "source replacement generation is invalid")
		}
		return nil
	}
	if proof.ID != state.generationID || proof.Fingerprint != state.fingerprint {
		return sourceGenerationError(SourceGenerationConflict, proof.ID, state.generationID, "", "source replacement generation is invalid")
	}
	return nil
}

func (s *SyncService) applySnapshotSourceReplacementInTx(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, proof SchemaGenerationProof, req *SnapshotSessionCreateRequest) (string, error) {
	if req == nil || req.SourceReplacement == nil {
		return actor.SourceID, nil
	}
	replacement, err := s.validateSnapshotSourceReplacement(actor, req.SourceReplacement)
	if err != nil {
		return "", err
	}
	states, err := lockReplacementSourceStates(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID)
	if err != nil {
		return "", err
	}
	bindings, err := lockReplacementSourceBindings(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID)
	if err != nil {
		return "", err
	}
	previousState := states[replacement.PreviousSourceID]
	newState := states[replacement.NewSourceID]
	previousBinding := bindings[replacement.PreviousSourceID]
	newBinding := bindings[replacement.NewSourceID]
	if previousState == nil && previousBinding == nil {
		return "", sourceGenerationError(SourceGenerationUnbound, proof.ID, 0, "", "source has not connected")
	}
	if previousState == nil {
		if err := validateBindingRow(previousBinding, proof.ID); err != nil {
			return "", err
		}
		return "", sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, previousBinding.generationID, "source_state", "source generation state is corrupt")
	}
	if previousBinding == nil {
		return "", sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "source_generation_binding", "source generation state is corrupt")
	}
	if err := validateBindingRow(previousBinding, proof.ID); err != nil {
		return "", err
	}
	if proof.ID < previousBinding.generationID {
		return "", sourceGenerationError(SchemaGenerationDowngradeForbidden, proof.ID, previousBinding.generationID, "", "schema generation downgrade is forbidden")
	}
	if replacement.Reason == "schema_generation_upgrade" {
		if proof.ID <= previousBinding.generationID {
			return "", sourceGenerationError(SourceGenerationConflict, proof.ID, previousBinding.generationID, "", "source replacement generation is invalid")
		}
	} else if proof.ID != previousBinding.generationID || proof.Fingerprint != previousBinding.fingerprint {
		return "", sourceGenerationError(SourceGenerationConflict, proof.ID, previousBinding.generationID, "", "source replacement generation is invalid")
	}

	if previousState != nil && previousState.State == sourceStateRetired {
		if previousState.ReplacedBySourceID != replacement.NewSourceID || previousState.RetirementReason != replacement.Reason {
			return "", &SourceRetiredError{
				UserID:             actor.UserID,
				SourceID:           replacement.PreviousSourceID,
				ReplacedBySourceID: previousState.ReplacedBySourceID,
			}
		}
		if newState == nil || newBinding == nil || (newState.State != sourceStateReserved && newState.State != sourceStateActive) {
			return "", &SourceReplacementInvalidError{Message: "source replacement is invalid"}
		}
		if err := requireBindingPair(newBinding, proof); err != nil {
			return "", err
		}
	} else {
		if previousState.State != sourceStateActive && previousState.State != sourceStateReserved {
			return "", &SourceReplacementInvalidError{Message: "source replacement is invalid"}
		}
		if newState != nil || newBinding != nil {
			return "", &SourceReplacementInvalidError{Message: "source replacement is invalid"}
		}
		conflict, err := replacementSourceHasConflict(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID)
		if err != nil {
			return "", err
		}
		if conflict {
			return "", &SourceReplacementInvalidError{Message: "source replacement is invalid"}
		}
		if err := s.admitReplacementSourceBinding(ctx, tx, userPK, replacement.NewSourceID, proof); err != nil {
			return "", err
		}
		if err := retireSourceState(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID, replacement.Reason); err != nil {
			return "", err
		}
	}

	return replacement.NewSourceID, nil
}

func lockReplacementSourceStates(ctx context.Context, tx pgx.Tx, userPK int64, sourceA, sourceB string) (map[string]*sourceStateRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT user_pk, source_id, state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		FROM sync.source_state
		WHERE user_pk=$1 AND source_id IN ($2,$3)
		ORDER BY source_id COLLATE "C"
		FOR UPDATE
	`, userPK, sourceA, sourceB)
	if err != nil {
		return nil, fmt.Errorf("lock replacement source state: %w", err)
	}
	defer rows.Close()
	result := make(map[string]*sourceStateRow, 2)
	for rows.Next() {
		row := &sourceStateRow{}
		if err := rows.Scan(&row.UserPK, &row.SourceID, &row.State, &row.MaxCommittedSourceBundleID, &row.ReplacedBySourceID, &row.RetirementReason); err != nil {
			return nil, fmt.Errorf("scan replacement source state: %w", err)
		}
		result[row.SourceID] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replacement source state: %w", err)
	}
	return result, nil
}

func lockReplacementSourceBindings(ctx context.Context, tx pgx.Tx, userPK int64, sourceA, sourceB string) (map[string]*sourceGenerationBindingRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT source_id, schema_generation_id, schema_generation_fingerprint
		FROM sync.source_generation_bindings
		WHERE user_pk=$1 AND source_id IN ($2,$3)
		ORDER BY source_id COLLATE "C"
		FOR UPDATE
	`, userPK, sourceA, sourceB)
	if err != nil {
		return nil, fmt.Errorf("lock replacement source binding: %w", err)
	}
	defer rows.Close()
	result := make(map[string]*sourceGenerationBindingRow, 2)
	for rows.Next() {
		var sourceID string
		var generationID int32
		row := &sourceGenerationBindingRow{}
		if err := rows.Scan(&sourceID, &generationID, &row.fingerprint); err != nil {
			return nil, fmt.Errorf("scan replacement source binding: %w", err)
		}
		row.generationID = SchemaGenerationID(generationID)
		result[sourceID] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replacement source binding: %w", err)
	}
	return result, nil
}

func replacementSourceHasConflict(ctx context.Context, tx pgx.Tx, userPK int64, previousSourceID, newSourceID string) (bool, error) {
	var conflict bool
	if err := tx.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM sync.bundle_log WHERE user_pk=$1 AND source_id=$3 LIMIT 1)
			OR EXISTS (SELECT 1 FROM sync.server_source_reservations WHERE source_id=$3 LIMIT 1)
			OR EXISTS (
				SELECT 1 FROM sync.scope_state
				WHERE user_pk=$1 AND state_code=$4 AND initializer_source_id=$2
				  AND lease_expires_at > transaction_timestamp()
				LIMIT 1
			)
	`, userPK, previousSourceID, newSourceID, scopeStateCodeInitializing).Scan(&conflict); err != nil {
		return false, fmt.Errorf("inspect source replacement guards: %w", err)
	}
	return conflict, nil
}

type snapshotBatchHighWater struct {
	rows    int
	bytes   int64
	batches int
}

func (s *SyncService) materializeSnapshotRows(ctx context.Context, tx pgx.Tx, view *schemaGenerationView, snapshotID, userID string, userPK, snapshotBundleSeq int64) (int64, int64, snapshotBatchHighWater, error) {
	if view == nil {
		return 0, 0, snapshotBatchHighWater{}, projectionError(GenerationProjectionIntegrityError, "snapshot", "generation view is unavailable")
	}
	tableInfos := make([]registeredTableRuntimeInfo, 0, len(view.byID))
	contracts := make(map[int32]GenerationTableContract, len(view.byID))
	for _, table := range view.byID {
		info, ok := s.registeredTableByID[table.tableID]
		if !ok {
			return 0, 0, snapshotBatchHighWater{}, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible table identity is unavailable")
		}
		tableInfos = append(tableInfos, info)
		contracts[table.tableID] = table.contract
	}
	sort.Slice(tableInfos, func(i, j int) bool { return tableInfos[i].tableID < tableInfos[j].tableID })
	activeTableIDs := make([]int32, 0, len(tableInfos))
	for _, info := range tableInfos {
		activeTableIDs = append(activeTableIDs, info.tableID)
	}

	wireGeneration := generationDescriptor(view.contract)
	var rowCount, byteCount int64
	highWater := snapshotBatchHighWater{}
	for _, info := range tableInfos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		keyIdent := pgx.Identifier{info.syncKeyColumn}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		contract := contracts[info.tableID]
		columnTypes := s.columnTypesForTable(info.schemaName, info.tableName)
		payloadExpression := databaseVisibleWirePayloadExpression("src", contract.Columns, columnTypes)
		physicalPayloadLimit, err := physicalDatabasePayloadByteLimitForColumnTypes(s.maxBytesPerSnapshotRow(), columnTypes)
		if err != nil {
			return 0, 0, highWater, fmt.Errorf("derive snapshot payload transfer limit for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		businessKeyBytesExpr := fmt.Sprintf("convert_to(CAST(src.%s AS text), 'UTF8')", keyIdent)
		if info.syncKeyType == syncKeyTypeUUID {
			businessKeyBytesExpr = fmt.Sprintf("uuid_send(src.%s)", keyIdent)
		}
		var lastKey []byte
		for {
			cursorPredicate := ""
			limitPlaceholder := "$6"
			queryArgs := []any{userID, userPK, info.tableID, s.maxBytesPerSnapshotRow(), physicalPayloadLimit, s.config.SnapshotMaterializationBatchRows}
			if lastKey != nil {
				cursorPredicate = " AND rs.key_bytes > $6"
				limitPlaceholder = "$7"
				queryArgs = []any{userID, userPK, info.tableID, s.maxBytesPerSnapshotRow(), physicalPayloadLimit, lastKey, s.config.SnapshotMaterializationBatchRows}
			}
			query := fmt.Sprintf(`
				WITH page AS MATERIALIZED (
					SELECT rs.key_bytes, rs.bundle_seq
					FROM sync.row_state AS rs
					WHERE rs.user_pk=$2 AND rs.table_id=$3 AND rs.deleted=FALSE%s
					ORDER BY rs.key_bytes
					LIMIT %s
				), projected AS MATERIALIZED (
					SELECT page.key_bytes, page.bundle_seq, %s AS payload_db
					FROM page
					JOIN %s AS src
					  ON src.%s=$1 AND %s=page.key_bytes
				)
				SELECT
					octet_length(key_bytes),
					CASE WHEN octet_length(key_bytes) <= $4 THEN key_bytes END,
					bundle_seq,
					octet_length(payload_db::text),
					CASE WHEN octet_length(payload_db::text) <= $5 THEN payload_db END
				FROM projected
				ORDER BY key_bytes
			`, cursorPredicate, limitPlaceholder, payloadExpression, tableIdent, ownerIdent, businessKeyBytesExpr)
			liveRows, err := tx.Query(ctx, query, queryArgs...)
			if err != nil {
				return 0, 0, highWater, fmt.Errorf("query snapshot page for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			s.snapshotMetrics.materializationPageQueries.Add(1)

			batch := make([]snapshotMaterializedRow, 0, s.config.SnapshotMaterializationBatchRows)
			var batchBytes int64
			for liveRows.Next() {
				var keyBytes, payloadDB []byte
				var keyByteCount, bundleSeq, payloadByteCount int64
				if err := liveRows.Scan(&keyByteCount, &keyBytes, &bundleSeq, &payloadByteCount, &payloadDB); err != nil {
					liveRows.Close()
					return 0, 0, highWater, fmt.Errorf("scan snapshot page: %w", err)
				}
				if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotMaterializedRowScan != nil {
					s.snapshotHooks.afterSnapshotMaterializedRowScan(snapshotPayloadScanObservation{
						ReportedKeyByteCount: keyByteCount, ReportedPayloadByteCount: payloadByteCount,
						KeyMaterialized: keyBytes != nil, PayloadMaterialized: payloadDB != nil,
						MaterializedKeyBytes: len(keyBytes), MaterializedPayloadBytes: len(payloadDB),
					})
				}
				if keyByteCount < 0 || keyByteCount > s.maxBytesPerSnapshotRow() || payloadByteCount < 0 || payloadByteCount > physicalPayloadLimit {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "row_decoded_body_bytes", Actual: max(keyByteCount, payloadByteCount), Limit: s.maxBytesPerSnapshotRow()}
				}
				if keyBytes == nil || payloadDB == nil {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot payload transfer is invalid")
				}
				if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotRowRead != nil {
					if err := s.snapshotHooks.afterSnapshotRowRead(ctx); err != nil {
						liveRows.Close()
						return 0, 0, highWater, err
					}
				}
				if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotCanonicalize != nil {
					if err := s.snapshotHooks.beforeSnapshotCanonicalize(ctx); err != nil {
						liveRows.Close()
						return 0, 0, highWater, err
					}
				}
				payloadWire, err := s.canonicalizeWirePayload(info.schemaName, info.tableName, payloadDB)
				if err != nil {
					liveRows.Close()
					if strings.Contains(err.Error(), "database value must be finite") {
						return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot payload integrity mismatch: database value must be finite")
					}
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot payload is invalid")
				}
				payloadObject, err := jcs.DecodeObjectLossless(payloadWire)
				if err != nil || len(payloadObject) != len(contract.Columns) {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot payload is invalid")
				}
				for _, column := range contract.Columns {
					value, ok := payloadObject[column.Name]
					if !ok || validateGenerationWireValue(column, value) != nil {
						liveRows.Close()
						return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot payload is invalid")
					}
				}
				key, err := wireSyncKeyFromBytes(info, keyBytes)
				if err != nil {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot key is invalid")
				}
				if len(contract.SyncKeyColumns) != 1 {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot key contract is invalid")
				}
				keyName := contract.SyncKeyColumns[0]
				keyValue, keyOK := key[keyName]
				payloadKey, payloadOK := payloadObject[keyName]
				keyCanonical, keyErr := jcs.Marshal(keyValue)
				payloadCanonical, payloadErr := jcs.Marshal(payloadKey)
				if !keyOK || !payloadOK || keyErr != nil || payloadErr != nil || string(keyCanonical) != string(payloadCanonical) {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot key does not match payload")
				}
				if bundleSeq <= 0 || bundleSeq > snapshotBundleSeq {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot row version is outside the pinned checkpoint")
				}
				nextRows := rowCount + int64(len(batch)) + 1
				snapshotRow := SnapshotRow{Schema: info.schemaName, Table: info.tableName, Key: key, RowVersion: bundleSeq, Payload: payloadWire}
				expandedBytes, err := ExpandedRowDecodedBytes(PushRequestRow{
					Schema: snapshotRow.Schema, Table: snapshotRow.Table, Key: snapshotRow.Key,
					Op: OpInsert, BaseRowVersion: snapshotRow.RowVersion, Payload: snapshotRow.Payload,
				})
				if err != nil || expandedBytes > s.maxBytesPerSnapshotRow() {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "row_decoded_body_bytes", Actual: expandedBytes, Limit: s.maxBytesPerSnapshotRow()}
				}
				groups, logicalBytes, err := CompactGroupsForSnapshotRowsAt(wireGeneration, []SnapshotRow{snapshotRow}, nextRows)
				if err != nil {
					liveRows.Close()
					return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot row cannot be compact encoded")
				}
				// false is the longer JSON literal, so this proves that the row can fit
				// even when it is the final row in the snapshot.
				body, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: nextRows, LogicalByteCount: logicalBytes, HasMore: false})
				if err != nil {
					liveRows.Close()
					return 0, 0, highWater, fmt.Errorf("encode compact snapshot body: %w", err)
				}
				if int64(len(body)) > s.maxBytesPerSnapshotChunk() {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "row_decoded_body_bytes", Actual: int64(len(body)), Limit: s.maxBytesPerSnapshotChunk()}
				}
				if len(batch) > 0 && batchBytes+logicalBytes > s.config.SnapshotMaterializationBatchBytes {
					break
				}
				nextBytes := byteCount + batchBytes + logicalBytes
				if limit := s.maxRowsPerSnapshotSession(); limit > 0 && nextRows > limit {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "row_count", Actual: nextRows, Limit: limit}
				}
				if limit := s.maxBytesPerSnapshotSession(); limit > 0 && nextBytes > limit {
					liveRows.Close()
					return 0, 0, highWater, &SnapshotSessionLimitExceededError{Dimension: "logical_byte_count", Actual: nextBytes, Limit: limit}
				}
				batch = append(batch, snapshotMaterializedRow{rowOrdinal: nextRows, tableID: info.tableID, keyBytes: append([]byte(nil), keyBytes...), bundleSeq: bundleSeq, payloadWire: append([]byte(nil), payloadWire...), wireByteCount: logicalBytes})
				batchBytes += logicalBytes
			}
			if err := liveRows.Err(); err != nil {
				liveRows.Close()
				return 0, 0, highWater, fmt.Errorf("iterate snapshot page: %w", err)
			}
			liveRows.Close()
			if len(batch) == 0 {
				break
			}
			copyRows := make([][]any, len(batch))
			for i := range batch {
				r := &batch[i]
				copyRows[i] = []any{snapshotID, r.rowOrdinal, r.tableID, r.keyBytes, r.bundleSeq, string(r.payloadWire), r.wireByteCount}
			}
			if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotCopy != nil {
				if err := s.snapshotHooks.beforeSnapshotCopy(ctx); err != nil {
					return 0, 0, highWater, err
				}
			}
			written, err := tx.CopyFrom(ctx, pgx.Identifier{"sync", "snapshot_session_rows"}, []string{"snapshot_id", "row_ordinal", "table_id", "key_bytes", "bundle_seq", "payload_wire", "wire_byte_count"}, pgx.CopyFromRows(copyRows))
			if err != nil {
				return 0, 0, highWater, fmt.Errorf("COPY snapshot rows: %w", err)
			}
			if written != int64(len(batch)) {
				return 0, 0, highWater, fmt.Errorf("COPY snapshot rows wrote %d, want %d", written, len(batch))
			}
			s.snapshotMetrics.materializationCopyBatches.Add(1)
			highWater.batches++
			rowCount += written
			byteCount += batchBytes
			lastKey = append(lastKey[:0], batch[len(batch)-1].keyBytes...)
			if len(batch) > highWater.rows {
				highWater.rows = len(batch)
			}
			if batchBytes > highWater.bytes {
				highWater.bytes = batchBytes
			}
			if len(batch) < s.config.SnapshotMaterializationBatchRows && batchBytes < s.config.SnapshotMaterializationBatchBytes {
				continue
			}
		}
	}
	var stateCount int64
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_pk=$1
		  AND deleted=FALSE
		  AND table_id=ANY($2::integer[])
	`, userPK, activeTableIDs).Scan(&stateCount); err != nil {
		return 0, 0, highWater, fmt.Errorf("count snapshot row_state: %w", err)
	}
	if rowCount != stateCount {
		return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot row state integrity mismatch")
	}
	// Each registered table count must also be represented by the exact row_state join.
	var allBusinessRows int64
	for _, info := range tableInfos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		ownerIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		var n int64
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s=$1`, tableIdent, ownerIdent), userID).Scan(&n); err != nil {
			return 0, 0, highWater, err
		}
		allBusinessRows += n
	}
	if rowCount != allBusinessRows {
		return 0, 0, highWater, projectionError(GenerationProjectionIntegrityError, "snapshot", "visible snapshot business row integrity mismatch")
	}
	return rowCount, byteCount, highWater, nil
}

func (s *SyncService) GetSnapshotChunk(ctx context.Context, actor Actor, proof SchemaGenerationProof, snapshotID string, afterRowOrdinal int64, maxRows int, maxBytes int64) (_ *SnapshotChunkResponse, err error) {
	if err := rejectRetryableCallbackContext(ctx, "GetSnapshotChunk"); err != nil {
		return nil, err
	}
	response, release, err := s.getSnapshotChunkWithLease(ctx, actor, proof, snapshotID, afterRowOrdinal, maxRows, maxBytes)
	if release != nil {
		defer release()
	}
	return response, err
}

type snapshotChunkObservation struct {
	requestedMaxRows       int
	requestedMaxBytes      int64
	effectiveMaxRows       int
	effectiveMaxBytes      int64
	returnedRows           int64
	returnedBytes          int64
	selectedRowsHighWater  int64
	selectedBytesHighWater int64
	retainedRowsHighWater  int64
	retainedBytesHighWater int64
	selectionStartedAt     time.Time
	admitted               bool
}

func (o *snapshotChunkObservation) finish(s *SyncService, actor Actor, snapshotID string, err error) {
	admissionOutcome := "rejected"
	outcome := "rejected"
	selectionDuration := time.Duration(0)
	if !o.selectionStartedAt.IsZero() {
		selectionDuration = time.Since(o.selectionStartedAt)
		observeAtomicHighWater(&s.snapshotMetrics.chunkSelectionDurationHighWater, selectionDuration.Nanoseconds())
	}
	if o.admitted {
		admissionOutcome = "admitted"
		if err == nil {
			outcome = "completed"
			s.snapshotMetrics.chunkCompletions.Add(1)
		} else {
			outcome = "failed"
			s.snapshotMetrics.chunkFailures.Add(1)
		}
	} else {
		s.snapshotMetrics.chunkRejections.Add(1)
	}
	observeAtomicHighWater(&s.snapshotMetrics.chunkRowsHighWater, o.selectedRowsHighWater)
	observeAtomicHighWater(&s.snapshotMetrics.chunkBytesHighWater, o.selectedBytesHighWater)
	observeAtomicHighWater(&s.snapshotMetrics.chunkRetainedRowsHighWater, o.retainedRowsHighWater)
	observeAtomicHighWater(&s.snapshotMetrics.chunkRetainedBytesHighWater, o.retainedBytesHighWater)

	attrs := []any{
		"user_id", actor.UserID,
		"snapshot_id", snapshotID,
		"requested_max_rows", o.requestedMaxRows,
		"requested_max_bytes", o.requestedMaxBytes,
		"effective_max_rows", o.effectiveMaxRows,
		"effective_max_bytes", o.effectiveMaxBytes,
		"returned_rows", o.returnedRows,
		"returned_bytes", o.returnedBytes,
		"selected_rows_high_water", o.selectedRowsHighWater,
		"selected_bytes_high_water", o.selectedBytesHighWater,
		"retained_rows_high_water", o.retainedRowsHighWater,
		"retained_bytes_high_water", o.retainedBytesHighWater,
		"selection_duration", selectionDuration,
		"admission_outcome", admissionOutcome,
		"outcome", outcome,
		"active_chunk_count_high_water", s.snapshotMetrics.chunkHighWater.Load(),
	}
	if outcome == "failed" {
		attrs = append(attrs, "error", err)
	}
	s.logger.Info("Snapshot chunk selection completed", attrs...)
}

func (s *SyncService) getSnapshotChunkWithLease(ctx context.Context, actor Actor, proof SchemaGenerationProof, snapshotID string, afterRowOrdinal int64, maxRows int, maxBytes int64) (_ *SnapshotChunkResponse, release func(), err error) {
	if err := s.validateClientActor(actor, true); err != nil {
		return nil, nil, err
	}
	if err := validateSchemaGenerationProof(proof); err != nil {
		return nil, nil, err
	}
	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, nil, err
	}
	permitAcquired := false
	var releaseOnce sync.Once
	release = func() {
		releaseOnce.Do(func() {
			if permitAcquired {
				s.releaseSnapshotChunk()
			}
			done()
		})
	}
	succeeded := false
	defer func() {
		if !succeeded {
			release()
		}
	}()
	view, err := s.resolveRequestGeneration(ctx, actor, proof)
	if err != nil {
		return nil, release, err
	}
	selection, err := selectionFromResolvedGeneration(proof, view)
	if err != nil {
		return nil, release, err
	}
	effectiveMaxRows, effectiveMaxBytes, err := s.snapshotChunkLimits(snapshotID, afterRowOrdinal, maxRows, maxBytes)
	if err != nil {
		return nil, release, err
	}
	if err := withOriginTransaction(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		_, _, err := validateSnapshotSessionAccess(ctx, tx, actor, proof, snapshotID)
		return err
	}); err != nil {
		return nil, release, err
	}
	physicalStoredPayloadLimit, err := physicalStoredWirePayloadByteLimit(s.maxBytesPerSnapshotRow())
	if err != nil {
		return nil, release, fmt.Errorf("derive retained snapshot payload transfer limit: %w", err)
	}
	observation := snapshotChunkObservation{
		requestedMaxRows:  maxRows,
		requestedMaxBytes: maxBytes,
		effectiveMaxRows:  effectiveMaxRows,
		effectiveMaxBytes: effectiveMaxBytes,
	}
	s.snapshotMetrics.chunkRequests.Add(1)
	defer func() { observation.finish(s, actor, snapshotID, err) }()
	if !s.tryAcquireSnapshotChunk() {
		return nil, release, &SnapshotCapacityError{Operation: "chunk"}
	}
	permitAcquired = true
	observation.admitted = true
	if s.snapshotHooks != nil && s.snapshotHooks.afterChunkPermit != nil {
		if err := s.snapshotHooks.afterChunkPermit(ctx); err != nil {
			return nil, release, err
		}
	}
	observation.selectionStartedAt = time.Now()

	var resp *SnapshotChunkResponse
	err = withOriginTransaction(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly, DeferrableMode: pgx.NotDeferrable}, func(tx pgx.Tx) error {
		_, snapshotBundleSeq, err := validateSnapshotSessionAccess(ctx, tx, actor, proof, snapshotID)
		if err != nil {
			return err
		}
		if s.snapshotHooks != nil && s.snapshotHooks.afterChunkSessionRead != nil {
			if err := s.snapshotHooks.afterChunkSessionRead(ctx); err != nil {
				return err
			}
		}

		queryRows, err := tx.Query(ctx, `
			SELECT
				row_ordinal,
				table_id,
				octet_length(key_bytes),
				CASE WHEN octet_length(key_bytes) <= $4 THEN key_bytes END,
				bundle_seq,
				octet_length(payload_wire::text),
				CASE WHEN octet_length(payload_wire::text) <= $5 THEN payload_wire END,
				wire_byte_count
			FROM sync.snapshot_session_rows
			WHERE snapshot_id = $1::uuid
			  AND row_ordinal > $2
			ORDER BY row_ordinal
			LIMIT $3
		`, snapshotID, afterRowOrdinal, effectiveMaxRows+1, s.maxBytesPerSnapshotRow(), physicalStoredPayloadLimit)
		if err != nil {
			return fmt.Errorf("query snapshot chunk rows: %w", err)
		}
		s.snapshotMetrics.chunkQueries.Add(1)
		defer queryRows.Close()

		resp = &SnapshotChunkResponse{
			SnapshotID: snapshotID, SnapshotBundleSeq: snapshotBundleSeq, NextRowOrdinal: afterRowOrdinal,
		}
		return s.selectSnapshotChunkRows(ctx, queryRows, selection, resp, effectiveMaxRows, effectiveMaxBytes, physicalStoredPayloadLimit, &observation)
	})
	if err != nil {
		return nil, release, err
	}
	observation.returnedRows = int64(len(resp.Rows))
	observation.returnedBytes = resp.ByteCount
	succeeded = true
	return resp, release, nil
}

func (s *SyncService) DeleteSnapshotSession(ctx context.Context, actor Actor, proof SchemaGenerationProof, snapshotID string) (err error) {
	if err := rejectRetryableCallbackContext(ctx, "DeleteSnapshotSession"); err != nil {
		return err
	}
	if err := s.validateClientActor(actor, true); err != nil {
		return err
	}
	if err := validateSchemaGenerationProof(proof); err != nil {
		return err
	}
	done, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	if _, err := s.resolveRequestGeneration(ctx, actor, proof); err != nil {
		return err
	}
	if snapshotID == "" {
		return &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}

	err = withOriginTransaction(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		if _, _, err := validateSnapshotSessionAccessForUpdate(ctx, tx, actor, proof, snapshotID); err != nil {
			return err
		}
		if s.snapshotHooks != nil && s.snapshotHooks.afterDeleteSessionRead != nil {
			if err := s.snapshotHooks.afterDeleteSessionRead(ctx); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=LEAST(expires_at, transaction_timestamp()) WHERE snapshot_id=$1::uuid`, snapshotID); err != nil {
			return fmt.Errorf("retire snapshot session: %w", err)
		}
		return nil
	})
	if err == nil {
		s.requestSnapshotCleanup("explicit_delete")
	}
	return err
}

func validateSnapshotSessionAccess(ctx context.Context, tx pgx.Tx, actor Actor, proof SchemaGenerationProof, snapshotID string) (int64, int64, error) {
	return validateSnapshotSessionAccessWithLock(ctx, tx, actor, proof, snapshotID, false)
}

func validateSnapshotSessionAccessForUpdate(ctx context.Context, tx pgx.Tx, actor Actor, proof SchemaGenerationProof, snapshotID string) (int64, int64, error) {
	return validateSnapshotSessionAccessWithLock(ctx, tx, actor, proof, snapshotID, true)
}

func validateSnapshotSessionAccessWithLock(ctx context.Context, tx pgx.Tx, actor Actor, proof SchemaGenerationProof, snapshotID string, forUpdate bool) (int64, int64, error) {
	var (
		sessionUserID      string
		sessionUserPK      int64
		sessionSourceID    string
		storedGenerationID int32
		storedFingerprint  string
		snapshotBundleSeq  int64
		expired            bool
	)
	query := `
		SELECT us.user_id, ss.user_pk, ss.source_id, ss.schema_generation_id, ss.schema_generation_fingerprint,
		       ss.snapshot_bundle_seq, ss.expires_at <= transaction_timestamp()
		FROM sync.snapshot_sessions AS ss
		JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
		WHERE ss.snapshot_id = $1::uuid
	`
	if forUpdate {
		query += ` FOR UPDATE OF ss`
	}
	if err := tx.QueryRow(ctx, query, snapshotID).Scan(&sessionUserID, &sessionUserPK, &sessionSourceID, &storedGenerationID, &storedFingerprint, &snapshotBundleSeq, &expired); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
		}
		return 0, 0, fmt.Errorf("query snapshot session: %w", err)
	}
	if sessionUserID != actor.UserID || sessionSourceID != actor.SourceID {
		return 0, 0, &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
	}
	if expired {
		return 0, 0, &SnapshotSessionExpiredError{SnapshotID: snapshotID}
	}
	if err := validateSnapshotSessionGeneration(SchemaGenerationID(storedGenerationID), storedFingerprint, proof); err != nil {
		return 0, 0, err
	}
	source, err := loadSourceStateRow(ctx, tx, sessionUserPK, actor.SourceID, forUpdate)
	if err != nil {
		return 0, 0, err
	}
	binding, err := loadSourceGenerationBindingRow(ctx, tx, sessionUserPK, actor.SourceID, forUpdate)
	if err != nil {
		return 0, 0, err
	}
	if binding == nil {
		return 0, 0, sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "source_generation_binding", "source generation state is corrupt")
	}
	if err := validateBindingRow(binding, proof.ID); err != nil {
		return 0, 0, err
	}
	if source == nil {
		return 0, 0, sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, binding.generationID, "source_state", "source generation state is corrupt")
	}
	if err := requireBindingPair(binding, proof); err != nil {
		return 0, 0, err
	}
	if source.State == sourceStateRetired {
		return 0, 0, &SourceRetiredError{UserID: actor.UserID, SourceID: actor.SourceID, ReplacedBySourceID: source.ReplacedBySourceID}
	}
	return sessionUserPK, snapshotBundleSeq, nil
}
