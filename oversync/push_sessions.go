package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func nullableUUIDString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

type PushSessionInvalidError struct {
	Message string
}

func (e *PushSessionInvalidError) Error() string { return e.Message }

type PushChunkInvalidError struct {
	Message string
}

func (e *PushChunkInvalidError) Error() string { return e.Message }

type PushCommitInvalidError struct {
	Message string
}

func (e *PushCommitInvalidError) Error() string { return e.Message }

type PushSessionNotFoundError struct {
	PushID string
}

func (e *PushSessionNotFoundError) Error() string {
	return fmt.Sprintf("push session %s was not found", e.PushID)
}

type PushSessionExpiredError struct {
	PushID string
}

func (e *PushSessionExpiredError) Error() string {
	return fmt.Sprintf("push session %s has expired; start a new push session", e.PushID)
}

type PushSessionForbiddenError struct {
	PushID string
}

func (e *PushSessionForbiddenError) Error() string {
	return fmt.Sprintf("push session %s does not belong to the authenticated user", e.PushID)
}

type PushChunkOutOfOrderError struct {
	PushID   string
	Expected int64
	Actual   int64
}

func (e *PushChunkOutOfOrderError) Error() string {
	return fmt.Sprintf("push session %s expected start_row_ordinal %d, got %d", e.PushID, e.Expected, e.Actual)
}

type CommittedBundleChunkInvalidError struct {
	Message string
}

func (e *CommittedBundleChunkInvalidError) Error() string { return e.Message }

type CommittedBundleNotFoundError struct {
	BundleSeq int64
}

func (e *CommittedBundleNotFoundError) Error() string {
	return fmt.Sprintf("committed bundle %d was not found", e.BundleSeq)
}

type SourceTupleHistoryPrunedError struct {
	UserID                         string
	SourceID                       string
	SourceBundleID                 int64
	MaxCommittedSourceBundleIDHint int64
}

func (e *SourceTupleHistoryPrunedError) Error() string {
	return fmt.Sprintf(
		"source tuple (%s, %d) for user %s is older than retained duplicate history; max committed source_bundle_id is %d",
		e.SourceID,
		e.SourceBundleID,
		e.UserID,
		e.MaxCommittedSourceBundleIDHint,
	)
}

type SourceSequenceOutOfOrderError struct {
	UserID   string
	SourceID string
	Expected int64
	Actual   int64
}

func (e *SourceSequenceOutOfOrderError) Error() string {
	return fmt.Sprintf(
		"source %s for user %s expected next source_bundle_id %d, got %d",
		e.SourceID,
		e.UserID,
		e.Expected,
		e.Actual,
	)
}

type SourceSequenceChangedError struct {
	UserID   string
	SourceID string
	Expected int64
	Actual   int64
}

func (e *SourceSequenceChangedError) Error() string {
	return fmt.Sprintf(
		"source %s for user %s changed expected next source_bundle_id from staged %d to %d before commit",
		e.SourceID,
		e.UserID,
		e.Actual,
		e.Expected,
	)
}

type committedBundleMeta struct {
	BundleSeq      int64
	SourceID       string
	SourceBundleID int64
	RowCount       int64
	BundleHash     string
}

type pushSessionState struct {
	PushID                 string
	UserPK                 int64
	UserID                 string
	SourceID               string
	SourceBundleID         int64
	PlannedRowCount        int64
	NextExpectedRowOrdinal int64
	InitializationID       string
	ExpiresAt              time.Time
}

func (s *SyncService) CreatePushSession(ctx context.Context, actor Actor, req *PushSessionCreateRequest) (_ *PushSessionCreateResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, &PushSessionInvalidError{Message: "push session request is required"}
	}
	if req.SourceBundleID <= 0 {
		return nil, &PushSessionInvalidError{Message: "source_bundle_id must be > 0"}
	}
	if req.PlannedRowCount <= 0 {
		return nil, &PushSessionInvalidError{Message: "planned_row_count must be > 0"}
	}
	req.InitializationID = strings.TrimSpace(req.InitializationID)

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var resp *PushSessionCreateResponse
	err = runRetryableTx(ctx, 3, 25*time.Millisecond, func() error {
		resp = nil
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if err := s.configureUploadTx(ctx, tx); err != nil {
				return err
			}
			if err := cleanupExpiredPushSessionsQuerier(ctx, tx); err != nil {
				return err
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
					return &PushSessionInvalidError{Message: "initialization_id is required while the scope is initializing"}
				}
				refreshUntil := time.Now().UTC().Add(s.initializationLeaseTTL())
				leasedState, err := requireActiveInitializationLease(ctx, tx, actor.UserID, actor.SourceID, req.InitializationID, refreshUntil)
				if err != nil {
					return err
				}
				initializationID = leasedState.InitializationID
			case scopeStateInitialized:
				if req.InitializationID != "" {
					return &PushSessionInvalidError{Message: "initialization_id must be absent once the scope is initialized"}
				}
			default:
				return fmt.Errorf("unexpected scope state %q", scopeState.State)
			}

			meta, err := loadCommittedPushMetadataBySourceTuple(ctx, tx, scopeState.UserPK, actor.SourceID, req.SourceBundleID)
			if err != nil {
				return err
			}
			if meta != nil {
				resp = &PushSessionCreateResponse{
					Status:         "already_committed",
					BundleSeq:      meta.BundleSeq,
					SourceID:       meta.SourceID,
					SourceBundleID: meta.SourceBundleID,
					RowCount:       meta.RowCount,
					BundleHash:     meta.BundleHash,
				}
				return nil
			}

			expectedSourceBundleID, maxCommittedSourceBundleID, err := loadNextExpectedSourceBundleID(ctx, tx, scopeState.UserPK, actor.UserID, actor.SourceID)
			if err != nil {
				return err
			}
			switch {
			case req.SourceBundleID < expectedSourceBundleID:
				return &SourceTupleHistoryPrunedError{
					UserID:                         actor.UserID,
					SourceID:                       actor.SourceID,
					SourceBundleID:                 req.SourceBundleID,
					MaxCommittedSourceBundleIDHint: maxCommittedSourceBundleID,
				}
			case req.SourceBundleID > expectedSourceBundleID:
				return &SourceSequenceOutOfOrderError{
					UserID:   actor.UserID,
					SourceID: actor.SourceID,
					Expected: expectedSourceBundleID,
					Actual:   req.SourceBundleID,
				}
			}

			if _, err := tx.Exec(ctx, `
				DELETE FROM sync.push_sessions
				WHERE user_pk = $1
				  AND source_id = $2
				  AND source_bundle_id = $3
			`, scopeState.UserPK, actor.SourceID, req.SourceBundleID); err != nil {
				return fmt.Errorf("delete existing staging push session: %w", err)
			}

			pushID := uuid.NewString()
			expiresAt := time.Now().UTC().Add(s.pushSessionTTL())
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.push_sessions (
					push_id, user_pk, source_id, source_bundle_id, planned_row_count, next_expected_row_ordinal, initialization_id, expires_at
				) VALUES ($1::uuid, $2, $3, $4, $5, 0, $6::uuid, $7)
			`, pushID, scopeState.UserPK, actor.SourceID, req.SourceBundleID, req.PlannedRowCount, nullableUUIDString(initializationID), expiresAt); err != nil {
				return fmt.Errorf("insert push session: %w", err)
			}

			resp = &PushSessionCreateResponse{
				PushID:                 pushID,
				Status:                 "staging",
				PlannedRowCount:        req.PlannedRowCount,
				NextExpectedRowOrdinal: 0,
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SyncService) UploadPushChunk(ctx context.Context, actor Actor, pushID string, req *PushSessionChunkRequest) (_ *PushSessionChunkResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, &PushChunkInvalidError{Message: "push chunk request is required"}
	}
	if req.StartRowOrdinal < 0 {
		return nil, &PushChunkInvalidError{Message: "start_row_ordinal must be >= 0"}
	}
	if len(req.Rows) == 0 {
		return nil, &PushChunkInvalidError{Message: "rows must not be empty"}
	}
	if len(req.Rows) > s.maxRowsPerPushChunk() {
		return nil, &PushChunkInvalidError{Message: fmt.Sprintf("rows length %d exceeds max_rows_per_push_chunk %d", len(req.Rows), s.maxRowsPerPushChunk())}
	}

	preparedRows, err := s.preparePushRowsPreservingInput(req.Rows)
	if err != nil {
		var validationErr *PushValidationError
		if errors.As(err, &validationErr) {
			return nil, &PushChunkInvalidError{Message: validationErr.Error()}
		}
		return nil, err
	}

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var resp *PushSessionChunkResponse
	err = runRetryableTx(ctx, 3, 25*time.Millisecond, func() error {
		resp = nil
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if err := s.configureUploadTx(ctx, tx); err != nil {
				return err
			}
			session, err := loadPushSessionForUpdate(ctx, tx, pushID)
			if err != nil {
				return err
			}
			if session.UserID != actor.UserID {
				return &PushSessionForbiddenError{PushID: pushID}
			}
			if time.Now().UTC().After(session.ExpiresAt) {
				return &PushSessionExpiredError{PushID: pushID}
			}
			if session.InitializationID != "" {
				refreshUntil := time.Now().UTC().Add(s.initializationLeaseTTL())
				if _, err := requireActiveInitializationLease(ctx, tx, actor.UserID, session.SourceID, session.InitializationID, refreshUntil); err != nil {
					return err
				}
			}

			if req.StartRowOrdinal != session.NextExpectedRowOrdinal {
				return &PushChunkOutOfOrderError{PushID: pushID, Expected: session.NextExpectedRowOrdinal, Actual: req.StartRowOrdinal}
			}
			if req.StartRowOrdinal+int64(len(preparedRows)) > session.PlannedRowCount {
				return &PushChunkInvalidError{Message: "chunk exceeds planned_row_count"}
			}

			rowsData := make([][]any, 0, len(preparedRows))
			for idx, row := range preparedRows {
				opCode, err := opCodeFromString(row.op)
				if err != nil {
					return err
				}
				rowOrdinal := req.StartRowOrdinal + int64(idx)
				var payload any
				if opCode != opCodeDelete {
					payload = string(row.payload)
				}
				rowsData = append(rowsData, []any{
					pushID,
					rowOrdinal,
					row.tableID,
					row.keyBytes,
					opCode,
					row.baseRowVersion,
					payload,
				})
			}
			if _, err := tx.CopyFrom(ctx,
				pgx.Identifier{"sync", "push_session_rows"},
				[]string{"push_id", "row_ordinal", "table_id", "key_bytes", "op_code", "base_bundle_seq", "payload_apply"},
				pgx.CopyFromRows(rowsData),
			); err != nil {
				return fmt.Errorf("insert push session rows: %w", err)
			}
			if _, err := tx.Exec(ctx, `
			UPDATE sync.push_sessions
			SET next_expected_row_ordinal = $2,
				expires_at = $3
			WHERE push_id = $1::uuid
		`, pushID, req.StartRowOrdinal+int64(len(preparedRows)), time.Now().UTC().Add(s.pushSessionTTL())); err != nil {
				return fmt.Errorf("refresh push session expiry: %w", err)
			}
			resp = &PushSessionChunkResponse{
				PushID:                 pushID,
				NextExpectedRowOrdinal: req.StartRowOrdinal + int64(len(preparedRows)),
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SyncService) CommitPushSession(ctx context.Context, actor Actor, pushID string) (_ *PushSessionCommitResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return nil, err
	}

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var resp *PushSessionCommitResponse
	err = runRetryableTx(ctx, 3, 25*time.Millisecond, func() error {
		resp = nil
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if err := s.configureUploadTx(ctx, tx); err != nil {
				return err
			}
			session, err := loadPushSessionForUpdate(ctx, tx, pushID)
			if err != nil {
				return err
			}
			if session.UserID != actor.UserID {
				return &PushSessionForbiddenError{PushID: pushID}
			}
			if time.Now().UTC().After(session.ExpiresAt) {
				return &PushSessionExpiredError{PushID: pushID}
			}

			rows, err := s.loadPushSessionPreparedRows(ctx, tx, pushID)
			if err != nil {
				return err
			}
			if session.NextExpectedRowOrdinal != session.PlannedRowCount || int64(len(rows)) != session.PlannedRowCount {
				return &PushCommitInvalidError{Message: fmt.Sprintf("staged row count %d does not match planned_row_count %d", len(rows), session.PlannedRowCount)}
			}

			seenTargets := make(map[string]struct{}, len(rows))
			for idx, row := range rows {
				if row.inputOrder != idx {
					return &PushCommitInvalidError{Message: fmt.Sprintf("staged rows are not contiguous at row_ordinal %d", idx)}
				}
				targetKey := string(appendInt32BigEndian(append([]byte(nil), row.keyBytes...), row.tableID))
				if _, exists := seenTargets[targetKey]; exists {
					return &PushCommitInvalidError{Message: fmt.Sprintf("duplicate target row in staged push session: %s.%s %s", row.schema, row.table, row.keyString)}
				}
				seenTargets[targetKey] = struct{}{}
			}

			if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
				return fmt.Errorf("defer push constraints: %w", err)
			}
			if session.InitializationID != "" {
				refreshUntil := time.Now().UTC().Add(s.initializationLeaseTTL())
				if _, err := requireActiveInitializationLease(ctx, tx, actor.UserID, session.SourceID, session.InitializationID, refreshUntil); err != nil {
					return err
				}
				if err := ensureUserStateBaselineWithExec(ctx, tx, actor.UserID); err != nil {
					return err
				}
			} else {
				if err := ensureUserStatePresent(ctx, tx, actor.UserID); err != nil {
					return err
				}
			}

			expectedSourceBundleID, _, err := loadNextExpectedSourceBundleIDForUpdate(ctx, tx, session.UserPK, session.UserID, session.SourceID)
			if err != nil {
				return err
			}
			if session.SourceBundleID != expectedSourceBundleID {
				return &SourceSequenceChangedError{
					UserID:   actor.UserID,
					SourceID: session.SourceID,
					Expected: expectedSourceBundleID,
					Actual:   session.SourceBundleID,
				}
			}
			if err := setBundleTxContext(ctx, tx, bundleTxContext{
				UserID:           actor.UserID,
				UserPK:           session.UserPK,
				SourceID:         session.SourceID,
				SourceBundleID:   session.SourceBundleID,
				InitializationID: session.InitializationID,
			}); err != nil {
				return err
			}

			rowStates, err := loadRowStateSnapshots(ctx, tx, session.UserPK, rows)
			if err != nil {
				return err
			}
			for i, row := range rows {
				var state *rowStateSnapshot
				if rowStates[i].found {
					state = &rowStates[i].rowStateSnapshot
				}
				if err := s.validatePushRowConflict(ctx, tx, actor.UserID, row, state); err != nil {
					return err
				}
			}

			for _, group := range batchPreparedPushRows(rows) {
				if err := applyPreparedPushRowBatch(ctx, tx, actor.UserID, group); err != nil {
					return err
				}
			}

			bundle, err := s.finalizeCapturedBundle(ctx, tx, actor, session.UserPK, BundleSource{
				SourceID:       session.SourceID,
				SourceBundleID: session.SourceBundleID,
			})
			if err != nil {
				return err
			}
			if bundle == nil {
				return &PushCommitInvalidError{Message: "push session produced no captured business-table effects"}
			}

			if err := activateSourceState(ctx, tx, session.UserPK, session.UserID, session.SourceID, session.SourceBundleID); err != nil {
				return err
			}
			if err := s.applyRetentionPolicyForUser(ctx, tx, session.UserPK); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx, `
			DELETE FROM sync.push_sessions
			WHERE push_id = $1::uuid
		`, pushID); err != nil {
				return fmt.Errorf("delete committed push session: %w", err)
			}
			if session.InitializationID != "" {
				if err := transitionScopeToInitialized(ctx, tx, actor.UserID, session.SourceID); err != nil {
					return err
				}
			}

			resp = &PushSessionCommitResponse{
				BundleSeq:      bundle.BundleSeq,
				SourceID:       bundle.SourceID,
				SourceBundleID: bundle.SourceBundleID,
				RowCount:       bundle.RowCount,
				BundleHash:     bundle.BundleHash,
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SyncService) GetCommittedBundleRows(ctx context.Context, actor Actor, bundleSeq int64, afterRowOrdinal *int64, maxRows int) (_ *CommittedBundleRowsResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return nil, err
	}
	if bundleSeq <= 0 {
		return nil, &CommittedBundleChunkInvalidError{Message: "bundle_seq must be > 0"}
	}
	if afterRowOrdinal != nil && *afterRowOrdinal < 0 {
		return nil, &CommittedBundleChunkInvalidError{Message: "after_row_ordinal must be >= 0"}
	}
	if maxRows <= 0 {
		return nil, &CommittedBundleChunkInvalidError{Message: "max_rows must be > 0"}
	}
	effectiveMaxRows := maxRows
	if effectiveMaxRows > s.maxRowsPerCommittedBundleChunk() {
		effectiveMaxRows = s.maxRowsPerCommittedBundleChunk()
	}

	var resp *CommittedBundleRowsResponse
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		meta, err := loadCommittedBundleMeta(ctx, tx, actor.UserID, bundleSeq)
		if err != nil {
			return err
		}
		userPK, err := lookupUserPK(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}

		logicalAfter := int64(-1)
		if afterRowOrdinal != nil {
			logicalAfter = *afterRowOrdinal
		}
		storageAfter := logicalAfter + 1

		queryRows, err := tx.Query(ctx, `
			SELECT row_ordinal, table_id, key_bytes, op_code, payload_wire
			FROM sync.bundle_rows
			WHERE user_pk = $1
			  AND bundle_seq = $2
			  AND row_ordinal > $3
			ORDER BY row_ordinal
			LIMIT $4
		`, userPK, bundleSeq, storageAfter, effectiveMaxRows+1)
		if err != nil {
			return fmt.Errorf("query committed bundle rows: %w", err)
		}
		defer queryRows.Close()

		rows := make([]BundleRow, 0, effectiveMaxRows+1)
		for queryRows.Next() {
			var (
				row         BundleRow
				tableID     int32
				keyBytes    []byte
				opCode      int16
				payloadWire []byte
			)
			if err := queryRows.Scan(new(int64), &tableID, &keyBytes, &opCode, &payloadWire); err != nil {
				return fmt.Errorf("scan committed bundle row: %w", err)
			}
			info, err := s.tableInfoForID(tableID)
			if err != nil {
				return err
			}
			row.Schema = info.schemaName
			row.Table = info.tableName
			row.Key, err = wireSyncKeyFromBytes(info, keyBytes)
			if err != nil {
				return fmt.Errorf("decode committed bundle row key: %w", err)
			}
			row.Op, err = opStringFromCode(opCode)
			if err != nil {
				return err
			}
			row.RowVersion = bundleSeq
			row.Payload = payloadWire
			rows = append(rows, row)
		}
		if err := queryRows.Err(); err != nil {
			return fmt.Errorf("iterate committed bundle rows: %w", err)
		}

		hasMore := false
		if len(rows) > effectiveMaxRows {
			hasMore = true
			rows = rows[:effectiveMaxRows]
		}
		nextRowOrdinal := int64(0)
		if len(rows) == 0 {
			if afterRowOrdinal != nil {
				nextRowOrdinal = *afterRowOrdinal
			}
		} else {
			nextRowOrdinal = logicalAfter + int64(len(rows))
		}

		resp = &CommittedBundleRowsResponse{
			BundleSeq:      meta.BundleSeq,
			SourceID:       meta.SourceID,
			SourceBundleID: meta.SourceBundleID,
			RowCount:       meta.RowCount,
			BundleHash:     meta.BundleHash,
			Rows:           rows,
			NextRowOrdinal: nextRowOrdinal,
			HasMore:        hasMore,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SyncService) DeletePushSession(ctx context.Context, actor Actor, pushID string) error {
	done, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return err
	}

	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		session, err := loadPushSessionForUpdate(ctx, tx, pushID)
		if err != nil {
			return err
		}
		if session.UserID != actor.UserID {
			return &PushSessionForbiddenError{PushID: pushID}
		}
		if time.Now().UTC().After(session.ExpiresAt) {
			return &PushSessionExpiredError{PushID: pushID}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM sync.push_sessions WHERE push_id = $1::uuid`, pushID); err != nil {
			return fmt.Errorf("delete push session: %w", err)
		}
		return nil
	})
}

func loadPushSessionForUpdate(ctx context.Context, tx pgx.Tx, pushID string) (*pushSessionState, error) {
	if _, err := uuid.Parse(strings.TrimSpace(pushID)); err != nil {
		return nil, &PushSessionNotFoundError{PushID: pushID}
	}

	var session pushSessionState
	if err := tx.QueryRow(ctx, `
		SELECT
			ps.push_id::text,
			ps.user_pk,
			us.user_id,
			ps.source_id,
			ps.source_bundle_id,
			ps.planned_row_count,
			ps.next_expected_row_ordinal,
			COALESCE(ps.initialization_id::text, ''),
			ps.expires_at
		FROM sync.push_sessions ps
		JOIN sync.user_state us ON us.user_pk = ps.user_pk
		WHERE ps.push_id = $1::uuid
		FOR UPDATE
	`, pushID).Scan(
		&session.PushID,
		&session.UserPK,
		&session.UserID,
		&session.SourceID,
		&session.SourceBundleID,
		&session.PlannedRowCount,
		&session.NextExpectedRowOrdinal,
		&session.InitializationID,
		&session.ExpiresAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &PushSessionNotFoundError{PushID: pushID}
		}
		return nil, fmt.Errorf("query push session: %w", err)
	}
	return &session, nil
}

func loadCommittedPushMetadataBySourceTuple(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, sourceBundleID int64) (*committedBundleMeta, error) {
	var meta committedBundleMeta
	var bundleHash []byte
	if err := tx.QueryRow(ctx, `
		SELECT bl.bundle_seq, bl.source_id, bl.source_bundle_id, bl.row_count, bl.bundle_hash
		FROM sync.bundle_log AS bl
		JOIN sync.user_state AS us ON us.user_pk = bl.user_pk
		WHERE bl.user_pk = $1
		  AND bl.source_id = $2
		  AND bl.source_bundle_id = $3
		  AND bl.bundle_seq > us.retained_bundle_floor
	`, userPK, sourceID, sourceBundleID).Scan(&meta.BundleSeq, &meta.SourceID, &meta.SourceBundleID, &meta.RowCount, &bundleHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query committed push metadata: %w", err)
	}
	meta.BundleHash = renderBundleHash(bundleHash)
	return &meta, nil
}

func loadNextExpectedSourceBundleID(ctx context.Context, tx pgx.Tx, userPK int64, userID, sourceID string) (int64, int64, error) {
	state, err := loadSourceStateRow(ctx, tx, userPK, sourceID, false)
	if err != nil {
		return 0, 0, fmt.Errorf("query source_state next expected bundle for %s: %w", sourceID, err)
	}
	if state == nil {
		return 1, 0, nil
	}
	if state.State == sourceStateRetired {
		return 0, 0, &SourceRetiredError{
			UserID:             userID,
			SourceID:           sourceID,
			ReplacedBySourceID: state.ReplacedBySourceID,
		}
	}
	return state.MaxCommittedSourceBundleID + 1, state.MaxCommittedSourceBundleID, nil
}

func loadNextExpectedSourceBundleIDForUpdate(ctx context.Context, tx pgx.Tx, userPK int64, userID, sourceID string) (int64, int64, error) {
	state, err := loadSourceStateRow(ctx, tx, userPK, sourceID, true)
	if err != nil {
		return 0, 0, fmt.Errorf("query source_state next expected bundle for update %s: %w", sourceID, err)
	}
	if state == nil {
		return 1, 0, nil
	}
	if state.State == sourceStateRetired {
		return 0, 0, &SourceRetiredError{
			UserID:             userID,
			SourceID:           sourceID,
			ReplacedBySourceID: state.ReplacedBySourceID,
		}
	}
	return state.MaxCommittedSourceBundleID + 1, state.MaxCommittedSourceBundleID, nil
}

func loadCommittedBundleMeta(ctx context.Context, tx pgx.Tx, userID string, bundleSeq int64) (*committedBundleMeta, error) {
	userPK, err := lookupUserPK(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	retainedState, err := loadRetainedHistoryStateByUserPK(ctx, tx, userPK)
	if err != nil {
		return nil, err
	}
	if retainedState != nil {
		if err := enforceRetainedBundleFloor(userID, bundleSeq, retainedState.RetainedFloor); err != nil {
			return nil, err
		}
	}
	var meta committedBundleMeta
	var bundleHash []byte
	if err := tx.QueryRow(ctx, `
		SELECT bundle_seq, source_id, source_bundle_id, row_count, bundle_hash
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq = $2
	`, userPK, bundleSeq).Scan(&meta.BundleSeq, &meta.SourceID, &meta.SourceBundleID, &meta.RowCount, &bundleHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &CommittedBundleNotFoundError{BundleSeq: bundleSeq}
		}
		return nil, fmt.Errorf("query committed bundle metadata: %w", err)
	}
	meta.BundleHash = renderBundleHash(bundleHash)
	return &meta, nil
}

func (s *SyncService) loadPushSessionPreparedRows(ctx context.Context, tx pgx.Tx, pushID string) ([]pushPreparedRow, error) {
	queryRows, err := tx.Query(ctx, `
		SELECT row_ordinal, table_id, key_bytes, op_code, base_bundle_seq, payload_apply
		FROM sync.push_session_rows
		WHERE push_id = $1::uuid
		ORDER BY row_ordinal
	`, pushID)
	if err != nil {
		return nil, fmt.Errorf("query push session rows: %w", err)
	}
	defer queryRows.Close()

	rows := make([]pushPreparedRow, 0)
	for queryRows.Next() {
		var (
			rowOrdinal int64
			row        pushPreparedRow
			payload    []byte
			opCode     int16
		)
		if err := queryRows.Scan(&rowOrdinal, &row.tableID, &row.keyBytes, &opCode, &row.baseRowVersion, &payload); err != nil {
			return nil, fmt.Errorf("scan push session row: %w", err)
		}
		info, err := s.tableInfoForID(row.tableID)
		if err != nil {
			return nil, err
		}
		row.schema = info.schemaName
		row.table = info.tableName
		normalizedKey, err := decodeNormalizedStoredSyncKeyBytes(row.schema, row.table, info, row.keyBytes)
		if err != nil {
			return nil, err
		}
		row.op, err = opStringFromCode(opCode)
		if err != nil {
			return nil, err
		}
		row.keyColumn = normalizedKey.column
		row.keyType = normalizedKey.keyType
		row.keyString = normalizedKey.keyString
		row.keyValue = normalizedKey.dbValue
		row.keyBytes = normalizedKey.keyBytes
		row.payload = payload
		if row.op != OpDelete {
			var payloadObj map[string]any
			if err := json.Unmarshal(payload, &payloadObj); err != nil {
				return nil, fmt.Errorf("decode push session payload for %s.%s: %w", row.schema, row.table, err)
			}
			for col := range payloadObj {
				row.payloadColumns = append(row.payloadColumns, strings.ToLower(col))
			}
			sort.Strings(row.payloadColumns)
		}
		row.inputOrder = int(rowOrdinal)
		rows = append(rows, row)
	}
	if err := queryRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate push session rows: %w", err)
	}
	return rows, nil
}

func cleanupExpiredPushSessionsQuerier(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	if _, err := q.Exec(ctx, `DELETE FROM sync.push_sessions WHERE expires_at <= now()`); err != nil {
		return fmt.Errorf("cleanup expired push sessions: %w", err)
	}
	return nil
}
