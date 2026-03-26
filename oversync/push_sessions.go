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

type committedBundleMeta struct {
	BundleSeq      int64
	SourceID       string
	SourceBundleID int64
	RowCount       int64
	BundleHash     string
}

type pushSessionState struct {
	PushID          string
	UserID          string
	SourceID        string
	SourceBundleID  int64
	PlannedRowCount int64
	Status          string
	BundleSeq       int64
	RowCount        int64
	BundleHash      string
	ExpiresAt       time.Time
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
	if strings.TrimSpace(req.SourceID) == "" {
		return nil, &PushSessionInvalidError{Message: "source_id is required"}
	}
	if req.SourceID != actor.SourceID {
		return nil, &PushSessionInvalidError{Message: "request source_id must match authenticated actor"}
	}
	if req.SourceBundleID <= 0 {
		return nil, &PushSessionInvalidError{Message: "source_bundle_id must be > 0"}
	}
	if req.PlannedRowCount <= 0 {
		return nil, &PushSessionInvalidError{Message: "planned_row_count must be > 0"}
	}

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var resp *PushSessionCreateResponse
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if err := s.configureUploadTx(ctx, tx); err != nil {
			return err
		}
		if err := cleanupExpiredPushSessionsQuerier(ctx, tx); err != nil {
			return err
		}

		meta, err := loadAppliedPushMetadata(ctx, tx, actor.UserID, req.SourceID, req.SourceBundleID)
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

		if _, err := tx.Exec(ctx, `
			DELETE FROM sync.push_sessions
			WHERE user_id = $1
			  AND source_id = $2
			  AND source_bundle_id = $3
			  AND status = 'staging'
		`, actor.UserID, req.SourceID, req.SourceBundleID); err != nil {
			return fmt.Errorf("delete existing staging push session: %w", err)
		}

		pushID := uuid.NewString()
		expiresAt := time.Now().UTC().Add(s.pushSessionTTL())
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.push_sessions (
				push_id, user_id, source_id, source_bundle_id, planned_row_count, status, expires_at
			) VALUES ($1::uuid, $2, $3, $4, $5, 'staging', $6)
		`, pushID, actor.UserID, req.SourceID, req.SourceBundleID, req.PlannedRowCount, expiresAt); err != nil {
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
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
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
		if session.Status != "staging" {
			return &PushChunkInvalidError{Message: fmt.Sprintf("push session %s is already committed", pushID)}
		}

		var nextExpected int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(COUNT(*), 0)
			FROM sync.push_session_rows
			WHERE push_id = $1::uuid
		`, pushID).Scan(&nextExpected); err != nil {
			return fmt.Errorf("count push session rows: %w", err)
		}
		if req.StartRowOrdinal != nextExpected {
			return &PushChunkOutOfOrderError{PushID: pushID, Expected: nextExpected, Actual: req.StartRowOrdinal}
		}
		if req.StartRowOrdinal+int64(len(preparedRows)) > session.PlannedRowCount {
			return &PushChunkInvalidError{Message: "chunk exceeds planned_row_count"}
		}

		rowsData := make([][]any, 0, len(preparedRows))
		for idx, row := range preparedRows {
			rowOrdinal := req.StartRowOrdinal + int64(idx)
			var payload any
			if row.op != OpDelete {
				payload = string(row.payload)
			}
			rowsData = append(rowsData, []any{
				pushID,
				rowOrdinal,
				row.schema,
				row.table,
				row.keyJSON,
				row.op,
				row.baseRowVersion,
				payload,
			})
		}
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"sync", "push_session_rows"},
			[]string{"push_id", "row_ordinal", "schema_name", "table_name", "key_json", "op", "base_row_version", "payload"},
			pgx.CopyFromRows(rowsData),
		); err != nil {
			return fmt.Errorf("insert push session rows: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sync.push_sessions
			SET expires_at = $2
			WHERE push_id = $1::uuid
		`, pushID, time.Now().UTC().Add(s.pushSessionTTL())); err != nil {
			return fmt.Errorf("refresh push session expiry: %w", err)
		}
		resp = &PushSessionChunkResponse{
			PushID:                 pushID,
			NextExpectedRowOrdinal: req.StartRowOrdinal + int64(len(preparedRows)),
		}
		return nil
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
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
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
		if session.Status == "committed" {
			resp = &PushSessionCommitResponse{
				BundleSeq:      session.BundleSeq,
				SourceID:       session.SourceID,
				SourceBundleID: session.SourceBundleID,
				RowCount:       session.RowCount,
				BundleHash:     session.BundleHash,
			}
			return nil
		}

		rows, err := s.loadPushSessionPreparedRows(ctx, tx, pushID)
		if err != nil {
			return err
		}
		if int64(len(rows)) != session.PlannedRowCount {
			return &PushCommitInvalidError{Message: fmt.Sprintf("staged row count %d does not match planned_row_count %d", len(rows), session.PlannedRowCount)}
		}

		seenTargets := make(map[string]struct{}, len(rows))
		for idx, row := range rows {
			if row.inputOrder != idx {
				return &PushCommitInvalidError{Message: fmt.Sprintf("staged rows are not contiguous at row_ordinal %d", idx)}
			}
			targetKey := row.schema + "\x00" + row.table + "\x00" + row.keyJSON
			if _, exists := seenTargets[targetKey]; exists {
				return &PushCommitInvalidError{Message: fmt.Sprintf("duplicate target row in staged push session: %s.%s %s", row.schema, row.table, row.keyString)}
			}
			seenTargets[targetKey] = struct{}{}
		}

		if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
			return fmt.Errorf("defer push constraints: %w", err)
		}
		if err := ensureUserStatePresent(ctx, tx, actor.UserID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('oversync.bundle_user_id', $1, true)`, actor.UserID); err != nil {
			return fmt.Errorf("set push bundle user_id: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('oversync.bundle_source_id', $1, true)`, session.SourceID); err != nil {
			return fmt.Errorf("set push bundle source_id: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('oversync.bundle_source_bundle_id', $1, true)`, fmt.Sprintf("%d", session.SourceBundleID)); err != nil {
			return fmt.Errorf("set push bundle source_bundle_id: %w", err)
		}

		rowStates, err := loadRowStateSnapshots(ctx, tx, actor.UserID, rows)
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

		bundle, err := s.finalizeCapturedBundle(ctx, tx, actor, BundleSource{
			SourceID:       session.SourceID,
			SourceBundleID: session.SourceBundleID,
		})
		if err != nil {
			return err
		}
		if bundle == nil {
			return &PushCommitInvalidError{Message: "push session produced no captured business-table effects"}
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.applied_pushes (
				user_id, source_id, source_bundle_id, bundle_seq, row_count, bundle_hash, committed_at, recorded_at
			) VALUES ($1, $2, $3, $4, $5, $6, now(), now())
			ON CONFLICT (user_id, source_id, source_bundle_id) DO UPDATE
			SET bundle_seq = EXCLUDED.bundle_seq,
				row_count = EXCLUDED.row_count,
				bundle_hash = EXCLUDED.bundle_hash,
				committed_at = EXCLUDED.committed_at,
				recorded_at = EXCLUDED.recorded_at
		`, actor.UserID, session.SourceID, session.SourceBundleID, bundle.BundleSeq, bundle.RowCount, bundle.BundleHash); err != nil {
			return fmt.Errorf("record applied push replay key: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE sync.push_sessions
			SET status = 'committed',
				bundle_seq = $2,
				row_count = $3,
				bundle_hash = $4
			WHERE push_id = $1::uuid
		`, pushID, bundle.BundleSeq, bundle.RowCount, bundle.BundleHash); err != nil {
			return fmt.Errorf("mark push session committed: %w", err)
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

		logicalAfter := int64(-1)
		if afterRowOrdinal != nil {
			logicalAfter = *afterRowOrdinal
		}
		storageAfter := logicalAfter + 1

		queryRows, err := tx.Query(ctx, `
			SELECT row_ordinal, schema_name, table_name, key_json, op, row_version, payload
			FROM sync.bundle_rows
			WHERE user_id = $1
			  AND bundle_seq = $2
			  AND row_ordinal > $3
			ORDER BY row_ordinal
			LIMIT $4
		`, actor.UserID, bundleSeq, storageAfter, effectiveMaxRows+1)
		if err != nil {
			return fmt.Errorf("query committed bundle rows: %w", err)
		}
		defer queryRows.Close()

		rows := make([]BundleRow, 0, effectiveMaxRows+1)
		for queryRows.Next() {
			var row BundleRow
			var keyJSON string
			if err := queryRows.Scan(new(int64), &row.Schema, &row.Table, &keyJSON, &row.Op, &row.RowVersion, &row.Payload); err != nil {
				return fmt.Errorf("scan committed bundle row: %w", err)
			}
			key, err := decodeSyncKeyJSON(keyJSON)
			if err != nil {
				return fmt.Errorf("decode committed bundle row key: %w", err)
			}
			row.Key = key
			if row.Op != OpDelete {
				row.Payload, err = s.canonicalizeWirePayload(row.Schema, row.Table, row.Payload)
				if err != nil {
					return fmt.Errorf("canonicalize committed bundle row payload for %s.%s: %w", row.Schema, row.Table, err)
				}
			}
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
		if session.Status != "staging" {
			return &PushChunkInvalidError{Message: fmt.Sprintf("push session %s is already committed", pushID)}
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
			push_id::text,
			user_id,
			source_id,
			source_bundle_id,
			planned_row_count,
			status,
			COALESCE(bundle_seq, 0),
			COALESCE(row_count, 0),
			COALESCE(bundle_hash, ''),
			expires_at
		FROM sync.push_sessions
		WHERE push_id = $1::uuid
		FOR UPDATE
	`, pushID).Scan(
		&session.PushID,
		&session.UserID,
		&session.SourceID,
		&session.SourceBundleID,
		&session.PlannedRowCount,
		&session.Status,
		&session.BundleSeq,
		&session.RowCount,
		&session.BundleHash,
		&session.ExpiresAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &PushSessionNotFoundError{PushID: pushID}
		}
		return nil, fmt.Errorf("query push session: %w", err)
	}
	return &session, nil
}

func loadAppliedPushMetadata(ctx context.Context, tx pgx.Tx, userID, sourceID string, sourceBundleID int64) (*committedBundleMeta, error) {
	var meta committedBundleMeta
	if err := tx.QueryRow(ctx, `
		SELECT bundle_seq, source_id, source_bundle_id, row_count, bundle_hash
		FROM sync.applied_pushes
		WHERE user_id = $1
		  AND source_id = $2
		  AND source_bundle_id = $3
	`, userID, sourceID, sourceBundleID).Scan(&meta.BundleSeq, &meta.SourceID, &meta.SourceBundleID, &meta.RowCount, &meta.BundleHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query applied push metadata: %w", err)
	}
	return &meta, nil
}

func loadCommittedBundleMeta(ctx context.Context, tx pgx.Tx, userID string, bundleSeq int64) (*committedBundleMeta, error) {
	var meta committedBundleMeta
	if err := tx.QueryRow(ctx, `
		SELECT bundle_seq, source_id, source_bundle_id, row_count, bundle_hash
		FROM sync.bundle_log
		WHERE user_id = $1
		  AND bundle_seq = $2
	`, userID, bundleSeq).Scan(&meta.BundleSeq, &meta.SourceID, &meta.SourceBundleID, &meta.RowCount, &meta.BundleHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, &CommittedBundleNotFoundError{BundleSeq: bundleSeq}
		}
		return nil, fmt.Errorf("query committed bundle metadata: %w", err)
	}
	return &meta, nil
}

func (s *SyncService) loadPushSessionPreparedRows(ctx context.Context, tx pgx.Tx, pushID string) ([]pushPreparedRow, error) {
	queryRows, err := tx.Query(ctx, `
		SELECT row_ordinal, schema_name, table_name, key_json, op, base_row_version, payload
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
		)
		if err := queryRows.Scan(&rowOrdinal, &row.schema, &row.table, &row.keyJSON, &row.op, &row.baseRowVersion, &payload); err != nil {
			return nil, fmt.Errorf("scan push session row: %w", err)
		}
		keyColumn, err := s.syncKeyColumnForTable(row.schema, row.table)
		if err != nil {
			return nil, err
		}
		keyType, err := s.syncKeyTypeForTable(row.schema, row.table)
		if err != nil {
			return nil, err
		}
		normalizedKey, err := decodeNormalizedStoredSyncKey(row.schema, row.table, registeredTableRuntimeInfo{
			syncKeyColumn: keyColumn,
			syncKeyType:   keyType,
		}, row.keyJSON)
		if err != nil {
			return nil, err
		}
		row.keyColumn = normalizedKey.column
		row.keyType = normalizedKey.keyType
		row.keyString = normalizedKey.keyString
		row.keyValue = normalizedKey.dbValue
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
	if _, err := q.Exec(ctx, `DELETE FROM sync.push_sessions WHERE status = 'staging' AND expires_at <= now()`); err != nil {
		return fmt.Errorf("cleanup expired push sessions: %w", err)
	}
	return nil
}
