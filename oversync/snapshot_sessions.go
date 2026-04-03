package oversync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func (e *SnapshotSessionLimitExceededError) Error() string {
	return fmt.Sprintf("snapshot session %s %d exceeds limit %d", e.Dimension, e.Actual, e.Limit)
}

type snapshotMaterializedRow struct {
	tableID     int32
	keyBytes    []byte
	bundleSeq   int64
	payloadWire []byte
}

type snapshotLiveState struct {
	tableID   int32
	keyBytes  []byte
	bundleSeq int64
}

func snapshotLogicalRowKey(tableID int32, keyBytes []byte) string {
	key := appendInt32BigEndian(nil, tableID)
	key = append(key, keyBytes...)
	return string(key)
}

func (s *SyncService) CreateSnapshotSession(ctx context.Context, actor Actor) (_ *SnapshotSession, err error) {
	return s.CreateSnapshotSessionWithRequest(ctx, actor, nil)
}

func (s *SyncService) CreateSnapshotSessionWithRequest(ctx context.Context, actor Actor, req *SnapshotSessionCreateRequest) (_ *SnapshotSession, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(false); err != nil {
		return nil, err
	}

	var resp *SnapshotSession
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if err := cleanupExpiredSnapshotSessionsQuerier(ctx, tx); err != nil {
			return err
		}
		if err := requireScopeInitializedQuerier(ctx, tx, actor.UserID); err != nil {
			return err
		}

		retainedState, err := loadRetainedHistoryStateByUserID(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}
		if retainedState == nil {
			return fmt.Errorf("missing retained history state for %q", actor.UserID)
		}
		if err := s.applySnapshotSourceReplacementInTx(ctx, tx, actor, retainedState.UserPK, req); err != nil {
			return err
		}

		snapshotBundleSeq := retainedState.highestBundleSeq()
		rows, byteCount, err := s.materializeSnapshotRows(ctx, tx, actor.UserID, retainedState.UserPK)
		if err != nil {
			return err
		}
		rowCount := int64(len(rows))
		if limit := s.maxRowsPerSnapshotSession(); limit > 0 && rowCount > limit {
			return &SnapshotSessionLimitExceededError{Dimension: "row_count", Actual: rowCount, Limit: limit}
		}
		if limit := s.maxBytesPerSnapshotSession(); limit > 0 && byteCount > limit {
			return &SnapshotSessionLimitExceededError{Dimension: "byte_count", Actual: byteCount, Limit: limit}
		}

		snapshotID := uuid.NewString()
		expiresAt := time.Now().UTC().Add(s.snapshotSessionTTL())
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.snapshot_sessions (
				snapshot_id, user_pk, snapshot_bundle_seq, row_count, byte_count, expires_at
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
		`, snapshotID, retainedState.UserPK, snapshotBundleSeq, rowCount, byteCount, expiresAt); err != nil {
			return fmt.Errorf("insert snapshot session: %w", err)
		}
		for i, row := range rows {
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.snapshot_session_rows (
					snapshot_id, row_ordinal, table_id, key_bytes, bundle_seq, payload_wire
				) VALUES ($1::uuid, $2, $3, $4, $5, $6)
			`, snapshotID, int64(i+1), row.tableID, row.keyBytes, row.bundleSeq, string(row.payloadWire)); err != nil {
				return fmt.Errorf("insert snapshot session row %d: %w", i+1, err)
			}
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
	return resp, nil
}

func validateSnapshotSourceReplacement(actor Actor, replacement *SnapshotSourceReplacement) (*SnapshotSourceReplacement, error) {
	if replacement == nil {
		return nil, nil
	}
	clean := &SnapshotSourceReplacement{
		PreviousSourceID: strings.TrimSpace(replacement.PreviousSourceID),
		NewSourceID:      strings.TrimSpace(replacement.NewSourceID),
		Reason:           strings.TrimSpace(replacement.Reason),
	}
	if clean.PreviousSourceID != actor.SourceID {
		return nil, &SnapshotSessionInvalidError{Message: "previous_source_id must match authenticated source_id"}
	}
	if clean.NewSourceID == "" {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id is required for source replacement"}
	}
	if clean.NewSourceID == clean.PreviousSourceID {
		return nil, &SnapshotSessionInvalidError{Message: "new_source_id must differ from previous_source_id"}
	}
	switch clean.Reason {
	case "history_pruned", "source_sequence_out_of_order", "source_sequence_changed", "source_retired":
	default:
		return nil, &SnapshotSessionInvalidError{Message: "source replacement reason is unsupported"}
	}
	return clean, nil
}

func (s *SyncService) applySnapshotSourceReplacementInTx(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, req *SnapshotSessionCreateRequest) error {
	if req == nil || req.SourceReplacement == nil {
		return nil
	}
	replacement, err := validateSnapshotSourceReplacement(actor, req.SourceReplacement)
	if err != nil {
		return err
	}

	previousState, err := loadSourceStateRow(ctx, tx, userPK, replacement.PreviousSourceID, true)
	if err != nil {
		return err
	}
	newState, err := loadSourceStateRow(ctx, tx, userPK, replacement.NewSourceID, true)
	if err != nil {
		return err
	}

	if previousState != nil && previousState.State == sourceStateRetired {
		if previousState.ReplacedBySourceID != replacement.NewSourceID {
			return &SourceRetiredError{
				UserID:             actor.UserID,
				SourceID:           replacement.PreviousSourceID,
				ReplacedBySourceID: previousState.ReplacedBySourceID,
			}
		}
		if newState == nil {
			if err := reserveSourceState(ctx, tx, userPK, replacement.NewSourceID); err != nil {
				return err
			}
		} else if newState.State != sourceStateReserved && newState.State != sourceStateActive {
			return &SourceReplacementInvalidError{Message: fmt.Sprintf("replacement source %s is not available for rotated rebuild", replacement.NewSourceID)}
		}
	} else {
		if newState != nil {
			return &SourceReplacementInvalidError{Message: fmt.Sprintf("replacement source %s is already known for user %s", replacement.NewSourceID, actor.UserID)}
		}
		if err := reserveSourceState(ctx, tx, userPK, replacement.NewSourceID); err != nil {
			return err
		}
		if err := retireSourceState(ctx, tx, userPK, replacement.PreviousSourceID, replacement.NewSourceID, replacement.Reason); err != nil {
			return err
		}
	}

	return nil
}

func (s *SyncService) materializeSnapshotRows(ctx context.Context, tx pgx.Tx, userID string, userPK int64) ([]snapshotMaterializedRow, int64, error) {
	liveRowState, err := loadSnapshotLiveRowState(ctx, tx, userPK)
	if err != nil {
		return nil, 0, err
	}
	seen := make(map[string]struct{}, len(liveRowState))
	rows := make([]snapshotMaterializedRow, 0, len(liveRowState))
	var byteCount int64

	tableInfos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		tableInfos = append(tableInfos, info)
	}
	sort.Slice(tableInfos, func(i, j int) bool {
		return tableInfos[i].tableID < tableInfos[j].tableID
	})

	for _, info := range tableInfos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		keyColumnIdent := pgx.Identifier{info.syncKeyColumn}.Sanitize()
		ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		query := fmt.Sprintf(`
			SELECT CAST(src.%s AS text), to_jsonb(src) - '_sync_scope_id'
			FROM %s AS src
			WHERE src.%s = $1
			ORDER BY CAST(src.%s AS text)
		`, keyColumnIdent, tableIdent, ownerColumnIdent, keyColumnIdent)
		liveRows, err := tx.Query(ctx, query, userID)
		if err != nil {
			return nil, 0, fmt.Errorf("query live snapshot rows for %s.%s: %w", info.schemaName, info.tableName, err)
		}

		for liveRows.Next() {
			var (
				keyText   string
				payloadDB []byte
			)
			if err := liveRows.Scan(&keyText, &payloadDB); err != nil {
				liveRows.Close()
				return nil, 0, fmt.Errorf("scan live snapshot row for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			keyBytes, _, err := encodeKeyBytes(info.syncKeyType, keyText)
			if err != nil {
				liveRows.Close()
				return nil, 0, fmt.Errorf("encode live snapshot key for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			logicalKey := snapshotLogicalRowKey(info.tableID, keyBytes)
			state, ok := liveRowState[logicalKey]
			if !ok {
				liveRows.Close()
				return nil, 0, fmt.Errorf("live row %s.%s %q is missing non-deleted sync.row_state", info.schemaName, info.tableName, keyText)
			}
			if _, duplicate := seen[logicalKey]; duplicate {
				liveRows.Close()
				return nil, 0, fmt.Errorf("duplicate live snapshot row detected for %s.%s %q", info.schemaName, info.tableName, keyText)
			}
			seen[logicalKey] = struct{}{}

			payloadWire, err := s.canonicalizeWirePayload(info.schemaName, info.tableName, payloadDB)
			if err != nil {
				liveRows.Close()
				return nil, 0, fmt.Errorf("canonicalize live snapshot payload for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			byteCount += int64(len(payloadWire))
			rows = append(rows, snapshotMaterializedRow{
				tableID:     info.tableID,
				keyBytes:    append([]byte(nil), keyBytes...),
				bundleSeq:   state.bundleSeq,
				payloadWire: append([]byte(nil), payloadWire...),
			})
		}
		if err := liveRows.Err(); err != nil {
			liveRows.Close()
			return nil, 0, fmt.Errorf("iterate live snapshot rows for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		liveRows.Close()
	}

	for logicalKey, state := range liveRowState {
		if _, ok := seen[logicalKey]; ok {
			continue
		}
		info, err := s.tableInfoForID(state.tableID)
		if err != nil {
			return nil, 0, err
		}
		key, err := wireSyncKeyFromBytes(info, state.keyBytes)
		if err != nil {
			return nil, 0, fmt.Errorf("decode missing live snapshot key for table_id %d: %w", state.tableID, err)
		}
		return nil, 0, fmt.Errorf("sync.row_state row %s.%s %v is missing a live business-table row", info.schemaName, info.tableName, key)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].tableID != rows[j].tableID {
			return rows[i].tableID < rows[j].tableID
		}
		return bytes.Compare(rows[i].keyBytes, rows[j].keyBytes) < 0
	})
	return rows, byteCount, nil
}

func loadSnapshotLiveRowState(ctx context.Context, tx pgx.Tx, userPK int64) (map[string]snapshotLiveState, error) {
	rows, err := tx.Query(ctx, `
		SELECT table_id, key_bytes, bundle_seq
		FROM sync.row_state
		WHERE user_pk = $1
		  AND deleted = FALSE
	`, userPK)
	if err != nil {
		return nil, fmt.Errorf("query live snapshot row_state: %w", err)
	}
	defer rows.Close()

	liveState := make(map[string]snapshotLiveState)
	for rows.Next() {
		var state snapshotLiveState
		if err := rows.Scan(&state.tableID, &state.keyBytes, &state.bundleSeq); err != nil {
			return nil, fmt.Errorf("scan live snapshot row_state: %w", err)
		}
		state.keyBytes = append([]byte(nil), state.keyBytes...)
		liveState[snapshotLogicalRowKey(state.tableID, state.keyBytes)] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live snapshot row_state: %w", err)
	}
	return liveState, nil
}

func (s *SyncService) GetSnapshotChunk(ctx context.Context, actor Actor, snapshotID string, afterRowOrdinal int64, maxRows int) (_ *SnapshotChunkResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(false); err != nil {
		return nil, err
	}
	if snapshotID == "" {
		return nil, &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}
	if afterRowOrdinal < 0 {
		return nil, &SnapshotChunkInvalidError{Message: "after_row_ordinal must be >= 0"}
	}
	if maxRows <= 0 {
		return nil, &SnapshotChunkInvalidError{Message: "max_rows must be > 0"}
	}

	effectiveMaxRows := maxRows
	if effectiveMaxRows > s.maxRowsPerSnapshotChunk() {
		effectiveMaxRows = s.maxRowsPerSnapshotChunk()
	}

	var resp *SnapshotChunkResponse
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		var (
			sessionUserID     string
			snapshotBundleSeq int64
			expiresAt         time.Time
		)
		if err := tx.QueryRow(ctx, `
			SELECT us.user_id, ss.snapshot_bundle_seq, ss.expires_at
			FROM sync.snapshot_sessions AS ss
			JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
			WHERE ss.snapshot_id = $1::uuid
		`, snapshotID).Scan(&sessionUserID, &snapshotBundleSeq, &expiresAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session: %w", err)
		}
		if sessionUserID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if time.Now().UTC().After(expiresAt) {
			return &SnapshotSessionExpiredError{SnapshotID: snapshotID}
		}

		queryRows, err := tx.Query(ctx, `
			SELECT row_ordinal, table_id, key_bytes, bundle_seq, payload_wire
			FROM sync.snapshot_session_rows
			WHERE snapshot_id = $1::uuid
			  AND row_ordinal > $2
			ORDER BY row_ordinal
			LIMIT $3
		`, snapshotID, afterRowOrdinal, effectiveMaxRows+1)
		if err != nil {
			return fmt.Errorf("query snapshot chunk rows: %w", err)
		}
		defer queryRows.Close()

		chunkRows := make([]SnapshotRow, 0, effectiveMaxRows+1)
		for queryRows.Next() {
			var (
				row        SnapshotRow
				tableID    int32
				keyBytes   []byte
				rowOrdinal int64
			)
			if err := queryRows.Scan(&rowOrdinal, &tableID, &keyBytes, &row.RowVersion, &row.Payload); err != nil {
				return fmt.Errorf("scan snapshot chunk row: %w", err)
			}
			info, err := s.tableInfoForID(tableID)
			if err != nil {
				return err
			}
			row.Schema = info.schemaName
			row.Table = info.tableName
			row.Key, err = wireSyncKeyFromBytes(info, keyBytes)
			if err != nil {
				return fmt.Errorf("decode snapshot chunk row key: %w", err)
			}
			chunkRows = append(chunkRows, row)
		}
		if err := queryRows.Err(); err != nil {
			return fmt.Errorf("iterate snapshot chunk rows: %w", err)
		}

		hasMore := false
		if len(chunkRows) > effectiveMaxRows {
			hasMore = true
			chunkRows = chunkRows[:effectiveMaxRows]
		}
		nextRowOrdinal := afterRowOrdinal
		if len(chunkRows) > 0 {
			nextRowOrdinal = afterRowOrdinal + int64(len(chunkRows))
		}

		resp = &SnapshotChunkResponse{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			Rows:              chunkRows,
			NextRowOrdinal:    nextRowOrdinal,
			HasMore:           hasMore,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *SyncService) DeleteSnapshotSession(ctx context.Context, actor Actor, snapshotID string) (err error) {
	done, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer done()
	if err := actor.validate(false); err != nil {
		return err
	}
	if snapshotID == "" {
		return &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}

	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var sessionUserID string
		if err := tx.QueryRow(ctx, `
			SELECT us.user_id
			FROM sync.snapshot_sessions AS ss
			JOIN sync.user_state AS us ON us.user_pk = ss.user_pk
			WHERE ss.snapshot_id = $1::uuid
		`, snapshotID).Scan(&sessionUserID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session for delete: %w", err)
		}
		if sessionUserID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM sync.snapshot_sessions WHERE snapshot_id = $1::uuid`, snapshotID); err != nil {
			return fmt.Errorf("delete snapshot session: %w", err)
		}
		return nil
	})
}

func cleanupExpiredSnapshotSessionsQuerier(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	if _, err := q.Exec(ctx, `DELETE FROM sync.snapshot_sessions WHERE expires_at <= now()`); err != nil {
		return fmt.Errorf("cleanup expired snapshot sessions: %w", err)
	}
	return nil
}
