package oversync

import (
	"context"
	"errors"
	"fmt"
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

func (s *SyncService) CreateSnapshotSession(ctx context.Context, actor Actor) (_ *SnapshotSession, err error) {
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

		snapshotBundleSeq, err := userHighestBundleSeqQuerier(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}

		snapshotID := uuid.NewString()
		expiresAt := time.Now().UTC().Add(s.snapshotSessionTTL())
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.snapshot_sessions (
				snapshot_id, user_id, snapshot_bundle_seq, row_count, byte_count, expires_at
			) VALUES ($1::uuid, $2, $3, 0, 0, $4)
		`, snapshotID, actor.UserID, snapshotBundleSeq, expiresAt); err != nil {
			return fmt.Errorf("insert snapshot session: %w", err)
		}

		var (
			rowCount  int64
			byteCount int64
		)
		if err := tx.QueryRow(ctx, `
			WITH inserted AS (
				INSERT INTO sync.snapshot_session_rows (
					snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
				)
				SELECT
					$1::uuid,
					row_number() OVER (ORDER BY rs.schema_name, rs.table_name, rs.key_json),
					rs.schema_name,
					rs.table_name,
					rs.key_json,
					rs.row_version,
					br.payload
				FROM sync.row_state AS rs
				JOIN sync.bundle_rows AS br
				  ON br.user_id = rs.user_id
				 AND br.bundle_seq = rs.bundle_seq
				 AND br.schema_name = rs.schema_name
				 AND br.table_name = rs.table_name
				 AND br.key_json = rs.key_json
				WHERE rs.user_id = $2
				  AND rs.deleted = FALSE
				ORDER BY rs.schema_name, rs.table_name, rs.key_json
				RETURNING payload
			)
			SELECT COUNT(*), COALESCE(SUM(octet_length(payload::text)), 0)
			FROM inserted
		`, snapshotID, actor.UserID).Scan(&rowCount, &byteCount); err != nil {
			return fmt.Errorf("materialize snapshot session rows: %w", err)
		}
		if limit := s.maxRowsPerSnapshotSession(); limit > 0 && rowCount > limit {
			return &SnapshotSessionLimitExceededError{Dimension: "row_count", Actual: rowCount, Limit: limit}
		}
		if limit := s.maxBytesPerSnapshotSession(); limit > 0 && byteCount > limit {
			return &SnapshotSessionLimitExceededError{Dimension: "byte_count", Actual: byteCount, Limit: limit}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sync.snapshot_sessions
			SET row_count = $2,
			    byte_count = $3
			WHERE snapshot_id = $1::uuid
		`, snapshotID, rowCount, byteCount); err != nil {
			return fmt.Errorf("update snapshot session counts: %w", err)
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
		var userID string
		var snapshotBundleSeq int64
		var expiresAt time.Time
		if err := tx.QueryRow(ctx, `
			SELECT user_id, snapshot_bundle_seq, expires_at
			FROM sync.snapshot_sessions
			WHERE snapshot_id = $1::uuid
		`, snapshotID).Scan(&userID, &snapshotBundleSeq, &expiresAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session: %w", err)
		}
		if userID != actor.UserID {
			return &SnapshotSessionForbiddenError{SnapshotID: snapshotID}
		}
		if time.Now().UTC().After(expiresAt) {
			return &SnapshotSessionExpiredError{SnapshotID: snapshotID}
		}

		rows, err := tx.Query(ctx, `
			SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
			FROM sync.snapshot_session_rows
			WHERE snapshot_id = $1::uuid
			  AND row_ordinal > $2
			ORDER BY row_ordinal
			LIMIT $3
		`, snapshotID, afterRowOrdinal, effectiveMaxRows+1)
		if err != nil {
			return fmt.Errorf("query snapshot chunk rows: %w", err)
		}
		defer rows.Close()

		chunkRows := make([]SnapshotRow, 0, effectiveMaxRows+1)
		nextRowOrdinal := afterRowOrdinal
		for rows.Next() {
			var row SnapshotRow
			var rowOrdinal int64
			var keyJSON string
			if err := rows.Scan(&rowOrdinal, &row.Schema, &row.Table, &keyJSON, &row.RowVersion, &row.Payload); err != nil {
				return fmt.Errorf("scan snapshot chunk row: %w", err)
			}
			key, err := decodeSyncKeyJSON(keyJSON)
			if err != nil {
				return fmt.Errorf("decode snapshot chunk row key: %w", err)
			}
			row.Key = key
			row.Payload, err = s.canonicalizeWirePayload(row.Schema, row.Table, row.Payload)
			if err != nil {
				return fmt.Errorf("canonicalize snapshot chunk payload for %s.%s: %w", row.Schema, row.Table, err)
			}
			chunkRows = append(chunkRows, row)
			nextRowOrdinal = rowOrdinal
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate snapshot chunk rows: %w", err)
		}

		hasMore := false
		if len(chunkRows) > effectiveMaxRows {
			hasMore = true
			chunkRows = chunkRows[:effectiveMaxRows]
			nextRowOrdinal = afterRowOrdinal + int64(len(chunkRows))
		}
		if len(chunkRows) == 0 {
			nextRowOrdinal = afterRowOrdinal
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
		var userID string
		if err := tx.QueryRow(ctx, `
			SELECT user_id
			FROM sync.snapshot_sessions
			WHERE snapshot_id = $1::uuid
		`, snapshotID).Scan(&userID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &SnapshotSessionNotFoundError{SnapshotID: snapshotID}
			}
			return fmt.Errorf("query snapshot session for delete: %w", err)
		}
		if userID != actor.UserID {
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
