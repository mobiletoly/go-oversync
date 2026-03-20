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

type snapshotSessionRowRecord struct {
	rowOrdinal int64
	schemaName string
	tableName  string
	keyJSON    string
	rowVersion int64
	payload    []byte
}

func (s *SyncService) CreateSnapshotSession(ctx context.Context, actor Actor) (_ *SnapshotSession, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return nil, err
	}

	var resp *SnapshotSession
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		if err := cleanupExpiredSnapshotSessionsQuerier(ctx, tx); err != nil {
			return err
		}

		snapshotBundleSeq, err := userHighestBundleSeqQuerier(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
SELECT
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
WHERE rs.user_id = $1
  AND rs.deleted = FALSE
ORDER BY rs.schema_name, rs.table_name, rs.key_json
		`, actor.UserID)
		if err != nil {
			return fmt.Errorf("query snapshot rows: %w", err)
		}
		defer rows.Close()

		snapshotID := uuid.NewString()
		expiresAt := time.Now().UTC().Add(s.snapshotSessionTTL())
		staged := make([]snapshotSessionRowRecord, 0)
		rowOrdinal := int64(0)
		byteCount := int64(0)

		for rows.Next() {
			var rec snapshotSessionRowRecord
			rowOrdinal++
			rec.rowOrdinal = rowOrdinal
			if err := rows.Scan(&rec.schemaName, &rec.tableName, &rec.keyJSON, &rec.rowVersion, &rec.payload); err != nil {
				return fmt.Errorf("scan snapshot row: %w", err)
			}
			byteCount += int64(len(rec.payload))
			staged = append(staged, rec)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate snapshot rows: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.snapshot_sessions (
				snapshot_id, user_id, snapshot_bundle_seq, row_count, byte_count, expires_at
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
		`, snapshotID, actor.UserID, snapshotBundleSeq, len(staged), byteCount, expiresAt); err != nil {
			return fmt.Errorf("insert snapshot session: %w", err)
		}

		if len(staged) > 0 {
			rowsData := make([][]any, 0, len(staged))
			for _, rec := range staged {
				rowsData = append(rowsData, []any{
					snapshotID,
					rec.rowOrdinal,
					rec.schemaName,
					rec.tableName,
					rec.keyJSON,
					rec.rowVersion,
					rec.payload,
				})
			}
			if _, err := tx.CopyFrom(ctx,
				pgx.Identifier{"sync", "snapshot_session_rows"},
				[]string{"snapshot_id", "row_ordinal", "schema_name", "table_name", "key_json", "row_version", "payload"},
				pgx.CopyFromRows(rowsData),
			); err != nil {
				return fmt.Errorf("insert snapshot session rows: %w", err)
			}
		}

		resp = &SnapshotSession{
			SnapshotID:        snapshotID,
			SnapshotBundleSeq: snapshotBundleSeq,
			RowCount:          int64(len(staged)),
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
	if err := actor.validate(true); err != nil {
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

		if _, err := tx.Exec(ctx, `
			UPDATE sync.snapshot_sessions
			SET last_accessed_at = now()
			WHERE snapshot_id = $1::uuid
		`, snapshotID); err != nil {
			return fmt.Errorf("update snapshot session access time: %w", err)
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
	if err := actor.validate(true); err != nil {
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
