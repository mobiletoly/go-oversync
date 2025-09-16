// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// MaterializeFailure represents a row in sync.materialize_failures
type MaterializeFailure struct {
	ID               int64     `json:"id"`
	UserID           string    `json:"user_id"`
	SchemaName       string    `json:"schema"`
	TableName        string    `json:"table"`
	PK               string    `json:"pk"`
	AttemptedVersion int64     `json:"attempted_version"`
	Op               string    `json:"op"`
	Payload          []byte    `json:"payload,omitempty"`
	Error            string    `json:"error"`
	FirstSeen        time.Time `json:"first_seen"`
	RetryCount       int       `json:"retry_count"`
}

// ListMaterializeFailures returns failures for the given user filtered by optional schema/table.
func (s *SyncService) ListMaterializeFailures(
	ctx context.Context, userID, schema, table string, limit int,
) ([]MaterializeFailure, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	args := pgx.NamedArgs{"user_id": userID, "limit": limit}
	where := "WHERE user_id=@user_id"
	if schema != "" {
		where += " AND schema_name=@schema"
		args["schema"] = schema
	}
	if table != "" {
		where += " AND table_name=@table_name"
		args["table_name"] = table
	}
	rows, err := s.pool.Query(ctx, `
        SELECT id, user_id, schema_name, table_name, pk_uuid::text, attempted_version, op, payload, error, first_seen, retry_count
        FROM sync.materialize_failures
        `+where+`
        ORDER BY first_seen DESC
        LIMIT @limit`, args)
	if err != nil {
		return nil, fmt.Errorf("list failures: %w", err)
	}
	defer rows.Close()
	var out []MaterializeFailure
	for rows.Next() {
		var r MaterializeFailure
		if err := rows.Scan(&r.ID, &r.UserID, &r.SchemaName, &r.TableName, &r.PK, &r.AttemptedVersion, &r.Op, &r.Payload, &r.Error, &r.FirstSeen, &r.RetryCount); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RetryMaterializeFailure replays a single failure by ID for the given user. On success, the failure row is removed.
func (s *SyncService) RetryMaterializeFailure(
	ctx context.Context, userID string, id int64,
) (*ChangeUploadStatus, error) {
	var f MaterializeFailure
	err := s.pool.QueryRow(ctx, `
        SELECT id, user_id, schema_name, table_name, pk_uuid::text, attempted_version, op, payload, error, first_seen, retry_count
        FROM sync.materialize_failures WHERE id=$1`, id).Scan(
		&f.ID, &f.UserID, &f.SchemaName, &f.TableName, &f.PK, &f.AttemptedVersion, &f.Op, &f.Payload, &f.Error, &f.FirstSeen, &f.RetryCount)
	if err != nil {
		return nil, fmt.Errorf("load failure: %w", err)
	}
	if f.UserID != userID {
		return nil, errors.New("not found")
	}

	// Build a synthetic change matching the failed attempt
	incomingVersion := f.AttemptedVersion - 1
	if incomingVersion < 0 {
		incomingVersion = 0
	}
	ch := ChangeUpload{
		SourceChangeID: time.Now().UnixNano(),
		Schema:         f.SchemaName,
		Table:          f.TableName,
		Op:             f.Op,
		PK:             f.PK,
		ServerVersion:  incomingVersion,
		Payload:        f.Payload,
	}

	var status ChangeUploadStatus
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadWrite}, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); e != nil {
			return e
		}
		// Use a special source for idempotency separation
		const sourceID = "admin-retry"
		var st ChangeUploadStatus
		var e error
		switch f.Op {
		case OpInsert, OpUpdate:
			st, e = s.applyUpsert(ctx, tx, userID, sourceID, ch)
		case OpDelete:
			st, e = s.applyDelete(ctx, tx, userID, sourceID, ch)
		default:
			return fmt.Errorf("unsupported op: %s", f.Op)
		}
		if e != nil {
			return e
		}
		status = st
		// On success, remove failure row
		if st.Status == StApplied {
			if _, e := tx.Exec(ctx, `DELETE FROM sync.materialize_failures WHERE id=@id`, pgx.NamedArgs{"id": id}); e != nil {
				return fmt.Errorf("delete failure row: %w", e)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &status, nil
}
