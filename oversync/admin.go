// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MaterializeFailure represents a row in sync.materialize_failures
type MaterializeFailure struct {
	ID               int64           `json:"id"`
	UserID           string          `json:"user_id"`
	SchemaName       string          `json:"schema"`
	TableName        string          `json:"table"`
	PK               string          `json:"pk"`
	AttemptedVersion int64           `json:"attempted_version"`
	Op               string          `json:"op"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	Error            string          `json:"error"`
	FirstSeen        time.Time       `json:"first_seen"`
	RetryCount       int             `json:"retry_count"`
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

	// Resolve handler
	s.mu.RLock()
	handlerKey := f.SchemaName + "." + f.TableName
	handler, ok := s.tableHandlers[handlerKey]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no materialization handler registered for %s", handlerKey)
	}

	pkUUID, parseErr := uuid.Parse(f.PK)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid pk uuid: %w", parseErr)
	}

	var status ChangeUploadStatus
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadWrite}, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); e != nil {
			return e
		}

		// Materialize from current sidecar state (source of truth).
		var serverVersion int64
		var deleted bool
		if err := tx.QueryRow(ctx, `
			SELECT server_version, deleted
			FROM sync.sync_row_meta
			WHERE user_id=@user_id AND schema_name=@schema_name AND table_name=@table_name AND pk_uuid=@pk_uuid::uuid`,
			pgx.NamedArgs{
				"user_id":     userID,
				"schema_name": f.SchemaName,
				"table_name":  f.TableName,
				"pk_uuid":     f.PK,
			},
		).Scan(&serverVersion, &deleted); err != nil {
			// Nothing to materialize; drop the failure row as stale.
			if _, e := tx.Exec(ctx, `DELETE FROM sync.materialize_failures WHERE id=@id`, pgx.NamedArgs{"id": id}); e != nil {
				return fmt.Errorf("delete stale failure row: %w", e)
			}
			status = statusApplied(0, f.AttemptedVersion)
			status.Message = "failure was stale; removed"
			return nil
		}

		if deleted {
			if err := handler.ApplyDelete(ctx, tx, f.SchemaName, f.TableName, pkUUID); err != nil {
				if _, e := tx.Exec(ctx, `
					UPDATE sync.materialize_failures
					SET retry_count = retry_count + 1, error = @error
					WHERE id=@id`,
					pgx.NamedArgs{"id": id, "error": err.Error()},
				); e != nil {
					return fmt.Errorf("update failure row: %w", e)
				}
				status = statusMaterializeError(0, serverVersion, err)
				return nil
			}
		} else {
			var payload []byte
			if err := tx.QueryRow(ctx, `
				SELECT payload
				FROM sync.sync_state
				WHERE user_id=@user_id AND schema_name=@schema_name AND table_name=@table_name AND pk_uuid=@pk_uuid::uuid`,
				pgx.NamedArgs{
					"user_id":     userID,
					"schema_name": f.SchemaName,
					"table_name":  f.TableName,
					"pk_uuid":     f.PK,
				},
			).Scan(&payload); err != nil {
				if _, e := tx.Exec(ctx, `
					UPDATE sync.materialize_failures
					SET retry_count = retry_count + 1, error = @error
					WHERE id=@id`,
					pgx.NamedArgs{"id": id, "error": err.Error()},
				); e != nil {
					return fmt.Errorf("update failure row: %w", e)
				}
				status = statusMaterializeError(0, serverVersion, fmt.Errorf("load sync_state payload: %w", err))
				return nil
			}

			if err := handler.ApplyUpsert(ctx, tx, f.SchemaName, f.TableName, pkUUID, payload); err != nil {
				if _, e := tx.Exec(ctx, `
					UPDATE sync.materialize_failures
					SET retry_count = retry_count + 1, error = @error
					WHERE id=@id`,
					pgx.NamedArgs{"id": id, "error": err.Error()},
				); e != nil {
					return fmt.Errorf("update failure row: %w", e)
				}
				status = statusMaterializeError(0, serverVersion, err)
				return nil
			}
		}

		// Success: remove failure row.
		if _, e := tx.Exec(ctx, `DELETE FROM sync.materialize_failures WHERE id=@id`, pgx.NamedArgs{"id": id}); e != nil {
			return fmt.Errorf("delete failure row: %w", e)
		}

		status = statusApplied(0, serverVersion)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &status, nil
}
