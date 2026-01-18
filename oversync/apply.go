// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (s *SyncService) serverChangeLogHasTriplet(ctx context.Context, tx pgx.Tx, userID, sourceID string, scid int64) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM sync.server_change_log
			WHERE user_id = $1 AND source_id = $2 AND source_change_id = $3
		)`, userID, sourceID, scid).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// applyUpsert applies an INSERT/UPDATE operation with SAVEPOINT isolation
func (s *SyncService) applyUpsert(ctx context.Context, tx pgx.Tx, userID, sourceID string, change ChangeUpload) (ChangeUploadStatus, error) {
	s.logger.Debug("Starting applyUpsert",
		"schema", change.Schema, "table", change.Table, "pk", change.PK,
		"source_change_id", change.SourceChangeID, "server_version", change.ServerVersion,
		"payload_size", len(change.Payload))

	// Create SAVEPOINT for isolation
	spName := fmt.Sprintf("sp_%d", change.SourceChangeID)
	_, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
	if err != nil {
		return ChangeUploadStatus{}, fmt.Errorf("failed to create savepoint: %w", err)
	}

	var (
		code           int
		newServerVer64 int64
	)
	err = tx.QueryRow(ctx, stmtApplyUpsert,
		userID,
		change.Schema,
		change.Table,
		change.Op,
		change.PK,
		change.Payload,
		sourceID,
		change.SourceChangeID,
		change.ServerVersion,
	).Scan(&code, &newServerVer64)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.SQLState() == "40001" || pgErr.SQLState() == "40P01") {
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			exists, checkErr := s.serverChangeLogHasTriplet(ctx, tx, userID, sourceID, change.SourceChangeID)
			_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			if checkErr != nil {
				return statusInvalidOther(change.SourceChangeID, ReasonInternalError, fmt.Errorf("idempotency gate check failed: %w", checkErr)), nil
			}
			if exists {
				return statusAppliedIdempotent(change.SourceChangeID), nil
			}
			return statusInvalidOther(change.SourceChangeID, ReasonInternalError, fmt.Errorf("apply failed with %s but triplet is missing; retry", pgErr.SQLState())), nil
		}
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to apply upsert: %w", err)
	}

	switch code {
	case 0:
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		return statusAppliedIdempotent(change.SourceChangeID), nil
	case 1:
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		if err := s.applyBusinessProjectionBestEffort(ctx, tx, userID, change, false, newServerVer64); err != nil {
			s.logger.Warn("Business projection failed; sync still applied",
				"error", err, "schema", change.Schema, "table", change.Table, "pk", change.PK,
				"server_version", newServerVer64, "source_change_id", change.SourceChangeID)
		}
		return statusApplied(change.SourceChangeID, newServerVer64), nil
	case 2:
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		serverRow, fetchErr := s.fetchServerRowJSON(ctx, tx, userID, change.Schema, change.Table, change.PK)
		if fetchErr != nil {
			if errors.Is(fetchErr, pgx.ErrNoRows) {
				return statusInvalidOther(change.SourceChangeID, ReasonInternalError, fmt.Errorf("conflict without server_row for %s.%s pk=%s", change.Schema, change.Table, change.PK)), nil
			}
			return ChangeUploadStatus{}, fmt.Errorf("failed to fetch server row for conflict: %w", fetchErr)
		}
		return statusConflict(change.SourceChangeID, serverRow), nil
	case 3:
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		return statusInvalidOther(change.SourceChangeID, ReasonInternalError, fmt.Errorf("conflict without server_row for %s.%s pk=%s", change.Schema, change.Table, change.PK)), nil
	default:
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("unknown apply upsert code %d", code)
	}
}

// applyDelete applies a DELETE operation with SAVEPOINT isolation
func (s *SyncService) applyDelete(ctx context.Context, tx pgx.Tx, userID, sourceID string, change ChangeUpload) (ChangeUploadStatus, error) {
	s.logger.Debug("Starting applyDelete",
		"schema", change.Schema, "table", change.Table, "pk", change.PK,
		"source_change_id", change.SourceChangeID, "server_version", change.ServerVersion)

	// Create SAVEPOINT for isolation
	spName := fmt.Sprintf("sp_%d", change.SourceChangeID)
	_, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
	if err != nil {
		return ChangeUploadStatus{}, fmt.Errorf("failed to create savepoint: %w", err)
	}

	var (
		code           int
		newServerVer64 int64
	)
	err = tx.QueryRow(ctx, stmtApplyDelete,
		userID,
		change.Schema,
		change.Table,
		change.Op,
		change.PK,
		sourceID,
		change.SourceChangeID,
		change.ServerVersion,
	).Scan(&code, &newServerVer64)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.SQLState() == "40001" || pgErr.SQLState() == "40P01") {
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			exists, checkErr := s.serverChangeLogHasTriplet(ctx, tx, userID, sourceID, change.SourceChangeID)
			_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			if checkErr != nil {
				return statusInvalidOther(change.SourceChangeID, ReasonInternalError, fmt.Errorf("idempotency gate check failed: %w", checkErr)), nil
			}
			if exists {
				return statusAppliedIdempotent(change.SourceChangeID), nil
			}
			return statusInvalidOther(change.SourceChangeID, ReasonInternalError, fmt.Errorf("apply failed with %s but triplet is missing; retry", pgErr.SQLState())), nil
		}
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to apply delete: %w", err)
	}

	switch code {
	case 0, 3:
		if code == 3 {
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		}
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		return statusAppliedIdempotent(change.SourceChangeID), nil
	case 1:
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		if err := s.applyBusinessProjectionBestEffort(ctx, tx, userID, change, true, newServerVer64); err != nil {
			s.logger.Warn("Business projection failed; sync still applied",
				"error", err, "schema", change.Schema, "table", change.Table, "pk", change.PK,
				"server_version", newServerVer64, "source_change_id", change.SourceChangeID)
		}
		return statusApplied(change.SourceChangeID, newServerVer64), nil
	case 2:
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		if err != nil {
			return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
		}
		serverRow, fetchErr := s.fetchServerRowJSON(ctx, tx, userID, change.Schema, change.Table, change.PK)
		if fetchErr != nil {
			if errors.Is(fetchErr, pgx.ErrNoRows) {
				return statusAppliedIdempotent(change.SourceChangeID), nil
			}
			return ChangeUploadStatus{}, fmt.Errorf("failed to fetch server row for conflict: %w", fetchErr)
		}
		return statusConflict(change.SourceChangeID, serverRow), nil
	default:
		return ChangeUploadStatus{}, fmt.Errorf("unknown apply delete code %d", code)
	}
}

// applyBusinessProjectionBestEffort runs the optional business-table projection in a nested SAVEPOINT.
// On failure, it rolls back only business-table effects and records a retryable failure row,
// while leaving sidecar/meta/state/log changes intact.
func (s *SyncService) applyBusinessProjectionBestEffort(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	change ChangeUpload,
	deleted bool,
	attemptedVersion int64,
) error {
	s.mu.RLock()
	handlerKey := change.Schema + "." + change.Table
	_, exists := s.tableHandlers[handlerKey]
	s.mu.RUnlock()
	if !exists {
		return nil
	}

	spName := fmt.Sprintf("sp_mat_%d", change.SourceChangeID)
	if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", pgx.Identifier{spName}.Sanitize())); err != nil {
		return fmt.Errorf("create materialize savepoint: %w", err)
	}

	err := s.applyBusinessProjection(ctx, tx, change, deleted)
	if err == nil {
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return nil
	}

	// Roll back business-table work but keep sidecar/meta/state/log.
	_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
	_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))

	var payload []byte
	if !deleted {
		payload = change.Payload
	}
	_ = s.recordMaterializeFailure(ctx, tx, userID, change.Schema, change.Table, change.PK, attemptedVersion, err, change.Op, payload)
	return err
}

// applyBusinessProjection handles optional business table materialization
func (s *SyncService) applyBusinessProjection(ctx context.Context, tx pgx.Tx, change ChangeUpload, deleted bool) error {
	s.mu.RLock()
	handlerKey := change.Schema + "." + change.Table
	handler, exists := s.tableHandlers[handlerKey]
	s.mu.RUnlock()

	if !exists {
		s.logger.Debug("No handler registered for table, skipping materialization",
			"schema", change.Schema, "table", change.Table, "handler_key", handlerKey)
		return nil // No handler registered, skip materialization
	}

	s.logger.Debug("Starting business table materialization",
		"schema", change.Schema, "table", change.Table, "pk", change.PK,
		"deleted", deleted, "payload_size", len(change.Payload))

	pkUUID, parseErr := uuid.Parse(change.PK)
	if parseErr != nil {
		s.logger.Error("Invalid UUID format for PK", "pk", change.PK, "error", parseErr)
		return fmt.Errorf("invalid UUID format: %w", parseErr)
	}

	var err error
	if deleted {
		s.logger.Debug("Calling handler ApplyDelete", "schema", change.Schema, "table", change.Table, "pk", change.PK)
		err = handler.ApplyDelete(ctx, tx, change.Schema, change.Table, pkUUID)
	} else {
		s.logger.Debug("Calling handler ApplyUpsert", "schema", change.Schema, "table", change.Table, "pk", change.PK)
		err = handler.ApplyUpsert(ctx, tx, change.Schema, change.Table, pkUUID, change.Payload)
	}

	if err != nil {
		s.logger.Error("Business table materialization failed",
			"schema", change.Schema, "table", change.Table, "pk", change.PK,
			"deleted", deleted, "error", err)
		return err
	}

	s.logger.Debug("Business table materialization completed successfully",
		"schema", change.Schema, "table", change.Table, "pk", change.PK, "deleted", deleted)
	return nil
}

// recordMaterializeFailure persists a materializer failure into sync.materialize_failures.
// It uses a unique key on (user_id, schema_name, table_name, pk_uuid, attempted_version)
// to increment retry_count on repeated failures of the same attempt.
func (s *SyncService) recordMaterializeFailure(ctx context.Context, tx pgx.Tx, userID, schema, table, pk string, attemptedVersion int64, cause error, op string, payload []byte) error {
	if cause == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `
        INSERT INTO sync.materialize_failures
            (user_id, schema_name, table_name, pk_uuid, attempted_version, op, payload, error)
        VALUES (@user_id, @schema_name, @table_name, @pk_uuid::uuid, @attempted_version, @op, @payload::json, @error)
        ON CONFLICT (user_id, schema_name, table_name, pk_uuid, attempted_version)
        DO UPDATE SET
            retry_count = sync.materialize_failures.retry_count + 1,
            error = EXCLUDED.error,
            op = EXCLUDED.op,
            payload = EXCLUDED.payload
    `, pgx.NamedArgs{
		"user_id":           userID,
		"schema_name":       schema,
		"table_name":        table,
		"pk_uuid":           pk,
		"attempted_version": attemptedVersion,
		"op":                op,
		"payload":           payload,
		"error":             cause.Error(),
	})
	if err != nil {
		s.logger.Warn("Failed to persist materialize failure", "error", err, "schema", schema, "table", table, "pk", pk, "attempted_version", attemptedVersion)
	}
	return err
}
