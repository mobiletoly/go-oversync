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

// insertChangeLog inserts a change into the server change log after successful application
func (s *SyncService) insertChangeLog(ctx context.Context, tx pgx.Tx, userID, sourceID string, ch ChangeUpload) error {
	// Use NULL payload for DELETE operations for cleaner stream
	var payload any = nil
	if ch.Op != OpDelete {
		payload = ch.Payload
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO sync.server_change_log
			(user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
		VALUES (@user_id, @schema_name, @table_name, @op, @pk_uuid::uuid, @payload::jsonb, @source_id, @source_change_id, @server_version)
		ON CONFLICT (user_id, source_id, source_change_id) DO NOTHING`,
		pgx.NamedArgs{
			"user_id":          userID,
			"schema_name":      ch.Schema,
			"table_name":       ch.Table,
			"op":               ch.Op,
			"pk_uuid":          ch.PK,
			"payload":          payload,
			"source_id":        sourceID,
			"source_change_id": ch.SourceChangeID,
			"server_version":   0, // Default value, will be updated later if needed
		},
	)
	return err
}

// applyUpsert applies an INSERT/UPDATE operation with SAVEPOINT isolation
func (s *SyncService) applyUpsert(ctx context.Context, tx pgx.Tx, userID, sourceID string, change ChangeUpload) (ChangeUploadStatus, error) {
	s.logger.Info("Starting applyUpsert",
		"schema", change.Schema, "table", change.Table, "pk", change.PK,
		"source_change_id", change.SourceChangeID, "server_version", change.ServerVersion,
		"payload_size", len(change.Payload))

	// Create SAVEPOINT for isolation
	spName := fmt.Sprintf("sp_%d", change.SourceChangeID)
	_, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
	if err != nil {
		return ChangeUploadStatus{}, fmt.Errorf("failed to create savepoint: %w", err)
	}

	// Idempotency gate: attempt to insert into change log first. If it's a duplicate, we skip work.
	// Using SAVEPOINT ensures rollback will remove this insert if later steps fail.
	var insertedTag pgconn.CommandTag
	// execute prepared gate insert (upsert path with payload) - we'll get server_version after version gate
	insertedTag, err = tx.Exec(ctx, stmtGateInsertUpsert,
		userID, change.Schema, change.Table, change.Op, change.PK, change.Payload, sourceID, change.SourceChangeID, 0,
	)
	if err != nil {
		// Under high concurrency, a serialization failure (40001) or deadlock (40P01)
		// during unique enforcement indicates another txn won the insert. Treat as idempotent.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.SQLState() == "40001" || pgErr.SQLState() == "40P01") {
			// Clear error state within the SAVEPOINT scope
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			// Optionally drop the savepoint
			_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			return statusAppliedIdempotent(change.SourceChangeID), nil
		}
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to insert change log (gate): %w", err)
	}
	if insertedTag.RowsAffected() == 0 {
		// Duplicate SCID for this (user, source). Treat as idempotent success.
		// No side effects applied.
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return statusAppliedIdempotent(change.SourceChangeID), nil
	}

	// Ensure meta for brand-new rows (only if incoming server_version == 0)
	if change.ServerVersion == 0 {
		_, err = tx.Exec(ctx, stmtEnsureMeta, userID, change.Schema, change.Table, change.PK)
		if err != nil {
			s.logger.Error("Failed to ensure meta for brand-new row", "error", err,
				"schema", change.Schema, "table", change.Table, "pk", change.PK)
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			return statusInvalidOther(change.SourceChangeID, ReasonBadPayload, err), nil
		}
	}

	// Version gate (optimistic concurrency)
	var newServerVersion int64
	err = tx.QueryRow(ctx, stmtVersionGateUp,
		userID, change.Schema, change.Table, change.PK, change.ServerVersion,
	).Scan(&newServerVersion)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logger.Error("Version gate failed - potential conflict detected",
				"schema", change.Schema, "table", change.Table, "pk", change.PK,
				"client_version", change.ServerVersion, "operation", "INSERT/UPDATE")

			// Conflict: fetch current server state
			serverRow, fetchErr := s.fetchServerRowJSON(ctx, tx, userID, change.Schema, change.Table, change.PK)
			if fetchErr != nil {
				if errors.Is(fetchErr, pgx.ErrNoRows) {
					// Row was deleted - still a conflict, but with no current server state
					s.logger.Warn("Conflict: Row was deleted on server",
						"schema", change.Schema, "table", change.Table, "pk", change.PK)
					_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
					return statusConflict(change.SourceChangeID, nil), nil
				}
				_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
				return ChangeUploadStatus{}, fmt.Errorf("failed to fetch server row for conflict: %w", fetchErr)
			}
			s.logger.Warn("Conflict: Version mismatch detected",
				"schema", change.Schema, "table", change.Table, "pk", change.PK,
				"client_version", change.ServerVersion)
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			return statusConflict(change.SourceChangeID, serverRow), nil
		}
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to update sync_row_meta: %w", err)
	}

	// Upsert into sync_state
	_, err = tx.Exec(ctx, stmtUpsertState, userID, change.Schema, change.Table, change.PK, change.Payload)
	if err != nil {
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		// Record failure for diagnostics and admin retry visibility
		_ = s.recordMaterializeFailure(ctx, tx, userID, change.Schema, change.Table, change.PK, newServerVersion, err, change.Op, change.Payload)
		return statusMaterializeError(change.SourceChangeID, newServerVersion, err), nil
	}

	// Business projection (optional materialization)
	if err := s.applyBusinessProjection(ctx, tx, change, false); err != nil {
		s.logger.Error("Failed to apply business projection", "error", err, "schema", change.Schema, "table", change.Table, "pk", change.PK)
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		// Record failure for diagnostics and admin retry visibility
		_ = s.recordMaterializeFailure(ctx, tx, userID, change.Schema, change.Table, change.PK, newServerVersion, err, change.Op, change.Payload)
		return statusMaterializeError(change.SourceChangeID, newServerVersion, err), nil
	}

	// Update the server_change_log with the correct server_version (it was inserted with 0 as placeholder)
	_, err = tx.Exec(ctx, `
		UPDATE sync.server_change_log
		SET server_version = $1
		WHERE user_id = $2 AND source_id = $3 AND source_change_id = $4`,
		newServerVersion, userID, sourceID, change.SourceChangeID)
	if err != nil {
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to update server_change_log with server_version: %w", err)
	}

	// Release SAVEPOINT
	_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
	if err != nil {
		return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
	}

	return statusApplied(change.SourceChangeID, newServerVersion), nil
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

	// Idempotency gate: insert into change log first; duplicate implies idempotent
	{
		var insertedTag pgconn.CommandTag
		insertedTag, err = tx.Exec(ctx, stmtGateInsertDelete,
			userID, change.Schema, change.Table, change.Op, change.PK, sourceID, change.SourceChangeID, 0,
		)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && (pgErr.SQLState() == "40001" || pgErr.SQLState() == "40P01") {
				_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
				_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
				return statusAppliedIdempotent(change.SourceChangeID), nil
			}
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			return ChangeUploadStatus{}, fmt.Errorf("failed to insert change log (gate): %w", err)
		}
		if insertedTag.RowsAffected() == 0 {
			_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			return statusAppliedIdempotent(change.SourceChangeID), nil
		}
	}

	// Version gate (optimistic concurrency)
	var newServerVersion int64
	err = tx.QueryRow(ctx, stmtVersionGateDel,
		userID, change.Schema, change.Table, change.PK, change.ServerVersion,
	).Scan(&newServerVersion)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.logger.Warn("Delete version gate failed - potential conflict detected",
				"schema", change.Schema, "table", change.Table, "pk", change.PK,
				"client_version", change.ServerVersion, "operation", "DELETE")

			// Record doesn't exist on server - check if it exists at all
			serverRow, fetchErr := s.fetchServerRowJSON(ctx, tx, userID, change.Schema, change.Table, change.PK)
			if fetchErr != nil {
				if errors.Is(fetchErr, pgx.ErrNoRows) {
					// Record doesn't exist at all - treat as "already deleted"
					s.logger.Info("Delete idempotent: Record already deleted",
						"schema", change.Schema, "table", change.Table, "pk", change.PK)
					_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
					return statusAppliedIdempotent(change.SourceChangeID), nil
				}
				_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
				return ChangeUploadStatus{}, fmt.Errorf("failed to fetch server row for conflict: %w", fetchErr)
			}
			// Record exists but version mismatch - conflict
			s.logger.Warn("Delete conflict: Version mismatch detected",
				"schema", change.Schema, "table", change.Table, "pk", change.PK,
				"client_version", change.ServerVersion)
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
			return statusConflict(change.SourceChangeID, serverRow), nil
		}
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to update sync_row_meta: %w", err)
	}

	// Remove from sync_state
	_, err = tx.Exec(ctx, stmtDeleteState, userID, change.Schema, change.Table, change.PK)
	if err != nil {
		s.logger.Warn("Failed to delete from sync_state", "error", err, "table", change.Table, "pk", change.PK)
		// Non-fatal - continue processing
	}

	// Business projection (optional materialization)
	if err := s.applyBusinessProjection(ctx, tx, change, true); err != nil {
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		// Record failure for diagnostics and admin retry visibility
		_ = s.recordMaterializeFailure(ctx, tx, userID, change.Schema, change.Table, change.PK, newServerVersion, err, change.Op, nil)
		return statusMaterializeError(change.SourceChangeID, newServerVersion, err), nil
	}

	// Update the server_change_log with the correct server_version (it was inserted with 0 as placeholder)
	_, err = tx.Exec(ctx, `
		UPDATE sync.server_change_log
		SET server_version = $1
		WHERE user_id = $2 AND source_id = $3 AND source_change_id = $4`,
		newServerVersion, userID, sourceID, change.SourceChangeID)
	if err != nil {
		_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
		return ChangeUploadStatus{}, fmt.Errorf("failed to update server_change_log with server_version: %w", err)
	}

	// Release SAVEPOINT
	_, err = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", pgx.Identifier{spName}.Sanitize()))
	if err != nil {
		return ChangeUploadStatus{}, fmt.Errorf("failed to release savepoint: %w", err)
	}

	return statusApplied(change.SourceChangeID, newServerVersion), nil
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
        VALUES (@user_id, @schema_name, @table_name, @pk_uuid::uuid, @attempted_version, @op, @payload::jsonb, @error)
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
