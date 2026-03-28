// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	scopeStateUninitialized       = "UNINITIALIZED"
	scopeStateInitializing        = "INITIALIZING"
	scopeStateInitialized         = "INITIALIZED"
	defaultInitializationLeaseTTL = 15 * time.Minute
)

type scopeStateRow struct {
	UserID              string
	State               string
	InitializerSourceID string
	InitializationID    string
	LeaseExpiresAt      time.Time
	InitializedAt       time.Time
	InitializedBySource string
}

type ConnectInvalidError struct {
	Message string
}

func (e *ConnectInvalidError) Error() string { return e.Message }

type ScopeUninitializedError struct {
	UserID string
}

func (e *ScopeUninitializedError) Error() string {
	return fmt.Sprintf("sync scope for user %s is not initialized", e.UserID)
}

type ScopeInitializingError struct {
	UserID         string
	LeaseExpiresAt time.Time
}

func (e *ScopeInitializingError) Error() string {
	if e.LeaseExpiresAt.IsZero() {
		return fmt.Sprintf("sync scope for user %s is initializing", e.UserID)
	}
	return fmt.Sprintf("sync scope for user %s is initializing until %s", e.UserID, e.LeaseExpiresAt.UTC().Format(time.RFC3339Nano))
}

type InitializationStaleError struct {
	Message string
}

func (e *InitializationStaleError) Error() string { return e.Message }

type InitializationExpiredError struct {
	Message string
}

func (e *InitializationExpiredError) Error() string { return e.Message }

func (s *SyncService) initializationLeaseTTL() time.Duration {
	if s != nil && s.config != nil && s.config.InitializationLeaseTTL > 0 {
		return s.config.InitializationLeaseTTL
	}
	return defaultInitializationLeaseTTL
}

func ensureScopeStateExistsWithExec(ctx context.Context, exec interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, userID string) error {
	if _, err := exec.Exec(ctx, `
		INSERT INTO sync.scope_state (user_id, state, created_at, updated_at)
		VALUES ($1, 'UNINITIALIZED', now(), now())
		ON CONFLICT (user_id) DO NOTHING
	`, userID); err != nil {
		return fmt.Errorf("ensure scope_state row: %w", err)
	}
	return nil
}

func loadScopeStateForUpdate(ctx context.Context, tx pgx.Tx, userID string) (*scopeStateRow, error) {
	var (
		row                 scopeStateRow
		initializerSourceID sql.NullString
		initializationID    sql.NullString
		leaseExpiresAt      sql.NullTime
		initializedAt       sql.NullTime
		initializedBySource sql.NullString
	)
	if err := tx.QueryRow(ctx, `
		SELECT user_id, state, initializer_source_id, initialization_id::text, lease_expires_at, initialized_at, initialized_by_source_id
		FROM sync.scope_state
		WHERE user_id = $1
		FOR UPDATE
	`, userID).Scan(
		&row.UserID,
		&row.State,
		&initializerSourceID,
		&initializationID,
		&leaseExpiresAt,
		&initializedAt,
		&initializedBySource,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, &ScopeUninitializedError{UserID: userID}
		}
		return nil, fmt.Errorf("load scope_state row: %w", err)
	}
	row.InitializerSourceID = initializerSourceID.String
	row.InitializationID = initializationID.String
	if leaseExpiresAt.Valid {
		row.LeaseExpiresAt = leaseExpiresAt.Time.UTC()
	}
	if initializedAt.Valid {
		row.InitializedAt = initializedAt.Time.UTC()
	}
	row.InitializedBySource = initializedBySource.String
	return &row, nil
}

func currentScopeStateQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string) (string, *time.Time, error) {
	var (
		state          string
		leaseExpiresAt sql.NullTime
	)
	if err := q.QueryRow(ctx, `
		SELECT state, lease_expires_at
		FROM sync.scope_state
		WHERE user_id = $1
	`, userID).Scan(&state, &leaseExpiresAt); err != nil {
		if err == pgx.ErrNoRows {
			return scopeStateUninitialized, nil, nil
		}
		return "", nil, fmt.Errorf("query scope_state row: %w", err)
	}
	if leaseExpiresAt.Valid {
		ts := leaseExpiresAt.Time.UTC()
		return state, &ts, nil
	}
	return state, nil, nil
}

func expireInitializationLeaseIfNeeded(ctx context.Context, tx pgx.Tx, row *scopeStateRow) (*scopeStateRow, error) {
	if row == nil || row.State != scopeStateInitializing || row.LeaseExpiresAt.IsZero() || row.LeaseExpiresAt.After(time.Now().UTC()) {
		return row, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sync.scope_state
		SET state = 'UNINITIALIZED',
			initializer_source_id = NULL,
			initialization_id = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE user_id = $1
	`, row.UserID); err != nil {
		return nil, fmt.Errorf("expire initialization lease: %w", err)
	}
	row.State = scopeStateUninitialized
	row.InitializerSourceID = ""
	row.InitializationID = ""
	row.LeaseExpiresAt = time.Time{}
	return row, nil
}

func ensureUserStateBaselineWithExec(ctx context.Context, exec interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, userID string) error {
	if _, err := exec.Exec(ctx, `
		INSERT INTO sync.user_state (user_id, next_bundle_seq, retained_bundle_floor, updated_at)
		VALUES ($1, 1, 0, now())
		ON CONFLICT (user_id) DO NOTHING
	`, userID); err != nil {
		return fmt.Errorf("ensure user_state baseline row: %w", err)
	}
	return nil
}

func requireScopeInitializedQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string) error {
	state, leaseExpiresAt, err := currentScopeStateQuerier(ctx, q, userID)
	if err != nil {
		return err
	}
	switch state {
	case scopeStateInitialized:
		return nil
	case scopeStateInitializing:
		if leaseExpiresAt != nil && leaseExpiresAt.Before(time.Now().UTC()) {
			return &ScopeUninitializedError{UserID: userID}
		}
		err := &ScopeInitializingError{UserID: userID}
		if leaseExpiresAt != nil {
			err.LeaseExpiresAt = *leaseExpiresAt
		}
		return err
	default:
		return &ScopeUninitializedError{UserID: userID}
	}
}

func requireActiveInitializationLease(ctx context.Context, tx pgx.Tx, userID, sourceID, initializationID string, refreshTo time.Time) (*scopeStateRow, error) {
	row, err := loadScopeStateForUpdate(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	row, err = expireInitializationLeaseIfNeeded(ctx, tx, row)
	if err != nil {
		return nil, err
	}
	if row.State != scopeStateInitializing {
		return nil, &InitializationStaleError{Message: "scope is no longer initializing"}
	}
	if !row.LeaseExpiresAt.After(time.Now().UTC()) {
		return nil, &InitializationExpiredError{Message: "initialization lease has expired"}
	}
	if strings.TrimSpace(sourceID) == "" || row.InitializerSourceID != sourceID {
		return nil, &InitializationStaleError{Message: "source does not own the active initialization lease"}
	}
	if strings.TrimSpace(initializationID) == "" || row.InitializationID != initializationID {
		return nil, &InitializationStaleError{Message: "initialization_id does not match the active initialization lease"}
	}
	if !refreshTo.IsZero() {
		if _, err := tx.Exec(ctx, `
			UPDATE sync.scope_state
			SET lease_expires_at = $2,
				updated_at = now()
			WHERE user_id = $1
		`, userID, refreshTo.UTC()); err != nil {
			return nil, fmt.Errorf("refresh initialization lease: %w", err)
		}
		row.LeaseExpiresAt = refreshTo.UTC()
	}
	return row, nil
}

func transitionScopeToInitialized(ctx context.Context, tx pgx.Tx, userID, sourceID string) error {
	if err := ensureUserStateBaselineWithExec(ctx, tx, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sync.scope_state
		SET state = 'INITIALIZED',
			initializer_source_id = NULL,
			initialization_id = NULL,
			lease_expires_at = NULL,
			initialized_at = now(),
			initialized_by_source_id = $2,
			updated_at = now()
		WHERE user_id = $1
	`, userID, sourceID); err != nil {
		return fmt.Errorf("transition scope_state to INITIALIZED: %w", err)
	}
	return nil
}

func transitionScopeToInitializing(ctx context.Context, tx pgx.Tx, userID, sourceID string, leaseTTL time.Duration) (*scopeStateRow, error) {
	if err := ensureUserStateBaselineWithExec(ctx, tx, userID); err != nil {
		return nil, err
	}
	initializationID := uuid.NewString()
	leaseExpiresAt := time.Now().UTC().Add(leaseTTL)
	if _, err := tx.Exec(ctx, `
		UPDATE sync.scope_state
		SET state = 'INITIALIZING',
			initializer_source_id = $2,
			initialization_id = $3::uuid,
			lease_expires_at = $4,
			updated_at = now()
		WHERE user_id = $1
	`, userID, sourceID, initializationID, leaseExpiresAt); err != nil {
		return nil, fmt.Errorf("transition scope_state to INITIALIZING: %w", err)
	}
	return &scopeStateRow{
		UserID:              userID,
		State:               scopeStateInitializing,
		InitializerSourceID: sourceID,
		InitializationID:    initializationID,
		LeaseExpiresAt:      leaseExpiresAt,
	}, nil
}
