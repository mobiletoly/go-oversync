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
	scopeStateCodeUninitialized   = int16(0)
	scopeStateCodeInitializing    = int16(1)
	scopeStateCodeInitialized     = int16(2)
	defaultInitializationLeaseTTL = 15 * time.Minute
)

type scopeStateRow struct {
	UserPK              int64
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

func scopeStateNameFromCode(code int16) (string, error) {
	switch code {
	case scopeStateCodeUninitialized:
		return scopeStateUninitialized, nil
	case scopeStateCodeInitializing:
		return scopeStateInitializing, nil
	case scopeStateCodeInitialized:
		return scopeStateInitialized, nil
	default:
		return "", fmt.Errorf("unexpected scope state code %d", code)
	}
}

func ensureScopeStateExistsWithExec(ctx context.Context, exec interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, userID string) error {
	if err := ensureUserStateBaselineWithExec(ctx, exec, userID); err != nil {
		return err
	}
	if _, err := exec.Exec(ctx, `
		INSERT INTO sync.scope_state (user_pk, state_code)
		SELECT user_pk, $2
		FROM sync.user_state
		WHERE user_id = $1
		ON CONFLICT (user_pk) DO NOTHING
	`, userID, scopeStateCodeUninitialized); err != nil {
		return fmt.Errorf("ensure scope_state row: %w", err)
	}
	return nil
}

func loadScopeStateForUpdate(ctx context.Context, tx pgx.Tx, userID string) (*scopeStateRow, error) {
	var (
		row                 scopeStateRow
		stateCode           int16
		initializerSourceID sql.NullString
		initializationID    sql.NullString
		leaseExpiresAt      sql.NullTime
		initializedAt       sql.NullTime
		initializedBySource sql.NullString
	)
	if err := tx.QueryRow(ctx, `
		SELECT
			us.user_pk,
			us.user_id,
			ss.state_code,
			ss.initializer_source_id,
			ss.initialization_id::text,
			ss.lease_expires_at,
			ss.initialized_at,
			ss.initialized_by_source_id
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
		FOR UPDATE
	`, userID).Scan(
		&row.UserPK,
		&row.UserID,
		&stateCode,
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
	stateName, err := scopeStateNameFromCode(stateCode)
	if err != nil {
		return nil, err
	}
	row.State = stateName
	row.InitializedBySource = initializedBySource.String
	return &row, nil
}

func currentScopeStateQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string) (string, *time.Time, error) {
	var (
		stateCode      int16
		leaseExpiresAt sql.NullTime
	)
	if err := q.QueryRow(ctx, `
		SELECT ss.state_code, ss.lease_expires_at
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&stateCode, &leaseExpiresAt); err != nil {
		if err == pgx.ErrNoRows {
			return scopeStateUninitialized, nil, nil
		}
		return "", nil, fmt.Errorf("query scope_state row: %w", err)
	}
	state, err := scopeStateNameFromCode(stateCode)
	if err != nil {
		return "", nil, err
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
		SET state_code = $2,
			initializer_source_id = NULL,
			initialization_id = NULL,
			lease_expires_at = NULL
		WHERE user_pk = $1
	`, row.UserPK, scopeStateCodeUninitialized); err != nil {
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
		INSERT INTO sync.user_state (user_id, next_bundle_seq, retained_bundle_floor)
		VALUES ($1, 1, 0)
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
			SET lease_expires_at = $2
			WHERE user_pk = $1
		`, row.UserPK, refreshTo.UTC()); err != nil {
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
		SET state_code = $2,
			initializer_source_id = NULL,
			initialization_id = NULL,
			lease_expires_at = NULL,
			initialized_at = now(),
			initialized_by_source_id = $3
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID, scopeStateCodeInitialized, sourceID); err != nil {
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
		SET state_code = $2,
			initializer_source_id = $3,
			initialization_id = $4::uuid,
			lease_expires_at = $5,
			initialized_at = NULL,
			initialized_by_source_id = NULL
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID, scopeStateCodeInitializing, sourceID, initializationID, leaseExpiresAt); err != nil {
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
