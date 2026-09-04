// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mobiletoly/go-oversync/internal/jcs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type userStateExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type userStateQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type userUploadConnTestHooks struct {
	beforeAcquire        func(context.Context) error
	beforeAdvisoryLock   func(context.Context, *pgxpool.Conn) error
	afterConflictRowScan func(conflictRowScanObservation)
}

type conflictRowScanObservation struct {
	ReportedPayloadByteCount int64
	PayloadMaterialized      bool
	MaterializedPayloadBytes int
}

// userUploadGate keeps same-service operations for one user from consuming
// multiple pool connections while PostgreSQL's cross-process advisory lock is
// held elsewhere.
type userUploadGate struct {
	token chan struct{}
	refs  int
}

func (s *SyncService) acquireLocalUserUploadGate(ctx context.Context, userID string) (func(), error) {
	s.userUploadGatesMu.Lock()
	if s.userUploadGates == nil {
		s.userUploadGates = make(map[string]*userUploadGate)
	}
	gate := s.userUploadGates[userID]
	if gate == nil {
		gate = &userUploadGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		s.userUploadGates[userID] = gate
	}
	gate.refs++
	s.userUploadGatesMu.Unlock()

	select {
	case <-gate.token:
		var once sync.Once
		return func() {
			once.Do(func() {
				gate.token <- struct{}{}
				s.releaseLocalUserUploadGateRef(userID, gate)
			})
		}, nil
	case <-ctx.Done():
		s.releaseLocalUserUploadGateRef(userID, gate)
		return nil, ctx.Err()
	}
}

func (s *SyncService) releaseLocalUserUploadGateRef(userID string, gate *userUploadGate) {
	s.userUploadGatesMu.Lock()
	defer s.userUploadGatesMu.Unlock()
	gate.refs--
	if gate.refs == 0 && s.userUploadGates[userID] == gate {
		delete(s.userUploadGates, userID)
	}
}

func ensureUserStateExistsWithExec(ctx context.Context, exec userStateExecer, userID string) error {
	if _, err := exec.Exec(ctx, `
		INSERT INTO sync.user_state (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING
	`, userID); err != nil {
		return fmt.Errorf("ensure user_state row: %w", err)
	}
	return nil
}

func lookupUserPK(ctx context.Context, q userStateQuerier, userID string) (int64, error) {
	var userPK int64
	if err := q.QueryRow(ctx, `
		SELECT user_pk
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&userPK); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("missing user_state row for %q", userID)
		}
		return 0, fmt.Errorf("lookup user_pk for %q: %w", userID, err)
	}
	return userPK, nil
}

type bundleTxContext struct {
	UserID           string
	UserPK           int64
	SourceID         string
	SourceBundleID   int64
	InitializationID string
}

func setBundleTxContext(ctx context.Context, tx pgx.Tx, bundle bundleTxContext) error {
	settings := []struct {
		name  string
		value string
	}{
		{name: "oversync.bundle_user_id", value: bundle.UserID},
		{name: "oversync.bundle_user_pk", value: strconv.FormatInt(bundle.UserPK, 10)},
		{name: "oversync.bundle_source_id", value: bundle.SourceID},
		{name: "oversync.bundle_source_bundle_id", value: strconv.FormatInt(bundle.SourceBundleID, 10)},
		{name: "oversync.bundle_initialization_id", value: bundle.InitializationID},
	}
	for _, setting := range settings {
		var existing string
		if err := tx.QueryRow(ctx, `SELECT COALESCE(current_setting($1, true), '')`, setting.name).Scan(&existing); err != nil {
			return fmt.Errorf("read existing bundle tx setting %s: %w", setting.name, err)
		}
		if existing != "" && existing != setting.value {
			return fmt.Errorf("transaction already carries conflicting sync bundle context for %s", setting.name)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config($1, $2, true)`, setting.name, setting.value); err != nil {
			return fmt.Errorf("set bundle tx setting %s: %w", setting.name, err)
		}
	}
	return nil
}

// acquireUserUploadConn pins push processing for one user to one connection and acquires
// a session-level advisory lock before the bundle transaction begins.
func (s *SyncService) acquireUserUploadConn(ctx context.Context, userID string) (*pgxpool.Conn, func(), error) {
	releaseLocalGate, err := s.acquireLocalUserUploadGate(ctx, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("acquire push connection: %w", err)
	}
	if s.uploadConnHooks != nil && s.uploadConnHooks.beforeAcquire != nil {
		if err := s.uploadConnHooks.beforeAcquire(ctx); err != nil {
			releaseLocalGate()
			return nil, nil, fmt.Errorf("acquire push connection: %w", err)
		}
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		releaseLocalGate()
		return nil, nil, fmt.Errorf("acquire push connection: %w", err)
	}
	if err := attestOriginSession(ctx, conn); err != nil {
		conn.Release()
		releaseLocalGate()
		return nil, nil, err
	}
	if s.uploadConnHooks != nil && s.uploadConnHooks.beforeAdvisoryLock != nil {
		if err := s.uploadConnHooks.beforeAdvisoryLock(ctx, conn); err != nil {
			conn.Release()
			releaseLocalGate()
			return nil, nil, fmt.Errorf("acquire user advisory lock: %w", err)
		}
	}

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, userID); err != nil {
		conn.Release()
		releaseLocalGate()
		return nil, nil, fmt.Errorf("acquire user advisory lock: %w", err)
	}

	release := func() {
		defer releaseLocalGate()
		defer conn.Release()
		if conn.Conn().IsClosed() {
			s.logger.Debug("Skipped releasing user advisory lock because connection is already closed", "user_id", userID)
			return
		}

		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, userID); err != nil {
			if conn.Conn().IsClosed() {
				s.logger.Debug("Skipped releasing user advisory lock because connection closed before unlock completed", "user_id", userID, "error", err)
				return
			}
			s.logger.Error("Failed to release user advisory lock", "user_id", userID, "error", err)
		}
	}
	return conn, release, nil
}

func ensureUserStatePresent(ctx context.Context, tx pgx.Tx, userID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&exists); err != nil {
		return fmt.Errorf("check user_state row: %w", err)
	}
	if !exists {
		return fmt.Errorf("missing user_state row for %q", userID)
	}
	return nil
}

func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	return jcs.Canonicalize(raw)
}
