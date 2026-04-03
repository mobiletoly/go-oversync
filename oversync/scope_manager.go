// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

type ScopeManagerConfig struct {
	Logger *slog.Logger
}

type ScopeManager struct {
	service *SyncService
	logger  *slog.Logger
}

func NewScopeManager(service *SyncService, cfg ScopeManagerConfig) *ScopeManager {
	logger := cfg.Logger
	if logger == nil && service != nil {
		logger = service.logger
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ScopeManager{
		service: service,
		logger:  logger,
	}
}

type ScopeWriteOptions struct {
	WriterID string
}

type ScopeWriteResult struct {
	AutoInitialized bool
	Bundle          *Bundle
}

type ScopeWriteInvalidError struct {
	Message string
}

func (e *ScopeWriteInvalidError) Error() string { return e.Message }

type ScopeWriteNoCapturedChangesError struct {
	ScopeID  string
	WriterID string
}

func (e *ScopeWriteNoCapturedChangesError) Error() string {
	if e == nil {
		return "scope write produced no visible registered-table effects"
	}
	scopeID := strings.TrimSpace(e.ScopeID)
	writerID := strings.TrimSpace(e.WriterID)
	switch {
	case scopeID != "" && writerID != "":
		return fmt.Sprintf("scope write for %s via %s produced no visible registered-table effects", scopeID, writerID)
	case scopeID != "":
		return fmt.Sprintf("scope write for %s produced no visible registered-table effects", scopeID)
	default:
		return "scope write produced no visible registered-table effects"
	}
}

func (m *ScopeManager) ExecWrite(
	ctx context.Context,
	scopeID string,
	opts ScopeWriteOptions,
	fn func(tx pgx.Tx) error,
) (_ *ScopeWriteResult, err error) {
	if m == nil || m.service == nil {
		return nil, &ScopeWriteInvalidError{Message: "scope manager requires a sync service"}
	}

	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return nil, &ScopeWriteInvalidError{Message: "scope_id is required"}
	}

	opts.WriterID = strings.TrimSpace(opts.WriterID)
	if opts.WriterID == "" {
		return nil, &ScopeWriteInvalidError{Message: "writer_id is required"}
	}
	if fn == nil {
		return nil, &ScopeWriteInvalidError{Message: "scope write callback is required"}
	}

	done, err := m.service.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()

	conn, releaseConn, err := m.service.acquireUserUploadConn(ctx, scopeID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var result *ScopeWriteResult
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if err := ensureScopeStateExistsWithExec(ctx, tx, scopeID); err != nil {
			return err
		}

		scopeState, err := loadScopeStateForUpdate(ctx, tx, scopeID)
		if err != nil {
			return err
		}
		scopeState, err = expireInitializationLeaseIfNeeded(ctx, tx, scopeState)
		if err != nil {
			return err
		}

		autoInitialized := false
		switch scopeState.State {
		case scopeStateInitialized:
		case scopeStateInitializing:
			return &ScopeInitializingError{UserID: scopeID, LeaseExpiresAt: scopeState.LeaseExpiresAt}
		case scopeStateUninitialized:
			if err := transitionScopeToInitialized(ctx, tx, scopeID, opts.WriterID); err != nil {
				return err
			}
			autoInitialized = true
		default:
			return fmt.Errorf("unexpected scope state %q for scope %s", scopeState.State, scopeID)
		}

		expectedSourceBundleID, _, err := loadNextExpectedSourceBundleIDForUpdate(ctx, tx, scopeState.UserPK, scopeID, opts.WriterID)
		if err != nil {
			return err
		}

		bundle, err := m.service.withinSyncBundleTx(ctx, tx, Actor{UserID: scopeID}, BundleSource{
			SourceID:       opts.WriterID,
			SourceBundleID: expectedSourceBundleID,
		}, fn)
		if err != nil {
			return err
		}
		if bundle == nil {
			return &ScopeWriteNoCapturedChangesError{ScopeID: scopeID, WriterID: opts.WriterID}
		}

		result = &ScopeWriteResult{
			AutoInitialized: autoInitialized,
			Bundle:          bundle,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
