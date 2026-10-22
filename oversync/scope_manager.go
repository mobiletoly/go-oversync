// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"log/slog"

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
	// MaxBytesPerBundle optionally tightens the service canonical effect limit.
	// Include application-selected limits in OperationHash when freezing a request.
	MaxBytesPerBundle int
	WriterID          string
	// RequireExistingScope rejects absent or uninitialized scopes instead of
	// creating sync metadata. Include this choice in OperationHash.
	RequireExistingScope bool
	RetryableWriteOptions
	RequiredEffectTables  []RegisteredEffectTable
	ForbiddenEffectTables []RegisteredEffectTable
}

type ScopeWriteResult struct {
	AutoInitialized bool
	Bundle          CommittedBundleRef
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
	scopeID := e.ScopeID
	writerID := e.WriterID
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
	fn RetryableDatabaseWrite,
) (_ *ScopeWriteResult, err error) {
	if m == nil || m.service == nil {
		return nil, &ScopeWriteInvalidError{Message: "scope manager requires a sync service"}
	}
	defer func() { err = m.service.classifyApplicationWriteError(err) }()
	if err := rejectRetryableCallbackContext(ctx, "ScopeManager.ExecWrite"); err != nil {
		return nil, err
	}
	if scopeID == "" {
		return nil, &ScopeWriteInvalidError{Message: "scope_id is required"}
	}
	if err := validateWriterID(opts.WriterID); err != nil {
		return nil, &ScopeWriteInvalidError{Message: err.Error()}
	}
	if err := validateRetryableWriteOptions(opts.RetryableWriteOptions); err != nil {
		return nil, &ScopeWriteInvalidError{Message: err.Error()}
	}
	if opts.MaxBytesPerBundle < 0 || opts.MaxBytesPerBundle > m.service.config.MaxBytesPerBundle {
		return nil, &ScopeWriteInvalidError{Message: "invalid scope write byte limit"}
	}
	if fn == nil {
		return nil, &ScopeWriteInvalidError{Message: "scope write callback is required"}
	}
	requiredEffects, err := m.service.canonicalizeEffectSet(opts.RequiredEffectTables)
	if err != nil {
		return nil, &ScopeWriteInvalidError{Message: err.Error()}
	}
	forbiddenEffects, err := m.service.canonicalizeEffectSet(opts.ForbiddenEffectTables)
	if err != nil {
		return nil, &ScopeWriteInvalidError{Message: err.Error()}
	}
	if err := validateDisjointEffectSets(requiredEffects, forbiddenEffects); err != nil {
		return nil, &ScopeWriteInvalidError{Message: err.Error()}
	}

	done, err := m.service.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	conn, releaseConn, err := m.service.acquireUserUploadConn(ctx, scopeID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	result, err := runRetryableWriteTransaction(ctx, conn, func(tx pgx.Tx) (*ScopeWriteResult, error) {
		return m.executeScopeWriteInTx(ctx, tx, scopeID, opts, fn, requiredEffects, forbiddenEffects)
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func markReservedServerSourceUsed(ctx context.Context, tx pgx.Tx, writerID string, required bool) error {
	var everUsed bool
	err := tx.QueryRow(ctx, `
		SELECT ever_used
		FROM sync.server_source_reservations
		WHERE source_id = $1
		FOR KEY SHARE
	`, writerID).Scan(&everUsed)
	if err == pgx.ErrNoRows {
		if required {
			return fmt.Errorf("configured server source reservation %q is unavailable", writerID)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock server source reservation: %w", err)
	}
	if everUsed {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sync.server_source_reservations
		SET ever_used = TRUE
		WHERE source_id = $1 AND ever_used = FALSE
	`, writerID); err != nil {
		return fmt.Errorf("publish server source reservation use: %w", err)
	}
	return nil
}

func (s *SyncService) rejectForbiddenRawEffects(ctx context.Context, tx pgx.Tx, userPK int64, forbidden canonicalEffectSet) error {
	if len(forbidden.keys) == 0 {
		return nil
	}
	var txid int64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txid); err != nil {
		return fmt.Errorf("read current txid for forbidden effect validation: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT DISTINCT table_id
		FROM sync.bundle_capture_stage
		WHERE txid = $1 AND user_pk = $2
	`, txid, userPK)
	if err != nil {
		return fmt.Errorf("load raw captured effects: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableID int32
		if err := rows.Scan(&tableID); err != nil {
			return fmt.Errorf("scan raw captured effect: %w", err)
		}
		info, err := s.tableInfoForID(tableID)
		if err != nil {
			return err
		}
		if _, rejected := forbidden.keys[Key(info.schemaName, info.tableName)]; rejected {
			return &RetryableCallbackViolationError{Message: "retryable database-write callback produced a forbidden registered-table effect"}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate raw captured effects: %w", err)
	}
	return nil
}

func requireNormalizedEffects(bundle *Bundle, required canonicalEffectSet) error {
	if len(required.keys) == 0 {
		return nil
	}
	for _, row := range bundle.Rows {
		if _, matched := required.keys[Key(row.Schema, row.Table)]; matched {
			return nil
		}
	}
	return &RetryableCallbackViolationError{Message: "retryable database-write callback produced no required normalized registered-table effect"}
}

// prepareScopeWriteInTx admits lifecycle policy, enforces existing-only writes,
// marks trusted reservations, then creates or resolves the scope's metadata.
// All locks and SQL effects remain in the attempt's transaction.
func (s *SyncService) prepareScopeWriteInTx(ctx context.Context, tx pgx.Tx, scopeID string, opts ScopeWriteOptions) (int64, error) {
	if err := s.admitScopeTransaction(ctx, tx, ScopeTransactionOperation{ScopeID: scopeID, Kind: ScopeTransactionServerWrite, SourceID: opts.WriterID}); err != nil {
		return 0, err
	}
	if opts.RequireExistingScope {
		state, err := loadScopeStateForUpdate(ctx, tx, scopeID)
		if err != nil {
			return 0, err
		}
		if state.State != scopeStateInitialized {
			return 0, &ScopeUninitializedError{UserID: scopeID}
		}
	}
	if err := markReservedServerSourceUsed(ctx, tx, opts.WriterID, s.isReservedServerSourceID(opts.WriterID)); err != nil {
		return 0, err
	}
	if !opts.RequireExistingScope {
		if err := ensureScopeStateExistsWithExec(ctx, tx, scopeID); err != nil {
			return 0, err
		}
	}
	return lookupUserPK(ctx, tx, scopeID)
}

// executeScopeWriteInTx owns the receipt, callback and canonical effects within
// one transaction, admitting lifecycle policy before initialization or receipt replay.
// ExecWrite owns readiness admission, connection and bounded retry.
func (m *ScopeManager) executeScopeWriteInTx(ctx context.Context, tx pgx.Tx, scopeID string, opts ScopeWriteOptions, fn RetryableDatabaseWrite, requiredEffects, forbiddenEffects canonicalEffectSet) (*ScopeWriteResult, error) {
	userPK, err := m.service.prepareScopeWriteInTx(ctx, tx, scopeID, opts)
	if err != nil {
		return nil, err
	}
	if err := requireServerWriteSource(ctx, tx, userPK, opts.WriterID); err != nil {
		return nil, err
	}
	decision, err := acquireScopeWriteReceipt(ctx, tx, scopeWriteReceiptSpec{
		userPK:               userPK,
		operationID:          opts.OperationID,
		operationHash:        opts.OperationHash,
		requiredEffectsHash:  requiredEffects.hash,
		forbiddenEffectsHash: forbiddenEffects.hash,
		writerID:             opts.WriterID,
		operationValidUntil:  opts.OperationValidUntil,
	})
	if err != nil {
		return nil, err
	}
	if decision.replay != nil {
		return decision.replay, nil
	}

	scopeState, err := loadScopeStateForUpdate(ctx, tx, scopeID)
	if err != nil {
		return nil, err
	}
	scopeState, err = expireInitializationLeaseIfNeeded(ctx, tx, scopeState)
	if err != nil {
		return nil, err
	}

	autoInitialized := false
	switch scopeState.State {
	case scopeStateInitialized:
	case scopeStateInitializing:
		return nil, &ScopeInitializingError{UserID: scopeID, LeaseExpiresAt: scopeState.LeaseExpiresAt}
	case scopeStateUninitialized:
		if err := transitionScopeToInitialized(ctx, tx, scopeID, opts.WriterID); err != nil {
			return nil, err
		}
		autoInitialized = true
	default:
		return nil, fmt.Errorf("unexpected scope state %q for scope %s", scopeState.State, scopeID)
	}

	expectedSourceBundleID, _, err := loadNextExpectedSourceBundleIDForUpdate(ctx, tx, scopeState.UserPK, scopeID, opts.WriterID)
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
		return nil, fmt.Errorf("defer bundle constraints: %w", err)
	}
	source := BundleSource{
		SourceID:       opts.WriterID,
		SourceBundleID: expectedSourceBundleID,
	}
	if err := setBundleTxContext(ctx, tx, bundleTxContext{
		UserID:         scopeID,
		UserPK:         scopeState.UserPK,
		SourceID:       opts.WriterID,
		SourceBundleID: expectedSourceBundleID,
	}); err != nil {
		return nil, err
	}
	callbackCtx, capability, err := newRestrictedDatabaseWriteTx(ctx, tx, scopeID)
	if err != nil {
		return nil, err
	}
	if err := fn(callbackCtx, capability); err != nil {
		return nil, err
	}
	if err := finishDeferredBundleEffects(ctx, tx); err != nil {
		return nil, err
	}
	if err := m.service.rejectForbiddenRawEffects(ctx, tx, scopeState.UserPK, forbiddenEffects); err != nil {
		return nil, err
	}
	bundle, err := m.service.finalizeCapturedBundleWithLimit(ctx, tx, Actor{UserID: scopeID}, scopeState.UserPK, source, opts.MaxBytesPerBundle)
	if err != nil {
		return nil, err
	}
	if bundle == nil {
		if len(requiredEffects.keys) != 0 {
			return nil, &RetryableCallbackViolationError{Message: "retryable database-write callback produced no required normalized registered-table effect"}
		}
		return nil, &ScopeWriteNoCapturedChangesError{ScopeID: scopeID, WriterID: opts.WriterID}
	}
	if err := requireNormalizedEffects(bundle, requiredEffects); err != nil {
		return nil, err
	}
	if err := activateSourceState(ctx, tx, scopeState.UserPK, scopeID, opts.WriterID, expectedSourceBundleID); err != nil {
		return nil, err
	}
	if err := m.service.applyRetentionPolicyForUser(ctx, tx, scopeState.UserPK); err != nil {
		return nil, err
	}

	result := &ScopeWriteResult{
		AutoInitialized: autoInitialized,
		Bundle:          committedBundleRef(bundle),
	}
	if err := completeScopeWriteReceipt(ctx, tx, scopeState.UserPK, opts.OperationID, result); err != nil {
		return nil, err
	}
	return result, nil
}
