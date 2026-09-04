// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type retryableReceiptFixture struct {
	ctx  context.Context
	pool interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
		QueryRow(context.Context, string, ...any) pgx.Row
	}
	service    *SyncService
	manager    *ScopeManager
	schemaName string
	tableIdent string
	userID     string
}

type lostCommitResponseTx struct {
	pgx.Tx
	commit bool
	err    error
}

func (tx *lostCommitResponseTx) Commit(ctx context.Context) error {
	var terminalErr error
	if tx.commit {
		terminalErr = tx.Tx.Commit(ctx)
	} else {
		terminalErr = tx.Tx.Rollback(ctx)
	}
	if terminalErr != nil {
		return terminalErr
	}
	return tx.err
}

func newRetryableReceiptFixture(t *testing.T) *retryableReceiptFixture {
	t.Helper()
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "retryable_receipt_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "retryable-receipt-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		ReservedServerSourceIDs:        []string{"server-writer", "server-unused"},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	return &retryableReceiptFixture{
		ctx:        ctx,
		pool:       pool,
		service:    service,
		manager:    NewScopeManager(service, ScopeManagerConfig{}),
		schemaName: schemaName,
		tableIdent: pgx.Identifier{schemaName, "users"}.Sanitize(),
		userID:     "retryable-receipt-user-" + suffix,
	}
}

func retryableOptionsForLogicalWork(label string) RetryableWriteOptions {
	return RetryableWriteOptions{
		OperationID:         uuid.NewSHA1(uuid.NameSpaceOID, []byte(label)),
		OperationHash:       sha256.Sum256([]byte(label)),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour),
	}
}

func (f *retryableReceiptFixture) insertUser(callbackCtx context.Context, tx DatabaseWriteTx, rowID uuid.UUID, name string) error {
	_, err := tx.Exec(callbackCtx, fmt.Sprintf(`INSERT INTO %s (id, name, email) VALUES ($1, $2, $3)`, f.tableIdent), rowID, name, name+"@example.com")
	return err
}

func (f *retryableReceiptFixture) insertExpiredCompletedReceipts(t *testing.T, count int) []uuid.UUID {
	t.Helper()
	_, err := f.service.pool.Exec(f.ctx, `INSERT INTO sync.user_state (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`, f.userID)
	require.NoError(t, err)
	operationIDs := make([]uuid.UUID, count)
	for i := range operationIDs {
		operationIDs[i] = uuid.New()
	}
	hash := sha256.Sum256([]byte("expired completed receipt"))
	_, err = f.service.pool.Exec(f.ctx, `
		WITH receipt_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS current_time
		)
		INSERT INTO sync.scope_write_receipts (
			user_pk, operation_id, operation_hash, required_effect_tables_hash,
			forbidden_effect_tables_hash, writer_id, operation_valid_until,
			receipt_state, committed_source_bundle_id, committed_bundle_seq,
			committed_row_count, committed_bundle_hash, committed_canonical_request_hash,
			auto_initialized, completed_at, receipt_expires_at
		)
		SELECT user_state.user_pk, operation_id, $3, $3, $3, 'server-writer',
			receipt_clock.current_time - interval '72 hours', 'completed', 1, 1, 1,
			$3, '', FALSE, receipt_clock.current_time - interval '48 hours',
			receipt_clock.current_time - interval '24 hours'
		FROM sync.user_state AS user_state
		CROSS JOIN receipt_clock
		CROSS JOIN unnest($2::uuid[]) AS operation_id
		WHERE user_state.user_id = $1
	`, f.userID, operationIDs, hash[:])
	require.NoError(t, err)
	return operationIDs
}

func (f *retryableReceiptFixture) installReceiptDeleteTrigger(t *testing.T, body string) {
	t.Helper()
	_, err := f.service.pool.Exec(f.ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION sync.test_receipt_delete_hook()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $function$
		BEGIN
			%s
			RETURN OLD;
		END;
		$function$;
		CREATE TRIGGER test_receipt_delete_hook
		BEFORE DELETE ON sync.scope_write_receipts
		FOR EACH ROW EXECUTE FUNCTION sync.test_receipt_delete_hook();
	`, body))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = f.service.pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_receipt_delete_hook ON sync.scope_write_receipts`)
		_, _ = f.service.pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS sync.test_receipt_delete_hook()`)
	})
}

func TestScopeWriteReceipt_ReplaySurvivesBundlePruningAndBindsContent(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	opts := retryableOptionsForLogicalWork("replay-" + f.userID)
	rowID := uuid.New()
	callbackCalls := 0
	result, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: opts,
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		callbackCalls++
		return f.insertUser(callbackCtx, tx, rowID, "receipt-replay")
	})
	require.NoError(t, err)
	require.Equal(t, 1, callbackCalls)
	require.Equal(t, int64(1), result.Bundle.RowCount)

	_, err = f.pool.Exec(f.ctx, `DELETE FROM sync.bundle_log WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1) AND bundle_seq = $2`, f.userID, result.Bundle.BundleSeq)
	require.NoError(t, err)

	replayed, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: opts,
	}, func(context.Context, DatabaseWriteTx) error {
		callbackCalls++
		return errors.New("completed replay invoked callback")
	})
	require.NoError(t, err)
	require.Equal(t, 1, callbackCalls)
	require.Equal(t, result, replayed)

	changed := opts
	changed.OperationHash = sha256.Sum256([]byte("changed logical work"))
	_, err = f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: changed,
	}, func(context.Context, DatabaseWriteTx) error {
		callbackCalls++
		return nil
	})
	var replayChanged *OperationReplayChangedError
	require.ErrorAs(t, err, &replayChanged)
	require.Equal(t, 1, callbackCalls)
}

func TestScopeWriteReceipt_DeferredFinalizerRejectsInProgressCommit(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	_, err := f.pool.Exec(f.ctx, `INSERT INTO sync.user_state (user_id) VALUES ($1)`, f.userID)
	require.NoError(t, err)
	var userPK int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT user_pk FROM sync.user_state WHERE user_id = $1`, f.userID).Scan(&userPK))
	tx, err := f.service.pool.Begin(f.ctx)
	require.NoError(t, err)
	hash := sha256.Sum256([]byte("in-progress-finalizer"))
	deadline := time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour)
	_, err = tx.Exec(f.ctx, `
		INSERT INTO sync.scope_write_receipts (
			user_pk, operation_id, operation_hash, required_effect_tables_hash,
			forbidden_effect_tables_hash, writer_id, operation_valid_until,
			receipt_state, receipt_expires_at
		) VALUES ($1, $2, $3, $3, $3, 'server-writer', $4::timestamptz, 'in_progress', $4::timestamptz + interval '24 hours')
	`, userPK, uuid.New(), hash[:], deadline)
	require.NoError(t, err)
	err = tx.Commit(f.ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot commit before completion")
}

func TestBootstrap_RequiresExactServerSourceReservationSet(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	config := func(reserved []string, appName string) *ServiceConfig {
		return &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
			{Schema: f.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		}), AppName: appName,
			RegisteredTables: []RegisteredTable{
				{Schema: f.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			},
			ReservedServerSourceIDs:        reserved,
			RegisteredTableCatalogRevision: 1,
		}
	}

	mismatched, err := newTestRuntimeService(f.service.pool, config([]string{"server-writer"}, "reservation-mismatch"), integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = mismatched.Close(context.Background()) })
	err = mismatched.Bootstrap(f.ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)

	reordered, err := newTestRuntimeService(f.service.pool, config([]string{"server-unused", "server-writer"}, "reservation-reordered"), integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reordered.Close(context.Background()) })
	require.NoError(t, reordered.Bootstrap(f.ctx))
}

func TestScopeWriteReceipt_RollbackExpiredAndUnavailableHistoryOutcomes(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	opts := retryableOptionsForLogicalWork("rollback-" + f.userID)
	rowID := uuid.New()
	rollbackErr := errors.New("application rollback")
	_, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: opts,
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		if err := f.insertUser(callbackCtx, tx, rowID, "rolled-back"); err != nil {
			return err
		}
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)

	result, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: opts,
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		return f.insertUser(callbackCtx, tx, rowID, "committed-after-rollback")
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Bundle.RowCount)

	expired := retryableOptionsForLogicalWork("expired-" + f.userID)
	expired.OperationValidUntil = time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	callbackCalled := false
	_, err = f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: expired,
	}, func(context.Context, DatabaseWriteTx) error {
		callbackCalled = true
		return nil
	})
	var historyExpired *OperationHistoryExpiredError
	require.ErrorAs(t, err, &historyExpired)
	require.False(t, callbackCalled)

	historyUser := f.userID + "-explicit"
	mustInitializeEmptyScope(t, f.ctx, f.service, historyUser, "server-app")
	bundleOpts := RetryableBundleWriteOptions{
		OperationHash:       sha256.Sum256([]byte("explicit-history-" + historyUser)),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour),
	}
	source := BundleSource{SourceID: "server-app", SourceBundleID: 1}
	require.NoError(t, f.service.WithinSyncBundle(f.ctx, Actor{UserID: historyUser}, source, bundleOpts, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		return f.insertUser(callbackCtx, tx, uuid.New(), "history")
	}))
	changedSource := source
	changedSource.CanonicalRequestHash = strings.Repeat("a", 64)
	err = f.service.WithinSyncBundle(f.ctx, Actor{UserID: historyUser}, changedSource, bundleOpts, func(context.Context, DatabaseWriteTx) error {
		callbackCalled = true
		return nil
	})
	var replayChanged *OperationReplayChangedError
	require.ErrorAs(t, err, &replayChanged)
	require.False(t, callbackCalled)
	operationID := withinSyncBundleOperationID(historyUser, source.SourceID, source.SourceBundleID)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM sync.scope_write_receipts WHERE operation_id = $1`, operationID)
	require.NoError(t, err)
	callbackCalled = false
	err = f.service.WithinSyncBundle(f.ctx, Actor{UserID: historyUser}, source, bundleOpts, func(context.Context, DatabaseWriteTx) error {
		callbackCalled = true
		return nil
	})
	var unavailable *OperationHistoryUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.False(t, callbackCalled)
}

func TestScopeWriteReceipt_LostCommitResponseResolvesByExactReplay(t *testing.T) {
	for _, test := range []struct {
		name                  string
		commit                bool
		wantReplayCallbackRun bool
	}{
		{name: "committed", commit: true, wantReplayCallbackRun: false},
		{name: "rolled_back", commit: false, wantReplayCallbackRun: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRetryableReceiptFixture(t)
			scopeID := f.userID + "-lost-" + test.name
			source := BundleSource{SourceID: "server-app", SourceBundleID: 1}
			mustInitializeEmptyScope(t, f.ctx, f.service, scopeID, source.SourceID)
			opts := RetryableBundleWriteOptions{
				OperationHash:       sha256.Sum256([]byte("lost-response-" + scopeID)),
				OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour),
			}
			writeOpts := RetryableWriteOptions{
				OperationID:         withinSyncBundleOperationID(scopeID, source.SourceID, source.SourceBundleID),
				OperationHash:       opts.OperationHash,
				OperationValidUntil: opts.OperationValidUntil,
			}
			emptyEffects, err := f.service.canonicalizeEffectSet(nil)
			require.NoError(t, err)
			conn, release, err := f.service.acquireUserUploadConn(f.ctx, scopeID)
			require.NoError(t, err)
			lostErr := errors.New("synthetic lost COMMIT response")
			callbackCalls := 0
			_, err = runRetryableWriteTransactionWithDriver(f.ctx, retryableWriteDriver{
				begin: func(ctx context.Context) (pgx.Tx, error) {
					tx, beginErr := conn.BeginTx(ctx, syncMutationTxOptions())
					if beginErr != nil {
						return nil, beginErr
					}
					return &lostCommitResponseTx{Tx: tx, commit: test.commit, err: lostErr}, nil
				},
				commitRollbackProven: commitRollbackProven,
				wait: func(context.Context, time.Duration) error {
					t.Fatal("ambiguous commit must not be retried")
					return nil
				},
			}, func(tx pgx.Tx) (struct{}, error) {
				callbackCalls++
				return struct{}{}, f.service.withinSyncBundleAttempt(f.ctx, tx, Actor{UserID: scopeID}, source, writeOpts, emptyEffects, func(callbackCtx context.Context, capability DatabaseWriteTx) error {
					return f.insertUser(callbackCtx, capability, uuid.NewSHA1(uuid.NameSpaceOID, []byte(scopeID)), "lost-response")
				})
			})
			release()
			var unknown *CommitOutcomeUnknownError
			require.ErrorAs(t, err, &unknown)
			require.ErrorIs(t, err, lostErr)
			require.Equal(t, 1, callbackCalls)

			err = f.service.WithinSyncBundle(f.ctx, Actor{UserID: scopeID}, source, opts, func(callbackCtx context.Context, capability DatabaseWriteTx) error {
				callbackCalls++
				return f.insertUser(callbackCtx, capability, uuid.NewSHA1(uuid.NameSpaceOID, []byte(scopeID)), "lost-response")
			})
			require.NoError(t, err)
			if test.wantReplayCallbackRun {
				require.Equal(t, 2, callbackCalls)
			} else {
				require.Equal(t, 1, callbackCalls)
			}
		})
	}
}

func TestScopeWriteReceipt_RejectsDeadlineBeyondNinetyDaysBeforeCallback(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	opts := retryableOptionsForLogicalWork("too-far-" + f.userID)
	opts.OperationValidUntil = time.Now().UTC().Truncate(time.Microsecond).Add(91 * 24 * time.Hour)
	callbackCalled := false
	_, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: opts,
	}, func(context.Context, DatabaseWriteTx) error {
		callbackCalled = true
		return nil
	})
	var invalid *RetryableWriteInvalidError
	require.ErrorAs(t, err, &invalid)
	require.False(t, callbackCalled)
}

func TestScopeWriteReceipt_ConcurrentTransactionsUseReceiptUniquenessForOwnership(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	_, err := f.pool.Exec(f.ctx, `INSERT INTO sync.user_state (user_id) VALUES ($1)`, f.userID)
	require.NoError(t, err)
	var userPK int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT user_pk FROM sync.user_state WHERE user_id = $1`, f.userID).Scan(&userPK))

	opts := retryableOptionsForLogicalWork("concurrent-" + f.userID)
	spec := scopeWriteReceiptSpec{
		userPK:              userPK,
		operationID:         opts.OperationID,
		operationHash:       opts.OperationHash,
		writerID:            "server-writer",
		operationValidUntil: opts.OperationValidUntil,
	}

	ownerConn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer ownerConn.Release()
	waiterConn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer waiterConn.Release()
	ownerTx, err := ownerConn.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer ownerTx.Rollback(context.Background())
	waiterTx, err := waiterConn.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer waiterTx.Rollback(context.Background())

	ownerDecision, err := acquireScopeWriteReceipt(f.ctx, ownerTx, spec)
	require.NoError(t, err)
	require.True(t, ownerDecision.inserted)
	require.NoError(t, completeScopeWriteReceipt(f.ctx, ownerTx, userPK, opts.OperationID, &ScopeWriteResult{
		Bundle: CommittedBundleRef{
			BundleSeq:            1,
			SourceID:             "server-writer",
			SourceBundleID:       1,
			RowCount:             1,
			BundleHash:           strings.Repeat("0", 64),
			CanonicalRequestHash: "",
		},
	}))

	var ownerPID, waiterPID int32
	require.NoError(t, ownerTx.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&ownerPID))
	require.NoError(t, waiterTx.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&waiterPID))
	type receiptResult struct {
		decision scopeWriteReceiptDecision
		err      error
	}
	waiterResult := make(chan receiptResult, 1)
	go func() {
		decision, acquireErr := acquireScopeWriteReceipt(f.ctx, waiterTx, spec)
		waiterResult <- receiptResult{decision: decision, err: acquireErr}
	}()

	require.Eventually(t, func() bool {
		var blockedByOwner bool
		queryErr := f.pool.QueryRow(f.ctx, `SELECT $1::integer = ANY(pg_blocking_pids($2::integer))`, ownerPID, waiterPID).Scan(&blockedByOwner)
		return queryErr == nil && blockedByOwner
	}, 5*time.Second, 10*time.Millisecond, "second transaction must wait on the uncommitted receipt row")

	require.NoError(t, ownerTx.Commit(f.ctx))
	result := <-waiterResult
	require.NoError(t, result.err)
	require.False(t, result.decision.inserted)
	require.NotNil(t, result.decision.replay)
	require.Equal(t, int64(1), result.decision.replay.Bundle.BundleSeq)
	require.NoError(t, waiterTx.Commit(f.ctx))
}

func TestScopeReceiptCleanup_SkipsLockedRowsAndDrainsOnClose(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	f.service.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))

	operationIDs := f.insertExpiredCompletedReceipts(t, 2)
	var userPK int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT user_pk FROM sync.user_state WHERE user_id = $1`, f.userID).Scan(&userPK))

	lockConn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer lockConn.Release()
	lockTx, err := lockConn.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer lockTx.Rollback(context.Background())
	_, err = lockTx.Exec(f.ctx, `
		SELECT 1 FROM sync.scope_write_receipts
		WHERE user_pk = $1 AND operation_id = $2
		FOR UPDATE
	`, userPK, operationIDs[0])
	require.NoError(t, err)

	deleted, err := f.service.cleanupScopeReceiptBatch(f.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	var remaining []uuid.UUID
	rows, err := f.service.pool.Query(f.ctx, `SELECT operation_id FROM sync.scope_write_receipts WHERE user_pk = $1`, userPK)
	require.NoError(t, err)
	for rows.Next() {
		var operationID uuid.UUID
		require.NoError(t, rows.Scan(&operationID))
		remaining = append(remaining, operationID)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, []uuid.UUID{operationIDs[0]}, remaining)

	require.NoError(t, lockTx.Rollback(f.ctx))
	deleted, err = f.service.cleanupScopeReceiptBatch(f.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.NoError(t, f.service.Close(f.ctx))
	select {
	case <-f.service.scopeReceiptCleanupDone:
	default:
		t.Fatal("scope receipt cleanup worker did not drain before Close returned")
	}
}

func TestScopeReceiptCleanup_FixedBoundsAndFourBatchRun(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	f.service.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
	require.Equal(t, time.Minute, scopeReceiptCleanupInterval)
	require.Equal(t, 500, scopeReceiptCleanupBatchSize)
	require.Equal(t, 4, scopeReceiptCleanupMaxBatchesPerRun)
	require.Equal(t, 5*time.Second, scopeReceiptCleanupBatchTimeout)
	f.insertExpiredCompletedReceipts(t, scopeReceiptCleanupBatchSize*scopeReceiptCleanupMaxBatchesPerRun+1)

	f.service.runScopeReceiptCleanup(f.ctx, integrationTestLogger(slog.LevelWarn), "bounds_test")
	var remaining int
	require.NoError(t, f.service.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.scope_write_receipts`).Scan(&remaining))
	require.Equal(t, 1, remaining)
}

func TestScopeReceiptCleanup_FailedBatchRollsBack(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	f.service.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
	f.insertExpiredCompletedReceipts(t, 2)
	f.installReceiptDeleteTrigger(t, `RAISE EXCEPTION 'synthetic receipt cleanup failure';`)

	deleted, err := f.service.cleanupScopeReceiptBatch(f.ctx)
	require.ErrorContains(t, err, "synthetic receipt cleanup failure")
	require.Zero(t, deleted)
	var remaining int
	require.NoError(t, f.service.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.scope_write_receipts`).Scan(&remaining))
	require.Equal(t, 2, remaining)
}

func TestScopeReceiptCleanup_RunAppliesFiveSecondBatchTimeout(t *testing.T) {
	t.Parallel()
	f := newRetryableReceiptFixture(t)
	f.service.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
	f.insertExpiredCompletedReceipts(t, 1)
	f.installReceiptDeleteTrigger(t, `PERFORM pg_sleep(30);`)

	startedAt := time.Now()
	f.service.runScopeReceiptCleanup(f.ctx, integrationTestLogger(slog.LevelError), "timeout_test")
	elapsed := time.Since(startedAt)
	require.GreaterOrEqual(t, elapsed, scopeReceiptCleanupBatchTimeout-500*time.Millisecond)
	require.Less(t, elapsed, 10*time.Second)
	var remaining int
	require.NoError(t, f.service.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.scope_write_receipts`).Scan(&remaining))
	require.Equal(t, 1, remaining)
}

func TestScopeReceiptCleanup_CloseCancelsActiveRunAndDrainsWorker(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	f.service.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
	f.insertExpiredCompletedReceipts(t, 1)
	worker, err := newTestRuntimeService(f.service.pool, f.service.config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = worker.Close(context.Background()) })
	worker.mu.Lock()
	worker.lifecycle = serviceLifecycleRunning
	worker.bootstrapReadiness = bootstrapReadinessReady
	worker.mu.Unlock()
	f.installReceiptDeleteTrigger(t, `PERFORM pg_sleep(30);`)
	worker.startScopeReceiptCleanupWorker()
	worker.requestScopeReceiptCleanup("close_cancellation_test")
	require.Eventually(t, func() bool {
		var active bool
		err := worker.pool.QueryRow(f.ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND query LIKE '%scope_write_receipts AS receipt%'
			)
		`).Scan(&active)
		return err == nil && active
	}, 5*time.Second, 10*time.Millisecond)

	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, worker.Close(closeCtx))
	select {
	case <-worker.scopeReceiptCleanupDone:
	default:
		t.Fatal("active scope receipt cleanup worker did not drain before Close returned")
	}
}

func TestScopeReceiptCleanup_ConcurrentInstancesSplitSkipLockedBatch(t *testing.T) {
	t.Parallel()
	f := newRetryableReceiptFixture(t)
	second, err := newTestRuntimeService(f.service.pool, f.service.config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(f.ctx))
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	f.service.stopScopeReceiptCleanupWorker()
	second.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
	require.NoError(t, second.waitScopeReceiptCleanupWorker(f.ctx))
	f.insertExpiredCompletedReceipts(t, scopeReceiptCleanupBatchSize*2)
	f.installReceiptDeleteTrigger(t, `PERFORM pg_sleep(0.001);`)

	start := make(chan struct{})
	results := make(chan struct {
		deleted int64
		err     error
	}, 2)
	for _, service := range []*SyncService{f.service, second} {
		go func(service *SyncService) {
			<-start
			deleted, cleanupErr := service.cleanupScopeReceiptBatch(f.ctx)
			results <- struct {
				deleted int64
				err     error
			}{deleted: deleted, err: cleanupErr}
		}(service)
	}
	close(start)
	first, secondResult := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, secondResult.err)
	require.Equal(t, int64(scopeReceiptCleanupBatchSize), first.deleted)
	require.Equal(t, int64(scopeReceiptCleanupBatchSize), secondResult.deleted)
}

func TestServerSourceReservation_OfflineAdditionLocksAndRejectsClientHistory(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	mustInitializeEmptyScope(t, f.ctx, f.service, f.userID, "existing-client-source")
	_, err := f.pool.Exec(f.ctx, `INSERT INTO sync.server_source_reservations (source_id, ever_used) VALUES ('existing-client-source', FALSE)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client source history prevents server source reservation")
	_, err = f.pool.Exec(f.ctx, `
		INSERT INTO sync.source_state (
			user_pk, source_id, state, max_committed_source_bundle_id,
			replaced_by_source_id, retirement_reason
		) VALUES (
			(SELECT user_pk FROM sync.user_state WHERE user_id = $1),
			'retired-client-source', 'retired', 0, 'replacement-only-source', 'replaced'
		)
	`, f.userID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `INSERT INTO sync.server_source_reservations (source_id, ever_used) VALUES ('replacement-only-source', FALSE)`)
	require.ErrorContains(t, err, "client source history prevents server source reservation")
	_, err = f.pool.Exec(f.ctx, `INSERT INTO sync.server_source_reservations (source_id, ever_used) VALUES ('invalid-used-addition', TRUE)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must start unused")

	conn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer conn.Release()
	tx, err := conn.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(f.ctx, `INSERT INTO sync.server_source_reservations (source_id, ever_used) VALUES ('offline-added-source', FALSE)`)
	require.NoError(t, err)
	var backendPID int32
	require.NoError(t, tx.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&backendPID))
	var hasBootstrapAdvisoryLock, hasSourceStateLock bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `
		SELECT
			EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted),
			EXISTS (
				SELECT 1
				FROM pg_locks AS locks
				JOIN pg_class AS relation ON relation.oid = locks.relation
				JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE locks.pid = $1 AND locks.locktype = 'relation' AND locks.mode = 'ShareRowExclusiveLock'
				  AND namespace.nspname = 'sync' AND relation.relname = 'source_state' AND locks.granted
			)
	`, backendPID).Scan(&hasBootstrapAdvisoryLock, &hasSourceStateLock))
	require.True(t, hasBootstrapAdvisoryLock)
	require.True(t, hasSourceStateLock)
}

func TestServerSourceReservation_OfflineUnusedRemovalSucceeds(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	_, err := f.service.pool.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
	require.NoError(t, err)
	var exists bool
	require.NoError(t, f.service.pool.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM sync.server_source_reservations WHERE source_id = 'server-unused')`).Scan(&exists))
	require.False(t, exists)
}

func TestServerSourceReservation_RemovalRejectsSourceStateAndReceiptReferences(t *testing.T) {
	t.Parallel()
	t.Run("source state", func(t *testing.T) {
		f := newRetryableReceiptFixture(t)
		_, err := f.service.pool.Exec(f.ctx, `INSERT INTO sync.user_state (user_id) VALUES ($1)`, f.userID)
		require.NoError(t, err)
		_, err = f.service.pool.Exec(f.ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id,
				replaced_by_source_id, retirement_reason
			) VALUES (
				(SELECT user_pk FROM sync.user_state WHERE user_id = $1),
				'server-unused', 'reserved', 0, '', ''
			)
		`, f.userID)
		require.NoError(t, err)
		_, err = f.service.pool.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
		require.ErrorContains(t, err, "referenced server source reservation cannot be removed")
	})

	t.Run("replacement source state", func(t *testing.T) {
		f := newRetryableReceiptFixture(t)
		_, err := f.service.pool.Exec(f.ctx, `INSERT INTO sync.user_state (user_id) VALUES ($1)`, f.userID)
		require.NoError(t, err)
		_, err = f.service.pool.Exec(f.ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id,
				replaced_by_source_id, retirement_reason
			) VALUES (
				(SELECT user_pk FROM sync.user_state WHERE user_id = $1),
				'retired-client-source', 'retired', 0, 'server-unused', 'replaced'
			)
		`, f.userID)
		require.NoError(t, err)
		_, err = f.service.pool.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
		require.ErrorContains(t, err, "referenced server source reservation cannot be removed")
	})

	t.Run("receipt", func(t *testing.T) {
		f := newRetryableReceiptFixture(t)
		f.service.stopScopeReceiptCleanupWorker()
		require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
		f.insertExpiredCompletedReceipts(t, 1)
		_, err := f.service.pool.Exec(f.ctx, `UPDATE sync.scope_write_receipts SET writer_id = 'server-unused'`)
		require.NoError(t, err)
		_, err = f.service.pool.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
		require.ErrorContains(t, err, "referenced server source reservation cannot be removed")
	})
}

func TestServerSourceReservation_RemovalTakesRequiredRelationLocks(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	conn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer conn.Release()
	tx, err := conn.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
	require.NoError(t, err)
	var backendPID int32
	require.NoError(t, tx.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&backendPID))

	var advisory, sourceState, receipts bool
	require.NoError(t, f.service.pool.QueryRow(f.ctx, `
		SELECT
			EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND mode = 'ExclusiveLock' AND granted),
			EXISTS (
				SELECT 1 FROM pg_locks AS locks
				JOIN pg_class AS relation ON relation.oid = locks.relation
				JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE locks.pid = $1 AND locks.mode = 'ShareRowExclusiveLock' AND locks.granted
				  AND namespace.nspname = 'sync' AND relation.relname = 'source_state'
			),
			EXISTS (
				SELECT 1 FROM pg_locks AS locks
				JOIN pg_class AS relation ON relation.oid = locks.relation
				JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE locks.pid = $1 AND locks.mode = 'ShareRowExclusiveLock' AND locks.granted
				  AND namespace.nspname = 'sync' AND relation.relname = 'scope_write_receipts'
			)
	`, backendPID).Scan(&advisory, &sourceState, &receipts))
	require.True(t, advisory)
	require.True(t, sourceState)
	require.True(t, receipts)
}

func TestServerSourceReservation_RacingPublicationWinsAndRemovalFailsClosed(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	callbackReached := make(chan struct{})
	releaseCallback := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		_, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
			WriterID:              "server-unused",
			RetryableWriteOptions: retryableOptionsForLogicalWork("reservation-race-" + f.userID),
		}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
			close(callbackReached)
			<-releaseCallback
			return f.insertUser(callbackCtx, tx, uuid.New(), "reservation-race")
		})
		writeDone <- err
	}()
	<-callbackReached

	deleteConn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer deleteConn.Release()
	var deletePID int32
	require.NoError(t, deleteConn.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&deletePID))
	deleteDone := make(chan error, 1)
	go func() {
		_, deleteErr := deleteConn.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
		deleteDone <- deleteErr
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := f.service.pool.QueryRow(f.ctx, `SELECT cardinality(pg_blocking_pids($1)) > 0`, deletePID).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	close(releaseCallback)
	require.NoError(t, <-writeDone)
	require.ErrorContains(t, <-deleteDone, "used server source reservation cannot be removed")

	var everUsed bool
	require.NoError(t, f.service.pool.QueryRow(f.ctx, `SELECT ever_used FROM sync.server_source_reservations WHERE source_id = 'server-unused'`).Scan(&everUsed))
	require.True(t, everUsed)
}

func TestServerSourceReservation_RacingRemovalWinsAndWriteFailsClosed(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	deleteConn, err := f.service.pool.Acquire(f.ctx)
	require.NoError(t, err)
	defer deleteConn.Release()
	deleteTx, err := deleteConn.BeginTx(f.ctx, syncMutationTxOptions())
	require.NoError(t, err)
	defer deleteTx.Rollback(context.Background())

	_, err = deleteTx.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-unused'`)
	require.NoError(t, err)

	opts := retryableOptionsForLogicalWork("reservation-removal-race-" + f.userID)
	callbackReached := make(chan struct{}, 1)
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
			WriterID:              "server-unused",
			RetryableWriteOptions: opts,
		}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
			callbackReached <- struct{}{}
			return f.insertUser(callbackCtx, tx, uuid.New(), "reservation-removal-race")
		})
		writeDone <- writeErr
	}()

	select {
	case err := <-writeDone:
		t.Fatalf("server write completed before reservation removal committed: %v", err)
	case <-callbackReached:
		t.Fatal("server write callback ran before reservation removal committed")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, deleteTx.Commit(f.ctx))
	err = <-writeDone
	require.ErrorContains(t, err, `configured server source reservation "server-unused" is unavailable`)
	select {
	case <-callbackReached:
		t.Fatal("server write callback ran after its configured reservation was removed")
	default:
	}

	var receiptCount int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM sync.scope_write_receipts WHERE operation_id = $1`, opts.OperationID).Scan(&receiptCount))
	require.Zero(t, receiptCount)
}

func TestScopeWriteEffectsAndReservationTombstones(t *testing.T) {
	f := newRetryableReceiptFixture(t)
	required := RegisteredEffectTable{Schema: f.schemaName, Table: "users"}
	requiredOpts := retryableOptionsForLogicalWork("required-" + f.userID)
	result, err := f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: requiredOpts,
		RequiredEffectTables:  []RegisteredEffectTable{required},
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		return f.insertUser(callbackCtx, tx, uuid.New(), "required")
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Bundle.RowCount)
	expectedRequiredHash := sha256.Sum256([]byte(fmt.Sprintf(`["oversync.effect-table-set.v1",[[%q,"users"]]]`, f.schemaName)))
	var storedRequiredHash []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT required_effect_tables_hash FROM sync.scope_write_receipts WHERE operation_id = $1`, requiredOpts.OperationID).Scan(&storedRequiredHash))
	require.Equal(t, expectedRequiredHash[:], storedRequiredHash)

	normalizedAwayID := uuid.New()
	_, err = f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: retryableOptionsForLogicalWork("required-normalized-away-" + f.userID),
		RequiredEffectTables:  []RegisteredEffectTable{required},
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		if err := f.insertUser(callbackCtx, tx, normalizedAwayID, "required-normalized-away"); err != nil {
			return err
		}
		_, err := tx.Exec(callbackCtx, fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, f.tableIdent), normalizedAwayID)
		return err
	})
	var violation *RetryableCallbackViolationError
	require.ErrorAs(t, err, &violation)

	callbackCalled := false
	_, err = f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: retryableOptionsForLogicalWork("overlapping-effects-" + f.userID),
		RequiredEffectTables:  []RegisteredEffectTable{required},
		ForbiddenEffectTables: []RegisteredEffectTable{required},
	}, func(context.Context, DatabaseWriteTx) error {
		callbackCalled = true
		return nil
	})
	var invalid *ScopeWriteInvalidError
	require.ErrorAs(t, err, &invalid)
	require.False(t, callbackCalled)

	rowID := uuid.New()
	_, err = f.manager.ExecWrite(f.ctx, f.userID, ScopeWriteOptions{
		WriterID:              "server-writer",
		RetryableWriteOptions: retryableOptionsForLogicalWork("forbidden-" + f.userID),
		ForbiddenEffectTables: []RegisteredEffectTable{required},
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		if err := f.insertUser(callbackCtx, tx, rowID, "forbidden"); err != nil {
			return err
		}
		_, err := tx.Exec(callbackCtx, fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, f.tableIdent), rowID)
		return err
	})
	require.ErrorAs(t, err, &violation)

	rollbackErr := errors.New("reservation rollback")
	_, err = f.manager.ExecWrite(f.ctx, f.userID+"-unused", ScopeWriteOptions{
		WriterID:              "server-unused",
		RetryableWriteOptions: retryableOptionsForLogicalWork("unused-" + f.userID),
	}, func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		if err := f.insertUser(callbackCtx, tx, uuid.New(), "unused"); err != nil {
			return err
		}
		return rollbackErr
	})
	require.ErrorIs(t, err, rollbackErr)

	var used, unused bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT ever_used FROM sync.server_source_reservations WHERE source_id = 'server-writer'`).Scan(&used))
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT ever_used FROM sync.server_source_reservations WHERE source_id = 'server-unused'`).Scan(&unused))
	require.True(t, used)
	require.False(t, unused)
	_, err = f.pool.Exec(f.ctx, `UPDATE sync.server_source_reservations SET ever_used = FALSE WHERE source_id = 'server-unused'`)
	require.Error(t, err)

	f.service.stopScopeReceiptCleanupWorker()
	require.NoError(t, f.service.waitScopeReceiptCleanupWorker(f.ctx))
	_, err = f.pool.Exec(f.ctx, `
		WITH receipt_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS current_time
		)
		UPDATE sync.scope_write_receipts
		SET operation_valid_until = receipt_clock.current_time - interval '72 hours',
		    completed_at = receipt_clock.current_time - interval '48 hours',
		    receipt_expires_at = receipt_clock.current_time - interval '24 hours'
		FROM receipt_clock
		WHERE operation_id = $1
	`, requiredOpts.OperationID)
	require.NoError(t, err)
	_, err = f.service.cleanupScopeReceiptBatch(f.ctx)
	require.NoError(t, err)
	var receiptExists bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT EXISTS (SELECT 1 FROM sync.scope_write_receipts WHERE operation_id = $1)`, requiredOpts.OperationID).Scan(&receiptExists))
	require.False(t, receiptExists)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT ever_used FROM sync.server_source_reservations WHERE source_id = 'server-writer'`).Scan(&used))
	require.True(t, used)

	_, err = f.pool.Exec(f.ctx, `DELETE FROM sync.user_state WHERE user_id = $1`, f.userID)
	require.NoError(t, err)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT ever_used FROM sync.server_source_reservations WHERE source_id = 'server-writer'`).Scan(&used))
	require.True(t, used)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM sync.server_source_reservations WHERE source_id = 'server-writer'`)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE sync.server_source_reservations SET ever_used = FALSE WHERE source_id = 'server-writer'`)
	require.Error(t, err)
}
