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

type scriptedRetryableTx struct {
	pgx.Tx
	commitErr   error
	rollbackErr error
	commits     *int
	rollbacks   *int
}

func (tx *scriptedRetryableTx) Commit(context.Context) error {
	if tx.commits != nil {
		*tx.commits++
	}
	return tx.commitErr
}

func (tx *scriptedRetryableTx) Rollback(context.Context) error {
	if tx.rollbacks != nil {
		*tx.rollbacks++
	}
	return tx.rollbackErr
}

func TestRetryableWriteTransaction_RetriesOnlyPermittedSQLStatesWithFixedPolicy(t *testing.T) {
	for _, sqlState := range []string{"40001", "40P01", "55P03"} {
		t.Run(sqlState, func(t *testing.T) {
			attempts := 0
			rollbacks := 0
			var waits []time.Duration
			lastErr := &pgconn.PgError{Code: sqlState, Message: "synthetic retry"}
			driver := retryableWriteDriver{
				begin: func(context.Context) (pgx.Tx, error) {
					return &scriptedRetryableTx{rollbacks: &rollbacks}, nil
				},
				commitRollbackProven: func(error) bool { return false },
				wait: func(_ context.Context, delay time.Duration) error {
					waits = append(waits, delay)
					return nil
				},
			}

			_, err := runRetryableWriteTransactionWithDriver(context.Background(), driver, func(pgx.Tx) (struct{}, error) {
				attempts++
				return struct{}{}, lastErr
			})

			require.ErrorIs(t, err, lastErr)
			require.Equal(t, 3, attempts)
			require.Equal(t, 3, rollbacks)
			require.Equal(t, []time.Duration{25 * time.Millisecond, 50 * time.Millisecond}, waits)
		})
	}
}

func TestRetryableWriteTransaction_CancellationInterruptsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	driver := retryableWriteDriver{
		begin:                func(context.Context) (pgx.Tx, error) { return &scriptedRetryableTx{}, nil },
		commitRollbackProven: func(error) bool { return false },
		wait: func(waitCtx context.Context, delay time.Duration) error {
			cancel()
			return sleepWithContext(waitCtx, delay)
		},
	}

	_, err := runRetryableWriteTransactionWithDriver(ctx, driver, func(pgx.Tx) (struct{}, error) {
		attempts++
		return struct{}{}, &pgconn.PgError{Code: "40001", Message: "retry then cancel"}
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

func TestRetryableWriteTransaction_AmbiguousCommitIsNeverRetried(t *testing.T) {
	attempts := 0
	commits := 0
	commitErr := errors.New("connection lost while committing")
	driver := retryableWriteDriver{
		begin: func(context.Context) (pgx.Tx, error) {
			return &scriptedRetryableTx{commitErr: commitErr, commits: &commits}, nil
		},
		commitRollbackProven: func(error) bool { return false },
		wait: func(context.Context, time.Duration) error {
			t.Fatal("ambiguous commit must not wait for a retry")
			return nil
		},
	}

	_, err := runRetryableWriteTransactionWithDriver(context.Background(), driver, func(pgx.Tx) (struct{}, error) {
		attempts++
		return struct{}{}, nil
	})

	var unknown *CommitOutcomeUnknownError
	require.ErrorAs(t, err, &unknown)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 1, attempts)
	require.Equal(t, 1, commits)
}

func TestRetryableWriteTransaction_DefiniteRollbackCommitErrorUsesBoundedRetry(t *testing.T) {
	attempts := 0
	commits := 0
	var waits []time.Duration
	commitErr := &pgconn.PgError{Code: "40P01", Message: "commit rolled back"}
	driver := retryableWriteDriver{
		begin: func(context.Context) (pgx.Tx, error) {
			return &scriptedRetryableTx{commitErr: commitErr, commits: &commits}, nil
		},
		commitRollbackProven: func(error) bool { return true },
		wait: func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	}

	_, err := runRetryableWriteTransactionWithDriver(context.Background(), driver, func(pgx.Tx) (struct{}, error) {
		attempts++
		return struct{}{}, nil
	})

	require.ErrorIs(t, err, commitErr)
	require.Equal(t, 3, attempts)
	require.Equal(t, 3, commits)
	require.Equal(t, []time.Duration{25 * time.Millisecond, 50 * time.Millisecond}, waits)
}

func TestRetryableWriteTransaction_UnprovenRollbackStopsImmediately(t *testing.T) {
	attempts := 0
	retryErr := &pgconn.PgError{Code: "55P03", Message: "lock timeout"}
	driver := retryableWriteDriver{
		begin: func(context.Context) (pgx.Tx, error) {
			return &scriptedRetryableTx{rollbackErr: errors.New("rollback transport failure")}, nil
		},
		commitRollbackProven: func(error) bool { return false },
		wait: func(context.Context, time.Duration) error {
			t.Fatal("unproven rollback must not wait for a retry")
			return nil
		},
	}

	_, err := runRetryableWriteTransactionWithDriver(context.Background(), driver, func(pgx.Tx) (struct{}, error) {
		attempts++
		return struct{}{}, retryErr
	})

	require.ErrorIs(t, err, retryErr)
	require.Equal(t, 1, attempts)
}

func TestCommitRollbackProof_RequiresExplicitDriverSentinel(t *testing.T) {
	require.True(t, commitRollbackProven(pgx.ErrTxCommitRollback))
	require.True(t, commitRollbackProven(fmt.Errorf("commit: %w", pgx.ErrTxCommitRollback)))
	require.True(t, commitRollbackProven(&pgconn.PgError{Code: "40001", Message: "commit rolled back"}))
	require.False(t, commitRollbackProven(errors.New("commit response was lost on an idle connection")))
}

func TestRetryableWriteTransaction_OverridesSessionTransactionDefaults(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	defer func() {
		_, resetErr := conn.Exec(context.Background(), `RESET ALL`)
		require.NoError(t, resetErr)
	}()

	_, err = conn.Exec(ctx, `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE`)
	require.NoError(t, err)

	_, err = runRetryableWriteTransaction(ctx, conn, func(tx pgx.Tx) (struct{}, error) {
		var isolation, readOnly, deferrable string
		if err := tx.QueryRow(ctx, `SHOW transaction_isolation`).Scan(&isolation); err != nil {
			return struct{}{}, err
		}
		if err := tx.QueryRow(ctx, `SHOW transaction_read_only`).Scan(&readOnly); err != nil {
			return struct{}{}, err
		}
		if err := tx.QueryRow(ctx, `SHOW transaction_deferrable`).Scan(&deferrable); err != nil {
			return struct{}{}, err
		}
		if isolation != "read committed" || readOnly != "off" || deferrable != "off" {
			return struct{}{}, fmt.Errorf("unexpected retryable transaction modes: isolation=%q read_only=%q deferrable=%q", isolation, readOnly, deferrable)
		}
		return struct{}{}, nil
	})
	require.NoError(t, err)
}

func TestSyncMutationTxOptions_AreFixed(t *testing.T) {
	opts := syncMutationTxOptions()
	require.Equal(t, pgx.ReadCommitted, opts.IsoLevel)
	require.Equal(t, pgx.ReadWrite, opts.AccessMode)
	require.Equal(t, pgx.NotDeferrable, opts.DeferrableMode)
}

func TestWithinSyncBundleOperationID_UsesFrozenFrameV1AndNamespace(t *testing.T) {
	require.Equal(
		t,
		uuid.MustParse("8d725cb8-a072-57cb-b973-29b13391644e"),
		withinSyncBundleOperationID("scope-A", "source-A", 42),
	)
}

func TestRetryableWriteOptions_RejectMissingAndSubMicrosecondIdentity(t *testing.T) {
	valid := RetryableWriteOptions{
		OperationID:         uuid.New(),
		OperationHash:       sha256.Sum256([]byte("valid logical request")),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour),
	}
	for _, mutate := range []func(*RetryableWriteOptions){
		func(opts *RetryableWriteOptions) { opts.OperationID = uuid.Nil },
		func(opts *RetryableWriteOptions) { opts.OperationHash = [32]byte{} },
		func(opts *RetryableWriteOptions) {
			opts.OperationValidUntil = opts.OperationValidUntil.Add(time.Nanosecond)
		},
	} {
		opts := valid
		mutate(&opts)
		var invalid *RetryableWriteInvalidError
		require.ErrorAs(t, validateRetryableWriteOptions(opts), &invalid)
	}
}

func TestRetryableCallbackSQL_RejectsTransactionControlAndDisguisedSetConfig(t *testing.T) {
	for _, query := range []string{
		`SET LOCAL search_path = public`,
		`RESET ALL`,
		`BEGIN`,
		`COMMIT`,
		`CALL business.apply_change()`,
		`DO $$ BEGIN PERFORM set_config('search_path', 'public', true); END $$`,
		`SELECT pg_catalog.set_config('search_path', 'public', true)`,
		`SELECT pg_catalog."set_config"('search_path', 'public', true)`,
		`SELECT "pg_catalog"."set_config"('search_path', 'public', true)`,
		`SELECT PG_CATALOG.SET_CONFIG('search_path', 'public', true)`,
		`WITH changed AS (SELECT pg_catalog./* disguise */ "set_config"('search_path', 'public', true)) SELECT 1`,
		`SELECT E'it\'s parsed', pg_catalog."set_config"('search_path', 'public', true)`,
		`SELECT U&"set\005fconfig"('search_path', 'public', true)`,
		`SELECT 1; SELECT pg_catalog.set_config('search_path', 'public', true)`,
	} {
		t.Run(query, func(t *testing.T) {
			var violation *RetryableCallbackViolationError
			require.ErrorAs(t, validateRetryableCallbackSQL(query), &violation)
		})
	}
}

func TestRetryableCallbackSQL_AllowsDMLAndIgnoresLiteralsAndComments(t *testing.T) {
	for _, query := range []string{
		`SELECT 'set_config', $$set_config$$, 1 /* set_config */`,
		`SELECT E'it\'s safe'`,
		`WITH current_row AS (SELECT id FROM business.users WHERE id = $1) UPDATE business.users SET name = $2 FROM current_row WHERE business.users.id = current_row.id`,
		`INSERT INTO business.users(id, name) VALUES($1, $2)`,
		`DELETE FROM business.users WHERE id = $1`,
		`SHOW transaction_isolation`,
	} {
		t.Run(query, func(t *testing.T) {
			require.NoError(t, validateRetryableCallbackSQL(query))
		})
	}
}

func TestRetryableDatabaseWriteCapability_IsRestrictedAndRequiresCallbackContext(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "retryable_capability_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "retryable-capability-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	actor := Actor{UserID: "retryable-capability-user-" + suffix}
	source := BundleSource{SourceID: "server-app", SourceBundleID: 1}
	mustInitializeEmptyScope(t, ctx, service, actor.UserID, "bootstrap-client")

	var exposesCommit, exposesRawConn, rowsConnWasNil bool
	err := service.WithinSyncBundle(ctx, actor, source, retryableBundleWriteOptionsForTest(), func(callbackCtx context.Context, tx DatabaseWriteTx) error {
		for operation, call := range map[string]func() error{
			"GetStatus": func() error {
				_, err := service.GetStatus(callbackCtx)
				return err
			},
			"RunBundleChangeListener": func() error {
				return service.RunBundleChangeListener(callbackCtx)
			},
		} {
			var callbackViolation *RetryableCallbackViolationError
			if err := call(); !errors.As(err, &callbackViolation) {
				return fmt.Errorf("%s did not reject retryable callback context: %w", operation, err)
			}
		}
		_, exposesCommit = tx.(interface{ Commit(context.Context) error })
		_, exposesRawConn = tx.(interface{ Conn() *pgx.Conn })
		rows, err := tx.Query(callbackCtx, `SELECT 1`)
		if err != nil {
			return err
		}
		rowsConnWasNil = rows.Conn() == nil
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if _, err := tx.Exec(callbackCtx, `SELECT pg_catalog."set_config"('search_path', 'public', true)`); err == nil {
			return fmt.Errorf("quoted set_config call was not rejected")
		} else {
			var callbackViolation *RetryableCallbackViolationError
			if !errors.As(err, &callbackViolation) {
				return fmt.Errorf("quoted set_config returned an unexpected error: %w", err)
			}
		}
		_, err = tx.Exec(callbackCtx, fmt.Sprintf(`INSERT INTO %s.users (id, name, email) VALUES ($1, 'capability', 'capability@example.com')`, pgx.Identifier{schemaName}.Sanitize()), uuid.New())
		return err
	})
	require.NoError(t, err)
	require.False(t, exposesCommit)
	require.False(t, exposesRawConn)
	require.True(t, rowsConnWasNil)

	err = service.WithinSyncBundle(ctx, actor, BundleSource{SourceID: source.SourceID, SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(_ context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `SELECT 1`)
		return err
	})
	var violation *RetryableCallbackViolationError
	require.ErrorAs(t, err, &violation)

	err = service.WithinSyncBundle(ctx, actor, BundleSource{SourceID: source.SourceID, SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(callbackCtx context.Context, _ DatabaseWriteTx) error {
		return service.WithinSyncBundle(callbackCtx, actor, BundleSource{SourceID: source.SourceID, SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(context.Context, DatabaseWriteTx) error { return nil })
	})
	require.ErrorAs(t, err, &violation)

	manager := NewScopeManager(service, ScopeManagerConfig{})
	err = service.WithinSyncBundle(ctx, actor, BundleSource{SourceID: source.SourceID, SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(callbackCtx context.Context, _ DatabaseWriteTx) error {
		_, err := manager.ExecWrite(callbackCtx, actor.UserID, ScopeWriteOptions{WriterID: "server-app", RetryableWriteOptions: retryableWriteOptionsForTest()}, func(context.Context, DatabaseWriteTx) error { return nil })
		return err
	})
	require.ErrorAs(t, err, &violation)

}
