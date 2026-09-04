package oversqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// This observer deliberately records the current counterexamples. Passing it
// establishes the prerequisite seam, not the proposed cleanup contract.
type statementLifecycleObservation struct {
	events                        []string
	prepareErr, rowsErr, closeErr error
	commitErr, rollbackErr        error
	execErr                       error
	afterPrepare                  func()
	afterExec                     func()
}

type statementObservationDriver struct {
	observation *statementLifecycleObservation
}
type statementObservationConn struct {
	observation *statementLifecycleObservation
}
type statementObservationStmt struct {
	observation *statementLifecycleObservation
	query       string
}
type statementObservationRows struct {
	observation *statementLifecycleObservation
	value       driver.Value
	done        bool
}

var statementObservationSequence atomic.Uint64

func (d statementObservationDriver) Open(string) (driver.Conn, error) {
	d.observation.events = append(d.observation.events, "connection-open")
	return &statementObservationConn{d.observation}, nil
}
func (c *statementObservationConn) Prepare(q string) (driver.Stmt, error) {
	c.observation.events = append(c.observation.events, "prepare:"+q)
	if c.observation.prepareErr != nil {
		return nil, c.observation.prepareErr
	}
	return &statementObservationStmt{c.observation, q}, nil
}
func (c *statementObservationConn) PrepareContext(_ context.Context, q string) (driver.Stmt, error) {
	stmt, err := c.Prepare(q)
	if err == nil && c.observation.afterPrepare != nil {
		c.observation.afterPrepare()
	}
	return stmt, err
}
func (c *statementObservationConn) Close() error {
	c.observation.events = append(c.observation.events, "connection-close")
	return nil
}
func (c *statementObservationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("observer requires BEGIN IMMEDIATE")
}
func (c *statementObservationConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.observation.events = append(c.observation.events, "exec:"+q)
	switch q {
	case "COMMIT":
		return driver.RowsAffected(1), c.observation.commitErr
	case "ROLLBACK":
		return driver.RowsAffected(1), c.observation.rollbackErr
	}
	return driver.RowsAffected(1), nil
}
func (c *statementObservationConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.observation.events = append(c.observation.events, "query:"+q)
	return &statementObservationRows{observation: c.observation, value: int64(1)}, nil
}
func (s *statementObservationStmt) Close() error {
	s.observation.events = append(s.observation.events, "statement-close:"+s.query)
	return s.observation.closeErr
}
func (s *statementObservationStmt) NumInput() int { return -1 }
func (s *statementObservationStmt) Exec([]driver.Value) (driver.Result, error) {
	s.observation.events = append(s.observation.events, "statement-exec:"+s.query)
	err := s.observation.execErr
	if s.observation.afterExec != nil {
		s.observation.afterExec()
	}
	return driver.RowsAffected(1), err
}

// Lifetime prototype only. The existing cache owns the sole temporary handle;
// removal happens after Close. It does not establish preparation admission.
func observedOneShotExec(ctx context.Context, cache *txStmtCache, query string, observe func(), args ...any) (result sql.Result, resultErr error) {
	if cache == nil || cache.stmts == nil {
		return nil, errors.New("one-shot statement owner is closed")
	}
	stmt, err := cache.stmt(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, stmt.Close())
		delete(cache.stmts, query)
	}()
	result, resultErr = stmt.ExecContext(ctx, args...)
	if observe != nil {
		observe()
	}
	return result, resultErr
}

func TestReplayStatementCleanup_OneShotPrerequisite(t *testing.T) {
	ctx := context.Background()
	executeFailure := errors.New("injected execute failure")
	closeFailure := errors.New("injected one-shot close failure")
	cases := []struct {
		name       string
		prepareErr error
		execErr    error
		closeErr   error
		cancel     bool
		commit     bool
	}{
		{name: "Success"},
		{name: "PrepareFailure", prepareErr: executeFailure},
		{name: "ExecuteAndCloseFailure", execErr: executeFailure, closeErr: closeFailure},
		{name: "CloseFailure", closeErr: closeFailure},
		{name: "Cancellation", cancel: true, closeErr: closeFailure},
		{name: "CloseBeforeCommit", commit: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			o := &statementLifecycleObservation{prepareErr: test.prepareErr, execErr: test.execErr, closeErr: test.closeErr}
			db := statementObservationDB(t, o)
			tx, err := beginImmediateTx(ctx, db)
			require.NoError(t, err)
			defer tx.Rollback()
			cache := newTxStmtCache(tx)
			defer cache.Close()
			executionCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if test.cancel {
				o.afterPrepare = cancel
			}
			_, err = observedOneShotExec(executionCtx, cache, "one-shot", nil)
			require.Empty(t, cache.stmts)
			if test.prepareErr != nil {
				require.ErrorIs(t, err, test.prepareErr)
				require.NotContains(t, o.events, "statement-close:one-shot")
			} else {
				require.Contains(t, o.events, "statement-close:one-shot")
			}
			if test.execErr != nil {
				require.ErrorIs(t, err, test.execErr)
			}
			if test.closeErr != nil {
				require.ErrorIs(t, err, test.closeErr)
			}
			if test.cancel {
				require.ErrorIs(t, err, context.Canceled)
				require.NotContains(t, o.events, "statement-exec:one-shot")
			}
			if test.prepareErr == nil && test.execErr == nil && test.closeErr == nil && !test.cancel {
				require.NoError(t, err)
				if test.commit {
					require.NoError(t, tx.Commit(ctx))
					require.Equal(t, "exec:COMMIT", o.events[len(o.events)-1])
				} else {
					_, err = observedOneShotExec(ctx, cache, "one-shot", nil)
					require.NoError(t, err)
					require.Empty(t, cache.stmts)
				}
			} else {
				require.Error(t, err)
				require.NotContains(t, o.events, "exec:COMMIT")
			}
			t.Logf("one-shot lifetime and error observation: %v", o.events)
		})
	}
}
func (s *statementObservationStmt) Query([]driver.Value) (driver.Rows, error) {
	s.observation.events = append(s.observation.events, "statement-query:"+s.query)
	return &statementObservationRows{observation: s.observation, value: "invalid integer"}, nil
}
func (r *statementObservationRows) Columns() []string { return []string{"value"} }
func (r *statementObservationRows) Close() error {
	r.observation.events = append(r.observation.events, "rows-close")
	return r.observation.rowsErr
}
func (r *statementObservationRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	values[0] = r.value
	return nil
}

func statementObservationDB(t *testing.T, o *statementLifecycleObservation) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("statement-observation-%d", statementObservationSequence.Add(1))
	sql.Register(name, statementObservationDriver{o})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// API prototype only: accepts an already prepared statement, not a second
// cache. A production helper would prepare through the existing owner and
// explicitly select raw fallback. It must join scan and rows-close causes.
func observedStatementScan(ctx context.Context, stmt *sql.Stmt, args []any, targets ...any) (resultErr error) {
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for _, target := range targets {
		if _, raw := target.(*sql.RawBytes); raw {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return rows.Scan(targets...)
}

func TestReplayStatementReuse_ReadAPIPrerequisite(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, `SELECT ? WHERE ?`)
	require.NoError(t, err)
	defer stmt.Close()
	for _, value := range []any{nil, "", []byte{}, []byte{0, 255}, "text", int64(42), 1.25} {
		var actual any
		require.NoError(t, observedStatementScan(ctx, stmt, []any{value, true}, &actual))
		require.Equal(t, value, actual)
	}
	var value int
	require.ErrorIs(t, observedStatementScan(ctx, stmt, []any{1, false}, &value), sql.ErrNoRows)
	require.ErrorContains(t, observedStatementScan(ctx, stmt, []any{"invalid integer", true}, &value), "converting driver.Value")
	var raw sql.RawBytes
	require.ErrorContains(t, observedStatementScan(ctx, stmt, []any{[]byte{1}, true}, &raw), "RawBytes isn't allowed")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, observedStatementScan(canceled, stmt, []any{1, true}, &value), context.Canceled)
	_, err = conn.PrepareContext(ctx, `SELECT FROM`)
	require.Error(t, err)
	t.Log("scan prototype preserves successive typed/empty values, no-row, conversion/RawBytes errors and context cause; preparation returns its error directly")
}

func TestReplayStatementCleanup_ObservationPrerequisite(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("injected statement cleanup failure")
	t.Run("PrepareFailure", func(t *testing.T) {
		o := &statementLifecycleObservation{prepareErr: failure}
		db := statementObservationDB(t, o)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		cache := newTxStmtCache(&immediateTx{conn: conn, ctx: ctx})
		_, err = cache.ExecContext(ctx, "prepare-failure")
		require.ErrorIs(t, err, failure)
		require.Empty(t, cache.stmts)
		require.NoError(t, cache.Close())
		t.Logf("prepare failure preserves cause and inserts no entry: %v", o.events)
	})
	t.Run("ScanAndRowsClose", func(t *testing.T) {
		o := &statementLifecycleObservation{rowsErr: failure}
		db := statementObservationDB(t, o)
		stmt, err := db.PrepareContext(ctx, "scan-and-close")
		require.NoError(t, err)
		defer stmt.Close()
		var value int
		err = stmt.QueryRowContext(ctx).Scan(&value)
		require.ErrorContains(t, err, "converting driver.Value")
		require.NotErrorIs(t, err, failure, "current Row.Scan drops cleanup cause after scan failure")
		err = observedStatementScan(ctx, stmt, nil, &value)
		require.ErrorContains(t, err, "converting driver.Value")
		require.ErrorIs(t, err, failure)
		t.Logf("COUNTEREXAMPLE: Row.Scan loses rows-close cause; prototype retains both: %v", o.events)
	})
	t.Run("CloseAllAndUseAfterClose", func(t *testing.T) {
		o := &statementLifecycleObservation{closeErr: failure}
		db := statementObservationDB(t, o)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		cache := newTxStmtCache(&immediateTx{conn: conn, ctx: ctx})
		for _, q := range []string{"first", "second"} {
			_, err = cache.ExecContext(ctx, q)
			require.NoError(t, err)
		}
		require.ErrorIs(t, cache.Close(), failure)
		require.Contains(t, o.events, "statement-close:first")
		require.Contains(t, o.events, "statement-close:second")
		require.NoError(t, cache.Close())
		_, err = cache.ExecContext(ctx, "after-close")
		require.ErrorContains(t, err, "is closed")
		require.NotContains(t, o.events, "prepare:after-close")
		t.Logf("closed cache rejects use without preparation: %v", o.events)
	})
	t.Run("CommitBeforeClose", func(t *testing.T) {
		o := &statementLifecycleObservation{closeErr: failure}
		db := statementObservationDB(t, o)
		tx, err := beginImmediateTx(ctx, db)
		require.NoError(t, err)
		defer tx.Rollback()
		cache := newTxStmtCache(tx)
		_, err = cache.ExecContext(ctx, "settlement-write")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		require.Zero(t, db.Stats().InUse, "reserved connection already released")
		require.NotContains(t, o.events, "statement-close:settlement-write")
		require.ErrorIs(t, cache.Close(), failure)
		t.Logf("COUNTEREXAMPLE: current deferred settlement order commits/releases before cleanup failure: %v", o.events)
	})
	t.Run("ProgressWithCleanupCause", func(t *testing.T) {
		o := &statementLifecycleObservation{}
		db := statementObservationDB(t, o)
		tx, err := beginImmediateTx(ctx, db)
		require.NoError(t, err)
		defer tx.Rollback()
		work := &SyncWorkLimitError{}
		err = commitApplyProgress(ctx, tx, errors.Join(work, failure))
		require.ErrorIs(t, err, work)
		require.ErrorIs(t, err, failure)
		require.NotContains(t, o.events, "exec:COMMIT")
		t.Logf("joined cleanup cause correctly refuses progress commit: %v", o.events)
	})
	t.Run("RollbackAndRelease", func(t *testing.T) {
		o := &statementLifecycleObservation{commitErr: failure, rollbackErr: failure}
		db := statementObservationDB(t, o)
		tx, err := beginImmediateTx(ctx, db)
		require.NoError(t, err)
		require.ErrorIs(t, tx.Commit(ctx), failure)
		result := error(context.Canceled)
		tx.rollbackOnReturn(&result, "observed transaction")
		require.ErrorIs(t, result, context.Canceled)
		require.ErrorIs(t, result, failure)
		require.Contains(t, o.events, "exec:ROLLBACK")
		require.Contains(t, o.events, "connection-close")
		require.Zero(t, db.Stats().InUse)
		t.Logf("current rollback/discard retains primary and rollback causes: %v", o.events)
	})
}

func TestReplayStatementCleanup_ErrorPrecedence(t *testing.T) {
	ctx := context.Background()
	body := errors.New("body")
	closeCause := errors.New("close")
	rowsCause := errors.New("rows")
	o := &statementLifecycleObservation{execErr: body, closeErr: closeCause}
	db := statementObservationDB(t, o)
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	defer tx.Rollback()
	o.rowsErr = rowsCause
	cache := newTxStmtCache(tx)
	cache.active = true
	cache.pool = &statementReservation{atomic: true, credits: 10000}
	_, err = cache.ExecContext(ctx, "temporary-write")
	require.ErrorIs(t, err, body)
	require.ErrorIs(t, err, closeCause)
	require.Empty(t, cache.stmts)
	var n int
	err = cache.ScanContext(ctx, "temporary-read", nil, &n)
	require.ErrorContains(t, err, "converting driver.Value")
	require.ErrorIs(t, err, rowsCause)
	require.ErrorIs(t, err, closeCause)
	require.NoError(t, cache.Close())
	_, err = cache.ExecContext(ctx, "after-close")
	require.ErrorContains(t, err, "is closed")
	require.NotContains(t, o.events, "prepare:after-close")
}

func TestReplayStatementCleanup_PlanWriteFailure(t *testing.T) {
	ctx := context.Background()
	body := errors.New("plan execute")
	cleanup := errors.New("plan close")
	for _, mode := range []string{"prepare", "execute", "close", "execute and close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			o := &statementLifecycleObservation{}
			db := statementObservationDB(t, o)
			tx, err := beginImmediateTx(ctx, db)
			require.NoError(t, err)
			defer tx.Rollback()
			cache := newTxStmtCache(tx)
			cache.active = true
			cache.pool = &statementReservation{atomic: true, credits: 10000, reuse: true, handles: 4, sqlBytes: 8192}
			executionCtx := context.WithValue(ctx, statementCacheKey{}, cache)
			q := &statementTransaction{sqliteTransaction: tx, cache: cache}
			if mode == "prepare" {
				o.prepareErr = body
			}
			if mode == "execute" || mode == "execute and close" {
				o.execErr = body
			}
			if mode == "close" || mode == "execute and close" {
				o.closeErr = cleanup
			}
			if mode == "cancel" {
				var cancel context.CancelFunc
				executionCtx, cancel = context.WithCancel(executionCtx)
				cancel()
			}
			_, err = executeApplyPlanStatement(executionCtx, q, 7, "plan write", sql.NullString{String: "value", Valid: true})
			switch mode {
			case "prepare":
				require.ErrorIs(t, err, body)
				require.Empty(t, cache.stmts)
				require.Zero(t, cache.queryBytes)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
				require.NotContains(t, o.events, "prepare:plan write")
			default:
				require.Len(t, cache.stmts, 1)
				require.NotContains(t, o.events, "statement-close:plan write")
				if o.execErr != nil {
					require.ErrorIs(t, err, body)
				} else {
					require.NoError(t, err)
				}
			}
			if o.closeErr != nil {
				_, second := executeApplyPlanStatement(executionCtx, q, 6, "second plan write")
				_ = second
				err = errors.Join(err, cache.closeAndRelease(executionCtx))
				require.ErrorIs(t, err, cleanup)
				require.Contains(t, o.events, "statement-close:plan write")
				require.Contains(t, o.events, "statement-close:second plan write")
				err = errors.Join(&SyncWorkLimitError{}, err)
				require.ErrorIs(t, commitApplyProgress(ctx, tx, err), cleanup)
				require.NotContains(t, o.events, "exec:COMMIT")
			} else {
				require.NoError(t, cache.Close())
			}
			_, err = executeApplyPlanStatement(ctx, q, 6, "after-close")
			require.ErrorContains(t, err, "is closed")
			require.NotContains(t, o.events, "prepare:after-close")
		})
	}
}
func TestReplayStatementCleanup_PendingAndCancellation(t *testing.T) {
	ctx := context.Background()
	work := &SyncWorkLimitError{}
	cleanup := errors.New("cleanup")
	for _, failure := range []error{errors.Join(work, cleanup), errors.Join(work, context.Canceled)} {
		o := &statementLifecycleObservation{}
		db := statementObservationDB(t, o)
		tx, err := beginImmediateTx(ctx, db)
		require.NoError(t, err)
		err = commitApplyProgress(ctx, tx, failure)
		require.ErrorIs(t, err, work)
		require.NotContains(t, o.events, "exec:COMMIT")
		require.NoError(t, tx.Rollback())
	}
	c, db := newA1SurvivorClient(t)
	executionCtx, tx, q, cache, _ := configuredReadCache(t, c, db)
	_, err := q.ExecContext(executionCtx, `UPDATE nodes SET name='partial' WHERE id='P'`)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(executionCtx)
	cancel()
	require.ErrorIs(t, cache.closeAndRelease(canceled), context.Canceled)
	require.NoError(t, tx.Rollback())
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id='P'`).Scan(&name))
	require.Equal(t, "parent", name)
}

func TestReplayStatementCleanup_PlanSaveWorkBoundary(t *testing.T) {
	for _, reuse := range []bool{true, false} {
		for _, remaining := range []int64{0, 17, 18, 29, 30} {
			t.Run(fmt.Sprintf("reuse=%t/remaining=%d", reuse, remaining), func(t *testing.T) {
				c, db, path := qualificationDurableClient(t)
				beforeState := qualificationState(t, db, "nodes")
				ctx, tx, q, cache, lease := configuredReadCache(t, c, db)
				cache.pool.reuse = reuse
				guard, err := c.applyGuard(ctx, q, lease.unit)
				require.NoError(t, err)
				// A guard restart has saved its new guard/clear phase, but still
				// carries the old preservation state until clearApplyStep finishes.
				old := &applyPlan{unit: lease.unit, guard: guard, phase: "clear",
					root: 7, visit: 11, table: 2, fk: 1, cursor: sql.NullString{String: "old cursor", Valid: true},
					kind: "settlement", preserve: [3]string{"main", "nodes", "old key"}, atomicWork: 1024, preservationComplete: true}
				require.NoError(t, saveApplyPlan(context.Background(), tx, old))
				reset := *old
				reset.workspaceContext, reset.workspaceQuery = ctx, q
				require.NoError(t, c.clearApplyStep(ctx, q, &reset))
				require.Equal(t, "roots", reset.phase)
				require.False(t, reset.preservationComplete)
				require.NoError(t, c.checkApplyWorkspace(&reset, 0, 0))
				reset.workspaceContext, reset.workspaceQuery = nil, nil
				limit := c.config.MaxSyncWorkUnitsPerInvocation
				used, statementWork := c.syncWorkUsed, lease.statementWork
				c.config.MaxSyncWorkUnitsPerInvocation = used + remaining
				err = saveApplyPlan(ctx, q, &reset)
				charged, statementCharged := c.syncWorkUsed-used, lease.statementWork-statementWork
				want := &reset
				if remaining < 30 {
					var refusal *SyncWorkLimitError
					require.ErrorAs(t, err, &refusal)
					want = old
				} else {
					require.NoError(t, err)
				}
				require.NoError(t, cache.closeAndRelease(ctx))
				if err != nil {
					require.ErrorIs(t, commitApplyProgress(ctx, tx, err), err)
				} else {
					require.NoError(t, tx.Commit(ctx))
				}
				require.True(t, tx.done)
				require.NoError(t, lease.release())
				require.Equal(t, beforeState, qualificationState(t, db, "nodes"))
				require.Zero(t, countA3(t, db, `SELECT apply_mode FROM _sync_apply_state`))
				c.config.MaxSyncWorkUnitsPerInvocation = limit
				reopened, durable := qualificationPhysicalRestart(t, c, db, path)
				load := func(q queryRower) *applyPlan {
					p, err := loadApplyPlan(context.Background(), q)
					require.NoError(t, err)
					p.workspaceContext, p.workspaceQuery = nil, nil
					return p
				}
				require.Equal(t, want, load(durable), "all 18 durable fields must come from one complete save")
				if remaining < 30 {
					require.Zero(t, charged, "refusal must precede both writes and their work debit")
					require.Zero(t, statementCharged)
				} else {
					require.EqualValues(t, 30, charged)
					require.EqualValues(t, 30, statementCharged)
				}
				// A fresh owner can complete the reset after a committed refusal.
				nextCtx, nextTx, nextQ, nextCache, nextLease := configuredReadCache(t, reopened, durable)
				require.NotSame(t, cache, nextCache)
				resumed := load(nextTx)
				if resumed.phase == "clear" {
					resumed.workspaceContext, resumed.workspaceQuery = nextCtx, nextQ
					require.NoError(t, reopened.clearApplyStep(nextCtx, nextQ, resumed))
					require.NoError(t, saveApplyPlan(nextCtx, nextQ, resumed))
				}
				require.Equal(t, &reset, load(nextTx))
				require.NoError(t, nextCache.closeAndRelease(nextCtx))
				require.NoError(t, nextTx.Commit(nextCtx))
				require.NoError(t, nextLease.release())
				require.Equal(t, beforeState, qualificationState(t, durable, "nodes"))
			})
		}
	}
}

func TestReplayStatementCleanup_PlanSaveCancellation(t *testing.T) {
	closeCause := errors.New("plan close after cancellation")
	for _, cleanup := range []error{nil, closeCause} {
		t.Run(fmt.Sprintf("closeFailure=%t", cleanup != nil), func(t *testing.T) {
			o := &statementLifecycleObservation{closeErr: cleanup}
			db := statementObservationDB(t, o)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := beginImmediateTx(ctx, db)
			require.NoError(t, err)
			defer tx.Rollback()
			cache := newTxStmtCache(tx)
			cache.active = true
			cache.pool = &statementReservation{atomic: true, credits: 30, reuse: true, handles: 2, sqlBytes: 8192}
			ctx = context.WithValue(ctx, statementCacheKey{}, cache)
			q := &statementTransaction{sqliteTransaction: tx, cache: cache}
			o.afterExec = cancel
			err = saveApplyPlan(ctx, q, &applyPlan{unit: "unit", guard: "guard", phase: "roots", kind: "settlement"})
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, cache.pool.credits, "the first write admits the complete save cost")
			require.Len(t, cache.order, 1, "the canceled second write must not prepare or execute")
			query := cache.order[0]
			err = errors.Join(err, cache.Close())
			if cleanup != nil {
				require.ErrorIs(t, err, cleanup)
			}
			require.ErrorIs(t, commitApplyProgress(ctx, tx, err), context.Canceled)
			require.NotContains(t, o.events, "exec:COMMIT")
			require.NoError(t, tx.Rollback())
			require.Contains(t, o.events, "exec:ROLLBACK")
			require.Contains(t, o.events, "statement-close:"+query)
		})
	}
}

func TestReplayStatementCleanup_PreparationCheckpoint(t *testing.T) {
	for _, phase := range []string{"roots", "index", "walk", "clear rows", "clear edges", "exception"} {
		for _, reuse := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/reuse=%t", phase, reuse), func(t *testing.T) {
				mutatingProgress := 0
				for _, room := range []int64{100, 112, 120, 128, 136, 144, 152, 160, 176, 192, 208, 224, 240} {
					t.Run(fmt.Sprint(room), func(t *testing.T) {
						c, db, path := qualificationDurableClient(t)
						if phase == "exception" {
							_, err := db.Exec(`UPDATE nodes SET name='desired' WHERE id='P';
								UPDATE _sync_apply_state SET apply_mode=1;
									UPDATE nodes SET name='physical' WHERE id='P';
									UPDATE _sync_apply_state SET apply_mode=0`)
							require.NoError(t, err)
						}
						before := qualificationState(t, db, "nodes")
						ctx, tx, q, cache, lease := configuredReadCache(t, c, db)
						cache.pool.reuse = reuse
						limit := c.config.MaxSyncWorkUnitsPerInvocation
						c.config.MaxSyncWorkUnitsPerInvocation = c.syncWorkUsed + room
						guard, err := c.applyGuard(context.Background(), tx, lease.unit)
						require.NoError(t, err)
						p := &applyPlan{unit: lease.unit, guard: guard, phase: phase, kind: "pull",
							workspaceContext: context.Background(), workspaceQuery: tx}
						roots := 1
						parent, err := c.captureApplySurvivor(context.Background(), tx, p, "nodes", "P")
						require.NoError(t, err)
						seed := func(int) (applySurvivor, error) {
							return parent, nil
						}
						if phase == "walk" || phase == "clear rows" {
							row, err := seed(0)
							require.NoError(t, err)
							require.NoError(t, c.putApplySurvivor(context.Background(), tx, p, row))
						}
						if phase == "walk" || phase == "clear edges" {
							_, err := tx.ExecContext(context.Background(), `INSERT INTO _sync_apply_edges VALUES(1,'nodes','P','nodes','C',136)`)
							require.NoError(t, err)
							p.edgeEntries, p.edgeBytes = 1, 136
						}
						switch phase {
						case "index":
							roots, p.table, p.fk, p.cursor = 0, 0, 0, sql.NullString{String: "C", Valid: true}
						case "walk":
							p.root, p.visit = 1, 1
						case "clear rows", "clear edges":
							p.phase = "clear"
						case "exception":
							roots, p.phase, p.kind = 0, "ready", "settlement"
						}
						require.NoError(t, saveApplyPlan(context.Background(), tx, p))
						beforeRows, beforeEdges := p.entries, p.edgeEntries
						err = c.prepareApplySurvivors(ctx, q, lease.unit, roots, seed, p.kind)
						var pending *SyncWorkLimitError
						if errors.As(err, &pending) {
							require.NoError(t, cache.closeAndRelease(ctx))
							require.ErrorIs(t, commitApplyProgress(ctx, tx, err), err)
						} else {
							var capacity *SyncWorkspaceLimitError
							require.ErrorAs(t, err, &capacity)
							require.NoError(t, cache.closeAndRelease(ctx))
							require.NoError(t, tx.Rollback())
						}
						require.NoError(t, lease.release())
						rows := countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`)
						edges := countA3(t, db, `SELECT count(*) FROM _sync_apply_edges`)
						reopened, durable := qualificationPhysicalRestart(t, c, db, path)
						require.NoError(t, reconcileWorkspaceOwners(context.Background(), durable, reopened.config))
						if pending != nil && (rows != beforeRows || edges != beforeEdges) {
							mutatingProgress++
							// Reach a real admitted opening only after commit and physical
							// reopen; stale maintained counters must fail this assertion.
							stored, err := loadApplyPlan(context.Background(), durable)
							require.NoError(t, err)
							require.EqualValues(t, rows, stored.entries)
							require.EqualValues(t, edges, stored.edgeEntries)
							require.EqualValues(t, countA3(t, durable, `SELECT COALESCE(sum(bytes),0) FROM _sync_apply_rows`), stored.bytes)
							require.EqualValues(t, countA3(t, durable, `SELECT COALESCE(sum(bytes),0) FROM _sync_apply_edges`), stored.edgeBytes)
							reopened.config.MaxSyncWorkUnitsPerInvocation = limit
							nextCtx, nextTx, nextQ, nextCache, nextLease := configuredReadCache(t, reopened, durable)
							require.NotSame(t, cache, nextCache)
							require.NoError(t, reopened.prepareApplySurvivors(nextCtx, nextQ, nextLease.unit, roots, seed, p.kind))
							require.NoError(t, reconcileWorkspaceOwners(context.Background(), nextTx, reopened.config))
							require.NoError(t, nextCache.closeAndRelease(nextCtx))
							require.NoError(t, nextTx.Commit(nextCtx))
							require.NoError(t, nextLease.release())
							require.Equal(t, before, qualificationState(t, durable, "nodes"))
						} else {
							require.Equal(t, before, qualificationState(t, durable, "nodes"))
						}
					})
				}
				require.Positive(t, mutatingProgress, "must execute a mutating transition and commit pending progress")
			})
		}
	}
}

func TestReplayStatementCleanup_CheckpointAbort(t *testing.T) {
	for _, second := range []bool{false, true} {
		t.Run(fmt.Sprintf("second=%t", second), func(t *testing.T) {
			c, db, path := qualificationDurableClient(t)
			before := qualificationState(t, db, "nodes")
			ctx, tx, q, cache, lease := configuredReadCache(t, c, db)
			guard, err := c.applyGuard(ctx, q, lease.unit)
			require.NoError(t, err)
			p := &applyPlan{unit: lease.unit, guard: guard, phase: "roots", kind: "pull"}
			require.NoError(t, saveApplyPlan(context.Background(), tx, p))
			p.workspaceContext, p.workspaceQuery = context.Background(), tx
			parent, err := c.captureApplySurvivor(context.Background(), tx, p, "nodes", "P")
			require.NoError(t, err)
			predicate := "NEW.root_cursor=1"
			if second {
				predicate += " AND OLD.root_cursor=1"
			}
			_, err = tx.ExecContext(context.Background(), `CREATE TEMP TRIGGER reject_checkpoint BEFORE UPDATE ON _sync_apply_plan WHEN `+predicate+` BEGIN SELECT RAISE(ABORT,'checkpoint rejected'); END`)
			require.NoError(t, err)
			err = c.prepareApplySurvivors(ctx, q, lease.unit, 1, func(int) (applySurvivor, error) {
				return parent, nil
			})
			require.ErrorContains(t, err, "checkpoint rejected")
			var rows int
			require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT count(*) FROM _sync_apply_rows`).Scan(&rows))
			require.Equal(t, 1, rows, "stage mutation precedes the native checkpoint abort")
			require.NoError(t, cache.closeAndRelease(ctx))
			require.ErrorContains(t, commitApplyProgress(ctx, tx, err), "checkpoint rejected")
			require.False(t, tx.done)
			require.NoError(t, tx.Rollback())
			require.NoError(t, lease.release())
			reopened, durable := qualificationPhysicalRestart(t, c, db, path)
			require.Zero(t, countA3(t, durable, `SELECT count(*) FROM _sync_apply_rows`))
			require.NoError(t, reconcileWorkspaceOwners(context.Background(), durable, reopened.config))
			require.Equal(t, before, qualificationState(t, durable, "nodes"))
		})
	}
}

func TestReplayStatementCleanup_RetryAndReopen(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("commit/rollback")
	o := &statementLifecycleObservation{commitErr: failure, rollbackErr: failure}
	db := statementObservationDB(t, o)
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	cache := newTxStmtCache(tx)
	_, err = cache.ExecContext(ctx, "first-owner")
	require.NoError(t, err)
	require.NoError(t, cache.Close())
	require.ErrorIs(t, tx.Commit(ctx), failure)
	require.ErrorIs(t, tx.Rollback(), failure)
	require.Contains(t, o.events, "connection-close")
	c, durable, path := qualificationDurableClient(t)
	executionCtx, real, q, owned, lease := configuredReadCache(t, c, durable)
	var n int
	require.NoError(t, scanTransactionRow(executionCtx, q, `SELECT count(*) FROM nodes`, nil, &n))
	require.NoError(t, owned.closeAndRelease(executionCtx))
	require.NoError(t, real.Rollback())
	require.NoError(t, lease.release())
	require.NoError(t, c.Close())
	require.NoError(t, durable.Close())
	reopened, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	defer reopened.Close()
	require.NoError(t, reopened.QueryRow(`SELECT count(*) FROM nodes`).Scan(&n))
	require.NoError(t, reopened.QueryRow(`SELECT reserved_entries FROM _sync_operation_state WHERE singleton_key=1`).Scan(&n))
	require.Zero(t, n)
}
