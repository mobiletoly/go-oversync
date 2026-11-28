// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type execContexter interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}
type sqliteTransaction interface {
	execContexter
	queryRower
	tableInfoQueryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

type statementCacheKey struct{}
type statementAccountingKey struct{}

// statementTransaction forwards every opted-in write to the same writer owner.
// QueryRowContext stays concrete; opted-in callers use scanTransactionRow.
type statementTransaction struct {
	sqliteTransaction
	cache *txStmtCache
}

func (t *statementTransaction) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return t.cache.ExecContext(ctx, q, args...)
}

type txStmtCache struct {
	tx             sqliteTransaction
	stmts          map[string]*sql.Stmt
	order          []string
	closed, active bool
	planningWrites bool
	rowsActive     bool
	pool           *statementReservation
	queryBytes     int64
	cleanupErr     error
}

func newTxStmtCache(tx sqliteTransaction) *txStmtCache {
	return &txStmtCache{tx: tx, stmts: make(map[string]*sql.Stmt)}
}
func (c *txStmtCache) check() error {
	if c == nil || c.tx == nil {
		return fmt.Errorf("transaction statement cache requires a transaction")
	}
	if c.closed {
		return fmt.Errorf("transaction statement cache is closed")
	}
	if c.rowsActive {
		return fmt.Errorf("transaction statement cache has an active cursor")
	}
	return nil
}
func (c *txStmtCache) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if s := c.stmts[query]; s != nil {
		return s, nil
	}
	s, err := c.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	c.stmts[query] = s
	c.order = append(c.order, query)
	return s, nil
}
func (c *txStmtCache) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if !c.active {
		s, err := c.stmt(ctx, query)
		if err != nil {
			return nil, err
		}
		return s.ExecContext(ctx, args...)
	}
	if c.planningWrites {
		return c.execPlanningStatement(ctx, query, args...)
	}
	if err := c.pool.work(ctx, 2); err != nil {
		return nil, err
	}
	// Ready writes debit the existing atomic credits and admitted statement pool.
	if c.pool.atomic {
		if err := c.pool.work(ctx, int64(4+len(args))); err != nil {
			return nil, err
		}
		values, within, err := boundedStatementArguments(args)
		if err == nil && within {
			stmt, admitted, err := c.retainedStatement(ctx, query)
			if err != nil {
				return nil, err
			}
			if admitted {
				return stmt.ExecContext(ctx, values...)
			}
		}
	}
	return c.execOneShot(ctx, query, args...)
}

func (c *txStmtCache) execOneShot(ctx context.Context, query string, args ...any) (result sql.Result, resultErr error) {
	s, err := c.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup := s.Close(); cleanup != nil {
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	return s.ExecContext(ctx, args...)
}

// statementArgument does not call arbitrary Valuer implementations or encode
// data to measure it. Private reads and plan writes use these scalar/null types.
func statementArgument(v any) (any, int64, error) {
	switch x := v.(type) {
	case nil:
		return nil, 4, nil
	case string:
		if len(x) > (1<<16)/6 {
			return x, 1 << 17, nil
		}
		return x, 6*int64(len(x)) + 2, nil
	case []byte:
		if len(x) > (1<<16)/2 {
			return x, 1 << 17, nil
		}
		return x, 2*int64(len(x)) + 2, nil
	case bool:
		return x, 4, nil
	case int:
		return int64(x), 32, nil
	case int8:
		return int64(x), 32, nil
	case int16:
		return int64(x), 32, nil
	case int32:
		return int64(x), 32, nil
	case int64:
		return x, 32, nil
	case float32:
		return float64(x), 32, nil
	case float64:
		return x, 32, nil
	case sql.NullString:
		if !x.Valid {
			return nil, 4, nil
		}
		return statementArgument(x.String)
	case sql.NullInt64:
		if !x.Valid {
			return nil, 4, nil
		}
		return x.Int64, 32, nil
	case sql.NullInt32:
		if !x.Valid {
			return nil, 4, nil
		}
		return int64(x.Int32), 32, nil
	case sql.NullInt16:
		if !x.Valid {
			return nil, 4, nil
		}
		return int64(x.Int16), 32, nil
	case sql.NullByte:
		if !x.Valid {
			return nil, 4, nil
		}
		return int64(x.Byte), 32, nil
	case sql.NullBool:
		if !x.Valid {
			return nil, 4, nil
		}
		return x.Bool, 4, nil
	case sql.NullFloat64:
		if !x.Valid {
			return nil, 4, nil
		}
		return x.Float64, 32, nil
	default:
		return nil, 0, fmt.Errorf("unsupported prepared read argument type %T", v)
	}
}

func scanStatement(ctx context.Context, s *sql.Stmt, args []any, targets ...any) (resultErr error) {
	rows, err := s.QueryContext(ctx, args...)
	if err != nil {
		return err
	}
	defer func() {
		if cleanup := rows.Close(); cleanup != nil {
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	for _, target := range targets {
		if _, ok := target.(*sql.RawBytes); ok {
			return fmt.Errorf("sql: RawBytes isn't allowed on Row.Scan")
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
func boundedStatementArguments(args []any) ([]any, bool, error) {
	var values [16]any
	if len(args) > len(values) {
		return nil, false, fmt.Errorf("prepared statement has too many arguments")
	}
	var encoded int64
	for i, v := range args {
		value, size, err := statementArgument(v)
		if err != nil {
			return nil, false, err
		}
		values[i] = value
		encoded += size
	}
	return values[:len(args)], encoded <= 1<<16, nil
}

// retainedStatement admits metadata before inserting a handle. A refused
// slot remains a one-shot operation; it never creates a second cache owner.
func (c *txStmtCache) retainedStatement(ctx context.Context, query string) (*sql.Stmt, bool, error) {
	if c.pool == nil || !c.pool.reuse {
		return nil, false, nil
	}
	fresh := c.stmts[query] == nil
	if fresh && (int64(len(c.stmts)) >= c.pool.handles || int64(len(query)) > c.pool.sqlBytes-c.queryBytes) {
		return nil, false, nil
	}
	s, err := c.stmt(ctx, query)
	if err == nil && fresh {
		c.queryBytes += int64(len(query))
	}
	return s, true, err
}

func (c *txStmtCache) execApplyPlan(ctx context.Context, work int64, query string, args ...any) (sql.Result, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	// The first save call prepays both statements' prepare/close and sizing
	// visits. The second uses that admission and still checks cancellation.
	if err := c.pool.work(ctx, work); err != nil {
		return nil, err
	}
	values, within, err := boundedStatementArguments(args)
	if err != nil {
		return nil, err
	}
	if within {
		s, admitted, err := c.retainedStatement(ctx, query)
		if err != nil {
			return nil, err
		}
		if admitted {
			return s.ExecContext(ctx, values...)
		}
	}
	return c.execOneShot(ctx, query, args...)
}

// Apply-plan saves use this opt-in before the ready atomic unit.
// Private planning writers opt into their separately charged scalar dispatcher.
func executeApplyPlanStatement(ctx context.Context, tx sqliteTransaction, work int64, query string, args ...any) (sql.Result, error) {
	if c := statementCacheFrom(ctx, tx); c != nil && c.active {
		return c.execApplyPlan(ctx, work, query, args...)
	}
	return tx.ExecContext(ctx, query, args...)
}

func admitApplyPlanCheckpoint(ctx context.Context, tx sqliteTransaction) error {
	if c := statementCacheFrom(ctx, tx); c != nil && c.active {
		if err := c.check(); err != nil {
			return err
		}
		// Cover both saves plus the added admission/completion visits before
		// any staging mutation. No new reservation owner or work refund.
		return c.pool.work(ctx, applyPlanStatementWork+2)
	}
	return ctx.Err()
}

func (c *txStmtCache) ScanContext(ctx context.Context, query string, args []any, targets ...any) (resultErr error) {
	if err := c.check(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	reuse := c.active && c.pool != nil && c.pool.reuse
	normalized := args
	if c.active {
		if err := c.pool.work(ctx, int64(4+len(args))); err != nil {
			return err
		}
		values, within, err := boundedStatementArguments(args)
		if err != nil {
			return err
		}
		normalized = values
		reuse = reuse && within
	}
	var s *sql.Stmt
	var err error
	if reuse {
		s, reuse, err = c.retainedStatement(ctx, query)
		args = normalized
	}
	if err != nil {
		return err
	}
	if !reuse {
		if c.active {
			if err = c.pool.work(ctx, 2); err != nil {
				return err
			}
		}
		s, err = c.tx.PrepareContext(ctx, query)
		if err == nil {
			defer func() {
				if cleanup := s.Close(); cleanup != nil {
					resultErr = errors.Join(resultErr, cleanup)
				}
			}()
		}
	}
	if err != nil {
		return err
	}
	c.rowsActive = true
	defer func() { c.rowsActive = false }()
	return scanStatement(ctx, s, args, targets...)
}
func (c *txStmtCache) Close() error {
	if c == nil || c.closed {
		return nil
	}
	if c.rowsActive {
		return fmt.Errorf("cannot close transaction statement cache with an active cursor")
	}
	c.closed = true
	var result error
	for i, q := range c.order {
		if s := c.stmts[q]; s != nil {
			if err := s.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("close prepared statement %d: %w", i+1, err))
			}
		}
	}
	c.stmts = nil
	c.order = nil
	c.queryBytes = 0
	return result
}
func statementCacheFrom(ctx context.Context, q any) *txStmtCache {
	c, _ := ctx.Value(statementCacheKey{}).(*txStmtCache)
	if c == nil {
		return nil
	}
	switch tx := q.(type) {
	case *statementTransaction:
		if tx.cache == c {
			return c
		}
	case *immediateTx:
		if c.tx == tx {
			return c
		}
	}
	return nil
}
func scanTransactionRow(ctx context.Context, q queryRower, query string, args []any, targets ...any) error {
	c := statementCacheFrom(ctx, q)
	accounting, _ := ctx.Value(statementAccountingKey{}).(bool)
	if c != nil && !accounting {
		return c.ScanContext(ctx, query, args, targets...)
	}
	if accounting {
		if raw, ok := q.(interface {
			QueryContext(context.Context, string, ...any) (*sql.Rows, error)
		}); ok {
			return scanRawRow(ctx, raw, query, args, targets...)
		}
	}
	return q.QueryRowContext(ctx, query, args...).Scan(targets...)
}
func scanRawRow(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, args []any, targets ...any) (resultErr error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() {
		if cleanup := rows.Close(); cleanup != nil {
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return rows.Scan(targets...)
}
func (c *txStmtCache) closeAndRelease(ctx context.Context) error {
	if err := c.Close(); err != nil {
		c.cleanupErr = errors.Join(c.cleanupErr, err)
	}
	if c.cleanupErr != nil {
		return c.cleanupErr
	}
	if c.pool != nil {
		return c.pool.release(ctx)
	}
	return nil
}

func executeSingleStatement(ctx context.Context, tx sqliteTransaction, query string, args ...any) (resultErr error) {
	s, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		if cleanup := s.Close(); cleanup != nil {
			resultErr = errors.Join(resultErr, cleanup)
		}
	}()
	_, resultErr = s.ExecContext(ctx, args...)
	return resultErr
}

func (c *txStmtCache) execPlanningStatement(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := c.pool.work(ctx, int64(4+len(args))); err != nil {
		return nil, err
	}
	values, within, err := boundedStatementArguments(args)
	if err != nil {
		return nil, err
	}
	if within {
		stmt, admitted, err := c.retainedStatement(ctx, query)
		if err != nil {
			return nil, err
		}
		if admitted {
			return stmt.ExecContext(ctx, values...)
		}
	}
	if err := c.pool.work(ctx, 2); err != nil {
		return nil, err
	}
	return c.execOneShot(ctx, query, args...)
}
