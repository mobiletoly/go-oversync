package oversqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

// immediateTx owns one dedicated SQLite connection between BEGIN IMMEDIATE
// and COMMIT/ROLLBACK. database/sql's deferred BeginTx does not reserve the
// writer before the final no-data-loss gate.
type immediateTx struct {
	conn        *sql.Conn
	ctx         context.Context
	done        bool
	closeOnDone bool
}

func beginImmediateTx(ctx context.Context, db *sql.DB) (*immediateTx, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to reserve SQLite connection for immediate transaction: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to enable foreign keys on reserved SQLite writer: %w", err)
	}
	var foreignKeys int
	if err := conn.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to verify foreign keys on reserved SQLite writer: %w", err)
	}
	if foreignKeys != 1 {
		_ = conn.Close()
		return nil, fmt.Errorf("foreign keys are disabled on reserved SQLite writer")
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to begin immediate SQLite transaction: %w", err)
	}
	return &immediateTx{conn: conn, ctx: ctx, closeOnDone: true}, nil
}

func validateForeignKeyIntegrity(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) error {
	rows, err := q.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("failed to verify foreign key integrity: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		var table, parent string
		var rowID, foreignKeyID any
		if err := rows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			return fmt.Errorf("failed to inspect foreign key violation: %w", err)
		}
		return fmt.Errorf("foreign key integrity violation in table %s referencing %s (row=%v constraint=%v)", table, parent, rowID, foreignKeyID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate foreign key integrity results: %w", err)
	}
	return nil
}

func (tx *immediateTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *immediateTx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.conn.ExecContext(tx.ctx, query, args...)
}

func (tx *immediateTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *immediateTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *immediateTx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(tx.ctx, query, args...)
}

func (tx *immediateTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.conn.PrepareContext(ctx, query)
}

func (tx *immediateTx) Commit(ctx context.Context) error {
	if tx == nil || tx.done {
		return fmt.Errorf("immediate SQLite transaction is not active")
	}
	if _, err := tx.conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	tx.done = true
	if tx.closeOnDone {
		_ = tx.conn.Close()
	}
	return nil
}

func (tx *immediateTx) Rollback() error {
	if tx == nil || tx.done {
		return nil
	}
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, rollbackErr := tx.conn.ExecContext(rollbackCtx, `ROLLBACK`)
	tx.done = true
	if rollbackErr != nil {
		discardErr := tx.conn.Raw(func(any) error { return driver.ErrBadConn })
		if discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) && !errors.Is(discardErr, sql.ErrConnDone) {
			return errors.Join(rollbackErr, fmt.Errorf("failed to discard SQLite connection after rollback failure: %w", discardErr))
		}
		return rollbackErr
	}
	if tx.closeOnDone {
		return tx.conn.Close()
	}
	return nil
}

func (tx *immediateTx) rollbackOnReturn(resultErr *error, operation string) {
	rollbackErr := tx.Rollback()
	if rollbackErr == nil {
		return
	}
	cleanupErr := fmt.Errorf("failed to roll back %s: %w", operation, rollbackErr)
	if *resultErr == nil {
		*resultErr = cleanupErr
		return
	}
	*resultErr = errors.Join(*resultErr, cleanupErr)
}
