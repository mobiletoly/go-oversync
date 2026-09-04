package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	sqlite3 "github.com/mattn/go-sqlite3"
)

type restrictedMigrationCapability struct {
	conn        *sql.Conn
	active      atomic.Bool
	scopeCtx    context.Context
	scopeCancel context.CancelFunc
	resultsMu   sync.Mutex
	results     []func() error
}

func (c *restrictedMigrationCapability) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	operationCtx, cleanup, err := c.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return c.conn.ExecContext(operationCtx, query, args...)
}

func (c *restrictedMigrationCapability) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	operationCtx, cleanup, err := c.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.conn.QueryContext(operationCtx, query, args...)
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := c.trackResult(func() error {
		cleanup()
		return rows.Close()
	}); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *restrictedMigrationCapability) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	operationCtx, cleanup, err := c.operationContext(ctx)
	if err != nil {
		return c.inactiveRow(err)
	}
	row := c.conn.QueryRowContext(operationCtx, query, args...)
	if err := c.trackResult(func() error {
		cleanup()
		return nil
	}); err != nil {
		return c.inactiveRow(err)
	}
	return row
}

func newRestrictedMigrationCapability(ctx context.Context, conn *sql.Conn) *restrictedMigrationCapability {
	scopeCtx, scopeCancel := context.WithCancel(ctx)
	capability := &restrictedMigrationCapability{conn: conn, scopeCtx: scopeCtx, scopeCancel: scopeCancel}
	capability.active.Store(true)
	return capability
}

func (c *restrictedMigrationCapability) operationContext(ctx context.Context) (context.Context, func(), error) {
	if c == nil || c.conn == nil || c.scopeCtx == nil || !c.active.Load() {
		return nil, nil, fmt.Errorf("local migration capability is no longer active")
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("local migration context is required")
	}
	operationCtx, cancel := context.WithCancel(c.scopeCtx)
	stop := context.AfterFunc(ctx, cancel)
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			stop()
			cancel()
		})
	}
	if err := ctx.Err(); err != nil {
		cleanup()
		return nil, nil, err
	}
	return operationCtx, cleanup, nil
}

func (c *restrictedMigrationCapability) trackResult(cleanup func() error) error {
	if c == nil || cleanup == nil {
		return fmt.Errorf("local migration result tracking is unavailable")
	}
	c.resultsMu.Lock()
	if !c.active.Load() {
		c.resultsMu.Unlock()
		_ = cleanup()
		return fmt.Errorf("local migration capability is no longer active")
	}
	c.results = append(c.results, cleanup)
	c.resultsMu.Unlock()
	return nil
}

func (c *restrictedMigrationCapability) expire() error {
	if c == nil || !c.active.Swap(false) {
		return nil
	}
	if c.scopeCancel != nil {
		c.scopeCancel()
	}
	c.resultsMu.Lock()
	results := c.results
	c.results = nil
	c.resultsMu.Unlock()
	var resultErr error
	for idx := len(results) - 1; idx >= 0; idx-- {
		resultErr = errors.Join(resultErr, results[idx]())
	}
	return resultErr
}

func (c *restrictedMigrationCapability) inactiveRow(cause error) *sql.Row {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	return c.conn.QueryRowContext(ctx, `SELECT 1`)
}

func installMigrationAuthorizer(conn *sql.Conn, readOnly bool) (func() error, error) {
	if conn == nil {
		return nil, fmt.Errorf("SQLite connection is required")
	}
	err := conn.Raw(func(driverConn any) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("oversqlite local migration requires mattn/go-sqlite3")
		}
		sqliteConn.RegisterAuthorizer(func(op int, arg1, arg2, arg3 string) int {
			if denyMigrationSQL(op, arg1, arg2, readOnly) {
				return sqlite3.SQLITE_DENY
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return func() error {
		return conn.Raw(func(driverConn any) error {
			sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("oversqlite local migration requires mattn/go-sqlite3")
			}
			sqliteConn.RegisterAuthorizer(nil)
			return nil
		})
	}, nil
}

func installReadOnlyMigrationGuard(ctx context.Context, conn *sql.Conn) (func() error, error) {
	if conn == nil {
		return nil, fmt.Errorf("SQLite connection is required")
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA query_only = ON`); err != nil {
		return nil, fmt.Errorf("failed to make local migration reader connection read-only: %w", err)
	}
	releaseAuthorizer, err := installMigrationAuthorizer(conn, true)
	if err != nil {
		_, resetErr := conn.ExecContext(context.Background(), `PRAGMA query_only = OFF`)
		return nil, errors.Join(err, resetErr)
	}
	return func() error {
		authorizerErr := releaseAuthorizer()
		_, resetErr := conn.ExecContext(context.Background(), `PRAGMA query_only = OFF`)
		if resetErr != nil {
			resetErr = fmt.Errorf("failed to restore local migration reader connection: %w", resetErr)
		}
		return errors.Join(authorizerErr, resetErr)
	}, nil
}

func denyMigrationSQL(op int, arg1, arg2 string, readOnly bool) bool {
	if op == sqlite3.SQLITE_TRANSACTION || op == sqlite3.SQLITE_SAVEPOINT || op == sqlite3.SQLITE_ATTACH || op == sqlite3.SQLITE_DETACH {
		return true
	}
	if op == sqlite3.SQLITE_PRAGMA {
		name := strings.ToLower(arg1)
		if readOnly {
			return name != "user_version" || arg2 != ""
		}
		if name == "user_version" {
			return false
		}
		switch name {
		case "foreign_keys", "defer_foreign_keys", "ignore_check_constraints", "writable_schema", "journal_mode", "locking_mode", "trusted_schema":
			return arg2 != ""
		}
		return false
	}
	mutating := op == sqlite3.SQLITE_INSERT || op == sqlite3.SQLITE_UPDATE || op == sqlite3.SQLITE_DELETE ||
		op == sqlite3.SQLITE_CREATE_INDEX || op == sqlite3.SQLITE_CREATE_TABLE || op == sqlite3.SQLITE_CREATE_TEMP_INDEX ||
		op == sqlite3.SQLITE_CREATE_TEMP_TABLE || op == sqlite3.SQLITE_CREATE_TEMP_TRIGGER || op == sqlite3.SQLITE_CREATE_TEMP_VIEW ||
		op == sqlite3.SQLITE_CREATE_TRIGGER || op == sqlite3.SQLITE_CREATE_VIEW || op == sqlite3.SQLITE_CREATE_VTABLE ||
		op == sqlite3.SQLITE_DROP_INDEX || op == sqlite3.SQLITE_DROP_TABLE || op == sqlite3.SQLITE_DROP_TEMP_INDEX ||
		op == sqlite3.SQLITE_DROP_TEMP_TABLE || op == sqlite3.SQLITE_DROP_TEMP_TRIGGER || op == sqlite3.SQLITE_DROP_TEMP_VIEW ||
		op == sqlite3.SQLITE_DROP_TRIGGER || op == sqlite3.SQLITE_DROP_VIEW || op == sqlite3.SQLITE_DROP_VTABLE ||
		op == sqlite3.SQLITE_ALTER_TABLE
	if readOnly && mutating {
		return true
	}
	if mutating && (strings.HasPrefix(strings.ToLower(arg1), "_sync_") || strings.HasPrefix(strings.ToLower(arg2), "_sync_")) {
		return true
	}
	return false
}

func readInstalledVersionRestricted(ctx context.Context, db *sql.DB, migrator LocalMigrator) (version int64, resultErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	release, err := installReadOnlyMigrationGuard(ctx, conn)
	if err != nil {
		return 0, err
	}
	capability := newRestrictedMigrationCapability(ctx, conn)
	defer func() {
		expireErr := capability.expire()
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf("local migrator panicked: %v", recovered)
			if cause, ok := recovered.(error); ok {
				panicErr = fmt.Errorf("local migrator panicked: %w", cause)
			}
			resultErr = errors.Join(panicErr, resultErr)
		}
		releaseErr := release()
		resultErr = errors.Join(resultErr, expireErr, releaseErr)
	}()
	version, err = migrator.InstalledVersion(ctx, capability)
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, fmt.Errorf("installed local version is negative")
	}
	return version, nil
}

func readInstalledVersionRestrictedInTx(ctx context.Context, tx *immediateTx, migrator LocalMigrator) (version int64, resultErr error) {
	if tx == nil || tx.conn == nil {
		return 0, fmt.Errorf("active upgrade transaction is required")
	}
	release, err := installMigrationAuthorizer(tx.conn, true)
	if err != nil {
		return 0, err
	}
	capability := newRestrictedMigrationCapability(ctx, tx.conn)
	defer func() {
		expireErr := capability.expire()
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf("local migrator panicked: %v", recovered)
			if cause, ok := recovered.(error); ok {
				panicErr = fmt.Errorf("local migrator panicked: %w", cause)
			}
			resultErr = errors.Join(panicErr, resultErr)
		}
		resultErr = errors.Join(resultErr, expireErr, release())
	}()
	version, err = migrator.InstalledVersion(ctx, capability)
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, fmt.Errorf("installed local version is negative")
	}
	return version, nil
}

func invokeMigratorInTx(ctx context.Context, tx *immediateTx, migrator LocalMigrator, step LocalMigrationStep) (version int64, resultErr error) {
	release, err := installMigrationAuthorizer(tx.conn, false)
	if err != nil {
		return 0, err
	}
	capability := newRestrictedMigrationCapability(ctx, tx.conn)
	defer func() {
		expireErr := capability.expire()
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf("local migrator panicked: %v", recovered)
			if cause, ok := recovered.(error); ok {
				panicErr = fmt.Errorf("local migrator panicked: %w", cause)
			}
			resultErr = errors.Join(panicErr, resultErr)
		}
		releaseErr := release()
		resultErr = errors.Join(resultErr, expireErr, releaseErr)
	}()
	if err := migrator.Migrate(ctx, capability, step); err != nil {
		return 0, err
	}
	if err := capability.expire(); err != nil {
		return 0, err
	}
	if err := release(); err != nil {
		return 0, err
	}
	release = func() error { return nil }
	readRelease, err := installReadOnlyMigrationGuard(ctx, tx.conn)
	if err != nil {
		return 0, err
	}
	release = readRelease
	reader := newRestrictedMigrationCapability(ctx, tx.conn)
	defer func() {
		resultErr = errors.Join(resultErr, reader.expire())
	}()
	version, err = migrator.InstalledVersion(ctx, reader)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func suspendInstalledCaptureTriggersInTx(ctx context.Context, tx *immediateTx, local LocalGenerationContract) error {
	provider := NewTableInfoProvider()
	seen := make(map[string]struct{}, len(local.Tables))
	for _, syncTable := range local.Tables {
		tableName := strings.ToLower(syncTable.TableName)
		if _, ok := seen[tableName]; ok {
			continue
		}
		seen[tableName] = struct{}{}
		info, err := provider.Get(tx, tableName)
		if err != nil {
			return fmt.Errorf("inspect installed table %s before trigger suspension: %w", tableName, err)
		}
		schema := syncTableSchema(local, tableName)
		if local.Proof.Fingerprint == "" {
			schema = unconfirmedLocalSchema
		}
		expected, err := expectedTriggerSQLByName(schema, syncTable, info)
		if err != nil {
			return err
		}
		existing, err := loadExistingTriggerSQLs(tx, tableName)
		if err != nil {
			return fmt.Errorf("inspect installed triggers for table %s: %w", tableName, err)
		}
		for _, suffix := range []string{"ai", "au", "ad"} {
			triggerName := fmt.Sprintf("trg_%s_%s", tableName, suffix)
			actualSQL, exists := existing[triggerName]
			if !exists {
				continue
			}
			if !sameTriggerSQL(actualSQL, expected[triggerName]) {
				return fmt.Errorf("trigger %s is not runtime-owned; refusing to suspend application SQL", triggerName)
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(triggerName))); err != nil {
				return fmt.Errorf("failed to suspend sync capture trigger %s: %w", triggerName, err)
			}
		}
	}
	return nil
}

func syncTableSchema(local LocalGenerationContract, tableName string) string {
	for _, table := range local.Generation.Tables {
		if strings.EqualFold(table.Table, tableName) {
			return table.Schema
		}
	}
	return "main"
}
