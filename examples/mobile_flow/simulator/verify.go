package simulator

import (
	"context"
	"database/sql"
	"fmt"
)

// DebugVerifyPendingCleared checks that the local pending queue is empty.
func (app *MobileApp) DebugVerifyPendingCleared(ctx context.Context) error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}
	var pending int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_pending`).Scan(&pending); err != nil {
		return fmt.Errorf("failed to count _sync_pending: %w", err)
	}
	if pending != 0 {
		return fmt.Errorf("pending not cleared: %d changes remain", pending)
	}
	return nil
}

// DebugVerifyBusinessFKIntegrity verifies basic FK integrity for the typical demo schema (users/posts).
// It is intended for simulator/testing only and is not used by the general-purpose library.
func (app *MobileApp) DebugVerifyBusinessFKIntegrity(ctx context.Context) error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// If these tables don't exist, skip silently (simulator-specific check)
	hasTable := func(db *sql.DB, name string) bool {
		var c int
		_ = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&c)
		return c > 0
	}
	if !hasTable(app.db, "users") || !hasTable(app.db, "posts") {
		return nil
	}

	// Count posts with missing users
	var invalidFK int
	err := app.db.QueryRowContext(ctx, `
        SELECT COUNT(*)
        FROM posts p
        LEFT JOIN users u ON p.author_id = u.id
        WHERE u.id IS NULL
    `).Scan(&invalidFK)
	if err != nil {
		return fmt.Errorf("failed FK integrity query: %w", err)
	}
	if invalidFK != 0 {
		return fmt.Errorf("fk integrity failed: %d posts without valid users", invalidFK)
	}
	return nil
}
