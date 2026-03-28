package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type HistoryPrunedError struct {
	Status  int
	Message string
}

type RebuildRequiredError struct{}

func (e *RebuildRequiredError) Error() string {
	return "client rebuild is required; run Hydrate or Recover before syncing"
}

type SyncOperationInProgressError struct{}

func (e *SyncOperationInProgressError) Error() string {
	return "another sync operation is already in progress for this client"
}

// IsExpectedSyncContention reports whether err means another sync operation
// already owns the client-wide sync lock, so the attempted operation did not run.
// Background schedulers can use this to suppress user-facing failure handling for
// skipped sync ticks and simply retry later.
func IsExpectedSyncContention(err error) bool {
	if err == nil {
		return false
	}
	var inProgressErr *SyncOperationInProgressError
	return errors.As(err, &inProgressErr)
}

type PendingPushReplayError struct {
	OutboundCount int
}

func (e *PendingPushReplayError) Error() string {
	return fmt.Sprintf("cannot rebuild while %d staged push rows are pending authoritative replay", e.OutboundCount)
}

func (e *HistoryPrunedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "server history required for incremental sync has been pruned"
}

func (c *Client) pendingChangeCount(ctx context.Context) (int, error) {
	var count int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count dirty changes: %w", err)
	}
	return count, nil
}

func (c *Client) pendingPushOutboundCount(ctx context.Context) (int, error) {
	var count int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count staged outbound push rows: %w", err)
	}
	return count, nil
}

func (c *Client) rebuildRequired(ctx context.Context) (bool, error) {
	var rebuildRequired int
	if err := c.DB.QueryRowContext(ctx, `SELECT rebuild_required FROM _sync_client_state WHERE user_id = ?`, c.UserID).Scan(&rebuildRequired); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, &ConnectRequiredError{Operation: "sync operations"}
		}
		return false, fmt.Errorf("failed to read rebuild_required: %w", err)
	}
	return rebuildRequired == 1, nil
}

func (c *Client) setRebuildRequired(ctx context.Context, required bool) error {
	value := 0
	if required {
		value = 1
	}
	result, err := c.DB.ExecContext(ctx, `UPDATE _sync_client_state SET rebuild_required = ? WHERE user_id = ?`, value, c.UserID)
	if err != nil {
		return fmt.Errorf("failed to update rebuild_required: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to inspect rebuild_required update result: %w", err)
	}
	if rowsAffected != 1 {
		return &ConnectRequiredError{Operation: "Hydrate()/Recover()"}
	}
	return nil
}

func (c *Client) clearManagedTablesInTx(ctx context.Context, tx *sql.Tx) error {
	managedTables, err := c.loadManagedSyncTablesForUninstall(ctx, tx)
	if err != nil {
		return err
	}
	for _, tableName := range managedTables {
		tableName = strings.ToLower(strings.TrimSpace(tableName))
		if tableName == "" {
			continue
		}
		tableExists, err := sqliteTableExists(ctx, tx, tableName)
		if err != nil {
			return fmt.Errorf("failed to inspect managed table %s: %w", tableName, err)
		}
		if tableExists {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM "%s"`, tableName)); err != nil {
				return fmt.Errorf("failed to clear managed table %s: %w", tableName, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?`, c.config.Schema, tableName); err != nil {
			return fmt.Errorf("failed to clear structured row state for %s: %w", tableName, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_dirty_rows`); err != nil {
		return fmt.Errorf("failed to clear dirty row queue during recovery: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_outbound`); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot during recovery: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("failed to clear staged push bundle rows during recovery: %w", err)
	}
	return nil
}

func (c *Client) clearFullLocalSyncStateInTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_state SET apply_mode = 1`); err != nil {
		return fmt.Errorf("failed to enable apply_mode before full local reset: %w", err)
	}
	if err := c.clearManagedTablesInTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return fmt.Errorf("failed to clear snapshot staging during recovery: %w", err)
	}
	return nil
}
