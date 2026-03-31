package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// HistoryPrunedError reports that pull history is no longer available and the client must rebuild from snapshot state.
type HistoryPrunedError struct {
	Status  int
	Message string
}

// RebuildRequiredError reports that normal sync is blocked until the client completes Rebuild.
type RebuildRequiredError struct{}

// Error implements error.
func (e *RebuildRequiredError) Error() string {
	return "client rebuild is required; run Rebuild before syncing"
}

// SyncOperationInProgressError reports that another sync operation is already active for the client.
type SyncOperationInProgressError struct{}

// Error implements error.
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

// PendingPushReplayError reports that pull or rebuild is blocked by an unfinished frozen outbox bundle.
type PendingPushReplayError struct {
	OutboundCount int
}

// Error implements error.
func (e *PendingPushReplayError) Error() string {
	return fmt.Sprintf("cannot rebuild while %d staged push rows are pending authoritative replay", e.OutboundCount)
}

// Error implements error.
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
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count staged outbound push rows: %w", err)
	}
	return count, nil
}

func (c *Client) rebuildRequired(ctx context.Context) (bool, error) {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return false, err
	}
	if attachment.BindingState != attachmentBindingAttached || attachment.AttachedUserID != c.UserID {
		return false, &AttachRequiredError{Operation: "sync operations"}
	}
	return attachment.RebuildRequired, nil
}

func (c *Client) setRebuildRequired(ctx context.Context, required bool) error {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	if attachment.BindingState != attachmentBindingAttached || attachment.AttachedUserID != c.UserID {
		return &AttachRequiredError{Operation: "Rebuild()"}
	}
	attachment.RebuildRequired = required
	return persistAttachmentState(ctx, c.DB, attachment)
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_outbox_rows`); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot during recovery: %w", err)
	}
	if err := clearOutboxBundle(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("failed to clear staged push bundle rows during recovery: %w", err)
	}
	return nil
}

func (c *Client) clearFullLocalSyncStateInTx(ctx context.Context, tx *sql.Tx) error {
	if err := setApplyMode(ctx, tx, true); err != nil {
		return fmt.Errorf("failed to enable apply_mode before full local reset: %w", err)
	}
	restoreApplyMode := true
	defer func() {
		if !restoreApplyMode {
			return
		}
		_ = setApplyMode(ctx, tx, false)
	}()
	if err := c.clearManagedTablesInTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return fmt.Errorf("failed to clear snapshot staging during recovery: %w", err)
	}
	if err := setApplyMode(ctx, tx, false); err != nil {
		return fmt.Errorf("failed to restore apply_mode after full local reset: %w", err)
	}
	restoreApplyMode = false
	return nil
}
