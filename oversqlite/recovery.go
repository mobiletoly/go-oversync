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

type SourceRecoveryCode string

const (
	SourceRecoveryHistoryPruned      SourceRecoveryCode = "history_pruned"
	SourceRecoverySequenceOutOfOrder SourceRecoveryCode = "source_sequence_out_of_order"
	SourceRecoverySequenceChanged    SourceRecoveryCode = "source_sequence_changed"
)

// SourceRecoveryRequiredError reports that the frozen outbox cannot be safely replayed under the
// current source identity and the client must rebuild from snapshot with source rotation.
type SourceRecoveryRequiredError struct {
	Code    SourceRecoveryCode
	Message string
}

type sourceSequenceOutOfOrderError struct {
	Status  int
	Message string
}

type sourceSequenceChangedError struct {
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

// Error implements error.
func (e *SourceRecoveryRequiredError) Error() string {
	if e != nil && strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	if e != nil {
		switch e.Code {
		case SourceRecoveryHistoryPruned:
			return "source recovery is required because retained committed history for the frozen source tuple is no longer available; rebuild from snapshot with RebuildRotateSource(newSourceID)"
		case SourceRecoverySequenceOutOfOrder:
			return "source recovery is required because the frozen source bundle sequence is not the next expected value; rebuild from snapshot with RebuildRotateSource(newSourceID)"
		case SourceRecoverySequenceChanged:
			return "source recovery is required because the frozen source bundle sequence changed before commit finalized; rebuild from snapshot with RebuildRotateSource(newSourceID)"
		}
	}
	return "source recovery is required; rebuild from snapshot with RebuildRotateSource(newSourceID)"
}

func (e *sourceSequenceOutOfOrderError) Error() string {
	if e != nil && strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	return "source bundle sequence is out of order"
}

func (e *sourceSequenceChangedError) Error() string {
	if e != nil && strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	return "source bundle sequence changed before commit"
}

func sourceRecoveryCodeFromReason(reason string) SourceRecoveryCode {
	switch SourceRecoveryCode(strings.TrimSpace(reason)) {
	case SourceRecoveryHistoryPruned, SourceRecoverySequenceOutOfOrder, SourceRecoverySequenceChanged:
		return SourceRecoveryCode(strings.TrimSpace(reason))
	default:
		return SourceRecoveryHistoryPruned
	}
}

func (c *Client) sourceRecoveryRequiredErrorLocked(ctx context.Context) (*SourceRecoveryRequiredError, error) {
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return nil, err
	}
	if operation.Kind != operationKindSourceRecovery {
		return nil, nil
	}
	return &SourceRecoveryRequiredError{Code: sourceRecoveryCodeFromReason(operation.Reason)}, nil
}

func (c *Client) markSourceRecoveryRequiredLocked(ctx context.Context, code SourceRecoveryCode, message string) error {
	_ = message
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin source recovery transaction: %w", err)
	}
	defer tx.Rollback()

	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.RebuildRequired = true
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	if err := persistOperationState(ctx, tx, &operationStateRecord{
		Kind:   operationKindSourceRecovery,
		Reason: string(code),
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit source recovery state: %w", err)
	}
	return nil
}

func (c *Client) beginSourceRecoveryLocked(ctx context.Context, code SourceRecoveryCode, message string) error {
	if err := c.markSourceRecoveryRequiredLocked(ctx, code, message); err != nil {
		return err
	}
	return &SourceRecoveryRequiredError{
		Code:    code,
		Message: message,
	}
}

func (c *Client) clearSourceRecoveryRequiredInTx(ctx context.Context, tx *sql.Tx) error {
	if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
		return err
	}
	return nil
}

func (c *Client) sourceRecoveryRequiredLocked(ctx context.Context) (bool, error) {
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return false, err
	}
	return operation.Kind == operationKindSourceRecovery, nil
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

type clearManagedTablesOptions struct {
	PreserveOutbox bool
}

func (c *Client) clearManagedTablesInTxWithOptions(ctx context.Context, tx *sql.Tx, options clearManagedTablesOptions) error {
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
	if !options.PreserveOutbox {
		if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_outbox_rows`); err != nil {
			return fmt.Errorf("failed to clear outbound push snapshot during recovery: %w", err)
		}
		if err := clearOutboxBundle(ctx, tx); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("failed to clear staged push bundle rows during recovery: %w", err)
	}
	return nil
}

func (c *Client) clearManagedTablesInTx(ctx context.Context, tx *sql.Tx) error {
	return c.clearManagedTablesInTxWithOptions(ctx, tx, clearManagedTablesOptions{})
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
