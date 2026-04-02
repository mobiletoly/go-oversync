package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

// AttachStatus describes the high-level result of Attach.
type AttachStatus string

const (
	AttachStatusConnected  AttachStatus = "connected"
	AttachStatusRetryLater AttachStatus = "retry_later"
)

// AttachOutcome describes how Attach reached a connected state.
type AttachOutcome string

const (
	AttachOutcomeResumedAttached AttachOutcome = "resumed_attached_state"
	AttachOutcomeUsedRemote      AttachOutcome = "used_remote_state"
	AttachOutcomeSeededLocal     AttachOutcome = "seeded_from_local"
	AttachOutcomeStartedEmpty    AttachOutcome = "started_empty"
)

// AttachResult reports the lifecycle result of Attach.
type AttachResult struct {
	Status     AttachStatus
	Outcome    AttachOutcome
	RetryAfter time.Duration
	SyncStatus SyncStatus
	Restore    *RestoreSummary
}

// PendingSyncStatus summarizes whether local durable sync state exists and whether it blocks Detach.
type PendingSyncStatus struct {
	HasPendingSyncData bool
	PendingRowCount    int64
	BlocksDetach       bool
}

// Open restores local lifecycle state and bootstraps internal source identity if needed.
func (c *Client) Open(ctx context.Context) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	state, err := c.openLocked(ctx)
	var pendingRemoteReplaceErr *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pendingRemoteReplaceErr) {
		return err
	}
	c.sourceID = state.SourceID
	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) != "" {
		c.UserID = state.BindingScope
	} else {
		c.UserID = ""
	}
	c.pendingInitializationID = state.PendingInitializationID
	c.sessionConnected = false
	return nil
}

// Attach resolves account attachment through the server connect lifecycle.
func (c *Client) Attach(ctx context.Context, userID string) (AttachResult, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return AttachResult{}, err
	}
	defer c.writeMu.Unlock()
	return c.connectLocked(ctx, userID)
}

func (c *Client) connectLocked(ctx context.Context, userID string) (AttachResult, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return AttachResult{}, fmt.Errorf("userID must be provided")
	}
	if strings.TrimSpace(c.sourceID) == "" {
		return AttachResult{}, &OpenRequiredError{Operation: "Attach(userID)"}
	}

	state, err := c.openLocked(ctx)
	var pendingRemoteReplaceErr *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pendingRemoteReplaceErr) {
		return AttachResult{}, err
	}
	c.sourceID = state.SourceID

	if state.PendingTransitionKind == lifecycleTransitionRemote {
		if strings.TrimSpace(state.PendingTargetScope) != userID || strings.TrimSpace(state.SourceID) != c.sourceID {
			return AttachResult{}, &AttachLocalStateConflictError{
				Reason: fmt.Sprintf("pending remote_replace belongs to scope %q on source %q", state.PendingTargetScope, state.SourceID),
			}
		}
		c.UserID = userID
		restore, err := c.finalizeRemoteReplaceLocked(ctx, state)
		if err != nil {
			return AttachResult{}, err
		}
		c.pendingInitializationID = ""
		if err := c.persistConnectedLifecycleState(ctx, userID, ""); err != nil {
			return AttachResult{}, err
		}
		c.sessionConnected = true
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return AttachResult{}, err
		}
		return AttachResult{
			Status:     AttachStatusConnected,
			Outcome:    AttachOutcomeUsedRemote,
			SyncStatus: status,
			Restore:    restore,
		}, nil
	}

	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) == userID && state.PendingTransitionKind == lifecycleTransitionNone {
		hasAttachedState, err := c.hasAttachedClientStateLocked(ctx, userID, c.sourceID)
		if err != nil {
			return AttachResult{}, err
		}
		if hasAttachedState {
			c.UserID = userID
			c.pendingInitializationID = state.PendingInitializationID
			c.sessionConnected = true
			status, err := c.syncStatusLocked(ctx)
			if err != nil {
				return AttachResult{}, err
			}
			return AttachResult{
				Status:     AttachStatusConnected,
				Outcome:    AttachOutcomeResumedAttached,
				SyncStatus: status,
			}, nil
		}
	}
	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) != "" && strings.TrimSpace(state.BindingScope) != userID {
		return AttachResult{}, &AttachBindingConflictError{
			AttachedUserID:  state.BindingScope,
			RequestedUserID: userID,
		}
	}
	if err := c.verifyConnectLifecycleSupported(ctx); err != nil {
		return AttachResult{}, err
	}

	hasPendingRows, err := c.pendingChangeCount(ctx)
	if err != nil {
		return AttachResult{}, err
	}
	resp, err := c.connectRequest(ctx, userID, c.sourceID, hasPendingRows > 0)
	if err != nil {
		return AttachResult{}, err
	}

	switch resp.Resolution {
	case "retry_later":
		retryAfter := time.Duration(resp.RetryAfterSec) * time.Second
		c.sessionConnected = false
		return AttachResult{Status: AttachStatusRetryLater, RetryAfter: retryAfter}, nil
	case "initialize_empty":
		if err := c.ensureAttachedClientStateLocked(ctx, userID, c.sourceID); err != nil {
			return AttachResult{}, err
		}
		c.pendingInitializationID = ""
		if err := c.persistConnectedLifecycleState(ctx, userID, ""); err != nil {
			return AttachResult{}, err
		}
		c.UserID = userID
		c.sessionConnected = true
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return AttachResult{}, err
		}
		return AttachResult{
			Status:     AttachStatusConnected,
			Outcome:    AttachOutcomeStartedEmpty,
			SyncStatus: status,
		}, nil
	case "initialize_local":
		if err := c.ensureAttachedClientStateLocked(ctx, userID, c.sourceID); err != nil {
			return AttachResult{}, err
		}
		c.pendingInitializationID = resp.InitializationID
		if err := c.persistConnectedLifecycleState(ctx, userID, resp.InitializationID); err != nil {
			return AttachResult{}, err
		}
		c.UserID = userID
		c.sessionConnected = true
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return AttachResult{}, err
		}
		return AttachResult{
			Status:     AttachStatusConnected,
			Outcome:    AttachOutcomeSeededLocal,
			SyncStatus: status,
		}, nil
	case "remote_authoritative":
		c.UserID = userID
		if err := c.beginRemoteReplaceLocked(ctx, userID, c.sourceID); err != nil {
			return AttachResult{}, err
		}
		state, err = c.loadLifecycleState(ctx)
		if err != nil {
			return AttachResult{}, err
		}
		restore, err := c.finalizeRemoteReplaceLocked(ctx, state)
		if err != nil {
			return AttachResult{}, err
		}
		c.pendingInitializationID = ""
		if err := c.persistConnectedLifecycleState(ctx, userID, ""); err != nil {
			return AttachResult{}, err
		}
		c.sessionConnected = true
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return AttachResult{}, err
		}
		return AttachResult{
			Status:     AttachStatusConnected,
			Outcome:    AttachOutcomeUsedRemote,
			SyncStatus: status,
			Restore:    restore,
		}, nil
	default:
		return AttachResult{}, fmt.Errorf("unexpected connect resolution %q", resp.Resolution)
	}
}

func (c *Client) clearStalePendingInitializationStateLocked(ctx context.Context) error {
	state, err := c.loadLifecycleState(ctx)
	if err != nil {
		return err
	}
	state.BindingState = lifecycleBindingAnonymous
	state.BindingScope = ""
	state.PendingTransitionKind = lifecycleTransitionNone
	state.PendingTargetScope = ""
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	state.PendingInitializationID = ""
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return err
	}
	c.pendingInitializationID = ""
	c.UserID = ""
	c.sessionConnected = false
	return nil
}

// Detach clears attached synced state after verifying that no attached durable sync state remains.
func (c *Client) Detach(ctx context.Context) (DetachResult, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return DetachResult{}, err
	}
	defer c.writeMu.Unlock()

	status, err := c.pendingSyncStatusLocked(ctx)
	if err != nil {
		return DetachResult{}, err
	}
	if status.BlocksDetach {
		return DetachResult{
			Outcome:         DetachOutcomeBlockedUnsyncedData,
			PendingRowCount: status.PendingRowCount,
		}, nil
	}
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return DetachResult{}, fmt.Errorf("failed to begin detach transaction: %w", err)
	}
	defer tx.Rollback()

	if err := c.clearFullLocalSyncStateInTx(ctx, tx); err != nil {
		return DetachResult{}, err
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return DetachResult{}, err
	}
	attachment.BindingState = attachmentBindingAnonymous
	attachment.AttachedUserID = ""
	attachment.PendingInitializationID = ""
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return DetachResult{}, err
	}
	if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
		return DetachResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DetachResult{}, fmt.Errorf("failed to commit detach transaction: %w", err)
	}
	c.pendingInitializationID = ""
	c.UserID = ""
	c.sessionConnected = false
	return DetachResult{Outcome: DetachOutcomeDetached, PendingRowCount: 0}, nil
}

// SyncThenDetach runs a bounded best-effort Sync followed by Detach.
func (c *Client) SyncThenDetach(ctx context.Context) (SyncThenDetachResult, error) {
	const maxAttempts = 3
	var (
		previousPending int64 = 1<<63 - 1
		lastSync        SyncReport
	)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		syncReport, err := c.Sync(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		lastSync = syncReport
		detachResult, err := c.Detach(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		if detachResult.Outcome == DetachOutcomeDetached {
			return SyncThenDetachResult{
				LastSync:                 syncReport,
				Detach:                   detachResult,
				SyncRounds:               attempt + 1,
				RemainingPendingRowCount: 0,
			}, nil
		}
		pending, err := c.PendingSyncStatus(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		if pending.PendingRowCount >= previousPending {
			return SyncThenDetachResult{
				LastSync:                 syncReport,
				Detach:                   detachResult,
				SyncRounds:               attempt + 1,
				RemainingPendingRowCount: pending.PendingRowCount,
			}, nil
		}
		previousPending = pending.PendingRowCount
	}
	pending, err := c.PendingSyncStatus(ctx)
	if err != nil {
		return SyncThenDetachResult{}, err
	}
	return SyncThenDetachResult{
		LastSync: lastSync,
		Detach: DetachResult{
			Outcome:         DetachOutcomeBlockedUnsyncedData,
			PendingRowCount: pending.PendingRowCount,
		},
		SyncRounds:               maxAttempts,
		RemainingPendingRowCount: pending.PendingRowCount,
	}, nil
}

// PendingSyncStatus reports pending local sync state and whether it blocks Detach.
func (c *Client) PendingSyncStatus(ctx context.Context) (PendingSyncStatus, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return PendingSyncStatus{}, err
	}
	defer c.writeMu.Unlock()
	return c.pendingSyncStatusLocked(ctx)
}

// SourceInfo exposes debug-only source diagnostics from persisted local state.
func (c *Client) SourceInfo(ctx context.Context) (SourceInfo, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return SourceInfo{}, err
	}
	defer c.writeMu.Unlock()

	if strings.TrimSpace(c.sourceID) == "" {
		return SourceInfo{}, &OpenRequiredError{Operation: "SourceInfo()"}
	}
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return SourceInfo{}, err
	}
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return SourceInfo{}, err
	}

	info := SourceInfo{
		CurrentSourceID: attachment.CurrentSourceID,
		RebuildRequired: attachment.RebuildRequired,
	}
	if operation.Kind == operationKindSourceRecovery {
		info.SourceRecoveryRequired = true
		info.SourceRecoveryReason = sourceRecoveryCodeFromReason(operation.Reason)
	}
	return info, nil
}

func (c *Client) pendingSyncStatusLocked(ctx context.Context) (PendingSyncStatus, error) {
	dirtyCount, err := c.pendingChangeCount(ctx)
	if err != nil {
		return PendingSyncStatus{}, err
	}
	outboundCount, err := c.pendingPushOutboundCount(ctx)
	if err != nil {
		return PendingSyncStatus{}, err
	}
	total := dirtyCount + outboundCount
	state, err := c.loadLifecycleState(ctx)
	if err != nil {
		return PendingSyncStatus{}, err
	}
	if strings.TrimSpace(state.PendingInitializationID) != "" {
		total++
	}
	return PendingSyncStatus{
		HasPendingSyncData: total > 0,
		PendingRowCount:    int64(total),
		BlocksDetach:       state.BindingState == lifecycleBindingAttached && total > 0,
	}, nil
}

// SyncStatus reports authority state, pending state, and the last durable bundle checkpoint for the currently connected scope.
func (c *Client) SyncStatus(ctx context.Context) (SyncStatus, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return SyncStatus{}, err
	}
	defer c.writeMu.Unlock()
	return c.syncStatusLocked(ctx)
}

// UninstallSync removes oversqlite-owned metadata tables and managed-table triggers from the database.
func (c *Client) UninstallSync(ctx context.Context) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin uninstall transaction: %w", err)
	}
	defer tx.Rollback()

	managedTables, err := c.loadManagedSyncTablesForUninstall(ctx, tx)
	if err != nil {
		return err
	}
	for _, tableName := range managedTables {
		if err := dropManagedTriggersForTableInTx(ctx, tx, tableName); err != nil {
			return err
		}
	}
	for _, tableName := range syncMetadataTableNames {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(tableName))); err != nil {
			return fmt.Errorf("failed to drop sync metadata table %s: %w", tableName, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit uninstall transaction: %w", err)
	}

	c.sourceID = ""
	c.UserID = ""
	c.pendingInitializationID = ""
	c.sessionConnected = false
	c.markClosedAndReleaseOwnershipLocked()
	return nil
}

func (c *Client) loadManagedSyncTablesForUninstall(ctx context.Context, tx *sql.Tx) ([]string, error) {
	tableNames := make([]string, 0, len(c.config.Tables))
	seen := make(map[string]struct{}, len(c.config.Tables))

	hasRegistry, err := sqliteTableExists(ctx, tx, "_sync_managed_tables")
	if err != nil {
		return nil, fmt.Errorf("failed to inspect sync managed-table registry: %w", err)
	}
	if hasRegistry {
		rows, err := tx.QueryContext(ctx, `SELECT table_name FROM _sync_managed_tables ORDER BY table_name`)
		if err != nil {
			return nil, fmt.Errorf("failed to load sync managed-table registry: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return nil, fmt.Errorf("failed to scan sync managed-table registry: %w", err)
			}
			tableName = strings.ToLower(strings.TrimSpace(tableName))
			if tableName == "" {
				continue
			}
			if _, exists := seen[tableName]; exists {
				continue
			}
			seen[tableName] = struct{}{}
			tableNames = append(tableNames, tableName)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to iterate sync managed-table registry: %w", err)
		}
	}

	for _, syncTable := range c.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			continue
		}
		if _, exists := seen[tableName]; exists {
			continue
		}
		seen[tableName] = struct{}{}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

func sqliteTableExists(ctx context.Context, tx *sql.Tx, tableName string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE type = 'table' AND name = ?
	`, tableName).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func dropManagedTriggersForTableInTx(ctx context.Context, tx *sql.Tx, tableName string) error {
	tableName = strings.ToLower(strings.TrimSpace(tableName))
	if tableName == "" {
		return nil
	}
	for _, suffix := range []string{"ai", "au", "ad"} {
		triggerName := fmt.Sprintf("trg_%s_%s", tableName, suffix)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(triggerName))); err != nil {
			return fmt.Errorf("failed to drop sync trigger %s: %w", triggerName, err)
		}
	}
	for _, suffix := range []string{"bi_guard", "bu_guard", "bd_guard"} {
		triggerName := fmt.Sprintf("trg_%s_%s", tableName, suffix)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(triggerName))); err != nil {
			return fmt.Errorf("failed to drop sync trigger %s: %w", triggerName, err)
		}
	}
	return nil
}

func (c *Client) connectRequest(ctx context.Context, userID, sourceID string, hasLocalPendingRows bool) (*oversync.ConnectResponse, error) {
	reqBody, err := json.Marshal(&oversync.ConnectRequest{
		HasLocalPendingRows: hasLocalPendingRows,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal connect request: %w", err)
	}

	body, statusCode, err := c.doAuthenticatedRequestWithRetry(ctx, "connect_request", http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/sync/connect", reqBody, "application/json")
	if err != nil {
		return nil, fmt.Errorf("failed to send connect request: %w", err)
	}
	if statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed || statusCode == http.StatusNotImplemented {
		return nil, &AttachLifecycleUnsupportedError{Reason: "missing /sync/connect endpoint"}
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", statusCode, decodeServerErrorBody(body))
	}

	var connectResp oversync.ConnectResponse
	if err := json.Unmarshal(body, &connectResp); err != nil {
		return nil, fmt.Errorf("failed to decode connect response: %w", err)
	}
	return &connectResp, nil
}

func (c *Client) persistConnectedLifecycleState(ctx context.Context, userID, initializationID string) error {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	attachment.CurrentSourceID = c.sourceID
	attachment.BindingState = attachmentBindingAttached
	attachment.AttachedUserID = userID
	attachment.PendingInitializationID = initializationID
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.Schema
	}
	if err := ensureSourceState(ctx, c.DB, c.sourceID); err != nil {
		return err
	}
	if err := persistAttachmentState(ctx, c.DB, attachment); err != nil {
		return err
	}
	return persistOperationState(ctx, c.DB, &operationStateRecord{Kind: operationKindNone})
}

func (c *Client) beginRemoteReplaceLocked(ctx context.Context, userID, sourceID string) error {
	state, err := c.loadLifecycleState(ctx)
	if err != nil {
		return err
	}
	state.BindingState = lifecycleBindingAnonymous
	state.BindingScope = ""
	state.PendingTransitionKind = lifecycleTransitionRemote
	state.PendingTargetScope = userID
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	state.PendingInitializationID = ""
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return err
	}
	return nil
}

func (c *Client) finalizeRemoteReplaceLocked(ctx context.Context, state *lifecycleState) (*RestoreSummary, error) {
	if state == nil {
		return nil, fmt.Errorf("remote_replace lifecycle state is required")
	}
	if state.PendingTransitionKind != lifecycleTransitionRemote {
		return nil, fmt.Errorf("cannot finalize lifecycle transition %q as remote_replace", state.PendingTransitionKind)
	}
	refreshedState, err := c.ensureRemoteReplaceSnapshotStagedLocked(ctx, state)
	if err != nil {
		return nil, err
	}
	if err := c.prepareAttachedClientStateForRemoteReplace(ctx, refreshedState.PendingTargetScope, refreshedState.SourceID); err != nil {
		return nil, err
	}
	session := &oversync.SnapshotSession{
		SnapshotID:        refreshedState.PendingStagedSnapshotID,
		SnapshotBundleSeq: refreshedState.PendingSnapshotBundleSeq,
		RowCount:          refreshedState.PendingSnapshotRowCount,
	}
	if err := c.applyStagedSnapshotLocked(ctx, session, snapshotApplyOptions{}); err != nil {
		return nil, err
	}
	return &RestoreSummary{
		BundleSeq: session.SnapshotBundleSeq,
		RowCount:  session.RowCount,
	}, nil
}

func (c *Client) ensureRemoteReplaceSnapshotStagedLocked(ctx context.Context, state *lifecycleState) (*lifecycleState, error) {
	if state == nil {
		return nil, fmt.Errorf("remote_replace lifecycle state is required")
	}
	if strings.TrimSpace(state.PendingStagedSnapshotID) != "" {
		stagedCount, err := c.countSnapshotStageRows(ctx, state.PendingStagedSnapshotID)
		if err != nil {
			return nil, err
		}
		if stagedCount == state.PendingSnapshotRowCount {
			return state, nil
		}
		if err := c.clearSnapshotStage(ctx); err != nil {
			return nil, err
		}
		state.PendingStagedSnapshotID = ""
		state.PendingSnapshotBundleSeq = 0
		state.PendingSnapshotRowCount = 0
		if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
			return nil, err
		}
	}

	session, err := c.createSnapshotSession(ctx)
	if err != nil {
		return nil, err
	}
	defer c.deleteSnapshotSessionBestEffort(context.Background(), session.SnapshotID)

	if err := c.clearSnapshotStage(ctx); err != nil {
		return nil, err
	}
	state.PendingStagedSnapshotID = session.SnapshotID
	state.PendingSnapshotBundleSeq = session.SnapshotBundleSeq
	state.PendingSnapshotRowCount = session.RowCount
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return nil, err
	}

	afterRowOrdinal := int64(0)
	for {
		chunk, err := c.fetchSnapshotChunk(ctx, session.SnapshotID, session.SnapshotBundleSeq, afterRowOrdinal, c.snapshotChunkRows())
		if err != nil {
			return nil, err
		}
		if err := c.stageSnapshotChunk(ctx, chunk, afterRowOrdinal); err != nil {
			return nil, err
		}
		if !chunk.HasMore {
			break
		}
		afterRowOrdinal = chunk.NextRowOrdinal
	}
	return state, nil
}

func (c *Client) prepareAttachedClientStateForRemoteReplace(ctx context.Context, userID, sourceID string) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin remote_replace attach transaction: %w", err)
	}
	defer tx.Rollback()

	if err := ensureSourceState(ctx, tx, sourceID); err != nil {
		return err
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.CurrentSourceID = sourceID
	attachment.BindingState = attachmentBindingAttached
	attachment.AttachedUserID = userID
	attachment.SchemaName = c.config.Schema
	attachment.LastBundleSeqSeen = 0
	attachment.RebuildRequired = false
	attachment.PendingInitializationID = ""
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	if err := persistOperationState(ctx, tx, &operationStateRecord{
		Kind:              operationKindRemoteReplace,
		TargetUserID:      userID,
		StagedSnapshotID:  "",
		SnapshotBundleSeq: 0,
		SnapshotRowCount:  0,
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit remote_replace attach transaction: %w", err)
	}
	return nil
}

func (c *Client) countSnapshotStageRows(ctx context.Context, snapshotID string) (int64, error) {
	var count int64
	if err := c.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM _sync_snapshot_stage
		WHERE snapshot_id = ?
	`, snapshotID).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count staged snapshot rows: %w", err)
	}
	return count, nil
}

func (c *Client) armDestructiveTransition(ctx context.Context, transitionKind, pendingTargetSourceID string) error {
	return nil
}
