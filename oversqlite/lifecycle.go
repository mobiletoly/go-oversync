package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/internal/sessionid"
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
func (c *Client) open(ctx context.Context) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	state, err := c.openLocked(ctx)
	var pendingRemoteReplaceErr *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pendingRemoteReplaceErr) {
		return err
	}
	c.applyOpenedLifecycleState(state)
	return nil
}

func (c *Client) applyOpenedLifecycleState(state *lifecycleState) {
	c.sourceID = state.SourceID
	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) != "" {
		c.UserID = state.BindingScope
	} else {
		c.UserID = ""
	}
	c.pendingInitializationID = state.PendingInitializationID
	c.sessionConnected = false
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
	if c.sourceID == "" {
		return AttachResult{}, &OpenRequiredError{Operation: "Attach(userID)"}
	}
	if err := validateSourceID(c.sourceID); err != nil {
		return AttachResult{}, err
	}

	state, err := c.openLocked(ctx)
	var pendingRemoteReplaceErr *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pendingRemoteReplaceErr) {
		return AttachResult{}, err
	}
	c.sourceID = state.SourceID
	confirmed, err := c.ensureAdmittedContract(ctx)
	if err != nil {
		return AttachResult{}, err
	}
	if !confirmed {
		if err := c.verifyConnectLifecycleSupported(ctx); err != nil {
			return AttachResult{}, err
		}
	}

	if state.PendingTransitionKind == lifecycleTransitionRemote {
		if strings.TrimSpace(state.PendingTargetScope) != userID || state.SourceID != c.sourceID {
			return AttachResult{}, &AttachLocalStateConflictError{
				Reason: "pending remote_replace belongs to a different scope or source",
			}
		}
		restore, err := c.finalizeRemoteReplaceLocked(ctx, state)
		if err != nil {
			return AttachResult{}, err
		}
		c.pendingInitializationID = ""
		c.UserID = userID
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
		if err := c.discardFrozenOutboxForInitializeEmpty(ctx); err != nil {
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
		c.UserID = userID
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

// discardFrozenOutboxForInitializeEmpty removes only the immutable request
// snapshot created for an expired initialization lease. Dirty rows remain so
// the next direct push can freeze a new request under the fresh lifecycle
// admission state.
func (c *Client) discardFrozenOutboxForInitializeEmpty(ctx context.Context) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_outbox_rows`); err != nil {
		return fmt.Errorf("discard expired initialization outbox rows: %w", err)
	}
	if err := clearOutboxBundle(ctx, tx); err != nil {
		return fmt.Errorf("discard expired initialization outbox bundle: %w", err)
	}
	return tx.Commit(ctx)
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
	return c.detachLocked(ctx)
}

func (c *Client) detachLocked(ctx context.Context) (result DetachResult, resultErr error) {
	before, err := c.pendingSyncStatus(ctx, c.db)
	if err != nil {
		return DetachResult{}, err
	}
	if !before.BlocksDetach {
		if err = c.clearPushPlanForLifecycle(ctx, false); err != nil {
			return DetachResult{}, c.withSyncErrorProgress(ctx, err)
		}
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return DetachResult{}, fmt.Errorf("failed to begin detach transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "detach")
	status, err := c.pendingSyncStatus(ctx, tx)
	if err != nil {
		return DetachResult{}, err
	}
	if status.BlocksDetach {
		return DetachResult{
			Outcome:         DetachOutcomeBlockedUnsyncedData,
			PendingRowCount: status.PendingRowCount,
		}, nil
	}

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
	if err := persistAttachmentState(ctx, tx, attachment, true); err != nil {
		return DetachResult{}, err
	}
	if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
		return DetachResult{}, err
	}
	if identity, identityErr := loadUpgradeIdentity(ctx, tx); identityErr != nil {
		return DetachResult{}, identityErr
	} else if identity != nil {
		identity.UserID = ""
		if err := persistUpgradeIdentity(ctx, tx, *identity); err != nil {
			return DetachResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return DetachResult{}, fmt.Errorf("failed to commit detach transaction: %w", err)
	}
	c.pendingInitializationID = ""
	c.UserID = ""
	c.sessionConnected = false
	return DetachResult{Outcome: DetachOutcomeDetached, PendingRowCount: 0}, nil
}

// SyncThenDetach runs a bounded best-effort Sync followed by Detach.
func (c *Client) SyncThenDetach(ctx context.Context) (SyncThenDetachResult, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return SyncThenDetachResult{}, err
	}
	defer c.writeMu.Unlock()
	if err := c.ensureConnectedSessionLocked(ctx, "SyncThenDetach()"); err != nil {
		return SyncThenDetachResult{}, err
	}
	if err := c.validateProtocolGateLocked(ctx, "sync_then_detach_discovery"); err != nil {
		return SyncThenDetachResult{}, err
	}

	ctx = context.WithValue(ctx, syncInvocationKey{}, &syncInvocation{})
	var lastSync *SyncReport
	blocked := func(round int, reason DetachBlockedReason) (SyncThenDetachResult, error) {
		pending, err := c.pendingSyncStatusLocked(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		return SyncThenDetachResult{
			LastSync:   lastSync,
			Detach:     DetachResult{Outcome: DetachOutcomeBlockedUnsyncedData, PendingRowCount: pending.PendingRowCount},
			SyncRounds: round, RemainingPendingRowCount: pending.PendingRowCount, BlockedReason: reason,
		}, nil
	}
	for round := 1; round <= 3; round++ {
		if err := ctx.Err(); err != nil {
			return SyncThenDetachResult{}, err
		}
		before, err := c.pendingWorkIdentity(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		syncReport, err := c.syncLocked(ctx)
		if err != nil {
			var incomplete *SyncIncompleteError
			if errors.As(err, &incomplete) {
				return blocked(round, DetachBlockedReason(incomplete.Reason))
			}
			return SyncThenDetachResult{}, err
		}
		if syncReport.RemoteOutcome == RemoteSyncOutcomeSkippedPaused {
			return blocked(round, DetachBlockedDownloadsPaused)
		}
		lastSync = &syncReport
		if err := ctx.Err(); err != nil {
			return SyncThenDetachResult{}, err
		}
		detachResult, err := c.detachLocked(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		if detachResult.Outcome == DetachOutcomeDetached {
			return SyncThenDetachResult{
				LastSync:                 lastSync,
				Detach:                   detachResult,
				SyncRounds:               round,
				RemainingPendingRowCount: 0,
			}, nil
		}
		after, err := c.pendingWorkIdentity(ctx)
		if err != nil {
			return SyncThenDetachResult{}, err
		}
		if before == after {
			return blocked(round, DetachBlockedNoProgress)
		}
	}
	return blocked(3, DetachBlockedSyncRoundLimit)
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

	if c.sourceID == "" {
		return SourceInfo{}, &OpenRequiredError{Operation: "SourceInfo()"}
	}
	if err := validateSourceID(c.sourceID); err != nil {
		return SourceInfo{}, err
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return SourceInfo{}, err
	}
	operation, err := loadOperationState(ctx, c.db)
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
	return c.pendingSyncStatus(ctx, c.db)
}

func (c *Client) pendingSyncStatus(ctx context.Context, q sqliteTransaction) (PendingSyncStatus, error) {
	var total int64
	if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM _sync_dirty_rows)+(SELECT count(*) FROM _sync_outbox_rows)`).Scan(&total); err != nil {
		return PendingSyncStatus{}, err
	}
	state, err := loadAttachmentState(ctx, q)
	if err != nil {
		return PendingSyncStatus{}, err
	}
	if state.PendingInitializationID != "" {
		total++
	}
	if state.BindingState == attachmentBindingAttached && total == 0 {
		table, err := unresolvedWorkTable(ctx, q)
		if err != nil {
			return PendingSyncStatus{}, err
		}
		if table != "" {
			total++
		}
	}
	return PendingSyncStatus{HasPendingSyncData: total > 0, PendingRowCount: total, BlocksDetach: state.BindingState == attachmentBindingAttached && total > 0}, nil
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
func (c *Client) UninstallSync(ctx context.Context) (resultErr error) {
	if c == nil || atomic.LoadUint32(&c.closed) == 1 {
		return &ClientClosedError{}
	}
	if !atomic.CompareAndSwapUint32(&c.loopState, clientLoopStateLive, clientLoopStateUninstalling) {
		if atomic.LoadUint32(&c.loopState) == clientLoopStateTerminal {
			return &ClientClosedError{}
		}
		return fmt.Errorf("oversqlite sync uninstall is already in progress")
	}
	committed := false
	defer func() {
		if !committed {
			atomic.CompareAndSwapUint32(&c.loopState, clientLoopStateUninstalling, clientLoopStateLive)
		}
	}()
	if err := c.Stop(ctx); err != nil {
		return err
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin uninstall transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "uninstall")
	if err := c.requireNoUnresolvedWork(ctx, tx); err != nil {
		return err
	}

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
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit uninstall transaction: %w", err)
	}

	c.sourceID = ""
	c.UserID = ""
	c.pendingInitializationID = ""
	c.sessionConnected = false
	atomic.StoreUint32(&c.closed, 1)
	atomic.StoreUint32(&c.loopState, clientLoopStateTerminal)
	committed = true
	if !c.ownsDB {
		return nil
	}
	closeErr := c.db.Close()
	releaseClientDBOwnership(c.dbOwnershipKey)
	c.dbOwnershipKey = ""
	return closeErr
}

func (c *Client) loadManagedSyncTablesForUninstall(ctx context.Context, tx sqliteTransaction) ([]string, error) {
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

func sqliteTableExists(ctx context.Context, tx queryRower, tableName string) (bool, error) {
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

func dropManagedTriggersForTableInTx(ctx context.Context, tx sqliteTransaction, tableName string) error {
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

	result, err := c.doAuthenticatedBoundedRequestWithRetry(
		ctx, "connect_request", http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/sync/connect",
		reqBody, "application/json", snapshotControlBodyLimit, snapshotControlBodyLimit, nil, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send connect request: %w", err)
	}
	body, statusCode := result.body, result.statusCode
	if statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed || statusCode == http.StatusNotImplemented {
		return nil, &AttachLifecycleUnsupportedError{Reason: "missing /sync/connect endpoint"}
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", statusCode, decodeServerErrorBody(body))
	}
	if _, err := requireJSONObjectFieldsWithOptional(body, "connect", []string{"resolution"}, []string{"initialization_id", "lease_expires_at", "retry_after_seconds"}); err != nil {
		return nil, err
	}

	var connectResp oversync.ConnectResponse
	if err := json.Unmarshal(body, &connectResp); err != nil {
		return nil, fmt.Errorf("failed to decode connect response: %w", err)
	}
	if err := validateConnectResponse(&connectResp); err != nil {
		return nil, err
	}
	return &connectResp, nil
}

func validateConnectResponse(resp *oversync.ConnectResponse) error {
	if resp == nil {
		return fmt.Errorf("connect response is invalid")
	}
	switch resp.Resolution {
	case "initialize_local":
		if err := sessionid.Validate(resp.InitializationID); err != nil {
			return fmt.Errorf("connect response initialization_id is invalid")
		}
		if _, err := time.Parse(time.RFC3339Nano, resp.LeaseExpiresAt); err != nil || resp.RetryAfterSec != 0 {
			return fmt.Errorf("connect response initialization lease is invalid")
		}
		return nil
	case "retry_later":
		if resp.InitializationID != "" || resp.RetryAfterSec < 1 {
			return fmt.Errorf("connect response retry state is invalid")
		}
		if _, err := time.Parse(time.RFC3339Nano, resp.LeaseExpiresAt); err != nil {
			return fmt.Errorf("connect response retry lease is invalid")
		}
		return nil
	case "remote_authoritative", "initialize_empty":
		if resp.InitializationID != "" || resp.LeaseExpiresAt != "" || resp.RetryAfterSec != 0 {
			return fmt.Errorf("connect response contains fields invalid for %s", resp.Resolution)
		}
		return nil
	default:
		return fmt.Errorf("connect response resolution is invalid")
	}
}

func (c *Client) persistConnectedLifecycleState(ctx context.Context, userID, initializationID string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.CurrentSourceID = c.sourceID
	attachment.BindingState = attachmentBindingAttached
	attachment.AttachedUserID = userID
	attachment.PendingInitializationID = initializationID
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.schema
	}
	if err := ensureSourceState(ctx, tx, c.sourceID); err != nil {
		return err
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
		return err
	}
	if identity, identityErr := loadUpgradeIdentity(ctx, tx); identityErr != nil {
		return identityErr
	} else if identity != nil {
		if identity.UserID != "" && identity.UserID != userID {
			return fmt.Errorf("installed upgrade identity belongs to another user")
		}
		identity.UserID = userID
		if err := persistUpgradeIdentity(ctx, tx, *identity); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *Client) beginRemoteReplaceLocked(ctx context.Context, userID, sourceID string) (resultErr error) {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin remote_replace guard transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "remote_replace guard transaction")

	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil {
		return err
	}
	state := toLifecycleState(attachment, operation)
	state.SourceID = sourceID
	state.BindingState = lifecycleBindingAnonymous
	state.BindingScope = ""
	state.PendingTransitionKind = lifecycleTransitionRemote
	state.PendingTargetScope = userID
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	state.PendingInitializationID = ""
	var outboxRows int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows); err != nil {
		return fmt.Errorf("failed to count outbox rows before remote_replace: %w", err)
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if outbox.State != outboxStateNone || outboxRows != 0 {
		return &PendingPushReplayError{OutboundCount: int(outboxRows)}
	}
	if err := c.persistLifecycleStateInTx(ctx, tx, state); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_dirty_rows`); err != nil {
		return fmt.Errorf("failed to discard pre-attach anonymous dirty rows for remote_replace: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit remote_replace guard transaction: %w", err)
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
	limits, err := c.negotiateSnapshotLimits(ctx)
	if err != nil {
		return nil, err
	}
	options := snapshotApplyOptions{
		FinalizeRemoteReplace: true,
		RemoteTargetUserID:    state.PendingTargetScope,
	}
	guard, err := c.pinSnapshotApplyGuard(ctx, options)
	if err != nil {
		return nil, err
	}
	options.PinnedGuard = guard
	if err := c.clearSnapshotStage(ctx); err != nil {
		return nil, err
	}
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return nil, err
	}

	startedAt := time.Now()
	session, err := c.createSnapshotSession(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer c.deleteSnapshotSessionBestEffort(session.SnapshotID)

	state.PendingStagedSnapshotID = session.SnapshotID
	state.PendingSnapshotBundleSeq = session.SnapshotBundleSeq
	state.PendingSnapshotRowCount = session.RowCount
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return nil, err
	}
	transfer, err := c.downloadSnapshotSession(ctx, session, limits)
	if err != nil {
		return nil, err
	}
	if c.beforeSnapshotApplyHook != nil {
		if err := c.beforeSnapshotApplyHook(ctx); err != nil {
			return nil, err
		}
	}
	if err := c.applyStagedSnapshotLocked(ctx, session, options); err != nil {
		return nil, err
	}
	if c.snapshotObserver != nil {
		c.snapshotObserver(snapshotRestoreObservation{
			StagedRows: transfer.rows, DeclaredWireBytes: transfer.bytes,
			AppliedRows: session.RowCount, Duration: time.Since(startedAt),
		})
	}
	return &RestoreSummary{
		BundleSeq: session.SnapshotBundleSeq,
		RowCount:  session.RowCount,
	}, nil
}

func (c *Client) countSnapshotStageRows(ctx context.Context, snapshotID string) (int64, error) {
	var count int64
	if err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM _sync_snapshot_stage
		WHERE snapshot_id = ?
	`, snapshotID).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count staged snapshot rows: %w", err)
	}
	return count, nil
}
