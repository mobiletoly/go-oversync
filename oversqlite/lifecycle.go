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

type ConnectStatus string

const (
	ConnectStatusConnected  ConnectStatus = "connected"
	ConnectStatusRetryLater ConnectStatus = "retry_later"
)

type ConnectOutcome string

const (
	ConnectOutcomeResumedAttached ConnectOutcome = "resumed_attached_state"
	ConnectOutcomeUsedRemote      ConnectOutcome = "used_remote_state"
	ConnectOutcomeSeededLocal     ConnectOutcome = "seeded_from_local"
	ConnectOutcomeStartedEmpty    ConnectOutcome = "started_empty"
)

type ConnectResult struct {
	Status     ConnectStatus
	Outcome    ConnectOutcome
	RetryAfter time.Duration
}

type PendingSyncStatus struct {
	HasPendingSyncData bool
	PendingRowCount    int
	BlocksSignOut      bool
}

type SignOutBlockedError struct {
	PendingRowCount int
}

func (e *SignOutBlockedError) Error() string {
	return fmt.Sprintf("cannot sign out while %d attached sync rows are pending upload", e.PendingRowCount)
}

func (c *Client) Open(ctx context.Context, sourceID string) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return fmt.Errorf("sourceID must be provided")
	}
	state, err := c.openLocked(ctx, sourceID)
	var pendingRemoteReplaceErr *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pendingRemoteReplaceErr) {
		return err
	}
	c.SourceID = state.SourceID
	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) != "" {
		c.UserID = state.BindingScope
	} else {
		c.UserID = ""
	}
	c.pendingInitializationID = state.PendingInitializationID
	return err
}

func (c *Client) Connect(ctx context.Context, userID string) (ConnectResult, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return ConnectResult{}, err
	}
	defer c.writeMu.Unlock()
	return c.connectLocked(ctx, userID)
}

func (c *Client) connectLocked(ctx context.Context, userID string) (ConnectResult, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return ConnectResult{}, fmt.Errorf("userID must be provided")
	}
	if strings.TrimSpace(c.SourceID) == "" {
		return ConnectResult{}, &OpenRequiredError{Operation: "Connect(userID)"}
	}

	state, err := c.openLocked(ctx, c.SourceID)
	var pendingRemoteReplaceErr *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pendingRemoteReplaceErr) {
		return ConnectResult{}, err
	}
	c.SourceID = state.SourceID

	if state.PendingTransitionKind == lifecycleTransitionRemote {
		if strings.TrimSpace(state.PendingTargetScope) != userID || strings.TrimSpace(state.PendingTargetSourceID) != c.SourceID {
			return ConnectResult{}, &ConnectLocalStateConflictError{
				Reason: fmt.Sprintf("pending remote_replace belongs to scope %q on source %q", state.PendingTargetScope, state.PendingTargetSourceID),
			}
		}
		c.UserID = userID
		if err := c.finalizeRemoteReplaceLocked(ctx, state); err != nil {
			return ConnectResult{}, err
		}
		c.pendingInitializationID = ""
		if err := c.persistConnectedLifecycleState(ctx, userID, ""); err != nil {
			return ConnectResult{}, err
		}
		c.sessionConnected = true
		return ConnectResult{Status: ConnectStatusConnected, Outcome: ConnectOutcomeUsedRemote}, nil
	}

	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) == userID && state.PendingTransitionKind == lifecycleTransitionNone {
		hasAttachedState, err := c.hasAttachedClientStateLocked(ctx, userID, c.SourceID)
		if err != nil {
			return ConnectResult{}, err
		}
		if hasAttachedState {
			c.UserID = userID
			c.pendingInitializationID = state.PendingInitializationID
			c.sessionConnected = true
			return ConnectResult{Status: ConnectStatusConnected, Outcome: ConnectOutcomeResumedAttached}, nil
		}
	}
	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) != "" && strings.TrimSpace(state.BindingScope) != userID {
		return ConnectResult{}, &ConnectBindingConflictError{
			AttachedUserID:  state.BindingScope,
			RequestedUserID: userID,
		}
	}
	if err := c.verifyConnectLifecycleSupported(ctx); err != nil {
		return ConnectResult{}, err
	}

	hasPendingRows, err := c.pendingChangeCount(ctx)
	if err != nil {
		return ConnectResult{}, err
	}
	resp, err := c.connectRequest(ctx, userID, c.SourceID, hasPendingRows > 0)
	if err != nil {
		return ConnectResult{}, err
	}

	switch resp.Resolution {
	case "retry_later":
		retryAfter := time.Duration(resp.RetryAfterSec) * time.Second
		c.sessionConnected = false
		return ConnectResult{Status: ConnectStatusRetryLater, RetryAfter: retryAfter}, nil
	case "initialize_empty":
		if err := c.ensureAttachedClientStateLocked(ctx, userID, c.SourceID); err != nil {
			return ConnectResult{}, err
		}
		c.pendingInitializationID = ""
		if err := c.persistConnectedLifecycleState(ctx, userID, ""); err != nil {
			return ConnectResult{}, err
		}
		c.UserID = userID
		c.sessionConnected = true
		return ConnectResult{Status: ConnectStatusConnected, Outcome: ConnectOutcomeStartedEmpty}, nil
	case "initialize_local":
		if err := c.ensureAttachedClientStateLocked(ctx, userID, c.SourceID); err != nil {
			return ConnectResult{}, err
		}
		c.pendingInitializationID = resp.InitializationID
		if err := c.persistConnectedLifecycleState(ctx, userID, resp.InitializationID); err != nil {
			return ConnectResult{}, err
		}
		c.UserID = userID
		c.sessionConnected = true
		return ConnectResult{Status: ConnectStatusConnected, Outcome: ConnectOutcomeSeededLocal}, nil
	case "remote_authoritative":
		c.UserID = userID
		if err := c.beginRemoteReplaceLocked(ctx, userID, c.SourceID); err != nil {
			return ConnectResult{}, err
		}
		state, err = c.loadLifecycleState(ctx)
		if err != nil {
			return ConnectResult{}, err
		}
		if err := c.finalizeRemoteReplaceLocked(ctx, state); err != nil {
			return ConnectResult{}, err
		}
		c.pendingInitializationID = ""
		if err := c.persistConnectedLifecycleState(ctx, userID, ""); err != nil {
			return ConnectResult{}, err
		}
		c.sessionConnected = true
		return ConnectResult{Status: ConnectStatusConnected, Outcome: ConnectOutcomeUsedRemote}, nil
	default:
		return ConnectResult{}, fmt.Errorf("unexpected connect resolution %q", resp.Resolution)
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
	state.PendingTargetSourceID = ""
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	state.PendingApplyCursor = 0
	state.PendingInitializationID = ""
	state.RuntimeBypassActive = false
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return err
	}
	c.pendingInitializationID = ""
	c.UserID = ""
	c.sessionConnected = false
	return nil
}

func (c *Client) SignOut(ctx context.Context) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	status, err := c.pendingSyncStatusLocked(ctx)
	if err != nil {
		return err
	}
	if status.BlocksSignOut {
		return &SignOutBlockedError{PendingRowCount: status.PendingRowCount}
	}
	if err := c.armDestructiveTransition(ctx, lifecycleTransitionSignOut, ""); err != nil {
		return err
	}
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin sign-out transaction: %w", err)
	}
	defer tx.Rollback()

	if err := c.clearFullLocalSyncStateInTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_client_state`); err != nil {
		return fmt.Errorf("failed to clear per-scope client state during sign-out: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE _sync_lifecycle_state
		SET binding_state = ?, binding_scope = ?, pending_transition_kind = ?, pending_target_scope = ?, pending_target_source_id = ?, pending_staged_snapshot_id = ?, pending_snapshot_bundle_seq = 0, pending_snapshot_row_count = 0, pending_apply_cursor = 0, pending_initialization_id = ?, preexisting_capture_done = 0, runtime_bypass_active = 0
		WHERE singleton_key = 1
	`, lifecycleBindingAnonymous, "", lifecycleTransitionNone, "", "", "", ""); err != nil {
		return fmt.Errorf("failed to persist anonymous lifecycle state during sign-out: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit sign-out transaction: %w", err)
	}
	c.pendingInitializationID = ""
	c.UserID = ""
	c.sessionConnected = false
	return nil
}

func (c *Client) SyncThenSignOut(ctx context.Context) error {
	if err := c.Sync(ctx); err != nil {
		return err
	}
	return c.SignOut(ctx)
}

func (c *Client) PendingSyncStatus(ctx context.Context) (PendingSyncStatus, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return PendingSyncStatus{}, err
	}
	defer c.writeMu.Unlock()
	return c.pendingSyncStatusLocked(ctx)
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
	return PendingSyncStatus{
		HasPendingSyncData: total > 0,
		PendingRowCount:    total,
		BlocksSignOut:      state.BindingState == lifecycleBindingAttached && total > 0,
	}, nil
}

func (c *Client) ResetForNewSource(ctx context.Context, sourceID string) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()

	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return fmt.Errorf("sourceID must be provided")
	}

	if err := c.armDestructiveTransition(ctx, lifecycleTransitionSourceReset, sourceID); err != nil {
		return err
	}
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin source reset transaction: %w", err)
	}
	defer tx.Rollback()

	if err := c.clearFullLocalSyncStateInTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_client_state`); err != nil {
		return fmt.Errorf("failed to clear client state during source reset: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE _sync_lifecycle_state
		SET source_id = ?, binding_state = ?, binding_scope = ?, pending_transition_kind = ?, pending_target_scope = ?, pending_target_source_id = ?, pending_staged_snapshot_id = ?, pending_snapshot_bundle_seq = 0, pending_snapshot_row_count = 0, pending_apply_cursor = 0, pending_initialization_id = ?, preexisting_capture_done = 0, runtime_bypass_active = 0
		WHERE singleton_key = 1
	`, sourceID, lifecycleBindingAnonymous, "", lifecycleTransitionNone, "", "", "", ""); err != nil {
		return fmt.Errorf("failed to persist source reset lifecycle state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit source reset: %w", err)
	}
	c.SourceID = sourceID
	c.pendingInitializationID = ""
	c.UserID = ""
	c.sessionConnected = false
	return nil
}

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

	c.SourceID = ""
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
	return nil
}

func (c *Client) connectRequest(ctx context.Context, userID, sourceID string, hasLocalPendingRows bool) (*oversync.ConnectResponse, error) {
	reqBody, err := json.Marshal(&oversync.ConnectRequest{
		SourceID:            sourceID,
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
		return nil, &ConnectLifecycleUnsupportedError{Reason: "missing /sync/connect endpoint"}
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
	if _, err := c.DB.ExecContext(ctx, `
		UPDATE _sync_lifecycle_state
		SET source_id = ?, binding_state = ?, binding_scope = ?, pending_transition_kind = ?, pending_target_scope = ?, pending_target_source_id = ?, pending_staged_snapshot_id = ?, pending_snapshot_bundle_seq = 0, pending_snapshot_row_count = 0, pending_apply_cursor = 0, pending_initialization_id = ?, preexisting_capture_done = 1, runtime_bypass_active = 0
		WHERE singleton_key = 1
	`, c.SourceID, lifecycleBindingAttached, userID, lifecycleTransitionNone, "", "", "", initializationID); err != nil {
		return fmt.Errorf("failed to persist attached lifecycle state: %w", err)
	}
	return nil
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
	state.PendingTargetSourceID = sourceID
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	state.PendingApplyCursor = 0
	state.PendingInitializationID = ""
	state.RuntimeBypassActive = false
	if _, err := c.DB.ExecContext(ctx, `DELETE FROM _sync_client_state`); err != nil {
		return fmt.Errorf("failed to clear attached client metadata before remote_replace: %w", err)
	}
	if err := c.persistLifecycleStateInTxless(ctx, state); err != nil {
		return err
	}
	return nil
}

func (c *Client) finalizeRemoteReplaceLocked(ctx context.Context, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("remote_replace lifecycle state is required")
	}
	if state.PendingTransitionKind != lifecycleTransitionRemote {
		return fmt.Errorf("cannot finalize lifecycle transition %q as remote_replace", state.PendingTransitionKind)
	}
	refreshedState, err := c.ensureRemoteReplaceSnapshotStagedLocked(ctx, state)
	if err != nil {
		return err
	}
	if err := c.prepareAttachedClientStateForRemoteReplace(ctx, refreshedState.PendingTargetScope, refreshedState.PendingTargetSourceID); err != nil {
		return err
	}
	session := &oversync.SnapshotSession{
		SnapshotID:        refreshedState.PendingStagedSnapshotID,
		SnapshotBundleSeq: refreshedState.PendingSnapshotBundleSeq,
		RowCount:          refreshedState.PendingSnapshotRowCount,
	}
	return c.applyStagedSnapshotLocked(ctx, session, false)
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

	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_client_state`); err != nil {
		return fmt.Errorf("failed to clear attached client metadata during remote_replace attach: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO _sync_client_state (
			user_id, source_id, schema_name, next_source_bundle_id, last_bundle_seq_seen, apply_mode, rebuild_required
		) VALUES (?, ?, ?, 1, 0, 0, 0)
	`, userID, sourceID, c.config.Schema); err != nil {
		return fmt.Errorf("failed to create attached client metadata during remote_replace attach: %w", err)
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

func (c *Client) persistLifecycleStateInTxless(ctx context.Context, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("lifecycle state is required")
	}
	captureDone := 0
	if state.PreexistingCaptureDone {
		captureDone = 1
	}
	runtimeBypass := 0
	if state.RuntimeBypassActive {
		runtimeBypass = 1
	}
	if _, err := c.DB.ExecContext(ctx, `
		UPDATE _sync_lifecycle_state
		SET source_id = ?,
			binding_state = ?,
			binding_scope = ?,
			pending_transition_kind = ?,
			pending_target_scope = ?,
			pending_target_source_id = ?,
			pending_staged_snapshot_id = ?,
			pending_snapshot_bundle_seq = ?,
			pending_snapshot_row_count = ?,
			pending_apply_cursor = ?,
			pending_initialization_id = ?,
			preexisting_capture_done = ?,
			runtime_bypass_active = ?
		WHERE singleton_key = 1
	`, state.SourceID, state.BindingState, state.BindingScope, state.PendingTransitionKind, state.PendingTargetScope, state.PendingTargetSourceID, state.PendingStagedSnapshotID, state.PendingSnapshotBundleSeq, state.PendingSnapshotRowCount, state.PendingApplyCursor, state.PendingInitializationID, captureDone, runtimeBypass); err != nil {
		return fmt.Errorf("failed to persist lifecycle state: %w", err)
	}
	return nil
}

func (c *Client) armDestructiveTransition(ctx context.Context, transitionKind, pendingTargetSourceID string) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin destructive transition arm transaction: %w", err)
	}
	defer tx.Rollback()

	state, err := c.loadLifecycleStateInTx(ctx, tx)
	if err != nil {
		return err
	}
	state.PendingTransitionKind = transitionKind
	state.PendingTargetScope = ""
	state.PendingTargetSourceID = strings.TrimSpace(pendingTargetSourceID)
	state.PendingStagedSnapshotID = ""
	state.PendingSnapshotBundleSeq = 0
	state.PendingSnapshotRowCount = 0
	state.PendingApplyCursor = 0
	state.PendingInitializationID = ""
	state.RuntimeBypassActive = true
	if err := c.persistLifecycleStateInTx(ctx, tx, state); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit destructive transition arm transaction: %w", err)
	}
	return nil
}
