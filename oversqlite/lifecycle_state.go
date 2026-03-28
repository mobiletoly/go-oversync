package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	lifecycleBindingAnonymous = "anonymous"
	lifecycleBindingAttached  = "attached"

	lifecycleTransitionNone        = "none"
	lifecycleTransitionRemote      = "remote_replace"
	lifecycleTransitionSignOut     = "signout_reset"
	lifecycleTransitionSourceReset = "source_reset"
)

type lifecycleState struct {
	SourceID                 string
	BindingState             string
	BindingScope             string
	PendingTransitionKind    string
	PendingTargetScope       string
	PendingTargetSourceID    string
	PendingStagedSnapshotID  string
	PendingSnapshotBundleSeq int64
	PendingSnapshotRowCount  int64
	PendingApplyCursor       int64
	PendingInitializationID  string
	PreexistingCaptureDone   bool
	RuntimeBypassActive      bool
}

type SourceMismatchError struct {
	PersistedSourceID string
	RequestedSourceID string
}

func (e *SourceMismatchError) Error() string {
	return fmt.Sprintf("persisted source_id %q does not match requested source_id %q; call ResetForNewSource first", e.PersistedSourceID, e.RequestedSourceID)
}

type ConnectLifecycleUnsupportedError struct {
	Reason string
}

func (e *ConnectLifecycleUnsupportedError) Error() string {
	if e == nil || strings.TrimSpace(e.Reason) == "" {
		return "server does not support the oversqlite connect lifecycle"
	}
	return fmt.Sprintf("server does not support the oversqlite connect lifecycle: %s", e.Reason)
}

type ConnectBindingConflictError struct {
	AttachedUserID  string
	RequestedUserID string
}

func (e *ConnectBindingConflictError) Error() string {
	return fmt.Sprintf("local database is already attached to user %q; sign out or reset before connecting user %q", e.AttachedUserID, e.RequestedUserID)
}

type ConnectLocalStateConflictError struct {
	Reason string
}

func (e *ConnectLocalStateConflictError) Error() string {
	if e == nil || strings.TrimSpace(e.Reason) == "" {
		return "local sync state is incompatible with the requested connect lifecycle"
	}
	return fmt.Sprintf("local sync state is incompatible with the requested connect lifecycle: %s", e.Reason)
}

type RemoteReplacePendingError struct {
	TargetUserID string
}

func (e *RemoteReplacePendingError) Error() string {
	if e == nil || strings.TrimSpace(e.TargetUserID) == "" {
		return "remote-authoritative replacement is pending and must be finalized by Connect"
	}
	return fmt.Sprintf("remote-authoritative replacement for user %q is pending and must be finalized by Connect", e.TargetUserID)
}

type DestructiveTransitionInProgressError struct {
	TransitionKind string
}

func (e *DestructiveTransitionInProgressError) Error() string {
	if e == nil || strings.TrimSpace(e.TransitionKind) == "" {
		return "a destructive local lifecycle transition is in progress"
	}
	return fmt.Sprintf("destructive local lifecycle transition %q is in progress", e.TransitionKind)
}

type OpenRequiredError struct {
	Operation string
}

func (e *OpenRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Open(sourceID) must be called before this oversqlite operation"
	}
	return fmt.Sprintf("Open(sourceID) must be called before %s", e.Operation)
}

type ConnectRequiredError struct {
	Operation string
}

func (e *ConnectRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Connect(userID) must complete successfully before this oversqlite operation"
	}
	return fmt.Sprintf("Connect(userID) must complete successfully before %s", e.Operation)
}

func IsLifecyclePreconditionError(err error) bool {
	if err == nil {
		return false
	}
	var openErr *OpenRequiredError
	if errors.As(err, &openErr) {
		return true
	}
	var connectErr *ConnectRequiredError
	if errors.As(err, &connectErr) {
		return true
	}
	var transitionErr *DestructiveTransitionInProgressError
	return errors.As(err, &transitionErr)
}

func (c *Client) loadLifecycleState(ctx context.Context) (*lifecycleState, error) {
	row := c.DB.QueryRowContext(ctx, `
		SELECT source_id, binding_state, binding_scope, pending_transition_kind, pending_target_scope, pending_target_source_id, pending_staged_snapshot_id, pending_snapshot_bundle_seq, pending_snapshot_row_count, pending_apply_cursor, pending_initialization_id, preexisting_capture_done, runtime_bypass_active
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`)
	return scanLifecycleState(row)
}

func (c *Client) loadLifecycleStateInTx(ctx context.Context, tx *sql.Tx) (*lifecycleState, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT source_id, binding_state, binding_scope, pending_transition_kind, pending_target_scope, pending_target_source_id, pending_staged_snapshot_id, pending_snapshot_bundle_seq, pending_snapshot_row_count, pending_apply_cursor, pending_initialization_id, preexisting_capture_done, runtime_bypass_active
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`)
	return scanLifecycleState(row)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanLifecycleState(row rowScanner) (*lifecycleState, error) {
	var (
		state                lifecycleState
		captureDoneInteger   int
		runtimeBypassInteger int
	)
	if err := row.Scan(
		&state.SourceID,
		&state.BindingState,
		&state.BindingScope,
		&state.PendingTransitionKind,
		&state.PendingTargetScope,
		&state.PendingTargetSourceID,
		&state.PendingStagedSnapshotID,
		&state.PendingSnapshotBundleSeq,
		&state.PendingSnapshotRowCount,
		&state.PendingApplyCursor,
		&state.PendingInitializationID,
		&captureDoneInteger,
		&runtimeBypassInteger,
	); err != nil {
		return nil, fmt.Errorf("failed to load lifecycle state: %w", err)
	}
	state.PreexistingCaptureDone = captureDoneInteger == 1
	state.RuntimeBypassActive = runtimeBypassInteger == 1
	return &state, nil
}

func (c *Client) persistLifecycleStateInTx(ctx context.Context, tx *sql.Tx, state *lifecycleState) error {
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
	if _, err := tx.ExecContext(ctx, `
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

func (c *Client) clearPersistedPendingInitializationID(ctx context.Context) error {
	if _, err := c.DB.ExecContext(ctx, `
		UPDATE _sync_lifecycle_state
		SET pending_initialization_id = ?
		WHERE singleton_key = 1
	`, ""); err != nil {
		return fmt.Errorf("failed to clear persisted initialization lease: %w", err)
	}
	return nil
}

func (c *Client) openLocked(ctx context.Context, sourceID string) (*lifecycleState, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin lifecycle open transaction: %w", err)
	}
	defer tx.Rollback()

	state, err := c.loadLifecycleStateInTx(ctx, tx)
	if err != nil {
		return nil, err
	}

	if persistedSourceID := strings.TrimSpace(state.SourceID); persistedSourceID == "" {
		state.SourceID = sourceID
	} else if persistedSourceID != sourceID {
		return nil, &SourceMismatchError{
			PersistedSourceID: persistedSourceID,
			RequestedSourceID: sourceID,
		}
	}

	if state.BindingState == lifecycleBindingAnonymous && !state.PreexistingCaptureDone {
		if err := c.capturePreexistingAnonymousRowsInTx(ctx, tx); err != nil {
			return nil, err
		}
		state.PreexistingCaptureDone = true
	}
	if strings.TrimSpace(state.BindingState) == "" {
		state.BindingState = lifecycleBindingAnonymous
	}
	if strings.TrimSpace(state.PendingTransitionKind) == "" {
		state.PendingTransitionKind = lifecycleTransitionNone
	}
	if err := c.normalizeLifecycleRecoveryInTx(ctx, tx, state); err != nil {
		return nil, err
	}

	if err := c.persistLifecycleStateInTx(ctx, tx, state); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lifecycle open transaction: %w", err)
	}
	if state.PendingTransitionKind == lifecycleTransitionRemote {
		return state, &RemoteReplacePendingError{TargetUserID: state.PendingTargetScope}
	}
	return state, nil
}

func (c *Client) normalizeLifecycleRecoveryInTx(ctx context.Context, tx *sql.Tx, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("lifecycle state is required")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_state SET apply_mode = 0 WHERE apply_mode = 1`); err != nil {
		return fmt.Errorf("failed to clear stale apply markers during Open: %w", err)
	}
	if state.RuntimeBypassActive {
		state.RuntimeBypassActive = false
	}
	switch state.PendingTransitionKind {
	case lifecycleTransitionSignOut, lifecycleTransitionSourceReset:
		state.PendingTransitionKind = lifecycleTransitionNone
		state.PendingTargetScope = ""
		state.PendingTargetSourceID = ""
		state.PendingStagedSnapshotID = ""
		state.PendingSnapshotBundleSeq = 0
		state.PendingSnapshotRowCount = 0
		state.PendingApplyCursor = 0
		state.PendingInitializationID = ""
	case "", lifecycleTransitionNone, lifecycleTransitionRemote:
	default:
		state.PendingApplyCursor = 0
	}
	return nil
}

func (c *Client) capturePreexistingAnonymousRowsInTx(ctx context.Context, tx *sql.Tx) error {
	for _, syncTable := range c.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		if tableName == "" {
			continue
		}

		pkColumn, err := c.primaryKeyColumnForTable(tableName)
		if err != nil {
			return err
		}
		isBlobPK, err := c.isPrimaryKeyBlobInTx(tx, tableName)
		if err != nil {
			return err
		}

		pkExpr := quoteIdent(pkColumn)
		if isBlobPK {
			pkExpr = fmt.Sprintf("lower(hex(%s))", quoteIdent(pkColumn))
		} else {
			pkExpr = fmt.Sprintf("CAST(%s AS TEXT)", quoteIdent(pkColumn))
		}
		query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", pkExpr, quoteIdent(tableName), quoteIdent(pkColumn))
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query preexisting keys for %s: %w", tableName, err)
		}

		var localPKs []string
		for rows.Next() {
			var localPK string
			if err := rows.Scan(&localPK); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan preexisting key for %s: %w", tableName, err)
			}
			localPKs = append(localPKs, localPK)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("failed to iterate preexisting keys for %s: %w", tableName, err)
		}
		rows.Close()

		keyColumns, err := c.syncKeyColumnsForTable(tableName)
		if err != nil {
			return err
		}
		if len(keyColumns) != 1 {
			return fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", tableName)
		}
		keyName := strings.ToLower(strings.TrimSpace(keyColumns[0]))

		for _, localPK := range localPKs {
			keyJSONBytes, err := json.Marshal(map[string]any{keyName: localPK})
			if err != nil {
				return fmt.Errorf("failed to encode preexisting key for %s.%s: %w", tableName, localPK, err)
			}
			payload, err := c.serializeRowInTx(ctx, tx, tableName, localPK)
			if err != nil {
				return fmt.Errorf("failed to serialize preexisting row for %s.%s: %w", tableName, localPK, err)
			}
			if err := c.requeueDirtyIntentInTx(
				ctx,
				tx,
				c.config.Schema,
				tableName,
				string(keyJSONBytes),
				oversync.OpInsert,
				0,
				sql.NullString{String: string(payload), Valid: true},
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) ensureAttachedClientStateLocked(ctx context.Context, userID, sourceID string) error {
	hasAttachedState, err := c.hasAttachedClientStateLocked(ctx, userID, sourceID)
	if err != nil {
		return err
	}
	if hasAttachedState {
		return nil
	}

	rows, err := c.DB.QueryContext(ctx, `
		SELECT user_id, source_id, schema_name
		FROM _sync_client_state
		ORDER BY user_id
	`)
	if err != nil {
		return fmt.Errorf("failed to inspect attached client state: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			persistedUserID   string
			persistedSourceID string
			persistedSchema   string
		)
		if err := rows.Scan(&persistedUserID, &persistedSourceID, &persistedSchema); err != nil {
			return fmt.Errorf("failed to scan attached client state: %w", err)
		}
		if persistedUserID != userID {
			return &ConnectLocalStateConflictError{Reason: fmt.Sprintf("existing per-scope metadata belongs to user %q", persistedUserID)}
		}
		if persistedSourceID != sourceID {
			return &ConnectLocalStateConflictError{Reason: fmt.Sprintf("existing attached state is bound to source %q, not %q", persistedSourceID, sourceID)}
		}
		if schemaName := strings.TrimSpace(c.config.Schema); schemaName != "" && strings.TrimSpace(persistedSchema) != "" && persistedSchema != schemaName {
			return &ConnectLocalStateConflictError{Reason: fmt.Sprintf("existing attached state is bound to schema %q, not %q", persistedSchema, schemaName)}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate attached client state: %w", err)
	}
	if _, err := c.DB.ExecContext(ctx, `
		INSERT INTO _sync_client_state (
			user_id, source_id, schema_name, next_source_bundle_id, last_bundle_seq_seen, apply_mode, rebuild_required
		) VALUES (?, ?, ?, 1, 0, 0, 0)
	`, userID, sourceID, c.config.Schema); err != nil {
		return fmt.Errorf("failed to persist attached client state: %w", err)
	}
	return nil
}

func (c *Client) hasAttachedClientStateLocked(ctx context.Context, userID, sourceID string) (bool, error) {
	var count int
	if err := c.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM _sync_client_state
		WHERE user_id = ? AND source_id = ?
	`, userID, sourceID).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check attached client state: %w", err)
	}
	return count > 0, nil
}

func (c *Client) verifyConnectLifecycleSupported(ctx context.Context) error {
	body, statusCode, err := c.doAuthenticatedRequestWithRetry(ctx, "connect_capabilities", http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/sync/capabilities", nil, "")
	if err != nil {
		return err
	}
	if statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed || statusCode == http.StatusNotImplemented {
		return &ConnectLifecycleUnsupportedError{Reason: "missing /sync/capabilities endpoint"}
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d while fetching capabilities", statusCode)
	}

	var caps oversync.CapabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		return &ConnectLifecycleUnsupportedError{Reason: "invalid capabilities response"}
	}
	if caps.Features == nil || !caps.Features["connect_lifecycle"] {
		return &ConnectLifecycleUnsupportedError{Reason: "connect_lifecycle capability is absent"}
	}
	return nil
}

func (c *Client) ensureNoDestructiveTransitionLocked(ctx context.Context) error {
	state, err := c.loadLifecycleState(ctx)
	if err != nil {
		return err
	}
	if state.RuntimeBypassActive {
		return &DestructiveTransitionInProgressError{TransitionKind: state.PendingTransitionKind}
	}
	switch state.PendingTransitionKind {
	case lifecycleTransitionNone, lifecycleTransitionRemote:
		return nil
	case lifecycleTransitionSignOut, lifecycleTransitionSourceReset:
		return &DestructiveTransitionInProgressError{TransitionKind: state.PendingTransitionKind}
	default:
		return &DestructiveTransitionInProgressError{TransitionKind: state.PendingTransitionKind}
	}
}

func (c *Client) ensureConnectedSessionLocked(ctx context.Context, operation string) error {
	if strings.TrimSpace(c.SourceID) == "" {
		return &OpenRequiredError{Operation: operation}
	}
	state, err := c.loadLifecycleState(ctx)
	if err != nil {
		return err
	}
	userID := strings.TrimSpace(c.UserID)
	if userID == "" {
		return &ConnectRequiredError{Operation: operation}
	}
	hasAttachedState, err := c.hasAttachedClientStateLocked(ctx, userID, c.SourceID)
	if err != nil {
		return err
	}
	if !hasAttachedState {
		return &ConnectRequiredError{Operation: operation}
	}
	if state.BindingState == lifecycleBindingAttached && strings.TrimSpace(state.BindingScope) != "" {
		if userID != state.BindingScope {
			return &ConnectRequiredError{Operation: operation}
		}
		if !c.sessionConnected {
			return &ConnectRequiredError{Operation: operation}
		}
		return nil
	}
	return &ConnectRequiredError{Operation: operation}
}
