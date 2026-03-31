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
	lifecycleBindingAnonymous = attachmentBindingAnonymous
	lifecycleBindingAttached  = attachmentBindingAttached

	lifecycleTransitionNone   = operationKindNone
	lifecycleTransitionRemote = operationKindRemoteReplace
)

type lifecycleState struct {
	SourceID                 string
	BindingState             string
	BindingScope             string
	PendingTransitionKind    string
	PendingTargetScope       string
	PendingStagedSnapshotID  string
	PendingSnapshotBundleSeq int64
	PendingSnapshotRowCount  int64
	PendingInitializationID  string
}

// SourceMismatchError reports that Open was called with a different source ID than the one already persisted locally.
type SourceMismatchError struct {
	PersistedSourceID string
	RequestedSourceID string
}

// Error implements error.
func (e *SourceMismatchError) Error() string {
	return fmt.Sprintf("persisted source_id %q does not match requested source_id %q; call RotateSource first", e.PersistedSourceID, e.RequestedSourceID)
}

// AttachLifecycleUnsupportedError reports that the server does not support the attach lifecycle contract.
type AttachLifecycleUnsupportedError struct {
	Reason string
}

// Error implements error.
func (e *AttachLifecycleUnsupportedError) Error() string {
	if e == nil || strings.TrimSpace(e.Reason) == "" {
		return "server does not support the oversqlite attach lifecycle"
	}
	return fmt.Sprintf("server does not support the oversqlite attach lifecycle: %s", e.Reason)
}

// AttachBindingConflictError reports that the local database is already attached to a different user.
type AttachBindingConflictError struct {
	AttachedUserID  string
	RequestedUserID string
}

// Error implements error.
func (e *AttachBindingConflictError) Error() string {
	return fmt.Sprintf("local database is already attached to user %q; detach or rotate source before attaching user %q", e.AttachedUserID, e.RequestedUserID)
}

// AttachLocalStateConflictError reports that local durable sync state is incompatible with the requested attach flow.
type AttachLocalStateConflictError struct {
	Reason string
}

// Error implements error.
func (e *AttachLocalStateConflictError) Error() string {
	if e == nil || strings.TrimSpace(e.Reason) == "" {
		return "local sync state is incompatible with the requested attach lifecycle"
	}
	return fmt.Sprintf("local sync state is incompatible with the requested attach lifecycle: %s", e.Reason)
}

// RemoteReplacePendingError reports that a pending remote-authoritative replace must be finalized by Attach.
type RemoteReplacePendingError struct {
	TargetUserID string
}

// Error implements error.
func (e *RemoteReplacePendingError) Error() string {
	if e == nil || strings.TrimSpace(e.TargetUserID) == "" {
		return "remote-authoritative replacement is pending and must be finalized by Attach"
	}
	return fmt.Sprintf("remote-authoritative replacement for user %q is pending and must be finalized by Attach", e.TargetUserID)
}

// DestructiveTransitionInProgressError reports that a local lifecycle transition blocks safe sync execution.
type DestructiveTransitionInProgressError struct {
	TransitionKind string
}

// Error implements error.
func (e *DestructiveTransitionInProgressError) Error() string {
	if e == nil || strings.TrimSpace(e.TransitionKind) == "" {
		return "a destructive local lifecycle transition is in progress"
	}
	return fmt.Sprintf("destructive local lifecycle transition %q is in progress", e.TransitionKind)
}

// OpenRequiredError reports that an operation requires Open(sourceID) first.
type OpenRequiredError struct {
	Operation string
}

// Error implements error.
func (e *OpenRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Open(sourceID) must be called before this oversqlite operation"
	}
	return fmt.Sprintf("Open(sourceID) must be called before %s", e.Operation)
}

// AttachRequiredError reports that an operation requires a successful Attach(userID).
type AttachRequiredError struct {
	Operation string
}

// Error implements error.
func (e *AttachRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Attach(userID) must complete successfully before this oversqlite operation"
	}
	return fmt.Sprintf("Attach(userID) must complete successfully before %s", e.Operation)
}

// IsLifecyclePreconditionError reports whether err is one of the typed lifecycle-precondition errors.
func IsLifecyclePreconditionError(err error) bool {
	if err == nil {
		return false
	}
	var openErr *OpenRequiredError
	if errors.As(err, &openErr) {
		return true
	}
	var connectErr *AttachRequiredError
	if errors.As(err, &connectErr) {
		return true
	}
	var transitionErr *DestructiveTransitionInProgressError
	return errors.As(err, &transitionErr)
}

func toLifecycleState(attachment *attachmentStateRecord, operation *operationStateRecord) *lifecycleState {
	if attachment == nil {
		attachment = &attachmentStateRecord{BindingState: attachmentBindingAnonymous}
	}
	if operation == nil {
		operation = &operationStateRecord{Kind: operationKindNone}
	}
	return &lifecycleState{
		SourceID:                 attachment.CurrentSourceID,
		BindingState:             attachment.BindingState,
		BindingScope:             attachment.AttachedUserID,
		PendingTransitionKind:    operation.Kind,
		PendingTargetScope:       operation.TargetUserID,
		PendingStagedSnapshotID:  operation.StagedSnapshotID,
		PendingSnapshotBundleSeq: operation.SnapshotBundleSeq,
		PendingSnapshotRowCount:  operation.SnapshotRowCount,
		PendingInitializationID:  attachment.PendingInitializationID,
	}
}

func (c *Client) loadLifecycleState(ctx context.Context) (*lifecycleState, error) {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return nil, err
	}
	return toLifecycleState(attachment, operation), nil
}

func (c *Client) loadLifecycleStateInTx(ctx context.Context, tx *sql.Tx) (*lifecycleState, error) {
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil {
		return nil, err
	}
	return toLifecycleState(attachment, operation), nil
}

func (c *Client) persistLifecycleStateInTx(ctx context.Context, tx *sql.Tx, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("lifecycle state is required")
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.CurrentSourceID = state.SourceID
	attachment.BindingState = state.BindingState
	attachment.AttachedUserID = state.BindingScope
	attachment.PendingInitializationID = state.PendingInitializationID
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.Schema
	}
	if err := ensureSourceState(ctx, tx, state.SourceID); err != nil {
		return err
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	return persistOperationState(ctx, tx, &operationStateRecord{
		Kind:              state.PendingTransitionKind,
		TargetUserID:      state.PendingTargetScope,
		StagedSnapshotID:  state.PendingStagedSnapshotID,
		SnapshotBundleSeq: state.PendingSnapshotBundleSeq,
		SnapshotRowCount:  state.PendingSnapshotRowCount,
	})
}

func (c *Client) persistLifecycleStateInTxless(ctx context.Context, state *lifecycleState) error {
	if state == nil {
		return fmt.Errorf("lifecycle state is required")
	}
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin lifecycle persistence transaction: %w", err)
	}
	defer tx.Rollback()
	if err := c.persistLifecycleStateInTx(ctx, tx, state); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit lifecycle persistence transaction: %w", err)
	}
	return nil
}

func (c *Client) clearPersistedPendingInitializationID(ctx context.Context) error {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	attachment.PendingInitializationID = ""
	return persistAttachmentState(ctx, c.DB, attachment)
}

func (c *Client) openLocked(ctx context.Context, sourceID string) (*lifecycleState, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin lifecycle open transaction: %w", err)
	}
	defer tx.Rollback()

	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil {
		return nil, err
	}

	persistedSourceID := strings.TrimSpace(attachment.CurrentSourceID)
	needsBootstrapAnonymousCapture := persistedSourceID == "" && attachment.BindingState == attachmentBindingAnonymous
	if persistedSourceID == "" {
		attachment.CurrentSourceID = sourceID
	} else if persistedSourceID != sourceID {
		return nil, &SourceMismatchError{
			PersistedSourceID: persistedSourceID,
			RequestedSourceID: sourceID,
		}
	}
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.Schema
	}
	if needsBootstrapAnonymousCapture {
		if err := c.capturePreexistingAnonymousRowsInTx(ctx, tx); err != nil {
			return nil, err
		}
	}
	if err := ensureSourceState(ctx, tx, attachment.CurrentSourceID); err != nil {
		return nil, err
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return nil, err
	}
	if err := setApplyMode(ctx, tx, false); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lifecycle open transaction: %w", err)
	}
	state := toLifecycleState(attachment, operation)
	if state.PendingTransitionKind == lifecycleTransitionRemote {
		return state, &RemoteReplacePendingError{TargetUserID: state.PendingTargetScope}
	}
	return state, nil
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
	if err := ensureSourceState(ctx, c.DB, sourceID); err != nil {
		return err
	}
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	if attachment.BindingState == attachmentBindingAttached && attachment.AttachedUserID != "" && attachment.AttachedUserID != userID {
		return &AttachLocalStateConflictError{Reason: fmt.Sprintf("existing attached state belongs to user %q", attachment.AttachedUserID)}
	}
	if attachment.CurrentSourceID != "" && attachment.CurrentSourceID != sourceID {
		return &AttachLocalStateConflictError{Reason: fmt.Sprintf("existing attached state is bound to source %q, not %q", attachment.CurrentSourceID, sourceID)}
	}
	return nil
}

func (c *Client) hasAttachedClientStateLocked(ctx context.Context, userID, sourceID string) (bool, error) {
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return false, err
	}
	return attachment.BindingState == attachmentBindingAttached &&
		attachment.AttachedUserID == userID &&
		attachment.CurrentSourceID == sourceID, nil
}

func (c *Client) verifyConnectLifecycleSupported(ctx context.Context) error {
	body, statusCode, err := c.doAuthenticatedRequestWithRetry(ctx, "connect_capabilities", http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/sync/capabilities", nil, "")
	if err != nil {
		return err
	}
	if statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed || statusCode == http.StatusNotImplemented {
		return &AttachLifecycleUnsupportedError{Reason: "missing /sync/capabilities endpoint"}
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d while fetching capabilities", statusCode)
	}

	var caps oversync.CapabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		return &AttachLifecycleUnsupportedError{Reason: "invalid capabilities response"}
	}
	if caps.Features == nil || !caps.Features["connect_lifecycle"] {
		return &AttachLifecycleUnsupportedError{Reason: "connect_lifecycle capability is absent"}
	}
	return nil
}

func (c *Client) ensureNoDestructiveTransitionLocked(ctx context.Context) error {
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return err
	}
	switch operation.Kind {
	case operationKindNone, operationKindRemoteReplace:
		return nil
	default:
		return &DestructiveTransitionInProgressError{TransitionKind: operation.Kind}
	}
}

func (c *Client) ensureConnectedSessionLocked(ctx context.Context, operation string) error {
	if strings.TrimSpace(c.SourceID) == "" {
		return &OpenRequiredError{Operation: operation}
	}
	userID := strings.TrimSpace(c.UserID)
	if userID == "" {
		return &AttachRequiredError{Operation: operation}
	}
	attachment, err := loadAttachmentState(ctx, c.DB)
	if err != nil {
		return err
	}
	if attachment.BindingState != attachmentBindingAttached ||
		attachment.AttachedUserID != userID ||
		attachment.CurrentSourceID != c.SourceID ||
		!c.sessionConnected {
		return &AttachRequiredError{Operation: operation}
	}
	return nil
}
