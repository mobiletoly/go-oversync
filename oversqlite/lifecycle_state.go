package oversqlite

import (
	"context"
	"database/sql"
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
	return fmt.Sprintf("local database is already attached to user %q; detach before attaching user %q", e.AttachedUserID, e.RequestedUserID)
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

// OpenRequiredError reports that an operation requires Open() first.
type OpenRequiredError struct {
	Operation string
}

// Error implements error.
func (e *OpenRequiredError) Error() string {
	if e == nil || strings.TrimSpace(e.Operation) == "" {
		return "Open() must be called before this oversqlite operation"
	}
	return fmt.Sprintf("Open() must be called before %s", e.Operation)
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
	pendingTransitionKind := operation.Kind
	pendingTargetScope := operation.TargetUserID
	if operation.Kind == operationKindSourceRecovery {
		pendingTransitionKind = lifecycleTransitionNone
		pendingTargetScope = ""
	}
	return &lifecycleState{
		SourceID:                 attachment.CurrentSourceID,
		BindingState:             attachment.BindingState,
		BindingScope:             attachment.AttachedUserID,
		PendingTransitionKind:    pendingTransitionKind,
		PendingTargetScope:       pendingTargetScope,
		PendingStagedSnapshotID:  operation.StagedSnapshotID,
		PendingSnapshotBundleSeq: operation.SnapshotBundleSeq,
		PendingSnapshotRowCount:  operation.SnapshotRowCount,
		PendingInitializationID:  attachment.PendingInitializationID,
	}
}

func (c *Client) loadLifecycleState(ctx context.Context) (*lifecycleState, error) {
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, c.db)
	if err != nil {
		return nil, err
	}
	return toLifecycleState(attachment, operation), nil
}

func (c *Client) persistLifecycleStateInTx(ctx context.Context, tx sqliteTransaction, state *lifecycleState) error {
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
		attachment.SchemaName = c.config.schema
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
	tx, err := c.db.BeginTx(ctx, nil)
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
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return err
	}
	attachment.PendingInitializationID = ""
	return persistAttachmentState(ctx, c.db, attachment)
}

func (c *Client) generateFreshSourceID(ctx context.Context, q queryRower, currentSourceID string) (string, error) {
	if c == nil || c.sourceIDGenerator == nil {
		return "", fmt.Errorf("source-id generator is not configured")
	}
	if err := validateOptionalSourceID(currentSourceID); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 100; attempt++ {
		candidate := c.sourceIDGenerator()
		if err := validateSourceID(candidate); err != nil {
			return "", fmt.Errorf("source-id generator returned an invalid value: %w", err)
		}
		if candidate == currentSourceID {
			continue
		}
		state, err := loadSourceState(ctx, q, candidate)
		if err != nil {
			return "", err
		}
		if state == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("failed to generate a fresh internal source id")
}

func (c *Client) openLocked(ctx context.Context) (*lifecycleState, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin lifecycle open transaction: %w", err)
	}
	defer tx.Rollback()

	state, err := c.restoreLifecycleStateInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lifecycle open transaction: %w", err)
	}
	return lifecycleStateResult(state)
}

func (c *Client) restoreLifecycleStateInTx(ctx context.Context, tx sqliteTransaction) (*lifecycleState, error) {
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil {
		return nil, err
	}

	persistedSourceID := attachment.CurrentSourceID
	if err := validateOptionalSourceID(persistedSourceID); err != nil {
		return nil, fmt.Errorf("persisted current source id is invalid: %w", err)
	}
	needsBootstrapAnonymousCapture := persistedSourceID == "" && attachment.BindingState == attachmentBindingAnonymous
	if persistedSourceID == "" {
		generatedSourceID, err := c.generateFreshSourceID(ctx, tx, "")
		if err != nil {
			return nil, err
		}
		attachment.CurrentSourceID = generatedSourceID
	}
	if strings.TrimSpace(attachment.SchemaName) == "" {
		attachment.SchemaName = c.config.schema
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
	return toLifecycleState(attachment, operation), nil
}

func lifecycleStateResult(state *lifecycleState) (*lifecycleState, error) {
	if state.PendingTransitionKind == lifecycleTransitionRemote {
		return state, &RemoteReplacePendingError{TargetUserID: state.PendingTargetScope}
	}
	return state, nil
}

func (c *Client) capturePreexistingAnonymousRowsInTx(ctx context.Context, tx sqliteTransaction) error {
	for _, table := range c.config.Tables {
		if err := c.captureAnonymousTableInTx(ctx, tx, table.TableName); err != nil {
			return err
		}
	}
	return nil
}

// Stream existing rows through the same capture expressions used by triggers.
// Keep only one local row in Go memory, even when opening a large offline database.
func (c *Client) captureAnonymousTableInTx(ctx context.Context, tx sqliteTransaction, table string) error {
	info, err := c.getTableInfoTx(tx, table)
	if err != nil {
		return err
	}
	pk, err := c.primaryKeyColumnForTable(table)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("SELECT %s,%s FROM %s AS captured ORDER BY %s",
		buildKeyJSONObjectExprHexAware(info, pk, "captured"), buildJsonObjectExprHexAware(info, "captured"), quoteIdent(table), quoteIdent(pk))
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key, payload string
		if err := rows.Scan(&key, &payload); err != nil {
			return err
		}
		if err := c.installAuthoritativeBaseline(ctx, tx, c.schemaForTable(table), table, key, "unpublished", 0, nil, baselineObservation{Origin: "never_published"}); err != nil {
			return err
		}
		if err := c.rebaseDirtyIntentInTx(ctx, tx, c.schemaForTable(table), table, key, oversync.OpInsert, 0, sql.NullString{String: payload, Valid: true}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *Client) ensureAttachedClientStateLocked(ctx context.Context, userID, sourceID string) error {
	if err := ensureSourceState(ctx, c.db, sourceID); err != nil {
		return err
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return err
	}
	if attachment.BindingState == attachmentBindingAttached && attachment.AttachedUserID != "" && attachment.AttachedUserID != userID {
		return &AttachLocalStateConflictError{Reason: "existing attached state belongs to another user"}
	}
	if attachment.CurrentSourceID != "" && attachment.CurrentSourceID != sourceID {
		return &AttachLocalStateConflictError{Reason: "existing attached state is bound to another source"}
	}
	return nil
}

func (c *Client) hasAttachedClientStateLocked(ctx context.Context, userID, sourceID string) (bool, error) {
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return false, err
	}
	return attachment.BindingState == attachmentBindingAttached &&
		attachment.AttachedUserID == userID &&
		attachment.CurrentSourceID == sourceID, nil
}

func (c *Client) verifyConnectLifecycleSupported(ctx context.Context) error {
	if c == nil || c.discovery == nil {
		return &AttachLifecycleUnsupportedError{Reason: "admitted discovery contract is unavailable"}
	}
	return requireSupportedProtocolVersion(c.discovery.ProtocolVersion)
}

const requiredProtocolVersion = "v1"

// ProtocolVersionMismatchError is a non-retryable decoded protocol incompatibility.
type ProtocolVersionMismatchError struct {
	Expected string
	Actual   string
}

func (e *ProtocolVersionMismatchError) Error() string {
	return "oversqlite protocol version mismatch"
}

func requireSupportedProtocolVersion(actual string) error {
	if actual != requiredProtocolVersion {
		return &ProtocolVersionMismatchError{Expected: requiredProtocolVersion}
	}
	return nil
}

func isProtocolVersionMismatch(err error) bool {
	var mismatch *ProtocolVersionMismatchError
	return errors.As(err, &mismatch)
}

func (c *Client) validateProtocolGateLocked(ctx context.Context, operation string) error {
	if c == nil || c.discovery == nil {
		return fmt.Errorf("%s requires an admitted discovery contract", operation)
	}
	return requireSupportedProtocolVersion(c.discovery.ProtocolVersion)
}

func decodeSchemaGenerationRequestError(statusCode int, body []byte, proof oversync.SchemaGenerationProof) error {
	response, ok := decodeServerErrorResponse(body)
	if !ok {
		return nil
	}
	code := oversync.SchemaGenerationRequestErrorCode(response.Error)
	switch code {
	case oversync.SchemaGenerationUnknown, oversync.SchemaGenerationFingerprintMismatch:
		if statusCode != http.StatusConflict {
			return nil
		}
	case oversync.SchemaGenerationUnsupported:
		if statusCode != http.StatusGone {
			return nil
		}
	case oversync.SchemaGenerationRetired:
		if statusCode != http.StatusUpgradeRequired {
			return nil
		}
	default:
		return nil
	}
	return &oversync.SchemaGenerationRequestError{
		Code: code, GenerationID: proof.ID, Message: string(code),
	}
}

type snapshotNegotiation struct {
	maxRows  int
	maxBytes int64
}

func (c *Client) negotiateSnapshotLimits(ctx context.Context) (snapshotNegotiation, error) {
	if c == nil || c.discovery == nil {
		return snapshotNegotiation{}, fmt.Errorf("snapshot requires an admitted discovery contract")
	}
	limits := c.discovery.Limits
	if c == nil || c.config == nil || c.config.SnapshotChunkRows <= 0 || c.config.SnapshotChunkBytes <= 0 {
		return snapshotNegotiation{}, fmt.Errorf("snapshot client row and byte budgets must be positive")
	}
	effectiveRows := min(c.config.SnapshotChunkRows, limits.MaxSnapshotRowsPerChunk)
	effectiveBytes := min(c.config.SnapshotChunkBytes, limits.MaxSnapshotDecodedBytesPerChunk)
	if effectiveBytes < limits.MaxRowDecodedBytes {
		return snapshotNegotiation{}, fmt.Errorf(
			"effective snapshot chunk byte budget %d is below server max_row_decoded_bytes %d; increase Config.SnapshotChunkBytes",
			effectiveBytes,
			limits.MaxRowDecodedBytes,
		)
	}
	return snapshotNegotiation{maxRows: effectiveRows, maxBytes: effectiveBytes}, nil
}

func (c *Client) ensureNoDestructiveTransitionLocked(ctx context.Context) error {
	operation, err := loadOperationState(ctx, c.db)
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
	if err := validateOptionalSourceID(c.sourceID); err != nil {
		return err
	}
	if c.sourceID == "" {
		return &OpenRequiredError{Operation: operation}
	}
	userID := strings.TrimSpace(c.UserID)
	if userID == "" {
		return &AttachRequiredError{Operation: operation}
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return err
	}
	if attachment.BindingState != attachmentBindingAttached ||
		attachment.AttachedUserID != userID ||
		attachment.CurrentSourceID != c.sourceID ||
		!c.sessionConnected {
		return &AttachRequiredError{Operation: operation}
	}
	return nil
}
