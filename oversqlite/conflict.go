package oversqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

const maxPushConflictAutoRetries = 2

// PushEffectCoverageError is a permanent server admission rejection. The
// frozen outbox remains available for explicit application resolution/retry.
type PushEffectCoverageError struct {
	Operation string
	Status    int
	Message   string
}

func (e *PushEffectCoverageError) Error() string {
	return "push did not produce a captured result for every uploaded key"
}

func (e *PushEffectCoverageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &retryHTTPError{Operation: e.Operation, StatusCode: e.Status}
}

// ConflictContext is the structured resolver input for push_conflict recovery.
type ConflictContext struct {
	Schema           string
	Table            string
	Key              oversync.SyncKey
	LocalOp          string
	LocalPayload     json.RawMessage
	BaseRowVersion   int64
	ServerRowVersion int64
	ServerRowDeleted bool
	ServerRowPayload json.RawMessage
}

// MergeResult is the explicit outcome of a structured conflict resolution decision.
type MergeResult interface {
	isMergeResult()
}

// AcceptServer accepts the authoritative server state and drops the conflicting local intent.
type AcceptServer struct{}

func (AcceptServer) isMergeResult() {}

// KeepLocal retries the current local intent when runtime rules allow it.
type KeepLocal struct{}

func (KeepLocal) isMergeResult() {}

// KeepMerged retries an explicit merged full-row payload when runtime rules allow it.
type KeepMerged struct {
	MergedPayload json.RawMessage
}

func (KeepMerged) isMergeResult() {}

// Resolver decides how to recover from a structured push_conflict.
type Resolver interface {
	Resolve(conflict ConflictContext) MergeResult
}

// ServerWinsResolver accepts authoritative server state.
type ServerWinsResolver struct{}

// Resolve selects the authoritative server state for a structured conflict.
func (r *ServerWinsResolver) Resolve(conflict ConflictContext) MergeResult {
	return AcceptServer{}
}

// ClientWinsResolver retries the latest local intent when valid for the conflict shape.
type ClientWinsResolver struct{}

// Resolve retries the latest local intent when the conflict shape allows it.
func (r *ClientWinsResolver) Resolve(conflict ConflictContext) MergeResult {
	return KeepLocal{}
}

// DefaultResolver preserves conflicting intent until the application resolves it.
type DefaultResolver struct{}

// Resolve never infers permission to discard a local edit.
func (r *DefaultResolver) Resolve(conflict ConflictContext) MergeResult {
	return DeferConflict{}
}

// PushConflictError is the typed transport/decode-layer representation of a structured push_conflict.
type PushConflictError struct {
	Status   int
	RawBody  string
	Response oversync.PushConflictResponse
}

// Error implements error.
func (e *PushConflictError) Error() string {
	if e == nil {
		return "push conflict"
	}
	return fmt.Sprintf("push commit conflict: HTTP %d error=push_conflict", e.Status)
}

// Conflict returns the structured conflict details when available.
func (e *PushConflictError) Conflict() *oversync.PushConflictDetails {
	if e == nil {
		return nil
	}
	return e.Response.Conflict
}

// InvalidConflictResolutionError reports a resolver result that the runtime cannot apply safely.
type InvalidConflictResolutionError struct {
	Conflict ConflictContext
	Result   MergeResult
	Message  string
}

// Error implements error.
func (e *InvalidConflictResolutionError) Error() string {
	if e == nil || strings.TrimSpace(e.Message) == "" {
		return "invalid conflict resolution"
	}
	return e.Message
}

// PushConflictRetryExhaustedError reports that structured auto-retry hit its retry budget.
type PushConflictRetryExhaustedError struct {
	RetryCount          int
	RemainingDirtyCount int
}

// Error implements error.
func (e *PushConflictRetryExhaustedError) Error() string {
	if e == nil {
		return "push conflict auto-retry exhausted"
	}
	return fmt.Sprintf(
		"push conflict auto-retry exhausted after %d retries; %d dirty rows remain replayable",
		e.RetryCount,
		e.RemainingDirtyCount,
	)
}

func syncKeysEqual(left, right oversync.SyncKey) (bool, error) {
	leftRaw, err := json.Marshal(left)
	if err != nil {
		return false, fmt.Errorf("failed to encode left sync key: %w", err)
	}
	rightRaw, err := json.Marshal(right)
	if err != nil {
		return false, fmt.Errorf("failed to encode right sync key: %w", err)
	}
	leftCanonical, err := canonicalizeJSONBytes(leftRaw)
	if err != nil {
		return false, fmt.Errorf("failed to canonicalize left sync key: %w", err)
	}
	rightCanonical, err := canonicalizeJSONBytes(rightRaw)
	if err != nil {
		return false, fmt.Errorf("failed to canonicalize right sync key: %w", err)
	}
	return bytes.Equal(leftCanonical, rightCanonical), nil
}

func normalizeOptionalJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return append(json.RawMessage(nil), trimmed...)
}
