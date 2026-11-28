// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"sync/atomic"
)

// RecoveryMode controls automatic baseline jumps. Explicit Rebuild retains its
// clean-state and capability gates in either mode.
type RecoveryMode string

const (
	RecoveryAutomaticClean RecoveryMode = "automatic_clean"
	RecoveryDisabled       RecoveryMode = "disabled"
)

// RecoveryStatus is the latest bounded coordinator state, never a business report.
// Keys are exact diagnostic identities; Truncated also covers the byte ceiling.
type RecoveryStatus struct {
	Phase            string   `json:"phase"`
	Reason           string   `json:"reason,omitempty"`
	RecoveryID       string   `json:"recovery_id,omitempty"`
	ObservedBoundary int64    `json:"observed_boundary"`
	Table            string   `json:"table,omitempty"`
	Index            string   `json:"index,omitempty"`
	Trigger          string   `json:"trigger,omitempty"`
	Dependency       string   `json:"dependency,omitempty"`
	Keys             []string `json:"keys,omitempty"`
	Truncated        bool     `json:"truncated,omitempty"`
	NextAction       string   `json:"next_action"`
}

type recoveryRecord struct {
	CapturedModeRevision int64        `json:"captured_mode_revision"`
	AuthorityHash        string       `json:"authority_hash,omitempty"`
	OriginalReason       string       `json:"original_reason,omitempty"`
	Version              int          `json:"version"`
	Mode                 RecoveryMode `json:"mode"`
	ModeRevision         int64        `json:"mode_revision"`
	RecoveryStatus
	UnitHash         string `json:"unit_hash,omitempty"`
	LocalGuard       string `json:"local_guard,omitempty"`
	FailureSignature string `json:"failure_signature,omitempty"`
	Armed            bool   `json:"armed"`
}

// RecoveryBlockedError leaves the entire application/upload state intact.
type RecoveryBlockedError struct{ Status RecoveryStatus }

func (e *RecoveryBlockedError) Error() string {
	return "automatic recovery blocked: " + e.Status.Reason
}

type automaticRecoveryKey struct{}
type automaticRecoveryPin struct {
	id       string
	revision int64
}

func validRecoveryMode(mode RecoveryMode) bool {
	return mode == RecoveryAutomaticClean || mode == RecoveryDisabled
}
func (c *Client) initialRecoveryMode() RecoveryMode {
	if c.config.RecoveryMode == "" {
		return RecoveryAutomaticClean
	}
	return c.config.RecoveryMode
}

// Control reads deliberately use the same durable workspace owners without
// touching invocation diagnostics: mode changes can join a yielded preparation.
func (c *Client) admitRecoveryControl(ctx context.Context, q queryRower, extra workspaceCharge) error {
	operation, err := c.operationWorkspaceCharge(ctx, q, "recovery-control")
	if err != nil {
		return err
	}
	apply, err := c.retainedApplyWorkspaceCharge(ctx, q, "recovery-control")
	if err != nil {
		return err
	}
	push, err := c.retainedPushWorkspaceCharge(ctx, q, "recovery-control")
	if err != nil {
		return err
	}
	total, err := c.sumWorkspaceCharges("recovery-control", operation, apply, push)
	if err != nil {
		return err
	}
	return c.checkSyncWorkspace("recovery-control", total.entries, total.bytes, extra.entries, extra.bytes)
}

func (c *Client) readRecovery(ctx context.Context, q queryRower) (*recoveryRecord, error) {
	var size int64
	if err := q.QueryRowContext(ctx, `SELECT length(CAST(recovery_state AS BLOB)) FROM _sync_operation_state WHERE singleton_key=1`).Scan(&size); err != nil {
		return nil, err
	}
	if size > 65536 {
		return nil, fmt.Errorf("corrupt recovery metadata: byte ceiling")
	}
	if err := c.admitRecoveryControl(ctx, q, workspaceCharge{16 + size, 4096 + 32*size}); err != nil {
		return nil, err
	}
	r := &recoveryRecord{Version: 1, Mode: c.initialRecoveryMode(), RecoveryStatus: RecoveryStatus{Phase: "idle", NextAction: "sync"}}
	if size == 0 {
		if !validRecoveryMode(r.Mode) {
			return nil, fmt.Errorf("invalid recovery mode %q", r.Mode)
		}
		return r, nil
	}
	var encoded string
	if err := q.QueryRowContext(ctx, `SELECT recovery_state FROM _sync_operation_state WHERE singleton_key=1`).Scan(&encoded); err != nil {
		return nil, err
	}
	if err := jcs.ValidateStructure([]byte(encoded)); err != nil {
		return nil, fmt.Errorf("corrupt recovery metadata: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &fields); err != nil {
		return nil, fmt.Errorf("corrupt recovery fields")
	}
	for _, field := range []string{"version", "mode", "mode_revision", "captured_mode_revision", "phase", "observed_boundary", "next_action", "armed"} {
		if value, ok := fields[field]; !ok || bytes.Equal(value, []byte("null")) {
			return nil, fmt.Errorf("corrupt recovery fields")
		}
	}
	r = &recoveryRecord{}
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(r); err != nil {
		return nil, fmt.Errorf("corrupt recovery metadata: %w", err)
	}
	if r.Version != 1 || !validRecoveryMode(r.Mode) || r.ModeRevision < 0 || r.CapturedModeRevision < 0 || r.CapturedModeRevision > r.ModeRevision || r.ObservedBoundary < 0 || len(r.Keys) > 64 || recoveryRecordBytes(r) > 16384 {
		return nil, fmt.Errorf("corrupt recovery identity")
	}
	switch r.Phase {
	case "idle", "captured", "stage_baseline", "prepare_transition", "ready", "completed", "blocked":
	default:
		return nil, fmt.Errorf("corrupt recovery phase")
	}
	if err := validateRecoveryRecord(r); err != nil {
		return nil, err
	}
	return r, nil
}

func validateRecoveryRecord(r *recoveryRecord) error {
	if r.Phase == "idle" {
		return nil
	}
	if !recoveryDigest(r.RecoveryID) || !recoveryDigest(r.AuthorityHash) || !recoveryDigest(r.LocalGuard) || !eligibleRecoveryReason(r.OriginalReason) || r.NextAction == "" {
		return fmt.Errorf("corrupt recovery authority or owner")
	}
	if r.Phase == "blocked" && !recoveryDigest(r.FailureSignature) {
		return fmt.Errorf("corrupt recovery failure signature")
	}
	return nil
}
func recoveryDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func eligibleRecoveryReason(reason string) bool {
	switch reason {
	case "history_pruned", "checkpoint_ahead", "historical_repair_required", "conflicting_desired_state":
		return true
	}
	return false
}

func recoveryRecordBytes(r *recoveryRecord) int64 {
	n := len(r.Phase) + len(r.Reason) + len(r.RecoveryID) + len(r.Table) + len(r.Index) + len(r.Trigger) + len(r.Dependency) + len(r.NextAction) + len(r.UnitHash) + len(r.LocalGuard) + len(r.FailureSignature) + len(r.AuthorityHash) + len(r.OriginalReason)
	for _, key := range r.Keys {
		n += len(key)
	}
	return int64(n)
}
func (c *Client) writeRecovery(ctx context.Context, tx execer, q queryRower, r *recoveryRecord) error {
	// Covers current decoded record, new JSON, escaping and persisted replacement.
	if len(r.Keys) > 64 || recoveryRecordBytes(r) > 16384 {
		return fmt.Errorf("recovery status exceeds bounded metadata")
	}
	if err := c.admitRecoveryControl(ctx, q, workspaceCharge{int64(32 + len(r.Keys)), 8192 + 24*recoveryRecordBytes(r)}); err != nil {
		return err
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_operation_state SET recovery_state=? WHERE singleton_key=1`, string(encoded))
	return err
}

// RecoveryStatus returns one current status. It does not start or re-arm work.
func (c *Client) RecoveryStatus(ctx context.Context) (RecoveryStatus, error) {
	if err := c.beginRecoveryControl(); err != nil {
		return RecoveryStatus{}, err
	}
	defer c.recoveryControlMu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryStatus{}, err
	}
	defer tx.Rollback()
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return RecoveryStatus{}, err
	}
	status := r.RecoveryStatus
	if r.Mode == RecoveryDisabled {
		status.Reason = "disabled"
		status.NextAction = "enable_recovery_or_rebuild"
	}
	return status, tx.Commit()
}

// SetRecoveryMode invalidates a pinned automatic attempt under the SQLite
// writer. It may be called while preparation yields; it never takes ownership.
func (c *Client) SetRecoveryMode(ctx context.Context, mode RecoveryMode) (resultErr error) {
	if err := c.beginRecoveryControl(); err != nil {
		return err
	}
	defer c.recoveryControlMu.Unlock()
	if !validRecoveryMode(mode) {
		return fmt.Errorf("invalid recovery mode %q", mode)
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.rollbackOnReturn(&resultErr, "recovery mode")
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	if r.Mode != mode {
		if r.ModeRevision == int64(^uint64(0)>>1) {
			return fmt.Errorf("recovery mode revision exhausted")
		}
		r.Mode = mode
		r.ModeRevision++
		if mode == RecoveryDisabled {
			if err = c.releaseAutomaticRequirement(ctx, tx, r); err != nil {
				return err
			}
		}
		if err = c.writeRecovery(ctx, tx, tx, r); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RetryRecovery re-arms one parked attempt. Duplicate requests coalesce, and
// pending intent, disabled mode and the final writer gates still take priority.
func (c *Client) RetryRecovery(ctx context.Context) (resultErr error) {
	if err := c.beginRecoveryControl(); err != nil {
		return err
	}
	defer c.recoveryControlMu.Unlock()
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.rollbackOnReturn(&resultErr, "recovery retry")
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	if r.Phase == "blocked" && !r.Armed {
		r.Armed = true
		if err = c.writeRecovery(ctx, tx, tx, r); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func historicalRecoveryReason(err error) string {
	var pruned *HistoryPrunedError
	if errors.As(err, &pruned) {
		return "history_pruned"
	}
	var ahead *CheckpointAheadError
	if errors.As(err, &ahead) {
		return "checkpoint_ahead"
	}
	var fk *FKReconciliationRequiredError
	if errors.As(err, &fk) && fk.Detail == "historical_repair_required" {
		return fk.Detail
	}
	var unique *FinalStateApplyBlockedError
	if errors.As(err, &unique) && unique.Operation == "pull" && unique.Reason == "conflicting_desired_state" {
		return unique.Reason
	}
	return ""
}

func (c *Client) captureAutomaticRecovery(ctx context.Context, reason string, boundary int64, hash string) error {
	return c.completeWork(ctx, "automatic recovery capture", func(ctx context.Context) error { return c.captureAutomaticRecoverySlice(ctx, reason, boundary, hash) })
}
func (c *Client) captureAutomaticRecoverySlice(ctx context.Context, reason string, boundary int64, hash string) (resultErr error) {
	if err := c.inspectSnapshotCapabilities(ctx); err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.rollbackOnReturn(&resultErr, "recovery capture")
	if err = requireNoUnresolvedUpload(ctx, tx); err != nil {
		return err
	}
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	if r.Mode == RecoveryDisabled {
		status := r.RecoveryStatus
		status.Reason = "disabled"
		status.NextAction = "ordinary_sync_or_rebuild"
		return &RecoveryBlockedError{status}
	}
	local, err := c.automaticLocalGuard(ctx, tx, "")
	if err != nil {
		return err
	}
	floor, err := c.snapshotFloor(ctx, tx)
	if err != nil {
		return err
	}
	floor = max(floor, boundary)
	if r.Phase == "blocked" && !r.Armed && r.LocalGuard == local && r.UnitHash == hash && r.CapturedModeRevision == r.ModeRevision {
		return &RecoveryBlockedError{r.RecoveryStatus}
	}
	if err := chargeAutomaticRecoveryReplan(ctx); err != nil {
		return err
	}
	identity := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("recovery-v1:%s:%d:%s:%s", reason, boundary, hash, local))))
	r.RecoveryStatus = RecoveryStatus{Phase: "captured", Reason: reason, RecoveryID: identity, ObservedBoundary: floor, NextAction: "sync"}
	r.UnitHash, r.LocalGuard, r.FailureSignature, r.Armed = hash, local, "", false
	r.CapturedModeRevision = r.ModeRevision
	r.AuthorityHash = c.recoveryAuthorityHash()
	r.OriginalReason = reason
	a, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	a.RebuildRequired = true
	if err = persistAttachmentState(ctx, tx, a); err != nil {
		return err
	}
	op, err := loadOperationState(ctx, tx)
	if err != nil {
		return err
	}
	if op.Kind != operationKindNone {
		return &DestructiveTransitionInProgressError{TransitionKind: op.Kind}
	}
	op.Reason = reason
	if err = persistOperationState(ctx, tx, op); err != nil {
		return err
	}
	if err = c.writeRecovery(ctx, tx, tx, r); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) updateAutomaticPhase(ctx context.Context, phase string) (resultErr error) {
	pin, ok := ctx.Value(automaticRecoveryKey{}).(automaticRecoveryPin)
	if !ok {
		return nil
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.rollbackOnReturn(&resultErr, "recovery phase")
	if err = c.validateAutomaticPin(ctx, tx, pin); err != nil {
		return err
	}
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	r.Phase = phase
	if err = c.writeRecovery(ctx, tx, tx, r); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (c *Client) validateAutomaticPin(ctx context.Context, q queryRower, pin automaticRecoveryPin) error {
	r, err := c.readRecovery(ctx, q)
	if err != nil {
		return err
	}
	if r.Mode != RecoveryAutomaticClean || r.ModeRevision != pin.revision || r.RecoveryID != pin.id {
		status := r.RecoveryStatus
		status.Reason = "mode_or_owner_changed"
		status.NextAction = "retry_recovery"
		return &RecoveryBlockedError{status}
	}
	return nil
}

func (c *Client) runAutomaticRecovery(ctx context.Context) (RemoteSyncReport, error) {
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return RemoteSyncReport{}, err
		}
		return RemoteSyncReport{Outcome: RemoteSyncOutcomeSkippedPaused, Status: status}, nil
	}
	r, err := c.readRecovery(ctx, c.db)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if r.Phase != "idle" && r.AuthorityHash != c.recoveryAuthorityHash() {
		return RemoteSyncReport{}, fmt.Errorf("recovery authority changed")
	}
	if r.Mode == RecoveryDisabled {
		s := r.RecoveryStatus
		s.Reason = "disabled"
		s.NextAction = "enable_recovery_or_rebuild"
		return RemoteSyncReport{}, &RecoveryBlockedError{s}
	}
	if r.Phase == "idle" || r.Phase == "completed" {
		return RemoteSyncReport{}, &RebuildRequiredError{}
	}
	if err = requireNoUnresolvedUpload(ctx, c.db); err != nil {
		return RemoteSyncReport{}, err
	}
	if i := invocationFromContext(ctx); i != nil {
		i.recoveryStarted = true
	}
	if r.Phase == "blocked" {
		r, err = c.rearmParkedAutomaticRecovery(ctx, r)
		if err != nil {
			return RemoteSyncReport{}, err
		}
	}
	ctx = context.WithValue(ctx, automaticRecoveryKey{}, automaticRecoveryPin{r.RecoveryID, r.ModeRevision})
	if err = c.updateAutomaticPhase(ctx, "stage_baseline"); err != nil {
		return RemoteSyncReport{}, err
	}
	report, err := c.rebuildFromSnapshotWithOptionsLocked(ctx, snapshotApplyOptions{})
	if err != nil {
		if parkErr := c.parkAutomaticRecovery(ctx, err); parkErr != nil {
			return report, errors.Join(err, parkErr)
		}
	}
	return report, err
}

func (c *Client) parkAutomaticRecovery(ctx context.Context, failure error) error {
	return c.completeWork(ctx, "automatic recovery parking", func(ctx context.Context) error { return c.parkAutomaticRecoverySlice(ctx, failure) })
}
func (c *Client) parkAutomaticRecoverySlice(ctx context.Context, failure error) (resultErr error) {
	if ctx.Err() != nil {
		return nil
	}
	diagnostic, park := recoveryFailureDiagnostic(failure)
	if !park {
		return nil
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.rollbackOnReturn(&resultErr, "recovery parking")
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	if err = c.admitRecoveryControl(ctx, tx, workspaceCharge{32 + min(int64(len(diagnostic.Keys)), 64), 8192 + 24*(recoveryRecordBytes(r)+recoveryDiagnosticByteBound(diagnostic))}); err != nil {
		return err
	}
	diagnostic = boundedRecoveryDiagnostic(diagnostic)
	diagnostic.Phase, diagnostic.RecoveryID, diagnostic.ObservedBoundary = "blocked", r.RecoveryID, r.ObservedBoundary
	r.RecoveryStatus, r.Armed = diagnostic, false
	r.LocalGuard, err = c.automaticLocalGuard(ctx, tx, r.Table)
	if err != nil {
		return err
	}
	signature := sha256.New()
	if err = json.NewEncoder(signature).Encode([]any{r.Version, r.UnitHash, r.Reason, r.Table, r.Index, r.Trigger, r.Dependency, r.LocalGuard}); err != nil {
		return err
	}
	r.FailureSignature = fmt.Sprintf("%x", signature.Sum(nil))
	if err = c.writeRecovery(ctx, tx, tx, r); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type snapshotEvidenceError struct{ reason string }

func (e *snapshotEvidenceError) Error() string { return "snapshot evidence: " + e.reason }

func recoveryFailureDiagnostic(failure error) (RecoveryStatus, bool) {
	status := RecoveryStatus{NextAction: "correct_blocker_or_retry_recovery"}
	var blocked *FinalStateApplyBlockedError
	if errors.As(failure, &blocked) {
		status.Reason, status.Table, status.Index, status.Trigger, status.Keys, status.Truncated = blocked.Reason, blocked.Table, blocked.Index, blocked.Trigger, blocked.Keys, blocked.HasMore
		return status, true
	}
	var fk *FKReconciliationRequiredError
	if errors.As(failure, &fk) {
		status.Reason, status.Dependency = fk.Detail, fk.Detail
		return status, true
	}
	var gate *SnapshotFinalApplyGateError
	if errors.As(failure, &gate) {
		status.Reason = "local_context_changed"
		return status, true
	}
	var capacity *SyncWorkspaceLimitError
	if errors.As(failure, &capacity) {
		status.Reason = "capacity_" + capacity.Dimension
		status.NextAction = "correct_capacity"
		return status, true
	}
	var mode *RecoveryBlockedError
	if errors.As(failure, &mode) {
		status.Reason = "mode_or_owner_changed"
		return status, true
	}
	var limit *SnapshotSessionLimitExceededError
	if errors.As(failure, &limit) {
		status.Reason = "snapshot_limit_" + limit.Dimension
		status.NextAction = "correct_capacity"
		return status, true
	}
	var chunk *SnapshotChunkTooSmallError
	if errors.As(failure, &chunk) {
		status.Reason = "snapshot_chunk_too_small"
		status.NextAction = "correct_snapshot_limits"
		return status, true
	}
	var response *snapshotSessionResponseError
	if errors.As(failure, &response) && (response.Code == "recovery_floor_ahead" || response.Code == "push_outcome_pending") {
		status.Reason = response.Code
		status.NextAction = "correct_authority_or_retry_recovery"
		return status, true
	}
	var body *SnapshotResponseBodyTooLargeError
	var encoded *EncodedResponseBodyTooLargeError
	if errors.As(failure, &body) || errors.As(failure, &encoded) {
		status.Reason = "snapshot_body_too_large"
		status.NextAction = "correct_snapshot_limits"
		return status, true
	}
	var encoding *SnapshotUnsupportedContentEncodingError
	if errors.As(failure, &encoding) {
		status.Reason = "snapshot_integrity"
		status.NextAction = "inspect_authority_or_retry_recovery"
		return status, true
	}
	var evidence *snapshotEvidenceError
	if errors.As(failure, &evidence) {
		status.Reason = "snapshot_integrity"
		status.NextAction = "inspect_authority_or_retry_recovery"
		return status, true
	}
	return RecoveryStatus{}, false
}

// Omit an oversized exact identity rather than returning a misleading prefix.
func boundedRecoveryDiagnostic(status RecoveryStatus) RecoveryStatus {
	bytes := 0
	for _, field := range []*string{&status.Table, &status.Index, &status.Trigger, &status.Dependency} {
		if bytes+len(*field) > 8192 {
			*field = ""
			status.Truncated = true
		} else {
			bytes += len(*field)
		}
	}
	keys := status.Keys
	status.Keys = nil
	for _, key := range keys {
		if len(status.Keys) == 64 || bytes+len(key) > 8192 {
			status.Truncated = true
			break
		}
		status.Keys = append(status.Keys, key)
		bytes += len(key)
	}
	return status
}

func (c *Client) completeAutomaticRecovery(ctx context.Context, tx sqliteTransaction, boundary int64) error {
	pin, ok := ctx.Value(automaticRecoveryKey{}).(automaticRecoveryPin)
	if !ok {
		return nil
	}
	if err := c.validateAutomaticPin(ctx, tx, pin); err != nil {
		return err
	}
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	r.Phase, r.NextAction, r.Armed = "completed", "sync", false
	r.ObservedBoundary = boundary
	r.Keys = nil
	r.Truncated = false
	return c.writeRecovery(ctx, tx, tx, r)
}

func (c *Client) recoveryAuthorityHash() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(c.BaseURL+"\x00"+c.config.DatabaseID+"\x00"+c.config.ServiceID+"\x00"+c.sourceID+"\x00"+fmt.Sprint(c.schemaGenerationProof))))
}

func (c *Client) automaticLocalGuard(ctx context.Context, tx sqliteTransaction, table string) (string, error) {
	ctx = context.WithValue(ctx, automaticRecoveryKey{}, nil)
	guard, err := c.snapshotContextGuard(ctx, tx)
	if err != nil {
		return "", err
	}
	deps, err := c.recoveryDependencyIdentity(ctx, tx, table)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%d", guard, deps, c.config.SnapshotChunkRows, c.config.SnapshotChunkBytes)))), nil
}

func (c *Client) readyAutomaticRecovery(ctx context.Context, tx sqliteTransaction) error {
	pin, ok := ctx.Value(automaticRecoveryKey{}).(automaticRecoveryPin)
	if !ok {
		return nil
	}
	if err := c.validateAutomaticPin(ctx, tx, pin); err != nil {
		return err
	}
	r, err := c.readRecovery(ctx, tx)
	if err != nil {
		return err
	}
	r.Phase = "ready"
	return c.writeRecovery(ctx, tx, tx, r)
}

func (c *Client) beginRecoveryControl() error {
	if c == nil || atomic.LoadUint32(&c.closed) != 0 {
		return &ClientClosedError{}
	}
	if !c.recoveryControlMu.TryLock() {
		return &SyncOperationInProgressError{}
	}
	if atomic.LoadUint32(&c.closed) != 0 {
		c.recoveryControlMu.Unlock()
		return &ClientClosedError{}
	}
	return nil
}

func (c *Client) automaticRecoveryEnabled(ctx context.Context) (bool, error) {
	record, err := c.readRecovery(ctx, c.db)
	if err != nil {
		return false, err
	}
	return record.Mode == RecoveryAutomaticClean, nil
}

// Disabling an automatic attempt releases only its derived pull requirement.
// It never changes the checkpoint, dirty intent or upload lifecycle. Ordinary
// replay can then retry the original unit under its own safety rules.
func (c *Client) releaseAutomaticRequirement(ctx context.Context, tx sqliteTransaction, r *recoveryRecord) error {
	if r.Phase == "idle" || r.Phase == "completed" || r.AuthorityHash != c.recoveryAuthorityHash() {
		return nil
	}
	op, err := loadOperationState(ctx, tx)
	if err != nil {
		return err
	}
	if op.Kind != operationKindNone || op.Reason != r.OriginalReason {
		return nil
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.RebuildRequired = false
	return persistAttachmentState(ctx, tx, attachment)
}

func recoveryDiagnosticByteBound(status RecoveryStatus) int64 {
	bytes := int64(len(status.Table) + len(status.Index) + len(status.Trigger) + len(status.Dependency))
	for i, key := range status.Keys {
		if i == 64 || bytes >= 8192 {
			break
		}
		bytes += int64(len(key))
	}
	return min(bytes, 8192)
}

func chargeAutomaticRecoveryReplan(ctx context.Context) error {
	if i := invocationFromContext(ctx); i != nil {
		if i.recoveryStarted && i.recoveryReplans >= 3 {
			return &SyncIncompleteError{Reason: SyncIncompleteRecoveryReplanLimit, PushAttempts: i.pushAttempts, ConfirmedBundleCount: len(i.confirmed)}
		}
		if i.recoveryStarted {
			i.recoveryReplans++
		} else {
			i.recoveryStarted = true
		}
	}
	return nil
}

func (c *Client) rearmParkedAutomaticRecovery(ctx context.Context, r *recoveryRecord) (*recoveryRecord, error) {
	var err error
	var current string
	err = c.completeWork(ctx, "recovery eligibility", func(ctx context.Context) error {
		tx, e := c.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		current, e = c.automaticLocalGuard(ctx, tx, r.Table)
		if e != nil {
			return e
		}
		return tx.Commit()
	})
	if err != nil {
		return nil, err
	}
	if !r.Armed && current == r.LocalGuard {
		return nil, &RecoveryBlockedError{r.RecoveryStatus}
	}
	if err = c.captureAutomaticRecovery(ctx, r.OriginalReason, r.ObservedBoundary, r.UnitHash); err != nil {
		return nil, err
	}
	r, err = c.readRecovery(ctx, c.db)
	if err != nil {
		return nil, err
	}
	return r, nil
}
