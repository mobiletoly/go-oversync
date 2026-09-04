package oversqlite

import (
	"context"
	"fmt"

	"github.com/mobiletoly/go-oversync/oversync"
)

type snapshotPendingWorkMode string

const (
	snapshotModeClearAll               snapshotPendingWorkMode = "CLEAR_ALL"
	snapshotModePreserveSourceRecovery snapshotPendingWorkMode = "PRESERVE_SOURCE_RECOVERY"
)

type outboxFingerprint struct {
	State                string
	SourceID             string
	SourceBundleID       int64
	CanonicalRequestHash string
	RowCount             int64
	ActualRowCount       int64
}

type snapshotLifecycleFingerprint struct {
	BindingState            string
	CurrentSourceID         string
	AttachedUserID          string
	SchemaName              string
	LastBundleSeqSeen       int64
	RebuildRequired         bool
	PendingInitializationID string
	OperationKind           string
	TargetUserID            string
	Reason                  string
	ReplacementSourceID     string
}

type snapshotApplyGuard struct {
	mode               snapshotPendingWorkMode
	dirtyRows          int64
	outbox             outboxFingerprint
	lifecycle          snapshotLifecycleFingerprint
	remoteReplace      bool
	remoteTargetUserID string
}

type SnapshotFinalApplyGateError struct {
	Mode, Reason string
	Cause        error
}

func (e *SnapshotFinalApplyGateError) Error() string {
	if e == nil {
		return "snapshot final apply gate rejected the authoritative replacement"
	}
	return fmt.Sprintf("snapshot final apply gate rejected %s mode: %s: %v", e.Mode, e.Reason, e.Cause)
}
func (e *SnapshotFinalApplyGateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func snapshotModeForOptions(options snapshotApplyOptions) snapshotPendingWorkMode {
	if options.PreserveOutbox || options.ClearSourceRecovery {
		return snapshotModePreserveSourceRecovery
	}
	return snapshotModeClearAll
}

func loadSnapshotGuardState(ctx context.Context, tx sqliteTransaction, mode snapshotPendingWorkMode) (*snapshotApplyGuard, error) {
	var dirtyRows, outboxRows int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyRows); err != nil {
		return nil, fmt.Errorf("failed to count dirty rows for snapshot gate: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboxRows); err != nil {
		return nil, fmt.Errorf("failed to count outbox rows for snapshot gate: %w", err)
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, err
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return nil, err
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil {
		return nil, err
	}
	return &snapshotApplyGuard{
		mode: mode, dirtyRows: dirtyRows,
		outbox:    outboxFingerprint{State: outbox.State, SourceID: outbox.SourceID, SourceBundleID: outbox.SourceBundleID, CanonicalRequestHash: outbox.CanonicalRequestHash, RowCount: outbox.RowCount, ActualRowCount: outboxRows},
		lifecycle: snapshotLifecycleFingerprint{BindingState: attachment.BindingState, CurrentSourceID: attachment.CurrentSourceID, AttachedUserID: attachment.AttachedUserID, SchemaName: attachment.SchemaName, LastBundleSeqSeen: attachment.LastBundleSeqSeen, RebuildRequired: attachment.RebuildRequired, PendingInitializationID: attachment.PendingInitializationID, OperationKind: operation.Kind, TargetUserID: operation.TargetUserID, Reason: operation.Reason, ReplacementSourceID: operation.ReplacementSourceID},
	}, nil
}

func validateSnapshotGuardMode(guard *snapshotApplyGuard) error {
	if guard.dirtyRows != 0 {
		return &DirtyStateRejectedError{DirtyCount: int(guard.dirtyRows)}
	}
	switch guard.mode {
	case snapshotModeClearAll:
		if guard.outbox.State != outboxStateNone || guard.outbox.ActualRowCount != 0 {
			return &PendingPushReplayError{OutboundCount: int(guard.outbox.ActualRowCount)}
		}
		if guard.remoteReplace {
			if guard.lifecycle.BindingState != attachmentBindingAnonymous || guard.lifecycle.OperationKind != operationKindRemoteReplace || guard.lifecycle.TargetUserID != guard.remoteTargetUserID {
				return &RebuildRequiredError{}
			}
			return nil
		}
		if guard.lifecycle.BindingState != attachmentBindingAttached || !guard.lifecycle.RebuildRequired || guard.lifecycle.OperationKind != operationKindNone {
			return &RebuildRequiredError{}
		}
	case snapshotModePreserveSourceRecovery:
		if guard.lifecycle.BindingState != attachmentBindingAttached || !guard.lifecycle.RebuildRequired || guard.lifecycle.OperationKind != operationKindSourceRecovery {
			return &SourceRecoveryRequiredError{Code: SourceRecoverySequenceChanged, Message: "source-recovery lifecycle guard is not active"}
		}
		switch guard.outbox.State {
		case outboxStateNone:
			if guard.outbox.ActualRowCount != 0 {
				return &PendingPushReplayError{OutboundCount: int(guard.outbox.ActualRowCount)}
			}
		case outboxStatePrepared:
			if guard.outbox.SourceID == "" || guard.outbox.SourceBundleID <= 0 || guard.outbox.CanonicalRequestHash == "" || guard.outbox.RowCount <= 0 || guard.outbox.ActualRowCount != guard.outbox.RowCount {
				return &PendingPushReplayError{OutboundCount: int(guard.outbox.ActualRowCount)}
			}
		default:
			return &SourceRecoveryRequiredError{Code: SourceRecoverySequenceChanged, Message: fmt.Sprintf("outbox state %q cannot be preserved as source recovery", guard.outbox.State)}
		}
	default:
		return fmt.Errorf("unsupported snapshot pending-work mode %q", guard.mode)
	}
	return nil
}

func (c *Client) pinSnapshotApplyGuard(ctx context.Context, options snapshotApplyOptions) (*snapshotApplyGuard, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin snapshot guard read transaction: %w", err)
	}
	defer tx.Rollback()
	guard, err := loadSnapshotGuardState(ctx, tx, snapshotModeForOptions(options))
	if err != nil {
		return nil, err
	}
	guard.remoteReplace, guard.remoteTargetUserID = options.FinalizeRemoteReplace, options.RemoteTargetUserID
	if err := validateSnapshotGuardMode(guard); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to pin snapshot guard: %w", err)
	}
	return guard, nil
}

func snapshotOutboxEqual(a, b outboxFingerprint) bool               { return a == b }
func snapshotLifecycleEqual(a, b snapshotLifecycleFingerprint) bool { return a == b }

func (c *Client) validateFinalSnapshotApplyGate(ctx context.Context, tx sqliteTransaction, pinned *snapshotApplyGuard, session *oversync.SnapshotSession) error {
	if pinned == nil {
		return fmt.Errorf("snapshot final apply gate is missing pinned state")
	}
	current, err := loadSnapshotGuardState(ctx, tx, pinned.mode)
	if err != nil {
		return err
	}
	current.remoteReplace, current.remoteTargetUserID = pinned.remoteReplace, pinned.remoteTargetUserID
	if current.dirtyRows != pinned.dirtyRows {
		return &SnapshotFinalApplyGateError{Mode: string(pinned.mode), Reason: "dirty row count changed", Cause: &DirtyStateRejectedError{DirtyCount: int(current.dirtyRows)}}
	}
	if !snapshotOutboxEqual(current.outbox, pinned.outbox) {
		return &SnapshotFinalApplyGateError{Mode: string(pinned.mode), Reason: "outbox fingerprint or row count changed", Cause: &PendingPushReplayError{OutboundCount: int(current.outbox.ActualRowCount)}}
	}
	if !snapshotLifecycleEqual(current.lifecycle, pinned.lifecycle) {
		cause := error(&RebuildRequiredError{})
		if pinned.mode == snapshotModePreserveSourceRecovery {
			cause = &SourceRecoveryRequiredError{Code: SourceRecoverySequenceChanged, Message: "source-recovery lifecycle guard changed during snapshot transfer"}
		}
		return &SnapshotFinalApplyGateError{Mode: string(pinned.mode), Reason: "lifecycle guard changed", Cause: cause}
	}
	if err := validateSnapshotGuardMode(current); err != nil {
		return &SnapshotFinalApplyGateError{Mode: string(pinned.mode), Reason: "pending-work mode is no longer valid", Cause: err}
	}
	return nil
}
