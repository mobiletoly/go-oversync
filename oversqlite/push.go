// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// PushPending uploads the current frozen or freshly collected outbox and replays the authoritative committed bundle.
func (c *Client) PushPending(ctx context.Context) (report PushReport, resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return PushReport{}, err
	}
	defer c.writeMu.Unlock()
	if invocationFromContext(ctx) == nil {
		ctx = context.WithValue(ctx, syncInvocationKey{}, &syncInvocation{})
	}
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	if err := c.ensureConnectedSessionLocked(ctx, "PushPending()"); err != nil {
		return PushReport{}, err
	}
	if err := c.acknowledgeUploadJournal(ctx); err != nil {
		return PushReport{}, err
	}
	if err := c.ensurePushPreconditionsLocked(ctx, false); err != nil {
		return PushReport{}, err
	}
	return c.pushPendingLocked(ctx, 0)
}

func atomicLoadPaused(v *int32) bool {
	return v != nil && atomic.LoadInt32(v) == 1
}

func (c *Client) pushPendingLocked(ctx context.Context, conflictRetryCount int) (PushReport, error) {
	return c.pushPendingLockedWithRecovery(ctx, conflictRetryCount, false)
}

func (c *Client) pushPendingLockedWithRecovery(ctx context.Context, conflictRetryCount int, allowCheckpointRecovery bool) (PushReport, error) {
	if err := c.ensurePushPreconditionsLocked(ctx, allowCheckpointRecovery); err != nil {
		return PushReport{}, err
	}
	if atomicLoadPaused(&c.uploadPaused) {
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return PushReport{}, err
		}
		return PushReport{
			Outcome: PushOutcomeSkippedPaused,
			Status:  status,
		}, nil
	}
	if invocation := invocationFromContext(ctx); invocation != nil {
		if invocation.pushAttempts == maxInteractivePushAttempts {
			return PushReport{}, c.incompleteSync(ctx, SyncIncompletePushAttemptLimit)
		}
		invocation.pushAttempts++
	}

	return c.pushPendingAfterPreflightLocked(ctx, conflictRetryCount, allowCheckpointRecovery)
}

func (c *Client) ensurePushPreconditionsLocked(ctx context.Context, allowCheckpointRecovery bool) error {
	reconnect, err := c.initializationReconnectRequired(ctx)
	if err != nil {
		return err
	}
	if reconnect {
		return &AttachRequiredError{Operation: "PushPending(): initialization lease requires reconnect"}
	}
	if frozen, err := c.frozenUploadMayResume(ctx); err != nil {
		return err
	} else if frozen {
		return nil
	}
	sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx)
	if err != nil {
		return err
	}
	if sourceRecoveryErr != nil {
		return sourceRecoveryErr
	}
	if err := c.ensureNoDestructiveTransitionLocked(ctx); err != nil {
		return err
	}
	rebuildRequired, err := c.rebuildRequired(ctx)
	if err != nil {
		return err
	}
	if rebuildRequired && !allowCheckpointRecovery {
		return &RebuildRequiredError{}
	}
	return nil
}

func (c *Client) pushPendingAfterPreflightLocked(ctx context.Context, conflictRetryCount int, allowCheckpointRecovery bool) (report PushReport, resultErr error) {
	if err := c.acknowledgeUploadJournal(ctx); err != nil {
		return report, err
	}
	if reconnect, err := c.initializationReconnectRequired(ctx); err != nil {
		return report, err
	} else if reconnect {
		return report, &AttachRequiredError{Operation: "PushPending(): initialization lease requires reconnect"}
	}
	var pendingWork bool
	if err := c.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM _sync_dirty_rows) OR EXISTS(SELECT 1 FROM _sync_outbox_rows) OR EXISTS(SELECT 1 FROM _sync_upload_journal)`).Scan(&pendingWork); err != nil {
		return report, err
	}
	if pendingWork {
		unit := applyUnit("push-buffers", 0, c.sourceID)
		token, err := c.reserveWorkspace(ctx, unit, workspaceCharge{64, 256 * 1024})
		if err != nil {
			return report, err
		}
		ctx = context.WithValue(ctx, workspaceBufferKey{}, &workspaceBufferScope{client: c, token: token, unit: unit, base: workspaceCharge{64, 256 * 1024}})
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, c.releaseWorkspace(cleanup, token))
		}()
	}
	snapshot, err := c.ensurePushOutboundSnapshot(ctx)
	if err != nil {
		return PushReport{}, err
	}
	if len(snapshot.Rows) == 0 {
		return c.pushReportWithOutcomeLocked(ctx, PushOutcomeNoChange)
	}

	pending, err := readPendingConflict(ctx, c.db)
	if err != nil {
		return PushReport{}, err
	}
	if pending != nil {
		// Upgrade resolution supplies an explicit resolver using its own journal.
		if _, ok := c.Resolver.(*oneShotUpgradeResolver); !ok {
			return PushReport{}, &ConflictNeedsResolutionError{Pending: *pending}
		}
		return c.handlePushCommitErrorLocked(ctx, snapshot, &PushConflictError{Response: pending.Response}, conflictRetryCount, allowCheckpointRecovery)
	}
	rejection, err := readPendingPushRejection(ctx, c.db)
	if err != nil {
		return PushReport{}, err
	}
	if rejection != nil {
		return PushReport{}, &PushRejectionNeedsResolutionError{Pending: *rejection}
	}
	committed, err := c.obtainCommittedPushBundleLocked(ctx, snapshot)
	if err != nil {
		return c.handlePushCommitErrorLocked(ctx, snapshot, err, conflictRetryCount, allowCheckpointRecovery)
	}
	if invocation := invocationFromContext(ctx); invocation != nil {
		invocation.recordCommitted(committed.SourceID, committed.SourceBundleID)
	}
	if err := c.runBeforePushReplayHook(ctx); err != nil {
		return PushReport{}, err
	}
	if err := c.applyStagedPushBundleLocked(ctx, snapshot.Rows, committed); err != nil {
		return PushReport{}, err
	}
	c.pendingInitializationID = ""
	if err := c.clearPushPlanForLifecycle(ctx, true); err != nil {
		return PushReport{}, err
	}
	if err := c.acknowledgeUploadJournal(ctx); err != nil {
		return PushReport{}, err
	}
	return c.pushReportWithOutcomeLocked(ctx, PushOutcomeCommitted)
}

func (c *Client) obtainCommittedPushBundleLocked(ctx context.Context, snapshot *pushOutboundSnapshot) (*committedPushBundle, error) {
	currentOutbox, err := loadOutboxBundle(ctx, c.db)
	if err != nil {
		return nil, err
	}
	if currentOutbox.State != outboxStatePrepared {
		return nil, fmt.Errorf("frozen outbox is not ready for a whole-request push")
	}

	return withRetryValue(ctx, c.normalizedRetryPolicy(), "push_pending", func() (*committedPushBundle, error) {
		committed, err := c.commitPushOutboundSnapshot(ctx, snapshot)
		if err != nil {
			return nil, err
		}
		return committed, nil
	})
}

func (c *Client) handlePushCommitErrorLocked(ctx context.Context, snapshot *pushOutboundSnapshot, commitErr error, conflictRetryCount int, allowCheckpointRecovery bool) (PushReport, error) {
	var cancelled *cancelledUploadOutcome
	if errors.As(commitErr, &cancelled) {
		if err := c.disposeCancelledUpload(ctx, snapshot); err != nil {
			return PushReport{}, err
		}
		if err := c.acknowledgeUploadJournal(ctx); err != nil {
			return PushReport{}, err
		}
		return c.pushReportWithOutcomeLocked(ctx, PushOutcomeNoChange)
	}
	if rejected := pushApplicationRejection(commitErr); rejected != nil {
		pending, err := c.preservePushRejection(ctx, snapshot, rejected)
		if err != nil {
			return PushReport{}, err
		}
		if err := c.acknowledgeUploadJournal(ctx); err != nil {
			return PushReport{}, err
		}
		return PushReport{}, &PushRejectionNeedsResolutionError{Pending: *pending}
	}
	var conflictErr *PushConflictError
	if !errors.As(commitErr, &conflictErr) {
		return PushReport{}, commitErr
	}
	if err := c.resolvePushConflict(ctx, snapshot, conflictErr); err != nil {
		return PushReport{}, err
	}
	if err := c.acknowledgeUploadJournal(ctx); err != nil {
		return PushReport{}, err
	}
	remainingDirtyCount, err := c.pendingChangeCount(ctx)
	if err != nil {
		return PushReport{}, err
	}
	if remainingDirtyCount == 0 {
		return c.pushReportWithOutcomeLocked(ctx, PushOutcomeNoChange)
	}
	if conflictRetryCount >= maxPushConflictAutoRetries {
		return PushReport{}, &PushConflictRetryExhaustedError{
			RetryCount:          maxPushConflictAutoRetries,
			RemainingDirtyCount: remainingDirtyCount,
		}
	}
	return c.pushPendingLockedWithRecovery(ctx, conflictRetryCount+1, allowCheckpointRecovery)
}

func (c *Client) pushReportWithOutcomeLocked(ctx context.Context, outcome PushOutcome) (PushReport, error) {
	status, err := c.syncStatusLocked(ctx)
	if err != nil {
		return PushReport{}, err
	}
	return PushReport{
		Outcome: outcome,
		Status:  status,
	}, nil
}

func (c *Client) runBeforePushReplayHook(ctx context.Context) error {
	if c == nil || c.beforePushReplayHook == nil {
		return nil
	}
	hook := c.beforePushReplayHook
	c.beforePushReplayHook = nil
	return hook(ctx)
}

func (c *Client) runBeforePushCommitHook(ctx context.Context) error {
	if c == nil || c.beforePushCommitHook == nil {
		return nil
	}
	hook := c.beforePushCommitHook
	c.beforePushCommitHook = nil
	return hook(ctx)
}

func (c *Client) runBeforePushFreezeHook(ctx context.Context) error {
	if c == nil || c.beforePushFreezeHook == nil {
		return nil
	}
	hook := c.beforePushFreezeHook
	c.beforePushFreezeHook = nil
	return hook(ctx)
}
