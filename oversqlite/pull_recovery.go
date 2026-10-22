// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) rebuildKeepSourceLocked(ctx context.Context) (RemoteSyncReport, error) {
	sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if sourceRecoveryErr != nil {
		return RemoteSyncReport{}, sourceRecoveryErr
	}
	if err := c.ensureRebuildPreconditionsLocked(ctx); err != nil {
		return RemoteSyncReport{}, err
	}
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return RemoteSyncReport{}, err
		}
		return RemoteSyncReport{
			Outcome: RemoteSyncOutcomeSkippedPaused,
			Status:  status,
		}, nil
	}
	return c.rebuildFromSnapshotWithOptionsLocked(ctx, snapshotApplyOptions{})
}

func (c *Client) resumeRequiredRecoveryLocked(ctx context.Context) (RemoteSyncReport, bool, error) {
	rebuildRequired, err := c.rebuildRequired(ctx)
	if err != nil {
		return RemoteSyncReport{}, false, err
	}
	if !rebuildRequired {
		return RemoteSyncReport{}, false, nil
	}
	operation, err := loadOperationState(ctx, c.db)
	if err != nil {
		return RemoteSyncReport{}, true, err
	}
	if operation.Kind == operationKindSourceRecovery {
		sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx)
		if err != nil {
			return RemoteSyncReport{}, true, err
		}
		return RemoteSyncReport{}, true, sourceRecoveryErr
	}
	if operation.Kind != operationKindNone {
		return RemoteSyncReport{}, true, &DestructiveTransitionInProgressError{TransitionKind: operation.Kind}
	}
	if !isCheckpointRecoveryReason(operation.Reason) {
		if err := c.markCheckpointRecoveryRequiredLocked(ctx, "resume"); err != nil {
			return RemoteSyncReport{}, true, err
		}
	}
	report, err := c.resumeCheckpointRecoveryLocked(ctx)
	return report, true, err
}

func (c *Client) resumeCheckpointRecoveryLocked(ctx context.Context) (RemoteSyncReport, error) {
	dirtyCount, outboundCount, replayState, err := c.checkpointRecoveryPendingState(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if dirtyCount > 0 || outboundCount > 0 {
		if atomic.LoadInt32(&c.uploadPaused) == 1 {
			return RemoteSyncReport{}, &CheckpointRecoveryBlockedError{
				Reason:        CheckpointRecoveryBlockedUploadPaused,
				DirtyCount:    dirtyCount,
				OutboundCount: outboundCount,
				ReplayState:   replayState,
			}
		}
		if _, err := c.pushPendingLockedWithRecovery(ctx, 0, true); err != nil {
			// Interactive sync owns its invocation allowance and terminal error
			// precedence. Direct recovery retains the blocked-result wrapper.
			if invocationFromContext(ctx) != nil {
				return RemoteSyncReport{}, err
			}
			var sourceRecoveryErr *SourceRecoveryRequiredError
			if errors.As(err, &sourceRecoveryErr) {
				return RemoteSyncReport{}, sourceRecoveryErr
			}
			dirtyCount, outboundCount, replayState, stateErr := c.checkpointRecoveryPendingState(ctx)
			if stateErr != nil {
				return RemoteSyncReport{}, stateErr
			}
			return RemoteSyncReport{}, &CheckpointRecoveryBlockedError{
				Reason:        CheckpointRecoveryBlockedPushFailed,
				DirtyCount:    dirtyCount,
				OutboundCount: outboundCount,
				ReplayState:   replayState,
			}
		}
		dirtyCount, outboundCount, replayState, err = c.checkpointRecoveryPendingState(ctx)
		if err != nil {
			return RemoteSyncReport{}, err
		}
		if dirtyCount > 0 || outboundCount > 0 {
			return RemoteSyncReport{}, &CheckpointRecoveryBlockedError{
				Reason:        CheckpointRecoveryBlockedPendingWork,
				DirtyCount:    dirtyCount,
				OutboundCount: outboundCount,
				ReplayState:   replayState,
			}
		}
	}
	return c.runAutomaticRecovery(ctx)
}

func (c *Client) rebuildSourceRecoveryLocked(ctx context.Context) (RemoteSyncReport, error) {
	return c.rebuildSourceRecoveryWithFinalizerLocked(ctx, nil)
}

func (c *Client) rebuildSourceRecoveryWithFinalizerLocked(
	ctx context.Context,
	finalize func(context.Context, *immediateTx, string) error,
) (RemoteSyncReport, error) {
	if err := c.ensureConnectedSessionLocked(ctx, "Rebuild()"); err != nil {
		return RemoteSyncReport{}, err
	}
	pendingCount, err := c.pendingChangeCount(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if pendingCount > 0 {
		return RemoteSyncReport{}, &DirtyStateRejectedError{DirtyCount: pendingCount}
	}
	if atomic.LoadInt32(&c.downloadPaused) == 1 {
		status, err := c.syncStatusLocked(ctx)
		if err != nil {
			return RemoteSyncReport{}, err
		}
		return RemoteSyncReport{
			Outcome: RemoteSyncOutcomeSkippedPaused,
			Status:  status,
		}, nil
	}
	operation, err := loadOperationState(ctx, c.db)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	newSourceID := operation.ReplacementSourceID
	if err := validateOptionalSourceID(newSourceID); err != nil {
		return RemoteSyncReport{}, err
	}
	if newSourceID == "" {
		newSourceID, err = c.generateFreshSourceID(ctx, c.db, c.sourceID)
		if err != nil {
			return RemoteSyncReport{}, err
		}
	}
	options := snapshotApplyOptions{
		RotateSource:              true,
		NewSourceID:               newSourceID,
		ReplacementReason:         strings.TrimSpace(operation.Reason),
		ClearSourceRecovery:       true,
		RequireFreshRotatedSource: true,
	}
	if finalize != nil {
		options.UpgradeFinalizer = func(finalCtx context.Context, tx *immediateTx) error {
			return finalize(finalCtx, tx, newSourceID)
		}
	}
	return c.rebuildFromSnapshotWithOptionsLocked(ctx, options)
}

func (c *Client) ensureRebuildPreconditionsLocked(ctx context.Context) error {
	if err := c.ensureConnectedSessionLocked(ctx, "Rebuild()"); err != nil {
		return err
	}
	pendingCount, err := c.pendingChangeCount(ctx)
	if err != nil {
		return err
	}
	if pendingCount > 0 {
		return &DirtyStateRejectedError{DirtyCount: pendingCount}
	}
	outboundCount, err := c.pendingPushOutboundCount(ctx)
	if err != nil {
		return err
	}
	if outboundCount > 0 {
		return &PendingPushReplayError{OutboundCount: outboundCount}
	}
	if err := c.ensureNoDestructiveTransitionLocked(ctx); err != nil {
		return err
	}
	state, err := c.loadLifecycleState(ctx)
	if err != nil {
		return err
	}
	if state.PendingTransitionKind != lifecycleTransitionNone {
		return fmt.Errorf("cannot rebuild while lifecycle operation %q is pending", state.PendingTransitionKind)
	}
	return nil
}

func (c *Client) rebuildFromSnapshotWithOptionsLocked(ctx context.Context, options snapshotApplyOptions) (RemoteSyncReport, error) {
	if err := requireNoUnresolvedUpload(ctx, c.db); err != nil {
		return RemoteSyncReport{}, err
	}
	limits, err := c.negotiateSnapshotLimits(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if !options.RotateSource && !options.ClearSourceRecovery {
		if err := c.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"); err != nil {
			return RemoteSyncReport{}, err
		}
	} else if err := c.setRebuildRequired(ctx, true); err != nil {
		return RemoteSyncReport{}, err
	}
	guard, err := completeWorkPhase(ctx, c, "snapshot guard", func(ctx context.Context) (*snapshotApplyGuard, error) { return c.pinSnapshotApplyGuard(ctx, options) })
	if err != nil {
		return RemoteSyncReport{}, err
	}
	options.PinnedGuard = guard
	startedAt := time.Now()
	session, transfer, err := c.stageOperationSnapshot(ctx, options, limits)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	effectiveSourceID := c.sourceID
	if options.RotateSource {
		effectiveSourceID = options.NewSourceID
	}
	if err := c.updateAutomaticPhase(ctx, "prepare_transition"); err != nil {
		return RemoteSyncReport{}, err
	}
	if c.beforeSnapshotApplyHook != nil {
		if err := c.beforeSnapshotApplyHook(ctx); err != nil {
			return RemoteSyncReport{}, err
		}
	}

	if err := c.applyStagedSnapshotLocked(ctx, session, options); err != nil {
		return RemoteSyncReport{}, err
	}
	c.retireSnapshotSession(ctx, session.SnapshotID, effectiveSourceID)
	if c.snapshotObserver != nil {
		c.snapshotObserver(snapshotRestoreObservation{
			StagedRows: transfer.rows, DeclaredWireBytes: transfer.bytes,
			AppliedRows: session.RowCount, Duration: time.Since(startedAt),
		})
	}
	status, err := c.syncStatusLocked(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	return RemoteSyncReport{
		Outcome: RemoteSyncOutcomeAppliedSnapshot,
		Status:  status,
		Restore: &RestoreSummary{
			BundleSeq: session.SnapshotBundleSeq,
			RowCount:  session.RowCount,
		},
	}, nil
}

func snapshotSessionCreateRequestFromOptions(currentSourceID string, options snapshotApplyOptions) (*oversync.SnapshotSessionCreateRequest, error) {
	if !options.RotateSource {
		return nil, nil
	}
	if err := validateSourceID(currentSourceID); err != nil {
		return nil, err
	}
	if err := validateSourceID(options.NewSourceID); err != nil {
		return nil, err
	}
	return &oversync.SnapshotSessionCreateRequest{
		SourceReplacement: &oversync.SnapshotSourceReplacement{
			PreviousSourceID: currentSourceID,
			NewSourceID:      options.NewSourceID,
			Reason:           strings.TrimSpace(options.ReplacementReason),
		},
	}, nil
}
