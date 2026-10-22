// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	pushConflictErrorBodyLimit int64 = (64 << 20) + 4096
)

// DirtyStateRejectedError reports that pull or rebuild was rejected because local dirty rows still exist.
type DirtyStateRejectedError struct {
	DirtyCount int
}

// Error implements error.
func (e *DirtyStateRejectedError) Error() string {
	return fmt.Sprintf("cannot pull while %d local dirty rows exist", e.DirtyCount)
}

func (c *Client) pullToStableLocked(ctx context.Context) (RemoteSyncReport, error) {
	if err := c.requireNoPendingConflict(ctx, c.db); err != nil {
		return RemoteSyncReport{}, err
	}
	if report, resumed, err := c.resumeRequiredRecoveryLocked(ctx); resumed || err != nil {
		return report, err
	}
	sourceRecoveryErr, err := c.sourceRecoveryRequiredErrorLocked(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if sourceRecoveryErr != nil {
		return RemoteSyncReport{}, sourceRecoveryErr
	}
	if err := c.ensureNoDestructiveTransitionLocked(ctx); err != nil {
		return RemoteSyncReport{}, err
	}
	rebuildRequired, err := c.rebuildRequired(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if rebuildRequired {
		return RemoteSyncReport{}, &RebuildRequiredError{}
	}
	outboundCount, err := c.pendingPushOutboundCount(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if outboundCount > 0 {
		return RemoteSyncReport{}, &PendingPushReplayError{OutboundCount: outboundCount}
	}

	pendingCount, err := c.pendingChangeCount(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if pendingCount > 0 {
		c.logger.Warn("pull rejected due to local dirty state", "dirty_rows", pendingCount)
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

	afterBundleSeq, err := c.lastSeenBundleSeq(ctx)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	if c.discovery == nil || c.discovery.Limits.MaxPullBundles < 1 || c.discovery.Limits.MaxPullDecodedBytes < 1 {
		return RemoteSyncReport{}, &InvalidAdmittedContractError{Message: "pull limits are unavailable"}
	}
	maxBundles := c.discovery.Limits.MaxPullBundles
	if c.config.DownloadLimit > 0 && c.config.DownloadLimit < maxBundles {
		maxBundles = c.config.DownloadLimit
	}
	return c.pullIncrementalToStableLocked(ctx, afterBundleSeq, maxBundles)
}

func (c *Client) pullIncrementalToStableLocked(ctx context.Context, afterBundleSeq int64, maxBundles int) (RemoteSyncReport, error) {
	targetBundleSeq := int64(0)
	appliedBundles := false
	for {
		resp, err := c.sendPullRequest(ctx, afterBundleSeq, maxBundles, targetBundleSeq)
		if err != nil {
			if report, recovered, recoveryErr := c.recoverIncrementalPullHistoryGapLocked(ctx, err); recovered {
				return report, recoveryErr
			}
			return RemoteSyncReport{}, err
		}
		if targetBundleSeq == 0 {
			targetBundleSeq = resp.StableBundleSeq
		} else if resp.StableBundleSeq != targetBundleSeq {
			return RemoteSyncReport{}, fmt.Errorf("pull response stable bundle seq changed from %d to %d", targetBundleSeq, resp.StableBundleSeq)
		}

		for _, bundle := range resp.Bundles {
			recovered, handled, applyErr := c.applyPullUnitOrRecover(ctx, &bundle)
			if handled || applyErr != nil {
				return recovered, applyErr
			}
			appliedBundles = true
			afterBundleSeq = bundle.BundleSeq
		}

		if afterBundleSeq >= resp.StableBundleSeq {
			status, err := c.syncStatusLocked(ctx)
			if err != nil {
				return RemoteSyncReport{}, err
			}
			outcome := RemoteSyncOutcomeAlreadyAtTarget
			if appliedBundles {
				outcome = RemoteSyncOutcomeAppliedIncremental
			}
			return RemoteSyncReport{
				Outcome: outcome,
				Status:  status,
			}, nil
		}
		if !resp.HasMore && len(resp.Bundles) == 0 {
			return RemoteSyncReport{}, fmt.Errorf("pull ended before reaching stable bundle seq %d", resp.StableBundleSeq)
		}
		if !resp.HasMore && afterBundleSeq < resp.StableBundleSeq {
			return RemoteSyncReport{}, fmt.Errorf("pull ended early at bundle seq %d before stable bundle seq %d", afterBundleSeq, resp.StableBundleSeq)
		}
	}
}

func (c *Client) recoverIncrementalPullHistoryGapLocked(ctx context.Context, pullErr error) (RemoteSyncReport, bool, error) {
	reason := historicalRecoveryReason(pullErr)
	if reason == "" {
		return RemoteSyncReport{}, false, nil
	}
	enabled, err := c.automaticRecoveryEnabled(ctx)
	if err != nil {
		return RemoteSyncReport{}, true, err
	}
	if !enabled {
		return RemoteSyncReport{}, false, nil
	}
	if err := c.captureAutomaticRecovery(ctx, reason, 0, reason); err != nil {
		return RemoteSyncReport{}, true, err
	}
	report, err := c.runAutomaticRecovery(ctx)
	return report, true, err
}

func (c *Client) sendPullRequest(ctx context.Context, afterBundleSeq int64, maxBundles int, targetBundleSeq int64) (*oversync.PullResponse, error) {
	if c.discovery == nil || c.discovery.Limits.MaxPullDecodedBytes < 1 {
		return nil, &InvalidAdmittedContractError{Message: "pull response limit is unavailable"}
	}
	endpoint := fmt.Sprintf("%s/sync/pull?after_bundle_seq=%d&max_bundles=%d", c.BaseURL, afterBundleSeq, maxBundles)
	if targetBundleSeq > 0 {
		endpoint += "&target_bundle_seq=" + strconv.FormatInt(targetBundleSeq, 10)
	}

	result, err := c.doAuthenticatedBoundedRequestWithRetry(
		ctx, "pull_request", http.MethodGet, endpoint, nil, "",
		c.discovery.Limits.MaxPullDecodedBytes, snapshotControlBodyLimit, nil, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to send pull request: %w", err)
	}
	body, statusCode := result.body, result.statusCode
	if statusCode != http.StatusOK {
		if generationErr := decodeSchemaGenerationRequestError(statusCode, body, c.schemaGenerationProof); generationErr != nil {
			return nil, generationErr
		}
		if statusCode == http.StatusConflict {
			var errorResp oversync.ErrorResponse
			if json.Unmarshal(body, &errorResp) == nil {
				switch errorResp.Error {
				case "history_pruned":
					return nil, &HistoryPrunedError{
						Status: statusCode,
					}
				case "checkpoint_ahead":
					return nil, &CheckpointAheadError{
						Status: statusCode,
					}
				}
			}
		}
		if statusCode == http.StatusInternalServerError {
			if response, ok := decodeServerErrorResponse(body); ok && response.Error == "committed_bundle_unrepresentable" {
				return nil, &CommittedBundleUnrepresentableError{}
			}
		}
		return nil, fmt.Errorf("pull request returned status %d: %s", statusCode, decodeServerErrorBody(body))
	}

	if err := validatePullResponseFieldPresence(body); err != nil {
		return nil, err
	}
	var compact oversync.CompactPullResponse
	if err := json.Unmarshal(body, &compact); err != nil {
		return nil, fmt.Errorf("failed to decode pull response: %w", err)
	}
	pullResp, err := c.expandCompactPullResponse(compact)
	if err != nil {
		return nil, err
	}
	if err := c.validatePullResponse(pullResp, afterBundleSeq); err != nil {
		return nil, err
	}
	return pullResp, nil
}

func (c *Client) expandCompactPullResponse(compact oversync.CompactPullResponse) (*oversync.PullResponse, error) {
	if c.discovery == nil {
		return nil, fmt.Errorf("pull response requires an admitted discovery contract")
	}
	response := &oversync.PullResponse{StableBundleSeq: compact.StableBundleSeq, HasMore: compact.HasMore, Bundles: make([]oversync.Bundle, 0, len(compact.Bundles))}
	for index, compactBundle := range compact.Bundles {
		var logicalRows []oversync.PushRequestRow
		if len(compactBundle.Rows) > 0 {
			var err error
			logicalRows, err = oversync.ExpandCompactPushRows(c.discovery.SchemaGeneration, compactBundle.Rows)
			if err != nil {
				return nil, fmt.Errorf("expand compact pull bundle %d: %w", index, err)
			}
		}
		if err := c.validateAdmittedResponseRows("pull response", logicalRows); err != nil {
			return nil, err
		}
		rows := make([]oversync.BundleRow, 0, len(logicalRows))
		for _, row := range logicalRows {
			rows = append(rows, oversync.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.BaseRowVersion, Payload: row.Payload})
		}
		hashRows := make([]protocolhash.BundleRow, 0, len(rows))
		for _, row := range rows {
			hashRows = append(hashRows, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload})
		}
		hash, byteCount, err := protocolhash.CommittedBundle(hashRows)
		if err != nil {
			return nil, fmt.Errorf("hash compact pull bundle %d: %w", index, err)
		}
		if hash != compactBundle.BundleHash {
			return nil, fmt.Errorf("compact pull bundle %d bundle_hash does not match rows", index)
		}
		response.Bundles = append(response.Bundles, oversync.Bundle{BundleSeq: compactBundle.BundleSeq, SourceID: compactBundle.SourceID, SourceBundleID: compactBundle.SourceBundleID, RowCount: int64(len(rows)), ByteCount: byteCount, BundleHash: hash, Rows: rows})
	}
	return response, nil
}

func (c *Client) applyPulledBundleLocked(ctx context.Context, bundle *oversync.Bundle) error {
	return c.completeWork(ctx, "pull apply", func(ctx context.Context) error { return c.applyPulledBundleLockedSlice(ctx, bundle) })
}

func (c *Client) applyPulledBundleLockedSlice(ctx context.Context, bundle *oversync.Bundle) (resultErr error) {
	ctx = withUniqueApplyContext(ctx)
	ctx = context.WithValue(ctx, applyInputSequenceKey{}, bundle.BundleSeq)
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to begin pulled bundle transaction: %w", err)
	}
	defer tx.rollbackOnReturn(&resultErr, "pulled bundle transaction")
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()
	if err := c.requireCleanPullApply(ctx, tx); err != nil {
		return err
	}

	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for pulled bundle: %w", err)
	}

	unit := applyUnit("pull", bundle.BundleSeq, bundle.BundleHash)
	if err := c.prepareApplySurvivors(ctx, tx, unit, len(bundle.Rows), func(index int) (applySurvivor, error) {
		return c.pulledApplySurvivor(ctx, tx, bundle.Rows[index])
	}); err != nil {
		return commitApplyProgress(ctx, tx, err)
	}

	if err := c.installUniqueFinalState(ctx, tx, stmtCache); err != nil {
		return err
	}

	if err := c.applyPulledRows(ctx, tx, stmtCache, bundle.Rows, bundle.BundleSeq); err != nil {
		return err
	}
	for pass := 0; pass < 64; pass++ {
		changed, err := c.restoreApplySurvivors(ctx, tx, stmtCache, false)
		if err != nil {
			return err
		}
		if !changed {
			break
		}
	}
	if _, err := c.restoreApplySurvivors(ctx, tx, stmtCache, true); err != nil {
		return err
	}
	if err := validateForeignKeyIntegrity(ctx, tx); err != nil {
		return fmt.Errorf("pulled bundle would violate local foreign keys: %w", err)
	}

	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	if attachment.LastBundleSeqSeen < bundle.BundleSeq {
		attachment.LastBundleSeqSeen = bundle.BundleSeq
	}
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return fmt.Errorf("failed to advance pulled bundle checkpoint: %w", err)
	}
	if err := finishApplySurvivors(ctx, tx); err != nil {
		return err
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit pulled bundle transaction: %w", err)
	}
	return nil
}

func (c *Client) lastSeenBundleSeq(ctx context.Context) (int64, error) {
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return 0, err
	}
	if attachment.BindingState != attachmentBindingAttached || attachment.AttachedUserID != c.UserID {
		return 0, &AttachRequiredError{Operation: "LastBundleSeqSeen()/PullToStable()"}
	}
	return attachment.LastBundleSeqSeen, nil
}

// LastBundleSeqSeen exposes the durable pulled-bundle checkpoint for diagnostics.
func (c *Client) LastBundleSeqSeen(ctx context.Context) (int64, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return 0, err
	}
	defer c.writeMu.Unlock()
	if err := c.ensureConnectedSessionLocked(ctx, "LastBundleSeqSeen()"); err != nil {
		return 0, err
	}
	return c.lastSeenBundleSeq(ctx)
}

func (c *Client) requireCleanPullApply(ctx context.Context, tx sqliteTransaction) error {
	// The pre-HTTP check cannot protect a write committed while the response was in flight.
	// Recheck under the same write lock that will apply rows and advance the checkpoint.
	var outboundCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount); err != nil {
		return fmt.Errorf("failed to count outbound rows before pulled bundle apply: %w", err)
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if outboundCount > 0 || outbox.State != outboxStateNone {
		return &PendingPushReplayError{OutboundCount: outboundCount}
	}
	var dirtyCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount); err != nil {
		return fmt.Errorf("failed to count dirty rows before pulled bundle apply: %w", err)
	}
	if dirtyCount > 0 {
		return &DirtyStateRejectedError{DirtyCount: dirtyCount}
	}

	return nil
}

func (c *Client) pulledApplySurvivor(ctx context.Context, tx sqliteTransaction, row oversync.BundleRow) (applySurvivor, error) {
	key, pk, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
	if err != nil {
		return applySurvivor{}, err
	}
	current, err := c.loadStructuredRowStateInTx(ctx, tx, row.Schema, row.Table, key)
	if err != nil {
		return applySurvivor{}, err
	}
	if current.Exists && current.RowVersion >= row.RowVersion {
		plan, err := loadApplyPlan(ctx, tx)
		if err != nil {
			return applySurvivor{}, err
		}
		return c.captureApplySurvivor(ctx, tx, plan, row.Table, pk)
	}
	return c.wireApplySurvivor(ctx, tx, row.Table, pk, row.Payload)
}

func (c *Client) applyPulledRows(ctx context.Context, tx sqliteTransaction, stmtCache *txStmtCache, rows []oversync.BundleRow, bundleSeq int64) error {
	// Preserve validated effect order. Shared staging restores survivors after
	// cascades without introducing new ordering constraints for UNIQUE values.
	for _, row := range rows {
		keyJSON, localPK, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		if err != nil {
			return err
		}
		currentState, err := c.loadStructuredRowStateInTx(ctx, tx, row.Schema, row.Table, keyJSON)
		if err != nil {
			return err
		}
		if currentState.Exists && currentState.RowVersion >= row.RowVersion {
			if currentState.RowVersion == row.RowVersion {
				kind := "live"
				if row.Op == oversync.OpDelete {
					kind = "deleted"
				}
				if err := c.installAuthoritativeBaseline(ctx, tx, row.Schema, row.Table, keyJSON, kind, row.RowVersion, row.Payload, baselineObservation{Origin: "pull", BundleSeq: &bundleSeq}); err != nil {
					return err
				}
			}
			continue
		}
		if err := c.applyBundleRowAuthoritativelyInTxUsing(ctx, stmtCache, tx, &row, localPK, baselineObservation{Origin: "pull", BundleSeq: &bundleSeq}); err != nil {
			return err
		}
		if _, err := stmtCache.ExecContext(ctx, `
		DELETE FROM _sync_dirty_rows
		WHERE schema_name = ? AND table_name = ? AND key_json = ?
	`, row.Schema, row.Table, keyJSON); err != nil {
			return fmt.Errorf("failed to clear dirty row after pulled bundle apply for %s.%s %s: %w", row.Schema, row.Table, keyJSON, err)
		}
	}
	return nil
}

// Eligibility is derived only from a concrete authoritative pull unit. Upload
// settlement and static/integrity/capacity failures never enter this path.
func (c *Client) applyPullUnitOrRecover(ctx context.Context, bundle *oversync.Bundle) (RemoteSyncReport, bool, error) {
	err := c.applyPulledBundleLocked(ctx, bundle)
	if err == nil {
		return RemoteSyncReport{}, false, nil
	}
	reason := historicalRecoveryReason(err)
	if reason == "" {
		return RemoteSyncReport{}, false, err
	}
	enabled, modeErr := c.automaticRecoveryEnabled(ctx)
	if modeErr != nil {
		return RemoteSyncReport{}, false, modeErr
	}
	if !enabled {
		return RemoteSyncReport{}, false, err
	}
	if captureErr := c.captureAutomaticRecovery(ctx, reason, bundle.BundleSeq, bundle.BundleHash); captureErr != nil {
		return RemoteSyncReport{}, true, captureErr
	}
	report, recoveryErr := c.runAutomaticRecovery(ctx)
	return report, true, recoveryErr
}
