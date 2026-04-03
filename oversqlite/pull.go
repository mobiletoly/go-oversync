package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/mobiletoly/go-oversync/oversync"
)

// DirtyStateRejectedError reports that pull or rebuild was rejected because local dirty rows still exist.
type DirtyStateRejectedError struct {
	DirtyCount int
}

type snapshotApplyOptions struct {
	RotateSource              bool
	NewSourceID               string
	ReplacementReason         string
	PreserveOutbox            bool
	ClearSourceRecovery       bool
	RequireFreshRotatedSource bool
	AdvanceSourceBundleFloor  int64
}

// Error implements error.
func (e *DirtyStateRejectedError) Error() string {
	return fmt.Sprintf("cannot pull while %d local dirty rows exist", e.DirtyCount)
}

func (c *Client) pullToStableLocked(ctx context.Context) (RemoteSyncReport, error) {
	if err := c.ensureConnectedSessionLocked(ctx, "PullToStable()"); err != nil {
		return RemoteSyncReport{}, err
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
	maxBundles := c.config.DownloadLimit
	if maxBundles <= 0 {
		maxBundles = 1000
	}

	targetBundleSeq := int64(0)
	appliedBundles := false
	for {
		resp, err := c.sendPullRequest(ctx, afterBundleSeq, maxBundles, targetBundleSeq)
		if err != nil {
			var prunedErr *HistoryPrunedError
			if errors.As(err, &prunedErr) {
				c.logger.Info("pull history pruned; rebuilding from chunked snapshot", "message", prunedErr.Message)
				return c.rebuildKeepSourceLocked(ctx)
			}
			return RemoteSyncReport{}, err
		}
		if targetBundleSeq == 0 {
			targetBundleSeq = resp.StableBundleSeq
		} else if resp.StableBundleSeq != targetBundleSeq {
			return RemoteSyncReport{}, fmt.Errorf("pull response stable bundle seq changed from %d to %d", targetBundleSeq, resp.StableBundleSeq)
		}

		for _, bundle := range resp.Bundles {
			if err := c.applyPulledBundleLocked(ctx, &bundle); err != nil {
				return RemoteSyncReport{}, err
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

func (c *Client) sendPullRequest(ctx context.Context, afterBundleSeq int64, maxBundles int, targetBundleSeq int64) (*oversync.PullResponse, error) {
	endpoint := fmt.Sprintf("%s/sync/pull?after_bundle_seq=%d&max_bundles=%d", c.BaseURL, afterBundleSeq, maxBundles)
	if targetBundleSeq > 0 {
		endpoint += "&target_bundle_seq=" + strconv.FormatInt(targetBundleSeq, 10)
	}

	body, statusCode, err := c.doAuthenticatedRequestWithRetry(ctx, "pull_request", http.MethodGet, endpoint, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to send pull request: %w", err)
	}
	if statusCode != http.StatusOK {
		if statusCode == http.StatusConflict {
			var errorResp oversync.ErrorResponse
			if json.Unmarshal(body, &errorResp) == nil && errorResp.Error == "history_pruned" {
				return nil, &HistoryPrunedError{
					Status:  statusCode,
					Message: errorResp.Message,
				}
			}
		}
		return nil, fmt.Errorf("server returned status %d: %s", statusCode, string(body))
	}

	var pullResp oversync.PullResponse
	if err := json.Unmarshal(body, &pullResp); err != nil {
		return nil, fmt.Errorf("failed to decode pull response: %w", err)
	}
	if err := validatePullResponse(&pullResp, afterBundleSeq); err != nil {
		return nil, err
	}
	return &pullResp, nil
}

func (c *Client) applyPulledBundleLocked(ctx context.Context, bundle *oversync.Bundle) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin pulled bundle transaction: %w", err)
	}
	defer tx.Rollback()
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for pulled bundle: %w", err)
	}

	for _, row := range bundle.Rows {
		keyJSON, localPK, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		if err != nil {
			return err
		}
		currentState, err := c.loadStructuredRowStateInTx(ctx, tx, row.Schema, row.Table, keyJSON)
		if err != nil {
			return err
		}
		if currentState.Exists && currentState.RowVersion >= row.RowVersion {
			continue
		}
		if err := c.applyBundleRowAuthoritativelyInTxUsing(ctx, stmtCache, tx, &row, localPK); err != nil {
			return err
		}
		if _, err := stmtCache.ExecContext(ctx, `
			DELETE FROM _sync_dirty_rows
			WHERE schema_name = ? AND table_name = ? AND key_json = ?
		`, row.Schema, row.Table, keyJSON); err != nil {
			return fmt.Errorf("failed to clear dirty row after pulled bundle apply for %s.%s %s: %w", row.Schema, row.Table, keyJSON, err)
		}
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
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit pulled bundle transaction: %w", err)
	}
	return nil
}

func (c *Client) lastSeenBundleSeq(ctx context.Context) (int64, error) {
	attachment, err := loadAttachmentState(ctx, c.DB)
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

func (c *Client) rebuildSourceRecoveryLocked(ctx context.Context) (RemoteSyncReport, error) {
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
	operation, err := loadOperationState(ctx, c.DB)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	newSourceID := strings.TrimSpace(operation.ReplacementSourceID)
	if newSourceID == "" {
		newSourceID, err = c.generateFreshSourceID(ctx, c.DB, c.sourceID)
		if err != nil {
			return RemoteSyncReport{}, err
		}
	}
	return c.rebuildFromSnapshotWithOptionsLocked(ctx, snapshotApplyOptions{
		RotateSource:              true,
		NewSourceID:               newSourceID,
		ReplacementReason:         strings.TrimSpace(operation.Reason),
		PreserveOutbox:            true,
		ClearSourceRecovery:       true,
		RequireFreshRotatedSource: true,
	})
}

func (c *Client) recoverCommittedRemoteReplayPrunedLocked(ctx context.Context, committed *committedPushBundle) (PushReport, error) {
	if committed == nil {
		return PushReport{}, fmt.Errorf("committed push bundle metadata is required for replay-pruned recovery")
	}
	remoteReport, err := c.rebuildFromSnapshotWithOptionsLocked(ctx, snapshotApplyOptions{
		AdvanceSourceBundleFloor: committed.SourceBundleID + 1,
	})
	if err != nil {
		return PushReport{}, err
	}
	return PushReport{
		Outcome: PushOutcomeCommitted,
		Status:  remoteReport.Status,
	}, nil
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
	if err := c.setRebuildRequired(ctx, true); err != nil {
		return RemoteSyncReport{}, err
	}
	if err := c.clearSnapshotStage(ctx); err != nil {
		return RemoteSyncReport{}, err
	}

	sessionReq := snapshotSessionCreateRequestFromOptions(c.sourceID, options)
	session, err := c.createSnapshotSession(ctx, sessionReq)
	if err != nil {
		return RemoteSyncReport{}, err
	}
	defer c.deleteSnapshotSessionBestEffort(context.Background(), session.SnapshotID)

	afterRowOrdinal := int64(0)
	for {
		chunk, err := c.fetchSnapshotChunk(ctx, session.SnapshotID, session.SnapshotBundleSeq, afterRowOrdinal, c.snapshotChunkRows())
		if err != nil {
			return RemoteSyncReport{}, err
		}
		if err := c.stageSnapshotChunk(ctx, chunk, afterRowOrdinal); err != nil {
			return RemoteSyncReport{}, err
		}
		if !chunk.HasMore {
			break
		}
		afterRowOrdinal = chunk.NextRowOrdinal
	}

	if err := c.applyStagedSnapshotLocked(ctx, session, options); err != nil {
		return RemoteSyncReport{}, err
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

func (c *Client) snapshotChunkRows() int {
	if c != nil && c.config != nil && c.config.SnapshotChunkRows > 0 {
		return c.config.SnapshotChunkRows
	}
	return 1000
}

func snapshotSessionCreateRequestFromOptions(currentSourceID string, options snapshotApplyOptions) *oversync.SnapshotSessionCreateRequest {
	if !options.RotateSource {
		return nil
	}
	return &oversync.SnapshotSessionCreateRequest{
		SourceReplacement: &oversync.SnapshotSourceReplacement{
			PreviousSourceID: currentSourceID,
			NewSourceID:      strings.TrimSpace(options.NewSourceID),
			Reason:           strings.TrimSpace(options.ReplacementReason),
		},
	}
}

func (c *Client) createSnapshotSession(ctx context.Context, req *oversync.SnapshotSessionCreateRequest) (*oversync.SnapshotSession, error) {
	var reqBody []byte
	var err error
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal snapshot session request: %w", err)
		}
	}
	contentType := ""
	if len(reqBody) > 0 {
		contentType = "application/json"
	}
	body, statusCode, err := c.doAuthenticatedRequestWithRetry(ctx, "snapshot_session_create", http.MethodPost, buildSnapshotSessionCreateURL(c.BaseURL), reqBody, contentType)
	if err != nil {
		return nil, fmt.Errorf("failed to send snapshot session request: %w", err)
	}
	if statusCode != http.StatusOK {
		if statusCode == http.StatusConflict && req != nil && req.SourceReplacement != nil {
			if retiredResp, ok := decodeSourceRetiredResponse(body); ok {
				return nil, &SourceReplacementDivergedError{
					LocalReplacement:  strings.TrimSpace(req.SourceReplacement.NewSourceID),
					RemoteReplacement: strings.TrimSpace(retiredResp.ReplacedBySourceID),
				}
			}
		}
		return nil, fmt.Errorf("server returned status %d: %s", statusCode, decodeServerErrorBody(body))
	}

	var session oversync.SnapshotSession
	if err := json.Unmarshal(body, &session); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot session response: %w", err)
	}
	if err := validateSnapshotSession(&session); err != nil {
		return nil, err
	}
	atomic.AddInt64(&c.snapshotStats.sessionsCreated, 1)
	return &session, nil
}

func (c *Client) fetchSnapshotChunk(ctx context.Context, snapshotID string, snapshotBundleSeq int64, afterRowOrdinal int64, maxRows int) (*oversync.SnapshotChunkResponse, error) {
	body, statusCode, err := c.doAuthenticatedRequestWithRetry(ctx, "snapshot_chunk_fetch", http.MethodGet, buildSnapshotChunkURL(c.BaseURL, snapshotID, afterRowOrdinal, maxRows), nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to send snapshot chunk request: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", statusCode, decodeServerErrorBody(body))
	}

	var chunk oversync.SnapshotChunkResponse
	if err := json.Unmarshal(body, &chunk); err != nil {
		return nil, fmt.Errorf("failed to decode snapshot chunk response: %w", err)
	}
	if err := validateSnapshotChunkResponse(&chunk, snapshotID, snapshotBundleSeq, afterRowOrdinal); err != nil {
		return nil, err
	}
	atomic.AddInt64(&c.snapshotStats.chunksFetched, 1)
	return &chunk, nil
}

func (c *Client) deleteSnapshotSessionBestEffort(ctx context.Context, snapshotID string) {
	if strings.TrimSpace(snapshotID) == "" {
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, buildSnapshotSessionDeleteURL(c.BaseURL, snapshotID), nil)
	if err != nil {
		return
	}
	token, err := c.Token(ctx)
	if err != nil {
		return
	}
	c.applyAuthenticatedSyncHeaders(httpReq, token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func (c *Client) clearSnapshotStage(ctx context.Context) error {
	if _, err := c.DB.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return fmt.Errorf("failed to clear snapshot staging: %w", err)
	}
	return nil
}

func (c *Client) stageSnapshotChunk(ctx context.Context, chunk *oversync.SnapshotChunkResponse, afterRowOrdinal int64) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin snapshot stage transaction: %w", err)
	}
	defer tx.Rollback()

	for idx, row := range chunk.Rows {
		if row.Schema != c.config.Schema {
			return fmt.Errorf("snapshot row schema %s does not match client schema %s", row.Schema, c.config.Schema)
		}
		keyJSON, _, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		if err != nil {
			return err
		}
		rowOrdinal := afterRowOrdinal + int64(idx) + 1
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO _sync_snapshot_stage (
				snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`, chunk.SnapshotID, rowOrdinal, row.Schema, row.Table, keyJSON, row.RowVersion, string(row.Payload)); err != nil {
			return fmt.Errorf("failed to stage snapshot row %d for %s.%s: %w", rowOrdinal, row.Schema, row.Table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit snapshot stage transaction: %w", err)
	}
	return nil
}

func (c *Client) ensureFreshRotatedSourceInTx(ctx context.Context, tx *sql.Tx, sourceID string) error {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return fmt.Errorf("newSourceID must be provided for rotated rebuild")
	}
	if err := ensureSourceState(ctx, tx, sourceID); err != nil {
		return err
	}
	state, err := loadSourceState(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("missing source state for rotated source %s", sourceID)
	}
	if state.NextSourceBundleID != 1 || strings.TrimSpace(state.ReplacedBySourceID) != "" {
		return fmt.Errorf("rotated rebuild requires a fresh source id; %s is already in use", sourceID)
	}
	return nil
}

func (c *Client) retargetPreparedOutboxInTx(ctx context.Context, tx *sql.Tx, sourceID string, sourceBundleID int64) error {
	if sourceBundleID <= 0 {
		return fmt.Errorf("sourceBundleID must be positive when preserving prepared outbox")
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if outbox.State != outboxStatePrepared {
		return fmt.Errorf("cannot preserve outbox during source recovery when outbox state is %q", outbox.State)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_outbox_rows SET source_bundle_id = ? WHERE source_bundle_id = ?`, sourceBundleID, outbox.SourceBundleID); err != nil {
		return fmt.Errorf("failed to retarget preserved outbox rows: %w", err)
	}
	outbox.SourceID = sourceID
	outbox.SourceBundleID = sourceBundleID
	outbox.RemoteBundleHash = ""
	outbox.RemoteBundleSeq = 0
	if err := persistOutboxBundle(ctx, tx, outbox); err != nil {
		return err
	}
	return nil
}

func (c *Client) reapplyPreparedOutboxIntentLocallyInTx(ctx context.Context, stmtCache *txStmtCache, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT table_name, key_json, op, local_payload
		FROM _sync_outbox_rows
		ORDER BY row_ordinal
	`)
	if err != nil {
		return fmt.Errorf("failed to query preserved outbox rows for local replay: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tableName    string
			keyJSON      string
			op           string
			localPayload sql.NullString
		)
		if err := rows.Scan(&tableName, &keyJSON, &op, &localPayload); err != nil {
			return fmt.Errorf("failed to scan preserved outbox row for local replay: %w", err)
		}

		switch op {
		case oversync.OpDelete:
			localPK, _, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
			if err != nil {
				return err
			}
			pkColumn, err := c.primaryKeyColumnForTable(tableName)
			if err != nil {
				return err
			}
			pkValue, err := c.convertPKForQueryInTx(tx, tableName, localPK)
			if err != nil {
				return fmt.Errorf("failed to convert preserved outbox delete key for %s: %w", tableName, err)
			}
			query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdent(tableName), quoteIdent(pkColumn))
			if _, err := stmtCache.ExecContext(ctx, query, pkValue); err != nil {
				return fmt.Errorf("failed to replay preserved outbox delete for %s: %w", tableName, err)
			}
		case oversync.OpInsert, oversync.OpUpdate:
			if !localPayload.Valid {
				return fmt.Errorf("preserved outbox row for %s is missing local_payload", tableName)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(localPayload.String), &payload); err != nil {
				return fmt.Errorf("failed to decode preserved outbox payload for %s: %w", tableName, err)
			}
			if err := c.upsertRowInTxUsing(ctx, stmtCache, tx, tableName, payload); err != nil {
				return fmt.Errorf("failed to replay preserved outbox upsert for %s: %w", tableName, err)
			}
		default:
			return fmt.Errorf("unsupported preserved outbox op %q", op)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate preserved outbox rows for local replay: %w", err)
	}
	return nil
}

func (c *Client) applyStagedSnapshotLocked(ctx context.Context, session *oversync.SnapshotSession, options snapshotApplyOptions) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin staged snapshot apply transaction: %w", err)
	}
	defer tx.Rollback()
	stmtCache := newTxStmtCache(tx)
	defer stmtCache.Close()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for staged snapshot apply: %w", err)
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	if attachment.BindingState != attachmentBindingAttached || strings.TrimSpace(attachment.AttachedUserID) == "" {
		return &AttachRequiredError{Operation: "Rebuild()"}
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if err := c.clearManagedTablesInTxWithOptions(ctx, tx, clearManagedTablesOptions{PreserveOutbox: options.PreserveOutbox}); err != nil {
		return err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT schema_name, table_name, key_json, row_version, payload
		FROM _sync_snapshot_stage
		WHERE snapshot_id = ?
		ORDER BY row_ordinal
	`, session.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to query staged snapshot rows: %w", err)
	}
	defer rows.Close()

	var stagedRowCount int64
	for rows.Next() {
		var (
			schemaName string
			tableName  string
			keyJSON    string
			rowVersion int64
			payload    string
		)
		if err := rows.Scan(&schemaName, &tableName, &keyJSON, &rowVersion, &payload); err != nil {
			return fmt.Errorf("failed to scan staged snapshot row: %w", err)
		}

		localPK, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return err
		}
		bundleRow := oversync.BundleRow{
			Schema:     schemaName,
			Table:      tableName,
			Key:        wireKey,
			Op:         oversync.OpInsert,
			RowVersion: rowVersion,
			Payload:    json.RawMessage(payload),
		}
		if err := c.applyBundleRowAuthoritativelyInTxUsing(ctx, stmtCache, tx, &bundleRow, localPK); err != nil {
			return fmt.Errorf("failed to apply staged snapshot row for %s.%s: %w", schemaName, tableName, err)
		}
		stagedRowCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate staged snapshot rows: %w", err)
	}
	if stagedRowCount != session.RowCount {
		return fmt.Errorf("staged snapshot row count %d does not match expected row_count %d", stagedRowCount, session.RowCount)
	}
	if options.PreserveOutbox {
		if err := c.reapplyPreparedOutboxIntentLocallyInTx(ctx, stmtCache, tx); err != nil {
			return err
		}
	}

	targetSourceID := c.sourceID
	if options.RotateSource {
		targetSourceID = strings.TrimSpace(options.NewSourceID)
		if options.RequireFreshRotatedSource {
			if err := c.ensureFreshRotatedSourceInTx(ctx, tx, targetSourceID); err != nil {
				return err
			}
		} else if err := ensureSourceState(ctx, tx, targetSourceID); err != nil {
			return err
		}
		if strings.TrimSpace(c.sourceID) != "" && c.sourceID != targetSourceID {
			if err := markSourceReplaced(ctx, tx, c.sourceID, targetSourceID); err != nil {
				return err
			}
		}
	}
	attachment.CurrentSourceID = targetSourceID
	attachment.LastBundleSeqSeen = session.SnapshotBundleSeq
	attachment.RebuildRequired = false
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return fmt.Errorf("failed to persist snapshot bundle checkpoint: %w", err)
	}
	if options.PreserveOutbox {
		if err := c.retargetPreparedOutboxInTx(ctx, tx, targetSourceID, 1); err != nil {
			return err
		}
	}
	if options.AdvanceSourceBundleFloor > 0 {
		if err := updateSourceNextBundleID(ctx, tx, targetSourceID, options.AdvanceSourceBundleFloor); err != nil {
			return err
		}
	}
	if options.ClearSourceRecovery {
		if err := c.clearSourceRecoveryRequiredInTx(ctx, tx); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?`, session.SnapshotID); err != nil {
		return fmt.Errorf("failed to clear snapshot staging after apply: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit staged snapshot apply: %w", err)
	}
	if options.RotateSource {
		c.sourceID = targetSourceID
	}
	return nil
}

func validatePullResponse(resp *oversync.PullResponse, afterBundleSeq int64) error {
	if resp == nil {
		return fmt.Errorf("pull response missing body")
	}
	if resp.StableBundleSeq < 0 {
		return fmt.Errorf("pull response stable_bundle_seq %d must be non-negative", resp.StableBundleSeq)
	}
	if resp.StableBundleSeq < afterBundleSeq {
		return fmt.Errorf("pull response stable_bundle_seq %d is behind requested after_bundle_seq %d", resp.StableBundleSeq, afterBundleSeq)
	}
	if len(resp.Bundles) > 0 && resp.StableBundleSeq == 0 {
		return fmt.Errorf("pull response missing stable_bundle_seq for non-empty bundle list")
	}

	prevBundleSeq := afterBundleSeq
	for idx := range resp.Bundles {
		bundle := &resp.Bundles[idx]
		if err := validateBundle(bundle); err != nil {
			return fmt.Errorf("invalid pull bundle %d: %w", idx, err)
		}
		if bundle.BundleSeq <= prevBundleSeq {
			return fmt.Errorf("pull response bundle_seq %d is not strictly greater than previous %d", bundle.BundleSeq, prevBundleSeq)
		}
		if bundle.BundleSeq > resp.StableBundleSeq {
			return fmt.Errorf("pull response bundle_seq %d exceeds stable_bundle_seq %d", bundle.BundleSeq, resp.StableBundleSeq)
		}
		prevBundleSeq = bundle.BundleSeq
	}
	return nil
}

func validateSnapshotSession(resp *oversync.SnapshotSession) error {
	if resp == nil {
		return fmt.Errorf("snapshot session response missing body")
	}
	if strings.TrimSpace(resp.SnapshotID) == "" {
		return fmt.Errorf("snapshot session response missing snapshot_id")
	}
	if resp.SnapshotBundleSeq < 0 {
		return fmt.Errorf("snapshot session snapshot_bundle_seq %d must be non-negative", resp.SnapshotBundleSeq)
	}
	if resp.RowCount < 0 {
		return fmt.Errorf("snapshot session row_count %d must be non-negative", resp.RowCount)
	}
	if resp.RowCount > 0 && resp.SnapshotBundleSeq == 0 {
		return fmt.Errorf("snapshot session missing snapshot_bundle_seq for non-empty row set")
	}
	if resp.ByteCount < 0 {
		return fmt.Errorf("snapshot session byte_count %d must be non-negative", resp.ByteCount)
	}
	if strings.TrimSpace(resp.ExpiresAt) == "" {
		return fmt.Errorf("snapshot session response missing expires_at")
	}
	return nil
}

func validateSnapshotChunkResponse(resp *oversync.SnapshotChunkResponse, snapshotID string, snapshotBundleSeq int64, afterRowOrdinal int64) error {
	if resp == nil {
		return fmt.Errorf("snapshot chunk response missing body")
	}
	if resp.SnapshotID != snapshotID {
		return fmt.Errorf("snapshot chunk response snapshot_id %q does not match requested %q", resp.SnapshotID, snapshotID)
	}
	if resp.SnapshotBundleSeq != snapshotBundleSeq {
		return fmt.Errorf("snapshot chunk response snapshot_bundle_seq %d does not match session %d", resp.SnapshotBundleSeq, snapshotBundleSeq)
	}
	if len(resp.Rows) > 0 && resp.SnapshotBundleSeq == 0 {
		return fmt.Errorf("snapshot chunk response missing snapshot_bundle_seq for non-empty row set")
	}
	if resp.NextRowOrdinal != afterRowOrdinal+int64(len(resp.Rows)) {
		return fmt.Errorf("snapshot chunk response next_row_ordinal %d does not match expected %d", resp.NextRowOrdinal, afterRowOrdinal+int64(len(resp.Rows)))
	}
	if resp.HasMore && len(resp.Rows) == 0 {
		return fmt.Errorf("snapshot chunk response with has_more=true must include at least one row")
	}
	for idx := range resp.Rows {
		row := &resp.Rows[idx]
		if err := validateSnapshotRow(row); err != nil {
			return fmt.Errorf("invalid snapshot row %d: %w", idx, err)
		}
	}
	return nil
}

func validateBundle(bundle *oversync.Bundle) error {
	if bundle.BundleSeq <= 0 {
		return fmt.Errorf("bundle_seq %d must be positive", bundle.BundleSeq)
	}
	if strings.TrimSpace(bundle.SourceID) == "" {
		return fmt.Errorf("bundle source_id must be non-empty")
	}
	if bundle.SourceBundleID <= 0 {
		return fmt.Errorf("bundle source_bundle_id %d must be positive", bundle.SourceBundleID)
	}
	for idx := range bundle.Rows {
		row := &bundle.Rows[idx]
		if err := validateBundleRow(row); err != nil {
			return fmt.Errorf("invalid bundle row %d: %w", idx, err)
		}
	}
	return nil
}

func validateBundleRow(row *oversync.BundleRow) error {
	if strings.TrimSpace(row.Schema) == "" {
		return fmt.Errorf("row schema must be non-empty")
	}
	if strings.TrimSpace(row.Table) == "" {
		return fmt.Errorf("row table must be non-empty")
	}
	if len(row.Key) == 0 {
		return fmt.Errorf("row key must be non-empty")
	}
	if row.RowVersion <= 0 {
		return fmt.Errorf("row_version %d must be positive", row.RowVersion)
	}
	switch row.Op {
	case oversync.OpInsert, oversync.OpUpdate:
		if len(row.Payload) == 0 {
			return fmt.Errorf("%s row payload must be present", strings.ToLower(row.Op))
		}
	case oversync.OpDelete:
	default:
		return fmt.Errorf("unsupported row op %q", row.Op)
	}
	return nil
}

func validateSnapshotRow(row *oversync.SnapshotRow) error {
	if strings.TrimSpace(row.Schema) == "" {
		return fmt.Errorf("row schema must be non-empty")
	}
	if strings.TrimSpace(row.Table) == "" {
		return fmt.Errorf("row table must be non-empty")
	}
	if len(row.Key) == 0 {
		return fmt.Errorf("row key must be non-empty")
	}
	if row.RowVersion <= 0 {
		return fmt.Errorf("row_version %d must be positive", row.RowVersion)
	}
	if len(row.Payload) == 0 {
		return fmt.Errorf("snapshot row payload must be present")
	}
	return nil
}

func buildSnapshotSessionCreateURL(base string) string {
	return fmt.Sprintf("%s/sync/snapshot-sessions", base)
}

func buildSnapshotChunkURL(base, snapshotID string, afterRowOrdinal int64, maxRows int) string {
	values := url.Values{}
	values.Set("after_row_ordinal", strconv.FormatInt(afterRowOrdinal, 10))
	if maxRows > 0 {
		values.Set("max_rows", strconv.Itoa(maxRows))
	}
	return fmt.Sprintf("%s/sync/snapshot-sessions/%s?%s", base, url.PathEscape(snapshotID), values.Encode())
}

func buildSnapshotSessionDeleteURL(base, snapshotID string) string {
	return fmt.Sprintf("%s/sync/snapshot-sessions/%s", base, url.PathEscape(snapshotID))
}

func decodeServerErrorBody(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	resp, ok := decodeServerErrorResponse(body)
	if !ok {
		return string(body)
	}
	if strings.TrimSpace(resp.Message) != "" {
		return fmt.Sprintf("%s: %s", resp.Error, resp.Message)
	}
	return resp.Error
}

func decodeServerErrorResponse(body []byte) (oversync.ErrorResponse, bool) {
	if len(body) == 0 {
		return oversync.ErrorResponse{}, false
	}
	var resp oversync.ErrorResponse
	if err := json.Unmarshal(body, &resp); err == nil && strings.TrimSpace(resp.Error) != "" {
		return resp, true
	}
	return oversync.ErrorResponse{}, false
}

func decodeSourceRetiredResponse(body []byte) (oversync.SourceRetiredResponse, bool) {
	if len(body) == 0 {
		return oversync.SourceRetiredResponse{}, false
	}
	var resp oversync.SourceRetiredResponse
	if err := json.Unmarshal(body, &resp); err == nil && strings.TrimSpace(resp.Error) == "source_retired" {
		return resp, true
	}
	return oversync.SourceRetiredResponse{}, false
}

func buildPullURL(base string, afterBundleSeq int64, maxBundles int, targetBundleSeq int64) string {
	values := url.Values{}
	values.Set("after_bundle_seq", strconv.FormatInt(afterBundleSeq, 10))
	values.Set("max_bundles", strconv.Itoa(maxBundles))
	if targetBundleSeq > 0 {
		values.Set("target_bundle_seq", strconv.FormatInt(targetBundleSeq, 10))
	}
	return fmt.Sprintf("%s/sync/pull?%s", base, values.Encode())
}
