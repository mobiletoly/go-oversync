package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

type DirtyStateRejectedError struct {
	DirtyCount int
}

func (e *DirtyStateRejectedError) Error() string {
	return fmt.Sprintf("cannot pull while %d local dirty rows exist", e.DirtyCount)
}

func (c *Client) pullToStableLocked(ctx context.Context) error {
	rebuildRequired, err := c.rebuildRequired(ctx)
	if err != nil {
		return err
	}
	if rebuildRequired {
		return &RebuildRequiredError{}
	}
	outboundCount, err := c.pendingPushOutboundCount(ctx)
	if err != nil {
		return err
	}
	if outboundCount > 0 {
		return &PendingPushReplayError{OutboundCount: outboundCount}
	}

	pendingCount, err := c.pendingChangeCount(ctx)
	if err != nil {
		return err
	}
	if pendingCount > 0 {
		c.logger.Warn("pull rejected due to local dirty state", "dirty_rows", pendingCount)
		return &DirtyStateRejectedError{DirtyCount: pendingCount}
	}

	afterBundleSeq, err := c.lastSeenBundleSeq(ctx)
	if err != nil {
		return err
	}
	maxBundles := c.config.DownloadLimit
	if maxBundles <= 0 {
		maxBundles = 1000
	}

	targetBundleSeq := int64(0)
	for {
		resp, err := c.sendPullRequest(ctx, afterBundleSeq, maxBundles, targetBundleSeq)
		if err != nil {
			var prunedErr *HistoryPrunedError
			if errors.As(err, &prunedErr) {
				c.logger.Info("pull history pruned; rebuilding from chunked snapshot", "message", prunedErr.Message)
				return c.hydrateLocked(ctx)
			}
			return err
		}
		if targetBundleSeq == 0 {
			targetBundleSeq = resp.StableBundleSeq
		} else if resp.StableBundleSeq != targetBundleSeq {
			return fmt.Errorf("pull response stable bundle seq changed from %d to %d", targetBundleSeq, resp.StableBundleSeq)
		}

		for _, bundle := range resp.Bundles {
			if err := c.applyPulledBundleLocked(ctx, &bundle); err != nil {
				return err
			}
			afterBundleSeq = bundle.BundleSeq
		}

		if afterBundleSeq >= resp.StableBundleSeq {
			return nil
		}
		if !resp.HasMore && len(resp.Bundles) == 0 {
			return fmt.Errorf("pull ended before reaching stable bundle seq %d", resp.StableBundleSeq)
		}
		if !resp.HasMore && afterBundleSeq < resp.StableBundleSeq {
			return fmt.Errorf("pull ended early at bundle seq %d before stable bundle seq %d", afterBundleSeq, resp.StableBundleSeq)
		}
	}
}

func (c *Client) sendPullRequest(ctx context.Context, afterBundleSeq int64, maxBundles int, targetBundleSeq int64) (*oversync.PullResponse, error) {
	endpoint := fmt.Sprintf("%s/sync/pull?after_bundle_seq=%d&max_bundles=%d", c.BaseURL, afterBundleSeq, maxBundles)
	if targetBundleSeq > 0 {
		endpoint += "&target_bundle_seq=" + strconv.FormatInt(targetBundleSeq, 10)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create pull request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send pull request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusConflict {
			var errorResp oversync.ErrorResponse
			if json.Unmarshal(body, &errorResp) == nil && errorResp.Error == "history_pruned" {
				return nil, &HistoryPrunedError{
					Status:  resp.StatusCode,
					Message: errorResp.Message,
				}
			}
		}
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
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
		if err := c.applyBundleRowAuthoritativelyInTx(ctx, tx, &row, localPK); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM _sync_dirty_rows
			WHERE schema_name = ? AND table_name = ? AND key_json = ?
		`, row.Schema, row.Table, keyJSON); err != nil {
			return fmt.Errorf("failed to clear dirty row after pulled bundle apply for %s.%s %s: %w", row.Schema, row.Table, keyJSON, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE _sync_client_state
		SET last_bundle_seq_seen = CASE
				WHEN last_bundle_seq_seen < ? THEN ?
				ELSE last_bundle_seq_seen
			END
		WHERE user_id = ?
	`, bundle.BundleSeq, bundle.BundleSeq, c.UserID); err != nil {
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
	var seq int64
	if err := c.DB.QueryRowContext(ctx, `
		SELECT last_bundle_seq_seen
		FROM _sync_client_state
		WHERE user_id = ?
	`, c.UserID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("failed to read last_bundle_seq_seen: %w", err)
	}
	return seq, nil
}

// LastBundleSeqSeen exposes the durable pulled-bundle checkpoint for diagnostics.
func (c *Client) LastBundleSeqSeen(ctx context.Context) (int64, error) {
	return c.lastSeenBundleSeq(ctx)
}

func (c *Client) hydrateLocked(ctx context.Context) error {
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
	return c.rebuildFromSnapshotLocked(ctx, false)
}

func (c *Client) Recover(ctx context.Context) error {
	if atomicLoadPaused(&c.downloadPaused) {
		return nil
	}
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	return c.recoverLocked(ctx)
}

func (c *Client) recoverLocked(ctx context.Context) error {
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
	return c.rebuildFromSnapshotLocked(ctx, true)
}

func (c *Client) rebuildFromSnapshotLocked(ctx context.Context, rotateSource bool) error {
	if err := c.setRebuildRequired(ctx, true); err != nil {
		return err
	}
	if err := c.clearSnapshotStage(ctx); err != nil {
		return err
	}

	session, err := c.createSnapshotSession(ctx)
	if err != nil {
		return err
	}
	defer c.deleteSnapshotSessionBestEffort(context.Background(), session.SnapshotID)

	afterRowOrdinal := int64(0)
	for {
		chunk, err := c.fetchSnapshotChunk(ctx, session.SnapshotID, session.SnapshotBundleSeq, afterRowOrdinal, c.snapshotChunkRows())
		if err != nil {
			return err
		}
		if err := c.stageSnapshotChunk(ctx, chunk, afterRowOrdinal); err != nil {
			return err
		}
		if !chunk.HasMore {
			break
		}
		afterRowOrdinal = chunk.NextRowOrdinal
	}

	return c.applyStagedSnapshotLocked(ctx, session, rotateSource)
}

func (c *Client) snapshotChunkRows() int {
	if c != nil && c.config != nil && c.config.SnapshotChunkRows > 0 {
		return c.config.SnapshotChunkRows
	}
	return 1000
}

func (c *Client) createSnapshotSession(ctx context.Context) (*oversync.SnapshotSession, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, buildSnapshotSessionCreateURL(c.BaseURL), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot session request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send snapshot session request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, decodeServerErrorBody(body))
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
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, buildSnapshotChunkURL(c.BaseURL, snapshotID, afterRowOrdinal, maxRows), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot chunk request: %w", err)
	}
	token, err := c.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWT token: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send snapshot chunk request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, decodeServerErrorBody(body))
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
	httpReq.Header.Set("Authorization", "Bearer "+token)

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

func (c *Client) applyStagedSnapshotLocked(ctx context.Context, session *oversync.SnapshotSession, rotateSource bool) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin staged snapshot apply transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for staged snapshot apply: %w", err)
	}
	if err := c.setBundleApplyModeInTx(ctx, tx, true); err != nil {
		return err
	}
	if err := c.clearManagedTablesInTx(ctx, tx); err != nil {
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
		if err := c.applyBundleRowAuthoritativelyInTx(ctx, tx, &bundleRow, localPK); err != nil {
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

	newSourceID := c.SourceID
	if rotateSource {
		newSourceID = uuid.NewString()
		if _, err := tx.ExecContext(ctx, `
			UPDATE _sync_client_state
			SET source_id = ?, next_source_bundle_id = 1, last_bundle_seq_seen = ?, rebuild_required = 0
			WHERE user_id = ?
		`, newSourceID, session.SnapshotBundleSeq, c.UserID); err != nil {
			return fmt.Errorf("failed to persist recovery client bundle state: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			UPDATE _sync_client_state
			SET last_bundle_seq_seen = ?, rebuild_required = 0
			WHERE user_id = ?
		`, session.SnapshotBundleSeq, c.UserID); err != nil {
			return fmt.Errorf("failed to persist snapshot bundle checkpoint: %w", err)
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
	if rotateSource {
		c.SourceID = newSourceID
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
	var resp oversync.ErrorResponse
	if err := json.Unmarshal(body, &resp); err == nil && strings.TrimSpace(resp.Error) != "" {
		if strings.TrimSpace(resp.Message) != "" {
			return fmt.Sprintf("%s: %s", resp.Error, resp.Message)
		}
		return resp.Error
	}
	return string(body)
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
