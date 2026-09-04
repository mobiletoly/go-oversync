// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

// commitPushOutboundSnapshot sends the entire frozen outbox exactly once. A
// response loss is recovered by replaying the same source tuple and complete
// request; no upload locator, chunk cursor, or result-page cursor is stored.
func (c *Client) commitPushOutboundSnapshot(ctx context.Context, snapshot *pushOutboundSnapshot) (*committedPushBundle, error) {
	if snapshot == nil || len(snapshot.Rows) == 0 || c == nil || c.discovery == nil {
		return nil, fmt.Errorf("complete push requires a frozen outbox and admitted discovery contract")
	}
	outbox, err := loadOutboxBundle(ctx, c.db)
	if err != nil {
		return nil, err
	}
	if outbox.State != outboxStatePrepared || outbox.SourceBundleID != snapshot.SourceBundleID || outbox.RowCount != int64(len(snapshot.Rows)) {
		return nil, fmt.Errorf("frozen outbox changed before whole-request push")
	}
	rows, err := c.buildPushRequestRows(snapshot.Rows)
	if err != nil {
		return nil, err
	}
	if len(rows) > c.discovery.Limits.MaxPushRows {
		return nil, &InvalidAdmittedContractError{Message: "frozen outbox exceeds max_push_rows"}
	}
	for _, row := range rows {
		rowBytes, sizeErr := oversync.ExpandedRowDecodedBytes(row)
		if sizeErr != nil {
			return nil, sizeErr
		}
		if rowBytes > c.discovery.Limits.MaxRowDecodedBytes {
			return nil, &PushRowExceedsAdmittedLimitError{Limit: c.discovery.Limits.MaxRowDecodedBytes}
		}
	}
	groups, err := oversync.CompactGroupsForRows(c.discovery.SchemaGeneration, rows)
	if err != nil {
		return nil, fmt.Errorf("compact frozen outbox: %w", err)
	}
	body, err := json.Marshal(oversync.CompactPushRequest{
		SourceBundleID: outbox.SourceBundleID, CanonicalRequestHash: outbox.CanonicalRequestHash,
		InitializationID: outbox.InitializationID, Rows: groups,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal complete push request: %w", err)
	}
	if int64(len(body)) > c.discovery.Limits.MaxPushDecodedBytes {
		return nil, &InvalidAdmittedContractError{Message: "frozen outbox exceeds max_push_decoded_bytes"}
	}
	if err := c.runBeforePushCommitHook(ctx); err != nil {
		return nil, err
	}
	result, err := c.doAuthenticatedPushRequest(ctx, "push", http.MethodPost, buildPushURL(c.BaseURL), body, "application/json", c.discovery.Limits.MaxPushDecodedBytes, snapshotControlBodyLimit, nil)
	if err != nil {
		return nil, err
	}
	if result.statusCode != http.StatusOK {
		return nil, c.decodeCompletePushError(ctx, result.statusCode, result.body)
	}
	committed, authoritative, err := c.decodeCommittedPushResponse(ctx, result.body, outbox)
	if err != nil {
		return nil, err
	}
	if err := c.stageAuthoritativePushRows(ctx, committed, authoritative); err != nil {
		return nil, err
	}
	c.pendingInitializationID = ""
	if err := c.clearPersistedPendingInitializationID(ctx); err != nil {
		return nil, err
	}
	return committed, nil
}

// Admit the response representation, then verify its complete authority before
// the caller stages any effects or clears initialization metadata.
func (c *Client) decodeCommittedPushResponse(ctx context.Context, body []byte, outbox *outboxBundleRecord) (*committedPushBundle, []oversync.PushRequestRow, error) {
	shape, err := responseAllocationShape(ctx, body)
	if err != nil {
		return nil, nil, err
	}
	if err = c.growPushHTTPBuffers(ctx, 2*int64(len(body))+65536, shape); err != nil {
		return nil, nil, err
	}
	if _, err := requireJSONObjectFields(body, "complete push", "bundle_seq", "bundle_hash", "rows"); err != nil {
		return nil, nil, err
	}
	var response oversync.CompactPushResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, nil, fmt.Errorf("decode complete push response: %w", err)
	}
	if response.BundleSeq < 1 || !protocolhash.IsCanonicalHash(response.BundleHash) {
		return nil, nil, fmt.Errorf("complete push response has invalid bundle identity")
	}
	authoritative, err := oversync.ExpandCompactPushRows(c.discovery.SchemaGeneration, response.Rows)
	if err != nil {
		return nil, nil, fmt.Errorf("expand complete push response: %w", err)
	}
	if err := c.validateAdmittedResponseRows("push response", authoritative); err != nil {
		return nil, nil, err
	}
	if len(authoritative) == 0 {
		return nil, nil, fmt.Errorf("complete push response has no authoritative rows")
	}
	logicalRows := make([]protocolhash.BundleRow, 0, len(authoritative))
	for _, row := range authoritative {
		logicalRows = append(logicalRows, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.BaseRowVersion, Payload: row.Payload})
	}
	computedHash, byteCount, err := protocolhash.CommittedBundle(logicalRows)
	if err != nil || computedHash != response.BundleHash {
		return nil, nil, fmt.Errorf("complete push response hash does not match authoritative rows")
	}
	committed := &committedPushBundle{BundleSeq: response.BundleSeq, SourceID: c.sourceID, SourceBundleID: outbox.SourceBundleID, PlannedRowCount: outbox.RowCount, RowCount: int64(len(authoritative)), ByteCount: byteCount, BundleHash: response.BundleHash, CanonicalRequestHash: outbox.CanonicalRequestHash}
	if err := validateCommittedPushBundleHardLimits(committed); err != nil {
		return nil, nil, err
	}
	return committed, authoritative, nil
}

func (c *Client) decodeCompletePushError(ctx context.Context, status int, body []byte) error {
	if generationErr := decodeSchemaGenerationRequestError(status, body, c.schemaGenerationProof); generationErr != nil {
		return generationErr
	}
	if response, ok := decodeServerErrorResponse(body); ok {
		switch {
		case status == http.StatusConflict && response.Error == "push_replay_mismatch":
			return &PushReplayMismatchError{}
		case status == http.StatusUnprocessableEntity && response.Error == "bundle_projection_too_large":
			return &BundleProjectionTooLargeError{}
		case status == http.StatusUnprocessableEntity:
			return decodePushApplicationRejection("push", status, body)
		}
	}
	if initErr, handled := c.handleInitializationLeaseErrorLocked(ctx, status, body); handled {
		return initErr
	}
	if conflictErr := decodePushConflictError(status, body); conflictErr != nil {
		return conflictErr
	}
	if response, ok := decodeServerErrorResponse(body); ok && status == http.StatusConflict {
		switch response.Error {
		case "history_pruned":
			return c.beginSourceRecoveryLocked(ctx, SourceRecoveryHistoryPruned, response.Message, "")
		case "source_sequence_out_of_order":
			return c.beginSourceRecoveryLocked(ctx, SourceRecoverySequenceOutOfOrder, response.Message, "")
		case "source_sequence_changed":
			return c.beginSourceRecoveryLocked(ctx, SourceRecoverySequenceChanged, response.Message, "")
		case "source_retired":
			if retired, ok := decodeSourceRetiredResponse(body, c.sourceID); ok {
				return c.beginSourceRecoveryLocked(ctx, SourceRecoveryRetired, "", retired.ReplacedBySourceID)
			}
		}
	}
	return &retryHTTPError{Operation: "push", StatusCode: status}
}

func decodePushApplicationRejection(operation string, status int, body []byte) error {
	response, ok := decodeServerErrorResponse(body)
	if !ok || response.Error == "" {
		return &PushApplicationRejectionError{Operation: operation, Status: status, Category: "invalid_422_response", Message: "server returned an invalid application rejection"}
	}
	if response.Error == "push_effect_coverage_incomplete" {
		return &PushEffectCoverageError{Operation: operation, Status: status, Message: response.Message}
	}
	return &PushApplicationRejectionError{Operation: operation, Status: status, Category: response.Error, Message: response.Message}
}

func (c *Client) handleInitializationLeaseErrorLocked(ctx context.Context, statusCode int, body []byte) (error, bool) {
	resp, ok := decodeServerErrorResponse(body)
	if !ok || resp.Error != "initialization_expired" && resp.Error != "initialization_stale" {
		return nil, false
	}
	if err := c.clearStalePendingInitializationStateLocked(ctx); err != nil {
		return fmt.Errorf("clear stale initialization state after %s: %w", resp.Error, err), true
	}
	return fmt.Errorf("server returned status %d: %s; reconnect required", statusCode, decodeServerErrorBody(body)), true
}

func (c *Client) doAuthenticatedPushRequest(ctx context.Context, operation, method, endpoint string, requestBody []byte, contentType string, successLimit, errorLimit int64, onDecodedBodyRead func(int64)) (boundedResponsePayload, error) {
	policy := c.normalizedPushCapacityRetryPolicy()
	started := time.Now()
	for {
		result, err := c.doAuthenticatedBoundedRequest(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, onDecodedBodyRead)
		if err != nil || !isHistoryProjectionCapacityResponse(result) {
			return result, err
		}
		atomic.AddInt64(&c.pushStats.capacityResponses, 1)
		waited, delay := time.Since(started), parseRetryAfterDeltaSeconds(result.retryAfter)
		if delay <= 0 {
			delay = policy.fallbackDelay
		}
		delay = positiveJitterDuration(delay, policy.jitterFraction)
		if !policy.enabled || delay <= 0 || waited >= policy.maxWait || delay > policy.maxWait-waited {
			return boundedResponsePayload{}, &PushCapacityRetryExhaustedError{Operation: operation, ErrorCode: "history_projection_capacity_exceeded", Waited: waited}
		}
		atomic.AddInt64(&c.pushStats.capacityRetries, 1)
		waitStarted := time.Now()
		if err := waitRetryBackoff(ctx, delay); err != nil {
			return boundedResponsePayload{}, err
		}
		atomic.AddInt64(&c.pushStats.capacityWaitNanos, int64(time.Since(waitStarted)))
	}
}

func isHistoryProjectionCapacityResponse(result boundedResponsePayload) bool {
	if result.statusCode != http.StatusServiceUnavailable {
		return false
	}
	response, ok := decodeServerErrorResponse(result.body)
	return ok && response.Error == "history_projection_capacity_exceeded"
}

func buildPushURL(base string) string { return strings.TrimRight(base, "/") + "/sync/push" }

func committedPushBundleFromFields(bundleSeq int64, sourceID string, sourceBundleID, plannedRowCount, rowCount, byteCount int64, bundleHash, canonicalRequestHash string) (*committedPushBundle, error) {
	committed := &committedPushBundle{BundleSeq: bundleSeq, SourceID: sourceID, SourceBundleID: sourceBundleID, PlannedRowCount: plannedRowCount, RowCount: rowCount, ByteCount: byteCount, BundleHash: bundleHash, CanonicalRequestHash: canonicalRequestHash}
	if err := validateCommittedPushBundleHardLimits(committed); err != nil {
		return nil, err
	}
	return committed, nil
}

func validateCommittedPushBundleHardLimits(committed *committedPushBundle) error {
	if committed == nil || committed.PlannedRowCount < 1 || committed.PlannedRowCount > maxCommittedPushBundleRows || committed.RowCount < 1 || committed.RowCount > maxCommittedPushBundleRows || committed.ByteCount < 2 || committed.ByteCount > maxCommittedPushBundleBytes || committed.BundleSeq < 1 || !protocolhash.IsCanonicalHash(committed.BundleHash) || committed.CanonicalRequestHash == "" {
		return fmt.Errorf("committed push bundle is outside protocol bounds")
	}
	return nil
}
