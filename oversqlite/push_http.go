// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

// An existing frozen request is conservatively possibly dispatched. Only a
// fresh freeze sends push first; resumed work resolves the exact original tuple.
func (c *Client) commitPushOutboundSnapshot(ctx context.Context, snapshot *pushOutboundSnapshot) (*committedPushBundle, error) {
	if snapshot == nil || len(snapshot.Rows) == 0 || c.discovery == nil {
		return nil, fmt.Errorf("complete push requires frozen rows and an admitted generation")
	}
	outbox, err := loadOutboxBundle(ctx, c.db)
	if err != nil {
		return nil, err
	}
	j, err := loadUploadJournal(ctx, c.db)
	if err != nil {
		return nil, err
	}
	if err = c.guardUploadJournal(ctx, c.db, j); err != nil {
		return nil, err
	}
	if j.Phase != "frozen" || outbox.State != outboxStatePrepared || outbox.SourceBundleID != snapshot.SourceBundleID || outbox.RowCount != int64(len(snapshot.Rows)) || j.Binding.Operation.SourceBundleID != outbox.SourceBundleID || j.Binding.Operation.CanonicalRequestHash != outbox.CanonicalRequestHash {
		return nil, &UploadJournalIntegrityError{"frozen outbox changed"}
	}
	requestBody, err := c.journalRequestBody(ctx, j)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if j.OutcomeBytes > 0 {
		if err = c.growPushHTTPBuffers(ctx, 2*j.OutcomeBytes+65536, 32); err != nil {
			return nil, err
		}
		err = c.db.QueryRowContext(ctx, `SELECT outcome_body FROM _sync_upload_journal WHERE singleton_key=1`).Scan(&raw)
	} else {
		raw, err = c.fetchUploadOutcome(ctx, snapshot, j, requestBody)
	}
	if err != nil {
		return nil, err
	}
	outcome, committed, rows, err := c.decodeJournalOutcome(ctx, raw, j, snapshot.Rows)
	if err != nil {
		return nil, err
	}
	if j.OutcomeBytes == 0 {
		if err = c.persistUploadOutcome(ctx, j, raw, outcome); err != nil {
			return nil, err
		}
	}
	switch outcome.Status {
	case "cancelled":
		return nil, &cancelledUploadOutcome{}
	case "rejected":
		return nil, terminalPushError(outcome)
	}
	if err = c.stageAuthoritativePushRows(ctx, committed, rows); err != nil {
		return nil, err
	}
	return committed, nil
}

type cancelledUploadOutcome struct{}

func (*cancelledUploadOutcome) Error() string {
	return "upload was durably cancelled; local intent remains pending"
}

func (c *Client) fetchUploadOutcome(ctx context.Context, snapshot *pushOutboundSnapshot, j *uploadJournal, frozenBody []byte) ([]byte, error) {
	var body []byte
	var err error
	operation, path := "push", "/sync/push"
	if snapshot.Fresh {
		body = frozenBody
	} else {
		operation, path = "push_resolve", "/sync/push-outcomes/resolve"
		o := j.Binding.Operation
		body, err = json.Marshal(oversync.PushOutcomeResolveRequest{SourceBundleID: o.SourceBundleID, CanonicalRequestHash: o.CanonicalRequestHash, InitializationID: o.InitializationID})
	}
	if err != nil {
		return nil, err
	}
	if snapshot.Fresh {
		if err = c.runBeforePushCommitHook(ctx); err != nil {
			return nil, err
		}
	}
	result, err := c.doAuthenticatedPushRequestForBinding(ctx, operation, http.MethodPost, strings.TrimRight(j.Binding.Authority, "/")+path, body, "application/json", c.discovery.Limits.MaxPushOutcomeDecodedBytes, snapshotControlBodyLimit, j.Binding.Operation, nil)
	if err != nil {
		return nil, err
	}
	if result.statusCode != http.StatusOK {
		return nil, c.decodeCompletePushError(ctx, result.statusCode, result.body)
	}
	return result.body, nil
}

// Kept as the owning decoder entry point for pure boundary consumers.
func (c *Client) decodeCommittedPushResponse(ctx context.Context, body []byte, outbox *outboxBundleRecord) (*committedPushBundle, []oversync.PushRequestRow, error) {
	j, err := loadUploadJournal(ctx, c.db)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := c.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	if snapshot == nil {
		return nil, nil, &UploadJournalIntegrityError{"missing frozen rows"}
	}
	outcome, committed, rows, err := c.decodeJournalOutcome(ctx, body, j, snapshot.Rows)
	if err != nil {
		return nil, nil, err
	}
	if outcome.Status != "committed" {
		return nil, nil, fmt.Errorf("expected committed terminal outcome")
	}
	return committed, rows, nil
}

func (c *Client) decodeCompletePushError(ctx context.Context, status int, body []byte) error {
	if generationErr := decodeSchemaGenerationRequestError(status, body, c.schemaGenerationProof); generationErr != nil {
		return generationErr
	}
	if response, ok := decodeServerErrorResponse(body); ok &&
		((status == http.StatusUnprocessableEntity && response.Error == "bundle_projection_too_large") ||
			(status == http.StatusRequestEntityTooLarge && response.Error == "push_outcome_too_large")) {
		// Intrinsic capacity stops automatic retries without disposing F.
		return &BundleProjectionTooLargeError{}
	}
	if response, ok := decodeServerErrorResponse(body); ok && status == http.StatusGone && response.Error == "push_outcome_acknowledged" {
		return &UploadJournalIntegrityError{"server acknowledged operation without local disposition"}
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

func (c *Client) doAuthenticatedPushRequest(ctx context.Context, operation, method, endpoint string, requestBody []byte, contentType string, successLimit, errorLimit int64, onDecodedBodyRead func(int64)) (boundedResponsePayload, error) {
	return c.doAuthenticatedPushRequestForBinding(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, oversync.PushOperationBinding{SourceID: c.sourceID, GenerationID: strconv.FormatInt(int64(c.schemaGenerationProof.ID), 10), GenerationFingerprint: c.schemaGenerationProof.Fingerprint}, onDecodedBodyRead)
}

func (c *Client) doAuthenticatedPushRequestForBinding(ctx context.Context, operation, method, endpoint string, requestBody []byte, contentType string, successLimit, errorLimit int64, binding oversync.PushOperationBinding, onDecodedBodyRead func(int64)) (boundedResponsePayload, error) {
	generationID, err := strconv.ParseInt(binding.GenerationID, 10, 32)
	if err != nil {
		return boundedResponsePayload{}, err
	}
	policy := c.normalizedPushCapacityRetryPolicy()
	started := time.Now()
	for {
		result, err := c.doAuthenticatedBoundedRequestForBinding(ctx, operation, method, endpoint, requestBody, contentType, successLimit, errorLimit, binding.SourceID, oversync.SchemaGenerationProof{ID: oversync.SchemaGenerationID(generationID), Fingerprint: binding.GenerationFingerprint}, onDecodedBodyRead)
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
