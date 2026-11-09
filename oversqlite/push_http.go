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
	// A journal durably rebound to a retained outcome holds evidence for the server's
	// earlier request, never for these frozen rows: continue the lost-freeze recovery.
	if j.Phase == "frozen" && j.OutcomeBytes > 0 && outbox.State == outboxStatePrepared && j.Binding.Operation.SourceBundleID == outbox.SourceBundleID && j.Binding.Operation.CanonicalRequestHash != outbox.CanonicalRequestHash {
		return nil, &retainedOutcomeMismatchError{}
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

// retainedOutcomeMismatchError means the server already retains a terminal outcome at
// the frozen sequence for a different request: an earlier freeze reached the server
// but its local commit was lost. The frozen request was never admitted.
type retainedOutcomeMismatchError struct{}

func (*retainedOutcomeMismatchError) Error() string {
	return "server retains a different terminal outcome for the frozen source sequence"
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

// fetchRetainedUploadOutcome reads this source's retained terminal outcome by sequence
// after push_outcome_mismatch. The lookup is read-only on the server; a nonterminal
// answer never proves noncommit and leaves the frozen request intact as a blocker.
func (c *Client) fetchRetainedUploadOutcome(ctx context.Context, j *uploadJournal) ([]byte, error) {
	o := j.Binding.Operation
	body, err := json.Marshal(oversync.PushOutcomeRetainedRequest{SourceBundleID: o.SourceBundleID})
	if err != nil {
		return nil, err
	}
	return withRetryValue(ctx, c.normalizedRetryPolicy(), "push_retained", func() ([]byte, error) {
		result, err := c.doAuthenticatedPushRequestForBinding(ctx, "push_retained", http.MethodPost, strings.TrimRight(j.Binding.Authority, "/")+"/sync/push-outcomes/retained", body, "application/json", c.discovery.Limits.MaxPushOutcomeDecodedBytes, snapshotControlBodyLimit, o, nil)
		if err != nil {
			return nil, err
		}
		if result.statusCode == http.StatusOK {
			return result.body, nil
		}
		if response, ok := decodeServerErrorResponse(result.body); ok {
			switch response.Error {
			case "push_outcome_absent", "push_outcome_pending", "push_outcome_mismatch", "push_outcome_acknowledged", "source_sequence_out_of_order":
				return nil, &UploadJournalIntegrityError{"retained outcome lookup refused: " + response.Error}
			}
		}
		if generationErr := decodeSchemaGenerationRequestError(result.statusCode, result.body, c.schemaGenerationProof); generationErr != nil {
			return nil, generationErr
		}
		return nil, &retryHTTPError{Operation: "push_retained", StatusCode: result.statusCode}
	})
}

// decodeRetainedJournalOutcome validates a retained outcome against the frozen source,
// generation and sequence. Its request hash and initialization token are discovered,
// never expected; the frozen rows are not evidence for its effect or its rejection.
func (c *Client) decodeRetainedJournalOutcome(ctx context.Context, raw []byte, j *uploadJournal, planned int64) (*oversync.PushOutcome, *committedPushBundle, []oversync.PushRequestRow, error) {
	if err := c.guardUploadJournal(ctx, c.db, j); err != nil {
		return nil, nil, nil, err
	}
	frozen := j.Binding.Operation
	if c.discovery == nil || c.discovery.SchemaGeneration.ID != frozen.GenerationID || c.discovery.SchemaGeneration.Fingerprint != frozen.GenerationFingerprint {
		return nil, nil, nil, &UploadJournalIntegrityError{"original generation unavailable"}
	}
	shape, err := responseAllocationShape(ctx, raw)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = c.growPushHTTPBuffers(ctx, 2*int64(len(raw))+65536, shape); err != nil {
		return nil, nil, nil, err
	}
	var outcome oversync.PushOutcome
	if err = json.Unmarshal(raw, &outcome); err != nil {
		return nil, nil, nil, err
	}
	discovered := frozen
	discovered.CanonicalRequestHash, discovered.InitializationID = outcome.CanonicalRequestHash, outcome.InitializationID
	if j.OutcomeBytes == 0 && sameUploadOperation(discovered, frozen) {
		return nil, nil, nil, &UploadJournalIntegrityError{"retained outcome matches the frozen request"}
	}
	if err = oversync.ValidatePushOutcomeBinding(outcome, discovered, c.discovery.SchemaGeneration); err != nil {
		return nil, nil, nil, err
	}
	if j.OutcomeBytes > 0 && outcome.OutcomeHash != j.OutcomeHash {
		return nil, nil, nil, &UploadJournalIntegrityError{"retained outcome hash changed"}
	}
	if outcome.Committed == nil {
		return &outcome, nil, nil, nil
	}
	e := outcome.Committed
	rows, err := oversync.ExpandCompactPushRows(c.discovery.SchemaGeneration, e.Rows)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = c.validateAdmittedResponseRows("push outcome", rows); err != nil {
		return nil, nil, nil, err
	}
	staged := make([]stagedPushBundleRow, 0, len(rows))
	logical := make([]protocolhash.BundleRow, 0, len(rows))
	for _, r := range rows {
		staged = append(staged, stagedPushBundleRow{SchemaName: r.Schema, TableName: r.Table, WireKey: r.Key})
		logical = append(logical, protocolhash.BundleRow{Schema: r.Schema, Table: r.Table, Key: r.Key, Op: r.Op, RowVersion: r.BaseRowVersion, Payload: r.Payload})
	}
	// No frozen row has to be covered; only reject a malformed duplicate effect stream.
	if err = verifyCommittedEffectCoverage(nil, staged); err != nil {
		return nil, nil, nil, err
	}
	_, bytes, err := protocolhash.CommittedBundle(logical)
	if err != nil {
		return nil, nil, nil, err
	}
	committed := &committedPushBundle{BundleSeq: e.BundleSeq, SourceID: outcome.SourceID, SourceBundleID: outcome.SourceBundleID, PlannedRowCount: planned, RowCount: e.RowCount, ByteCount: bytes, BundleHash: e.BundleHash, CanonicalRequestHash: outcome.CanonicalRequestHash, Retained: true}
	if err = validateCommittedPushBundleHardLimits(committed); err != nil {
		return nil, nil, nil, err
	}
	return &outcome, committed, rows, nil
}

// rebindUploadJournalToRetainedOutcome replaces the frozen request's hash and token
// with the retained outcome's and stores that outcome in one writer, so settlement
// and acknowledgment use the binding the server actually retains.
func (c *Client) rebindUploadJournalToRetainedOutcome(ctx context.Context, j *uploadJournal, raw []byte, outcome *oversync.PushOutcome) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUploadJournal(ctx, tx)
	if err != nil {
		return err
	}
	if current == nil || !sameUploadOperation(current.Binding.Operation, j.Binding.Operation) || current.Phase != "frozen" || current.OutcomeBytes != 0 {
		return &UploadJournalIntegrityError{"operation changed during retained outcome capture"}
	}
	if err = c.guardUploadJournal(ctx, tx, current); err != nil {
		return err
	}
	rebound := current.Binding
	rebound.Operation.CanonicalRequestHash = outcome.CanonicalRequestHash
	rebound.Operation.InitializationID = outcome.InitializationID
	binding, err := json.Marshal(rebound)
	if err != nil {
		return err
	}
	if len(binding) > 4096 {
		return &UploadJournalIntegrityError{"binding too large"}
	}
	if err = c.checkAggregateWorkspace(ctx, tx, "upload-outcome", nil, nil, workspaceCharge{0, int64(len(raw))}); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE _sync_upload_journal SET binding=?,outcome_body=?,outcome_hash=? WHERE singleton_key=1`, string(binding), raw, outcome.OutcomeHash); err != nil {
		return err
	}
	return tx.Commit(ctx)
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
		case "push_outcome_mismatch":
			return &retainedOutcomeMismatchError{}
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
