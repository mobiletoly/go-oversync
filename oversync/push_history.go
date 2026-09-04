// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

// PushInvalidError reports an accepted push envelope whose logical contents
// cannot be admitted. It intentionally carries no database detail.
type PushInvalidError struct {
	Message string
}

// PushReplayMismatchError reports reuse of a committed source tuple with a
// different canonical request. It is a deterministic conflict, not malformed
// logical input and not permission to attempt a second write.
type PushReplayMismatchError struct{}

func (*PushReplayMismatchError) Error() string {
	return "canonical_request_hash does not match the committed source tuple"
}

func (e *PushInvalidError) Error() string { return e.Message }

// PushFailedError is the safe public form of an otherwise unclassified write
// failure. Driver errors can disclose SQL, constraints, or application values.
type PushFailedError struct{}

func (*PushFailedError) Error() string { return "push failed" }

func sanitizePushError(err error) error {
	var applicationErr *ApplicationWriteRejectedError
	if errors.As(err, &applicationErr) || err == nil {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errServiceShuttingDown) || errors.Is(err, errServiceNotReady) {
		return err
	}
	var unknown *CommitOutcomeUnknownError
	if errors.As(err, &unknown) {
		return &CommitOutcomeUnknownError{}
	}
	var projectionErr *GenerationProjectionError
	if errors.As(err, &projectionErr) {
		return projectionErr
	}
	var unrepresentableErr *CommittedBundleUnrepresentableError
	if errors.As(err, &unrepresentableErr) {
		return unrepresentableErr
	}
	var coverageErr *PushEffectCoverageError
	if errors.As(err, &coverageErr) {
		return coverageErr
	}
	var invalidErr *PushInvalidError
	if errors.As(err, &invalidErr) {
		return invalidErr
	}
	var replayMismatchErr *PushReplayMismatchError
	if errors.As(err, &replayMismatchErr) {
		return replayMismatchErr
	}
	var validationErr *PushValidationError
	if errors.As(err, &validationErr) {
		return validationErr
	}
	var generationRequestErr *SchemaGenerationRequestError
	if errors.As(err, &generationRequestErr) {
		return generationRequestErr
	}
	var committedStateErr *CommittedPushStateError
	if errors.As(err, &committedStateErr) {
		return committedStateErr
	}
	var callbackErr *RetryableCallbackViolationError
	if errors.As(err, &callbackErr) {
		return callbackErr
	}
	var conflictErr *PushConflictError
	if errors.As(err, &conflictErr) {
		return conflictErr
	}
	var initializationExpiredErr *InitializationExpiredError
	if errors.As(err, &initializationExpiredErr) {
		return initializationExpiredErr
	}
	var initializationStaleErr *InitializationStaleError
	if errors.As(err, &initializationStaleErr) {
		return initializationStaleErr
	}
	var historyErr *SourceTupleHistoryPrunedError
	if errors.As(err, &historyErr) {
		return historyErr
	}
	var sequenceErr *SourceSequenceOutOfOrderError
	if errors.As(err, &sequenceErr) {
		return sequenceErr
	}
	var sequenceChangedErr *SourceSequenceChangedError
	if errors.As(err, &sequenceChangedErr) {
		return sequenceChangedErr
	}
	var uninitializedErr *ScopeUninitializedError
	if errors.As(err, &uninitializedErr) {
		return uninitializedErr
	}
	var initializingErr *ScopeInitializingError
	if errors.As(err, &initializingErr) {
		return initializingErr
	}
	var sourceGenerationErr *SourceGenerationError
	if errors.As(err, &sourceGenerationErr) {
		return sourceGenerationErr
	}
	var sourceRetiredErr *SourceRetiredError
	if errors.As(err, &sourceRetiredErr) {
		return sourceRetiredErr
	}
	var safeErr *PushFailedError
	if errors.As(err, &safeErr) {
		return safeErr
	}
	return &PushFailedError{}
}

type SourceTupleHistoryPrunedError struct {
	UserID                         string
	SourceID                       string
	SourceBundleID                 int64
	MaxCommittedSourceBundleIDHint int64
}

func (e *SourceTupleHistoryPrunedError) Error() string { return "source history is no longer retained" }

type SourceSequenceOutOfOrderError struct {
	UserID   string
	SourceID string
	Expected int64
	Actual   int64
}

func (e *SourceSequenceOutOfOrderError) Error() string { return "source sequence is out of order" }

type SourceSequenceChangedError struct {
	UserID   string
	SourceID string
	Expected int64
	Actual   int64
}

func (e *SourceSequenceChangedError) Error() string { return "source sequence changed before commit" }

type committedBundleMeta struct {
	BundleSeq            int64
	SourceID             string
	SourceBundleID       int64
	RowCount             int64
	PlannedRowCount      *int64
	BundleHash           string
	CanonicalRequestHash string
}

func enforcePushRequestByteLimit(proof SchemaGenerationProof, rows []pushPreparedRow, limit int64) error {
	hasher := protocolhash.NewGenerationPinnedPushRequestHasher(canonicalSchemaGenerationID(proof.ID), proof.Fingerprint)
	for _, row := range rows {
		if err := hasher.Add(protocolhash.PushRow{
			Schema: row.schema, Table: row.table, Key: map[string]any{row.keyColumn: row.keyString},
			Op: row.op, BaseRowVersion: row.baseRowVersion, Payload: row.requestPayload,
		}); err != nil {
			return fmt.Errorf("compute canonical push request size: %w", err)
		}
		if hasher.CurrentByteCount() > limit {
			return projectionError(BundleLimitExceeded, "push", "push request exceeds configured bundle limits")
		}
	}
	return nil
}

func preflightPushPayloadDestinations(ctx context.Context, tx pgx.Tx, rows []pushPreparedRow) error {
	rowsByTable := make(map[string][]pushPreparedRow)
	for _, row := range rows {
		if row.op != OpDelete {
			rowsByTable[Key(row.schema, row.table)] = append(rowsByTable[Key(row.schema, row.table)], row)
		}
	}
	tableKeys := make([]string, 0, len(rowsByTable))
	for tableKey := range rowsByTable {
		tableKeys = append(tableKeys, tableKey)
	}
	sort.Strings(tableKeys)
	for _, tableKey := range tableKeys {
		tableRows := rowsByTable[tableKey]
		payloadArray, err := marshalPayloadJSONArray(tableRows, "")
		if err != nil {
			return fmt.Errorf("marshal destination preflight payload for %s: %w", tableKey, err)
		}
		stmt := fmt.Sprintf(`SELECT count(*) FROM jsonb_populate_recordset(NULL::%s, $1::jsonb)`, pgx.Identifier{tableRows[0].schema, tableRows[0].table}.Sanitize())
		var convertedRows int64
		if err := tx.QueryRow(ctx, stmt, payloadArray).Scan(&convertedRows); err != nil {
			return &PushInvalidError{Message: fmt.Sprintf("payload for %s contains a value invalid for its PostgreSQL destination type (category=destination_type_invalid)", tableKey)}
		}
	}
	return nil
}

func loadCommittedPushMetadataBySourceTuple(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, sourceBundleID int64) (*committedBundleMeta, error) {
	if err := sourceid.Validate(sourceID); err != nil {
		return nil, fmt.Errorf("committed bundle lookup source_id is invalid: %w", err)
	}
	var meta committedBundleMeta
	var bundleHash []byte
	if err := tx.QueryRow(ctx, `
		SELECT bl.bundle_seq, bl.source_id, bl.source_bundle_id, bl.row_count, bl.planned_row_count, bl.bundle_hash, bl.canonical_request_hash
		FROM sync.bundle_log AS bl
		JOIN sync.user_state AS us ON us.user_pk = bl.user_pk
		WHERE bl.user_pk = $1 AND bl.source_id = $2 AND bl.source_bundle_id = $3 AND bl.bundle_seq > us.retained_bundle_floor
	`, userPK, sourceID, sourceBundleID).Scan(&meta.BundleSeq, &meta.SourceID, &meta.SourceBundleID, &meta.RowCount, &meta.PlannedRowCount, &bundleHash, &meta.CanonicalRequestHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query committed push metadata: %w", err)
	}
	if err := sourceid.Validate(meta.SourceID); err != nil {
		return nil, fmt.Errorf("persisted bundle_log source_id is invalid: %w", err)
	}
	meta.BundleHash = renderBundleHash(bundleHash)
	return &meta, nil
}

func loadNextExpectedSourceBundleIDForUpdate(ctx context.Context, tx pgx.Tx, userPK int64, userID, sourceID string) (int64, int64, error) {
	state, err := loadSourceStateRow(ctx, tx, userPK, sourceID, true)
	if err != nil {
		return 0, 0, fmt.Errorf("query source_state next expected bundle for update: %w", err)
	}
	if state == nil {
		return 1, 0, nil
	}
	if state.State == sourceStateRetired {
		return 0, 0, &SourceRetiredError{UserID: userID, SourceID: sourceID, ReplacedBySourceID: state.ReplacedBySourceID}
	}
	return state.MaxCommittedSourceBundleID + 1, state.MaxCommittedSourceBundleID, nil
}
