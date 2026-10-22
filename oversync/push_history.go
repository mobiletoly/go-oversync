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
)

// PushInvalidError reports an accepted push envelope whose logical contents
// cannot be admitted. It intentionally carries no database detail.
type PushInvalidError struct {
	Message string
}

func (e *PushInvalidError) Error() string { return e.Message }

// PushFailedError is the safe public form of an otherwise unclassified write
// failure. Driver errors can disclose SQL, constraints, or application values.
type PushFailedError struct{}

func (*PushFailedError) Error() string { return "push failed" }

func sanitizePushError(err error) error {
	var denied *ScopeAccessDeniedError
	var unavailable *ScopeAdmissionUnavailableError
	if errors.As(err, &denied) || errors.As(err, &unavailable) {
		return err
	}
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
