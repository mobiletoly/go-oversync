// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/wirevalue"
)

// These private boundaries make the validated proof and immutable joined view
// an explicit capability. history will replace their canonical-surface bodies
// with generation-visible admission and projection.

func validateGenerationSelection(selection generationSelection) error {
	if selection.view == nil || selection.proof.ID != selection.view.contract.ID() || selection.proof.Fingerprint != selection.view.contract.Fingerprint() {
		return fmt.Errorf("invalid generation data-plane selection")
	}
	if !selection.view.dataPlaneReady {
		return &SchemaGenerationRequestError{Code: SchemaGenerationUnsupported, GenerationID: selection.proof.ID, Message: "schema generation data plane is not supported"}
	}
	return nil
}

func (s *SyncService) preparePushRowsForGeneration(selection generationSelection, rows []PushRequestRow) ([]pushPreparedRow, error) {
	if err := validateGenerationSelection(selection); err != nil {
		return nil, err
	}
	if err := validateGenerationPushRows(selection.view.contract, rows); err != nil {
		return nil, err
	}
	return s.preparePushRowsPreservingInput(rows)
}

func validateGenerationPushRows(contract SchemaGenerationContract, rows []PushRequestRow) error {
	tables := make(map[string]GenerationTableContract)
	for _, table := range contract.Tables() {
		tables[table.Schema+"\x00"+table.Table] = table
	}
	for _, row := range rows {
		table, ok := tables[row.Schema+"\x00"+row.Table]
		if !ok {
			return &PushValidationError{Message: fmt.Sprintf("table %s.%s is not present in schema generation %d", row.Schema, row.Table, contract.ID())}
		}
		if row.Op != OpInsert && row.Op != OpUpdate && row.Op != OpDelete {
			return &PushValidationError{Message: fmt.Sprintf("invalid op %q", row.Op)}
		}
		if len(table.SyncKeyColumns) != 1 || len(row.Key) != 1 {
			return &PushValidationError{Message: fmt.Sprintf("row key for %s.%s must contain exactly the declared sync key", row.Schema, row.Table)}
		}
		columns := make(map[string]GenerationColumn, len(table.Columns))
		for _, column := range table.Columns {
			columns[column.Name] = column
		}
		keyName := table.SyncKeyColumns[0]
		keyValue, ok := row.Key[keyName]
		if !ok {
			return &PushValidationError{Message: fmt.Sprintf("row key for %s.%s must include %q", row.Schema, row.Table, keyName)}
		}
		if err := validateGenerationWireValue(columns[keyName], keyValue); err != nil {
			if columns[keyName].WireType == GenerationWireTypeUUID {
				return &PushValidationError{Message: fmt.Sprintf("row key %s.%s.%s must use canonical dashed lowercase UUID text", row.Schema, row.Table, keyName)}
			}
			return &PushValidationError{Message: fmt.Sprintf("row key %s.%s.%s violates its schema-generation wire contract: %v", row.Schema, row.Table, keyName, err)}
		}
		if row.Op == OpDelete {
			if len(row.Payload) != 0 {
				return &PushValidationError{Message: fmt.Sprintf("DELETE for %s.%s must not include payload", row.Schema, row.Table)}
			}
			continue
		}
		payload, err := jcs.DecodeObject(row.Payload)
		if err != nil {
			return &PushValidationError{Message: fmt.Sprintf("payload for %s.%s must be an exact JSON object: %v", row.Schema, row.Table, err)}
		}
		for name, value := range payload {
			if name == syncScopeColumnName {
				return &PushValidationError{Message: fmt.Sprintf("payload for %s.%s must not include hidden scope column %q", row.Schema, row.Table, syncScopeColumnName)}
			}
			column, ok := columns[name]
			if !ok {
				return &PushValidationError{Message: fmt.Sprintf("payload for %s.%s contains undeclared generation column %q", row.Schema, row.Table, name)}
			}
			if err := validateGenerationWireValue(column, value); err != nil {
				return generationWirePushError(row.Schema, row.Table, name, err)
			}
		}
	}
	return nil
}

func validateGenerationWireValue(column GenerationColumn, value any) error {
	if value == nil {
		if !column.Nullable {
			return fmt.Errorf("must not be null")
		}
		return nil
	}
	switch column.WireType {
	case GenerationWireTypeBoolean:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("must be a JSON Boolean")
		}
	case GenerationWireTypeInt64:
		raw, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a canonical signed integer JSON string (category=legacy_json_number)")
		}
		_, err := wirevalue.ParseInt64(raw)
		if err != nil {
			return err
		}
	case GenerationWireTypeDecimal:
		raw, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a bounded exact-decimal JSON string")
		}
		if err := wirevalue.ValidateDecimal(raw); err != nil {
			return err
		}
	case GenerationWireTypeFloat64:
		raw, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a canonical finite binary64 JSON string")
		}
		if _, err := wirevalue.ParseFloat64(raw); err != nil {
			return err
		}
	case GenerationWireTypeText:
		raw, ok := value.(string)
		if !ok || !utf8.ValidString(raw) || strings.IndexByte(raw, 0) >= 0 {
			return fmt.Errorf("must be a PostgreSQL-compatible Unicode scalar JSON string without U+0000")
		}
	case GenerationWireTypeBytes:
		raw, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a canonical standard-base64 JSON string")
		}
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != raw {
			return fmt.Errorf("must be a canonical standard-base64 JSON string")
		}
	case GenerationWireTypeUUID:
		raw, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be a canonical lowercase dashed UUID JSON string")
		}
		parsed, err := uuid.Parse(raw)
		if err != nil || parsed.String() != raw || len(raw) != 36 {
			return fmt.Errorf("must be a canonical lowercase dashed UUID JSON string")
		}
	case GenerationWireTypeJSON:
		raw, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be canonical inner JCS bytes carried in a JSON string")
		}
		canonical, err := jcs.Canonicalize([]byte(raw))
		if err != nil || string(canonical) != raw {
			return fmt.Errorf("must be canonical inner JCS bytes carried in a JSON string")
		}
	default:
		return fmt.Errorf("uses unsupported wire type %q", column.WireType)
	}
	return nil
}

func generationWirePushError(schema, table, column string, err error) error {
	return &PushValidationError{Message: fmt.Sprintf("payload field %s.%s.%s violates its schema-generation wire contract: %v", schema, table, column, err)}
}

func (s *SyncService) validatePushRowConflictForGeneration(ctx context.Context, tx pgx.Tx, selection generationSelection, ownerUserID string, row pushPreparedRow, state *rowStateSnapshot) error {
	if err := validateGenerationSelection(selection); err != nil {
		return err
	}
	table, ok := selection.view.tables[Key(row.schema, row.table)]
	if !ok {
		return projectionError(GenerationProjectionIntegrityError, "conflict", "visible conflict table is unavailable")
	}
	err := s.validatePushRowConflict(ctx, tx, ownerUserID, row, state, table.contract)
	var conflict *PushConflictError
	if !errors.As(err, &conflict) || conflict.Conflict == nil || string(conflict.Conflict.ServerRow) == "null" || len(conflict.Conflict.ServerRow) == 0 {
		return err
	}
	object, decodeErr := jcs.DecodeObjectLossless(conflict.Conflict.ServerRow)
	if decodeErr != nil {
		return projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict row is invalid")
	}
	visible := make(map[string]any, len(table.contract.Columns))
	for _, column := range table.contract.Columns {
		value, present := object[column.Name]
		if !present || validateGenerationWireValue(column, value) != nil {
			return projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict row is invalid")
		}
		visible[column.Name] = value
	}
	if len(table.contract.SyncKeyColumns) != 1 {
		return projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict key contract is invalid")
	}
	keyName := table.contract.SyncKeyColumns[0]
	conflictKey, keyOK := conflict.Conflict.Key[keyName]
	payloadKey, payloadOK := visible[keyName]
	conflictKeyWire, conflictKeyErr := jcs.Marshal(conflictKey)
	payloadKeyWire, payloadKeyErr := jcs.Marshal(payloadKey)
	if !keyOK || !payloadOK || conflictKeyErr != nil || payloadKeyErr != nil || string(conflictKeyWire) != string(payloadKeyWire) {
		return projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict key does not match row")
	}
	conflict.Conflict.ServerRow, decodeErr = jcs.Marshal(visible)
	if decodeErr != nil {
		return projectionError(GenerationProjectionIntegrityError, "conflict", "authoritative conflict row is invalid")
	}
	keyWire, keyErr := jcs.Marshal(conflict.Conflict.Key)
	if keyErr != nil || int64(len(keyWire)+len(conflict.Conflict.ServerRow)) > int64(s.config.MaxBytesPerBundle) {
		return projectionError(BundleLimitExceeded, "conflict", "conflict data exceeds configured bundle limit")
	}
	return conflict
}

func (s *SyncService) applyPreparedPushRowBatchForGeneration(ctx context.Context, tx pgx.Tx, selection generationSelection, ownerUserID string, rows []pushPreparedRow) error {
	if err := validateGenerationSelection(selection); err != nil {
		return err
	}
	return applyPreparedPushRowBatch(ctx, tx, ownerUserID, rows)
}

func (s *SyncService) processPullForGeneration(ctx context.Context, tx pgx.Tx, selection generationSelection, userID string, afterBundleSeq int64, maxBundles int, targetBundleSeq int64) (*PullResponse, error) {
	if err := validateGenerationSelection(selection); err != nil {
		return nil, err
	}
	stableBundleSeq, bundleSeqs, hasMore, err := s.planPullPage(ctx, tx, userID, afterBundleSeq, maxBundles, targetBundleSeq)
	if err != nil {
		return nil, err
	}
	return s.projectPullPageForGeneration(ctx, tx, selection, userID, stableBundleSeq, bundleSeqs, hasMore)
}

func (s *SyncService) projectPullPageForGeneration(ctx context.Context, tx pgx.Tx, selection generationSelection, userID string, stableBundleSeq int64, bundleSeqs []int64, hasMore bool) (*PullResponse, error) {
	if err := validateGenerationSelection(selection); err != nil {
		return nil, err
	}
	projected := make([]Bundle, 0, len(bundleSeqs))
	var admittedRows int64
	stageStart := s.stageStart()
	for _, bundleSeq := range bundleSeqs {
		canonical, err := s.loadCommittedBundle(ctx, tx, userID, bundleSeq)
		if err != nil {
			s.observeStageErr(ctx, "pull", "load_bundles", stageStart, len(projected), 0, err)
			return nil, err
		}
		bundle, err := s.projectCommittedBundle(selection, canonical)
		if err != nil {
			s.observeStageErr(ctx, "pull", "load_bundles", stageStart, len(projected), 0, err)
			return nil, err
		}
		bundle.CanonicalRequestHash = ""
		s.observeHistoryProjectionRetention(
			int64(len(projected))+2,
			admittedRows+canonical.RowCount+bundle.RowCount,
			canonical.ByteCount+bundle.ByteCount,
		)
		ownedBuffers, ownedBytes := retainedPullAllocations(projected, canonical, bundle)
		s.observeHistoryProjectionOwnedRetention(ownedBuffers, ownedBytes)
		candidate := append(append([]Bundle(nil), projected...), *bundle)
		responseBytes, sizeErr := compactPullResponseDecodedBytes(generationDescriptor(selection.view.contract), stableBundleSeq, candidate, hasMore)
		if sizeErr != nil {
			return nil, &CommittedBundleUnrepresentableError{BundleSeq: bundle.BundleSeq}
		}
		if responseBytes > s.config.MaxBytesPerPullResponse {
			if len(projected) == 0 {
				return nil, &CommittedBundleUnrepresentableError{BundleSeq: bundle.BundleSeq}
			}
			hasMore = true
			break
		}
		admittedRows += bundle.RowCount
		projected = append(projected, *bundle)
	}
	s.observeStage(ctx, "pull", "load_bundles", stageStart, len(projected), 0, false)
	return &PullResponse{StableBundleSeq: stableBundleSeq, Bundles: projected, HasMore: hasMore}, nil
}

func (s *SyncService) materializeSnapshotRowsForGeneration(ctx context.Context, tx pgx.Tx, selection generationSelection, snapshotID, userID string, userPK, snapshotBundleSeq int64) (int64, int64, snapshotBatchHighWater, error) {
	if err := validateGenerationSelection(selection); err != nil {
		return 0, 0, snapshotBatchHighWater{}, err
	}
	return s.materializeSnapshotRows(ctx, tx, selection.view, snapshotID, userID, userPK, snapshotBundleSeq)
}

func watchGenerationBoundary(selection generationSelection) error {
	return validateGenerationSelection(selection)
}
