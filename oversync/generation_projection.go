// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

type retainHistoryProjectionContextKey struct{}

func retainHistoryProjectionForHTTP(ctx context.Context) context.Context {
	return context.WithValue(ctx, retainHistoryProjectionContextKey{}, true)
}

func shouldRetainHistoryProjection(ctx context.Context) bool {
	retain, _ := ctx.Value(retainHistoryProjectionContextKey{}).(bool)
	return retain
}

func releaseHistoryProjection(release *func()) {
	if release != nil && *release != nil {
		(*release)()
		*release = nil
	}
}

// transferHistoryProjectionForHTTP joins the projection permit and the
// service-operation lease into one response-owned release. This keeps graceful
// shutdown aware of response encoding and writing, not only projection work.
func transferHistoryProjectionForHTTP(ctx context.Context, target, projection, operation *func()) {
	if !shouldRetainHistoryProjection(ctx) || target == nil || projection == nil || *projection == nil {
		return
	}
	projectionRelease := *projection
	operationRelease := func() {}
	if operation != nil && *operation != nil {
		operationRelease = *operation
	}
	var once sync.Once
	*target = func() {
		once.Do(func() {
			projectionRelease()
			operationRelease()
		})
	}
	*projection = nil
	if operation != nil {
		*operation = nil
	}
}

type GenerationProjectionErrorCode string

const (
	GenerationHistoryUnavailable       GenerationProjectionErrorCode = "generation_history_unavailable"
	GenerationProjectionIntegrityError GenerationProjectionErrorCode = "generation_projection_integrity_error"
	BundleLimitExceeded                GenerationProjectionErrorCode = "bundle_limit_exceeded"
	BundleProjectionTooLarge           GenerationProjectionErrorCode = "bundle_projection_too_large"
	HistoryProjectionCapacityExceeded  GenerationProjectionErrorCode = "history_projection_capacity_exceeded"
)

type GenerationProjectionError struct {
	Code      GenerationProjectionErrorCode
	Operation string
	Message   string
}

// CommittedBundleUnrepresentableError reports retained history that cannot be
// encoded within the published pull contract. It is terminal corruption, not
// pagination pressure.
type CommittedBundleUnrepresentableError struct {
	BundleSeq int64
}

func (e *CommittedBundleUnrepresentableError) Error() string {
	return fmt.Sprintf("committed bundle %d cannot be represented within the admitted pull limit", e.BundleSeq)
}

func (e *GenerationProjectionError) Error() string {
	if e == nil || e.Message == "" {
		return "schema generation projection failed"
	}
	return e.Message
}

type PushEffectCoverageError struct{}

func (*PushEffectCoverageError) Error() string {
	return "push did not produce a captured result for every uploaded key"
}

func projectionError(code GenerationProjectionErrorCode, operation, message string) error {
	return &GenerationProjectionError{Code: code, Operation: operation, Message: message}
}

func (s *SyncService) acquireHistoryProjection(ctx context.Context) (func(), error) {
	if s == nil || s.historyProjectionPermits == nil {
		if s != nil {
			s.observeHistoryProjection(nil, nil, projectionError(HistoryProjectionCapacityExceeded, "admission", "history projection capacity exceeded"))
		}
		return nil, projectionError(HistoryProjectionCapacityExceeded, "admission", "history projection capacity exceeded")
	}
	select {
	case s.historyProjectionPermits <- struct{}{}:
		active := s.historyProjectionMetrics.active.Add(1)
		observeAtomicHighWater(&s.historyProjectionMetrics.activeHighWater, active)
		var once sync.Once
		return func() {
			once.Do(func() {
				s.historyProjectionMetrics.active.Add(-1)
				<-s.historyProjectionPermits
			})
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		s.historyProjectionMetrics.capacityFailures.Add(1)
		return nil, projectionError(HistoryProjectionCapacityExceeded, "admission", "history projection capacity exceeded")
	}
}

func projectCommittedBundle(selection generationSelection, canonical *Bundle) (*Bundle, error) {
	if err := validateGenerationSelection(selection); err != nil {
		return nil, err
	}
	if canonical == nil || canonical.BundleSeq <= 0 || canonical.SourceBundleID <= 0 || canonical.Rows == nil || canonical.RowCount != int64(len(canonical.Rows)) {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle framing is invalid")
	}
	canonicalHash, canonicalBytes, err := computeCommittedBundleHash(canonical.Rows)
	if err != nil || canonical.BundleHash != renderBundleHash(canonicalHash) || canonical.ByteCount != canonicalBytes {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical bundle integrity check failed")
	}

	projected := &Bundle{
		BundleSeq: canonical.BundleSeq, SourceID: canonical.SourceID,
		SourceBundleID: canonical.SourceBundleID, CanonicalRequestHash: canonical.CanonicalRequestHash,
		Rows: make([]BundleRow, 0, len(canonical.Rows)),
	}
	for _, row := range canonical.Rows {
		if row.RowVersion != canonical.BundleSeq {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row version is invalid")
		}
		if row.Op != OpInsert && row.Op != OpUpdate && row.Op != OpDelete {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row operation is invalid")
		}
		if row.Op == OpDelete && len(row.Payload) != 0 {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical delete payload is invalid")
		}
		if row.Op != OpDelete && len(row.Payload) == 0 {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row payload is missing")
		}
		var canonicalObject map[string]any
		if row.Op != OpDelete {
			canonicalObject, err = jcs.DecodeObjectLossless(row.Payload)
			if err != nil {
				return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row payload is invalid")
			}
		}
		canonicalTable, registered := selection.view.canonicalTables[Key(row.Schema, row.Table)]
		if !registered || canonicalTable.syncKeyColumn == "" {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row table is not registered")
		}
		keyBytes, err := jcs.Marshal(row.Key)
		if err != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row key is invalid")
		}
		var canonicalKey SyncKey
		if err := json.Unmarshal(keyBytes, &canonicalKey); err != nil || len(canonicalKey) != 1 {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row key is invalid")
		}
		keyValue, keyOK := canonicalKey[canonicalTable.syncKeyColumn]
		keyWireType := GenerationWireTypeText
		if canonicalTable.syncKeyType == syncKeyTypeUUID {
			keyWireType = GenerationWireTypeUUID
		}
		if !keyOK || validateGenerationWireValue(GenerationColumn{Name: canonicalTable.syncKeyColumn, WireType: keyWireType}, keyValue) != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row key is invalid")
		}
		if row.Op != OpDelete {
			payloadKey, payloadOK := canonicalObject[canonicalTable.syncKeyColumn]
			keyCanonical, keyErr := jcs.Marshal(keyValue)
			payloadCanonical, payloadErr := jcs.Marshal(payloadKey)
			if !payloadOK || keyErr != nil || payloadErr != nil || string(keyCanonical) != string(payloadCanonical) {
				return nil, projectionError(GenerationProjectionIntegrityError, "history", "canonical row key does not match payload")
			}
		}
		table, visible := selection.view.tables[Key(row.Schema, row.Table)]
		if !visible {
			continue
		}
		out := BundleRow{Schema: row.Schema, Table: row.Table, Op: row.Op, RowVersion: row.RowVersion}
		out.Key = canonicalKey
		if len(table.contract.SyncKeyColumns) != 1 || len(out.Key) != 1 {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "visible row key is invalid")
		}
		keyName := table.contract.SyncKeyColumns[0]
		keyValue, keyOK = out.Key[keyName]
		var keyColumn GenerationColumn
		for _, column := range table.contract.Columns {
			if column.Name == keyName {
				keyColumn = column
				break
			}
		}
		if !keyOK || keyColumn.Name == "" || validateGenerationWireValue(keyColumn, keyValue) != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "visible row key is invalid")
		}
		if row.Op == OpDelete {
			projected.Rows = append(projected.Rows, out)
			continue
		}
		visibleObject := make(map[string]any, len(table.contract.Columns))
		for _, column := range table.contract.Columns {
			value, ok := canonicalObject[column.Name]
			if !ok {
				return nil, projectionError(GenerationHistoryUnavailable, "history", "generation history is unavailable")
			}
			if err := validateGenerationWireValue(column, value); err != nil {
				return nil, projectionError(GenerationProjectionIntegrityError, "history", "visible historical value is invalid")
			}
			visibleObject[column.Name] = value
		}
		payloadKey, payloadOK := visibleObject[keyName]
		keyCanonical, keyErr := jcs.Marshal(keyValue)
		payloadCanonical, payloadErr := jcs.Marshal(payloadKey)
		if !keyOK || !payloadOK || keyErr != nil || payloadErr != nil || string(keyCanonical) != string(payloadCanonical) {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "visible row key does not match payload")
		}
		out.Payload, err = jcs.Marshal(visibleObject)
		if err != nil {
			return nil, projectionError(GenerationProjectionIntegrityError, "history", "visible historical payload is invalid")
		}
		projected.Rows = append(projected.Rows, out)
	}
	projected.RowCount = int64(len(projected.Rows))
	projectedHash, projectedBytes, err := computeCommittedBundleHash(projected.Rows)
	if err != nil {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "projected bundle framing is invalid")
	}
	if projectedBytes > canonical.ByteCount {
		return nil, projectionError(GenerationProjectionIntegrityError, "history", "projected bundle exceeds canonical bundle")
	}
	projected.ByteCount = projectedBytes
	projected.BundleHash = renderBundleHash(projectedHash)
	return projected, nil
}

func effectIdentity(schema, table string, key SyncKey) (string, error) {
	wire, err := jcs.Marshal(key)
	if err != nil {
		return "", err
	}
	return schema + "\x00" + table + "\x00" + string(wire), nil
}

func requireCompletePushEffectCoverage(request []pushPreparedRow, projected *Bundle) error {
	effects := make(map[string]struct{}, len(projected.Rows))
	for _, row := range projected.Rows {
		id, err := effectIdentity(row.Schema, row.Table, row.Key)
		if err != nil {
			return projectionError(GenerationProjectionIntegrityError, "push", "projected effect key is invalid")
		}
		if _, duplicate := effects[id]; duplicate {
			return projectionError(GenerationProjectionIntegrityError, "push", "projected effects contain a duplicate key")
		}
		effects[id] = struct{}{}
	}
	for _, row := range request {
		id, err := effectIdentity(row.schema, row.table, SyncKey{row.keyColumn: row.keyString})
		if err != nil {
			return projectionError(GenerationProjectionIntegrityError, "push", "staged request key is invalid")
		}
		if _, ok := effects[id]; !ok {
			return &PushEffectCoverageError{}
		}
	}
	return nil
}

func checkedPullBodyLimit(projectedBytes int64, envelopes int) (int64, error) {
	if envelopes < 0 || projectedBytes < 0 || int64(envelopes+1) > (1<<63-1)/historyEnvelopeAllowance {
		return 0, fmt.Errorf("pull body limit overflow")
	}
	return projectedBytes + int64(envelopes+1)*historyEnvelopeAllowance, nil
}
