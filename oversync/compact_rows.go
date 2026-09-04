// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

// CompactRowGroup is the v1 transport representation
// [table_index, first_row_ordinal, rows]. It is expanded before any logical
// validation, hashing, conflict detection, or database application.
type CompactRowGroup struct {
	TableIndex      int
	FirstRowOrdinal int64
	Rows            []CompactRowTuple
}

// CompactRowTuple is the v1 compact transport representation. Deletes have
// exactly three fields; inserts and updates have exactly four.
type CompactRowTuple struct {
	Op        string
	KeyValues []any
	Version   int64
	Payload   json.RawMessage
}

func (g CompactRowGroup) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{g.TableIndex, g.FirstRowOrdinal, g.Rows})
}

func (g *CompactRowGroup) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || len(raw) != 3 {
		return fmt.Errorf("compact row group must have exactly three fields")
	}
	if err := json.Unmarshal(raw[0], &g.TableIndex); err != nil || g.TableIndex < 0 {
		return fmt.Errorf("compact row group table_index is invalid")
	}
	if err := json.Unmarshal(raw[1], &g.FirstRowOrdinal); err != nil || g.FirstRowOrdinal < 0 {
		return fmt.Errorf("compact row group first_row_ordinal is invalid")
	}
	if err := json.Unmarshal(raw[2], &g.Rows); err != nil || len(g.Rows) == 0 {
		return fmt.Errorf("compact row group rows are invalid")
	}
	return nil
}

func (r CompactRowTuple) MarshalJSON() ([]byte, error) {
	values := []any{r.Op, r.KeyValues, r.Version}
	if r.Op != "d" {
		values = append(values, json.RawMessage(r.Payload))
	}
	return json.Marshal(values)
}

func (r *CompactRowTuple) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("compact row tuple must be an array: %w", err)
	}
	if len(raw) != 3 && len(raw) != 4 {
		return fmt.Errorf("compact row tuple has invalid arity")
	}
	if err := json.Unmarshal(raw[0], &r.Op); err != nil || (r.Op != "i" && r.Op != "u" && r.Op != "d") {
		return fmt.Errorf("compact row tuple operation is invalid")
	}
	if (r.Op == "d" && len(raw) != 3) || (r.Op != "d" && len(raw) != 4) {
		return fmt.Errorf("compact row tuple arity does not match operation")
	}
	if err := json.Unmarshal(raw[1], &r.KeyValues); err != nil || r.KeyValues == nil {
		return fmt.Errorf("compact row tuple key values are invalid")
	}
	if err := json.Unmarshal(raw[2], &r.Version); err != nil || r.Version < 0 {
		return fmt.Errorf("compact row tuple version is invalid")
	}
	r.Payload = nil
	if len(raw) == 4 {
		payload := bytes.TrimSpace(raw[3])
		if !json.Valid(payload) || len(payload) == 0 || payload[0] != '{' {
			return fmt.Errorf("compact row tuple payload must be an object")
		}
		r.Payload = append(json.RawMessage(nil), payload...)
	}
	return nil
}

// ExpandCompactPushRows converts the compact transport into retained logical
// rows. It rejects table/key/ordinal and payload-shape errors before a caller
// can hash or apply any row.
func ExpandCompactPushRows(generation SchemaGenerationCapabilities, groups []CompactRowGroup) ([]PushRequestRow, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("compact rows must not be empty")
	}
	rows := make([]PushRequestRow, 0)
	nextOrdinal := int64(0)
	seen := map[string]struct{}{}
	previousTable := -1
	for _, group := range groups {
		if group.TableIndex >= len(generation.Tables) {
			return nil, fmt.Errorf("unknown compact table index %d", group.TableIndex)
		}
		if group.FirstRowOrdinal != nextOrdinal {
			return nil, fmt.Errorf("compact row ordinal gap or overlap at %d", group.FirstRowOrdinal)
		}
		if group.TableIndex == previousTable {
			return nil, fmt.Errorf("adjacent compact groups must not repeat table index %d", group.TableIndex)
		}
		table := generation.Tables[group.TableIndex]
		for _, tuple := range group.Rows {
			if len(tuple.KeyValues) != len(table.SyncKeyColumns) {
				return nil, fmt.Errorf("compact key arity for %s.%s is invalid", table.Schema, table.Table)
			}
			key := make(SyncKey, len(tuple.KeyValues))
			for index, column := range table.SyncKeyColumns {
				key[column] = tuple.KeyValues[index]
			}
			keyBytes, err := json.Marshal(key)
			if err != nil {
				return nil, fmt.Errorf("encode compact key: %w", err)
			}
			identity := table.Schema + "." + table.Table + ":" + string(keyBytes)
			if _, duplicate := seen[identity]; duplicate {
				return nil, fmt.Errorf("duplicate compact logical target %s", identity)
			}
			seen[identity] = struct{}{}
			op := map[string]string{"i": "INSERT", "u": "UPDATE", "d": "DELETE"}[tuple.Op]
			payload := append(json.RawMessage(nil), tuple.Payload...)
			if tuple.Op == "d" {
				payload = nil
			}
			rows = append(rows, PushRequestRow{
				Schema: table.Schema, Table: table.Table, Key: key, Op: op,
				BaseRowVersion: tuple.Version, Payload: payload,
			})
			nextOrdinal++
		}
		previousTable = group.TableIndex
	}
	return rows, nil
}

// CompactGroupsForRows is intentionally limited to an already-expanded,
// generation-pinned logical sequence. It preserves order and starts a group
// whenever the table changes.
func CompactGroupsForRows(generation SchemaGenerationCapabilities, rows []PushRequestRow) ([]CompactRowGroup, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("compact rows must not be empty")
	}
	tableIndexes := make(map[string]int, len(generation.Tables))
	for index, table := range generation.Tables {
		tableIndexes[table.Schema+"."+table.Table] = index
	}
	var groups []CompactRowGroup
	for ordinal, row := range rows {
		index, ok := tableIndexes[row.Schema+"."+row.Table]
		if !ok {
			return nil, fmt.Errorf("row table %s.%s is not in generation", row.Schema, row.Table)
		}
		values := make([]any, len(generation.Tables[index].SyncKeyColumns))
		for keyIndex, keyColumn := range generation.Tables[index].SyncKeyColumns {
			value, present := row.Key[keyColumn]
			if !present {
				return nil, fmt.Errorf("row key %s is missing", keyColumn)
			}
			values[keyIndex] = value
		}
		op := map[string]string{"INSERT": "i", "UPDATE": "u", "DELETE": "d"}[row.Op]
		if op == "" {
			return nil, fmt.Errorf("logical operation %q is invalid", row.Op)
		}
		tuple := CompactRowTuple{Op: op, KeyValues: values, Version: row.BaseRowVersion, Payload: append(json.RawMessage(nil), row.Payload...)}
		if op != "d" && (len(tuple.Payload) == 0 || !bytes.HasPrefix(tuple.Payload, []byte("{"))) {
			return nil, fmt.Errorf("logical %s payload must be an object", row.Op)
		}
		if len(groups) == 0 || groups[len(groups)-1].TableIndex != index {
			groups = append(groups, CompactRowGroup{TableIndex: index, FirstRowOrdinal: int64(ordinal)})
		}
		groups[len(groups)-1].Rows = append(groups[len(groups)-1].Rows, tuple)
	}
	return groups, nil
}

func CompactGroupsForBundleRows(generation SchemaGenerationCapabilities, rows []BundleRow) ([]CompactRowGroup, error) {
	if len(rows) == 0 {
		return []CompactRowGroup{}, nil
	}
	logical := make([]PushRequestRow, 0, len(rows))
	for _, row := range rows {
		logical = append(logical, PushRequestRow{
			Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
			BaseRowVersion: row.RowVersion, Payload: row.Payload,
		})
	}
	return CompactGroupsForRows(generation, logical)
}

// ExpandedRowDecodedBytes returns the canonical expanded key-plus-payload
// size governed by max_row_decoded_bytes. Transport grouping and JSON member
// names are intentionally excluded from this logical per-row bound.
func ExpandedRowDecodedBytes(row PushRequestRow) (int64, error) {
	keyBytes, err := jcs.Marshal(row.Key)
	if err != nil {
		return 0, fmt.Errorf("encode canonical expanded row key: %w", err)
	}
	total := int64(len(keyBytes))
	if row.Op == OpDelete {
		return total, nil
	}
	payload, err := jcs.DecodeObjectLossless(row.Payload)
	if err != nil {
		return 0, fmt.Errorf("decode canonical expanded row payload: %w", err)
	}
	payloadBytes, err := jcs.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encode canonical expanded row payload: %w", err)
	}
	if int64(len(payloadBytes)) > int64(^uint64(0)>>1)-total {
		return 0, fmt.Errorf("canonical expanded row size overflows")
	}
	return total + int64(len(payloadBytes)), nil
}

func compactPushRequestDecodedBytes(generation SchemaGenerationCapabilities, request OneShotPushRequest) (int64, error) {
	groups, err := CompactGroupsForRows(generation, request.Rows)
	if err != nil {
		return 0, err
	}
	return decodedJSONBytes(CompactPushRequest{
		SourceBundleID: request.SourceBundleID, CanonicalRequestHash: request.CanonicalRequestHash,
		InitializationID: request.InitializationID, Rows: groups,
	})
}

func compactPushResponseDecodedBytes(generation SchemaGenerationCapabilities, bundle *Bundle) (int64, error) {
	if bundle == nil {
		return 0, fmt.Errorf("push response bundle is required")
	}
	groups, err := CompactGroupsForBundleRows(generation, bundle.Rows)
	if err != nil {
		return 0, err
	}
	return decodedJSONBytes(CompactPushResponse{BundleSeq: bundle.BundleSeq, BundleHash: bundle.BundleHash, Rows: groups})
}

func compactPullResponseDecodedBytes(generation SchemaGenerationCapabilities, stableBundleSeq int64, bundles []Bundle, hasMore bool) (int64, error) {
	response := CompactPullResponse{StableBundleSeq: stableBundleSeq, Bundles: make([]CompactBundle, 0, len(bundles)), HasMore: hasMore}
	for _, bundle := range bundles {
		groups, err := CompactGroupsForBundleRows(generation, bundle.Rows)
		if err != nil {
			return 0, err
		}
		response.Bundles = append(response.Bundles, CompactBundle{
			BundleSeq: bundle.BundleSeq, SourceID: bundle.SourceID, SourceBundleID: bundle.SourceBundleID,
			BundleHash: bundle.BundleHash, Rows: groups,
		})
	}
	return decodedJSONBytes(response)
}

// validateFutureBundleRepresentations proves the publication invariants shared
// by client pushes and server-originated writes before retained history exists.
// Every supported generation must be able to expose each visible logical row
// and the indivisible one-bundle pull response under the advertised limits.
func (s *SyncService) validateFutureBundleRepresentations(canonical *Bundle) error {
	if s == nil || canonical == nil {
		return projectionError(BundleProjectionTooLarge, "commit", "committed bundle representation is unavailable")
	}
	s.mu.RLock()
	views := make([]*schemaGenerationView, 0, len(s.generationViews))
	for _, view := range s.generationViews {
		views = append(views, view)
	}
	s.mu.RUnlock()
	if len(views) == 0 {
		return projectionError(BundleProjectionTooLarge, "commit", "schema generation views are unavailable")
	}
	for _, view := range views {
		selection := generationSelection{
			proof: SchemaGenerationProof{ID: view.contract.ID(), Fingerprint: view.contract.Fingerprint()},
			view:  view,
		}
		projected, err := projectCommittedBundle(selection, canonical)
		if err != nil {
			return err
		}
		for _, row := range projected.Rows {
			rowBytes, sizeErr := ExpandedRowDecodedBytes(PushRequestRow{
				Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
				BaseRowVersion: row.RowVersion, Payload: row.Payload,
			})
			if sizeErr != nil || rowBytes > s.maxBytesPerSnapshotRow() {
				return projectionError(BundleProjectionTooLarge, "commit", "committed row exceeds max_row_decoded_bytes")
			}
		}
		pullBytes, sizeErr := compactPullResponseDecodedBytes(generationDescriptor(view.contract), projected.BundleSeq, []Bundle{*projected}, false)
		if sizeErr != nil || pullBytes > s.config.MaxBytesPerPullResponse {
			return projectionError(BundleProjectionTooLarge, "commit", "committed bundle cannot fit an indivisible pull response")
		}
	}
	return nil
}

func decodedJSONBytes(value any) (int64, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	return int64(len(body)), nil
}

// CompactGroupsForSnapshotRows encodes the only permitted snapshot tuple form:
// an insert-coded full after-image. It also returns the pagination-independent
// logical byte total defined by oversync.snapshot-logical-row.v1.
func CompactGroupsForSnapshotRows(generation SchemaGenerationCapabilities, rows []SnapshotRow) ([]CompactRowGroup, int64, error) {
	return CompactGroupsForSnapshotRowsAt(generation, rows, 0)
}

// CompactGroupsForSnapshotRowsAt is the snapshot encoder with the durable
// snapshot row ordinal carried into compact group headers. Snapshot sessions
// start at ordinal one, unlike push requests which start at zero.
func CompactGroupsForSnapshotRowsAt(generation SchemaGenerationCapabilities, rows []SnapshotRow, firstRowOrdinal int64) ([]CompactRowGroup, int64, error) {
	if firstRowOrdinal < 0 {
		return nil, 0, fmt.Errorf("snapshot first row ordinal is invalid")
	}
	logical := make([]PushRequestRow, 0, len(rows))
	var total int64
	tableIndexes := map[string]int{}
	for index, table := range generation.Tables {
		tableIndexes[table.Schema+"."+table.Table] = index
	}
	for _, row := range rows {
		index, present := tableIndexes[row.Schema+"."+row.Table]
		if !present {
			return nil, 0, fmt.Errorf("snapshot table %s.%s is not in generation", row.Schema, row.Table)
		}
		keyValues := make([]any, len(generation.Tables[index].SyncKeyColumns))
		for keyIndex, keyColumn := range generation.Tables[index].SyncKeyColumns {
			value, ok := row.Key[keyColumn]
			if !ok {
				return nil, 0, fmt.Errorf("snapshot key %s is missing", keyColumn)
			}
			keyValues[keyIndex] = value
		}
		if len(row.Payload) == 0 || !bytes.HasPrefix(row.Payload, []byte("{")) {
			return nil, 0, fmt.Errorf("snapshot payload must be a full object")
		}
		value, err := jcs.Marshal([]any{"i", index, keyValues, row.RowVersion, json.RawMessage(row.Payload)})
		if err != nil {
			return nil, 0, fmt.Errorf("encode snapshot logical row: %w", err)
		}
		if int64(len(value)) > int64(^uint64(0)>>1)-total {
			return nil, 0, fmt.Errorf("snapshot logical byte count overflows")
		}
		total += int64(len(value))
		logical = append(logical, PushRequestRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: "INSERT", BaseRowVersion: row.RowVersion, Payload: row.Payload})
	}
	if len(logical) == 0 {
		return []CompactRowGroup{}, 0, nil
	}
	groups, err := CompactGroupsForRows(generation, logical)
	if err != nil {
		return nil, 0, err
	}
	for index := range groups {
		groups[index].FirstRowOrdinal += firstRowOrdinal
	}
	return groups, total, nil
}
