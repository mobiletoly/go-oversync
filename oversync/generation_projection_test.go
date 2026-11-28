// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/stretchr/testify/require"
)

func projectionTestSelection(t *testing.T, tables ...GenerationTableContract) generationSelection {
	t.Helper()
	contract := SchemaGenerationContract{id: 1, fingerprint: strings.Repeat("a", 64), tables: append([]GenerationTableContract(nil), tables...)}
	view := &schemaGenerationView{
		contract: contract, tables: make(map[string]generationTableView),
		canonicalTables: make(map[string]canonicalProjectionTable), dataPlaneReady: true,
	}
	for index, table := range tables {
		tableView := generationTableView{contract: table, tableID: int32(index + 1)}
		view.tables[Key(table.Schema, table.Table)] = tableView
		view.canonicalTables[Key(table.Schema, table.Table)] = canonicalProjectionTable{syncKeyColumn: table.SyncKeyColumns[0], syncKeyType: syncKeyTypeText}
		view.byID = append(view.byID, tableView)
	}
	view.canonicalTables[Key("app", "audit")] = canonicalProjectionTable{syncKeyColumn: "id", syncKeyType: syncKeyTypeText}
	view.canonicalTables[Key("app", "hidden")] = canonicalProjectionTable{syncKeyColumn: "id", syncKeyType: syncKeyTypeText}
	return generationSelection{proof: SchemaGenerationProof{ID: contract.id, Fingerprint: contract.fingerprint}, view: view}
}

func canonicalBundleForProjectionTest(t *testing.T, rows ...BundleRow) *Bundle {
	t.Helper()
	hash, byteCount, err := computeCommittedBundleHash(rows)
	require.NoError(t, err)
	return &Bundle{
		BundleSeq: 9, SourceID: "server", SourceBundleID: 3,
		RowCount: int64(len(rows)), ByteCount: byteCount, BundleHash: hex.EncodeToString(hash), Rows: append([]BundleRow(nil), rows...),
	}
}

func TestProjectCommittedBundle_FiltersDeterministicallyAndPreservesVisibleEffects(t *testing.T) {
	selection := projectionTestSelection(t, GenerationTableContract{
		Schema: "app", Table: "users", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeText}, {Name: "name", WireType: GenerationWireTypeText}},
	})
	canonical := canonicalBundleForProjectionTest(t,
		BundleRow{Schema: "app", Table: "users", Key: SyncKey{"id": "a"}, Op: OpUpdate, RowVersion: 9, Payload: json.RawMessage(`{"id":"a","name":"Alice","secret":"changed"}`)},
		BundleRow{Schema: "app", Table: "audit", Key: SyncKey{"id": "hidden"}, Op: OpInsert, RowVersion: 9, Payload: json.RawMessage(`{"id":"hidden","detail":"private"}`)},
		BundleRow{Schema: "app", Table: "users", Key: SyncKey{"id": "b"}, Op: OpDelete, RowVersion: 9},
	)

	first, err := projectCommittedBundle(selection, canonical)
	require.NoError(t, err)
	second, err := projectCommittedBundle(selection, canonical)
	require.NoError(t, err)
	require.Equal(t, int64(2), first.RowCount)
	require.Equal(t, []string{"a", "b"}, []string{first.Rows[0].Key["id"].(string), first.Rows[1].Key["id"].(string)})
	require.Equal(t, OpUpdate, first.Rows[0].Op, "a hidden-field-only update remains an authoritative visible effect")
	require.JSONEq(t, `{"id":"a","name":"Alice"}`, string(first.Rows[0].Payload))
	require.Equal(t, first.BundleHash, second.BundleHash)
	require.Equal(t, first.ByteCount, second.ByteCount)
	require.LessOrEqual(t, first.ByteCount, canonical.ByteCount)

	first.Rows[0].Key["id"] = "mutated"
	first.Rows[0].Payload[0] = '['
	require.Equal(t, "a", second.Rows[0].Key["id"])
	require.JSONEq(t, `{"id":"a","name":"Alice"}`, string(second.Rows[0].Payload))
}

func TestProjectCommittedBundle_EmptyProjectionUsesExactEmptyArrayDescriptor(t *testing.T) {
	selection := projectionTestSelection(t, GenerationTableContract{
		Schema: "app", Table: "users", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeText}},
	})
	canonical := canonicalBundleForProjectionTest(t,
		BundleRow{Schema: "app", Table: "audit", Key: SyncKey{"id": "hidden"}, Op: OpDelete, RowVersion: 9},
	)
	projected, err := projectCommittedBundle(selection, canonical)
	require.NoError(t, err)
	emptyHash, emptyBytes, err := protocolhash.CommittedBundle(nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), projected.RowCount)
	require.Empty(t, projected.Rows)
	require.Equal(t, int64(2), emptyBytes)
	require.Equal(t, emptyBytes, projected.ByteCount)
	require.Equal(t, emptyHash, projected.BundleHash)
}

func TestProjectCommittedBundle_FailsClosedOnCanonicalOrVisibleCorruption(t *testing.T) {
	selection := projectionTestSelection(t, GenerationTableContract{
		Schema: "app", Table: "users", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeText}, {Name: "name", WireType: GenerationWireTypeText}},
	})
	valid := BundleRow{Schema: "app", Table: "users", Key: SyncKey{"id": "a"}, Op: OpInsert, RowVersion: 9, Payload: json.RawMessage(`{"id":"a","name":"Alice"}`)}
	tests := []struct {
		name string
		edit func(*Bundle)
		code GenerationProjectionErrorCode
	}{
		{name: "wrong count", edit: func(bundle *Bundle) { bundle.RowCount++ }, code: GenerationProjectionIntegrityError},
		{name: "wrong bytes", edit: func(bundle *Bundle) { bundle.ByteCount++ }, code: GenerationProjectionIntegrityError},
		{name: "wrong hash", edit: func(bundle *Bundle) {
			bundle.BundleHash = "0000000000000000000000000000000000000000000000000000000000000000"
		}, code: GenerationProjectionIntegrityError},
		{name: "wrong version", edit: func(bundle *Bundle) { bundle.Rows[0].RowVersion-- }, code: GenerationProjectionIntegrityError},
		{name: "missing historical field", edit: func(bundle *Bundle) {
			bundle.Rows[0].Payload = json.RawMessage(`{"id":"a"}`)
			refreshProjectionTestDescriptor(t, bundle)
		}, code: GenerationHistoryUnavailable},
		{name: "key payload mismatch", edit: func(bundle *Bundle) {
			bundle.Rows[0].Payload = json.RawMessage(`{"id":"other","name":"Alice"}`)
			refreshProjectionTestDescriptor(t, bundle)
		}, code: GenerationProjectionIntegrityError},
		{name: "hidden key payload mismatch", edit: func(bundle *Bundle) {
			bundle.Rows[0].Schema, bundle.Rows[0].Table = "app", "hidden"
			bundle.Rows[0].Payload = json.RawMessage(`{"id":"other","name":"Alice"}`)
			refreshProjectionTestDescriptor(t, bundle)
		}, code: GenerationProjectionIntegrityError},
		{name: "malformed hidden payload", edit: func(bundle *Bundle) {
			bundle.Rows[0].Schema, bundle.Rows[0].Table = "app", "hidden"
			bundle.Rows[0].Payload = json.RawMessage(`{"id":`)
		}, code: GenerationProjectionIntegrityError},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			bundle := canonicalBundleForProjectionTest(t, valid)
			testCase.edit(bundle)
			_, err := projectCommittedBundle(selection, bundle)
			var projectionErr *GenerationProjectionError
			require.ErrorAs(t, err, &projectionErr)
			require.Equal(t, testCase.code, projectionErr.Code)
		})
	}
}

func refreshProjectionTestDescriptor(t *testing.T, bundle *Bundle) {
	t.Helper()
	hash, byteCount, err := computeCommittedBundleHash(bundle.Rows)
	require.NoError(t, err)
	bundle.RowCount = int64(len(bundle.Rows))
	bundle.ByteCount = byteCount
	bundle.BundleHash = hex.EncodeToString(hash)
}
