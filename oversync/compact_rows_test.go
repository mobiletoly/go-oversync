// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func compactTestGeneration() SchemaGenerationCapabilities {
	return SchemaGenerationCapabilities{Tables: []SchemaGenerationTableCapabilities{
		{Schema: "main", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "main", Table: "projects", SyncKeyColumns: []string{"project_id", "tenant_id"}},
	}}
}

func TestCompactRowsRoundTripAndExpand(t *testing.T) {
	input := []PushRequestRow{
		{Schema: "main", Table: "users", Key: SyncKey{"id": "u1"}, Op: "INSERT", BaseRowVersion: 0, Payload: json.RawMessage(`{"id":"u1","name":"Ada"}`)},
		{Schema: "main", Table: "projects", Key: SyncKey{"project_id": 7, "tenant_id": "t"}, Op: "UPDATE", BaseRowVersion: 4, Payload: json.RawMessage(`{"name":"Renamed"}`)},
		{Schema: "main", Table: "users", Key: SyncKey{"id": "u2"}, Op: "DELETE", BaseRowVersion: 7},
	}
	groups, err := CompactGroupsForRows(compactTestGeneration(), input)
	require.NoError(t, err)
	require.JSONEq(t, `[[0,0,[["i",["u1"],0,{"id":"u1","name":"Ada"}]]],[1,1,[["u",[7,"t"],4,{"name":"Renamed"}]]],[0,2,[["d",["u2"],7]]]]`, mustJSON(t, groups))
	expanded, err := ExpandCompactPushRows(compactTestGeneration(), groups)
	require.NoError(t, err)
	require.Equal(t, input, expanded)
}

func TestCompactRowsRejectMalformedTransport(t *testing.T) {
	for _, body := range []string{
		`[[2,0,[["i",["u1"],0,{"id":"u1"}]]]]`,
		`[[0,1,[["i",["u1"],0,{"id":"u1"}]]]]`,
		`[[0,0,[["d",["u1"],0,{}]]]]`,
		`[[0,0,[["i",["u1"],0]]]]`,
	} {
		var groups []CompactRowGroup
		err := json.Unmarshal([]byte(body), &groups)
		if err == nil {
			_, err = ExpandCompactPushRows(compactTestGeneration(), groups)
		}
		require.Error(t, err, body)
	}
}

func TestCompactSnapshotRowsUseInsertOnlyLogicalAccounting(t *testing.T) {
	rows := []SnapshotRow{{
		Schema: "main", Table: "users", Key: SyncKey{"id": "u1"}, RowVersion: 7,
		Payload: json.RawMessage(`{"id":"u1","name":"Ada"}`),
	}}
	groups, logicalBytes, err := CompactGroupsForSnapshotRows(compactTestGeneration(), rows)
	require.NoError(t, err)
	require.JSONEq(t, `[[0,0,[["i",["u1"],7,{"id":"u1","name":"Ada"}]]]]`, mustJSON(t, groups))
	require.Positive(t, logicalBytes)
	atGroups, atLogicalBytes, err := CompactGroupsForSnapshotRowsAt(compactTestGeneration(), rows, 9)
	require.NoError(t, err)
	require.Equal(t, logicalBytes, atLogicalBytes)
	require.JSONEq(t, `[[0,9,[["i",["u1"],7,{"id":"u1","name":"Ada"}]]]]`, mustJSON(t, atGroups))
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return string(body)
}
