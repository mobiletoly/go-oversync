// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactPushResponseWire_ContainsOnlyAuthoritativeEffect(t *testing.T) {
	response, err := json.Marshal(CompactPushResponse{
		BundleSeq:  7,
		BundleHash: "bundle-hash",
		Rows: []CompactRowGroup{{TableIndex: 0, FirstRowOrdinal: 0, Rows: []CompactRowTuple{{
			Op: "i", KeyValues: []any{"row-1"}, Version: 0, Payload: json.RawMessage(`{"id":"row-1"}`),
		}}}},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"bundle_seq":7,
		"bundle_hash":"bundle-hash",
		"rows":[[0,0,[["i",["row-1"],0,{"id":"row-1"}]]]]
	}`, string(response))
}

func TestRequestEnvelopePresenceContracts(t *testing.T) {
	t.Run("connect", func(t *testing.T) {
		for _, body := range []string{
			`{}`,
			`null`,
			`{"has_local_pending_rows":null}`,
			`{"has_local_pending_rows":false,"unknown":true}`,
		} {
			var request ConnectRequest
			require.Error(t, decodeStrictJSONRequest(strings.NewReader(body), &request, true), body)
		}
		for _, body := range []string{
			`{"has_local_pending_rows":false}`,
			`{"has_local_pending_rows":true}`,
		} {
			var request ConnectRequest
			require.NoError(t, decodeStrictJSONRequest(strings.NewReader(body), &request, true), body)
		}
	})

	t.Run("snapshot create", func(t *testing.T) {
		for _, body := range []string{
			`null`,
			`{"source_replacement":null}`,
			`{"source_replacement":{}}`,
			`{"source_replacement":{"previous_source_id":"old","new_source_id":"new"}}`,
			`{"source_replacement":{"previous_source_id":"old","new_source_id":"new","reason":null}}`,
			`{"source_replacement":{"previous_source_id":"old","new_source_id":"new","reason":"history_pruned","unknown":true}}`,
		} {
			var request SnapshotSessionCreateRequest
			require.Error(t, decodeStrictJSONRequest(strings.NewReader(body), &request, true), body)
		}
		for _, body := range []string{
			`{}`,
			`{"source_replacement":{"previous_source_id":"old","new_source_id":"new","reason":"history_pruned"}}`,
		} {
			var request SnapshotSessionCreateRequest
			require.NoError(t, decodeStrictJSONRequest(strings.NewReader(body), &request, true), body)
		}
	})
}
