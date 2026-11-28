// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRequireJSONObjectFieldsWithOptional_EnforcesExactResponseShape(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "exact required", body: `{"resolution":"initialize_empty"}`},
		{name: "optional present", body: `{"resolution":"retry_later","retry_after_seconds":1}`},
		{name: "missing required", body: `{}`, wantErr: "missing required resolution"},
		{name: "unknown", body: `{"resolution":"initialize_empty","legacy":true}`, wantErr: "unknown field legacy"},
		{name: "null optional", body: `{"resolution":"retry_later","retry_after_seconds":null}`, wantErr: "must be omitted or non-null"},
		{name: "duplicate", body: `{"resolution":"initialize_empty","resolution":"retry_later"}`, wantErr: "duplicate"},
		{name: "case collision", body: `{"resolution":"initialize_empty","Resolution":"retry_later"}`, wantErr: "unknown field Resolution"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object, err := requireJSONObjectFieldsWithOptional(
				[]byte(test.body),
				"connect",
				[]string{"resolution"},
				[]string{"retry_after_seconds"},
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Contains(t, object, "resolution")
		})
	}
}

func TestCommittedEffectCoverage_RequiresEveryUploadedKeyRejectsDuplicatesAndAllowsExtras(t *testing.T) {
	uploaded := []dirtyRowCapture{{SchemaName: "main", TableName: "users", WireKey: oversync.SyncKey{"id": "uploaded"}}}
	effect := stagedPushBundleRow{SchemaName: "main", TableName: "users", WireKey: oversync.SyncKey{"id": "uploaded"}}
	extra := stagedPushBundleRow{SchemaName: "main", TableName: "audit", WireKey: oversync.SyncKey{"id": "extra"}}

	require.NoError(t, verifyCommittedEffectCoverage(uploaded, []stagedPushBundleRow{effect}))
	require.NoError(t, verifyCommittedEffectCoverage(uploaded, []stagedPushBundleRow{extra, effect}), "genuine extra effects are allowed")
	require.ErrorContains(t, verifyCommittedEffectCoverage(uploaded, []stagedPushBundleRow{extra}), "does not cover every frozen outbox key")
	require.ErrorContains(t, verifyCommittedEffectCoverage(uploaded, []stagedPushBundleRow{effect, effect}), "duplicate key")
}
func TestValidatePullResponse_RejectsMissingCanonicalEnvelope(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	empty := func(seq int64) oversync.Bundle {
		return oversync.Bundle{BundleSeq: seq, SourceID: "server", SourceBundleID: seq, RowCount: 0, ByteCount: 2,
			BundleHash: "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945", Rows: []oversync.BundleRow{}}
	}
	for _, tc := range []struct {
		name     string
		response oversync.PullResponse
	}{
		{name: "missing first", response: oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{empty(2)}}},
		{name: "missing middle", response: oversync.PullResponse{StableBundleSeq: 3, Bundles: []oversync.Bundle{empty(1), empty(3)}}},
		{name: "empty page claims more", response: oversync.PullResponse{StableBundleSeq: 3, Bundles: []oversync.Bundle{}, HasMore: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, client.validatePullResponse(&tc.response, 0), "canonical bundle sequence")
		})
	}
}

func TestValidatePullResponse_EnforcesProtocolLogicalLimitsBeforeProportionalValidation(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	emptyHash := "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
	emptyBundle := func(seq int64) oversync.Bundle {
		return oversync.Bundle{BundleSeq: seq, SourceID: "server", SourceBundleID: seq, RowCount: 0, ByteCount: 2, BundleHash: emptyHash, Rows: []oversync.BundleRow{}}
	}

	t.Run("exact envelope limit is accepted", func(t *testing.T) {
		bundles := make([]oversync.Bundle, maxPullResponseEnvelopes)
		for index := range bundles {
			bundles[index] = emptyBundle(int64(index + 1))
		}
		require.NoError(t, client.validatePullResponse(&oversync.PullResponse{StableBundleSeq: maxPullResponseEnvelopes, Bundles: bundles}, 0))
	})

	t.Run("envelope limit plus one is rejected", func(t *testing.T) {
		resp := &oversync.PullResponse{Bundles: make([]oversync.Bundle, maxPullResponseEnvelopes+1)}
		require.ErrorContains(t, client.validatePullResponse(resp, 0), "envelope count exceeds the protocol hard limit")
	})

	t.Run("bundle row limit plus one is rejected before row decoding", func(t *testing.T) {
		bundle := emptyBundle(1)
		bundle.RowCount = maxCommittedPushBundleRows
		bundle.Rows = make([]oversync.BundleRow, maxCommittedPushBundleRows)
		require.NoError(t, validatePullBundleLogicalLimits(&bundle))
		bundle.RowCount = maxCommittedPushBundleRows + 1
		bundle.Rows = make([]oversync.BundleRow, maxCommittedPushBundleRows+1)
		err := client.validatePullResponse(&oversync.PullResponse{StableBundleSeq: 1, Bundles: []oversync.Bundle{bundle}}, 0)
		require.ErrorContains(t, err, "row count exceeds the protocol hard limit")
	})

	t.Run("bundle byte limit plus one is rejected before hashing", func(t *testing.T) {
		bundle := emptyBundle(1)
		bundle.ByteCount = maxCommittedPushBundleBytes
		require.NoError(t, validatePullBundleLogicalLimits(&bundle))
		bundle.ByteCount = maxCommittedPushBundleBytes + 1
		err := client.validatePullResponse(&oversync.PullResponse{StableBundleSeq: 1, Bundles: []oversync.Bundle{bundle}}, 0)
		require.ErrorContains(t, err, "byte_count exceeds the protocol hard limit")
	})

	t.Run("aggregate exact limit is admitted and plus one is rejected", func(t *testing.T) {
		exact := []oversync.Bundle{emptyBundle(1), emptyBundle(2), emptyBundle(3)}
		exact[0].ByteCount = maxCommittedPushBundleBytes
		exact[1].ByteCount = maxCommittedPushBundleBytes - 2
		require.NoError(t, validatePullResponseLogicalLimits(exact))
		exact[2].ByteCount++
		require.ErrorContains(t, validatePullResponseLogicalLimits(exact), "aggregate logical byte count exceeds the protocol hard limit")
	})
}
