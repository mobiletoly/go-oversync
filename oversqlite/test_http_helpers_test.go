package oversqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/sync/generations/") {
		generationID := strings.TrimPrefix(r.URL.Path, "/sync/generations/")
		fingerprint := r.Header.Get(oversync.SchemaGenerationFingerprintHeader)
		if fingerprint == "" {
			fingerprint = testSchemaGenerationProof().Fingerprint
		}
		return jsonResponse(oversync.GenerationDiscoveryResponse{
			ProtocolVersion:  requiredProtocolVersion,
			SchemaGeneration: oversync.SchemaGenerationCapabilities{ID: generationID, Fingerprint: fingerprint, Tables: []oversync.SchemaGenerationTableCapabilities{}},
			Limits:           testWireLimits(),
		}), nil
	}
	return fn(r)
}

type rawRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn rawRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonResponse(v any) *http.Response {
	switch typed := v.(type) {
	case oversync.PullResponse:
		if typed.Bundles == nil {
			typed.Bundles = []oversync.Bundle{}
		}
		v = compactTestPullResponse(typed)
	case *oversync.PullResponse:
		if typed != nil {
			copy := *typed
			copy.Bundles = append([]oversync.Bundle(nil), typed.Bundles...)
			if copy.Bundles == nil {
				copy.Bundles = []oversync.Bundle{}
			}
			v = compactTestPullResponse(copy)
		}
	case oversync.GenerationDiscoveryResponse:
		if typed.SchemaGeneration.ID == "" {
			typed.SchemaGeneration = oversync.SchemaGenerationCapabilities{ID: "1", Fingerprint: testSchemaGenerationProof().Fingerprint, Tables: []oversync.SchemaGenerationTableCapabilities{}}
		}
		if typed.Limits.MaxPushRows == 0 {
			typed.Limits = testWireLimits()
		}
		v = typed
	case oversync.SnapshotSession:
		if typed.RowCount > 0 && typed.ByteCount == 0 {
			typed.ByteCount = typed.RowCount
		}
		v = oversync.CompactSnapshotSessionResponse{SnapshotID: typed.SnapshotID, SnapshotBundleSeq: typed.SnapshotBundleSeq, RowCount: typed.RowCount, LogicalByteCount: typed.ByteCount, ExpiresAt: typed.ExpiresAt}
	case *oversync.SnapshotSession:
		if typed != nil {
			copy := *typed
			if copy.RowCount > 0 && copy.ByteCount == 0 {
				copy.ByteCount = copy.RowCount
			}
			v = oversync.CompactSnapshotSessionResponse{SnapshotID: copy.SnapshotID, SnapshotBundleSeq: copy.SnapshotBundleSeq, RowCount: copy.RowCount, LogicalByteCount: copy.ByteCount, ExpiresAt: copy.ExpiresAt}
		}
	case oversync.SnapshotChunkResponse:
		if typed.Rows == nil {
			typed.Rows = []oversync.SnapshotRow{}
		}
		if len(typed.Rows) > 0 && typed.ByteCount == 0 {
			typed.ByteCount = testSnapshotRowsByteCount(typed.Rows)
		}
		v = compactTestSnapshotChunk(typed)
	case *oversync.SnapshotChunkResponse:
		if typed != nil {
			copy := *typed
			if copy.Rows == nil {
				copy.Rows = []oversync.SnapshotRow{}
			}
			if len(copy.Rows) > 0 && copy.ByteCount == 0 {
				copy.ByteCount = testSnapshotRowsByteCount(copy.Rows)
			}
			v = compactTestSnapshotChunk(copy)
		}
	}
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func compactTestPullResponse(response oversync.PullResponse) oversync.CompactPullResponse {
	generation := testGenerationForBundleRows(response.Bundles)
	compact := oversync.CompactPullResponse{StableBundleSeq: response.StableBundleSeq, HasMore: response.HasMore, Bundles: make([]oversync.CompactBundle, 0, len(response.Bundles))}
	for index := range response.Bundles {
		bundle := response.Bundles[index]
		fillTestBundleDescriptor(&bundle)
		groups, err := oversync.CompactGroupsForBundleRows(generation, bundle.Rows)
		if err != nil {
			panic(err)
		}
		compact.Bundles = append(compact.Bundles, oversync.CompactBundle{BundleSeq: bundle.BundleSeq, SourceID: bundle.SourceID, SourceBundleID: bundle.SourceBundleID, BundleHash: bundle.BundleHash, Rows: groups})
	}
	return compact
}

func compactTestSnapshotChunk(chunk oversync.SnapshotChunkResponse) oversync.CompactSnapshotChunkResponse {
	generation := testGenerationForSnapshotRows(chunk.Rows)
	groups, logicalBytes, err := oversync.CompactGroupsForSnapshotRowsAt(generation, chunk.Rows, chunk.NextRowOrdinal-int64(len(chunk.Rows)))
	if err != nil {
		panic(err)
	}
	return oversync.CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: chunk.NextRowOrdinal, HasMore: chunk.HasMore, LogicalByteCount: logicalBytes}
}

func testGenerationForBundleRows(bundles []oversync.Bundle) oversync.SchemaGenerationCapabilities {
	rows := make([]oversync.SnapshotRow, 0)
	for _, bundle := range bundles {
		for _, row := range bundle.Rows {
			rows = append(rows, oversync.SnapshotRow{Schema: row.Schema, Table: row.Table, Key: row.Key})
		}
	}
	return testGenerationForSnapshotRows(rows)
}

func testGenerationForSnapshotRows(rows []oversync.SnapshotRow) oversync.SchemaGenerationCapabilities {
	tables := map[string]oversync.SchemaGenerationTableCapabilities{}
	for _, row := range rows {
		key := row.Schema + "." + row.Table
		table := tables[key]
		table.Schema, table.Table = row.Schema, row.Table
		seenColumns := make(map[string]struct{}, len(table.SyncKeyColumns))
		for _, column := range table.SyncKeyColumns {
			seenColumns[column] = struct{}{}
		}
		for column := range row.Key {
			if _, present := seenColumns[column]; !present {
				table.SyncKeyColumns = append(table.SyncKeyColumns, column)
				seenColumns[column] = struct{}{}
			}
		}
		sort.Strings(table.SyncKeyColumns)
		tables[key] = table
	}
	keys := make([]string, 0, len(tables))
	for key := range tables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	generation := oversync.SchemaGenerationCapabilities{Tables: make([]oversync.SchemaGenerationTableCapabilities, 0, len(keys))}
	for _, key := range keys {
		generation.Tables = append(generation.Tables, tables[key])
	}
	return generation
}

func testSnapshotRowsByteCount(rows []oversync.SnapshotRow) int64 {
	_, total, err := oversync.CompactGroupsForSnapshotRowsAt(testGenerationForSnapshotRows(rows), rows, 1)
	if err != nil {
		panic(err)
	}
	return total
}

func testWireLimits() oversync.WireLimits {
	return oversync.WireLimits{
		MaxPushRows: 1000, MaxPushDecodedBytes: 1 << 20,
		MaxPullBundles: 1000, MaxPullDecodedBytes: 4 << 20,
		MaxSnapshotRowsPerChunk: 1000, MaxSnapshotDecodedBytesPerChunk: 4 << 20,
		MaxSnapshotRows: 100_000, MaxSnapshotLogicalBytes: 64 << 20, MaxRowDecodedBytes: 1 << 20,
	}
}

func fillTestBundleDescriptor(bundle *oversync.Bundle) {
	if bundle == nil {
		return
	}
	bundle.RowCount = int64(len(bundle.Rows))
	logical := make([]protocolhash.BundleRow, 0, len(bundle.Rows))
	for _, row := range bundle.Rows {
		logical = append(logical, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload})
	}
	hash, byteCount, err := protocolhash.CommittedBundle(logical)
	if err == nil {
		bundle.BundleHash = hash
		bundle.ByteCount = byteCount
	}
}

func errorJSONResponse(status int, v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func requireAuthenticatedSyncHeaders(t *testing.T, r *http.Request, sourceID string) {
	t.Helper()
	require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
	if sourceID == "" {
		require.NotEmpty(t, r.Header.Get(oversync.SourceIDHeader))
		return
	}
	require.Equal(t, sourceID, r.Header.Get(oversync.SourceIDHeader))
}

type staticResolver struct {
	result MergeResult
}

func (r *staticResolver) Resolve(conflict ConflictContext) MergeResult {
	if r == nil || r.result == nil {
		return AcceptServer{}
	}
	return r.result
}

func attachTestClient(t *testing.T, client *Client, userID, sourceID string) AttachResult {
	t.Helper()
	if strings.TrimSpace(sourceID) != "" {
		client.sourceIDGenerator = func() string { return sourceID }
	}

	prevHTTP := client.HTTP
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})}
	t.Cleanup(func() {
		client.HTTP = prevHTTP
	})

	mustOpen(t, client, context.Background())
	result, err := client.Attach(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, result.Status)
	return result
}

func tokenProviderForTests(_ context.Context) (string, error) {
	return "token", nil
}

func newTestRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		Enabled:        true,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		JitterFraction: 0,
	}
}
