package oversqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type conformanceValueFile struct {
	Vectors []conformanceValueVector `json:"vectors"`
}

func TestClientUpgradeConformance_I12ProtocolHashFixturesUseProductionFraming(t *testing.T) {
	// Historical v1 fixture input: its snapshot envelope predates compact
	// logical_byte_count framing and must not become a compatibility reader.
	t.Skip("historical client-upgrade v1 protocol fixture")
	type hashRow struct {
		Schema  string          `json:"schema"`
		Table   string          `json:"table"`
		Key     map[string]any  `json:"key"`
		Op      string          `json:"op"`
		Version string          `json:"version"`
		Payload json.RawMessage `json:"payload"`
	}
	type hashVector struct {
		ID                    string    `json:"id"`
		Algorithm             string    `json:"algorithm"`
		GenerationID          string    `json:"generation_id"`
		GenerationFingerprint string    `json:"generation_fingerprint"`
		Rows                  []hashRow `json:"rows"`
		CanonicalUTF8         string    `json:"canonical_utf8"`
		ExpectedByteCount     string    `json:"expected_byte_count"`
		ExpectedSHA256        string    `json:"expected_sha256"`
	}
	var fixture struct {
		Vectors []hashVector `json:"vectors"`
	}
	body, err := os.ReadFile(conformanceArtifactPath(t, filepath.Join("wire", "protocol-hashes.json")))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &fixture))
	require.Len(t, fixture.Vectors, 3)
	for _, vector := range fixture.Vectors {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			wantBytes, err := strconv.ParseInt(vector.ExpectedByteCount, 10, 64)
			require.NoError(t, err)
			canonicalSum := sha256.Sum256([]byte(vector.CanonicalUTF8))
			require.Equal(t, vector.ExpectedSHA256, hex.EncodeToString(canonicalSum[:]))
			switch vector.Algorithm {
			case "generation_pinned_push_request_v1":
				rows := make([]protocolhash.PushRow, 0, len(vector.Rows))
				for _, row := range vector.Rows {
					version, err := strconv.ParseInt(row.Version, 10, 64)
					require.NoError(t, err)
					rows = append(rows, protocolhash.PushRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, BaseRowVersion: version, Payload: row.Payload})
				}
				gotHash, gotBytes, err := protocolhash.GenerationPinnedPushRequest(vector.GenerationID, vector.GenerationFingerprint, rows)
				require.NoError(t, err)
				require.Equal(t, vector.ExpectedSHA256, gotHash)
				require.Equal(t, wantBytes, gotBytes)
			case "committed_bundle_v1":
				rows := make([]protocolhash.BundleRow, 0, len(vector.Rows))
				for _, row := range vector.Rows {
					version, err := strconv.ParseInt(row.Version, 10, 64)
					require.NoError(t, err)
					rows = append(rows, protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: version, Payload: row.Payload})
				}
				gotHash, gotBytes, err := protocolhash.CommittedBundle(rows)
				require.NoError(t, err)
				require.Equal(t, vector.ExpectedSHA256, gotHash)
				require.Equal(t, wantBytes, gotBytes)
			case "snapshot_chunk_wire_json_v1":
				raw := []byte(vector.CanonicalUTF8)
				require.Equal(t, wantBytes, int64(len(raw)))
				require.NoError(t, validateSnapshotChunkResponseFieldPresence(raw))
				var chunk oversync.SnapshotChunkResponse
				require.NoError(t, json.Unmarshal(raw, &chunk))
				var logicalBytes int64
				for idx := range chunk.Rows {
					rowBytes, err := snapshotRowEncodedByteCount(&chunk.Rows[idx])
					require.NoError(t, err)
					logicalBytes += rowBytes
				}
				require.Equal(t, chunk.ByteCount, logicalBytes)
				require.NoError(t, validateSnapshotChunkResponse(&chunk, "snapshot-hash-vector", 12, 0, len(chunk.Rows), logicalBytes))
			default:
				t.Fatalf("unconsumed protocol hash algorithm %s", vector.Algorithm)
			}
		})
	}
}

type conformanceValueVector struct {
	ID            string         `json:"id"`
	Semantic      map[string]any `json:"semantic"`
	Codec         string         `json:"codec"`
	SQLiteValue   any            `json:"sqlite_value"`
	Expected      string         `json:"expected"`
	WireHex       string         `json:"wire_hex"`
	LimitKind     string         `json:"limit_kind"`
	ActualDecimal string         `json:"actual_decimal"`
}

func conformanceArtifactPath(t *testing.T, name string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Join(filepath.Dir(source), "..", "conformance", "client-upgrade", "v1", name)
}

func loadConformanceVectors(t *testing.T, name string) []conformanceValueVector {
	t.Helper()
	body, err := os.ReadFile(conformanceArtifactPath(t, name))
	require.NoError(t, err)
	var file conformanceValueFile
	require.NoError(t, json.Unmarshal(body, &file))
	require.NotEmpty(t, file.Vectors)
	return file.Vectors
}

func TestClientUpgradeConformance_I12TypedVectorsUseProductionCodecs(t *testing.T) {
	codecByName := map[string]LocalColumnCodec{
		"boolean_integer": LocalCodecBooleanInteger, "int64_integer": LocalCodecInt64Integer,
		"decimal_text": LocalCodecDecimalText,
		"float64_real": LocalCodecFloat64Real, "text": LocalCodecText,
		"bytes_blob": LocalCodecBytesBlob, "uuid_text": LocalCodecUUIDText,
		"uuid_blob": LocalCodecUUIDBlob, "json_text": LocalCodecJSONText,
	}
	seen := make(map[string]bool)
	for _, vector := range loadConformanceVectors(t, "values.json") {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			seen[vector.ID] = true
			wire, err := hex.DecodeString(vector.WireHex)
			require.NoError(t, err)
			switch vector.Codec {
			case "presence":
				var value map[string]any
				require.NoError(t, json.Unmarshal([]byte(`{}`), &value))
				_, present := value["field"]
				require.False(t, present)
				return
			case "nullable":
				client := nullableConformanceClient(t)
				got, err := client.sqliteValueWithDeclaredCodec("people", ColumnInfo{Name: "note"}, nil)
				require.NoError(t, err)
				require.Nil(t, got)
				return
			case "application_text", "application_integer":
				db := openUpgradeTestDB(t)
				columnType := strings.TrimPrefix(vector.Codec, "application_")
				_, err := db.Exec(`CREATE TABLE application_value(value ` + columnType + ` NOT NULL)`)
				require.NoError(t, err)
				_, err = db.Exec(`INSERT INTO application_value(value) VALUES (?)`, vector.SQLiteValue)
				require.NoError(t, err)
				var observed any
				if vector.Codec == "application_text" {
					var value string
					require.NoError(t, db.QueryRow(`SELECT value FROM application_value`).Scan(&value))
					observed = value
				} else {
					var value int64
					require.NoError(t, db.QueryRow(`SELECT value FROM application_value`).Scan(&value))
					observed = value
				}
				codec := LocalCodecText
				if vector.Codec == "application_integer" {
					codec = LocalCodecInt64Integer
				}
				encodedValue, err := wireValueFromSQLiteCodec(codec, observed)
				require.NoError(t, err)
				encoded, err := json.Marshal(encodedValue)
				require.NoError(t, err)
				require.Equal(t, string(wire), string(encoded))
				return
			}
			codec, ok := codecByName[vector.Codec]
			require.True(t, ok, vector.Codec)
			var want any
			decoder := json.NewDecoder(bytes.NewReader(wire))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&want))
			input := vector.SQLiteValue
			if vector.Expected == "rejected" {
				input = want
			}
			got, err := wireValueFromLocalCodec(codec, input)
			if vector.Expected == "rejected" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, want, got, "wire JSON type and value must both match the fixture")
		})
	}
	require.Len(t, seen, len(loadConformanceVectors(t, "values.json")))
}

func nullableConformanceClient(t *testing.T) *Client {
	t.Helper()
	local := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "note", WireType: oversync.GenerationWireTypeText, Nullable: true},
	})
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	return &Client{config: admittedFixtureConfig("main", local.Tables, local.Proof), localGeneration: compiled}
}

func TestClientUpgradeConformance_I12LimitVectorsUseProductionReaders(t *testing.T) {
	// Historical v1 fixture input: it models retired staged-upload limits.
	t.Skip("historical client-upgrade v1 limit fixture")
	seen := make(map[string]bool)
	for _, vector := range loadConformanceVectors(t, "limits.json") {
		vector := vector
		if vector.LimitKind == "generation_id_ascii_bytes" || vector.LimitKind == "generation_fingerprint_ascii_bytes" || vector.LimitKind == "snapshot_materialized_row_bytes" {
			continue
		}
		t.Run(vector.ID, func(t *testing.T) {
			actual, err := strconv.ParseInt(vector.ActualDecimal, 10, 64)
			require.NoError(t, err)
			wantError := vector.Expected == "rejected"
			var observed error
			switch vector.LimitKind {
			case "source_id_bytes":
				observed = validateSourceID(strings.Repeat("s", int(actual)))
			case "bundle_rows_hard":
				observed = validateCommittedPushBundleHardLimits(&committedPushBundle{PlannedRowCount: actual, RowCount: actual, ByteCount: 3})
			case "bundle_logical_bytes_hard":
				observed = validateCommittedPushBundleHardLimits(&committedPushBundle{PlannedRowCount: 1, RowCount: 1, ByteCount: actual})
			case "pull_logical_bytes_hard":
				remaining := actual
				var bundles []oversync.Bundle
				for remaining > 0 {
					bytes := min(remaining, int64(maxCommittedPushBundleBytes))
					bundles = append(bundles, oversync.Bundle{RowCount: 0, ByteCount: bytes})
					remaining -= bytes
				}
				observed = validatePullResponseLogicalLimits(bundles)
			case "negotiated_snapshot_chunk_rows":
				rows := make([]oversync.SnapshotRow, actual)
				for idx := range rows {
					rows[idx] = oversync.SnapshotRow{Schema: "main", Table: "people", Key: oversync.SyncKey{"id": fmt.Sprint(idx)}, RowVersion: 1, Payload: json.RawMessage(`{"id":"x"}`)}
				}
				byteCount := int64(0)
				for idx := range rows {
					encoded, err := snapshotRowEncodedByteCount(&rows[idx])
					require.NoError(t, err)
					byteCount += encoded
				}
				observed = validateSnapshotChunkResponse(&oversync.SnapshotChunkResponse{
					SnapshotID: "snapshot", SnapshotBundleSeq: 1, Rows: rows,
					NextRowOrdinal: actual, ByteCount: byteCount,
				}, "snapshot", 1, 0, 1000, byteCount)
			case "client_snapshot_chunk_bytes", "control_response_decoded_bytes":
				limit := int64(4 << 20)
				if vector.LimitKind == "control_response_decoded_bytes" {
					limit = snapshotControlBodyLimit
				}
				response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: ioNopCloser{Reader: bytes.NewReader(bytes.Repeat([]byte{'a'}, int(actual)))}}
				_, observed = readDecodedBodyBounded(context.Background(), vector.ID, response, limit)
			case "inner_json_depth":
				value := strings.Repeat("[", int(actual)) + "null" + strings.Repeat("]", int(actual))
				_, observed = wireValueFromLocalCodec(LocalCodecJSONText, value)
			default:
				t.Fatalf("unconsumed limit kind %s", vector.LimitKind)
			}
			if wantError {
				require.Error(t, observed)
			} else {
				require.NoError(t, observed)
			}
			seen[vector.ID] = true
		})
	}
	require.Len(t, seen, len(loadConformanceVectors(t, "limits.json"))-6)
}

func TestClientUpgradeConformance_I12SparseUpgradePreviewUsesProductionCapabilities(t *testing.T) {
	target := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	response, err := os.ReadFile(conformanceArtifactPath(t, filepath.Join("wire", "capabilities-target.response.json")))
	require.NoError(t, err)
	db := openUpgradeTestDB(t)
	require.NoError(t, initializeDatabase(db))
	require.NoError(t, persistUpgradeIdentity(context.Background(), db, upgradeIdentityRecord{
		DatabaseID: "conformance-preview", ServiceID: "service", UserID: "user-1",
		GenerationID: 1, GenerationFingerprint: strings.Repeat("a", 64), LocalVersion: 1, Ready: true,
	}))
	_, err = db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES ('preview-source',1)`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_attachment_state SET current_source_id='preview-source',binding_state='attached',attached_user_id='user-1',schema_name='main' WHERE singleton_key=1`)
	require.NoError(t, err)
	coordinator := &Opening{db: db, config: openingRuntimeConfig{
		Plan: UpgradePlan{TargetGenerationID: target.Proof.ID, testTarget: target, UserID: "user-1"}, BaseURL: "http://conformance.invalid", Token: tokenProviderForTests,
		TargetClientConfig: DefaultConfig(target.Proof.ID, target.Tables),
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			require.Equal(t, http.MethodGet, request.Method)
			if request.URL.Path == "/sync/generations/3" {
				return jsonResponse(oversync.GenerationDiscoveryResponse{
					ProtocolVersion: requiredProtocolVersion, SchemaGeneration: upgradeCapabilitiesValue(target).SchemaGeneration,
				}), nil
			}
			require.Equal(t, "/sync/generations/1", request.URL.Path)
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: ioNopCloser{Reader: bytes.NewReader(response)}}, nil
		})},
	}}
	var before int64
	require.NoError(t, db.QueryRow(`PRAGMA schema_version`).Scan(&before))
	require.NoError(t, coordinator.previewTarget(context.Background()))
	var after int64
	require.NoError(t, db.QueryRow(`PRAGMA schema_version`).Scan(&after))
	require.Equal(t, before, after)
}

func TestClientUpgradeConformance_I12WireResponsesUseProductionReaders(t *testing.T) {
	// Historical v1 fixture input: it contains verbose snapshot envelopes.
	t.Skip("historical client-upgrade v1 wire fixture")
	target := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	replacementRequest := snapshotApplyOptions{RotateSource: true, NewSourceID: "source-new", ReplacementReason: "schema_generation_upgrade"}
	requestBody, err := snapshotSessionCreateRequestFromOptions("source-old", replacementRequest)
	require.NoError(t, err)
	for _, test := range []struct {
		name       string
		status     int
		bodyFile   string
		body       string
		wantError  bool
		wantResult bool
	}{
		{name: "success", status: http.StatusOK, bodyFile: "replacement.response.json", wantResult: true},
		{name: "missing-byte-count", status: http.StatusOK, body: `{"snapshot_id":"snapshot-1","snapshot_bundle_seq":42,"row_count":0,"expires_at":"2030-01-01T00:00:00Z"}`, wantError: true},
		{name: "divergent-retirement", status: http.StatusConflict, body: `{"error":"source_retired","source_id":"source-old","replaced_by_source_id":"different-source"}`, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.body)
			if test.bodyFile != "" {
				body, err = os.ReadFile(conformanceArtifactPath(t, filepath.Join("wire", test.bodyFile)))
				require.NoError(t, err)
			}
			config := admittedFixtureConfig("main", target.Tables, target.Proof)
			client := &Client{
				BaseURL: "http://conformance.invalid", Token: tokenProviderForTests, config: config,
				sourceID: "source-old", UserID: "user-1", sessionConnected: true, schemaGenerationProof: target.Proof,
				HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodDelete {
						return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
					}
					require.Equal(t, http.MethodPost, request.Method)
					require.Equal(t, "/sync/snapshot-sessions", request.URL.Path)
					require.Equal(t, fmt.Sprint(target.Proof.ID), request.Header.Get(oversync.SchemaGenerationIDHeader))
					require.Equal(t, target.Proof.Fingerprint, request.Header.Get(oversync.SchemaGenerationFingerprintHeader))
					return &http.Response{StatusCode: test.status, Header: http.Header{"Content-Type": {"application/json"}}, Body: ioNopCloser{Reader: bytes.NewReader(body)}}, nil
				})},
			}
			session, callErr := client.createSnapshotSession(context.Background(), requestBody)
			if test.wantError {
				require.Error(t, callErr)
				return
			}
			require.NoError(t, callErr)
			require.Equal(t, test.wantResult, session != nil)
			require.Equal(t, "snapshot-1", session.SnapshotID)
		})
	}
}

type ioNopCloser struct {
	*bytes.Reader
}

func (ioNopCloser) Close() error { return nil }
