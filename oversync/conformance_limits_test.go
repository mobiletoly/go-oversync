package oversync

import (
	"bytes"
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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClientUpgradeConformance_I12GenerationProofLimitVectorsUseProductionReader(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	body, err := os.ReadFile(filepath.Join(filepath.Dir(source), "..", "conformance", "client-upgrade", "v1", "limits.json"))
	require.NoError(t, err)
	var file struct {
		Vectors []struct {
			ID            string `json:"id"`
			LimitKind     string `json:"limit_kind"`
			ActualDecimal string `json:"actual_decimal"`
			Expected      string `json:"expected"`
			WireHex       string `json:"wire_hex"`
			RepeatByteHex string `json:"repeat_byte_hex"`
			RepeatLength  int    `json:"repeat_length"`
		} `json:"vectors"`
	}
	require.NoError(t, json.Unmarshal(body, &file))
	seen := 0
	for _, vector := range file.Vectors {
		vector := vector
		if vector.LimitKind != "generation_id_ascii_bytes" && vector.LimitKind != "generation_fingerprint_ascii_bytes" {
			continue
		}
		t.Run(vector.ID, func(t *testing.T) {
			fixtureBytes, err := hex.DecodeString(vector.WireHex)
			if vector.WireHex == "" {
				repeated, decodeErr := hex.DecodeString(vector.RepeatByteHex)
				require.NoError(t, decodeErr)
				require.Len(t, repeated, 1)
				fixtureBytes = bytes.Repeat(repeated, vector.RepeatLength)
			}
			require.NoError(t, err)
			require.Equal(t, vector.ActualDecimal, fmt.Sprint(len(fixtureBytes)))
			generationID := "1"
			fingerprint := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			if vector.LimitKind == "generation_id_ascii_bytes" {
				generationID = string(fixtureBytes)
			} else {
				fingerprint = string(fixtureBytes)
			}
			_, err = parseSchemaGenerationProofHeaders(http.Header{
				SchemaGenerationIDHeader:          {generationID},
				SchemaGenerationFingerprintHeader: {fingerprint},
			})
			if vector.Expected == "rejected" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			seen++
		})
	}
	require.Equal(t, 4, seen)
}

func TestClientUpgradeConformance_I12WireRequestsUseProductionReaders(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Join(filepath.Dir(source), "..", "conformance", "client-upgrade", "v1")
	body, err := os.ReadFile(filepath.Join(root, "wire", "exchanges.json"))
	require.NoError(t, err)
	var file struct {
		Exchanges []struct {
			ID             string            `json:"id"`
			RequestBody    string            `json:"request_body"`
			RequestHeaders map[string]string `json:"request_headers"`
		} `json:"exchanges"`
	}
	require.NoError(t, json.Unmarshal(body, &file))
	for _, exchange := range file.Exchanges {
		exchange := exchange
		t.Run(exchange.ID, func(t *testing.T) {
			headers := make(http.Header)
			for name, value := range exchange.RequestHeaders {
				headers.Set(name, value)
			}
			_, err := parseSchemaGenerationProofHeaders(headers)
			require.NoError(t, err)
			if exchange.RequestBody != "replacement.request.json" {
				return
			}
			body, err := os.ReadFile(filepath.Join(root, "wire", exchange.RequestBody))
			require.NoError(t, err)
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			var request SnapshotSessionCreateRequest
			require.NoError(t, decoder.Decode(&request))
			require.NotNil(t, request.SourceReplacement)
			_, err = validateSnapshotSourceReplacement(Actor{SourceID: exchange.RequestHeaders[SourceIDHeader]}, request.SourceReplacement)
			require.NoError(t, err)
		})
	}
	require.Len(t, file.Exchanges, 7)
}

func TestClientUpgradeConformance_I12SnapshotRowByteVectorsUsePostgresMaterializer(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	body, err := os.ReadFile(filepath.Join(filepath.Dir(source), "..", "conformance", "client-upgrade", "v1", "limits.json"))
	require.NoError(t, err)
	var file struct {
		Vectors []struct {
			ID            string `json:"id"`
			LimitDecimal  string `json:"limit_decimal"`
			ActualDecimal string `json:"actual_decimal"`
			Expected      string `json:"expected"`
			RepeatByteHex string `json:"repeat_byte_hex"`
			RepeatLength  int    `json:"repeat_length"`
		} `json:"vectors"`
	}
	require.NoError(t, json.Unmarshal(body, &file))
	vectors := make(map[string]struct {
		LimitDecimal, ActualDecimal, Expected, RepeatByteHex string
		RepeatLength                                         int
	})
	for _, vector := range file.Vectors {
		if vector.ID == "snapshot-row-bytes-exact" || vector.ID == "snapshot-row-bytes-plus-one" {
			vectors[vector.ID] = struct {
				LimitDecimal, ActualDecimal, Expected, RepeatByteHex string
				RepeatLength                                         int
			}{vector.LimitDecimal, vector.ActualDecimal, vector.Expected, vector.RepeatByteHex, vector.RepeatLength}
		}
	}
	require.Len(t, vectors, 2)
	const rowID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	for _, test := range []struct {
		id, scenario string
	}{
		{id: "snapshot-row-bytes-exact", scenario: "rowbytesa"},
		{id: "snapshot-row-bytes-plus-one", scenario: "rowbytesb"},
	} {
		t.Run(test.id, func(t *testing.T) {
			vector := vectors[test.id]
			limit, err := strconv.ParseInt(vector.LimitDecimal, 10, 64)
			require.NoError(t, err)
			actual, err := strconv.ParseInt(vector.ActualDecimal, 10, 64)
			require.NoError(t, err)
			fixture := newSnapshotSessionFixture(t, test.scenario, snapshotSessionFixtureOptions{
				maxBytesPerSnapshotRow: defaultMaxBytesPerSnapshotRow, maxBytesPerSnapshotSession: limit, snapshotMaterializationBatchBytes: defaultMaxBytesPerSnapshotRow,
			})
			paddingByte, err := hex.DecodeString(vector.RepeatByteHex)
			require.NoError(t, err)
			require.Len(t, paddingByte, 1)
			padding := strings.Repeat(string(paddingByte), vector.RepeatLength)
			rowUUID := uuid.MustParse(rowID)
			_, err = pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{{
				Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": rowID}, Op: OpInsert,
				Payload: json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"%s","email":"fixed@example.com"}`, rowUUID, padding)),
			}})
			require.NoError(t, err)
			session, createErr := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			if vector.Expected == "accepted" {
				require.NoError(t, createErr)
				require.Equal(t, actual, session.ByteCount)
				var materialized int64
				require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT wire_byte_count FROM sync.snapshot_session_rows WHERE snapshot_id=$1::uuid`, session.SnapshotID).Scan(&materialized))
				require.Equal(t, actual, materialized)
				return
			}
			var limitErr *SnapshotSessionLimitExceededError
			require.ErrorAs(t, createErr, &limitErr)
			require.Equal(t, "logical_byte_count", limitErr.Dimension)
			require.Equal(t, actual, limitErr.Actual)
			require.Equal(t, limit, limitErr.Limit)
			require.Zero(t, fixture.snapshotSessionCountForUser(t))
			require.Zero(t, fixture.snapshotSessionRowCountForUser(t))
		})
	}
}
