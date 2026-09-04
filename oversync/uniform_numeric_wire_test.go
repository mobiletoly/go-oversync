package oversync

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func uniformNumericWireTestService() *SyncService {
	return &SyncService{
		columnTypesByTable: map[string]map[string]string{
			"business.numeric_values": {
				"small_count": "int2",
				"count":       "int4",
				"large_count": "int8",
				"amount":      "numeric",
				"ratio":       "float4",
				"score":       "float8",
				"disabled":    "bool",
				"enabled":     "bool",
			},
		},
	}
}

func TestUniformNumericWire_NormalizePushAcceptsExactGenerationTokens(t *testing.T) {
	t.Parallel()
	service := uniformNumericWireTestService()
	payload := map[string]any{
		"small_count": "-32768",
		"count":       "2147483647",
		"large_count": "9007199254740993",
		"amount":      "1234567890.123456789",
		"ratio":       "1.2345678901234567",
		"score":       "5e-324",
		"disabled":    false,
		"enabled":     true,
	}

	require.NoError(t, service.normalizePushPayloadFields("business", "numeric_values", payload))
	require.Equal(t, false, payload["disabled"])
	require.Equal(t, true, payload["enabled"])
	require.Equal(t, "1.2345678901234567", payload["ratio"], "request stays binary64; PostgreSQL rounds the committed value")
}

func TestUniformNumericWire_NormalizePushRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	service := uniformNumericWireTestService()
	tests := []struct {
		name   string
		column string
		value  any
	}{
		{name: "integer JSON number", column: "large_count", value: json.Number("42")},
		{name: "int2 overflow", column: "small_count", value: "32768"},
		{name: "int4 overflow", column: "count", value: "2147483648"},
		{name: "float JSON number", column: "score", value: json.Number("1.25")},
		{name: "noncanonical float", column: "score", value: "1.0"},
		{name: "float4 destination overflow", column: "ratio", value: "1e+100"},
		{name: "float NaN", column: "score", value: "NaN"},
		{name: "Boolean word", column: "enabled", value: "true"},
		{name: "Boolean other integer", column: "enabled", value: "2"},
		{name: "Boolean SQLite bridge integer", column: "enabled", value: "1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := map[string]any{test.column: test.value}
			var validationErr *PushValidationError
			require.ErrorAs(t, service.normalizePushPayloadFields("business", "numeric_values", payload), &validationErr)
		})
	}
}

func TestUniformNumericWire_CanonicalizesEveryServerNumericFamily(t *testing.T) {
	t.Parallel()
	service := uniformNumericWireTestService()
	payload := json.RawMessage(`{
		"small_count":-32768,
		"count":2147483647,
		"large_count":9007199254740993,
		"amount":1234567890.1234567890,
		"ratio":1.2345679,
		"score":5e-324,
		"disabled":false,
		"enabled":true
	}`)

	canonical, err := service.canonicalizeWirePayload("business", "numeric_values", payload)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"small_count":"-32768",
		"count":"2147483647",
		"large_count":"9007199254740993",
		"amount":"1234567890.1234567890",
		"ratio":"1.2345678806304932",
		"score":"5e-324",
		"disabled":false,
		"enabled":true
	}`, string(canonical))
}

func TestUniformNumericWire_RejectsNonFiniteServerValues(t *testing.T) {
	t.Parallel()
	service := uniformNumericWireTestService()

	for _, payload := range []json.RawMessage{
		json.RawMessage(`{"ratio":"NaN"}`),
		json.RawMessage(`{"score":"Infinity"}`),
		json.RawMessage(`{"score":"-Infinity"}`),
	} {
		_, err := service.canonicalizeWirePayload("business", "numeric_values", payload)
		require.Error(t, err)
	}
}

func TestUniformNumericWire_AdvertisesSnapshotProtocolV1(t *testing.T) {
	t.Parallel()
	require.Equal(t, "v1", SyncProtocolVersion)
}

func TestUniformNumericWire_ValidatesRegisteredPostgresTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		typeSchema     string
		typeName       string
		typeKind       string
		typeCategory   string
		baseTypeSchema string
		baseTypeName   string
		wantError      bool
	}{
		{name: "int2", typeSchema: "pg_catalog", typeName: "int2", typeKind: "b", typeCategory: "N"},
		{name: "int4", typeSchema: "pg_catalog", typeName: "int4", typeKind: "b", typeCategory: "N"},
		{name: "int8", typeSchema: "pg_catalog", typeName: "int8", typeKind: "b", typeCategory: "N"},
		{name: "numeric", typeSchema: "pg_catalog", typeName: "numeric", typeKind: "b", typeCategory: "N"},
		{name: "float4", typeSchema: "pg_catalog", typeName: "float4", typeKind: "b", typeCategory: "N"},
		{name: "float8", typeSchema: "pg_catalog", typeName: "float8", typeKind: "b", typeCategory: "N"},
		{name: "Boolean", typeSchema: "pg_catalog", typeName: "bool", typeKind: "b", typeCategory: "B"},
		{name: "text", typeSchema: "pg_catalog", typeName: "text", typeKind: "b", typeCategory: "S"},
		{name: "money", typeSchema: "pg_catalog", typeName: "money", typeKind: "b", typeCategory: "N", wantError: true},
		{name: "numeric domain", typeSchema: "business", typeName: "amount", typeKind: "d", typeCategory: "N", baseTypeSchema: "pg_catalog", baseTypeName: "numeric", wantError: true},
		{name: "custom numeric", typeSchema: "extension", typeName: "custom_number", typeKind: "b", typeCategory: "N", wantError: true},
		{name: "array", typeSchema: "pg_catalog", typeName: "_int8", typeKind: "b", typeCategory: "A", wantError: true},
		{name: "range", typeSchema: "pg_catalog", typeName: "numrange", typeKind: "r", typeCategory: "R", wantError: true},
		{name: "composite", typeSchema: "business", typeName: "numeric_pair", typeKind: "c", typeCategory: "C", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRegisteredPayloadColumnType(
				"business.values",
				"value",
				test.typeSchema,
				test.typeName,
				test.typeKind,
				test.typeCategory,
				test.baseTypeSchema,
				test.baseTypeName,
			)
			if test.wantError {
				var unsupported *UnsupportedSchemaError
				require.ErrorAs(t, err, &unsupported)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
