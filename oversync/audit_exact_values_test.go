//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Green characterization probes document exact-value behavior that the current
// implementation already satisfies. Keep these separate from the contract
// reproducers below so each group can be run and reported independently.

func TestAuditExactValuesCharacterization_CanonicalJSONNestedStructure(t *testing.T) {
	raw := json.RawMessage(`{
		"z":[{"b":2,"a":1},["text",{"d":4,"c":3}]],
		"a":{"beta":false,"alpha":null}
	}`)

	canonical, err := canonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(
		t,
		`{"a":{"alpha":null,"beta":false},"z":[{"a":1,"b":2},["text",{"c":3,"d":4}]]}`,
		string(canonical),
	)
}

func TestAuditExactValuesCharacterization_CanonicalJSONExponentForms(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "positive exponent", input: `1E+30`, want: `1e+30`},
		{name: "small decimal notation", input: `4.50e-6`, want: `0.0000045`},
		{name: "small exponent notation", input: `1e-7`, want: `1e-7`},
		{name: "upper fixed boundary", input: `1e+20`, want: `100000000000000000000`},
		{name: "lower exponent boundary", input: `1e+21`, want: `1e+21`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, err := canonicalJSON(json.RawMessage(test.input))
			require.NoError(t, err)
			require.Equal(t, test.want, string(canonical))
		})
	}
}

func TestAuditExactValuesCharacterization_CanonicalJSONDoesNotNormalizeUnicode(t *testing.T) {
	raw := json.RawMessage(`{"nfd":"e\u0301","nfc":"\u00e9"}`)

	canonical, err := canonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(t, "{\"nfc\":\"é\",\"nfd\":\"é\"}", string(canonical))
	require.NotEqual(t, []byte("é"), []byte("é"), "NFC and NFD strings must remain byte-distinct")
}

func TestAuditExactValuesCharacterization_CanonicalJSONOrdersCommonUnicodeCodePoints(t *testing.T) {
	raw := json.RawMessage(`{"\u20ac":"euro","a":"ascii","\r":"control"}`)

	canonical, err := canonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(t, "{\"\\r\":\"control\",\"a\":\"ascii\",\"€\":\"euro\"}", string(canonical))
}

func TestAuditExactValuesCharacterization_ByteaBase64RoundTrip(t *testing.T) {
	const (
		schemaName = "audit"
		tableName  = "files"
	)
	binaryValue := []byte{0x00, 0x01, 0x02, 0x7f, 0x80, 0xfe, 0xff}
	encodedValue := base64.StdEncoding.EncodeToString(binaryValue)
	service := &SyncService{
		columnTypesByTable: map[string]map[string]string{
			Key(schemaName, tableName): {"data": "bytea"},
		},
	}
	payloadObject := map[string]any{
		"name": "binary-vector",
		"data": encodedValue,
	}

	require.NoError(t, service.normalizePushPayloadFields(schemaName, tableName, payloadObject))
	require.Equal(t, `\x0001027f80feff`, payloadObject["data"])

	storedPayload, err := json.Marshal(payloadObject)
	require.NoError(t, err)
	wirePayload, err := service.canonicalizeWirePayload(schemaName, tableName, storedPayload)
	require.NoError(t, err)
	require.Equal(t, `{"data":"AAECf4D+/w==","name":"binary-vector"}`, string(wirePayload))
	requirePayloadFieldBase64(t, wirePayload, "data", binaryValue)
}

func TestAuditExactValuesCharacterization_CommittedBundleHashDeterminism(t *testing.T) {
	rows := []BundleRow{
		{
			Schema:     "audit",
			Table:      "records",
			Key:        SyncKey{"id": "row-a"},
			Op:         OpInsert,
			RowVersion: 7,
			Payload:    json.RawMessage(`{"z":{"b":2,"a":1},"a":[true,null,"text"]}`),
		},
		{
			Schema:     "audit",
			Table:      "records",
			Key:        SyncKey{"id": "row-b"},
			Op:         OpUpdate,
			RowVersion: 8,
			Payload:    json.RawMessage(`{"value":42,"label":"second"}`),
		},
	}
	equivalentRows := []BundleRow{
		{
			Schema:     "audit",
			Table:      "records",
			Key:        SyncKey{"id": "row-a"},
			Op:         OpInsert,
			RowVersion: 7,
			Payload:    json.RawMessage(`{"a":[true,null,"text"],"z":{"a":1,"b":2}}`),
		},
		{
			Schema:     "audit",
			Table:      "records",
			Key:        SyncKey{"id": "row-b"},
			Op:         OpUpdate,
			RowVersion: 8,
			Payload:    json.RawMessage(`{"label":"second","value":42}`),
		},
	}

	wantHash, wantByteCount, err := computeCommittedBundleHash(rows)
	require.NoError(t, err)
	require.Len(t, wantHash, 32)
	require.Len(t, renderBundleHash(wantHash), 64)

	for range 10 {
		gotHash, gotByteCount, err := computeCommittedBundleHash(rows)
		require.NoError(t, err)
		require.Equal(t, wantHash, gotHash)
		require.Equal(t, wantByteCount, gotByteCount)
	}

	equivalentHash, equivalentByteCount, err := computeCommittedBundleHash(equivalentRows)
	require.NoError(t, err)
	require.Equal(t, wantHash, equivalentHash, "object member order must not alter the logical bundle hash")
	require.Equal(t, wantByteCount, equivalentByteCount)

	reversedRows := []BundleRow{rows[1], rows[0]}
	reversedHash, _, err := computeCommittedBundleHash(reversedRows)
	require.NoError(t, err)
	require.NotEqual(t, wantHash, reversedHash, "authoritative row order must contribute to the bundle hash")
}

// Contract regressions assert JCS plus uniform numeric strings.

func TestAuditExactValuesContract_CanonicalJSONPreservesTypedInt64Bounds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "minimum", raw: `-9223372036854775808`},
		{name: "maximum", raw: `9223372036854775807`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, err := canonicalJSON(json.RawMessage(`"` + test.raw + `"`))
			require.NoError(t, err)
			require.Equal(t, `"`+test.raw+`"`, string(canonical), "typed string canonicalization must preserve the exact int64 value")
		})
	}
}

func TestAuditExactValuesContract_CanonicalJSONPreservesTypedIntegersAboveTwoTo53(t *testing.T) {
	tests := []string{
		`9007199254740993`,
		`9007199254740995`,
	}

	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			canonical, err := canonicalJSON(json.RawMessage(`"` + raw + `"`))
			require.NoError(t, err)
			require.Equal(t, `"`+raw+`"`, string(canonical), "typed string canonicalization must not round an integer above 2^53")
		})
	}
}

func TestAuditExactValuesContract_CanonicalJSONPreservesTypedPreciseDecimals(t *testing.T) {
	tests := []string{
		`1234567890.123456789`,
		`0.100000000000000005`,
	}

	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			canonical, err := canonicalJSON(json.RawMessage(`"` + raw + `"`))
			require.NoError(t, err)
			require.Equal(t, `"`+raw+`"`, string(canonical), "typed string canonicalization must not round a precise decimal")
		})
	}
}

func TestAuditExactValuesContract_CanonicalJSONAcceptsTypedLargeExponent(t *testing.T) {
	const raw = `1e700`

	canonical, err := canonicalJSON(json.RawMessage(`"` + raw + `"`))
	require.NoError(t, err)
	require.Equal(t, `"1e700"`, string(canonical))
}

func TestAuditExactValuesContract_CanonicalJSONPreservesTypedExactNumbersWhenNested(t *testing.T) {
	raw := json.RawMessage(`{"outer":{"values":["9007199254740993",{"decimal":"1234567890.123456789"}]}}`)

	canonical, err := canonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(t, string(raw), string(canonical), "recursive canonicalization must preserve exact nested numbers")
}

func TestAuditExactValuesContract_CanonicalJSONUsesRFC8785UTF16PropertyOrder(t *testing.T) {
	// RFC 8785 sorts raw property names by UTF-16 code units. U+10000 starts
	// with 0xD800 and therefore sorts before BMP code point U+E000.
	raw := json.RawMessage(`{"\ue000":"bmp","\ud800\udc00":"supplementary"}`)

	canonical, err := canonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(t, "{\"𐀀\":\"supplementary\",\"\":\"bmp\"}", string(canonical))
}

func TestAuditExactValuesContract_CommittedBundleHashDistinguishesAdjacentTypedIntegers(t *testing.T) {
	rowsForNumber := func(rawNumber string) []BundleRow {
		return []BundleRow{{
			Schema:     "audit",
			Table:      "records",
			Key:        SyncKey{"id": "exact-number"},
			Op:         OpInsert,
			RowVersion: 1,
			Payload:    json.RawMessage(`{"value":"` + rawNumber + `"}`),
		}}
	}

	lowerHash, _, err := computeCommittedBundleHash(rowsForNumber(`9007199254740992`))
	require.NoError(t, err)
	upperHash, _, err := computeCommittedBundleHash(rowsForNumber(`9007199254740993`))
	require.NoError(t, err)
	require.NotEqual(t, lowerHash, upperHash, "different authoritative numeric values must not share a committed bundle hash")
}

func TestAuditExactValuesContract_PostgresNumericPayloadRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_exact_numeric_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "exact_numbers"}.Sanitize()

	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			bigint_value BIGINT NOT NULL,
			decimal_value NUMERIC NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, tableIdent))
	require.NoError(t, err)

	registeredTables := []RegisteredTable{{Schema: schemaName, Table: "exact_numbers", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations:              testSchemaGenerationsFromDatabase(t, ctx, pool, registeredTables, nil),
		AppName:                        "audit-exact-numeric-round-trip",
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	const (
		bigintMax      = `9223372036854775807`
		preciseDecimal = `12345678901234567890.12345678901234567890`
	)
	assertExactPayload := func(t *testing.T, channel string, payload json.RawMessage, rowID uuid.UUID) {
		t.Helper()

		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(payload, &fields))
		assert.Equal(t, `"`+bigintMax+`"`, string(fields["bigint_value"]), "%s BIGINT value must remain an exact typed string", channel)
		assert.Equal(t, `"`+preciseDecimal+`"`, string(fields["decimal_value"]), "%s NUMERIC value must remain an exact string", channel)
		assert.Equal(t, `"`+rowID.String()+`"`, string(fields["id"]), "%s row id must remain stable", channel)
	}

	verifyAuthoritativeRoundTrip := func(t *testing.T, writer Actor, rowID uuid.UUID, bundle *Bundle) {
		t.Helper()

		t.Run("business row", func(t *testing.T) {
			var storedBigint, storedDecimal string
			require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
				SELECT bigint_value::text, decimal_value::text
				FROM %s
				WHERE _sync_scope_id = $1 AND id = $2
			`, tableIdent), writer.UserID, rowID).Scan(&storedBigint, &storedDecimal))
			require.Equal(t, bigintMax, storedBigint, "business BIGINT must retain its exact maximum value")
			require.Equal(t, preciseDecimal, storedDecimal, "business NUMERIC must retain its declared precision")
		})

		t.Run("committed bundle", func(t *testing.T) {
			require.NotNil(t, bundle)
			require.Len(t, bundle.Rows, 1)
			assertExactPayload(t, "committed bundle payload", bundle.Rows[0].Payload, rowID)
		})

		reader := Actor{UserID: writer.UserID, SourceID: writer.SourceID + "-reader"}
		_, err := service.Connect(
			ctx,
			reader,
			testSchemaGenerationProofForService(service),
			&ConnectRequest{HasLocalPendingRows: false},
		)
		require.NoError(t, err)
		t.Run("pull", func(t *testing.T) {
			page, err := service.ProcessPull(ctx, reader, testSchemaGenerationProofForService(service), 0, 10, 0)
			require.NoError(t, err)
			require.Len(t, page.Bundles, 1)
			require.Len(t, page.Bundles[0].Rows, 1)
			assertExactPayload(t, "pull payload", page.Bundles[0].Rows[0].Payload, rowID)
		})

		t.Run("snapshot", func(t *testing.T) {
			session, err := service.CreateSnapshotSession(ctx, reader, testSchemaGenerationProofForService(service))
			require.NoError(t, err)
			chunk, err := service.GetSnapshotChunk(ctx, reader, testSchemaGenerationProofForService(service), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
			require.NoError(t, err)
			require.Len(t, chunk.Rows, 1)
			assertExactPayload(t, "snapshot payload", chunk.Rows[0].Payload, rowID)
		})
	}

	t.Run("native exact JSON numbers reject before mutation", func(t *testing.T) {
		rowID := uuid.New()
		writer := Actor{UserID: "audit-native-numeric-" + suffix, SourceID: "writer"}
		bundle, err := pushRowsOneShot(t, ctx, service, writer, 1, []PushRequestRow{{
			Schema:         schemaName,
			Table:          "exact_numbers",
			Key:            SyncKey{"id": rowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"id":"%s","bigint_value":%s,"decimal_value":%s}`,
				rowID,
				bigintMax,
				preciseDecimal,
			)),
		}})
		require.Error(t, err)
		require.Nil(t, bundle)
		var count int
		require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE _sync_scope_id = $1`, tableIdent), writer.UserID).Scan(&count))
		require.Zero(t, count, "unsupported native exact-number mapping must reject before mutation")
	})

	t.Run("exact typed database values", func(t *testing.T) {
		rowID := uuid.New()
		writer := Actor{UserID: "audit-uniform-numeric-" + suffix, SourceID: "writer"}
		// Quoted numeric lexemes isolate the outbound path: PostgreSQL accepts
		// them when populating typed BIGINT/NUMERIC columns, after which bundle,
		// pull, and snapshot payloads originate from exact database values.
		bundle, err := pushRowsOneShot(t, ctx, service, writer, 1, []PushRequestRow{{
			Schema:         schemaName,
			Table:          "exact_numbers",
			Key:            SyncKey{"id": rowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"id":"%s","bigint_value":"%s","decimal_value":"%s"}`,
				rowID,
				bigintMax,
				preciseDecimal,
			)),
		}})
		require.NoError(t, err)
		verifyAuthoritativeRoundTrip(t, writer, rowID, bundle)
	})
}
