package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type snapshotAccountingQueryRecorder struct {
	*sql.DB
	stagedQueries int
}

func (r *snapshotAccountingQueryRecorder) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, "FROM _sync_snapshot_stage") {
		r.stagedQueries++
		if !strings.Contains(query, "LIMIT ?") {
			return nil, fmt.Errorf("snapshot accounting query is not paged")
		}
	}
	return r.DB.QueryContext(ctx, query, args...)
}

func codecTestClient(t *testing.T) *Client {
	t.Helper()
	columns := []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "boolean_value", WireType: oversync.GenerationWireTypeBoolean},
		{Name: "int16_value", WireType: oversync.GenerationWireTypeInt64},
		{Name: "int32_value", WireType: oversync.GenerationWireTypeInt64},
		{Name: "int64_value", WireType: oversync.GenerationWireTypeInt64},
		{Name: "decimal_value", WireType: oversync.GenerationWireTypeDecimal},
		{Name: "float32_value", WireType: oversync.GenerationWireTypeFloat64},
		{Name: "float64_value", WireType: oversync.GenerationWireTypeFloat64},
		{Name: "text_value", WireType: oversync.GenerationWireTypeText},
		{Name: "bytes_value", WireType: oversync.GenerationWireTypeBytes},
		{Name: "uuid_blob_value", WireType: oversync.GenerationWireTypeUUID},
		{Name: "json_value", WireType: oversync.GenerationWireTypeJSON},
	}
	local := testUpgradeContract(t, 1, columns)
	local.Columns[0].Columns = []LocalColumnContract{
		{Name: "id", Codec: LocalCodecUUIDText},
		{Name: "boolean_value", Codec: LocalCodecBooleanInteger},
		{Name: "int16_value", Codec: LocalCodecInt64Integer},
		{Name: "int32_value", Codec: LocalCodecInt64Integer},
		{Name: "int64_value", Codec: LocalCodecInt64Integer},
		{Name: "decimal_value", Codec: LocalCodecDecimalText},
		{Name: "float32_value", Codec: LocalCodecFloat64Real},
		{Name: "float64_value", Codec: LocalCodecFloat64Real},
		{Name: "text_value", Codec: LocalCodecText},
		{Name: "bytes_value", Codec: LocalCodecBytesBlob},
		{Name: "uuid_blob_value", Codec: LocalCodecUUIDBlob},
		{Name: "json_value", Codec: LocalCodecJSONText},
	}
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	return &Client{config: admittedFixtureConfig("main", local.Tables, local.Proof), localGeneration: compiled}
}

func TestUpgradeCodec_I07TypedValueRoundTripAndBounds(t *testing.T) {
	client := codecTestClient(t)
	local := map[string]any{
		"id": "550e8400-e29b-41d4-a716-446655440000", "boolean_value": "1",
		"int16_value": "-32768", "int32_value": "2147483647", "int64_value": "9007199254740993",
		"decimal_value": "1234567890.123456789", "float32_value": "1.5", "float64_value": "-0",
		"text_value": "Zażółć 😀", "bytes_value": "00ff", "uuid_blob_value": "550e8400e29b41d4a716446655440000",
		"json_value": `{"a":[true,null],"z":0}`,
	}
	require.NoError(t, client.processPayloadWithDeclaredCodecs("people", local))
	require.Equal(t, true, local["boolean_value"])
	require.Equal(t, "9007199254740993", local["int64_value"])
	require.Equal(t, "1.5", local["float32_value"])
	require.Equal(t, "0", local["float64_value"])
	require.Equal(t, "AP8=", local["bytes_value"])
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", local["uuid_blob_value"])

	_, err := wireValueFromLocalCodec(LocalCodecBooleanInteger, "2")
	require.Error(t, err)
	_, err = wireValueFromLocalCodec(LocalCodecInt64Integer, "9223372036854775808")
	require.Error(t, err)
	_, err = wireValueFromLocalCodec(LocalCodecDecimalText, "1e2")
	require.Error(t, err)
	roundedFloat32, err := wireValueFromLocalCodec(LocalCodecFloat64Real, "1.2345678901234567")
	require.NoError(t, err)
	require.Equal(t, "1.2345678901234567", roundedFloat32)
	_, err = wireValueFromLocalCodec(LocalCodecText, "a\x00b")
	require.Error(t, err)
	_, err = wireValueFromLocalCodec(LocalCodecJSONText, `{"bad":"\u0000"}`)
	require.Error(t, err)
	_, err = wireValueFromLocalCodec(LocalCodecUUIDText, "550E8400-E29B-41D4-A716-446655440000")
	require.Error(t, err)
	_, err = wireValueFromLocalCodec(LocalCodecBytesBlob, "Ff")
	require.Error(t, err)
}

func TestUpgradeCodec_I07RealLocalWritePreservesBinary64ThroughUpload(t *testing.T) {
	client := codecTestClient(t)
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(float32_value REAL NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO people(float32_value) VALUES (?)`, 1.2345678901234567)
	require.NoError(t, err)

	var local float64
	require.NoError(t, db.QueryRow(`SELECT float32_value FROM people`).Scan(&local))
	payload := map[string]any{"float32_value": local}
	require.NoError(t, client.processPayloadWithDeclaredCodecs("people", payload))
	require.Equal(t, "1.2345678901234567", payload["float32_value"])
}

func TestUpgradeCodec_I07WireToSQLiteUsesDeclaredCodec(t *testing.T) {
	client := codecTestClient(t)
	tests := []struct {
		column string
		wire   any
		want   any
	}{
		{column: "boolean_value", wire: true, want: int64(1)},
		{column: "int64_value", wire: "9007199254740993", want: int64(9007199254740993)},
		{column: "decimal_value", wire: "123.450", want: "123.450"},
		{column: "float32_value", wire: "1.5", want: float64(1.5)},
		{column: "float64_value", wire: "0", want: float64(0)},
		{column: "text_value", wire: "hello", want: "hello"},
		{column: "bytes_value", wire: "AP8=", want: []byte{0, 255}},
		{column: "uuid_blob_value", wire: "550e8400-e29b-41d4-a716-446655440000", want: []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}},
		{column: "json_value", wire: `{"a":1}`, want: `{"a":1}`},
	}
	for _, test := range tests {
		got, err := client.sqliteValueWithDeclaredCodec("people", ColumnInfo{Name: test.column}, test.wire)
		require.NoError(t, err, test.column)
		require.Equal(t, test.want, got, test.column)
	}
	_, err := client.sqliteValueWithDeclaredCodec("people", ColumnInfo{Name: "boolean_value"}, "true")
	require.Error(t, err)
	_, err = client.sqliteValueWithDeclaredCodec("people", ColumnInfo{Name: "bytes_value"}, "AA")
	require.Error(t, err)
}

func TestUpgradeCodec_I04DeclaredBooleanAuthoritativeComparisonUsesCodecRoundTrip(t *testing.T) {
	client := codecTestClient(t)
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE people(id TEXT PRIMARY KEY, boolean_value INTEGER NOT NULL)`)
	require.NoError(t, err)
	client.db = db
	normalized, err := client.normalizeWirePayloadForSQLiteRoundTrip("people", `{"boolean_value":true}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"boolean_value":true}`, string(normalized))
}

func TestUpgradeCodec_I04StoredLocalPayloadRestoresBooleanAndBytesWithoutWireReinterpretation(t *testing.T) {
	columns := []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "enabled", WireType: oversync.GenerationWireTypeBoolean},
		{Name: "raw", WireType: oversync.GenerationWireTypeBytes},
	}
	local := testUpgradeContract(t, 1, columns)
	local.Columns[0].Columns = []LocalColumnContract{
		{Name: "id", Codec: LocalCodecUUIDText},
		{Name: "enabled", Codec: LocalCodecBooleanInteger},
		{Name: "raw", Codec: LocalCodecBytesBlob},
	}
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	db := openUpgradeTestDB(t)
	_, err = db.Exec(`CREATE TABLE people(id TEXT PRIMARY KEY, enabled INTEGER NOT NULL, raw BLOB NOT NULL)`)
	require.NoError(t, err)
	discovery := upgradeCapabilitiesValue(local)
	client := &Client{
		db: db, config: admittedFixtureConfig("main", local.Tables, local.Proof), localGeneration: compiled,
		discovery: &discovery,
		pkByTable: map[string]string{"people": "id"}, keyByTable: map[string][]string{"people": {"id"}},
	}
	tx, err := db.Begin()
	require.NoError(t, err)
	id := "550e8400-e29b-41d4-a716-446655440000"
	require.NoError(t, client.upsertLocalRowInTxUsing(context.Background(), tx, tx, "people", map[string]any{
		"id":      map[string]any{"storage": "text", "value": id},
		"enabled": map[string]any{"storage": "integer", "value": "1"},
		"raw":     map[string]any{"storage": "blob", "value": "00ff"},
	}))
	require.NoError(t, tx.Commit())
	var enabled int64
	var raw []byte
	require.NoError(t, db.QueryRow(`SELECT enabled,raw FROM people WHERE id=?`, id).Scan(&enabled, &raw))
	require.Equal(t, int64(1), enabled)
	require.Equal(t, []byte{0, 255}, raw)
}

func TestUpgradeCodec_I09UUIDBlobSnapshotTotalsUseWireKeyBytes(t *testing.T) {
	columns := []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}
	local := testUpgradeContract(t, 1, columns)
	local.Columns[0].Columns[0].Codec = LocalCodecUUIDBlob
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	db := openUpgradeTestDB(t)
	require.NoError(t, initializeDatabase(db))
	_, err = db.Exec(`CREATE TABLE people(id BLOB PRIMARY KEY NOT NULL)`)
	require.NoError(t, err)
	discovery := upgradeCapabilitiesValue(local)
	client := &Client{
		db: db, config: admittedFixtureConfig("main", local.Tables, local.Proof), localGeneration: compiled,
		discovery: &discovery,
		pkByTable: map[string]string{"people": "id"}, keyByTable: map[string][]string{"people": {"id"}},
	}
	id := "550e8400-e29b-41d4-a716-446655440000"
	localKey := `{"id":"550e8400e29b41d4a716446655440000"}`
	payload := `{"id":"550e8400-e29b-41d4-a716-446655440000"}`
	_, err = db.Exec(`INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES ('s',0,'main','people',?,1,?)`, localKey, payload)
	require.NoError(t, err)
	count, byteCount, err := client.stagedSnapshotTotals(context.Background(), db, "s")
	require.NoError(t, err)
	wantBytes := testSnapshotRowsByteCount([]oversync.SnapshotRow{{
		Schema: "main", Table: "people", Key: oversync.SyncKey{"id": id}, RowVersion: 1, Payload: []byte(payload),
	}})
	require.Equal(t, int64(1), count)
	require.Equal(t, wantBytes, byteCount)
}

func TestUpgradeCodec_I09SnapshotTotalsPageWithoutWholeSnapshotMaterialization(t *testing.T) {
	columns := []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}
	local := testUpgradeContract(t, 1, columns)
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	db := openUpgradeTestDB(t)
	require.NoError(t, initializeDatabase(db))
	_, err = db.Exec(`CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL)`)
	require.NoError(t, err)
	discovery := upgradeCapabilitiesValue(local)
	client := &Client{
		db: db, config: admittedFixtureConfig("main", local.Tables, local.Proof), localGeneration: compiled,
		discovery: &discovery,
		pkByTable: map[string]string{"people": "id"}, keyByTable: map[string][]string{"people": {"id"}},
	}
	stmt, err := db.Prepare(`INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES ('paged',?,'main','people',?,1,?)`)
	require.NoError(t, err)
	defer stmt.Close()
	var wantBytes int64
	const rowCount = upgradeSnapshotAccountingPageRows*2 + 1
	for ordinal := 0; ordinal < rowCount; ordinal++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", ordinal)
		keyJSON := fmt.Sprintf(`{"id":%q}`, id)
		payload := fmt.Sprintf(`{"id":%q}`, id)
		_, err := stmt.Exec(ordinal, keyJSON, payload)
		require.NoError(t, err)
		rowBytes := testSnapshotRowsByteCount([]oversync.SnapshotRow{{
			Schema: "main", Table: "people", Key: oversync.SyncKey{"id": id}, RowVersion: 1, Payload: []byte(payload),
		}})
		wantBytes += rowBytes
	}
	recorder := &snapshotAccountingQueryRecorder{DB: db}
	gotRows, gotBytes, err := client.stagedSnapshotTotals(context.Background(), recorder, "paged")
	require.NoError(t, err)
	require.Equal(t, int64(rowCount), gotRows)
	require.Equal(t, wantBytes, gotBytes)
	require.Equal(t, 3, recorder.stagedQueries, "accounting must consume bounded pages")
}
