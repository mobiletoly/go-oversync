package oversync

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/stretchr/testify/require"
)

// snapshotEncodingTestGeneration lists eleven filler tables before main.items so its table index has
// two digits, and gives items every wire type.
func snapshotEncodingTestGeneration() SchemaGenerationCapabilities {
	var tables []SchemaGenerationTableCapabilities
	for i := 0; i < 11; i++ {
		tables = append(tables, SchemaGenerationTableCapabilities{Schema: "main", Table: fmt.Sprintf("filler_%02d", i), SyncKeyColumns: []string{"id"},
			Columns: []SchemaGenerationColumnCapabilities{{Name: "id", WireType: GenerationWireTypeUUID}}})
	}
	return SchemaGenerationCapabilities{ID: "generation", Fingerprint: strings.Repeat("a", 64), Tables: append(tables, SchemaGenerationTableCapabilities{
		Schema: "main", Table: "items", SyncKeyColumns: []string{"id"},
		Columns: []SchemaGenerationColumnCapabilities{
			{Name: "id", WireType: GenerationWireTypeUUID},
			{Name: "name", WireType: GenerationWireTypeText, Nullable: true},
			{Name: "flag", WireType: GenerationWireTypeBoolean, Nullable: true},
			{Name: "count", WireType: GenerationWireTypeInt64},
			{Name: "amount", WireType: GenerationWireTypeDecimal},
			{Name: "ratio", WireType: GenerationWireTypeFloat64},
			{Name: "data", WireType: GenerationWireTypeBytes},
			{Name: "doc", WireType: GenerationWireTypeJSON, Nullable: true},
		},
	})}
}

// The hash frame, logical size and expanded size assembled from one canonical payload equal what the
// validating hash entry and the existing encoders compute by serializing the whole row again.
func TestSnapshotRowEncoding_CanonicalPartsMatchTheValidatingHash(t *testing.T) {
	generation := snapshotEncodingTestGeneration()
	itemsIndex := len(generation.Tables) - 1
	staged := newStagedSnapshotTable(generation, registeredTableRuntimeInfo{schemaName: "main", tableName: "items"}, GenerationTableContract{SyncKeyColumns: []string{"id"}})
	require.Equal(t, itemsIndex, staged.index)

	texts := []string{"", "Ada", "Zoë", "名前", "emoji 🎉", `quote " and \ backslash`, "tab\tnew\nline\r", "<script>&amp;</script>",
		"x" + string(rune(0x2028)) + "y" + string(rune(0x2029)), "\u0001\u001f\u007f", "/slash/", strings.Repeat("long ", 400)}
	reference, err := NewSnapshotHashWriter("source", generation, "snapshot", 123456789012, nil)
	require.NoError(t, err)
	assembled, err := NewSnapshotHashWriter("source", generation, "snapshot", 123456789012, nil)
	require.NoError(t, err)
	versions := []int64{1, 9, 10, 99, 100, 123456789012}
	for i, text := range texts {
		id := uuid.NewString()
		version := versions[i%len(versions)]
		var name any = text
		if i == 3 {
			name = nil
		}
		// Database-style JSON: spaces after separators and keys out of canonical order.
		payload := json.RawMessage(fmt.Sprintf(`{"name": %s, "id": %q, "flag": %t, "count": "%d", "amount": "-12.5", "ratio": "0.1", "data": "AAEC/w==", "doc": %s}`,
			mustJSONText(t, name), id, i%2 == 0, i*1000-3, mustJSONText(t, `{"a":[true,null],"b":"`+strconv.Itoa(i)+`"}`)))
		key := []any{id}

		object, err := jcs.DecodeObjectLossless(payload)
		require.NoError(t, err)
		keyCanonical, err := jcs.Marshal(id)
		require.NoError(t, err)
		encoding, err := encodeStagedSnapshotRow(staged, keyCanonical, version, object)
		require.NoError(t, err)

		expectedFrame, err := jcs.Marshal([]any{"i", strconv.Itoa(itemsIndex), key, strconv.FormatInt(version, 10), object})
		require.NoError(t, err)
		require.Equal(t, string(expectedFrame), string(encoding.parts.frameBody()), "row %d", i)

		row := SnapshotRow{Schema: "main", Table: "items", Key: SyncKey{"id": id}, RowVersion: version, Payload: payload}
		_, logical, err := CompactGroupsForSnapshotRowsAt(generation, []SnapshotRow{row}, 1)
		require.NoError(t, err)
		require.Equal(t, logical, encoding.parts.logicalByteCount(), "row %d", i)

		expanded, err := ExpandedRowDecodedBytes(PushRequestRow{Schema: "main", Table: "items", Key: row.Key, Op: OpInsert, BaseRowVersion: version, Payload: payload})
		require.NoError(t, err)
		require.Equal(t, expanded, encoding.expandedBytes, "row %d", i)

		require.NoError(t, reference.Add(itemsIndex, key, version, payload))
		require.NoError(t, assembled.addCanonical(encoding.parts))
	}
	require.Equal(t, reference.rows, assembled.rows)
	require.Equal(t, reference.logicalBytes, assembled.logicalBytes)
	referenceHash, err := reference.Sum(reference.rows, reference.logicalBytes)
	require.NoError(t, err)
	assembledHash, err := assembled.Sum(assembled.rows, assembled.logicalBytes)
	require.NoError(t, err)
	require.Equal(t, referenceHash, assembledHash)

	require.Error(t, assembled.addCanonical(snapshotRowCanonicalParts{tableIndex: itemsIndex, keyValues: []byte(`["x"]`), version: 1, payload: []byte(`{}`)}), "finalized writer")
	fresh, err := NewSnapshotHashWriter("source", generation, "snapshot", 1, nil)
	require.NoError(t, err)
	require.Error(t, fresh.addCanonical(snapshotRowCanonicalParts{tableIndex: len(generation.Tables), keyValues: []byte(`["x"]`), version: 1, payload: []byte(`{}`)}))
	require.Error(t, fresh.addCanonical(snapshotRowCanonicalParts{tableIndex: 0, keyValues: []byte(`["x"]`), version: 0, payload: []byte(`{}`)}))
}

// Chunk groups built row by row while selecting serialize to the same bytes as the snapshot encoder
// over the whole chunk, across table changes and composite keys.
func TestSnapshotRowEncoding_ChunkGroupsMatchTheSnapshotEncoder(t *testing.T) {
	generation := compactTestGeneration()
	rows := []SnapshotRow{
		{Schema: "main", Table: "users", Key: SyncKey{"id": "u1"}, RowVersion: 3, Payload: json.RawMessage(`{"id": "u1", "name": "<Ada>"}`)},
		{Schema: "main", Table: "users", Key: SyncKey{"id": "u2"}, RowVersion: 4, Payload: json.RawMessage(`{"id":"u2","name":"Zoë"}`)},
		{Schema: "main", Table: "projects", Key: SyncKey{"project_id": "7", "tenant_id": "t"}, RowVersion: 5, Payload: json.RawMessage(`{"project_id":"7","tenant_id":"t"}`)},
		{Schema: "main", Table: "users", Key: SyncKey{"id": "u3"}, RowVersion: 6, Payload: json.RawMessage(`{"id":"u3","name":"x y"}`)},
	}
	for count := 0; count <= len(rows); count++ {
		encoder := newSnapshotChunkGroupEncoder(generation)
		groups := make([]CompactRowGroup, 0, 1)
		for i, row := range rows[:count] {
			group, err := encoder.rowGroup(row, int64(7+i))
			require.NoError(t, err)
			groups = appendSnapshotRowGroup(groups, group)
		}
		expected, _, err := CompactGroupsForSnapshotRowsAt(generation, rows[:count], 7)
		require.NoError(t, err)
		require.Equal(t, mustJSON(t, expected), mustJSON(t, groups), "first %d rows", count)
	}

	encoder := newSnapshotChunkGroupEncoder(generation)
	_, err := encoder.rowGroup(SnapshotRow{Schema: "main", Table: "missing", Key: SyncKey{"id": "u"}, Payload: json.RawMessage(`{}`)}, 1)
	require.Error(t, err)
	_, err = encoder.rowGroup(SnapshotRow{Schema: "main", Table: "projects", Key: SyncKey{"project_id": "7"}, Payload: json.RawMessage(`{}`)}, 1)
	require.Error(t, err)
	_, err = encoder.rowGroup(SnapshotRow{Schema: "main", Table: "users", Key: SyncKey{"id": "u"}, Payload: json.RawMessage(`[]`)}, 1)
	require.Error(t, err)
}

// Serving no longer decodes stored payloads, so a staged row changed after sealing is served, and the
// client's check of the served rows against the sealed hash rejects it.
func TestSnapshotRowEncoding_StagedRowChangedAfterSealingFailsTheClientHashCheck(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "row_encoding_tamper", snapshotSessionFixtureOptions{})
	id := uuid.New()
	session := fixture.createSessionWithUser(t, id, "Alpha")
	require.Equal(t, session.SnapshotHash, servedSnapshotHash(t, fixture, session, 10))

	tag, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE sync.snapshot_session_rows
		SET payload_wire = replace(payload_wire::text, '"Alpha"', '"Omega"')::json
		WHERE snapshot_id = $1::uuid
	`, session.SnapshotID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
	require.NotEqual(t, session.SnapshotHash, servedSnapshotHash(t, fixture, session, 10))
}

func mustJSONText(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return string(encoded)
}
