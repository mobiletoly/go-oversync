package oversqlite

import (
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// declaredColumn runs for every column of every row the client applies, captures or verifies. It must
// resolve from an index built with the local generation, not by copying the generation's tables.
func TestDeclaredColumn_ResolvesEveryColumnWithoutAllocating(t *testing.T) {
	c, _ := newBundleClient(t, "main", []SyncTable{{TableName: "authors", SyncKeyColumnName: "id"}, {TableName: "books", SyncKeyColumnName: "id"}},
		`CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, rating REAL)`,
		`CREATE TABLE books(id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, pages INTEGER, cover BLOB,
			author_id TEXT REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED)`)

	// Reference: the declaration as the generation contract itself states it.
	reference := func(table, column string) (LocalColumnCodec, oversync.GenerationColumn, bool) {
		codec, ok := c.localGeneration.codecs[c.schemaForTable(table)+"."+table][column]
		if !ok {
			return "", oversync.GenerationColumn{}, false
		}
		for _, contract := range c.localGeneration.contract.Tables() {
			if contract.Schema == c.schemaForTable(table) && contract.Table == table {
				for _, declared := range contract.Columns {
					if declared.Name == column {
						return codec, declared, true
					}
				}
			}
		}
		return "", oversync.GenerationColumn{}, false
	}
	checked := 0
	for _, contract := range c.localGeneration.contract.Tables() {
		for _, declared := range contract.Columns {
			for _, spelling := range []string{declared.Name, strings.ToUpper(declared.Name)} {
				wantCodec, wantColumn, wantOK := reference(contract.Table, declared.Name)
				codec, column, ok := c.declaredColumn(strings.ToUpper(contract.Table), spelling)
				require.Equal(t, wantOK, ok, "%s.%s", contract.Table, spelling)
				require.Equal(t, wantCodec, codec, "%s.%s", contract.Table, spelling)
				require.Equal(t, wantColumn, column, "%s.%s", contract.Table, spelling)
				checked++
			}
		}
	}
	require.Equal(t, 2*8, checked, "8 declared columns, each looked up in two spellings")
	_, _, ok := c.declaredColumn("books", "missing")
	require.False(t, ok)
	_, _, ok = c.declaredColumn("missing", "id")
	require.False(t, ok)
	for _, table := range []string{"authors", "books", "missing"} {
		resolved, local := c.localGeneration.schemaByTable[table]
		if !local {
			resolved = c.config.schema
		}
		require.Equal(t, c.schemaForTable(table), resolved, "schema of %s", table)
	}

	allocations := testing.AllocsPerRun(200, func() {
		_, _, _ = c.declaredColumn("books", "author_id")
		_, _, _ = c.declaredColumn("authors", "rating")
	})
	require.Zero(t, allocations, "declaredColumn must not copy the generation contract per lookup")
}

// The index holds a column only when it is both declared by the generation and given a local codec,
// and the first declaration of a table wins, as the contract search it replaces did.
func TestIndexDeclaredColumns_RequiresDeclarationAndCodecAndKeepsTheFirstTable(t *testing.T) {
	first := oversync.GenerationTableContract{Schema: "main", Table: "items", Columns: []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeInt64},
		{Name: "declared_only", WireType: oversync.GenerationWireTypeDecimal, Nullable: true},
	}}
	duplicate := oversync.GenerationTableContract{Schema: "main", Table: "items", Columns: []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeBoolean},
	}}
	codecs := map[string]map[string]LocalColumnCodec{"main.items": {
		"id":         LocalCodecInt64Integer,
		"codec_only": LocalCodecDecimalText,
	}}
	index := indexDeclaredColumns([]oversync.GenerationTableContract{first, duplicate}, codecs)
	require.Len(t, index, 1)
	entry, ok := index[declaredColumnKey{schema: "main", table: "items", column: "id"}]
	require.True(t, ok)
	require.Equal(t, LocalCodecInt64Integer, entry.codec)
	require.Equal(t, oversync.GenerationWireTypeInt64, entry.declared.WireType, "the first table declaration wins")
	for _, column := range []string{"declared_only", "codec_only"} {
		_, ok := index[declaredColumnKey{schema: "main", table: "items", column: column}]
		require.False(t, ok, column)
	}

	schemas := indexLocalTableSchemas([]oversync.GenerationTableContract{
		{Schema: "first", Table: "items"}, {Schema: "second", Table: "items"}, {Schema: "main", Table: "notes"},
	})
	require.Equal(t, map[string]string{"items": "first", "notes": "main"}, schemas)
}
