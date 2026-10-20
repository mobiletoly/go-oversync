// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaGenerationGoldenVectors(t *testing.T) {
	registered := []RegisteredTable{{
		Schema: "business", Table: "person", SyncKeyColumns: []string{"id"},
	}}
	generations := []SchemaGeneration{
		{
			ID: 1,
			Tables: []GenerationTable{{
				Schema: "business", Table: "person",
				Columns: []GenerationColumn{
					{Name: "id", WireType: GenerationWireTypeUUID},
					{Name: "full_name", WireType: GenerationWireTypeText},
				},
			}},
		},
		{
			ID: 2,
			Tables: []GenerationTable{{
				Schema: "business", Table: "person",
				Columns: []GenerationColumn{
					{Name: "id", WireType: GenerationWireTypeUUID},
					{Name: "first_name", WireType: GenerationWireTypeText},
					{Name: "last_name", WireType: GenerationWireTypeText},
				},
			}},
		},
	}

	registry, err := CompileSchemaGenerationRegistry(registered, generations)
	require.NoError(t, err)
	require.NotNil(t, registry)
	require.Equal(t, []SchemaGenerationID{1, 2}, registry.GenerationIDs())

	generation1, ok := registry.Generation(1)
	require.True(t, ok)
	require.Equal(t, SchemaGenerationID(1), generation1.ID())
	require.Equal(t, "992eb04cc323c90fdaac0de49ed9832fc896b770ab74d26166c7302e72c65e87", generation1.Fingerprint())
	canonical1, err := schemaGenerationCanonicalBytes(generation1)
	require.NoError(t, err)
	require.Equal(t,
		`["oversync.schema-generation.v1","1",[["business","person",["id"],[["full_name","text",false],["id","uuid",false]],[]]]]`,
		string(canonical1),
	)

	generation2, ok := registry.Generation(2)
	require.True(t, ok)
	require.Equal(t, "161c1aa04bbac905089af9db9f106c8c6737ef9a046549ea0c0d500f3af6680d", generation2.Fingerprint())
	canonical2, err := schemaGenerationCanonicalBytes(generation2)
	require.NoError(t, err)
	require.Equal(t,
		`["oversync.schema-generation.v1","2",[["business","person",["id"],[["first_name","text",false],["id","uuid",false],["last_name","text",false]],[]]]]`,
		string(canonical2),
	)

	registryCanonical, err := schemaGenerationRegistryCanonicalBytes(registry)
	require.NoError(t, err)
	require.Equal(t,
		`["oversync.schema-generation-registry.v1",[["1","992eb04cc323c90fdaac0de49ed9832fc896b770ab74d26166c7302e72c65e87"],["2","161c1aa04bbac905089af9db9f106c8c6737ef9a046549ea0c0d500f3af6680d"]]]`,
		string(registryCanonical),
	)
	require.Equal(t, "d1fea34479511681d628a9e0713571a159e7b3a8198c9f89b06fc996f73c51be", registry.Fingerprint())
}

func TestSchemaGenerationRegistry_StableUnderEveryInputPermutation(t *testing.T) {
	registered, generations := validForeignKeyGenerationInput()
	child := &generations[0].Tables[0]
	child.Columns = append(child.Columns,
		GenerationColumn{Name: "owner_id", WireType: GenerationWireTypeUUID, Nullable: true},
		GenerationColumn{Name: "title", WireType: GenerationWireTypeText},
	)
	child.ForeignKeys = append(child.ForeignKeys, GenerationForeignKey{
		Columns: []string{"owner_id"}, ReferencedSchema: "business", ReferencedTable: "parent",
		ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeySetNull, OnUpdate: GenerationForeignKeyNoAction,
	})
	generation2 := cloneSchemaGenerations(generations)[0]
	generation2.ID = 9
	generation2.Tables[0].Columns = append(generation2.Tables[0].Columns,
		GenerationColumn{Name: "summary", WireType: GenerationWireTypeText, Nullable: true},
	)
	generations = append(generations, generation2)

	want, err := CompileSchemaGenerationRegistry(registered, generations)
	require.NoError(t, err)

	permutedRegistered := cloneRegisteredTables(registered)
	permutedGenerations := cloneSchemaGenerations(generations)
	slices.Reverse(permutedRegistered)
	slices.Reverse(permutedGenerations)
	for generationIndex := range permutedGenerations {
		slices.Reverse(permutedGenerations[generationIndex].Tables)
		for tableIndex := range permutedGenerations[generationIndex].Tables {
			table := &permutedGenerations[generationIndex].Tables[tableIndex]
			slices.Reverse(table.Columns)
			slices.Reverse(table.ForeignKeys)
		}
	}

	got, err := CompileSchemaGenerationRegistry(permutedRegistered, permutedGenerations)
	require.NoError(t, err)
	require.Equal(t, want.Fingerprint(), got.Fingerprint())
	require.Equal(t, want.GenerationIDs(), got.GenerationIDs())
	for _, id := range want.GenerationIDs() {
		wantGeneration, wantOK := want.Generation(id)
		gotGeneration, gotOK := got.Generation(id)
		require.Equal(t, wantOK, gotOK)
		require.Equal(t, wantGeneration.Fingerprint(), gotGeneration.Fingerprint())
		require.Equal(t, wantGeneration.Tables(), gotGeneration.Tables())
	}
}

func TestSchemaGenerationRegistry_GenerationCountsAndSparseIDs(t *testing.T) {
	registered, one := validSingleTableGenerationInput()
	two := append(cloneSchemaGenerations(one), cloneSchemaGenerations(one)[0])
	two[1].ID = 42
	three := append(cloneSchemaGenerations(two), cloneSchemaGenerations(one)[0])
	three[2].ID = 99

	tests := []struct {
		name        string
		generations []SchemaGeneration
		wantIDs     []SchemaGenerationID
		wantErr     bool
		wantCount   int
	}{
		{name: "zero", generations: nil, wantErr: true, wantCount: 0},
		{name: "one", generations: one, wantIDs: []SchemaGenerationID{1}},
		{name: "two sparse", generations: two, wantIDs: []SchemaGenerationID{1, 42}},
		{name: "three", generations: three, wantErr: true, wantCount: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := CompileSchemaGenerationRegistry(registered, test.generations)
			if test.wantErr {
				requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
					Code: SchemaGenerationInvalidCount, Field: "SchemaGenerations", ObservedCount: test.wantCount,
				})
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantIDs, registry.GenerationIDs())
		})
	}

	for _, id := range []SchemaGenerationID{0, -1} {
		t.Run("invalid id "+strconv.FormatInt(int64(id), 10), func(t *testing.T) {
			invalid := cloneSchemaGenerations(one)
			invalid[0].ID = id
			_, err := CompileSchemaGenerationRegistry(registered, invalid)
			requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
				Code: SchemaGenerationInvalidID, GenerationID: id,
				Field: "ID", Value: strconv.FormatInt(int64(id), 10),
			})
		})
	}
}

func TestSchemaGenerationRegistry_RejectsEveryTypedConfigurationFailure(t *testing.T) {
	minimalRegistered, minimalGenerations := validSingleTableGenerationInput()
	fkRegistered, fkGenerations := validForeignKeyGenerationInput()

	tests := []struct {
		name       string
		registered []RegisteredTable
		generation []SchemaGeneration
		want       SchemaGenerationConfigError
	}{
		{
			name: "empty registered tables", generation: minimalGenerations,
			want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyRegisteredTables},
		},
		{
			name: "invalid registered table", registered: []RegisteredTable{{Schema: "Business", Table: "person", SyncKeyColumns: []string{"id"}}}, generation: minimalGenerations,
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredTable, Schema: "Business", Table: "person", Field: "Schema", Value: "Business"},
		},
		{
			name: "duplicate registered table", registered: append(cloneRegisteredTables(minimalRegistered), minimalRegistered[0]), generation: minimalGenerations,
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateRegisteredTable, Schema: "business", Table: "person", ObservedCount: 2},
		},
		{
			name: "invalid registered sync key count", registered: []RegisteredTable{{Schema: "business", Table: "person"}}, generation: minimalGenerations,
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKeyCount, Schema: "business", Table: "person", Field: "SyncKeyColumns", ObservedCount: 0},
		},
		{
			name: "invalid registered sync key", registered: []RegisteredTable{{Schema: "business", Table: "person", SyncKeyColumns: []string{"ID"}}}, generation: minimalGenerations,
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKey, Schema: "business", Table: "person", Column: "ID", Field: "SyncKeyColumns", Value: "ID"},
		},
		{
			name: "hidden registered sync key", registered: []RegisteredTable{{Schema: "business", Table: "person", SyncKeyColumns: []string{syncScopeColumnName}}}, generation: minimalGenerations,
			want: SchemaGenerationConfigError{Code: SchemaGenerationHiddenRegisteredSyncKey, Schema: "business", Table: "person", Column: syncScopeColumnName},
		},
		{
			name: "invalid generation count", registered: minimalRegistered,
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidCount, Field: "SchemaGenerations", ObservedCount: 0},
		},
		{
			name: "invalid generation id", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) { generations[0].ID = 0 }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidID, Field: "ID", Value: "0"},
		},
		{
			name: "duplicate generation id", registered: minimalRegistered, generation: append(cloneSchemaGenerations(minimalGenerations), cloneSchemaGenerations(minimalGenerations)[0]),
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateID, GenerationID: 1, ObservedCount: 2},
		},
		{
			name: "empty generation tables", registered: minimalRegistered, generation: []SchemaGeneration{{ID: 1}},
			want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyTables, GenerationID: 1, Field: "Tables"},
		},
		{
			name: "invalid generation table", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].Table = "Person" }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidTable, GenerationID: 1, Schema: "business", Table: "Person", Field: "Table", Value: "Person"},
		},
		{
			name: "duplicate generation table", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables = append(generations[0].Tables, generations[0].Tables[0])
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateTable, GenerationID: 1, Schema: "business", Table: "person", ObservedCount: 2},
		},
		{
			name: "unregistered generation table", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].Table = "other" }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationUnregisteredTable, GenerationID: 1, Schema: "business", Table: "other"},
		},
		{
			name: "empty generation columns", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].Columns = nil }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyColumns, GenerationID: 1, Schema: "business", Table: "person", Field: "Columns"},
		},
		{
			name: "invalid generation column", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].Columns[0].Name = "ID" }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidColumn, GenerationID: 1, Schema: "business", Table: "person", Column: "ID", Field: "Name", Value: "ID"},
		},
		{
			name: "hidden generation column", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns, GenerationColumn{Name: syncScopeColumnName, WireType: GenerationWireTypeText})
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationHiddenColumn, GenerationID: 1, Schema: "business", Table: "person", Column: syncScopeColumnName, Field: "Name", Value: syncScopeColumnName},
		},
		{
			name: "duplicate generation column", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns, generations[0].Tables[0].Columns[0])
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateColumn, GenerationID: 1, Schema: "business", Table: "person", Column: "id", ObservedCount: 2},
		},
		{
			name: "invalid wire type", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].Columns[0].WireType = "varchar" }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidWireType, GenerationID: 1, Schema: "business", Table: "person", Column: "id", Field: "WireType", Value: "varchar"},
		},
		{
			name: "missing sync key", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns[0] = GenerationColumn{Name: "value", WireType: GenerationWireTypeText}
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationMissingSyncKey, GenerationID: 1, Schema: "business", Table: "person", Column: "id"},
		},
		{
			name: "invalid sync key contract", registered: minimalRegistered, generation: mutateGenerations(minimalGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns[0].WireType = GenerationWireTypeInt64
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidSyncKeyContract, GenerationID: 1, Schema: "business", Table: "person", Column: "id", Field: "WireType", Value: "int64"},
		},
		{
			name: "invalid foreign key shape", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].ForeignKeys[0].Columns = nil }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyShape, GenerationID: 1, Schema: "business", Table: "child", Field: "Columns"},
		},
		{
			name: "invalid foreign key identifier", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].ForeignKeys[0].Columns[0] = "Parent_ID" }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyIdentifier, GenerationID: 1, Schema: "business", Table: "child", Column: "Parent_ID", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "id", Field: "Columns", Value: "Parent_ID"},
		},
		{
			name: "invalid foreign key action", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) { generations[0].Tables[0].ForeignKeys[0].OnDelete = "set_default" }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyAction, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "id", Field: "OnDelete", Value: "set_default"},
		},
		{
			name: "duplicate foreign key", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				foreignKey := generations[0].Tables[0].ForeignKeys[0]
				foreignKey.OnDelete = GenerationForeignKeyCascade
				generations[0].Tables[0].ForeignKeys = append(generations[0].Tables[0].ForeignKeys, foreignKey)
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateForeignKey, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "id", ObservedCount: 2},
		},
		{
			name: "foreign key column not visible", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].ForeignKeys[0].Columns[0] = "missing_id"
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyColumnNotVisible, GenerationID: 1, Schema: "business", Table: "child", Column: "missing_id"},
		},
		{
			name: "foreign key target not visible", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) { generations[0].Tables = generations[0].Tables[:1] }),
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTargetNotVisible, GenerationID: 1, Schema: "business", Table: "child", ReferencedSchema: "business", ReferencedTable: "parent"},
		},
		{
			name: "foreign key target column not visible", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].ForeignKeys[0].ReferencedColumns[0] = "other_id"
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTargetColumnNotVisible, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "other_id"},
		},
		{
			name: "foreign key target not sync key", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].ForeignKeys[0].ReferencedColumns[0] = "other_id"
				generations[0].Tables[1].Columns = append(generations[0].Tables[1].Columns, GenerationColumn{Name: "other_id", WireType: GenerationWireTypeUUID})
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTargetNotSyncKey, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "other_id"},
		},
		{
			name: "foreign key type mismatch", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns[1].WireType = GenerationWireTypeText
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTypeMismatch, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "id"},
		},
		{
			name: "foreign key set null non-nullable on delete", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns[1].Nullable = false
				generations[0].Tables[0].ForeignKeys[0].OnDelete = GenerationForeignKeySetNull
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeySetNullNonNullable, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", Field: "OnDelete", Value: "set_null"},
		},
		{
			name: "foreign key set null unsupported on update", registered: fkRegistered, generation: mutateGenerations(fkGenerations, func(generations []SchemaGeneration) {
				generations[0].Tables[0].Columns[1].Nullable = false
				generations[0].Tables[0].ForeignKeys[0].OnDelete = GenerationForeignKeyNoAction
				generations[0].Tables[0].ForeignKeys[0].OnUpdate = GenerationForeignKeySetNull
			}),
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyAction, GenerationID: 1, Schema: "business", Table: "child", Column: "parent_id", ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumn: "id", Field: "OnUpdate", Value: "set_null"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := CompileSchemaGenerationRegistry(test.registered, test.generation)
			require.Nil(t, registry)
			requireSchemaGenerationConfigError(t, err, test.want)
		})
	}
}

func TestSchemaGenerationRegistry_IdentifierGrammarIsExact(t *testing.T) {
	invalid := []string{
		"",
		" person",
		"person ",
		"Person",
		strings.Repeat("a", 64),
		"person-name",
		"person.name",
		"é",
		string([]byte{0xff}),
	}

	for _, value := range invalid {
		t.Run(strings.ReplaceAll(value, "/", "_"), func(t *testing.T) {
			registered, generations := validSingleTableGenerationInput()
			generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns,
				GenerationColumn{Name: value, WireType: GenerationWireTypeText},
			)
			_, err := CompileSchemaGenerationRegistry(registered, generations)
			requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
				Code: SchemaGenerationInvalidColumn, GenerationID: 1,
				Schema: "business", Table: "person", Column: value,
				Field: "Name", Value: value,
			})
		})
	}

	valid := []string{"a", "0", "_", "a_0", strings.Repeat("z", 63)}
	for _, value := range valid {
		t.Run("valid_"+value[:1], func(t *testing.T) {
			require.True(t, isCanonicalGenerationIdentifier(value))
		})
	}

	t.Run("registered empty schema is not public", func(t *testing.T) {
		registered, generations := validSingleTableGenerationInput()
		registered[0].Schema = ""
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
			Code: SchemaGenerationInvalidRegisteredTable, Schema: "", Table: "person", Field: "Schema", Value: "",
		})
	})

	t.Run("generation empty schema is not public", func(t *testing.T) {
		registered, generations := validSingleTableGenerationInput()
		generations[0].Tables[0].Schema = ""
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
			Code: SchemaGenerationInvalidTable, GenerationID: 1, Schema: "", Table: "person", Field: "Schema", Value: "",
		})
	})
}

func TestSchemaGenerationRegistry_RegisteredSyncKeyContracts(t *testing.T) {
	registered, generations := validSingleTableGenerationInput()

	tests := []struct {
		name    string
		keys    []string
		want    SchemaGenerationConfigError
		wantErr bool
	}{
		{name: "zero", wantErr: true, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKeyCount, Schema: "business", Table: "person", Field: "SyncKeyColumns"}},
		{name: "one", keys: []string{"id"}},
		{name: "several", keys: []string{"id", "other_id"}, wantErr: true, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKeyCount, Schema: "business", Table: "person", Field: "SyncKeyColumns", ObservedCount: 2}},
		{name: "invalid", keys: []string{" id"}, wantErr: true, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKey, Schema: "business", Table: "person", Column: " id", Field: "SyncKeyColumns", Value: " id"}},
		{name: "hidden", keys: []string{syncScopeColumnName}, wantErr: true, want: SchemaGenerationConfigError{Code: SchemaGenerationHiddenRegisteredSyncKey, Schema: "business", Table: "person", Column: syncScopeColumnName}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := cloneRegisteredTables(registered)
			input[0].SyncKeyColumns = test.keys
			_, err := CompileSchemaGenerationRegistry(input, generations)
			if !test.wantErr {
				require.NoError(t, err)
				return
			}
			test.want.ObservedCount = len(test.keys)
			if test.want.Code != SchemaGenerationInvalidRegisteredSyncKeyCount {
				test.want.ObservedCount = 0
			}
			requireSchemaGenerationConfigError(t, err, test.want)
		})
	}
}

func TestSchemaGenerationRegistry_WireTypeVocabularyIsClosed(t *testing.T) {
	accepted := []GenerationWireType{
		GenerationWireTypeBoolean,
		GenerationWireTypeInt64,
		GenerationWireTypeDecimal,
		GenerationWireTypeFloat64,
		GenerationWireTypeText,
		GenerationWireTypeBytes,
		GenerationWireTypeUUID,
		GenerationWireTypeJSON,
	}
	for _, wireType := range accepted {
		t.Run(string(wireType), func(t *testing.T) {
			registered, generations := validSingleTableGenerationInput()
			generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns,
				GenerationColumn{Name: "value", WireType: wireType, Nullable: true},
			)
			_, err := CompileSchemaGenerationRegistry(registered, generations)
			require.NoError(t, err)
		})
	}

	for _, wireType := range []GenerationWireType{"", "varchar", "TEXT", "integer"} {
		t.Run("reject_"+string(wireType), func(t *testing.T) {
			registered, generations := validSingleTableGenerationInput()
			generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns,
				GenerationColumn{Name: "value", WireType: wireType},
			)
			_, err := CompileSchemaGenerationRegistry(registered, generations)
			requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
				Code: SchemaGenerationInvalidWireType, GenerationID: 1,
				Schema: "business", Table: "person", Column: "value",
				Field: "WireType", Value: string(wireType),
			})
		})
	}
}

func TestSchemaGenerationRegistry_SyncKeyVisibilityTypeAndNullability(t *testing.T) {
	tests := []struct {
		name     string
		wireType GenerationWireType
		nullable bool
		missing  bool
		want     SchemaGenerationConfigError
	}{
		{name: "uuid", wireType: GenerationWireTypeUUID},
		{name: "text", wireType: GenerationWireTypeText},
		{name: "missing", missing: true, want: SchemaGenerationConfigError{Code: SchemaGenerationMissingSyncKey, GenerationID: 1, Schema: "business", Table: "person", Column: "id"}},
		{name: "unsupported", wireType: GenerationWireTypeInt64, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidSyncKeyContract, GenerationID: 1, Schema: "business", Table: "person", Column: "id", Field: "WireType", Value: "int64"}},
		{name: "nullable", wireType: GenerationWireTypeUUID, nullable: true, want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidSyncKeyContract, GenerationID: 1, Schema: "business", Table: "person", Column: "id", Field: "Nullable", Value: "true"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registered, generations := validSingleTableGenerationInput()
			if test.missing {
				generations[0].Tables[0].Columns = []GenerationColumn{{Name: "value", WireType: GenerationWireTypeText}}
			} else {
				generations[0].Tables[0].Columns[0].WireType = test.wireType
				generations[0].Tables[0].Columns[0].Nullable = test.nullable
			}
			_, err := CompileSchemaGenerationRegistry(registered, generations)
			if test.want.Code == "" {
				require.NoError(t, err)
				return
			}
			requireSchemaGenerationConfigError(t, err, test.want)
		})
	}
}

func TestSchemaGenerationRegistry_ForeignKeyShapeIdentifierActionAndSetRules(t *testing.T) {
	t.Run("shape counts", func(t *testing.T) {
		for _, field := range []string{"Columns", "ReferencedColumns"} {
			for _, count := range []int{0, 2} {
				t.Run(field+strings.Repeat("_item", count), func(t *testing.T) {
					registered, generations := validForeignKeyGenerationInput()
					values := slices.Repeat([]string{"id"}, count)
					if field == "Columns" {
						generations[0].Tables[0].ForeignKeys[0].Columns = values
					} else {
						generations[0].Tables[0].ForeignKeys[0].ReferencedColumns = values
					}
					_, err := CompileSchemaGenerationRegistry(registered, generations)
					requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
						Code: SchemaGenerationInvalidForeignKeyShape, GenerationID: 1,
						Schema: "business", Table: "child", Field: field, ObservedCount: count,
					})
				})
			}
		}
	})

	t.Run("identifier fields", func(t *testing.T) {
		tests := []struct {
			field string
			apply func(*GenerationForeignKey)
		}{
			{field: "Columns", apply: func(foreignKey *GenerationForeignKey) { foreignKey.Columns[0] = syncScopeColumnName }},
			{field: "ReferencedSchema", apply: func(foreignKey *GenerationForeignKey) { foreignKey.ReferencedSchema = "Business" }},
			{field: "ReferencedTable", apply: func(foreignKey *GenerationForeignKey) { foreignKey.ReferencedTable = "parent-table" }},
			{field: "ReferencedColumns", apply: func(foreignKey *GenerationForeignKey) { foreignKey.ReferencedColumns[0] = syncScopeColumnName }},
		}
		for _, test := range tests {
			t.Run(test.field, func(t *testing.T) {
				registered, generations := validForeignKeyGenerationInput()
				foreignKey := &generations[0].Tables[0].ForeignKeys[0]
				test.apply(foreignKey)
				_, err := CompileSchemaGenerationRegistry(registered, generations)
				var configError *SchemaGenerationConfigError
				require.ErrorAs(t, err, &configError)
				require.Equal(t, SchemaGenerationInvalidForeignKeyIdentifier, configError.Code)
				require.Equal(t, test.field, configError.Field)
				require.NotEmpty(t, configError.Value)
			})
		}
	})

	t.Run("actions", func(t *testing.T) {
		accepted := []GenerationForeignKeyAction{
			GenerationForeignKeyNoAction,
			GenerationForeignKeyRestrict,
			GenerationForeignKeyCascade,
		}
		t.Run("set_null_delete_only", func(t *testing.T) {
			registered, generations := validForeignKeyGenerationInput()
			generations[0].Tables[0].ForeignKeys[0].OnDelete = GenerationForeignKeySetNull
			require.NoError(t, func() error {
				_, err := CompileSchemaGenerationRegistry(registered, generations)
				return err
			}())
		})
		for _, action := range accepted {
			t.Run(string(action), func(t *testing.T) {
				registered, generations := validForeignKeyGenerationInput()
				generations[0].Tables[0].ForeignKeys[0].OnDelete = action
				generations[0].Tables[0].ForeignKeys[0].OnUpdate = action
				_, err := CompileSchemaGenerationRegistry(registered, generations)
				require.NoError(t, err)
			})
		}
		for _, field := range []string{"OnDelete", "OnUpdate"} {
			t.Run("invalid_"+field, func(t *testing.T) {
				registered, generations := validForeignKeyGenerationInput()
				foreignKey := &generations[0].Tables[0].ForeignKeys[0]
				if field == "OnDelete" {
					foreignKey.OnDelete = "SET NULL"
				} else {
					foreignKey.OnUpdate = "SET NULL"
				}
				_, err := CompileSchemaGenerationRegistry(registered, generations)
				var configError *SchemaGenerationConfigError
				require.ErrorAs(t, err, &configError)
				require.Equal(t, SchemaGenerationInvalidForeignKeyAction, configError.Code)
				require.Equal(t, field, configError.Field)
				require.Equal(t, "SET NULL", configError.Value)
			})
		}
	})

	t.Run("set null delete requires nullable child", func(t *testing.T) {
		registered, generations := validForeignKeyGenerationInput()
		generations[0].Tables[0].Columns[1].Nullable = false
		generations[0].Tables[0].ForeignKeys[0].OnDelete = GenerationForeignKeySetNull
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		var configError *SchemaGenerationConfigError
		require.ErrorAs(t, err, &configError)
		require.Equal(t, SchemaGenerationForeignKeySetNullNonNullable, configError.Code)
		require.Equal(t, "OnDelete", configError.Field)
	})

	t.Run("unsupported update set null precedes delete nullability", func(t *testing.T) {
		registered, generations := validForeignKeyGenerationInput()
		generations[0].Tables[0].Columns[1].Nullable = false
		foreignKey := &generations[0].Tables[0].ForeignKeys[0]
		foreignKey.OnDelete = GenerationForeignKeySetNull
		foreignKey.OnUpdate = GenerationForeignKeySetNull
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		var configError *SchemaGenerationConfigError
		require.ErrorAs(t, err, &configError)
		require.Equal(t, SchemaGenerationInvalidForeignKeyAction, configError.Code)
		require.Equal(t, "OnUpdate", configError.Field)
	})
}

func TestSchemaGenerationRegistry_PermitsPhysicalOmission(t *testing.T) {
	registered := []RegisteredTable{
		{Schema: "business", Table: "person", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "unused", SyncKeyColumns: []string{"id"}},
	}
	generations := []SchemaGeneration{{
		ID: 3,
		Tables: []GenerationTable{{
			Schema: "business", Table: "person",
			Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}},
		}},
	}}

	registry, err := CompileSchemaGenerationRegistry(registered, generations)
	require.NoError(t, err)
	contract, ok := registry.Generation(3)
	require.True(t, ok)
	require.Equal(t, []GenerationTableContract{{
		Schema: "business", Table: "person", SyncKeyColumns: []string{"id"},
		Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}},
	}}, contract.Tables())
}

func TestSchemaGenerationRegistry_DeepCopyIsolation(t *testing.T) {
	registered, generations := validForeignKeyGenerationInput()
	registry, err := CompileSchemaGenerationRegistry(registered, generations)
	require.NoError(t, err)
	wantRegistryFingerprint := registry.Fingerprint()
	wantIDs := registry.GenerationIDs()
	wantContract, ok := registry.Generation(1)
	require.True(t, ok)
	wantGenerationFingerprint := wantContract.Fingerprint()
	wantTables := wantContract.Tables()

	registered[0].Schema = "mutated"
	registered[0].SyncKeyColumns[0] = "mutated"
	registered = append(registered, RegisteredTable{Schema: "new", Table: "table", SyncKeyColumns: []string{"id"}})
	generations[0].ID = 99
	generations[0].Tables[0].Schema = "mutated"
	generations[0].Tables[0].Columns[0].Name = "mutated"
	generations[0].Tables[0].ForeignKeys[0].Columns[0] = "mutated"
	generations[0].Tables[0].ForeignKeys[0].ReferencedColumns[0] = "mutated"
	generations[0].Tables = nil

	require.Equal(t, wantRegistryFingerprint, registry.Fingerprint())
	require.Equal(t, wantIDs, registry.GenerationIDs())
	contract, ok := registry.Generation(1)
	require.True(t, ok)
	require.Equal(t, wantGenerationFingerprint, contract.Fingerprint())
	require.Equal(t, wantTables, contract.Tables())

	returnedIDs := registry.GenerationIDs()
	returnedIDs[0] = 77
	returnedTables := contract.Tables()
	returnedTables[0].Schema = "changed"
	returnedTables[0].SyncKeyColumns[0] = "changed"
	returnedTables[0].Columns[0].Name = "changed"
	returnedTables[0].ForeignKeys[0].Columns[0] = "changed"
	returnedTables[0].ForeignKeys[0].ReferencedColumns[0] = "changed"
	returnedTables = append(returnedTables, GenerationTableContract{Schema: "changed"})
	contract.tables[0].Columns[0].Name = "same-package mutation"

	require.Equal(t, wantIDs, registry.GenerationIDs())
	again, ok := registry.Generation(1)
	require.True(t, ok)
	require.Equal(t, wantGenerationFingerprint, again.Fingerprint())
	require.Equal(t, wantTables, again.Tables())
}

func TestSchemaGenerationRegistry_NilAndZeroValueAccessors(t *testing.T) {
	var registry *SchemaGenerationRegistry
	require.Empty(t, registry.Fingerprint())
	require.Nil(t, registry.GenerationIDs())
	contract, ok := registry.Generation(1)
	require.False(t, ok)
	require.Zero(t, contract.ID())
	require.Empty(t, contract.Fingerprint())
	require.Nil(t, contract.Tables())

	zero := SchemaGenerationContract{}
	require.Zero(t, zero.ID())
	require.Empty(t, zero.Fingerprint())
	require.Nil(t, zero.Tables())

	registered, generations := validSingleTableGenerationInput()
	compiled, err := CompileSchemaGenerationRegistry(registered, generations)
	require.NoError(t, err)
	missing, ok := compiled.Generation(999)
	require.False(t, ok)
	require.Zero(t, missing.ID())
}

func TestSchemaGenerationComparator_TotalRecursiveOrder(t *testing.T) {
	tests := []struct {
		name    string
		forward int
		reverse int
	}{
		{name: "unsigned exact bytes", forward: compareStrings(string([]byte{0x7f}), string([]byte{0x80})), reverse: compareStrings(string([]byte{0x80}), string([]byte{0x7f}))},
		{name: "false before true", forward: compareBooleans(false, true), reverse: compareBooleans(true, false)},
		{name: "empty sequence", forward: compareStringSequences(nil, []string{"a"}), reverse: compareStringSequences([]string{"a"}, nil)},
		{name: "proper prefix sequence", forward: compareStringSequences([]string{"a"}, []string{"a", "b"}), reverse: compareStringSequences([]string{"a", "b"}, []string{"a"})},
		{name: "multi element sequence", forward: compareStringSequences([]string{"a", "b"}, []string{"a", "c"}), reverse: compareStringSequences([]string{"a", "c"}, []string{"a", "b"})},
		{name: "column name", forward: compareGenerationColumns(
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText},
			GenerationColumn{Name: "b", WireType: GenerationWireTypeText},
		), reverse: compareGenerationColumns(
			GenerationColumn{Name: "b", WireType: GenerationWireTypeText},
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText},
		)},
		{name: "column wire type", forward: compareGenerationColumns(
			GenerationColumn{Name: "a", WireType: GenerationWireTypeInt64},
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText},
		), reverse: compareGenerationColumns(
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText},
			GenerationColumn{Name: "a", WireType: GenerationWireTypeInt64},
		)},
		{name: "column nullable", forward: compareGenerationColumns(
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText},
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText, Nullable: true},
		), reverse: compareGenerationColumns(
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText, Nullable: true},
			GenerationColumn{Name: "a", WireType: GenerationWireTypeText},
		)},
		{name: "foreign key child columns", forward: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}},
			GenerationForeignKey{Columns: []string{"a", "b"}},
		), reverse: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a", "b"}},
			GenerationForeignKey{Columns: []string{"a"}},
		)},
		{name: "foreign key referenced schema", forward: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "a"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "b"},
		), reverse: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "b"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "a"},
		)},
		{name: "foreign key referenced table", forward: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "a"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "b"},
		), reverse: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "b"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "a"},
		)},
		{name: "foreign key referenced columns", forward: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a", "b"}},
		), reverse: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a", "b"}},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}},
		)},
		{name: "foreign key on delete", forward: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "a"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "b"},
		), reverse: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "b"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "a"},
		)},
		{name: "foreign key on update", forward: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "same", OnUpdate: "a"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "same", OnUpdate: "b"},
		), reverse: compareGenerationForeignKeys(
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "same", OnUpdate: "b"},
			GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"a"}, OnDelete: "same", OnUpdate: "a"},
		)},
		{name: "table schema", forward: compareGenerationTables(
			GenerationTable{Schema: "a", Table: "t"}, GenerationTable{Schema: "b", Table: "t"},
		), reverse: compareGenerationTables(
			GenerationTable{Schema: "b", Table: "t"}, GenerationTable{Schema: "a", Table: "t"},
		)},
		{name: "table name", forward: compareGenerationTables(
			GenerationTable{Schema: "s", Table: "a"}, GenerationTable{Schema: "s", Table: "b"},
		), reverse: compareGenerationTables(
			GenerationTable{Schema: "s", Table: "b"}, GenerationTable{Schema: "s", Table: "a"},
		)},
		{name: "table sorted columns", forward: compareGenerationTables(
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a", WireType: GenerationWireTypeInt64}}},
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a", WireType: GenerationWireTypeText}}},
		), reverse: compareGenerationTables(
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a", WireType: GenerationWireTypeText}}},
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a", WireType: GenerationWireTypeInt64}}},
		)},
		{name: "table sorted foreign keys", forward: compareGenerationTables(
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a"}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"a"}, OnDelete: "a"}}},
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a"}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"a"}, OnDelete: "b"}}},
		), reverse: compareGenerationTables(
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a"}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"a"}, OnDelete: "b"}}},
			GenerationTable{Schema: "s", Table: "t", Columns: []GenerationColumn{{Name: "a"}}, ForeignKeys: []GenerationForeignKey{{Columns: []string{"a"}, OnDelete: "a"}}},
		)},
		{name: "generation numeric id", forward: compareSchemaGenerations(SchemaGeneration{ID: 2}, SchemaGeneration{ID: 10}), reverse: compareSchemaGenerations(SchemaGeneration{ID: 10}, SchemaGeneration{ID: 2})},
		{name: "generation sorted tables", forward: compareSchemaGenerations(
			SchemaGeneration{ID: 1, Tables: []GenerationTable{{Schema: "a"}}},
			SchemaGeneration{ID: 1, Tables: []GenerationTable{{Schema: "b"}}},
		), reverse: compareSchemaGenerations(
			SchemaGeneration{ID: 1, Tables: []GenerationTable{{Schema: "b"}}},
			SchemaGeneration{ID: 1, Tables: []GenerationTable{{Schema: "a"}}},
		)},
		{name: "registered schema", forward: compareRegisteredTables(
			RegisteredTable{Schema: "a", Table: "t", SyncKeyColumns: []string{"id"}},
			RegisteredTable{Schema: "b", Table: "t", SyncKeyColumns: []string{"id"}},
		), reverse: compareRegisteredTables(
			RegisteredTable{Schema: "b", Table: "t", SyncKeyColumns: []string{"id"}},
			RegisteredTable{Schema: "a", Table: "t", SyncKeyColumns: []string{"id"}},
		)},
		{name: "registered table", forward: compareRegisteredTables(
			RegisteredTable{Schema: "s", Table: "a", SyncKeyColumns: []string{"id"}},
			RegisteredTable{Schema: "s", Table: "b", SyncKeyColumns: []string{"id"}},
		), reverse: compareRegisteredTables(
			RegisteredTable{Schema: "s", Table: "b", SyncKeyColumns: []string{"id"}},
			RegisteredTable{Schema: "s", Table: "a", SyncKeyColumns: []string{"id"}},
		)},
		{name: "registered ordered sync keys", forward: compareRegisteredTables(
			RegisteredTable{Schema: "s", Table: "t", SyncKeyColumns: []string{"a"}},
			RegisteredTable{Schema: "s", Table: "t", SyncKeyColumns: []string{"a", "b"}},
		), reverse: compareRegisteredTables(
			RegisteredTable{Schema: "s", Table: "t", SyncKeyColumns: []string{"a", "b"}},
			RegisteredTable{Schema: "s", Table: "t", SyncKeyColumns: []string{"a"}},
		)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Negative(t, test.forward)
			require.Positive(t, test.reverse)
		})
	}

	require.Zero(t, compareStringSequences([]string{"a", "b"}, []string{"a", "b"}))
	require.Zero(t, compareGenerationColumns(
		GenerationColumn{Name: "a", WireType: GenerationWireTypeText, Nullable: true},
		GenerationColumn{Name: "a", WireType: GenerationWireTypeText, Nullable: true},
	))
	require.Zero(t, compareGenerationForeignKeys(
		GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyCascade, OnUpdate: GenerationForeignKeyRestrict},
		GenerationForeignKey{Columns: []string{"a"}, ReferencedSchema: "s", ReferencedTable: "t", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyCascade, OnUpdate: GenerationForeignKeyRestrict},
	))
}

func TestSchemaGenerationCompiler_FirstErrorUsesGlobalSubphases(t *testing.T) {
	t.Run("column identifier precedes earlier table wire type", func(t *testing.T) {
		registered := []RegisteredTable{
			{Schema: "business", Table: "a", SyncKeyColumns: []string{"id"}},
			{Schema: "business", Table: "z", SyncKeyColumns: []string{"id"}},
		}
		generations := []SchemaGeneration{{ID: 1, Tables: []GenerationTable{
			{Schema: "business", Table: "a", Columns: []GenerationColumn{{Name: "id", WireType: "bad"}}},
			{Schema: "business", Table: "z", Columns: []GenerationColumn{{Name: "ID", WireType: GenerationWireTypeUUID}}},
		}}}
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
			Code: SchemaGenerationInvalidColumn, GenerationID: 1,
			Schema: "business", Table: "z", Column: "ID", Field: "Name", Value: "ID",
		})
	})

	t.Run("sync key presence scans all tables before wire type", func(t *testing.T) {
		registered := []RegisteredTable{
			{Schema: "business", Table: "a", SyncKeyColumns: []string{"id"}},
			{Schema: "business", Table: "z", SyncKeyColumns: []string{"id"}},
		}
		generations := []SchemaGeneration{{ID: 1, Tables: []GenerationTable{
			{Schema: "business", Table: "a", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeInt64}}},
			{Schema: "business", Table: "z", Columns: []GenerationColumn{{Name: "value", WireType: GenerationWireTypeText}}},
		}}}
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
			Code: SchemaGenerationMissingSyncKey, GenerationID: 1,
			Schema: "business", Table: "z", Column: "id",
		})
	})

	t.Run("foreign key child count scans all before referenced count", func(t *testing.T) {
		registered, generations := validForeignKeyGenerationInput()
		foreignKey := generations[0].Tables[0].ForeignKeys[0]
		generations[0].Tables[0].ForeignKeys = []GenerationForeignKey{
			{
				ReferencedSchema: "business", ReferencedTable: "parent", ReferencedColumns: []string{"id"},
				OnDelete: GenerationForeignKeyNoAction, OnUpdate: GenerationForeignKeyNoAction,
			},
			{
				Columns: []string{"parent_id"}, ReferencedSchema: "business", ReferencedTable: "parent",
				OnDelete: foreignKey.OnDelete, OnUpdate: foreignKey.OnUpdate,
			},
		}
		for permutation := 0; permutation < 2; permutation++ {
			_, err := CompileSchemaGenerationRegistry(registered, generations)
			requireSchemaGenerationConfigError(t, err, SchemaGenerationConfigError{
				Code: SchemaGenerationInvalidForeignKeyShape, GenerationID: 1,
				Schema: "business", Table: "child", Field: "Columns",
			})
			slices.Reverse(generations[0].Tables[0].ForeignKeys)
		}
	})

	t.Run("on delete scans all before on update", func(t *testing.T) {
		registered, generations := validForeignKeyGenerationInput()
		first := generations[0].Tables[0].ForeignKeys[0]
		first.Columns = []string{"a_id"}
		first.OnUpdate = "invalid_update"
		second := generations[0].Tables[0].ForeignKeys[0]
		second.Columns = []string{"z_id"}
		second.OnDelete = "invalid_delete"
		generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns,
			GenerationColumn{Name: "a_id", WireType: GenerationWireTypeUUID, Nullable: true},
			GenerationColumn{Name: "z_id", WireType: GenerationWireTypeUUID, Nullable: true},
		)
		generations[0].Tables[0].ForeignKeys = []GenerationForeignKey{first, second}
		_, err := CompileSchemaGenerationRegistry(registered, generations)
		var configError *SchemaGenerationConfigError
		require.ErrorAs(t, err, &configError)
		require.Equal(t, SchemaGenerationInvalidForeignKeyAction, configError.Code)
		require.Equal(t, "z_id", configError.Column)
		require.Equal(t, "OnDelete", configError.Field)
	})
}

func TestSchemaGenerationCompiler_TotalFirstErrorOrder(t *testing.T) {
	type testCase struct {
		name   string
		mutate func(*testing.T, *[]RegisteredTable, *[]SchemaGeneration)
		want   SchemaGenerationConfigError
	}

	tests := []testCase{
		{
			name: "registered count gates the remaining population",
			mutate: func(_ *testing.T, registered *[]RegisteredTable, generations *[]SchemaGeneration) {
				*registered = nil
				(*generations)[0].ID = 0
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyRegisteredTables},
		},
		{
			name: "registered schema before registered table",
			mutate: func(t *testing.T, registered *[]RegisteredTable, _ *[]SchemaGeneration) {
				phaseRegisteredTable(t, *registered, "z_parent").Schema = "z!"
				phaseRegisteredTable(t, *registered, "a_child").Table = "!"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredTable, Schema: "z!", Table: "z_parent", Field: "Schema", Value: "z!"},
		},
		{
			name: "registered table before duplicate registered table",
			mutate: func(t *testing.T, registered *[]RegisteredTable, _ *[]SchemaGeneration) {
				phaseRegisteredTable(t, *registered, "a_child").Table = "!"
				duplicate := *phaseRegisteredTable(t, *registered, "b_parent")
				*registered = append(*registered, duplicate)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredTable, Schema: "business", Table: "!", Field: "Table", Value: "!"},
		},
		{
			name: "duplicate registered table before sync key count",
			mutate: func(t *testing.T, registered *[]RegisteredTable, _ *[]SchemaGeneration) {
				duplicate := *phaseRegisteredTable(t, *registered, "a_child")
				*registered = append(*registered, duplicate)
				phaseRegisteredTable(t, *registered, "z_parent").SyncKeyColumns = nil
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateRegisteredTable, Schema: "business", Table: "a_child", ObservedCount: 2},
		},
		{
			name: "sync key count before sync key identifier",
			mutate: func(t *testing.T, registered *[]RegisteredTable, _ *[]SchemaGeneration) {
				phaseRegisteredTable(t, *registered, "z_parent").SyncKeyColumns = nil
				phaseRegisteredTable(t, *registered, "a_child").SyncKeyColumns[0] = "Bad"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKeyCount, Schema: "business", Table: "z_parent", Field: "SyncKeyColumns"},
		},
		{
			name: "sync key identifier before hidden sync key",
			mutate: func(t *testing.T, registered *[]RegisteredTable, _ *[]SchemaGeneration) {
				phaseRegisteredTable(t, *registered, "z_parent").SyncKeyColumns[0] = "Bad"
				phaseRegisteredTable(t, *registered, "a_child").SyncKeyColumns[0] = syncScopeColumnName
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidRegisteredSyncKey, Schema: "business", Table: "z_parent", Column: "Bad", Field: "SyncKeyColumns", Value: "Bad"},
		},
		{
			name: "hidden sync key before generation count",
			mutate: func(t *testing.T, registered *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseRegisteredTable(t, *registered, "a_child").SyncKeyColumns[0] = syncScopeColumnName
				second := cloneSchemaGenerations(*generations)[0]
				second.ID = 2
				third := cloneSchemaGenerations(*generations)[0]
				third.ID = 3
				*generations = append(*generations, second, third)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationHiddenRegisteredSyncKey, Schema: "business", Table: "a_child", Column: syncScopeColumnName},
		},
		{
			name: "generation count before generation id",
			mutate: func(_ *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				second := cloneSchemaGenerations(*generations)[0]
				second.ID = 2
				third := cloneSchemaGenerations(*generations)[0]
				third.ID = 0
				*generations = append(*generations, second, third)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidCount, Field: "SchemaGenerations", ObservedCount: 3},
		},
		{
			name: "generation id before duplicate generation id",
			mutate: func(_ *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				first := cloneSchemaGenerations(*generations)[0]
				first.ID = 0
				second := cloneSchemaGenerations([]SchemaGeneration{first})[0]
				*generations = []SchemaGeneration{first, second}
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidID, Field: "ID", Value: "0"},
		},
		{
			name: "duplicate generation id before table count",
			mutate: func(_ *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				duplicate := cloneSchemaGenerations(*generations)[0]
				duplicate.Tables = nil
				*generations = append(*generations, duplicate)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateID, GenerationID: 1, ObservedCount: 2},
		},
		{
			name: "table count before table schema",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationTable(t, *generations, "z_parent").Schema = "z!"
				second := cloneSchemaGenerations(*generations)[0]
				second.ID = 2
				second.Tables = nil
				*generations = append(*generations, second)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyTables, GenerationID: 2, Field: "Tables"},
		},
		{
			name: "table schema before table name",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationTable(t, *generations, "z_parent").Schema = "z!"
				phaseGenerationTable(t, *generations, "a_child").Table = "!"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidTable, GenerationID: 1, Schema: "z!", Table: "z_parent", Field: "Schema", Value: "z!"},
		},
		{
			name: "table name before duplicate table",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationTable(t, *generations, "a_child").Table = "!"
				duplicate := *phaseGenerationTable(t, *generations, "b_parent")
				(*generations)[0].Tables = append((*generations)[0].Tables, duplicate)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidTable, GenerationID: 1, Schema: "business", Table: "!", Field: "Table", Value: "!"},
		},
		{
			name: "duplicate table before registered membership",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				duplicate := *phaseGenerationTable(t, *generations, "a_child")
				(*generations)[0].Tables = append((*generations)[0].Tables, duplicate, GenerationTable{
					Schema: "business", Table: "x_unregistered", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}},
				})
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateTable, GenerationID: 1, Schema: "business", Table: "a_child", ObservedCount: 2},
		},
		{
			name: "registered membership before column count",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				(*generations)[0].Tables = append((*generations)[0].Tables, GenerationTable{
					Schema: "business", Table: "x_unregistered", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}},
				})
				phaseGenerationTable(t, *generations, "z_parent").Columns = nil
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationUnregisteredTable, GenerationID: 1, Schema: "business", Table: "x_unregistered"},
		},
		{
			name: "column count before column identifier",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationTable(t, *generations, "z_parent").Columns = nil
				aChild := phaseGenerationTable(t, *generations, "a_child")
				aChild.Columns = append(aChild.Columns, GenerationColumn{Name: "Bad", WireType: GenerationWireTypeText})
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationEmptyColumns, GenerationID: 1, Schema: "business", Table: "z_parent", Field: "Columns"},
		},
		{
			name: "column identifier before hidden column",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				zParent := phaseGenerationTable(t, *generations, "z_parent")
				zParent.Columns = append(zParent.Columns, GenerationColumn{Name: "Bad", WireType: GenerationWireTypeText})
				aChild := phaseGenerationTable(t, *generations, "a_child")
				aChild.Columns = append(aChild.Columns, GenerationColumn{Name: syncScopeColumnName, WireType: GenerationWireTypeUUID})
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidColumn, GenerationID: 1, Schema: "business", Table: "z_parent", Column: "Bad", Field: "Name", Value: "Bad"},
		},
		{
			name: "hidden column before duplicate column",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				aChild := phaseGenerationTable(t, *generations, "a_child")
				aChild.Columns = append(aChild.Columns, GenerationColumn{Name: syncScopeColumnName, WireType: GenerationWireTypeUUID})
				zParent := phaseGenerationTable(t, *generations, "z_parent")
				zParent.Columns = append(zParent.Columns, GenerationColumn{Name: "id", WireType: GenerationWireTypeText})
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationHiddenColumn, GenerationID: 1, Schema: "business", Table: "a_child", Column: syncScopeColumnName, Field: "Name", Value: syncScopeColumnName},
		},
		{
			name: "duplicate column before wire type",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				aChild := phaseGenerationTable(t, *generations, "a_child")
				aChild.Columns = append(aChild.Columns, GenerationColumn{Name: "id", WireType: GenerationWireTypeText})
				phaseGenerationColumn(t, phaseGenerationTable(t, *generations, "z_parent"), "id").WireType = "bad"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateColumn, GenerationID: 1, Schema: "business", Table: "a_child", Column: "id", ObservedCount: 2},
		},
		{
			name: "wire type before sync key presence",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationColumn(t, phaseGenerationTable(t, *generations, "z_parent"), "id").WireType = "bad"
				phaseRemoveGenerationColumn(t, phaseGenerationTable(t, *generations, "a_child"), "id")
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidWireType, GenerationID: 1, Schema: "business", Table: "z_parent", Column: "id", Field: "WireType", Value: "bad"},
		},
		{
			name: "sync key presence before sync key wire type",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseRemoveGenerationColumn(t, phaseGenerationTable(t, *generations, "z_parent"), "id")
				phaseGenerationColumn(t, phaseGenerationTable(t, *generations, "a_child"), "id").WireType = GenerationWireTypeInt64
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationMissingSyncKey, GenerationID: 1, Schema: "business", Table: "z_parent", Column: "id"},
		},
		{
			name: "sync key wire type before sync key nullability",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationColumn(t, phaseGenerationTable(t, *generations, "z_parent"), "id").WireType = GenerationWireTypeInt64
				phaseGenerationColumn(t, phaseGenerationTable(t, *generations, "a_child"), "id").Nullable = true
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidSyncKeyContract, GenerationID: 1, Schema: "business", Table: "z_parent", Column: "id", Field: "WireType", Value: "int64"},
		},
		{
			name: "sync key nullability before foreign key child count",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				phaseGenerationColumn(t, phaseGenerationTable(t, *generations, "z_parent"), "id").Nullable = true
				phaseGenerationForeignKey(t, phaseGenerationTable(t, *generations, "a_child"), "a_parent_id").Columns = nil
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidSyncKeyContract, GenerationID: 1, Schema: "business", Table: "z_parent", Column: "id", Field: "Nullable", Value: "true"},
		},
		{
			name: "foreign key child count before referenced count",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				zForeignKey := phaseGenerationForeignKey(t, table, "z_parent_id")
				zForeignKey.Columns = append(zForeignKey.Columns, "extra")
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedColumns = nil
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyShape, GenerationID: 1, Schema: "business", Table: "a_child", Field: "Columns", ObservedCount: 2},
		},
		{
			name: "foreign key referenced count before child identifier",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				zForeignKey := phaseGenerationForeignKey(t, table, "z_parent_id")
				zForeignKey.ReferencedColumns = append(zForeignKey.ReferencedColumns, "extra")
				phaseGenerationForeignKey(t, table, "a_parent_id").Columns[0] = "bad!"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyShape, GenerationID: 1, Schema: "business", Table: "a_child", Field: "ReferencedColumns", ObservedCount: 2},
		},
		{
			name: "foreign key child identifier before referenced schema",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").Columns[0] = "zz!"
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedSchema = "!"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyIdentifier, GenerationID: 1, Schema: "business", Table: "a_child", Column: "zz!", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "id", Field: "Columns", Value: "zz!"},
		},
		{
			name: "foreign key referenced schema before referenced table",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").ReferencedSchema = "z!"
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedTable = "!"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyIdentifier, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "z!", ReferencedTable: "b_parent", ReferencedColumn: "id", Field: "ReferencedSchema", Value: "z!"},
		},
		{
			name: "foreign key referenced table before referenced column",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").ReferencedTable = "z!"
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedColumns[0] = "!"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyIdentifier, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "z!", ReferencedColumn: "id", Field: "ReferencedTable", Value: "z!"},
		},
		{
			name: "foreign key referenced column before on delete",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").ReferencedColumns[0] = "z!"
				phaseGenerationForeignKey(t, table, "a_parent_id").OnDelete = "bad_delete"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyIdentifier, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "z!", Field: "ReferencedColumns", Value: "z!"},
		},
		{
			name: "foreign key on delete before on update",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").OnDelete = "bad_delete"
				phaseGenerationForeignKey(t, table, "a_parent_id").OnUpdate = "bad_update"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyAction, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "id", Field: "OnDelete", Value: "bad_delete"},
		},
		{
			name: "foreign key on update before duplicate endpoint",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").OnUpdate = "bad_update"
				duplicate := *phaseGenerationForeignKey(t, table, "a_parent_id")
				duplicate.Columns = slices.Clone(duplicate.Columns)
				duplicate.ReferencedColumns = slices.Clone(duplicate.ReferencedColumns)
				table.ForeignKeys = append(table.ForeignKeys, duplicate)
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationInvalidForeignKeyAction, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "id", Field: "OnUpdate", Value: "bad_update"},
		},
		{
			name: "duplicate foreign key before child visibility",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				duplicate := *phaseGenerationForeignKey(t, table, "a_parent_id")
				duplicate.Columns = slices.Clone(duplicate.Columns)
				duplicate.ReferencedColumns = slices.Clone(duplicate.ReferencedColumns)
				table.ForeignKeys = append(table.ForeignKeys, duplicate)
				phaseGenerationForeignKey(t, table, "z_parent_id").Columns[0] = "zz_missing"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationDuplicateForeignKey, GenerationID: 1, Schema: "business", Table: "a_child", Column: "a_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "id", ObservedCount: 2},
		},
		{
			name: "child visibility before target visibility",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").Columns[0] = "zz_missing"
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedTable = "missing"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyColumnNotVisible, GenerationID: 1, Schema: "business", Table: "a_child", Column: "zz_missing"},
		},
		{
			name: "target visibility before target column visibility",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").ReferencedTable = "zz_missing"
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedColumns[0] = "missing_id"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTargetNotVisible, GenerationID: 1, Schema: "business", Table: "a_child", ReferencedSchema: "business", ReferencedTable: "zz_missing"},
		},
		{
			name: "target column visibility before target sync key",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").ReferencedColumns[0] = "zz_missing"
				phaseGenerationForeignKey(t, table, "a_parent_id").ReferencedColumns[0] = "other_id"
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTargetColumnNotVisible, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "zz_missing"},
		},
		{
			name: "target sync key before wire type equality",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationForeignKey(t, table, "z_parent_id").ReferencedColumns[0] = "other_id"
				phaseGenerationColumn(t, table, "a_parent_id").WireType = GenerationWireTypeText
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTargetNotSyncKey, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "other_id"},
		},
		{
			name: "wire type equality before set null nullability",
			mutate: func(t *testing.T, _ *[]RegisteredTable, generations *[]SchemaGeneration) {
				table := phaseGenerationTable(t, *generations, "a_child")
				phaseGenerationColumn(t, table, "z_parent_id").WireType = GenerationWireTypeText
				phaseGenerationColumn(t, table, "a_parent_id").Nullable = false
				phaseGenerationForeignKey(t, table, "a_parent_id").OnDelete = GenerationForeignKeySetNull
			},
			want: SchemaGenerationConfigError{Code: SchemaGenerationForeignKeyTypeMismatch, GenerationID: 1, Schema: "business", Table: "a_child", Column: "z_parent_id", ReferencedSchema: "business", ReferencedTable: "b_parent", ReferencedColumn: "id"},
		},
	}
	require.Len(t, tests, 37, "one case per adjacent boundary in the 38 ordered phases and subphases")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registered, generations := validSchemaGenerationPhaseOrderInput()
			test.mutate(t, &registered, &generations)
			requireSchemaGenerationErrorAcrossPermutations(t, registered, generations, test.want)
		})
	}
}

func TestSchemaGenerationCompiler_DuplicateErrorsIgnoreSecondaryDefectOrder(t *testing.T) {
	tests := []struct {
		name       string
		registered []RegisteredTable
		generation []SchemaGeneration
		wantCode   SchemaGenerationConfigErrorCode
	}{
		{
			name: "registered tables",
			registered: []RegisteredTable{
				{Schema: "business", Table: "person", SyncKeyColumns: nil},
				{Schema: "business", Table: "person", SyncKeyColumns: []string{"id", "other"}},
			},
			generation: mustValidSingleGeneration(), wantCode: SchemaGenerationDuplicateRegisteredTable,
		},
		{
			name:       "generation ids",
			registered: mustValidSingleRegistered(),
			generation: []SchemaGeneration{
				{ID: 1},
				{ID: 1, Tables: []GenerationTable{{Schema: "bad"}}},
			},
			wantCode: SchemaGenerationDuplicateID,
		},
		{
			name:       "generation tables",
			registered: mustValidSingleRegistered(),
			generation: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{
				{Schema: "business", Table: "person"},
				{Schema: "business", Table: "person", Columns: []GenerationColumn{{Name: "BAD"}}},
			}}},
			wantCode: SchemaGenerationDuplicateTable,
		},
		{
			name:       "columns",
			registered: mustValidSingleRegistered(),
			generation: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{
				Schema: "business", Table: "person",
				Columns: []GenerationColumn{
					{Name: "id", WireType: "bad_a"},
					{Name: "id", WireType: "bad_b", Nullable: true},
				},
			}}}},
			wantCode: SchemaGenerationDuplicateColumn,
		},
		{
			name:       "foreign keys",
			registered: func() []RegisteredTable { registered, _ := validForeignKeyGenerationInput(); return registered }(),
			generation: func() []SchemaGeneration {
				_, generations := validForeignKeyGenerationInput()
				first := generations[0].Tables[0].ForeignKeys[0]
				first.Columns[0] = "missing_a"
				second := first
				second.Columns = slices.Clone(first.Columns)
				second.OnDelete = GenerationForeignKeyCascade
				generations[0].Tables[0].ForeignKeys = []GenerationForeignKey{first, second}
				return generations
			}(),
			wantCode: SchemaGenerationDuplicateForeignKey,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var first *SchemaGenerationConfigError
			for permutation := 0; permutation < 2; permutation++ {
				_, err := CompileSchemaGenerationRegistry(test.registered, test.generation)
				var configError *SchemaGenerationConfigError
				require.ErrorAs(t, err, &configError)
				require.Equal(t, test.wantCode, configError.Code)
				if first == nil {
					copy := *configError
					first = &copy
				} else {
					require.Equal(t, first, configError)
				}
				slices.Reverse(test.registered)
				slices.Reverse(test.generation)
				for generationIndex := range test.generation {
					slices.Reverse(test.generation[generationIndex].Tables)
					for tableIndex := range test.generation[generationIndex].Tables {
						slices.Reverse(test.generation[generationIndex].Tables[tableIndex].Columns)
						slices.Reverse(test.generation[generationIndex].Tables[tableIndex].ForeignKeys)
					}
				}
			}
		})
	}
}

func TestSchemaGenerationFingerprint_ChangesWithCompatibilityContract(t *testing.T) {
	registered, generations := validForeignKeyGenerationInput()
	baseline, err := CompileSchemaGenerationRegistry(registered, generations)
	require.NoError(t, err)
	baselineGeneration, ok := baseline.Generation(1)
	require.True(t, ok)

	tests := []struct {
		name   string
		mutate func([]RegisteredTable, []SchemaGeneration)
	}{
		{name: "generation id", mutate: func(_ []RegisteredTable, generations []SchemaGeneration) { generations[0].ID = 2 }},
		{name: "schema and table", mutate: func(registered []RegisteredTable, generations []SchemaGeneration) {
			registered[0].Table = "child_v2"
			generations[0].Tables[0].Table = "child_v2"
		}},
		{name: "sync key", mutate: func(registered []RegisteredTable, generations []SchemaGeneration) {
			registered[0].SyncKeyColumns[0] = "alternate_id"
			generations[0].Tables[0].Columns = append(generations[0].Tables[0].Columns, GenerationColumn{Name: "alternate_id", WireType: GenerationWireTypeUUID})
		}},
		{name: "column name", mutate: func(_ []RegisteredTable, generations []SchemaGeneration) {
			generations[0].Tables[0].Columns[1].Name = "parent_ref"
			generations[0].Tables[0].ForeignKeys[0].Columns[0] = "parent_ref"
		}},
		{name: "column wire type", mutate: func(_ []RegisteredTable, generations []SchemaGeneration) {
			generations[0].Tables[0].Columns[1].WireType = GenerationWireTypeText
			generations[0].Tables[1].Columns[0].WireType = GenerationWireTypeText
		}},
		{name: "column nullability", mutate: func(_ []RegisteredTable, generations []SchemaGeneration) {
			generations[0].Tables[0].Columns[1].Nullable = false
			generations[0].Tables[0].ForeignKeys[0].OnDelete = GenerationForeignKeyNoAction
		}},
		{name: "foreign key action", mutate: func(_ []RegisteredTable, generations []SchemaGeneration) {
			generations[0].Tables[0].ForeignKeys[0].OnDelete = GenerationForeignKeyCascade
		}},
		{name: "foreign key removal", mutate: func(_ []RegisteredTable, generations []SchemaGeneration) {
			generations[0].Tables[0].ForeignKeys = nil
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changedRegistered := cloneRegisteredTables(registered)
			changedGenerations := cloneSchemaGenerations(generations)
			test.mutate(changedRegistered, changedGenerations)
			changed, err := CompileSchemaGenerationRegistry(changedRegistered, changedGenerations)
			require.NoError(t, err)
			changedGeneration, ok := changed.Generation(changed.GenerationIDs()[0])
			require.True(t, ok)
			require.NotEqual(t, baselineGeneration.Fingerprint(), changedGeneration.Fingerprint())
			require.NotEqual(t, baseline.Fingerprint(), changed.Fingerprint())
		})
	}
}

func TestSchemaGenerationDeclarationSurface_IsStrictlyDeclarative(t *testing.T) {
	_, testFilename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	actualSurface, err := schemaGenerationPackageExportedSurface(filepath.Dir(testFilename))
	require.NoError(t, err)
	require.Equal(t, schemaGenerationAllowedPackageSurfaceHash, schemaGenerationPackageSurfaceHash(actualSurface), strings.Join(actualSurface, "\n"))

	require.Equal(t, reflect.Int32, reflect.TypeOf(SchemaGenerationID(0)).Kind())
	require.Equal(t, reflect.String, reflect.TypeOf(GenerationWireType("")).Kind())
	require.Equal(t, reflect.String, reflect.TypeOf(GenerationForeignKeyAction("")).Kind())
	require.Equal(t, reflect.String, reflect.TypeOf(SchemaGenerationConfigErrorCode("")).Kind())

	require.Equal(t, map[string]string{
		"GenerationWireTypeBoolean": "boolean", "GenerationWireTypeInt64": "int64",
		"GenerationWireTypeDecimal": "decimal", "GenerationWireTypeFloat64": "float64", "GenerationWireTypeText": "text",
		"GenerationWireTypeBytes": "bytes", "GenerationWireTypeUUID": "uuid", "GenerationWireTypeJSON": "json",
	}, map[string]string{
		"GenerationWireTypeBoolean": string(GenerationWireTypeBoolean), "GenerationWireTypeInt64": string(GenerationWireTypeInt64),
		"GenerationWireTypeDecimal": string(GenerationWireTypeDecimal), "GenerationWireTypeFloat64": string(GenerationWireTypeFloat64), "GenerationWireTypeText": string(GenerationWireTypeText),
		"GenerationWireTypeBytes": string(GenerationWireTypeBytes), "GenerationWireTypeUUID": string(GenerationWireTypeUUID), "GenerationWireTypeJSON": string(GenerationWireTypeJSON),
	})
	require.Equal(t, map[string]string{
		"GenerationForeignKeyNoAction": "no_action", "GenerationForeignKeyRestrict": "restrict",
		"GenerationForeignKeyCascade": "cascade", "GenerationForeignKeySetNull": "set_null",
	}, map[string]string{
		"GenerationForeignKeyNoAction": string(GenerationForeignKeyNoAction), "GenerationForeignKeyRestrict": string(GenerationForeignKeyRestrict),
		"GenerationForeignKeyCascade": string(GenerationForeignKeyCascade), "GenerationForeignKeySetNull": string(GenerationForeignKeySetNull),
	})
	require.Equal(t, schemaGenerationErrorCodeValues(), map[string]string{
		"SchemaGenerationEmptyRegisteredTables":            string(SchemaGenerationEmptyRegisteredTables),
		"SchemaGenerationInvalidRegisteredTable":           string(SchemaGenerationInvalidRegisteredTable),
		"SchemaGenerationDuplicateRegisteredTable":         string(SchemaGenerationDuplicateRegisteredTable),
		"SchemaGenerationInvalidRegisteredSyncKeyCount":    string(SchemaGenerationInvalidRegisteredSyncKeyCount),
		"SchemaGenerationInvalidRegisteredSyncKey":         string(SchemaGenerationInvalidRegisteredSyncKey),
		"SchemaGenerationHiddenRegisteredSyncKey":          string(SchemaGenerationHiddenRegisteredSyncKey),
		"SchemaGenerationInvalidCount":                     string(SchemaGenerationInvalidCount),
		"SchemaGenerationInvalidID":                        string(SchemaGenerationInvalidID),
		"SchemaGenerationDuplicateID":                      string(SchemaGenerationDuplicateID),
		"SchemaGenerationEmptyTables":                      string(SchemaGenerationEmptyTables),
		"SchemaGenerationInvalidTable":                     string(SchemaGenerationInvalidTable),
		"SchemaGenerationDuplicateTable":                   string(SchemaGenerationDuplicateTable),
		"SchemaGenerationUnregisteredTable":                string(SchemaGenerationUnregisteredTable),
		"SchemaGenerationEmptyColumns":                     string(SchemaGenerationEmptyColumns),
		"SchemaGenerationInvalidColumn":                    string(SchemaGenerationInvalidColumn),
		"SchemaGenerationHiddenColumn":                     string(SchemaGenerationHiddenColumn),
		"SchemaGenerationDuplicateColumn":                  string(SchemaGenerationDuplicateColumn),
		"SchemaGenerationInvalidWireType":                  string(SchemaGenerationInvalidWireType),
		"SchemaGenerationMissingSyncKey":                   string(SchemaGenerationMissingSyncKey),
		"SchemaGenerationInvalidSyncKeyContract":           string(SchemaGenerationInvalidSyncKeyContract),
		"SchemaGenerationInvalidForeignKeyShape":           string(SchemaGenerationInvalidForeignKeyShape),
		"SchemaGenerationInvalidForeignKeyIdentifier":      string(SchemaGenerationInvalidForeignKeyIdentifier),
		"SchemaGenerationInvalidForeignKeyAction":          string(SchemaGenerationInvalidForeignKeyAction),
		"SchemaGenerationDuplicateForeignKey":              string(SchemaGenerationDuplicateForeignKey),
		"SchemaGenerationForeignKeyColumnNotVisible":       string(SchemaGenerationForeignKeyColumnNotVisible),
		"SchemaGenerationForeignKeyTargetNotVisible":       string(SchemaGenerationForeignKeyTargetNotVisible),
		"SchemaGenerationForeignKeyTargetColumnNotVisible": string(SchemaGenerationForeignKeyTargetColumnNotVisible),
		"SchemaGenerationForeignKeyTargetNotSyncKey":       string(SchemaGenerationForeignKeyTargetNotSyncKey),
		"SchemaGenerationForeignKeyTypeMismatch":           string(SchemaGenerationForeignKeyTypeMismatch),
		"SchemaGenerationForeignKeySetNullNonNullable":     string(SchemaGenerationForeignKeySetNullNonNullable),
	})

	requireExactSchemaGenerationStruct(t, SchemaGeneration{}, []schemaGenerationField{
		{name: "ID", fieldType: reflect.TypeOf(SchemaGenerationID(0))},
		{name: "Tables", fieldType: reflect.TypeOf([]GenerationTable(nil))},
	})
	requireExactSchemaGenerationStruct(t, GenerationTable{}, []schemaGenerationField{
		{name: "Schema", fieldType: reflect.TypeOf("")}, {name: "Table", fieldType: reflect.TypeOf("")},
		{name: "Columns", fieldType: reflect.TypeOf([]GenerationColumn(nil))},
		{name: "ForeignKeys", fieldType: reflect.TypeOf([]GenerationForeignKey(nil))},
	})
	requireExactSchemaGenerationStruct(t, GenerationColumn{}, []schemaGenerationField{
		{name: "Name", fieldType: reflect.TypeOf("")}, {name: "WireType", fieldType: reflect.TypeOf(GenerationWireType(""))},
		{name: "Nullable", fieldType: reflect.TypeOf(false)},
	})
	requireExactSchemaGenerationStruct(t, GenerationForeignKey{}, []schemaGenerationField{
		{name: "Columns", fieldType: reflect.TypeOf([]string(nil))}, {name: "ReferencedSchema", fieldType: reflect.TypeOf("")},
		{name: "ReferencedTable", fieldType: reflect.TypeOf("")}, {name: "ReferencedColumns", fieldType: reflect.TypeOf([]string(nil))},
		{name: "OnDelete", fieldType: reflect.TypeOf(GenerationForeignKeyAction(""))},
		{name: "OnUpdate", fieldType: reflect.TypeOf(GenerationForeignKeyAction(""))},
	})
	requireExactSchemaGenerationStruct(t, GenerationTableContract{}, []schemaGenerationField{
		{name: "Schema", fieldType: reflect.TypeOf("")}, {name: "Table", fieldType: reflect.TypeOf("")},
		{name: "SyncKeyColumns", fieldType: reflect.TypeOf([]string(nil))},
		{name: "Columns", fieldType: reflect.TypeOf([]GenerationColumn(nil))},
		{name: "ForeignKeys", fieldType: reflect.TypeOf([]GenerationForeignKey(nil))},
	})
	requireExactSchemaGenerationStruct(t, SchemaGenerationConfigError{}, []schemaGenerationField{
		{name: "Code", fieldType: reflect.TypeOf(SchemaGenerationConfigErrorCode(""))},
		{name: "GenerationID", fieldType: reflect.TypeOf(SchemaGenerationID(0))},
		{name: "Schema", fieldType: reflect.TypeOf("")}, {name: "Table", fieldType: reflect.TypeOf("")},
		{name: "Column", fieldType: reflect.TypeOf("")}, {name: "ReferencedSchema", fieldType: reflect.TypeOf("")},
		{name: "ReferencedTable", fieldType: reflect.TypeOf("")}, {name: "ReferencedColumn", fieldType: reflect.TypeOf("")},
		{name: "Field", fieldType: reflect.TypeOf("")}, {name: "Value", fieldType: reflect.TypeOf("")},
		{name: "ObservedCount", fieldType: reflect.TypeOf(0)},
	})

	for _, value := range []any{SchemaGenerationContract{}, SchemaGenerationRegistry{}} {
		typeOf := reflect.TypeOf(value)
		for index := 0; index < typeOf.NumField(); index++ {
			assert.False(t, typeOf.Field(index).IsExported(), "%s.%s must remain private", typeOf.Name(), typeOf.Field(index).Name)
		}
	}

	compileType := reflect.TypeOf(CompileSchemaGenerationRegistry)
	require.Equal(t, 2, compileType.NumIn())
	require.Equal(t, 2, compileType.NumOut())
	require.False(t, compileType.IsVariadic())
	require.Equal(t, reflect.TypeOf([]RegisteredTable(nil)), compileType.In(0))
	require.Equal(t, reflect.TypeOf([]SchemaGeneration(nil)), compileType.In(1))
	require.Equal(t, reflect.TypeOf((*SchemaGenerationRegistry)(nil)), compileType.Out(0))
	require.Equal(t, reflect.TypeOf((*error)(nil)).Elem(), compileType.Out(1))
	requireExactSchemaGenerationMethods(t, reflect.TypeOf(SchemaGenerationContract{}), map[string]reflect.Type{
		"ID":          reflect.TypeOf((func(SchemaGenerationContract) SchemaGenerationID)(nil)),
		"Fingerprint": reflect.TypeOf((func(SchemaGenerationContract) string)(nil)),
		"Tables":      reflect.TypeOf((func(SchemaGenerationContract) []GenerationTableContract)(nil)),
	})
	requireExactSchemaGenerationMethods(t, reflect.TypeOf((*SchemaGenerationRegistry)(nil)), map[string]reflect.Type{
		"Fingerprint":   reflect.TypeOf((func(*SchemaGenerationRegistry) string)(nil)),
		"GenerationIDs": reflect.TypeOf((func(*SchemaGenerationRegistry) []SchemaGenerationID)(nil)),
		"Generation":    reflect.TypeOf((func(*SchemaGenerationRegistry, SchemaGenerationID) (SchemaGenerationContract, bool))(nil)),
	})
	requireExactSchemaGenerationMethods(t, reflect.TypeOf((*SchemaGenerationConfigError)(nil)), map[string]reflect.Type{
		"Error": reflect.TypeOf((func(*SchemaGenerationConfigError) string)(nil)),
	})
}

func TestRegisteredTableCatalog_RepositoryProductionCallersSetRevision(t *testing.T) {
	_, testFilename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repositoryRoot := filepath.Dir(filepath.Dir(testFilename))
	fileSet := token.NewFileSet()
	var missing []string
	require.NoError(t, filepath.WalkDir(repositoryRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok || !isServiceConfigLiteralType(literal.Type) {
				return true
			}
			for _, element := range literal.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				identifier, ok := field.Key.(*ast.Ident)
				if ok && identifier.Name == "RegisteredTableCatalogRevision" {
					return true
				}
			}
			position := fileSet.Position(literal.Pos())
			relative, _ := filepath.Rel(repositoryRoot, position.Filename)
			missing = append(missing, fmt.Sprintf("%s:%d", relative, position.Line))
			return true
		})
		return nil
	}))
	require.Empty(t, missing, "production ServiceConfig literals must set RegisteredTableCatalogRevision explicitly")
}

func isServiceConfigLiteralType(expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name == "ServiceConfig"
	case *ast.SelectorExpr:
		return typed.Sel.Name == "ServiceConfig"
	default:
		return false
	}
}

func TestSchemaGenerationDeclarationSurface_DetectsPackageWideDrift(t *testing.T) {
	allowedSources := map[string]string{
		"existing.go": `package synthetic

type ExistingUnrelated struct {
	Allowed string
}

type hiddenExisting string
`,
		"surface.go": `package synthetic

type ExistingToken string

const ExistingValue ExistingToken = "existing"

func ExistingFunction(ExistingToken) ExistingToken { return "" }

func (ExistingUnrelated) ExistingMethod() string { return "" }
`,
	}
	allowedSurface, err := schemaGenerationExportedSurfaceFromSources(allowedSources)
	require.NoError(t, err)
	require.Contains(t, allowedSurface, "type ExistingUnrelated struct")

	tests := []struct {
		name            string
		filename        string
		source          string
		replaceSurface  string
		unexpectedEntry string
	}{
		{name: "defined type in second file", filename: "extra.go", source: "package synthetic\n\ntype ExtraType struct{}\n", unexpectedEntry: "type ExtraType struct"},
		{name: "alias in second file", filename: "extra.go", source: "package synthetic\n\ntype ExtraAlias = ExistingToken\n", unexpectedEntry: "type ExtraAlias = ExistingToken"},
		{name: "constant in second file", filename: "extra.go", source: "package synthetic\n\nconst ExtraConstant ExistingToken = \"extra\"\n", unexpectedEntry: "const ExtraConstant ExistingToken = \"extra\""},
		{name: "variable in second file", filename: "extra.go", source: "package synthetic\n\nvar ExtraVariable ExistingToken\n", unexpectedEntry: "var ExtraVariable ExistingToken"},
		{name: "function in second file", filename: "extra.go", source: "package synthetic\n\nfunc ExtraFunction() {}\n", unexpectedEntry: "func ExtraFunction()"},
		{name: "method on additional type", filename: "extra.go", source: "package synthetic\n\ntype extraReceiver struct{}\nfunc (*extraReceiver) ExtraMethod() {}\n", unexpectedEntry: "method *extraReceiver.ExtraMethod()"},
		{
			name: "typed constant becomes untyped",
			replaceSurface: `package synthetic

type ExistingToken string

const ExistingValue = "existing"

func ExistingFunction(ExistingToken) ExistingToken { return "" }

func (ExistingUnrelated) ExistingMethod() string { return "" }
`,
			unexpectedEntry: "const ExistingValue untyped string = \"existing\"",
		},
		{
			name: "method receiver changes",
			replaceSurface: `package synthetic

type ExistingToken string

const ExistingValue ExistingToken = "existing"

func ExistingFunction(ExistingToken) ExistingToken { return "" }

func (*ExistingUnrelated) ExistingMethod() string { return "" }
`,
			unexpectedEntry: "method *ExistingUnrelated.ExistingMethod() string",
		},
		{
			name:     "build constrained second file",
			filename: "extra_plan9.go",
			source: `//go:build plan9

package synthetic

var BuildConstrainedExtra int
`,
			unexpectedEntry: "var BuildConstrainedExtra int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sources := make(map[string]string, len(allowedSources)+1)
			for filename, source := range allowedSources {
				sources[filename] = source
			}
			if test.replaceSurface != "" {
				sources["surface.go"] = test.replaceSurface
			}
			if test.filename != "" {
				sources[test.filename] = test.source
			}

			actualSurface, err := schemaGenerationExportedSurfaceFromSources(sources)
			require.NoError(t, err)
			require.NotEqual(t, allowedSurface, actualSurface)
			require.Contains(t, actualSurface, test.unexpectedEntry)
			require.Contains(t, actualSurface, "type ExistingUnrelated struct")
		})
	}
}

func validSingleTableGenerationInput() ([]RegisteredTable, []SchemaGeneration) {
	return mustValidSingleRegistered(), mustValidSingleGeneration()
}

func mustValidSingleRegistered() []RegisteredTable {
	return []RegisteredTable{{Schema: "business", Table: "person", SyncKeyColumns: []string{"id"}}}
}

func mustValidSingleGeneration() []SchemaGeneration {
	return []SchemaGeneration{{
		ID: 1,
		Tables: []GenerationTable{{
			Schema: "business", Table: "person",
			Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}},
		}},
	}}
}

func validForeignKeyGenerationInput() ([]RegisteredTable, []SchemaGeneration) {
	registered := []RegisteredTable{
		{Schema: "business", Table: "child", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "parent", SyncKeyColumns: []string{"id"}},
	}
	generations := []SchemaGeneration{{
		ID: 1,
		Tables: []GenerationTable{
			{
				Schema: "business", Table: "child",
				Columns: []GenerationColumn{
					{Name: "id", WireType: GenerationWireTypeUUID},
					{Name: "parent_id", WireType: GenerationWireTypeUUID, Nullable: true},
				},
				ForeignKeys: []GenerationForeignKey{{
					Columns: []string{"parent_id"}, ReferencedSchema: "business", ReferencedTable: "parent",
					ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeySetNull, OnUpdate: GenerationForeignKeyNoAction,
				}},
			},
			{
				Schema: "business", Table: "parent",
				Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}},
			},
		},
	}}
	return registered, generations
}

func validSchemaGenerationPhaseOrderInput() ([]RegisteredTable, []SchemaGeneration) {
	registered := []RegisteredTable{
		{Schema: "business", Table: "a_child", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "b_parent", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "y_child", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "z_parent", SyncKeyColumns: []string{"id"}},
	}
	childTable := func(table string, referencedTable string) GenerationTable {
		return GenerationTable{
			Schema: "business", Table: table,
			Columns: []GenerationColumn{
				{Name: "id", WireType: GenerationWireTypeUUID},
				{Name: "a_parent_id", WireType: GenerationWireTypeUUID, Nullable: true},
				{Name: "z_parent_id", WireType: GenerationWireTypeUUID, Nullable: true},
			},
			ForeignKeys: []GenerationForeignKey{
				{
					Columns: []string{"a_parent_id"}, ReferencedSchema: "business", ReferencedTable: referencedTable,
					ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyNoAction, OnUpdate: GenerationForeignKeyNoAction,
				},
				{
					Columns: []string{"z_parent_id"}, ReferencedSchema: "business", ReferencedTable: referencedTable,
					ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyNoAction, OnUpdate: GenerationForeignKeyNoAction,
				},
			},
		}
	}
	parentTable := func(table string) GenerationTable {
		return GenerationTable{
			Schema: "business", Table: table,
			Columns: []GenerationColumn{
				{Name: "id", WireType: GenerationWireTypeUUID},
				{Name: "other_id", WireType: GenerationWireTypeUUID, Nullable: true},
			},
		}
	}
	return registered, []SchemaGeneration{{
		ID: 1,
		Tables: []GenerationTable{
			childTable("a_child", "b_parent"),
			parentTable("b_parent"),
			childTable("y_child", "z_parent"),
			parentTable("z_parent"),
		},
	}}
}

func phaseRegisteredTable(t *testing.T, registered []RegisteredTable, table string) *RegisteredTable {
	t.Helper()
	for index := range registered {
		if registered[index].Table == table {
			return &registered[index]
		}
	}
	t.Fatalf("registered table %q not found", table)
	return nil
}

func phaseGenerationTable(t *testing.T, generations []SchemaGeneration, table string) *GenerationTable {
	t.Helper()
	for generationIndex := range generations {
		for tableIndex := range generations[generationIndex].Tables {
			if generations[generationIndex].Tables[tableIndex].Table == table {
				return &generations[generationIndex].Tables[tableIndex]
			}
		}
	}
	t.Fatalf("generation table %q not found", table)
	return nil
}

func phaseGenerationColumn(t *testing.T, table *GenerationTable, column string) *GenerationColumn {
	t.Helper()
	for index := range table.Columns {
		if table.Columns[index].Name == column {
			return &table.Columns[index]
		}
	}
	t.Fatalf("generation column %q not found", column)
	return nil
}

func phaseRemoveGenerationColumn(t *testing.T, table *GenerationTable, column string) {
	t.Helper()
	for index := range table.Columns {
		if table.Columns[index].Name == column {
			table.Columns = slices.Delete(table.Columns, index, index+1)
			return
		}
	}
	t.Fatalf("generation column %q not found", column)
}

func phaseGenerationForeignKey(t *testing.T, table *GenerationTable, column string) *GenerationForeignKey {
	t.Helper()
	for index := range table.ForeignKeys {
		if len(table.ForeignKeys[index].Columns) == 1 && table.ForeignKeys[index].Columns[0] == column {
			return &table.ForeignKeys[index]
		}
	}
	t.Fatalf("foreign key for child column %q not found", column)
	return nil
}

func requireSchemaGenerationErrorAcrossPermutations(
	t *testing.T,
	registered []RegisteredTable,
	generations []SchemaGeneration,
	want SchemaGenerationConfigError,
) {
	t.Helper()
	for permutation := 0; permutation < 4; permutation++ {
		permutedRegistered := cloneRegisteredTables(registered)
		permutedGenerations := cloneSchemaGenerations(generations)
		if permutation&1 != 0 {
			slices.Reverse(permutedRegistered)
			slices.Reverse(permutedGenerations)
		}
		if permutation&2 != 0 {
			for generationIndex := range permutedGenerations {
				slices.Reverse(permutedGenerations[generationIndex].Tables)
				for tableIndex := range permutedGenerations[generationIndex].Tables {
					table := &permutedGenerations[generationIndex].Tables[tableIndex]
					slices.Reverse(table.Columns)
					slices.Reverse(table.ForeignKeys)
				}
			}
		}
		_, err := CompileSchemaGenerationRegistry(permutedRegistered, permutedGenerations)
		requireSchemaGenerationConfigError(t, err, want)
	}
}

type schemaGenerationField struct {
	name      string
	fieldType reflect.Type
}

func schemaGenerationPackageExportedSurface(packageDir string) ([]string, error) {
	entries, err := os.ReadDir(packageDir)
	if err != nil {
		return nil, err
	}

	fileSet := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, filepath.Join(packageDir, name), nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return nil, parseErr
		}
		files = append(files, parsed)
	}

	packageImporter, err := schemaGenerationGoListImporter(fileSet, packageDir)
	if err != nil {
		return nil, err
	}
	configuration := types.Config{Importer: packageImporter, IgnoreFuncBodies: true}
	typedPackage, err := configuration.Check("github.com/mobiletoly/go-oversync/oversync", fileSet, files, nil)
	if err != nil {
		return nil, err
	}
	return schemaGenerationExportedSurface(typedPackage), nil
}

func schemaGenerationGoListImporter(fileSet *token.FileSet, packageDir string) (types.Importer, error) {
	command := exec.Command("go", "list", "-export", "-deps", "-json", ".")
	command.Dir = packageDir
	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("go list package exports: %w: %s", err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf("go list package exports: %w", err)
	}

	exportFiles := make(map[string]string)
	decoder := json.NewDecoder(bytes.NewReader(output))
	for decoder.More() {
		var listed struct {
			ImportPath string
			Export     string
		}
		if err := decoder.Decode(&listed); err != nil {
			return nil, fmt.Errorf("decode go list package exports: %w", err)
		}
		if listed.Export != "" {
			exportFiles[listed.ImportPath] = listed.Export
		}
	}

	lookup := func(importPath string) (io.ReadCloser, error) {
		exportFilename, ok := exportFiles[importPath]
		if !ok {
			return nil, fmt.Errorf("missing export data for %q", importPath)
		}
		return os.Open(exportFilename)
	}
	return importer.ForCompiler(fileSet, "gc", lookup), nil
}

func schemaGenerationExportedSurface(typedPackage *types.Package) []string {
	qualifier := func(imported *types.Package) string {
		if imported == typedPackage {
			return ""
		}
		return imported.Path()
	}

	var surface []string
	scope := typedPackage.Scope()
	for _, name := range scope.Names() {
		object := scope.Lookup(name)
		switch object := object.(type) {
		case *types.Const:
			if !object.Exported() {
				continue
			}
			surface = append(surface, fmt.Sprintf("const %s %s = %s", name, types.TypeString(object.Type(), qualifier), object.Val().ExactString()))
		case *types.Func:
			if !object.Exported() {
				continue
			}
			surface = append(surface, "func "+name+schemaGenerationSignatureString(object.Type().(*types.Signature), qualifier))
		case *types.TypeName:
			exportedType := object.Exported()
			if object.IsAlias() {
				if exportedType {
					surface = append(surface, "type "+name+" = "+types.TypeString(types.Unalias(object.Type()), qualifier))
				}
				continue
			}
			named, ok := object.Type().(*types.Named)
			if !ok {
				if exportedType {
					surface = append(surface, "type "+name+" "+types.TypeString(object.Type().Underlying(), qualifier))
				}
				continue
			}
			if exportedType {
				switch underlying := named.Underlying().(type) {
				case *types.Struct:
					surface = append(surface, "type "+name+" struct")
					for index := 0; index < underlying.NumFields(); index++ {
						field := underlying.Field(index)
						if field.Exported() {
							surface = append(surface, fmt.Sprintf("field %s.%s %s tag=%q", name, field.Name(), types.TypeString(field.Type(), qualifier), underlying.Tag(index)))
						}
					}
				case *types.Interface:
					surface = append(surface, "type "+name+" interface")
				default:
					surface = append(surface, "type "+name+" "+types.TypeString(underlying, qualifier))
				}
			}
			for methodIndex := 0; methodIndex < named.NumMethods(); methodIndex++ {
				method := named.Method(methodIndex)
				if !method.Exported() {
					continue
				}
				signature := method.Type().(*types.Signature)
				receiver := types.TypeString(signature.Recv().Type(), qualifier)
				surface = append(surface, "method "+receiver+"."+method.Name()+schemaGenerationSignatureString(signature, qualifier))
			}
		case *types.Var:
			if !object.Exported() {
				continue
			}
			surface = append(surface, "var "+name+" "+types.TypeString(object.Type(), qualifier))
		}
	}
	slices.Sort(surface)
	return surface
}

func schemaGenerationExportedSurfaceFromSources(sources map[string]string) ([]string, error) {
	fileSet := token.NewFileSet()
	filenames := make([]string, 0, len(sources))
	for filename := range sources {
		filenames = append(filenames, filename)
	}
	slices.Sort(filenames)
	files := make([]*ast.File, 0, len(filenames))
	for _, filename := range filenames {
		parsed, err := parser.ParseFile(fileSet, filename, sources[filename], parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		files = append(files, parsed)
	}
	configuration := types.Config{IgnoreFuncBodies: true}
	typedPackage, err := configuration.Check("example.com/synthetic", fileSet, files, nil)
	if err != nil {
		return nil, err
	}
	return schemaGenerationExportedSurface(typedPackage), nil
}

func schemaGenerationSignatureString(signature *types.Signature, qualifier types.Qualifier) string {
	parameterTuple := schemaGenerationAnonymousTuple(signature.Params())
	resultTuple := schemaGenerationAnonymousTuple(signature.Results())
	normalized := types.NewSignatureType(nil, nil, nil, parameterTuple, resultTuple, signature.Variadic())
	return strings.TrimPrefix(types.TypeString(normalized, qualifier), "func")
}

func schemaGenerationAnonymousTuple(tuple *types.Tuple) *types.Tuple {
	variables := make([]*types.Var, tuple.Len())
	for index := range variables {
		variables[index] = types.NewVar(token.NoPos, nil, "", tuple.At(index).Type())
	}
	return types.NewTuple(variables...)
}

// rollout adds typed lifecycle, retirement permits, bounded observations and readiness digests.
// No executable field or transform was added to generation declarations.
// schemaGenerationAllowedPackageSurfaceHash freezes the sorted go/types surface
// for every production file in package oversync, including files excluded by
// the active build context. The assertion prints the complete canonical surface
// on failure so an API change cannot be accepted without inspecting the object,
// field, constant type/value, function signature, or method receiver that moved.
// transition adds typed readiness evidence/admission and application write rejection;
// revision 5 adds the explicit admission pool, tighter write byte bound and
// unconfirmed rollback error. Its complete API delta is retained in revision5 evidence.
// generation declarations, compiled registry and fingerprint rules remain unchanged.
// table-evolution adds bounded private staging cleanup, catalog readiness association,
// catalog report fields, and the private background-worker switch.
// Discovery adds GetGeneration, HandleGeneration and its two-field response;
// the unreleased v1 vocabulary drops int16, int32 and float32 constants.
// Scope lifecycle adds the SQL-only transaction admission capability, closed origin
// kinds, safe admission errors and the existing-only ScopeManager write option.
// Generation declaration types and fingerprints remain unchanged.
const schemaGenerationAllowedPackageSurfaceHash = "a8ed6636c04ef884eae95b4517bcc5ac31bc9d7938344cc4cda249dae0612a37"

func schemaGenerationPackageSurfaceHash(surface []string) string {
	digest := sha256.Sum256([]byte(strings.Join(surface, "\n")))
	return hex.EncodeToString(digest[:])
}

func schemaGenerationErrorCodeValues() map[string]string {
	return map[string]string{
		"SchemaGenerationEmptyRegisteredTables":            "empty_registered_tables",
		"SchemaGenerationInvalidRegisteredTable":           "invalid_registered_table",
		"SchemaGenerationDuplicateRegisteredTable":         "duplicate_registered_table",
		"SchemaGenerationInvalidRegisteredSyncKeyCount":    "invalid_registered_sync_key_count",
		"SchemaGenerationInvalidRegisteredSyncKey":         "invalid_registered_sync_key",
		"SchemaGenerationHiddenRegisteredSyncKey":          "hidden_registered_sync_key",
		"SchemaGenerationInvalidCount":                     "invalid_generation_count",
		"SchemaGenerationInvalidID":                        "invalid_generation_id",
		"SchemaGenerationDuplicateID":                      "duplicate_generation_id",
		"SchemaGenerationEmptyTables":                      "empty_generation_tables",
		"SchemaGenerationInvalidTable":                     "invalid_generation_table",
		"SchemaGenerationDuplicateTable":                   "duplicate_generation_table",
		"SchemaGenerationUnregisteredTable":                "unregistered_generation_table",
		"SchemaGenerationEmptyColumns":                     "empty_generation_columns",
		"SchemaGenerationInvalidColumn":                    "invalid_generation_column",
		"SchemaGenerationHiddenColumn":                     "hidden_generation_column",
		"SchemaGenerationDuplicateColumn":                  "duplicate_generation_column",
		"SchemaGenerationInvalidWireType":                  "invalid_generation_wire_type",
		"SchemaGenerationMissingSyncKey":                   "missing_generation_sync_key",
		"SchemaGenerationInvalidSyncKeyContract":           "invalid_generation_sync_key_contract",
		"SchemaGenerationInvalidForeignKeyShape":           "invalid_generation_foreign_key_shape",
		"SchemaGenerationInvalidForeignKeyIdentifier":      "invalid_generation_foreign_key_identifier",
		"SchemaGenerationInvalidForeignKeyAction":          "invalid_generation_foreign_key_action",
		"SchemaGenerationDuplicateForeignKey":              "duplicate_generation_foreign_key",
		"SchemaGenerationForeignKeyColumnNotVisible":       "generation_foreign_key_column_not_visible",
		"SchemaGenerationForeignKeyTargetNotVisible":       "generation_foreign_key_target_not_visible",
		"SchemaGenerationForeignKeyTargetColumnNotVisible": "generation_foreign_key_target_column_not_visible",
		"SchemaGenerationForeignKeyTargetNotSyncKey":       "generation_foreign_key_target_not_sync_key",
		"SchemaGenerationForeignKeyTypeMismatch":           "generation_foreign_key_type_mismatch",
		"SchemaGenerationForeignKeySetNullNonNullable":     "generation_foreign_key_set_null_non_nullable",
	}
}

func requireExactSchemaGenerationStruct(t *testing.T, value any, expected []schemaGenerationField) {
	t.Helper()
	typeOf := reflect.TypeOf(value)
	require.Equal(t, len(expected), typeOf.NumField())
	for index, field := range expected {
		require.Equal(t, field.name, typeOf.Field(index).Name)
		require.Equal(t, field.fieldType, typeOf.Field(index).Type)
		require.NotEqual(t, reflect.Func, typeOf.Field(index).Type.Kind())
	}
}

func requireExactSchemaGenerationMethods(t *testing.T, typeOf reflect.Type, expected map[string]reflect.Type) {
	t.Helper()
	require.Equal(t, len(expected), typeOf.NumMethod())
	for name, methodType := range expected {
		method, ok := typeOf.MethodByName(name)
		require.True(t, ok, "%s.%s is missing", typeOf, name)
		require.Equal(t, methodType, method.Type)
	}
}

func mutateGenerations(generations []SchemaGeneration, mutate func([]SchemaGeneration)) []SchemaGeneration {
	cloned := cloneSchemaGenerations(generations)
	mutate(cloned)
	return cloned
}

func requireSchemaGenerationConfigError(t *testing.T, err error, want SchemaGenerationConfigError) {
	t.Helper()
	require.Error(t, err)
	var got *SchemaGenerationConfigError
	require.True(t, errors.As(err, &got), "expected SchemaGenerationConfigError, got %T: %v", err, err)
	require.Equal(t, &want, got)
	require.NotEmpty(t, got.Error())
}
