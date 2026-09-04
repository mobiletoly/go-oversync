package oversync

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var testPhysicalGenerationTables sync.Map

func recordTestBusinessSchemaGeneration(schema string) {
	testPhysicalGenerationTables.Store(schema, map[string]GenerationTable{
		"users": {
			Schema: schema, Table: "users",
			Columns: []GenerationColumn{{Name: "email", WireType: GenerationWireTypeText}, {Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}},
		},
		"posts": {
			Schema: schema, Table: "posts",
			Columns:     []GenerationColumn{{Name: "author_id", WireType: GenerationWireTypeUUID}, {Name: "content", WireType: GenerationWireTypeText}, {Name: "id", WireType: GenerationWireTypeUUID}, {Name: "title", WireType: GenerationWireTypeText}},
			ForeignKeys: []GenerationForeignKey{{Columns: []string{"author_id"}, ReferencedSchema: schema, ReferencedTable: "users", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyCascade, OnUpdate: GenerationForeignKeyNoAction}},
		},
		"files": {
			Schema: schema, Table: "files",
			Columns: []GenerationColumn{{Name: "data", WireType: GenerationWireTypeBytes}, {Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}},
		},
		"file_reviews": {
			Schema: schema, Table: "file_reviews",
			Columns:     []GenerationColumn{{Name: "file_id", WireType: GenerationWireTypeUUID}, {Name: "id", WireType: GenerationWireTypeUUID}, {Name: "review", WireType: GenerationWireTypeText}},
			ForeignKeys: []GenerationForeignKey{{Columns: []string{"file_id"}, ReferencedSchema: schema, ReferencedTable: "files", ReferencedColumns: []string{"id"}, OnDelete: GenerationForeignKeyCascade, OnUpdate: GenerationForeignKeyNoAction}},
		},
	})
}

func testSchemaGenerationProofForService(service *SyncService) SchemaGenerationProof {
	if service == nil || service.schemaGenerationRegistry == nil {
		return SchemaGenerationProof{ID: 1, Fingerprint: "0000000000000000000000000000000000000000000000000000000000000000"}
	}
	ids := service.schemaGenerationRegistry.GenerationIDs()
	if len(ids) == 0 {
		return SchemaGenerationProof{ID: 1, Fingerprint: "0000000000000000000000000000000000000000000000000000000000000000"}
	}
	contract, _ := service.schemaGenerationRegistry.Generation(ids[0])
	return SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}
}

func testSchemaGenerationProofForTables(t *testing.T, tables []RegisteredTable) SchemaGenerationProof {
	t.Helper()
	registry, err := CompileSchemaGenerationRegistry(tables, testSchemaGenerationsForRegisteredTables(tables))
	require.NoError(t, err)
	contract, ok := registry.Generation(1)
	require.True(t, ok)
	return SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}
}

func setTestSchemaGenerationHeaders(request *http.Request, service *SyncService) {
	proof := testSchemaGenerationProofForService(service)
	request.Header.Set(SchemaGenerationIDHeader, canonicalSchemaGenerationID(proof.ID))
	request.Header.Set(SchemaGenerationFingerprintHeader, proof.Fingerprint)
}

func testSchemaGenerationsForRegisteredTables(tables []RegisteredTable) []SchemaGeneration {
	return testSchemaGenerationsForRegisteredTablesWithWireTypes(tables, nil)
}

func testSchemaGenerationsForRegisteredTablesWithWireTypes(tables []RegisteredTable, wireTypes map[string]GenerationWireType) []SchemaGeneration {
	generationTables := make([]GenerationTable, 0, len(tables))
	for _, table := range tables {
		if schemaTables, ok := testPhysicalGenerationTables.Load(table.Schema); ok {
			if declared, ok := schemaTables.(map[string]GenerationTable)[table.Table]; ok {
				declared.Columns = append([]GenerationColumn(nil), declared.Columns...)
				declared.ForeignKeys = append([]GenerationForeignKey(nil), declared.ForeignKeys...)
				for i := range declared.Columns {
					if configured := wireTypes[Key(table.Schema, table.Table)+"."+declared.Columns[i].Name]; configured != "" {
						declared.Columns[i].WireType = configured
					}
				}
				generationTables = append(generationTables, declared)
				continue
			}
		}
		columns := make([]GenerationColumn, 0, len(table.SyncKeyColumns))
		for _, column := range table.SyncKeyColumns {
			wireType := GenerationWireTypeUUID
			if configured := wireTypes[Key(table.Schema, table.Table)+"."+column]; configured != "" {
				wireType = configured
			}
			columns = append(columns, GenerationColumn{Name: column, WireType: wireType, Nullable: false})
		}
		generationTables = append(generationTables, GenerationTable{Schema: table.Schema, Table: table.Table, Columns: columns})
	}
	return []SchemaGeneration{{ID: 1, Tables: generationTables}}
}

func testSchemaGenerationsFromDatabase(t *testing.T, ctx context.Context, q syncCatalogQuerier, tables []RegisteredTable, wireTypes map[string]GenerationWireType) []SchemaGeneration {
	t.Helper()
	physicalColumns, err := loadGenerationPhysicalColumns(ctx, q, tables)
	require.NoError(t, err)
	registeredSet := make(map[string]bool, len(tables))
	for _, table := range tables {
		registeredSet[Key(table.Schema, table.Table)] = true
	}
	physicalFKs, err := newSchemaDiscoveryWithQuerier(q, nil).getForeignKeyConstraints(ctx, registeredSet)
	require.NoError(t, err)
	fksByTable := make(map[string][]GenerationForeignKey)
	for _, fk := range physicalFKs {
		if fk.ColumnName == syncScopeColumnName || fk.RefColumn == syncScopeColumnName {
			continue
		}
		key := Key(fk.TableSchema, fk.TableName)
		fksByTable[key] = append(fksByTable[key], GenerationForeignKey{
			Columns: []string{fk.ColumnName}, ReferencedSchema: fk.RefSchema, ReferencedTable: fk.RefTable,
			ReferencedColumns: []string{fk.RefColumn}, OnDelete: generationFKAction(fk.OnDeleteAction), OnUpdate: generationFKAction(fk.OnUpdateAction),
		})
	}
	generationTables := make([]GenerationTable, 0, len(tables))
	for _, table := range tables {
		key := Key(table.Schema, table.Table)
		columns := make([]GenerationColumn, 0, len(physicalColumns[key])-1)
		for name, physical := range physicalColumns[key] {
			if name == syncScopeColumnName {
				continue
			}
			wireType, ok := postgresGenerationWireType(physical)
			require.True(t, ok, "unsupported test wire type for %s.%s", key, name)
			if configured := wireTypes[key+"."+name]; configured != "" {
				wireType = configured
			}
			columns = append(columns, GenerationColumn{Name: name, WireType: wireType, Nullable: physical.nullable})
		}
		generationTables = append(generationTables, GenerationTable{Schema: table.Schema, Table: table.Table, Columns: columns, ForeignKeys: fksByTable[key]})
	}
	return []SchemaGeneration{{ID: 1, Tables: generationTables}}
}
