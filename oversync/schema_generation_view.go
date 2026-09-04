// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

type generationPhysicalColumn struct {
	typeSchema string
	typeName   string
	typeMod    int32
	nullable   bool
	generated  string
	identity   string
}

func (s *SyncService) buildAndValidateGenerationViews(ctx context.Context, q syncCatalogQuerier) error {
	if s == nil || s.schemaGenerationRegistry == nil {
		return fmt.Errorf("schema generation registry is unavailable")
	}
	columns, err := loadGenerationPhysicalColumns(ctx, q, s.config.RegisteredTables)
	if err != nil {
		return err
	}
	fks, err := newSchemaDiscoveryWithQuerier(q, s.logger).getForeignKeyConstraints(ctx, s.registeredTables)
	if err != nil {
		return fmt.Errorf("load generation foreign keys: %w", err)
	}
	views := make(map[SchemaGenerationID]*schemaGenerationView)
	ids := s.schemaGenerationRegistry.GenerationIDs()

	// Complete-population existence pass.
	for _, id := range ids {
		contract, _ := s.schemaGenerationRegistry.Generation(id)
		for _, table := range contract.Tables() {
			for _, column := range table.Columns {
				if _, ok := columns[Key(table.Schema, table.Table)][column.Name]; !ok {
					return generationSchemaError(UnsupportedSchemaGenerationColumnMissing, id, table.Schema, table.Table, column.Name, "column", column.Name, "", "generation column is missing")
				}
			}
		}
	}
	// Type pass.
	for _, id := range ids {
		contract, _ := s.schemaGenerationRegistry.Generation(id)
		for _, table := range contract.Tables() {
			for _, column := range table.Columns {
				physical := columns[Key(table.Schema, table.Table)][column.Name]
				actual, ok := postgresGenerationWireType(physical)
				if !ok || actual != column.WireType {
					return generationSchemaError(UnsupportedSchemaGenerationColumnTypeMismatch, id, table.Schema, table.Table, column.Name, "wire_type", string(column.WireType), physical.typeSchema+"."+physical.typeName, "generation column type does not match")
				}
			}
		}
	}
	// Nullability and writability pass.
	for _, id := range ids {
		contract, _ := s.schemaGenerationRegistry.Generation(id)
		for _, table := range contract.Tables() {
			for _, column := range table.Columns {
				physical := columns[Key(table.Schema, table.Table)][column.Name]
				if physical.nullable != column.Nullable {
					return generationSchemaError(UnsupportedSchemaGenerationColumnNullabilityMismatch, id, table.Schema, table.Table, column.Name, "nullable", fmt.Sprint(column.Nullable), fmt.Sprint(physical.nullable), "generation column nullability does not match")
				}
				if physical.generated != "" || physical.identity != "" {
					return generationSchemaError(UnsupportedSchemaGenerationColumnNotWritable, id, table.Schema, table.Table, column.Name, "writable", "true", "false", "generation column is not writable")
				}
			}
		}
	}

	for _, id := range ids {
		contract, _ := s.schemaGenerationRegistry.Generation(id)
		if err := validateGenerationForeignKeys(id, contract, fks); err != nil {
			return err
		}
		view := &schemaGenerationView{
			contract:        contract,
			tables:          make(map[string]generationTableView),
			canonicalTables: make(map[string]canonicalProjectionTable, len(s.retainedTableByID)),
		}
		for _, info := range s.retainedTableByID {
			view.canonicalTables[Key(info.schemaName, info.tableName)] = canonicalProjectionTable{syncKeyColumn: info.syncKeyColumn, syncKeyType: info.syncKeyType}
		}
		for _, table := range contract.Tables() {
			info, ok := s.registeredTableInfo[Key(table.Schema, table.Table)]
			if !ok {
				return fmt.Errorf("generation %d table %s.%s has no active catalog join", id, table.Schema, table.Table)
			}
			tableView := generationTableView{contract: table, tableID: info.tableID}
			view.tables[Key(table.Schema, table.Table)] = tableView
			view.byID = append(view.byID, tableView)
		}
		sort.Slice(view.byID, func(i, j int) bool { return view.byID[i].tableID < view.byID[j].tableID })
		views[id] = view
	}
	for _, id := range ids {
		views[id].dataPlaneReady = true
	}
	s.generationViews = views
	return nil
}

func generationMatchesCanonicalDataPlane(view *schemaGenerationView, registered map[string]registeredTableRuntimeInfo, physical map[string]map[string]generationPhysicalColumn) bool {
	if view == nil || len(view.tables) != len(registered) {
		return false
	}
	for tableKey := range registered {
		tableView, ok := view.tables[tableKey]
		if !ok {
			return false
		}
		physicalColumns := physical[tableKey]
		if len(physicalColumns) == 0 {
			return false
		}
		visibleColumns := make(map[string]struct{}, len(tableView.contract.Columns))
		for _, column := range tableView.contract.Columns {
			visibleColumns[column.Name] = struct{}{}
		}
		canonicalColumnCount := 0
		for column := range physicalColumns {
			if column == syncScopeColumnName {
				continue
			}
			canonicalColumnCount++
			if _, ok := visibleColumns[column]; !ok {
				return false
			}
		}
		if len(visibleColumns) != canonicalColumnCount {
			return false
		}
	}
	return true
}

func loadGenerationPhysicalColumns(ctx context.Context, q syncCatalogQuerier, registeredTables []RegisteredTable) (map[string]map[string]generationPhysicalColumn, error) {
	var schemas, tables []string
	seen := map[string]bool{}
	for _, table := range registeredTables {
		key := Key(table.Schema, table.Table)
		if !seen[key] {
			seen[key] = true
			schemas = append(schemas, table.Schema)
			tables = append(tables, table.Table)
		}
	}
	rows, err := q.Query(ctx, `
		WITH configured AS (SELECT * FROM unnest(@schemas::text[], @tables::text[]) AS x(schema_name, table_name))
		SELECT n.nspname, c.relname, a.attname, tn.nspname, t.typname, a.atttypmod,
		       NOT a.attnotnull, a.attgenerated::text, a.attidentity::text
		FROM configured x
		JOIN pg_namespace n ON n.nspname=x.schema_name
		JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=x.table_name
		JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
		JOIN pg_type t ON t.oid=a.atttypid
		JOIN pg_namespace tn ON tn.oid=t.typnamespace
		ORDER BY n.nspname,c.relname,a.attnum`, pgx.NamedArgs{"schemas": schemas, "tables": tables})
	if err != nil {
		return nil, fmt.Errorf("load generation physical columns: %w", err)
	}
	defer rows.Close()
	result := make(map[string]map[string]generationPhysicalColumn)
	for rows.Next() {
		var schema, table, column string
		var fact generationPhysicalColumn
		if err := rows.Scan(&schema, &table, &column, &fact.typeSchema, &fact.typeName, &fact.typeMod, &fact.nullable, &fact.generated, &fact.identity); err != nil {
			return nil, fmt.Errorf("scan generation physical column: %w", err)
		}
		key := Key(schema, table)
		if result[key] == nil {
			result[key] = make(map[string]generationPhysicalColumn)
		}
		result[key][column] = fact
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate generation physical columns: %w", err)
	}
	return result, nil
}

func postgresGenerationWireType(column generationPhysicalColumn) (GenerationWireType, bool) {
	if column.typeSchema != "pg_catalog" {
		return "", false
	}
	switch column.typeName {
	case "bool":
		return GenerationWireTypeBoolean, true
	case "int2", "int4", "int8":
		return GenerationWireTypeInt64, true
	case "numeric":
		return GenerationWireTypeDecimal, column.typeMod == -1
	case "float4", "float8":
		return GenerationWireTypeFloat64, true
	case "text":
		return GenerationWireTypeText, true
	case "varchar":
		return GenerationWireTypeText, column.typeMod == -1
	case "bytea":
		return GenerationWireTypeBytes, true
	case "uuid":
		return GenerationWireTypeUUID, true
	case "json", "jsonb":
		return GenerationWireTypeJSON, true
	default:
		return "", false
	}
}

type logicalGenerationFK struct {
	childSchema, childTable, childColumn    string
	parentSchema, parentTable, parentColumn string
	onDelete, onUpdate                      GenerationForeignKeyAction
}

type generationForeignKeyDefect struct {
	code                                 UnsupportedSchemaErrorCode
	childSchema, childTable, childColumn string
	field, expected, actual, message     string
}

func validateGenerationForeignKeys(id SchemaGenerationID, contract SchemaGenerationContract, physical []ForeignKeyConstraint) error {
	visible := map[string]map[string]bool{}
	for _, table := range contract.Tables() {
		visible[Key(table.Schema, table.Table)] = map[string]bool{}
		for _, col := range table.Columns {
			visible[Key(table.Schema, table.Table)][col.Name] = true
		}
	}
	var actual []logicalGenerationFK
	var defects []generationForeignKeyDefect
	groups := map[string][]ForeignKeyConstraint{}
	for _, fk := range physical {
		groupKey := Key(fk.TableSchema, fk.TableName) + "\x00" + foreignKeyGroupIdentity(fk)
		groups[groupKey] = append(groups[groupKey], fk)
	}
	for _, group := range groups {
		sort.Slice(group, func(i, j int) bool { return group[i].Ordinal < group[j].Ordinal })
		var child, parent *ForeignKeyConstraint
		for i := range group {
			if group[i].ColumnName == syncScopeColumnName {
				continue
			}
			if visible[Key(group[i].TableSchema, group[i].TableName)][group[i].ColumnName] {
				child = &group[i]
			}
		}
		if child == nil {
			continue
		}
		for i := range group {
			if group[i].RefColumn != syncScopeColumnName {
				parent = &group[i]
				break
			}
		}
		if parent == nil || !visible[Key(parent.RefSchema, parent.RefTable)][parent.RefColumn] {
			defects = append(defects, generationForeignKeyDefect{
				code:        UnsupportedSchemaGenerationForeignKeyMismatch,
				childSchema: child.TableSchema, childTable: child.TableName, childColumn: child.ColumnName,
				field: "foreign_key", expected: "visible target", actual: "hidden target", message: "generation foreign key target is not visible",
			})
			continue
		}
		if generationFKAction(child.OnDeleteAction) == GenerationForeignKeySetNull && !scopePreservingDeleteSetNull(group, child.ColumnName) {
			defects = append(defects, generationForeignKeyDefect{
				code:        UnsupportedSchemaGenerationForeignKeyMismatch,
				childSchema: child.TableSchema, childTable: child.TableName, childColumn: child.ColumnName,
				field: "on_delete", expected: "set_null_visible_column", actual: "set_null_all_columns", message: "generation foreign key SET NULL action would clear the hidden scope column",
			})
		}
		actual = append(actual, logicalGenerationFK{child.TableSchema, child.TableName, child.ColumnName, parent.RefSchema, parent.RefTable, parent.RefColumn, generationFKAction(child.OnDeleteAction), generationFKAction(child.OnUpdateAction)})
	}
	var expected []logicalGenerationFK
	for _, table := range contract.Tables() {
		for _, fk := range table.ForeignKeys {
			expected = append(expected, logicalGenerationFK{table.Schema, table.Table, fk.Columns[0], fk.ReferencedSchema, fk.ReferencedTable, fk.ReferencedColumns[0], fk.OnDelete, fk.OnUpdate})
		}
	}
	sortLogicalGenerationFK(actual)
	sortLogicalGenerationFK(expected)
	for i := 0; i < len(expected) || i < len(actual); i++ {
		if i >= len(actual) {
			e := expected[i]
			defects = append(defects, generationForeignKeyDefect{code: UnsupportedSchemaGenerationForeignKeyMissing, childSchema: e.childSchema, childTable: e.childTable, childColumn: e.childColumn, field: "foreign_key", expected: fmt.Sprint(e), message: "generation foreign key is missing"})
			continue
		}
		if i >= len(expected) {
			a := actual[i]
			defects = append(defects, generationForeignKeyDefect{code: UnsupportedSchemaGenerationForeignKeyUnexpected, childSchema: a.childSchema, childTable: a.childTable, childColumn: a.childColumn, field: "foreign_key", actual: fmt.Sprint(a), message: "generation foreign key is unexpected"})
			continue
		}
		if expected[i] != actual[i] {
			e, a := expected[i], actual[i]
			defects = append(defects, generationForeignKeyDefect{code: UnsupportedSchemaGenerationForeignKeyMismatch, childSchema: e.childSchema, childTable: e.childTable, childColumn: e.childColumn, field: "foreign_key", expected: fmt.Sprint(e), actual: fmt.Sprint(a), message: "generation foreign key does not match"})
		}
	}
	if len(defects) > 0 {
		sort.Slice(defects, func(i, j int) bool {
			left := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", defects[i].childSchema, defects[i].childTable, defects[i].childColumn, defects[i].field, defects[i].expected, defects[i].actual, defects[i].code)
			right := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", defects[j].childSchema, defects[j].childTable, defects[j].childColumn, defects[j].field, defects[j].expected, defects[j].actual, defects[j].code)
			return left < right
		})
		first := defects[0]
		return generationSchemaError(first.code, id, first.childSchema, first.childTable, first.childColumn, first.field, first.expected, first.actual, first.message)
	}
	return nil
}

func scopePreservingDeleteSetNull(group []ForeignKeyConstraint, visibleChildColumn string) bool {
	for _, fk := range group {
		if !fk.deleteSetColumnsSpecified {
			return false
		}
		if fk.ColumnName == visibleChildColumn {
			if !fk.deleteSetsThisColumn {
				return false
			}
			continue
		}
		if fk.deleteSetsThisColumn {
			return false
		}
	}
	return true
}

func generationFKAction(value string) GenerationForeignKeyAction {
	return GenerationForeignKeyAction(strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), " ", "_")))
}
func sortLogicalGenerationFK(values []logicalGenerationFK) {
	sort.Slice(values, func(i, j int) bool { return fmt.Sprint(values[i]) < fmt.Sprint(values[j]) })
}
func generationSchemaError(code UnsupportedSchemaErrorCode, id SchemaGenerationID, schema, table, column, field, expected, actual, message string) error {
	return &UnsupportedSchemaError{Message: message, Code: code, GenerationID: id, Schema: schema, Table: table, Column: column, Field: field, Expected: expected, Actual: actual}
}
