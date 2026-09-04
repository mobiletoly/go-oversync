// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

type SchemaGenerationID int32

type GenerationWireType string

const (
	GenerationWireTypeBoolean GenerationWireType = "boolean"
	GenerationWireTypeInt64   GenerationWireType = "int64"
	GenerationWireTypeDecimal GenerationWireType = "decimal"
	GenerationWireTypeFloat64 GenerationWireType = "float64"
	GenerationWireTypeText    GenerationWireType = "text"
	GenerationWireTypeBytes   GenerationWireType = "bytes"
	GenerationWireTypeUUID    GenerationWireType = "uuid"
	GenerationWireTypeJSON    GenerationWireType = "json"
)

type GenerationForeignKeyAction string

const (
	GenerationForeignKeyNoAction GenerationForeignKeyAction = "no_action"
	GenerationForeignKeyRestrict GenerationForeignKeyAction = "restrict"
	GenerationForeignKeyCascade  GenerationForeignKeyAction = "cascade"
	GenerationForeignKeySetNull  GenerationForeignKeyAction = "set_null"
)

type SchemaGeneration struct {
	ID     SchemaGenerationID
	Tables []GenerationTable
}

type GenerationTable struct {
	Schema      string
	Table       string
	Columns     []GenerationColumn
	ForeignKeys []GenerationForeignKey
}

type GenerationColumn struct {
	Name     string
	WireType GenerationWireType
	Nullable bool
}

type GenerationForeignKey struct {
	Columns           []string
	ReferencedSchema  string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          GenerationForeignKeyAction
	OnUpdate          GenerationForeignKeyAction
}

type GenerationTableContract struct {
	Schema         string
	Table          string
	SyncKeyColumns []string
	Columns        []GenerationColumn
	ForeignKeys    []GenerationForeignKey
}

type SchemaGenerationContract struct {
	id          SchemaGenerationID
	fingerprint string
	tables      []GenerationTableContract
}

func (c SchemaGenerationContract) ID() SchemaGenerationID {
	return c.id
}

func (c SchemaGenerationContract) Fingerprint() string {
	return c.fingerprint
}

func (c SchemaGenerationContract) Tables() []GenerationTableContract {
	return cloneGenerationTableContracts(c.tables)
}

type SchemaGenerationRegistry struct {
	fingerprint   string
	generationIDs []SchemaGenerationID
	generations   map[SchemaGenerationID]SchemaGenerationContract
}

func (r *SchemaGenerationRegistry) Fingerprint() string {
	if r == nil {
		return ""
	}
	return r.fingerprint
}

func (r *SchemaGenerationRegistry) GenerationIDs() []SchemaGenerationID {
	if r == nil {
		return nil
	}
	return slices.Clone(r.generationIDs)
}

func (r *SchemaGenerationRegistry) Generation(id SchemaGenerationID) (SchemaGenerationContract, bool) {
	if r == nil {
		return SchemaGenerationContract{}, false
	}
	contract, ok := r.generations[id]
	if !ok {
		return SchemaGenerationContract{}, false
	}
	return cloneSchemaGenerationContract(contract), true
}

type SchemaGenerationConfigErrorCode string

const (
	SchemaGenerationEmptyRegisteredTables            SchemaGenerationConfigErrorCode = "empty_registered_tables"
	SchemaGenerationInvalidRegisteredTable           SchemaGenerationConfigErrorCode = "invalid_registered_table"
	SchemaGenerationDuplicateRegisteredTable         SchemaGenerationConfigErrorCode = "duplicate_registered_table"
	SchemaGenerationInvalidRegisteredSyncKeyCount    SchemaGenerationConfigErrorCode = "invalid_registered_sync_key_count"
	SchemaGenerationInvalidRegisteredSyncKey         SchemaGenerationConfigErrorCode = "invalid_registered_sync_key"
	SchemaGenerationHiddenRegisteredSyncKey          SchemaGenerationConfigErrorCode = "hidden_registered_sync_key"
	SchemaGenerationInvalidCount                     SchemaGenerationConfigErrorCode = "invalid_generation_count"
	SchemaGenerationInvalidID                        SchemaGenerationConfigErrorCode = "invalid_generation_id"
	SchemaGenerationDuplicateID                      SchemaGenerationConfigErrorCode = "duplicate_generation_id"
	SchemaGenerationEmptyTables                      SchemaGenerationConfigErrorCode = "empty_generation_tables"
	SchemaGenerationInvalidTable                     SchemaGenerationConfigErrorCode = "invalid_generation_table"
	SchemaGenerationDuplicateTable                   SchemaGenerationConfigErrorCode = "duplicate_generation_table"
	SchemaGenerationUnregisteredTable                SchemaGenerationConfigErrorCode = "unregistered_generation_table"
	SchemaGenerationEmptyColumns                     SchemaGenerationConfigErrorCode = "empty_generation_columns"
	SchemaGenerationInvalidColumn                    SchemaGenerationConfigErrorCode = "invalid_generation_column"
	SchemaGenerationHiddenColumn                     SchemaGenerationConfigErrorCode = "hidden_generation_column"
	SchemaGenerationDuplicateColumn                  SchemaGenerationConfigErrorCode = "duplicate_generation_column"
	SchemaGenerationInvalidWireType                  SchemaGenerationConfigErrorCode = "invalid_generation_wire_type"
	SchemaGenerationMissingSyncKey                   SchemaGenerationConfigErrorCode = "missing_generation_sync_key"
	SchemaGenerationInvalidSyncKeyContract           SchemaGenerationConfigErrorCode = "invalid_generation_sync_key_contract"
	SchemaGenerationInvalidForeignKeyShape           SchemaGenerationConfigErrorCode = "invalid_generation_foreign_key_shape"
	SchemaGenerationInvalidForeignKeyIdentifier      SchemaGenerationConfigErrorCode = "invalid_generation_foreign_key_identifier"
	SchemaGenerationInvalidForeignKeyAction          SchemaGenerationConfigErrorCode = "invalid_generation_foreign_key_action"
	SchemaGenerationDuplicateForeignKey              SchemaGenerationConfigErrorCode = "duplicate_generation_foreign_key"
	SchemaGenerationForeignKeyColumnNotVisible       SchemaGenerationConfigErrorCode = "generation_foreign_key_column_not_visible"
	SchemaGenerationForeignKeyTargetNotVisible       SchemaGenerationConfigErrorCode = "generation_foreign_key_target_not_visible"
	SchemaGenerationForeignKeyTargetColumnNotVisible SchemaGenerationConfigErrorCode = "generation_foreign_key_target_column_not_visible"
	SchemaGenerationForeignKeyTargetNotSyncKey       SchemaGenerationConfigErrorCode = "generation_foreign_key_target_not_sync_key"
	SchemaGenerationForeignKeyTypeMismatch           SchemaGenerationConfigErrorCode = "generation_foreign_key_type_mismatch"
	SchemaGenerationForeignKeySetNullNonNullable     SchemaGenerationConfigErrorCode = "generation_foreign_key_set_null_non_nullable"
)

type SchemaGenerationConfigError struct {
	Code             SchemaGenerationConfigErrorCode
	GenerationID     SchemaGenerationID
	Schema           string
	Table            string
	Column           string
	ReferencedSchema string
	ReferencedTable  string
	ReferencedColumn string
	Field            string
	Value            string
	ObservedCount    int
}

func (e *SchemaGenerationConfigError) Error() string {
	if e == nil {
		return "schema generation configuration is invalid"
	}
	return fmt.Sprintf(
		"schema generation configuration error: code=%s generation_id=%d schema=%q table=%q column=%q referenced_schema=%q referenced_table=%q referenced_column=%q field=%q value=%q observed_count=%d",
		e.Code,
		e.GenerationID,
		e.Schema,
		e.Table,
		e.Column,
		e.ReferencedSchema,
		e.ReferencedTable,
		e.ReferencedColumn,
		e.Field,
		e.Value,
		e.ObservedCount,
	)
}

func CompileSchemaGenerationRegistry(
	registeredTables []RegisteredTable,
	generations []SchemaGeneration,
) (*SchemaGenerationRegistry, error) {
	registered := cloneRegisteredTables(registeredTables)
	compiledGenerations := cloneSchemaGenerations(generations)
	sortSchemaGenerationInput(registered, compiledGenerations)

	if err := validateSchemaGenerationInput(registered, compiledGenerations); err != nil {
		return nil, err
	}

	registeredByTable := make(map[generationTableKey]RegisteredTable, len(registered))
	for _, table := range registered {
		registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}] = table
	}

	registry := &SchemaGenerationRegistry{
		generationIDs: make([]SchemaGenerationID, 0, len(compiledGenerations)),
		generations:   make(map[SchemaGenerationID]SchemaGenerationContract, len(compiledGenerations)),
	}
	for _, generation := range compiledGenerations {
		contract := SchemaGenerationContract{
			id:     generation.ID,
			tables: make([]GenerationTableContract, 0, len(generation.Tables)),
		}
		for _, table := range generation.Tables {
			registeredTable := registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}]
			contract.tables = append(contract.tables, GenerationTableContract{
				Schema:         table.Schema,
				Table:          table.Table,
				SyncKeyColumns: slices.Clone(registeredTable.SyncKeyColumns),
				Columns:        slices.Clone(table.Columns),
				ForeignKeys:    cloneGenerationForeignKeys(table.ForeignKeys),
			})
		}

		fingerprint, err := fingerprintSchemaGeneration(contract)
		if err != nil {
			return nil, err
		}
		contract.fingerprint = fingerprint
		registry.generationIDs = append(registry.generationIDs, contract.id)
		registry.generations[contract.id] = contract
	}

	canonical, err := schemaGenerationRegistryCanonicalBytes(registry)
	if err != nil {
		return nil, fmt.Errorf("fingerprint schema generation registry: %w", err)
	}
	registry.fingerprint = sha256Hex(canonical)
	return registry, nil
}

type generationTableKey struct {
	schema string
	table  string
}

// validateRegisteredTables owns registered-table rules 1 through 6. Callers sort a deep copy with
// compareRegisteredTables before invoking it so every public entry point has the same first error.
func validateRegisteredTables(registered []RegisteredTable) error {
	if len(registered) == 0 {
		return &SchemaGenerationConfigError{Code: SchemaGenerationEmptyRegisteredTables}
	}

	for _, table := range registered {
		if !isCanonicalGenerationIdentifier(table.Schema) {
			return &SchemaGenerationConfigError{
				Code:   SchemaGenerationInvalidRegisteredTable,
				Schema: table.Schema, Table: table.Table,
				Field: "Schema", Value: table.Schema,
			}
		}
	}
	for _, table := range registered {
		if !isCanonicalGenerationIdentifier(table.Table) {
			return &SchemaGenerationConfigError{
				Code:   SchemaGenerationInvalidRegisteredTable,
				Schema: table.Schema, Table: table.Table,
				Field: "Table", Value: table.Table,
			}
		}
	}
	for start := 0; start < len(registered); {
		end := start + 1
		for end < len(registered) && sameRegisteredTableIdentity(registered[start], registered[end]) {
			end++
		}
		if end-start > 1 {
			return &SchemaGenerationConfigError{
				Code:   SchemaGenerationDuplicateRegisteredTable,
				Schema: registered[start].Schema, Table: registered[start].Table,
				ObservedCount: end - start,
			}
		}
		start = end
	}
	for _, table := range registered {
		if len(table.SyncKeyColumns) != 1 {
			return &SchemaGenerationConfigError{
				Code:   SchemaGenerationInvalidRegisteredSyncKeyCount,
				Schema: table.Schema, Table: table.Table,
				Field: "SyncKeyColumns", ObservedCount: len(table.SyncKeyColumns),
			}
		}
	}
	for _, table := range registered {
		for _, column := range table.SyncKeyColumns {
			if !isCanonicalGenerationIdentifier(column) {
				return &SchemaGenerationConfigError{
					Code:   SchemaGenerationInvalidRegisteredSyncKey,
					Schema: table.Schema, Table: table.Table, Column: column,
					Field: "SyncKeyColumns", Value: column,
				}
			}
		}
	}
	for _, table := range registered {
		for _, column := range table.SyncKeyColumns {
			if column == syncScopeColumnName {
				return &SchemaGenerationConfigError{
					Code:   SchemaGenerationHiddenRegisteredSyncKey,
					Schema: table.Schema, Table: table.Table, Column: column,
				}
			}
		}
	}
	return nil
}

func validateSchemaGenerationInput(registered []RegisteredTable, generations []SchemaGeneration) error {
	if err := validateRegisteredTables(registered); err != nil {
		return err
	}
	if len(generations) == 0 || len(generations) > 2 {
		return &SchemaGenerationConfigError{
			Code:  SchemaGenerationInvalidCount,
			Field: "SchemaGenerations", ObservedCount: len(generations),
		}
	}
	for _, generation := range generations {
		if generation.ID < 1 {
			return &SchemaGenerationConfigError{
				Code: SchemaGenerationInvalidID, GenerationID: generation.ID,
				Field: "ID", Value: strconv.FormatInt(int64(generation.ID), 10),
			}
		}
	}
	for start := 0; start < len(generations); {
		end := start + 1
		for end < len(generations) && generations[end].ID == generations[start].ID {
			end++
		}
		if end-start > 1 {
			return &SchemaGenerationConfigError{
				Code:          SchemaGenerationDuplicateID,
				GenerationID:  generations[start].ID,
				ObservedCount: end - start,
			}
		}
		start = end
	}
	for _, generation := range generations {
		if len(generation.Tables) == 0 {
			return &SchemaGenerationConfigError{
				Code: SchemaGenerationEmptyTables, GenerationID: generation.ID,
				Field: "Tables", ObservedCount: 0,
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			if !isCanonicalGenerationIdentifier(table.Schema) {
				return generationTableFieldError(SchemaGenerationInvalidTable, generation.ID, table, "Schema", table.Schema)
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			if !isCanonicalGenerationIdentifier(table.Table) {
				return generationTableFieldError(SchemaGenerationInvalidTable, generation.ID, table, "Table", table.Table)
			}
		}
	}
	for _, generation := range generations {
		for start := 0; start < len(generation.Tables); {
			end := start + 1
			for end < len(generation.Tables) && sameGenerationTableIdentity(generation.Tables[start], generation.Tables[end]) {
				end++
			}
			if end-start > 1 {
				table := generation.Tables[start]
				return &SchemaGenerationConfigError{
					Code: SchemaGenerationDuplicateTable, GenerationID: generation.ID,
					Schema: table.Schema, Table: table.Table, ObservedCount: end - start,
				}
			}
			start = end
		}
	}

	registeredByTable := make(map[generationTableKey]RegisteredTable, len(registered))
	for _, table := range registered {
		registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}] = table
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			if _, ok := registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}]; !ok {
				return &SchemaGenerationConfigError{
					Code: SchemaGenerationUnregisteredTable, GenerationID: generation.ID,
					Schema: table.Schema, Table: table.Table,
				}
			}
		}
	}

	for _, generation := range generations {
		for _, table := range generation.Tables {
			if len(table.Columns) == 0 {
				return &SchemaGenerationConfigError{
					Code: SchemaGenerationEmptyColumns, GenerationID: generation.ID,
					Schema: table.Schema, Table: table.Table,
					Field: "Columns", ObservedCount: 0,
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, column := range table.Columns {
				if !isCanonicalGenerationIdentifier(column.Name) {
					return generationColumnFieldError(SchemaGenerationInvalidColumn, generation.ID, table, column, "Name", column.Name)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, column := range table.Columns {
				if column.Name == syncScopeColumnName {
					return &SchemaGenerationConfigError{
						Code: SchemaGenerationHiddenColumn, GenerationID: generation.ID,
						Schema: table.Schema, Table: table.Table, Column: column.Name,
						Field: "Name", Value: column.Name,
					}
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for start := 0; start < len(table.Columns); {
				end := start + 1
				for end < len(table.Columns) && table.Columns[end].Name == table.Columns[start].Name {
					end++
				}
				if end-start > 1 {
					return &SchemaGenerationConfigError{
						Code: SchemaGenerationDuplicateColumn, GenerationID: generation.ID,
						Schema: table.Schema, Table: table.Table, Column: table.Columns[start].Name,
						ObservedCount: end - start,
					}
				}
				start = end
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, column := range table.Columns {
				if !validGenerationWireType(column.WireType) {
					return generationColumnFieldError(
						SchemaGenerationInvalidWireType, generation.ID, table, column,
						"WireType", string(column.WireType),
					)
				}
			}
		}
	}

	for _, generation := range generations {
		for _, table := range generation.Tables {
			registeredTable := registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}]
			syncKey := registeredTable.SyncKeyColumns[0]
			if _, ok := findGenerationColumn(table, syncKey); !ok {
				return &SchemaGenerationConfigError{
					Code: SchemaGenerationMissingSyncKey, GenerationID: generation.ID,
					Schema: table.Schema, Table: table.Table, Column: syncKey,
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			registeredTable := registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}]
			syncKey := registeredTable.SyncKeyColumns[0]
			column, _ := findGenerationColumn(table, syncKey)
			if column.WireType != GenerationWireTypeUUID && column.WireType != GenerationWireTypeText {
				return generationColumnFieldError(
					SchemaGenerationInvalidSyncKeyContract, generation.ID, table, column,
					"WireType", string(column.WireType),
				)
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			registeredTable := registeredByTable[generationTableKey{schema: table.Schema, table: table.Table}]
			syncKey := registeredTable.SyncKeyColumns[0]
			column, _ := findGenerationColumn(table, syncKey)
			if column.Nullable {
				return generationColumnFieldError(
					SchemaGenerationInvalidSyncKeyContract, generation.ID, table, column,
					"Nullable", "true",
				)
			}
		}
	}

	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if len(foreignKey.Columns) != 1 {
					return generationForeignKeyShapeError(generation.ID, table, "Columns", len(foreignKey.Columns))
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if len(foreignKey.ReferencedColumns) != 1 {
					return generationForeignKeyShapeError(generation.ID, table, "ReferencedColumns", len(foreignKey.ReferencedColumns))
				}
			}
		}
	}

	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				for _, column := range foreignKey.Columns {
					if !isCanonicalGenerationIdentifier(column) || column == syncScopeColumnName {
						return generationForeignKeyFieldError(
							SchemaGenerationInvalidForeignKeyIdentifier, generation.ID, table, foreignKey,
							"Columns", column,
						)
					}
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if !isCanonicalGenerationIdentifier(foreignKey.ReferencedSchema) || foreignKey.ReferencedSchema == syncScopeColumnName {
					return generationForeignKeyFieldError(
						SchemaGenerationInvalidForeignKeyIdentifier, generation.ID, table, foreignKey,
						"ReferencedSchema", foreignKey.ReferencedSchema,
					)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if !isCanonicalGenerationIdentifier(foreignKey.ReferencedTable) || foreignKey.ReferencedTable == syncScopeColumnName {
					return generationForeignKeyFieldError(
						SchemaGenerationInvalidForeignKeyIdentifier, generation.ID, table, foreignKey,
						"ReferencedTable", foreignKey.ReferencedTable,
					)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				for _, column := range foreignKey.ReferencedColumns {
					if !isCanonicalGenerationIdentifier(column) || column == syncScopeColumnName {
						return generationForeignKeyFieldError(
							SchemaGenerationInvalidForeignKeyIdentifier, generation.ID, table, foreignKey,
							"ReferencedColumns", column,
						)
					}
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if !validGenerationForeignKeyAction(foreignKey.OnDelete) {
					return generationForeignKeyFieldError(
						SchemaGenerationInvalidForeignKeyAction, generation.ID, table, foreignKey,
						"OnDelete", string(foreignKey.OnDelete),
					)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if !validGenerationForeignKeyAction(foreignKey.OnUpdate) || foreignKey.OnUpdate == GenerationForeignKeySetNull {
					return generationForeignKeyFieldError(
						SchemaGenerationInvalidForeignKeyAction, generation.ID, table, foreignKey,
						"OnUpdate", string(foreignKey.OnUpdate),
					)
				}
			}
		}
	}

	for _, generation := range generations {
		for _, table := range generation.Tables {
			for start := 0; start < len(table.ForeignKeys); {
				end := start + 1
				for end < len(table.ForeignKeys) && sameGenerationForeignKeyEndpoint(table.ForeignKeys[start], table.ForeignKeys[end]) {
					end++
				}
				if end-start > 1 {
					err := generationForeignKeyError(SchemaGenerationDuplicateForeignKey, generation.ID, table, table.ForeignKeys[start])
					err.ObservedCount = end - start
					return err
				}
				start = end
			}
		}
	}

	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if _, ok := findGenerationColumn(table, foreignKey.Columns[0]); !ok {
					return &SchemaGenerationConfigError{
						Code: SchemaGenerationForeignKeyColumnNotVisible, GenerationID: generation.ID,
						Schema: table.Schema, Table: table.Table, Column: foreignKey.Columns[0],
					}
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				if _, ok := findGenerationTable(generation, foreignKey.ReferencedSchema, foreignKey.ReferencedTable); !ok {
					return &SchemaGenerationConfigError{
						Code: SchemaGenerationForeignKeyTargetNotVisible, GenerationID: generation.ID,
						Schema: table.Schema, Table: table.Table,
						ReferencedSchema: foreignKey.ReferencedSchema, ReferencedTable: foreignKey.ReferencedTable,
					}
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				referencedTable, _ := findGenerationTable(generation, foreignKey.ReferencedSchema, foreignKey.ReferencedTable)
				if _, ok := findGenerationColumn(referencedTable, foreignKey.ReferencedColumns[0]); !ok {
					return generationForeignKeyError(SchemaGenerationForeignKeyTargetColumnNotVisible, generation.ID, table, foreignKey)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				referencedRegistered := registeredByTable[generationTableKey{
					schema: foreignKey.ReferencedSchema,
					table:  foreignKey.ReferencedTable,
				}]
				if foreignKey.ReferencedColumns[0] != referencedRegistered.SyncKeyColumns[0] {
					return generationForeignKeyError(SchemaGenerationForeignKeyTargetNotSyncKey, generation.ID, table, foreignKey)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				childColumn, _ := findGenerationColumn(table, foreignKey.Columns[0])
				referencedTable, _ := findGenerationTable(generation, foreignKey.ReferencedSchema, foreignKey.ReferencedTable)
				referencedColumn, _ := findGenerationColumn(referencedTable, foreignKey.ReferencedColumns[0])
				if childColumn.WireType != referencedColumn.WireType {
					return generationForeignKeyError(SchemaGenerationForeignKeyTypeMismatch, generation.ID, table, foreignKey)
				}
			}
		}
	}
	for _, generation := range generations {
		for _, table := range generation.Tables {
			for _, foreignKey := range table.ForeignKeys {
				childColumn, _ := findGenerationColumn(table, foreignKey.Columns[0])
				if childColumn.Nullable {
					continue
				}
				if foreignKey.OnDelete == GenerationForeignKeySetNull {
					return &SchemaGenerationConfigError{
						Code: SchemaGenerationForeignKeySetNullNonNullable, GenerationID: generation.ID,
						Schema: table.Schema, Table: table.Table, Column: foreignKey.Columns[0],
						Field: "OnDelete", Value: string(foreignKey.OnDelete),
					}
				}
			}
		}
	}

	return nil
}

func generationTableFieldError(
	code SchemaGenerationConfigErrorCode,
	generationID SchemaGenerationID,
	table GenerationTable,
	field string,
	value string,
) *SchemaGenerationConfigError {
	return &SchemaGenerationConfigError{
		Code: code, GenerationID: generationID,
		Schema: table.Schema, Table: table.Table,
		Field: field, Value: value,
	}
}

func generationColumnFieldError(
	code SchemaGenerationConfigErrorCode,
	generationID SchemaGenerationID,
	table GenerationTable,
	column GenerationColumn,
	field string,
	value string,
) *SchemaGenerationConfigError {
	return &SchemaGenerationConfigError{
		Code: code, GenerationID: generationID,
		Schema: table.Schema, Table: table.Table, Column: column.Name,
		Field: field, Value: value,
	}
}

func generationForeignKeyShapeError(
	generationID SchemaGenerationID,
	table GenerationTable,
	field string,
	observedCount int,
) *SchemaGenerationConfigError {
	return &SchemaGenerationConfigError{
		Code: SchemaGenerationInvalidForeignKeyShape, GenerationID: generationID,
		Schema: table.Schema, Table: table.Table,
		Field: field, ObservedCount: observedCount,
	}
}

func generationForeignKeyFieldError(
	code SchemaGenerationConfigErrorCode,
	generationID SchemaGenerationID,
	table GenerationTable,
	foreignKey GenerationForeignKey,
	field string,
	value string,
) *SchemaGenerationConfigError {
	err := generationForeignKeyError(code, generationID, table, foreignKey)
	err.Field = field
	err.Value = value
	return err
}

func generationForeignKeyError(
	code SchemaGenerationConfigErrorCode,
	generationID SchemaGenerationID,
	table GenerationTable,
	foreignKey GenerationForeignKey,
) *SchemaGenerationConfigError {
	err := &SchemaGenerationConfigError{
		Code: code, GenerationID: generationID,
		Schema: table.Schema, Table: table.Table,
		ReferencedSchema: foreignKey.ReferencedSchema,
		ReferencedTable:  foreignKey.ReferencedTable,
	}
	if len(foreignKey.Columns) > 0 {
		err.Column = foreignKey.Columns[0]
	}
	if len(foreignKey.ReferencedColumns) > 0 {
		err.ReferencedColumn = foreignKey.ReferencedColumns[0]
	}
	return err
}

func isCanonicalGenerationIdentifier(value string) bool {
	if len(value) < 1 || len(value) > 63 {
		return false
	}
	for index := 0; index < len(value); index++ {
		char := value[index]
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

func validGenerationWireType(wireType GenerationWireType) bool {
	switch wireType {
	case GenerationWireTypeBoolean,
		GenerationWireTypeInt64,
		GenerationWireTypeDecimal,
		GenerationWireTypeFloat64,
		GenerationWireTypeText,
		GenerationWireTypeBytes,
		GenerationWireTypeUUID,
		GenerationWireTypeJSON:
		return true
	default:
		return false
	}
}

func validGenerationForeignKeyAction(action GenerationForeignKeyAction) bool {
	switch action {
	case GenerationForeignKeyNoAction,
		GenerationForeignKeyRestrict,
		GenerationForeignKeyCascade,
		GenerationForeignKeySetNull:
		return true
	default:
		return false
	}
}

func findGenerationTable(generation SchemaGeneration, schema string, table string) (GenerationTable, bool) {
	for _, candidate := range generation.Tables {
		if candidate.Schema == schema && candidate.Table == table {
			return candidate, true
		}
	}
	return GenerationTable{}, false
}

func findGenerationColumn(table GenerationTable, name string) (GenerationColumn, bool) {
	for _, column := range table.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return GenerationColumn{}, false
}

func sortSchemaGenerationInput(registered []RegisteredTable, generations []SchemaGeneration) {
	for generationIndex := range generations {
		for tableIndex := range generations[generationIndex].Tables {
			table := &generations[generationIndex].Tables[tableIndex]
			slices.SortFunc(table.Columns, compareGenerationColumns)
			slices.SortFunc(table.ForeignKeys, compareGenerationForeignKeys)
		}
		slices.SortFunc(generations[generationIndex].Tables, compareGenerationTables)
	}
	slices.SortFunc(generations, compareSchemaGenerations)
	slices.SortFunc(registered, compareRegisteredTables)
}

func compareStrings(left string, right string) int {
	return cmp.Compare(left, right)
}

func compareBooleans(left bool, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}

func compareStringSequences(left []string, right []string) int {
	return compareSequences(left, right, compareStrings)
}

func compareGenerationColumns(left GenerationColumn, right GenerationColumn) int {
	if order := compareStrings(left.Name, right.Name); order != 0 {
		return order
	}
	if order := compareStrings(string(left.WireType), string(right.WireType)); order != 0 {
		return order
	}
	return compareBooleans(left.Nullable, right.Nullable)
}

func compareGenerationColumnSequences(left []GenerationColumn, right []GenerationColumn) int {
	return compareSequences(left, right, compareGenerationColumns)
}

func compareGenerationForeignKeys(left GenerationForeignKey, right GenerationForeignKey) int {
	if order := compareStringSequences(left.Columns, right.Columns); order != 0 {
		return order
	}
	if order := compareStrings(left.ReferencedSchema, right.ReferencedSchema); order != 0 {
		return order
	}
	if order := compareStrings(left.ReferencedTable, right.ReferencedTable); order != 0 {
		return order
	}
	if order := compareStringSequences(left.ReferencedColumns, right.ReferencedColumns); order != 0 {
		return order
	}
	if order := compareStrings(string(left.OnDelete), string(right.OnDelete)); order != 0 {
		return order
	}
	return compareStrings(string(left.OnUpdate), string(right.OnUpdate))
}

func compareGenerationForeignKeySequences(left []GenerationForeignKey, right []GenerationForeignKey) int {
	return compareSequences(left, right, compareGenerationForeignKeys)
}

func compareGenerationTables(left GenerationTable, right GenerationTable) int {
	if order := compareStrings(left.Schema, right.Schema); order != 0 {
		return order
	}
	if order := compareStrings(left.Table, right.Table); order != 0 {
		return order
	}
	if order := compareGenerationColumnSequences(left.Columns, right.Columns); order != 0 {
		return order
	}
	return compareGenerationForeignKeySequences(left.ForeignKeys, right.ForeignKeys)
}

func compareGenerationTableSequences(left []GenerationTable, right []GenerationTable) int {
	return compareSequences(left, right, compareGenerationTables)
}

func compareSequences[T any](left []T, right []T, compareElements func(T, T) int) int {
	for index := 0; index < min(len(left), len(right)); index++ {
		if order := compareElements(left[index], right[index]); order != 0 {
			return order
		}
	}
	return cmp.Compare(len(left), len(right))
}

func compareSchemaGenerations(left SchemaGeneration, right SchemaGeneration) int {
	if order := cmp.Compare(left.ID, right.ID); order != 0 {
		return order
	}
	return compareGenerationTableSequences(left.Tables, right.Tables)
}

func compareRegisteredTables(left RegisteredTable, right RegisteredTable) int {
	if order := compareStrings(left.Schema, right.Schema); order != 0 {
		return order
	}
	if order := compareStrings(left.Table, right.Table); order != 0 {
		return order
	}
	return compareStringSequences(left.SyncKeyColumns, right.SyncKeyColumns)
}

func sameRegisteredTableIdentity(left RegisteredTable, right RegisteredTable) bool {
	return left.Schema == right.Schema && left.Table == right.Table
}

func sameGenerationTableIdentity(left GenerationTable, right GenerationTable) bool {
	return left.Schema == right.Schema && left.Table == right.Table
}

func sameGenerationForeignKeyEndpoint(left GenerationForeignKey, right GenerationForeignKey) bool {
	return compareStringSequences(left.Columns, right.Columns) == 0 &&
		left.ReferencedSchema == right.ReferencedSchema &&
		left.ReferencedTable == right.ReferencedTable &&
		compareStringSequences(left.ReferencedColumns, right.ReferencedColumns) == 0
}

func fingerprintSchemaGeneration(contract SchemaGenerationContract) (string, error) {
	canonical, err := schemaGenerationCanonicalBytes(contract)
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func schemaGenerationCanonicalBytes(contract SchemaGenerationContract) ([]byte, error) {
	tables := make([]any, 0, len(contract.tables))
	for _, table := range contract.tables {
		columns := make([]any, 0, len(table.Columns))
		for _, column := range table.Columns {
			columns = append(columns, []any{column.Name, string(column.WireType), column.Nullable})
		}
		foreignKeys := make([]any, 0, len(table.ForeignKeys))
		for _, foreignKey := range table.ForeignKeys {
			foreignKeys = append(foreignKeys, []any{
				foreignKey.Columns,
				foreignKey.ReferencedSchema,
				foreignKey.ReferencedTable,
				foreignKey.ReferencedColumns,
				string(foreignKey.OnDelete),
				string(foreignKey.OnUpdate),
			})
		}
		tables = append(tables, []any{
			table.Schema,
			table.Table,
			table.SyncKeyColumns,
			columns,
			foreignKeys,
		})
	}
	return jcs.Marshal([]any{
		"oversync.schema-generation.v1",
		strconv.FormatInt(int64(contract.id), 10),
		tables,
	})
}

func schemaGenerationRegistryCanonicalBytes(registry *SchemaGenerationRegistry) ([]byte, error) {
	entries := make([]any, 0, len(registry.generationIDs))
	for _, id := range registry.generationIDs {
		contract := registry.generations[id]
		entries = append(entries, []any{
			strconv.FormatInt(int64(contract.id), 10),
			contract.fingerprint,
		})
	}
	return jcs.Marshal([]any{
		"oversync.schema-generation-registry.v1",
		entries,
	})
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func cloneRegisteredTables(tables []RegisteredTable) []RegisteredTable {
	if tables == nil {
		return nil
	}
	cloned := make([]RegisteredTable, len(tables))
	for index, table := range tables {
		cloned[index] = table
		cloned[index].SyncKeyColumns = slices.Clone(table.SyncKeyColumns)
	}
	return cloned
}

func cloneSchemaGenerations(generations []SchemaGeneration) []SchemaGeneration {
	if generations == nil {
		return nil
	}
	cloned := make([]SchemaGeneration, len(generations))
	for generationIndex, generation := range generations {
		cloned[generationIndex].ID = generation.ID
		cloned[generationIndex].Tables = make([]GenerationTable, len(generation.Tables))
		for tableIndex, table := range generation.Tables {
			clonedTable := GenerationTable{
				Schema:      table.Schema,
				Table:       table.Table,
				Columns:     slices.Clone(table.Columns),
				ForeignKeys: cloneGenerationForeignKeys(table.ForeignKeys),
			}
			cloned[generationIndex].Tables[tableIndex] = clonedTable
		}
	}
	return cloned
}

func cloneGenerationForeignKeys(foreignKeys []GenerationForeignKey) []GenerationForeignKey {
	if foreignKeys == nil {
		return nil
	}
	cloned := make([]GenerationForeignKey, len(foreignKeys))
	for index, foreignKey := range foreignKeys {
		cloned[index] = foreignKey
		cloned[index].Columns = slices.Clone(foreignKey.Columns)
		cloned[index].ReferencedColumns = slices.Clone(foreignKey.ReferencedColumns)
	}
	return cloned
}

func cloneGenerationTableContracts(tables []GenerationTableContract) []GenerationTableContract {
	if tables == nil {
		return nil
	}
	cloned := make([]GenerationTableContract, len(tables))
	for index, table := range tables {
		cloned[index] = table
		cloned[index].SyncKeyColumns = slices.Clone(table.SyncKeyColumns)
		cloned[index].Columns = slices.Clone(table.Columns)
		cloned[index].ForeignKeys = cloneGenerationForeignKeys(table.ForeignKeys)
	}
	return cloned
}

func cloneSchemaGenerationContract(contract SchemaGenerationContract) SchemaGenerationContract {
	contract.tables = cloneGenerationTableContracts(contract.tables)
	return contract
}
