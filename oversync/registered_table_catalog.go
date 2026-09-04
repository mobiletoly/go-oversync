// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/jcs"
)

type RegisteredTableCatalogErrorCode string

const (
	RegisteredTableCatalogInvalidTargetRevision     RegisteredTableCatalogErrorCode = "invalid_target_revision"
	RegisteredTableCatalogLayoutReplacementRequired RegisteredTableCatalogErrorCode = "layout_replacement_required"
	RegisteredTableCatalogInvalidMetadata           RegisteredTableCatalogErrorCode = "invalid_metadata"
	RegisteredTableCatalogInvalidEntry              RegisteredTableCatalogErrorCode = "invalid_entry"
	RegisteredTableCatalogFingerprintMismatch       RegisteredTableCatalogErrorCode = "fingerprint_mismatch"
	RegisteredTableCatalogRevisionConflict          RegisteredTableCatalogErrorCode = "revision_conflict"
	RegisteredTableCatalogActiveTableOmitted        RegisteredTableCatalogErrorCode = "active_table_omitted"
	RegisteredTableCatalogRetiredTableConfigured    RegisteredTableCatalogErrorCode = "retired_table_configured"
	RegisteredTableCatalogIdentityMismatch          RegisteredTableCatalogErrorCode = "identity_mismatch"
	RegisteredTableCatalogEmptyAppend               RegisteredTableCatalogErrorCode = "empty_append"
	RegisteredTableCatalogAppendNotDrained          RegisteredTableCatalogErrorCode = "append_not_drained"
	RegisteredTableCatalogAppendTableNotEmpty       RegisteredTableCatalogErrorCode = "append_table_not_empty"
	RegisteredTableCatalogIDExhausted               RegisteredTableCatalogErrorCode = "table_id_exhausted"
)

type RegisteredTableCatalogError struct {
	Code            RegisteredTableCatalogErrorCode
	TableID         int32
	Schema          string
	Table           string
	Field           string
	Value           string
	Expected        string
	Actual          string
	CatalogRevision int64
	TargetRevision  int64
	NextTableID     int64
	ObservedCount   int64
}

func (e *RegisteredTableCatalogError) Error() string {
	if e == nil {
		return "registered table catalog is invalid"
	}
	return fmt.Sprintf(
		"registered table catalog error: code=%s table_id=%d schema=%q table=%q field=%q value=%q expected=%q actual=%q catalog_revision=%d target_revision=%d next_table_id=%d observed_count=%d",
		e.Code,
		e.TableID,
		e.Schema,
		e.Table,
		e.Field,
		e.Value,
		e.Expected,
		e.Actual,
		e.CatalogRevision,
		e.TargetRevision,
		e.NextTableID,
		e.ObservedCount,
	)
}

const (
	registeredTableCatalogStateActive  int16 = 1
	registeredTableCatalogStateRetired int16 = 2
	registeredTableCatalogMaxTableID   int64 = 2147483647
	registeredTableCatalogExhaustedID  int64 = registeredTableCatalogMaxTableID + 1
)

type registeredTableCatalogMetadata struct {
	catalogRevision    int64
	nextTableID        int64
	catalogFingerprint string
}

type registeredTableCatalogEntry struct {
	tableID            int32
	schemaName         string
	tableName          string
	syncKeyColumn      string
	syncKeyKind        int16
	stateCode          int16
	introducedRevision int64
	retiredRevision    *int64
}

func (e registeredTableCatalogEntry) key() string {
	return Key(e.schemaName, e.tableName)
}

func catalogKindToken(kind int16) (string, bool) {
	switch kind {
	case syncKeyKindUUIDCode:
		return syncKeyTypeUUID, true
	case syncKeyKindTextCode:
		return syncKeyTypeText, true
	default:
		return "", false
	}
}

func catalogStateToken(state int16) (string, bool) {
	switch state {
	case registeredTableCatalogStateActive:
		return "active", true
	case registeredTableCatalogStateRetired:
		return "retired", true
	default:
		return "", false
	}
}

func registeredTableCatalogCanonicalBytes(
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
) ([]byte, error) {
	ordered := append([]registeredTableCatalogEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].tableID < ordered[j].tableID })
	if err := validateRegisteredTableCatalogEntries(metadata, ordered); err != nil {
		return nil, err
	}
	encodedEntries := make([]any, 0, len(ordered))
	for _, entry := range ordered {
		kind, _ := catalogKindToken(entry.syncKeyKind)
		state, _ := catalogStateToken(entry.stateCode)
		var retired any
		if entry.retiredRevision != nil {
			retired = strconv.FormatInt(*entry.retiredRevision, 10)
		}
		encodedEntries = append(encodedEntries, []any{
			strconv.FormatInt(int64(entry.tableID), 10),
			entry.schemaName,
			entry.tableName,
			entry.syncKeyColumn,
			kind,
			state,
			strconv.FormatInt(entry.introducedRevision, 10),
			retired,
		})
	}
	return jcs.Marshal([]any{
		"oversync.registered-table-catalog.v1",
		strconv.FormatInt(metadata.catalogRevision, 10),
		strconv.FormatInt(metadata.nextTableID, 10),
		encodedEntries,
	})
}

func registeredTableCatalogFingerprint(
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
) (string, error) {
	canonical, err := registeredTableCatalogCanonicalBytes(metadata, entries)
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func validateRegisteredTableCatalogEntries(
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
) error {
	if metadata.catalogRevision < 1 {
		return &RegisteredTableCatalogError{
			Code:     RegisteredTableCatalogInvalidMetadata,
			Field:    "catalog_revision",
			Expected: ">=1",
			Actual:   strconv.FormatInt(metadata.catalogRevision, 10),
		}
	}
	seenNames := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		expectedID := int64(index + 1)
		if int64(entry.tableID) != expectedID {
			return &RegisteredTableCatalogError{
				Code:     RegisteredTableCatalogInvalidEntry,
				TableID:  entry.tableID,
				Schema:   entry.schemaName,
				Table:    entry.tableName,
				Field:    "table_id",
				Expected: strconv.FormatInt(expectedID, 10),
				Actual:   strconv.FormatInt(int64(entry.tableID), 10),
			}
		}
		if !isCanonicalGenerationIdentifier(entry.schemaName) {
			return catalogEntryFieldError(entry, "schema_name", entry.schemaName)
		}
		if !isCanonicalGenerationIdentifier(entry.tableName) {
			return catalogEntryFieldError(entry, "table_name", entry.tableName)
		}
		if !isCanonicalGenerationIdentifier(entry.syncKeyColumn) || entry.syncKeyColumn == syncScopeColumnName {
			return catalogEntryFieldError(entry, "sync_key_column", entry.syncKeyColumn)
		}
		if _, ok := catalogKindToken(entry.syncKeyKind); !ok {
			return catalogEntryFieldError(entry, "sync_key_kind", strconv.FormatInt(int64(entry.syncKeyKind), 10))
		}
		if _, ok := catalogStateToken(entry.stateCode); !ok {
			return catalogEntryFieldError(entry, "state_code", strconv.FormatInt(int64(entry.stateCode), 10))
		}
		if entry.introducedRevision < 1 || entry.introducedRevision > metadata.catalogRevision {
			return catalogEntryFieldError(entry, "introduced_revision", strconv.FormatInt(entry.introducedRevision, 10))
		}
		switch entry.stateCode {
		case registeredTableCatalogStateActive:
			if entry.retiredRevision != nil {
				return catalogEntryFieldError(entry, "retired_revision", strconv.FormatInt(*entry.retiredRevision, 10))
			}
		case registeredTableCatalogStateRetired:
			if entry.retiredRevision == nil || *entry.retiredRevision <= entry.introducedRevision || *entry.retiredRevision > metadata.catalogRevision {
				value := "<null>"
				if entry.retiredRevision != nil {
					value = strconv.FormatInt(*entry.retiredRevision, 10)
				}
				return catalogEntryFieldError(entry, "retired_revision", value)
			}
		}
		if _, exists := seenNames[entry.key()]; exists {
			return &RegisteredTableCatalogError{
				Code:          RegisteredTableCatalogInvalidEntry,
				TableID:       entry.tableID,
				Schema:        entry.schemaName,
				Table:         entry.tableName,
				Field:         "schema_table",
				ObservedCount: 2,
			}
		}
		seenNames[entry.key()] = struct{}{}
	}
	expectedNext := int64(len(entries)) + 1
	if expectedNext > registeredTableCatalogExhaustedID {
		expectedNext = registeredTableCatalogExhaustedID
	}
	if metadata.nextTableID > expectedNext {
		return &RegisteredTableCatalogError{
			Code:     RegisteredTableCatalogInvalidEntry,
			Field:    "table_id",
			Expected: strconv.FormatInt(expectedNext, 10),
			Actual:   "end",
		}
	}
	if metadata.nextTableID < expectedNext {
		return &RegisteredTableCatalogError{
			Code:            RegisteredTableCatalogInvalidMetadata,
			Field:           "next_table_id",
			Expected:        strconv.FormatInt(expectedNext, 10),
			Actual:          strconv.FormatInt(metadata.nextTableID, 10),
			CatalogRevision: metadata.catalogRevision,
			NextTableID:     metadata.nextTableID,
		}
	}
	return nil
}

func catalogEntryFieldError(entry registeredTableCatalogEntry, field, value string) error {
	return &RegisteredTableCatalogError{
		Code:    RegisteredTableCatalogInvalidEntry,
		TableID: entry.tableID,
		Schema:  entry.schemaName,
		Table:   entry.tableName,
		Field:   field,
		Value:   value,
	}
}

func validCatalogFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == fmt.Sprintf("%x", decoded)
}

type registeredTableCatalogPlanMode int

const (
	registeredTableCatalogPlanAttach registeredTableCatalogPlanMode = iota + 1
	registeredTableCatalogPlanAppend
)

type registeredTableCatalogPlan struct {
	mode     registeredTableCatalogPlanMode
	metadata registeredTableCatalogMetadata
	entries  []registeredTableCatalogEntry
	appended []registeredTableCatalogEntry
}

type registeredTableCatalogTestHooks struct {
	inspectTransaction func(context.Context, pgx.Tx) error
	afterMutation      func(phase string, index int) error
}

func (s *SyncService) runRegisteredTableCatalogMutationHook(phase string, index int) error {
	if s == nil || s.catalogHooks == nil || s.catalogHooks.afterMutation == nil {
		return nil
	}
	return s.catalogHooks.afterMutation(phase, index)
}

func planExistingRegisteredTableCatalog(
	configured []RegisteredTable,
	targetRevision int64,
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
	physicalByName map[string]registeredTableRuntimeInfo,
) (registeredTableCatalogPlan, error) {
	if err := validateLoadedRegisteredTableCatalog(metadata, entries); err != nil {
		return registeredTableCatalogPlan{}, err
	}
	candidates, err := reconcileRegisteredTableCatalog(configured, targetRevision, metadata, entries, physicalByName)
	if err != nil {
		return registeredTableCatalogPlan{}, err
	}
	mode, err := registeredTableCatalogPlanModeForTarget(metadata, targetRevision, len(candidates))
	if err != nil {
		return registeredTableCatalogPlan{}, err
	}
	if mode == registeredTableCatalogPlanAttach {
		return registeredTableCatalogPlan{
			mode: registeredTableCatalogPlanAttach, metadata: metadata,
			entries: append([]registeredTableCatalogEntry(nil), entries...),
		}, nil
	}

	sort.Slice(candidates, func(i, j int) bool { return compareRegisteredTables(candidates[i], candidates[j]) < 0 })
	return allocateRegisteredTableCatalogAppend(metadata, entries, candidates, physicalByName, targetRevision)
}

func registeredTableCatalogPlanModeForTarget(
	metadata registeredTableCatalogMetadata,
	targetRevision int64,
	candidateCount int,
) (registeredTableCatalogPlanMode, error) {
	switch {
	case targetRevision == metadata.catalogRevision:
		if candidateCount != 0 {
			return 0, catalogRevisionConflict(metadata.catalogRevision, targetRevision, metadata.catalogRevision+1)
		}
		return registeredTableCatalogPlanAttach, nil
	case targetRevision == metadata.catalogRevision+1:
		if candidateCount == 0 {
			return 0, &RegisteredTableCatalogError{
				Code:            RegisteredTableCatalogEmptyAppend,
				CatalogRevision: metadata.catalogRevision, TargetRevision: targetRevision,
			}
		}
		return registeredTableCatalogPlanAppend, nil
	default:
		return 0, catalogRevisionConflict(metadata.catalogRevision, targetRevision, metadata.catalogRevision+1)
	}
}

func allocateRegisteredTableCatalogAppend(
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
	candidates []RegisteredTable,
	physicalByName map[string]registeredTableRuntimeInfo,
	targetRevision int64,
) (registeredTableCatalogPlan, error) {
	nextID := metadata.nextTableID
	updated := append([]registeredTableCatalogEntry(nil), entries...)
	appended := make([]registeredTableCatalogEntry, 0, len(candidates))
	for _, table := range candidates {
		if nextID > registeredTableCatalogMaxTableID {
			return registeredTableCatalogPlan{}, &RegisteredTableCatalogError{
				Code: RegisteredTableCatalogIDExhausted, Schema: table.Schema, Table: table.Table,
				NextTableID: nextID,
			}
		}
		physical, ok := physicalByName[Key(table.Schema, table.Table)]
		if !ok {
			return registeredTableCatalogPlan{}, fmt.Errorf("catalog append candidate %s has no physical metadata", Key(table.Schema, table.Table))
		}
		entry := registeredTableCatalogEntry{
			tableID: int32(nextID), schemaName: table.Schema, tableName: table.Table,
			syncKeyColumn: table.SyncKeyColumns[0], syncKeyKind: physical.syncKeyKind,
			stateCode: registeredTableCatalogStateActive, introducedRevision: targetRevision,
		}
		updated = append(updated, entry)
		appended = append(appended, entry)
		nextID++
	}
	updatedMetadata := registeredTableCatalogMetadata{catalogRevision: targetRevision, nextTableID: nextID}
	fingerprint, err := registeredTableCatalogFingerprint(updatedMetadata, updated)
	if err != nil {
		return registeredTableCatalogPlan{}, err
	}
	updatedMetadata.catalogFingerprint = fingerprint
	return registeredTableCatalogPlan{
		mode: registeredTableCatalogPlanAppend, metadata: updatedMetadata,
		entries: updated, appended: appended,
	}, nil
}

func validateLoadedRegisteredTableCatalog(metadata registeredTableCatalogMetadata, entries []registeredTableCatalogEntry) error {
	if err := validateRegisteredTableCatalogEntries(metadata, entries); err != nil {
		return err
	}
	expectedFingerprint, err := registeredTableCatalogFingerprint(metadata, entries)
	if err != nil {
		return err
	}
	if metadata.catalogFingerprint != expectedFingerprint {
		return &RegisteredTableCatalogError{
			Code:            RegisteredTableCatalogFingerprintMismatch,
			Expected:        expectedFingerprint,
			Actual:          metadata.catalogFingerprint,
			CatalogRevision: metadata.catalogRevision,
		}
	}
	return nil
}

func reconcileRegisteredTableCatalog(
	configured []RegisteredTable,
	targetRevision int64,
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
	physicalByName map[string]registeredTableRuntimeInfo,
) ([]RegisteredTable, error) {
	configured = cloneRegisteredTables(configured)
	sort.Slice(configured, func(i, j int) bool { return compareRegisteredTables(configured[i], configured[j]) < 0 })
	entriesByName := append([]registeredTableCatalogEntry(nil), entries...)
	sort.Slice(entriesByName, func(i, j int) bool {
		if entriesByName[i].schemaName != entriesByName[j].schemaName {
			return entriesByName[i].schemaName < entriesByName[j].schemaName
		}
		return entriesByName[i].tableName < entriesByName[j].tableName
	})
	byName := make(map[string]registeredTableCatalogEntry, len(entries))
	activeSeen := make(map[string]bool, len(entries))
	for _, entry := range entriesByName {
		byName[entry.key()] = entry
	}
	candidates := make([]RegisteredTable, 0)
	// Reconciliation is phase-separated so one table's later defect cannot mask an
	// earlier subphase defect on another table.
	for _, table := range configured {
		key := Key(table.Schema, table.Table)
		entry, exists := byName[key]
		if exists && entry.stateCode == registeredTableCatalogStateRetired {
			retiredRevision := int64(0)
			if entry.retiredRevision != nil {
				retiredRevision = *entry.retiredRevision
			}
			return nil, &RegisteredTableCatalogError{
				Code: RegisteredTableCatalogRetiredTableConfigured, TableID: entry.tableID,
				Schema: entry.schemaName, Table: entry.tableName, CatalogRevision: retiredRevision,
			}
		}
	}
	for _, table := range configured {
		key := Key(table.Schema, table.Table)
		entry, exists := byName[key]
		if !exists || entry.stateCode != registeredTableCatalogStateActive {
			continue
		}
		if entry.syncKeyColumn != table.SyncKeyColumns[0] {
			return nil, &RegisteredTableCatalogError{
				Code: RegisteredTableCatalogIdentityMismatch, TableID: entry.tableID,
				Schema: entry.schemaName, Table: entry.tableName, Field: "sync_key_column",
				Expected: entry.syncKeyColumn, Actual: table.SyncKeyColumns[0],
				CatalogRevision: metadata.catalogRevision, TargetRevision: targetRevision,
			}
		}
		activeSeen[key] = true
	}
	for _, table := range configured {
		key := Key(table.Schema, table.Table)
		entry, exists := byName[key]
		if !exists || entry.stateCode != registeredTableCatalogStateActive || physicalByName == nil {
			continue
		}
		physical, physicalExists := physicalByName[key]
		if physicalExists && physical.syncKeyKind != entry.syncKeyKind {
			return nil, &RegisteredTableCatalogError{
				Code: RegisteredTableCatalogIdentityMismatch, TableID: entry.tableID,
				Schema: entry.schemaName, Table: entry.tableName, Field: "sync_key_kind",
				Expected:        strconv.FormatInt(int64(entry.syncKeyKind), 10),
				Actual:          strconv.FormatInt(int64(physical.syncKeyKind), 10),
				CatalogRevision: metadata.catalogRevision, TargetRevision: targetRevision,
			}
		}
	}
	for _, entry := range entriesByName {
		if entry.stateCode == registeredTableCatalogStateActive && !activeSeen[entry.key()] {
			return nil, &RegisteredTableCatalogError{
				Code: RegisteredTableCatalogActiveTableOmitted, TableID: entry.tableID,
				Schema: entry.schemaName, Table: entry.tableName,
				CatalogRevision: metadata.catalogRevision, TargetRevision: targetRevision,
			}
		}
	}
	for _, table := range configured {
		key := Key(table.Schema, table.Table)
		_, exists := byName[key]
		if !exists {
			candidates = append(candidates, table)
			continue
		}
	}

	return candidates, nil
}

// loadConfiguredCatalogIdentityKinds reads only the physical sync-key kind needed by the
// reconciliation identity phase. Missing tables, missing/unsupported columns, and every other
// physical defect are deliberately left for the later active/candidate physical phases.
func loadConfiguredCatalogIdentityKinds(
	ctx context.Context,
	q syncCatalogQuerier,
	configured []RegisteredTable,
	entries []registeredTableCatalogEntry,
) (map[string]registeredTableRuntimeInfo, error) {
	byName := make(map[string]registeredTableCatalogEntry, len(entries))
	for _, entry := range entries {
		byName[entry.key()] = entry
	}
	var schemas, tables, columns []string
	for _, table := range configured {
		entry, exists := byName[Key(table.Schema, table.Table)]
		if !exists || entry.stateCode != registeredTableCatalogStateActive {
			continue
		}
		schemas = append(schemas, table.Schema)
		tables = append(tables, table.Table)
		columns = append(columns, table.SyncKeyColumns[0])
	}
	kinds := make(map[string]registeredTableRuntimeInfo, len(schemas))
	if len(schemas) == 0 {
		return kinds, nil
	}
	rows, err := q.Query(ctx, `
		WITH configured AS (
			SELECT *
			FROM unnest(@schemas::text[], @tables::text[], @columns::text[])
				AS item(schema_name, table_name, column_name)
		)
		SELECT configured.schema_name, configured.table_name,
		       type_namespace.nspname, type.typname
		FROM configured
		JOIN pg_namespace AS table_namespace
		  ON table_namespace.nspname=configured.schema_name
		JOIN pg_class AS relation
		  ON relation.relnamespace=table_namespace.oid
		 AND relation.relname=configured.table_name
		JOIN pg_attribute AS attribute
		  ON attribute.attrelid=relation.oid
		 AND attribute.attname=configured.column_name
		 AND attribute.attnum > 0
		 AND NOT attribute.attisdropped
		JOIN pg_type AS type ON type.oid=attribute.atttypid
		JOIN pg_namespace AS type_namespace ON type_namespace.oid=type.typnamespace
		ORDER BY configured.schema_name, configured.table_name
	`, pgx.NamedArgs{"schemas": schemas, "tables": tables, "columns": columns})
	if err != nil {
		return nil, fmt.Errorf("load configured catalog identity kinds: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schemaName, tableName, typeSchema, typeName string
		if err := rows.Scan(&schemaName, &tableName, &typeSchema, &typeName); err != nil {
			return nil, fmt.Errorf("scan configured catalog identity kind: %w", err)
		}
		if typeSchema != "pg_catalog" || (typeName != syncKeyTypeUUID && typeName != syncKeyTypeText) {
			continue
		}
		kind, err := syncKeyKindCode(typeName)
		if err != nil {
			return nil, err
		}
		kinds[Key(schemaName, tableName)] = registeredTableRuntimeInfo{syncKeyKind: kind}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate configured catalog identity kinds: %w", err)
	}
	return kinds, nil
}

func catalogRevisionConflict(catalogRevision, targetRevision, expected int64) error {
	return &RegisteredTableCatalogError{
		Code:            RegisteredTableCatalogRevisionConflict,
		CatalogRevision: catalogRevision,
		TargetRevision:  targetRevision,
		Expected:        strconv.FormatInt(expected, 10),
		Actual:          strconv.FormatInt(targetRevision, 10),
	}
}

func seedRegisteredTableCatalog(
	configured []RegisteredTable,
	physicalByName map[string]registeredTableRuntimeInfo,
) (registeredTableCatalogMetadata, []registeredTableCatalogEntry, error) {
	ordered := cloneRegisteredTables(configured)
	sort.Slice(ordered, func(i, j int) bool { return compareRegisteredTables(ordered[i], ordered[j]) < 0 })
	entries := make([]registeredTableCatalogEntry, 0, len(ordered))
	for index, table := range ordered {
		physical := physicalByName[Key(table.Schema, table.Table)]
		entries = append(entries, registeredTableCatalogEntry{
			tableID: int32(index + 1), schemaName: table.Schema, tableName: table.Table,
			syncKeyColumn: table.SyncKeyColumns[0], syncKeyKind: physical.syncKeyKind,
			stateCode: registeredTableCatalogStateActive, introducedRevision: 1,
		})
	}
	metadata := registeredTableCatalogMetadata{catalogRevision: 1, nextTableID: int64(len(entries)) + 1}
	fingerprint, err := registeredTableCatalogFingerprint(metadata, entries)
	if err != nil {
		return registeredTableCatalogMetadata{}, nil, err
	}
	metadata.catalogFingerprint = fingerprint
	return metadata, entries, nil
}

func classifyRegisteredTableCatalogLayout(ctx context.Context, q syncCatalogQuerier) (bool, error) {
	var metaExists bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('sync.meta') IS NOT NULL`).Scan(&metaExists); err != nil {
		return false, fmt.Errorf("inspect sync.meta marker relation: %w", err)
	}
	if !metaExists {
		exists, err := syncSchemaObjectsExist(ctx, q)
		if err != nil {
			return false, err
		}
		if !exists {
			return true, nil
		}
		return false, layoutReplacementRequired("<missing>")
	}

	var scalarShape bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_attribute
			WHERE attrelid = 'sync.meta'::regclass
			  AND attname = 'layout_name'
			  AND attnum > 0
			  AND NOT attisdropped
			  AND atttypid = 'text'::regtype
			  AND attndims = 0
		)
	`).Scan(&scalarShape); err != nil {
		return false, fmt.Errorf("inspect sync.meta layout_name shape: %w", err)
	}
	if !scalarShape {
		return false, layoutReplacementRequired("<invalid-shape>")
	}

	rows, err := q.Query(ctx, `SELECT layout_name FROM sync.meta ORDER BY ctid`)
	if err != nil {
		return false, fmt.Errorf("load sync.meta layout marker: %w", err)
	}
	defer rows.Close()
	total := int64(0)
	markers := make([]string, 0, 1)
	targetFound := false
	for rows.Next() {
		total++
		var marker *string
		if err := rows.Scan(&marker); err != nil {
			return false, fmt.Errorf("scan sync.meta layout marker: %w", err)
		}
		if marker == nil {
			continue
		}
		markers = append(markers, *marker)
		if *marker == syncSchemaLayoutName {
			targetFound = true
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate sync.meta layout markers: %w", err)
	}
	if !targetFound {
		switch len(markers) {
		case 0:
			return false, layoutReplacementRequired("<missing>")
		case 1:
			return false, layoutReplacementRequired(markers[0])
		default:
			return false, layoutReplacementRequired("<ambiguous>")
		}
	}
	if total != 1 {
		return false, &RegisteredTableCatalogError{
			Code: RegisteredTableCatalogInvalidMetadata, Field: "row_count",
			Expected: "1", Actual: strconv.FormatInt(total, 10),
		}
	}
	return false, nil
}

func layoutReplacementRequired(actual string) error {
	return &RegisteredTableCatalogError{
		Code:     RegisteredTableCatalogLayoutReplacementRequired,
		Expected: syncSchemaLayoutName,
		Actual:   actual,
	}
}

func loadRegisteredTableCatalog(
	ctx context.Context,
	q syncCatalogQuerier,
) (registeredTableCatalogMetadata, []registeredTableCatalogEntry, error) {
	return readRegisteredTableCatalog(ctx, q, true)
}

// Bootstrap already holds the exclusive sync bootstrap advisory lock. Exact
// attachment only reads catalog state and must work with SELECT-only credentials.
// A revision change retains the provisioning metadata row lock before append.
func loadRegisteredTableCatalogForTarget(ctx context.Context, q syncCatalogQuerier, target int64) (registeredTableCatalogMetadata, []registeredTableCatalogEntry, error) {
	var sameRevision bool
	if err := q.QueryRow(ctx, `SELECT count(*)=1 AND COALESCE(bool_and(catalog_revision=$1),false) FROM sync.meta`, target).Scan(&sameRevision); err != nil {
		return registeredTableCatalogMetadata{}, nil, fmt.Errorf("inspect sync.meta target revision: %w", err)
	}
	return readRegisteredTableCatalog(ctx, q, !sameRevision)
}

func readRegisteredTableCatalog(ctx context.Context, q syncCatalogQuerier, lockMetadata bool) (registeredTableCatalogMetadata, []registeredTableCatalogEntry, error) {
	query := `
		SELECT singleton_key, protocol_label, layout_name, catalog_revision, next_table_id,
		       catalog_fingerprint
		FROM sync.meta
		ORDER BY singleton_key DESC
	`
	if lockMetadata {
		query += " FOR UPDATE"
	}
	rows, err := q.Query(ctx, query)
	if err != nil {
		return registeredTableCatalogMetadata{}, nil, fmt.Errorf("load sync.meta catalog metadata: %w", err)
	}
	defer rows.Close()
	type metaRow struct {
		singletonKey       *bool
		protocolLabel      *string
		layoutName         *string
		catalogRevision    *int64
		nextTableID        *int64
		catalogFingerprint *string
	}
	metaRows := make([]metaRow, 0, 1)
	for rows.Next() {
		var row metaRow
		if err := rows.Scan(
			&row.singletonKey,
			&row.protocolLabel,
			&row.layoutName,
			&row.catalogRevision,
			&row.nextTableID,
			&row.catalogFingerprint,
		); err != nil {
			return registeredTableCatalogMetadata{}, nil, fmt.Errorf("scan sync.meta catalog metadata: %w", err)
		}
		metaRows = append(metaRows, row)
	}
	if err := rows.Err(); err != nil {
		return registeredTableCatalogMetadata{}, nil, fmt.Errorf("iterate sync.meta catalog metadata: %w", err)
	}
	if len(metaRows) != 1 {
		return registeredTableCatalogMetadata{}, nil, &RegisteredTableCatalogError{
			Code: RegisteredTableCatalogInvalidMetadata, Field: "row_count",
			Expected: "1", Actual: strconv.Itoa(len(metaRows)),
		}
	}
	row := metaRows[0]
	readableRevision := catalogReadableRevision(row.catalogRevision)
	if row.singletonKey == nil || !*row.singletonKey {
		return registeredTableCatalogMetadata{}, nil, invalidCatalogMetadata("singleton_key", "true", catalogOptionalBool(row.singletonKey), readableRevision)
	}
	if row.protocolLabel == nil || *row.protocolLabel != syncSchemaProtocolLabel {
		return registeredTableCatalogMetadata{}, nil, invalidCatalogMetadata("protocol_label", syncSchemaProtocolLabel, catalogOptionalString(row.protocolLabel), readableRevision)
	}
	if row.catalogRevision == nil || *row.catalogRevision < 1 {
		return registeredTableCatalogMetadata{}, nil, invalidCatalogMetadata("catalog_revision", ">=1", catalogOptionalInt64(row.catalogRevision), readableRevision)
	}
	if row.nextTableID == nil || *row.nextTableID < 1 || *row.nextTableID > registeredTableCatalogExhaustedID {
		return registeredTableCatalogMetadata{}, nil, invalidCatalogMetadata("next_table_id", "1..2147483648", catalogOptionalInt64(row.nextTableID), *row.catalogRevision)
	}
	if row.catalogFingerprint == nil || !validCatalogFingerprint(*row.catalogFingerprint) {
		return registeredTableCatalogMetadata{}, nil, invalidCatalogMetadata("catalog_fingerprint", "64 lowercase hexadecimal characters", catalogOptionalString(row.catalogFingerprint), *row.catalogRevision)
	}
	metadata := registeredTableCatalogMetadata{
		catalogRevision:    *row.catalogRevision,
		nextTableID:        *row.nextTableID,
		catalogFingerprint: *row.catalogFingerprint,
	}
	catalogRows, err := q.Query(ctx, `
		SELECT table_id, schema_name, table_name, sync_key_column, sync_key_kind,
		       state_code, introduced_revision, retired_revision
		FROM sync.table_catalog
		ORDER BY table_id
	`)
	if err != nil {
		return registeredTableCatalogMetadata{}, nil, fmt.Errorf("load sync.table_catalog: %w", err)
	}
	defer catalogRows.Close()
	entries := make([]registeredTableCatalogEntry, 0)
	for catalogRows.Next() {
		var (
			tableID            *int32
			schemaName         *string
			tableName          *string
			syncKeyColumn      *string
			syncKeyKind        *int16
			stateCode          *int16
			introducedRevision *int64
			retiredRevision    *int64
		)
		if err := catalogRows.Scan(
			&tableID,
			&schemaName,
			&tableName,
			&syncKeyColumn,
			&syncKeyKind,
			&stateCode,
			&introducedRevision,
			&retiredRevision,
		); err != nil {
			return registeredTableCatalogMetadata{}, nil, fmt.Errorf("scan sync.table_catalog row: %w", err)
		}
		if tableID == nil || schemaName == nil || tableName == nil || syncKeyColumn == nil ||
			syncKeyKind == nil || stateCode == nil || introducedRevision == nil {
			field := "table_id"
			switch {
			case tableID == nil:
			case schemaName == nil:
				field = "schema_name"
			case tableName == nil:
				field = "table_name"
			case syncKeyColumn == nil:
				field = "sync_key_column"
			case syncKeyKind == nil:
				field = "sync_key_kind"
			case stateCode == nil:
				field = "state_code"
			case introducedRevision == nil:
				field = "introduced_revision"
			}
			catalogErr := &RegisteredTableCatalogError{Code: RegisteredTableCatalogInvalidEntry, Field: field, Value: "<null>"}
			if tableID != nil {
				catalogErr.TableID = *tableID
			}
			if schemaName != nil {
				catalogErr.Schema = *schemaName
			}
			if tableName != nil {
				catalogErr.Table = *tableName
			}
			return registeredTableCatalogMetadata{}, nil, catalogErr
		}
		entry := registeredTableCatalogEntry{
			tableID: *tableID, schemaName: *schemaName, tableName: *tableName,
			syncKeyColumn: *syncKeyColumn, syncKeyKind: *syncKeyKind,
			stateCode: *stateCode, introducedRevision: *introducedRevision,
			retiredRevision: retiredRevision,
		}
		entries = append(entries, entry)
	}
	if err := catalogRows.Err(); err != nil {
		return registeredTableCatalogMetadata{}, nil, fmt.Errorf("iterate sync.table_catalog: %w", err)
	}
	return metadata, entries, nil
}

func invalidCatalogMetadata(field, expected, actual string, revision int64) error {
	return &RegisteredTableCatalogError{
		Code: RegisteredTableCatalogInvalidMetadata, Field: field,
		Expected: expected, Actual: actual, CatalogRevision: revision,
	}
}

func catalogOptionalString(value *string) string {
	if value == nil {
		return "<null>"
	}
	return *value
}

func catalogOptionalInt64(value *int64) string {
	if value == nil {
		return "<null>"
	}
	return strconv.FormatInt(*value, 10)
}

func catalogOptionalBool(value *bool) string {
	if value == nil {
		return "<null>"
	}
	return strconv.FormatBool(*value)
}

func catalogReadableRevision(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func persistRegisteredTableCatalogSeed(
	ctx context.Context,
	tx pgx.Tx,
	metadata registeredTableCatalogMetadata,
	entries []registeredTableCatalogEntry,
) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.meta (
			singleton_key, protocol_label, layout_name, catalog_revision, next_table_id,
			catalog_fingerprint
		) VALUES (TRUE, $1, $2, $3, $4, $5)
	`, syncSchemaProtocolLabel, syncSchemaLayoutName, metadata.catalogRevision, metadata.nextTableID, metadata.catalogFingerprint); err != nil {
		return fmt.Errorf("insert sync.meta row: %w", err)
	}
	return insertRegisteredTableCatalogEntries(ctx, tx, entries)
}

func insertRegisteredTableCatalogEntries(ctx context.Context, tx pgx.Tx, entries []registeredTableCatalogEntry) error {
	for _, entry := range entries {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.table_catalog (
				table_id, schema_name, table_name, sync_key_column, sync_key_kind,
				state_code, introduced_revision, retired_revision
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, entry.tableID, entry.schemaName, entry.tableName, entry.syncKeyColumn, entry.syncKeyKind,
			entry.stateCode, entry.introducedRevision, entry.retiredRevision); err != nil {
			return fmt.Errorf("insert sync.table_catalog row %d: %w", entry.tableID, err)
		}
	}
	return nil
}

func persistRegisteredTableCatalogAppend(ctx context.Context, tx pgx.Tx, plan registeredTableCatalogPlan) error {
	if err := insertRegisteredTableCatalogEntries(ctx, tx, plan.appended); err != nil {
		return err
	}
	return publishRegisteredTableCatalogMetadata(ctx, tx, plan.metadata)
}

func publishRegisteredTableCatalogMetadata(
	ctx context.Context,
	tx pgx.Tx,
	metadata registeredTableCatalogMetadata,
) error {
	if _, err := tx.Exec(ctx, `
		UPDATE sync.meta
		SET catalog_revision = $1, next_table_id = $2, catalog_fingerprint = $3
		WHERE singleton_key = TRUE
	`, metadata.catalogRevision, metadata.nextTableID, metadata.catalogFingerprint); err != nil {
		return fmt.Errorf("update sync.meta catalog metadata: %w", err)
	}
	return nil
}

func (s *SyncService) newCatalogBootstrapCandidate() *SyncService {
	return s.newCatalogBootstrapCandidateForTables(s.config.RegisteredTables)
}

func (s *SyncService) newCatalogBootstrapCandidateForTables(tables []RegisteredTable) *SyncService {
	configCopy := *s.config
	configCopy.RegisteredTables = cloneRegisteredTables(tables)
	return &SyncService{
		pool:                      s.pool,
		logger:                    s.logger,
		config:                    &configCopy,
		registeredTables:          make(map[string]bool),
		registeredTableInfo:       make(map[string]registeredTableRuntimeInfo),
		registeredTableByID:       make(map[int32]registeredTableRuntimeInfo),
		retainedTableByID:         make(map[int32]registeredTableRuntimeInfo),
		columnTypesByTable:        make(map[string]map[string]string),
		schemaGenerationRegistry:  s.schemaGenerationRegistry,
		generationViews:           make(map[SchemaGenerationID]*schemaGenerationView),
		adoptionSpoolDir:          s.adoptionSpoolDir,
		adoptionHooks:             s.adoptionHooks,
		catalogHooks:              s.catalogHooks,
		bootstrapProgressNow:      s.bootstrapProgressNow,
		bootstrapProgressInterval: s.bootstrapProgressInterval,
	}
}

func (s *SyncService) prepareCatalogBootstrapCandidateForTables(
	ctx context.Context,
	q syncCatalogQuerier,
	tables []RegisteredTable,
) (*SyncService, error) {
	candidate := s.newCatalogBootstrapCandidateForTables(tables)
	if err := validateRegisteredTablePersistence(ctx, q, candidate.config.RegisteredTables); err != nil {
		return nil, err
	}
	if err := candidate.normalizeAndValidateRegisteredSyncKeys(ctx, q); err != nil {
		return nil, err
	}
	if err := candidate.validateRegisteredIdentityNullability(ctx, q); err != nil {
		return nil, err
	}
	if err := candidate.validateIdentityCollations(ctx, q); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (s *SyncService) prepareCatalogBootstrapCandidate(
	ctx context.Context,
	q syncCatalogQuerier,
) (*SyncService, error) {
	return s.prepareCatalogBootstrapCandidateForTables(ctx, q, s.config.RegisteredTables)
}

func (s *SyncService) applyCatalogEntries(entries []registeredTableCatalogEntry) error {
	physicalByName := s.registeredTableInfo
	activeNames := make(map[string]bool)
	activeByName := make(map[string]registeredTableRuntimeInfo)
	activeByID := make(map[int32]registeredTableRuntimeInfo)
	retainedByID := make(map[int32]registeredTableRuntimeInfo, len(entries))
	for _, entry := range entries {
		keyType, ok := catalogKindToken(entry.syncKeyKind)
		if !ok {
			return catalogEntryFieldError(entry, "sync_key_kind", strconv.FormatInt(int64(entry.syncKeyKind), 10))
		}
		info := registeredTableRuntimeInfo{
			schemaName: entry.schemaName, tableName: entry.tableName, tableID: entry.tableID,
			syncKeyColumn: entry.syncKeyColumn, syncKeyType: keyType, syncKeyKind: entry.syncKeyKind,
		}
		if entry.stateCode == registeredTableCatalogStateActive {
			physical, exists := physicalByName[entry.key()]
			if !exists {
				return fmt.Errorf("active registered table %s is missing physical runtime metadata", entry.key())
			}
			physical.tableID = entry.tableID
			info = physical
			activeNames[entry.key()] = true
			activeByName[entry.key()] = info
			activeByID[entry.tableID] = info
		}
		retainedByID[entry.tableID] = info
	}
	s.registeredTables = activeNames
	s.registeredTableInfo = activeByName
	s.registeredTableByID = activeByID
	s.retainedTableByID = retainedByID
	return nil
}

func (s *SyncService) publishCatalogBootstrapCandidate(candidate *SyncService, metadata registeredTableCatalogMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registeredTables = candidate.registeredTables
	s.registeredTableInfo = candidate.registeredTableInfo
	s.registeredTableByID = candidate.registeredTableByID
	s.retainedTableByID = candidate.retainedTableByID
	s.columnTypesByTable = candidate.columnTypesByTable
	s.discoveredSchema = candidate.discoveredSchema
	s.catalogMetadata = metadata
	s.generationViews = candidate.generationViews
}

func (s *SyncService) activeRegisteredTableDeclarations() []RegisteredTable {
	infos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].tableID < infos[j].tableID })
	tables := make([]RegisteredTable, 0, len(infos))
	for _, info := range infos {
		tables = append(tables, RegisteredTable{
			Schema: info.schemaName, Table: info.tableName,
			SyncKeyColumns: []string{info.syncKeyColumn},
		})
	}
	return tables
}

func registeredTableDeclarationsForCatalogEntries(entries []registeredTableCatalogEntry) []RegisteredTable {
	tables := make([]RegisteredTable, 0, len(entries))
	for _, entry := range entries {
		tables = append(tables, RegisteredTable{
			Schema: entry.schemaName, Table: entry.tableName,
			SyncKeyColumns: []string{entry.syncKeyColumn},
		})
	}
	return tables
}

func activeRegisteredTableCatalogEntries(entries []registeredTableCatalogEntry) []registeredTableCatalogEntry {
	active := make([]registeredTableCatalogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.stateCode == registeredTableCatalogStateActive {
			active = append(active, entry)
		}
	}
	return active
}

func validateRegisteredTableCatalogAppendDrain(ctx context.Context, q syncCatalogQuerier) error {
	for _, relation := range []string{"snapshot_sessions", "bundle_capture_stage"} {
		var count int64
		query := fmt.Sprintf(`SELECT count(*) FROM sync.%s`, pgx.Identifier{relation}.Sanitize())
		if err := q.QueryRow(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("check catalog append drain relation sync.%s: %w", relation, err)
		}
		if count != 0 {
			return &RegisteredTableCatalogError{
				Code:  RegisteredTableCatalogAppendNotDrained,
				Field: "sync." + relation, ObservedCount: count,
			}
		}
	}
	return nil
}

func validateRegisteredTableCatalogAppendTablesEmpty(
	ctx context.Context,
	q syncCatalogQuerier,
	tables []RegisteredTable,
) error {
	for _, table := range tables {
		query := fmt.Sprintf(
			`SELECT EXISTS (SELECT 1 FROM %s LIMIT 1)`,
			pgx.Identifier{table.Schema, table.Table}.Sanitize(),
		)
		var exists bool
		if err := q.QueryRow(ctx, query).Scan(&exists); err != nil {
			return fmt.Errorf("check catalog append table %s.%s emptiness: %w", table.Schema, table.Table, err)
		}
		if exists {
			return &RegisteredTableCatalogError{
				Code:   RegisteredTableCatalogAppendTableNotEmpty,
				Schema: table.Schema, Table: table.Table, ObservedCount: 1,
			}
		}
	}
	return nil
}
