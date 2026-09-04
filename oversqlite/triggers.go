// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"text/template"
)

var triggerSQLTemplates = []struct {
	name   string
	suffix string
	tmpl   *template.Template
}{
	{
		name:   "guard_insert",
		suffix: "bi_guard",
		tmpl:   template.Must(template.New("guard_insert").Parse(guardInsertTriggerTemplate)),
	},
	{
		name:   "guard_update",
		suffix: "bu_guard",
		tmpl:   template.Must(template.New("guard_update").Parse(guardUpdateTriggerTemplate)),
	},
	{
		name:   "guard_delete",
		suffix: "bd_guard",
		tmpl:   template.Must(template.New("guard_delete").Parse(guardDeleteTriggerTemplate)),
	},
	{
		name:   "insert",
		suffix: "ai",
		tmpl:   template.Must(template.New("insert").Parse(insertTriggerTemplate)),
	},
	{
		name:   "update",
		suffix: "au",
		tmpl:   template.Must(template.New("update").Parse(updateTriggerTemplate)),
	},
	{
		name:   "delete",
		suffix: "ad",
		tmpl:   template.Must(template.New("delete").Parse(deleteTriggerTemplate)),
	},
}

// TriggerData holds the data needed for trigger template rendering
type TriggerData struct {
	SchemaName         string
	TableName          string
	PKColumn           string
	NewRowJSON         string
	InsertRowJSON      string
	UpdateRowJSON      string
	EnsureNewBaseline  string
	RequireOldBaseline string
	OldKeyJSON         string
	NewKeyJSON         string
	PKExprNew          string
	PKExprOld          string
}

const transitionPendingTriggerError = "SYNC_TRANSITION_PENDING"

const guardInsertTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_bi_guard
BEFORE INSERT ON {{.TableName}}
WHEN (COALESCE((SELECT kind FROM _sync_operation_state WHERE singleton_key = 1), 'none') != 'none'
	OR COALESCE((SELECT phase FROM _sync_upgrade_journal WHERE singleton_key = 1), 'ready') != 'ready'
	)
	AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) != 1
BEGIN
	SELECT RAISE(ABORT, '` + transitionPendingTriggerError + `');
END`

const guardUpdateTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_bu_guard
BEFORE UPDATE ON {{.TableName}}
WHEN (COALESCE((SELECT kind FROM _sync_operation_state WHERE singleton_key = 1), 'none') != 'none'
	OR COALESCE((SELECT phase FROM _sync_upgrade_journal WHERE singleton_key = 1), 'ready') != 'ready')
	AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) != 1
BEGIN
	SELECT RAISE(ABORT, '` + transitionPendingTriggerError + `');
END`

const guardDeleteTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_bd_guard
BEFORE DELETE ON {{.TableName}}
WHEN (COALESCE((SELECT kind FROM _sync_operation_state WHERE singleton_key = 1), 'none') != 'none'
	OR COALESCE((SELECT phase FROM _sync_upgrade_journal WHERE singleton_key = 1), 'ready') != 'ready')
	AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) != 1
BEGIN
	SELECT RAISE(ABORT, '` + transitionPendingTriggerError + `');
END`

// Template for INSERT trigger
const insertTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_ai
AFTER INSERT ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
    {{.EnsureNewBaseline}}
	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
	VALUES (
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.NewKeyJSON}},
		CASE
			WHEN EXISTS (
				SELECT 1
				FROM _sync_row_state
				WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}} AND server_kind='live'
			) THEN 'UPDATE'
			ELSE 'INSERT'
		END,
		COALESCE(
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			0
		),
		{{.InsertRowJSON}},
        (SELECT CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
        (SELECT server_payload FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
        (SELECT server_provenance FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
		COALESCE(
			(SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			(SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
		),
		strftime('%Y-%m-%dT%H:%M:%fZ','now')
	)
	ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
		op=excluded.op,
		payload=excluded.payload,
		updated_at=excluded.updated_at;
END`

// Template for UPDATE trigger
const updateTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_au
AFTER UPDATE ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
    {{.RequireOldBaseline}}
    {{.EnsureNewBaseline}}
	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
	SELECT
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.OldKeyJSON}},
		'DELETE',
		COALESCE(
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			0
		),
		NULL,
        (SELECT CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
        (SELECT server_payload FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
        (SELECT server_provenance FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
		COALESCE(
			(SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			(SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
		),
		strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE {{.OldKeyJSON}} != {{.NewKeyJSON}}
	ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
		op='DELETE',
		payload=NULL,
		updated_at=excluded.updated_at;

	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
	VALUES (
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.NewKeyJSON}},
		CASE
			WHEN EXISTS (
				SELECT 1
				FROM _sync_row_state
				WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}} AND server_kind='live'
			) THEN 'UPDATE'
			ELSE 'INSERT'
		END,
		COALESCE(
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			0
		),
		{{.UpdateRowJSON}},
        (SELECT CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
        (SELECT server_payload FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
        (SELECT server_provenance FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
		COALESCE(
			(SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			(SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
		),
		strftime('%Y-%m-%dT%H:%M:%fZ','now')
	)
	ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
		op=excluded.op,
		payload=excluded.payload,
		updated_at=excluded.updated_at;
END`

// Template for DELETE trigger
const deleteTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_ad
AFTER DELETE ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
    {{.RequireOldBaseline}}
	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
	VALUES (
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.OldKeyJSON}},
		'DELETE',
		COALESCE(
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			0
		),
		NULL,
        (SELECT CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
        (SELECT server_payload FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
        (SELECT server_provenance FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
		COALESCE(
			(SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			(SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
		),
		strftime('%Y-%m-%dT%H:%M:%fZ','now')
	)
	ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
		op='DELETE',
		payload=NULL,
		updated_at=excluded.updated_at;
END`

// Capture storage classes before any server contract is known. Values remain
// SQLite primitives; no UUID, Boolean, JSON or decimal meaning is inferred here.
func buildJsonObjectExprHexAware(tableInfo *TableInfo, prefix string) string {
	pairs := make([]string, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		value := prefix + "." + quoteIdent(col.Name)
		encoded := fmt.Sprintf("CASE typeof(%[1]s) WHEN 'blob' THEN lower(hex(%[1]s)) WHEN 'integer' THEN CAST(%[1]s AS TEXT) WHEN 'real' THEN printf('%%!.26g', %[1]s) ELSE %[1]s END", value)
		pairs = append(pairs, fmt.Sprintf("'%s', json_object('storage', typeof(%s), 'value', %s)", col.Name, value, encoded))
	}
	return "json_object(" + strings.Join(pairs, ", ") + ")"
}

func buildKeyJSONObjectExprHexAware(tableInfo *TableInfo, keyColumn, prefix string) string {
	for _, col := range tableInfo.Columns {
		if !strings.EqualFold(col.Name, keyColumn) {
			continue
		}
		name := strings.ToLower(col.Name)
		if col.IsBlob() {
			return fmt.Sprintf("json_object('%s', CASE WHEN %s.%s IS NULL THEN NULL ELSE lower(hex(%s.%s)) END)", name, prefix, col.Name, prefix, col.Name)
		}
		if col.IsInteger() {
			return fmt.Sprintf("json_object('%s', CAST(%s.%s AS TEXT))", name, prefix, col.Name)
		}
		return fmt.Sprintf("json_object('%s', %s.%s)", name, prefix, col.Name)
	}
	return fmt.Sprintf("json_object('%s', %s.%s)", strings.ToLower(keyColumn), prefix, keyColumn)
}

// createTriggersForTable creates INSERT, UPDATE, DELETE triggers for a table
// according to the technical specification using the KMP-inspired approach
func createTriggersForTable(db *sql.DB, args ...any) error {
	schemaName := "main"
	var syncTable SyncTable
	switch len(args) {
	case 1:
		var ok bool
		syncTable, ok = args[0].(SyncTable)
		if !ok {
			return fmt.Errorf("createTriggersForTable requires SyncTable argument")
		}
	case 2:
		var ok bool
		schemaName, ok = args[0].(string)
		if !ok {
			return fmt.Errorf("createTriggersForTable schema must be a string")
		}
		syncTable, ok = args[1].(SyncTable)
		if !ok {
			return fmt.Errorf("createTriggersForTable requires SyncTable argument")
		}
	default:
		return fmt.Errorf("createTriggersForTable requires SyncTable argument")
	}
	tableInfo, err := GetTableInfo(db, strings.ToLower(syncTable.TableName))
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", syncTable.TableName, err)
	}
	return createTriggersForTableWithInfo(db, schemaName, syncTable, tableInfo)
}

type triggerSQLDB interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
}

func createTriggersForTableWithInfo(db triggerSQLDB, schemaName string, syncTable SyncTable, tableInfo *TableInfo) error {
	tableName := syncTable.TableName
	tableLc := strings.ToLower(tableName)
	if tableInfo == nil {
		return fmt.Errorf("table info is required for %s", tableName)
	}

	pkColumn, err := configuredPrimaryKeyColumn(tableInfo, syncTable)
	if err != nil {
		return err
	}

	data := triggerRenderData(schemaName, tableLc, pkColumn, tableInfo)

	existingSQLByName, err := loadExistingTriggerSQLs(db, tableLc)
	if err != nil {
		return fmt.Errorf("failed to inspect triggers for table %s: %w", tableName, err)
	}

	for _, tmpl := range triggerSQLTemplates {
		triggerName := fmt.Sprintf("trg_%s_%s", tableLc, tmpl.suffix)
		triggerSQL, err := renderTriggerSQL(tmpl.tmpl, data)
		if err != nil {
			return fmt.Errorf("failed to render %s trigger template for table %s: %w", tmpl.name, tableName, err)
		}

		existingSQL, exists := existingSQLByName[triggerName]
		if exists && sameTriggerSQL(existingSQL, triggerSQL) {
			continue
		}
		if exists {
			return fmt.Errorf("incompatible managed capture trigger %s; recreate the development database", triggerName)
		}

		if _, err := db.Exec(triggerSQL); err != nil {
			return fmt.Errorf("failed to create %s trigger for table %s: %w", tmpl.name, tableName, err)
		}
	}

	return nil
}

func validateRuntimeTriggerInventory(db triggerSQLDB, client *Client, validatedTables *validatedSyncTables) error {
	ownedTables := make(map[string]struct{}, len(syncMetadataTableNames))
	for _, name := range syncMetadataTableNames {
		ownedTables[name] = struct{}{}
	}
	allowed := map[string]string{
		"_sync_conflict_revision_insert": "",
		"_sync_conflict_revision_update": "",
		"_sync_conflict_revision_delete": "",
	}
	for name := range baselineMetadataTriggerSQL() {
		allowed[name] = ""
	}
	for name, sqlText := range stageAccountingDDL() {
		allowed[name] = sqlText
	}
	for _, syncTable := range client.config.Tables {
		tableName := strings.ToLower(strings.TrimSpace(syncTable.TableName))
		expected, err := expectedTriggerSQLByName(client.schemaForTable(tableName), syncTable, validatedTables.tableInfoByKey[tableName])
		if err != nil {
			return err
		}
		for name, sqlText := range expected {
			allowed[strings.ToLower(name)] = sqlText
		}

	}
	rows, err := db.Query(`SELECT name,COALESCE(sql,'') FROM sqlite_master WHERE type='trigger' ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, sqlText string
		if err := rows.Scan(&name, &sqlText); err != nil {
			return err
		}
		_, reserved := allowed[strings.ToLower(name)]
		if !reserved && !triggerReferencesRetainedTable(sqlText, ownedTables) {
			continue
		}
		expectedSQL, ok := allowed[strings.ToLower(name)]
		if !ok || (expectedSQL != "" && !sameTriggerSQL(sqlText, expectedSQL)) {
			return fmt.Errorf("trigger %s has unauthorized access to retained runtime tables", name)
		}
	}
	return rows.Err()
}

func triggerRenderData(schemaName, tableName, pkColumn string, tableInfo *TableInfo) TriggerData {
	pkExprNew := fmt.Sprintf("NEW.%s", pkColumn)
	pkExprOld := fmt.Sprintf("OLD.%s", pkColumn)
	if isPrimaryKeyBlob(tableInfo, pkColumn) {
		pkExprNew = fmt.Sprintf("lower(hex(NEW.%s))", pkColumn)
		pkExprOld = fmt.Sprintf("lower(hex(OLD.%s))", pkColumn)
	}
	data := TriggerData{
		SchemaName: schemaName,
		TableName:  tableName,
		PKColumn:   pkColumn,
		NewRowJSON: buildJsonObjectExprHexAware(tableInfo, "NEW"),
		OldKeyJSON: buildKeyJSONObjectExprHexAware(tableInfo, pkColumn, "OLD"),
		NewKeyJSON: buildKeyJSONObjectExprHexAware(tableInfo, pkColumn, "NEW"),
		PKExprNew:  pkExprNew,
		PKExprOld:  pkExprOld,
	}
	data.InsertRowJSON = data.NewRowJSON
	data.UpdateRowJSON = data.NewRowJSON

	lookup := " FROM _sync_row_state WHERE schema_name='" + schemaName + "' AND table_name='" + tableName + "' AND key_json="
	data.RequireOldBaseline = "SELECT CASE WHEN NOT EXISTS(SELECT 1" + lookup + data.OldKeyJSON + ") THEN RAISE(ABORT,'SYNC_BASELINE_MISSING') END;"
	data.EnsureNewBaseline = "INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known) SELECT '" + schemaName + "','" + tableName + "'," + data.NewKeyJSON + ",0,'unpublished',NULL," + baselineProvenanceSQL("'never_published'", "0", "NULL") + ",1 WHERE true ON CONFLICT(schema_name,table_name,key_json) DO NOTHING;"

	return data
}

func expectedTriggerSQLByName(schemaName string, syncTable SyncTable, tableInfo *TableInfo) (map[string]string, error) {
	tableName := strings.ToLower(syncTable.TableName)
	pkColumn, err := configuredPrimaryKeyColumn(tableInfo, syncTable)
	if err != nil {
		return nil, err
	}
	data := triggerRenderData(schemaName, tableName, pkColumn, tableInfo)
	expected := make(map[string]string, len(triggerSQLTemplates))
	for _, tmpl := range triggerSQLTemplates {
		sqlText, err := renderTriggerSQL(tmpl.tmpl, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render %s trigger template for table %s: %w", tmpl.name, tableName, err)
		}
		expected[fmt.Sprintf("trg_%s_%s", tableName, tmpl.suffix)] = sqlText
	}
	return expected, nil
}

func renderTriggerSQL(tmpl *template.Template, data TriggerData) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func loadExistingTriggerSQLs(db triggerSQLDB, tableName string) (map[string]string, error) {
	rows, err := db.Query(`SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?`, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	triggerSQLByName := make(map[string]string)
	for rows.Next() {
		var triggerName string
		var sqlText sql.NullString
		if err := rows.Scan(&triggerName, &sqlText); err != nil {
			return nil, err
		}
		triggerSQLByName[triggerName] = sqlText.String
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return triggerSQLByName, nil
}

func sameTriggerSQL(left, right string) bool {
	return normalizeTriggerSQL(left) == normalizeTriggerSQL(right)
}

func normalizeTriggerSQL(sqlText string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(sqlText)), " "))
	return strings.ReplaceAll(normalized, "create trigger if not exists", "create trigger")
}

func configuredPrimaryKeyColumn(tableInfo *TableInfo, syncTable SyncTable) (string, error) {
	keyColumns, err := normalizedSyncKeyColumns(syncTable)
	if err != nil {
		return "", err
	}
	if len(keyColumns) != 1 {
		return "", fmt.Errorf("table %s must declare exactly one sync key column in the current client runtime", syncTable.TableName)
	}
	pkColumn := keyColumns[0]
	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, pkColumn) {
			if !col.IsPrimaryKey {
				return "", fmt.Errorf("configured primary key column %s for table %s is not declared as PRIMARY KEY", col.Name, syncTable.TableName)
			}
			if _, err := supportedLocalSyncKeyKind(tableInfo, col.Name, syncTable.TableName); err != nil {
				return "", err
			}
			if !col.NotNull {
				return "", fmt.Errorf("configured visible sync-key column %s for table %s must declare NOT NULL explicitly; repair or recreate the local application database before sync initialization", col.Name, syncTable.TableName)
			}
			return col.Name, nil
		}
	}
	return "", fmt.Errorf("table %s does not contain configured primary key column %s", syncTable.TableName, pkColumn)
}

// isPrimaryKeyBlob checks if the primary key column is a BLOB type
func isPrimaryKeyBlob(tableInfo *TableInfo, primaryKeyColumn string) bool {
	for _, col := range tableInfo.Columns {
		if strings.EqualFold(col.Name, primaryKeyColumn) {
			return col.IsBlob()
		}
	}
	return false
}
