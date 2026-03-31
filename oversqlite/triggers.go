// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
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
	SchemaName string
	TableName  string
	PKColumn   string
	NewRowJSON string
	OldKeyJSON string
	NewKeyJSON string
	PKExprNew  string
	PKExprOld  string
}

const transitionPendingTriggerError = "SYNC_TRANSITION_PENDING"

const guardInsertTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_bi_guard
BEFORE INSERT ON {{.TableName}}
WHEN COALESCE((SELECT kind FROM _sync_operation_state WHERE singleton_key = 1), 'none') != 'none'
	AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) != 1
BEGIN
	SELECT RAISE(ABORT, '` + transitionPendingTriggerError + `');
END`

const guardUpdateTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_bu_guard
BEFORE UPDATE ON {{.TableName}}
WHEN COALESCE((SELECT kind FROM _sync_operation_state WHERE singleton_key = 1), 'none') != 'none'
	AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) != 1
BEGIN
	SELECT RAISE(ABORT, '` + transitionPendingTriggerError + `');
END`

const guardDeleteTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_bd_guard
BEFORE DELETE ON {{.TableName}}
WHEN COALESCE((SELECT kind FROM _sync_operation_state WHERE singleton_key = 1), 'none') != 'none'
	AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) != 1
BEGIN
	SELECT RAISE(ABORT, '` + transitionPendingTriggerError + `');
END`

// Template for INSERT trigger
const insertTriggerTemplate = `CREATE TRIGGER trg_{{.TableName}}_ai
AFTER INSERT ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
	VALUES (
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.NewKeyJSON}},
		CASE
			WHEN EXISTS (
				SELECT 1
				FROM _sync_row_state
				WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}} AND deleted=0
			) THEN 'UPDATE'
			ELSE 'INSERT'
		END,
		COALESCE(
			(SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			0
		),
		{{.NewRowJSON}},
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
	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
	SELECT
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.OldKeyJSON}},
		'DELETE',
		COALESCE(
			(SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			0
		),
		NULL,
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

	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
	VALUES (
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.NewKeyJSON}},
		CASE
			WHEN EXISTS (
				SELECT 1
				FROM _sync_row_state
				WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}} AND deleted=0
			) THEN 'UPDATE'
			ELSE 'INSERT'
		END,
		COALESCE(
			(SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.NewKeyJSON}}),
			0
		),
		{{.NewRowJSON}},
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
	INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
	VALUES (
		'{{.SchemaName}}',
		'{{.TableName}}',
		{{.OldKeyJSON}},
		'DELETE',
		COALESCE(
			(SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			(SELECT row_version FROM _sync_row_state WHERE schema_name='{{.SchemaName}}' AND table_name='{{.TableName}}' AND key_json={{.OldKeyJSON}}),
			0
		),
		NULL,
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

// buildJsonObjectExprHexAware creates a SQLite json_object() call that uses hex encoding for BLOB columns
func buildJsonObjectExprHexAware(tableInfo *TableInfo, prefix string) string {
	var pairs []string
	for _, col := range tableInfo.Columns {
		name := strings.ToLower(col.Name)
		var expr string
		if col.IsBlob() {
			// Preserve NULL blobs as JSON null instead of collapsing them to empty hex text.
			expr = fmt.Sprintf("CASE WHEN %s.%s IS NULL THEN NULL ELSE lower(hex(%s.%s)) END", prefix, col.Name, prefix, col.Name)
		} else {
			// Use column value directly for non-BLOB columns
			expr = fmt.Sprintf("%s.%s", prefix, col.Name)
		}
		pairs = append(pairs, fmt.Sprintf("'%s', %s", name, expr))
	}
	return fmt.Sprintf("json_object(%s)", strings.Join(pairs, ", "))
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

func createTriggersForTableWithInfo(db *sql.DB, schemaName string, syncTable SyncTable, tableInfo *TableInfo) error {
	tableName := syncTable.TableName
	tableLc := strings.ToLower(tableName)
	if tableInfo == nil {
		return fmt.Errorf("table info is required for %s", tableName)
	}

	pkColumn, err := configuredPrimaryKeyColumn(tableInfo, syncTable)
	if err != nil {
		return err
	}

	// Check if primary key is BLOB
	pkIsBlob := isPrimaryKeyBlob(tableInfo, pkColumn)

	// Build primary key expressions for NEW and OLD rows
	var pkExprNew, pkExprOld string
	if pkIsBlob {
		pkExprNew = fmt.Sprintf("lower(hex(NEW.%s))", pkColumn)
		pkExprOld = fmt.Sprintf("lower(hex(OLD.%s))", pkColumn)
	} else {
		pkExprNew = fmt.Sprintf("NEW.%s", pkColumn)
		pkExprOld = fmt.Sprintf("OLD.%s", pkColumn)
	}

	// Build BLOB-aware payload expression
	payloadExpr := buildJsonObjectExprHexAware(tableInfo, "NEW")
	oldKeyExpr := buildKeyJSONObjectExprHexAware(tableInfo, pkColumn, "OLD")
	newKeyExpr := buildKeyJSONObjectExprHexAware(tableInfo, pkColumn, "NEW")

	// Prepare template data
	data := TriggerData{
		SchemaName: schemaName,
		TableName:  tableLc,
		PKColumn:   pkColumn,
		NewRowJSON: payloadExpr,
		OldKeyJSON: oldKeyExpr,
		NewKeyJSON: newKeyExpr,
		PKExprNew:  pkExprNew,
		PKExprOld:  pkExprOld,
	}

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
			if _, err := db.Exec(fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(triggerName))); err != nil {
				return fmt.Errorf("failed to drop %s trigger for table %s: %w", tmpl.name, tableName, err)
			}
		}
		if _, err := db.Exec(triggerSQL); err != nil {
			return fmt.Errorf("failed to create %s trigger for table %s: %w", tmpl.name, tableName, err)
		}
	}

	return nil
}

func renderTriggerSQL(tmpl *template.Template, data TriggerData) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func loadExistingTriggerSQLs(db *sql.DB, tableName string) (map[string]string, error) {
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
