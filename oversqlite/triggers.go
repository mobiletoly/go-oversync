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

// Template for INSERT trigger
const insertTriggerTemplate = `CREATE TRIGGER IF NOT EXISTS trg_{{.TableName}}_ai
AFTER INSERT ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_state LIMIT 1), 0) = 0
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
const updateTriggerTemplate = `CREATE TRIGGER IF NOT EXISTS trg_{{.TableName}}_au
AFTER UPDATE ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_state LIMIT 1), 0) = 0
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
const deleteTriggerTemplate = `CREATE TRIGGER IF NOT EXISTS trg_{{.TableName}}_ad
AFTER DELETE ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_state LIMIT 1), 0) = 0
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
			// Use hex encoding for BLOB columns to preserve binary data
			expr = fmt.Sprintf("lower(hex(%s.%s))", prefix, col.Name)
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
			return fmt.Sprintf("json_object('%s', lower(hex(%s.%s)))", name, prefix, col.Name)
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
	tableName := syncTable.TableName
	tableLc := strings.ToLower(tableName)

	// Get table information
	tableInfo, err := GetTableInfo(db, tableLc)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", tableName, err)
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

	// Parse and execute templates
	templates := []struct {
		name     string
		template string
	}{
		{"insert", insertTriggerTemplate},
		{"update", updateTriggerTemplate},
		{"delete", deleteTriggerTemplate},
	}

	for _, tmpl := range templates {
		triggerName := fmt.Sprintf("trg_%s_%s", tableLc, map[string]string{
			"insert": "ai",
			"update": "au",
			"delete": "ad",
		}[tmpl.name])
		if _, err := db.Exec(fmt.Sprintf("DROP TRIGGER IF EXISTS %s", quoteIdent(triggerName))); err != nil {
			return fmt.Errorf("failed to drop %s trigger for table %s: %w", tmpl.name, tableName, err)
		}

		// Parse template
		t, err := template.New(tmpl.name).Parse(tmpl.template)
		if err != nil {
			return fmt.Errorf("failed to parse %s trigger template for table %s: %w", tmpl.name, tableName, err)
		}

		// Execute template
		var buf bytes.Buffer
		if err := t.Execute(&buf, data); err != nil {
			return fmt.Errorf("failed to execute %s trigger template for table %s: %w", tmpl.name, tableName, err)
		}

		// Create trigger
		if _, err := db.Exec(buf.String()); err != nil {
			return fmt.Errorf("failed to create %s trigger for table %s: %w", tmpl.name, tableName, err)
		}
	}

	return nil
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
