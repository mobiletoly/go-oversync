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
	TableName  string
	PKColumn   string
	NewRowJSON string
	PKExprNew  string
	PKExprOld  string
}

// Template for INSERT trigger
const insertTriggerTemplate = `CREATE TRIGGER IF NOT EXISTS trg_{{.TableName}}_ai
AFTER INSERT ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
	INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
	VALUES ('{{.TableName}}', {{.PKExprNew}}, 0, 0);

	-- Reset deleted flag when record is reinserted
	UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}} AND deleted=1;

	-- Check if this is a new pending change (doesn't exist yet)
	INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
	SELECT '{{.TableName}}', {{.PKExprNew}}, 'INSERT', 0, {{.NewRowJSON}}, (SELECT next_change_id FROM _sync_client_info LIMIT 1)
	WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}});

	-- Update existing pending change if it exists
	UPDATE _sync_pending SET
		op='INSERT',
		base_version=(SELECT server_version FROM _sync_row_meta WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}}),
		payload={{.NewRowJSON}},
		queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}};

	-- Increment change ID counter only for new changes (when INSERT actually happened)
	UPDATE _sync_client_info SET next_change_id = next_change_id + 1
	WHERE changes() > 0 AND last_insert_rowid() > 0;
END`

// Template for UPDATE trigger
const updateTriggerTemplate = `CREATE TRIGGER IF NOT EXISTS trg_{{.TableName}}_au
AFTER UPDATE ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
	-- Ensure meta exists; mark not deleted
	INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
	VALUES ('{{.TableName}}', {{.PKExprNew}}, 0, 0);

	UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}};

	-- Queue UPDATE, but if an INSERT is already pending, keep it as INSERT and only refresh payload
	-- Check if this is a new pending change (doesn't exist yet)
	INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
	SELECT '{{.TableName}}', {{.PKExprNew}}, 'UPDATE',
		(SELECT server_version FROM _sync_row_meta WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}}),
		{{.NewRowJSON}},
		(SELECT next_change_id FROM _sync_client_info LIMIT 1)
	WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}});

	-- Update existing pending change if it exists
	UPDATE _sync_pending SET
		op = CASE WHEN op = 'INSERT' THEN 'INSERT' ELSE 'UPDATE' END,
		base_version = CASE WHEN op = 'INSERT' THEN base_version ELSE (SELECT server_version FROM _sync_row_meta WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}}) END,
		payload = {{.NewRowJSON}},
		queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprNew}};

	-- Increment change ID counter only for new changes (when INSERT actually happened)
	UPDATE _sync_client_info SET next_change_id = next_change_id + 1
	WHERE changes() > 0 AND last_insert_rowid() > 0;
END`

// Template for DELETE trigger
const deleteTriggerTemplate = `CREATE TRIGGER IF NOT EXISTS trg_{{.TableName}}_ad
AFTER DELETE ON {{.TableName}}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
	-- Ensure meta row exists (for already-synced rows this will no-op)
	INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
	VALUES ('{{.TableName}}', {{.PKExprOld}}, 0, 1);

	-- 1) Attempt to queue a DELETE **only if** there is no pending INSERT for this row.
	INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
	SELECT '{{.TableName}}', {{.PKExprOld}}, 'DELETE',
	       (SELECT server_version FROM _sync_row_meta WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}}),
	       NULL,
	       (SELECT next_change_id FROM _sync_client_info LIMIT 1)
	WHERE NOT EXISTS (
	  SELECT 1 FROM _sync_pending WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}}
	);

	-- Update existing pending change to DELETE if it's not an INSERT
	UPDATE _sync_pending SET
		op = 'DELETE',
		base_version = (SELECT server_version FROM _sync_row_meta WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}}),
		payload = NULL,
		queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}} AND op != 'INSERT';

	-- Increment change ID counter only for new DELETE changes (when INSERT actually happened)
	UPDATE _sync_client_info SET next_change_id = next_change_id + 1
	WHERE changes() > 0 AND last_insert_rowid() > 0;

	-- 2) If there WAS a pending INSERT (unsynced new row), cancel it now (net no-op).
	DELETE FROM _sync_pending WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}} AND op='INSERT';

	-- 3) Update meta for regular deletes
	UPDATE _sync_row_meta SET deleted=1, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}};

	-- 4) Cleanup meta for canceled unsynced INSERTs (server_version=0 and no pending left)
	DELETE FROM _sync_row_meta
	WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}} AND server_version=0
	  AND NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='{{.TableName}}' AND pk_uuid={{.PKExprOld}});
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

// createTriggersForTable creates INSERT, UPDATE, DELETE triggers for a table
// according to the technical specification using the KMP-inspired approach
func createTriggersForTable(db *sql.DB, syncTable SyncTable) error {
	tableName := syncTable.TableName
	tableLc := strings.ToLower(tableName)

	// Get table information
	tableInfo, err := GetTableInfo(db, tableLc)
	if err != nil {
		return fmt.Errorf("failed to get table info for %s: %w", tableName, err)
	}

	// Determine the primary key column name
	pkColumn := determinePrimaryKeyColumn(tableInfo, syncTable)

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

	// Prepare template data
	data := TriggerData{
		TableName:  tableLc,
		PKColumn:   pkColumn,
		NewRowJSON: payloadExpr,
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

// determinePrimaryKeyColumn determines the primary key column for a table
func determinePrimaryKeyColumn(tableInfo *TableInfo, syncTable SyncTable) string {
	// If syncKeyColumnName is explicitly specified, use it
	if syncTable.SyncKeyColumnName != "" {
		return syncTable.SyncKeyColumnName
	}
	// or auto-detect primary key from table schema
	if tableInfo.PrimaryKey != nil {
		return tableInfo.PrimaryKey.Name
	}
	// or fall back to "id" if no primary key is detected
	return "id"
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
