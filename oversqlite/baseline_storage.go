package oversqlite

import (
	"context"
	"fmt"
	"strings"
)

const authoritativeRowStateDDL = `CREATE TABLE IF NOT EXISTS _sync_row_state (
 schema_name TEXT NOT NULL, table_name TEXT NOT NULL, key_json TEXT NOT NULL,
 row_version INTEGER NOT NULL DEFAULT 0,
 server_kind TEXT NOT NULL CHECK(server_kind IN ('live','deleted','absent','unpublished','unknown')),
 server_payload TEXT,
 server_provenance TEXT NOT NULL CHECK(json_valid(server_provenance) AND json_type(server_provenance)='object'),
 relationships_known INTEGER NOT NULL CHECK(relationships_known IN (0,1) AND relationships_known=(server_kind!='unknown')),
 server_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(server_revision)='integer' AND server_revision>=0),
 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
 PRIMARY KEY(schema_name,table_name,key_json),
 CHECK(typeof(row_version)='integer' AND CASE server_kind
 WHEN 'live' THEN row_version>0 AND server_payload IS NOT NULL AND json_valid(server_payload) AND json_type(server_payload)='object'
 WHEN 'deleted' THEN row_version>0 AND server_payload IS NULL
 ELSE row_version=0 AND server_payload IS NULL END)
)`

const relationshipProjectionDDL = `CREATE TABLE IF NOT EXISTS _sync_relationships (
 view TEXT NOT NULL CHECK(view IN ('server','desired')),
 child_schema TEXT NOT NULL, child_table TEXT NOT NULL, child_key TEXT NOT NULL,
 fk_column TEXT NOT NULL, parent_schema TEXT NOT NULL, parent_table TEXT NOT NULL, parent_key TEXT NOT NULL,
 owner_revision INTEGER NOT NULL CHECK(typeof(owner_revision)='integer' AND owner_revision>=0),
 PRIMARY KEY(view,child_schema,child_table,child_key,fk_column)
)`

// The same FK shape is inspected at admission and at every retained opening.
// This projection reads only its owner's typed payload, never business values.
func relationshipProjectionInsertSQL(view, payload, revision, predicate string) string {
	return fmt.Sprintf(`INSERT INTO _sync_relationships
 (view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision)
 SELECT '%s',NEW.schema_name,NEW.table_name,NEW.key_json,lower(fk."from"),COALESCE(parent.schema_name,NEW.schema_name),lower(fk."table"),
 json_object(lower(COALESCE(fk."to",(SELECT name FROM pragma_table_info(fk."table") WHERE pk=1))),json_extract(%s,'$."'||lower(fk."from")||'".value')),%s
 FROM pragma_foreign_key_list(NEW.table_name) AS fk
 LEFT JOIN _sync_managed_tables AS parent ON parent.table_name=lower(fk."table")
 WHERE %s AND json_extract(%s,'$."'||lower(fk."from")||'".storage')!='null';`, view, payload, revision, predicate, payload)
}

func projectionDeleteSQL(view, row string) string {
	return fmt.Sprintf(`DELETE FROM _sync_relationships WHERE view='%s' AND child_schema=%s.schema_name AND child_table=%s.table_name AND child_key=%s.key_json;`, view, row, row, row)
}

func baselineMetadataTriggerSQL() map[string]string {
	result := map[string]string{}
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		oldDelete := ""
		if event != "INSERT" {
			oldDelete = projectionDeleteSQL("server", "OLD")
		}
		body := `UPDATE _sync_conflict_state SET baseline_revision=baseline_revision+1 WHERE singleton_key=1;` + oldDelete
		clause := event
		if event == "UPDATE" {
			clause = "UPDATE OF schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known"
		}
		if event != "DELETE" {
			body += `UPDATE _sync_row_state SET server_revision=(SELECT baseline_revision FROM _sync_conflict_state WHERE singleton_key=1) WHERE schema_name=NEW.schema_name AND table_name=NEW.table_name AND key_json=NEW.key_json;`
			body += relationshipProjectionInsertSQL("server", "NEW.server_payload", "(SELECT baseline_revision FROM _sync_conflict_state WHERE singleton_key=1)", "NEW.server_kind='live'")
		}
		name := "_sync_baseline_revision_" + strings.ToLower(event)
		result[name] = fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %s AFTER %s ON _sync_row_state BEGIN %s END", name, clause, body)
	}
	return result
}

func ensureBaselineMetadata(ctx context.Context, e execer) error {
	statements := []string{
		relationshipProjectionDDL,
		`CREATE INDEX IF NOT EXISTS _sync_relationships_parent_idx ON _sync_relationships(view,parent_schema,parent_table,parent_key,child_schema,child_table,child_key,fk_column)`,
		`CREATE INDEX IF NOT EXISTS _sync_row_state_unknown_idx ON _sync_row_state(relationships_known,table_name,key_json)`,
		`CREATE INDEX IF NOT EXISTS _sync_dirty_rows_order_idx ON _sync_dirty_rows(dirty_ordinal,schema_name,table_name,key_json)`,
	}
	for _, statement := range statements {
		if _, err := e.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	for _, statement := range baselineMetadataTriggerSQL() {
		if _, err := e.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
