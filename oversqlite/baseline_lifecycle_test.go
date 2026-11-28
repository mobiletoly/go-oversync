package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func baselineText(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var value string
	require.NoError(t, db.QueryRow(query, args...).Scan(&value))
	return value
}

func TestBaselineLifecycle_LocalCaptureFreezeRequeue(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	before := baselineText(t, db, `SELECT server_payload FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("C"))
	_, err := db.Exec(`UPDATE nodes SET parent_id='Q',name='first' WHERE id='C'; UPDATE nodes SET name='second' WHERE id='C'`)
	require.NoError(t, err)
	require.JSONEq(t, before, baselineText(t, db, `SELECT base_payload FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("C")))
	require.JSONEq(t, before, baselineText(t, db, `SELECT server_payload FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("C")))
	require.Equal(t, "P", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("C")))
	require.Equal(t, "Q", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='desired' AND child_key=?`, rowKeyJSON("C")))
	frozen, err := c.ensurePushOutboundSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, frozen.Rows, 1)
	require.JSONEq(t, before, frozen.Rows[0].BasePayload.String)
	_, err = db.Exec(`UPDATE nodes SET name='during flight' WHERE id='C'`)
	require.NoError(t, err)
	// A new observation must not alter the original frozen B during proven non-commit requeue.
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c.installAuthoritativeBaseline(ctx, tx, "main", "nodes", rowKeyJSON("C"), "live", 2, []byte(`{"id":"C","name":"remote","parent_id":"Q"}`), baselineObservation{Origin: "explicit_conflict"}))
	require.NoError(t, tx.Commit())
	// Only resolve's durable cancelled outcome proves non-commit in v1.
	_, err = c.PushPending(ctx)
	require.NoError(t, err)
	require.Empty(t, qualificationState(t, db, "nodes")["_sync_outbox_rows"])
	require.JSONEq(t, before, baselineText(t, db, `SELECT base_payload FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("C")))
	require.Equal(t, "during flight", baselineText(t, db, `SELECT json_extract(payload,'$.name.value') FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("C")))
	require.Equal(t, "remote", baselineText(t, db, `SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("C")))
	require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
}

func TestBaselineLifecycle_SettlementRebasesOnlyAffectedIntent(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='pending' WHERE id IN ('C','D')`)
	require.NoError(t, err)
	unrelated := baselineText(t, db, `SELECT base_payload FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("D"))
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c.installAuthoritativeBaseline(ctx, tx, "main", "nodes", rowKeyJSON("C"), "live", 2, []byte(`{"id":"C","name":"normalized","parent_id":"Q"}`), baselineObservation{Origin: "settlement"}))
	current, err := c.loadDirtyUploadStateInTx(ctx, tx, "main", "nodes", rowKeyJSON("C"))
	require.NoError(t, err)
	require.NoError(t, c.rebaseDirtyIntentInTx(ctx, tx, "main", "nodes", rowKeyJSON("C"), "UPDATE", 2, current.Payload))
	require.NoError(t, tx.Commit())
	require.Equal(t, "normalized", baselineText(t, db, `SELECT json_extract(base_payload,'$.name.value') FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("C")))
	require.JSONEq(t, unrelated, baselineText(t, db, `SELECT base_payload FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("D")))
	require.Equal(t, "pending", baselineText(t, db, `SELECT name FROM nodes WHERE id='C'`))
}

func TestBaselineLifecycle_DeleteRetainsServerEdgesAndRekeyBases(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_ = c
	_, err := db.Exec(`UPDATE nodes SET id='R' WHERE id='Q'; INSERT INTO nodes VALUES('X','transient',NULL); DELETE FROM nodes WHERE id='X'; DELETE FROM nodes WHERE id='D'`)
	require.NoError(t, err)
	require.Equal(t, "present", baselineText(t, db, `SELECT base_kind FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("Q")))
	require.Equal(t, "absent", baselineText(t, db, `SELECT base_kind FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("R")))
	require.Equal(t, "C", baselineText(t, db, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("D")))
	require.Equal(t, "unpublished", baselineText(t, db, `SELECT server_kind FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("X")))
	require.Equal(t, "1", baselineText(t, db, `SELECT payload IS NULL FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("D")))
}

func TestBaselineLifecycle_WireEvidenceAndRollback(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`CREATE TRIGGER rewrite_name AFTER UPDATE ON nodes WHEN NEW.id='C' BEGIN UPDATE nodes SET name='triggered' WHERE id='C'; END`)
	require.NoError(t, err)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, c.setBundleApplyModeInTx(ctx, tx, true))
	row := oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("C"), Op: "UPDATE", RowVersion: 2, Payload: []byte(`{"id":"C","name":"wire","parent_id":"Q"}`)}
	require.NoError(t, c.applyBundleRowAuthoritativelyInTx(ctx, tx, &row, "C"))
	var stored string
	require.NoError(t, tx.QueryRow(`SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("C")).Scan(&stored))
	require.Equal(t, "wire", stored)
	require.NoError(t, tx.Rollback())
	require.Equal(t, "child", baselineText(t, db, `SELECT name FROM nodes WHERE id='C'`))
	require.Equal(t, "child", baselineText(t, db, `SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json=?`, rowKeyJSON("C")))
}

func TestBaselineLifecycle_EqualVersionRejectsDisagreement(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	err = c.installAuthoritativeBaseline(ctx, tx, "main", "nodes", rowKeyJSON("C"), "live", 1, []byte(`{"id":"C","name":"different","parent_id":"P"}`), baselineObservation{Origin: "pull"})
	require.ErrorContains(t, err, "equal version")
}

func TestBaselineLifecycle_MissingEvidenceAndCounterOverflowRollback(t *testing.T) {
	_, db := newA1SurvivorClient(t)
	_, err := db.Exec(`DELETE FROM _sync_row_state WHERE key_json='{"id":"C"}'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nodes SET name='unsafe' WHERE id='C'`)
	require.ErrorContains(t, err, "SYNC_BASELINE_MISSING")
	require.Equal(t, "child", baselineText(t, db, `SELECT name FROM nodes WHERE id='C'`))
	_, err = db.Exec(`UPDATE _sync_conflict_state SET revision=9223372036854775806`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nodes SET name='overflow' WHERE id='Q'`)
	require.Error(t, err)
	require.Equal(t, "other", baselineText(t, db, `SELECT name FROM nodes WHERE id='Q'`))
}

func TestBaselineLifecycle_FileReopenAndSecondConnection(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	path := t.TempDir() + "/baseline.sqlite"
	_, err := db.Exec(`VACUUM INTO ?`, path)
	require.NoError(t, err)
	reopenedDB, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	defer reopenedDB.Close()
	reopenedDB.SetMaxOpenConns(1)
	reopened, err := newAdmittedFixtureClient(reopenedDB, c.BaseURL, c.Token, c.config)
	require.NoError(t, err)
	defer reopened.Close()
	writer, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	_, err = writer.Exec(`UPDATE nodes SET name='other connection',parent_id='Q' WHERE id='C'`)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.Equal(t, "child", baselineText(t, reopenedDB, `SELECT json_extract(base_payload,'$.name.value') FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("C")))
	require.Equal(t, "P", baselineText(t, reopenedDB, `SELECT json_extract(parent_key,'$.id') FROM _sync_relationships WHERE view='server' AND child_key=?`, rowKeyJSON("C")))
	require.NoError(t, validateCompleteRetainedRuntime(ctx, reopenedDB))
}

func TestBaselineLifecycle_CensusRejectsBeforeConfiguration(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`DROP TRIGGER trg_nodes_au; CREATE TRIGGER trg_nodes_au AFTER UPDATE ON nodes BEGIN SELECT 1; END`)
	require.NoError(t, err)
	before := baselineText(t, db, `PRAGMA journal_mode`)
	cfg := cloneCoordinatorClientConfig(c.config)
	cfg.DatabaseID = "fixture"
	cfg.ServiceID = "fixture"
	require.ErrorContains(t, prepareRetainedContract(ctx, db, cfg), "capture trigger")
	require.Equal(t, before, baselineText(t, db, `PRAGMA journal_mode`))
	require.Contains(t, baselineText(t, db, `SELECT sql FROM sqlite_master WHERE name='trg_nodes_au'`), "SELECT 1")
}

func TestBaselineLifecycle_TypedWireCapture(t *testing.T) {
	ctx := context.Background()
	c := codecTestClient(t)
	c.tableInfo = NewTableInfoProvider()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	c.db = db
	_, err = db.Exec(`CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,boolean_value INTEGER NOT NULL,int16_value INTEGER NOT NULL,int32_value INTEGER NOT NULL,int64_value INTEGER NOT NULL,decimal_value TEXT NOT NULL,float32_value REAL NOT NULL,float64_value REAL NOT NULL,text_value TEXT NOT NULL,bytes_value BLOB NOT NULL,uuid_blob_value BLOB NOT NULL,json_value TEXT NOT NULL)`)
	require.NoError(t, err)
	wire := []byte(`{"id":"550e8400-e29b-41d4-a716-446655440000","boolean_value":true,"int16_value":"-32768","int32_value":"2147483647","int64_value":"9223372036854775807","decimal_value":"1234567890.123456789","float32_value":"1.5","float64_value":"2.25","text_value":"unchanged","bytes_value":"AP8=","uuid_blob_value":"550e8400-e29b-41d4-a716-446655440001","json_value":"{\"exact\":\"9007199254740993\",\"null\":null}"}`)
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	capture, err := c.captureAuthoritativePayload(ctx, tx, "people", wire)
	require.NoError(t, err)
	require.Contains(t, capture, `"int64_value":{"storage":"integer","value":"9223372036854775807"}`)
	require.Contains(t, capture, `"bytes_value":{"storage":"blob","value":"00ff"}`)
	restored, err := c.processPayloadForUpload("people", capture)
	require.NoError(t, err)
	require.JSONEq(t, string(wire), string(restored))
}

func TestBaselineLifecycle_CorruptProjectionBlocksFreeze(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='dirty' WHERE id='C'; DELETE FROM _sync_relationships WHERE view='server' AND child_key='{"id":"C"}'`)
	require.NoError(t, err)
	_, err = c.ensurePushOutboundSnapshot(context.Background())
	var pending *FKReconciliationRequiredError
	require.ErrorAs(t, err, &pending)
	require.Equal(t, "historical_repair_required", pending.Detail)
	require.Equal(t, "1", baselineText(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Equal(t, "0", baselineText(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
}

func TestBaselineLifecycle_CorruptEvidenceBlocksApplyAndFreeze(t *testing.T) {
	t.Run("desired projection before apply", func(t *testing.T) {
		c, db := newA1SurvivorClient(t)
		_, err := db.Exec(`UPDATE nodes SET parent_id='Q' WHERE id='C'; DELETE FROM _sync_relationships WHERE view='desired'`)
		require.NoError(t, err)
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = c.loadDirtyUploadStateInTx(context.Background(), tx, "main", "nodes", rowKeyJSON("C"))
		require.ErrorContains(t, err, "projection")
	})
	t.Run("server projection", func(t *testing.T) {
		c, db := newA1SurvivorClient(t)
		_, err := db.Exec(`DELETE FROM _sync_relationships WHERE view='server' AND child_key='{"id":"C"}'`)
		require.NoError(t, err)
		tx, err := db.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer tx.Rollback()
		err = c.installAuthoritativeBaseline(context.Background(), tx, "main", "nodes", rowKeyJSON("C"), "live", 2, []byte(`{"id":"C","name":"remote","parent_id":"Q"}`), baselineObservation{Origin: "pull"})
		require.ErrorContains(t, err, "projection")
	})
	t.Run("historical provenance", func(t *testing.T) {
		c, db := newA1SurvivorClient(t)
		_, err := db.Exec(`UPDATE nodes SET name='pending' WHERE id='C'; UPDATE _sync_dirty_rows SET base_provenance='{}'`)
		require.NoError(t, err)
		_, err = c.ensurePushOutboundSnapshot(context.Background())
		require.ErrorAs(t, err, new(*ResolutionBaseUnavailableError))
		require.Equal(t, "1", baselineText(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	})
	t.Run("baseline overflow", func(t *testing.T) {
		_, db := newA1SurvivorClient(t)
		_, err := db.Exec(`UPDATE _sync_conflict_state SET baseline_revision=9223372036854775806`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO nodes VALUES('overflow','pending',NULL)`)
		require.Error(t, err)
		require.Equal(t, "0", baselineText(t, db, `SELECT count(*) FROM nodes WHERE id='overflow'`))
	})
}

func TestBaselineLifecycle_TombstoneReinsertAndUnpublishedNoop(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	sequence := baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`)
	_, err := db.Exec(`INSERT INTO nodes VALUES('temporary','value',NULL); DELETE FROM nodes WHERE id='temporary'`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, sequence, baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Equal(t, "0", baselineText(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	_, err = db.Exec(`DELETE FROM nodes WHERE id='Q'`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, "deleted", baselineText(t, db, `SELECT server_kind FROM _sync_row_state WHERE key_json='{"id":"Q"}'`))
	tombstoneVersion := baselineText(t, db, `SELECT row_version FROM _sync_row_state WHERE key_json='{"id":"Q"}'`)
	_, err = db.Exec(`INSERT INTO nodes VALUES('Q','reinsert',NULL)`)
	require.NoError(t, err)
	require.Equal(t, "deleted", baselineText(t, db, `SELECT base_kind FROM _sync_dirty_rows WHERE key_json='{"id":"Q"}'`))
	require.Equal(t, tombstoneVersion, baselineText(t, db, `SELECT base_row_version FROM _sync_dirty_rows WHERE key_json='{"id":"Q"}'`))
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, "reinsert", baselineText(t, db, `SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json='{"id":"Q"}'`))
}

func TestBaselineLifecycle_RejectsInvalidProvenance(t *testing.T) {
	for _, owner := range []string{"_sync_dirty_rows", "_sync_outbox_rows"} {
		for _, mutation := range []string{"'$.generation_id',NULL", "'$.account_scope.account_id','other-account'", "'$.generation_id','1'", "'$.generation_id',-1", "'$.generation_id',2", "'$.database_id','other-db'", "'$.service_id','other-service'", "'$.generation_fingerprint','other-generation'", "'$.admission','unknown'", "'$.local_version','0'", "'$.local_version',-1", "'$.physical_shape',NULL", "'$.account_scope.binding','anonymous'", "'$.account_scope.account_id',1", "'$.origin',NULL", "'$.origin','unknown'", "'$.origin','snapshot','$.bundle_sequence',NULL", "'$.origin','never_published'", "'$.origin_source_id',NULL", "'$.origin_source_id',1", "'$.origin_source_id',''", "'$.bundle_sequence','1'", "'$.bundle_sequence',-1", "'$.bundle_sequence',1.5", "'$.authoritative_version','1'", "'$.authoritative_version',NULL"} {
			t.Run(owner+mutation, func(t *testing.T) {
				c, db := newA1SurvivorClient(t)
				_, err := db.Exec(`UPDATE nodes SET name='pending' WHERE id='C'`)
				require.NoError(t, err)
				if owner == "_sync_outbox_rows" {
					_, err = c.ensurePushOutboundSnapshot(context.Background())
					require.NoError(t, err)
				}
				_, err = db.Exec("UPDATE " + owner + " SET base_provenance=json_set(base_provenance," + mutation + ")")
				require.NoError(t, err)
				before := baselineIntentState(t, db)
				sequence := baselineText(t, db, "SELECT next_source_bundle_id FROM _sync_source_state")
				_, err = c.ensurePushOutboundSnapshot(context.Background())
				require.ErrorAs(t, err, new(*ResolutionBaseUnavailableError))
				require.Equal(t, before, baselineIntentState(t, db))
				require.Equal(t, "1", baselineText(t, db, "SELECT count(*) FROM "+owner))
				require.Equal(t, sequence, baselineText(t, db, "SELECT next_source_bundle_id FROM _sync_source_state"))
				if owner == "_sync_dirty_rows" {
					require.Equal(t, "0", baselineText(t, db, "SELECT count(*) FROM _sync_outbox_rows"))
				}
			})
		}
	}
}

func TestBaselineLifecycle_IncompleteOwnerRollsBackPull(t *testing.T) {
	for _, corrupt := range []string{
		`'{}'`,
		`json_remove(server_payload,'$.name')`,
		`json_set(server_payload,'$.parent_id.storage','null')`,
		`json_remove(json_set(server_payload,'$.ID',json_extract(server_payload,'$.id')),'$.name')`,
		`json_remove(json_set(server_payload,'$.ID',json_extract(server_payload,'$.id')),'$.id')`,
	} {
		t.Run(corrupt, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			_, err := db.Exec(`UPDATE _sync_row_state SET server_payload=` + corrupt + ` WHERE key_json='{"id":"C"}'`)
			require.NoError(t, err)
			before := baselineText(t, db, `SELECT server_payload FROM _sync_row_state WHERE key_json='{"id":"C"}'`)
			checkpoint := baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`)
			revision := baselineText(t, db, `SELECT baseline_revision FROM _sync_conflict_state`)
			err = c.applyPulledBundleLocked(context.Background(), &oversync.Bundle{BundleSeq: 2, Rows: []oversync.BundleRow{{Schema: "main", Table: "nodes", Key: mustBundleKey("C"), Op: "UPDATE", RowVersion: 2, Payload: []byte(`{"id":"C","name":"remote","parent_id":"Q"}`)}}})
			require.Error(t, err)
			require.Equal(t, before, baselineText(t, db, `SELECT server_payload FROM _sync_row_state WHERE key_json='{"id":"C"}'`))
			require.Equal(t, "1", baselineText(t, db, `SELECT row_version FROM _sync_row_state WHERE key_json='{"id":"C"}'`))
			require.Equal(t, "child", baselineText(t, db, `SELECT name FROM nodes WHERE id='C'`))
			require.Equal(t, checkpoint, baselineText(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
			require.Equal(t, revision, baselineText(t, db, `SELECT baseline_revision FROM _sync_conflict_state`))
		})
	}
}

func baselineIntentState(t *testing.T, db *sql.DB) []string {
	return []string{
		baselineText(t, db, `SELECT json_group_array(json_array(schema_name,table_name,key_json,op,base_row_version,payload,base_kind,base_payload,base_provenance,dirty_ordinal,updated_at)) FROM _sync_dirty_rows`),
		baselineText(t, db, `SELECT json_group_array(json_array(source_bundle_id,row_ordinal,schema_name,table_name,key_json,wire_key_json,op,base_row_version,wire_payload,local_payload,base_kind,base_payload,base_provenance)) FROM _sync_outbox_rows`),
		baselineText(t, db, `SELECT json_group_array(json_array(state,source_id,source_bundle_id,canonical_request_hash,row_count,initialization_id)) FROM _sync_outbox_bundle`),
	}
}

func TestBaselineLifecycle_HistoricalSourceSurvivesFreezeAndReplay(t *testing.T) {
	for _, unpublished := range []bool{false, true} {
		t.Run(fmt.Sprint(unpublished), func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			statement := `UPDATE nodes SET name='pending' WHERE id='C'`
			source := "retired-source"
			if unpublished {
				statement = `INSERT INTO nodes VALUES('N','new',NULL)`
				source = ""
			}
			_, err := db.Exec(statement)
			require.NoError(t, err)
			_, err = db.Exec(`UPDATE _sync_dirty_rows SET base_provenance=json_set(base_provenance,'$.origin_source_id',?)`, source)
			require.NoError(t, err)
			provenance := baselineText(t, db, `SELECT base_provenance FROM _sync_dirty_rows`)
			frozen, err := c.ensurePushOutboundSnapshot(context.Background())
			require.NoError(t, err)
			require.Equal(t, provenance, frozen.Rows[0].BaseProvenance)
			replay, err := c.loadPushOutboundSnapshot(context.Background())
			require.NoError(t, err)
			require.Equal(t, provenance, replay.Rows[0].BaseProvenance)
		})
	}
}

func TestBaselineLifecycle_AliasedBaseBlocksFreezeAndReplay(t *testing.T) {
	for _, owner := range []string{"_sync_dirty_rows", "_sync_outbox_rows"} {
		t.Run(owner, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			_, err := db.Exec(`UPDATE nodes SET name='pending' WHERE id='C'`)
			require.NoError(t, err)
			if owner == "_sync_outbox_rows" {
				_, err = c.ensurePushOutboundSnapshot(context.Background())
				require.NoError(t, err)
			}
			_, err = db.Exec("UPDATE " + owner + " SET base_payload=json_remove(json_set(base_payload,'$.ID',json_extract(base_payload,'$.id')),'$.name')")
			require.NoError(t, err)
			before := baselineIntentState(t, db)
			sequence := baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`)
			_, err = c.ensurePushOutboundSnapshot(context.Background())
			require.ErrorAs(t, err, new(*ResolutionBaseUnavailableError))
			require.Equal(t, before, baselineIntentState(t, db))
			require.Equal(t, sequence, baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
		})
	}
}
