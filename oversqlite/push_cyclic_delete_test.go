// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func cyclicDeleteClient(t *testing.T) (*Client, *sql.DB, *mockPushServer) {
	t.Helper()
	client, db := newBundleClient(t, "main", []SyncTable{
		{TableName: "teams", SyncKeyColumnName: "id"},
		{TableName: "team_members", SyncKeyColumnName: "id"},
	}, `CREATE TABLE teams (
		id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, captain_member_id TEXT,
		FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY IMMEDIATE
	)`, `CREATE TABLE team_members (
		id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, team_id TEXT NOT NULL,
		FOREIGN KEY (team_id) REFERENCES teams(id) DEFERRABLE INITIALLY IMMEDIATE
	)`)
	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`PRAGMA defer_foreign_keys = ON`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO teams VALUES ('team-1','Core','member-1')`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO team_members VALUES ('member-1','Ada','team-1')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	return client, db, server
}

func deleteCyclicPair(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`PRAGMA defer_foreign_keys = ON`)
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM teams WHERE id='team-1'`)
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM team_members WHERE id='member-1'`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func pushStateCounts(t *testing.T, db *sql.DB) (dirty, outbound int, nextSource int64) {
	t.Helper()
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outbound))
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state LIMIT 1`).Scan(&nextSource))
	return
}

func TestPushPending_CyclicDeletesStayTogetherAcrossSoftTargets(t *testing.T) {
	for _, target := range []string{"rows", "bytes", "legacy"} {
		t.Run(target, func(t *testing.T) {
			client, db, server := cyclicDeleteClient(t)
			deleteCyclicPair(t, db)
			if target == "legacy" {
				_, err := db.Exec(`UPDATE _sync_dirty_rows SET payload=NULL WHERE op='DELETE'`)
				require.NoError(t, err)
			}
			if target != "bytes" {
				client.config.UploadLimit = 1
			} else {
				client.config.UploadByteLimit = 1
			}
			dirty, outbound, next := pushStateCounts(t, db)
			require.Equal(t, 2, dirty)
			require.Zero(t, outbound)
			_, err := client.PushPending(context.Background())
			require.NoError(t, err)
			require.Len(t, server.pushRequests, 2)
			require.Len(t, expandCapturedPushRequest(t, client, server.pushRequests[1]), 2)
			dirty, outbound, after := pushStateCounts(t, db)
			require.Zero(t, dirty)
			require.Zero(t, outbound)
			require.Equal(t, next+1, after)
		})
	}
}

func TestPushPending_CyclicDeleteHardLimitFailsBeforeFreeze(t *testing.T) {
	client, db, server := cyclicDeleteClient(t)
	deleteCyclicPair(t, db)
	client.discovery.Limits.MaxPushRows = 1
	_, _, next := pushStateCounts(t, db)
	_, err := client.PushPending(context.Background())
	var limit *PushDependencyGroupExceedsAdmittedLimitError
	require.True(t, errors.As(err, &limit), "error: %v", err)
	require.EqualValues(t, 2, limit.ComponentRows)
	require.Len(t, server.pushRequests, 1)
	dirty, outbound, after := pushStateCounts(t, db)
	require.Equal(t, 2, dirty)
	require.Zero(t, outbound)
	require.Equal(t, next, after)
}

func TestPushPending_UpdateThenDeleteOrdersSplittableServerBaseDependencies(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}},
		`CREATE TABLE nodes (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED)`)
	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}
	_, err := db.Exec(`INSERT INTO nodes VALUES ('P','parent',NULL),('C','child','P')`)
	require.NoError(t, err)
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	client.config.UploadLimit = 1
	_, err = db.Exec(`UPDATE nodes SET name='changed' WHERE id='P'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE nodes SET parent_id=NULL WHERE id='C'`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM nodes WHERE id='P'`)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM nodes WHERE id='C'`)
	require.NoError(t, err)
	var baseParent string
	require.NoError(t, db.QueryRow(`SELECT json_extract(base_payload,'$.parent_id.value') FROM _sync_dirty_rows WHERE key_json='{"id":"C"}'`).Scan(&baseParent))
	require.Equal(t, "P", baseParent)
	before := qualificationState(t, db, "nodes")
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, 2)
	rows := expandCapturedPushRequest(t, client, server.pushRequests[1])
	require.Len(t, rows, 1)
	require.Equal(t, "C", rows[0].Key["id"])
	afterChild := qualificationState(t, db, "nodes")
	require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", "P"), qualificationRow(t, afterChild, "_sync_dirty_rows", "P"))
	require.Equal(t, qualificationRow(t, before, "_sync_row_state", "P"), qualificationRow(t, afterChild, "_sync_row_state", "P"))
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	rows = expandCapturedPushRequest(t, client, server.pushRequests[2])
	require.Len(t, rows, 1)
	require.Equal(t, "P", rows[0].Key["id"])
	dirty, outbound, _ := pushStateCounts(t, db)
	require.Zero(t, dirty)
	require.Zero(t, outbound)
}

func TestPushPending_FKDeletePeerBeyondInitialBatchPage(t *testing.T) {
	client, db := newBundleClient(t, "main", []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}},
		`CREATE TABLE nodes (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED)`)
	server := newMockPushServer(t)
	client.HTTP = &http.Client{Transport: server}
	_, err := db.Exec(`INSERT INTO nodes VALUES ('P','parent',NULL),('C','child','P'),('U1','one',NULL),('U2','two',NULL),('U3','three',NULL)`)
	require.NoError(t, err)
	_, err = client.PushPending(context.Background())
	require.NoError(t, err)
	client.discovery.Limits.MaxPushRows = 2
	client.config.UploadLimit = 1
	for _, statement := range []string{
		`UPDATE nodes SET name='changed' WHERE id='P'`,
		`UPDATE nodes SET name='changed' WHERE id='U1'`,
		`UPDATE nodes SET name='changed' WHERE id='U2'`,
		`UPDATE nodes SET name='changed' WHERE id='U3'`,
		`UPDATE nodes SET parent_id=NULL WHERE id='C'`,
		`DELETE FROM nodes WHERE id='P'`,
		`DELETE FROM nodes WHERE id='C'`,
		`DELETE FROM nodes WHERE id='U1'`,
		`DELETE FROM nodes WHERE id='U2'`,
		`DELETE FROM nodes WHERE id='U3'`,
	} {
		_, err = db.Exec(statement)
		require.NoError(t, err)
	}
	before := qualificationState(t, db, "nodes")
	for i, key := range []string{"U1", "U2", "U3", "C", "P"} {
		_, err = client.PushPending(context.Background())
		require.NoError(t, err)
		rows := expandCapturedPushRequest(t, client, server.pushRequests[i+1])
		require.Len(t, rows, 1)
		require.Equal(t, key, rows[0].Key["id"])
		dirty, outbound, _ := pushStateCounts(t, db)
		require.Equal(t, 4-i, dirty)
		require.Zero(t, outbound)
		if i < 3 {
			after := qualificationState(t, db, "nodes")
			for _, blocked := range []string{"P", "C"} {
				require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", blocked), qualificationRow(t, after, "_sync_dirty_rows", blocked))
				require.Equal(t, qualificationRow(t, before, "_sync_row_state", blocked), qualificationRow(t, after, "_sync_row_state", blocked))
			}
		}
	}
}

func TestRuntimeCaptureRejectsSupersededTrigger(t *testing.T) {
	client, db, _ := cyclicDeleteClient(t)
	validated, err := validateSyncTables(db, client.tableInfo, client.config.Tables)
	require.NoError(t, err)
	table := client.config.Tables[0]
	name := "trg_" + table.TableName + "_ad"
	_, err = db.Exec("DROP TRIGGER " + quoteIdent(name))
	require.NoError(t, err)
	_, err = db.Exec("CREATE TRIGGER " + quoteIdent(name) + " AFTER DELETE ON " + quoteIdent(table.TableName) + " BEGIN SELECT 1; END")
	require.NoError(t, err)
	require.Error(t, validateRuntimeTriggerInventory(db, client, validated))
	require.Error(t, createTriggersForTableWithInfo(db, client.schemaForTable(table.TableName), table, validated.tableInfoByKey[table.TableName]))
	var body string
	require.NoError(t, db.QueryRow("SELECT sql FROM sqlite_master WHERE name=?", name).Scan(&body))
	require.Contains(t, body, "SELECT 1")
}
