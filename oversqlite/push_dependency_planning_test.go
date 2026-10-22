// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0
package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type pushEntryObservedTx struct {
	sqliteTransaction
	t            *testing.T
	client       *Client
	stepWork     int64
	dirtyQueries []string
}

func (tx *pushEntryObservedTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(strings.ToLower(query), "from _sync_dirty_rows") {
		tx.dirtyQueries = append(tx.dirtyQueries, query)
		require.Equal(tx.t, tx.stepWork, tx.client.syncWorkUsed, "dirty query must follow full step reservation")
		require.True(tx.t, strings.HasSuffix(strings.TrimSpace(query), "LIMIT 1"), query)
		require.NotContains(tx.t, strings.ToLower(query), "count(")
	}
	return tx.sqliteTransaction.QueryRowContext(ctx, query, args...)
}

func TestPushDependencyPlanning_EntryLookupIsBoundedAndCharged(t *testing.T) {
	for _, count := range []int{0, 1, 10000} {
		for _, oneStep := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/one-step=%t", count, oneStep), func(t *testing.T) {
				c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
				if count > 0 {
					_, err := db.Exec(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO users SELECT CAST(i AS TEXT),'value' FROM n`, count)
					require.NoError(t, err)
				}
				before := qualificationState(t, db, "users")
				step, err := c.pushPreparationStepWork()
				require.NoError(t, err)
				c.config.MaxSyncWorkUnitsPerInvocation = 1
				if oneStep {
					c.config.MaxSyncWorkUnitsPerInvocation = step
				}
				base, err := db.Begin()
				require.NoError(t, err)
				tx := &pushEntryObservedTx{sqliteTransaction: base, t: t, client: c, stepWork: step}
				_, ready, err := c.advancePushPlanInTx(context.Background(), tx)
				require.NoError(t, err)
				require.Equal(t, count == 0, ready)
				require.NoError(t, base.Commit())
				require.Equal(t, step, c.syncWorkUsed)
				expectedQueries := 3
				if count == 0 {
					expectedQueries = 1
				}
				require.Len(t, tx.dirtyQueries, expectedQueries)
				if !oneStep {
					next, e := db.Begin()
					require.NoError(t, e)
					_, _, e = c.advancePushPlanInTx(context.Background(), next)
					var work *SyncWorkLimitError
					require.ErrorAs(t, e, &work)
					require.Equal(t, step, work.Used)
					require.NoError(t, next.Rollback())
				}
				require.Equal(t, before, qualificationState(t, db, "users"))
				var nodes int
				require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_nodes`).Scan(&nodes))
				expectedNodes := 0
				if count > 0 {
					expectedNodes = 1
				}
				require.Equal(t, expectedNodes, nodes)
				{
					rows, e := db.Query("EXPLAIN " + tx.dirtyQueries[0])
					require.NoError(t, e)
					var opcodes []string
					for rows.Next() {
						var addr, p1, p2, p3, p5 int
						var opcode string
						var p4, comment any
						require.NoError(t, rows.Scan(&addr, &opcode, &p1, &p2, &p3, &p4, &p5, &comment))
						opcodes = append(opcodes, opcode)
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
					require.Contains(t, opcodes, "DecrJumpZero")
					require.NotContains(t, opcodes, "Count")
					require.NotContains(t, opcodes, "SorterOpen")
				}
				t.Logf("bounded entry dirty=%d used=%d dirty queries=%d", count, c.syncWorkUsed, len(tx.dirtyQueries))
			})
		}
	}
}

func TestPushDependencyPlanning_ReparentBeyondActualHardLookahead(t *testing.T) {
	for _, newParent := range []bool{false, true} {
		t.Run(fmt.Sprint(newParent), func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "employees", SyncKeyColumnName: "id"}}, `CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)`)
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: server}
			_, err := db.Exec(`INSERT INTO employees VALUES('P',NULL,'parent'),('Q',NULL,'other'),('C','P','child'),('D','C','clean'),('f1',NULL,'one'),('f2',NULL,'two'),('f3',NULL,'three'),('f4',NULL,'four')`)
			require.NoError(t, err)
			_, err = c.PushPending(context.Background())
			require.NoError(t, err)
			c.discovery.Limits.MaxPushRows = 2
			c.config.UploadLimit = 2
			tx, err := db.Begin()
			require.NoError(t, err)
			_, err = tx.Exec(`DELETE FROM employees WHERE id='P'`)
			require.NoError(t, err)
			for i := 1; i <= 4; i++ {
				_, err = tx.Exec(`UPDATE employees SET name=name||'!' WHERE id=?`, fmt.Sprintf("f%d", i))
				require.NoError(t, err)
			}
			parent := "Q"
			if newParent {
				parent = "N"
			}
			_, err = tx.Exec(`UPDATE employees SET manager_id=? WHERE id='C'`, parent)
			require.NoError(t, err)
			if newParent {
				_, err = tx.Exec(`INSERT INTO employees VALUES('N',NULL,'new')`)
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			var pOrdinal, cOrdinal int64
			require.NoError(t, db.QueryRow(`SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("P")).Scan(&pOrdinal))
			require.NoError(t, db.QueryRow(`SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json=?`, rowKeyJSON("C")).Scan(&cOrdinal))
			require.Equal(t, int64(5), cOrdinal-pOrdinal)
			require.Greater(t, cOrdinal-pOrdinal, int64(c.discovery.Limits.MaxPushRows+1))
			before := qualificationState(t, db, "employees")
			clean := qualificationRow(t, before, "_sync_row_state", "D")
			cleanBusiness := baselineText(t, db, `SELECT json_array(id,manager_id,name) FROM employees WHERE id='D'`)
			cleanEdges := baselineText(t, db, `SELECT json_group_array(json_array(view,child_key,parent_key,owner_revision)) FROM _sync_relationships WHERE child_key='{"id":"D"}'`)
			calls := 3
			if newParent {
				calls = 4
			}
			sent := map[string]bool{}
			for call := 0; call < calls; call++ {
				_, err = c.PushPending(context.Background())
				require.NoError(t, err)
				req := server.pushRequests[len(server.pushRequests)-1]
				rows := expandCapturedPushRequest(t, c, req)
				require.LessOrEqual(t, len(rows), 2)
				for _, row := range rows {
					key := fmt.Sprint(row.Key["id"])
					if key == "P" {
						require.True(t, sent["C"] || func() bool {
							for _, r := range rows {
								if r.Key["id"] == "C" {
									return true
								}
							}
							return false
						}(), "P may disappear only with/after C")
					}
					if key == "C" && newParent {
						require.True(t, sent["N"] || func() bool {
							for _, r := range rows {
								if r.Key["id"] == "N" {
									return true
								}
							}
							return false
						}())
					}
					sent[key] = true
				}
				after := qualificationState(t, db, "employees")
				require.Equal(t, clean, qualificationRow(t, after, "_sync_row_state", "D"))
				require.Equal(t, cleanBusiness, baselineText(t, db, `SELECT json_array(id,manager_id,name) FROM employees WHERE id='D'`))
				require.Equal(t, cleanEdges, baselineText(t, db, `SELECT json_group_array(json_array(view,child_key,parent_key,owner_revision)) FROM _sync_relationships WHERE child_key='{"id":"D"}'`))
				require.Empty(t, after["_sync_outbox_rows"])
				encoded, e := json.Marshal(struct {
					Call   int
					State  qualificationVector
					Wire   any
					Remote any
				}{call + 1, after, req, server.committedBySeq})
				require.NoError(t, e)
				t.Log(string(encoded))
			}
			require.True(t, sent["P"])
			require.True(t, sent["C"])
			final := qualificationState(t, db, "employees")
			require.Empty(t, final["_sync_dirty_rows"])
			require.Equal(t, parent, decodeDirtyPayload(t, qualificationText(qualificationRow(t, final, "_sync_row_state", "C"), "server_payload").(string))["manager_id"])
		})
	}
}

func TestPushDependencyPlanning_ExactEncodedLimitsAndDigitWidth(t *testing.T) {
	for _, boundary := range []string{"exact", "envelope-minus-one", "row-minus-one", "digit-width"} {
		t.Run(boundary, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
			_, err := db.Exec(`INSERT INTO users VALUES('one','quoted" unicode ü')`)
			require.NoError(t, err)
			_, err = db.Exec(`UPDATE _sync_source_state SET next_source_bundle_id=9`)
			require.NoError(t, err)
			tx, err := db.Begin()
			require.NoError(t, err)
			id, _, captures, err := c.collectDirtyRowsForPushInTx(context.Background(), tx)
			require.NoError(t, err)
			require.Equal(t, int64(9), id)
			require.NoError(t, tx.Rollback())
			body, err := c.compactPushRequestBody(9, c.pendingInitializationID, captures)
			require.NoError(t, err)
			body10, err := c.compactPushRequestBody(10, c.pendingInitializationID, captures)
			require.NoError(t, err)
			require.Equal(t, len(body)+1, len(body10))
			logical, err := c.buildPushRequestRows(captures)
			require.NoError(t, err)
			rowBytes, err := oversync.ExpandedRowDecodedBytes(logical[0])
			require.NoError(t, err)
			c.discovery.Limits.MaxPushDecodedBytes = int64(len(body))
			c.discovery.Limits.MaxRowDecodedBytes = rowBytes
			switch boundary {
			case "envelope-minus-one":
				c.discovery.Limits.MaxPushDecodedBytes--
			case "row-minus-one":
				c.discovery.Limits.MaxRowDecodedBytes--
			case "digit-width":
				_, err = db.Exec(`UPDATE _sync_source_state SET next_source_bundle_id=10`)
				require.NoError(t, err)
			}
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: server}
			before := qualificationState(t, db, "users")
			_, err = c.PushPending(context.Background())
			if boundary == "exact" {
				require.NoError(t, err)
				require.Len(t, server.pushRequests, 1)
				encoded, e := json.Marshal(server.pushRequests[0])
				require.NoError(t, e)
				require.Equal(t, len(body), len(encoded))
				require.Equal(t, string(body), string(encoded))
				require.Empty(t, qualificationState(t, db, "users")["_sync_dirty_rows"])
			} else {
				if boundary == "row-minus-one" {
					var limit *PushRowExceedsAdmittedLimitError
					require.ErrorAs(t, err, &limit)
				} else {
					var limit *InvalidAdmittedContractError
					require.ErrorAs(t, err, &limit)
				}
				require.Empty(t, server.pushRequests)
				require.Equal(t, before, qualificationState(t, db, "users"))
			}
			t.Logf("rowBytes=%d completeBytes9=%d completeBytes10=%d fullState=%+v", rowBytes, len(body), len(body10), qualificationState(t, db, "users"))
		})
	}
}

func TestPushDependencyPlanning_SharedWorkspaceAndCancelledResume(t *testing.T) {
	for _, dimension := range []string{"entries", "bytes", "cancel"} {
		t.Run(dimension, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
			_, err := db.Exec(`INSERT INTO users VALUES('one','value')`)
			require.NoError(t, err)
			tx, err := db.Begin()
			require.NoError(t, err)
			require.NoError(t, saveApplyPlan(context.Background(), tx, &applyPlan{unit: "independent-apply", guard: "retained", phase: "roots"}))
			require.NoError(t, tx.Commit())
			before := qualificationState(t, db, "users")
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: server}
			if dimension == "cancel" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err = c.PushPending(ctx)
				require.ErrorIs(t, err, context.Canceled)
			} else {
				if dimension == "entries" {
					c.config.MaxSyncWorkspaceEntries = 37
				} else {
					c.config.MaxSyncWorkspaceBytes = 8192
				}
				_, err = c.PushPending(context.Background())
				var limit *SyncWorkspaceLimitError
				require.ErrorAs(t, err, &limit)
				require.Equal(t, dimension, limit.Dimension)
				require.GreaterOrEqual(t, limit.Used, int64(2))
			}
			require.Equal(t, before, qualificationState(t, db, "users"))
			require.Empty(t, server.pushRequests)
			var retained string
			require.NoError(t, db.QueryRow(`SELECT unit FROM _sync_apply_plan`).Scan(&retained))
			require.Equal(t, "independent-apply", retained)
			c.config.MaxSyncWorkspaceEntries = 100000
			c.config.MaxSyncWorkspaceBytes = 256 << 20
			_, err = c.PushPending(context.Background())
			require.NoError(t, err)
			require.Len(t, server.pushRequests, 1)
			require.Empty(t, qualificationState(t, db, "users")["_sync_dirty_rows"])
			var plans int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_apply_plan`).Scan(&plans))
			require.Zero(t, plans, "successful settlement replaces the obsolete apply unit through its existing owner")
		})
	}
}

func TestPushDependencyPlanning_SecondConnectionInvalidatesReadyGuard(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
	_, err := db.Exec(`INSERT INTO users VALUES('one','original')`)
	require.NoError(t, err)
	for {
		tx, e := db.Begin()
		require.NoError(t, e)
		_, ready, e := c.advancePushPlanInTx(context.Background(), tx)
		require.NoError(t, e)
		require.NoError(t, tx.Commit())
		if ready {
			break
		}
	}
	var oldUnit string
	require.NoError(t, db.QueryRow(`SELECT unit FROM _sync_push_plan`).Scan(&oldUnit))
	dbPath := filepath.Join(t.TempDir(), "guard.sqlite")
	_, err = db.Exec(`VACUUM INTO ?`, dbPath)
	require.NoError(t, err)
	other, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	require.NoError(t, err)
	defer other.Close()
	other.SetMaxOpenConns(1)
	cfg := *c.config
	reopenedDB, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	require.NoError(t, err)
	defer reopenedDB.Close()
	reopenedDB.SetMaxOpenConns(1)
	reopened, err := newAdmittedFixtureClient(reopenedDB, c.BaseURL, c.Token, &cfg)
	require.NoError(t, err)
	attachTestClient(t, reopened, "bundle-user", "bundle-device")
	_, err = other.Exec(`UPDATE users SET name='second-writer' WHERE id='one'`)
	require.NoError(t, err)
	before := qualificationState(t, reopenedDB, "users")
	server := newMockPushServer(t)
	reopened.HTTP = &http.Client{Transport: server}
	_, err = reopened.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, 1)
	rows := expandCapturedPushRequest(t, reopened, server.pushRequests[0])
	require.Len(t, rows, 1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(rows[0].Payload, &payload))
	require.Equal(t, "second-writer", payload["name"])
	require.Equal(t, int64(1), server.pushRequests[0].SourceBundleID)
	require.Empty(t, qualificationState(t, reopenedDB, "users")["_sync_dirty_rows"])
	t.Logf("invalidated unit=%s before=%+v after=%+v wire=%+v remote=%+v", oldUnit, before, qualificationState(t, reopenedDB, "users"), server.pushRequests[0], server.committedBySeq)
}

func TestPushDependencyPlanning_InsertComponents(t *testing.T) {
	for _, shape := range []string{"chain", "self", "cycle"} {
		t.Run(shape, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "employees", SyncKeyColumnName: "id"}}, `CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)`)
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: server}
			values := `('C','B','child'),('B','A','middle'),('A',NULL,'root')`
			if shape == "self" {
				values = `('S','S','self')`
			}
			if shape == "cycle" {
				values = `('X','Y','x'),('Y','X','y')`
			}
			_, err := db.Exec(`INSERT INTO employees VALUES` + values)
			require.NoError(t, err)
			c.discovery.Limits.MaxPushRows = 1
			c.config.UploadLimit = 1
			before := qualificationState(t, db, "employees")
			if shape == "cycle" {
				_, err = c.PushPending(context.Background())
				var blocked *PushDependencyGroupExceedsAdmittedLimitError
				require.ErrorAs(t, err, &blocked)
				require.Equal(t, int64(2), blocked.ComponentRows)
				require.Empty(t, server.pushRequests)
				require.Equal(t, before, qualificationState(t, db, "employees"))
				return
			}
			want := []string{"A", "B", "C"}
			if shape == "self" {
				want = []string{"S"}
			}
			for i, key := range want {
				_, err = c.PushPending(context.Background())
				require.NoError(t, err)
				rows := expandCapturedPushRequest(t, c, server.pushRequests[i])
				require.Len(t, rows, 1)
				require.Equal(t, key, rows[0].Key["id"])
				t.Logf("selected=%+v completeState=%+v remote=%+v", server.pushRequests[i], qualificationState(t, db, "employees"), server.committedBySeq)
			}
			require.Empty(t, qualificationState(t, db, "employees")["_sync_dirty_rows"])
		})
	}
}

func TestPushDependencyPlanning_MissingAndUnknownParentPreserveConfirmedPrefix(t *testing.T) {
	for _, kind := range []string{"absent", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "employees", SyncKeyColumnName: "id"}}, `CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)`)
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: server}
			_, err := db.Exec(`INSERT INTO employees VALUES('P',NULL,'parent'),('C','P','child'),('D',NULL,'clean')`)
			require.NoError(t, err)
			_, err = c.PushPending(context.Background())
			require.NoError(t, err)
			known := 1
			if kind == "unknown" {
				known = 0
			}
			_, err = db.Exec(`UPDATE _sync_row_state SET server_kind=?,row_version=0,server_payload=NULL,relationships_known=? WHERE key_json=?`, kind, known, rowKeyJSON("P"))
			require.NoError(t, err)
			_, err = db.Exec(`UPDATE employees SET name='changed' WHERE id='C'; INSERT INTO employees VALUES('I',NULL,'independent')`)
			require.NoError(t, err)
			before := qualificationState(t, db, "employees")
			_, err = c.Sync(context.Background())
			var blocked *FKReconciliationRequiredError
			require.ErrorAs(t, err, &blocked)
			require.Equal(t, SyncProgress{DirtyRows: 1, CommittedBundles: 1}, blocked.Progress)
			require.Len(t, server.pushRequests, 2)
			require.Equal(t, "I", expandCapturedPushRequest(t, c, server.pushRequests[1])[0].Key["id"])
			after := qualificationState(t, db, "employees")
			for _, key := range []string{"P", "C", "D"} {
				require.Equal(t, qualificationRow(t, before, "_sync_row_state", key), qualificationRow(t, after, "_sync_row_state", key))
			}
			require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", "C"), qualificationRow(t, after, "_sync_dirty_rows", "C"))
			require.Empty(t, after["_sync_outbox_rows"])
			t.Logf("failure=%+v completeState=%+v remote=%+v", blocked, after, server.committedBySeq)
		})
	}
}

func TestPushDependencyPlanning_FreshProcessResume(t *testing.T) {
	if dbPath := os.Getenv("A2_TEST_RESTART_DB"); dbPath != "" {
		db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
		cfg = testConfigWithLocalContract(t, db, cfg)
		c, err := newAdmittedFixtureClient(db, "http://localhost", func(context.Context) (string, error) { return "token", nil }, cfg)
		require.NoError(t, err)
		attachTestClient(t, c, "bundle-user", "bundle-device")
		before := qualificationState(t, db, "users")
		var unit string
		require.NoError(t, db.QueryRow(`SELECT unit FROM _sync_push_plan`).Scan(&unit))
		require.Equal(t, os.Getenv("A2_TEST_RESTART_UNIT"), unit)
		step, err := c.pushPreparationStepWork()
		require.NoError(t, err)
		c.config.MaxSyncWorkUnitsPerInvocation = 2 * step
		server := newMockPushServer(t)
		c.HTTP = &http.Client{Transport: server}
		_, err = c.preparePushPlanSlice(context.Background())
		err = c.withSyncErrorProgress(context.Background(), err)
		var work *SyncWorkLimitError
		require.ErrorAs(t, err, &work)
		var nodes int64
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_nodes`).Scan(&nodes))
		require.Equal(t, int64(4), nodes)
		require.Equal(t, before, qualificationState(t, db, "users"))
		require.Empty(t, server.pushRequests)
		c.config.MaxSyncWorkUnitsPerInvocation = 1_000_000
		_, err = c.PushPending(context.Background())
		require.NoError(t, err)
		require.Len(t, server.pushRequests, 1)
		require.Empty(t, qualificationState(t, db, "users")["_sync_dirty_rows"])
		t.Logf("fresh-process unit=%s completeState=%+v wire=%+v remote=%+v", unit, qualificationState(t, db, "users"), server.pushRequests, server.committedBySeq)
		return
	}
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
	for i := 0; i < 8; i++ {
		_, err := db.Exec(`INSERT INTO users VALUES(?,?)`, fmt.Sprint(i), "value")
		require.NoError(t, err)
	}
	step, err := c.pushPreparationStepWork()
	require.NoError(t, err)
	c.config.MaxSyncWorkUnitsPerInvocation = 2 * step
	_, err = c.preparePushPlanSlice(context.Background())
	err = c.withSyncErrorProgress(context.Background(), err)
	var work *SyncWorkLimitError
	require.ErrorAs(t, err, &work)
	var unit string
	require.NoError(t, db.QueryRow(`SELECT unit FROM _sync_push_plan`).Scan(&unit))
	dbPath := filepath.Join(t.TempDir(), "restart.sqlite")
	_, err = db.Exec(`VACUUM INTO ?`, dbPath)
	require.NoError(t, err)
	child := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestPushDependencyPlanning_FreshProcessResume$", "-test.v", "-test.timeout=60s")
	child.Env = append(os.Environ(), "A2_TEST_RESTART_DB="+dbPath, "A2_TEST_RESTART_UNIT="+unit)
	output, err := child.CombinedOutput()
	t.Log(string(output))
	require.NoError(t, err)
}

func TestPushDependencyPlanning_IndependentWorkAndDiagnosticLifecycle(t *testing.T) {
	c, db, server := cyclicDeleteClient(t)
	deleteCyclicPair(t, db)
	_, err := db.Exec(`INSERT INTO teams VALUES('independent','safe',NULL)`)
	require.NoError(t, err)
	c.discovery.Limits.MaxPushRows = 1
	c.config.UploadLimit = 1
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, 2)
	require.Equal(t, "independent", expandCapturedPushRequest(t, c, server.pushRequests[1])[0].Key["id"])
	before := qualificationState(t, db, "teams")
	_, err = c.PushPending(context.Background())
	var oversized *PushDependencyGroupExceedsAdmittedLimitError
	require.ErrorAs(t, err, &oversized)
	require.Equal(t, int64(2), oversized.ComponentRows)
	require.Len(t, oversized.Sample, 2)
	require.False(t, oversized.HasMore)
	require.Equal(t, SyncProgress{DirtyRows: 2}, oversized.Progress)
	require.Equal(t, before, qualificationState(t, db, "teams"))
	require.Len(t, server.pushRequests, 2)
	first, err := c.ReadPushDependencyDiagnostic(context.Background(), oversized.DiagnosticID, "", 1)
	require.NoError(t, err)
	require.Len(t, first.Identities, 1)
	require.NotEmpty(t, first.NextCursor)
	second, err := c.ReadPushDependencyDiagnostic(context.Background(), oversized.DiagnosticID, first.NextCursor, 1)
	require.NoError(t, err)
	require.Len(t, second.Identities, 1)
	require.Empty(t, second.NextCursor)
	_, err = c.ReadPushDependencyDiagnostic(context.Background(), oversized.DiagnosticID, first.NextCursor+"x", 1)
	var stale *StalePushDependencyDiagnosticError
	require.ErrorAs(t, err, &stale)
	for _, modified := range []string{"0" + first.NextCursor, "+" + first.NextCursor} {
		_, err = c.ReadPushDependencyDiagnostic(context.Background(), oversized.DiagnosticID, modified, 1)
		require.ErrorAs(t, err, &stale)
		require.Equal(t, before, qualificationState(t, db, "teams"))
	}
	validAgain, err := c.ReadPushDependencyDiagnostic(context.Background(), oversized.DiagnosticID, first.NextCursor, 1)
	require.NoError(t, err)
	require.Equal(t, second, validAgain)
	require.NoError(t, c.ReleasePushDependencyDiagnostic(context.Background(), oversized.DiagnosticID))
	require.NoError(t, c.ReleasePushDependencyDiagnostic(context.Background(), oversized.DiagnosticID))
	require.Equal(t, before, qualificationState(t, db, "teams"))
	_, err = c.ReadPushDependencyDiagnostic(context.Background(), oversized.DiagnosticID, "", 64)
	require.ErrorAs(t, err, &stale)
}

func TestPushDependencyPlanning_WorkResumePreservesPrimaryState(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	for i := 0; i < 8; i++ {
		_, err := db.Exec(`INSERT INTO users VALUES(?,?)`, fmt.Sprint(i), "value")
		require.NoError(t, err)
	}
	before := qualificationState(t, db, "users")
	stepWork, e := c.pushPreparationStepWork()
	require.NoError(t, e)
	c.config.MaxSyncWorkUnitsPerInvocation = 2 * stepWork
	_, err := c.preparePushPlanSlice(context.Background())
	err = c.withSyncErrorProgress(context.Background(), err)
	var work *SyncWorkLimitError
	require.ErrorAs(t, err, &work)
	require.Equal(t, SyncProgress{DirtyRows: 8}, work.Progress)
	require.Empty(t, server.pushRequests)
	require.Equal(t, before, qualificationState(t, db, "users"))
	var nodes int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_nodes`).Scan(&nodes))
	require.Equal(t, int64(2), nodes)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.PushPending(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, qualificationState(t, db, "users"))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_nodes`).Scan(&nodes))
	require.Equal(t, int64(2), nodes)
	c.config.MaxSyncWorkUnitsPerInvocation = 1_000_000
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, 1)
	require.Empty(t, qualificationState(t, db, "users")["_sync_dirty_rows"])
}

func TestPushDependencyPlanning_ChargedDiagnosticCleanupResumes(t *testing.T) {
	c, db, server := cyclicDeleteClient(t)
	deleteCyclicPair(t, db)
	c.discovery.Limits.MaxPushRows = 1
	before := qualificationState(t, db, "teams")
	_, err := c.PushPending(context.Background())
	var blocked *PushDependencyGroupExceedsAdmittedLimitError
	require.ErrorAs(t, err, &blocked)
	c.config.MaxSyncWorkUnitsPerInvocation = 8
	c.config.MaxSyncWorkspaceEntries = 1
	c.config.MaxSyncWorkspaceBytes = 1
	var last int64
	require.NoError(t, db.QueryRow(`SELECT (SELECT COUNT(*) FROM _sync_push_nodes)+(SELECT COUNT(*) FROM _sync_push_edges)`).Scan(&last))
	require.Greater(t, last, int64(1))
	interruptions := 0
	var clearingUnit string
	for i := 0; i < 16; i++ {
		c.syncWorkUsed = 0
		err = c.clearPushPlanForLifecycleSlice(context.Background(), &clearingUnit, false, blocked.DiagnosticID)
		var count int64
		require.NoError(t, db.QueryRow(`SELECT (SELECT COUNT(*) FROM _sync_push_nodes)+(SELECT COUNT(*) FROM _sync_push_edges)`).Scan(&count))
		require.Equal(t, before, qualificationState(t, db, "teams"))
		require.Len(t, server.pushRequests, 1)
		if err == nil {
			require.Zero(t, count)
			break
		}
		var work *SyncWorkLimitError
		require.ErrorAs(t, err, &work)
		require.Equal(t, int64(8), work.Used)
		require.Equal(t, last-1, count)
		var target string
		require.NoError(t, db.QueryRow(`SELECT json_extract(state,'$.CleanupDiagnosticID') FROM _sync_push_plan`).Scan(&target))
		require.Equal(t, blocked.DiagnosticID, target)
		last = count
		interruptions++
	}
	require.Greater(t, interruptions, 0)
	var plans int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_push_plan`).Scan(&plans))
	require.Zero(t, plans)
	require.NoError(t, c.ReleasePushDependencyDiagnostic(context.Background(), blocked.DiagnosticID))
}

func TestPushDependencyPlanning_DerivedLayoutRefusalBeforeMutation(t *testing.T) {
	for _, mutation := range []string{"DROP TABLE _sync_push_plan", "DROP TABLE _sync_push_nodes", "DROP INDEX _sync_push_nodes_frontier_idx", "ALTER TABLE _sync_push_plan ADD COLUMN extension TEXT", "DROP TABLE _sync_row_state; CREATE TABLE _sync_row_state(schema_name TEXT,table_name TEXT,key_json TEXT,row_version INTEGER,deleted INTEGER)"} {
		t.Run(mutation, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
			require.NoError(t, err)
			client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
			require.NoError(t, client.Close())
			_, err = db.Exec(`INSERT INTO people VALUES('one','retained')`)
			require.NoError(t, err)
			_, err = db.Exec(mutation)
			require.NoError(t, err)
			before := qualificationState(t, db, "people")
			schemaBefore := baselineText(t, db, `SELECT json_group_array(json_array(type,name,tbl_name,rootpage,sql)) FROM (SELECT * FROM sqlite_schema ORDER BY type,name)`)
			journalBefore := baselineText(t, db, `PRAGMA journal_mode`)
			var objects int64
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema`).Scan(&objects))
			_, err = newClientFromDB(db, "http://example.invalid", tokenProviderForTests, &Config{GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1, Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}}})
			require.Error(t, err)
			require.Equal(t, before, qualificationState(t, db, "people"))
			var after int64
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema`).Scan(&after))
			require.Equal(t, objects, after)
			require.Equal(t, schemaBefore, baselineText(t, db, `SELECT json_group_array(json_array(type,name,tbl_name,rootpage,sql)) FROM (SELECT * FROM sqlite_schema ORDER BY type,name)`))
			require.Equal(t, journalBefore, baselineText(t, db, `PRAGMA journal_mode`))
		})
	}
}

func TestPushDependencyPlanning_ThreeIndependentDeletePairs(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "parents", SyncKeyColumnName: "id"}, {TableName: "children", SyncKeyColumnName: "id"}},
		`CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`,
		`CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT NOT NULL REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)`)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	for i := 1; i <= 3; i++ {
		_, err := db.Exec(`INSERT INTO parents VALUES(?,?);`, fmt.Sprint(i), "parent")
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO children VALUES(?,?,?)`, "c"+fmt.Sprint(i), fmt.Sprint(i), "child")
		require.NoError(t, err)
	}
	_, err := c.PushPending(context.Background())
	require.NoError(t, err)
	c.discovery.Limits.MaxPushRows = 2
	c.config.UploadLimit = 2
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM parents; DELETE FROM children`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	before := qualificationState(t, db, "parents")
	beforeChildren := qualificationState(t, db, "children")
	remaining := map[string]bool{"1": true, "2": true, "3": true, "c1": true, "c2": true, "c3": true}
	for call := 1; call <= 3; call++ {
		_, err = c.PushPending(context.Background())
		require.NoError(t, err)
		require.Len(t, server.pushRequests, call+1)
		req := server.pushRequests[call]
		rows := expandCapturedPushRequest(t, c, req)
		require.Len(t, rows, 2)
		for _, row := range rows {
			require.Equal(t, "DELETE", row.Op)
			key := fmt.Sprint(row.Key["id"])
			require.True(t, remaining[key])
			delete(remaining, key)
		}
		after := qualificationState(t, db, "parents")
		require.Len(t, after["_sync_dirty_rows"], 6-2*call)
		require.Empty(t, after["_sync_outbox_rows"])
		require.Empty(t, after["parents"])
		require.Empty(t, qualificationState(t, db, "children")["children"])
		for key := range remaining {
			require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", key), qualificationRow(t, after, "_sync_dirty_rows", key))
			require.Equal(t, qualificationRow(t, before, "_sync_row_state", key), qualificationRow(t, after, "_sync_row_state", key))
		}
		require.Equal(t, int64(call+1), req.SourceBundleID)
		require.Equal(t, int64(call+2), server.nextBundleSeq)
		var next, checkpoint int64
		require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state`).Scan(&next))
		require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
		require.Equal(t, int64(call+2), next)
		require.Equal(t, int64(call+1), checkpoint)
		t.Logf("independent-pairs call=%d state=%+v children-before=%+v wire=%+v remote=%+v", call, after, beforeChildren, req, server.committedBySeq)
	}
	require.Empty(t, remaining)
}

func TestPushDependencyPlanning_ThreeRowCycleKeepsConfirmedIndependentPrefix(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "employees", SyncKeyColumnName: "id"}}, `CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)`)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	_, err := db.Exec(`INSERT INTO employees VALUES('A','B','a'),('B','C','b'),('C','A','c'),('U',NULL,'old')`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	c.discovery.Limits.MaxPushRows = 2
	c.config.UploadLimit = 2
	_, err = db.Exec(`BEGIN; DELETE FROM employees WHERE id!='U'; UPDATE employees SET name='safe' WHERE id='U'; COMMIT`)
	require.NoError(t, err)
	before := qualificationState(t, db, "employees")
	_, err = c.Sync(context.Background())
	var limit *PushDependencyGroupExceedsAdmittedLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, int64(3), limit.ComponentRows)
	require.Equal(t, SyncProgress{DirtyRows: 3, CommittedBundles: 1}, limit.Progress)
	require.Len(t, server.pushRequests, 2)
	require.Equal(t, "U", expandCapturedPushRequest(t, c, server.pushRequests[1])[0].Key["id"])
	after := qualificationState(t, db, "employees")
	for _, key := range []string{"A", "B", "C"} {
		require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", key), qualificationRow(t, after, "_sync_dirty_rows", key))
		require.Equal(t, qualificationRow(t, before, "_sync_row_state", key), qualificationRow(t, after, "_sync_row_state", key))
	}
	require.Empty(t, after["_sync_outbox_rows"])
	_, err = c.PushPending(context.Background())
	require.ErrorAs(t, err, &limit)
	require.Equal(t, after, qualificationState(t, db, "employees"))
	t.Logf("confirmed-prefix error=%+v state=%+v wire=%+v remote=%+v", limit, after, server.pushRequests, server.committedBySeq)
}

func TestPushDependencyPlanning_CycleCompleteByteBoundary(t *testing.T) {
	for _, over := range []bool{false, true} {
		t.Run(fmt.Sprint(over), func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "employees", SyncKeyColumnName: "id"}}, `CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)`)
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: server}
			_, err := db.Exec(`INSERT INTO employees VALUES('X','Y',?),('Y','X',?)`, strings.Repeat("x", 512), strings.Repeat("y", 512))
			require.NoError(t, err)
			c.discovery.Limits.MaxPushRows = 2
			tx, err := db.Begin()
			require.NoError(t, err)
			id, _, captures, err := c.collectDirtyRowsForPushInTx(context.Background(), tx)
			require.NoError(t, err)
			require.Len(t, captures, 2)
			require.NoError(t, tx.Rollback())
			body, err := c.compactPushRequestBody(id, c.pendingInitializationID, captures)
			require.NoError(t, err)
			c.discovery.Limits.MaxPushDecodedBytes = int64(len(body))
			if over {
				c.discovery.Limits.MaxPushDecodedBytes--
			}
			before := qualificationState(t, db, "employees")
			_, err = c.PushPending(context.Background())
			if over {
				var limit *PushDependencyGroupExceedsAdmittedLimitError
				require.ErrorAs(t, err, &limit)
				require.Equal(t, "decoded_bytes", limit.Dimension)
				require.Equal(t, int64(2), limit.ComponentRows)
				require.Empty(t, server.pushRequests)
				require.Equal(t, before, qualificationState(t, db, "employees"))
			} else {
				require.NoError(t, err)
				require.Len(t, server.pushRequests, 1)
				actual, e := json.Marshal(server.pushRequests[0])
				require.NoError(t, e)
				require.Equal(t, string(body), string(actual))
				require.Empty(t, qualificationState(t, db, "employees")["_sync_dirty_rows"])
			}
			t.Logf("cycle-bytes exact=%d over=%t state=%+v wire=%+v remote=%+v", len(body), over, qualificationState(t, db, "employees"), server.pushRequests, server.committedBySeq)
		})
	}
}

func TestPushDependencyPlanning_RekeyEquivalentFactsAndPartialFailure(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	_, err := db.Exec(`INSERT INTO users VALUES('P','same')`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	c.discovery.Limits.MaxPushRows = 1
	c.config.UploadLimit = 1
	var histories []qualificationVector
	var requests [][]byte
	for _, statements := range []string{`UPDATE users SET id='Q' WHERE id='P'`, `DELETE FROM users WHERE id='P'; INSERT INTO users VALUES('Q','same')`} {
		tx, e := db.Begin()
		require.NoError(t, e)
		_, e = tx.Exec(statements)
		require.NoError(t, e)
		// Observe every primary owner on the same transaction, then roll back
		// the capture and derived plan. Both histories start from identical facts.
		var raw string
		require.NoError(t, tx.QueryRow(`SELECT json_group_array(json_object('key',key_json,'op',op,'ordinal',dirty_ordinal,'base_kind',base_kind,'base_payload',base_payload,'base_provenance',base_provenance,'payload',payload,'base_version',base_row_version)) FROM (SELECT * FROM _sync_dirty_rows ORDER BY dirty_ordinal)`).Scan(&raw))
		histories = append(histories, qualificationVector{"facts": {{"raw": raw}}})
		id, _, captures, e := c.collectDirtyRowsForPushInTx(context.Background(), tx)
		require.NoError(t, e)
		require.Len(t, captures, 1)
		require.Equal(t, rowKeyJSON("P"), captures[0].KeyJSON)
		body, e := c.compactPushRequestBody(id, c.pendingInitializationID, captures)
		require.NoError(t, e)
		requests = append(requests, body)
		require.NoError(t, tx.Rollback())
	}
	require.Equal(t, histories[0], histories[1])
	require.Equal(t, requests[0], requests[1], "actual selection/compact encoding must not infer a rename")
	_, err = db.Exec(`UPDATE users SET id='Q' WHERE id='P'`)
	require.NoError(t, err)
	before := qualificationState(t, db, "users")
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Equal(t, "P", expandCapturedPushRequest(t, c, server.pushRequests[1])[0].Key["id"])
	prefix := qualificationState(t, db, "users")
	require.Equal(t, qualificationRow(t, before, "_sync_dirty_rows", "Q"), qualificationRow(t, prefix, "_sync_dirty_rows", "Q"))
	require.Equal(t, "deleted", qualificationText(qualificationRow(t, prefix, "_sync_row_state", "P"), "server_kind"))
	server.pushHook = func(*mockDirectPush) *http.Response {
		return errorJSONResponse(http.StatusUnprocessableEntity, map[string]string{"error": "transition_input_invalid", "message": "qualification insertion refusal"})
	}
	_, err = c.PushPending(context.Background())
	var rejected *PushRejectionNeedsResolutionError
	require.ErrorAs(t, err, &rejected)
	failed := qualificationState(t, db, "users")
	require.Equal(t, prefix["users"], failed["users"])
	for _, owner := range []string{"_sync_row_state", "_sync_relationships", "_sync_attachment_state"} {
		require.Equal(t, prefix[owner], failed[owner], owner)
	}
	require.EqualValues(t, 4, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.Len(t, failed["_sync_outbox_rows"], 1)
	require.Equal(t, qualificationPayload(t, qualificationRow(t, before, "_sync_dirty_rows", "Q"), "base_provenance"), qualificationPayload(t, qualificationRow(t, failed, "_sync_outbox_rows", "Q"), "base_provenance"))
	require.Equal(t, "absent", qualificationText(qualificationRow(t, failed, "_sync_outbox_rows", "Q"), "base_kind"))
	require.Len(t, server.committedBySeq, 2, "Q refusal must not roll back or recreate P")
	path := filepath.Join(t.TempDir(), "partial.sqlite")
	_, err = db.Exec(`VACUUM INTO ?`, path)
	require.NoError(t, err)
	physical, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
	require.NoError(t, err)
	defer physical.Close()
	physical.SetMaxOpenConns(1)
	require.Equal(t, failed, qualificationState(t, physical, "users"))
	cfg := *c.config
	reopened, err := newAdmittedFixtureClient(physical, c.BaseURL, c.Token, &cfg)
	require.NoError(t, err)
	attachTestClient(t, reopened, "bundle-user", "bundle-device")
	reopened.HTTP = c.HTTP
	_, err = reopened.PushPending(context.Background())
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, failed, qualificationState(t, physical, "users"))
	require.Len(t, server.pushRequests, 3)
	t.Logf("rekey equivalent=%+v bodies=%s prefix=%+v failure=%+v wire=%+v remote=%+v", histories, requests, prefix, failed, server.pushRequests, server.committedBySeq)
}
