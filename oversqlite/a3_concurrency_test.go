package oversqlite

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestA3Concurrency_SecondConnection(t *testing.T) {
	for _, scenario := range []struct{ name, sql string }{
		{"edit", `UPDATE nodes SET name='new independent L' WHERE id='D'`},
		{"new-dependent", `INSERT INTO nodes VALUES('E','new child','D')`},
		{"reparent", `UPDATE nodes SET parent_id='Q' WHERE id='D'`},
		{"parent-write", `UPDATE nodes SET name='new independent parent' WHERE id='Q'`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c, db, dbPath := qualificationDurableClient(t)
			other, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on&_busy_timeout=2000")
			require.NoError(t, err)
			defer other.Close()
			other.SetMaxOpenConns(1)
			_, err = db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
			require.NoError(t, err)
			server := a1SettlementServer(t)
			c.HTTP = &http.Client{Transport: server}
			barrier := make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				<-barrier
				_, err := other.Exec(scenario.sql)
				finished <- err
			}()
			var captured qualificationVector
			c.SetBeforePushReplayHook(func(context.Context) error {
				close(barrier)
				if err := <-finished; err != nil {
					return err
				}
				captured = qualificationState(t, db, "nodes")
				return nil
			})
			_, err = c.PushPending(context.Background())
			require.NoError(t, err)
			settled := qualificationState(t, db, "nodes")
			for _, table := range []string{"nodes", "_sync_dirty_rows", "_sync_row_state", "_sync_relationships"} {
				protected := func(vector qualificationVector) []map[string]any {
					rows := []map[string]any{}
					for _, row := range vector[table] {
						column := "key_json"
						if table == "nodes" {
							column = "id"
						}
						if table == "_sync_relationships" {
							column = "child_key"
						}
						key := qualificationText(row, column)
						if key != "P" && key != "C" && key != rowKeyJSON("P") && key != rowKeyJSON("C") {
							rows = append(rows, row)
						}
					}
					return rows
				}
				require.Equal(t, protected(captured), protected(settled), "complete independent writer vector: "+table)
			}
			require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
			require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
			require.EqualValues(t, 2, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
			require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.Len(t, server.pushRequests, 1)
		})
	}
}

// Exercise a persisted ready plan, not just a write before first capture.
func TestA3Concurrency_SecondConnectionReadyGuard(t *testing.T) {
	for _, change := range []string{
		`UPDATE nodes SET name='new independent L' WHERE id='D'`,
		`INSERT INTO nodes VALUES('E','new child','D')`,
		`UPDATE nodes SET parent_id='Q' WHERE id='D'`,
		`UPDATE nodes SET name='new independent parent' WHERE id='Q'`,
	} {
		t.Run(change, func(t *testing.T) {
			c, db, dbPath := qualificationDurableClient(t)
			other, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on&_busy_timeout=2000")
			require.NoError(t, err)
			defer other.Close()
			other.SetMaxOpenConns(1)
			unit := applyUnit("snapshot", 2, "ready-independent-writer")
			prepare := func() string {
				c.syncWorkUsed = 0
				tx, err := beginImmediateTx(context.Background(), db)
				require.NoError(t, err)
				require.NoError(t, c.prepareApplySurvivors(context.Background(), tx, unit, 0, nil, "snapshot"))
				plan, err := loadApplyPlan(context.Background(), tx)
				require.NoError(t, err)
				require.Equal(t, "ready", plan.phase)
				require.NoError(t, tx.Commit(context.Background()))
				return plan.guard
			}
			guard := prepare()
			_, err = other.Exec(change)
			require.NoError(t, err)
			before := a3ProtectedPrimaryVector(t, db)
			require.NotEqual(t, guard, prepare(), "independent managed DML invalidates the captured ready context")
			after := a3ProtectedPrimaryVector(t, db)
			require.Equal(t, before, after, "revalidation changes derived work only; complete primary vector retained")
		})
	}
}
