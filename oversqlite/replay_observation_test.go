//go:build replay_observation

package oversqlite

import (
	"context"
	"database/sql"
	"runtime"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// Requires the investigation's isolated diagnostic modfile. Never linked into
// normal tests or a production build.
func init() {
	replayDiagnosticStart = sqlite3.ReplayDiagnosticStart
	replayDiagnosticStop = func() any { return sqlite3.ReplayDiagnosticStop() }
	bindingProofNativeMemory = sqlite3.ReplayBindingMemory
	replayDiagnosticMemory = sqlite3.ReplayBindingMemory
}

// These prerequisites require this packet's isolated native-memory observer.
// MEMUSED includes VM metadata and native bindings; deltas isolate a large
// binding while global memory_used is logged separately, never used as quota.
func TestReplayStatementReuse_BindingLifetimePrerequisite(t *testing.T) {
	ctx := context.Background()
	const size = 1 << 20
	for _, kind := range []string{"TEXT", "BLOB"} {
		t.Run(kind, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			const query = `SELECT length(?) /* retained-binding-lifetime */`
			stmt, err := conn.PrepareContext(ctx, query)
			require.NoError(t, err)
			defer stmt.Close()
			before := sqlite3.ReplayBindingMemory()
			var arg any = strings.Repeat("x", size)
			if kind == "BLOB" {
				arg = []byte(arg.(string))
			}
			var n int
			require.NoError(t, stmt.QueryRowContext(ctx, arg).Scan(&n))
			require.Equal(t, size, n)
			afterRowsClose := sqlite3.ReplayBindingMemory()
			require.GreaterOrEqual(t, afterRowsClose[query]-before[query], int64(size))
			arg = nil
			runtime.GC()
			afterGC := sqlite3.ReplayBindingMemory()
			require.Equal(t, afterRowsClose[query], afterGC[query])
			var unsupported struct{ Value int }
			require.Error(t, stmt.QueryRowContext(ctx, strings.Repeat("s", size)).Scan(&unsupported))
			require.GreaterOrEqual(t, sqlite3.ReplayBindingMemory()[query]-before[query], int64(size), "scan failure does not release bound payload")
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			require.ErrorIs(t, stmt.QueryRowContext(canceled, int64(7)).Scan(&n), context.Canceled)
			afterCanceledQuery := sqlite3.ReplayBindingMemory()
			require.Less(t, afterCanceledQuery[query], afterGC[query]-int64(size/2), "driver query binds synchronously before canceled Next")
			require.NoError(t, stmt.QueryRowContext(ctx, int64(7)).Scan(&n))
			afterRebind := sqlite3.ReplayBindingMemory()
			require.Less(t, afterRebind[query], afterGC[query]-int64(size/2))
			var null sql.NullInt64
			require.NoError(t, stmt.QueryRowContext(ctx, nil).Scan(&null))
			require.False(t, null.Valid)
			require.NoError(t, stmt.QueryRowContext(ctx, "").Scan(&n))
			require.Zero(t, n)
			require.NoError(t, stmt.QueryRowContext(ctx, []byte{}).Scan(&n))
			require.Zero(t, n)
			require.NoError(t, stmt.Close())
			require.Zero(t, sqlite3.ReplayBindingMemory()[query])
			t.Logf("%s native snapshots before=%v rowsClosed=%v afterGC=%v canceledScalarQuery=%v scalarRebind=%v final=%v", kind, before, afterRowsClose, afterGC, afterCanceledQuery, afterRebind, sqlite3.ReplayBindingMemory())
		})
	}
	t.Run("CachedWritesAcrossTables", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		require.NoError(t, err)
		defer db.Close()
		_, err = db.Exec(`CREATE TABLE parent(id TEXT PRIMARY KEY,payload BLOB); CREATE TABLE child(id TEXT PRIMARY KEY,parent_id TEXT REFERENCES parent(id),payload BLOB)`)
		require.NoError(t, err)
		tx, err := beginImmediateTx(ctx, db)
		require.NoError(t, err)
		defer tx.Rollback()
		cache := newTxStmtCache(tx)
		defer cache.Close()
		const parent = `INSERT INTO parent VALUES(?,?)`
		const child = `INSERT INTO child VALUES(?,?,?)`
		_, err = cache.ExecContext(ctx, parent, "P", []byte(strings.Repeat("x", size)))
		require.NoError(t, err)
		_, err = cache.ExecContext(ctx, child, "C", "P", strings.Repeat("y", size))
		require.NoError(t, err)
		afterExec := sqlite3.ReplayBindingMemory()
		require.Greater(t, afterExec[parent], int64(size))
		require.Greater(t, afterExec[child], int64(size))
		_, err = tx.ExecContext(ctx, `DELETE FROM child; DELETE FROM parent`)
		require.NoError(t, err)
		require.Equal(t, afterExec[parent], sqlite3.ReplayBindingMemory()[parent])
		require.Equal(t, afterExec[child], sqlite3.ReplayBindingMemory()[child])
		// Constraint failure clears the parent statement's bindings in execSync.
		_, err = cache.ExecContext(ctx, child, "C", "missing", strings.Repeat("z", size))
		require.Error(t, err)
		afterError := sqlite3.ReplayBindingMemory()
		require.Less(t, afterError[child], afterExec[child]-int64(size/2))
		require.NoError(t, cache.Close())
		require.Zero(t, sqlite3.ReplayBindingMemory()[parent])
		require.Zero(t, sqlite3.ReplayBindingMemory()[child])
		t.Logf("two tables, one FK: successful Exec retains both bindings after stored rows deleted; exec error clears its own bindings: successful=%v afterError=%v", afterExec, afterError)
	})
}

func TestReplayStatementReuse_BindingAccountingPrerequisite(t *testing.T) {
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	defer tx.Rollback()
	unit := applyUnit("binding-prerequisite", 0, "synthetic")
	used, err := c.workspaceUsage(ctx, tx, unit, nil, nil)
	require.NoError(t, err)
	const size = 1 << 20
	c.config.MaxSyncWorkspaceEntries = used.entries + 1
	c.config.MaxSyncWorkspaceBytes = used.bytes + size
	// This is a deliberate counterexample to simply forwarding the diagnostic
	// adapter into production. Admission checks are not retained reservations.
	queries := []string{`SELECT length(?) /* first admitted copy */`, `SELECT length(CAST(? AS BLOB)) /* second admitted copy */`}
	var statements []*sql.Stmt
	defer func() {
		for _, stmt := range statements {
			require.NoError(t, stmt.Close())
		}
	}()
	for _, query := range queries {
		require.NoError(t, c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, workspaceCharge{1, size}))
		stmt, err := tx.PrepareContext(ctx, query)
		require.NoError(t, err)
		statements = append(statements, stmt)
		var n int
		require.NoError(t, stmt.QueryRowContext(ctx, strings.Repeat("x", size)).Scan(&n))
		require.Equal(t, size, n)
	}
	native := sqlite3.ReplayBindingMemory()
	require.GreaterOrEqual(t, native[queries[0]]+native[queries[1]], int64(2*size))
	after, err := c.workspaceUsage(ctx, tx, unit, nil, nil)
	require.NoError(t, err)
	require.Equal(t, used, after)
	require.NoError(t, c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, workspaceCharge{1, size}))
	err = c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, workspaceCharge{2, 2 * size})
	var capacity *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "entries", capacity.Dimension)
	c.config.MaxSyncWorkspaceEntries = used.entries + 2
	err = c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, workspaceCharge{2, 2 * size})
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, "bytes", capacity.Dimension)
	t.Logf("COUNTEREXAMPLE: successive admission checks each accept one copy, used=%+v after=%+v limitBytes=%d, two native statements retain >2MiB; accounting both copies rejects entries then bytes; native=%v", used, after, c.config.MaxSyncWorkspaceBytes, native)
}
