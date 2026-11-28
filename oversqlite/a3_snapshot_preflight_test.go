package oversqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type snapshotCountingTx struct {
	sqliteTransaction
	fullCounts int
	visits     int
}

func (tx *snapshotCountingTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(query, "SELECT count(*) FROM \"nodes\"") ||
		strings.Contains(query, "SELECT count(*) FROM _sync_row_state WHERE table_name=") {
		tx.fullCounts++
	}
	if strings.Contains(query, "ORDER BY") && strings.Contains(query, "LIMIT 1") &&
		(strings.Contains(query, "FROM \"nodes\"") || strings.Contains(query, "FROM _sync_row_state")) {
		tx.visits++
	}
	return tx.sqliteTransaction.QueryRowContext(ctx, query, args...)
}

func TestA3SnapshotPreflight_BoundedResume(t *testing.T) {
	c, db := newA1SurvivorClient(t, 100)
	_, err := db.Exec(`INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known,server_revision)
        SELECT a.schema_name,a.table_name,json_object('id',n.id),a.row_version,a.server_kind,
        json_set(a.server_payload,'$.id.value',n.id,'$.name.value',n.name),a.server_provenance,1,0
        FROM nodes n CROSS JOIN _sync_row_state a WHERE n.id LIKE 'Z%' AND a.key_json='{"id":"P"}'`)
	require.NoError(t, err)
	c.config.MaxSyncWorkUnitsPerInvocation = 100000
	unit := applyUnit("snapshot", 2, "bounded-empty")
	prepare := func(used int64) (*applyPlan, int, int, error) {
		c.syncWorkUsed = used
		c.readyAtomicUnitDispatched = false
		tx, e := db.BeginTx(context.Background(), nil)
		require.NoError(t, e)
		tracked := &snapshotCountingTx{sqliteTransaction: tx}
		e = c.prepareApplySurvivors(context.Background(), tracked, unit, 0, nil, "snapshot")
		p, loadErr := loadApplyPlan(context.Background(), tx)
		require.NoError(t, loadErr)
		require.NoError(t, tx.Commit()) // Only derivative preparation, never business writes.
		return p, tracked.fullCounts, tracked.visits, e
	}
	p, counts, visits, err := prepare(100000 - 128)
	var limited *SyncWorkLimitError
	require.ErrorAs(t, err, &limited)
	require.EqualValues(t, 104, countA3(t, db, `SELECT count(*) FROM nodes`))
	require.LessOrEqual(t, counts, 2)
	require.LessOrEqual(t, visits, 416)
	require.NotNil(t, p)
	require.Greater(t, c.syncWorkUsed, c.config.MaxSyncWorkUnitsPerInvocation-128,
		"first ready atomic unit must proceed after retained preparation")
	p, counts, visits, err = prepare(0)
	require.NoError(t, err)
	require.Zero(t, counts)
	require.Zero(t, p.atomicWork)
	require.LessOrEqual(t, visits, 2*(104+104)+2, "writer revalidates bounded current scope; obsolete skip-scan oracle is historical")
	_, counts, visits, err = prepare(0)
	require.NoError(t, err)
	require.Zero(t, counts)
	require.LessOrEqual(t, visits, 2*(104+104)+2, "writer revalidates bounded current scope; obsolete skip-scan oracle is historical")
	require.EqualValues(t, 104, countA3(t, db, "SELECT count(*) FROM nodes"))
	require.EqualValues(t, 104, countA3(t, db, "SELECT count(*) FROM _sync_row_state"))
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
}

func TestA3SnapshotPreflight_CancelDuringPreparation(t *testing.T) {
	c, db := newA1SurvivorClient(t, 100)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracked := &snapshotCountingTx{sqliteTransaction: tx}
	err = c.prepareApplySurvivors(ctx, tracked, applyUnit("snapshot", 2, "cancel-preparation"), 1,
		func(int) (applySurvivor, error) {
			cancel()
			return applySurvivor{table: "nodes", pk: "new", role: "intended"}, nil
		}, "snapshot")
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, tx.Rollback())
	require.EqualValues(t, 104, countA3(t, db, "SELECT count(*) FROM nodes"))
	require.Zero(t, countA3(t, db, "SELECT count(*) FROM _sync_apply_plan"))
}

func TestA3SnapshotPreflight_IndexedQueries(t *testing.T) {
	_, db := newA1SurvivorClient(t)
	for name, query := range map[string]string{
		"business": `EXPLAIN QUERY PLAN SELECT CAST(id AS TEXT) FROM nodes WHERE id>'C' ORDER BY id LIMIT 1`,
		"baseline": `EXPLAIN QUERY PLAN SELECT key_json FROM _sync_row_state WHERE schema_name='main' AND table_name='nodes' AND key_json>'{"id":"C"}' ORDER BY key_json LIMIT 1`,
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := db.Query(query)
			require.NoError(t, err)
			defer rows.Close()
			var detail string
			for rows.Next() {
				var id, parent, unused int
				var line string
				require.NoError(t, rows.Scan(&id, &parent, &unused, &line))
				detail += line
			}
			require.NoError(t, rows.Err())
			require.Contains(t, detail, "SEARCH")
			require.NotContains(t, detail, "TEMP B-TREE")
		})
	}
}
