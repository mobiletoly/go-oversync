package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func countA3(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.QueryRow(query).Scan(&n))
	return n
}

func TestA3Workspace_Boundaries(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	unit := applyUnit("a3-boundary", 0, "")
	_, err := db.Exec(`INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('snapshot',1,'main','nodes','{"id":"X"}',1,'{"id":"X","name":"x","parent_id":null}')`)
	require.NoError(t, err)
	var entries, bytes, charge int64
	require.NoError(t, db.QueryRow(`SELECT snapshot_stage_entries,snapshot_stage_bytes,(SELECT charge FROM _sync_snapshot_stage) FROM _sync_operation_state`).Scan(&entries, &bytes, &charge))
	require.EqualValues(t, 1, entries)
	require.Equal(t, charge, bytes)
	used, err := c.workspaceUsage(context.Background(), db, unit, nil, nil)
	require.NoError(t, err)
	for _, delta := range []int64{-1, 0, 1} {
		c.config.MaxSyncWorkspaceEntries = used.entries + delta
		err = c.checkAggregateWorkspace(context.Background(), db, unit, nil, nil, workspaceCharge{})
		if delta < 0 {
			var limit *SyncWorkspaceLimitError
			require.ErrorAs(t, err, &limit)
			require.Equal(t, used.entries, limit.Used)
		} else {
			require.NoError(t, err)
		}
	}
	c.config.MaxSyncWorkspaceEntries = 100000
	for _, delta := range []int64{-1, 0, 1} {
		c.config.MaxSyncWorkspaceBytes = used.bytes + delta
		err = c.checkAggregateWorkspace(context.Background(), db, unit, nil, nil, workspaceCharge{})
		if delta < 0 {
			var limit *SyncWorkspaceLimitError
			require.ErrorAs(t, err, &limit)
			require.Equal(t, "bytes", limit.Dimension)
		} else {
			require.NoError(t, err)
		}
	}
	c.config.MaxSyncWorkspaceBytes = 256 << 20
	_, err = c.addWorkspaceCharge(unit, workspaceCharge{math.MaxInt64, 0}, workspaceCharge{1, 0})
	var limit *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &limit)
	c.config.MaxSyncWorkUnitsPerInvocation = 17
	for _, work := range []int64{16, 17, 18} {
		c.syncWorkUsed = 0
		c.readyAtomicUnitDispatched = false
		err = c.reserveSyncWork(context.Background(), unit, work)
		require.NoError(t, err)
		require.Equal(t, work, c.syncWorkUsed)
		if work == 16 {
			require.NoError(t, c.reserveSyncWork(context.Background(), unit, 1))
		}
		var pending *SyncWorkLimitError
		require.ErrorAs(t, c.reserveSyncWork(context.Background(), unit, 1), &pending)
		require.EqualValues(t, 17, pending.Limit)
	}
	c.syncWorkUsed = 17
	c.readyAtomicUnitDispatched = false
	require.NoError(t, c.reserveReadySyncWork(context.Background(), unit, 1_000_000))
	require.EqualValues(t, 1_000_017, c.syncWorkUsed)
	var later *SyncWorkLimitError
	require.ErrorAs(t, c.reserveReadySyncWork(context.Background(), unit, 1), &later)
	require.EqualValues(t, 1_000_017, later.Used)
	c.config.MaxSyncWorkUnitsPerInvocation = 1000000
	_, err = db.Exec(`UPDATE _sync_snapshot_stage SET payload='{}'`)
	require.NoError(t, err)
	require.NoError(t, reconcileWorkspaceOwners(context.Background(), db, c.config))
	_, err = db.Exec(`UPDATE _sync_snapshot_stage SET charge=charge+1`)
	require.NoError(t, err)
	require.ErrorContains(t, reconcileWorkspaceOwners(context.Background(), db, c.config), "invalid retained row charge")
}

func TestA3Workspace_CleanupAndHandles(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	token, err := c.reserveWorkspace(context.Background(), applyUnit("first", 0, ""), workspaceCharge{3, 100})
	require.NoError(t, err)
	other, err := newAdmittedFixtureClient(db, c.BaseURL, tokenProviderForTests, c.config)
	require.NoError(t, err)
	defer other.Close()
	require.Positive(t, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
	second, err := other.reserveWorkspace(context.Background(), applyUnit("second", 0, ""), workspaceCharge{5, 200})
	require.NoError(t, err)
	require.EqualValues(t, 10, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
	require.NoError(t, c.releaseWorkspace(context.Background(), token))
	require.NoError(t, c.releaseWorkspace(context.Background(), token))
	require.EqualValues(t, 6, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
	require.NoError(t, other.releaseWorkspace(context.Background(), second))
	require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
}

func TestA3Format_RejectAndResume(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := c.reserveWorkspace(context.Background(), applyUnit("orphan", 0, ""), workspaceCharge{3, 100})
	require.NoError(t, err)
	// A ordinary client reopen cannot prove abandonment.
	ordinary, err := newAdmittedFixtureClient(db, c.BaseURL, tokenProviderForTests, c.config)
	require.NoError(t, err)
	require.Positive(t, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
	require.NoError(t, ordinary.Close())
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	require.NoError(t, reclaimWorkspaceReservations(context.Background(), tx, c.config))
	require.NoError(t, tx.Rollback())
	require.Positive(t, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
	_, err = db.Exec(`UPDATE _sync_operation_state SET workspace_context=replace(workspace_context,'"entries":4','"entries":3')`)
	require.NoError(t, err)
	beforeReject := qualificationState(t, db, "nodes")
	tx, err = beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	require.Error(t, reclaimWorkspaceReservations(context.Background(), tx, c.config))
	require.NoError(t, tx.Rollback())
	require.Equal(t, beforeReject, qualificationState(t, db, "nodes"))
	require.Positive(t, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
}

func TestA3Workspace_AtomicResume(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	before, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	protected := a3ProtectedPrimaryVector(t, db)
	c.config.MaxSyncWorkUnitsPerInvocation = 100
	c.HTTP = &http.Client{Transport: a1SettlementServer(t)}
	_, err = c.PushPending(context.Background())
	require.Error(t, err)
	var work *SyncWorkLimitError
	var space *SyncWorkspaceLimitError
	require.True(t, errors.As(err, &work) || errors.As(err, &space), err)
	refused := a3ProtectedPrimaryVector(t, db)
	require.Equal(t, protected, refused, "complete primary vector at atomic refusal")
	after, err := loadOutboxBundle(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, before.CanonicalRequestHash, after.CanonicalRequestHash)
	require.Equal(t, before.SourceBundleID, after.SourceBundleID)
	require.EqualValues(t, 0, countA3(t, db, `SELECT reserved_entries FROM _sync_operation_state`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	c.config.MaxSyncWorkUnitsPerInvocation = 1000000
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, db)
	require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
	require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
}

// Keep recovery identity while excluding only admitted derived accounting progress.
func a3ProtectedPrimaryVector(t *testing.T, db *sql.DB) qualificationVector {
	t.Helper()
	vector := qualificationState(t, db, "nodes")
	for _, row := range vector["_sync_operation_state"] {
		for _, field := range []string{"workspace_context", "reserved_entries", "reserved_bytes", "snapshot_stage_entries", "snapshot_stage_bytes", "snapshot_cleanup_ordinal", "push_stage_entries", "push_stage_bytes", "push_cleanup_ordinal"} {
			delete(row, field)
		}
	}
	return vector
}
