package oversqlite

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"testing"
)

func TestFinalStateUniqueApply_TransfersAndCycles(t *testing.T) {
	for _, tc := range []struct {
		name, index string
		ids, emails []string
	}{
		{"transfer", `CREATE UNIQUE INDEX email_unique ON users(email)`, []string{"A", "B"}, []string{"b", "c"}},
		{"swap", `CREATE UNIQUE INDEX email_unique ON users(email)`, []string{"A", "B"}, []string{"b", "a"}},
		{"longer cycle", `CREATE UNIQUE INDEX email_unique ON users(email)`, []string{"A", "B", "C"}, []string{"b", "c", "a"}},
		{"collation", `CREATE UNIQUE INDEX email_unique ON users(email COLLATE NOCASE)`, []string{"A", "B"}, []string{"B", "C"}},
		{"composite", `CREATE UNIQUE INDEX email_unique ON users(name,email)`, []string{"A", "B"}, []string{"b", "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
			for _, id := range tc.ids {
				seedSyncedUserRow(t, c, db, id, "same", 1)
			}
			_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0`)
			require.NoError(t, err)
			_, err = db.Exec(tc.index)
			require.NoError(t, err)
			effects := make([]oversync.BundleRow, 0, len(tc.ids))
			for i, id := range tc.ids {
				effects = append(effects, oversync.BundleRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": "same", "email": tc.emails[i]})})
			}
			setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, effects...)}})
			_, err = c.PullToStable(context.Background())
			require.NoError(t, err)
			for i, id := range tc.ids {
				var email string
				require.NoError(t, db.QueryRow(`SELECT email FROM users WHERE id=?`, id).Scan(&email))
				require.Equal(t, tc.emails[i], email)
			}
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
			for _, table := range []string{"_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan", "_sync_dirty_rows"} {
				var n int
				require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&n))
				require.Zero(t, n, table)
			}
		})
	}
	t.Run("default_200_cycle_refusal", func(t *testing.T) {
		c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
		require.EqualValues(t, 131072, c.config.MaxSyncWorkspaceEntries)
		require.EqualValues(t, 256*1024*1024, c.config.MaxSyncWorkspaceBytes)
		require.EqualValues(t, 1000000, c.config.MaxSyncWorkUnitsPerInvocation)
		const size = 200
		rows := make([]oversync.BundleRow, 0, size)
		for i := 0; i < size; i++ {
			id := fmt.Sprintf("U%04d", i)
			seedSyncedUserRow(t, c, db, id, "same", 1)
			_, err := db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=? WHERE id=?; UPDATE _sync_apply_state SET apply_mode=0`, fmt.Sprintf("v%04d", i), id)
			require.NoError(t, err)
			rows = append(rows, oversync.BundleRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustBundlePayload(t, id, "same", fmt.Sprintf("v%04d", (i+1)%size))})
		}
		_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; CREATE UNIQUE INDEX unique_email ON users(email)`)
		require.NoError(t, err)
		before := logicalRecoveryVector(t, db) + recoveryVector(t, db, "_sync_relationships", "_sync_conflict_state")
		setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, rows...)}})
		_, err = c.PullToStable(context.Background())
		var limit *SyncWorkspaceLimitError
		require.ErrorAs(t, err, &limit)
		require.Equal(t, "entries", limit.Dimension)
		require.EqualValues(t, 131072, limit.Limit)
		require.Greater(t, limit.Used+limit.Reserved, limit.Limit)
		require.Equal(t, before, logicalRecoveryVector(t, db)+recoveryVector(t, db, "_sync_relationships", "_sync_conflict_state"))
		requireLastBundleSeqSeen(t, c, context.Background(), 1)
		require.Zero(t, countA3(t, db, `SELECT apply_mode FROM _sync_apply_state`))
		require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes FROM _sync_operation_state`))
		for _, table := range []string{"_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"} {
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM `+table), table)
		}
		t.Logf("default cycle vertices=%d typed_refusal=%+v retained_plan_rows=%d retained_edges=%d", size, limit, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`), countA3(t, db, `SELECT count(*) FROM _sync_apply_edges`))
	})
}
func TestFinalStateUniqueApply_UnsafeTriggerPreservesAndRetries(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	for _, id := range []string{"A", "B"} {
		seedSyncedUserRow(t, c, db, id, "same", 1)
	}
	_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0; CREATE UNIQUE INDEX email_unique ON users(email); CREATE TRIGGER user_delete_effect AFTER DELETE ON users BEGIN DELETE FROM users WHERE id='B'; END`)
	require.NoError(t, err)
	effects := []oversync.BundleRow{}
	for i, id := range []string{"A", "B"} {
		effects = append(effects, oversync.BundleRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": "same", "email": []string{"b", "a"}[i]})})
	}
	response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, effects...)}}
	setPullResponses(t, c, response)
	_, err = c.PullToStable(context.Background())
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, "user_delete_effect", blocked.Trigger)
	for _, id := range []string{"A", "B"} {
		var email string
		require.NoError(t, db.QueryRow(`SELECT email FROM users WHERE id=?`, id).Scan(&email))
		require.Equal(t, fmt.Sprintf("%c", id[0]+32), email)
	}
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	_, err = db.Exec(`DROP TRIGGER user_delete_effect`)
	require.NoError(t, err)
	setPullResponses(t, c, response)
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
}

func TestFinalStateUniqueApply_CyclePreservesCascadeClosureAtTinyTargets(t *testing.T) {
	for _, target := range []int64{1, 8, 17, 50000} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			c.config.MaxSyncWorkUnitsPerInvocation = target
			_, err := db.Exec(`CREATE UNIQUE INDEX nodes_name ON nodes(name)`)
			require.NoError(t, err)
			effects := []oversync.BundleRow{}
			for i, id := range []string{"P", "Q"} {
				effects = append(effects, oversync.BundleRow{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": []string{"other", "parent"}[i], "parent_id": nil})})
			}
			setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, effects...)}})
			_, err = c.PullToStable(context.Background())
			require.NoError(t, err)
			for _, pair := range [][2]string{{"P", "other"}, {"Q", "parent"}, {"C", "child"}, {"D", "grandchild"}} {
				var name string
				require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id=?`, pair[0]).Scan(&name))
				require.Equal(t, pair[1], name)
			}
			var parent string
			require.NoError(t, db.QueryRow(`SELECT parent_id FROM nodes WHERE id='C'`).Scan(&parent))
			require.Equal(t, "P", parent)
			require.NoError(t, db.QueryRow(`SELECT parent_id FROM nodes WHERE id='D'`).Scan(&parent))
			require.Equal(t, "C", parent)
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
		})
	}
}
func TestFinalStateUniqueApply_ConflictingDesiredStateIsDiagnosticAndAtomic(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	for _, id := range []string{"A", "B"} {
		seedSyncedUserRow(t, c, db, id, "same", 1)
	}
	_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=lower(id); UPDATE _sync_apply_state SET apply_mode=0; CREATE UNIQUE INDEX email_unique ON users(email)`)
	require.NoError(t, err)
	effects := []oversync.BundleRow{}
	for _, id := range []string{"A", "B"} {
		effects = append(effects, oversync.BundleRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": "same", "email": "unused"})})
	}
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, effects...)}})
	_, err = c.PullToStable(context.Background())
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, "conflicting_desired_state", blocked.Reason)
	require.Equal(t, "email_unique", blocked.Index)
	require.ElementsMatch(t, []string{"A", "B"}, blocked.Keys)
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE email IN ('a','b')`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestFinalStateUniqueApply_SnapshotClearsUnstagedHolder(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	seedSyncedUserRow(t, c, db, "old", "same", 1)
	_, err := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; CREATE UNIQUE INDEX unique_email ON users(email)`)
	require.NoError(t, err)
	var email string
	require.NoError(t, db.QueryRow(`SELECT email FROM users WHERE id='old'`).Scan(&email))

	invalid := []oversync.BundleRow{}
	for _, id := range []string{"old", "new"} {
		invalid = append(invalid, oversync.BundleRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustBundlePayload(t, id, "same", email)})
	}
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, invalid...)}})
	_, err = c.PullToStable(context.Background())
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	rows := []oversync.SnapshotRow{{Schema: "main", Table: "users", Key: mustBundleKey("new"), RowVersion: 9, Payload: mustBundlePayload(t, "new", "new", email)}}
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/pull":
			return errorJSONResponse(http.StatusConflict, oversync.ErrorResponse{Error: "history_pruned", Message: "pruned"}), nil
		case "/sync/snapshot-sessions":
			return snapshotFixtureResponse(t, c, r, "unique-snapshot", 9, rows), nil
		case "/sync/snapshot-sessions/unique-snapshot":
			if r.Method == http.MethodGet {
				return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "unique-snapshot", SnapshotBundleSeq: 9, Rows: rows, NextRowOrdinal: 1}), nil
			}
			return &http.Response{StatusCode: 204, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		return nil, io.EOF
	})}
	_, err = c.Rebuild(context.Background())
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM users WHERE id='old'`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.QueryRow(`SELECT email FROM users WHERE id='new'`).Scan(&email))
	requireLastBundleSeqSeen(t, c, context.Background(), 9)
}

// B releases its value before A claims it. Failure on A therefore exercises
// rollback after the scheduler has already mutated a business row.
func TestFinalStateUniqueApply_MutationFailureRollsBackThenRetries(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`CREATE UNIQUE INDEX unique_name ON nodes(name); CREATE TRIGGER fail_install BEFORE UPDATE ON nodes WHEN NEW.id='P' AND NEW.name='other' BEGIN SELECT RAISE(ABORT,'injected install failure'); END`)
	require.NoError(t, err)
	rows := []oversync.BundleRow{}
	for i, id := range []string{"P", "Q"} {
		rows = append(rows, oversync.BundleRow{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": []string{"other", "free"}[i], "parent_id": nil})})
	}
	response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, rows...)}}
	setPullResponses(t, c, response)
	_, err = c.PullToStable(context.Background())
	require.ErrorContains(t, err, "injected install failure")
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id='Q'`).Scan(&name))
	require.Equal(t, "other", name)
	require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id='P'`).Scan(&name))
	require.Equal(t, "parent", name)
	require.EqualValues(t, 4, countA3(t, db, `SELECT count(*) FROM nodes`))
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	_, err = db.Exec(`DROP TRIGGER fail_install`)
	require.NoError(t, err)
	setPullResponses(t, c, response)
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
}

func TestFinalStateUniqueApply_PreparationSurvivesReopen(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`CREATE UNIQUE INDEX unique_name ON nodes(name)`)
	require.NoError(t, err)
	rows := []oversync.BundleRow{}
	for i, id := range []string{"P", "Q"} {
		rows = append(rows, oversync.BundleRow{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": []string{"other", "parent"}[i], "parent_id": nil})})
	}
	bundle := pullBundle(2, "peer", 1, rows...)
	fillTestBundleDescriptor(&bundle)
	c.config.MaxSyncWorkUnitsPerInvocation = 1
	for i := 0; i < 3; i++ {
		c.syncWorkUsed = 0
		c.readyAtomicUnitDispatched = false
		err = c.applyPulledBundleLockedSlice(context.Background(), &bundle)
		var work *SyncWorkLimitError
		require.ErrorAs(t, err, &work)
		requireLastBundleSeqSeen(t, c, context.Background(), 1)
		old := c
		require.NoError(t, old.Close())
		c, err = newAdmittedFixtureClient(db, old.BaseURL, old.Token, old.config)
		require.NoError(t, err)
		attachTestClient(t, c, old.UserID, "bundle-device")
	}
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{bundle}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
	require.EqualValues(t, 4, countA3(t, db, `SELECT count(*) FROM nodes`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`))
}

func TestFinalStateUniqueApply_AcyclicWorkspaceGrowsLinearly(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	const size = 200
	_, err := db.Exec(`CREATE TABLE unique_apply_peak(entries INTEGER,bytes INTEGER,edges INTEGER); INSERT INTO unique_apply_peak VALUES(0,0,0); CREATE TRIGGER unique_peak AFTER INSERT ON _sync_apply_plan BEGIN UPDATE unique_apply_peak SET entries=max(entries,NEW.entries),bytes=max(bytes,NEW.bytes),edges=max(edges,NEW.edge_entries); END; CREATE TRIGGER unique_peak_update AFTER UPDATE ON _sync_apply_plan BEGIN UPDATE unique_apply_peak SET entries=max(entries,NEW.entries),bytes=max(bytes,NEW.bytes),edges=max(edges,NEW.edge_entries); END`)
	require.NoError(t, err)
	rows := make([]oversync.BundleRow, 0, size)
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("U%04d", i)
		seedSyncedUserRow(t, c, db, id, "same", 1)
		_, err := db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; UPDATE users SET email=? WHERE id=?; UPDATE _sync_apply_state SET apply_mode=0`, fmt.Sprintf("v%04d", i), id)
		require.NoError(t, err)
		rows = append(rows, oversync.BundleRow{Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustBundlePayload(t, id, "same", fmt.Sprintf("v%04d", i+1))})
	}
	_, err = db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=1; CREATE UNIQUE INDEX unique_email ON users(email)`)
	require.NoError(t, err)
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, rows...)}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, size, countA3(t, db, `SELECT count(*) FROM users`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_edges`))
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
	var entries, bytes, edges int64
	require.NoError(t, db.QueryRow(`SELECT entries,bytes,edges FROM unique_apply_peak`).Scan(&entries, &bytes, &edges))
	require.EqualValues(t, size, entries)
	require.EqualValues(t, size-1, edges)
	t.Logf("unique chain rows=%d staged_entries=%d staged_bytes=%d dependency_edges=%d", size, entries, bytes, edges)
}

func TestFinalStateUniqueApply_CommittedReplayPreservesDirtyClosure(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`CREATE UNIQUE INDEX unique_name ON nodes(name); UPDATE nodes SET name='changed child' WHERE id='C'; CREATE TRIGGER application_delete AFTER DELETE ON nodes BEGIN SELECT 1; END`)
	require.NoError(t, err)
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		_, err := db.Exec(`UPDATE nodes SET name='local grandchild' WHERE id='D'`)
		require.NoError(t, err)
		rows := []oversync.BundleRow{}
		for i, id := range []string{"C", "P", "Q"} {
			var parent any
			if id == "C" {
				parent = "P"
			}
			rows = append(rows, oversync.BundleRow{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": []string{"changed child", "other", "parent"}[i], "parent_id": parent})})
		}
		committed := &mockCommittedBundle{BundleSeq: 2, SourceID: session.SourceID, SourceBundleID: session.SourceBundleID, PlannedRowCount: session.RequestRowCount, Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows), CanonicalRequestHash: session.CanonicalRequestHash, Generation: session.Generation}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[2] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	c.HTTP = &http.Client{Transport: server}
	_, err = c.PushPending(context.Background())
	var blocked *FinalStateApplyBlockedError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, "application_delete", blocked.Trigger)
	var identity string
	require.NoError(t, db.QueryRow(`SELECT source_id||':'||source_bundle_id||':'||canonical_request_hash FROM _sync_outbox_bundle`).Scan(&identity))
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	_, err = db.Exec(`DROP TRIGGER application_delete`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id='D'`).Scan(&name))
	require.Equal(t, "local grandchild", name)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{"id":"D"}'`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
	require.Len(t, server.pushRequests, 1)
	require.Empty(t, server.resolveRequests)
	require.Len(t, server.ackRequests, 1)
	require.NotEmpty(t, identity)
}

func TestFinalStateUniqueApply_IndexChangeInvalidatesPreparedGraph(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`CREATE UNIQUE INDEX unique_parent ON nodes(parent_id)`)
	require.NoError(t, err)
	rows := []oversync.BundleRow{}
	for i, id := range []string{"P", "Q"} {
		rows = append(rows, oversync.BundleRow{Schema: "main", Table: "nodes", Key: oversync.SyncKey{"id": id}, Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": id, "name": []string{"other", "free"}[i], "parent_id": nil})})
	}
	bundle := pullBundle(2, "peer", 1, rows...)
	fillTestBundleDescriptor(&bundle)
	ctx := withUniqueApplyContext(context.Background())
	c.syncWorkUsed = 0
	c.readyAtomicUnitDispatched = false
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	require.NoError(t, c.prepareApplySurvivors(ctx, tx, applyUnit("pull", 2, bundle.BundleHash), len(rows), func(i int) (applySurvivor, error) { return c.pulledApplySurvivor(ctx, tx, rows[i]) }))
	require.NoError(t, tx.Commit(ctx))
	require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'`))
	_, err = db.Exec(`CREATE UNIQUE INDEX unique_name ON nodes(name)`)
	require.NoError(t, err)
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{bundle}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
}
