package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func newA1SurvivorClient(t *testing.T, extra ...int) (*Client, *sql.DB) {
	return newA1SurvivorClientWithAction(t, "CASCADE", extra...)
}

func newA1SurvivorClientWithAction(t *testing.T, action string, extra ...int) (*Client, *sql.DB) {
	t.Helper()
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}},
		`CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL,
		parent_id TEXT REFERENCES nodes(id) ON DELETE `+action+` DEFERRABLE INITIALLY DEFERRED)`)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	_, err := db.Exec(`INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	if len(extra) > 0 {
		_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=1`)
		require.NoError(t, err)
		for i := 0; i < extra[0]; i++ {
			_, err = db.Exec(`INSERT INTO nodes VALUES(?,?,NULL)`, fmt.Sprintf("Z%04d", i), "unrelated")
			require.NoError(t, err)
		}
		_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=0`)
		require.NoError(t, err)
	}
	return c, db
}

func TestA1PullResumesDiscoveryAfterReopen(t *testing.T) {
	c, db := newA1SurvivorClient(t, 250)
	c.config.MaxSyncWorkUnitsPerInvocation = 1800
	response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}}
	fillTestBundleDescriptor(&response.Bundles[0])
	interruptions := 0
	for invocation := 0; invocation < 2; invocation++ {
		c.syncWorkUsed = 0
		c.readyAtomicUnitDispatched = false
		err := c.applyPulledBundleLockedSlice(context.Background(), &response.Bundles[0])
		var limit *SyncWorkLimitError
		require.ErrorAs(t, err, &limit)
		interruptions++
		requireLastBundleSeqSeen(t, c, context.Background(), 1)
		var original, mode, staged int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM nodes WHERE id IN ('P','C','D')`).Scan(&original))
		require.Equal(t, 3, original)
		require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_apply_state`).Scan(&mode))
		require.Zero(t, mode)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_apply_rows`).Scan(&staged))
		require.Positive(t, staged)
		old := c
		require.NoError(t, old.Close())
		c, err = newAdmittedFixtureClient(db, old.BaseURL, old.Token, old.config)
		require.NoError(t, err)
		attachTestClient(t, c, old.UserID, "bundle-device")
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	setPullResponses(t, c, response)
	_, err := c.PullToStable(context.Background())
	require.NoError(t, err, "one public call resumes the exact saved traversal")
	require.Positive(t, c.a3Stats.workSlices)
	require.Greater(t, interruptions, 1)
	requireA1Survivors(t, db)
	var remaining int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_apply_rows`).Scan(&remaining))
	require.Zero(t, remaining)
}

func TestA1CapacityFailurePreservesBusinessAndCheckpoint(t *testing.T) {
	for _, dimension := range []string{"entries", "bytes"} {
		t.Run(dimension, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			switch dimension {
			case "entries":
				c.config.MaxSyncWorkspaceEntries = 2
			case "bytes":
				c.config.MaxSyncWorkspaceBytes = 600
			}
			for attempt := 0; attempt < 8; attempt++ {
				setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}})
				_, err := c.PullToStable(context.Background())
				var work *SyncWorkLimitError
				if errors.As(err, &work) {
					continue
				}
				var limit *SyncWorkspaceLimitError
				require.ErrorAs(t, err, &limit)
				require.Equal(t, dimension, limit.Dimension)
				requireLastBundleSeqSeen(t, c, context.Background(), 1)
				var count int
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM nodes`).Scan(&count))
				require.Equal(t, 4, count)
				return
			}
			t.Fatal("hard workspace refusal did not occur")
		})
	}
}

func TestA1SmallWorkTargetEventuallyApplies(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	c.config.MaxSyncWorkUnitsPerInvocation = 100
	for attempt := 0; attempt < 32; attempt++ {
		setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}})
		_, err := c.PullToStable(context.Background())
		if err == nil {
			requireA1Survivors(t, db)
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
			return
		}
		var work *SyncWorkLimitError
		require.ErrorAs(t, err, &work)
		checkpoint := countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`)
		switch checkpoint {
		case 1:
			require.EqualValues(t, 4, countA3(t, db, `SELECT count(*) FROM nodes`))
		case 2:
			requireA1Survivors(t, db)
		default:
			t.Fatalf("unexpected checkpoint %d after work yield", checkpoint)
		}
	}
	t.Fatal("small work target did not make useful progress to atomic apply")
}

func a1NormalizedEffects(t *testing.T) []oversync.BundleRow {
	t.Helper()
	return []oversync.BundleRow{
		{Schema: "main", Table: "nodes", Key: mustBundleKey("P"), Op: oversync.OpDelete, RowVersion: 2},
		{Schema: "main", Table: "nodes", Key: mustBundleKey("C"), Op: oversync.OpUpdate, RowVersion: 2,
			Payload: mustJSONPayload(t, map[string]any{"id": "C", "name": "child", "parent_id": "Q"})},
	}
}

func requireA1Survivors(t *testing.T, db *sql.DB) {
	t.Helper()
	var name, parent string
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id='C'`).Scan(&name, &parent))
	require.Equal(t, "child", name)
	require.Equal(t, "Q", parent)
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id='D'`).Scan(&name, &parent))
	require.Equal(t, "grandchild", name)
	require.Equal(t, "C", parent)
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM nodes WHERE id='P'`).Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pragma_foreign_key_check`).Scan(&count))
	require.Zero(t, count)
}

func TestA1PullPreservesCleanGrandchild(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2,
		Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}})
	_, err := c.PullToStable(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, db)
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
	requireA1ControlState(t, db, 2)
}

func TestA1SettlementPreservesCleanGrandchild(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	server := a1SettlementServer(t)
	c.HTTP = &http.Client{Transport: server}
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, db)
	dirty, outbox, _ := pushStateCounts(t, db)
	require.Zero(t, dirty)
	require.Zero(t, outbox)
	requireA1ControlState(t, db, 3)
}

func TestA1OrdinaryRequestCounts(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	server := c.HTTP.Transport.(*mockPushServer)
	before := len(server.pushRequests)
	_, err := c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, before, "no-change push has no request")
	_, err = db.Exec(`UPDATE nodes SET name='single edit' WHERE id='D'`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, before+1, "one-row push has one request")
}

func TestA1VersionSkippedSurvivorAfterTriggerCascade(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE _sync_apply_state SET apply_mode=1;
        UPDATE nodes SET name='newer grandchild' WHERE id='D';
        UPDATE _sync_row_state SET row_version=3 WHERE key_json='{"id":"D"}';
        UPDATE _sync_apply_state SET apply_mode=0;
        CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END`)
	require.NoError(t, err)
	rows := append(a1NormalizedEffects(t), oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("D"), Op: oversync.OpUpdate, RowVersion: 2,
		Payload: mustJSONPayload(t, map[string]any{"id": "D", "name": "older", "parent_id": "C"})})
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, rows...)}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
	var name, parent string
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id='D'`).Scan(&name, &parent))
	require.Equal(t, "newer grandchild", name)
	require.Equal(t, "C", parent)
	var version int64
	require.NoError(t, db.QueryRow(`SELECT row_version FROM _sync_row_state WHERE key_json='{"id":"D"}'`).Scan(&version))
	require.EqualValues(t, 3, version)
}

func a1SettlementServer(t *testing.T) *mockPushServer {
	t.Helper()
	server := newMockPushServer(t)
	server.pushHook = func(session *mockDirectPush) *http.Response {
		rows := a1NormalizedEffects(t)
		committed := &mockCommittedBundle{BundleSeq: 2, SourceID: session.SourceID,
			SourceBundleID: session.SourceBundleID, PlannedRowCount: session.RequestRowCount,
			Rows: rows, BundleHash: mustBundleHash(t, rows), ByteCount: mustBundleByteCount(t, rows),
			CanonicalRequestHash: session.CanonicalRequestHash, Generation: session.Generation}
		server.committedBySource[session.SourceBundleID] = committed
		server.committedBySeq[2] = committed
		return server.compactResponseWithGeneration(session.Generation, committed)
	}
	return server
}

func TestA1LostSettlementResponseReplaysAfterReopen(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	server := a1SettlementServer(t)
	c.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := server.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		response.Body.Close()
		return nil, context.Canceled
	})}
	_, err = c.PushPending(context.Background())
	require.ErrorContains(t, err, "push HTTP transport failed")
	var frozen string
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&frozen))
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	require.NoError(t, c.Close())
	reopened, err := newAdmittedFixtureClient(db, c.BaseURL, c.Token, c.config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	attachTestClient(t, reopened, c.UserID, "bundle-device")
	var retained string
	require.NoError(t, db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&retained))
	require.Equal(t, frozen, retained)
	reopened.HTTP = &http.Client{Transport: server}
	_, err = reopened.PushPending(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, db)
	dirty, outbox, _ := pushStateCounts(t, db)
	require.Zero(t, dirty)
	require.Zero(t, outbox)
	require.Len(t, server.pushRequests, 1)
	require.Len(t, server.resolveRequests, 1)
	require.Len(t, server.ackRequests, 1)
}

func TestA1SettlementPreservesDirtyGrandchild(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	server := a1SettlementServer(t)
	commit := server.pushHook
	server.pushHook = func(request *mockDirectPush) *http.Response {
		_, err := db.Exec(`UPDATE nodes SET name='local grandchild' WHERE id='D'`)
		require.NoError(t, err)
		return commit(request)
	}
	c.HTTP = &http.Client{Transport: server}
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	var name, parent, payload string
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id='D'`).Scan(&name, &parent))
	require.Equal(t, "local grandchild", name)
	require.Equal(t, "C", parent)
	require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows WHERE key_json='{"id":"D"}'`).Scan(&payload))
	require.Contains(t, payload, "local grandchild")
	dirty, outbox, _ := pushStateCounts(t, db)
	require.Equal(t, 1, dirty)
	require.Zero(t, outbox)
}

func TestA1RevisionMismatchRestartsSavedDiscovery(t *testing.T) {
	c, db := newA1SurvivorClient(t, 250)
	c.config.MaxSyncWorkUnitsPerInvocation = 1800
	response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}}
	fillTestBundleDescriptor(&response.Bundles[0])
	c.syncWorkUsed = 0
	c.readyAtomicUnitDispatched = false
	err := c.applyPulledBundleLockedSlice(context.Background(), &response.Bundles[0])
	var limit *SyncWorkLimitError
	require.ErrorAs(t, err, &limit)
	var before string
	require.NoError(t, db.QueryRow(`SELECT guard FROM _sync_apply_plan`).Scan(&before))
	// A retained continuation must recheck the committed dirty revision under
	// the writer lock. Inject that durable boundary without altering wire history.
	_, err = db.Exec(`UPDATE _sync_conflict_state SET revision=revision+1`)
	require.NoError(t, err)
	c.syncWorkUsed = 0
	c.readyAtomicUnitDispatched = false
	err = c.applyPulledBundleLockedSlice(context.Background(), &response.Bundles[0])
	require.ErrorAs(t, err, &limit)
	var after string
	require.NoError(t, db.QueryRow(`SELECT guard FROM _sync_apply_plan`).Scan(&after))
	require.NotEqual(t, before, after)
	setPullResponses(t, c, response)
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
	requireA1Survivors(t, db)
}

func TestA1OldLayoutAndRetainedLimitRejectOpening(t *testing.T) {
	for _, oldLayout := range []bool{false, true} {
		t.Run(fmt.Sprint(oldLayout), func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
			require.NoError(t, err)
			client := newDiscoveryPeopleClient(t, db, "http://example.invalid")
			require.NoError(t, client.Close())
			if oldLayout {
				_, err = db.Exec(`DROP TABLE _sync_apply_plan`)
			} else {
				_, err = db.Exec(`INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes) VALUES(1,'unit','guard','walk',0,0,0,0,NULL,0,0)`)
			}
			require.NoError(t, err)
			_, err = newClientFromDB(db, "http://example.invalid", tokenProviderForTests, &Config{
				GenerationID: 1, DatabaseID: "people-db", ServiceID: "people-service", LocalVersion: 1,
				Tables: []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}}, MaxSyncWorkspaceEntries: 1})
			require.Error(t, err)
			if oldLayout {
				require.Contains(t, err.Error(), "required table _sync_apply_plan is missing")
			} else {
				var limit *SyncWorkspaceLimitError
				require.ErrorAs(t, err, &limit)
				require.Equal(t, "entries", limit.Dimension)
			}
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestA1SetNullRestorationAndRollback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			c, db := newA1SurvivorClientWithAction(t, "SET NULL")
			_, err := db.Exec(`CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END`)
			require.NoError(t, err)
			if fail {
				_, err = db.Exec(`CREATE TRIGGER deny_restore BEFORE INSERT ON nodes WHEN NEW.id='C' BEGIN SELECT RAISE(ABORT,'restore blocked'); END`)
				require.NoError(t, err)
			}
			response := oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}}
			setPullResponses(t, c, response)
			_, err = c.PullToStable(context.Background())
			if fail {
				require.Error(t, err)
				requireLastBundleSeqSeen(t, c, context.Background(), 1)
				var parent string
				require.NoError(t, db.QueryRow(`SELECT parent_id FROM nodes WHERE id='C'`).Scan(&parent))
				require.Equal(t, "P", parent)
				require.NoError(t, db.QueryRow(`SELECT parent_id FROM nodes WHERE id='D'`).Scan(&parent))
				require.Equal(t, "C", parent)
				var count int
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_apply_rows`).Scan(&count))
				require.Zero(t, count)
				_, err = db.Exec(`DROP TRIGGER deny_restore`)
				require.NoError(t, err)
				setPullResponses(t, c, response)
				_, err = c.PullToStable(context.Background())
			}
			require.NoError(t, err)
			requireA1Survivors(t, db)
		})
	}
}

func TestA1EntryLimitBoundary(t *testing.T) {
	for _, entries := range []int64{44, 45, 46} {
		t.Run(fmt.Sprint(entries), func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			c.config.MaxSyncWorkspaceEntries = entries
			setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}})
			_, err := c.PullToStable(context.Background())
			if entries == 44 {
				var limit *SyncWorkspaceLimitError
				require.ErrorAs(t, err, &limit)
				require.Equal(t, "entries", limit.Dimension)
				require.EqualValues(t, 45, limit.Used+limit.Reserved)
			} else {
				require.NoError(t, err)
				requireA1Survivors(t, db)
			}
		})
	}
}

func TestA1SurvivorApplyRetainsDeleteFirstUniqueOrdering(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`CREATE UNIQUE INDEX nodes_name ON nodes(name)`)
	require.NoError(t, err)
	effects := a1NormalizedEffects(t)
	effects[1].Payload = mustJSONPayload(t, map[string]any{"id": "C", "name": "parent", "parent_id": "Q"})
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, effects...)}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
	var name, parent string
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id='C'`).Scan(&name, &parent))
	require.Equal(t, "parent", name)
	require.Equal(t, "Q", parent)
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM nodes WHERE id='D'`).Scan(&name, &parent))
	require.Equal(t, "grandchild", name)
	require.Equal(t, "C", parent)
	requireLastBundleSeqSeen(t, c, context.Background(), 2)
}

func requireA1ControlState(t *testing.T, db *sql.DB, nextSourceBundle int) {
	t.Helper()
	for _, expected := range []struct {
		key              string
		version, deleted int
	}{{"P", 2, 1}, {"C", 2, 0}, {"D", 1, 0}, {"Q", 1, 0}} {
		var version, deleted int
		require.NoError(t, db.QueryRow(`SELECT row_version,server_kind='deleted' FROM _sync_row_state WHERE table_name='nodes' AND json_extract(key_json,'$.id')=?`, expected.key).Scan(&version, &deleted))
		require.Equal(t, expected.version, version)
		require.Equal(t, expected.deleted, deleted)
	}
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_rows", "_sync_apply_plan"} {
		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM `+table).Scan(&count))
		require.Zero(t, count)
	}
	var next, mode int
	var state string
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=(SELECT current_source_id FROM _sync_attachment_state)`).Scan(&next))
	require.Equal(t, nextSourceBundle, next)
	require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle`).Scan(&state))
	require.Equal(t, "none", state)
	require.NoError(t, db.QueryRow(`SELECT apply_mode FROM _sync_apply_state`).Scan(&mode))
	require.Zero(t, mode)
}

func TestA1ReviewTriggerDeletesAlreadyReparentedSurvivor(t *testing.T) {
	for _, mode := range []string{"settlement", "version_skipped_pull"} {
		t.Run(mode, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			_, err := db.Exec(`CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END`)
			require.NoError(t, err)
			if mode == "settlement" {
				_, err = db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
				require.NoError(t, err)
				server := a1SettlementServer(t)
				commit := server.pushHook
				server.pushHook = func(request *mockDirectPush) *http.Response {
					_, err := db.Exec(`UPDATE nodes SET parent_id='Q' WHERE id='C'`)
					require.NoError(t, err)
					return commit(request)
				}
				c.HTTP = &http.Client{Transport: server}
				_, err = c.PushPending(context.Background())
			} else {
				_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; UPDATE nodes SET parent_id='Q' WHERE id='C'; UPDATE _sync_row_state SET row_version=3 WHERE json_extract(key_json,'$.id')='C'; UPDATE _sync_apply_state SET apply_mode=0`)
				require.NoError(t, err)
				setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1, a1NormalizedEffects(t)...)}})
				_, err = c.PullToStable(context.Background())
			}
			require.NoError(t, err)
			requireA1Survivors(t, db)
			requireLastBundleSeqSeen(t, c, context.Background(), 2)
		})
	}
}

func TestA1ReviewDiscoveryWorkDoesNotRescanChildrenPerParent(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	effects := a1NormalizedEffects(t)
	for i := range effects {
		effects[i].RowVersion = 3
	}
	for i := 1; i < 8; i++ {
		p, cid, d := fmt.Sprintf("P%d", i), fmt.Sprintf("C%d", i), fmt.Sprintf("D%d", i)
		_, err := db.Exec(`INSERT INTO nodes VALUES(?,'parent',NULL),(?,'child',?),(?,'grandchild',?)`, p, cid, p, d, cid)
		require.NoError(t, err)
		effects = append(effects, oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey(p), Op: oversync.OpDelete, RowVersion: 3}, oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey(cid), Op: oversync.OpUpdate, RowVersion: 3, Payload: mustJSONPayload(t, map[string]any{"id": cid, "name": "child", "parent_id": "Q"})})
	}
	_, err := c.PushPending(context.Background())
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=1`)
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		_, err = db.Exec(`INSERT INTO nodes VALUES(?,'unrelated',NULL)`, fmt.Sprintf("Z%04d", i))
		require.NoError(t, err)
	}
	_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=0`)
	require.NoError(t, err)
	c.config.MaxSyncWorkUnitsPerInvocation = 50000
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 3, Bundles: []oversync.Bundle{pullBundle(3, "peer", 1, effects...)}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err, "one indexed build must fit; per-parent child-table scans cannot")
	requireA1Survivors(t, db)
	var grandkids int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM nodes WHERE name='grandchild'`).Scan(&grandkids))
	require.Equal(t, 8, grandkids)
}

func TestA1ReviewReopenReorderedRegistrationInvalidatesTraversal(t *testing.T) {
	tables := []SyncTable{{TableName: "parents", SyncKeyColumnName: "id"}, {TableName: "children", SyncKeyColumnName: "id"}, {TableName: "grandchildren", SyncKeyColumnName: "id"}}
	c, db := newBundleClient(t, "main", tables,
		`CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`,
		`CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES parents(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TABLE grandchildren(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES children(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`)
	c.HTTP = &http.Client{Transport: newMockPushServer(t)}
	_, err := db.Exec(`INSERT INTO parents VALUES('P','parent'),('Q','other'); INSERT INTO children VALUES('C','child','P'); INSERT INTO grandchildren VALUES('D','grandchild','C')`)
	require.NoError(t, err)
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=1`)
	require.NoError(t, err)
	for i := 0; i < 250; i++ {
		_, err = db.Exec(`INSERT INTO children VALUES(?,'unrelated','Q')`, fmt.Sprintf("A%04d", i))
		require.NoError(t, err)
	}
	_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=0`)
	require.NoError(t, err)
	rows := []oversync.BundleRow{{Schema: "main", Table: "parents", Key: mustBundleKey("P"), Op: oversync.OpDelete, RowVersion: 2}, {Schema: "main", Table: "children", Key: mustBundleKey("C"), Op: oversync.OpUpdate, RowVersion: 2, Payload: mustJSONPayload(t, map[string]any{"id": "C", "name": "child", "parent_id": "Q"})}}
	bundle := pullBundle(2, "peer", 1, rows...)
	fillTestBundleDescriptor(&bundle)
	registered, ok := mockGenerationByFingerprint.Load(c.config.generationProof.Fingerprint)
	require.True(t, ok)
	groups, err := oversync.CompactGroupsForBundleRows(registered.(oversync.SchemaGenerationCapabilities), rows)
	require.NoError(t, err)
	response := oversync.CompactPullResponse{StableBundleSeq: 2, Bundles: []oversync.CompactBundle{{BundleSeq: 2, SourceID: "peer", SourceBundleID: 1, BundleHash: bundle.BundleHash, Rows: groups}}}
	c.config.MaxSyncWorkUnitsPerInvocation = 1800
	c.syncWorkUsed = 0
	c.readyAtomicUnitDispatched = false
	err = c.applyPulledBundleLockedSlice(context.Background(), &bundle)
	var limit *SyncWorkLimitError
	require.ErrorAs(t, err, &limit)
	var before string
	require.NoError(t, db.QueryRow(`SELECT guard FROM _sync_apply_plan`).Scan(&before))
	require.NoError(t, c.Close())
	config := *c.config
	config.Tables = []SyncTable{tables[1], tables[0], tables[2]}
	reopened, err := newAdmittedFixtureClient(db, c.BaseURL, c.Token, &config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	attachTestClient(t, reopened, c.UserID, "bundle-device")
	reopened.syncWorkUsed = 0
	reopened.readyAtomicUnitDispatched = false
	err = reopened.applyPulledBundleLockedSlice(context.Background(), &bundle)
	require.ErrorAs(t, err, &limit)
	var after string
	require.NoError(t, db.QueryRow(`SELECT guard FROM _sync_apply_plan`).Scan(&after))
	require.NotEqual(t, before, after)
	setPullResponses(t, reopened, response)
	_, err = reopened.PullToStable(context.Background())
	require.NoError(t, err)
	var name, parent string
	require.NoError(t, db.QueryRow(`SELECT name,parent_id FROM grandchildren WHERE id='D'`).Scan(&name, &parent))
	require.Equal(t, "grandchild", name)
	require.Equal(t, "C", parent)
	requireLastBundleSeqSeen(t, reopened, context.Background(), 2)
}

func TestA1ReviewWireAdmissionPrecedesPayloadCopy(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	c.config.MaxSyncWorkspaceBytes = 8192
	payload := []byte(`{"id":"C","name":"` + strings.Repeat("x", 8<<20) + `","parent_id":"P"}`)
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, saveApplyPlan(context.Background(), tx, &applyPlan{unit: "allocation", guard: "guard", phase: "roots"}))
	_, err = c.wireApplySurvivor(context.Background(), tx, "nodes", "C", payload[:1024])
	// Warm database/driver paths before measuring the oversized refusal.
	require.NoError(t, err)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	row, err := c.wireApplySurvivor(context.Background(), tx, "nodes", "C", payload)
	runtime.ReadMemStats(&after)
	var limit *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "bytes", limit.Dimension)
	require.False(t, row.payload.Valid)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(len(payload)/2), "refused payload must not allocate its string copy")
	runtime.KeepAlive(payload)
	var name string
	require.NoError(t, tx.QueryRow(`SELECT name FROM nodes WHERE id='C'`).Scan(&name))
	require.Equal(t, "child", name)
	require.NoError(t, tx.Rollback())
	// The public pull path must use the same admission, preserving progress and
	// business state for a valid payload below the existing wire-body limit.
	rowPayload := mustJSONPayload(t, map[string]any{"id": "C", "name": strings.Repeat("x", 65536), "parent_id": "P"})
	setPullResponses(t, c, oversync.PullResponse{StableBundleSeq: 2, Bundles: []oversync.Bundle{pullBundle(2, "peer", 1,
		oversync.BundleRow{Schema: "main", Table: "nodes", Key: mustBundleKey("C"), Op: oversync.OpUpdate, RowVersion: 2, Payload: rowPayload})}})
	_, err = c.PullToStable(context.Background())
	require.ErrorAs(t, err, &limit)
	require.Equal(t, "bytes", limit.Dimension)
	requireLastBundleSeqSeen(t, c, context.Background(), 1)
	require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id='C'`).Scan(&name))
	require.Equal(t, "child", name)
}
