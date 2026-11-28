package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// Pull planning discovers dependents by walking foreign-key relationships out of the pulled rows, and
// only ever looks a relationship up by a row already in the plan. Relationships whose parent table
// cannot be reached from the pulled rows' tables are never read and need not be indexed. These tests
// record every relationship the planner indexes and hold the final state to the same survivor
// guarantees as before.
//
// parents <- children <- toys form one family; crews <- sailors another; notes stands alone.
func newPullFamiliesClient(t *testing.T, notes int) (*Client, *sql.DB) {
	t.Helper()
	tables := []SyncTable{}
	for _, name := range []string{"children", "crews", "notes", "parents", "sailors", "toys"} {
		tables = append(tables, SyncTable{TableName: name, SyncKeyColumnName: "id"})
	}
	c, db := newBundleClient(t, "main", tables,
		`CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES parents(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TABLE toys(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, child_id TEXT REFERENCES children(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TABLE crews(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)`,
		`CREATE TABLE sailors(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, crew_id TEXT REFERENCES crews(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)`)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: server}
	exec := func(query string, args ...any) {
		_, err := db.Exec(query, args...)
		require.NoError(t, err)
	}
	exec(`INSERT INTO parents VALUES('P','parent'),('Q','other parent')`)
	exec(`INSERT INTO children VALUES('C','child','P')`)
	for i := 0; i < 30; i++ {
		exec(`INSERT INTO children VALUES(?,'sibling','Q')`, fmt.Sprintf("K%d", i))
	}
	exec(`INSERT INTO toys VALUES('T','toy','C')`)
	exec(`INSERT INTO crews VALUES('W','crew')`)
	for i := 0; i < 20; i++ {
		exec(`INSERT INTO sailors VALUES(?,'sailor','W')`, fmt.Sprintf("S%d", i))
	}
	for i := 0; i < notes; i++ {
		exec(`INSERT INTO notes VALUES(?,'note')`, fmt.Sprintf("N%d", i))
	}
	for dirty := 1; dirty > 0; {
		_, err := c.PushPending(context.Background())
		require.NoError(t, err)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
	}
	exec(`CREATE TEMP TABLE indexed_relationships(parent_table TEXT NOT NULL, child_table TEXT NOT NULL)`)
	exec(`CREATE TEMP TRIGGER record_indexed_relationship AFTER INSERT ON main._sync_apply_edges
		BEGIN INSERT INTO indexed_relationships VALUES(NEW.parent_table, NEW.child_table); END`)
	return c, db
}

func indexedRelationships(t *testing.T, db *sql.DB) string {
	t.Helper()
	var pairs string
	require.NoError(t, db.QueryRow(`SELECT coalesce(group_concat(pair, ' '), '') FROM
		(SELECT DISTINCT parent_table || '<-' || child_table AS pair FROM indexed_relationships ORDER BY 1)`).Scan(&pairs))
	return pairs
}

func nextPulledBundleSeq(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var seq int64
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&seq))
	return seq + 1
}

func familyUpdate(t *testing.T, table, id string, version int64, payload map[string]any) oversync.BundleRow {
	return oversync.BundleRow{Schema: "main", Table: table, Key: mustBundleKey(id), Op: oversync.OpUpdate, RowVersion: version, Payload: mustJSONPayload(t, payload)}
}

func scalarString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var value string
	require.NoError(t, db.QueryRow(query).Scan(&value))
	return value
}

func requireUntouchedFamiliesAndCleanState(t *testing.T, c *Client, db *sql.DB, seq int64) {
	t.Helper()
	require.EqualValues(t, 20, countA3(t, db, `SELECT count(*) FROM sailors WHERE crew_id='W' AND name='sailor'`))
	require.EqualValues(t, 30, countA3(t, db, `SELECT count(*) FROM children WHERE parent_id='Q' AND name='sibling'`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_edges`))
	requireLastBundleSeqSeen(t, c, context.Background(), seq)
}

func pullFamilyRows(t *testing.T, c *Client, seq int64, rows ...oversync.BundleRow) {
	t.Helper()
	bundle := pullBundle(seq, "peer", 1, rows...)
	fillTestBundleDescriptor(&bundle)
	// Encode against this client's six-table generation; the generic helper derives a generation from
	// the bundle's own tables, which numbers them differently.
	groups, err := oversync.CompactGroupsForBundleRows(c.discovery.SchemaGeneration, bundle.Rows)
	require.NoError(t, err)
	setPullResponses(t, c, oversync.CompactPullResponse{StableBundleSeq: seq, Bundles: []oversync.CompactBundle{{
		BundleSeq: bundle.BundleSeq, SourceID: bundle.SourceID, SourceBundleID: bundle.SourceBundleID, BundleHash: bundle.BundleHash, Rows: groups}}})
	_, err = c.PullToStable(context.Background())
	require.NoError(t, err)
}

func TestPullDependencyIndexScope_StandaloneTableIndexesNoRelationships(t *testing.T) {
	c, db := newPullFamiliesClient(t, 5)
	seq := nextPulledBundleSeq(t, db)
	pullFamilyRows(t, c, seq, familyUpdate(t, "notes", "N0", seq, map[string]any{"id": "N0", "name": "remote note"}))
	require.Equal(t, "remote note", scalarString(t, db, `SELECT name FROM notes WHERE id='N0'`))
	require.Equal(t, "", indexedRelationships(t, db), "nothing references notes")
	require.Equal(t, "C", scalarString(t, db, `SELECT child_id FROM toys WHERE id='T'`))
	requireUntouchedFamiliesAndCleanState(t, c, db, seq)
}

func TestPullDependencyIndexScope_UnreferencedChildTableIndexesNoRelationships(t *testing.T) {
	c, db := newPullFamiliesClient(t, 5)
	seq := nextPulledBundleSeq(t, db)
	pullFamilyRows(t, c, seq, familyUpdate(t, "sailors", "S0", seq, map[string]any{"id": "S0", "name": "remote sailor", "crew_id": "W"}))
	require.Equal(t, "remote sailor", scalarString(t, db, `SELECT name FROM sailors WHERE id='S0'`))
	require.Equal(t, "", indexedRelationships(t, db), "sailors refers to crews, but nothing refers to sailors")
	require.EqualValues(t, 19, countA3(t, db, `SELECT count(*) FROM sailors WHERE crew_id='W' AND name='sailor'`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
	requireLastBundleSeqSeen(t, c, context.Background(), seq)
}

func TestPullDependencyIndexScope_MiddleTableIndexesOnlyRelationshipsBelowIt(t *testing.T) {
	c, db := newPullFamiliesClient(t, 5)
	seq := nextPulledBundleSeq(t, db)
	pullFamilyRows(t, c, seq, familyUpdate(t, "children", "C", seq, map[string]any{"id": "C", "name": "remote child", "parent_id": "P"}))
	require.Equal(t, "remote child", scalarString(t, db, `SELECT name FROM children WHERE id='C'`))
	// children's own reference up to parents is not walked; toys below it is.
	require.Equal(t, "children<-toys", indexedRelationships(t, db))
	require.Equal(t, "C", scalarString(t, db, `SELECT child_id FROM toys WHERE id='T'`))
	requireUntouchedFamiliesAndCleanState(t, c, db, seq)
}

func TestPullDependencyIndexScope_ParentPullRestoresSurvivorsAcrossTables(t *testing.T) {
	c, db := newPullFamiliesClient(t, 5)
	seq := nextPulledBundleSeq(t, db)
	// Deleting P cascades through C to T locally; the bundle keeps C under Q, so T must survive.
	pullFamilyRows(t, c, seq,
		oversync.BundleRow{Schema: "main", Table: "parents", Key: mustBundleKey("P"), Op: oversync.OpDelete, RowVersion: seq},
		familyUpdate(t, "children", "C", seq, map[string]any{"id": "C", "name": "child", "parent_id": "Q"}))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM parents WHERE id='P'`))
	require.Equal(t, "Q", scalarString(t, db, `SELECT parent_id FROM children WHERE id='C'`))
	require.Equal(t, "C", scalarString(t, db, `SELECT child_id FROM toys WHERE id='T'`))
	require.Equal(t, "toy", scalarString(t, db, `SELECT name FROM toys WHERE id='T'`))
	require.Equal(t, "children<-toys parents<-children", indexedRelationships(t, db))
	requireUntouchedFamiliesAndCleanState(t, c, db, seq)
}

func TestPullDependencyIndexScope_ResumedPullCompletesOnTheFullIndex(t *testing.T) {
	const notes = 150
	c, db := newPullFamiliesClient(t, notes)
	c.config.MaxSyncWorkUnitsPerInvocation = 1800
	seq := nextPulledBundleSeq(t, db)
	rows := make([]oversync.BundleRow, notes)
	for i := range rows {
		rows[i] = familyUpdate(t, "notes", fmt.Sprintf("N%d", i), seq, map[string]any{"id": fmt.Sprintf("N%d", i), "name": "remote note"})
	}
	bundle := pullBundle(seq, "peer", 1, rows...)
	fillTestBundleDescriptor(&bundle)
	slices := 0
	for {
		c.syncWorkUsed = 0
		c.readyAtomicUnitDispatched = false
		err := c.applyPulledBundleLockedSlice(context.Background(), &bundle)
		if err == nil {
			break
		}
		var limit *SyncWorkLimitError
		require.True(t, errors.As(err, &limit), "unexpected error: %v", err)
		slices++
		require.Less(t, slices, 50)
	}
	require.Positive(t, slices, "the pull must span work slices")
	require.EqualValues(t, notes, countA3(t, db, `SELECT count(*) FROM notes WHERE name='remote note'`))
	// The pulled tables are known only to the invocation that seeded every root; a resumed plan cannot
	// tell which relationships are reachable, so it indexes all of them, as before.
	require.Contains(t, indexedRelationships(t, db), "crews<-sailors")
	require.Equal(t, "C", scalarString(t, db, `SELECT child_id FROM toys WHERE id='T'`))
	requireUntouchedFamiliesAndCleanState(t, c, db, seq)
}
