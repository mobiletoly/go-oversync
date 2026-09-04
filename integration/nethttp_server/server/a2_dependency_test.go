package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type a2LiveWire struct{ bodies []string }

func (w *a2LiveWire) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/sync/push" {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, err
		}
		if request.Header.Get("Content-Encoding") == "gzip" {
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			data, err = io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return nil, err
			}
		}
		w.bodies = append(w.bodies, string(data))
	}
	return http.DefaultTransport.RoundTrip(request)
}

func TestA2Live_DependencyReparentBeyondHardLookahead(t *testing.T) {
	authority, err := NewA2DependencyTestServer()
	require.NoError(t, err)
	defer func() { require.NoError(t, authority.CloseWithError()) }()
	ctx := context.Background()
	user := "a2-go-" + uuid.NewString()
	token, err := authority.GenerateToken(user, time.Hour)
	require.NoError(t, err)
	config := defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
	config.UploadLimit = 2
	client := newReadyOnlyTestClient(t, filepath.Join(t.TempDir(), "client.sqlite"), authority.URL(),
		func(context.Context) (string, error) { return token, nil }, config,
		`CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED)`)
	defer client.Close()
	wire := &a2LiveWire{}
	client.HTTP = &http.Client{Transport: wire}
	_, err = client.Attach(ctx, user)
	require.NoError(t, err)
	ids := make([]string, 8)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	parent, other, child, clean := ids[0], ids[1], ids[2], ids[3]
	_, err = client.Database().Exec(`INSERT INTO categories VALUES(?,'parent',NULL),(?,'other',NULL),(?,'child',?),(?,'clean',?)`, parent, other, child, parent, clean, other)
	require.NoError(t, err)
	for i := 4; i < 8; i++ {
		_, err = client.Database().Exec(`INSERT INTO categories VALUES(?,?,NULL)`, ids[i], fmt.Sprint(i))
		require.NoError(t, err)
	}
	for i := 0; i < 4; i++ {
		_, err = client.PushPending(ctx)
		require.NoError(t, err)
	}
	require.Len(t, wire.bodies, 4)
	db := client.Database()
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM categories WHERE id=?`, parent)
	require.NoError(t, err)
	for i := 4; i < 8; i++ {
		_, err = tx.Exec(`UPDATE categories SET name=name||'!' WHERE id=?`, ids[i])
		require.NoError(t, err)
	}
	_, err = tx.Exec(`UPDATE categories SET parent_id=? WHERE id=?`, other, child)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	var parentOrdinal, childOrdinal int64
	require.NoError(t, db.QueryRow(`SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json=?`, fmt.Sprintf(`{"id":"%s"}`, parent)).Scan(&parentOrdinal))
	require.NoError(t, db.QueryRow(`SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json=?`, fmt.Sprintf(`{"id":"%s"}`, child)).Scan(&childOrdinal))
	require.Equal(t, int64(5), childOrdinal-parentOrdinal)
	require.Greater(t, childOrdinal-parentOrdinal, int64(3))
	before := baselineLiveVector(t, db)
	cleanBefore := a2LiveRecord(t, before, "_sync_row_state", clean)
	childBefore := a2LiveRecord(t, before, "_sync_dirty_rows", child)
	for call := 0; call < 3; call++ {
		_, err = client.PushPending(ctx)
		require.NoError(t, err)
		require.Len(t, wire.bodies, 5+call)
		var body oversync.CompactPushRequest
		require.NoError(t, json.Unmarshal([]byte(wire.bodies[4+call]), &body))
		rows := 0
		for _, group := range body.Rows {
			rows += len(group.Rows)
		}
		require.Equal(t, 2, rows)
		require.Equal(t, int64(5+call), body.SourceBundleID)
		require.Len(t, body.CanonicalRequestHash, 64)
		after := baselineLiveVector(t, db)
		require.JSONEq(t, string(cleanBefore), string(a2LiveRecord(t, after, "_sync_row_state", clean)))
		var remote string
		require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.id),'[]'::jsonb)::text FROM business.categories c WHERE _sync_scope_id=$1`, user).Scan(&remote))
		var actualParent string
		var count int64
		require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT parent_id::text FROM business.categories WHERE _sync_scope_id=$1 AND id=$2`, user, child).Scan(&actualParent))
		require.NoError(t, authority.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM business.categories WHERE _sync_scope_id=$1 AND id=$2`, user, parent).Scan(&count))
		if call < 2 {
			require.Equal(t, parent, actualParent)
			require.Equal(t, int64(1), count)
			require.JSONEq(t, string(childBefore), string(a2LiveRecord(t, after, "_sync_dirty_rows", child)))
		} else {
			require.Equal(t, other, actualParent)
			require.Zero(t, count)
		}
		t.Logf("A2-03 real Go call=%d selected=%s local=%s remote=%s", call+1, wire.bodies[4+call], after, remote)
	}
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows"} {
		var count int64
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM `+table).Scan(&count))
		require.Zero(t, count)
	}
}

func a2LiveRecord(t *testing.T, vector, table, id string) json.RawMessage {
	t.Helper()
	var tables map[string][]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(vector), &tables))
	for _, row := range tables[table] {
		var fields map[string]struct {
			Value *string `json:"value"`
		}
		require.NoError(t, json.Unmarshal(row, &fields))
		key := fields["key_json"].Value
		if key != nil && *key == fmt.Sprintf(`{"id":"%s"}`, id) {
			return row
		}
	}
	t.Fatalf("missing %s/%s in full vector", table, id)
	return nil
}

// a2PublicSyncFixture shares only real-server/client setup and observation.
// Every named test below owns its transition and expected public-call result.
type a2PublicSyncFixture struct {
	t                         *testing.T
	authority                 *TestServer
	client                    *oversqlite.Client
	wire                      *a2LiveWire
	user, token, path, source string
	config                    *oversqlite.Config
}

type a2PublicSyncObserver struct {
	wire          *a2LiveWire
	before, after func()
}

func (w *a2PublicSyncObserver) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/sync/push" {
		w.before()
	}
	response, err := w.wire.RoundTrip(r)
	if err == nil && r.URL.Path == "/sync/push" {
		w.after()
	}
	return response, err
}
func newA2PublicSyncFixture(t *testing.T) *a2PublicSyncFixture {
	t.Helper()
	authority, err := NewA2DependencyTestServer()
	require.NoError(t, err)
	f := &a2PublicSyncFixture{t: t, authority: authority, user: "a205-go-" + uuid.NewString(), path: filepath.Join(t.TempDir(), "public-sync.sqlite"), wire: &a2LiveWire{}}
	f.token, err = authority.GenerateToken(f.user, time.Hour)
	require.NoError(t, err)
	f.config = defaultConfigForTest(t, "business", []oversqlite.SyncTable{{TableName: "categories", SyncKeyColumnName: "id"}})
	f.config.UploadLimit = 2
	f.reopen()
	t.Cleanup(func() { require.NoError(t, f.client.Close()); require.NoError(t, authority.CloseWithError()) })
	_, err = f.client.Attach(context.Background(), f.user)
	require.NoError(t, err)
	require.NoError(t, f.client.Database().QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&f.source))
	return f
}
func (f *a2PublicSyncFixture) reopen() {
	f.client = newReadyOnlyTestClient(f.t, f.path, f.authority.URL(), func(context.Context) (string, error) { return f.token, nil }, f.config,
		`CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED)`)
	f.client.HTTP = &http.Client{Transport: &a2PublicSyncObserver{wire: f.wire, before: func() { f.t.Logf("A2-05 pre-push local=%s", baselineLiveVector(f.t, f.client.Database())) }, after: func() { f.t.Logf("A2-05 wire=%s remote=%s", f.wire.bodies[len(f.wire.bodies)-1], f.remote()) }}}
}
func (f *a2PublicSyncFixture) remote() string {
	var rows string
	require.NoError(f.t, f.authority.Pool.QueryRow(context.Background(), `SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c.id),'[]'::jsonb)::text FROM business.categories c WHERE _sync_scope_id=$1`, f.user).Scan(&rows))
	return rows
}
func (f *a2PublicSyncFixture) next() int64 {
	var n int64
	require.NoError(f.t, f.client.Database().QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=(SELECT current_source_id FROM _sync_attachment_state)`).Scan(&n))
	return n
}
func (f *a2PublicSyncFixture) assertBatches(start int, next int64, count int) {
	require.Len(f.t, f.wire.bodies, start+count)
	for i, raw := range f.wire.bodies[start:] {
		var b oversync.CompactPushRequest
		require.NoError(f.t, json.Unmarshal([]byte(raw), &b))
		n := 0
		for _, g := range b.Rows {
			n += len(g.Rows)
		}
		require.Greater(f.t, n, 0)
		require.LessOrEqual(f.t, n, 2)
		require.Equal(f.t, next+int64(i), b.SourceBundleID)
		require.Len(f.t, b.CanonicalRequestHash, 64)
	}
	require.Equal(f.t, next+int64(count), f.next())
	var source string
	require.NoError(f.t, f.client.Database().QueryRow(`SELECT current_source_id FROM _sync_attachment_state`).Scan(&source))
	require.Equal(f.t, f.source, source)
	checkpoint, err := f.client.LastBundleSeqSeen(context.Background())
	require.NoError(f.t, err)
	require.Equal(f.t, next+int64(count)-1, checkpoint)
}
func (f *a2PublicSyncFixture) assertDrained() {
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows"} {
		var count int64
		require.NoError(f.t, f.client.Database().QueryRow(`SELECT COUNT(*) FROM `+table).Scan(&count))
		require.Zero(f.t, count)
	}
	var state string
	require.NoError(f.t, f.client.Database().QueryRow(`SELECT state FROM _sync_outbox_bundle`).Scan(&state))
	require.Equal(f.t, "none", state)
	rows, err := f.client.Database().Query(`PRAGMA foreign_key_check`)
	require.NoError(f.t, err)
	require.False(f.t, rows.Next())
	require.NoError(f.t, rows.Close())
}
func TestA2Live_PublicSyncReparentBeyondHardLookahead(t *testing.T) {
	for _, newParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("new-parent-%t", newParent), func(t *testing.T) {
			f := newA2PublicSyncFixture(t)
			db := f.client.Database()
			ids := make([]string, 9)
			for i := range ids {
				ids[i] = uuid.NewString()
			}
			p, q, c, d := ids[0], ids[1], ids[2], ids[3]
			_, err := db.Exec(`INSERT INTO categories VALUES(?,'parent',NULL),(?,'other',NULL),(?,'child',?),(?,'clean',?)`, p, q, c, p, d, c)
			require.NoError(t, err)
			for i := 4; i < 8; i++ {
				_, err = db.Exec(`INSERT INTO categories VALUES(?,?,NULL)`, ids[i], fmt.Sprint(i))
				require.NoError(t, err)
			}
			_, err = f.client.Sync(context.Background())
			require.NoError(t, err)
			f.assertDrained()
			require.Len(t, f.wire.bodies, 4)
			before := baselineLiveVector(t, db)
			clean := a2LiveRecord(t, before, "_sync_row_state", d)
			cleanBusiness := f.remote()
			var cleanLocal, cleanEdges string
			require.NoError(t, db.QueryRow(`SELECT json_array(id,name,parent_id) FROM categories WHERE id=?`, d).Scan(&cleanLocal))
			require.NoError(t, db.QueryRow(`SELECT COALESCE(json_group_array(json_array(view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision)), '[]') FROM (SELECT view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision FROM _sync_relationships WHERE child_key=? ORDER BY view,child_schema,child_table,fk_column)`, fmt.Sprintf(`{"id":"%s"}`, d)).Scan(&cleanEdges))
			tx, err := db.Begin()
			require.NoError(t, err)
			_, err = tx.Exec(`DELETE FROM categories WHERE id=?`, p)
			require.NoError(t, err)
			for i := 4; i < 8; i++ {
				_, err = tx.Exec(`UPDATE categories SET name=name||'!' WHERE id=?`, ids[i])
				require.NoError(t, err)
			}
			parent := q
			if newParent {
				parent = ids[8]
			}
			_, err = tx.Exec(`UPDATE categories SET parent_id=? WHERE id=?`, parent, c)
			require.NoError(t, err)
			if newParent {
				_, err = tx.Exec(`INSERT INTO categories VALUES(?,'new',NULL)`, parent)
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			var po, co int64
			require.NoError(t, db.QueryRow(`SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json=?`, fmt.Sprintf(`{"id":"%s"}`, p)).Scan(&po))
			require.NoError(t, db.QueryRow(`SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json=?`, fmt.Sprintf(`{"id":"%s"}`, c)).Scan(&co))
			require.Equal(t, int64(5), co-po)
			require.Greater(t, co-po, int64(3))
			start, next := len(f.wire.bodies), f.next()
			prepared := baselineLiveVector(t, db)
			// Exactly one public high-level call owns this measured drain.
			report, err := f.client.Sync(context.Background())
			require.NoError(t, err)
			require.Equal(t, oversqlite.PushOutcomeCommitted, report.PushOutcome)
			batches := 3
			if newParent {
				batches = 4
			}
			f.assertBatches(start, next, batches)
			f.assertDrained()
			after := baselineLiveVector(t, db)
			require.JSONEq(t, string(clean), string(a2LiveRecord(t, after, "_sync_row_state", d)))
			var actualLocal, actualEdges string
			require.NoError(t, db.QueryRow(`SELECT json_array(id,name,parent_id) FROM categories WHERE id=?`, d).Scan(&actualLocal))
			require.NoError(t, db.QueryRow(`SELECT COALESCE(json_group_array(json_array(view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision)), '[]') FROM (SELECT view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision FROM _sync_relationships WHERE child_key=? ORDER BY view,child_schema,child_table,fk_column)`, fmt.Sprintf(`{"id":"%s"}`, d)).Scan(&actualEdges))
			require.JSONEq(t, cleanLocal, actualLocal)
			require.JSONEq(t, cleanEdges, actualEdges)
			var count int64
			var actual, cleanParent, name string
			require.NoError(t, f.authority.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM business.categories WHERE _sync_scope_id=$1 AND id=$2`, f.user, p).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, f.authority.Pool.QueryRow(context.Background(), `SELECT parent_id::text FROM business.categories WHERE _sync_scope_id=$1 AND id=$2`, f.user, c).Scan(&actual))
			require.Equal(t, parent, actual)
			require.NoError(t, f.authority.Pool.QueryRow(context.Background(), `SELECT parent_id::text,name FROM business.categories WHERE _sync_scope_id=$1 AND id=$2`, f.user, d).Scan(&cleanParent, &name))
			require.Equal(t, c, cleanParent)
			require.Equal(t, "clean", name)
			t.Logf("A2-05 ONE-CALL reparent newParent=%t seedRemote=%s before=%s after=%s remote=%s report=%+v", newParent, cleanBusiness, prepared, after, f.remote(), report)
		})
	}
}
func TestA2Live_PublicSyncIndependentDeletePairs(t *testing.T) {
	f := newA2PublicSyncFixture(t)
	db := f.client.Database()
	parents, children := []string{}, []string{}
	for i := 0; i < 3; i++ {
		p, c := uuid.NewString(), uuid.NewString()
		parents = append(parents, p)
		children = append(children, c)
		_, err := db.Exec(`INSERT INTO categories VALUES(?,'parent',NULL),(?,'child',?)`, p, c, p)
		require.NoError(t, err)
	}
	_, err := f.client.Sync(context.Background())
	require.NoError(t, err)
	f.assertDrained()
	tx, err := db.Begin()
	require.NoError(t, err)
	for _, p := range parents {
		_, err = tx.Exec(`DELETE FROM categories WHERE id=?`, p)
		require.NoError(t, err)
	}
	for _, c := range children {
		_, err = tx.Exec(`DELETE FROM categories WHERE id=?`, c)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	before := baselineLiveVector(t, db)
	start, next := len(f.wire.bodies), f.next()
	report, err := f.client.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, report.PushOutcome)
	f.assertBatches(start, next, 3)
	f.assertDrained()
	require.JSONEq(t, "[]", f.remote())
	var count int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM categories`).Scan(&count))
	require.Zero(t, count)
	t.Logf("A2-05 ONE-CALL independent-pairs before=%s after=%s remote=%s report=%+v", before, baselineLiveVector(t, db), f.remote(), report)
}
func TestA2Live_PublicSyncConfirmedPrefixBeforeOversizedCycle(t *testing.T) {
	f := newA2PublicSyncFixture(t)
	db := f.client.Database()
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	u := uuid.NewString()
	for _, id := range append(append([]string{}, ids...), u) {
		_, err := db.Exec(`INSERT INTO categories VALUES(?,'old',NULL)`, id)
		require.NoError(t, err)
	}
	_, err := f.client.Sync(context.Background())
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	for i, id := range ids {
		_, err = tx.Exec(`UPDATE categories SET parent_id=? WHERE id=?`, ids[(i+1)%3], id)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
	_, err = f.client.Sync(context.Background())
	require.NoError(t, err)
	f.assertDrained()
	cycleRemote := map[string]string{}
	for _, id := range ids {
		var row string
		require.NoError(t, f.authority.Pool.QueryRow(context.Background(), `SELECT to_jsonb(c)::text FROM business.categories c WHERE _sync_scope_id=$1 AND id=$2`, f.user, id).Scan(&row))
		cycleRemote[id] = row
	}
	tx, err = db.Begin()
	require.NoError(t, err)
	for _, id := range ids {
		_, err = tx.Exec(`DELETE FROM categories WHERE id=?`, id)
		require.NoError(t, err)
	}
	_, err = tx.Exec(`UPDATE categories SET name='safe' WHERE id=?`, u)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	before := baselineLiveVector(t, db)
	start, next := len(f.wire.bodies), f.next()
	_, err = f.client.Sync(context.Background())
	var group *oversqlite.PushDependencyGroupExceedsAdmittedLimitError
	require.ErrorAs(t, err, &group)
	require.Equal(t, "rows", group.Dimension)
	require.Equal(t, int64(3), group.ComponentRows)
	require.Equal(t, int64(1), group.Progress.CommittedBundles)
	require.Equal(t, int64(3), group.Progress.DirtyRows)
	f.assertBatches(start, next, 1)
	after := baselineLiveVector(t, db)
	for _, id := range ids {
		for _, table := range []string{"_sync_dirty_rows", "_sync_row_state"} {
			require.JSONEq(t, string(a2LiveRecord(t, before, table, id)), string(a2LiveRecord(t, after, table, id)))
		}
	}
	var name string
	require.NoError(t, f.authority.Pool.QueryRow(context.Background(), `SELECT name FROM business.categories WHERE _sync_scope_id=$1 AND id=$2`, f.user, u).Scan(&name))
	require.Equal(t, "safe", name)
	var count int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&count))
	require.Zero(t, count)
	var outboxState string
	require.NoError(t, db.QueryRow(`SELECT state FROM _sync_outbox_bundle`).Scan(&outboxState))
	require.Equal(t, "none", outboxState)
	for _, id := range ids {
		var row string
		require.NoError(t, f.authority.Pool.QueryRow(context.Background(), `SELECT to_jsonb(c)::text FROM business.categories c WHERE _sync_scope_id=$1 AND id=$2`, f.user, id).Scan(&row))
		require.JSONEq(t, cycleRemote[id], row)
	}
	remote := f.remote()
	calls := len(f.wire.bodies)
	require.NoError(t, f.client.Close())
	f.reopen()
	_, err = f.client.Attach(context.Background(), f.user)
	require.NoError(t, err)
	require.JSONEq(t, after, baselineLiveVector(t, f.client.Database()))
	_, err = f.client.Sync(context.Background())
	require.ErrorAs(t, err, &group)
	require.Zero(t, group.Progress.CommittedBundles)
	require.Len(t, f.wire.bodies, calls)
	require.JSONEq(t, after, baselineLiveVector(t, f.client.Database()))
	require.JSONEq(t, remote, f.remote())
	t.Logf("A2-05 ONE-CALL confirmed-prefix before=%s after=%s remote=%s error=%+v", before, after, remote, group)
}
