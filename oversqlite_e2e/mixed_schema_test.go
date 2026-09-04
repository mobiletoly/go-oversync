package oversqlite_e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	exampleserver "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const mixedP = "11111111-1111-4111-8111-111111111111"
const mixedQ = "22222222-2222-4222-8222-222222222222"
const mixedOther = "33333333-3333-4333-8333-333333333333"
const mixedSchema = "mixed"
const mixedScope = "mixed-scope"

const mixedOperationTimeout = 30 * time.Second

func mixedOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, mixedOperationTimeout)
}

func mixedBoundedDatabaseURL(dsn string) string {
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "statement_timeout=30000&lock_timeout=30000"
}

func mixedDeclarations() ([]oversync.RegisteredTable, []oversync.SchemaGeneration) {
	c := func(n string, wire oversync.GenerationWireType, nullable bool) oversync.GenerationColumn {
		return oversync.GenerationColumn{Name: n, WireType: wire, Nullable: nullable}
	}
	id := c("id", oversync.GenerationWireTypeUUID, false)
	old := oversync.GenerationTable{Schema: mixedSchema, Table: "people", Columns: []oversync.GenerationColumn{id, c("name", oversync.GenerationWireTypeText, false)}}
	newer := old
	newer.Columns = append(append([]oversync.GenerationColumn{}, old.Columns...), c("note", oversync.GenerationWireTypeText, true), c("project_id", oversync.GenerationWireTypeUUID, true))
	newer.ForeignKeys = []oversync.GenerationForeignKey{{Columns: []string{"project_id"}, ReferencedSchema: mixedSchema, ReferencedTable: "projects", ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeyNoAction, OnUpdate: oversync.GenerationForeignKeyNoAction}}
	projects := oversync.GenerationTable{Schema: mixedSchema, Table: "projects", Columns: []oversync.GenerationColumn{id, c("title", oversync.GenerationWireTypeText, false)}}
	notes := oversync.GenerationTable{Schema: mixedSchema, Table: "notes", Columns: []oversync.GenerationColumn{id, c("body", oversync.GenerationWireTypeText, false)}}
	return []oversync.RegisteredTable{{Schema: mixedSchema, Table: "people", SyncKeyColumns: []string{"id"}}, {Schema: mixedSchema, Table: "projects", SyncKeyColumns: []string{"id"}}, {Schema: mixedSchema, Table: "notes", SyncKeyColumns: []string{"id"}}}, []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{old}}, {ID: 2, Tables: []oversync.GenerationTable{newer, projects, notes}}}
}

func mixedServerConfig(dsn string, fresh bool) *exampleserver.ServerConfig {
	reg, gens := mixedDeclarations()
	dsn = mixedBoundedDatabaseURL(dsn)
	cfg := &exampleserver.ServerConfig{DatabaseURL: dsn, JWTSecret: "mixed-schema-test-only", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BusinessSchema: mixedSchema, AppName: "mixed", ApplicationSchemaPrepared: true, RegisteredTables: reg, SchemaGenerations: gens}
	if fresh {
		cfg.BeforeBootstrap = func(ctx context.Context, p *pgxpool.Pool, _ string) error {
			_, err := p.Exec(ctx, `CREATE SCHEMA mixed;
 CREATE TABLE mixed.projects(_sync_scope_id text NOT NULL,id uuid NOT NULL,title text NOT NULL,PRIMARY KEY(_sync_scope_id,id));
 CREATE TABLE mixed.people(_sync_scope_id text NOT NULL,id uuid NOT NULL,name text NOT NULL,note text DEFAULT 'server-default',project_id uuid,PRIMARY KEY(_sync_scope_id,id),FOREIGN KEY(_sync_scope_id,project_id) REFERENCES mixed.projects(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED);
 CREATE TABLE mixed.notes(_sync_scope_id text NOT NULL,id uuid NOT NULL,body text NOT NULL,PRIMARY KEY(_sync_scope_id,id));
 CREATE TABLE mixed.effects(id uuid PRIMARY KEY, n bigint NOT NULL);
 CREATE FUNCTION mixed.count_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mixed.effects VALUES(NEW.id,1) ON CONFLICT(id) DO UPDATE SET n=mixed.effects.n+1; RETURN NEW; END $$;
 CREATE TRIGGER mixed_effect AFTER INSERT OR UPDATE ON mixed.people FOR EACH ROW EXECUTE FUNCTION mixed.count_effect();`)
			return err
		}
	}
	return cfg
}

func mixedConfig(t *testing.T, g int) *oversqlite.Config {
	t.Helper()
	_, gens := mixedDeclarations()
	cfg := &oversqlite.Config{
		GenerationID: oversync.SchemaGenerationID(g), DatabaseID: "coexistence-client", ServiceID: "coexistence-service", LocalVersion: 1,
		UploadLimit: 1000, DownloadLimit: 1000,
	}
	for _, table := range gens[g-1].Tables {
		cfg.Tables = append(cfg.Tables, oversqlite.SyncTable{TableName: table.Table, SyncKeyColumnName: "id"})
	}
	cfg.LocalOnlyTables = []string{"sentinel"}
	cfg.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}
	cfg.SnapshotChunkRows = 1
	return cfg
}

type mixedClient struct {
	c    *oversqlite.Client
	db   *sql.DB
	path string
	g    int
}
type mixedFixture struct {
	t        *testing.T
	ctx      context.Context
	server   *exampleserver.TestServer
	dsn      string
	a, b     *mixedClient
	expected map[string]map[string][]any
}

func mixedOpen(t *testing.T, ctx context.Context, path, url, token string, g int, fresh bool) *mixedClient {
	t.Helper()
	opCtx, cancel := mixedOperationContext(ctx)
	defer cancel()
	tokenProvider := func(context.Context) (string, error) { return token, nil }
	var c *oversqlite.Client
	if fresh {
		ddl := `CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL);`
		if g == 2 {
			ddl = `CREATE TABLE projects(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL); CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,body TEXT NOT NULL); CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,note TEXT DEFAULT 'server-default',project_id TEXT REFERENCES projects(id) DEFERRABLE INITIALLY DEFERRED);`
		}
		c = openFreshSQLiteClient(t, path, url, tokenProvider, mixedConfig(t, g), ddl+`CREATE TABLE sentinel(id TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO sentinel VALUES('local','preserve');`)
	} else {
		c = openReadySQLiteClient(t, path, url, tokenProvider, mixedConfig(t, g))
	}
	c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: mixedVisibilityTransport(g, http.DefaultTransport)}
	r, err := c.Attach(opCtx, mixedScope)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, r.Status)
	return &mixedClient{c: c, db: c.Database(), path: path, g: g}
}

// Inspect real response bodies before returning the same bytes to the SDK. No successful response is invented.
func mixedVisibilityTransport(g int, next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := next.RoundTrip(r)
		if err != nil || g != 1 {
			return resp, err
		}
		if strings.Contains(r.URL.Path, "/watch") {
			return resp, nil
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if len(bytes.TrimSpace(body)) != 0 {
			var document any
			if err := json.Unmarshal(body, &document); err != nil {
				return nil, fmt.Errorf("G1 visibility response is not valid JSON: %w", err)
			}
			if err := validateMixedGenerationOneVisibility(document, "$"); err != nil {
				return nil, err
			}
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		return resp, nil
	})
}

func validateMixedGenerationOneVisibility(value any, path string) error {
	switch node := value.(type) {
	case []any:
		for index, child := range node {
			if err := validateMixedGenerationOneVisibility(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	case map[string]any:
		for key, child := range node {
			if key == "note" || key == "project_id" {
				return fmt.Errorf("G1 hidden field disclosed at %s.%s", path, key)
			}
			if key == "table" || key == "table_name" || key == "referenced_table" {
				if table, ok := child.(string); ok && (table == "projects" || table == "notes") {
					return fmt.Errorf("G1 hidden table disclosed at %s.%s", path, key)
				}
			}
			if key == "column" || key == "column_name" {
				if column, ok := child.(string); ok && (column == "note" || column == "project_id") {
					return fmt.Errorf("G1 hidden column disclosed at %s.%s", path, key)
				}
			}
			if err := validateMixedGenerationOneVisibility(child, path+"."+key); err != nil {
				return err
			}
		}
	}
	return nil
}

func newMixedFixture(t *testing.T, seed bool) *mixedFixture {
	t.Helper()
	if os.Getenv("MIXED_GENERATION_QUALIFY") != "1" {
		t.Skip("explicit MIXED_GENERATION_QUALIFY=1 coexistence lane required")
	}
	require.NotEmpty(t, os.Getenv("TEST_DATABASE_URL"), "explicit run-owned PostgreSQL required")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	f := &mixedFixture{t: t, ctx: ctx, expected: map[string]map[string][]any{"people": {}, "projects": {}, "notes": {}}}
	f.dsn = createIsolatedExampleDatabase(t, os.Getenv("TEST_DATABASE_URL"))
	s, err := exampleserver.NewTestServer(mixedServerConfig(f.dsn, true))
	require.NoError(t, err)
	f.server = s
	t.Cleanup(s.Close)
	var major int
	var fsync, syncCommit, full string
	opCtx, cancel := mixedOperationContext(ctx)
	require.NoError(t, s.Pool.QueryRow(opCtx, `SELECT current_setting('server_version_num')::int/10000,current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')`).Scan(&major, &fsync, &syncCommit, &full))
	cancel()
	require.Equal(t, os.Getenv("MIXED_GENERATION_PG_MAJOR"), fmt.Sprint(major))
	require.Equal(t, "on", fsync)
	require.Equal(t, "on", syncCommit)
	require.Equal(t, "on", full)
	t.Logf("PostgreSQL %d fsync=%s synchronous_commit=%s full_page_writes=%s", major, fsync, syncCommit, full)
	token, err := s.GenerateToken(mixedScope, time.Hour)
	require.NoError(t, err)
	dir := t.TempDir()
	f.a = mixedOpen(t, ctx, filepath.Join(dir, "A.sqlite"), s.URL(), token, 1, true)
	f.b = mixedOpen(t, ctx, filepath.Join(dir, "B.sqlite"), s.URL(), token, 2, true)
	t.Cleanup(func() {
		for _, c := range []*mixedClient{f.a, f.b} {
			if c.c != nil {
				c.c.Close()
				c.db.Close()
			}
		}
	})
	require.NotEqual(t, requireCurrentSourceIDE2E(t, f.a.c, ctx), requireCurrentSourceIDE2E(t, f.b.c, ctx))
	var n int
	require.NoError(t, f.a.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('people')`).Scan(&n))
	require.Equal(t, 2, n)
	require.NoError(t, f.a.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name IN ('projects','notes')`).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, f.b.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('people')`).Scan(&n))
	require.Equal(t, 4, n)
	t.Cleanup(func() {
		if t.Failed() {
			f.diagnostics()
		}
		if !t.Failed() {
			f.exactDatabase()
		}
	})
	if seed {
		f.expected["people"][mixedP] = []any{mixedP, "base", "secret-1", mixedQ}
		f.expected["projects"][mixedQ] = []any{mixedQ, "Q"}
		f.exec(f.b, `INSERT INTO projects VALUES(?,'Q'); INSERT INTO people VALUES(?,'base','secret-1',?)`, mixedQ, mixedP, mixedQ)
		f.push(f.b)
		f.pull(f.a)
	}
	return f
}
func (f *mixedFixture) exec(c *mixedClient, q string, args ...any) {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	_, err := c.db.ExecContext(opCtx, q, args...)
	require.NoError(f.t, err)
}
func (f *mixedFixture) push(c *mixedClient) {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	mustPushPendingE2E(f.t, c.c, opCtx)
}
func (f *mixedFixture) pull(c *mixedClient) {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	mustPullToStableE2E(f.t, c.c, opCtx)
}
func (f *mixedFixture) seq() int64 {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	var n int64
	require.NoError(f.t, f.server.Pool.QueryRow(opCtx, `SELECT next_bundle_seq-1 FROM sync.user_state WHERE user_id=$1`, mixedScope).Scan(&n))
	return n
}
func (f *mixedFixture) person(id, name string, note, project any, present bool) {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	if present {
		f.expected["people"][id] = []any{id, name, note, project}
	} else {
		delete(f.expected["people"], id)
	}
	for _, c := range []*mixedClient{f.a, f.b} {
		var actual string
		err := c.db.QueryRowContext(opCtx, `SELECT name FROM people WHERE id=?`, id).Scan(&actual)
		if !present {
			require.ErrorIs(f.t, err, sql.ErrNoRows)
			continue
		}
		require.NoError(f.t, err)
		require.Equal(f.t, name, actual)
		if c.g == 2 {
			var n, p any
			require.NoError(f.t, c.db.QueryRowContext(opCtx, `SELECT note,project_id FROM people WHERE id=?`, id).Scan(&n, &p))
			require.Equal(f.t, note, n)
			require.Equal(f.t, project, p)
		}
	}
	var actual string
	var n, p any
	err := f.server.Pool.QueryRow(opCtx, `SELECT name,note,project_id::text FROM mixed.people WHERE id=$1`, id).Scan(&actual, &n, &p)
	if !present {
		require.ErrorIs(f.t, err, pgx.ErrNoRows)
		return
	}
	require.NoError(f.t, err)
	require.Equal(f.t, name, actual)
	require.Equal(f.t, note, n)
	require.Equal(f.t, project, p)
}
func (f *mixedFixture) settle() {
	f.t.Helper()
	f.pull(f.a)
	f.pull(f.b)
	target := f.seq()
	for _, c := range []*mixedClient{f.a, f.b} {
		opCtx, cancel := mixedOperationContext(f.ctx)
		cp, err := c.c.LastBundleSeqSeen(opCtx)
		require.NoError(f.t, err)
		require.Equal(f.t, target, cp)
		for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_push_stage", "_sync_snapshot_stage"} {
			var n int
			require.NoError(f.t, c.db.QueryRowContext(opCtx, `SELECT count(*) FROM `+table).Scan(&n))
			require.Zero(f.t, n, table)
		}
		var state string
		require.NoError(f.t, c.db.QueryRowContext(opCtx, `SELECT state FROM _sync_outbox_bundle`).Scan(&state))
		require.Equal(f.t, "none", state)
		pending, err := c.c.PendingConflict(opCtx)
		require.NoError(f.t, err)
		require.Nil(f.t, pending)
		var sentinel string
		require.NoError(f.t, c.db.QueryRowContext(opCtx, `SELECT value FROM sentinel WHERE id='local'`).Scan(&sentinel))
		require.Equal(f.t, "preserve", sentinel)
		rows, err := c.db.QueryContext(opCtx, `PRAGMA foreign_key_check`)
		require.NoError(f.t, err)
		require.False(f.t, rows.Next())
		require.NoError(f.t, rows.Close())
		beforeIdle := mixedSQLRows(f.t, opCtx, c.db, "SELECT * FROM people ORDER BY id")
		_, err = c.c.Sync(opCtx)
		require.NoError(f.t, err)
		after, err := c.c.LastBundleSeqSeen(opCtx)
		require.NoError(f.t, err)
		require.Equal(f.t, target, after)
		require.Equal(f.t, beforeIdle, mixedSQLRows(f.t, opCtx, c.db, "SELECT * FROM people ORDER BY id"))
		cancel()
	}
	require.Equal(f.t, target, f.seq())
}
func (f *mixedFixture) pending(c *mixedClient) oversqlite.PendingConflict {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	checkpoint := mixedCheckpoint(f.t, opCtx, c.db)
	_, err := c.c.PushPending(opCtx)
	var conflict *oversqlite.ConflictNeedsResolutionError
	require.ErrorAs(f.t, err, &conflict)
	p, err := c.c.PendingConflict(opCtx)
	require.NoError(f.t, err)
	require.NotNil(f.t, p)
	require.Equal(f.t, requireCurrentSourceIDE2E(f.t, c.c, opCtx), p.SourceID)
	require.NotEmpty(f.t, p.RequestHash)
	require.Positive(f.t, p.SourceBundleID)
	frozen := mixedFrozen(f.t, opCtx, c.db)
	require.ErrorAs(f.t, c.c.ResolveConflict(opCtx, *p, oversqlite.DeferConflict{}), &conflict)
	require.Equal(f.t, frozen, mixedFrozen(f.t, opCtx, c.db))
	again, err := c.c.PendingConflict(opCtx)
	require.NoError(f.t, err)
	require.Equal(f.t, p, again)
	require.Equal(f.t, checkpoint, mixedCheckpoint(f.t, opCtx, c.db), "pending conflict must not skip the unseen winning bundle")
	return *p
}
func mixedDecision(c *mixedClient, decision, name string) oversqlite.MergeResult {
	switch decision {
	case "accept":
		return oversqlite.AcceptServer{}
	case "local":
		return oversqlite.KeepLocal{}
	}
	payload := map[string]any{"id": mixedP, "name": name}
	if c.g == 2 {
		payload["note"] = "secret-2"
		payload["project_id"] = mixedQ
	}
	raw, _ := json.Marshal(payload)
	return oversqlite.KeepMerged{MergedPayload: raw}
}

func TestMixedSchema_C01_BidirectionalCRUD(t *testing.T) {
	f := newMixedFixture(t, false)
	for index, first := range []*mixedClient{f.a, f.b} {
		id := mixedP
		if index == 1 {
			id = mixedOther
		}
		second := f.b
		if first == f.b {
			second = f.a
		}
		f.exec(first, `INSERT INTO people(id,name) VALUES(?,'insert')`, id)
		f.push(first)
		f.pull(second)
		f.person(id, "insert", "server-default", nil, true)
		f.exec(second, `UPDATE people SET name='update' WHERE id=?`, id)
		f.push(second)
		f.pull(first)
		f.person(id, "update", "server-default", nil, true)
		f.exec(first, `DELETE FROM people WHERE id=?`, id)
		f.push(first)
		f.pull(second)
		f.person(id, "", nil, nil, false)
	}
	f.settle()
}
func TestMixedSchema_C02_HiddenFieldPreservation(t *testing.T) {
	f := newMixedFixture(t, true)
	f.exec(f.b, `UPDATE people SET note='secret-2'`)
	f.push(f.b)
	f.pull(f.a)
	f.exec(f.a, `UPDATE people SET name='from-A'`)
	f.push(f.a)
	f.settle()
	f.person(mixedP, "from-A", "secret-2", mixedQ, true)
}
func TestMixedSchema_C03_DefaultsAndNull(t *testing.T) {
	f := newMixedFixture(t, true)
	for _, note := range []any{nil, "secret-3"} {
		f.exec(f.b, `UPDATE people SET note=?`, note)
		f.push(f.b)
		f.pull(f.a)
		f.exec(f.a, `UPDATE people SET name='from-A'`)
		f.push(f.a)
		f.pull(f.b)
		f.person(mixedP, "from-A", note, mixedQ, true)
	}
	f.exec(f.a, `INSERT INTO people VALUES(?,'old')`, mixedOther)
	f.push(f.a)
	f.pull(f.b)
	f.person(mixedOther, "old", "server-default", nil, true)
	f.exec(f.b, `INSERT INTO people(id,name,note) VALUES(?,'new',NULL)`, mixedQ)
	f.push(f.b)
	f.settle()
	f.person(mixedQ, "new", nil, nil, true)
}
func TestMixedSchema_C04_NullableRelationships(t *testing.T) {
	f := newMixedFixture(t, true)
	for _, q := range []any{mixedQ, nil, mixedQ} {
		f.exec(f.b, `UPDATE people SET project_id=?`, q)
		f.push(f.b)
		f.pull(f.a)
		f.exec(f.a, `UPDATE people SET name='from-A'`)
		f.push(f.a)
		f.pull(f.b)
		f.person(mixedP, "from-A", "secret-1", q, true)
	}
	for _, table := range []string{"projects", "notes"} {
		column := "title"
		if table == "notes" {
			column = "body"
		}
		f.exec(f.b, `INSERT INTO `+table+` VALUES(?,'created')`, mixedOther)
		f.push(f.b)
		f.pull(f.a)
		f.exec(f.b, `UPDATE `+table+` SET `+column+`='updated' WHERE id=?`, mixedOther)
		f.push(f.b)
		f.pull(f.a)
		var v string
		require.NoError(t, f.b.db.QueryRowContext(f.ctx, `SELECT `+column+` FROM `+table+` WHERE id=?`, mixedOther).Scan(&v))
		require.Equal(t, "updated", v)
		require.NoError(t, f.server.Pool.QueryRow(f.ctx, `SELECT `+column+` FROM mixed.`+table+` WHERE id=$1`, mixedOther).Scan(&v))
		require.Equal(t, "updated", v)
		f.exec(f.b, `DELETE FROM `+table+` WHERE id=?`, mixedOther)
		f.push(f.b)
		f.pull(f.a)
		var n int
		require.NoError(t, f.b.db.QueryRowContext(f.ctx, `SELECT count(*) FROM `+table+` WHERE id=?`, mixedOther).Scan(&n))
		require.Zero(t, n)
		require.NoError(t, f.server.Pool.QueryRow(f.ctx, `SELECT count(*) FROM mixed.`+table+` WHERE id=$1`, mixedOther).Scan(&n))
		require.Zero(t, n)
	}
	f.settle()
}
func TestMixedSchema_C05_HiddenCheckpoint(t *testing.T) {
	f := newMixedFixture(t, true)
	before, err := f.a.c.LastBundleSeqSeen(f.ctx)
	require.NoError(t, err)
	f.exec(f.b, `INSERT INTO notes VALUES(?,'hidden')`, mixedOther)
	f.expected["notes"][mixedOther] = []any{mixedOther, "hidden"}
	f.push(f.b)
	f.pull(f.a)
	after, err := f.a.c.LastBundleSeqSeen(f.ctx)
	require.NoError(t, err)
	require.Greater(t, after, before)
	source := requireCurrentSourceIDE2E(t, f.a.c, f.ctx)
	require.NoError(t, f.a.c.Close())
	require.NoError(t, f.a.db.Close())
	token, err := f.server.GenerateToken(mixedScope, time.Hour)
	require.NoError(t, err)
	f.a = mixedOpen(t, f.ctx, f.a.path, f.server.URL(), token, 1, false)
	require.Equal(t, source, requireCurrentSourceIDE2E(t, f.a.c, f.ctx))
	reopened, err := f.a.c.LastBundleSeqSeen(f.ctx)
	require.NoError(t, err)
	require.Equal(t, after, reopened)
	report := mustPullToStableE2E(t, f.a.c, f.ctx)
	require.Equal(t, oversqlite.RemoteSyncOutcomeAlreadyAtTarget, report.Outcome)
	var v1, v2 int64
	require.NoError(t, f.a.db.QueryRowContext(f.ctx, `SELECT row_version FROM _sync_row_state WHERE table_name='people'`).Scan(&v1))
	f.exec(f.b, `UPDATE people SET note='secret-2'`)
	f.push(f.b)
	f.settle()
	require.NoError(t, f.a.db.QueryRowContext(f.ctx, `SELECT row_version FROM _sync_row_state WHERE table_name='people'`).Scan(&v2))
	require.Greater(t, v2, v1)
	f.person(mixedP, "base", "secret-2", mixedQ, true)
}
func TestMixedSchema_C06_OfflineConflicts(t *testing.T) {
	for _, order := range []string{"A_first", "B_first"} {
		for _, decision := range []string{"accept", "local", "merged"} {
			t.Run(order+"_"+decision, func(t *testing.T) {
				f := newMixedFixture(t, true)
				f.exec(f.a, `UPDATE people SET name='A-edit'`)
				f.exec(f.b, `UPDATE people SET name='B-edit',note='secret-2'`)
				winner, loser := f.a, f.b
				name, note := "A-edit", "secret-1"
				if order == "B_first" {
					winner, loser = f.b, f.a
					name, note = "B-edit", "secret-2"
				}
				f.push(winner)
				p := f.pending(loser)
				require.NoError(t, loser.c.ResolveConflict(f.ctx, p, mixedDecision(loser, decision, "M")))
				f.push(loser)
				if decision != "accept" {
					name = "B-edit"
					if loser == f.a {
						name = "A-edit"
					}
					note = "secret-2"
				}
				if decision == "merged" {
					name = "M"
				}
				f.settle()
				f.person(mixedP, name, note, mixedQ, true)
			})
		}
	}
}
func TestMixedSchema_C07_DisjointConflicts(t *testing.T) {
	for _, order := range []string{"A_first", "B_first"} {
		t.Run(order, func(t *testing.T) {
			f := newMixedFixture(t, true)
			f.exec(f.a, `UPDATE people SET name='A-edit'`)
			f.exec(f.b, `UPDATE people SET note='secret-2'`)
			winner, loser := f.a, f.b
			if order == "B_first" {
				winner, loser = f.b, f.a
			}
			f.push(winner)
			p := f.pending(loser)
			require.NoError(t, loser.c.ResolveConflict(f.ctx, p, mixedDecision(loser, "merged", "A-edit")))
			f.push(loser)
			f.settle()
			f.person(mixedP, "A-edit", "secret-2", mixedQ, true)
		})
	}
}
func TestMixedSchema_C09_DeleteConflicts(t *testing.T) {
	for _, role := range []string{"A_updates", "B_updates"} {
		for _, order := range []string{"delete_first", "update_first"} {
			for _, decision := range []string{"accept", "local", "merged"} {
				t.Run(role+"_"+order+"_"+decision, func(t *testing.T) {
					f := newMixedFixture(t, true)
					up, del := f.a, f.b
					if role == "B_updates" {
						up, del = f.b, f.a
					}
					f.exec(up, `UPDATE people SET name='updated'`)
					f.exec(del, `DELETE FROM people`)
					winner, loser := del, up
					if order == "update_first" {
						winner, loser = up, del
					}
					f.push(winner)
					p := f.pending(loser)
					opCtx, cancel := mixedOperationContext(f.ctx)
					frozenBeforeDecision := mixedFrozen(t, opCtx, loser.db)
					checkpointBeforeDecision := mixedCheckpoint(t, opCtx, loser.db)
					err := loser.c.ResolveConflict(opCtx, p, mixedDecision(loser, decision, "M"))
					invalid := decision == "merged" || (order == "delete_first" && decision == "local")
					if invalid {
						require.Error(t, err)
						require.Equal(t, frozenBeforeDecision, mixedFrozen(t, opCtx, loser.db), "invalid decision changed the frozen request")
						require.Equal(t, checkpointBeforeDecision, mixedCheckpoint(t, opCtx, loser.db), "invalid decision advanced the checkpoint")
						retained, e := loser.c.PendingConflict(opCtx)
						require.NoError(t, e)
						require.Equal(t, p, *retained)
						require.NoError(t, loser.c.ResolveConflict(opCtx, *retained, oversqlite.AcceptServer{}))
					} else {
						require.NoError(t, err)
					}
					cancel()
					f.push(loser)
					f.settle()
					present := order == "update_first" && (decision != "local")
					f.person(mixedP, "updated", "secret-1", mixedQ, present)
				})
			}
		}
	}
}

// The complete business oracle includes absence of unexpected rows and exact
// hidden-table values, independently queried from all three physical databases.
func (f *mixedFixture) exactDatabase() {
	f.t.Helper()
	opCtx, cancel := mixedOperationContext(f.ctx)
	defer cancel()
	for _, table := range []string{"people", "projects", "notes"} {
		keys := []string{}
		for key := range f.expected[table] {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		expected := [][]any{}
		for _, key := range keys {
			expected = append(expected, f.expected[table][key])
		}
		columns := "id::text,title"
		if table == "people" {
			columns = "id::text,name,note,project_id::text"
		}
		if table == "notes" {
			columns = "id::text,body"
		}
		rows, err := f.server.Pool.Query(opCtx, "SELECT "+columns+" FROM mixed."+table+" ORDER BY id")
		require.NoError(f.t, err)
		actual := [][]any{}
		for rows.Next() {
			v, e := rows.Values()
			require.NoError(f.t, e)
			actual = append(actual, v)
		}
		require.NoError(f.t, rows.Err())
		rows.Close()
		require.Equal(f.t, expected, actual, "PostgreSQL "+table)
		for _, c := range []*mixedClient{f.a, f.b} {
			if c.g == 1 && table != "people" {
				continue
			}
			cols := strings.ReplaceAll(columns, "::text", "")
			want := expected
			if c.g == 1 {
				cols = "id,name"
				want = [][]any{}
				for _, row := range expected {
					want = append(want, row[:2])
				}
			}
			require.Equal(f.t, want, mixedSQLRows(f.t, opCtx, c.db, "SELECT "+cols+" FROM "+table+" ORDER BY id"), fmt.Sprintf("G%d %s", c.g, table))
		}
	}
}
func mixedSQLRows(t *testing.T, ctx context.Context, db *sql.DB, query string) [][]any {
	t.Helper()
	opCtx, cancel := mixedOperationContext(ctx)
	defer cancel()
	rows, err := db.QueryContext(opCtx, query)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	result := [][]any{}
	for rows.Next() {
		values := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		result = append(result, values)
	}
	require.NoError(t, rows.Err())
	return result
}

func (f *mixedFixture) diagnostics() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, c := range []*mixedClient{f.a, f.b} {
		if c.c == nil {
			continue
		}
		var state string
		var dirty, stage int
		e1 := c.db.QueryRowContext(ctx, `SELECT state FROM _sync_outbox_bundle`).Scan(&state)
		e2 := c.db.QueryRowContext(ctx, `SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty)
		e3 := c.db.QueryRowContext(ctx, `SELECT count(*) FROM _sync_snapshot_stage`).Scan(&stage)
		f.t.Logf("redacted diagnostic generation=%d outbox=%s dirty=%d snapshot_stage=%d readable=%t", c.g, state, dirty, stage, e1 == nil && e2 == nil && e3 == nil)
	}
}
