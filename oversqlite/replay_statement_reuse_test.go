package oversqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test-only numeric suballocation model: the existing txStmtCache owns every
// statement. This model neither owns a second token nor stores statements.
type bindingReservationProof struct {
	client   *Client
	tx       *immediateTx
	token    string
	base     workspaceCharge
	capacity workspaceCharge
}

func TestReplayStatementReuse_OneShotBindingPrerequisite(t *testing.T) {
	ctx := context.Background()
	const size = 1 << 20
	for _, kind := range []string{"TEXT", "BLOB"} {
		t.Run(kind, func(t *testing.T) {
			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec(`CREATE TABLE parent(id TEXT PRIMARY KEY,payload BLOB);
			CREATE TABLE child(id TEXT PRIMARY KEY,parent_id TEXT REFERENCES parent(id),payload BLOB)`)
			require.NoError(t, err)
			_, err = db.Exec(`CREATE TRIGGER app_parent_literal AFTER INSERT ON parent BEGIN SELECT length('` + strings.Repeat("x", size) + `'); END`)
			require.NoError(t, err)
			tx, err := beginImmediateTx(ctx, db)
			require.NoError(t, err)
			defer tx.Rollback()
			cache := newTxStmtCache(tx)
			defer cache.Close()
			const read = `SELECT length(?) /* retained read beside temporary write */`
			const write = `INSERT INTO parent(id,payload) VALUES(?,?)`
			stmt, err := cache.stmt(ctx, read)
			require.NoError(t, err)
			var n int
			require.NoError(t, observedStatementScan(ctx, stmt, []any{strings.Repeat("r", size)}, &n))
			retained := bindingProofNativeMemory()[read]
			var payload any = strings.Repeat("p", size)
			if kind == "BLOB" {
				payload = []byte(payload.(string))
			}
			var during int64
			_, err = observedOneShotExec(ctx, cache, write, func() {
				require.Len(t, cache.stmts, 2)
				during = bindingProofNativeMemory()[write]
				if retained != 0 {
					require.Greater(t, during, int64(3*size))
					require.Equal(t, retained, bindingProofNativeMemory()[read])
				}
			}, "P", payload)
			require.NoError(t, err)
			require.Len(t, cache.stmts, 1)
			require.Zero(t, bindingProofNativeMemory()[write])
			if retained != 0 {
				require.Equal(t, retained, bindingProofNativeMemory()[read])
			}
			require.NoError(t, cache.Close())
			require.Zero(t, bindingProofNativeMemory()[read])
			t.Logf("ONE_SHOT %s: retained read=%d native bytes; temporary write after Exec before Close=%d; after Close write=0; temporary program exists before finalization and is not admission proof", kind, retained, during)
		})
	}
}

// Preparation owns schema-dependent native programs even before any parameter
// is bound. Table/FK/handle counts alone do not bound that retained metadata.
func TestReplayStatementReuse_MetadataPrerequisite(t *testing.T) {
	ctx := context.Background()
	const query = `INSERT INTO parent(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`
	for _, kind := range []string{"TriggerProgram", "IndexProgram"} {
		t.Run(kind, func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{
				{TableName: "parent", SyncKeyColumnName: "id"},
				{TableName: "child", SyncKeyColumnName: "id"},
			}, `CREATE TABLE parent(id TEXT PRIMARY KEY NOT NULL,payload TEXT NOT NULL);
			CREATE TABLE child(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT REFERENCES parent(id),payload TEXT NOT NULL)`)
			beforeShape, err := managedPhysicalShape(ctx, db, c.config.Tables)
			require.NoError(t, err)
			preparedMemory := func() int64 {
				tx, err := beginImmediateTx(ctx, db)
				require.NoError(t, err)
				cache := newTxStmtCache(tx)
				t.Cleanup(func() { require.NoError(t, cache.Close()); require.NoError(t, tx.Rollback()) })
				_, err = cache.stmt(ctx, query)
				require.NoError(t, err)
				require.Len(t, cache.stmts, 1)
				memory := bindingProofNativeMemory()[query]
				require.NoError(t, cache.Close())
				require.Zero(t, bindingProofNativeMemory()[query])
				require.NoError(t, tx.Rollback())
				return memory
			}
			before := preparedMemory()
			if kind == "TriggerProgram" {
				_, err = db.Exec(`CREATE TRIGGER app_parent_literal AFTER INSERT ON parent BEGIN SELECT length('` + strings.Repeat("x", 1<<20) + `'); END`)
				require.NoError(t, err)
			} else {
				for i := range 32 {
					_, err = db.Exec(fmt.Sprintf(`CREATE INDEX app_parent_payload_%d ON parent(payload)`, i))
					require.NoError(t, err)
				}
			}
			afterShape, err := managedPhysicalShape(ctx, db, c.config.Tables)
			require.NoError(t, err)
			require.Equal(t, beforeShape, afterShape)
			// Ordinary retained admission allows application SQL unrelated to the
			// runtime's retained tables. This is a supported metadata dimension.
			admission := cloneCoordinatorClientConfig(c.config)
			// The direct engine fixture keeps GenerationID=0 to select its
			// construction path; its installed identity uses generationProof.ID.
			admission.GenerationID = admission.generationProof.ID
			require.NoError(t, readRetainedContract(ctx, db, admission, afterShape))
			after := preparedMemory()
			if before != 0 {
				require.Greater(t, after, before)
				if kind == "TriggerProgram" {
					require.GreaterOrEqual(t, after-before, int64(1<<20))
				}
			}
			t.Logf("METADATA %s: two tables, one FK, identical SQL (%d bytes), one handle, zero binds; native prepared bytes before=%d after=%d; managed shape unchanged and retained admission succeeds", kind, len(query), before, after)
		})
	}
}

var bindingProofNativeMemory = func() map[string]int64 { return nil }

func (p *bindingReservationProof) grow(ctx context.Context, capacity workspaceCharge) error {
	capacity.entries = max(capacity.entries, p.capacity.entries)
	capacity.bytes = max(capacity.bytes, p.capacity.bytes)
	requested, err := p.client.addWorkspaceCharge(applyUnit("binding-proof", 0, ""), p.base, capacity)
	if err != nil {
		return err
	}
	if err = p.client.resizeWorkspaceInTx(ctx, p.tx, p.token, requested); err != nil {
		return err
	}
	p.capacity = capacity
	return nil
}

// A production release must also preserve close/rollback causes and prepaid
// cleanup work. This model demonstrates the unchanged five-field record only.
func (p *bindingReservationProof) release(ctx context.Context, cache *txStmtCache) error {
	if cache.stmts != nil {
		return errors.New("binding copies must close before releasing capacity")
	}
	records, err := p.client.loadWorkspaceReservations(ctx, p.tx)
	if err != nil {
		return err
	}
	base, err := p.client.addWorkspaceCharge(applyUnit("binding-proof", 0, ""), p.base, workspaceCharge{1, 4096})
	if err != nil {
		return err
	}
	for i := range records {
		if records[i].Token == p.token {
			records[i].Entries, records[i].Bytes = base.entries, base.bytes
			p.capacity = workspaceCharge{}
			return storeWorkspaceReservations(ctx, p.tx, records)
		}
	}
	return errors.New("existing operation token is missing")
}

func bindingProofRecord(t *testing.T, c *Client, tx *immediateTx, token string) workspaceReservation {
	t.Helper()
	records, err := c.loadWorkspaceReservations(context.Background(), tx)
	require.NoError(t, err)
	for _, record := range records {
		if record.Token == token {
			return record
		}
	}
	t.Fatal("missing reservation record")
	return workspaceReservation{}
}

func bindingProofOwner(t *testing.T, count ...int) (*bindingReservationProof, *txStmtCache, string) {
	t.Helper()
	ctx := context.Background()
	c, db := newA1SurvivorClient(t)
	other, err := newAdmittedFixtureClient(db, c.BaseURL, tokenProviderForTests, c.config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, other.Close()) })
	base := workspaceCharge{4, 512}
	token, err := c.reserveWorkspace(ctx, applyUnit("binding-owner", 0, ""), base)
	require.NoError(t, err)
	second, err := other.reserveWorkspace(ctx, applyUnit("other-owner", 0, ""), workspaceCharge{3, 100})
	require.NoError(t, err)
	var extra []string
	if len(count) == 1 {
		for i := 2; i < count[0]; i++ {
			token, err := c.reserveWorkspace(ctx, applyUnit("ledger-work", int64(i), ""), workspaceCharge{})
			require.NoError(t, err)
			extra = append(extra, token)
		}
	}
	t.Cleanup(func() {
		require.NoError(t, c.releaseWorkspace(ctx, token))
		require.NoError(t, other.releaseWorkspace(ctx, second))
		for _, token := range extra {
			require.NoError(t, c.releaseWorkspace(ctx, token))
		}
	})
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	cache := newTxStmtCache(tx)
	t.Cleanup(func() { require.NoError(t, cache.Close()); require.NoError(t, tx.Rollback()) })
	return &bindingReservationProof{client: c, tx: tx, token: token, base: base}, cache, second
}

func TestReplayBindingReservation_ExistingOwnerProof(t *testing.T) {
	ctx := context.Background()
	const size = 1 << 20
	// Two variable-copy slots cover the retained native copy and replacement
	// conversion scratch. Canonical escaped bytes plus a fixed descriptor bound
	// are conservative; allocator MEMUSED is an independent observation.
	capacity := workspaceCharge{3, 4096 + 12*size}
	t.Run("TokenAndSharedOwner", func(t *testing.T) {
		p, cache, other := bindingProofOwner(t)
		before := bindingProofRecord(t, p.client, p.tx, p.token)
		protected := bindingProofRecord(t, p.client, p.tx, other)
		require.NoError(t, p.grow(ctx, capacity))
		admitted := bindingProofRecord(t, p.client, p.tx, p.token)
		require.Equal(t, before.Token, admitted.Token)
		require.Equal(t, before.Unit, admitted.Unit)
		require.Equal(t, before.Context, admitted.Context)
		require.Equal(t, before.Entries+capacity.entries, admitted.Entries)
		require.Equal(t, before.Bytes+capacity.bytes, admitted.Bytes)
		require.Equal(t, protected, bindingProofRecord(t, p.client, p.tx, other))
		const query = `SELECT length(?) /* existing-token text */`
		stmt, err := cache.stmt(ctx, query)
		require.NoError(t, err)
		var n int
		require.NoError(t, observedStatementScan(ctx, stmt, []any{strings.Repeat("x", size)}, &n))
		require.Equal(t, size, n)
		if native := bindingProofNativeMemory(); native != nil {
			require.GreaterOrEqual(t, native[query], int64(size))
			t.Logf("native retained allocation under existing token: %v", native)
		}
		require.ErrorContains(t, p.release(ctx, cache), "must close")
		require.NoError(t, cache.Close())
		require.NoError(t, p.release(ctx, cache))
		require.Equal(t, before, bindingProofRecord(t, p.client, p.tx, p.token))
		require.Equal(t, protected, bindingProofRecord(t, p.client, p.tx, other))
		t.Log("same five fields/token/context, independently visible aggregate charge, other shared client's reservation preserved")
	})
	t.Run("ReplacementAndRelease", func(t *testing.T) {
		p, cache, _ := bindingProofOwner(t)
		base := bindingProofRecord(t, p.client, p.tx, p.token)
		require.NoError(t, p.grow(ctx, capacity))
		const query = `SELECT length(?) /* existing-token blob replacement */`
		stmt, err := cache.stmt(ctx, query)
		require.NoError(t, err)
		var n int
		require.NoError(t, observedStatementScan(ctx, stmt, []any{[]byte(strings.Repeat("b", size))}, &n))
		wide := bindingProofNativeMemory()
		require.NoError(t, observedStatementScan(ctx, stmt, []any{int64(7)}, &n))
		if native := bindingProofNativeMemory(); native != nil {
			require.Less(t, native[query], wide[query]-size/2)
			t.Logf("wide binding released at scalar replacement: before=%v after=%v", wide, native)
		}
		// Monotonic capacity remains an explicit reservation, including unused
		// replacement capacity. This is not an assertion that the old copy lives.
		require.Equal(t, base.Bytes+capacity.bytes, bindingProofRecord(t, p.client, p.tx, p.token).Bytes)
		require.NoError(t, cache.Close())
		if native := bindingProofNativeMemory(); native != nil {
			require.Zero(t, native[query])
		}
		require.NoError(t, p.release(ctx, cache))
		require.Equal(t, base, bindingProofRecord(t, p.client, p.tx, p.token))
		t.Log("all handles closed before exact base restored; conservative spare capacity requires explicit policy, not a false live-copy count")
	})
	t.Run("RefusalBeforeCopy", func(t *testing.T) {
		p, cache, _ := bindingProofOwner(t)
		before := bindingProofRecord(t, p.client, p.tx, p.token)
		used, err := p.client.workspaceUsage(ctx, p.tx, before.Unit, nil, nil)
		require.NoError(t, err)
		p.client.config.MaxSyncWorkspaceEntries = used.entries
		p.client.config.MaxSyncWorkspaceBytes = used.bytes
		err = p.grow(ctx, capacity)
		var refusal *SyncWorkspaceLimitError
		require.ErrorAs(t, err, &refusal)
		require.Equal(t, "entries", refusal.Dimension)
		p.client.config.MaxSyncWorkspaceEntries = used.entries + capacity.entries
		err = p.grow(ctx, capacity)
		require.ErrorAs(t, err, &refusal)
		require.Equal(t, "bytes", refusal.Dimension)
		require.Equal(t, before, bindingProofRecord(t, p.client, p.tx, p.token))
		require.Empty(t, cache.stmts, "no prepare, native bind or string construction attempted after refusal")
		require.Equal(t, workspaceCharge{}, p.capacity)
	})
	t.Run("LedgerWorkBound", func(t *testing.T) {
		p, cache, _ := bindingProofOwner(t, 65)
		// Two clients under the same supported opening share finite reservations;
		// no contract caps the ledger at the original 16/32-step allowance.
		records, err := p.client.loadWorkspaceReservations(ctx, p.tx)
		require.NoError(t, err)
		require.Len(t, records, 65)
		before := p.client.syncWorkUsed
		require.NoError(t, p.client.reserveSyncWork(ctx, records[0].Unit, 32))
		replayDiagnosticStart()
		require.NoError(t, p.grow(ctx, capacity))
		observation := replayDiagnosticStop()
		require.Equal(t, before+32, p.client.syncWorkUsed, "current resizer leaves new ledger visits unmetered")
		data, err := json.Marshal(observation)
		require.NoError(t, err)
		var native map[string]struct{ Steps int64 }
		require.NoError(t, json.Unmarshal(data, &native))
		if native != nil {
			var ledgerSteps int64
			for query, counts := range native {
				if strings.Contains(query, "FROM _sync_operation_state o,json_each(o.workspace_context) j WHERE singleton_key=1") {
					ledgerSteps += counts.Steps
				}
			}
			require.GreaterOrEqual(t, ledgerSteps, int64(66))
			t.Logf("new resize performs %d native ledger row steps for 65 live records; automatic SQLite/internal JSON steps are excluded", ledgerSteps)
		}
		// 65 is only the additional ledger-visit lower bound, excluding parsing,
		// marshaling, scalar checks and the stage write. It exceeds the entire
		// original 32-unit logical-step allowance before any original work.
		require.NoError(t, p.client.reserveSyncWork(ctx, records[0].Unit, int64(len(records))))
		require.Equal(t, before+32+65, p.client.syncWorkUsed)
		require.NoError(t, cache.Close())
		require.NoError(t, p.release(ctx, cache))
		t.Logf("COUNTEREXAMPLE to unchanged total-work equality: original component=32, new bookkeeping minimum=65, correct metered total delta=%d; existing owner and limits retained", p.client.syncWorkUsed-before)
	})
}

type statementTestLease struct {
	*workspaceBufferScope
	release func() error
}

func configuredReadCache(t *testing.T, c *Client, db *sql.DB) (context.Context, *immediateTx, sqliteTransaction, *txStmtCache, *statementTestLease) {
	t.Helper()
	ctx := context.Background()
	unit := applyUnit("read-cache-test", 0, "")
	token, err := c.reserveWorkspace(ctx, unit, workspaceCharge{64, 256 * 1024})
	require.NoError(t, err)
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	cache := newTxStmtCache(tx)
	scope := &workspaceBufferScope{client: c, token: token, unit: unit, base: workspaceCharge{64, 256 * 1024}}
	ctx = context.WithValue(ctx, workspaceBufferKey{}, scope)
	ctx, execution, err := c.configureStatementCache(ctx, tx, cache)
	require.NoError(t, err)
	released := false
	lease := &statementTestLease{workspaceBufferScope: scope}
	lease.release = func() error {
		if released {
			return nil
		}
		err := c.releaseWorkspace(context.Background(), token)
		if err == nil {
			released = true
		}
		return err
	}
	t.Cleanup(func() {
		if !tx.done {
			require.NoError(t, cache.closeAndRelease(context.Background()))
			require.NoError(t, tx.Rollback())
		} else {
			require.NoError(t, cache.Close())
		}
		require.NoError(t, lease.release())
	})
	return ctx, tx, execution, cache, lease
}

type readAllocationValuer struct{ calls *int }

func (v readAllocationValuer) Value() (driver.Value, error) {
	*v.calls++
	return strings.Repeat("v", 1<<20), nil
}

func TestReplayStatementReuse_TypedReads(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx, _, _, cache, _ := configuredReadCache(t, c, db)
	values := []any{nil, "", []byte{}, []byte{0, 255}, "text", "123456789.0001", int64(-9), 1.25, true, sql.NullString{String: "nullable", Valid: true}, sql.NullString{}}
	for _, v := range values {
		var got any
		require.NoError(t, cache.ScanContext(ctx, `SELECT ?`, []any{v}, &got))
		want := v
		if n, ok := v.(sql.NullString); ok {
			if n.Valid {
				want = n.String
			} else {
				want = nil
			}
		}
		if b, ok := v.(bool); ok {
			if b {
				want = int64(1)
			} else {
				want = int64(0)
			}
		}
		require.Equal(t, want, got)
	}
	require.Len(t, cache.stmts, 1)
	var integer int
	require.ErrorIs(t, cache.ScanContext(ctx, `SELECT ? WHERE 0`, []any{1}, &integer), sql.ErrNoRows)
	require.ErrorContains(t, cache.ScanContext(ctx, `SELECT ?`, []any{"not integer"}, &integer), "converting driver.Value")
	var raw sql.RawBytes
	require.ErrorContains(t, cache.ScanContext(ctx, `SELECT ?`, []any{[]byte{1}}, &raw), "RawBytes")
	calls := 0
	require.ErrorContains(t, cache.ScanContext(ctx, `SELECT ?`, []any{readAllocationValuer{&calls}}, &integer), "unsupported")
	require.Zero(t, calls)
	require.Error(t, cache.ScanContext(ctx, `SELECT FROM`, nil, &integer))
}
func TestReplayStatementReuse_TriggerVisibility(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx, _, q, cache, _ := configuredReadCache(t, c, db)
	const query = `SELECT name FROM nodes WHERE id=?`
	var name string
	require.NoError(t, scanTransactionRow(ctx, q, query, []any{"P"}, &name))
	require.Equal(t, "parent", name)
	_, err := q.ExecContext(ctx, `CREATE TEMP TRIGGER read_visibility AFTER UPDATE ON nodes WHEN NEW.id='P' BEGIN UPDATE nodes SET name='trigger value' WHERE id='Q'; END`)
	require.NoError(t, err)
	require.NoError(t, scanTransactionRow(ctx, q, query, []any{"Q"}, &name))
	require.Equal(t, "other", name)
	_, err = q.ExecContext(ctx, `UPDATE nodes SET name='changed' WHERE id='P'`)
	require.NoError(t, err)
	require.NoError(t, scanTransactionRow(ctx, q, query, []any{"Q"}, &name))
	require.Equal(t, "trigger value", name)
	require.Len(t, cache.stmts, 1)
}
func TestReplayStatementReuse_PlanAndAccounting(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx, tx, q, cache, scope := configuredReadCache(t, c, db)
	before := bindingProofRecord(t, c, tx, scope.token)
	plan := &applyPlan{unit: scope.unit, guard: "guard", phase: "ready", root: 7, visit: 11, table: 2, fk: 1, cursor: sql.NullString{String: "cursor", Valid: true}, entries: 3, bytes: 1234, edgeEntries: 4, edgeBytes: 112, kind: "settlement", preserve: [3]string{"main", "nodes", "key"}, atomicWork: 1024, preservationComplete: true}
	require.NoError(t, saveApplyPlan(ctx, q, plan))
	loaded, err := loadApplyPlan(ctx, q)
	require.NoError(t, err)
	loaded.workspaceContext = nil
	loaded.workspaceQuery = nil
	require.Equal(t, plan, loaded)
	require.NoError(t, c.resizeWorkspaceInTx(ctx, q, scope.token, workspaceCharge{70, 300000}))
	grown := bindingProofRecord(t, c, tx, scope.token)
	require.Greater(t, grown.Bytes, before.Bytes)
	pool := cache.pool.capacity
	require.NoError(t, cache.closeAndRelease(ctx))
	released := bindingProofRecord(t, c, tx, scope.token)
	require.Equal(t, grown.Entries-pool.entries, released.Entries)
	require.Equal(t, grown.Bytes-pool.bytes, released.Bytes)
	require.Positive(t, scope.statementWork)
}
func TestReplayStatementReuse_ShapeAndOwnership(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "parents", SyncKeyColumnName: "id"}, {TableName: "children", SyncKeyColumnName: "id"}}, `CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,name TEXT);CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,parent_id TEXT REFERENCES parents(id),name TEXT)`)
	ctx, tx, q, cache, scope := configuredReadCache(t, c, db)
	require.GreaterOrEqual(t, cache.pool.handles, int64(64+24+4))
	for _, query := range []string{`SELECT count(*) FROM parents`, `SELECT count(*) FROM children`} {
		var n int
		require.NoError(t, scanTransactionRow(ctx, q, query, nil, &n))
	}
	require.Len(t, cache.stmts, 2)
	before := len(cache.stmts)
	other := &immediateTx{conn: tx.conn, ctx: ctx}
	var n int
	require.NoError(t, scanTransactionRow(ctx, other, `SELECT 42`, nil, &n))
	require.Equal(t, 42, n)
	require.Len(t, cache.stmts, before)
	require.NoError(t, cache.closeAndRelease(ctx))
	require.ErrorContains(t, cache.ScanContext(ctx, `SELECT 1`, nil, &n), "is closed")
	_ = scope
}
func TestReplayStatementReuse_RetainedBindings(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx, tx, _, cache, scope := configuredReadCache(t, c, db)
	before := bindingProofRecord(t, c, tx, scope.token)
	const query = `SELECT length(?) /* owned bounded binding */`
	var n int
	small := strings.Repeat("x", 8192)
	require.NoError(t, cache.ScanContext(ctx, query, []any{small}, &n))
	require.Equal(t, len(small), n)
	retained := bindingProofNativeMemory()[query]
	wide := strings.Repeat("w", 1<<20)
	require.NoError(t, cache.ScanContext(ctx, query, []any{wide}, &n))
	require.Equal(t, len(wide), n)
	require.Len(t, cache.stmts, 1)
	if retained != 0 {
		require.Equal(t, retained, bindingProofNativeMemory()[query])
	}
	require.NoError(t, cache.ScanContext(ctx, query, []any{int64(7)}, &n))
	if retained != 0 {
		require.Less(t, bindingProofNativeMemory()[query], retained)
	}
	require.Equal(t, before, bindingProofRecord(t, c, tx, scope.token))
	require.NoError(t, cache.closeAndRelease(ctx))
	require.Zero(t, bindingProofNativeMemory()[query])
}

func TestReplayStatementReuse_PlanWrites(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx, tx, q, cache, scope := configuredReadCache(t, c, db)
	before := bindingProofRecord(t, c, tx, scope.token)
	plan := &applyPlan{unit: scope.unit, guard: "first", phase: "roots", root: 1, visit: 2, table: 3, fk: 4,
		cursor: sql.NullString{String: "first cursor", Valid: true}, entries: 5, bytes: 6, edgeEntries: 7, edgeBytes: 8,
		kind: "settlement", preserve: [3]string{"main", "nodes", "first key"}, atomicWork: 9, preservationComplete: true}
	replayDiagnosticStart()
	for i := 0; i < 3; i++ {
		require.NoError(t, saveApplyPlan(ctx, q, plan))
		loaded, err := loadApplyPlan(ctx, q)
		require.NoError(t, err)
		loaded.workspaceContext, loaded.workspaceQuery = nil, nil
		require.Equal(t, plan, loaded)
		plan.guard, plan.phase, plan.kind = "next", "ready", "conflict"
		plan.root, plan.visit, plan.table, plan.fk = 11, 12, 13, -1
		plan.entries, plan.bytes, plan.edgeEntries, plan.edgeBytes = 14, 15, 16, 17
		plan.preserve, plan.atomicWork, plan.preservationComplete = [3]string{"other", "nodes", "next key"}, 18, false
		plan.cursor = sql.NullString{String: "", Valid: i == 0}
	}
	require.Len(t, cache.stmts, 3, "two writes and one read, each owned once")
	require.Equal(t, before, bindingProofRecord(t, c, tx, scope.token))
	var nativeQueries []string
	for _, query := range cache.order {
		nativeQueries = append(nativeQueries, query)
	}
	require.NoError(t, cache.closeAndRelease(ctx))
	observation := replayDiagnosticStop()
	if observation != nil {
		data, err := json.Marshal(observation)
		require.NoError(t, err)
		var counts map[string]struct{ Prepares, Binds, Steps, Finalizes int64 }
		require.NoError(t, json.Unmarshal(data, &counts))
		for _, query := range nativeQueries {
			require.EqualValues(t, 1, counts[query].Prepares)
			require.EqualValues(t, 3, counts[query].Binds)
			require.EqualValues(t, 1, counts[query].Finalizes)
		}
	}
	for _, query := range nativeQueries {
		require.Zero(t, bindingProofNativeMemory()[query])
	}
	plan.guard = "raw after close"
	require.NoError(t, saveApplyPlan(context.Background(), tx, plan), "raw callers keep their original path")
	require.ErrorContains(t, saveApplyPlan(ctx, q, plan), "is closed")
}

func TestReplayStatementReuse_PlanWriteBounds(t *testing.T) {
	for _, mode := range []string{"retained", "pool fallback", "handle fallback", "SQL fallback"} {
		t.Run(mode, func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			ctx, tx, q, cache, scope := configuredReadCache(t, c, db)
			before := bindingProofRecord(t, c, tx, scope.token)
			switch mode {
			case "pool fallback":
				cache.pool.reuse = false
			case "handle fallback":
				cache.pool.handles = 0
			case "SQL fallback":
				cache.pool.sqlBytes = 0
			}
			plan := &applyPlan{unit: scope.unit, guard: "guard", phase: "roots", kind: "settlement",
				cursor: sql.NullString{String: strings.Repeat("s", 8192), Valid: true}}
			require.NoError(t, saveApplyPlan(ctx, q, plan))
			var query string
			var retained int64
			if mode == "retained" {
				require.Len(t, cache.stmts, 2)
				query = cache.order[0]
				retained = bindingProofNativeMemory()[query]
			}
			plan.cursor.String = strings.Repeat("w", 1<<20)
			require.NoError(t, saveApplyPlan(ctx, q, plan))
			var saved string
			require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT row_cursor FROM _sync_apply_plan`).Scan(&saved))
			require.Equal(t, plan.cursor.String, saved)
			if mode == "retained" {
				require.Len(t, cache.stmts, 2, "oversized first save is one-shot; second save reuses")
				if retained != 0 {
					require.Equal(t, retained, bindingProofNativeMemory()[query])
				}
			} else {
				require.Empty(t, cache.stmts)
			}
			plan.cursor = sql.NullString{}
			require.NoError(t, saveApplyPlan(ctx, q, plan))
			if retained != 0 {
				require.Less(t, bindingProofNativeMemory()[query], retained)
			}
			require.Equal(t, before, bindingProofRecord(t, c, tx, scope.token))
			calls := 0
			_, err := executeApplyPlanStatement(ctx, q, 7, "unsupported plan argument", readAllocationValuer{&calls})
			require.ErrorContains(t, err, "unsupported")
			require.Zero(t, calls)
			_, err = executeApplyPlanStatement(ctx, q, 23, "too many plan arguments", make([]any, 17)...)
			require.ErrorContains(t, err, "too many")
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			require.ErrorIs(t, saveApplyPlan(canceled, q, plan), context.Canceled)
			require.False(t, cache.closed)
			require.NoError(t, cache.closeAndRelease(ctx))
			require.Zero(t, bindingProofNativeMemory()[query])
		})
	}
}
