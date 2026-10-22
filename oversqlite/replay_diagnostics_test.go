package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Explicit opt-in diagnostics. Normal package runs do not run this workload.
// The optional observation build uses a separately copied, instrumented driver.
var replayDiagnosticStart = func() {}
var replayDiagnosticStop = func() any { return nil }
var replayDiagnosticMemory = func() map[string]int64 { return nil }

func replayDiagnosticEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("OVERSQLITE_REPLAY_DIAGNOSTICS") != "1" {
		t.Skip("set OVERSQLITE_REPLAY_DIAGNOSTICS=1 for investigation support")
	}
}

func replayDiagnosticRecord(t *testing.T, value map[string]any) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	t.Logf("REPLAY_DIAGNOSTIC %s", data)
	if dir := os.Getenv("OVERSQLITE_REPLAY_DIAGNOSTIC_OUTPUT"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".json"), data, 0644))
	}
}

// Same one-row frozen upload and unrelated A/B/L backlog as BacklogBound.
func replayDiagnosticBacklog(t *testing.T, c *Client, db *sql.DB, n int, widths ...int) string {
	t.Helper()
	width := 16
	if len(widths) > 0 {
		width = widths[0]
	}
	c.config.MaxSyncWorkUnitsPerInvocation = 100000000
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	// Freeze a committed certificate before replay, as in BacklogBound. A cut
	// before commit is resolved as cancelled by the v1 fence and cannot measure
	// settlement of the original committed outcome.
	c.HTTP = &http.Client{Transport: a1SettlementServer(t)}
	c.SetBeforePushReplayHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen' AND length(outcome_body)>0`))
	_, err = db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO nodes SELECT printf('Z%05d',i),?,NULL FROM n`, n, strings.Repeat("b", width))
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_row_state SET row_version=1,server_kind='live',
 server_payload=(SELECT payload FROM _sync_dirty_rows d WHERE d.schema_name=_sync_row_state.schema_name AND d.table_name=_sync_row_state.table_name AND d.key_json=_sync_row_state.key_json),
 server_provenance=(SELECT server_provenance FROM _sync_row_state WHERE key_json='{"id":"P"}'),relationships_known=1
 WHERE json_extract(key_json,'$.id') LIKE 'Z%';
 DELETE FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id') LIKE 'Z%';
 UPDATE nodes SET name=? WHERE id LIKE 'Z%'`, strings.Repeat("x", width))
	require.NoError(t, err)
	require.EqualValues(t, n, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows WHERE base_kind='present' AND base_payload IS NOT NULL`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	return a3UnrelatedIntentDigest(t, db)
}

func TestReplayDiagnostics_Public(t *testing.T) {
	replayDiagnosticEnabled(t)
	memoryMode, err := replayMemoryMode(os.Getenv("OVERSQLITE_REPLAY_MEMORY") == "1", os.Getenv("OVERSQLITE_REPLAY_MEMORY_MODE"))
	require.NoError(t, err)
	for _, n := range []int{1, 1000, 10000} {
		for _, width := range []int{16, 1024} {
			if n == 1 && width != 16 {
				continue
			}
			t.Run(fmt.Sprintf("%d/%d", n, width), func(t *testing.T) {
				start := time.Now()
				c, db, path := qualificationDurableClient(t)
				retained := replayDiagnosticBacklog(t, c, db, n, width)
				// Keep the authority that committed C: its retained outcome is also
				// the owner of the final ack obligation.
				var frozen string
				require.NoError(t, db.QueryRow(`SELECT canonical_request_hash FROM _sync_outbox_bundle`).Scan(&frozen))
				setup := time.Since(start)
				var replayStart time.Time
				var scope *workspaceBufferScope
				c.SetBeforePushReplayHook(func(ctx context.Context) error {
					replayStart = time.Now()
					scope, _ = ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
					return nil
				})
				var profile *os.File
				if p := os.Getenv("OVERSQLITE_REPLAY_CPU_PROFILE"); p != "" {
					var err error
					profile, err = os.Create(p)
					require.NoError(t, err)
					require.NoError(t, pprof.StartCPUProfile(profile))
				}
				measurement := prepareReplayMemoryMeasurement(memoryMode)
				replayDiagnosticStart()
				if measurement != nil {
					measurement.start()
					defer measurement.finish()
				}
				callStart := time.Now()
				report, err := c.PushPending(context.Background())
				callEnd := time.Now()
				memory := map[string]int64(nil)
				if measurement != nil {
					measurement.finish()
					memory = measurement.Peak
				}
				observation := replayDiagnosticStop()
				if profile != nil {
					pprof.StopCPUProfile()
					require.NoError(t, profile.Close())
				}
				require.NoError(t, err)
				if measurement != nil {
					measurement.captureRetained(c, db)
				}
				require.False(t, replayStart.IsZero())
				require.Equal(t, PushOutcomeCommitted, report.Outcome)
				require.Equal(t, retained, a3UnrelatedIntentDigest(t, db))
				requireA1Survivors(t, db)
				require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
				require.EqualValues(t, 2, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
				require.Zero(t, countA3(t, db, `SELECT apply_mode FROM _sync_apply_state`))
				require.EqualValues(t, n, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
				require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
				require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
				require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`))
				require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_edges`))
				require.Zero(t, countA3(t, db, `SELECT push_stage_entries+push_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state`))
				expected, err := replayStateFingerprint(db, "nodes")
				require.NoError(t, err)
				require.NoError(t, c.Close())
				require.NoError(t, db.Close())
				reopened, err := sql.Open("sqlite3", path+"?_foreign_keys=on")
				require.NoError(t, err)
				actual, err := replayStateFingerprint(reopened, "nodes")
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				require.NoError(t, reopened.Close())
				replayDiagnosticRecord(t, map[string]any{"rows": n, "width": width, "additional_work": replayStatementWork(scope), "logical_work": c.syncWorkUsed - replayStatementWork(scope), "memory": memory, "memory_measurement": measurement, "measurement_version": 2, "state_fingerprint": actual, "setup_ns": setup.Nanoseconds(), "public_ns": callEnd.Sub(callStart).Nanoseconds(), "replay_tail_ns": callEnd.Sub(replayStart).Nanoseconds(), "case_ns": time.Since(start).Nanoseconds(), "work": c.syncWorkUsed, "stats": fmt.Sprintf("%+v", c.a3Stats), "frozen_hash": frozen, "observation": observation})
			})
		}
	}
}

// Only the diagnostic preservation kernel uses this adapter. It preserves each
// invocation and binding; it does not cache rows, values, plans or accounting.
// Preparation errors fail the experiment immediately, not an SDK error policy.
type replayDiagnosticTx struct {
	sqliteTransaction
	t     *testing.T
	mode  string
	stmts map[string]*sql.Stmt
}

func (tx *replayDiagnosticTx) cached(query string) bool {
	return tx.mode == "all" || (tx.mode == "workspace" && (strings.HasPrefix(query, "SELECT reserved_entries,reserved_bytes") || strings.HasPrefix(query, "SELECT entries,bytes,length(CAST(unit")))
}

func (tx *replayDiagnosticTx) statement(ctx context.Context, query string) *sql.Stmt {
	if stmt := tx.stmts[query]; stmt != nil {
		return stmt
	}
	stmt, err := tx.sqliteTransaction.PrepareContext(ctx, query)
	require.NoError(tx.t, err)
	tx.stmts[query] = stmt
	return stmt
}

func (tx *replayDiagnosticTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx.cached(query) {
		return tx.statement(ctx, query).ExecContext(ctx, args...)
	}
	return tx.sqliteTransaction.ExecContext(ctx, query, args...)
}

func (tx *replayDiagnosticTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if tx.cached(query) {
		return tx.statement(ctx, query).QueryRowContext(ctx, args...)
	}
	return tx.sqliteTransaction.QueryRowContext(ctx, query, args...)
}

func (tx *replayDiagnosticTx) close() {
	for _, stmt := range tx.stmts {
		require.NoError(tx.t, stmt.Close())
	}
}

func TestReplayDiagnostics_Kernel(t *testing.T) {
	replayDiagnosticEnabled(t)
	for _, n := range []int{1, 1000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			retained := replayDiagnosticBacklog(t, c, db, n)
			var expected map[string]any
			modes := []string{"raw", "workspace", "all"}
			if os.Getenv("OVERSQLITE_REPLAY_REVERSE") == "1" {
				modes = []string{"all", "workspace", "raw"}
			}
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					ctx := context.Background()
					owner, err := beginImmediateTx(ctx, db)
					require.NoError(t, err)
					defer owner.Rollback()
					tx := &replayDiagnosticTx{sqliteTransaction: owner, t: t, mode: mode, stmts: map[string]*sql.Stmt{}}
					defer tx.close()
					c.syncWorkUsed = 0
					c.a3Stats = a3AccountingStats{}
					replayDiagnosticStart()
					start := time.Now()
					err = c.prepareApplySurvivors(ctx, tx, applyUnit("diagnostic", 0, ""), 1, func(int) (applySurvivor, error) {
						p, err := loadApplyPlan(ctx, tx)
						if err != nil {
							return applySurvivor{}, err
						}
						return c.captureApplySurvivor(ctx, tx, p, "nodes", "Z00001")
					}, "settlement")
					require.NoError(t, err)
					cache := newTxStmtCache(tx)
					_, err = c.restorePrimarySettlementIntent(ctx, tx, cache, true)
					require.NoError(t, err)
					require.NoError(t, cache.Close())
					tx.close()
					elapsed := time.Since(start)
					observation := replayDiagnosticStop()
					// Read directly from owner after ending instrumentation; don't reuse closed handles.
					p, err := loadApplyPlan(ctx, owner)
					require.NoError(t, err)
					vector := map[string]any{"plan": fmt.Sprintf("%s/%s/%s/%d/%d/%d/%d/%v/%d/%d/%d/%d/%s/%v/%d/%t", p.unit, p.guard, p.phase, p.root, p.visit, p.table, p.fk, p.cursor, p.entries, p.bytes, p.edgeEntries, p.edgeBytes, p.kind, p.preserve, p.atomicWork, p.preservationComplete), "stats": fmt.Sprintf("%+v", c.a3Stats), "work": c.syncWorkUsed}
					if expected == nil {
						expected = vector
					} else {
						require.Equal(t, expected, vector)
					}
					require.NoError(t, owner.Rollback())
					require.Equal(t, retained, a3UnrelatedIntentDigest(t, db))
					replayDiagnosticRecord(t, map[string]any{"rows": n, "mode": mode, "kernel_ns": elapsed.Nanoseconds(), "handles": len(tx.stmts), "vector": vector, "observation": observation})
				})
			}
		})
	}
}

// Test-only compatibility across the matched pre-cache and candidate copies.
// This reads SDK numeric state, never a private driver pointer.
func replayStatementWork(scope *workspaceBufferScope) int64 {
	if scope == nil {
		return 0
	}
	v := reflect.ValueOf(scope).Elem().FieldByName("statementWork")
	if !v.IsValid() {
		return 0
	}
	return v.Int()
}

type replayMemorySampler struct {
	done           chan struct{}
	stopped        chan struct{}
	mu             sync.Mutex
	peak           map[string]int64
	stopOnce       sync.Once
	nativeObserved bool
}

func startReplayMemorySampler() *replayMemorySampler {
	s := &replayMemorySampler{done: make(chan struct{}), stopped: make(chan struct{}), peak: map[string]int64{}}
	go func() {
		defer close(s.stopped)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				s.sample()
			}
		}
	}()
	return s
}
func (s *replayMemorySampler) sample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	values := replayDiagnosticMemory()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peak["go_heap_alloc"] = max(s.peak["go_heap_alloc"], int64(m.HeapAlloc))
	s.peak["go_heap_inuse"] = max(s.peak["go_heap_inuse"], int64(m.HeapInuse))
	if values == nil {
		return
	}
	s.nativeObserved = true
	var statements int64
	for key, n := range values {
		if key == "native_memory_used" {
			s.peak[key] = max(s.peak[key], n)
		} else {
			statements += n
		}
	}
	s.peak["native_statement_memused_bytes"] = max(s.peak["native_statement_memused_bytes"], statements)
}
func (s *replayMemorySampler) stop() map[string]int64 {
	s.stopOnce.Do(func() {
		close(s.done)
		<-s.stopped
		s.sample()
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]int64, len(s.peak))
	for key, value := range s.peak {
		result[key] = value
	}
	return result
}
