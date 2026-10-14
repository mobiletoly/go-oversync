package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestA3Settlement_BacklogBound(t *testing.T) {
	for _, n := range []int{1000, 10000} {
		for _, width := range []int{16, 1024} {
			t.Run(fmt.Sprintf("%d/%d", n, width), func(t *testing.T) {
				var readScope *workspaceBufferScope
				c, db := newA1SurvivorClient(t)
				c.config.MaxSyncWorkUnitsPerInvocation = 1000000
				_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
				require.NoError(t, err)
				c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
				_, err = c.PushPending(context.Background())
				require.ErrorIs(t, err, context.Canceled)
				// Setup ends after the exact one-row outbox is frozen. The measured public
				// call cannot drain or upload the unrelated backlog to simplify the proof.
				payload := strings.Repeat("x", width)
				_, err = db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO nodes SELECT printf('Z%05d',i),?,NULL FROM n`, n, strings.Repeat("b", width))
				require.NoError(t, err)
				// Seed published historical A, then let managed DML capture full B.
				_, err = db.Exec(`UPDATE _sync_row_state SET row_version=1,server_kind='live',
 server_payload=(SELECT payload FROM _sync_dirty_rows d WHERE d.schema_name=_sync_row_state.schema_name AND d.table_name=_sync_row_state.table_name AND d.key_json=_sync_row_state.key_json),
 server_provenance=(SELECT server_provenance FROM _sync_row_state WHERE key_json='{"id":"P"}'),relationships_known=1
 WHERE json_extract(key_json,'$.id') LIKE 'Z%';
 DELETE FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id') LIKE 'Z%';
 UPDATE nodes SET name=? WHERE id LIKE 'Z%'`, payload)
				require.NoError(t, err)
				require.EqualValues(t, n, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows WHERE base_kind='present' AND base_payload IS NOT NULL`))
				var frozenHash string
				retained := a3UnrelatedIntentDigest(t, db)
				var frozenID int64
				require.NoError(t, db.QueryRow(`SELECT canonical_request_hash,source_bundle_id FROM _sync_outbox_bundle`).Scan(&frozenHash, &frozenID))
				require.EqualValues(t, 2, frozenID)
				c.HTTP = &http.Client{Transport: a1SettlementServer(t)}
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				rssBefore := currentProcessPeakRSSBytes()
				highWater := startGoMemoryHighWater()
				c.SetBeforePushReplayHook(func(ctx context.Context) error {
					readScope, _ = ctx.Value(workspaceBufferKey{}).(*workspaceBufferScope)
					return nil
				})
				report, err := c.PushPending(context.Background())
				peakHeap, peakInUse := highWater.finish()
				rssAfter := currentProcessPeakRSSBytes()
				runtime.ReadMemStats(&after)
				require.NoError(t, err)
				require.Equal(t, retained, a3UnrelatedIntentDigest(t, db), "complete unrelated L, B, provenance, ordinal and timestamps")
				require.Equal(t, PushOutcomeCommitted, report.Outcome)
				requireA1Survivors(t, db)
				var mismatches, dirty, outbox int64
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM nodes n LEFT JOIN _sync_dirty_rows d ON d.table_name='nodes' AND d.key_json=json_object('id',n.id) WHERE n.id LIKE 'Z%' AND (n.name<>? OR n.parent_id IS NOT NULL OR d.payload IS NULL OR json_extract(d.payload,'$.id.value')<>n.id OR json_extract(d.payload,'$.name.value')<>? OR json_extract(d.payload,'$.parent_id.storage')<>'null')`, payload, payload).Scan(&mismatches))
				require.Zero(t, mismatches)
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_dirty_rows`).Scan(&dirty))
				require.EqualValues(t, n, dirty)
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM _sync_outbox_rows`).Scan(&outbox))
				require.Zero(t, outbox)
				require.EqualValues(t, 3, countA3(t, db, `SELECT next_source_bundle_id FROM _sync_source_state`))
				require.EqualValues(t, 2, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
				require.EqualValues(t, 0, countA3(t, db, `SELECT apply_mode FROM _sync_apply_state`))
				require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`))
				require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
				stats := c.a3Stats
				require.LessOrEqual(t, stats.savedCopies, int64(5))
				require.Less(t, stats.savedBytes, int64(4096))
				require.LessOrEqual(t, stats.peakPrimaryCopies, int64(14))
				require.Less(t, stats.peakPrimaryBytes, int64(64*(width+1024)))
				require.GreaterOrEqual(t, stats.primaryBytes, int64(n*width))
				require.Positive(t, stats.serializedBytes)
				require.LessOrEqual(t, stats.serializedBytes, 4*stats.primaryBytes+4096, "cumulative streamed live-row conversions; retained copies are measured separately")
				require.GreaterOrEqual(t, stats.primaryReads, int64(n))
				require.LessOrEqual(t, stats.primaryReads, int64(5*n+10))
				require.Equal(t, 2*stats.primaryReads, stats.indexedVisits)
				require.LessOrEqual(t, stats.restorationPasses, int64(3))
				require.Greater(t, stats.duplicateChecks, int64(0))
				require.GreaterOrEqual(t, stats.totalWork, int64(1024*n))
				require.Less(t, stats.totalWork-replayStatementWork(readScope), int64(2048*n+10000))
				var id, parent, detail string
				var selectID int
				require.NoError(t, db.QueryRow(`EXPLAIN QUERY PLAN SELECT payload FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1`, "", "", "").Scan(&selectID, &id, &parent, &detail))
				require.Contains(t, detail, "SEARCH")
				require.Contains(t, detail, "INDEX")
				require.NotContains(t, detail, "TEMP")
				t.Logf("one public PushPending; setup excluded; frozen=%s/%d; stats=%+v work=%d totalAlloc=%d heapBefore=%d heapAfter=%d", frozenHash, frozenID, stats, stats.totalWork, after.TotalAlloc-before.TotalAlloc, before.HeapAlloc, after.HeapAlloc)
				t.Logf("measured public call: heapPeak=%d heapInUsePeak=%d processRSSHighWaterBefore=%d processRSSHighWaterAfter=%d (process cumulative high-water, not invocation-only RSS)", peakHeap, peakInUse, rssBefore, rssAfter)
			})
		}
	}
}

func a3UnrelatedIntentDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	hash := sha256.New()
	for _, owner := range []struct{ table, key string }{
		{"nodes", "id"}, {"_sync_dirty_rows", "json_extract(key_json,'$.id')"},
		{"_sync_row_state", "json_extract(key_json,'$.id')"}, {"_sync_relationships", "json_extract(child_key,'$.id')"},
	} {
		columns, err := db.Query(`PRAGMA table_info("` + owner.table + `")`)
		require.NoError(t, err)
		var projection, ordering []string
		for columns.Next() {
			var cid, notnull, pk int
			var name, kind string
			var defaultValue any
			require.NoError(t, columns.Scan(&cid, &name, &kind, &notnull, &defaultValue, &pk))
			q := `"` + name + `"`
			ordering = append(ordering, q)
			projection = append(projection, fmt.Sprintf(`'%s',json_object('storage',typeof(%s),'value',CASE WHEN typeof(%s)='blob' THEN lower(hex(%s)) WHEN %s IS NULL THEN NULL ELSE CAST(%s AS TEXT) END)`, name, q, q, q, q, q))
		}
		require.NoError(t, columns.Err())
		require.NoError(t, columns.Close())
		require.NotEmpty(t, projection)
		rows, err := db.Query(`SELECT json_object(` + strings.Join(projection, ",") + `) FROM "` + owner.table + `" WHERE ` + owner.key + ` LIKE 'Z%' ORDER BY ` + strings.Join(ordering, ","))
		require.NoError(t, err)
		_, err = hash.Write([]byte(owner.table + "\n"))
		require.NoError(t, err)
		for rows.Next() {
			var encoded string
			require.NoError(t, rows.Scan(&encoded))
			_, err = hash.Write([]byte(encoded + "\n"))
			require.NoError(t, err)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func TestA3Settlement_TriggerRestoration(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	c.SetBeforePushCommitHook(func(context.Context) error { return context.Canceled })
	_, err = c.PushPending(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	// U now has newer L, while an apply-mode business write produces a supported
	// live deviation from L. It must become an exception, never a replacement B.
	_, err = db.Exec(`INSERT INTO nodes VALUES('U','new L',NULL); UPDATE _sync_apply_state SET apply_mode=1; UPDATE nodes SET name='live exception' WHERE id='U'; UPDATE _sync_apply_state SET apply_mode=0; CREATE TRIGGER alter_u AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='U'; END`)
	require.NoError(t, err)
	c.HTTP = &http.Client{Transport: a1SettlementServer(t)}
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	var name, desired string
	require.NoError(t, db.QueryRow(`SELECT name FROM nodes WHERE id='U'`).Scan(&name))
	require.Equal(t, "live exception", name)
	require.NoError(t, db.QueryRow(`SELECT payload FROM _sync_dirty_rows WHERE key_json='{"id":"U"}'`).Scan(&desired))
	require.Contains(t, desired, "new L")
	requireA1Survivors(t, db)
	require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_apply_rows`))
}

// Representative memory benchmark for the same paged primary preservation
// kernel. The public test above owns settlement and checkpoint assertions.
func BenchmarkA3SettlementBacklog(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		for _, width := range []int{16, 1024} {
			b.Run(fmt.Sprintf("%d/%d", n, width), func(b *testing.B) {
				for iteration := 0; iteration < b.N; iteration++ {
					b.StopTimer()
					db, err := sql.Open("sqlite3", ":memory:")
					require.NoError(b, err)
					db.SetMaxOpenConns(1)
					_, err = db.Exec(`CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT)`)
					require.NoError(b, err)
					tables := []SyncTable{{TableName: "nodes", SyncKeyColumnName: "id"}}
					config := admittedFixtureConfig("main", tables, testSchemaGenerationProof())
					registered := []oversync.RegisteredTable{{Schema: "main", Table: "nodes", SyncKeyColumns: []string{"id"}}}
					generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{{Schema: "main", Table: "nodes", Columns: []oversync.GenerationColumn{
						{Name: "id", WireType: oversync.GenerationWireTypeText},
						{Name: "name", WireType: oversync.GenerationWireTypeText},
						{Name: "parent_id", WireType: oversync.GenerationWireTypeText, Nullable: true},
					}}}}
					registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
					require.NoError(b, err)
					compiled, ok := registry.Generation(1)
					require.True(b, ok)
					config.generationProof = oversync.SchemaGenerationProof{ID: 1, Fingerprint: compiled.Fingerprint()}
					config.localContract = &LocalGenerationContract{Proof: config.generationProof, RegisteredTables: registered, Generation: generation, Tables: tables,
						Columns: []LocalTableColumns{{Schema: "main", Table: "nodes", Columns: []LocalColumnContract{
							{Name: "id", Codec: LocalCodecText}, {Name: "name", Codec: LocalCodecText}, {Name: "parent_id", Codec: LocalCodecText},
						}}}}
					config.DatabaseID = "benchmark"
					config.ServiceID = "benchmark"
					config.LocalVersion = 1
					config.MaxSyncWorkUnitsPerInvocation = 100000000
					c, err := newAdmittedFixtureClient(db, "http://benchmark.invalid", tokenProviderForTests, config)
					require.NoError(b, err)
					_, err = db.Exec(`INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES('benchmark-source',1);
                        INSERT INTO _sync_conflict_state(singleton_key) VALUES(1) ON CONFLICT DO NOTHING;
                        UPDATE _sync_attachment_state SET current_source_id='benchmark-source',binding_state='attached',attached_user_id='benchmark-user',schema_name='main'`)
					require.NoError(b, err)
					_, err = db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO nodes SELECT printf('Z%05d',i),?,NULL FROM n`, n, strings.Repeat("x", width))
					require.NoError(b, err)
					tx, err := beginImmediateTx(context.Background(), db)
					require.NoError(b, err)
					_, err = c.applyGuard(context.Background(), tx, applyUnit("benchmark", 0, ""))
					require.NoError(b, err, "benchmark guard")
					b.ReportAllocs()
					b.StartTimer()
					err = c.prepareApplySurvivors(context.Background(), tx, applyUnit("benchmark", 0, ""), 1, func(int) (applySurvivor, error) {
						plan, err := loadApplyPlan(context.Background(), tx)
						if err != nil {
							return applySurvivor{}, err
						}
						return c.captureApplySurvivor(context.Background(), tx, plan, "nodes", "Z00001")
					}, "settlement")
					require.NoError(b, err)
					cache := newTxStmtCache(tx)
					_, err = c.restorePrimarySettlementIntent(context.Background(), tx, cache, true)
					require.NoError(b, err)
					require.NoError(b, cache.Close())
					require.NoError(b, tx.Commit(context.Background()))
					b.StopTimer()
					b.ReportMetric(float64(c.a3Stats.primaryReads), "primary-reads/op")
					b.ReportMetric(float64(c.a3Stats.savedCopies), "saved-copies/op")
					require.NoError(b, c.Close())
					require.NoError(b, db.Close())
				}
			})
		}
	}
}
