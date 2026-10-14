package oversqlite

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestWorkSliceCompletion_BackgroundPushDoesNotBackOffForLocalSlices(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
	c.config.MaxSyncWorkUnitsPerInvocation = 1
	c.config.BackoffMin, c.config.BackoffMax = time.Hour, time.Hour
	c.PauseDownloads()
	server := newMockPushServer(t)
	var requests atomic.Int64
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		return server.RoundTrip(r)
	})}
	_, err := db.Exec(`INSERT INTO users VALUES('a','A'),('b','B'),('c','C'),('d','D'),('e','E'),('f','F')`)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.Start(ctx))
	t.Cleanup(func() { require.NoError(t, c.Stop(context.Background())) })
	for countA3(t, db, `SELECT (SELECT count(*) FROM _sync_dirty_rows)+(SELECT count(*) FROM _sync_outbox_rows)+(SELECT reserved_entries+reserved_bytes FROM _sync_operation_state)`) > 0 {
		select {
		case <-ctx.Done():
			t.Fatal("background push failed to finish local slices before failure backoff")
		case <-time.After(time.Millisecond):
		}
	}
	require.NoError(t, c.Stop(ctx))
	require.EqualValues(t, 1, requests.Load())
	require.Positive(t, c.a3Stats.workSlices)
	require.EqualValues(t, 0, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
	require.EqualValues(t, 0, countA3(t, db, `SELECT reserved_entries+reserved_bytes FROM _sync_operation_state`))
}

func TestWorkSliceCompletion_OnePushAndOneSync(t *testing.T) {
	for _, target := range []int64{1, 8, 17, 1000000} {
		t.Run(strconv.FormatInt(target, 10), func(t *testing.T) {
			c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/sync/push" {
					return server.RoundTrip(r)
				}
				require.Equal(t, "/sync/pull", r.URL.Path)
				return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
			})}
			c.config.UploadLimit = 2
			c.config.MaxSyncWorkUnitsPerInvocation = target
			_, err := db.Exec(`INSERT INTO users VALUES('a','A'),('b','B'),('c','C'),('d','D'),('e','E')`)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err = c.PushPending(ctx)
			require.NoError(t, err, "one push must complete its bundle without caller continuation")
			require.Len(t, server.pushRequests, 1)
			require.EqualValues(t, 3, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			if target < 1000000 {
				require.Positive(t, c.a3Stats.workSlices)
			}
			_, err = c.Sync(ctx)
			require.NoError(t, err, "one sync must drain the remaining bundles")
			require.Len(t, server.pushRequests, 3)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
		})
	}
}

func TestWorkSliceCompletion_NoProgressAndMixedFailure(t *testing.T) {
	c, _ := newA1SurvivorClient(t)
	_, err := completeWorkPhase(context.Background(), c, "stuck", func(context.Context) (int, error) {
		return 0, &SyncWorkLimitError{Limit: 1, Used: 1}
	})
	var stalled *SyncWorkNoProgressError
	require.ErrorAs(t, err, &stalled)
	cleanup := errors.New("cleanup failed")
	calls := 0
	_, err = completeWorkPhase(context.Background(), c, "mixed", func(ctx context.Context) (int, error) {
		calls++
		checkpointWorkStep(ctx)
		commitWorkStep(ctx)
		return 0, errors.Join(&SyncWorkLimitError{Limit: 1, Used: 1}, cleanup)
	})
	require.ErrorIs(t, err, cleanup)
	require.Equal(t, 1, calls, "cleanup failure must never be absorbed")
}

func TestPlanningWorkspaceSnapshot_ChargesAndWriterIsolation(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx := context.Background()
	tx, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	defer tx.Rollback()
	push := workspaceCharge{entries: 8, bytes: 4096}
	before, err := c.workspaceUsage(ctx, tx, "snapshot-test", nil, &push)
	require.NoError(t, err)
	scoped := context.WithValue(ctx, pushPlanningWorkspaceKey{}, &pushPlanningWorkspace{writer: tx})
	actual, err := c.workspaceUsage(scoped, tx, "snapshot-test", nil, &push)
	require.NoError(t, err)
	require.Equal(t, workspaceCharge{before.entries + 2, before.bytes + 256}, actual)
	reads := c.a3Stats.workspaceMetadataReads
	push.entries += 3
	push.bytes += 128
	next, err := c.workspaceUsage(scoped, tx, "snapshot-test", nil, &push)
	require.NoError(t, err)
	require.Equal(t, workspaceCharge{actual.entries + 3, actual.bytes + 128}, next)
	require.Equal(t, reads, c.a3Stats.workspaceMetadataReads, "only current push charges change within the writer")
	snapshot := scoped.Value(pushPlanningWorkspaceKey{}).(*pushPlanningWorkspace)
	snapshot.guard = &pushPlanningGuard{hash: "synthetic", initialization: "", next: 1}
	c.config.MaxSyncWorkspaceEntries = before.entries + 2
	require.NoError(t, c.checkAggregateWorkspace(scoped, tx, "snapshot-test", nil, &workspaceCharge{8, 4096}, workspaceCharge{}))
	require.Nil(t, snapshot.guard, "optional guard cache yields to useful workspace")
	c.config.MaxSyncWorkspaceEntries = before.entries + 1
	var capacity *SyncWorkspaceLimitError
	require.ErrorAs(t, c.checkAggregateWorkspace(scoped, tx, "snapshot-test", nil, &workspaceCharge{8, 4096}, workspaceCharge{}), &capacity)
	require.Equal(t, "entries", capacity.Dimension)
	require.NoError(t, tx.Commit(ctx))
	other, err := beginImmediateTx(ctx, db)
	require.NoError(t, err)
	defer other.Rollback()
	uncached, err := c.workspaceUsage(scoped, other, "snapshot-test", nil, &workspaceCharge{8, 4096})
	require.NoError(t, err)
	require.Equal(t, before, uncached, "a completed writer's snapshot cannot cross a transaction boundary")
}

func TestWorkSliceCompletion_CancellationPreservesIdentity(t *testing.T) {
	c, _ := newA1SurvivorClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	_, err := completeWorkPhase(ctx, c, "cancelled", func(ctx context.Context) (int, error) {
		checkpointWorkStep(ctx)
		commitWorkStep(ctx)
		cancel()
		return 0, &SyncWorkLimitError{Limit: 1, Used: 1}
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestWorkSliceCompletion_RebuildKeepsOneSnapshot(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,email TEXT NOT NULL)`)
	c.config.MaxSyncWorkUnitsPerInvocation = 1
	rows := []oversync.SnapshotRow{
		{Schema: "main", Table: "users", Key: mustBundleKey("a"), RowVersion: 9, Payload: mustBundlePayload(t, "a", "A", "a@t")},
		{Schema: "main", Table: "users", Key: mustBundleKey("b"), RowVersion: 9, Payload: mustBundlePayload(t, "b", "B", "b@t")},
	}
	creates, chunks := 0, 0
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost {
			creates++
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "one-snapshot", SnapshotBundleSeq: 9, RowCount: 2, ByteCount: testSnapshotRowsByteCount(rows), ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		}
		if r.Method == http.MethodDelete {
			return &http.Response{StatusCode: 204, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		}
		chunks++
		return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "one-snapshot", SnapshotBundleSeq: 9, Rows: rows, NextRowOrdinal: 2}), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, creates)
	require.Equal(t, 1, chunks, "local work slicing must not fetch the complete input again")
	require.EqualValues(t, 2, countA3(t, db, `SELECT count(*) FROM users`))
	requireLastBundleSeqSeen(t, c, ctx, 9)
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_snapshot_stage`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_apply_plan`))
}

func TestWorkSliceCompletion_FourteenTableSnapshot3399(t *testing.T) {
	const totalRows = 3399
	tables := make([]SyncTable, 14)
	ddl := make([]string, len(tables))
	var rows []oversync.SnapshotRow
	for table := range tables {
		name := fmt.Sprintf("capacity_%02d", table)
		tables[table] = SyncTable{TableName: name, SyncKeyColumnName: "id"}
		columns := "id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL"
		if table == 13 {
			columns += ",parent_a TEXT REFERENCES capacity_00(id),parent_b TEXT REFERENCES capacity_01(id),parent_c TEXT REFERENCES capacity_02(id)"
		}
		ddl[table] = "CREATE TABLE " + name + "(" + columns + ")"
		count := totalRows / len(tables)
		if table < totalRows%len(tables) {
			count++
		}
		for row := range count {
			id := fmt.Sprintf("row-%06d", row)
			payload := map[string]any{"id": id, "name": fmt.Sprintf("table-%02d-value-%06d", table, row)}
			if table == 13 {
				payload["parent_a"], payload["parent_b"], payload["parent_c"] = id, id, id
			}
			rows = append(rows, oversync.SnapshotRow{Schema: "main", Table: name, Key: mustBundleKey(id), RowVersion: 91, Payload: mustJSONPayload(t, payload)})
		}
	}
	c, db := newBundleClient(t, "main", tables, ddl...)
	require.EqualValues(t, 1000000, c.config.MaxSyncWorkUnitsPerInvocation)
	var pages [][]oversync.SnapshotRow
	for start := 0; start < len(rows); start += 200 {
		pages = append(pages, rows[start:min(start+200, len(rows))])
	}
	setChunkedSnapshotSession(t, c, "fourteen-table-snapshot", 91, pages)
	transport := c.HTTP.Transport
	creates, chunks, deletes := 0, 0, 0
	c.HTTP.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPost:
			creates++
		case http.MethodGet:
			chunks++
		case http.MethodDelete:
			deletes++
		}
		return transport.RoundTrip(request)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := c.Rebuild(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, creates)
	require.Equal(t, len(pages), chunks, "each snapshot cursor is fetched once across local slices")
	require.Equal(t, 1, deletes)
	require.Greater(t, c.a3Stats.totalWork, int64(0), "the complete snapshot remains charged")
	for table, definition := range tables {
		count := totalRows / len(tables)
		if table < totalRows%len(tables) {
			count++
		}
		require.EqualValues(t, count, countA3(t, db, "SELECT count(*) FROM "+definition.TableName))
		query := fmt.Sprintf("SELECT count(*) FROM %s WHERE name<>printf('table-%02d-value-%%06d',CAST(substr(id,5) AS INTEGER))", definition.TableName, table)
		require.Zero(t, countA3(t, db, query), "complete business payload readback")
	}
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM capacity_13 WHERE parent_a<>id OR parent_b<>id OR parent_c<>id`))
	require.Zero(t, countA3(t, db, `SELECT count(*) FROM pragma_foreign_key_check`))
	require.EqualValues(t, totalRows, countA3(t, db, `SELECT count(*) FROM _sync_row_state WHERE row_version=91`))
	requireLastBundleSeqSeen(t, c, ctx, 91)
	for _, table := range []string{"_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"} {
		require.Zero(t, countA3(t, db, "SELECT count(*) FROM "+table))
	}
	require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
	stats := c.a3Stats
	require.NoError(t, c.Close())
	require.Zero(t, db.Stats().InUse, "close leaves no live database owner")
	t.Logf("one public Rebuild: rows=%d tables=14 chunks=%d slices=%d total_work=%d", totalRows, chunks, stats.workSlices, stats.totalWork)
}

func TestWorkSliceCompletion_MetadataMeasurements(t *testing.T) {
	tables := make([]SyncTable, 8)
	ddl := make([]string, 8)
	for i := range tables {
		name := "users"
		if i > 0 {
			name = fmt.Sprintf("aux_%d", i)
		}
		tables[i] = SyncTable{TableName: name, SyncKeyColumnName: "id"}
		ddl[i] = "CREATE TABLE " + name + "(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)"
	}
	c, db := newBundleClient(t, "main", tables, ddl...)
	c.config.UploadLimit = 40
	_, err := db.Exec(`WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<125) INSERT INTO users SELECT printf('user-%03d',i),'value' FROM n`)
	require.NoError(t, err)
	server := newMockPushServer(t)
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			decoder, err := gzip.NewReader(r.Body)
			require.NoError(t, err)
			body, err := io.ReadAll(decoder)
			require.NoError(t, err)
			require.NoError(t, decoder.Close())
			require.NoError(t, r.Body.Close())
			copy := r.Clone(r.Context())
			copy.Body = io.NopCloser(bytes.NewReader(body))
			copy.Header.Del("Content-Encoding")
			copy.ContentLength = int64(len(body))
			r = copy
		}
		return server.RoundTrip(r)
	})}
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, 1)
	require.EqualValues(t, 85, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.EqualValues(t, 2, c.a3Stats.pushMetadataMeasurements, "one preparation measurement plus one final freeze measurement")
	t.Logf("rows=125 tables=8 metadata_measurements=%d slices=%d planning_steps=%d planning_transactions=%d", c.a3Stats.pushMetadataMeasurements, c.a3Stats.workSlices, c.a3Stats.pushPlanningSteps, c.a3Stats.pushPlanningTransactions)
	t.Logf("guard_reconstructions=%d workspace_metadata_reads=%d", c.a3Stats.pushGuardReconstructions, c.a3Stats.workspaceMetadataReads)
}

func TestWorkSliceCompletion_UnpublishedDeleteSeedVisits(t *testing.T) {
	c, db := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, `CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)`)
	c.config.UploadLimit = 40
	_, err := db.Exec(`WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<125) INSERT INTO users SELECT printf('live-%03d',i),'value' FROM n`)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("noop-%03d", i)
		_, err = db.Exec(`INSERT INTO users VALUES(?,'temporary')`, id)
		require.NoError(t, err)
		_, err = db.Exec(`DELETE FROM users WHERE id=?`, id)
		require.NoError(t, err)
	}
	plan, err := c.preparePushPlan(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 40, plan.CandidateRows)
	require.EqualValues(t, 125, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
	require.EqualValues(t, 125, countA3(t, db, `SELECT count(*) FROM _sync_push_nodes`))
	require.EqualValues(t, 136, c.a3Stats.seedVisits, "each live/no-op key plus the terminal seed step is visited once")
	t.Logf("live_rows=125 noops=10 seed_visits=%d slices=%d", c.a3Stats.seedVisits, c.a3Stats.workSlices)
}

// A refusal inside the next step must not publish that step's partial writes.
func TestWorkSliceCompletion_PartialStepRollback(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint(mixed), func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			_, err := db.Exec("CREATE TABLE step_witness(id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), workSliceKey{}, &workSlice{})
			tx, err := beginImmediateTx(ctx, db)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, c.withPushPlanningStep(ctx, tx, func() error { _, e := tx.ExecContext(ctx, "INSERT INTO step_witness VALUES(1)"); return e }))
			checkpointWorkStep(ctx)
			signal := &SyncWorkLimitError{Unit: applyUnit("step-test", 0, c.sourceID), Limit: c.config.MaxSyncWorkUnitsPerInvocation, Used: c.syncWorkUsed}
			err = c.withPushPlanningStep(ctx, tx, func() error {
				_, e := tx.ExecContext(ctx, "INSERT INTO step_witness VALUES(2)")
				if e != nil {
					return e
				}
				if mixed {
					_, e = tx.ExecContext(ctx, "RELEASE oversqlite_push_step")
					if e != nil {
						return e
					}
				}
				return signal
			})
			require.ErrorIs(t, err, signal)
			if mixed {
				require.False(t, pureApplyWork(err), "a failed savepoint cleanup cannot be treated as a scheduling signal")
				require.NoError(t, tx.Rollback())
			} else {
				require.True(t, pureApplyWork(err))
				require.NoError(t, tx.Commit(ctx))
			}
			var n int
			require.NoError(t, db.QueryRow("SELECT count(*) FROM step_witness").Scan(&n))
			if mixed {
				require.Zero(t, n)
			} else {
				require.Equal(t, 1, n, "only the earlier complete frontier is committed")
			}
		})
	}
}

func TestWorkSliceCompletion_PlanningStatementBindingsAndFallback(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	ctx, tx, q, cache, scope := configuredReadCache(t, c, db)
	cache.planningWrites = true
	query := "UPDATE nodes SET name=? WHERE id=?"
	for _, name := range []string{"first", "second"} {
		_, err := q.ExecContext(ctx, query, name, "P")
		require.NoError(t, err)
	}
	require.Len(t, cache.stmts, 1, "bounded scalar mutations reuse the current writer's sole handle")
	before := cache.pool.capacity
	_, err := q.ExecContext(ctx, query, string(bytes.Repeat([]byte{'x'}, 12000)), "P")
	require.NoError(t, err)
	require.Len(t, cache.stmts, 1, "oversized binding stays one-shot")
	require.Equal(t, before, cache.pool.capacity, "fallback does not grow the admitted handle pool")
	require.Positive(t, scope.statementWork)
	require.NoError(t, cache.closeAndRelease(ctx))
	require.NoError(t, tx.Rollback())
}
