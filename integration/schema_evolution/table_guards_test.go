package schemaevolution

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestTableEvolution_CleanupTakeoverFencesWholeAudit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	p := tablePushRequest(t, old, 1)
	_, err = old.PushOneShot(ctx, p.actor, p.proof, &p.request)
	require.NoError(t, err)
	_, err = old.CreateSnapshotSession(ctx, p.actor, p.proof)
	require.NoError(t, err)
	_, s := tableCleanupStart(t, pool, old, tableTestBounds())
	_, err = pool.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=clock_timestamp()-interval '1 second'`)
	require.NoError(t, err)
	first, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(first, workerLock)
	cap, err := claimWorker(ctx, first)
	require.NoError(t, err)
	oldCtx := context.WithValue(ctx, migrationContextKey{}, cap)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	result, err := s.CleanupExpiredStaging(oldCtx, tx)
	require.NoError(t, err)
	require.NotNil(t, result)
	var terminated bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, cap.pid).Scan(&terminated))
	require.True(t, terminated)
	next, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(next, workerLock)
	claimed := make(chan error, 1)
	go func() { _, err := claimWorker(ctx, next); claimed <- err }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND classid=1869833075 AND objid=9 AND NOT granted)`, int32(next.Conn().PgConn().PID())).Scan(&waiting)
		return err == nil && waiting
	}, time.Second, 10*time.Millisecond)
	// The already admitted predecessor can finish its atomic audit before takeover.
	_, err = tx.Exec(ctx, `INSERT INTO app_evolution.table_cleanup VALUES($1,$2,$3)`, uuid.New(), cap.epoch, result)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `UPDATE app_evolution.table_run SET batches=batches+1 WHERE singleton`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-claimed)
	denied, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer denied.Rollback(ctx)
	_, err = s.CleanupExpiredStaging(oldCtx, denied)
	require.ErrorContains(t, err, "fenced")
	w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
	_, err = w.cleanupBatch(oldCtx, uuid.New())
	require.ErrorContains(t, err, "fenced")
}

func TestTableEvolution_BudgetsAndExplicitRepair(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	key := uuid.New()
	require.NoError(t, fixtureWrite(t, old, "scope", `INSERT INTO main.people VALUES('scope',$1,'Person')`, key))
	proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	s := tableBackfillStart(t, pool, proof, cleanup)
	w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
	// A transient database defect rolls back the captured INSERT and checkpoint.
	_, err = pool.Exec(ctx, `ALTER TABLE main.projects ADD CONSTRAINT test_reject CHECK(false)`)
	require.NoError(t, err)
	require.Error(t, w.Populate(ctx))
	var failed uuid.UUID
	var original tableDescriptor
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id,descriptor FROM app_evolution.table_batches WHERE state='failed'`).Scan(&failed, &original))
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.projects`).Scan(&count))
	require.Zero(t, count)
	_, err = pool.Exec(ctx, `ALTER TABLE main.projects DROP CONSTRAINT test_reject`)
	require.NoError(t, err)
	require.ErrorContains(t, w.Populate(ctx), "unresolved")
	require.NoError(t, w.RepairTableBatch(ctx, failed))
	require.NoError(t, w.RepairTableBatch(ctx, failed))
	var replacement tableDescriptor
	require.NoError(t, pool.QueryRow(ctx, `SELECT descriptor FROM app_evolution.table_batches WHERE replaces=$1`, failed).Scan(&replacement))
	require.Equal(t, original.Keys, replacement.Keys)
	require.Equal(t, original.Deadline, replacement.Deadline)
	require.NotEqual(t, original.Operation, replacement.Operation)
	require.NoError(t, w.Populate(ctx))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log WHERE source_id='schema-evolution-backfill'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestTableEvolution_ClosedExhaustionAndCoverage(t *testing.T) {
	for _, reason := range []string{"capacity", "deadline", "history", "missing_capture", "scope_drift"} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			pool, old := tableBaseline(t)
			_, err := CreateScope(ctx, pool, "scope")
			require.NoError(t, err)
			key := uuid.New()
			require.NoError(t, fixtureWrite(t, old, "scope", `INSERT INTO main.people VALUES('scope',$1,'Person')`, key))
			b := tableTestBounds()
			proof, cleanup := tableCleanupStart(t, pool, old, b)
			s := tableBackfillStart(t, pool, proof, cleanup)
			w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
			switch reason {
			case "capacity":
				w.Capacity = func(context.Context) (Capacity, error) { return Capacity{}, nil }
			case "deadline":
				_, err = pool.Exec(ctx, `UPDATE app_evolution.table_run SET bounds=jsonb_set(bounds,'{Deadline}',to_jsonb((clock_timestamp()-interval '1 second')::text))`)
				require.NoError(t, err)
			case "history":
				_, err = pool.Exec(ctx, `UPDATE app_evolution.table_run SET history_bytes=$1`, b.MaxHistoryBytes)
				require.NoError(t, err)
			case "missing_capture":
				w.boundary = func(point string) {
					if point == "population_after_commit" {
						_, err := pool.Exec(ctx, `DELETE FROM sync.row_state WHERE table_id=4`)
						require.NoError(t, err)
					}
				}
			case "scope_drift":
				_, err = pool.Exec(ctx, `UPDATE app_evolution.scope_inventory SET incarnation=$1`, uuid.New())
				require.Error(t, err, "closed lifecycle blocks scope mutations")
				_, err = pool.Exec(ctx, `UPDATE app_evolution.table_run SET watermark=watermark+1`)
				require.NoError(t, err)

			}
			require.Error(t, w.Populate(ctx))
			status, err := MaintenanceStatus(ctx, pool)
			require.NoError(t, err)
			require.False(t, status.AdmissionOpen)
			require.Error(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{s}))
			require.NoError(t, s.Close(ctx))
			a, err := OpenAuthority(ctx, proof)
			require.NoError(t, err)
			cfg, _ := TableEvolutionConfiguration(TablesServing)
			require.Error(t, a.Publish(ctx, cfg))
			a.Close()
		})
	}
}

func TestTableEvolution_RuntimeRoleCannotAppendOrForge(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	w := &TableWorker{Pool: pool, Service: cleanup, Capacity: tableTestCapacity}
	require.NoError(t, w.Cleanup(ctx))
	require.NoError(t, cleanup.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesAppend))
	role := "table_evolution_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN PASSWORD 'table_evolution-test-role'`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := pool.Exec(ctx, `DROP OWNED BY `+pgx.Identifier{role}.Sanitize())
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `DROP ROLE `+pgx.Identifier{role}.Sanitize())
		require.NoError(t, err)
	})
	// Upgrading an older broad grant must remove direct catalog authority.
	_, err = a.conn.Exec(ctx, `GRANT INSERT,UPDATE,DELETE ON sync.meta,sync.table_catalog TO `+pgx.Identifier{role}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, a.GrantTableRuntime(ctx, role))
	tablePublish(t, a, TablesAppend)
	runtimeCfg := pool.Config().Copy()
	runtimeCfg.ConnConfig.User = role
	runtimeCfg.ConnConfig.Password = "table_evolution-test-role"
	pair, err := oversync.NewReadinessPoolPair(ctx, runtimeCfg, 2)
	require.NoError(t, err)
	defer pair.Close()
	cfg, _ := TableEvolutionConfiguration(TablesAppend)
	cfg.Readiness.PoolPair = pair
	s, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer s.Close(ctx)
	require.Error(t, s.Bootstrap(ctx))
	// Provisioning installs the catalog; serving can now validate the same graph.
	provisioned := tableTestService(t, pool, TablesAppend)
	require.NoError(t, provisioned.Close(ctx))
	require.NoError(t, s.Bootstrap(ctx))
	for _, statement := range []string{
		`INSERT INTO sync.table_catalog SELECT 999,schema_name,'forged',sync_key_column,sync_key_kind,state_code,introduced_revision,retired_revision FROM sync.table_catalog LIMIT 1`,
		`UPDATE sync.meta SET catalog_revision=catalog_revision`,
		`UPDATE sync.meta SET catalog_fingerprint=catalog_fingerprint`,
		`DELETE FROM sync.meta WHERE false`,
		`UPDATE sync.table_catalog SET introduced_revision=introduced_revision`,
	} {
		_, err := pair.ExecutionPool().Exec(ctx, statement)
		var denied *pgconn.PgError
		require.ErrorAs(t, err, &denied, statement)
		require.Equal(t, "42501", denied.Code, statement)
	}
	for _, statement := range []string{`UPDATE app_evolution.table_run SET phase='populated'`, `DELETE FROM app_evolution.table_cleanup`, `ALTER TABLE main.people DISABLE TRIGGER aa_app_write_guard`, `INSERT INTO app_evolution.worker_transactions VALUES(pg_backend_pid(),pg_current_xact_id(),1,'11111111-1111-4111-8111-111111111111')`} {
		_, err := pair.ExecutionPool().Exec(ctx, statement)
		require.Error(t, err, statement)
	}
	// Revocation of direct table grants cannot neutralize column-level or
	// inherited authority; the grant transaction must reject those roles.
	for _, grant := range []string{
		`GRANT UPDATE(catalog_revision) ON sync.meta TO ` + pgx.Identifier{role}.Sanitize(),
		`GRANT INSERT ON sync.table_catalog TO PUBLIC`,
		`CREATE ROLE ` + pgx.Identifier{role + "_parent"}.Sanitize() + `; GRANT UPDATE ON sync.meta TO ` + pgx.Identifier{role + "_parent"}.Sanitize() + `; GRANT ` + pgx.Identifier{role + "_parent"}.Sanitize() + ` TO ` + pgx.Identifier{role}.Sanitize(),
	} {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, grant)
		require.NoError(t, err)
		require.ErrorContains(t, validateTableCatalogPrivileges(ctx, tx, role), "catalog provisioning privileges")
		require.NoError(t, tx.Rollback(ctx))
	}
}

func TestTableEvolution_TargetCatalogMismatchRollsBackAppend(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	proof, s := tableCleanupStart(t, pool, old, tableTestBounds())
	w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
	require.NoError(t, w.Cleanup(ctx))
	require.NoError(t, s.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesAppend))
	tablePublish(t, a, TablesAppend)
	cfg, _ := TableEvolutionConfiguration(TablesAppend)
	adapter := cfg.Readiness.Adapter.(tableAdapter)
	adapter.revision = 1
	cfg.Readiness.Adapter = adapter
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	defer pair.Close()
	cfg.Readiness.PoolPair = pair
	bad, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer bad.Close(ctx)
	err = bad.Bootstrap(ctx)
	var readiness *oversync.ReadinessError
	require.ErrorAs(t, err, &readiness)
	require.False(t, bad.Readiness().Ready)
	var revision, count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision,(SELECT count(*) FROM sync.table_catalog) FROM sync.meta`).Scan(&revision, &count))
	require.Equal(t, 1, revision)
	require.Equal(t, 2, count)
	correct := tableTestService(t, pool, TablesAppend)
	require.EqualValues(t, 2, correct.Readiness().CatalogRevision)
}

func TestTableEvolution_CleanupRejectsCaptureResidue(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	p := tablePushRequest(t, old, 1)
	_, err = old.PushOneShot(ctx, p.actor, p.proof, &p.request)
	require.NoError(t, err)
	_, s := tableCleanupStart(t, pool, old, tableTestBounds())
	_, err = pool.Exec(ctx, `INSERT INTO sync.bundle_capture_stage(txid,user_pk,table_id,op_code,key_bytes,payload_db) SELECT 1,user_pk,1,1,'\x01','{}'::jsonb FROM sync.user_state LIMIT 1`)
	require.NoError(t, err)
	w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
	require.Error(t, w.Cleanup(ctx))
	status, err := MaintenanceStatus(ctx, pool)
	require.NoError(t, err)
	require.False(t, status.AdmissionOpen)
}
