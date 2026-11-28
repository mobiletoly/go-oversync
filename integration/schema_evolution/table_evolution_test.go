package schemaevolution

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func tableTestBounds() TableBounds {
	b := DefaultBounds()
	b.Deadline = time.Now().UTC().Add(2 * time.Minute)
	b.MaxEvidenceBytes = 8 << 20
	b.MaxHistoryBytes = 8 << 20
	b.MaxBundles = 100
	b.MinFreeDiskBytes = 1
	b.Rows = 2
	return TableBounds{Bounds: b, CleanupSessions: 2, CleanupRows: 2, CleanupBytes: 8192, MaxCleanupBatches: 100}
}
func tableTestCapacity(context.Context) (Capacity, error) {
	return Capacity{Available: true, FreeDiskBytes: 1 << 30, RetentionRemaining: 48 * time.Hour}, nil
}

func tableTestService(t *testing.T, pool *pgxpool.Pool, phase TablePhase) *oversync.SyncService {
	t.Helper()
	ctx := context.Background()
	cfg, err := TableEvolutionConfiguration(phase)
	require.NoError(t, err)
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	t.Cleanup(pair.Close)
	cfg.Readiness.PoolPair = pair
	s, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, s.Bootstrap(ctx))
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func tablePublish(t *testing.T, a *Authority, phase TablePhase) {
	t.Helper()
	t.Cleanup(a.Close)
	cfg, err := TableEvolutionConfiguration(phase)
	require.NoError(t, err)
	require.NoError(t, a.Publish(context.Background(), cfg))
	a.Close()
}

func tableBaseline(t *testing.T) (*pgxpool.Pool, *oversync.SyncService) {
	t.Helper()
	ctx := context.Background()
	pool := fixturePool(t)
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesBaseline))
	tablePublish(t, a, TablesBaseline)
	s := tableTestService(t, pool, TablesBaseline)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{s}))
	return pool, s
}

func tableCleanupStart(t *testing.T, pool *pgxpool.Pool, s *oversync.SyncService, b TableBounds) (DrainProof, *oversync.SyncService) {
	t.Helper()
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{s}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.RequirePreservingClients(ctx, "table_evolution-preserving-v1"))
	capacity, _ := tableTestCapacity(ctx)
	require.NoError(t, a.ActivateTableEvolution(ctx, uuid.New(), b, capacity))
	tablePublish(t, a, TablesCleanup)
	return proof, tableTestService(t, pool, TablesCleanup)
}

func tableBackfillStart(t *testing.T, pool *pgxpool.Pool, proof DrainProof, cleanup *oversync.SyncService) *oversync.SyncService {
	t.Helper()
	ctx := context.Background()
	w := &TableWorker{Pool: pool, Service: cleanup, Capacity: tableTestCapacity}
	require.NoError(t, w.Cleanup(ctx))
	require.NoError(t, cleanup.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesAppend))
	tablePublish(t, a, TablesAppend)
	appendRuntime := tableTestService(t, pool, TablesAppend)
	require.Error(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{appendRuntime}))
	require.NoError(t, appendRuntime.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesBackfill))
	tablePublish(t, a, TablesBackfill)
	return tableTestService(t, pool, TablesBackfill)
}

func TestTableEvolution_ExistingChildNewParentLifecycle(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, "scope")
	require.NoError(t, err)
	keys := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000000"), uuid.New(), uuid.New()}
	for _, key := range keys {
		require.NoError(t, fixtureWrite(t, old, "scope", `INSERT INTO main.people(_sync_scope_id,id,name) VALUES($1,$2,'Person')`, "scope", key))
	}
	proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	w := &TableWorker{Pool: pool, Service: cleanup, Capacity: tableTestCapacity}
	require.NoError(t, w.Cleanup(ctx))
	require.NoError(t, cleanup.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesAppend))
	tablePublish(t, a, TablesAppend)
	// Reproduce the reviewed P1 direction, then remove only this test's rejected DDL.
	_, err = pool.Exec(ctx, `ALTER TABLE main.people ADD CONSTRAINT people_project FOREIGN KEY(_sync_scope_id,project_id) REFERENCES main.projects(_sync_scope_id,id) DEFERRABLE`)
	require.NoError(t, err)
	cfg, err := TableEvolutionConfiguration(TablesAppend)
	require.NoError(t, err)
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	defer pair.Close()
	cfg.Readiness.PoolPair = pair
	// Exclude application inventory rejection to exercise the core's old-set closure.
	cfg.Readiness.Adapter = tablePermissiveInventory{tableAdapter: cfg.Readiness.Adapter.(tableAdapter)}
	bad, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.ErrorContains(t, bad.Bootstrap(ctx), "not FK-closed")
	require.NoError(t, bad.Close(ctx))
	_, err = pool.Exec(ctx, `ALTER TABLE main.people DROP CONSTRAINT people_project`)
	require.NoError(t, err)
	appendRuntime := tableTestService(t, pool, TablesAppend)
	require.EqualValues(t, 2, appendRuntime.Readiness().CatalogRevision)
	require.NoError(t, appendRuntime.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.ApplyTableEvolution(ctx, TablesBackfill))
	tablePublish(t, a, TablesBackfill)
	backfill := tableTestService(t, pool, TablesBackfill)
	w.Service = backfill
	require.NoError(t, w.Populate(ctx))
	require.NoError(t, backfill.Close(ctx))
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	tablePublish(t, a, TablesServing)
	final := tableTestService(t, pool, TablesServing)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{final}))
	var people, projects int
	require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM main.people WHERE project_id=$1::uuid),(SELECT count(*) FROM main.projects)`, TableProjectID).Scan(&people, &projects))
	require.Equal(t, 3, people)
	require.Equal(t, 1, projects)
	var ids []string
	rows, err := pool.Query(ctx, `SELECT table_id::text||':'||table_name FROM sync.table_catalog ORDER BY table_id`)
	require.NoError(t, err)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	rows.Close()
	require.Equal(t, []string{"1:legacy_notes", "2:people", "3:attachments", "4:projects"}, ids)
	// A late old INSERT leaves the new FK NULL; UPDATE omission preserves assignment.
	late := uuid.New()
	require.NoError(t, fixtureWrite(t, final, "scope", `INSERT INTO main.people(_sync_scope_id,id,name) VALUES('scope',$1,'Late')`, late))
	require.NoError(t, fixtureWrite(t, final, "scope", `UPDATE main.people SET name='Changed' WHERE id=$1`, keys[0]))
	var value *string
	require.NoError(t, pool.QueryRow(ctx, `SELECT project_id::text FROM main.people WHERE id=$1`, late).Scan(&value))
	require.Nil(t, value)
	require.NoError(t, pool.QueryRow(ctx, `SELECT project_id::text FROM main.people WHERE id=$1`, keys[0]).Scan(&value))
	require.Equal(t, TableProjectID, *value)
}

// This test adapter preserves valid deployment facts but omits application graph
// inspection, demonstrating that core FK validation cannot be overridden by evidence.
type tablePermissiveInventory struct{ tableAdapter }

func (a tablePermissiveInventory) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var e oversync.ReadinessEvidence
	if err := r.QueryRow(ctx, `SELECT contract,generations FROM app_evolution.deployment WHERE singleton`).Scan(&e.Deployment, &e.Generations); err != nil {
		return e, err
	}
	for _, m := range e.Deployment.Migrations {
		var v oversync.MigrationEvidence
		v.Identity = m
		if err := r.QueryRow(ctx, `SELECT state,started_at,completed_at FROM app_evolution.migrations WHERE version=$1`, m.Version).Scan(&v.State, &v.StartedAt, &v.CompletedAt); err != nil {
			return e, err
		}
		e.Migrations = append(e.Migrations, v)
	}
	e.Mechanisms = append([]oversync.MechanismIdentity(nil), e.Deployment.Mechanisms...)
	return e, nil
}

func TestTableEvolution_CoreRejectsUnvalidatedFK(t *testing.T) {
	ctx := context.Background()
	pool, old := tableBaseline(t)
	proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	backfill := tableBackfillStart(t, pool, proof, cleanup)
	require.NoError(t, backfill.Close(ctx))
	_, err := pool.Exec(ctx, `ALTER TABLE main.people DROP CONSTRAINT people_project; ALTER TABLE main.people ADD CONSTRAINT people_project FOREIGN KEY(_sync_scope_id,project_id) REFERENCES main.projects(_sync_scope_id,id) DEFERRABLE NOT VALID`)
	require.NoError(t, err)
	cfg, err := TableEvolutionConfiguration(TablesBackfill)
	require.NoError(t, err)
	cfg.Readiness.Adapter = tablePermissiveInventory{tableAdapter: cfg.Readiness.Adapter.(tableAdapter)}
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	defer pair.Close()
	cfg.Readiness.PoolPair = pair
	s, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer s.Close(ctx)
	require.ErrorContains(t, s.Bootstrap(ctx), "unvalidated FK")
}

func TestTableEvolution_ConfigurationKeepsOldProof(t *testing.T) {
	old, err := TableEvolutionConfiguration(TablesBaseline)
	require.NoError(t, err)
	target, err := TableEvolutionConfiguration(TablesServing)
	require.NoError(t, err)
	require.Equal(t, old.Readiness.Generations[0].Proof, target.Readiness.Generations[0].Proof)
	b, _ := json.Marshal(target.SchemaGenerations[1])
	require.NotContains(t, string(b), "legacy_notes")
}
