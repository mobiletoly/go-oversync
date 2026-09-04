package oversync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestReadiness_ConcurrentIndexSnapshotOverlap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	var major int
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer/10000`).Scan(&major))
	require.Equal(t, 16, major, "transition lock qualification requires PostgreSQL 16")
	_, err := pool.Exec(ctx, `CREATE SCHEMA readiness_index; CREATE TABLE readiness_index.people(_sync_scope_id text NOT NULL,id uuid NOT NULL,name text NOT NULL,PRIMARY KEY(_sync_scope_id,id))`)
	require.NoError(t, err)
	tables := []RegisteredTable{{Schema: "readiness_index", Table: "people", SyncKeyColumns: []string{"id"}}}
	generations := []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{Schema: "readiness_index", Table: "people", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}}}}}}
	service, err := newTestRuntimeService(pool, &ServiceConfig{RegisteredTables: tables, SchemaGenerations: generations, RegisteredTableCatalogRevision: 1}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	authority, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer authority.Release()
	_, err = authority.Exec(ctx, `SELECT pg_advisory_lock(1869833075,6)`)
	require.NoError(t, err)
	defer authority.Exec(context.Background(), `SELECT pg_advisory_unlock(1869833075,6)`)
	reader, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	defer reader.Rollback(context.Background())
	var count int64
	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM readiness_index.people`).Scan(&count))
	indexResult := make(chan error, 1)
	go func() {
		_, err := authority.Exec(ctx, `CREATE INDEX CONCURRENTLY readiness_people_name_idx ON readiness_index.people(name)`)
		indexResult <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_progress_create_index WHERE relid='readiness_index.people'::regclass AND phase='waiting for old snapshots')`).Scan(&waiting)
		return err == nil && waiting
	}, 3*time.Second, 10*time.Millisecond, "must exercise the old-snapshot boundary, not probabilistic overlap")
	contended := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	service.readinessContentionHook = func() { once.Do(func() { close(contended); <-resume }) }
	bootstrapResult := make(chan error, 1)
	started := time.Now()
	go func() { bootstrapResult <- service.Bootstrap(ctx) }()
	select {
	case <-contended:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, service.Readiness().Ready)
	// At the post-cleanup/pre-backoff boundary, only the deliberately retained
	// reader may own a snapshot; Bootstrap has no waiting transaction or portal.
	var snapshots int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND pid<>$1 AND pid<>$2 AND (xact_start IS NOT NULL OR backend_xmin IS NOT NULL)`, reader.Conn().PgConn().PID(), authority.Conn().PgConn().PID()).Scan(&snapshots))
	require.Zero(t, snapshots)
	require.NoError(t, reader.Commit(ctx))
	select {
	case err := <-indexResult:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var valid bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid='readiness_index.readiness_people_name_idx'::regclass`).Scan(&valid))
	require.True(t, valid)
	_, err = authority.Exec(ctx, `SELECT pg_advisory_unlock(1869833075,6)`)
	require.NoError(t, err)
	close(resume)
	select {
	case err := <-bootstrapResult:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Less(t, time.Since(started), 10*time.Second)
	require.True(t, service.Readiness().Ready)
}

func TestReadiness_LockBudgetAndCancellationCleanup(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget", true: "cancel"}[cancelled], func(t *testing.T) {
			ctx := context.Background()
			pool := newIntegrationTestPool(t, ctx)
			cfg := &ServiceConfig{RegisteredTables: runtimeServiceTestRegisteredTables(), SchemaGenerations: testSchemaGenerationsForRegisteredTables(runtimeServiceTestRegisteredTables()), RegisteredTableCatalogRevision: 1}
			service, err := newTestRuntimeService(pool, cfg, nil)
			require.NoError(t, err)
			service.config.Readiness.LockTimeout = 100 * time.Millisecond
			authority, err := pool.Acquire(ctx)
			require.NoError(t, err)
			_, err = authority.Exec(ctx, `SELECT pg_advisory_lock(1869833075,6)`)
			require.NoError(t, err)
			bounded, cancel := context.WithCancel(ctx)
			if cancelled {
				service.readinessContentionHook = cancel
			}
			defer cancel()
			require.Error(t, service.Bootstrap(bounded))
			require.False(t, service.Readiness().Ready)
			_, err = authority.Exec(ctx, `SELECT pg_advisory_unlock(1869833075,6)`)
			require.NoError(t, err)
			authority.Release()
			require.Eventually(t, func() bool { return pool.Stat().AcquiredConns() == 0 }, time.Second, 10*time.Millisecond)
			require.NoError(t, service.Close(ctx))
		})
	}
}
