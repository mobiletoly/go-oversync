package schemaevolution

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_UnexpectedSourceChangesFailClosed(t *testing.T) {
	for _, values := range [][]string{{"A|B"}, {"A|B", "C|D"}} {
		t.Run(strings.Join(values, ","), func(t *testing.T) {
			pool, service, _ := fixtureMaintainingRows(t, values)
			ctx := fixtureWorkerContext(t, pool)
			worker := fixtureWorker(pool, service)
			require.NoError(t, worker.resumeJob(ctx))
			d, err := worker.prepareNext(ctx)
			require.NoError(t, err)
			// A historical pending descriptor whose prepared version predates the current
			// still-invalid row. Its exact bytes are installed before any apply attempt.
			d.Rows[0].Version = "0"
			encoded, err := d.CanonicalBytes()
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `UPDATE app_evolution.batches SET spec=$2,descriptor=$3,hash=$4 WHERE operation_id=$1`, d.OperationID, d, encoded, digest(encoded))
			require.NoError(t, err)
			done, err := worker.step(ctx)
			require.ErrorContains(t, err, "selected_source_changed")
			require.False(t, done)
			var pending, changed int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.batches WHERE state='pending'`).Scan(&pending))
			require.Equal(t, 1, pending)
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL`).Scan(&changed))
			require.Zero(t, changed)

		})
	}
}

func TestEvolution_OversizedRowAndUnavailableCapacityRemainUnresolved(t *testing.T) {
	pool, service, _ := fixtureMaintainingRows(t, []string{strings.Repeat("A", 1<<20) + "|B"})
	worker := fixtureWorker(pool, service)
	require.ErrorContains(t, worker.Run(context.Background()), "oversized_row")
	stats, err := worker.Telemetry(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Failed)
	require.EqualValues(t, 1, stats.Unresolved)
	require.Zero(t, stats.Changed)
	worker.Capacity = func(context.Context) (Capacity, error) {
		return Capacity{FreeDiskBytes: 1 << 30, Available: true, BundlesPerSecond: 1, RetentionRemaining: 7 * 24 * time.Hour}, nil
	}
	require.ErrorContains(t, worker.Run(context.Background()), "capacity")
}

func TestEvolution_InitializingScopeRetriesThenPauses(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	// Persist a valid core initializing-state fixture, including its lease fields.
	_, err := pool.Exec(ctx, `UPDATE sync.scope_state SET state_code=1,initializer_source_id='initializing-test',initialization_id='00000000-0000-4000-8000-000000000001',lease_expires_at=clock_timestamp()+interval '1 hour',initialized_at=NULL,initialized_by_source_id=NULL`)
	require.NoError(t, err)
	worker := fixtureWorker(pool, service)
	var initializing *oversync.ScopeInitializingError
	require.ErrorAs(t, worker.Run(ctx), &initializing)
	stats, err := worker.Telemetry(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.Retries)
	require.Equal(t, "paused", stats.State)
	require.Zero(t, stats.Changed)
	validation, err := validateAllRows(ctx, pool)
	require.NoError(t, err)
	require.Positive(t, validation.Violations)
	_, err = pool.Exec(ctx, `UPDATE sync.scope_state SET state_code=0,initializer_source_id=NULL,initialization_id=NULL,lease_expires_at=NULL`)
	require.NoError(t, err)
	require.NoError(t, worker.Run(ctx))
}

func TestEvolution_FinalizationLimitRollsBackApplicationCheckpoint(t *testing.T) {
	pool, service, _ := fixtureMaintainingRows(t, []string{"A|B", "C|D"})
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	cfg.MaxRowsPerBundle = 1
	service = bootstrapFixture(t, pool, cfg)
	require.Error(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	worker := fixtureWorker(pool, service)
	require.Error(t, worker.Run(ctx))
	var changed, checkpoints, receipts int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL`).Scan(&changed))
	require.Zero(t, changed)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.batches WHERE state<>'pending'`).Scan(&checkpoints))
	require.Zero(t, checkpoints)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.scope_write_receipts WHERE writer_id='schema-evolution-backfill'`).Scan(&receipts))
	require.Zero(t, receipts)
	stats, err := worker.Telemetry(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Changed)
	require.Equal(t, "paused", stats.State)
}
