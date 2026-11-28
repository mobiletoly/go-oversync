package schemaevolution

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_ProcessHelper(t *testing.T) {
	dsn := os.Getenv("TRANSITION_HELPER_DATABASE_URL")
	if dsn == "" {
		t.Skip("subprocess entry point")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	service := bootstrapFixture(t, pool, cfg)
	worker := fixtureWorker(pool, service)
	worker.boundary = func(boundary string) {
		if boundary == os.Getenv("TRANSITION_HELPER_BOUNDARY") {
			require.NoError(t, os.WriteFile(os.Getenv("TRANSITION_HELPER_MARKER"), []byte(boundary), 0600))
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}
	}
	require.NoError(t, worker.Run(ctx))
	t.Fatal("process boundary not reached")
}

func TestEvolution_ProcessDeathRecovery(t *testing.T) {
	for _, boundary := range []string{"before_descriptor", "after_descriptor", "before_apply", "inside_callback", "after_commit", "inside_noop_followup"} {
		t.Run(boundary, func(t *testing.T) {
			pool, service, id := fixtureMaintaining(t)
			if boundary == "inside_noop_followup" {
				require.NoError(t, fixtureAuthorizedWrite(t, pool, service, "scope", `UPDATE main.upgrade_people SET legacy_name='A|B' WHERE id=$1`, id))
			}
			var initial int64
			require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM sync.bundle_log`).Scan(&initial))
			marker := filepath.Join(t.TempDir(), "boundary")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEvolution_ProcessHelper$")
			command.Env = append(os.Environ(), "TRANSITION_HELPER_DATABASE_URL="+pool.Config().ConnString(), "TRANSITION_HELPER_BOUNDARY="+boundary, "TRANSITION_HELPER_MARKER="+marker)
			output, err := command.CombinedOutput()
			require.Error(t, err, "%s", output)
			require.NoError(t, ctx.Err(), "helper must die at boundary, not timeout")
			value, err := os.ReadFile(marker)
			require.NoError(t, err, "%s", output)
			require.Equal(t, boundary, string(value))
			require.NoError(t, fixtureWorker(pool, service).Run(context.Background()))
			var final int64
			require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM sync.bundle_log`).Scan(&final))
			expected := initial + 1
			if boundary == "inside_noop_followup" {
				expected = initial
			}
			require.Equal(t, expected, final, "restart must neither duplicate nor lose effects")
			var pending int
			require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM app_evolution.batches WHERE state='pending'`).Scan(&pending))
			require.Zero(t, pending)
			var phase string
			require.NoError(t, pool.QueryRow(context.Background(), `SELECT state FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&phase))
			require.Equal(t, "traversed", phase)
		})
	}
}

func TestEvolution_NoopAndRequiredEffects(t *testing.T) {
	pool, service, id := fixtureMaintaining(t)
	ctx := context.Background()
	require.NoError(t, fixtureAuthorizedWrite(t, pool, service, "scope", `UPDATE main.upgrade_people SET legacy_name='A|B' WHERE id=$1`, id))
	worker := fixtureWorker(pool, service)
	require.NoError(t, worker.Run(ctx))
	var changed, skipped int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT changed,skipped FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&changed, &skipped))
	require.Zero(t, changed)
	require.EqualValues(t, 1, skipped)
	var d BatchDescriptor
	require.NoError(t, pool.QueryRow(ctx, `SELECT spec FROM app_evolution.batches ORDER BY ordinal LIMIT 1`).Scan(&d))
	require.Empty(t, d.Required)
	opts, err := d.WriteOptions()
	require.NoError(t, err)
	ctx = fixtureWorkerContext(t, pool)
	opts.RequiredEffectTables = []oversync.RegisteredEffectTable{{Schema: "main", Table: "upgrade_people"}}
	_, err = worker.Manager.ExecWrite(ctx, "scope", opts, func(context.Context, oversync.DatabaseWriteTx) error { return nil })
	var violation *oversync.RetryableCallbackViolationError
	require.ErrorAs(t, err, &violation)
}
