package schemaevolution

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestTableEvolution_ProcessHelper(t *testing.T) {
	dsn := os.Getenv("TABLE_EVOLUTION_DATABASE")
	if dsn == "" {
		t.Skip("process entry")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	phase := TablesBackfill
	boundary := os.Getenv("TABLE_EVOLUTION_BOUNDARY")
	if strings.HasPrefix(boundary, "cleanup") {
		phase = TablesCleanup
	}
	s := tableTestService(t, pool, phase)
	w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity, boundary: func(point string) {
		if point == boundary {
			require.NoError(t, os.WriteFile(os.Getenv("TABLE_EVOLUTION_MARKER"), []byte(point), 0600))
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}
	}}
	if phase == TablesCleanup {
		require.NoError(t, w.Cleanup(ctx))
	} else {
		require.NoError(t, w.Populate(ctx))
	}
	t.Fatal("boundary not reached")
}
func tableKillHelper(t *testing.T, pool *pgxpool.Pool, boundary string) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "boundary")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTableEvolution_ProcessHelper$")
	command.Env = append(os.Environ(), "TABLE_EVOLUTION_DATABASE="+pool.Config().ConnString(), "TABLE_EVOLUTION_BOUNDARY="+boundary, "TABLE_EVOLUTION_MARKER="+marker)
	out, err := command.CombinedOutput()
	require.Error(t, err, string(out))
	require.NoError(t, ctx.Err(), string(out))
	value, err := os.ReadFile(marker)
	require.NoError(t, err, string(out))
	require.Equal(t, boundary, string(value))
}
func TestTableEvolution_ProcessDeathRecovery(t *testing.T) {
	for _, boundary := range []string{"cleanup_before_commit", "cleanup_after_commit", "population_after_prepare", "population_inside_apply", "population_after_commit", "population_inside_noop"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			pool, old := tableBaseline(t)
			_, err := CreateScope(ctx, pool, "scope")
			require.NoError(t, err)
			for i := 0; i < 2; i++ {
				require.NoError(t, fixtureWrite(t, old, "scope", `INSERT INTO main.people VALUES('scope',$1,'Person')`, uuid.New()))
			}
			if strings.HasPrefix(boundary, "cleanup") {
				p := tablePushRequest(t, old, 1)
				_, err = old.PushOneShot(ctx, p.actor, p.proof, &p.request)
				require.NoError(t, err)
				_, err = old.CreateSnapshotSession(ctx, p.actor, p.proof)
				require.NoError(t, err)
			}
			proof, s := tableCleanupStart(t, pool, old, tableTestBounds())
			var initial int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_log`).Scan(&initial))
			if strings.HasPrefix(boundary, "cleanup") {
				_, err = pool.Exec(ctx, `UPDATE sync.snapshot_sessions SET expires_at=clock_timestamp()-interval '1 second'`)
				require.NoError(t, err)
			} else {
				s = tableBackfillStart(t, pool, proof, s)
			}
			tableKillHelper(t, pool, boundary)
			w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
			if strings.HasPrefix(boundary, "cleanup") {
				require.NoError(t, w.Cleanup(ctx))
				require.NoError(t, w.Cleanup(ctx))
				var snapshots int
				require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.snapshot_sessions`).Scan(&snapshots))
				require.Zero(t, snapshots)
			} else {
				require.NoError(t, w.Populate(ctx))
				require.NoError(t, w.Populate(ctx))
				var final, pending int
				require.NoError(t, pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM sync.bundle_log),(SELECT count(*) FROM app_evolution.table_batches WHERE state<>'applied')`).Scan(&final, &pending))
				require.Equal(t, initial+1, final)
				require.Zero(t, pending)
			}
		})
	}
}
