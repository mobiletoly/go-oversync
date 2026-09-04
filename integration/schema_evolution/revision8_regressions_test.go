package schemaevolution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvolution_WorkerTakeoverFencesLivePredecessor(t *testing.T) {
	for _, mode := range []string{"capacity_error", "capacity_success", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			pool, svc, _ := fixtureMaintaining(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			oldCtx, stopOld := context.WithCancel(ctx)
			defer stopOld()
			enteredOld, enteredNext := make(chan struct{}), make(chan struct{})
			releaseOld, releaseNext := make(chan struct{}), make(chan struct{})
			errOld, errNext := make(chan error, 1), make(chan error, 1)
			old, next := fixtureWorker(pool, svc), fixtureWorker(pool, svc)
			callbackErr := errors.New("predecessor capacity unavailable")
			old.Capacity = func(c context.Context) (Capacity, error) {
				close(enteredOld)
				select {
				case <-releaseOld:
				case <-ctx.Done():
					return Capacity{}, ctx.Err()
				}
				if mode == "capacity_error" {
					return Capacity{}, callbackErr
				}
				if mode == "cancelled" {
					return Capacity{}, c.Err()
				}
				value := fixtureDiskCapacity()
				value.Lag = 123 * time.Second
				return value, nil
			}
			next.Capacity = func(c context.Context) (Capacity, error) {
				select {
				case <-enteredNext:
				default:
					close(enteredNext)
				}
				select {
				case <-releaseNext:
					return fixtureDiskCapacity(), nil
				case <-c.Done():
					return Capacity{}, c.Err()
				}
			}
			go func() { errOld <- old.Run(oldCtx) }()
			select {
			case <-enteredOld:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var oldPID int32
			require.NoError(t, pool.QueryRow(ctx, `SELECT worker_pid FROM app_evolution.maintenance`).Scan(&oldPID))
			var terminated bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, oldPID).Scan(&terminated))
			require.True(t, terminated)
			go func() { errNext <- next.Run(ctx) }()
			select {
			case <-enteredNext:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// B is running but blocked before its capacity write, so all of its metadata
			// must remain byte-equivalent while A returns and runs deferred cleanup.
			snapshot := func() []byte {
				var v []byte
				require.NoError(t, pool.QueryRow(ctx, `SELECT jsonb_build_object('job',(SELECT to_jsonb(x) FROM app_evolution.jobs x),'maintenance',(SELECT to_jsonb(x) FROM app_evolution.maintenance x),'batches',(SELECT jsonb_agg(x) FROM app_evolution.batches x),'errors',(SELECT jsonb_agg(x) FROM app_evolution.row_work x))`).Scan(&v))
				return v
			}
			before := snapshot()
			if mode == "cancelled" {
				stopOld()
			}
			close(releaseOld)
			select {
			case err := <-errOld:
				if mode == "capacity_error" {
					require.ErrorIs(t, err, callbackErr)
				} else if mode == "cancelled" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorContains(t, err, "fenced")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.JSONEq(t, string(before), string(snapshot()))
			close(releaseNext)
			select {
			case err := <-errNext:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			revision7AssertCapacity(t, pool, next.Bounds)
			var state string
			var changed, completed int
			require.NoError(t, pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM main.upgrade_people WHERE name IS NOT NULL),(SELECT count(*) FROM app_evolution.batches WHERE state='completed') FROM app_evolution.jobs`).Scan(&state, &changed, &completed))
			require.Equal(t, "traversed", state)
			require.Equal(t, 1, changed)
			require.Equal(t, 1, completed)
		})
	}
}

func TestEvolution_WorkerMetadataSerializesWithTakeover(t *testing.T) {
	pool, svc, _ := fixtureMaintaining(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	oldLock, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(oldLock, workerLock)
	old, err := claimWorker(ctx, oldLock)
	require.NoError(t, err)
	oldCtx := context.WithValue(ctx, migrationContextKey{}, old)
	gate, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer gate.Release()
	_, err = gate.Exec(ctx, `SELECT pg_advisory_lock(1869833075,19)`)
	require.NoError(t, err)
	defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(1869833075,19)`)
	_, err = pool.Exec(ctx, `CREATE FUNCTION app_evolution.test_metadata_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(1869833075,19); RETURN NEW; END $$; CREATE TRIGGER test_metadata_gate BEFORE UPDATE ON app_evolution.jobs FOR EACH ROW EXECUTE FUNCTION app_evolution.test_metadata_gate()`)
	require.NoError(t, err)
	w := fixtureWorker(pool, svc)
	metadataDone := make(chan error, 1)
	go func() {
		metadataDone <- w.updateEvidence(oldCtx, `UPDATE app_evolution.jobs SET retries=retries+1 WHERE job=$1`, JobID)
	}()
	// The real metadata UPDATE is blocked after admission and ownership validation.
	require.Eventually(t, func() bool {
		var blocked bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks wait JOIN pg_locks held ON held.pid=wait.pid WHERE wait.locktype='advisory' AND wait.classid=1869833075 AND wait.objid=19 AND NOT wait.granted AND held.locktype='advisory' AND held.classid=1869833075 AND held.objid=9 AND held.granted AND held.mode='ShareLock')`).Scan(&blocked)
		return err == nil && blocked
	}, time.Second, 10*time.Millisecond)
	var terminated bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, old.pid).Scan(&terminated))
	require.True(t, terminated)
	nextLock, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(nextLock, workerLock)
	claimed := make(chan error, 1)
	go func() { _, err := claimWorker(ctx, nextLock); claimed <- err }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND classid=1869833075 AND objid=9 AND NOT granted)`, int32(nextLock.Conn().PgConn().PID())).Scan(&waiting)
		return err == nil && waiting
	}, time.Second, 10*time.Millisecond)
	_, err = gate.Exec(ctx, `SELECT pg_advisory_unlock(1869833075,19)`)
	require.NoError(t, err)
	select {
	case err := <-metadataDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-claimed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.ErrorContains(t, w.updateEvidence(oldCtx, `UPDATE app_evolution.jobs SET retries=retries+1 WHERE job=$1`, JobID), "fenced")
	require.ErrorContains(t, w.updateEvidence(ctx, `UPDATE app_evolution.jobs SET retries=retries+1 WHERE job=$1`, JobID), "worker_required")
	var retries int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT retries FROM app_evolution.jobs`).Scan(&retries))
	require.EqualValues(t, 1, retries)
}

func TestEvolution_WorkerCancelledOwnerRetainsCleanupCapability(t *testing.T) {
	pool, svc, _ := fixtureMaintaining(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	w := fixtureWorker(pool, svc)
	w.Capacity = func(context.Context) (Capacity, error) { stop(); return Capacity{}, context.Canceled }
	require.ErrorIs(t, w.Run(runCtx), context.Canceled)
	var state, reason string
	var duration int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT state,pause_reason,duration_ns FROM app_evolution.jobs`).Scan(&state, &reason, &duration))
	require.Equal(t, "paused", state)
	require.Equal(t, "capacity", reason)
	require.Positive(t, duration)
	revision7AssertCapacity(t, pool, w.Bounds)
}
