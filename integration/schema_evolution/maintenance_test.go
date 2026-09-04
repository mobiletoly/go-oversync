package schemaevolution

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_MaintenanceRejectsEveryOrdinaryEntry(t *testing.T) {
	pool, service, id := fixtureMaintaining(t)
	ctx := context.Background()
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	actor := oversync.Actor{UserID: "unknown", SourceID: "stale"}
	_, err = service.Connect(ctx, actor, cfg.Readiness.Generations[0].Proof, &oversync.ConnectRequest{})
	require.Error(t, err)
	require.Error(t, fixtureWrite(t, service, "scope", `UPDATE main.upgrade_people SET legacy_name='Wrong|Value' WHERE id=$1`, id))
	_, err = CreateScope(ctx, pool, "unexpected")
	require.Error(t, err)
	for _, statement := range []string{`UPDATE main.upgrade_people SET legacy_name='Wrong|Value'`, `DELETE FROM main.upgrade_people`, `TRUNCATE main.upgrade_people`} {
		_, err = pool.Exec(ctx, statement)
		require.ErrorContains(t, err, "application_admission_closed")
	}
	reached := false
	handler := service.ReadinessGuard(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))
	for _, path := range []string{"connect", "generations/1", "push", "pull", "watch", "snapshot-sessions"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/sync/"+path, nil))
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
	}
	require.False(t, reached)
	var users int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.user_state WHERE user_id='unknown'`).Scan(&users))
	require.Zero(t, users)
}

func TestEvolution_DrainFailurePersistsClosureBeforeCallback(t *testing.T) {
	pool, service, id := fixtureMaintaining(t)
	service = fixtureCoexistence(t, pool, service)
	ctx := context.Background()
	cfg, err := Configuration(4, 3, true)
	require.NoError(t, err)
	stale := bootstrapFixture(t, pool, cfg)
	_, err = Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return errors.New("writer stop failed") })
	require.ErrorContains(t, err, "writer stop failed")
	var requested bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT requested FROM app_evolution.maintenance`).Scan(&requested))
	require.True(t, requested)
	require.Error(t, fixtureWrite(t, stale, "scope", `UPDATE main.upgrade_people SET name='Wrong' WHERE id=$1`, id))
	// Even a fresh process has no permission to reopen a failed, undrained epoch.
	pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
	require.NoError(t, err)
	defer pair.Close()
	cfg.Readiness.PoolPair = pair
	fresh, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, nil)
	require.NoError(t, err)
	defer fresh.Close(ctx)
	require.Error(t, fresh.Bootstrap(ctx))
	require.Error(t, ReopenServing(ctx, pool, []*oversync.SyncService{fresh}))
}

func TestEvolution_AdmissionLeaseDrainAndWorkerTakeover(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	oldLock, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	old, err := claimWorker(ctx, oldLock)
	require.NoError(t, err)
	held, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer held.Rollback(ctx)
	require.NoError(t, lockAdmission(ctx, held, false))
	releaseSessionLock(oldLock, workerLock)
	nextLock, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(nextLock, workerLock)
	finished := make(chan error, 1)
	go func() { _, err := claimWorker(ctx, nextLock); finished <- err }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND objid=9 AND NOT granted)`, int32(nextLock.Conn().PgConn().PID())).Scan(&waiting)
		return err == nil && waiting
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, held.Commit(ctx))
	require.NoError(t, <-finished)
	require.ErrorContains(t, verifyWorker(ctx, pool, old), "fenced")
	// The obsolete capability cannot enter even the private Go write path.
	manager := oversync.NewScopeManager(service, oversync.ScopeManagerConfig{})
	_, err = manager.ExecWrite(context.WithValue(ctx, migrationContextKey{}, old), "scope", oversync.ScopeWriteOptions{WriterID: "schema-evolution-backfill", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{1}, OperationValidUntil: time.Now().Add(time.Hour)}}, func(context.Context, oversync.DatabaseWriteTx) error { return nil })
	require.Error(t, err)
}

func TestEvolution_CensusRejectsUnaccountedPhysicalPopulation(t *testing.T) {
	pool, _, _ := fixtureMaintaining(t)
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	// A privileged restored census mismatch must fail even if the row invariant is valid.
	_, err = tx.Exec(ctx, `DELETE FROM app_evolution.census_rows`)
	require.NoError(t, err)
	require.ErrorContains(t, verifyCensus(ctx, tx), "census_changed")
}

func TestEvolution_CensusCoversPreexistingRowsAndUninitializedEmptyScopes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := fixturePool(t)
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Apply(ctx, 1))
	// Populate the application's database before the SDK's first Bootstrap.
	// This fixture is not backfill: no managed capture mechanism exists yet.
	lock, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	capability, err := claimWorker(ctx, lock)
	require.NoError(t, err)
	seedCtx := context.WithValue(ctx, migrationContextKey{}, capability)
	err = pgx.BeginTxFunc(seedCtx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := permitWorkerTransaction(seedCtx, tx); err != nil {
			return err
		}
		for _, scope := range []string{"physical", "empty", "deleted"} {
			if _, err := tx.Exec(seedCtx, `INSERT INTO app_evolution.scope_inventory(scope_id,incarnation,deleted) VALUES($1,$2,$3)`, scope, uuid.New(), scope == "deleted"); err != nil {
				return err
			}
		}
		_, err := tx.Exec(seedCtx, `INSERT INTO main.upgrade_people VALUES('physical',$1,'Imported|Row')`, uuid.New())
		return err
	})
	require.NoError(t, err)
	releaseSessionLock(lock, workerLock)
	initial, err := Configuration(1, 1, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, initial))
	a.Close()
	service := bootstrapFixture(t, pool, initial)
	var initialized int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.scope_state WHERE state_code=2`).Scan(&initialized))
	require.Equal(t, 1, initialized) // Bootstrap adopts the preexisting physical scope before the migration.
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	require.NoError(t, a.Apply(ctx, 3))
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	require.NoError(t, a.Activate(ctx, fixtureBounds(), fixtureDiskCapacity()))
	a.Close()
	service = bootstrapFixture(t, pool, cfg)
	require.NoError(t, fixtureWorker(pool, service).Run(ctx))
	var scopes, rows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.census_scopes`).Scan(&scopes))
	require.Equal(t, 3, scopes)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.census_rows`).Scan(&rows))
	require.Equal(t, 1, rows)
	var name, label string
	require.NoError(t, pool.QueryRow(ctx, `SELECT name,new_label FROM main.upgrade_people`).Scan(&name, &label))
	require.Equal(t, []string{"Imported", "Row"}, []string{name, label})
	require.NoError(t, verifyCensus(ctx, pool))
}
