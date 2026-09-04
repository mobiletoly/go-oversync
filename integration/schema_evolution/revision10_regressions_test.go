package schemaevolution

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_CensusRejectsPopulationPastFrozenWatermark(t *testing.T) {
	b := fixtureBounds()
	b.Rows = 1
	b.ScopePage = 1
	pool, service, _ := fixtureMaintainingBounds(t, []string{"A|B"}, nil, b)
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	_, err = a.conn.Exec(ctx, `DELETE FROM app_evolution.census_progress; DELETE FROM app_evolution.jobs; DELETE FROM app_evolution.census_rows; DELETE FROM app_evolution.census_scopes`)
	require.NoError(t, err)
	require.NoError(t, a.initializeCensus(ctx, b))
	done, err := a.captureCensusPage(ctx, b)
	require.NoError(t, err)
	require.False(t, done)

	lateKey := uuid.New()
	_, err = a.conn.Exec(ctx, `SET session_replication_role=replica; INSERT INTO app_evolution.scope_inventory(scope_id,incarnation) VALUES('late',gen_random_uuid())`)
	require.NoError(t, err)
	_, err = a.conn.Exec(ctx, `INSERT INTO main.upgrade_people(_sync_scope_id,id,legacy_name,name,new_label) VALUES('late',$1,'Late|Row','Late','Row')`, lateKey)
	require.NoError(t, err)
	_, err = a.conn.Exec(ctx, `SET session_replication_role=origin`)
	require.NoError(t, err)
	done, err = a.captureCensusPage(ctx, b)
	require.False(t, done)
	require.ErrorContains(t, err, "maintenance_census_changed")
	var beyond int
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT count(*) FROM app_evolution.census_scopes WHERE sequence>(SELECT watermark FROM app_evolution.jobs WHERE job=$1)`, JobID).Scan(&beyond))
	require.Zero(t, beyond)
	require.Error(t, a.Certify(ctx, 3))
}

func TestEvolution_RepairValidityStaysWithinCoreMaximum(t *testing.T) {
	b := fixtureBounds()
	b.Deadline = time.Now().UTC().Add(120 * 24 * time.Hour).Truncate(time.Microsecond)
	pool, service, keys := fixtureMaintainingBounds(t, []string{"invalid historical"}, nil, b)
	ctx := context.Background()
	w := fixtureWorker(pool, service)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	var failed uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id FROM app_evolution.batches WHERE state='pending'`).Scan(&failed))
	started := time.Now().UTC()
	require.NoError(t, w.Repair(ctx, failed, keys[0], "A|B"))
	var repair BatchDescriptor
	require.NoError(t, pool.QueryRow(ctx, `SELECT b.spec FROM app_evolution.repair_evidence e JOIN app_evolution.batches b ON b.operation_id=e.repair_operation WHERE e.failed_operation=$1`, failed).Scan(&repair))
	require.True(t, repair.ValidUntil.After(started))
	require.False(t, repair.ValidUntil.After(started.Add(7*24*time.Hour+time.Second)))
}

func TestEvolution_RepairStepUsesQueryTimeout(t *testing.T) {
	b := fixtureBounds()
	b.QueryTimeout = 100 * time.Millisecond
	pool, service, keys := fixtureMaintainingBounds(t, []string{"invalid historical"}, nil, b)
	ctx := context.Background()
	w := fixtureWorker(pool, service)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	var failed uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id FROM app_evolution.batches WHERE state='pending'`).Scan(&failed))
	canceled, cancel := context.WithCancel(ctx)
	w.boundary = func(at string) {
		if at == "after_repair_descriptor" {
			cancel()
		}
	}
	require.Error(t, w.Repair(canceled, failed, keys[0], "A|B"))
	w.boundary = nil

	lock, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer lock.Rollback(ctx)
	_, err = lock.Exec(ctx, `SELECT 1 FROM main.upgrade_people WHERE id=$1 FOR UPDATE`, keys[0])
	require.NoError(t, err)
	started := time.Now()
	require.Error(t, w.Repair(ctx, failed, keys[0], "A|B"))
	require.Less(t, time.Since(started), time.Second)
	var legacy string
	require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name FROM main.upgrade_people WHERE id=$1`, keys[0]).Scan(&legacy))
	require.Equal(t, "invalid historical", legacy)
}

func TestEvolution_RepairLockWaitUsesImmutableDeadline(t *testing.T) {
	b := fixtureBounds()
	pool, service, keys := fixtureMaintainingBounds(t, []string{"invalid historical"}, nil, b)
	ctx := context.Background()
	w := fixtureWorker(pool, service)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	var failed uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id FROM app_evolution.batches WHERE state='pending'`).Scan(&failed))

	held, err := acquireSessionLock(ctx, pool, workerLock)
	require.NoError(t, err)
	defer releaseSessionLock(held, workerLock)
	w.Bounds.Deadline = time.Now().UTC().Add(250 * time.Millisecond)
	_, err = pool.Exec(ctx, `UPDATE app_evolution.jobs SET bounds=$2 WHERE job=$1`, JobID, w.Bounds)
	require.NoError(t, err)
	var beforeState string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&beforeState))

	started := time.Now()
	err = w.Repair(ctx, failed, keys[0], "A|B")
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
	var afterState string
	var repairs int
	require.NoError(t, pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM app_evolution.repair_evidence) FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&afterState, &repairs))
	require.Equal(t, beforeState, afterState)
	require.Zero(t, repairs)
}

func TestEvolution_RepairCapacityUsesImmutableDeadline(t *testing.T) {
	b := fixtureBounds()
	pool, service, keys := fixtureMaintainingBounds(t, []string{"invalid historical"}, nil, b)
	ctx := context.Background()
	w := fixtureWorker(pool, service)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	var failed uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id FROM app_evolution.batches WHERE state='pending'`).Scan(&failed))

	w.Bounds.Deadline = time.Now().UTC().Add(5 * time.Second)
	_, err := pool.Exec(ctx, `UPDATE app_evolution.jobs SET bounds=$2 WHERE job=$1`, JobID, w.Bounds)
	require.NoError(t, err)
	var observed time.Time
	w.Capacity = func(ctx context.Context) (Capacity, error) {
		var ok bool
		observed, ok = ctx.Deadline()
		require.True(t, ok)
		return Capacity{}, context.Canceled
	}
	err = w.Repair(ctx, failed, keys[0], "A|B")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, observed.IsZero())
	require.False(t, observed.After(w.Bounds.Deadline))
}

func TestEvolution_ExpiredRepairDescriptorIsDurablyReplaced(t *testing.T) {
	pool, service, keys := fixtureMaintainingBounds(t, []string{"invalid historical"}, nil, fixtureBounds())
	ctx := context.Background()
	w := fixtureWorker(pool, service)
	require.ErrorContains(t, w.Run(ctx), "invalid_historical_input")
	var failed uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT operation_id FROM app_evolution.batches WHERE state='pending'`).Scan(&failed))

	canceled, cancel := context.WithCancel(ctx)
	w.boundary = func(at string) {
		if at == "after_repair_descriptor" {
			cancel()
		}
	}
	require.Error(t, w.Repair(canceled, failed, keys[0], "A|B"))
	var expired BatchDescriptor
	require.NoError(t, pool.QueryRow(ctx, `SELECT b.spec FROM app_evolution.repair_evidence e JOIN app_evolution.batches b ON b.operation_id=e.repair_operation WHERE e.failed_operation=$1 AND e.is_current`, failed).Scan(&expired))
	expired.ValidUntil = time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	encoded, err := expired.CanonicalBytes()
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE app_evolution.batches SET spec=$2,descriptor=$3,hash=$4 WHERE operation_id=$1`, expired.OperationID, expired, encoded, digest(encoded))
	require.NoError(t, err)

	w = fixtureWorker(pool, service)
	require.NoError(t, w.Repair(ctx, failed, keys[0], "A|B"))
	var attempts, current int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE is_current) FROM app_evolution.repair_evidence WHERE failed_operation=$1`, failed).Scan(&attempts, &current))
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, current)
	var expiredState, failedState, legacy string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM app_evolution.batches WHERE operation_id=$1`, expired.OperationID).Scan(&expiredState))
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM app_evolution.batches WHERE operation_id=$1`, failed).Scan(&failedState))
	require.NoError(t, pool.QueryRow(ctx, `SELECT legacy_name FROM main.upgrade_people WHERE id=$1`, keys[0]).Scan(&legacy))
	require.Equal(t, "expired_reselect", expiredState)
	require.Equal(t, "repaired", failedState)
	require.Equal(t, "A|B", legacy)
}

func TestEvolution_ScopeLifecycleRejectsSuppressedInventoryMutation(t *testing.T) {
	ctx := context.Background()
	pool := fixturePool(t)
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, a.Apply(ctx, 1))
	cfg, err := Configuration(1, 1, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	service := bootstrapFixture(t, pool, cfg)
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	incarnation, err := CreateScope(ctx, pool, "delete-me")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE FUNCTION app_evolution.suppress_scope_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER zz_suppress_scope_mutation BEFORE INSERT OR UPDATE ON app_evolution.scope_inventory FOR EACH ROW EXECUTE FUNCTION app_evolution.suppress_scope_mutation()`)
	require.NoError(t, err)
	_, err = CreateScope(ctx, pool, "suppressed-create")
	require.ErrorContains(t, err, "scope_inventory_create_not_applied")

	opts := oversync.ScopeWriteOptions{WriterID: "scope-lifecycle-test", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{1}, OperationValidUntil: time.Now().Add(time.Hour)}}
	err = DeleteScope(ctx, pool, oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}), "delete-me", incarnation, opts)
	require.ErrorContains(t, err, "scope_inventory_delete_not_applied")
	var activeCreate, activeDelete bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='suppressed-create'),EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='delete-me' AND NOT deleted)`).Scan(&activeCreate, &activeDelete))
	require.False(t, activeCreate)
	require.True(t, activeDelete)
}

func TestEvolution_ScopeLifecycleRejectsDivertedInventoryMutation(t *testing.T) {
	ctx := context.Background()
	pool, service := fixtureServingEvolutionV1(t)
	incarnation, err := CreateScope(ctx, pool, "delete-me")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE FUNCTION app_evolution.divert_scope_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_OP='INSERT' THEN NEW.scope_id=NEW.scope_id||'-diverted'; ELSE NEW.deleted=false; END IF; RETURN NEW; END $$; CREATE TRIGGER zz_divert_scope_mutation BEFORE INSERT OR UPDATE ON app_evolution.scope_inventory FOR EACH ROW EXECUTE FUNCTION app_evolution.divert_scope_mutation()`)
	require.NoError(t, err)
	_, err = CreateScope(ctx, pool, "diverted-create")
	require.ErrorContains(t, err, "scope_inventory_create_not_applied")

	opts := oversync.ScopeWriteOptions{WriterID: "scope-lifecycle-test", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{1}, OperationValidUntil: time.Now().Add(time.Hour)}}
	err = DeleteScope(ctx, pool, oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}), "delete-me", incarnation, opts)
	require.ErrorContains(t, err, "scope_inventory_delete_not_applied")
	var diverted, activeDelete bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='diverted-create-diverted'),EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='delete-me' AND NOT deleted)`).Scan(&diverted, &activeDelete))
	require.False(t, diverted)
	require.True(t, activeDelete)
}

func TestEvolution_ScopeLifecycleRejectsAdditionalInventoryMutation(t *testing.T) {
	ctx := context.Background()
	pool, service := fixtureServingEvolutionV1(t)
	incarnation, err := CreateScope(ctx, pool, "delete-me")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE FUNCTION app_evolution.duplicate_scope_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.scope_id NOT LIKE 'side-effect-%' THEN INSERT INTO app_evolution.scope_inventory(scope_id,incarnation) VALUES('side-effect-'||NEW.scope_id,gen_random_uuid()); END IF; RETURN NEW; END $$; CREATE TRIGGER zz_duplicate_scope_mutation AFTER INSERT OR UPDATE ON app_evolution.scope_inventory FOR EACH ROW EXECUTE FUNCTION app_evolution.duplicate_scope_mutation()`)
	require.NoError(t, err)
	_, err = CreateScope(ctx, pool, "duplicated-create")
	require.ErrorContains(t, err, "scope_inventory_mutation_contract_changed")

	opts := oversync.ScopeWriteOptions{WriterID: "scope-lifecycle-test", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{1}, OperationValidUntil: time.Now().Add(time.Hour)}}
	err = DeleteScope(ctx, pool, oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}), "delete-me", incarnation, opts)
	require.ErrorContains(t, err, "scope_inventory_mutation_contract_changed")
	var unexpected, activeDelete bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id LIKE 'side-effect-%'),EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='delete-me' AND NOT deleted)`).Scan(&unexpected, &activeDelete))
	require.False(t, unexpected)
	require.True(t, activeDelete)
}

func TestEvolution_ScopeLifecycleRejectsInternalForeignKeyTriggers(t *testing.T) {
	ctx := context.Background()
	pool, service := fixtureServingEvolutionV1(t)
	incarnation, err := CreateScope(ctx, pool, "delete-me")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ALTER TABLE app_evolution.scope_inventory ADD CONSTRAINT unexpected_scope_self_fk FOREIGN KEY(incarnation) REFERENCES app_evolution.scope_inventory(incarnation) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED`)
	require.NoError(t, err)

	_, err = CreateScope(ctx, pool, "fk-trigger-create")
	require.ErrorContains(t, err, "scope_inventory_mutation_contract_changed")
	opts := oversync.ScopeWriteOptions{WriterID: "scope-lifecycle-test", RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: uuid.New(), OperationHash: [32]byte{1}, OperationValidUntil: time.Now().Add(time.Hour)}}
	err = DeleteScope(ctx, pool, oversync.NewScopeManager(service, oversync.ScopeManagerConfig{}), "delete-me", incarnation, opts)
	require.ErrorContains(t, err, "scope_inventory_mutation_contract_changed")
	var created, stillActive bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='fk-trigger-create'),EXISTS(SELECT 1 FROM app_evolution.scope_inventory WHERE scope_id='delete-me' AND NOT deleted)`).Scan(&created, &stillActive))
	require.False(t, created)
	require.True(t, stillActive)
}

func fixtureServingEvolutionV1(t *testing.T) (*pgxpool.Pool, *oversync.SyncService) {
	t.Helper()
	ctx := context.Background()
	pool := fixturePool(t)
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, a.Apply(ctx, 1))
	cfg, err := Configuration(1, 1, false)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	service := bootstrapFixture(t, pool, cfg)
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	return pool, service
}
