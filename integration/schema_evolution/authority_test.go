package schemaevolution

import (
	"context"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestEvolution_AuthorityProcessHelper(t *testing.T) {
	dsn := os.Getenv("TRANSITION_AUTHORITY_DATABASE_URL")
	if dsn == "" {
		t.Skip("subprocess entry point")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	require.NoError(t, a.Apply(ctx, 2))
	m := a.manifest[2]
	_, err = a.conn.Exec(ctx, `INSERT INTO app_evolution.migrations(version,identity,state,started_at) VALUES(3,$1,'started',clock_timestamp())`, m)
	require.NoError(t, err)
	sql, err := migrationFiles.ReadFile(m.Filename)
	require.NoError(t, err)
	_, err = a.conn.Exec(ctx, string(sql))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(os.Getenv("TRANSITION_AUTHORITY_MARKER"), []byte("index_committed_without_ledger_completion"), 0600))
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
	// Signal delivery may occur after Kill returns. Never let the helper exit
	// successfully before the parent observes the intended crash boundary.
	time.Sleep(5 * time.Second)
	t.Fatal("SIGKILL did not terminate the helper")
}

func TestEvolution_AuthorityCrashAndAmbiguousIndex(t *testing.T) {
	pool := fixturePool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "boundary")
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEvolution_AuthorityProcessHelper$")
	command.Env = append(os.Environ(), "TRANSITION_AUTHORITY_DATABASE_URL="+pool.Config().ConnString(), "TRANSITION_AUTHORITY_MARKER="+marker)
	output, err := command.CombinedOutput()
	require.Error(t, err, "%s", output)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, status.Signaled(), "%s", output)
	require.Equal(t, syscall.SIGKILL, status.Signal())
	require.NoError(t, ctx.Err())
	_, err = os.ReadFile(marker)
	require.NoError(t, err, "%s", output)
	proof, err := Drain(ctx, pool, nil, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	var before uint32
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT 'main.upgrade_people_name_idx'::regclass::oid`).Scan(&before))
	require.NoError(t, a.Apply(ctx, 3))
	var after uint32
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT 'main.upgrade_people_name_idx'::regclass::oid`).Scan(&after))
	require.Equal(t, before, after, "valid ambiguous index is reconciled, not rebuilt")
	var state string
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT state FROM app_evolution.migrations WHERE version=3`).Scan(&state))
	require.Equal(t, "completed", state)
}

func TestEvolution_InvalidConcurrentIndexRecovery(t *testing.T) {
	pool := fixturePool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, err := OpenFreshAuthority(ctx, pool)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Apply(ctx, 2))
	reader, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	defer reader.Rollback(context.Background())
	var n int
	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM main.upgrade_people`).Scan(&n))
	indexCtx, indexCancel := context.WithCancel(ctx)
	defer indexCancel()
	finished := make(chan error, 1)
	go func() { finished <- a.Apply(indexCtx, 3) }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_progress_create_index WHERE relid='main.upgrade_people'::regclass AND phase='waiting for old snapshots')`).Scan(&waiting)
		return err == nil && waiting
	}, 3*time.Second, 10*time.Millisecond)
	indexCancel()
	require.Error(t, <-finished)
	require.NoError(t, reader.Commit(ctx))
	// A canceled socket may have been discarded by pgx; reopen authority after
	// releasing this dedicated connection before doing forward reconciliation.
	a.Close()
	proof, err := Drain(ctx, pool, nil, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err = OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Apply(ctx, 3))
	var valid bool
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT indisvalid AND indisready AND indislive FROM pg_index WHERE indexrelid='main.upgrade_people_name_idx'::regclass`).Scan(&valid))
	require.True(t, valid)
}

func TestEvolution_ReadinessDriftAndDrainFailure(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	failedCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	proof, err := Drain(failedCtx, pool, []*oversync.SyncService{service}, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	require.Error(t, err)
	require.Nil(t, proof.pool)
	require.False(t, service.Readiness().Ready)
	require.NoError(t, service.Close(ctx))
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	cases := []struct {
		name, sql string
		category  oversync.ReadinessCategory
	}{
		{"missing", `DELETE FROM app_evolution.migrations WHERE version=3`, oversync.MigrationMissing},
		{"partial", `UPDATE app_evolution.migrations SET state='started' WHERE version=3`, oversync.MigrationIncomplete},
		{"checksum", `UPDATE app_evolution.migrations SET identity=jsonb_set(identity,'{SHA256}','"invalid"') WHERE version=2`, oversync.MigrationChecksumMismatch},
		{"missing_trigger", `DROP TRIGGER zz_app_person_transition ON main.upgrade_people`, oversync.TransitionMechanismMismatch},
		{"misordered_trigger", `ALTER TRIGGER zz_app_person_transition ON main.upgrade_people RENAME TO aa_app_person_transition`, oversync.TransitionMechanismMismatch},
		{"altered_function", `ALTER FUNCTION app_evolution.transition_person() IMMUTABLE`, oversync.TransitionMechanismMismatch},
		{"disabled", `ALTER TABLE main.upgrade_people DISABLE TRIGGER zz_app_person_transition`, oversync.TransitionMechanismMismatch},
		{"wrong_phase", `DROP TRIGGER zz_app_person_transition ON main.upgrade_people; CREATE TRIGGER zz_app_person_transition BEFORE INSERT OR UPDATE ON main.upgrade_people FOR EACH ROW EXECUTE FUNCTION app_evolution.transition_person('coexistence')`, oversync.TransitionMechanismMismatch},
		{"unexpected_scope_trigger", `CREATE TRIGGER zz_unexpected_scope_trigger BEFORE INSERT OR UPDATE ON app_evolution.scope_inventory FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_write()`, oversync.TransitionMechanismMismatch},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			_, err = tx.Exec(ctx, test.sql)
			require.NoError(t, err)
			evidence, inspectErr := cfg.Readiness.Adapter.Inspect(ctx, tx)
			registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
			require.NoError(t, err)
			if inspectErr == nil {
				inspectErr = oversync.ValidateApplicationReadiness(cfg.Readiness, registry, evidence)
			}
			var failure *oversync.ReadinessError
			require.ErrorAs(t, inspectErr, &failure)
			require.Equal(t, test.category, failure.Category)
			require.NoError(t, tx.Rollback(ctx))
		})
	}
}

func TestEvolution_ForgedCompletionCannotEnable(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	require.Error(t, a.Certify(ctx, 3))
	_, err = a.conn.Exec(ctx, `UPDATE app_evolution.jobs SET state='traversed' WHERE job=$1`, JobID)
	require.NoError(t, err)
	require.ErrorContains(t, a.Certify(ctx, 3), "backfill_validation_failed")
	var count int
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT count(*) FROM app_evolution.certificates`).Scan(&count))
	require.Zero(t, count)
	require.Error(t, a.Apply(ctx, 4))
}

func TestEvolution_UnvalidatedConstraintAndPostWatermarkInvalidRows(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	service = fixtureCoexistence(t, pool, service)
	cfg, err := Configuration(4, 3, true)
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `ALTER TABLE main.upgrade_people DROP CONSTRAINT person_encoding; ALTER TABLE main.upgrade_people ADD CONSTRAINT person_encoding `+encodingConstraintDefinition+` NOT VALID`)
	require.NoError(t, err)
	evidence, err := cfg.Readiness.Adapter.Inspect(ctx, tx)
	if err == nil {
		registry, compileErr := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
		require.NoError(t, compileErr)
		err = oversync.ValidateApplicationReadiness(cfg.Readiness, registry, evidence)
	}
	var failure *oversync.ReadinessError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, oversync.TransitionMechanismMismatch, failure.Category)
	require.NoError(t, tx.Rollback(ctx))
	_, err = CreateScope(ctx, pool, "late")
	require.NoError(t, err)
	require.NoError(t, fixtureWrite(t, service, "late", `INSERT INTO main.upgrade_people(_sync_scope_id,id,legacy_name) VALUES('late',$1,'Late|Scope')`, uuid.New()))
	// A privileged corrupted/restore artifact cannot turn its old completion
	// certificate into fresh admission. No serving path disables any trigger.
	tx, err = pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `UPDATE app_evolution.scope_inventory SET deleted=true WHERE scope_id='late'`)
	require.NoError(t, err)
	evidence, err = cfg.Readiness.Adapter.Inspect(ctx, tx)
	if err == nil {
		registry, compileErr := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
		require.NoError(t, compileErr)
		err = oversync.ValidateApplicationReadiness(cfg.Readiness, registry, evidence)
	}
	require.Error(t, err)
}

func TestEvolution_RuntimeCannotForgeMigrationEvidence(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	role := "transition_runtime_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = a.conn.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize())
	require.NoError(t, err)
	defer func() {
		_, err := a.conn.Exec(ctx, `DROP OWNED BY `+pgx.Identifier{role}.Sanitize())
		require.NoError(t, err)
		_, err = a.conn.Exec(ctx, `DROP ROLE `+pgx.Identifier{role}.Sanitize())
		require.NoError(t, err)
	}()
	require.NoError(t, a.GrantRuntime(ctx, role))
	for _, sql := range []string{`CREATE OR REPLACE TRIGGER zz_app_person_transition BEFORE INSERT OR UPDATE ON main.upgrade_people FOR EACH ROW WHEN(false) EXECUTE FUNCTION app_evolution.transition_person()`, `CREATE OR REPLACE TRIGGER aa_app_write_guard BEFORE INSERT OR UPDATE OR DELETE ON main.upgrade_people FOR EACH STATEMENT WHEN(false) EXECUTE FUNCTION app_evolution.guard_write()`, `UPDATE app_evolution.migrations SET state='completed'`, `UPDATE app_evolution.deployment SET revision=999`, `INSERT INTO app_evolution.certificates(certificate) VALUES('{}')`, `ALTER FUNCTION app_evolution.transition_person() IMMUTABLE`, `ALTER TABLE main.upgrade_people DISABLE TRIGGER zz_app_person_transition`, `UPDATE app_evolution.jobs SET algorithm='forged'`, `UPDATE app_evolution.maintenance SET requested=false`, `INSERT INTO app_evolution.worker_transactions VALUES(pg_backend_pid(),pg_current_xact_id(),1,'00000000-0000-0000-0000-000000000001')`, `DELETE FROM app_evolution.census_rows`} {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `SET LOCAL ROLE `+pgx.Identifier{role}.Sanitize())
		require.NoError(t, err)
		_, err = tx.Exec(ctx, sql)
		require.Error(t, err, sql)
		require.NoError(t, tx.Rollback(ctx))
	}
}

func TestEvolution_LateScopeCommitSerializesWithDrain(t *testing.T) {
	pool, service, _ := fixtureMaintaining(t)
	service = fixtureCoexistence(t, pool, service)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	var closed bool
	require.NoError(t, tx.QueryRow(ctx, `SELECT closed FROM app_evolution.barrier WHERE singleton FOR SHARE`).Scan(&closed))
	require.False(t, closed)
	_, err = tx.Exec(ctx, `INSERT INTO app_evolution.scope_inventory(scope_id,incarnation) VALUES('late-commit',$1)`, uuid.New())
	require.NoError(t, err)
	type result struct {
		proof DrainProof
		err   error
	}
	done := make(chan result, 1)
	go func() {
		proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
		done <- result{proof, err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=1869833075 AND objid=9 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`).Scan(&waiting)
		return err == nil && waiting
	}, 3*time.Second, 10*time.Millisecond)
	select {
	case <-done:
		t.Fatal("drain passed uncommitted inventory")
	default:
	}
	require.NoError(t, tx.Commit(ctx))
	completed := <-done
	require.NoError(t, completed.err)
	a, err := OpenAuthority(ctx, completed.proof)
	require.NoError(t, err)
	defer a.Close()
	var high, watermark int64
	require.NoError(t, a.conn.QueryRow(ctx, `SELECT (SELECT max(sequence) FROM app_evolution.scope_inventory),(SELECT watermark FROM app_evolution.jobs)`).Scan(&high, &watermark))
	require.Greater(t, high, watermark)
	validation, err := validateAllRows(ctx, a.conn)
	require.NoError(t, err)
	require.Equal(t, high, validation.CoverageWatermark)
}

func TestEvolution_RuntimeAttachesWithoutTriggerPrivilege(t *testing.T) {
	pool, service, id := fixtureMaintaining(t)
	ctx := context.Background()
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	role := "transition_serve_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{role}.Sanitize()
	_, err = a.conn.Exec(ctx, `CREATE ROLE `+quoted+` LOGIN PASSWORD 'isolated-fixture'`)
	require.NoError(t, err)
	defer func() {
		_, err := pool.Exec(ctx, `DROP OWNED BY `+quoted+`; DROP ROLE `+quoted)
		require.NoError(t, err)
	}()
	require.NoError(t, a.GrantRuntime(ctx, role))
	a.Close()
	maintaining, err := Configuration(3, 2, false)
	require.NoError(t, err)
	service = bootstrapFixture(t, pool, maintaining)
	service = fixtureCoexistence(t, pool, service)
	require.NoError(t, service.Close(ctx))
	cfg, err := Configuration(4, 3, true)
	require.NoError(t, err)
	pc := pool.Config().Copy()
	pc.ConnConfig.User = role
	pc.ConnConfig.Password = "isolated-fixture"
	pair, err := oversync.NewReadinessPoolPair(ctx, pc, 2)
	require.NoError(t, err)
	defer pair.Close()
	runtime := pair.ExecutionPool()
	cfg.Readiness.PoolPair = pair
	serving, err := oversync.NewRuntimeService(runtime, cfg, nil)
	require.NoError(t, err)
	defer serving.Close(ctx)
	require.NoError(t, serving.Bootstrap(ctx))
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{serving}))
	require.NoError(t, fixtureWrite(t, serving, "scope", `UPDATE main.upgrade_people SET legacy_name='Runtime|Write' WHERE id=$1`, id))
	for _, trigger := range []string{"aa_app_write_guard", "zz_app_person_transition"} {
		_, err = runtime.Exec(ctx, `CREATE OR REPLACE TRIGGER `+pgx.Identifier{trigger}.Sanitize()+` BEFORE INSERT ON main.upgrade_people FOR EACH ROW WHEN(false) EXECUTE FUNCTION app_evolution.transition_person()`)
		require.ErrorContains(t, err, "permission denied")
	}
}
