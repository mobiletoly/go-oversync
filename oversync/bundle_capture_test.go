package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
	"github.com/stretchr/testify/require"
)

func TestBootstrap_InstallsRegisteredTableCaptureTriggers(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_trigger_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-trigger-bootstrap-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	var captureTriggerCount int
	var ownerGuardTriggerCount int
	var truncateGuardTriggerCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname IN ('users', 'posts')
		  AND t.tgname = $2
		  AND NOT t.tgisinternal
	`, schemaName, registeredTableCaptureTriggerName).Scan(&captureTriggerCount))
	require.Equal(t, 2, captureTriggerCount)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname IN ('users', 'posts')
		  AND t.tgname = $2
		  AND NOT t.tgisinternal
	`, schemaName, registeredTableOwnerGuardTrigger).Scan(&ownerGuardTriggerCount))
	require.Equal(t, 2, ownerGuardTriggerCount)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname IN ('users', 'posts')
		  AND t.tgname = $2
		  AND NOT t.tgisinternal
	`, schemaName, registeredTableTruncateGuardTrigger).Scan(&truncateGuardTriggerCount))
	require.Equal(t, 2, truncateGuardTriggerCount)

	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname IN ('users', 'posts')
		  AND t.tgname = $2
		  AND NOT t.tgisinternal
	`, schemaName, registeredTableTruncateGuardTrigger).Scan(&truncateGuardTriggerCount))
	require.Equal(t, 2, truncateGuardTriggerCount, "repeated bootstrap must replace rather than duplicate truncate guards")

	require.NoError(t, svc.Close(context.Background()))
}

func TestRegisteredTableGuard_RejectsNullIdentityDrift(t *testing.T) {
	f := newBundleOwnerGuardFixture(t, "bundle_null_drift_", "bundle-null-drift-test")
	f.initializeOwner(t, "owner-a")

	_, err := f.pool.Exec(f.ctx, fmt.Sprintf(`
		ALTER TABLE %s.users DROP CONSTRAINT users_pkey CASCADE;
		ALTER TABLE %s.users ALTER COLUMN _sync_scope_id DROP NOT NULL;
		ALTER TABLE %s.users ALTER COLUMN id DROP NOT NULL;
		ALTER TABLE %s.users DISABLE TRIGGER USER;
		INSERT INTO %s.users(id, _sync_scope_id, name, email) VALUES
			('11111111-1111-1111-1111-111111111111', NULL, 'Null owner', 'owner@example.com'),
			(NULL, 'owner-a', 'Null key', 'key@example.com');
		ALTER TABLE %s.users ENABLE TRIGGER USER;
	`, f.schemaIdent, f.schemaIdent, f.schemaIdent, f.schemaIdent, f.schemaIdent, f.schemaIdent))
	require.NoError(t, err)

	actor := Actor{UserID: "owner-a"}
	source := f.source(1)
	for _, statement := range []string{
		fmt.Sprintf(`UPDATE %s.users SET name = 'changed' WHERE email = 'owner@example.com'`, f.schemaIdent),
		fmt.Sprintf(`DELETE FROM %s.users WHERE email = 'owner@example.com'`, f.schemaIdent),
		fmt.Sprintf(`UPDATE %s.users SET name = 'changed' WHERE email = 'key@example.com'`, f.schemaIdent),
		fmt.Sprintf(`DELETE FROM %s.users WHERE email = 'key@example.com'`, f.schemaIdent),
	} {
		err := f.svc.WithinSyncBundle(f.ctx, actor, source, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
			_, execErr := tx.Exec(ctx, statement)
			return execErr
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "nullable")
	}

	var unchanged, captureRows int
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE name IN ('Null owner', 'Null key')`, f.schemaIdent)).Scan(&unchanged))
	require.Equal(t, 2, unchanged)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM sync.bundle_capture_stage`).Scan(&captureRows))
	require.Zero(t, captureRows)

	err = f.svc.WithinSyncBundle(f.ctx, actor, f.source(1), retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users(id, name, email) VALUES ('22222222-2222-2222-2222-222222222222', 'Valid', 'valid@example.com')`, f.schemaIdent))
		return execErr
	})
	require.NoError(t, err)
	var owner string
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT _sync_scope_id FROM %s.users WHERE email = 'valid@example.com'`, f.schemaIdent)).Scan(&owner))
	require.Equal(t, "owner-a", owner)
}

func TestBootstrap_MarkedLayoutPerformsNoRegisteredTriggerDDL(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "truncate_install_rollback_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "truncate-install-rollback-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	eventTriggerName := "reject_c3_trigger_ddl_" + suffix
	eventFunctionName := "reject_c3_trigger_ddl_" + suffix
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s.%s()
		RETURNS event_trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'synthetic trigger installation failure';
		END;
		$$;
		CREATE EVENT TRIGGER %s
		ON ddl_command_start
		WHEN TAG IN ('CREATE TRIGGER')
		EXECUTE FUNCTION %s.%s();
	`, pgx.Identifier{schemaName}.Sanitize(), pgx.Identifier{eventFunctionName}.Sanitize(), pgx.Identifier{eventTriggerName}.Sanitize(), pgx.Identifier{schemaName}.Sanitize(), pgx.Identifier{eventFunctionName}.Sanitize()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP EVENT TRIGGER IF EXISTS "+pgx.Identifier{eventTriggerName}.Sanitize())
	})

	require.NoError(t, svc.Bootstrap(ctx), "a coherent marked layout must not execute trigger DDL")

	var count int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger AS trigger
		JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1
		  AND relation.relname = 'users'
		  AND trigger.tgname IN ($2, $3, $4)
		  AND NOT trigger.tgisinternal
	`, schemaName, registeredTableCaptureTriggerName, registeredTableOwnerGuardTrigger, registeredTableTruncateGuardTrigger).Scan(&count))
	require.Equal(t, 3, count, "marked-layout validation must preserve every managed trigger")
}

func TestBootstrap_ExistingLayoutPreservesDirectWriteAndTruncateGuards(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "fast_attach_guards_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "fast-attach-guards",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}
	first := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, first.Close(ctx))
	service, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(ctx))

	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	scopeID := "fast-attach-owner-" + suffix
	mustInitializeEmptyScope(t, ctx, service, scopeID, "seed")
	directID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name, email)
		VALUES ($1, $2, 'direct', 'direct@example.com')
	`, tableIdent), scopeID, directID)
	require.ErrorContains(t, err, "requires oversync sync bundle context")

	manager := NewScopeManager(service, ScopeManagerConfig{Logger: integrationTestLogger(slog.LevelWarn)})
	managedID := uuid.New()
	managedResult, err := manager.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest()}, func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, email) VALUES ($1, 'managed', 'managed@example.com')`, tableIdent), managedID)
		return execErr
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), managedResult.Bundle.BundleSeq)
	require.Equal(t, int64(1), managedResult.Bundle.RowCount)

	bundleID := uuid.New()
	require.NoError(t, service.WithinSyncBundle(ctx, Actor{UserID: scopeID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, email) VALUES ($1, 'bundle', 'bundle@example.com')`, tableIdent), bundleID)
		return execErr
	}))

	for _, statement := range []string{
		fmt.Sprintf(`UPDATE %s SET name = 'direct update' WHERE id = '%s'`, tableIdent, managedID),
		fmt.Sprintf(`DELETE FROM %s WHERE id = '%s'`, tableIdent, bundleID),
	} {
		_, err = pool.Exec(ctx, statement)
		require.ErrorContains(t, err, "requires oversync sync bundle context")
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s CASCADE`, tableIdent))
	requireRegisteredTruncateError(t, err, schemaName, "users")

	mustInitializeEmptyScope(t, ctx, service, scopeID, "reader")
	pull, err := service.ProcessPull(ctx, Actor{UserID: scopeID, SourceID: "reader"}, testSchemaGenerationProofForService(service), 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), pull.StableBundleSeq)
	require.Len(t, pull.Bundles, 2)
	for _, bundle := range pull.Bundles {
		require.Len(t, bundle.Rows, 1)
	}
	var businessRows, bundleRows int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE _sync_scope_id = $1`, tableIdent), scopeID).Scan(&businessRows))
	require.Equal(t, 2, businessRows)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM sync.bundle_rows AS rows
		JOIN sync.user_state AS users ON users.user_pk = rows.user_pk
		WHERE users.user_id = $1
	`, scopeID).Scan(&bundleRows))
	require.Equal(t, 2, bundleRows)
}

type truncateGuardState struct {
	businessRows      int64
	rowStateRows      int64
	bundleRows        int64
	bundleLogRows     int64
	nextBundleSeq     int64
	maxSourceBundleID int64
}

func loadTruncateGuardState(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, schemaName, userID, sourceID string) truncateGuardState {
	t.Helper()

	var state truncateGuardState
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableIdent)).Scan(&state.businessRows))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = users.user_pk),
			(SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = users.user_pk),
			users.next_bundle_seq,
			COALESCE((SELECT max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = users.user_pk AND source_id = $2), 0)
		FROM sync.user_state AS users
		WHERE users.user_id = $1
	`, userID, sourceID).Scan(
		&state.rowStateRows,
		&state.bundleRows,
		&state.bundleLogRows,
		&state.nextBundleSeq,
		&state.maxSourceBundleID,
	))
	return state
}

func requireRegisteredTruncateError(t *testing.T, err error, schemaName, tableName string) {
	t.Helper()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "expected PostgreSQL error, got %T: %v", err, err)
	t.Logf("registered truncate rejected: code=%s message=%q detail=%q hint=%q", pgErr.Code, pgErr.Message, pgErr.Detail, pgErr.Hint)
	require.Equal(t, "55000", pgErr.Code)
	require.Equal(t, fmt.Sprintf("TRUNCATE is not allowed on registered table %s.%s", schemaName, tableName), pgErr.Message)
	require.Equal(t, "Oversync cannot capture TRUNCATE as row-level bundle events.", pgErr.Detail)
	require.Contains(t, pgErr.Hint, "recreate PostgreSQL and every client database")
}

func requireRetryableCallbackStatementClassError(t *testing.T, err error, statementClass string) {
	t.Helper()
	require.Error(t, err)
	var violation *RetryableCallbackViolationError
	require.ErrorAs(t, err, &violation)
	require.Contains(t, violation.Error(), "statement class is not allowed: "+statementClass)
}

func TestRegisteredTableGuard_RejectsDirectAndWithinSyncBundleTruncate(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "truncate_guard_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "truncate-guard-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	userID := "truncate-guard-user-" + suffix
	sourceID := "server-app"
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: sourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, email) VALUES ($1, $2, $3)`, tableIdent), uuid.New(), "Before Truncate", "before@example.com")
		return err
	}))

	want := loadTruncateGuardState(t, ctx, pool, schemaName, userID, sourceID)
	_, err := pool.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s CASCADE`, tableIdent))
	requireRegisteredTruncateError(t, err, schemaName, "users")
	require.Equal(t, want, loadTruncateGuardState(t, ctx, pool, schemaName, userID, sourceID))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s CASCADE`, tableIdent))
	requireRegisteredTruncateError(t, err, schemaName, "users")
	require.NoError(t, tx.Rollback(ctx), "an explicitly aborted transaction must remain rollbackable")
	require.Equal(t, want, loadTruncateGuardState(t, ctx, pool, schemaName, userID, sourceID))

	err = svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: sourceID, SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s CASCADE`, tableIdent))
		return err
	})
	requireRetryableCallbackStatementClassError(t, err, "truncate")
	require.Equal(t, want, loadTruncateGuardState(t, ctx, pool, schemaName, userID, sourceID))

	err = svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: sourceID, SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET name = 'Changed' WHERE _sync_scope_id = $1`, tableIdent), userID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s CASCADE`, tableIdent))
		return err
	})
	requireRetryableCallbackStatementClassError(t, err, "truncate")
	require.Equal(t, want, loadTruncateGuardState(t, ctx, pool, schemaName, userID, sourceID))
}

func TestRegisteredTableGuard_RejectsCascadeAndMultiTableTruncate(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "truncate_cascade_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	userTableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()
	require.NoError(t, func() error {
		_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.unregistered_rows (id UUID PRIMARY KEY)`, schemaIdent))
		return err
	}())
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), AppName: "truncate-cascade-test",
		RegisteredTables:               []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	userID := "truncate-cascade-user-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")
	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, email) VALUES ($1, 'User', 'user@example.com')`, userTableIdent), uuid.New())
		return err
	}))
	_, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.unregistered_rows (id) VALUES ($1)`, schemaIdent), uuid.New())
	require.NoError(t, err)

	_, err = pool.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s.unregistered_rows, %s CASCADE`, schemaIdent, userTableIdent))
	requireRegisteredTruncateError(t, err, schemaName, "users")
	var unregisteredCount int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.unregistered_rows`, schemaIdent)).Scan(&unregisteredCount))
	require.Equal(t, int64(1), unregisteredCount)

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.parents (id UUID PRIMARY KEY);
		INSERT INTO %s.parents (id) VALUES ('00000000-0000-0000-0000-000000000001');
		ALTER TABLE %s ADD COLUMN parent_id UUID REFERENCES %s.parents(id);
	`, schemaIdent, schemaIdent, userTableIdent, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s.parents CASCADE`, schemaIdent))
	requireRegisteredTruncateError(t, err, schemaName, "users")
	var parentCount int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.parents`, schemaIdent)).Scan(&parentCount))
	require.Equal(t, int64(1), parentCount)
}

func TestRegisteredTableGuard_RejectsPartitionTruncate(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "truncate_partition_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	require.NoError(t, func() error {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			CREATE SCHEMA %s;
			CREATE TABLE %s.records (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				body TEXT NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			) PARTITION BY HASH (id);
			CREATE TABLE %s.records_p0 PARTITION OF %s.records FOR VALUES WITH (MODULUS 2, REMAINDER 0);
			CREATE TABLE %s.records_p1 PARTITION OF %s.records FOR VALUES WITH (MODULUS 2, REMAINDER 1);
		`, schemaIdent, schemaIdent, schemaIdent, schemaIdent, schemaIdent, schemaIdent))
		return err
	}())
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	_ = newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}}}), AppName: "truncate-partition-test",
		RegisteredTables:               []RegisteredTable{{Schema: schemaName, Table: "records", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	for _, tableName := range []string{"records", "records_p0", "records_p1"} {
		var count int
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_trigger AS trigger
			JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
			JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1 AND relation.relname = $2
			  AND trigger.tgname = $3 AND NOT trigger.tgisinternal
		`, schemaName, tableName, registeredTableTruncateGuardTrigger).Scan(&count))
		require.Equal(t, 1, count)
	}

	for _, tableName := range []string{"records", "records_p0", "records_p1"} {
		_, err := pool.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s`, pgx.Identifier{schemaName, tableName}.Sanitize()))
		requireRegisteredTruncateError(t, err, schemaName, "records")
	}
}

func TestWithinSyncBundle_CapturesDirectServerWrite(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_direct_write_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-direct-write-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-user-" + suffix
	rowID := uuid.New()
	actor := Actor{UserID: userID}
	source := BundleSource{SourceID: "server-app", SourceBundleID: 1}
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")

	err := svc.WithinSyncBundle(ctx, actor, source, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Alice", "alice@example.com")
		return err
	})
	require.NoError(t, err)

	var (
		bundleSeq      int64
		bundleSourceID string
		rowCount       int
		byteCount      int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT bundle_seq, source_id, row_count, byte_count
		FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&bundleSeq, &bundleSourceID, &rowCount, &byteCount))
	require.Equal(t, int64(1), bundleSeq)
	require.Equal(t, source.SourceID, bundleSourceID)
	require.Equal(t, 1, rowCount)
	require.Positive(t, byteCount)

	bundle := loadCommittedBundleForUser(t, ctx, svc, userID, bundleSeq)
	require.Len(t, bundle.Rows, 1)
	require.Equal(t, OpInsert, bundle.Rows[0].Op)
	require.Equal(t, bundleSeq, bundle.Rows[0].RowVersion)
	require.Equal(t, SyncKey{"id": rowID.String()}, bundle.Rows[0].Key)

	var payloadMap map[string]any
	require.NoError(t, json.Unmarshal(bundle.Rows[0].Payload, &payloadMap))
	require.Equal(t, rowID.String(), payloadMap["id"])
	require.Equal(t, "Alice", payloadMap["name"])
	require.Equal(t, "alice@example.com", payloadMap["email"])

	var (
		deleted        bool
		stateBundleSeq int64
	)
	userPK, tableID, keyBytes := mustCompactStorageIdentity(t, ctx, svc, userID, schemaName, "users", rowID.String())
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted, bundle_seq
		FROM sync.row_state
		WHERE user_pk = $1
		  AND table_id = $2
		  AND key_bytes = $3
	`, userPK, tableID, keyBytes).Scan(&deleted, &stateBundleSeq))
	require.False(t, deleted)
	require.Equal(t, bundleSeq, stateBundleSeq)

	var maxCommittedSourceBundleID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1 AND source_id = $2
	`, userPK, source.SourceID).Scan(&maxCommittedSourceBundleID))
	require.Equal(t, int64(1), maxCommittedSourceBundleID)
}

func TestWithinSyncBundle_RejectsReservedActorAndBundleSource(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "bundle_reserved_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), AppName: "bundle-reserved-test",
		RegisteredTables:               []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		ReservedServerSourceIDs:        []string{"server-reserved"},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	callbackCalled := false
	callback := func(context.Context, DatabaseWriteTx) error {
		callbackCalled = true
		return nil
	}
	for _, tc := range []struct {
		name   string
		actor  Actor
		source BundleSource
	}{
		{name: "actor", actor: Actor{UserID: "reserved-user", SourceID: "server-reserved"}, source: BundleSource{SourceID: "client-source", SourceBundleID: 1}},
		{name: "bundle source", actor: Actor{UserID: "reserved-user", SourceID: "client-source"}, source: BundleSource{SourceID: "server-reserved", SourceBundleID: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			callbackCalled = false
			err := service.WithinSyncBundle(ctx, tc.actor, tc.source, retryableBundleWriteOptionsForTest(), callback)
			require.ErrorIs(t, err, sourceid.ErrInvalid)
			require.False(t, callbackCalled)
		})
	}
}

func TestWithinSyncBundle_RollbackLeavesNoVisibleBundle(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_rollback_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-rollback-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-rollback-user-" + suffix
	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")

	err := svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Bob", "bob@example.com"); err != nil {
			return err
		}
		return fmt.Errorf("force rollback")
	})
	require.ErrorContains(t, err, "force rollback")

	var businessCount int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.users
		WHERE id = $1
	`, pgx.Identifier{schemaName}.Sanitize()), rowID).Scan(&businessCount))
	require.Zero(t, businessCount)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&bundleCount))
	require.Zero(t, bundleCount)

	var bundleRowCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&bundleRowCount))
	require.Zero(t, bundleRowCount)

	var rowStateCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&rowStateCount))
	require.Zero(t, rowStateCount)

	var stagedCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_capture_stage WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&stagedCount))
	require.Zero(t, stagedCount)
}

func TestWithinSyncBundle_RetriesDatabaseOnlyCallbackAndCommitsExactlyOneTransaction(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_single_attempt_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-single-attempt-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-single-attempt-user-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")
	rowID := uuid.New()

	attempts := 0
	err := svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		attempts++
		if attempts == 1 {
			return &pgconn.PgError{Code: "40001", Message: "synthetic serialization failure"}
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users (id, name, email) VALUES ($1, $2, $3)`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Retry", "retry@example.com")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&bundleCount))
	require.Equal(t, 1, bundleCount)

	var sourceStateCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.source_state
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
		  AND source_id = 'server-app'
	`, userID).Scan(&sourceStateCount))
	require.Equal(t, 1, sourceStateCount)

	var businessRowCount int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE id = $1`, pgx.Identifier{schemaName}.Sanitize()), rowID).Scan(&businessRowCount))
	require.Equal(t, 1, businessRowCount)
}

func TestWithinSyncBundle_RetiredSourceFailsClosed(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_retired_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-retired-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-retired-user-" + suffix
	oldSourceID := "server-old"
	newSourceID := "server-new"
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")
	// Trusted source retirement is independent of client snapshot replacement.
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		userPK, err := lookupUserPK(ctx, tx, userID)
		if err != nil {
			return err
		}
		return retireSourceState(ctx, tx, userPK, oldSourceID, newSourceID, "test")
	}))

	err := svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: oldSourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, execErr := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), uuid.New(), "Alice", "alice@example.com")
		return execErr
	})
	var retiredErr *SourceRetiredError
	require.ErrorAs(t, err, &retiredErr)
	require.Equal(t, oldSourceID, retiredErr.SourceID)
	require.Equal(t, newSourceID, retiredErr.ReplacedBySourceID)
}

func TestWithinSyncBundle_CapturesCascadeDeletes(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_cascade_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-cascade-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-cascade-user-" + suffix
	userRowID := uuid.New()
	postRowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), userRowID, "Carol", "carol@example.com"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.posts (id, title, content, author_id)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), postRowID, "Hello", "Post body", userRowID)
		return err
	}))

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			DELETE FROM %s.users
			WHERE id = $1
		`, pgx.Identifier{schemaName}.Sanitize()), userRowID)
		return err
	}))

	type bundleDelete struct {
		schema string
		table  string
		op     string
	}
	var deletes []bundleDelete
	bundle := loadCommittedBundleForUser(t, ctx, svc, userID, 2)
	for _, row := range bundle.Rows {
		deletes = append(deletes, bundleDelete{schema: row.Schema, table: row.Table, op: row.Op})
	}
	require.Len(t, deletes, 2)
	require.ElementsMatch(t, []bundleDelete{
		{schema: schemaName, table: "posts", op: OpDelete},
		{schema: schemaName, table: "users", op: OpDelete},
	}, deletes)

	var remainingUsers int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users`, pgx.Identifier{schemaName}.Sanitize())).Scan(&remainingUsers))
	require.Zero(t, remainingUsers)

	var remainingPosts int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.posts`, pgx.Identifier{schemaName}.Sanitize())).Scan(&remainingPosts))
	require.Zero(t, remainingPosts)
}

func TestWithinSyncBundle_CapturesCascadeUpdates(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_cascade_update_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.parents (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.children (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT children_parent_id_fkey
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.parents(_sync_scope_id, id)
				ON UPDATE CASCADE
				ON DELETE CASCADE
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "parents", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "children", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-cascade-update-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parents", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "children", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-cascade-update-user-" + suffix
	parentID := uuid.New()
	newParentID := uuid.New()
	childID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.parents (id, name)
			VALUES ($1, $2)
		`, schemaIdent), parentID, "Parent"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.children (id, parent_id, name)
			VALUES ($1, $2, $3)
		`, schemaIdent), childID, parentID, "Child")
		return err
	}))

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 2}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.parents
			SET id = $2
			WHERE id = $1
		`, schemaIdent), parentID, newParentID)
		return err
	}))

	type bundleEffect struct {
		table   string
		op      string
		payload []byte
	}
	var effects []bundleEffect
	bundle := loadCommittedBundleForUser(t, ctx, svc, userID, 2)
	for _, row := range bundle.Rows {
		effects = append(effects, bundleEffect{table: row.Table, op: row.Op, payload: row.Payload})
	}
	require.Len(t, effects, 3)

	type effectShape struct {
		table string
		op    string
	}
	var shapes []effectShape
	for _, effect := range effects {
		shapes = append(shapes, effectShape{table: effect.table, op: effect.op})
	}
	require.ElementsMatch(t, []effectShape{
		{table: "parents", op: OpDelete},
		{table: "parents", op: OpInsert},
		{table: "children", op: OpUpdate},
	}, shapes)

	var childPayload map[string]any
	for _, effect := range effects {
		if effect.table == "children" && effect.op == OpUpdate {
			require.NoError(t, json.Unmarshal(effect.payload, &childPayload))
		}
	}
	require.Equal(t, newParentID.String(), childPayload["parent_id"])

	var persistedParentID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT id
		FROM %s.parents
	`, schemaIdent)).Scan(&persistedParentID))
	require.Equal(t, newParentID, persistedParentID)

	var persistedChildParentID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT parent_id
		FROM %s.children
		WHERE id = $1
	`, schemaIdent), childID).Scan(&persistedChildParentID))
	require.Equal(t, newParentID, persistedChildParentID)
}

func TestWithinSyncBundle_CapturesServerSideTriggerWritesOnRegisteredTables(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_trigger_write_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.profiles (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			nickname TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s.create_profile_for_user()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			INSERT INTO %s.profiles (_sync_scope_id, id, nickname)
			VALUES (NEW._sync_scope_id, NEW.id, NEW.name);
			RETURN NEW;
		END;
		$$
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER users_create_profile
		AFTER INSERT ON %s.users
		FOR EACH ROW
		EXECUTE FUNCTION %s.create_profile_for_user()
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "profiles", SyncKeyColumns: []string{"id"}},
	}), AppName: "bundle-trigger-write-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "profiles", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "bundle-trigger-write-user-" + suffix
	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "bootstrap-client")

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, schemaIdent), rowID, "Trigger User", "trigger@example.com")
		return err
	}))

	type triggerEffect struct {
		table string
		op    string
	}
	var effects []triggerEffect
	bundle := loadCommittedBundleForUser(t, ctx, svc, userID, 1)
	for _, row := range bundle.Rows {
		effects = append(effects, triggerEffect{table: row.Table, op: row.Op})
	}
	require.ElementsMatch(t, []triggerEffect{
		{table: "users", op: OpInsert},
		{table: "profiles", op: OpInsert},
	}, effects)

	var nickname string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT nickname
		FROM %s.profiles
		WHERE id = $1
	`, schemaIdent), rowID).Scan(&nickname))
	require.Equal(t, "Trigger User", nickname)
}

type bundleOwnerGuardFixture struct {
	ctx         context.Context
	svc         *SyncService
	pool        *pgxpool.Pool
	schemaIdent string
	rowID       uuid.UUID
}

func newBundleOwnerGuardFixture(t *testing.T, schemaPrefix, appName string) *bundleOwnerGuardFixture {
	t.Helper()

	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := schemaPrefix + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: appName,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	return &bundleOwnerGuardFixture{
		ctx:         ctx,
		svc:         svc,
		pool:        pool,
		schemaIdent: pgx.Identifier{schemaName}.Sanitize(),
		rowID:       uuid.New(),
	}
}

func (f *bundleOwnerGuardFixture) initializeOwner(t *testing.T, ownerID string) {
	t.Helper()

	mustInitializeEmptyScope(t, f.ctx, f.svc, ownerID, "bootstrap-client")
}

func (f *bundleOwnerGuardFixture) source(bundleID int64) BundleSource {
	return BundleSource{SourceID: "server-app", SourceBundleID: bundleID}
}

func (f *bundleOwnerGuardFixture) seedOwnerUser(t *testing.T, ownerID string, bundleID int64) {
	t.Helper()

	f.initializeOwner(t, ownerID)
	require.NoError(t, f.svc.WithinSyncBundle(f.ctx, Actor{UserID: ownerID}, f.source(bundleID), retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, f.schemaIdent), f.rowID, "Owner A", "owner-a@example.com")
		return err
	}))
}

func (f *bundleOwnerGuardFixture) requireRejectedUserWrite(t *testing.T, ownerID string, bundleID int64, expectedMessage, query string, args ...any) {
	t.Helper()

	err := f.svc.WithinSyncBundle(f.ctx, Actor{UserID: ownerID}, f.source(bundleID), retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(query, f.schemaIdent), args...)
		return err
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), expectedMessage)
}

func TestWithinSyncBundle_RejectsMismatchedOwnerOnInsert(t *testing.T) {
	f := newBundleOwnerGuardFixture(t, "bundle_owner_insert_reject_", "bundle-owner-insert-reject-test")
	f.initializeOwner(t, "actor-a")

	f.requireRejectedUserWrite(t, "actor-a", 1, "scope mismatch", `
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, "actor-b", f.rowID, "Mallory", "mallory@example.com")
}

func TestWithinSyncBundle_RejectsCrossOwnerWriteByVisibleKeyAlone(t *testing.T) {
	f := newBundleOwnerGuardFixture(t, "bundle_cross_owner_reject_", "bundle-cross-owner-reject-test")
	f.seedOwnerUser(t, "owner-a", 1)
	f.initializeOwner(t, "owner-b")

	f.requireRejectedUserWrite(t, "owner-b", 1, "scope mismatch", `
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, f.rowID, "Intruder")

	f.requireRejectedUserWrite(t, "owner-b", 1, "scope mismatch", `
			DELETE FROM %s.users
			WHERE id = $1
		`, f.rowID)
}

func TestWithinSyncBundle_RejectsOwnerMutationOnUpdate(t *testing.T) {
	f := newBundleOwnerGuardFixture(t, "bundle_owner_update_reject_", "bundle-owner-update-reject-test")
	f.seedOwnerUser(t, "owner-a", 1)

	f.requireRejectedUserWrite(t, "owner-a", 2, "scope mutation is not allowed", `
			UPDATE %s.users
			SET _sync_scope_id = $2
			WHERE id = $1
		`, f.rowID, "owner-b")
}
