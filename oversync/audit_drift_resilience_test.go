//go:build oversync_audit

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestAuditDriftResilience_MarkedLayoutRejectsManagedObjectDrift(t *testing.T) {
	t.Run("missing_managed_index", func(t *testing.T) {
		ctx := context.Background()
		fixture := newAuditDatabaseContractFixture(t, ctx, "driftindex", auditDatabaseContractOptions{})
		_, err := fixture.pool.Exec(ctx, `DROP INDEX sync.rs_user_live_snapshot_idx`)
		require.NoError(t, err)

		second := newAuditDriftService(t, fixture, "audit-drift-missing-index")
		err = second.Bootstrap(ctx)
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr)

		var exists bool
		require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT to_regclass('sync.rs_user_live_snapshot_idx') IS NOT NULL`).Scan(&exists))
		require.False(t, exists, "validation must reject without recreating the missing managed index")
		requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	})

	t.Run("missing_managed_constraint", func(t *testing.T) {
		ctx := context.Background()
		fixture := newAuditDatabaseContractFixture(t, ctx, "driftconstraint", auditDatabaseContractOptions{})
		_, err := fixture.pool.Exec(ctx, `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_op_code_chk`)
		require.NoError(t, err)

		second := newAuditDriftService(t, fixture, "audit-drift-missing-constraint")
		err = second.Bootstrap(ctx)
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr)

		var exists bool
		require.NoError(t, fixture.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_constraint
				WHERE conrelid = 'sync.bundle_rows'::regclass
				  AND conname = 'bundle_rows_op_code_chk'
			)
		`).Scan(&exists))
		require.False(t, exists, "validation must reject without recreating the missing managed constraint")
		requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	})

	t.Run("drifted_capture_function", func(t *testing.T) {
		ctx := context.Background()
		fixture := newAuditDatabaseContractFixture(t, ctx, "driftfunction", auditDatabaseContractOptions{})
		_, err := fixture.pool.Exec(ctx, `
			CREATE OR REPLACE FUNCTION sync.capture_registered_row_change()
			RETURNS TRIGGER
			LANGUAGE plpgsql
			AS $$
			BEGIN
				IF TG_OP = 'DELETE' THEN
					RETURN OLD;
				END IF;
				RETURN NEW;
			END;
			$$
		`)
		require.NoError(t, err)

		second := newAuditDriftService(t, fixture, "audit-drift-capture-function")
		var before string
		require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT prosrc FROM pg_proc WHERE oid = 'sync.capture_registered_row_change()'::regprocedure`).Scan(&before))
		err = second.Bootstrap(ctx)
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr)
		var after string
		require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT prosrc FROM pg_proc WHERE oid = 'sync.capture_registered_row_change()'::regprocedure`).Scan(&after))
		require.Equal(t, before, after)
		require.Zero(t, fixture.businessUserCount(t, ctx))
		require.Zero(t, fixture.committedBundleCount(t, ctx))
		requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	})
}

func TestAuditDriftResilience_BootstrapRejectsDriftedRegisteredTableTriggerWithoutRepair(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "drifttrigger", auditDatabaseContractOptions{})
	tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
	_, err := fixture.pool.Exec(ctx, fmt.Sprintf(`
		DROP TRIGGER %s ON %s;
		CREATE TRIGGER %s
		AFTER INSERT OR UPDATE OR DELETE ON %s
		FOR EACH ROW EXECUTE FUNCTION sync.enforce_registered_row_owner()
	`, registeredTableCaptureTriggerName, tableIdent, registeredTableCaptureTriggerName, tableIdent))
	require.NoError(t, err)

	second := newAuditDriftService(t, fixture, "audit-drift-capture-trigger")
	err = second.Bootstrap(ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)

	var triggerDefinition string
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT pg_get_triggerdef(oid)
		FROM pg_trigger
		WHERE tgrelid = to_regclass($1)
		  AND tgname = $2
		  AND NOT tgisinternal
	`, fixture.schemaName+".users", registeredTableCaptureTriggerName).Scan(&triggerDefinition))
	require.Contains(t, triggerDefinition, "enforce_registered_row_owner")
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT pg_get_triggerdef(oid)
		FROM pg_trigger
		WHERE tgrelid = to_regclass($1)
		  AND tgname = $2
		  AND NOT tgisinternal
	`, fixture.schemaName+".users", registeredTableTruncateGuardTrigger).Scan(&triggerDefinition))
	require.Contains(t, triggerDefinition, "BEFORE TRUNCATE")
	require.Contains(t, triggerDefinition, "reject_registered_table_truncate")

	require.Zero(t, fixture.businessUserCount(t, ctx))
	require.Zero(t, fixture.committedBundleCount(t, ctx))
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}

func TestAuditExpectedDriftResilience_MissingManagedConstraintRejectsBootstrap(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "expectedconstraint", auditDatabaseContractOptions{})
	_, err := fixture.pool.Exec(ctx, `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_op_code_chk`)
	require.NoError(t, err)

	second := newAuditDriftService(t, fixture, "audit-expected-missing-constraint")
	err = second.Bootstrap(ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr, "a marked layout missing a managed constraint must fail closed as unsupported")
}

func newAuditDriftService(t *testing.T, fixture *auditDatabaseContractFixture, appName string) *SyncService {
	t.Helper()
	service, err := newTestRuntimeService(
		fixture.pool,
		fixture.newServiceConfig(appName),
		integrationTestLogger(slog.LevelWarn),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}
