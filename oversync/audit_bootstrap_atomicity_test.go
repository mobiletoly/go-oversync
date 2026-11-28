//go:build oversync_audit

package oversync

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var auditBootstrapManagedTables = []string{
	"meta",
	"table_catalog",
	"user_state",
	"scope_state",
	"source_state",
	"row_state",
	"bundle_capture_stage",
	"bundle_log",
	"bundle_rows",
	"snapshot_sessions",
	"snapshot_session_rows",
}

type auditBootstrapLayoutState struct {
	protocolLabel       string
	layoutName          string
	metaRows            int64
	catalogRows         []expectedTableCatalogRow
	managedTableCount   int64
	captureTriggerCount int64
	truncateGuardCount  int64
}

func TestAuditBootstrapAtomicity_ParallelBootstrapConvergesOnOneReadyLayout(t *testing.T) {
	ctx, pool, schemaName := newAuditBootstrapAtomicityDatabase(t)
	releaseBootstrapLock := holdAuditBootstrapLock(t, ctx, pool)

	services := []*SyncService{
		newAuditBootstrapAtomicityService(t, pool, schemaName),
		newAuditBootstrapAtomicityService(t, pool, schemaName),
	}

	type bootstrapResult struct {
		serviceIndex int
		err          error
	}
	start := make(chan struct{})
	results := make(chan bootstrapResult, len(services))
	for i, service := range services {
		go func(serviceIndex int, svc *SyncService) {
			<-start
			results <- bootstrapResult{serviceIndex: serviceIndex, err: svc.Bootstrap(ctx)}
		}(i, service)
	}
	close(start)

	require.Eventually(t, func() bool {
		_, waiting, err := auditBootstrapLockCounts(ctx, pool)
		return err == nil && waiting == len(services)
	}, 10*time.Second, 10*time.Millisecond, "parallel bootstraps did not both reach the global advisory lock")
	requireAuditBootstrapLayoutAbsent(t, ctx, pool, services[0])

	releaseBootstrapLock()
	bootstrapErrors := make([]error, len(services))
	for range services {
		select {
		case result := <-results:
			bootstrapErrors[result.serviceIndex] = result.err
		case <-time.After(20 * time.Second):
			t.Fatal("parallel bootstraps did not finish after releasing the global advisory lock")
		}
	}
	for _, err := range bootstrapErrors {
		require.NoError(t, err)
	}

	firstState := requireAuditBootstrapReady(t, ctx, pool, schemaName, services[0])
	secondState := requireAuditBootstrapReady(t, ctx, pool, schemaName, services[1])
	require.Equal(t, firstState, secondState)
	require.Eventually(t, func() bool {
		total, waiting, err := auditBootstrapLockCounts(ctx, pool)
		return err == nil && total == 0 && waiting == 0
	}, 5*time.Second, 10*time.Millisecond, "parallel bootstrap leaked the global advisory lock")
}

func TestAuditBootstrapAtomicity_CancelledLockWaitLeavesNoLayoutAndRetryRecovers(t *testing.T) {
	ctx, pool, schemaName := newAuditBootstrapAtomicityDatabase(t)
	service := newAuditBootstrapAtomicityService(t, pool, schemaName)
	releaseBootstrapLock := holdAuditBootstrapLock(t, ctx, pool)

	waitCtx, cancelWait := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		result <- service.Bootstrap(waitCtx)
	}()

	require.Eventually(t, func() bool {
		_, waiting, err := auditBootstrapLockCounts(ctx, pool)
		return err == nil && waiting == 1
	}, 10*time.Second, 10*time.Millisecond, "bootstrap did not reach the global advisory lock wait")
	requireAuditBootstrapLayoutAbsent(t, ctx, pool, service)

	cancelWait()
	select {
	case err := <-result:
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "cancel")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled bootstrap advisory-lock wait did not return")
	}
	require.ErrorIs(t, waitCtx.Err(), context.Canceled)
	requireAuditBootstrapLayoutAbsent(t, ctx, pool, service)
	require.Eventually(t, func() bool {
		total, waiting, err := auditBootstrapLockCounts(ctx, pool)
		return err == nil && total == 1 && waiting == 0 && pool.Stat().AcquiredConns() == 1
	}, 5*time.Second, 10*time.Millisecond, "cancelled bootstrap leaked its lock waiter or pool connection")

	releaseBootstrapLock()
	require.NoError(t, service.Bootstrap(ctx))
	recoveredState := requireAuditBootstrapReady(t, ctx, pool, schemaName, service)

	require.NoError(t, service.Bootstrap(ctx))
	repeatedState := requireAuditBootstrapReady(t, ctx, pool, schemaName, service)
	require.Equal(t, recoveredState, repeatedState, "retry must converge on a repeatable bootstrap layout")
	require.Eventually(t, func() bool {
		total, waiting, err := auditBootstrapLockCounts(ctx, pool)
		return err == nil && total == 0 && waiting == 0 && pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond, "bootstrap retry leaked the global advisory lock or a pool connection")
}

func newAuditBootstrapAtomicityDatabase(t *testing.T) (context.Context, *pgxpool.Pool, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "audit_bootstrap_atomicity_" + strings.ReplaceAll(uuid.New().String(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := dropTestSchema(cleanupCtx, pool, schemaName); err != nil {
			t.Errorf("drop bootstrap atomicity schema: %v", err)
		}
	})
	return ctx, pool, schemaName
}

func newAuditBootstrapAtomicityService(t *testing.T, pool *pgxpool.Pool, schemaName string) *SyncService {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "audit-bootstrap-atomicity",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(closeCtx); err != nil {
			t.Errorf("close bootstrap atomicity service: %v", err)
		}
	})
	return service
}

func holdAuditBootstrapLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) func() {
	t.Helper()

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, syncBootstrapLockKey)
	require.NoError(t, err)

	var once sync.Once
	release := func() {
		once.Do(func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var unlocked bool
			if err := conn.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey).Scan(&unlocked); err != nil {
				t.Errorf("release global bootstrap advisory lock: %v", err)
			} else if !unlocked {
				t.Error("global bootstrap advisory lock was not held by the gate connection")
			}
			conn.Release()
		})
	}
	t.Cleanup(release)
	return release
}

func auditBootstrapLockCounts(ctx context.Context, pool *pgxpool.Pool) (int, int, error) {
	var total, waiting int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE NOT granted)
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND classid::bigint = ($1::bigint >> 32)
		  AND objid::bigint = ($1::bigint & 4294967295::bigint)
		  AND objsubid = 1
	`, syncBootstrapLockKey).Scan(&total, &waiting)
	return total, waiting, err
}

func requireAuditBootstrapLayoutAbsent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	service *SyncService,
) {
	t.Helper()

	var schemaExists, metaExists, catalogExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'sync'),
			to_regclass('sync.meta') IS NOT NULL,
			to_regclass('sync.table_catalog') IS NOT NULL
	`).Scan(&schemaExists, &metaExists, &catalogExists))
	require.False(t, schemaExists, "cancelled or blocked bootstrap exposed a partial sync schema")
	require.False(t, metaExists)
	require.False(t, catalogExists)

	status, err := service.GetStatus(ctx)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, "unhealthy", status.Status)
	require.False(t, status.AcceptingOperations, "service must not report readiness before bootstrap commits")
}

func requireAuditBootstrapReady(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
	service *SyncService,
) auditBootstrapLayoutState {
	t.Helper()

	state := auditBootstrapLayoutState{}
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT protocol_label, layout_name, (SELECT COUNT(*) FROM sync.meta)
		FROM sync.meta
		WHERE singleton_key = TRUE
	`).Scan(&state.protocolLabel, &state.layoutName, &state.metaRows))

	rows, err := pool.Query(ctx, `
		SELECT table_id, schema_name, table_name, sync_key_column, sync_key_kind
		FROM sync.table_catalog
		ORDER BY table_id
	`)
	require.NoError(t, err)
	for rows.Next() {
		var row expectedTableCatalogRow
		require.NoError(t, rows.Scan(
			&row.TableID,
			&row.SchemaName,
			&row.TableName,
			&row.SyncKeyColumn,
			&row.SyncKeyKind,
		))
		state.catalogRows = append(state.catalogRows, row)
	}
	require.NoError(t, rows.Err())
	rows.Close()

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM unnest($1::text[]) AS expected(table_name)
		WHERE to_regclass(format('sync.%I', expected.table_name)) IS NOT NULL
	`, auditBootstrapManagedTables).Scan(&state.managedTableCount))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger AS trigger
		JOIN pg_class AS registered_table ON registered_table.oid = trigger.tgrelid
		JOIN pg_namespace AS registered_schema ON registered_schema.oid = registered_table.relnamespace
		WHERE registered_schema.nspname = $1
		  AND registered_table.relname = 'users'
		  AND trigger.tgname = $2
		  AND NOT trigger.tgisinternal
	`, schemaName, registeredTableCaptureTriggerName).Scan(&state.captureTriggerCount))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger AS trigger
		JOIN pg_class AS registered_table ON registered_table.oid = trigger.tgrelid
		JOIN pg_namespace AS registered_schema ON registered_schema.oid = registered_table.relnamespace
		WHERE registered_schema.nspname = $1
		  AND registered_table.relname = 'users'
		  AND trigger.tgname = $2
		  AND NOT trigger.tgisinternal
	`, schemaName, registeredTableTruncateGuardTrigger).Scan(&state.truncateGuardCount))

	expectedCatalog, err := service.expectedTableCatalogRows()
	require.NoError(t, err)
	require.Equal(t, syncSchemaProtocolLabel, state.protocolLabel)
	require.Equal(t, syncSchemaLayoutName, state.layoutName)
	require.Equal(t, int64(1), state.metaRows)
	require.Equal(t, expectedCatalog, state.catalogRows)
	require.Equal(t, int64(len(auditBootstrapManagedTables)), state.managedTableCount)
	require.Equal(t, int64(1), state.captureTriggerCount)
	require.Equal(t, int64(1), state.truncateGuardCount)

	status, err := service.GetStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, "healthy", status.Status)
	require.Equal(t, string(serviceLifecycleRunning), status.Lifecycle)
	require.True(t, status.AcceptingOperations)
	return state
}
