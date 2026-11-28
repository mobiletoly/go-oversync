package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestBootstrap_CreatesExplicitLayoutMarkerAndExactTableCatalog(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "schema_bootstrap_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), AppName: "schema-bootstrap-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	require.NoError(t, svc.Bootstrap(ctx))

	var (
		metaExists                       bool
		userStateExists                  bool
		tableCatalogExists               bool
		scopeStateExists                 bool
		sourceStateExists                bool
		bundleCaptureStageExists         bool
		rowStateExists                   bool
		bundleLogExists                  bool
		bundleRowsExists                 bool
		appliedPushesExists              bool
		snapshotSessionsExists           bool
		snapshotSessionRowsExists        bool
		acceptedPushReplaySeqExists      bool
		rejectedRegisteredWriteSeqExists bool
		historyPrunedErrorSeqExists      bool
		bundleCaptureIndexExists         bool
		rowStateSnapshotIndexExists      bool
		bundleRowsKeyIndexExists         bool
		snapshotSessionsTTLIndexExists   bool
		oldReadinessIndexExists          bool
		ownerGuardFunctionExists         bool
		captureFunctionExists            bool
		snapshotLastAccessedColumnCount  int
		userStateUpdatedAtColumnCount    int
		scopeStateTextColumnCount        int
		scopeStateCodeColumnCount        int
		sourceStatePKCount               int
		sourceStateStateColumnCount      int
		sourceStateMaxColumnCount        int
		sourceStateReplacedColumnCount   int
		sourceStateReasonColumnCount     int
		sourceStateStateChkCount         int
		sourceStateMaxChkCount           int
		sourceStateActiveChkCount        int
		sourceStateReservedChkCount      int
		sourceStateRetiredChkCount       int
		layoutProtocolLabel              string
		layoutName                       string
	)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			to_regclass('sync.meta') IS NOT NULL,
			to_regclass('sync.user_state') IS NOT NULL,
			to_regclass('sync.table_catalog') IS NOT NULL,
			to_regclass('sync.scope_state') IS NOT NULL,
			to_regclass('sync.source_state') IS NOT NULL,
			to_regclass('sync.bundle_capture_stage') IS NOT NULL,
			to_regclass('sync.row_state') IS NOT NULL,
			to_regclass('sync.bundle_log') IS NOT NULL,
			to_regclass('sync.bundle_rows') IS NOT NULL,
			to_regclass('sync.applied_pushes') IS NOT NULL,
			to_regclass('sync.snapshot_sessions') IS NOT NULL,
			to_regclass('sync.snapshot_session_rows') IS NOT NULL,
			to_regclass('sync.accepted_push_replay_seq') IS NOT NULL,
			to_regclass('sync.rejected_registered_write_seq') IS NOT NULL,
			to_regclass('sync.history_pruned_error_seq') IS NOT NULL,
			to_regclass('sync.bcs_tx_user_ordinal_idx') IS NOT NULL,
			to_regclass('sync.rs_user_live_snapshot_idx') IS NOT NULL,
			to_regclass('sync.br_user_bundle_key_idx') IS NOT NULL,
			to_regclass('sync.ss_expires_at_idx') IS NOT NULL,
			to_regclass('sync.rs_user_table_bundle_idx') IS NOT NULL,
			to_regprocedure('sync.enforce_registered_row_owner()') IS NOT NULL,
			to_regprocedure('sync.capture_registered_row_change()') IS NOT NULL
	`).Scan(
		&metaExists,
		&userStateExists,
		&tableCatalogExists,
		&scopeStateExists,
		&sourceStateExists,
		&bundleCaptureStageExists,
		&rowStateExists,
		&bundleLogExists,
		&bundleRowsExists,
		&appliedPushesExists,
		&snapshotSessionsExists,
		&snapshotSessionRowsExists,
		&acceptedPushReplaySeqExists,
		&rejectedRegisteredWriteSeqExists,
		&historyPrunedErrorSeqExists,
		&bundleCaptureIndexExists,
		&rowStateSnapshotIndexExists,
		&bundleRowsKeyIndexExists,
		&snapshotSessionsTTLIndexExists,
		&oldReadinessIndexExists,
		&ownerGuardFunctionExists,
		&captureFunctionExists,
	))

	require.True(t, metaExists)
	require.True(t, userStateExists)
	require.True(t, tableCatalogExists)
	require.True(t, scopeStateExists)
	require.True(t, sourceStateExists)
	require.True(t, bundleCaptureStageExists)
	require.True(t, rowStateExists)
	require.True(t, bundleLogExists)
	require.True(t, bundleRowsExists)
	require.False(t, appliedPushesExists)
	require.True(t, snapshotSessionsExists)
	require.True(t, snapshotSessionRowsExists)
	require.True(t, acceptedPushReplaySeqExists)
	require.True(t, rejectedRegisteredWriteSeqExists)
	require.True(t, historyPrunedErrorSeqExists)
	require.True(t, bundleCaptureIndexExists)
	require.True(t, rowStateSnapshotIndexExists)
	require.False(t, bundleRowsKeyIndexExists)
	require.True(t, snapshotSessionsTTLIndexExists)
	require.False(t, oldReadinessIndexExists)
	require.True(t, ownerGuardFunctionExists)
	require.True(t, captureFunctionExists)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT protocol_label, layout_name
		FROM sync.meta
		WHERE singleton_key = TRUE
	`).Scan(&layoutProtocolLabel, &layoutName))
	require.Equal(t, syncSchemaProtocolLabel, layoutProtocolLabel)
	require.Equal(t, syncSchemaLayoutName, layoutName)

	rows, err := pool.Query(ctx, `
		SELECT table_id, schema_name, table_name, sync_key_column, sync_key_kind
		FROM sync.table_catalog
		ORDER BY table_id
	`)
	require.NoError(t, err)
	defer rows.Close()

	var catalogRows []expectedTableCatalogRow
	for rows.Next() {
		var row expectedTableCatalogRow
		require.NoError(t, rows.Scan(&row.TableID, &row.SchemaName, &row.TableName, &row.SyncKeyColumn, &row.SyncKeyKind))
		catalogRows = append(catalogRows, row)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []expectedTableCatalogRow{
		{TableID: 1, SchemaName: schemaName, TableName: "files", SyncKeyColumn: "id", SyncKeyKind: syncKeyKindUUIDCode},
		{TableID: 2, SchemaName: schemaName, TableName: "users", SyncKeyColumn: "id", SyncKeyKind: syncKeyKindUUIDCode},
	}, catalogRows)

	fkRows, err := pool.Query(ctx, `
		SELECT relation.relname, attribute.attname, constraint_entry.confdeltype::text
		FROM pg_constraint constraint_entry
		JOIN pg_class relation ON relation.oid = constraint_entry.conrelid
		JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
		JOIN unnest(constraint_entry.conkey) WITH ORDINALITY AS key(attnum, ordinal) ON TRUE
		JOIN pg_attribute attribute ON attribute.attrelid = relation.oid AND attribute.attnum = key.attnum
		WHERE namespace.nspname='sync'
		  AND constraint_entry.contype='f'
		  AND constraint_entry.confrelid='sync.table_catalog'::regclass
		ORDER BY relation.relname, attribute.attname
	`)
	require.NoError(t, err)
	var catalogForeignKeys []string
	for fkRows.Next() {
		var tableName, columnName, deleteAction string
		require.NoError(t, fkRows.Scan(&tableName, &columnName, &deleteAction))
		catalogForeignKeys = append(catalogForeignKeys, tableName+"."+columnName+":"+deleteAction)
	}
	require.NoError(t, fkRows.Err())
	fkRows.Close()
	require.Equal(t, []string{
		"bundle_capture_stage.table_id:a",
		"bundle_rows.table_id:a",
		"row_state.table_id:a",
		"snapshot_session_rows.table_id:a",
	}, catalogForeignKeys)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'snapshot_sessions'
		  AND column_name = 'last_accessed_at'
	`).Scan(&snapshotLastAccessedColumnCount))
	require.Zero(t, snapshotLastAccessedColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'user_state'
		  AND column_name = 'updated_at'
	`).Scan(&userStateUpdatedAtColumnCount))
	require.Zero(t, userStateUpdatedAtColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'scope_state'
		  AND column_name = 'state'
	`).Scan(&scopeStateTextColumnCount))
	require.Zero(t, scopeStateTextColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'scope_state'
		  AND column_name = 'state_code'
	`).Scan(&scopeStateCodeColumnCount))
	require.Equal(t, 1, scopeStateCodeColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.table_constraints
		WHERE table_schema = 'sync'
		  AND table_name = 'source_state'
		  AND constraint_type = 'PRIMARY KEY'
	`).Scan(&sourceStatePKCount))
	require.Equal(t, 1, sourceStatePKCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'source_state'
		  AND column_name = 'state'
	`).Scan(&sourceStateStateColumnCount))
	require.Equal(t, 1, sourceStateStateColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'source_state'
		  AND column_name = 'max_committed_source_bundle_id'
	`).Scan(&sourceStateMaxColumnCount))
	require.Equal(t, 1, sourceStateMaxColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'source_state'
		  AND column_name = 'replaced_by_source_id'
	`).Scan(&sourceStateReplacedColumnCount))
	require.Equal(t, 1, sourceStateReplacedColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'sync'
		  AND table_name = 'source_state'
		  AND column_name = 'retirement_reason'
	`).Scan(&sourceStateReasonColumnCount))
	require.Equal(t, 1, sourceStateReasonColumnCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'sync'
		  AND t.relname = 'source_state'
		  AND c.conname = 'source_state_state_chk'
	`).Scan(&sourceStateStateChkCount))
	require.Equal(t, 1, sourceStateStateChkCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'sync'
		  AND t.relname = 'source_state'
		  AND c.conname = 'source_state_max_committed_chk'
	`).Scan(&sourceStateMaxChkCount))
	require.Equal(t, 1, sourceStateMaxChkCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'sync'
		  AND t.relname = 'source_state'
		  AND c.conname = 'source_state_active_chk'
	`).Scan(&sourceStateActiveChkCount))
	require.Equal(t, 1, sourceStateActiveChkCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'sync'
		  AND t.relname = 'source_state'
		  AND c.conname = 'source_state_reserved_chk'
	`).Scan(&sourceStateReservedChkCount))
	require.Equal(t, 1, sourceStateReservedChkCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = 'sync'
		  AND t.relname = 'source_state'
		  AND c.conname = 'source_state_retired_chk'
	`).Scan(&sourceStateRetiredChkCount))
	require.Equal(t, 1, sourceStateRetiredChkCount)
}

func TestBootstrap_FailsClosedWhenRegisteredTableCatalogDiffers(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "table_catalog_mismatch_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	firstSvc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "schema-bootstrap-first",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	require.NoError(t, err)
	require.NoError(t, firstSvc.Bootstrap(ctx))

	secondSvc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), AppName: "schema-bootstrap-second",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	require.NoError(t, err)

	err = secondSvc.Bootstrap(ctx)
	require.Error(t, err)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogActiveTableOmitted, catalogErr.Code)
	require.Equal(t, schemaName, catalogErr.Schema)
	require.Equal(t, "users", catalogErr.Table)
}

func TestBootstrap_FailsClosedForLegacySyncSchemaWithoutLayoutMarker(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	_, err := pool.Exec(ctx, `CREATE SCHEMA sync`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		CREATE TABLE sync.user_state (
			user_id TEXT PRIMARY KEY
		)
	`)
	require.NoError(t, err)

	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(runtimeServiceTestRegisteredTables()), AppName: "legacy-sync-layout-test",
		RegisteredTables:               runtimeServiceTestRegisteredTables(),
		RegisteredTableCatalogRevision: 1,
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var catalogErr *RegisteredTableCatalogError
	require.ErrorAs(t, err, &catalogErr)
	require.Equal(t, RegisteredTableCatalogLayoutReplacementRequired, catalogErr.Code)
	require.Equal(t, "<missing>", catalogErr.Actual)
}

type schemaBootstrapFailureHarness struct {
	ctx         context.Context
	logger      *slog.Logger
	pool        *pgxpool.Pool
	schemaName  string
	schemaIdent string
}

func newSchemaBootstrapFailureHarness(t *testing.T, schemaPrefix string) *schemaBootstrapFailureHarness {
	t.Helper()

	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := schemaPrefix + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() {
		_ = dropTestSchema(context.Background(), pool, schemaName)
	})

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)

	return &schemaBootstrapFailureHarness{
		ctx:         ctx,
		logger:      logger,
		pool:        pool,
		schemaName:  schemaName,
		schemaIdent: schemaIdent,
	}
}

func (h *schemaBootstrapFailureHarness) execf(t *testing.T, query string, args ...any) {
	t.Helper()

	_, err := h.pool.Exec(h.ctx, fmt.Sprintf(query, args...))
	require.NoError(t, err)
}

func (h *schemaBootstrapFailureHarness) registeredTable(table string, syncKeyColumns ...string) RegisteredTable {
	return RegisteredTable{Schema: h.schemaName, Table: table, SyncKeyColumns: syncKeyColumns}
}

func (h *schemaBootstrapFailureHarness) requireSuccessfulBootstrap(
	t *testing.T,
	appName string,
	registeredTables []RegisteredTable,
) {
	t.Helper()

	svc, err := newTestRuntimeService(h.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registeredTables), AppName: appName,
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}, h.logger)
	require.NoError(t, err)

	require.NoError(t, svc.Bootstrap(h.ctx))
	require.NoError(t, svc.Close(context.Background()))
}

func (h *schemaBootstrapFailureHarness) requireSuccessfulBootstrapWithGenerations(
	t *testing.T,
	appName string,
	registeredTables []RegisteredTable,
	generations []SchemaGeneration,
) {
	t.Helper()

	svc, err := newTestRuntimeService(h.pool, &ServiceConfig{
		SchemaGenerations:              generations,
		AppName:                        appName,
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}, h.logger)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(h.ctx))
	require.NoError(t, svc.Close(context.Background()))
}

func (h *schemaBootstrapFailureHarness) requireUnsupportedBootstrap(
	t *testing.T,
	appName string,
	registeredTables []RegisteredTable,
	expectedMessages ...string,
) {
	t.Helper()

	err := h.unsupportedBootstrapError(t, appName, registeredTables)
	for _, expected := range expectedMessages {
		require.Contains(t, err.Error(), expected)
	}
}

func (h *schemaBootstrapFailureHarness) unsupportedBootstrapError(
	t *testing.T,
	appName string,
	registeredTables []RegisteredTable,
) error {
	t.Helper()

	svc, err := newTestRuntimeService(h.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registeredTables), AppName: appName,
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}, h.logger)
	require.NoError(t, err)

	err = svc.Bootstrap(h.ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.NoError(t, svc.Close(context.Background()))
	return err
}

func requireNoSyncLayoutOrCaptureTriggers(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
) {
	t.Helper()

	var syncSchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
	require.False(t, syncSchemaExists)

	var captureTriggerCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger AS trigger
		JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1
		  AND trigger.tgname IN ($2, $3, $4)
		  AND NOT trigger.tgisinternal
	`, schemaName, registeredTableCaptureTriggerName, registeredTableOwnerGuardTrigger, registeredTableTruncateGuardTrigger).Scan(&captureTriggerCount))
	require.Zero(t, captureTriggerCount)
}

func TestBootstrap_RejectsNullableRegisteredIdentityDeclarations(t *testing.T) {
	tests := []struct {
		name      string
		ownerDecl string
		idDecl    string
		slugDecl  string
		keyColumn string
		role      string
	}{
		{name: "nullable scope", ownerDecl: "TEXT", idDecl: "UUID NOT NULL", slugDecl: "TEXT NOT NULL", keyColumn: "id", role: "scope"},
		{name: "nullable uuid key", ownerDecl: "TEXT NOT NULL", idDecl: "UUID", slugDecl: "TEXT NOT NULL", keyColumn: "id", role: "sync key"},
		{name: "nullable text key", ownerDecl: "TEXT NOT NULL", idDecl: "UUID NOT NULL", slugDecl: "TEXT", keyColumn: "slug", role: "sync key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newSchemaBootstrapFailureHarness(t, "nullable_identity_")
			h.execf(t, `
				CREATE TABLE %s.records (
					row_pk BIGSERIAL,
					id %s,
					slug %s,
					_sync_scope_id %s,
					payload TEXT NOT NULL,
					CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, %s)
				)
			`, h.schemaIdent, tt.idDecl, tt.slugDecl, tt.ownerDecl, tt.keyColumn)
			err := h.unsupportedBootstrapError(t, "nullable-identity", []RegisteredTable{h.registeredTable("records", tt.keyColumn)})
			require.Contains(t, err.Error(), "nullable identity")
			require.Contains(t, err.Error(), h.schemaName+".records")
			require.Contains(t, err.Error(), tt.role)
			require.Contains(t, err.Error(), "NOT NULL")
			requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)
		})
	}
}

func TestBootstrap_RejectsPreexistingNullIdentityRowsAtomically(t *testing.T) {
	for _, tt := range []struct {
		name       string
		ownerValue string
		keyValue   string
	}{
		{name: "null scope", ownerValue: "NULL", keyValue: "'11111111-1111-1111-1111-111111111111'"},
		{name: "null key", ownerValue: "'owner-a'", keyValue: "NULL"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newSchemaBootstrapFailureHarness(t, "nullable_rows_")
			h.execf(t, `
				CREATE TABLE %s.records (
					id UUID,
					_sync_scope_id TEXT,
					payload TEXT NOT NULL,
					CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
				);
				INSERT INTO %s.records(id, _sync_scope_id, payload) VALUES (%s, %s, 'keep');
			`, h.schemaIdent, h.schemaIdent, tt.keyValue, tt.ownerValue)
			h.requireUnsupportedBootstrap(t, "nullable-rows", []RegisteredTable{h.registeredTable("records", "id")}, "nullable identity", "NOT NULL")
			var rows int
			require.NoError(t, h.pool.QueryRow(h.ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.records WHERE payload = 'keep'`, h.schemaIdent)).Scan(&rows))
			require.Equal(t, 1, rows)
			requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)
		})
	}
}

func TestBootstrap_ValidatesPartitionIdentityNullability(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "nullable_partition_")
	h.execf(t, `
		CREATE TABLE %s.records (
			id UUID NOT NULL,
			_sync_scope_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		) PARTITION BY HASH (_sync_scope_id);
		CREATE TABLE %s.records_p0 PARTITION OF %s.records
			FOR VALUES WITH (MODULUS 2, REMAINDER 0);
		CREATE TABLE %s.records_p1 PARTITION OF %s.records
			FOR VALUES WITH (MODULUS 2, REMAINDER 1);
	`, h.schemaIdent, h.schemaIdent, h.schemaIdent, h.schemaIdent, h.schemaIdent)
	h.requireSuccessfulBootstrap(t, "valid-partition-identities", []RegisteredTable{h.registeredTable("records", "id")})
}

func TestBootstrap_NullableIdentityConcurrency(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "nullable_concurrency_")
	h.execf(t, `
		CREATE TABLE %s.records (
			id UUID NOT NULL,
			_sync_scope_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
		)
	`, h.schemaIdent)

	gate, err := h.pool.Acquire(h.ctx)
	require.NoError(t, err)
	gateHeld := true
	t.Cleanup(func() {
		if gateHeld {
			var unlocked bool
			_ = gate.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey).Scan(&unlocked)
		}
		gate.Release()
	})
	_, err = gate.Exec(h.ctx, `SELECT pg_advisory_lock($1)`, syncBootstrapLockKey)
	require.NoError(t, err)

	svc, err := newTestRuntimeService(h.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{h.registeredTable("records", "id")}), AppName: "nullable-concurrency",
		RegisteredTables:               []RegisteredTable{h.registeredTable("records", "id")},
		RegisteredTableCatalogRevision: 1,
	}, h.logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })

	bootstrapResult := make(chan error, 1)
	go func() {
		bootstrapResult <- svc.Bootstrap(h.ctx)
	}()
	require.Eventually(t, func() bool {
		var waiters int
		err := h.pool.QueryRow(h.ctx, `
			SELECT COUNT(*)
			FROM pg_locks
			WHERE locktype = 'advisory'
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			  AND classid::bigint = ($1::bigint >> 32)
			  AND objid::bigint = ($1::bigint & 4294967295::bigint)
			  AND objsubid = 1
			  AND NOT granted
		`, syncBootstrapLockKey).Scan(&waiters)
		return err == nil && waiters == 1
	}, 10*time.Second, 10*time.Millisecond, "bootstrap never reached the advisory-lock wait")

	// The pool preflight observed a valid declaration. The locked authoritative
	// pass must reload the catalog after this concurrent DDL commits.
	h.execf(t, `ALTER TABLE %s.records ALTER COLUMN id DROP NOT NULL`, h.schemaIdent)
	var unlocked bool
	require.NoError(t, gate.QueryRow(h.ctx, `SELECT pg_advisory_unlock($1)`, syncBootstrapLockKey).Scan(&unlocked))
	require.True(t, unlocked)
	gateHeld = false

	select {
	case err := <-bootstrapResult:
		require.Error(t, err)
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr)
		require.Contains(t, err.Error(), "nullable identity")
		require.Contains(t, err.Error(), "sync key")
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap did not finish after releasing the advisory lock")
	}
	requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)

	h.execf(t, `ALTER TABLE %s.records ALTER COLUMN id SET NOT NULL`, h.schemaIdent)
	require.NoError(t, svc.Bootstrap(h.ctx))
}

func TestBootstrap_NullableIdentityRollbackReadinessAndRetry(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "nullable_retry_")
	h.execf(t, `
		CREATE TABLE %s.records (
			id UUID,
			_sync_scope_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
		)
	`, h.schemaIdent)
	svc, err := newTestRuntimeService(h.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{h.registeredTable("records", "id")}), AppName: "nullable-retry",
		RegisteredTables:               []RegisteredTable{h.registeredTable("records", "id")},
		RegisteredTableCatalogRevision: 1,
	}, h.logger)
	require.NoError(t, err)
	err = svc.Bootstrap(h.ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	_, err = svc.Connect(h.ctx, Actor{UserID: "owner-a", SourceID: "reader"}, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)
	requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)

	h.execf(t, `ALTER TABLE %s.records ALTER COLUMN id SET NOT NULL`, h.schemaIdent)
	require.NoError(t, svc.Bootstrap(h.ctx))
	_, err = svc.Connect(h.ctx, Actor{UserID: "owner-a", SourceID: "reader"}, testSchemaGenerationProofForService(svc), &ConnectRequest{})
	require.NoError(t, err)
	require.NoError(t, svc.Close(context.Background()))
}

func TestBootstrap_RejectsPreH3IdentityFunctionsForMarkedLayoutWithoutRepair(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "nullable_upgrade_")
	h.execf(t, `
		CREATE TABLE %s.records (
			id UUID NOT NULL,
			_sync_scope_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
		)
	`, h.schemaIdent)
	registeredTables := []RegisteredTable{h.registeredTable("records", "id")}
	h.requireSuccessfulBootstrap(t, "nullable-upgrade-initial", registeredTables)

	// Model a marked layout whose managed functions still have pre-H3 bodies.
	h.execf(t, `
		CREATE OR REPLACE FUNCTION sync.enforce_registered_row_owner()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF TG_OP = 'DELETE' THEN
				RETURN OLD;
			END IF;
			RETURN NEW;
		END;
		$$;
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
		$$;
	`)

	before := make(map[string]string)
	for _, functionName := range []string{"sync.enforce_registered_row_owner()", "sync.capture_registered_row_change()"} {
		var body string
		require.NoError(t, h.pool.QueryRow(h.ctx, `SELECT prosrc FROM pg_proc WHERE oid = $1::regprocedure`, functionName).Scan(&body))
		before[functionName] = body
	}
	err := h.unsupportedBootstrapError(t, "nullable-upgrade-marked", registeredTables)
	require.Contains(t, err.Error(), "managed sync layout mismatch")
	require.Contains(t, err.Error(), "source.sha256")
	for functionName, expectedBody := range before {
		var actualBody string
		require.NoError(t, h.pool.QueryRow(h.ctx, `SELECT prosrc FROM pg_proc WHERE oid = $1::regprocedure`, functionName).Scan(&actualBody))
		require.Equal(t, expectedBody, actualBody, "marked-layout validation must not replace %s", functionName)
	}
}

func TestBootstrap_RequiresPermanentRegisteredTables(t *testing.T) {
	t.Parallel()
	t.Run("permanent table accepted", func(t *testing.T) {
		h := newSchemaBootstrapFailureHarness(t, "permanent_accept_")
		h.execf(t, `
			CREATE TABLE %s.records (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)`, h.schemaIdent)

		h.requireSuccessfulBootstrap(t, "permanent-accepted", []RegisteredTable{h.registeredTable("records", "id")})
	})

	t.Run("unlogged table rejected atomically", func(t *testing.T) {
		h := newSchemaBootstrapFailureHarness(t, "unlogged_reject_")
		h.execf(t, `
			CREATE UNLOGGED TABLE %s.records (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)`, h.schemaIdent)

		h.requireUnsupportedBootstrap(
			t,
			"unlogged-rejected",
			[]RegisteredTable{h.registeredTable("records", "id")},
			h.schemaName+".records",
			"UNLOGGED",
			`relpersistence="u"`,
		)
		requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)
	})

	t.Run("temporary table on another connection rejected atomically", func(t *testing.T) {
		h := newSchemaBootstrapFailureHarness(t, "temporary_reject_")
		tempConn, err := h.pool.Acquire(h.ctx)
		require.NoError(t, err)
		t.Cleanup(tempConn.Release)

		_, err = tempConn.Exec(h.ctx, `
			CREATE TEMP TABLE records (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)`)
		require.NoError(t, err)

		var tempSchema string
		require.NoError(t, tempConn.QueryRow(h.ctx, `
			SELECT nspname
			FROM pg_namespace
			WHERE oid = pg_my_temp_schema()
		`).Scan(&tempSchema))

		h.requireUnsupportedBootstrap(
			t,
			"temporary-rejected",
			[]RegisteredTable{{Schema: tempSchema, Table: "records", SyncKeyColumns: []string{"id"}}},
			tempSchema+".records",
			"TEMPORARY",
			`relpersistence="t"`,
		)
		requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, tempSchema)
	})

	t.Run("mixed registrations report every offender deterministically", func(t *testing.T) {
		h := newSchemaBootstrapFailureHarness(t, "mixed_persistence_reject_")
		for _, table := range []struct {
			name     string
			unlogged bool
		}{
			{name: "logged_records"},
			{name: "z_unlogged", unlogged: true},
			{name: "a_unlogged", unlogged: true},
		} {
			persistence := ""
			if table.unlogged {
				persistence = "UNLOGGED "
			}
			h.execf(t, `
				CREATE %sTABLE %s.%s (
					_sync_scope_id TEXT NOT NULL,
					id UUID NOT NULL,
					PRIMARY KEY (_sync_scope_id, id)
				)`, persistence, h.schemaIdent, table.name)
		}

		err := h.unsupportedBootstrapError(t, "mixed-persistence-rejected", []RegisteredTable{
			h.registeredTable("z_unlogged", "id"),
			h.registeredTable("logged_records", "id"),
			h.registeredTable("a_unlogged", "id"),
		})
		a := h.schemaName + ".a_unlogged"
		z := h.schemaName + ".z_unlogged"
		require.Contains(t, err.Error(), a)
		require.Contains(t, err.Error(), z)
		require.Less(t, strings.Index(err.Error(), a), strings.Index(err.Error(), z))
		requireNoSyncLayoutOrCaptureTriggers(t, h.ctx, h.pool, h.schemaName)
	})

	t.Run("existing layout fast path still revalidates persistence", func(t *testing.T) {
		h := newSchemaBootstrapFailureHarness(t, "repeat_persistence_reject_")
		h.execf(t, `
			CREATE TABLE %s.records (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)`, h.schemaIdent)
		registered := []RegisteredTable{h.registeredTable("records", "id")}
		h.requireSuccessfulBootstrap(t, "repeat-persistence-initial", registered)
		h.execf(t, `ALTER TABLE %s.records SET UNLOGGED`, h.schemaIdent)

		h.requireUnsupportedBootstrap(
			t,
			"repeat-persistence-rejected",
			registered,
			h.schemaName+".records",
			"UNLOGGED",
		)

		var metaRows int64
		require.NoError(t, h.pool.QueryRow(h.ctx, `SELECT COUNT(*) FROM sync.meta`).Scan(&metaRows))
		require.Equal(t, int64(1), metaRows)
	})
}

func TestBootstrap_FailsWhenRegisteredTablesAreNotFKClosed(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_closure_reject_")
	h.execf(t, `
		CREATE TABLE %s.users (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)
	h.execf(t, `
		CREATE TABLE %s.posts (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			author_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT posts_author_id_fkey
				FOREIGN KEY (_sync_scope_id, author_id) REFERENCES %s.users(_sync_scope_id, id)
		)`, h.schemaIdent, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"fk-closure-reject-test",
		[]RegisteredTable{h.registeredTable("posts", "id")},
		"not FK-closed",
		h.schemaName+".posts",
		h.schemaName+".users",
	)
}

func TestBootstrap_AllowsSelfReferencingRegisteredTable(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_self_ref_ok_")
	h.execf(t, `
		CREATE TABLE %s.categories (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT categories_parent_id_fkey
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.categories(_sync_scope_id, id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, h.schemaIdent, h.schemaIdent)

	h.requireSuccessfulBootstrap(t,
		"fk-self-ref-ok-test",
		[]RegisteredTable{h.registeredTable("categories", "id")},
	)
}

func TestBootstrap_AcceptsTextVisibleSyncKey(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_key_type_reject_")
	h.execf(t, `
		CREATE TABLE %s.products (
			_sync_scope_id TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, code)
		)`, h.schemaIdent)

	registered := []RegisteredTable{h.registeredTable("products", "code")}
	h.requireSuccessfulBootstrapWithGenerations(t,
		"fk-key-type-accept-test",
		registered,
		testSchemaGenerationsForRegisteredTablesWithWireTypes(registered, map[string]GenerationWireType{
			Key(h.schemaName, "products") + ".code": GenerationWireTypeText,
		}),
	)
}

func TestBootstrap_FailsWhenRegisteredTableUsesUnsupportedNumericSyncKey(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_composite_key_reject_")
	h.execf(t, `
		CREATE TABLE %s.memberships (
			_sync_scope_id TEXT NOT NULL,
			membership_no BIGINT NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, membership_no)
		)`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"fk-key-type-reject-test",
		[]RegisteredTable{h.registeredTable("memberships", "membership_no")},
		"allows only uuid and text",
		h.schemaName+".memberships",
	)
}

func TestBootstrap_FailsWhenRegisteredTableUsesUnsupportedIntegerSyncKey(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_integer_key_reject_")
	h.execf(t, `
		CREATE TABLE %s.counters (
			_sync_scope_id TEXT NOT NULL,
			counter_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, counter_id)
		)`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"fk-integer-key-reject-test",
		[]RegisteredTable{h.registeredTable("counters", "counter_id")},
		"allows only uuid and text",
		h.schemaName+".counters",
	)
}

func TestBootstrap_AllowsVisibleSyncKeyThatDiffersFromPrimaryKey(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_declared_key_reject_")
	h.execf(t, `
		CREATE TABLE %s.users (
			_sync_scope_id TEXT NOT NULL,
			pk_id UUID NOT NULL,
			external_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, pk_id),
			UNIQUE (_sync_scope_id, external_id)
		)`, h.schemaIdent)

	h.requireSuccessfulBootstrap(t,
		"fk-declared-key-reject-test",
		[]RegisteredTable{h.registeredTable("users", "external_id")},
	)
}

func TestBootstrap_FailsWhenRegisteredTableLacksOwnerScopedSyncKeyUniqueness(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_owner_uniqueness_reject_")
	h.execf(t, `
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			pk_id UUID NOT NULL,
			doc_id UUID NOT NULL,
			title TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, pk_id)
		)`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"fk-owner-uniqueness-reject-test",
		[]RegisteredTable{h.registeredTable("docs", "doc_id")},
		"must provide unique identity (_sync_scope_id, doc_id)",
	)
}

func TestBootstrap_FailsWhenRegisteredSchemaContainsOwnerlessUniqueConstraint(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_composite_reject_")
	h.execf(t, `
		CREATE TABLE %s.profiles (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			UNIQUE (name)
		)`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"ownerless-unique-reject-test",
		[]RegisteredTable{h.registeredTable("profiles", "id")},
		"does not begin with _sync_scope_id",
		"profiles",
	)
}

func TestBootstrap_FailsClosedWhenRegisteredFKRemainsNonDeferrable(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_nondeferrable_reject_")
	h.execf(t, `
		CREATE TABLE %s.parent (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)
	h.execf(t, `
		CREATE TABLE %s.child (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT child_parent_fk
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.parent(_sync_scope_id, id)
				NOT DEFERRABLE
		)`, h.schemaIdent, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"fk-nondeferrable-reject-test",
		[]RegisteredTable{
			h.registeredTable("parent", "id"),
			h.registeredTable("child", "id"),
		},
		"non-deferrable FK constraints",
		h.schemaName+".child_parent_fk",
		"make these constraints DEFERRABLE before bootstrap",
	)
}

func TestBootstrap_FailsWhenOwnerColumnIsNotText(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "owner_type_reject_")
	h.execf(t, `
		CREATE TABLE %s.docs (
			_sync_scope_id UUID NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"owner-type-reject-test",
		[]RegisteredTable{h.registeredTable("docs", "id")},
		"must define _sync_scope_id TEXT",
	)
}

func TestBootstrap_FailsWhenRegisteredTableUsesPartialUniqueIndex(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "partial_unique_reject_")
	h.execf(t, `
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			deleted_at TIMESTAMPTZ
		)`, h.schemaIdent)
	h.execf(t, `
		CREATE UNIQUE INDEX docs_owner_id_live_idx
		ON %s.docs (_sync_scope_id, id)
		WHERE deleted_at IS NULL
	`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"partial-unique-reject-test",
		[]RegisteredTable{h.registeredTable("docs", "id")},
		"partial or expression unique index",
	)
}

func TestBootstrap_FailsWhenRegisteredTableUsesExpressionUniqueIndex(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "expression_unique_reject_")
	h.execf(t, `
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			title TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)
	h.execf(t, `
		CREATE UNIQUE INDEX docs_owner_title_expr_uidx
		ON %s.docs (_sync_scope_id, lower(title))
	`, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"expression-unique-reject-test",
		[]RegisteredTable{h.registeredTable("docs", "id")},
		"partial or expression unique index",
	)
}

func TestBootstrap_FailsWhenRegisteredChildFKOmitsOwnerColumn(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_ownerless_reject_")
	h.execf(t, `
		CREATE TABLE %s.parent (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)
	h.execf(t, `
		CREATE TABLE %s.child (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT child_parent_fk
				FOREIGN KEY (parent_id, _sync_scope_id) REFERENCES %s.parent(id, _sync_scope_id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, h.schemaIdent, h.schemaIdent)

	h.requireUnsupportedBootstrap(t,
		"fk-ownerless-reject-test",
		[]RegisteredTable{
			h.registeredTable("parent", "id"),
			h.registeredTable("child", "id"),
		},
		"scope-inclusive",
		"child_parent_fk",
	)
}

func TestBootstrap_AllowsDeferrableButInitiallyImmediateFKs(t *testing.T) {
	h := newSchemaBootstrapFailureHarness(t, "fk_immediate_ok_")
	h.execf(t, `
		CREATE TABLE %s.parent (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, h.schemaIdent)
	h.execf(t, `
		CREATE TABLE %s.child (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT child_parent_fk
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.parent(_sync_scope_id, id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, h.schemaIdent, h.schemaIdent)

	h.requireSuccessfulBootstrap(t,
		"fk-initially-immediate-ok-test",
		[]RegisteredTable{
			h.registeredTable("parent", "id"),
			h.registeredTable("child", "id"),
		},
	)
}
