//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

type auditDatabaseContractOptions struct {
	maxRowsPerBundle       int
	maxBytesPerBundle      int
	retainedBundlesPerUser int64
}

type auditDatabaseContractFixture struct {
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	writer     Actor
	reader     Actor
	options    auditDatabaseContractOptions
}

func newAuditDatabaseContractFixture(
	t *testing.T,
	ctx context.Context,
	scenario string,
	options auditDatabaseContractOptions,
) *auditDatabaseContractFixture {
	t.Helper()

	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_" + strings.ReplaceAll(scenario, "_", "") + "_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "audit-database-contract-" + scenario,
		MaxRowsPerBundle:       options.maxRowsPerBundle,
		MaxBytesPerBundle:      options.maxBytesPerBundle,
		RetainedBundlesPerUser: options.retainedBundlesPerUser,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	userID := "audit-database-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, svc, userID, writer.SourceID)
	mustInitializeEmptyScope(t, ctx, svc, userID, reader.SourceID)

	return &auditDatabaseContractFixture{
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		writer:     writer,
		reader:     reader,
		options:    options,
	}
}

func (f *auditDatabaseContractFixture) userRow(id uuid.UUID, name string) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"id":"%s","name":"%s","email":"%s@example.com"}`,
			id,
			name,
			strings.ToLower(name),
		)),
	}
}

func (f *auditDatabaseContractFixture) businessUserCount(t *testing.T, ctx context.Context) int64 {
	t.Helper()

	var count int64
	tableIdent := pgx.Identifier{f.schemaName, "users"}.Sanitize()
	require.NoError(t, f.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableIdent)).Scan(&count))
	return count
}

func (f *auditDatabaseContractFixture) committedBundleCount(t *testing.T, ctx context.Context) int64 {
	t.Helper()

	var count int64
	require.NoError(t, f.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log AS bundle
		JOIN sync.user_state AS users ON users.user_pk = bundle.user_pk
		WHERE users.user_id = $1
	`, f.writer.UserID).Scan(&count))
	return count
}

func (f *auditDatabaseContractFixture) newServiceConfig(appName string) *ServiceConfig {
	return &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: f.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: appName,
		MaxRowsPerBundle:       f.options.maxRowsPerBundle,
		MaxBytesPerBundle:      f.options.maxBytesPerBundle,
		RetainedBundlesPerUser: f.options.retainedBundlesPerUser,
		RegisteredTables: []RegisteredTable{
			{Schema: f.schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}
}

func TestAuditMaxRowsPerBundle_RejectsOversizedBundle(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "rowlimit", auditDatabaseContractOptions{
		maxRowsPerBundle: 1,
	})

	_, err := pushRowsOneShot(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(uuid.New(), "One"),
		fixture.userRow(uuid.New(), "Two"),
	})
	require.Error(t, err, "a bundle with more than MaxRowsPerBundle effects must be rejected")
	require.Zero(t, fixture.businessUserCount(t, ctx), "a rejected oversized bundle must not mutate business state")
	require.Zero(t, fixture.committedBundleCount(t, ctx), "a rejected oversized bundle must not create bundle history")
}

func TestAuditMaxBytesPerBundle_RejectsOversizedBundle(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "bytelimit", auditDatabaseContractOptions{
		maxBytesPerBundle: 2,
	})

	_, err := pushRowsOneShot(t, ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(uuid.New(), strings.Repeat("payload", 32)),
	})
	require.Error(t, err, "a bundle with payload bytes above MaxBytesPerBundle must be rejected")
	require.Zero(t, fixture.businessUserCount(t, ctx), "a rejected oversized bundle must not mutate business state")
	require.Zero(t, fixture.committedBundleCount(t, ctx), "a rejected oversized bundle must not create bundle history")
}

func TestAuditBootstrap_AdoptsPopulatedRegisteredTableIntoSnapshot(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_adopt_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "users"}.Sanitize()

	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, tableIdent))
	require.NoError(t, err)

	userID := "audit-existing-user-" + suffix
	rowID := uuid.New()
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, name)
		VALUES ($1, $2, $3)
	`, tableIdent), userID, rowID, "Existing")
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}
	svc, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "audit-populated-adoption",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close(context.Background()) })
	require.NoError(t, svc.Bootstrap(ctx))

	actor := Actor{UserID: userID, SourceID: "snapshot-reader"}
	mustInitializeEmptyScope(t, ctx, svc, userID, actor.SourceID)
	session, err := svc.CreateSnapshotSession(ctx, actor, testSchemaGenerationProofForService(svc))
	if err != nil {
		var userStateCount, rowStateCount int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.user_state WHERE user_id = $1`, userID).Scan(&userStateCount))
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM sync.row_state AS state
			JOIN sync.user_state AS users ON users.user_pk = state.user_pk
			WHERE users.user_id = $1
		`, userID).Scan(&rowStateCount))
		t.Fatalf(
			"bootstrap did not adopt the populated owner into snapshot state: %v (user_state=%d row_state=%d)",
			err,
			userStateCount,
			rowStateCount,
		)
	}
	require.Equal(t, int64(1), session.RowCount)
	require.Positive(t, session.SnapshotBundleSeq)

	chunk, err := svc.GetSnapshotChunk(ctx, actor, testSchemaGenerationProofForService(svc), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, schemaName, chunk.Rows[0].Schema)
	require.Equal(t, "users", chunk.Rows[0].Table)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
	require.JSONEq(t, fmt.Sprintf(`{"id":"%s","name":"Existing"}`, rowID), string(chunk.Rows[0].Payload))
	requireAuditMetadataIntegrity(t, ctx, pool)
}

func TestAuditBootstrap_RejectsNullableScopeOrSyncKeyColumns(t *testing.T) {
	tests := []struct {
		name       string
		definition string
	}{
		{
			name: "scope_column",
			definition: `
				CREATE TABLE %s.records (
					_sync_scope_id TEXT,
					id UUID NOT NULL,
					UNIQUE (_sync_scope_id, id)
				)`,
		},
		{
			name: "sync_key_column",
			definition: `
				CREATE TABLE %s.records (
					_sync_scope_id TEXT NOT NULL,
					id UUID,
					UNIQUE (_sync_scope_id, id)
				)`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newSchemaBootstrapFailureHarness(t, "audit_nullable_")
			harness.execf(t, test.definition, harness.schemaIdent)

			svc, err := newTestRuntimeService(harness.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
				harness.registeredTable("records", "id"),
			}), AppName: "audit-nullable-" + test.name,
				RegisteredTables: []RegisteredTable{
					harness.registeredTable("records", "id"),
				},
				RegisteredTableCatalogRevision: 1,
			}, harness.logger)
			require.NoError(t, err)
			t.Cleanup(func() { _ = svc.Close(context.Background()) })

			err = svc.Bootstrap(harness.ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr, "nullable owner and visible sync-key columns must fail bootstrap")
			message := strings.ToLower(err.Error())
			require.True(t,
				strings.Contains(message, "not null") || strings.Contains(message, "nullable"),
				"bootstrap error must identify the nullable column contract: %v",
				err,
			)
		})
	}
}

func TestAuditRegisteredTable_TruncateFailsClosed(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "truncate", auditDatabaseContractOptions{})
	rowID := uuid.New()
	mustPushUserBundle(t, ctx, fixture.svc, fixture.writer, fixture.schemaName, 1, rowID, "BeforeTruncate")

	tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
	_, truncateErr := fixture.pool.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s CASCADE`, tableIdent))
	if truncateErr == nil {
		var liveRowStateCount int64
		require.NoError(t, fixture.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM sync.row_state AS state
			JOIN sync.user_state AS users ON users.user_pk = state.user_pk
			WHERE users.user_id = $1
			  AND state.deleted = FALSE
		`, fixture.writer.UserID).Scan(&liveRowStateCount))
		t.Fatalf(
			"TRUNCATE bypassed registered-table guards (business_rows=%d live_row_state=%d)",
			fixture.businessUserCount(t, ctx),
			liveRowStateCount,
		)
	}
	require.Equal(t, int64(1), fixture.businessUserCount(t, ctx), "a rejected TRUNCATE must preserve business data")
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)

	pull, err := fixture.svc.ProcessPull(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.Equal(t, rowID.String(), pull.Bundles[0].Rows[0].Key["id"])

	snapshot, err := fixture.svc.CreateSnapshotSession(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, int64(1), snapshot.RowCount)
	chunk, err := fixture.svc.GetSnapshotChunk(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), snapshot.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, chunk.Rows, 1)
	require.Equal(t, rowID.String(), chunk.Rows[0].Key["id"])
}

func TestAuditBootstrap_RejectsDamagedExistingLayout(t *testing.T) {
	tests := []struct {
		name      string
		damageSQL string
	}{
		{
			name:      "missing_required_index",
			damageSQL: `DROP INDEX sync.rs_user_live_snapshot_idx`,
		},
		{
			name:      "missing_capture_function",
			damageSQL: `DROP FUNCTION sync.capture_registered_row_change() CASCADE`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newAuditDatabaseContractFixture(t, ctx, "damage"+test.name, auditDatabaseContractOptions{})
			_, err := fixture.pool.Exec(ctx, test.damageSQL)
			require.NoError(t, err)

			second, err := newTestRuntimeService(
				fixture.pool,
				fixture.newServiceConfig("audit-damaged-layout-"+test.name),
				integrationTestLogger(slog.LevelWarn),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = second.Close(context.Background()) })

			err = second.Bootstrap(ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr, "a marked layout missing required objects must fail closed as unsupported")
		})
	}
}

func TestAuditProcessPull_RejectsZeroCheckpointAfterDatabasePruning(t *testing.T) {
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "pullfloor", auditDatabaseContractOptions{
		retainedBundlesPerUser: 1,
	})

	mustPushUserBundle(t, ctx, fixture.svc, fixture.writer, fixture.schemaName, 1, uuid.New(), "One")
	mustPushUserBundle(t, ctx, fixture.svc, fixture.writer, fixture.schemaName, 2, uuid.New(), "Two")

	var retainedFloor, remainingBundleCount int64
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT users.retained_bundle_floor, COUNT(bundle.bundle_seq)
		FROM sync.user_state AS users
		LEFT JOIN sync.bundle_log AS bundle ON bundle.user_pk = users.user_pk
		WHERE users.user_id = $1
		GROUP BY users.user_pk
	`, fixture.writer.UserID).Scan(&retainedFloor, &remainingBundleCount))
	require.Equal(t, int64(1), retainedFloor)
	require.Equal(t, int64(1), remainingBundleCount)

	_, err := fixture.svc.ProcessPull(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr, "checkpoint zero must not receive a successful incomplete history")
	require.Equal(t, int64(0), prunedErr.ProvidedSeq)
	require.Equal(t, retainedFloor, prunedErr.RetainedFloor)
}
