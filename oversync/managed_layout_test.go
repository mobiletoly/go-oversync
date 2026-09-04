package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestManagedLayoutFacts_DeterministicOrderingAndFingerprint(t *testing.T) {
	facts := []managedLayoutFact{
		{Kind: "table", Identity: "sync.z", Attribute: "type", Value: "ordinary"},
		{Kind: "table", Identity: "sync.a", Attribute: "type", Value: "ordinary"},
	}
	reversed := []managedLayoutFact{facts[1], facts[0]}
	left, err := managedLayoutFingerprint(facts)
	require.NoError(t, err)
	right, err := managedLayoutFingerprint(reversed)
	require.NoError(t, err)
	require.Equal(t, left, right)
}

func TestManagedLayoutFacts_LengthDelimitedFingerprint(t *testing.T) {
	left, err := managedLayoutFingerprint([]managedLayoutFact{
		{Kind: "ab", Identity: "c", Attribute: "d", Value: "e"},
	})
	require.NoError(t, err)
	right, err := managedLayoutFingerprint([]managedLayoutFact{
		{Kind: "a", Identity: "bc", Attribute: "d", Value: "e"},
	})
	require.NoError(t, err)
	require.NotEqual(t, left, right)
}

func TestManagedLayoutFacts_RejectDuplicateKeys(t *testing.T) {
	_, err := managedLayoutFingerprint([]managedLayoutFact{
		{Kind: "table", Identity: "sync.meta", Attribute: "kind", Value: "r"},
		{Kind: "table", Identity: "sync.meta", Attribute: "kind", Value: "r"},
	})
	require.ErrorContains(t, err, "duplicate managed layout fact")
}

func TestManagedLayoutFacts_BoundedSortedDifferences(t *testing.T) {
	expected := []managedLayoutFact{
		{Kind: "index", Identity: "sync.b", Attribute: "valid", Value: "true"},
		{Kind: "index", Identity: "sync.a", Attribute: "valid", Value: "true"},
		{Kind: "table", Identity: "sync.c", Attribute: "kind", Value: "r"},
	}
	actual := []managedLayoutFact{
		{Kind: "index", Identity: "sync.a", Attribute: "valid", Value: "false"},
		{Kind: "function", Identity: "sync.extra()", Attribute: "language", Value: "sql"},
	}
	differences, total, err := compareManagedLayoutFacts(expected, actual, 2)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Equal(t, []managedLayoutDifference{
		{Category: "unexpected", Kind: "function", Identity: "sync.extra()", Attribute: "language", Actual: "sql"},
		{Category: "changed", Kind: "index", Identity: "sync.a", Attribute: "valid", Expected: "true", Actual: "false"},
	}, differences)
}

type managedLayoutTestFixture struct {
	ctx              context.Context
	harness          *schemaBootstrapFailureHarness
	registeredTables []RegisteredTable
	service          *SyncService
}

type managedLayoutCountingQuerier struct {
	syncCatalogQuerier
	queryCount int
}

func (q *managedLayoutCountingQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.queryCount++
	return q.syncCatalogQuerier.Query(ctx, sql, args...)
}

func newManagedLayoutTestFixture(t *testing.T, prefix string) *managedLayoutTestFixture {
	t.Helper()
	h := newSchemaBootstrapFailureHarness(t, prefix)
	h.execf(t, `
		CREATE TABLE %s.records (
			id UUID NOT NULL,
			_sync_scope_id TEXT NOT NULL,
			payload TEXT NOT NULL,
			CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
		)
	`, h.schemaIdent)
	rowID := uuid.New()
	h.execf(t, `INSERT INTO %s.records (id, _sync_scope_id, payload) VALUES ('%s', 'managed-owner', 'original')`, h.schemaIdent, rowID)
	registeredTables := []RegisteredTable{h.registeredTable("records", "id")}
	service, err := newTestRuntimeService(h.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registeredTables), AppName: "managed-layout-initial",
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	require.NoError(t, service.Bootstrap(h.ctx))
	return &managedLayoutTestFixture{
		ctx:              h.ctx,
		harness:          h,
		registeredTables: registeredTables,
		service:          service,
	}
}

func (f *managedLayoutTestFixture) newService(t *testing.T, appName string) *SyncService {
	t.Helper()
	service, err := newTestRuntimeService(f.harness.pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(f.registeredTables), AppName: appName,
		RegisteredTables:               f.registeredTables,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}

func (f *managedLayoutTestFixture) authoritativeCounts(t *testing.T) []int64 {
	t.Helper()
	var businessRows, users, rows, bundles, bundleRows, sources int64
	require.NoError(t, f.harness.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.records`, f.harness.schemaIdent)).Scan(&businessRows))
	require.NoError(t, f.harness.pool.QueryRow(f.ctx, `
		SELECT
			(SELECT COUNT(*) FROM sync.user_state),
			(SELECT COUNT(*) FROM sync.row_state),
			(SELECT COUNT(*) FROM sync.bundle_log),
			(SELECT COUNT(*) FROM sync.bundle_rows),
			(SELECT COUNT(*) FROM sync.source_state)
	`).Scan(&users, &rows, &bundles, &bundleRows, &sources))
	return []int64{businessRows, users, rows, bundles, bundleRows, sources}
}

func TestBootstrap_ValidatesManagedLayoutManifest(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_valid_")
	var functionOID, captureTriggerOID uint32
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT 'sync.capture_registered_row_change()'::regprocedure::oid`).Scan(&functionOID))
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `
		SELECT trigger.oid
		FROM pg_trigger AS trigger
		JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = $1 AND relation.relname = 'records' AND trigger.tgname = $2
	`, fixture.harness.schemaName, registeredTableCaptureTriggerName).Scan(&captureTriggerOID))

	applicationTriggerName := "application_owned_trigger"
	fixture.harness.execf(t, `
		CREATE FUNCTION %s.application_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		CREATE TRIGGER %s BEFORE UPDATE ON %s.records FOR EACH ROW EXECUTE FUNCTION %s.application_trigger()
	`, fixture.harness.schemaIdent, applicationTriggerName, fixture.harness.schemaIdent, fixture.harness.schemaIdent)
	require.NoError(t, fixture.service.Bootstrap(fixture.ctx))
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, fixture.harness.pool))

	var functionOIDAfter, captureTriggerOIDAfter uint32
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT 'sync.capture_registered_row_change()'::regprocedure::oid`).Scan(&functionOIDAfter))
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT oid FROM pg_trigger WHERE tgrelid = to_regclass($1) AND tgname = $2`, fixture.harness.schemaName+".records", registeredTableCaptureTriggerName).Scan(&captureTriggerOIDAfter))
	require.Equal(t, functionOID, functionOIDAfter)
	require.Equal(t, captureTriggerOID, captureTriggerOIDAfter)
}

func TestManagedLayout_RevisedAndLegacyManifestsRejectEachOther(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_retryable_reject_")
	revised, err := fixture.service.expectedManagedLayoutFacts(fixture.ctx, fixture.harness.pool)
	require.NoError(t, err)

	retryableWriteKeys := make(map[managedLayoutFactKey]struct{}, len(retryableWriteManagedLayoutFacts))
	for _, fact := range retryableWriteManagedLayoutFacts {
		retryableWriteKeys[managedLayoutFactKey{Kind: fact.Kind, Identity: fact.Identity, Attribute: fact.Attribute}] = struct{}{}
	}
	legacyValues := map[managedLayoutFactKey]string{
		{Kind: "column", Identity: "sync.bundle_log.canonical_request_hash", Attribute: "collation"}:     "pg_catalog.default",
		{Kind: "column", Identity: "sync.bundle_log.source_id", Attribute: "collation"}:                  "pg_catalog.default",
		{Kind: "column", Identity: "sync.scope_state.initialized_by_source_id", Attribute: "collation"}:  "pg_catalog.default",
		{Kind: "column", Identity: "sync.scope_state.initializer_source_id", Attribute: "collation"}:     "pg_catalog.default",
		{Kind: "column", Identity: "sync.source_state.replaced_by_source_id", Attribute: "collation"}:    "pg_catalog.default",
		{Kind: "column", Identity: "sync.source_state.source_id", Attribute: "collation"}:                "pg_catalog.default",
		{Kind: "column", Identity: "sync.user_state.user_id", Attribute: "collation"}:                    "pg_catalog.default",
		{Kind: "function", Identity: "sync.capture_registered_row_change()", Attribute: "source.sha256"}: "3304a5f48a65f815c501e633c7b8645acbd992ac54d6256d0b90c458f1df0049",
		{Kind: "function", Identity: "sync.enforce_registered_row_owner()", Attribute: "source.sha256"}:  "ea9676345ff44e0978f2621bfe215c2265dc3ed7d34cf88e9b55ebc850e67cd4",
		{Kind: "index", Identity: "sync.bundle_log_source_tuple_key", Attribute: "attribute.2"}:          `{"role": "key", "column": "source_id", "opclass": "pg_catalog.text_ops", "collation": "pg_catalog.default", "descending": false, "expression": "", "nulls_first": false}`,
		{Kind: "index", Identity: "sync.source_state_pkey", Attribute: "attribute.2"}:                    `{"role": "key", "column": "source_id", "opclass": "pg_catalog.text_ops", "collation": "pg_catalog.default", "descending": false, "expression": "", "nulls_first": false}`,
		{Kind: "index", Identity: "sync.user_state_user_id_key", Attribute: "attribute.1"}:               `{"role": "key", "column": "user_id", "opclass": "pg_catalog.text_ops", "collation": "pg_catalog.default", "descending": false, "expression": "", "nulls_first": false}`,
	}
	legacy := make([]managedLayoutFact, 0, len(revised)-len(retryableWriteManagedLayoutFacts))
	for _, fact := range revised {
		key := managedLayoutFactKey{Kind: fact.Kind, Identity: fact.Identity, Attribute: fact.Attribute}
		if _, added := retryableWriteKeys[key]; added {
			continue
		}
		if value, changed := legacyValues[key]; changed {
			fact.Value = value
		}
		legacy = append(legacy, fact)
	}
	legacy, err = normalizeManagedLayoutFacts(legacy)
	require.NoError(t, err)

	err = validateManagedLayoutFacts(syncSchemaLayoutName, revised, legacy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "managed sync layout mismatch")
	err = validateManagedLayoutFacts(syncSchemaLayoutName, legacy, revised)
	require.Error(t, err)
	require.Contains(t, err.Error(), "managed sync layout mismatch")
}

func TestBootstrap_ExistingLayoutRejectsManagedDriftWithoutMutation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		{name: "missing table", sql: `DROP TABLE sync.snapshot_session_rows`},
		{name: "extra relation", sql: `CREATE TABLE sync.operator_table (id integer)`},
		{name: "extra partitioned relation", sql: `CREATE TABLE sync.operator_partitioned (id integer) PARTITION BY RANGE (id)`},
		{name: "extra view", sql: `CREATE VIEW sync.operator_view AS SELECT 1 AS value`},
		{name: "extra sequence", sql: `CREATE SEQUENCE sync.operator_sequence`},
		{name: "extra function", sql: `CREATE FUNCTION sync.operator_function() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$`},
		{name: "extra column", sql: `ALTER TABLE sync.snapshot_sessions ADD COLUMN operator_column text`},
		{name: "extra generated column", sql: `ALTER TABLE sync.snapshot_sessions ADD COLUMN operator_generated bigint GENERATED ALWAYS AS (row_count + 1) STORED`},
		{name: "column type", sql: `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count TYPE integer`},
		{name: "column nullability", sql: `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count DROP NOT NULL`},
		{name: "column default", sql: `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count SET DEFAULT 9`},
		{name: "column identity", sql: `ALTER TABLE sync.user_state ALTER COLUMN user_pk DROP IDENTITY`},
		{name: "column collation", sql: `ALTER TABLE sync.bundle_log ALTER COLUMN canonical_request_hash TYPE text COLLATE "default"`},
		{name: "table persistence", sql: `ALTER TABLE sync.snapshot_session_rows SET UNLOGGED`},
		{name: "table row security", sql: `ALTER TABLE sync.snapshot_sessions ENABLE ROW LEVEL SECURITY`},
		{name: "table replica identity", sql: `ALTER TABLE sync.snapshot_sessions REPLICA IDENTITY FULL`},
		{name: "missing constraint", sql: `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_op_code_chk`},
		{name: "missing constraint backed index", sql: `ALTER TABLE sync.bundle_log DROP CONSTRAINT bundle_log_source_tuple_key`},
		{name: "constraint validation state", sql: `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_op_code_chk; ALTER TABLE sync.bundle_rows ADD CONSTRAINT bundle_rows_op_code_chk CHECK (op_code IN (1, 2, 3)) NOT VALID`},
		{name: "foreign key action", sql: `ALTER TABLE sync.bundle_rows DROP CONSTRAINT bundle_rows_bundle_fk; ALTER TABLE sync.bundle_rows ADD CONSTRAINT bundle_rows_bundle_fk FOREIGN KEY (user_pk, bundle_seq) REFERENCES sync.bundle_log(user_pk, bundle_seq)`},
		{name: "extra constraint", sql: `ALTER TABLE sync.snapshot_sessions ADD CONSTRAINT operator_check CHECK (row_count >= 0) NOT VALID`},
		{name: "missing explicit index", sql: `DROP INDEX sync.rs_user_live_snapshot_idx`},
		{name: "altered explicit index", sql: `DROP INDEX sync.rs_user_live_snapshot_idx; CREATE INDEX rs_user_live_snapshot_idx ON sync.row_state(user_pk, bundle_seq)`},
		{name: "sequence configuration", sql: `ALTER SEQUENCE sync.accepted_push_replay_seq INCREMENT BY 2`},
		{name: "sequence ownership", sql: `ALTER SEQUENCE sync.accepted_push_replay_seq OWNED BY sync.snapshot_sessions.row_count`},
		{name: "function body", sql: `CREATE OR REPLACE FUNCTION sync.reject_registered_table_truncate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`},
		{name: "function attributes", sql: `ALTER FUNCTION sync.capture_registered_row_change() IMMUTABLE`},
		{name: "extra rule", sql: `CREATE RULE operator_rule AS ON UPDATE TO sync.snapshot_sessions DO ALSO SELECT 1`},
		{name: "extra sync trigger", sql: `CREATE FUNCTION sync.operator_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; CREATE TRIGGER operator_trigger BEFORE UPDATE ON sync.snapshot_sessions FOR EACH ROW EXECUTE FUNCTION sync.operator_trigger()`},
	}
	// Every case owns a separate database (caller-managed databases remain
	// serialized by the integration helper). Bound concurrent fixtures explicitly
	// so complete drift coverage fits the established aggregate deadline.
	slots := make(chan struct{}, 4)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			slots <- struct{}{}
			t.Cleanup(func() { <-slots })
			fixture := newManagedLayoutTestFixture(t, "managed_drift_")
			before := fixture.authoritativeCounts(t)
			_, err := fixture.harness.pool.Exec(fixture.ctx, test.sql)
			require.NoError(t, err)
			second := fixture.newService(t, "managed-layout-drift-"+strings.ReplaceAll(test.name, " ", "-"))
			err = second.Bootstrap(fixture.ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr)
			require.Contains(t, err.Error(), "managed sync layout mismatch")
			require.Contains(t, err.Error(), syncSchemaLayoutName)
			require.Contains(t, err.Error(), "expected fingerprint")
			require.Contains(t, err.Error(), "actual fingerprint")
			for _, forbidden := range []string{"drop and recreate", "recreate the", "truncate the database", "rewrite the"} {
				require.NotContains(t, strings.ToLower(err.Error()), forbidden)
			}
			require.Equal(t, before, fixture.authoritativeCounts(t))
			_, err = second.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, testSchemaGenerationProofForService(second), &ConnectRequest{})
			require.ErrorIs(t, err, errServiceNotReady)
		})
	}
}

func TestBootstrap_ExistingLayoutCancellationLeavesDatabaseUnchanged(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_cancel_")
	before := fixture.authoritativeCounts(t)
	service := fixture.newService(t, "managed-layout-cancel")

	lockTx, err := fixture.harness.pool.Begin(fixture.ctx)
	require.NoError(t, err)
	_, err = lockTx.Exec(fixture.ctx, `SELECT pg_advisory_xact_lock($1)`, syncBootstrapLockKey)
	require.NoError(t, err)

	attachCtx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- service.Bootstrap(attachCtx) }()
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := fixture.harness.pool.QueryRow(fixture.ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE locktype = 'advisory' AND NOT granted
			)
		`).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, before, fixture.authoritativeCounts(t))
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, fixture.harness.pool))
	_, err = service.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, testSchemaGenerationProofForService(service), &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)

	require.NoError(t, lockTx.Rollback(fixture.ctx))
	require.Eventually(t, func() bool {
		return fixture.harness.pool.Stat().AcquiredConns() == 0
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, service.Bootstrap(fixture.ctx))
	require.Equal(t, before, fixture.authoritativeCounts(t))
}

func TestBootstrap_ManagedLayoutDriftReadinessAndRetry(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_retry_")
	service := fixture.newService(t, "managed-layout-retry")
	_, err := fixture.harness.pool.Exec(fixture.ctx, `DROP INDEX sync.rs_user_live_snapshot_idx`)
	require.NoError(t, err)
	err = service.Bootstrap(fixture.ctx)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	_, err = service.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, testSchemaGenerationProofForService(service), &ConnectRequest{})
	require.ErrorIs(t, err, errServiceNotReady)
	_, err = fixture.harness.pool.Exec(fixture.ctx, `CREATE INDEX rs_user_live_snapshot_idx ON sync.row_state(user_pk, table_id, key_bytes) WHERE deleted = FALSE`)
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(fixture.ctx))
	_, err = service.Connect(fixture.ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, testSchemaGenerationProofForService(service), &ConnectRequest{})
	require.NoError(t, err)
	require.NoError(t, fixture.newService(t, "managed-layout-new-instance").Bootstrap(fixture.ctx))
}

func TestBootstrap_ManagedLayoutValidationConcurrency(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_concurrent_")
	_, err := fixture.harness.pool.Exec(fixture.ctx, `ALTER SEQUENCE sync.history_pruned_error_seq CACHE 2`)
	require.NoError(t, err)
	services := []*SyncService{
		fixture.newService(t, "managed-layout-concurrent-a"),
		fixture.newService(t, "managed-layout-concurrent-b"),
	}
	errors := make([]error, len(services))
	var wait sync.WaitGroup
	for i, service := range services {
		wait.Add(1)
		go func(index int, service *SyncService) {
			defer wait.Done()
			errors[index] = service.Bootstrap(fixture.ctx)
		}(i, service)
	}
	wait.Wait()
	for _, err := range errors {
		var schemaErr *UnsupportedSchemaError
		require.ErrorAs(t, err, &schemaErr)
	}
	_, err = fixture.harness.pool.Exec(fixture.ctx, `ALTER SEQUENCE sync.history_pruned_error_seq CACHE 1`)
	require.NoError(t, err)
	for _, service := range services {
		require.NoError(t, service.Bootstrap(fixture.ctx))
	}
}

func TestBootstrap_FreshLayoutSelfValidates(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_fresh_")
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, fixture.harness.pool))
	var markerCount, catalogCount int
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.meta`).Scan(&markerCount))
	require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.table_catalog`).Scan(&catalogCount))
	require.Equal(t, 1, markerCount)
	require.Equal(t, 1, catalogCount)
}

func TestBootstrap_ManagedLayoutSupportedPostgresVersions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		image string
		major int
	}{
		{image: managedIntegrationPostgresImage, major: 16},
		{image: "postgres:17.10", major: 17},
	}
	for _, test := range tests {
		t.Run(strings.ReplaceAll(test.image, ":", "_"), func(t *testing.T) {
			var (
				server *managedIntegrationPostgresServer
				err    error
			)
			if test.image == managedIntegrationPostgresImage {
				server, err = getManagedIntegrationPostgres()
			} else {
				server, err = startManagedIntegrationPostgres(test.image)
				if err == nil {
					t.Cleanup(func() {
						require.NoError(t, server.close())
					})
				}
			}
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			t.Cleanup(cancel)
			pool := newIntegrationTestPoolForServer(t, ctx, server)
			var serverVersionNum int
			require.NoError(t, pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer`).Scan(&serverVersionNum))
			require.Equal(t, test.major, serverVersionNum/10000)

			schemaName := fmt.Sprintf("managed_version_%d", test.major)
			schemaIdent := pgx.Identifier{schemaName}.Sanitize()
			_, err = pool.Exec(ctx, fmt.Sprintf(`
				CREATE SCHEMA %s;
				CREATE TABLE %s.records (
					id UUID NOT NULL,
					_sync_scope_id TEXT NOT NULL,
					payload TEXT NOT NULL,
					CONSTRAINT records_owner_key UNIQUE (_sync_scope_id, id)
				);
				INSERT INTO %s.records (id, _sync_scope_id, payload)
				VALUES ('5dc911f0-6b38-43a9-a247-ec910eac7bd1', 'managed-owner', 'original')
			`, schemaIdent, schemaIdent, schemaIdent))
			require.NoError(t, err)

			config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{
				Schema:         schemaName,
				Table:          "records",
				SyncKeyColumns: []string{"id"},
			}}), AppName: "managed-layout-version-" + strconv.Itoa(test.major),
				RegisteredTables: []RegisteredTable{{
					Schema:         schemaName,
					Table:          "records",
					SyncKeyColumns: []string{"id"},
				}},
				RegisteredTableCatalogRevision: 1,
			}
			service, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = service.Close(context.Background()) })
			require.NoError(t, service.Bootstrap(ctx))
			require.NoError(t, service.validateManagedLayout(ctx, pool))

			actualFacts, err := loadManagedLayoutFacts(ctx, pool)
			require.NoError(t, err)
			for _, identity := range []string{
				"sync.bundle_rows.payload_wire",
				"sync.snapshot_session_rows.payload_wire",
			} {
				require.Equal(t, "pg_catalog.json", managedLayoutFactValue(t, actualFacts, "column", identity, "type"))
			}
			require.Equal(
				t,
				"pg_catalog.C",
				managedLayoutFactValue(t, actualFacts, "column", "sync.meta.layout_name", "collation"),
			)
			var indexAttribute map[string]any
			require.NoError(
				t,
				json.Unmarshal(
					[]byte(managedLayoutFactValue(t, actualFacts, "index", "sync.user_state_user_id_key", "attribute.1")),
					&indexAttribute,
				),
			)
			require.Equal(t, "pg_catalog.C", indexAttribute["collation"])

			second, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = second.Close(context.Background()) })
			require.NoError(t, second.Bootstrap(ctx))

			_, err = pool.Exec(ctx, `ALTER TABLE sync.snapshot_sessions ALTER COLUMN row_count TYPE integer`)
			require.NoError(t, err)
			drifted, err := newTestRuntimeService(pool, config, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, err)
			t.Cleanup(func() { _ = drifted.Close(context.Background()) })
			err = drifted.Bootstrap(ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr)
			require.ErrorContains(t, err, "changed column sync.snapshot_sessions.row_count attribute type")
			_, err = drifted.Connect(ctx, Actor{UserID: "managed-owner", SourceID: "reader"}, testSchemaGenerationProofForService(drifted), &ConnectRequest{})
			require.ErrorIs(t, err, errServiceNotReady)
		})
	}
}

func managedLayoutFactValue(
	t *testing.T,
	facts []managedLayoutFact,
	kind string,
	identity string,
	attribute string,
) string {
	t.Helper()
	for _, fact := range facts {
		if fact.Kind == kind && fact.Identity == identity && fact.Attribute == attribute {
			return fact.Value
		}
	}
	require.FailNowf(t, "managed layout fact missing", "%s %s attribute %s", kind, identity, attribute)
	return ""
}

func TestManagedLayoutValidation_UsesFixedCatalogQueryCount(t *testing.T) {
	fixture := newManagedLayoutTestFixture(t, "managed_queries_")
	full := &managedLayoutCountingQuerier{syncCatalogQuerier: fixture.harness.pool}
	require.NoError(t, fixture.service.validateManagedLayout(fixture.ctx, full))
	require.Equal(t, 9, full.queryCount, "one target query plus eight object-class queries")
	volatile := &managedLayoutCountingQuerier{syncCatalogQuerier: fixture.harness.pool}
	require.NoError(t, fixture.service.validateManagedLayoutVolatileFacts(fixture.ctx, volatile))
	require.Equal(t, 3, volatile.queryCount, "one target query plus function and trigger queries")
}

func TestRegisteredTableTriggers_ValidateWithoutRepair(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mutation func(tableIdent string) string
	}{
		{name: "disabled", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER %s`, tableIdent, registeredTableCaptureTriggerName)
		}},
		{name: "missing", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`DROP TRIGGER %s ON %s`, registeredTableCaptureTriggerName, tableIdent)
		}},
		{name: "wrong function", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`DROP TRIGGER %s ON %s; CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.enforce_registered_row_owner()`, registeredTableCaptureTriggerName, tableIdent, registeredTableCaptureTriggerName, tableIdent)
		}},
		{name: "wrong arguments", mutation: func(tableIdent string) string {
			return fmt.Sprintf(`DROP TRIGGER %s ON %s; CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.capture_registered_row_change('id', '1', '999')`, registeredTableCaptureTriggerName, tableIdent, registeredTableCaptureTriggerName, tableIdent)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newManagedLayoutTestFixture(t, "managed_trigger_")
			tableIdent := pgx.Identifier{fixture.harness.schemaName, "records"}.Sanitize()
			_, err := fixture.harness.pool.Exec(fixture.ctx, test.mutation(tableIdent))
			require.NoError(t, err)
			var beforeCount int
			var beforeDefinition string
			err = fixture.harness.pool.QueryRow(fixture.ctx, `
				SELECT COUNT(*), COALESCE(max(pg_get_triggerdef(oid)), '')
				FROM pg_trigger
				WHERE tgrelid = to_regclass($1) AND tgname = $2
			`, fixture.harness.schemaName+".records", registeredTableCaptureTriggerName).Scan(&beforeCount, &beforeDefinition)
			require.NoError(t, err)
			second := fixture.newService(t, "managed-layout-trigger-"+strings.ReplaceAll(test.name, " ", "-"))
			err = second.Bootstrap(fixture.ctx)
			var schemaErr *UnsupportedSchemaError
			require.ErrorAs(t, err, &schemaErr)
			var afterCount int
			var afterDefinition string
			require.NoError(t, fixture.harness.pool.QueryRow(fixture.ctx, `
				SELECT COUNT(*), COALESCE(max(pg_get_triggerdef(oid)), '')
				FROM pg_trigger
				WHERE tgrelid = to_regclass($1) AND tgname = $2
			`, fixture.harness.schemaName+".records", registeredTableCaptureTriggerName).Scan(&afterCount, &afterDefinition))
			require.Equal(t, beforeCount, afterCount)
			require.Equal(t, beforeDefinition, afterDefinition)
		})
	}
}
