package oversync

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

type testCatalogRetirementAuthority struct {
	ReadinessAdapter
	permit CatalogRetirementPermit
}

func (a testCatalogRetirementAuthority) AuthorizeCatalogRetirement(ctx context.Context, r DatabaseReadinessReader) (CatalogRetirementPermit, error) {
	var held bool
	err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND classid=1869833075 AND objid=6 AND mode='ExclusiveLock' AND granted)`).Scan(&held)
	if err != nil {
		return CatalogRetirementPermit{}, err
	}
	if !held {
		return CatalogRetirementPermit{}, fmt.Errorf("test retirement fence missing")
	}
	return a.permit, nil
}

func TestCatalogRetirement_MultipleTablesPartitionsAndAtomicFailure(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	if os.Getenv("ROLLOUT_PG_MAJOR") == "17" {
		server, err := startManagedIntegrationPostgres("postgres:17")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, server.close()) })
		pool = newIntegrationTestPoolForServer(t, ctx, server)
	}
	var major int
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int/10000`).Scan(&major))
	expectedMajor := 16
	if os.Getenv("ROLLOUT_PG_MAJOR") == "17" {
		expectedMajor = 17
	}
	require.Equal(t, expectedMajor, major)
	schema := "retire_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ident := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, `CREATE SCHEMA `+ident+`;CREATE TABLE `+ident+`.users(_sync_scope_id text NOT NULL,id uuid NOT NULL,name text NOT NULL,email text NOT NULL,PRIMARY KEY(_sync_scope_id,id));
 CREATE TABLE `+ident+`.archive(_sync_scope_id text NOT NULL,id uuid NOT NULL,PRIMARY KEY(_sync_scope_id,id));
 CREATE TABLE `+ident+`.records(_sync_scope_id text NOT NULL,id uuid NOT NULL,PRIMARY KEY(_sync_scope_id,id)) PARTITION BY HASH(_sync_scope_id);
 CREATE TABLE `+ident+`.records_p0 PARTITION OF `+ident+`.records FOR VALUES WITH(MODULUS 2,REMAINDER 0);
 CREATE TABLE `+ident+`.records_p1 PARTITION OF `+ident+`.records FOR VALUES WITH(MODULUS 2,REMAINDER 1)`)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	active := []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}
	tables := append(append([]RegisteredTable(nil), active...), RegisteredTable{Schema: schema, Table: "archive", SyncKeyColumns: []string{"id"}}, RegisteredTable{Schema: schema, Table: "records", SyncKeyColumns: []string{"id"}})
	service, err := newTestRuntimeService(pool, &ServiceConfig{RegisteredTables: tables, RegisteredTableCatalogRevision: 1, SchemaGenerations: testSchemaGenerationsForRegisteredTables(active)}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, service.Bootstrap(ctx))
	defer service.Close(ctx)
	service.config.Readiness.DisableBackgroundWorkers = true
	p := CatalogRetirementPermit{OperationID: uuid.NewString(), PlanDigest: strings.Repeat("a", 64), Epoch: 1, DeploymentRevision: 1, CatalogRevision: 1, CatalogFingerprint: service.catalogMetadata.catalogFingerprint, RegistryFingerprint: service.schemaGenerationRegistry.Fingerprint(), TableIDs: []int32{1, 2}, Deadline: time.Now().Add(time.Minute), MaxRows: 2}
	service.config.Readiness.Adapter = testCatalogRetirementAuthority{ReadinessAdapter: service.config.Readiness.Adapter, permit: p}
	check := func(revision int64, retired, guards int) {
		var actualRevision int64
		var actualRetired, actualGuards int
		require.NoError(t, pool.QueryRow(ctx, `SELECT catalog_revision FROM sync.meta`).Scan(&actualRevision))
		require.Equal(t, revision, actualRevision)
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.table_catalog WHERE state_code=2`).Scan(&actualRetired))
		require.Equal(t, retired, actualRetired)
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname IN ('archive','records','records_p0','records_p1') AND NOT t.tgisinternal`, schema).Scan(&actualGuards))
		require.Equal(t, guards, actualGuards)
	}
	check(1, 0, 12)
	attempt := func() error {
		return pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1869833075,6)`); err != nil {
				return err
			}
			_, err := service.RetireRegisteredTables(ctx, tx)
			return err
		})
	}
	_, err = pool.Exec(ctx, `ALTER TABLE `+ident+`.users ADD CONSTRAINT still_referenced FOREIGN KEY(_sync_scope_id,id) REFERENCES `+ident+`.records(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED`)
	require.NoError(t, err)
	require.Error(t, attempt(), "an active physical FK still requires the retiring parent")
	check(1, 0, 12)
	_, err = pool.Exec(ctx, `ALTER TABLE `+ident+`.users DROP CONSTRAINT still_referenced`)
	require.NoError(t, err)
	// The second identity update fails after the first table's trigger removal.
	_, err = pool.Exec(ctx, `CREATE FUNCTION `+ident+`.reject_second() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF position('."records"' in current_query())>0 THEN RAISE EXCEPTION 'injected second retirement'; END IF;END $$;
 CREATE EVENT TRIGGER retirement_fault ON sql_drop EXECUTE FUNCTION `+ident+`.reject_second()`)
	require.NoError(t, err)

	require.ErrorContains(t, attempt(), "injected second retirement")
	check(1, 0, 12)
	_, err = pool.Exec(ctx, `DROP EVENT TRIGGER retirement_fault`)
	require.NoError(t, err)
	require.NoError(t, attempt())
	check(2, 2, 0)
	var next int
	require.NoError(t, pool.QueryRow(ctx, `SELECT next_table_id FROM sync.meta`).Scan(&next))
	require.Equal(t, 4, next)
	// Surviving registered tables retain all of their managed protection.
	var live int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger t WHERE tgrelid=($1||'.users')::regclass AND NOT tgisinternal`, schema).Scan(&live))
	require.Equal(t, 3, live)
}
