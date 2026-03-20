package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestBootstrap_CreatesScaleRedesignSchemaObjects(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "schema-bootstrap-test",
	}, logger)

	// Bootstrap should remain idempotent for the bundle-era metadata schema.
	require.NoError(t, svc.Bootstrap(ctx))

	var (
		userStateExists                  bool
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
		rowStateIndexExists              bool
		bundleLogIndexExists             bool
		bundleRowsIndexExists            bool
		snapshotSessionsTTLIndexExists   bool
		snapshotSessionRowsIndexExists   bool
		captureFunctionExists            bool
		primaryKeyCount                  int
		namedCheckConstraintCount        int
	)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			to_regclass('sync.user_state') IS NOT NULL,
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
			to_regclass('sync.rs_user_table_bundle_idx') IS NOT NULL,
			to_regclass('sync.bl_user_committed_idx') IS NOT NULL,
			to_regclass('sync.br_user_bundle_ordinal_idx') IS NOT NULL,
			to_regclass('sync.ss_expires_at_idx') IS NOT NULL,
			to_regclass('sync.ssr_snapshot_row_ordinal_idx') IS NOT NULL,
			to_regprocedure('sync.capture_registered_row_change()') IS NOT NULL
	`).Scan(
		&userStateExists,
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
		&rowStateIndexExists,
		&bundleLogIndexExists,
		&bundleRowsIndexExists,
		&snapshotSessionsTTLIndexExists,
		&snapshotSessionRowsIndexExists,
		&captureFunctionExists,
	))

	require.True(t, userStateExists)
	require.True(t, bundleCaptureStageExists)
	require.True(t, rowStateExists)
	require.True(t, bundleLogExists)
	require.True(t, bundleRowsExists)
	require.True(t, appliedPushesExists)
	require.True(t, snapshotSessionsExists)
	require.True(t, snapshotSessionRowsExists)
	require.True(t, acceptedPushReplaySeqExists)
	require.True(t, rejectedRegisteredWriteSeqExists)
	require.True(t, historyPrunedErrorSeqExists)
	require.True(t, bundleCaptureIndexExists)
	require.True(t, rowStateIndexExists)
	require.True(t, bundleLogIndexExists)
	require.True(t, bundleRowsIndexExists)
	require.True(t, snapshotSessionsTTLIndexExists)
	require.True(t, snapshotSessionRowsIndexExists)
	require.True(t, captureFunctionExists)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.table_constraints
		WHERE table_schema = 'sync'
		  AND table_name IN ('user_state', 'bundle_capture_stage', 'row_state', 'bundle_log', 'bundle_rows', 'applied_pushes', 'snapshot_sessions', 'snapshot_session_rows')
		  AND constraint_type = 'PRIMARY KEY'
	`).Scan(&primaryKeyCount))
	require.Equal(t, 8, primaryKeyCount)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT c.conname)
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
		WHERE n.nspname = 'sync'
		  AND c.conname IN ('bundle_rows_payload_by_op_chk', 'bundle_capture_stage_payload_by_op_chk')
	`).Scan(&namedCheckConstraintCount))
	require.Equal(t, 2, namedCheckConstraintCount)
}

func TestBootstrap_FailsWhenRegisteredTablesAreNotFKClosed(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_closure_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.users (id UUID PRIMARY KEY)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.posts (
			id UUID PRIMARY KEY,
			author_id UUID NOT NULL REFERENCES %s.users(id)
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-closure-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "posts"},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "not FK-closed")
	require.Contains(t, err.Error(), schemaName+".posts")
	require.Contains(t, err.Error(), schemaName+".users")
}

func TestBootstrap_AllowsSelfReferencingRegisteredTable(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_self_ref_ok_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.categories (
			id UUID PRIMARY KEY,
			parent_id UUID,
			CONSTRAINT categories_parent_id_fkey
				FOREIGN KEY (parent_id) REFERENCES %s.categories(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-self-ref-ok-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "categories"},
		},
	}, logger)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, svc.Close(context.Background()))
}

func TestBootstrap_FailsWhenRegisteredTableUsesUnsupportedNonUUIDSyncKey(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_key_type_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.products (
			code TEXT PRIMARY KEY,
			name TEXT NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-key-type-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "products"},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "unsupported sync key column type")
	require.Contains(t, err.Error(), schemaName+".products")
}

func TestBootstrap_FailsWhenRegisteredTableUsesCompositePrimaryKey(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_composite_key_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.memberships (
			user_id UUID NOT NULL,
			group_id UUID NOT NULL,
			PRIMARY KEY (user_id, group_id)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-composite-key-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "memberships"},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "must have a single-column primary key")
	require.Contains(t, err.Error(), schemaName+".memberships")
}

func TestBootstrap_FailsWhenDeclaredSyncKeyDoesNotMatchPrimaryKey(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_declared_key_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			id UUID PRIMARY KEY,
			external_id UUID UNIQUE
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-declared-key-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"external_id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "requires the table primary key")
	require.Contains(t, err.Error(), "external_id")
}

func TestBootstrap_FailsWhenRegisteredSchemaContainsCompositeFKConstraint(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_composite_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.parent (
			id UUID PRIMARY KEY,
			tenant_id UUID NOT NULL,
			UNIQUE (tenant_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.child (
			id UUID PRIMARY KEY,
			tenant_id UUID NOT NULL,
			parent_id UUID NOT NULL,
			CONSTRAINT child_parent_composite_fk
				FOREIGN KEY (tenant_id, parent_id) REFERENCES %s.parent(tenant_id, id)
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-composite-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parent"},
			{Schema: schemaName, Table: "child"},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "unsupported composite FK constraints")
	require.Contains(t, err.Error(), "child_parent_composite_fk")
}

func TestBootstrap_FailsClosedWhenRegisteredFKRemainsNonDeferrable(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_nondeferrable_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.parent (id UUID PRIMARY KEY)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.child (
			id UUID PRIMARY KEY,
			parent_id UUID NOT NULL,
			CONSTRAINT child_parent_fk
				FOREIGN KEY (parent_id) REFERENCES %s.parent(id)
				NOT DEFERRABLE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-nondeferrable-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parent"},
			{Schema: schemaName, Table: "child"},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "non-deferrable FK constraints")
	require.Contains(t, err.Error(), schemaName+".child_parent_fk")
	require.Contains(t, err.Error(), "make these constraints DEFERRABLE before bootstrap")
}

func TestBootstrap_AllowsDeferrableButInitiallyImmediateFKs(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_immediate_ok_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.parent (id UUID PRIMARY KEY)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.child (
			id UUID PRIMARY KEY,
			parent_id UUID,
			CONSTRAINT child_parent_fk
				FOREIGN KEY (parent_id) REFERENCES %s.parent(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-initially-immediate-ok-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parent"},
			{Schema: schemaName, Table: "child"},
		},
	}, logger)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, svc.Close(context.Background()))
}
