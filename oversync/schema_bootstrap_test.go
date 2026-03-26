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
		rowStateSnapshotIndexExists      bool
		bundleLogIndexExists             bool
		bundleRowsIndexExists            bool
		bundleRowsKeyIndexExists         bool
		snapshotSessionsTTLIndexExists   bool
		snapshotSessionRowsIndexExists   bool
		ownerGuardFunctionExists         bool
		captureFunctionExists            bool
		snapshotLastAccessedColumnCount  int
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
			to_regclass('sync.rs_user_live_snapshot_idx') IS NOT NULL,
			to_regclass('sync.bl_user_committed_idx') IS NOT NULL,
			to_regclass('sync.br_user_bundle_ordinal_idx') IS NOT NULL,
			to_regclass('sync.br_user_bundle_key_idx') IS NOT NULL,
			to_regclass('sync.ss_expires_at_idx') IS NOT NULL,
			to_regclass('sync.ssr_snapshot_row_ordinal_idx') IS NOT NULL,
			to_regprocedure('sync.enforce_registered_row_owner()') IS NOT NULL,
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
		&rowStateSnapshotIndexExists,
		&bundleLogIndexExists,
		&bundleRowsIndexExists,
		&bundleRowsKeyIndexExists,
		&snapshotSessionsTTLIndexExists,
		&snapshotSessionRowsIndexExists,
		&ownerGuardFunctionExists,
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
	require.True(t, rowStateSnapshotIndexExists)
	require.True(t, bundleLogIndexExists)
	require.True(t, bundleRowsIndexExists)
	require.True(t, bundleRowsKeyIndexExists)
	require.True(t, snapshotSessionsTTLIndexExists)
	require.True(t, snapshotSessionRowsIndexExists)
	require.True(t, ownerGuardFunctionExists)
	require.True(t, captureFunctionExists)

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
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.posts (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			author_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT posts_author_id_fkey
				FOREIGN KEY (_sync_scope_id, author_id) REFERENCES %s.users(_sync_scope_id, id)
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-closure-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
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
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT categories_parent_id_fkey
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.categories(_sync_scope_id, id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-self-ref-ok-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "categories", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, svc.Close(context.Background()))
}

func TestBootstrap_AcceptsTextVisibleSyncKey(t *testing.T) {
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
			_sync_scope_id TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, code)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-key-type-accept-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "products", SyncKeyColumns: []string{"code"}},
		},
	}, logger)
	require.NoError(t, err)

	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, svc.Close(context.Background()))
}

func TestBootstrap_FailsWhenRegisteredTableUsesUnsupportedNumericSyncKey(t *testing.T) {
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
			_sync_scope_id TEXT NOT NULL,
			membership_no BIGINT NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, membership_no)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-key-type-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "memberships", SyncKeyColumns: []string{"membership_no"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "allows only uuid and text")
	require.Contains(t, err.Error(), schemaName+".memberships")
}

func TestBootstrap_FailsWhenRegisteredTableUsesUnsupportedIntegerSyncKey(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_integer_key_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.counters (
			_sync_scope_id TEXT NOT NULL,
			counter_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, counter_id)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-integer-key-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "counters", SyncKeyColumns: []string{"counter_id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "allows only uuid and text")
	require.Contains(t, err.Error(), schemaName+".counters")
}

func TestBootstrap_AllowsVisibleSyncKeyThatDiffersFromPrimaryKey(t *testing.T) {
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
			_sync_scope_id TEXT NOT NULL,
			pk_id UUID NOT NULL,
			external_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, pk_id),
			UNIQUE (_sync_scope_id, external_id)
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

	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, svc.Close(context.Background()))
}

func TestBootstrap_FailsWhenRegisteredTableLacksOwnerScopedSyncKeyUniqueness(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_owner_uniqueness_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			pk_id UUID NOT NULL,
			doc_id UUID NOT NULL,
			title TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, pk_id)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-owner-uniqueness-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"doc_id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "must provide unique identity (_sync_scope_id, doc_id)")
}

func TestBootstrap_FailsWhenRegisteredSchemaContainsOwnerlessUniqueConstraint(t *testing.T) {
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
		CREATE TABLE %s.profiles (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			UNIQUE (name)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "ownerless-unique-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "profiles", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "does not begin with _sync_scope_id")
	require.Contains(t, err.Error(), "profiles")
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
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.parent (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.child (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT child_parent_fk
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.parent(_sync_scope_id, id)
				NOT DEFERRABLE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-nondeferrable-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parent", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "child", SyncKeyColumns: []string{"id"}},
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

func TestBootstrap_FailsWhenOwnerColumnIsNotText(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "owner_type_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.docs (
			_sync_scope_id UUID NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "owner-type-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "must define _sync_scope_id TEXT")
}

func TestBootstrap_FailsWhenRegisteredTableUsesPartialUniqueIndex(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "partial_unique_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			deleted_at TIMESTAMPTZ
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE UNIQUE INDEX docs_owner_id_live_idx
		ON %s.docs (_sync_scope_id, id)
		WHERE deleted_at IS NULL
	`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "partial-unique-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "partial or expression unique index")
}

func TestBootstrap_FailsWhenRegisteredTableUsesExpressionUniqueIndex(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "expression_unique_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.docs (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			title TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE UNIQUE INDEX docs_owner_title_expr_uidx
		ON %s.docs (_sync_scope_id, lower(title))
	`, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "expression-unique-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "docs", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "partial or expression unique index")
}

func TestBootstrap_FailsWhenRegisteredChildFKOmitsOwnerColumn(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "fk_ownerless_reject_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.parent (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.child (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT child_parent_fk
				FOREIGN KEY (parent_id, _sync_scope_id) REFERENCES %s.parent(id, _sync_scope_id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-ownerless-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parent", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "child", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)

	err = svc.Bootstrap(ctx)
	require.Error(t, err)
	var schemaErr *UnsupportedSchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Contains(t, err.Error(), "scope-inclusive")
	require.Contains(t, err.Error(), "child_parent_fk")
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
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.parent (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.child (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT child_parent_fk
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.parent(_sync_scope_id, id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc, err := NewRuntimeService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "fk-initially-immediate-ok-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parent", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "child", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	require.NoError(t, err)
	require.NoError(t, svc.Bootstrap(ctx))
	require.NoError(t, svc.Close(context.Background()))
}
