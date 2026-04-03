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
	"github.com/jackc/pgx/v5/pgconn"
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

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-trigger-bootstrap-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	var captureTriggerCount int
	var ownerGuardTriggerCount int
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

	require.NoError(t, svc.Close(context.Background()))
}

func TestWithinSyncBundle_CapturesDirectServerWrite(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_direct_write_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-direct-write-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-user-" + suffix
	rowID := uuid.New()
	actor := Actor{UserID: userID}
	source := BundleSource{SourceID: "server-app", SourceBundleID: 1}
	mustInitializeEmptyScope(t, ctx, svc, userID, source.SourceID)

	err := svc.WithinSyncBundle(ctx, actor, source, func(tx pgx.Tx) error {
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

func TestWithinSyncBundle_RollbackLeavesNoVisibleBundle(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_rollback_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-rollback-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-rollback-user-" + suffix
	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "server-app")

	err := svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
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

func TestWithinSyncBundle_DoesNotRetryCallbackOnRetryableTransactionError(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_single_attempt_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-single-attempt-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-single-attempt-user-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, userID, "server-app")

	attempts := 0
	err := svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		attempts++
		return &pgconn.PgError{Code: "40001", Message: "synthetic serialization failure"}
	})
	require.Error(t, err)
	require.Equal(t, 1, attempts)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&bundleCount))
	require.Zero(t, bundleCount)

	var sourceStateCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.source_state
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&sourceStateCount))
	require.Zero(t, sourceStateCount)
}

func TestWithinSyncBundle_RetiredSourceFailsClosed(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_retired_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-retired-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-retired-user-" + suffix
	oldSourceID := "server-old"
	newSourceID := "server-new"
	mustInitializeEmptyScope(t, ctx, svc, userID, oldSourceID)

	_, err := svc.CreateSnapshotSessionWithRequest(ctx, Actor{UserID: userID, SourceID: oldSourceID}, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{
			PreviousSourceID: oldSourceID,
			NewSourceID:      newSourceID,
			Reason:           "history_pruned",
		},
	})
	require.NoError(t, err)

	err = svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: oldSourceID, SourceBundleID: 1}, func(tx pgx.Tx) error {
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

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-cascade-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-cascade-user-" + suffix
	userRowID := uuid.New()
	postRowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "server-app")

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
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

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 2}, func(tx pgx.Tx) error {
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

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-cascade-update-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parents", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "children", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-cascade-update-user-" + suffix
	parentID := uuid.New()
	newParentID := uuid.New()
	childID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "server-app")

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
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

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 2}, func(tx pgx.Tx) error {
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

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-trigger-write-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "profiles", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "bundle-trigger-write-user-" + suffix
	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, userID, "server-app")

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
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

func TestWithinSyncBundle_RejectsMismatchedOwnerOnInsert(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_owner_insert_reject_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-owner-insert-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, "actor-a", "server-app")
	err := svc.WithinSyncBundle(ctx, Actor{UserID: "actor-a"}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), "actor-b", rowID, "Mallory", "mallory@example.com")
		return err
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scope mismatch")
}

func TestWithinSyncBundle_RejectsCrossOwnerWriteByVisibleKeyAlone(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_cross_owner_reject_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-cross-owner-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, "owner-a", "server-app")
	mustInitializeEmptyScope(t, ctx, svc, "owner-b", "server-app")
	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: "owner-a"}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Owner A", "owner-a@example.com")
		return err
	}))

	err := svc.WithinSyncBundle(ctx, Actor{UserID: "owner-b"}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Intruder")
		return err
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scope mismatch")

	err = svc.WithinSyncBundle(ctx, Actor{UserID: "owner-b"}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			DELETE FROM %s.users
			WHERE id = $1
		`, pgx.Identifier{schemaName}.Sanitize()), rowID)
		return err
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scope mismatch")
}

func TestWithinSyncBundle_RejectsOwnerMutationOnUpdate(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "bundle_owner_update_reject_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-owner-update-reject-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	rowID := uuid.New()
	mustInitializeEmptyScope(t, ctx, svc, "owner-a", "server-app")
	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: "owner-a"}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Owner A", "owner-a@example.com")
		return err
	}))

	err := svc.WithinSyncBundle(ctx, Actor{UserID: "owner-a"}, BundleSource{SourceID: "server-app", SourceBundleID: 2}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET _sync_scope_id = $2
			WHERE id = $1
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "owner-b")
		return err
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "scope mutation is not allowed")
}
