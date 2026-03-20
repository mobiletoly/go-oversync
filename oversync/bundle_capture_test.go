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
			{Schema: schemaName, Table: "users"},
			{Schema: schemaName, Table: "posts"},
		},
	}, logger)

	var triggerCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relname IN ('users', 'posts')
		  AND t.tgname = $2
		  AND NOT t.tgisinternal
	`, schemaName, registeredTableCaptureTriggerName).Scan(&triggerCount))
	require.Equal(t, 2, triggerCount)

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
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "bundle-user-" + suffix
	rowID := uuid.New()
	actor := Actor{UserID: userID}
	source := BundleSource{SourceID: "server-app", SourceBundleID: 1}

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
		WHERE user_id = $1
	`, userID).Scan(&bundleSeq, &bundleSourceID, &rowCount, &byteCount))
	require.Equal(t, int64(1), bundleSeq)
	require.Equal(t, source.SourceID, bundleSourceID)
	require.Equal(t, 1, rowCount)
	require.Positive(t, byteCount)

	var (
		op         string
		keyJSON    string
		rowVersion int64
		payload    []byte
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT op, key_json, row_version, payload
		FROM sync.bundle_rows
		WHERE user_id = $1 AND bundle_seq = $2 AND row_ordinal = 1
	`, userID, bundleSeq).Scan(&op, &keyJSON, &rowVersion, &payload))
	require.Equal(t, OpInsert, op)
	require.Equal(t, bundleSeq, rowVersion)

	key, err := decodeSyncKeyJSON(keyJSON)
	require.NoError(t, err)
	require.Equal(t, rowID.String(), key["id"])

	var payloadMap map[string]any
	require.NoError(t, json.Unmarshal(payload, &payloadMap))
	require.Equal(t, rowID.String(), payloadMap["id"])
	require.Equal(t, "Alice", payloadMap["name"])
	require.Equal(t, "alice@example.com", payloadMap["email"])

	var (
		deleted        bool
		stateBundleSeq int64
		stateVersion   int64
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted, bundle_seq, row_version
		FROM sync.row_state
		WHERE user_id = $1
		  AND schema_name = $2
		  AND table_name = 'users'
		  AND key_json = $3
	`, userID, schemaName, keyJSON).Scan(&deleted, &stateBundleSeq, &stateVersion))
	require.False(t, deleted)
	require.Equal(t, bundleSeq, stateBundleSeq)
	require.Equal(t, bundleSeq, stateVersion)
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
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "bundle-rollback-user-" + suffix
	rowID := uuid.New()

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
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_id = $1`, userID).Scan(&bundleCount))
	require.Zero(t, bundleCount)

	var bundleRowCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_rows WHERE user_id = $1`, userID).Scan(&bundleRowCount))
	require.Zero(t, bundleRowCount)

	var rowStateCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.row_state WHERE user_id = $1`, userID).Scan(&rowStateCount))
	require.Zero(t, rowStateCount)

	var stagedCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_capture_stage WHERE user_id = $1`, userID).Scan(&stagedCount))
	require.Zero(t, stagedCount)
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
			{Schema: schemaName, Table: "users"},
			{Schema: schemaName, Table: "posts"},
		},
	}, logger)

	userID := "bundle-cascade-user-" + suffix
	userRowID := uuid.New()
	postRowID := uuid.New()

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

	rows, err := pool.Query(ctx, `
		SELECT schema_name, table_name, op
		FROM sync.bundle_rows
		WHERE user_id = $1
		  AND bundle_seq = 2
		ORDER BY row_ordinal
	`, userID)
	require.NoError(t, err)
	defer rows.Close()

	type bundleDelete struct {
		schema string
		table  string
		op     string
	}
	var deletes []bundleDelete
	for rows.Next() {
		var item bundleDelete
		require.NoError(t, rows.Scan(&item.schema, &item.table, &item.op))
		deletes = append(deletes, item)
	}
	require.NoError(t, rows.Err())
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
			id UUID PRIMARY KEY,
			name TEXT NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.children (
			id UUID PRIMARY KEY,
			parent_id UUID NOT NULL,
			name TEXT NOT NULL,
			CONSTRAINT children_parent_id_fkey
				FOREIGN KEY (parent_id) REFERENCES %s.parents(id)
				ON UPDATE CASCADE
				ON DELETE CASCADE
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bundle-cascade-update-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "parents"},
			{Schema: schemaName, Table: "children"},
		},
	}, logger)

	userID := "bundle-cascade-update-user-" + suffix
	parentID := uuid.New()
	newParentID := uuid.New()
	childID := uuid.New()

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

	rows, err := pool.Query(ctx, `
		SELECT table_name, op, key_json, payload
		FROM sync.bundle_rows
		WHERE user_id = $1
		  AND bundle_seq = 2
		ORDER BY row_ordinal
	`, userID)
	require.NoError(t, err)
	defer rows.Close()

	type bundleEffect struct {
		table   string
		op      string
		keyJSON string
		payload []byte
	}
	var effects []bundleEffect
	for rows.Next() {
		var item bundleEffect
		require.NoError(t, rows.Scan(&item.table, &item.op, &item.keyJSON, &item.payload))
		effects = append(effects, item)
	}
	require.NoError(t, rows.Err())
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
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.profiles (
			id UUID PRIMARY KEY,
			nickname TEXT NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s.create_profile_for_user()
		RETURNS TRIGGER
		LANGUAGE plpgsql
		AS $$
		BEGIN
			INSERT INTO %s.profiles (id, nickname)
			VALUES (NEW.id, NEW.name);
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
			{Schema: schemaName, Table: "users"},
			{Schema: schemaName, Table: "profiles"},
		},
	}, logger)

	userID := "bundle-trigger-write-user-" + suffix
	rowID := uuid.New()

	require.NoError(t, svc.WithinSyncBundle(ctx, Actor{UserID: userID}, BundleSource{SourceID: "server-app", SourceBundleID: 1}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, schemaIdent), rowID, "Trigger User", "trigger@example.com")
		return err
	}))

	rows, err := pool.Query(ctx, `
		SELECT table_name, op
		FROM sync.bundle_rows
		WHERE user_id = $1
		  AND bundle_seq = 1
		ORDER BY row_ordinal
	`, userID)
	require.NoError(t, err)
	defer rows.Close()

	type triggerEffect struct {
		table string
		op    string
	}
	var effects []triggerEffect
	for rows.Next() {
		var item triggerEffect
		require.NoError(t, rows.Scan(&item.table, &item.op))
		effects = append(effects, item)
	}
	require.NoError(t, rows.Err())
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
