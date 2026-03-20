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

func requireBundlesSemanticallyEqual(t *testing.T, expected, actual *Bundle) {
	t.Helper()
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	require.Equal(t, expected.BundleSeq, actual.BundleSeq)
	require.Equal(t, expected.SourceID, actual.SourceID)
	require.Equal(t, expected.SourceBundleID, actual.SourceBundleID)
	require.Equal(t, expected.RowCount, actual.RowCount)
	require.Equal(t, expected.BundleHash, actual.BundleHash)
	require.Len(t, actual.Rows, len(expected.Rows))
	for i := range expected.Rows {
		require.Equal(t, expected.Rows[i].Schema, actual.Rows[i].Schema)
		require.Equal(t, expected.Rows[i].Table, actual.Rows[i].Table)
		require.Equal(t, expected.Rows[i].Key, actual.Rows[i].Key)
		require.Equal(t, expected.Rows[i].Op, actual.Rows[i].Op)
		require.Equal(t, expected.Rows[i].RowVersion, actual.Rows[i].RowVersion)
		if expected.Rows[i].Payload == nil || actual.Rows[i].Payload == nil {
			require.Equal(t, expected.Rows[i].Payload, actual.Rows[i].Payload)
			continue
		}
		require.JSONEq(t, string(expected.Rows[i].Payload), string(actual.Rows[i].Payload))
	}
}

func TestPushSessions_ConflictRejectsWholeBundle(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_conflict_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-conflict-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "push-conflict-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "device-a"}
	rowID := uuid.New()

	_, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice","email":"alice@example.com"}`, rowID)),
	}})
	require.NoError(t, err)

	_, err = pushRowsViaSession(t, ctx, svc, actor, 2, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpUpdate,
		BaseRowVersion: 999,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice 2","email":"alice2@example.com"}`, rowID)),
	}})
	var conflictErr *PushConflictError
	require.ErrorAs(t, err, &conflictErr)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_id = $1`, userID).Scan(&bundleCount))
	require.Equal(t, 1, bundleCount)

	var name string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE id = $1`, pgx.Identifier{schemaName}.Sanitize()), rowID).Scan(&name))
	require.Equal(t, "Alice", name)
}

func TestPushSessions_UpdateAfterInsertUsesCapturedRowStateKey(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_update_after_insert_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-update-after-insert-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "push-update-after-insert-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "device-a"}
	rowID := uuid.New()

	firstBundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice","email":"alice@example.com"}`, rowID)),
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstBundle.BundleSeq)

	secondBundle, err := pushRowsViaSession(t, ctx, svc, actor, 2, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpUpdate,
		BaseRowVersion: 1,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice Updated","email":"alice-updated@example.com"}`, rowID)),
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), secondBundle.BundleSeq)

	var canonicalKeyJSON string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT key_json
		FROM sync.row_state
		WHERE user_id = $1 AND schema_name = $2 AND table_name = 'users' AND row_version = 2
	`, userID, schemaName).Scan(&canonicalKeyJSON))
	require.Equal(t, fmt.Sprintf(`{"id":"%s"}`, rowID), canonicalKeyJSON)

	var name string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE id = $1`, pgx.Identifier{schemaName}.Sanitize()), rowID).Scan(&name))
	require.Equal(t, "Alice Updated", name)
}

func TestPushSessions_DeleteParentAfterInsertCascadesChildRowsAndUpdatesState(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_delete_cascade_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-delete-cascade-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
			{Schema: schemaName, Table: "posts"},
		},
	}, logger)

	userID := "push-delete-cascade-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "device-a"}
	userRowID := uuid.New()
	postRowID := uuid.New()

	insertBundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{
		{
			Schema:         schemaName,
			Table:          "posts",
			Key:            SyncKey{"id": postRowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","title":"Hello","content":"World","author_id":"%s"}`, postRowID, userRowID)),
		},
		{
			Schema:         schemaName,
			Table:          "users",
			Key:            SyncKey{"id": userRowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alice","email":"alice@example.com"}`, userRowID)),
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), insertBundle.BundleSeq)

	deleteBundle, err := pushRowsViaSession(t, ctx, svc, actor, 2, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": userRowID.String()},
		Op:             OpDelete,
		BaseRowVersion: 1,
	}})
	require.NoError(t, err)
	require.Equal(t, int64(2), deleteBundle.BundleSeq)
	require.Len(t, deleteBundle.Rows, 2)
	require.ElementsMatch(t, []string{"users:DELETE", "posts:DELETE"}, []string{
		deleteBundle.Rows[0].Table + ":" + deleteBundle.Rows[0].Op,
		deleteBundle.Rows[1].Table + ":" + deleteBundle.Rows[1].Op,
	})

	var userCount int
	var postCount int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, pgx.Identifier{schemaName}.Sanitize()), userRowID).Scan(&userCount))
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.posts WHERE id = $1`, pgx.Identifier{schemaName}.Sanitize()), postRowID).Scan(&postCount))
	require.Equal(t, 0, userCount)
	require.Equal(t, 0, postCount)

	type rowState struct {
		deleted    bool
		rowVersion int64
	}
	var userState rowState
	var postState rowState
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted, row_version
		FROM sync.row_state
		WHERE user_id = $1 AND schema_name = $2 AND table_name = 'users' AND key_json = $3
	`, userID, schemaName, fmt.Sprintf(`{"id":"%s"}`, userRowID)).Scan(&userState.deleted, &userState.rowVersion))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted, row_version
		FROM sync.row_state
		WHERE user_id = $1 AND schema_name = $2 AND table_name = 'posts' AND key_json = $3
	`, userID, schemaName, fmt.Sprintf(`{"id":"%s"}`, postRowID)).Scan(&postState.deleted, &postState.rowVersion))
	require.True(t, userState.deleted)
	require.True(t, postState.deleted)
	require.Equal(t, int64(2), userState.rowVersion)
	require.Equal(t, int64(2), postState.rowVersion)
}

func TestPushSessions_AlreadyCommittedReturnsSameCommittedBundle(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_replay_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-replay-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "push-replay-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "device-a"}
	rowID := uuid.New()
	rows := []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Replay","email":"replay@example.com"}`, rowID)),
	}}

	firstBundle, err := pushRowsViaSession(t, ctx, svc, actor, 7, rows)
	require.NoError(t, err)
	replayBundle, err := pushRowsViaSession(t, ctx, svc, actor, 7, rows)
	require.NoError(t, err)
	requireBundlesSemanticallyEqual(t, firstBundle, replayBundle)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_id = $1`, userID).Scan(&bundleCount))
	require.Equal(t, 1, bundleCount)
}

func TestPushSessions_AllowsSelfReferentialInsert(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_self_ref_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.categories (
			id UUID PRIMARY KEY,
			parent_id UUID,
			name TEXT NOT NULL,
			CONSTRAINT categories_parent_id_fkey
				FOREIGN KEY (parent_id) REFERENCES %s.categories(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-self-ref-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "categories"},
		},
	}, logger)

	actor := Actor{UserID: "push-self-ref-user-" + suffix, SourceID: "device-a"}
	rowID := uuid.New()

	bundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "categories",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","parent_id":"%s","name":"Root"}`, rowID, rowID)),
	}})
	require.NoError(t, err)
	require.Len(t, bundle.Rows, 1)

	var parentID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT parent_id FROM %s.categories WHERE id = $1`, schemaIdent), rowID).Scan(&parentID))
	require.Equal(t, rowID, parentID)
}

func TestPushSessions_AllowsTwoTableCycleInsert(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_two_cycle_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.alpha (
			id UUID PRIMARY KEY,
			beta_id UUID NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.beta (
			id UUID PRIMARY KEY,
			alpha_id UUID NOT NULL,
			CONSTRAINT beta_alpha_id_fkey
				FOREIGN KEY (alpha_id) REFERENCES %s.alpha(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s.alpha
		ADD CONSTRAINT alpha_beta_id_fkey
			FOREIGN KEY (beta_id) REFERENCES %s.beta(id)
			DEFERRABLE INITIALLY IMMEDIATE
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-two-cycle-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "alpha"},
			{Schema: schemaName, Table: "beta"},
		},
	}, logger)

	actor := Actor{UserID: "push-two-cycle-user-" + suffix, SourceID: "device-a"}
	alphaID := uuid.New()
	betaID := uuid.New()

	bundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{
		{
			Schema:         schemaName,
			Table:          "alpha",
			Key:            SyncKey{"id": alphaID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","beta_id":"%s"}`, alphaID, betaID)),
		},
		{
			Schema:         schemaName,
			Table:          "beta",
			Key:            SyncKey{"id": betaID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","alpha_id":"%s"}`, betaID, alphaID)),
		},
	})
	require.NoError(t, err)
	require.Len(t, bundle.Rows, 2)

	var persistedBetaID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT beta_id FROM %s.alpha WHERE id = $1`, schemaIdent), alphaID).Scan(&persistedBetaID))
	require.Equal(t, betaID, persistedBetaID)
	var persistedAlphaID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT alpha_id FROM %s.beta WHERE id = $1`, schemaIdent), betaID).Scan(&persistedAlphaID))
	require.Equal(t, alphaID, persistedAlphaID)
}

func TestPushSessions_AllowsThreeTableCycleInsert(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_three_cycle_" + suffix
	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.alpha (
			id UUID PRIMARY KEY,
			beta_id UUID NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.beta (
			id UUID PRIMARY KEY,
			gamma_id UUID NOT NULL
		)`, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.gamma (
			id UUID PRIMARY KEY,
			alpha_id UUID NOT NULL,
			CONSTRAINT gamma_alpha_id_fkey
				FOREIGN KEY (alpha_id) REFERENCES %s.alpha(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s.alpha
		ADD CONSTRAINT alpha_beta_id_fkey
			FOREIGN KEY (beta_id) REFERENCES %s.beta(id)
			DEFERRABLE INITIALLY IMMEDIATE
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s.beta
		ADD CONSTRAINT beta_gamma_id_fkey
			FOREIGN KEY (gamma_id) REFERENCES %s.gamma(id)
			DEFERRABLE INITIALLY IMMEDIATE
	`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-three-cycle-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "alpha"},
			{Schema: schemaName, Table: "beta"},
			{Schema: schemaName, Table: "gamma"},
		},
	}, logger)

	actor := Actor{UserID: "push-three-cycle-user-" + suffix, SourceID: "device-a"}
	alphaID := uuid.New()
	betaID := uuid.New()
	gammaID := uuid.New()

	bundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{
		{
			Schema:         schemaName,
			Table:          "alpha",
			Key:            SyncKey{"id": alphaID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","beta_id":"%s"}`, alphaID, betaID)),
		},
		{
			Schema:         schemaName,
			Table:          "beta",
			Key:            SyncKey{"id": betaID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","gamma_id":"%s"}`, betaID, gammaID)),
		},
		{
			Schema:         schemaName,
			Table:          "gamma",
			Key:            SyncKey{"id": gammaID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","alpha_id":"%s"}`, gammaID, alphaID)),
		},
	})
	require.NoError(t, err)
	require.Len(t, bundle.Rows, 3)

	var persistedBetaID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT beta_id FROM %s.alpha WHERE id = $1`, schemaIdent), alphaID).Scan(&persistedBetaID))
	require.Equal(t, betaID, persistedBetaID)
	var persistedGammaID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT gamma_id FROM %s.beta WHERE id = $1`, schemaIdent), betaID).Scan(&persistedGammaID))
	require.Equal(t, gammaID, persistedGammaID)
	var persistedAlphaID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT alpha_id FROM %s.gamma WHERE id = $1`, schemaIdent), gammaID).Scan(&persistedAlphaID))
	require.Equal(t, alphaID, persistedAlphaID)
}
