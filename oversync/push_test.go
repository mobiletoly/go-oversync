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

func runPushCycleInsertTest(t *testing.T, scenario string, tables []string) {
	t.Helper()
	require.GreaterOrEqual(t, len(tables), 2)

	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "push_" + scenario + "_" + suffix
	createPushCycleSchema(t, ctx, pool, schemaName, tables)
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	registeredTables := make([]RegisteredTable, 0, len(tables))
	for _, table := range tables {
		registeredTables = append(registeredTables, RegisteredTable{
			Schema:         schemaName,
			Table:          table,
			SyncKeyColumns: []string{"id"},
		})
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-" + strings.ReplaceAll(scenario, "_", "-") + "-test",
		RegisteredTables:          registeredTables,
	}, logger)

	actor := Actor{UserID: "push-" + scenario + "-user-" + suffix, SourceID: "device-a"}
	ids := make(map[string]uuid.UUID, len(tables))
	for _, table := range tables {
		ids[table] = uuid.New()
	}

	rows := make([]PushRequestRow, 0, len(tables))
	for i, table := range tables {
		nextTable := tables[(i+1)%len(tables)]
		rows = append(rows, pushCycleInsertRow(t, schemaName, table, ids[table], nextTable+"_id", ids[nextTable]))
	}
	bundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, rows)
	require.NoError(t, err)
	require.Len(t, bundle.Rows, len(tables))

	for i, table := range tables {
		nextTable := tables[(i+1)%len(tables)]
		requirePushCycleLink(t, ctx, pool, schemaName, table, ids[table], nextTable+"_id", ids[nextTable])
	}
}

func createPushCycleSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaName string, tables []string) {
	t.Helper()

	require.NoError(t, dropTestSchema(ctx, pool, schemaName))
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent))
	require.NoError(t, err)

	for i, table := range tables {
		nextTable := tables[(i+1)%len(tables)]
		tableIdent := pgx.Identifier{schemaName, table}.Sanitize()
		nextColumnIdent := pgx.Identifier{nextTable + "_id"}.Sanitize()
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				%s UUID NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)`, tableIdent, nextColumnIdent))
		require.NoError(t, err)
	}

	for i, table := range tables {
		nextTable := tables[(i+1)%len(tables)]
		tableIdent := pgx.Identifier{schemaName, table}.Sanitize()
		nextTableIdent := pgx.Identifier{schemaName, nextTable}.Sanitize()
		nextColumnIdent := pgx.Identifier{nextTable + "_id"}.Sanitize()
		constraintIdent := pgx.Identifier{table + "_" + nextTable + "_id_fkey"}.Sanitize()
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			ALTER TABLE %s
			ADD CONSTRAINT %s
				FOREIGN KEY (_sync_scope_id, %s) REFERENCES %s(_sync_scope_id, id)
				DEFERRABLE INITIALLY IMMEDIATE
		`, tableIdent, constraintIdent, nextColumnIdent, nextTableIdent))
		require.NoError(t, err)
	}
}

func pushCycleInsertRow(t *testing.T, schemaName, table string, id uuid.UUID, nextColumn string, nextID uuid.UUID) PushRequestRow {
	t.Helper()

	payload, err := json.Marshal(map[string]string{
		"id":       id.String(),
		nextColumn: nextID.String(),
	})
	require.NoError(t, err)

	return PushRequestRow{
		Schema:         schemaName,
		Table:          table,
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(payload),
	}
}

func requirePushCycleLink(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
	table string,
	id uuid.UUID,
	nextColumn string,
	expectedNextID uuid.UUID,
) {
	t.Helper()

	var persistedNextID uuid.UUID
	tableIdent := pgx.Identifier{schemaName, table}.Sanitize()
	nextColumnIdent := pgx.Identifier{nextColumn}.Sanitize()
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE id = $1`, nextColumnIdent, tableIdent), id).Scan(&persistedNextID))
	require.Equal(t, expectedNextID, persistedNextID)
}

type pushUsersFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	suffix     string
	actor      Actor
}

func newPushUsersFixture(t *testing.T, ctx context.Context, schemaPrefix, appName, userPrefix string) *pushUsersFixture {
	t.Helper()

	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := schemaPrefix + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   appName,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	return &pushUsersFixture{
		ctx:        ctx,
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		suffix:     suffix,
		actor:      Actor{UserID: userPrefix + suffix, SourceID: "device-a"},
	}
}

func (f *pushUsersFixture) actorWith(userPrefix, sourceID string) Actor {
	return Actor{UserID: userPrefix + f.suffix, SourceID: sourceID}
}

func (f *pushUsersFixture) push(t *testing.T, actor Actor, sourceBundleID int64, rows ...PushRequestRow) (*Bundle, error) {
	t.Helper()
	return pushRowsViaSession(t, f.ctx, f.svc, actor, sourceBundleID, rows)
}

func (f *pushUsersFixture) userInsert(rowID uuid.UUID, name, email string) PushRequestRow {
	return f.userWrite(rowID, OpInsert, 0, name, email)
}

func (f *pushUsersFixture) userUpdate(rowID uuid.UUID, baseRowVersion int64, name, email string) PushRequestRow {
	return f.userWrite(rowID, OpUpdate, baseRowVersion, name, email)
}

func (f *pushUsersFixture) userDelete(rowID uuid.UUID, baseRowVersion int64) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpDelete,
		BaseRowVersion: baseRowVersion,
	}
}

func (f *pushUsersFixture) userWrite(rowID uuid.UUID, op string, baseRowVersion int64, name, email string) PushRequestRow {
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             op,
		BaseRowVersion: baseRowVersion,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"%s","email":"%s"}`, rowID, name, email)),
	}
}

func (f *pushUsersFixture) bundleLogCount(t *testing.T, userID string) int {
	t.Helper()

	var bundleCount int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM sync.bundle_log WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)`, userID).Scan(&bundleCount))
	return bundleCount
}

func (f *pushUsersFixture) userRowCount(t *testing.T, rowID uuid.UUID) int {
	t.Helper()

	var rowCount int
	require.NoError(t, f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.users WHERE id = $1`, pgx.Identifier{f.schemaName}.Sanitize()), rowID).Scan(&rowCount))
	return rowCount
}

func (f *pushUsersFixture) userName(t *testing.T, rowID uuid.UUID) string {
	t.Helper()

	return f.scanUserName(t, fmt.Sprintf(`SELECT name FROM %s.users WHERE id = $1`, pgx.Identifier{f.schemaName}.Sanitize()), rowID)
}

func (f *pushUsersFixture) scopedUserName(t *testing.T, userID string, rowID uuid.UUID) string {
	t.Helper()

	return f.scanUserName(t, fmt.Sprintf(`
		SELECT name
		FROM %s.users
		WHERE _sync_scope_id = $1 AND id = $2
	`, pgx.Identifier{f.schemaName}.Sanitize()), userID, rowID)
}

func (f *pushUsersFixture) scanUserName(t *testing.T, query string, args ...any) string {
	t.Helper()

	var name string
	require.NoError(t, f.pool.QueryRow(f.ctx, query, args...).Scan(&name))
	return name
}

func (f *pushUsersFixture) rowStateBundleSeq(t *testing.T, userID string, rowID uuid.UUID) int64 {
	t.Helper()

	userPK, tableID, keyBytes := mustCompactStorageIdentity(t, f.ctx, f.svc, userID, f.schemaName, "users", rowID.String())
	var stateBundleSeq int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `
		SELECT bundle_seq
		FROM sync.row_state
		WHERE user_pk = $1 AND table_id = $2 AND key_bytes = $3
	`, userPK, tableID, keyBytes).Scan(&stateBundleSeq))
	return stateBundleSeq
}

func TestPushSessions_ConflictRejectsWholeBundle(t *testing.T) {
	ctx := context.Background()
	fixture := newPushUsersFixture(t, ctx, "push_conflict_", "push-conflict-test", "push-conflict-user-")
	rowID := uuid.New()

	_, err := fixture.push(t, fixture.actor, 1, fixture.userInsert(rowID, "Alice", "alice@example.com"))
	require.NoError(t, err)

	_, err = fixture.push(t, fixture.actor, 2, fixture.userUpdate(rowID, 999, "Alice 2", "alice2@example.com"))
	var conflictErr *PushConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.NotNil(t, conflictErr.Conflict)
	require.NotContains(t, string(conflictErr.Conflict.ServerRow), `"_sync_scope_id"`)
	require.Equal(t, 1, fixture.bundleLogCount(t, fixture.actor.UserID))
	require.Equal(t, "Alice", fixture.userName(t, rowID))
}

func TestPushSessions_AllowsSameVisibleSyncKeyForDifferentUsers(t *testing.T) {
	ctx := context.Background()
	fixture := newPushUsersFixture(t, ctx, "push_same_key_users_", "push-same-key-users-test", "push-same-key-user-a-")
	rowID := uuid.New()
	actorA := fixture.actor
	actorB := fixture.actorWith("push-same-key-user-b-", "device-b")

	_, err := fixture.push(t, actorA, 1, fixture.userInsert(rowID, "Alice", "alice@example.com"))
	require.NoError(t, err)

	_, err = fixture.push(t, actorB, 1, fixture.userInsert(rowID, "Bob", "bob@example.com"))
	require.NoError(t, err)

	require.Equal(t, 2, fixture.userRowCount(t, rowID))
	require.Equal(t, "Alice", fixture.scopedUserName(t, actorA.UserID, rowID))
	require.Equal(t, "Bob", fixture.scopedUserName(t, actorB.UserID, rowID))
}

func TestPushSessions_WrongOwnerUpdateDeleteDoNotAffectAnotherUsersRow(t *testing.T) {
	ctx := context.Background()
	fixture := newPushUsersFixture(t, ctx, "push_wrong_owner_conflict_", "push-wrong-scope-conflict-test", "push-owner-a-")
	rowID := uuid.New()
	actorA := fixture.actor
	actorB := fixture.actorWith("push-owner-b-", "device-b")

	_, err := fixture.push(t, actorA, 1, fixture.userInsert(rowID, "Owner A", "owner-a@example.com"))
	require.NoError(t, err)

	_, err = fixture.push(t, actorB, 1, fixture.userUpdate(rowID, 1, "Intruder", "intruder@example.com"))
	var updateConflict *PushConflictError
	require.ErrorAs(t, err, &updateConflict)
	require.Equal(t, SyncKey{"id": rowID.String()}, updateConflict.Conflict.Key)

	_, err = fixture.push(t, actorB, 1, fixture.userDelete(rowID, 1))
	var deleteConflict *PushConflictError
	require.ErrorAs(t, err, &deleteConflict)
	require.Equal(t, SyncKey{"id": rowID.String()}, deleteConflict.Conflict.Key)

	require.Equal(t, "Owner A", fixture.scopedUserName(t, actorA.UserID, rowID))
	require.Equal(t, 1, fixture.userRowCount(t, rowID))
}

func TestPushSessions_UpdateAfterInsertUsesCapturedRowStateKey(t *testing.T) {
	ctx := context.Background()
	fixture := newPushUsersFixture(t, ctx, "push_update_after_insert_", "push-update-after-insert-test", "push-update-after-insert-user-")
	rowID := uuid.New()

	firstBundle, err := fixture.push(t, fixture.actor, 1, fixture.userInsert(rowID, "Alice", "alice@example.com"))
	require.NoError(t, err)
	require.Equal(t, int64(1), firstBundle.BundleSeq)

	secondBundle, err := fixture.push(t, fixture.actor, 2, fixture.userUpdate(rowID, 1, "Alice Updated", "alice-updated@example.com"))
	require.NoError(t, err)
	require.Equal(t, int64(2), secondBundle.BundleSeq)
	require.Equal(t, int64(2), fixture.rowStateBundleSeq(t, fixture.actor.UserID, rowID))
	require.Equal(t, "Alice Updated", fixture.userName(t, rowID))
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
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
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
	userPK, userTableID, userKeyBytes := mustCompactStorageIdentity(t, ctx, svc, userID, schemaName, "users", userRowID.String())
	_, postTableID, postKeyBytes := mustCompactStorageIdentity(t, ctx, svc, userID, schemaName, "posts", postRowID.String())
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted, bundle_seq
		FROM sync.row_state
		WHERE user_pk = $1 AND table_id = $2 AND key_bytes = $3
	`, userPK, userTableID, userKeyBytes).Scan(&userState.deleted, &userState.rowVersion))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted, bundle_seq
		FROM sync.row_state
		WHERE user_pk = $1 AND table_id = $2 AND key_bytes = $3
	`, userPK, postTableID, postKeyBytes).Scan(&postState.deleted, &postState.rowVersion))
	require.True(t, userState.deleted)
	require.True(t, postState.deleted)
	require.Equal(t, int64(2), userState.rowVersion)
	require.Equal(t, int64(2), postState.rowVersion)
}

func TestPushSessions_AlreadyCommittedReturnsSameCommittedBundle(t *testing.T) {
	ctx := context.Background()
	fixture := newPushUsersFixture(t, ctx, "push_replay_", "push-replay-test", "push-replay-user-")
	rowID := uuid.New()
	rows := []PushRequestRow{fixture.userInsert(rowID, "Replay", "replay@example.com")}

	firstBundle, err := fixture.push(t, fixture.actor, 1, rows...)
	require.NoError(t, err)
	replayBundle, err := fixture.push(t, fixture.actor, 1, rows...)
	require.NoError(t, err)
	requireBundlesSemanticallyEqual(t, firstBundle, replayBundle)
	require.Equal(t, 1, fixture.bundleLogCount(t, fixture.actor.UserID))
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
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			parent_id UUID,
			name TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT categories_parent_id_fkey
				FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s.categories(_sync_scope_id, id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`, schemaIdent, schemaIdent))
	require.NoError(t, err)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "push-self-ref-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "categories", SyncKeyColumns: []string{"id"}},
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

func TestPushSessions_AllowsCycleInsert(t *testing.T) {
	tests := []struct {
		name   string
		tables []string
	}{
		{name: "two_table", tables: []string{"alpha", "beta"}},
		{name: "three_table", tables: []string{"alpha", "beta", "gamma"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPushCycleInsertTest(t, tc.name+"_cycle", tc.tables)
		})
	}
}
