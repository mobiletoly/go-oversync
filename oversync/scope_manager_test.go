package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func newScopeManagerIntegrationService(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName string,
	logger *slog.Logger,
) (*SyncService, *ScopeManager) {
	t.Helper()

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "scope-manager-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "file_reviews", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	return svc, NewScopeManager(svc, ScopeManagerConfig{Logger: logger})
}

func TestScopeManager_ExecWrite_ValidatesInputs(t *testing.T) {
	ctx := context.Background()

	nilMgr := NewScopeManager(nil, ScopeManagerConfig{})
	require.NotNil(t, nilMgr)
	require.NotNil(t, nilMgr.logger)
	_, err := nilMgr.ExecWrite(ctx, "scope-a", ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error { return nil })
	var invalidErr *ScopeWriteInvalidError
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, err.Error(), "sync service")

	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_validate_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)
	require.NotNil(t, mgr.logger)

	_, err = mgr.ExecWrite(ctx, "", ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error { return nil })
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, err.Error(), "scope_id")

	_, err = mgr.ExecWrite(ctx, "scope-a", ScopeWriteOptions{WriterID: ""}, func(tx pgx.Tx) error { return nil })
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, err.Error(), "writer_id")

	_, err = mgr.ExecWrite(ctx, "scope-a", ScopeWriteOptions{WriterID: "admin-panel"}, nil)
	require.ErrorAs(t, err, &invalidErr)
	require.Contains(t, err.Error(), "callback")
}

func TestScopeManager_ExecWrite_AutoInitializesAndCommitsVisibleBundle(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_auto_init_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-auto-" + suffix
	rowID := uuid.New()
	result, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Auto Init", "auto@example.com")
		return err
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.AutoInitialized)
	require.NotNil(t, result.Bundle)
	require.Equal(t, "admin-panel", result.Bundle.SourceID)
	require.Equal(t, int64(1), result.Bundle.SourceBundleID)

	state, _, err := currentScopeStateQuerier(ctx, pool, scopeID)
	require.NoError(t, err)
	require.Equal(t, scopeStateInitialized, state)

	var storedScopeID string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT _sync_scope_id
		FROM %s.users
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, pgx.Identifier{schemaName}.Sanitize()), scopeID, rowID).Scan(&storedScopeID))
	require.Equal(t, scopeID, storedScopeID)

	reader := Actor{UserID: scopeID, SourceID: "reader"}
	pullResp, err := svc.ProcessPull(ctx, reader, 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, result.Bundle.BundleSeq, pullResp.StableBundleSeq)
	require.Len(t, pullResp.Bundles, 1)
	require.Equal(t, result.Bundle.BundleSeq, pullResp.Bundles[0].BundleSeq)
}

func TestScopeManager_ExecWrite_DoesNotAutoInitializeInitializedScope(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_initialized_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-initialized-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, scopeID, "device-a")

	rowID := uuid.New()
	result, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Initialized", "initialized@example.com")
		return err
	})
	require.NoError(t, err)
	require.False(t, result.AutoInitialized)
	require.Equal(t, int64(1), result.Bundle.SourceBundleID)
}

func TestScopeManager_ExecWrite_FailsClosedWhenScopeInitializing(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_initializing_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-initializing-" + suffix
	resp, err := svc.Connect(ctx, Actor{UserID: scopeID, SourceID: "device-a"}, &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", resp.Resolution)

	_, err = mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		return nil
	})
	var initializingErr *ScopeInitializingError
	require.ErrorAs(t, err, &initializingErr)
	require.Equal(t, scopeID, initializingErr.UserID)
}

func TestScopeManager_ExecWrite_FailsForRetiredWriter(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_retired_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-retired-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, scopeID, "seed")

	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := ensureUserStateExistsWithExec(ctx, tx, scopeID); err != nil {
			return err
		}
		userPK, err := lookupUserPK(ctx, tx, scopeID)
		if err != nil {
			return err
		}
		return retireSourceState(ctx, tx, userPK, "admin-panel", "replacement-writer", "test")
	}))

	_, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		return nil
	})
	var retiredErr *SourceRetiredError
	require.ErrorAs(t, err, &retiredErr)
	require.Equal(t, "replacement-writer", retiredErr.ReplacedBySourceID)
}

func TestScopeManager_ExecWrite_NoCapturedChanges(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_noop_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.unregistered_notes (
			id UUID PRIMARY KEY,
			body TEXT NOT NULL
		)
	`, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)
	scopeID := "scope-noop-" + suffix

	for _, tc := range []struct {
		name string
		fn   func(tx pgx.Tx) error
	}{
		{
			name: "no_writes",
			fn: func(tx pgx.Tx) error {
				return nil
			},
		},
		{
			name: "unregistered_only",
			fn: func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s.unregistered_notes (id, body)
					VALUES ($1, $2)
				`, schemaIdent), uuid.New(), "hello")
				return err
			},
		},
		{
			name: "normalized_away",
			fn: func(tx pgx.Tx) error {
				rowID := uuid.New()
				if _, err := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s.users (id, name, email)
					VALUES ($1, $2, $3)
				`, schemaIdent), rowID, "Temp", "temp@example.com"); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					DELETE FROM %s.users
					WHERE _sync_scope_id = $1
					  AND id = $2
				`, schemaIdent), scopeID, rowID)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, tc.fn)
			var noChangesErr *ScopeWriteNoCapturedChangesError
			require.ErrorAs(t, err, &noChangesErr)
			require.Equal(t, scopeID, noChangesErr.ScopeID)
		})
	}

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log bl
		JOIN sync.user_state us ON us.user_pk = bl.user_pk
		WHERE us.user_id = $1
	`, scopeID).Scan(&bundleCount))
	require.Zero(t, bundleCount)

	var notesCount int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.unregistered_notes`, schemaIdent)).Scan(&notesCount))
	require.Zero(t, notesCount)
}

func TestScopeManager_ExecWrite_CallbackErrorRollsBack(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_rollback_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-rollback-" + suffix
	rowID := uuid.New()
	errBoom := fmt.Errorf("boom")
	_, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Rollback", "rollback@example.com"); err != nil {
			return err
		}
		return errBoom
	})
	require.ErrorIs(t, err, errBoom)

	var rowCount int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.users
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, pgx.Identifier{schemaName}.Sanitize()), scopeID, rowID).Scan(&rowCount))
	require.Zero(t, rowCount)

	var bundleCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log bl
		JOIN sync.user_state us ON us.user_pk = bl.user_pk
		WHERE us.user_id = $1
	`, scopeID).Scan(&bundleCount))
	require.Zero(t, bundleCount)
}

func TestScopeManager_ExecWrite_WriterSequencingAcrossScopesAndWriters(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_sequences_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	insertUser := func(scopeID, writerID, name string) *ScopeWriteResult {
		t.Helper()
		rowID := uuid.New()
		result, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: writerID}, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s.users (id, name, email)
				VALUES ($1, $2, $3)
			`, pgx.Identifier{schemaName}.Sanitize()), rowID, name, strings.ToLower(name)+"@example.com")
			return err
		})
		require.NoError(t, err)
		return result
	}

	scopeA := "scope-a-" + suffix
	scopeB := "scope-b-" + suffix

	a1 := insertUser(scopeA, "admin-panel", "Alice One")
	b1 := insertUser(scopeB, "admin-panel", "Bob One")
	a2 := insertUser(scopeA, "admin-panel", "Alice Two")
	aOther := insertUser(scopeA, "billing-worker", "Alice Billing")

	require.Equal(t, int64(1), a1.Bundle.SourceBundleID)
	require.Equal(t, int64(1), b1.Bundle.SourceBundleID)
	require.Equal(t, int64(2), a2.Bundle.SourceBundleID)
	require.Equal(t, int64(1), aOther.Bundle.SourceBundleID)
}

func TestScopeManager_ExecWrite_CollisionWithClientSourceUsesUnderlyingRuntimeRules(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_collision_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-collision-" + suffix
	sharedWriter := "shared-writer"
	rowID := uuid.New()
	actor := Actor{UserID: scopeID, SourceID: sharedWriter}
	bundle, err := pushRowsViaSession(t, ctx, svc, actor, 1, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Client","email":"client@example.com"}`, rowID)),
	}})
	require.NoError(t, err)
	require.NotNil(t, bundle)

	result, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: sharedWriter}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $3
			WHERE _sync_scope_id = $1
			  AND id = $2
		`, pgx.Identifier{schemaName}.Sanitize()), scopeID, rowID, "Server")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Bundle.SourceBundleID)
}

func TestScopeManager_ExecWrite_RejectsCrossScopeMutations(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_owner_guard_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	ownerA := "owner-a-" + suffix
	ownerB := "owner-b-" + suffix
	rowID := uuid.New()
	_, err := mgr.ExecWrite(ctx, ownerA, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Owner A", "owner-a@example.com")
		return err
	})
	require.NoError(t, err)

	_, err = mgr.ExecWrite(ctx, ownerB, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.users
			SET name = $2
			WHERE id = $1
		`, pgx.Identifier{schemaName}.Sanitize()), rowID, "Intruder")
		return err
	})
	require.ErrorContains(t, err, "scope mismatch")

	_, err = mgr.ExecWrite(ctx, ownerB, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			DELETE FROM %s.users
			WHERE id = $1
		`, pgx.Identifier{schemaName}.Sanitize()), rowID)
		return err
	})
	require.ErrorContains(t, err, "scope mismatch")
}

func TestScopeManager_ExecWrite_RejectsMismatchedScopeOnInsert(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_insert_mismatch_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	_, err := mgr.ExecWrite(ctx, "actor-a-"+suffix, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (_sync_scope_id, id, name, email)
			VALUES ($1, $2, $3, $4)
		`, pgx.Identifier{schemaName}.Sanitize()), "actor-b-"+suffix, uuid.New(), "Mallory", "mallory@example.com")
		return err
	})
	require.ErrorContains(t, err, "scope mismatch")
}

func TestScopeManager_ExecWrite_ComplexScopeSafeSQLAndTriggerEffects(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_complex_" + suffix
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
		CREATE TABLE %s.posts (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT posts_author_id_fkey FOREIGN KEY (_sync_scope_id, author_id)
				REFERENCES %s.users(_sync_scope_id, id)
				DEFERRABLE INITIALLY DEFERRED
		)`, schemaIdent, schemaIdent))
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
		AppName:                   "scope-manager-complex",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "profiles", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "posts", SyncKeyColumns: []string{"id"}},
		},
	}, logger)
	mgr := NewScopeManager(svc, ScopeManagerConfig{Logger: logger})

	scopeID := "scope-complex-" + suffix
	userID := uuid.New()
	postID := uuid.New()

	result, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, schemaIdent), userID, "Complex User", "complex@example.com"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			WITH target AS (
				SELECT id
				FROM %s.users
				WHERE _sync_scope_id = $1
				  AND id = $2
			)
			INSERT INTO %s.posts (id, title, content, author_id)
			SELECT $3, $4, $5, id
			FROM target
		`, schemaIdent, schemaIdent), scopeID, userID, postID, "Hello", "world")
		return err
	})
	require.NoError(t, err)
	require.NotNil(t, result.Bundle)
	require.Len(t, result.Bundle.Rows, 3)

	reader := Actor{UserID: scopeID, SourceID: "reader"}
	pullResp, err := svc.ProcessPull(ctx, reader, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, pullResp.Bundles, 1)
	require.Len(t, pullResp.Bundles[0].Rows, 3)
}

func TestScopeManager_ExecWrite_DifferentScopeDoesNotObserveChanges(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_scope_isolation_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeA := "scope-a-" + suffix
	scopeB := "scope-b-" + suffix
	mustInitializeEmptyScope(t, ctx, svc, scopeB, "reader-b")
	_, err := mgr.ExecWrite(ctx, scopeA, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, $2, $3)
		`, pgx.Identifier{schemaName}.Sanitize()), uuid.New(), "Only A", "a@example.com")
		return err
	})
	require.NoError(t, err)

	respB, err := svc.ProcessPull(ctx, Actor{UserID: scopeB, SourceID: "reader-b"}, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, respB.Bundles, 0)
	require.Equal(t, int64(0), respB.StableBundleSeq)
}

func TestScopeManager_ExecWrite_ConcurrentUsage(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_concurrency_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	_, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	runGroup := func(scopeID string, writerIDs []string) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, len(writerIDs))
		for i, writerID := range writerIDs {
			wg.Add(1)
			go func(i int, writerID string) {
				defer wg.Done()
				_, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: writerID}, func(tx pgx.Tx) error {
					_, err := tx.Exec(ctx, fmt.Sprintf(`
						INSERT INTO %s.users (id, name, email)
						VALUES ($1, $2, $3)
					`, pgx.Identifier{schemaName}.Sanitize()), uuid.New(), fmt.Sprintf("%s-%d", writerID, i), fmt.Sprintf("%s-%d@example.com", writerID, i))
					return err
				})
				errs <- err
			}(i, writerID)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}

	runGroup("scope-same-writer-"+suffix, []string{"admin-panel", "admin-panel", "admin-panel"})
	runGroup("scope-different-writers-"+suffix, []string{"admin-panel", "billing-worker", "tool-runner"})

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, scopeID := range []string{"scope-left-" + suffix, "scope-right-" + suffix} {
		wg.Add(1)
		go func(scopeID string) {
			defer wg.Done()
			_, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s.users (id, name, email)
					VALUES ($1, $2, $3)
				`, pgx.Identifier{schemaName}.Sanitize()), uuid.New(), scopeID, scopeID+"@example.com")
				return err
			})
			errs <- err
		}(scopeID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestScopeManager_ExecWrite_PreservesPullRetentionAndCommittedBundleRowVisibility(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "scope_manager_retention_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc, mgr := newScopeManagerIntegrationService(t, ctx, pool, schemaName, logger)

	scopeID := "scope-retention-" + suffix
	reader := Actor{UserID: scopeID, SourceID: "reader"}

	writeUser := func(name string) *ScopeWriteResult {
		t.Helper()
		result, err := mgr.ExecWrite(ctx, scopeID, ScopeWriteOptions{WriterID: "admin-panel"}, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s.users (id, name, email)
				VALUES ($1, $2, $3)
			`, pgx.Identifier{schemaName}.Sanitize()), uuid.New(), name, strings.ToLower(strings.ReplaceAll(name, " ", "."))+"@example.com")
			return err
		})
		require.NoError(t, err)
		return result
	}

	first := writeUser("Retention One")
	second := writeUser("Retention Two")
	require.Equal(t, int64(1), first.Bundle.SourceBundleID)
	require.Equal(t, int64(2), second.Bundle.SourceBundleID)

	rowsResp, err := svc.GetCommittedBundleRows(ctx, reader, second.Bundle.BundleSeq, nil, 10)
	require.NoError(t, err)
	require.NotNil(t, rowsResp)
	require.Equal(t, second.Bundle.BundleSeq, rowsResp.BundleSeq)
	require.Len(t, rowsResp.Rows, 1)
	require.Equal(t, schemaName, rowsResp.Rows[0].Schema)
	require.Equal(t, "users", rowsResp.Rows[0].Table)

	_, err = pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = $2 WHERE user_id = $1`, scopeID, second.Bundle.BundleSeq)
	require.NoError(t, err)

	_, err = svc.ProcessPull(ctx, reader, first.Bundle.BundleSeq, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, first.Bundle.BundleSeq, prunedErr.ProvidedSeq)
	require.Equal(t, second.Bundle.BundleSeq, prunedErr.RetainedFloor)

	_, err = svc.GetCommittedBundleRows(ctx, reader, first.Bundle.BundleSeq, nil, 10)
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, second.Bundle.BundleSeq, prunedErr.RetainedFloor)
}
