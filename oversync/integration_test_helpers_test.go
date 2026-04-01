package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func integrationTestDatabaseURL() string {
	if dbURL := os.Getenv("TEST_DATABASE_URL"); dbURL != "" {
		return dbURL
	}
	return "postgres://postgres:password@localhost:5432/clisync_test?sslmode=disable"
}

func integrationTestLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func newIntegrationTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(ctx, integrationTestDatabaseURL())
	if err != nil {
		t.Fatalf("create integration test pool: %v", err)
	}
	require.NoError(t, resetTestSyncSchema(ctx, pool))
	t.Cleanup(func() { _ = resetTestSyncSchema(context.Background(), pool) })
	t.Cleanup(pool.Close)
	return pool
}

func newBootstrappedIntegrationService(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	config *ServiceConfig,
	logger *slog.Logger,
) *SyncService {
	t.Helper()

	if logger == nil {
		logger = slog.Default()
	}
	svc, err := NewRuntimeService(pool, config, logger)
	if err != nil {
		t.Fatalf("create runtime service: %v", err)
	}
	if err := svc.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap runtime service: %v", err)
	}
	t.Cleanup(func() {
		_ = svc.Close(context.Background())
	})
	return svc
}

func resetTestBusinessSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	schemaIdent := pgx.Identifier{schema}.Sanitize()

	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaIdent)); err != nil {
		return fmt.Errorf("drop test schema %s: %w", schema, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent)); err != nil {
		return fmt.Errorf("create test schema %s: %w", schema, err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.users (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.users: %w", schema, err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.posts (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT posts_author_id_fkey FOREIGN KEY (_sync_scope_id, author_id)
				REFERENCES %s.users(_sync_scope_id, id)
				ON DELETE CASCADE
				DEFERRABLE INITIALLY DEFERRED
		)`, schemaIdent, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.posts: %w", schema, err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.files (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			name TEXT NOT NULL,
			data BYTEA NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)`, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.files: %w", schema, err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.file_reviews (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			review TEXT NOT NULL,
			file_id UUID NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT file_reviews_file_id_fkey FOREIGN KEY (_sync_scope_id, file_id)
				REFERENCES %s.files(_sync_scope_id, id)
				ON DELETE CASCADE
				DEFERRABLE INITIALLY DEFERRED
		)`, schemaIdent, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.file_reviews: %w", schema, err)
	}

	return nil
}

func resetTestSyncSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS sync CASCADE`); err != nil {
		return fmt.Errorf("drop sync schema: %w", err)
	}
	return nil
}

func cleanupSyncUser(ctx context.Context, pool *pgxpool.Pool, userID string) error {
	userPK, err := lookupUserPK(ctx, pool, userID)
	if err != nil {
		if strings.Contains(err.Error(), "missing user_state row") {
			return nil
		}
		return err
	}
	queries := []string{
		`DELETE FROM sync.bundle_capture_stage WHERE user_pk = $1`,
		`DELETE FROM sync.push_sessions WHERE user_pk = $1`,
		`DELETE FROM sync.source_state WHERE user_pk = $1`,
		`DELETE FROM sync.bundle_rows WHERE user_pk = $1`,
		`DELETE FROM sync.bundle_log WHERE user_pk = $1`,
		`DELETE FROM sync.row_state WHERE user_pk = $1`,
		`DELETE FROM sync.scope_state WHERE user_pk = $1`,
		`DELETE FROM sync.user_state WHERE user_pk = $1`,
	}
	for _, q := range queries {
		if _, err := pool.Exec(ctx, q, userPK); err != nil {
			return err
		}
	}
	return nil
}

func dropTestSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaIdent)); err != nil {
		return fmt.Errorf("drop test schema %s: %w", schema, err)
	}
	return nil
}

func loadCommittedBundleForUser(t *testing.T, ctx context.Context, svc *SyncService, userID string, bundleSeq int64) *Bundle {
	t.Helper()

	var bundle *Bundle
	err := pgx.BeginTxFunc(ctx, svc.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var txErr error
		bundle, txErr = svc.loadCommittedBundle(ctx, tx, userID, bundleSeq)
		return txErr
	})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func resolveConnectForPushSession(t *testing.T, ctx context.Context, svc *SyncService, actor Actor, hasLocalPendingRows bool) string {
	t.Helper()

	resp, err := svc.Connect(ctx, Actor{UserID: actor.UserID}, &ConnectRequest{
		SourceID:            actor.SourceID,
		HasLocalPendingRows: hasLocalPendingRows,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	switch resp.Resolution {
	case "initialize_local":
		require.NotEmpty(t, resp.InitializationID)
		return resp.InitializationID
	case "remote_authoritative", "initialize_empty":
		return ""
	default:
		t.Fatalf("unexpected connect resolution %q", resp.Resolution)
		return ""
	}
}

func mustInitializeEmptyScope(t *testing.T, ctx context.Context, svc *SyncService, userID, sourceID string) {
	t.Helper()

	resp, err := svc.Connect(ctx, Actor{UserID: userID}, &ConnectRequest{
		SourceID:            sourceID,
		HasLocalPendingRows: false,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Contains(t, []string{"initialize_empty", "remote_authoritative"}, resp.Resolution)
}

func pushRowsViaSession(
	t *testing.T,
	ctx context.Context,
	svc *SyncService,
	actor Actor,
	sourceBundleID int64,
	rows []PushRequestRow,
) (*Bundle, error) {
	t.Helper()

	initializationID := resolveConnectForPushSession(t, ctx, svc, actor, len(rows) > 0)
	createResp, err := svc.CreatePushSession(ctx, actor, &PushSessionCreateRequest{
		SourceID:         actor.SourceID,
		SourceBundleID:   sourceBundleID,
		PlannedRowCount:  int64(len(rows)),
		InitializationID: initializationID,
	})
	if err != nil {
		return nil, err
	}

	switch createResp.Status {
	case "already_committed":
		return loadCommittedBundleForUser(t, ctx, svc, actor.UserID, createResp.BundleSeq), nil
	case "staging":
		_, err = svc.UploadPushChunk(ctx, actor, createResp.PushID, &PushSessionChunkRequest{
			StartRowOrdinal: 0,
			Rows:            rows,
		})
		if err != nil {
			return nil, err
		}
		commitResp, err := svc.CommitPushSession(ctx, actor, createResp.PushID)
		if err != nil {
			return nil, err
		}
		return loadCommittedBundleForUser(t, ctx, svc, actor.UserID, commitResp.BundleSeq), nil
	default:
		return nil, fmt.Errorf("unexpected push session status %q", createResp.Status)
	}
}

func mustCompactStorageIdentity(
	t *testing.T,
	ctx context.Context,
	svc *SyncService,
	userID string,
	schemaName string,
	tableName string,
	keyString string,
) (int64, int32, []byte) {
	t.Helper()

	userPK, err := lookupUserPK(ctx, svc.pool, userID)
	require.NoError(t, err)

	info, err := svc.syncKeyInfoForTable(schemaName, tableName)
	require.NoError(t, err)

	keyBytes, _, err := encodeKeyBytes(info.syncKeyType, keyString)
	require.NoError(t, err)
	return userPK, info.tableID, keyBytes
}
