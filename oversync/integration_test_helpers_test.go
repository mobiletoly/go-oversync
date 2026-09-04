package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func integrationTestLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func newIntegrationTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	return newIntegrationTestPoolForDatabaseURL(t, ctx, databaseURL, managed)
}

func newIntegrationTestPoolForServer(
	t *testing.T,
	ctx context.Context,
	server *managedIntegrationPostgresServer,
) *pgxpool.Pool {
	t.Helper()

	databaseURL, managed := provisionIntegrationTestDatabaseOnServer(t, ctx, server)
	return newIntegrationTestPoolForDatabaseURL(t, ctx, databaseURL, managed)
}

func newIntegrationTestPoolForDatabaseURL(
	t *testing.T,
	ctx context.Context,
	databaseURL string,
	managed bool,
) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create integration test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))

	if !managed {
		require.NoError(t, resetTestSyncSchema(ctx, pool))
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed integration test database: %v", err)
			}
		})
	}
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
	svc, err := newTestRuntimeService(pool, config, logger)
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
	recordTestBusinessSchemaGeneration(schema)

	return nil
}

func resetTestSyncSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS sync CASCADE`); err != nil {
		return fmt.Errorf("drop sync schema: %w", err)
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

func resolveConnectForPush(t *testing.T, ctx context.Context, svc *SyncService, actor Actor, hasLocalPendingRows bool) string {
	t.Helper()

	resp, err := svc.Connect(ctx, actor, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: hasLocalPendingRows})
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

	resp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: sourceID}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Contains(t, []string{"initialize_empty", "remote_authoritative"}, resp.Resolution)
}

func mustCanonicalPushRequestHash(t *testing.T, svc *SyncService, rows []PushRequestRow) string {
	t.Helper()
	hash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(svc), rows)
	require.NoError(t, err)
	return hash
}

func pushRowsOneShot(
	t *testing.T,
	ctx context.Context,
	svc *SyncService,
	actor Actor,
	sourceBundleID int64,
	rows []PushRequestRow,
) (*Bundle, error) {
	t.Helper()

	initializationID := resolveConnectForPush(t, ctx, svc, actor, len(rows) > 0)
	canonicalRequestHash, hashErr := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(svc), rows)
	if hashErr != nil {
		return nil, hashErr
	}
	response, err := svc.PushOneShot(ctx, actor, testSchemaGenerationProofForService(svc), &OneShotPushRequest{
		SourceBundleID:       sourceBundleID,
		CanonicalRequestHash: canonicalRequestHash,
		InitializationID:     initializationID,
		Rows:                 rows,
	})
	if err != nil {
		return nil, err
	}
	return response.Bundle, nil
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
