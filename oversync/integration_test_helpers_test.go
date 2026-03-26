package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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

func cleanupSyncUser(ctx context.Context, pool *pgxpool.Pool, userID string) error {
	queries := []string{
		`DELETE FROM sync.bundle_capture_stage WHERE user_id = $1`,
		`DELETE FROM sync.applied_pushes WHERE user_id = $1`,
		`DELETE FROM sync.bundle_rows WHERE user_id = $1`,
		`DELETE FROM sync.bundle_log WHERE user_id = $1`,
		`DELETE FROM sync.row_state WHERE user_id = $1`,
		`DELETE FROM sync.user_state WHERE user_id = $1`,
	}
	for _, q := range queries {
		if _, err := pool.Exec(ctx, q, userID); err != nil {
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

func pushRowsViaSession(
	t *testing.T,
	ctx context.Context,
	svc *SyncService,
	actor Actor,
	sourceBundleID int64,
	rows []PushRequestRow,
) (*Bundle, error) {
	t.Helper()

	createResp, err := svc.CreatePushSession(ctx, actor, &PushSessionCreateRequest{
		SourceID:        actor.SourceID,
		SourceBundleID:  sourceBundleID,
		PlannedRowCount: int64(len(rows)),
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
