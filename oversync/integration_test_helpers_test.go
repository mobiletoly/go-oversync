package oversync

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)`, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.users: %w", schema, err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s.posts (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id UUID NOT NULL,
			CONSTRAINT posts_author_id_fkey FOREIGN KEY (author_id)
				REFERENCES %s.users(id)
				ON DELETE CASCADE
		)`, schemaIdent, schemaIdent)); err != nil {
		return fmt.Errorf("create %s.posts: %w", schema, err)
	}

	return nil
}

func cleanupSyncUser(ctx context.Context, pool *pgxpool.Pool, userID string) error {
	queries := []string{
		`DELETE FROM sync.materialize_failures WHERE user_id = $1`,
		`DELETE FROM sync.server_change_log WHERE user_id = $1`,
		`DELETE FROM sync.sync_state WHERE user_id = $1`,
		`DELETE FROM sync.sync_row_meta WHERE user_id = $1`,
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
