package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func qualifiedSchema(schema string) string {
	return pgx.Identifier{schema}.Sanitize()
}

func qualifiedTable(schema, table string) string {
	return pgx.Identifier{schema, table}.Sanitize()
}

// InitializeApplicationTables creates clean business tables for bundle-based sync examples.
func InitializeApplicationTables(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, schema string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		schemaName := qualifiedSchema(schema)
		usersTable := qualifiedTable(schema, "users")
		postsTable := qualifiedTable(schema, "posts")
		categoriesTable := qualifiedTable(schema, "categories")
		teamsTable := qualifiedTable(schema, "teams")
		teamMembersTable := qualifiedTable(schema, "team_members")
		filesTable := qualifiedTable(schema, "files")
		fileReviewsTable := qualifiedTable(schema, "file_reviews")
		typedRowsTable := qualifiedTable(schema, "typed_rows")

		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaName)); err != nil {
			return fmt.Errorf("failed to create business schema: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	name TEXT NOT NULL,
	email TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now(),
	PRIMARY KEY (_sync_scope_id, id)
)
`, usersTable)); err != nil {
			return fmt.Errorf("failed to create users table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	title TEXT NOT NULL,
	content TEXT NOT NULL,
	author_id UUID,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now(),
	PRIMARY KEY (_sync_scope_id, id),
	CONSTRAINT posts_author_id_fkey
		FOREIGN KEY (_sync_scope_id, author_id) REFERENCES %s(_sync_scope_id, id) DEFERRABLE INITIALLY DEFERRED
)
`, postsTable, usersTable)); err != nil {
			return fmt.Errorf("failed to create posts table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	name TEXT NOT NULL,
	parent_id UUID,
	PRIMARY KEY (_sync_scope_id, id),
	CONSTRAINT categories_parent_id_fkey
		FOREIGN KEY (_sync_scope_id, parent_id) REFERENCES %s(_sync_scope_id, id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
)
`, categoriesTable, categoriesTable)); err != nil {
			return fmt.Errorf("failed to create categories table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	name TEXT NOT NULL,
	captain_member_id UUID,
	PRIMARY KEY (_sync_scope_id, id)
)
`, teamsTable)); err != nil {
			return fmt.Errorf("failed to create teams table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	name TEXT NOT NULL,
	team_id UUID NOT NULL,
	PRIMARY KEY (_sync_scope_id, id),
	CONSTRAINT team_members_team_id_fkey
		FOREIGN KEY (_sync_scope_id, team_id) REFERENCES %s(_sync_scope_id, id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
)
`, teamMembersTable, teamsTable)); err != nil {
			return fmt.Errorf("failed to create team_members table: %w", err)
		}

		var teamsCaptainFKExists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_constraint c
				JOIN pg_class t ON t.oid = c.conrelid
				JOIN pg_namespace n ON n.oid = t.relnamespace
				WHERE c.conname = 'teams_captain_member_id_fkey'
				  AND n.nspname = $1
				  AND t.relname = 'teams'
			)
		`, schema).Scan(&teamsCaptainFKExists); err != nil {
			return fmt.Errorf("failed to check teams captain_member_id FK: %w", err)
		}
		if !teamsCaptainFKExists {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
				ALTER TABLE %s
				ADD CONSTRAINT teams_captain_member_id_fkey
				FOREIGN KEY (_sync_scope_id, captain_member_id)
				REFERENCES %s(_sync_scope_id, id)
				DEFERRABLE INITIALLY DEFERRED
			`, teamsTable, teamMembersTable)); err != nil {
				return fmt.Errorf("failed to add teams captain_member_id FK: %w", err)
			}
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	name TEXT NOT NULL,
	data BYTEA NOT NULL,
	PRIMARY KEY (_sync_scope_id, id)
)
`, filesTable)); err != nil {
			return fmt.Errorf("failed to create files table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	review TEXT NOT NULL,
	file_id UUID NOT NULL,
	PRIMARY KEY (_sync_scope_id, id),
	CONSTRAINT file_reviews_file_id_fkey
		FOREIGN KEY (_sync_scope_id, file_id) REFERENCES %s(_sync_scope_id, id) DEFERRABLE INITIALLY DEFERRED
)
`, fileReviewsTable, filesTable)); err != nil {
			return fmt.Errorf("failed to create file_reviews table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	_sync_scope_id TEXT NOT NULL,
	id UUID NOT NULL,
	name TEXT NOT NULL,
	note TEXT NULL,
	count_value BIGINT NULL,
	enabled_flag BIGINT NOT NULL,
	rating DOUBLE PRECISION NULL,
	data BYTEA NULL,
	created_at TIMESTAMPTZ NULL,
	PRIMARY KEY (_sync_scope_id, id)
)
`, typedRowsTable)); err != nil {
			return fmt.Errorf("failed to create typed_rows table: %w", err)
		}

		logger.Info("Initialized business schema and tables", "schema", schema)
		return nil
	})
}
