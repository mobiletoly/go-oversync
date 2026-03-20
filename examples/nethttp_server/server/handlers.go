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

		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaName)); err != nil {
			return fmt.Errorf("failed to create business schema: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now()
)
`, usersTable)); err != nil {
			return fmt.Errorf("failed to create users table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	title TEXT NOT NULL,
	content TEXT NOT NULL,
	author_id UUID REFERENCES %s(id) DEFERRABLE INITIALLY DEFERRED,
	created_at TIMESTAMPTZ DEFAULT now(),
	updated_at TIMESTAMPTZ DEFAULT now()
)
`, postsTable, usersTable)); err != nil {
			return fmt.Errorf("failed to create posts table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	parent_id UUID REFERENCES %s(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
)
`, categoriesTable, categoriesTable)); err != nil {
			return fmt.Errorf("failed to create categories table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	captain_member_id UUID
)
`, teamsTable)); err != nil {
			return fmt.Errorf("failed to create teams table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	team_id UUID NOT NULL REFERENCES %s(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
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
				FOREIGN KEY (captain_member_id)
				REFERENCES %s(id)
				DEFERRABLE INITIALLY DEFERRED
			`, teamsTable, teamMembersTable)); err != nil {
				return fmt.Errorf("failed to add teams captain_member_id FK: %w", err)
			}
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	name TEXT NOT NULL,
	data BYTEA NOT NULL
)
`, filesTable)); err != nil {
			return fmt.Errorf("failed to create files table: %w", err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id UUID PRIMARY KEY,
	review TEXT NOT NULL,
	file_id UUID NOT NULL REFERENCES %s(id) DEFERRABLE INITIALLY DEFERRED
)
`, fileReviewsTable, filesTable)); err != nil {
			return fmt.Errorf("failed to create file_reviews table: %w", err)
		}

		logger.Info("Initialized business schema and tables", "schema", schema)
		return nil
	})
}
