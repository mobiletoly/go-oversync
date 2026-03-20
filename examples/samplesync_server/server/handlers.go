package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InitializeApplicationTables creates `business` schema + business tables.
func InitializeApplicationTables(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS business`); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.person(
  id UUID PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name  TEXT NOT NULL,
  email      TEXT NOT NULL,
  phone      TEXT,
  birth_date TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  score      DOUBLE PRECISION,
  is_active  BOOLEAN NOT NULL DEFAULT true,
  ssn        BIGINT,
  notes      TEXT
)`); err != nil {
			return fmt.Errorf("create person: %w", err)
		}
		if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS business.person_user_email_unique`); err != nil {
			return fmt.Errorf("drop legacy person email index: %w", err)
		}
		if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS business.idx_person_user_name`); err != nil {
			return fmt.Errorf("drop legacy person name index: %w", err)
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE business.person DROP COLUMN IF EXISTS owner_user_id`); err != nil {
			return fmt.Errorf("drop legacy person.owner_user_id: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE UNIQUE INDEX IF NOT EXISTS person_user_email_unique
ON business.person(email)`); err != nil {
			return fmt.Errorf("create person email index: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.person_address(
  id UUID PRIMARY KEY,
  person_id UUID NOT NULL REFERENCES business.person(id) DEFERRABLE INITIALLY DEFERRED,
  address_type TEXT NOT NULL,
  street TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT,
  postal_code TEXT,
  country TEXT NOT NULL,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
)`); err != nil {
			return fmt.Errorf("create person_address: %w", err)
		}
		if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS business.idx_address_user_person`); err != nil {
			return fmt.Errorf("drop legacy address person index: %w", err)
		}
		if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS business.idx_address_user_primary`); err != nil {
			return fmt.Errorf("drop legacy address primary index: %w", err)
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE business.person_address DROP COLUMN IF EXISTS owner_user_id`); err != nil {
			return fmt.Errorf("drop legacy person_address.owner_user_id: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.comment(
  id UUID PRIMARY KEY,
  person_id UUID NOT NULL REFERENCES business.person(id) DEFERRABLE INITIALLY DEFERRED,
  comment TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  tags TEXT
)`); err != nil {
			return fmt.Errorf("create comment: %w", err)
		}
		if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS business.idx_comment_user_person`); err != nil {
			return fmt.Errorf("drop legacy comment person index: %w", err)
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE business.comment DROP COLUMN IF EXISTS owner_user_id`); err != nil {
			return fmt.Errorf("drop legacy comment.owner_user_id: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_person_name ON business.person(last_name, first_name)`); err != nil {
			logger.Warn("Failed to create person name index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_address_person ON business.person_address(person_id)`); err != nil {
			logger.Warn("Failed to create address person index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_address_person_primary ON business.person_address(person_id, is_primary)`); err != nil {
			logger.Warn("Failed to create address primary index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_comment_person ON business.comment(person_id)`); err != nil {
			logger.Warn("Failed to create comment person index", "error", err)
			return err
		}
		logger.Info("Initialized business schema and tables")
		return nil
	})
}
