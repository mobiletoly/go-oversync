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
  _sync_scope_id TEXT NOT NULL,
  id UUID NOT NULL,
  first_name TEXT NOT NULL,
  last_name  TEXT NOT NULL,
  email      TEXT NOT NULL,
  phone      TEXT,
  birth_date TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  score      DOUBLE PRECISION,
  is_active  BOOLEAN NOT NULL DEFAULT true,
  ssn        BIGINT,
  notes      TEXT,
  PRIMARY KEY (_sync_scope_id, id)
)`); err != nil {
			return fmt.Errorf("create person: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE UNIQUE INDEX IF NOT EXISTS person_user_email_unique
ON business.person(_sync_scope_id, email)`); err != nil {
			return fmt.Errorf("create person email index: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.person_address(
  _sync_scope_id TEXT NOT NULL,
  id UUID NOT NULL,
  person_id UUID NOT NULL,
  address_type TEXT NOT NULL,
  street TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT,
  postal_code TEXT,
  country TEXT NOT NULL,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (_sync_scope_id, id),
  CONSTRAINT person_address_person_id_fkey
    FOREIGN KEY (_sync_scope_id, person_id)
    REFERENCES business.person(_sync_scope_id, id)
    DEFERRABLE INITIALLY DEFERRED
)`); err != nil {
			return fmt.Errorf("create person_address: %w", err)
		}
		if _, err := tx.Exec(ctx,
			/*language=postgresql*/ `
CREATE TABLE IF NOT EXISTS business.comment(
  _sync_scope_id TEXT NOT NULL,
  id UUID NOT NULL,
  person_id UUID NOT NULL,
  comment TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  tags TEXT,
  PRIMARY KEY (_sync_scope_id, id),
  CONSTRAINT comment_person_id_fkey
    FOREIGN KEY (_sync_scope_id, person_id)
    REFERENCES business.person(_sync_scope_id, id)
    DEFERRABLE INITIALLY DEFERRED
)`); err != nil {
			return fmt.Errorf("create comment: %w", err)
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_person_name ON business.person(_sync_scope_id, last_name, first_name)`); err != nil {
			logger.Warn("Failed to create person name index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_address_person ON business.person_address(_sync_scope_id, person_id)`); err != nil {
			logger.Warn("Failed to create address person index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_address_person_primary ON business.person_address(_sync_scope_id, person_id, is_primary)`); err != nil {
			logger.Warn("Failed to create address primary index", "error", err)
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_comment_person ON business.comment(_sync_scope_id, person_id)`); err != nil {
			logger.Warn("Failed to create comment person index", "error", err)
			return err
		}
		logger.Info("Initialized business schema and tables")
		return nil
	})
}
