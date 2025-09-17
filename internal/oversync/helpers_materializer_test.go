// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// TestMaterializer is a test-only utility for materializing downloaded changes
// This is NOT part of the SDK - it's only used for testing download scenarios
type TestMaterializer struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewTestMaterializer creates a new test materializer
func NewTestMaterializer(pool *pgxpool.Pool, logger *slog.Logger) *TestMaterializer {
	return &TestMaterializer{
		pool:   pool,
		logger: logger,
	}
}

// MaterializeBatch processes a batch of changes atomically for testing
// Any error (row apply error or deferred FK check) rolls back the entire batch
func (m *TestMaterializer) MaterializeBatch(ctx context.Context, changes []oversync.ChangeDownloadResponse) error {
	if len(changes) == 0 {
		return nil
	}

	m.logger.Info("Starting atomic materialization batch", "count", len(changes))

	return pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		// Enable deferrable constraints for this transaction
		if _, err := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
			return fmt.Errorf("defer constraints: %w", err)
		}

		// Process each change - any error fails the entire batch
		for _, change := range changes {
			if err := m.materializeChange(ctx, tx, change); err != nil {
				return fmt.Errorf("materialize %s(%s): %w", change.TableName, change.PK, err)
			}
		}

		// Optional early validation to fail fast on FK/deferrable constraints
		if _, err := tx.Exec(ctx, "SET CONSTRAINTS ALL IMMEDIATE"); err != nil {
			return fmt.Errorf("immediate constraints check: %w", err)
		}

		m.logger.Info("Atomic materialization batch completed successfully", "count", len(changes))
		// Returning nil commits the whole batch
		return nil
	})
}

// materializeChange applies a single change to the business table (test-only)
func (m *TestMaterializer) materializeChange(ctx context.Context, tx pgx.Tx, change oversync.ChangeDownloadResponse) error {
	switch change.Op {
	case "INSERT", "UPDATE":
		return m.upsertRecord(ctx, tx, change)
	case "DELETE":
		return m.deleteRecord(ctx, tx, change)
	default:
		return fmt.Errorf("unsupported operation: %s", change.Op)
	}
}

// upsertRecord performs an idempotent upsert for INSERT/UPDATE operations (test-only)
func (m *TestMaterializer) upsertRecord(ctx context.Context, tx pgx.Tx, change oversync.ChangeDownloadResponse) error {
	// Parse the payload to extract business data
	var payload map[string]interface{}
	if err := json.Unmarshal(change.Payload, &payload); err != nil {
		return fmt.Errorf("failed to parse payload: %w", err)
	}

	// Remove sync metadata from payload (if present)
	delete(payload, "server_version")

	// Test-specific table handling - this would be different for each application
	switch change.TableName {
	case "users":
		return m.upsertUser(ctx, tx, change.PK, payload)
	case "posts":
		return m.upsertPost(ctx, tx, change.PK, payload)
	case "note":
		return m.upsertNote(ctx, tx, change.PK, payload)
	case "task":
		return m.upsertTask(ctx, tx, change.PK, payload)
	default:
		// Unknown table - skip for forward compatibility in tests
		m.logger.Warn("Unknown table in test materialization, skipping",
			"table", change.TableName,
			"pk", change.PK)
		return nil
	}
}

// deleteRecord removes a record from the business table (test-only)
func (m *TestMaterializer) deleteRecord(ctx context.Context, tx pgx.Tx, change oversync.ChangeDownloadResponse) error {
	// Test-specific table handling
	switch change.TableName {
	case "users":
		_, err := tx.Exec(ctx, "DELETE FROM users WHERE id = $1", change.PK)
		return err
	case "posts":
		_, err := tx.Exec(ctx, "DELETE FROM posts WHERE id = $1", change.PK)
		return err
	case "note":
		_, err := tx.Exec(ctx, "DELETE FROM note WHERE id = $1", change.PK)
		return err
	case "task":
		_, err := tx.Exec(ctx, "DELETE FROM task WHERE id = $1", change.PK)
		return err
	default:
		// Unknown table - skip for forward compatibility in tests
		m.logger.Warn("Unknown table in test materialization, skipping",
			"table", change.TableName,
			"pk", change.PK)
		return nil
	}
}

// Test-specific upsert methods (these would be different for each application)

func (m *TestMaterializer) upsertUser(ctx context.Context, tx pgx.Tx, pk string, payload map[string]interface{}) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO users (id, name, email, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			email = EXCLUDED.email`,
		pk,
		payload["name"],
		payload["email"],
		payload["created_at"])
	return err
}

func (m *TestMaterializer) upsertPost(ctx context.Context, tx pgx.Tx, pk string, payload map[string]interface{}) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO posts (id, title, content, author_id, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			author_id = EXCLUDED.author_id`,
		pk,
		payload["title"],
		payload["content"],
		payload["author_id"],
		payload["created_at"])
	return err
}

func (m *TestMaterializer) upsertNote(ctx context.Context, tx pgx.Tx, pk string, payload map[string]interface{}) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO note (id, title, content, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			updated_at = EXCLUDED.updated_at`,
		pk,
		payload["title"],
		payload["content"],
		payload["updated_at"])
	return err
}

func (m *TestMaterializer) upsertTask(ctx context.Context, tx pgx.Tx, pk string, payload map[string]interface{}) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO task (id, title, done, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			done = EXCLUDED.done,
			updated_at = EXCLUDED.updated_at`,
		pk,
		payload["title"],
		payload["done"],
		payload["updated_at"])
	return err
}
