package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type HistoryPrunedError struct {
	Status  int
	Message string
}

type RebuildRequiredError struct{}

func (e *RebuildRequiredError) Error() string {
	return "client rebuild is required; run Hydrate or Recover before syncing"
}

type SyncOperationInProgressError struct{}

func (e *SyncOperationInProgressError) Error() string {
	return "another sync operation is already in progress for this client"
}

// IsExpectedSyncContention reports whether err means another sync operation
// already owns the client-wide sync lock, so the attempted operation did not run.
// Background schedulers can use this to suppress user-facing failure handling for
// skipped sync ticks and simply retry later.
func IsExpectedSyncContention(err error) bool {
	if err == nil {
		return false
	}
	var inProgressErr *SyncOperationInProgressError
	return errors.As(err, &inProgressErr)
}

type PendingPushReplayError struct {
	OutboundCount int
}

func (e *PendingPushReplayError) Error() string {
	return fmt.Sprintf("cannot rebuild while %d staged push rows are pending authoritative replay", e.OutboundCount)
}

func (e *HistoryPrunedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "server history required for incremental sync has been pruned"
}

func (c *Client) pendingChangeCount(ctx context.Context) (int, error) {
	var count int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count dirty changes: %w", err)
	}
	return count, nil
}

func (c *Client) pendingPushOutboundCount(ctx context.Context) (int, error) {
	var count int
	if err := c.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count staged outbound push rows: %w", err)
	}
	return count, nil
}

func (c *Client) rebuildRequired(ctx context.Context) (bool, error) {
	var rebuildRequired int
	if err := c.DB.QueryRowContext(ctx, `SELECT rebuild_required FROM _sync_client_state WHERE user_id = ?`, c.UserID).Scan(&rebuildRequired); err != nil {
		return false, fmt.Errorf("failed to read rebuild_required: %w", err)
	}
	return rebuildRequired == 1, nil
}

func (c *Client) setRebuildRequired(ctx context.Context, required bool) error {
	value := 0
	if required {
		value = 1
	}
	if _, err := c.DB.ExecContext(ctx, `UPDATE _sync_client_state SET rebuild_required = ? WHERE user_id = ?`, value, c.UserID); err != nil {
		return fmt.Errorf("failed to update rebuild_required: %w", err)
	}
	return nil
}

func (c *Client) clearManagedTablesInTx(ctx context.Context, tx *sql.Tx) error {
	for _, syncTable := range c.config.Tables {
		tableName := strings.ToLower(syncTable.TableName)
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM "%s"`, tableName)); err != nil {
			return fmt.Errorf("failed to clear managed table %s: %w", tableName, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?`, c.config.Schema, tableName); err != nil {
			return fmt.Errorf("failed to clear structured row state for %s: %w", tableName, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_dirty_rows`); err != nil {
		return fmt.Errorf("failed to clear dirty row queue during recovery: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_outbound`); err != nil {
		return fmt.Errorf("failed to clear outbound push snapshot during recovery: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("failed to clear staged push bundle rows during recovery: %w", err)
	}
	return nil
}

func (c *Client) clearFullLocalSyncStateInTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_client_state SET apply_mode = 1`); err != nil {
		return fmt.Errorf("failed to enable apply_mode before full local reset: %w", err)
	}
	if err := c.clearManagedTablesInTx(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return fmt.Errorf("failed to clear snapshot staging during recovery: %w", err)
	}
	return nil
}

type persistedClientIdentity struct {
	UserID     string
	SourceID   string
	SchemaName string
}

func (c *Client) ensureBootstrapStateLocked(ctx context.Context) (string, error) {
	userID := strings.TrimSpace(c.UserID)
	if userID == "" {
		return "", fmt.Errorf("userID must be provided")
	}

	preferredSourceID := strings.TrimSpace(c.SourceID)
	schemaName := strings.TrimSpace(c.config.Schema)

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin bootstrap identity transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT user_id, source_id, schema_name
		FROM _sync_client_state
		ORDER BY user_id
	`)
	if err != nil {
		return "", fmt.Errorf("failed to query bootstrap client identity: %w", err)
	}
	defer rows.Close()

	identities := make([]persistedClientIdentity, 0)
	for rows.Next() {
		var identity persistedClientIdentity
		if err := rows.Scan(&identity.UserID, &identity.SourceID, &identity.SchemaName); err != nil {
			return "", fmt.Errorf("failed to scan bootstrap client identity: %w", err)
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("failed to iterate bootstrap client identity: %w", err)
	}

	if len(identities) == 0 {
		sourceID := preferredSourceID
		if sourceID == "" {
			sourceID = uuid.NewString()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO _sync_client_state (
				user_id, source_id, schema_name, next_source_bundle_id, last_bundle_seq_seen, apply_mode, rebuild_required
			) VALUES (?, ?, ?, 1, 0, 0, 0)
		`, userID, sourceID, schemaName); err != nil {
			return "", fmt.Errorf("failed to insert initial bootstrap client state: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return "", fmt.Errorf("failed to commit initial bootstrap client state: %w", err)
		}
		return sourceID, nil
	}

	if len(identities) == 1 && identities[0].UserID == userID {
		persisted := identities[0]
		if schemaName != "" && strings.TrimSpace(persisted.SchemaName) != "" && strings.TrimSpace(persisted.SchemaName) != schemaName {
			return "", fmt.Errorf("client state for user %s is bound to schema %s, not %s", userID, persisted.SchemaName, schemaName)
		}
		if preferredSourceID == "" || preferredSourceID == persisted.SourceID {
			if schemaName != "" && strings.TrimSpace(persisted.SchemaName) == "" {
				if _, err := tx.ExecContext(ctx, `
					UPDATE _sync_client_state
					SET schema_name = ?
					WHERE user_id = ?
				`, schemaName, userID); err != nil {
					return "", fmt.Errorf("failed to persist client schema identity: %w", err)
				}
			}
			if err := tx.Commit(); err != nil {
				return "", fmt.Errorf("failed to commit bootstrap client identity: %w", err)
			}
			return persisted.SourceID, nil
		}
	}

	sourceID := preferredSourceID
	if sourceID == "" {
		sourceID = uuid.NewString()
	}
	if err := c.clearFullLocalSyncStateInTx(ctx, tx); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_client_state`); err != nil {
		return "", fmt.Errorf("failed to clear persisted client identity during bootstrap reset: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO _sync_client_state (
			user_id, source_id, schema_name, next_source_bundle_id, last_bundle_seq_seen, apply_mode, rebuild_required
		) VALUES (?, ?, ?, 1, 0, 0, 1)
	`, userID, sourceID, schemaName); err != nil {
		return "", fmt.Errorf("failed to persist reset bootstrap client identity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit bootstrap identity reset: %w", err)
	}
	return sourceID, nil
}
