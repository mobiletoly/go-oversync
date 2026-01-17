// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Validation error sentinels for better error mapping
var (
	ErrBadPayload        = errors.New("bad_payload")
	ErrUnregisteredTable = errors.New("unregistered_table")
)

// validateChange validates a single change upload
func (s *SyncService) validateChange(change *ChangeUpload) error {
	// Normalize case and whitespace
	change.Schema = strings.ToLower(strings.TrimSpace(change.Schema))
	change.Table = strings.ToLower(strings.TrimSpace(change.Table))
	change.Op = strings.ToUpper(strings.TrimSpace(change.Op))

	// Default schema to "public" if not provided
	if change.Schema == "" {
		change.Schema = "public"
	}

	// Validate schema name (must match ^[a-z0-9_]+$)
	if !isValidSchemaName(change.Schema) {
		return fmt.Errorf("%w: invalid schema name %q", ErrBadPayload, change.Schema)
	}

	// Validate table name (must match ^[a-z0-9_]+$)
	if !isValidTableName(change.Table) {
		return fmt.Errorf("%w: invalid table name %q", ErrBadPayload, change.Table)
	}

	// Validate schema.table combination is registered for sync operations
	if !s.IsTableRegistered(change.Schema, change.Table) {
		return fmt.Errorf("%w: table not registered %s.%s", ErrUnregisteredTable, change.Schema, change.Table)
	}

	// Validate operation
	switch change.Op {
	case OpInsert, OpUpdate, OpDelete:
		// Valid operations
	default:
		return fmt.Errorf("invalid operation: %s", change.Op)
	}

	// Validate UUID format (PK should already be converted by this point, unless it was invalid)
	if _, err := uuid.Parse(change.PK); err != nil {
		return fmt.Errorf("%w: invalid UUID format: %s", ErrBadPayload, change.PK)
	}

	// Payload rules
	if change.Op == OpInsert || change.Op == OpUpdate {
		if len(change.Payload) == 0 {
			return fmt.Errorf("payload required for %s operation", change.Op)
		}

		// Must be a JSON object
		var obj map[string]any
		if err := json.Unmarshal(change.Payload, &obj); err != nil || obj == nil {
			return fmt.Errorf("invalid JSON payload")
		}

		// Enforce per-change payload size limit (bytes of raw JSON)
		if s.config.MaxPayloadBytes > 0 && len(change.Payload) > s.config.MaxPayloadBytes {
			return fmt.Errorf("%w: payload too large: %d > %d", ErrBadPayload, len(change.Payload), s.config.MaxPayloadBytes)
		}

		// Disallow reserved keys
		if _, ok := obj["server_version"]; ok {
			return fmt.Errorf("payload may not contain server_version")
		}
		if _, ok := obj["deleted"]; ok {
			return fmt.Errorf("payload may not contain deleted")
		}

		// Optional: enforce payload.id == pk if present
		//if v, ok := obj["id"]; ok {
		//	if s, ok2 := v.(string); !ok2 || !strings.EqualFold(s, change.PK) {
		//		return fmt.Errorf("payload.id must equal pk %s", change.PK)
		//	}
		//}
	} else { // DELETE
		if len(change.Payload) != 0 {
			return fmt.Errorf("DELETE must not include payload")
		}
	}

	return nil
}

// fetchServerRowJSON fetches current server state for conflict resolution (user-scoped and schema-aware)
func (s *SyncService) fetchServerRowJSON(ctx context.Context, tx pgx.Tx, userID, schemaName, tableName, pkUUID string) (json.RawMessage, error) {
	var serverRow json.RawMessage
	err := tx.QueryRow(ctx, `
		SELECT to_jsonb(x)
		FROM (
			SELECT m.schema_name,
				   m.table_name,
				   m.pk_uuid::text AS id,
				   m.server_version,
				   m.deleted,
				   COALESCE(s.payload, 'null'::jsonb) AS payload
			FROM sync.sync_row_meta m
			LEFT JOIN sync.sync_state s
			  ON s.user_id = m.user_id
			 AND s.schema_name = m.schema_name
			 AND s.table_name = m.table_name
			 AND s.pk_uuid = m.pk_uuid
			WHERE m.user_id = @user_id
			  AND m.schema_name = @schema_name
			  AND m.table_name = @table_name
			  AND m.pk_uuid = @pk_uuid::uuid
		) x`,
		pgx.NamedArgs{
			"user_id":     userID,
			"schema_name": schemaName,
			"table_name":  tableName,
			"pk_uuid":     pkUUID,
		},
	).Scan(&serverRow)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pgx.ErrNoRows
		}
		return nil, fmt.Errorf("fetch server row %s.%s(%s): %w", schemaName, tableName, pkUUID, err)
	}

	return serverRow, nil
}

// isValidSchemaName checks if schema name matches ^[a-z0-9_]+$
func isValidSchemaName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// isValidTableName checks if table name matches ^[a-z0-9_]+$
func isValidTableName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// isValidColumnName checks if column name matches ^[a-z0-9_]+$
func isValidColumnName(name string) bool {
	return isValidTableName(name)
}
