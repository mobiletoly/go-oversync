// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"time"
)

// Database entity models for PostgreSQL tables
// These models are used for database operations and have db struct tags

// SyncRowMetaEntity represents a row in sync.sync_row_meta table
type SyncRowMetaEntity struct {
	UserID        string    `db:"user_id"`        // User identifier (from JWT sub)
	SchemaName    string    `db:"schema_name"`    // Schema name
	TableName     string    `db:"table_name"`     // Table name
	PkUUID        string    `db:"pk_uuid"`        // UUID as string
	ServerVersion int64     `db:"server_version"` // Current server version
	Deleted       bool      `db:"deleted"`        // Deletion status
	UpdatedAt     time.Time `db:"updated_at"`     // Last update timestamp
}

// SyncStateEntity represents a row in sync.sync_state table
type SyncStateEntity struct {
	UserID     string          `db:"user_id"`     // User identifier (from JWT sub)
	SchemaName string          `db:"schema_name"` // Schema name
	TableName  string          `db:"table_name"`  // Table name
	PkUUID     string          `db:"pk_uuid"`     // UUID as string
	Payload    json.RawMessage `db:"payload"`     // Current state payload
}

// ServerChangeLogEntity represents a row in sync.server_change_log table
type ServerChangeLogEntity struct {
	ServerID       int64           `db:"server_id"`        // BIGSERIAL PRIMARY KEY
	UserID         string          `db:"user_id"`          // User identifier (from JWT sub)
	SchemaName     string          `db:"schema_name"`      // Schema name
	TableName      string          `db:"table_name"`       // Table name
	Op             string          `db:"op"`               // INSERT, UPDATE, DELETE
	PkUUID         string          `db:"pk_uuid"`          // UUID as string
	Payload        json.RawMessage `db:"payload"`          // JSON payload
	SourceID       string          `db:"source_id"`        // Device identifier (from JWT did)
	SourceChangeID int64           `db:"source_change_id"` // Client change ID
	ServerVersion  int64           `db:"server_version"`   // Server version for chronological ordering
	Timestamp      time.Time       `db:"ts"`               // Timestamp
}

// ChangeDownloadEntity represents a change for download with database fields
// This is used for database queries that join multiple tables
type ChangeDownloadEntity struct {
	ServerID       int64           `db:"server_id"`        // Server sequence number
	UserID         string          `db:"user_id"`          // User identifier (from JWT sub)
	SchemaName     string          `db:"schema_name"`      // Schema name
	TableName      string          `db:"table_name"`       // Table name
	Op             string          `db:"op"`               // INSERT, UPDATE, DELETE
	PkUUID         string          `db:"pk_uuid"`          // UUID as string
	Payload        json.RawMessage `db:"payload"`          // JSON payload (may be null for DELETE)
	ServerVersion  int64           `db:"server_version"`   // Current server version from sync_row_meta
	Deleted        bool            `db:"deleted"`          // Deletion status from sync_row_meta
	SourceID       string          `db:"source_id"`        // Originating device (from JWT did)
	SourceChangeID int64           `db:"source_change_id"` // Original client change ID
	Timestamp      time.Time       `db:"ts"`               // When change was applied
}
