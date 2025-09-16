// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"time"
)

// REST/JSON models for HTTP API requests and responses
// These models are used for serialization/deserialization of HTTP requests and responses

// UploadRequest represents a batch upload request from client
// Note: source_id is derived from JWT did claim, not from request body
type UploadRequest struct {
	LastServerSeqSeen int64          `json:"last_server_seq_seen"` // Last server_id client has seen
	Changes           []ChangeUpload `json:"changes"`              // Batch of changes to upload
}

// ChangeUpload represents a single change in upload request
type ChangeUpload struct {
	SourceChangeID int64           `json:"source_change_id"`  // Client-local change ID (from _change_log.id)
	Schema         string          `json:"schema,omitempty"`  // Schema name (default: "public")
	Table          string          `json:"table"`             // Table name (e.g., "note")
	Op             string          `json:"op"`                // INSERT, UPDATE, DELETE
	PK             string          `json:"pk"`                // UUID as string
	ServerVersion  int64           `json:"server_version"`    // Expected server version for conflict detection
	Payload        json.RawMessage `json:"payload,omitempty"` // JSON payload (null for DELETE)
}

// UploadResponse represents server response to upload request
type UploadResponse struct {
	Accepted         bool                 `json:"accepted"`           // Overall success
	HighestServerSeq int64                `json:"highest_server_seq"` // Current max server_id
	Statuses         []ChangeUploadStatus `json:"statuses"`           // Per-change results
}

// ChangeUploadStatus represents the result of processing a single change
type ChangeUploadStatus struct {
	SourceChangeID   int64           `json:"source_change_id"`             // Echo back the client's ID
	Status           string          `json:"status"`                       // "applied", "conflict", "invalid", "materialize_error"
	NewServerVersion *int64          `json:"new_server_version,omitempty"` // New version if applied
	ServerRow        json.RawMessage `json:"server_row,omitempty"`         // Current server state if conflict
	Message          string          `json:"message,omitempty"`            // Optional details for errors
	Invalid          map[string]any  `json:"invalid,omitempty"`            // Structured invalid information with reason and details
}

// DownloadResponse represents server response to download request
type DownloadResponse struct {
	Changes     []ChangeDownloadResponse `json:"changes"`      // Changes to apply
	HasMore     bool                     `json:"has_more"`     // More changes available
	NextAfter   int64                    `json:"next_after"`   // Next server_id to request
	WindowUntil int64                    `json:"window_until"` // Upper-bound snapshot for this paging session
}

// ChangeDownloadResponse represents a single change in download response
type ChangeDownloadResponse struct {
	ServerID       int64           `json:"server_id"`         // Server sequence number
	Schema         string          `json:"schema"`            // Schema name
	TableName      string          `json:"table"`             // Table name (renamed from table_name for consistency)
	Op             string          `json:"op"`                // INSERT, UPDATE, DELETE
	PK             string          `json:"pk"`                // UUID as string
	Payload        json.RawMessage `json:"payload,omitempty"` // JSON payload (may be null for DELETE)
	ServerVersion  int64           `json:"server_version"`    // Current server version from sync_row_meta
	Deleted        bool            `json:"deleted"`           // Deletion status from sync_row_meta
	SourceID       string          `json:"source_id"`         // Originating client (for filtering)
	SourceChangeID int64           `json:"source_change_id"`  // Original client change ID
	Timestamp      time.Time       `json:"ts"`                // When change was applied (renamed for consistency)
}

// Common response models

// SchemaVersionResponse represents the current schema version
type SchemaVersionResponse struct {
	Version int `json:"schema_version"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// StatusResponse represents service status response
type StatusResponse struct {
	Status           string          `json:"status"`            // healthy, degraded, unhealthy
	Version          string          `json:"version"`           // API version
	AppName          string          `json:"app_name"`          // Application name
	RegisteredTables []string        `json:"registered_tables"` // Tables registered for sync
	Features         map[string]bool `json:"features"`          // Enabled features
}

// ToChangeDownloadResponse converts a ChangeDownloadEntity to ChangeDownloadResponse
func (e *ChangeDownloadEntity) ToChangeDownloadResponse() *ChangeDownloadResponse {
	return &ChangeDownloadResponse{
		ServerID:       e.ServerID,
		Schema:         e.SchemaName,
		TableName:      e.TableName,
		Op:             e.Op,
		PK:             e.PkUUID,
		Payload:        e.Payload,
		ServerVersion:  e.ServerVersion,
		Deleted:        e.Deleted,
		SourceID:       e.SourceID,
		SourceChangeID: e.SourceChangeID,
		Timestamp:      e.Timestamp,
	}
}

// ToServerChangeLogEntity converts upload change data to ServerChangeLogEntity
func (c *ChangeUpload) ToServerChangeLogEntity(userID, sourceID string, serverID int64) *ServerChangeLogEntity {
	schema := c.Schema
	if schema == "" {
		schema = "public" // Default schema
	}

	return &ServerChangeLogEntity{
		ServerID:       serverID,
		UserID:         userID,
		SchemaName:     schema,
		TableName:      c.Table,
		Op:             c.Op,
		PkUUID:         c.PK,
		Payload:        c.Payload,
		SourceID:       sourceID,
		SourceChangeID: c.SourceChangeID,
	}
}
