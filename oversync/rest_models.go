// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import "encoding/json"

// REST/JSON models for HTTP API requests and responses
// These models are used for serialization/deserialization of HTTP requests and responses

// SyncKey represents the canonical structured identity for a synced row.
type SyncKey map[string]any

// BundleRow describes one normalized row effect inside a committed sync bundle.
type BundleRow struct {
	Schema     string          `json:"schema"`
	Table      string          `json:"table"`
	Key        SyncKey         `json:"key"`
	Op         string          `json:"op"`
	RowVersion int64           `json:"row_version"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// Bundle represents one committed durable sync unit in the target bundle-based contract.
type Bundle struct {
	BundleSeq      int64       `json:"bundle_seq"`
	SourceID       string      `json:"source_id"`
	SourceBundleID int64       `json:"source_bundle_id"`
	RowCount       int64       `json:"row_count,omitempty"`
	BundleHash     string      `json:"bundle_hash,omitempty"`
	Rows           []BundleRow `json:"rows"`
}

// PushRequestRow is one locally dirty row intent sent by the client.
type PushRequestRow struct {
	Schema         string          `json:"schema"`
	Table          string          `json:"table"`
	Key            SyncKey         `json:"key"`
	Op             string          `json:"op"`
	BaseRowVersion int64           `json:"base_row_version"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type PushSessionCreateRequest struct {
	SourceBundleID   int64  `json:"source_bundle_id"`
	PlannedRowCount  int64  `json:"planned_row_count"`
	InitializationID string `json:"initialization_id,omitempty"`
}

type PushSessionCreateResponse struct {
	PushID                 string `json:"push_id,omitempty"`
	Status                 string `json:"status"`
	PlannedRowCount        int64  `json:"planned_row_count,omitempty"`
	NextExpectedRowOrdinal int64  `json:"next_expected_row_ordinal,omitempty"`
	BundleSeq              int64  `json:"bundle_seq,omitempty"`
	SourceID               string `json:"source_id,omitempty"`
	SourceBundleID         int64  `json:"source_bundle_id,omitempty"`
	RowCount               int64  `json:"row_count,omitempty"`
	BundleHash             string `json:"bundle_hash,omitempty"`
}

type ConnectRequest struct {
	HasLocalPendingRows bool `json:"has_local_pending_rows"`
}

type ConnectResponse struct {
	Resolution       string `json:"resolution"`
	InitializationID string `json:"initialization_id,omitempty"`
	LeaseExpiresAt   string `json:"lease_expires_at,omitempty"`
	RetryAfterSec    int    `json:"retry_after_seconds,omitempty"`
}

type PushSessionChunkRequest struct {
	StartRowOrdinal int64            `json:"start_row_ordinal"`
	Rows            []PushRequestRow `json:"rows"`
}

type PushSessionChunkResponse struct {
	PushID                 string `json:"push_id"`
	NextExpectedRowOrdinal int64  `json:"next_expected_row_ordinal"`
}

type PushSessionCommitResponse struct {
	BundleSeq      int64  `json:"bundle_seq"`
	SourceID       string `json:"source_id"`
	SourceBundleID int64  `json:"source_bundle_id"`
	RowCount       int64  `json:"row_count"`
	BundleHash     string `json:"bundle_hash"`
}

type CommittedBundleRowsResponse struct {
	BundleSeq      int64       `json:"bundle_seq"`
	SourceID       string      `json:"source_id"`
	SourceBundleID int64       `json:"source_bundle_id"`
	RowCount       int64       `json:"row_count"`
	BundleHash     string      `json:"bundle_hash"`
	Rows           []BundleRow `json:"rows"`
	NextRowOrdinal int64       `json:"next_row_ordinal"`
	HasMore        bool        `json:"has_more"`
}

// PullResponse returns one or more complete committed bundles.
type PullResponse struct {
	StableBundleSeq int64    `json:"stable_bundle_seq"`
	Bundles         []Bundle `json:"bundles"`
	HasMore         bool     `json:"has_more"`
}

// SnapshotRow is the current after-image for one non-deleted row in the bundle-based contract.
type SnapshotRow struct {
	Schema     string          `json:"schema"`
	Table      string          `json:"table"`
	Key        SyncKey         `json:"key"`
	RowVersion int64           `json:"row_version"`
	Payload    json.RawMessage `json:"payload"`
}

type SnapshotSessionCreateRequest struct {
	SourceReplacement *SnapshotSourceReplacement `json:"source_replacement,omitempty"`
}

type SnapshotSourceReplacement struct {
	PreviousSourceID string `json:"previous_source_id"`
	NewSourceID      string `json:"new_source_id"`
	Reason           string `json:"reason"`
}

// SnapshotSession describes one frozen server-side snapshot session.
type SnapshotSession struct {
	SnapshotID        string `json:"snapshot_id"`
	SnapshotBundleSeq int64  `json:"snapshot_bundle_seq"`
	RowCount          int64  `json:"row_count"`
	ByteCount         int64  `json:"byte_count,omitempty"`
	ExpiresAt         string `json:"expires_at"`
}

// SnapshotChunkResponse returns one chunk of snapshot rows from a frozen session.
type SnapshotChunkResponse struct {
	SnapshotID        string        `json:"snapshot_id"`
	SnapshotBundleSeq int64         `json:"snapshot_bundle_seq"`
	Rows              []SnapshotRow `json:"rows"`
	NextRowOrdinal    int64         `json:"next_row_ordinal"`
	HasMore           bool          `json:"has_more"`
}

// Common response models

const SyncProtocolVersion = "v1"

// CapabilitiesResponse describes the currently supported sync protocol surface.
type CapabilitiesResponse struct {
	ProtocolVersion      string                    `json:"protocol_version"`
	SchemaVersion        int                       `json:"schema_version"`
	AppName              string                    `json:"app_name,omitempty"`
	RegisteredTables     []string                  `json:"registered_tables,omitempty"`
	RegisteredTableSpecs []RegisteredTableSpec     `json:"registered_table_specs,omitempty"`
	Features             map[string]bool           `json:"features"`
	BundleLimits         *BundleCapabilitiesLimits `json:"bundle_limits,omitempty"`
}

// BundleCapabilitiesLimits reports bundle-oriented guardrails in the target contract.
type BundleCapabilitiesLimits struct {
	MaxRowsPerBundle                   int   `json:"max_rows_per_bundle,omitempty"`
	MaxBytesPerBundle                  int   `json:"max_bytes_per_bundle,omitempty"`
	MaxBundlesPerPull                  int   `json:"max_bundles_per_pull,omitempty"`
	DefaultRowsPerPushChunk            int   `json:"default_rows_per_push_chunk,omitempty"`
	MaxRowsPerPushChunk                int   `json:"max_rows_per_push_chunk,omitempty"`
	PushSessionTTLSeconds              int   `json:"push_session_ttl_seconds,omitempty"`
	DefaultRowsPerCommittedBundleChunk int   `json:"default_rows_per_committed_bundle_chunk,omitempty"`
	MaxRowsPerCommittedBundleChunk     int   `json:"max_rows_per_committed_bundle_chunk,omitempty"`
	DefaultRowsPerSnapshotChunk        int   `json:"default_rows_per_snapshot_chunk,omitempty"`
	MaxRowsPerSnapshotChunk            int   `json:"max_rows_per_snapshot_chunk,omitempty"`
	SnapshotSessionTTLSeconds          int   `json:"snapshot_session_ttl_seconds,omitempty"`
	MaxRowsPerSnapshotSession          int64 `json:"max_rows_per_snapshot_session,omitempty"`
	MaxBytesPerSnapshotSession         int64 `json:"max_bytes_per_snapshot_session,omitempty"`
	InitializationLeaseTTLSeconds      int   `json:"initialization_lease_ttl_seconds,omitempty"`
}

// RegisteredTableSpec describes one registered sync table in the newer contract surface.
type RegisteredTableSpec struct {
	Schema         string   `json:"schema"`
	Table          string   `json:"table"`
	SyncKeyColumns []string `json:"sync_key_columns,omitempty"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type SourceRetiredResponse struct {
	Error              string `json:"error"`
	Message            string `json:"message"`
	SourceID           string `json:"source_id"`
	ReplacedBySourceID string `json:"replaced_by_source_id,omitempty"`
}

// PushConflictDetails describes one authoritative row state that rejected a push commit.
// The payload is deterministic:
//   - schema/table are normalized lowercase identifiers
//   - key uses the same canonical sync-key JSON shape as the rest of the API
//   - server_row_version is 0 when no authoritative row_state exists yet
//   - server_row_deleted distinguishes tombstones from an absent row when server_row is null
//   - server_row is the canonical wire-format full row after-image, or null if deleted/missing
type PushConflictDetails struct {
	Schema           string          `json:"schema"`
	Table            string          `json:"table"`
	Key              SyncKey         `json:"key"`
	Op               string          `json:"op"`
	BaseRowVersion   int64           `json:"base_row_version"`
	ServerRowVersion int64           `json:"server_row_version"`
	ServerRowDeleted bool            `json:"server_row_deleted"`
	ServerRow        json.RawMessage `json:"server_row"`
}

// PushConflictResponse preserves the legacy error/message envelope while adding machine-readable conflict details.
type PushConflictResponse struct {
	Error    string               `json:"error"`
	Message  string               `json:"message"`
	Conflict *PushConflictDetails `json:"conflict,omitempty"`
}

// StatusResponse represents service status response
type StatusResponse struct {
	Status                            string          `json:"status"`                                 // healthy or unhealthy
	Version                           string          `json:"version"`                                // API version
	AppName                           string          `json:"app_name"`                               // Application name
	Lifecycle                         string          `json:"lifecycle"`                              // running, shutting_down, closed
	AcceptingOperations               bool            `json:"accepting_operations"`                   // Whether new sync operations are accepted
	InFlightOperations                int             `json:"inflight_operations"`                    // Current in-flight sync operations
	RegisteredTables                  []string        `json:"registered_tables"`                      // Tables registered for sync
	Features                          map[string]bool `json:"features"`                               // Enabled features
	UserStateRetentionFloorAheadCount int64           `json:"user_state_retention_floor_ahead_count"` // user_state rows with retained_bundle_floor > next_bundle_seq - 1
	LatestBundleSeqMax                int64           `json:"latest_bundle_seq_max"`                  // highest committed bundle seq visible across sync.user_state
	RetainedBundleFloorMin            int64           `json:"retained_bundle_floor_min"`              // minimum retained_bundle_floor across sync.user_state
	RetainedBundleFloorMax            int64           `json:"retained_bundle_floor_max"`              // maximum retained_bundle_floor across sync.user_state
	RetainedBundleWindowMin           int64           `json:"retained_bundle_window_min"`             // minimum retained history window (latest bundle seq - retained floor)
	RetainedBundleWindowMax           int64           `json:"retained_bundle_window_max"`             // maximum retained history window (latest bundle seq - retained floor)
	HistoryPrunedErrorCount           int64           `json:"history_pruned_error_count"`             // total history_pruned responses observed by the server
	AcceptedPushReplayCount           int64           `json:"accepted_push_replay_count"`             // total accepted-push replay hits observed by the server
	RejectedRegisteredWriteCount      int64           `json:"rejected_registered_write_count"`        // total writes rejected for missing sync bundle context
	CommittedBundleCount              int64           `json:"committed_bundle_count"`                 // total committed bundle count visible in sync.bundle_log
	CommittedBundleBytes              int64           `json:"committed_bundle_bytes"`                 // total committed bundle bytes visible in sync.bundle_log
}
