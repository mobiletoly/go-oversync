// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

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
	BundleSeq            int64       `json:"bundle_seq"`
	SourceID             string      `json:"source_id"`
	SourceBundleID       int64       `json:"source_bundle_id"`
	RowCount             int64       `json:"row_count"`
	ByteCount            int64       `json:"byte_count"`
	BundleHash           string      `json:"bundle_hash,omitempty"`
	CanonicalRequestHash string      `json:"canonical_request_hash,omitempty"`
	Rows                 []BundleRow `json:"rows"`
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

// CompactPushRequest carries exactly one complete v1 source bundle. The rows
// are compact transport groups; their retained logical form is reconstructed
// before canonical-hash validation and any service call.
type CompactPushRequest struct {
	SourceBundleID       int64             `json:"source_bundle_id"`
	CanonicalRequestHash string            `json:"canonical_request_hash"`
	InitializationID     string            `json:"initialization_id,omitempty"`
	Rows                 []CompactRowGroup `json:"rows"`
}

func validateJSONObjectFields(data []byte, objectName string, required, optional []string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("%s must be an object", objectName)
	}
	allowed := make(map[string]struct{}, len(required)+len(optional))
	for _, name := range required {
		allowed[name] = struct{}{}
	}
	for _, name := range optional {
		allowed[name] = struct{}{}
	}
	for name := range fields {
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("%s contains unknown member %q", objectName, name)
		}
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s missing required %s", objectName, name)
		}
	}
	for _, name := range optional {
		if value, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s %s must be omitted or non-null", objectName, name)
		}
	}
	return nil
}

func (request *CompactPushRequest) UnmarshalJSON(data []byte) error {
	type plain CompactPushRequest
	if err := validateJSONObjectFields(data, "compact push request",
		[]string{"source_bundle_id", "canonical_request_hash", "rows"},
		[]string{"initialization_id"}); err != nil {
		return err
	}
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*request = CompactPushRequest(decoded)
	return nil
}

// CompactPushResponse is the complete committed/replayed authoritative
// effect. It intentionally omits request/session bookkeeping the client
// already owns.
type CompactPushResponse struct {
	BundleSeq  int64             `json:"bundle_seq"`
	BundleHash string            `json:"bundle_hash"`
	Rows       []CompactRowGroup `json:"rows"`
}

// CompactBundle and CompactPullResponse remove redundant row-count, byte-count
// and push-only request-hash fields from the retained incremental read path.
type CompactBundle struct {
	BundleSeq      int64             `json:"bundle_seq"`
	SourceID       string            `json:"source_id"`
	SourceBundleID int64             `json:"source_bundle_id"`
	BundleHash     string            `json:"bundle_hash"`
	Rows           []CompactRowGroup `json:"rows"`
}

type CompactPullResponse struct {
	StableBundleSeq int64           `json:"stable_bundle_seq"`
	Bundles         []CompactBundle `json:"bundles"`
	HasMore         bool            `json:"has_more"`
}

type ConnectRequest struct {
	HasLocalPendingRows bool `json:"has_local_pending_rows"`
}

func (request *ConnectRequest) UnmarshalJSON(data []byte) error {
	type plain ConnectRequest
	if err := validateJSONObjectFields(data, "connect request", []string{"has_local_pending_rows"}, nil); err != nil {
		return err
	}
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*request = ConnectRequest(decoded)
	return nil
}

type ConnectResponse struct {
	Resolution       string `json:"resolution"`
	InitializationID string `json:"initialization_id,omitempty"`
	LeaseExpiresAt   string `json:"lease_expires_at,omitempty"`
	RetryAfterSec    int    `json:"retry_after_seconds,omitempty"`
}

// PullResponse returns one or more complete committed bundles.
type PullResponse struct {
	StableBundleSeq          int64    `json:"stable_bundle_seq"`
	Bundles                  []Bundle `json:"bundles"`
	HasMore                  bool     `json:"has_more"`
	historyProjectionRelease func()   `json:"-"`
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
	Recovery          *SnapshotRecovery          `json:"recovery,omitempty"`
}

func (request *SnapshotSessionCreateRequest) UnmarshalJSON(data []byte) error {
	type plain SnapshotSessionCreateRequest
	if err := decodeRecoveryObject(data, nil, []string{"source_replacement", "recovery"}, "", new(map[string]json.RawMessage)); err != nil {
		return err
	}
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.SourceReplacement != nil && decoded.Recovery != nil {
		return fmt.Errorf("source replacement and recovery are mutually exclusive")
	}
	*request = SnapshotSessionCreateRequest(decoded)
	return nil
}

type SnapshotSourceReplacement struct {
	PreviousSourceID string `json:"previous_source_id"`
	NewSourceID      string `json:"new_source_id"`
	Reason           string `json:"reason"`
}

func (replacement *SnapshotSourceReplacement) UnmarshalJSON(data []byte) error {
	type plain SnapshotSourceReplacement
	if err := validateJSONObjectFields(data, "snapshot source replacement",
		[]string{"previous_source_id", "new_source_id", "reason"}, nil); err != nil {
		return err
	}
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*replacement = SnapshotSourceReplacement(decoded)
	return nil
}

// SnapshotSession describes one frozen server-side snapshot session.
type SnapshotSession struct {
	SnapshotID        string            `json:"snapshot_id"`
	SnapshotBundleSeq int64             `json:"snapshot_bundle_seq"`
	RowCount          int64             `json:"row_count"`
	ByteCount         int64             `json:"byte_count"`
	ExpiresAt         string            `json:"expires_at"`
	SnapshotHash      string            `json:"snapshot_hash"`
	Recovery          *SnapshotRecovery `json:"recovery,omitempty"`
}

// SnapshotChunkResponse returns one chunk of snapshot rows from a frozen session.
type SnapshotChunkResponse struct {
	SnapshotID        string        `json:"snapshot_id"`
	SnapshotBundleSeq int64         `json:"snapshot_bundle_seq"`
	Rows              []SnapshotRow `json:"rows"`
	NextRowOrdinal    int64         `json:"next_row_ordinal"`
	HasMore           bool          `json:"has_more"`
	ByteCount         int64         `json:"byte_count"`
}

type CompactSnapshotSessionResponse struct {
	SnapshotID        string            `json:"snapshot_id"`
	SnapshotBundleSeq int64             `json:"snapshot_bundle_seq"`
	RowCount          int64             `json:"row_count"`
	LogicalByteCount  int64             `json:"logical_byte_count"`
	ExpiresAt         string            `json:"expires_at"`
	SnapshotHash      string            `json:"snapshot_hash"`
	Recovery          *SnapshotRecovery `json:"recovery,omitempty"`
}

type CompactSnapshotChunkResponse struct {
	Rows             []CompactRowGroup `json:"rows"`
	NextRowOrdinal   int64             `json:"next_row_ordinal"`
	LogicalByteCount int64             `json:"logical_byte_count"`
	HasMore          bool              `json:"has_more"`
}

// Common response models

const SyncProtocolVersion = "v1"

// GenerationDiscoveryResponse is the sole admitted wire-contract response.
type GenerationDiscoveryResponse struct {
	ProtocolVersion  string                       `json:"protocol_version"`
	SchemaGeneration SchemaGenerationCapabilities `json:"schema_generation"`
	Limits           WireLimits                   `json:"limits"`
	WatchEnabled     bool                         `json:"watch_enabled"`
}

// WireLimits contains only client-enforced protocol bounds, never server
// scheduling or materialization tuning.
type WireLimits struct {
	MaxPushRows                     int   `json:"max_push_rows"`
	MaxPushDecodedBytes             int64 `json:"max_push_decoded_bytes"`
	MaxPullBundles                  int   `json:"max_pull_bundles"`
	MaxPullDecodedBytes             int64 `json:"max_pull_decoded_bytes"`
	MaxSnapshotRowsPerChunk         int   `json:"max_snapshot_rows_per_chunk"`
	MaxSnapshotDecodedBytesPerChunk int64 `json:"max_snapshot_decoded_bytes_per_chunk"`
	MaxSnapshotRows                 int64 `json:"max_snapshot_rows"`
	MaxSnapshotLogicalBytes         int64 `json:"max_snapshot_logical_bytes"`
	MaxRowDecodedBytes              int64 `json:"max_row_decoded_bytes"`
	MaxPushOutcomeDecodedBytes      int64 `json:"max_push_outcome_decoded_bytes"`
}

type SchemaGenerationCapabilities struct {
	ID          string                              `json:"id"`
	Fingerprint string                              `json:"fingerprint"`
	Tables      []SchemaGenerationTableCapabilities `json:"tables"`
}

type SchemaGenerationTableCapabilities struct {
	Schema         string                                   `json:"schema"`
	Table          string                                   `json:"table"`
	SyncKeyColumns []string                                 `json:"sync_key_columns"`
	Columns        []SchemaGenerationColumnCapabilities     `json:"columns"`
	ForeignKeys    []SchemaGenerationForeignKeyCapabilities `json:"foreign_keys"`
}

type SchemaGenerationColumnCapabilities struct {
	Name     string             `json:"name"`
	WireType GenerationWireType `json:"wire_type"`
	Nullable bool               `json:"nullable"`
}

type SchemaGenerationForeignKeyCapabilities struct {
	Columns           []string                   `json:"columns"`
	ReferencedSchema  string                     `json:"referenced_schema"`
	ReferencedTable   string                     `json:"referenced_table"`
	ReferencedColumns []string                   `json:"referenced_columns"`
	OnDelete          GenerationForeignKeyAction `json:"on_delete"`
	OnUpdate          GenerationForeignKeyAction `json:"on_update"`
}

// RegisteredTableSpec describes one registered sync table in the newer contract surface.
type RegisteredTableSpec struct {
	Schema         string   `json:"schema"`
	Table          string   `json:"table"`
	SyncKeyColumns []string `json:"sync_key_columns"`
}

func (spec *RegisteredTableSpec) UnmarshalJSON(data []byte) error {
	type plainSpec RegisteredTableSpec
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"schema", "table", "sync_key_columns"} {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("registered table spec missing required %s", name)
		}
	}
	var decoded plainSpec
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if strings.TrimSpace(decoded.Schema) == "" {
		return fmt.Errorf("registered table spec schema must be a non-blank identifier")
	}
	if strings.TrimSpace(decoded.Table) == "" {
		return fmt.Errorf("registered table spec table must be a non-blank identifier")
	}
	if len(decoded.SyncKeyColumns) != 1 {
		return fmt.Errorf("registered table spec sync_key_columns must contain exactly one entry")
	}
	if strings.TrimSpace(decoded.SyncKeyColumns[0]) == "" {
		return fmt.Errorf("registered table spec sync_key_columns must contain a non-blank identifier")
	}
	*spec = RegisteredTableSpec(decoded)
	return nil
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error                    string `json:"error"`
	Message                  string `json:"message"`
	RequiredDecodedBodyBytes int64  `json:"required_decoded_body_bytes,omitempty"`
}

// SnapshotSessionLimitResponse reports a stable snapshot materialization limit failure.
type SnapshotSessionLimitResponse struct {
	Error     string `json:"error"`
	Message   string `json:"message"`
	Dimension string `json:"dimension"`
	Actual    int64  `json:"actual"`
	Limit     int64  `json:"limit"`
}

type SourceRetiredResponse struct {
	Error              string `json:"error"`
	Message            string `json:"message"`
	SourceID           string `json:"source_id"`
	ReplacedBySourceID string `json:"replaced_by_source_id,omitempty"`
}

func (response *SourceRetiredResponse) UnmarshalJSON(data []byte) error {
	type plainResponse SourceRetiredResponse
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"error", "message", "source_id"} {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("source_retired response missing required %s", name)
		}
	}
	if replacement, ok := fields["replaced_by_source_id"]; ok && string(replacement) == "null" {
		return fmt.Errorf("source_retired replaced_by_source_id must be omitted or non-null")
	}
	var decoded plainResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Error != "source_retired" {
		return fmt.Errorf("source_retired response has invalid error code")
	}
	if err := sourceid.Validate(decoded.SourceID); err != nil {
		return fmt.Errorf("source_retired response source_id is invalid: %w", err)
	}
	if _, present := fields["replaced_by_source_id"]; present {
		if err := sourceid.Validate(decoded.ReplacedBySourceID); err != nil {
			return fmt.Errorf("source_retired response replaced_by_source_id is invalid: %w", err)
		}
	}
	*response = SourceRetiredResponse(decoded)
	return nil
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
