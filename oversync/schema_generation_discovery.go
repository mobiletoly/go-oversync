// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/syncroute"
)

const generationControlResponseLimit = 65536

// GetGeneration returns registered metadata without binding or admitting a source.
func (s *SyncService) GetGeneration(ctx context.Context, actor Actor, id SchemaGenerationID) (*GenerationDiscoveryResponse, error) {
	done, err := s.beginLifecycleOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	return s.getGeneration(ctx, actor, id)
}

func (s *SyncService) getGeneration(ctx context.Context, actor Actor, id SchemaGenerationID) (*GenerationDiscoveryResponse, error) {
	if err := rejectRetryableCallbackContext(ctx, "GetGeneration"); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.validateClientActor(actor, true); err != nil {
		return nil, err
	}
	if id < 1 {
		return nil, &SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Message: "schema generation ID is invalid"}
	}
	// Discovery needs a published registry, but must not acquire a database
	// admission lease or consult/create the actor's source binding.
	if s.config.Readiness != nil && s.config.Readiness.Expected.GenerationLedger != nil {
		for _, entry := range s.config.Readiness.Expected.GenerationLedger.Entries {
			if entry.Proof.ID == id && entry.State == "retired" {
				return nil, &SchemaGenerationRequestError{Code: SchemaGenerationRetired, GenerationID: id, Message: "Update the application; preserve local data and pending sync work"}
			}
		}
	}
	contract, ok := s.schemaGenerationRegistry.Generation(id)
	if !ok {
		return nil, &SchemaGenerationRequestError{Code: SchemaGenerationUnknown, GenerationID: id, Message: "schema generation is unknown"}
	}
	return s.generationDiscoveryResponse(contract, s.bundleChangeWatchEnabled()), nil
}

func (s *SyncService) generationDiscoveryResponse(contract SchemaGenerationContract, watchEnabled bool) *GenerationDiscoveryResponse {
	maxSnapshotRows := s.maxRowsPerSnapshotSession()
	maxSnapshotLogicalBytes := s.maxBytesPerSnapshotSession()
	// The service retains zero as the internal representation of an unbounded
	// session. The wire contract has no unbounded sentinel: advertise a large,
	// JSON-integer-safe positive bound instead.
	if maxSnapshotRows == 0 {
		maxSnapshotRows = 1 << 50
	}
	if maxSnapshotLogicalBytes == 0 {
		maxSnapshotLogicalBytes = 1 << 50
	}
	return &GenerationDiscoveryResponse{
		ProtocolVersion:  SyncProtocolVersion,
		SchemaGeneration: generationDescriptor(contract),
		Limits: WireLimits{
			MaxPushRows:                     s.config.MaxRowsPerBundle,
			MaxPushDecodedBytes:             int64(s.config.MaxBytesPerBundle) + historyEnvelopeAllowance,
			MaxPullBundles:                  defaultMaxBundlesPerPull,
			MaxPullDecodedBytes:             s.config.MaxBytesPerPullResponse,
			MaxSnapshotRowsPerChunk:         s.maxRowsPerSnapshotChunk(),
			MaxSnapshotDecodedBytesPerChunk: s.maxBytesPerSnapshotChunk(),
			MaxSnapshotRows:                 maxSnapshotRows,
			MaxSnapshotLogicalBytes:         maxSnapshotLogicalBytes,
			MaxRowDecodedBytes:              s.maxBytesPerSnapshotRow(),
		},
		WatchEnabled: watchEnabled,
	}
}

// HandleGeneration exposes discovery through the same actor validation as direct calls.
func (h *HTTPSyncHandlers) HandleGeneration(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}
	rawID := r.PathValue("generation_id")
	if rawID == "" && strings.HasPrefix(r.URL.Path, "/sync/generations/") {
		rawID = strings.TrimPrefix(r.URL.Path, "/sync/generations/")
	}
	id, err := parseSchemaGenerationID(rawID)
	if !syncroute.IsCanonicalGenerationDiscovery(r) || err != nil || len(exactHeaderValues(r.Header, SchemaGenerationIDHeader)) != 0 || len(exactHeaderValues(r.Header, SchemaGenerationFingerprintHeader)) != 0 {
		h.writeDiscoveryError(w, r, http.StatusBadRequest, "invalid_generation_discovery_request", "Supply a canonical generation path and omit generation proof headers", "correct_request", id)
		return
	}
	done, err := h.service.beginLifecycleOperation()
	if err != nil {
		h.writeServiceUnavailable(w, r, err)
		return
	}
	defer done()
	response, err := h.service.getGeneration(r.Context(), actor, id)
	if err != nil {
		var requestErr *SchemaGenerationRequestError
		if errors.As(err, &requestErr) {
			status, remediation := http.StatusConflict, "upgrade_or_reconfigure_client"
			if requestErr.Code == SchemaGenerationRetired {
				status, remediation = http.StatusUpgradeRequired, "upgrade_application"
			}
			h.writeDiscoveryError(w, r, status, string(requestErr.Code), requestErr.Message, remediation, id)
			return
		}
		if !h.writeServiceUnavailable(w, r, err) {
			h.writeError(w, http.StatusInternalServerError, "generation_discovery_failed", "Failed to discover schema generation", r)
		}
		return
	}
	h.writeBoundedJSON(w, r, http.StatusOK, response, generationControlResponseLimit, "Failed to encode generation discovery response")
}

func (h *HTTPSyncHandlers) writeDiscoveryError(w http.ResponseWriter, r *http.Request, status int, code, message, remediation string, id SchemaGenerationID) {
	response := struct {
		GenerationErrorResponse
		GenerationID string `json:"schema_generation_id,omitempty"`
	}{GenerationErrorResponse: GenerationErrorResponse{Error: code, Message: message, Remediation: remediation}}
	if id > 0 {
		response.GenerationID = canonicalSchemaGenerationID(id)
	}
	h.writeBoundedJSON(w, r, status, response, generationControlResponseLimit, "Failed to encode generation discovery error")
}

func (s *SyncService) validateGenerationResponseSizes() error {
	for _, id := range s.schemaGenerationRegistry.GenerationIDs() {
		contract, _ := s.schemaGenerationRegistry.Generation(id)
		for _, response := range []any{s.generationDiscoveryResponse(contract, false)} {
			body, err := json.Marshal(response)
			if err != nil {
				return err
			}
			if len(body)+1 > generationControlResponseLimit {
				return fmt.Errorf("schema generation %d control response exceeds %d bytes", id, generationControlResponseLimit)
			}
		}
	}
	return nil
}

func generationDescriptor(contract SchemaGenerationContract) SchemaGenerationCapabilities {
	tables := make([]SchemaGenerationTableCapabilities, 0, len(contract.Tables()))
	for _, table := range contract.Tables() {
		out := SchemaGenerationTableCapabilities{Schema: table.Schema, Table: table.Table, SyncKeyColumns: append([]string(nil), table.SyncKeyColumns...), Columns: make([]SchemaGenerationColumnCapabilities, 0, len(table.Columns)), ForeignKeys: make([]SchemaGenerationForeignKeyCapabilities, 0, len(table.ForeignKeys))}
		for _, column := range table.Columns {
			out.Columns = append(out.Columns, SchemaGenerationColumnCapabilities{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable})
		}
		for _, fk := range table.ForeignKeys {
			out.ForeignKeys = append(out.ForeignKeys, SchemaGenerationForeignKeyCapabilities{Columns: append([]string(nil), fk.Columns...), ReferencedSchema: fk.ReferencedSchema, ReferencedTable: fk.ReferencedTable, ReferencedColumns: append([]string(nil), fk.ReferencedColumns...), OnDelete: fk.OnDelete, OnUpdate: fk.OnUpdate})
		}
		tables = append(tables, out)
	}

	return SchemaGenerationCapabilities{ID: canonicalSchemaGenerationID(contract.ID()), Fingerprint: contract.Fingerprint(), Tables: tables}
}
