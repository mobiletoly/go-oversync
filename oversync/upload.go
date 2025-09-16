// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type pkset map[string]struct{}
type batchIndex map[string]pkset // table -> set(pk)

// splitAndOrder splits changes into upserts and deletes, ordering them appropriately
func (s *SyncService) splitAndOrder(changes []ChangeUpload) (upserts, deletes []ChangeUpload) {
	for _, ch := range changes {
		if ch.Op == OpDelete {
			deletes = append(deletes, ch)
		} else {
			upserts = append(upserts, ch)
		}
	}

	// Use discovered schema for ordering
	if s.discoveredSchema != nil {
		if s.discoveredSchema.HasDependencies(changes) {
			s.discoveredSchema.SortUpserts(upserts)
		}
		// Always sort deletes child-first as a harmless default that avoids surprises
		// when FKs get added later (will be a no-op for tables not in the order index)
		s.discoveredSchema.SortDeletes(deletes)
	}

	return
}

// Under snapshot isolation, ParentsMissing must consider parent rows introduced by this request.
// buildBatchPKIndex computes the set of PKs that *will exist* at the end of the request:
// all upserts minus any deletes in the same batch. This feeds FK precheck logic.
// buildBatchPKIndex indexes the PKs that will exist by the end of this request
func buildBatchPKIndex(ups, dels []ChangeUpload) batchIndex {
	m := batchIndex{}
	add := func(schema, table, pk string) {
		if schema == "" {
			schema = "public"
		}
		k := Key(schema, table)
		if m[k] == nil {
			m[k] = pkset{}
		}
		m[k][pk] = struct{}{}
	}

	// Add all upserts as "will exist"
	for _, ch := range ups {
		add(ch.Schema, ch.Table, ch.PK)
	}

	// Subtract deletes - they remove rows that would otherwise exist
	for _, ch := range dels {
		schema := ch.Schema
		if schema == "" {
			schema = "public"
		}
		k := Key(schema, ch.Table)
		if set, ok := m[k]; ok {
			delete(set, ch.PK)
			if len(set) == 0 {
				delete(m, k)
			}
		}
	}

	return m
}

// parentsMissing checks if referenced parents exist now or will exist via this request
func (s *SyncService) parentsMissing(ctx context.Context, tx pgx.Tx, ch ChangeUpload, inBatch batchIndex) (missing []string) {
	// Parse JSON payload once
	var payload map[string]any
	if err := json.Unmarshal(ch.Payload, &payload); err != nil {
		return []string{ReasonBadPayload} // caller will map to invalid.bad_payload
	}
	if s.discoveredSchema == nil {
		return nil // No schema discovery available
	}

	// Convert batchIndex to the expected type
	willExist := make(map[string]map[string]struct{})
	for table, pkSet := range inBatch {
		willExist[table] = pkSet
	}

	// Default schema to "public" if not provided (same as validation)
	schema := ch.Schema
	if schema == "" {
		schema = "public"
	}

	// Get table handler for key conversion if available
	var tableHandler TableHandler
	s.mu.RLock()
	handlerKey := schema + "." + ch.Table
	if handler, exists := s.tableHandlers[handlerKey]; exists {
		tableHandler = handler
	}
	s.mu.RUnlock()

	missingParents, err := s.discoveredSchema.ParentsMissing(ctx, tx, schema, ch.Table, payload, willExist, tableHandler)
	if err != nil {
		s.logger.Error("FK precheck failed", "error", err, "table", schema+"."+ch.Table)
		return []string{ReasonPrecheckError}
	}

	return missingParents
}

// processUpserts processes INSERT/UPDATE operations with FK precheck and SAVEPOINTs
func (s *SyncService) processUpserts(ctx context.Context, tx pgx.Tx, userID, sourceID string, upserts []ChangeUpload, inBatch batchIndex) ([]ChangeUploadStatus, error) {
	var statuses []ChangeUploadStatus

	s.logger.Info("Processing upserts batch", "count", len(upserts), "user_id", userID, "source_id", sourceID)

	for i, change := range upserts {
		s.logger.Debug("Processing upsert", "index", i, "schema", change.Schema, "table", change.Table,
			"pk", change.PK, "source_change_id", change.SourceChangeID)

		// Rely on insert-first idempotency gate in applyUpsert; no pre-check here.

		// Validate change (PK should already be converted by this point)
		if err := s.validateChange(&change); err != nil {
			// Log validation failure with helpful context
			reason := ReasonBadPayload
			if errors.Is(err, ErrUnregisteredTable) {
				reason = ReasonUnregisteredTable
			}
			s.logger.Error("Upload validation failed",
				"user_id", userID,
				"source_id", sourceID,
				"op", change.Op,
				"schema", change.Schema,
				"table", change.Table,
				"pk", change.PK,
				"reason", reason,
				"error", err,
			)
			if errors.Is(err, ErrUnregisteredTable) {
				statuses = append(statuses, statusInvalidUnregisteredTable(change.SourceChangeID, change.Schema, change.Table))
			} else {
				statuses = append(statuses, statusInvalidOther(change.SourceChangeID, ReasonBadPayload, err))
			}
			continue
		}

		// FK precheck - handle both bad payload and precheck errors explicitly
		missing := s.parentsMissing(ctx, tx, change, inBatch)
		if len(missing) > 0 {
			if len(missing) == 1 {
				switch missing[0] {
				case ReasonPrecheckError:
					statuses = append(statuses, statusInvalidOther(change.SourceChangeID, ReasonPrecheckError, fmt.Errorf("FK precheck failed for %s.%s pk=%s", change.Schema, change.Table, change.PK)))
					continue
				case ReasonBadPayload:
					statuses = append(statuses, statusInvalidOther(change.SourceChangeID, ReasonBadPayload, fmt.Errorf("bad payload for %s.%s pk=%s", change.Schema, change.Table, change.PK)))
					continue
				}
			}
			statuses = append(statuses, statusInvalidFKMissing(change.SourceChangeID, missing))
			continue
		}

		// Process with SAVEPOINT
		status, err := s.applyUpsert(ctx, tx, userID, sourceID, change)
		if err != nil {
			s.logger.Error("Failed to apply upsert", "error", err, "source_change_id", change.SourceChangeID,
				"schema", change.Schema, "table", change.Table, "pk", change.PK)
			return nil, fmt.Errorf("failed to apply upsert for change %d: %w", change.SourceChangeID, err)
		}

		s.logger.Info("Upsert completed", "source_change_id", change.SourceChangeID,
			"status", status.Status, "schema", change.Schema, "table", change.Table, "pk", change.PK)
		statuses = append(statuses, status)
	}

	return statuses, nil
}

// processDeletes processes DELETE operations with SAVEPOINTs
func (s *SyncService) processDeletes(
	ctx context.Context, tx pgx.Tx, userID, sourceID string, deletes []ChangeUpload,
) ([]ChangeUploadStatus, error) {
	var statuses []ChangeUploadStatus

	for _, change := range deletes {
		// Rely on insert-first idempotency gate in applyDelete; no pre-check here.

		// Validate change (PK should already be converted by this point)
		if err := s.validateChange(&change); err != nil {
			// Log validation failure with helpful context
			reason := ReasonBadPayload
			if errors.Is(err, ErrUnregisteredTable) {
				reason = ReasonUnregisteredTable
			}
			s.logger.Error("Upload validation failed",
				"user_id", userID,
				"source_id", sourceID,
				"op", change.Op,
				"schema", change.Schema,
				"table", change.Table,
				"pk", change.PK,
				"reason", reason,
				"error", err,
			)
			if errors.Is(err, ErrUnregisteredTable) {
				statuses = append(statuses, statusInvalidUnregisteredTable(change.SourceChangeID, change.Schema, change.Table))
			} else {
				statuses = append(statuses, statusInvalidOther(change.SourceChangeID, ReasonBadPayload, err))
			}
			continue
		}

		// Process with SAVEPOINT
		status, err := s.applyDelete(ctx, tx, userID, sourceID, change)
		if err != nil {
			return nil, fmt.Errorf("failed to apply delete for change %d: %w", change.SourceChangeID, err)
		}
		statuses = append(statuses, status)
	}

	return statuses, nil
}
