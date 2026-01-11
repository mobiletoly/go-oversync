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

type pkset map[string]struct{}
type batchIndex map[string]pkset // table -> set(pk)

// splitAndOrder splits changes into upserts and deletes, ordering them appropriately
func (s *SyncService) splitAndOrder(changes []ChangeUpload) (upserts, deletes []ChangeUpload) {
	for _, ch := range changes {
		if strings.ToUpper(ch.Op) == OpDelete {
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
	var tableHandler MaterializationHandler
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

type fkPrecheckGroup struct {
	refSchema string
	refTable  string
	refCol    string
	values    map[string]struct{}
	refs      []fkPrecheckRef
}

type fkPrecheckRef struct {
	changeIdx    int
	dbRefValStr  string
	missingLabel string
}

func (s *SyncService) parentsMissingBatch(
	ctx context.Context,
	tx pgx.Tx,
	upserts []ChangeUpload,
	validIdx []int,
	inBatch batchIndex,
) map[int][]string {
	if s.discoveredSchema == nil || len(validIdx) == 0 {
		return nil
	}

	// Convert batchIndex to the expected type
	willExist := make(map[string]map[string]struct{}, len(inBatch))
	for table, pkSet := range inBatch {
		willExist[table] = pkSet
	}

	missingByIdx := make(map[int][]string, len(validIdx))
	groups := make(map[string]*fkPrecheckGroup)

	for _, idx := range validIdx {
		ch := upserts[idx]
		childSchema := ch.Schema
		if childSchema == "" {
			childSchema = "public"
		}
		childKey := Key(childSchema, ch.Table)

		fks := s.discoveredSchema.FKMap[childKey]
		if len(fks) == 0 {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal(ch.Payload, &payload); err != nil {
			missingByIdx[idx] = []string{ReasonBadPayload}
			continue
		}

		// Get table handler for key conversion if available
		var tableHandler MaterializationHandler
		s.mu.RLock()
		if handler, exists := s.tableHandlers[childKey]; exists {
			tableHandler = handler
		}
		s.mu.RUnlock()

		for _, fk := range fks {
			// 1) Extract the foreign key value from payload
			refVal, found := payload[fk.Col]
			if !found {
				continue // FK column not present - skip validation
			}

			dbRefVal, badPayload := convertFKValueForHandler(tableHandler, fk.Col, refVal)
			if badPayload {
				missingByIdx[idx] = []string{ReasonBadPayload}
				break
			}

			// Convert to string for comparison with proper type handling
			refValStr, hasValue := formatFKValue(refVal)
			if !hasValue {
				continue // Null or empty FK value - skip validation
			}

			// Convert database value to string for DB query
			dbRefValStr, dbHasValue := formatFKValue(dbRefVal)
			if !dbHasValue {
				continue // Converted value is null or empty - skip validation
			}
			if parsed, err := uuid.Parse(dbRefValStr); err == nil {
				dbRefValStr = parsed.String()
			}

			// 2) Check if parent will be created in this request (and parent table sorts before this table)
			parentKey := Key(fk.RefSchema, fk.RefTable)
			if willExistSet, ok := willExist[parentKey]; ok {
				if _, exists := willExistSet[dbRefValStr]; exists {
					// Parent will be created - check if it comes before child in ordering
					parentOrder, parentExists := s.discoveredSchema.OrderIdx[parentKey]
					childOrder, childExists := s.discoveredSchema.OrderIdx[childKey]

					// For self-references (parentKey == childKey), allow if parent PK will be created
					// For different tables, require parent to sort before child
					if parentExists && childExists && (parentKey == childKey || parentOrder < childOrder) {
						continue
					}
				}
			}

			groupKey := fk.RefSchema + "." + fk.RefTable + "." + fk.RefCol
			g := groups[groupKey]
			if g == nil {
				g = &fkPrecheckGroup{
					refSchema: fk.RefSchema,
					refTable:  fk.RefTable,
					refCol:    fk.RefCol,
					values:    make(map[string]struct{}),
				}
				groups[groupKey] = g
			}
			g.values[dbRefValStr] = struct{}{}
			g.refs = append(g.refs, fkPrecheckRef{
				changeIdx:    idx,
				dbRefValStr:  dbRefValStr,
				missingLabel: fmt.Sprintf("%s.%s:%s", fk.RefSchema, fk.RefTable, refValStr),
			})
		}
	}

	const chunkSize = 1000
	for _, g := range groups {
		if len(g.refs) == 0 {
			continue
		}

		vals := make([]string, 0, len(g.values))
		for v := range g.values {
			vals = append(vals, v)
		}

		existing := make(map[string]struct{})
		for start := 0; start < len(vals); start += chunkSize {
			end := start + chunkSize
			if end > len(vals) {
				end = len(vals)
			}
			chunk := vals[start:end]

			colIdent := pgx.Identifier{g.refCol}.Sanitize()
			schemaIdent := pgx.Identifier{g.refSchema}.Sanitize()
			tableIdent := pgx.Identifier{g.refTable}.Sanitize()

			query := fmt.Sprintf(
				`SELECT DISTINCT %s::text FROM %s.%s WHERE %s::text = ANY(@vals::text[])`,
				colIdent, schemaIdent, tableIdent, colIdent,
			)

			rows, err := tx.Query(ctx, query, pgx.NamedArgs{"vals": chunk})
			if err != nil {
				s.logger.Error("FK precheck batch failed", "error", err, "parent", g.refSchema+"."+g.refTable, "column", g.refCol)
				for _, ref := range g.refs {
					if prior, ok := missingByIdx[ref.changeIdx]; ok && len(prior) == 1 && prior[0] == ReasonBadPayload {
						continue
					}
					missingByIdx[ref.changeIdx] = []string{ReasonPrecheckError}
				}
				goto nextGroup
			}
			for rows.Next() {
				var v string
				if scanErr := rows.Scan(&v); scanErr != nil {
					s.logger.Error("FK precheck batch scan failed", "error", scanErr, "parent", g.refSchema+"."+g.refTable, "column", g.refCol)
					for _, ref := range g.refs {
						if prior, ok := missingByIdx[ref.changeIdx]; ok && len(prior) == 1 && prior[0] == ReasonBadPayload {
							continue
						}
						missingByIdx[ref.changeIdx] = []string{ReasonPrecheckError}
					}
					rows.Close()
					goto nextGroup
				}
				existing[v] = struct{}{}
			}
			if rows.Err() != nil {
				s.logger.Error("FK precheck batch rows failed", "error", rows.Err(), "parent", g.refSchema+"."+g.refTable, "column", g.refCol)
				for _, ref := range g.refs {
					if prior, ok := missingByIdx[ref.changeIdx]; ok && len(prior) == 1 && prior[0] == ReasonBadPayload {
						continue
					}
					missingByIdx[ref.changeIdx] = []string{ReasonPrecheckError}
				}
				rows.Close()
				goto nextGroup
			}
			rows.Close()
		}

		for _, ref := range g.refs {
			if prior, ok := missingByIdx[ref.changeIdx]; ok {
				if len(prior) == 1 && (prior[0] == ReasonBadPayload || prior[0] == ReasonPrecheckError) {
					continue
				}
			}

			if _, ok := existing[ref.dbRefValStr]; ok {
				continue
			}
			missingByIdx[ref.changeIdx] = append(missingByIdx[ref.changeIdx], ref.missingLabel)
		}
	nextGroup:
	}

	return missingByIdx
}

// processUpserts processes INSERT/UPDATE operations with FK precheck and SAVEPOINTs
func (s *SyncService) processUpserts(ctx context.Context, tx pgx.Tx, userID, sourceID string, upserts []ChangeUpload, inBatch batchIndex) ([]ChangeUploadStatus, error) {
	var statuses []ChangeUploadStatus

	s.logger.Info("Processing upserts batch", "count", len(upserts), "user_id", userID, "source_id", sourceID)

	validIdx := make([]int, 0, len(upserts))
	statusesByIdx := make([]*ChangeUploadStatus, len(upserts))

	for i := range upserts {
		change := &upserts[i]

		s.logger.Debug("Processing upsert", "index", i, "schema", change.Schema, "table", change.Table,
			"pk", change.PK, "source_change_id", change.SourceChangeID)

		// Rely on insert-first idempotency gate in applyUpsert; no pre-check here.

		// Validate change (PK should already be converted by this point)
		if err := s.validateChange(change); err != nil {
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
			var st ChangeUploadStatus
			if errors.Is(err, ErrUnregisteredTable) {
				st = statusInvalidUnregisteredTable(change.SourceChangeID, change.Schema, change.Table)
			} else {
				st = statusInvalidOther(change.SourceChangeID, ReasonBadPayload, err)
			}
			statusesByIdx[i] = &st
			continue
		}

		validIdx = append(validIdx, i)
	}

	missingByIdx := s.parentsMissingBatch(ctx, tx, upserts, validIdx, inBatch)

	for i := range upserts {
		change := upserts[i]

		if statusesByIdx[i] != nil {
			statuses = append(statuses, *statusesByIdx[i])
			continue
		}

		// FK precheck - handle both bad payload and precheck errors explicitly.
		missing := missingByIdx[i]
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

		s.logger.Debug("Upsert completed", "source_change_id", change.SourceChangeID,
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
