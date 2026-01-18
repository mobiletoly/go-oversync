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

type refKey struct {
	table string // schema.table
	col   string // referenced column name
}

type inBatchIndex struct {
	pk  batchIndex
	ref map[refKey]pkset
}

// splitAndOrder splits changes into upserts and deletes, ordering them appropriately
func (s *SyncService) splitAndOrder(changes []ChangeUpload) (upserts, deletes []ChangeUpload) {
	for _, ch := range changes {
		if strings.ToUpper(strings.TrimSpace(ch.Op)) == OpDelete {
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

func (s *SyncService) buildInBatchIndex(ups, dels []ChangeUpload, upPayloadObjs []map[string]any) inBatchIndex {
	idx := inBatchIndex{
		pk:  buildBatchPKIndex(ups, dels),
		ref: nil,
	}

	if s.discoveredSchema == nil || s.config == nil || s.config.FKPrecheckMode != FKPrecheckRefColumnAware {
		return idx
	}

	// Build a map of parent tables -> referenced columns (RefCol) that appear in FK constraints.
	neededRefCols := make(map[string]map[string]struct{})
	for _, fks := range s.discoveredSchema.FKMap {
		for _, fk := range fks {
			parentKey := Key(fk.RefSchema, fk.RefTable)
			col := strings.ToLower(fk.RefCol)
			m := neededRefCols[parentKey]
			if m == nil {
				m = make(map[string]struct{})
				neededRefCols[parentKey] = m
			}
			m[col] = struct{}{}
		}
	}
	if len(neededRefCols) == 0 {
		return idx
	}

	// If a row is upserted and deleted within the same batch, treat it as "will not exist" for ref-column indexing.
	type rowKey struct {
		table string
		pk    string
	}
	deletedRows := make(map[rowKey]struct{}, len(dels))
	for _, ch := range dels {
		schema := ch.Schema
		if schema == "" {
			schema = "public"
		}
		deletedRows[rowKey{table: Key(schema, ch.Table), pk: ch.PK}] = struct{}{}
	}

	idx.ref = make(map[refKey]pkset)

	usePayloadCache := upPayloadObjs != nil && len(upPayloadObjs) == len(ups)

	for i, ch := range ups {
		schema := ch.Schema
		if schema == "" {
			schema = "public"
		}
		tableKey := Key(schema, ch.Table)

		cols := neededRefCols[tableKey]
		if len(cols) == 0 {
			continue
		}
		if _, deleted := deletedRows[rowKey{table: tableKey, pk: ch.PK}]; deleted {
			continue
		}

		var payload map[string]any
		if usePayloadCache {
			payload = upPayloadObjs[i]
			if payload == nil {
				continue
			}
		} else {
			if err := json.Unmarshal(ch.Payload, &payload); err != nil {
				continue
			}
		}

		// Optional table handler conversion (allows payload encoding differences).
		var tableHandler MaterializationHandler
		s.mu.RLock()
		tableHandler = s.tableHandlers[tableKey]
		s.mu.RUnlock()

		for col := range cols {
			raw, ok := payload[col]
			if !ok {
				continue
			}

			dbVal, badPayload := convertFKValueForHandler(tableHandler, col, raw)
			if badPayload {
				continue
			}

			valStr, hasValue := formatFKValue(dbVal)
			if !hasValue {
				continue
			}
			if parsed, err := uuid.Parse(valStr); err == nil {
				valStr = parsed.String()
			}

			key := refKey{table: tableKey, col: col}
			set := idx.ref[key]
			if set == nil {
				set = pkset{}
				idx.ref[key] = set
			}
			set[valStr] = struct{}{}
		}
	}

	return idx
}

// parentsMissing checks if referenced parents exist now or will exist via this request
func (s *SyncService) parentsMissing(ctx context.Context, tx pgx.Tx, ch ChangeUpload, inBatch inBatchIndex) (missing []string) {
	// Parse JSON payload once
	var payload map[string]any
	if err := json.Unmarshal(ch.Payload, &payload); err != nil {
		return []string{ReasonBadPayload} // caller will map to invalid.bad_payload
	}
	if s.discoveredSchema == nil {
		return nil // No schema discovery available
	}
	if s.config != nil && s.config.FKPrecheckMode == FKPrecheckDisabled {
		return nil
	}

	// Convert batchIndex to the expected type
	willExist := make(map[string]map[string]struct{})
	for table, pkSet := range inBatch.pk {
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
	userID string,
	upserts []ChangeUpload,
	validIdx []int,
	inBatch inBatchIndex,
	payloadObjs []map[string]any,
) map[int][]string {
	if s.discoveredSchema == nil || len(validIdx) == 0 {
		return nil
	}
	if s.config != nil && s.config.FKPrecheckMode == FKPrecheckDisabled {
		return nil
	}

	// Convert batchIndex to the expected type
	willExist := make(map[string]map[string]struct{}, len(inBatch.pk))
	for table, pkSet := range inBatch.pk {
		willExist[table] = pkSet
	}

	missingByIdx := make(map[int][]string, len(validIdx))
	groups := make(map[string]*fkPrecheckGroup)

	tenantCol := ""
	if s.config != nil {
		tenantCol = strings.ToLower(strings.TrimSpace(s.config.TenantScopeColumn))
	}

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
		if payloadObjs != nil && idx >= 0 && idx < len(payloadObjs) {
			payload = payloadObjs[idx]
		}
		if payload == nil {
			if err := json.Unmarshal(ch.Payload, &payload); err != nil {
				missingByIdx[idx] = []string{ReasonBadPayload}
				continue
			}
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

			// If the parent column is UUID, treat non-UUID payload values as bad payload early (avoid
			// server-side cast errors and keep precheck queries index-friendly).
			if s.discoveredSchema != nil && s.discoveredSchema.IsUUIDColumn(fk.RefSchema, fk.RefTable, fk.RefCol) {
				parsed, err := uuid.Parse(dbRefValStr)
				if err != nil {
					missingByIdx[idx] = []string{ReasonBadPayload}
					break
				}
				dbRefValStr = parsed.String()
			} else if parsed, err := uuid.Parse(dbRefValStr); err == nil {
				dbRefValStr = parsed.String()
			}

			// 2) Check if parent will be created in this request (and parent table sorts before this table)
			parentKey := Key(fk.RefSchema, fk.RefTable)
			if s.config != nil && s.config.FKPrecheckMode == FKPrecheckRefColumnAware {
				if inBatch.ref != nil {
					if refSet, ok := inBatch.ref[refKey{table: parentKey, col: strings.ToLower(fk.RefCol)}]; ok {
						if _, exists := refSet[dbRefValStr]; exists {
							parentOrder, parentExists := s.discoveredSchema.OrderIdx[parentKey]
							childOrder, childExists := s.discoveredSchema.OrderIdx[childKey]
							if parentExists && childExists && (parentKey == childKey || parentOrder < childOrder) {
								continue
							}
						}
					}
				}
			}
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

			args := pgx.NamedArgs{"vals": chunk}

			// Keep this query index-friendly:
			// - Prefer typed comparisons (uuid/text) without casting the column.
			// - Fall back to text casting only when we don't know the column type.
			refType := ""
			if s.discoveredSchema != nil {
				refType = s.discoveredSchema.ColumnType(g.refSchema, g.refTable, g.refCol)
			}
			isUUID := refType == "uuid"
			isText := refType == "text" || refType == "varchar" || refType == "bpchar" || refType == "citext"

			where := fmt.Sprintf(`%s::text = ANY(@vals::text[])`, colIdent)
			if isUUID {
				where = fmt.Sprintf(`%s = ANY(@vals::uuid[])`, colIdent)
			} else if isText {
				where = fmt.Sprintf(`%s = ANY(@vals::text[])`, colIdent)
			}

			if tenantCol != "" {
				tenantIdent := pgx.Identifier{tenantCol}.Sanitize()
				tenantType := ""
				if s.discoveredSchema != nil {
					tenantType = s.discoveredSchema.ColumnType(g.refSchema, g.refTable, tenantCol)
				}
				switch tenantType {
				case "uuid":
					where += fmt.Sprintf(` AND %s = @tenant::uuid`, tenantIdent)
				case "text", "varchar", "bpchar", "citext":
					where += fmt.Sprintf(` AND %s = @tenant`, tenantIdent)
				default:
					where += fmt.Sprintf(` AND %s::text = @tenant::text`, tenantIdent)
				}
				args["tenant"] = userID
			}

			// Use a simple SELECT and de-duplicate in memory (map) to avoid extra DB work.
			query := fmt.Sprintf(`SELECT %s::text FROM %s.%s WHERE %s`, colIdent, schemaIdent, tableIdent, where)

			rows, err := tx.Query(ctx, query, args)
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
func (s *SyncService) processUpserts(ctx context.Context, tx pgx.Tx, userID, sourceID string, upserts, deletes []ChangeUpload, useBatchedApply bool) ([]ChangeUploadStatus, error) {
	s.logger.Info("Processing upserts batch", "count", len(upserts), "user_id", userID, "source_id", sourceID)

	validIdx := make([]int, 0, len(upserts))
	statusesByIdx := make([]*ChangeUploadStatus, len(upserts))
	payloadObjs := make([]map[string]any, len(upserts))

	for i := range upserts {
		change := &upserts[i]

		s.logger.Debug("Processing upsert", "index", i, "schema", change.Schema, "table", change.Table,
			"pk", change.PK, "source_change_id", change.SourceChangeID)

		// Validate change (PK should already be converted by this point)
		payloadObj, err := s.validateChangeAndMaybeParsePayload(change, payloadObjs[i])
		if err != nil {
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

		// splitAndOrder should have separated deletes, but guard against whitespace/invalid ops.
		if change.Op == OpDelete {
			msg := fmt.Errorf("unexpected delete in upserts batch")
			st := statusInvalidOther(change.SourceChangeID, ReasonBadPayload, msg)
			statusesByIdx[i] = &st
			continue
		}

		payloadObjs[i] = payloadObj
		validIdx = append(validIdx, i)
	}

	inBatch := s.buildInBatchIndex(upserts, deletes, payloadObjs)
	missingByIdx := s.parentsMissingBatch(ctx, tx, userID, upserts, validIdx, inBatch, payloadObjs)

	applyIdx := make([]int, 0, len(validIdx))

	for i := range upserts {
		change := upserts[i]

		if statusesByIdx[i] != nil {
			continue
		}

		// FK precheck - handle both bad payload and precheck errors explicitly.
		missing := missingByIdx[i]
		if len(missing) > 0 {
			var st ChangeUploadStatus
			if len(missing) == 1 {
				switch missing[0] {
				case ReasonPrecheckError:
					st = statusInvalidOther(change.SourceChangeID, ReasonPrecheckError, fmt.Errorf("FK precheck failed for %s.%s pk=%s", change.Schema, change.Table, change.PK))
				case ReasonBadPayload:
					st = statusInvalidOther(change.SourceChangeID, ReasonBadPayload, fmt.Errorf("bad payload for %s.%s pk=%s", change.Schema, change.Table, change.PK))
				default:
					st = statusInvalidFKMissing(change.SourceChangeID, missing)
				}
			} else {
				st = statusInvalidFKMissing(change.SourceChangeID, missing)
			}
			statusesByIdx[i] = &st
			continue
		}

		applyIdx = append(applyIdx, i)
	}

	type matItem struct {
		change           ChangeUpload
		deleted          bool
		attemptedVersion int64
	}
	var mats []matItem

	if useBatchedApply && len(applyIdx) > 1 {
		outcomes, err := s.applyUpsertsSidecarBatched(ctx, tx, userID, sourceID, upserts, applyIdx)
		if err != nil {
			return nil, err
		}
		if len(outcomes) != len(applyIdx) {
			return nil, fmt.Errorf("batched upsert apply outcomes mismatch: got=%d want=%d", len(outcomes), len(applyIdx))
		}

		for i, idx := range applyIdx {
			ch := upserts[idx]
			o := outcomes[i]

			var st ChangeUploadStatus
			switch o.code {
			case 0:
				st = statusAppliedIdempotent(ch.SourceChangeID)
			case 1:
				st = statusApplied(ch.SourceChangeID, o.newServerVer64)
				mats = append(mats, matItem{change: ch, deleted: false, attemptedVersion: o.newServerVer64})
			case 2:
				serverRow, fetchErr := s.fetchServerRowJSON(ctx, tx, userID, ch.Schema, ch.Table, ch.PK)
				if fetchErr != nil {
					if errors.Is(fetchErr, pgx.ErrNoRows) {
						st = statusInvalidOther(ch.SourceChangeID, ReasonInternalError, fmt.Errorf("conflict without server_row for %s.%s pk=%s", ch.Schema, ch.Table, ch.PK))
					} else {
						return nil, fmt.Errorf("failed to fetch server row for conflict: %w", fetchErr)
					}
				} else {
					st = statusConflict(ch.SourceChangeID, serverRow)
				}
			case 3:
				st = statusInvalidOther(ch.SourceChangeID, ReasonInternalError, fmt.Errorf("conflict without server_row for %s.%s pk=%s", ch.Schema, ch.Table, ch.PK))
			default:
				return nil, fmt.Errorf("unknown apply upsert code %d", o.code)
			}
			statusesByIdx[idx] = &st
		}
	} else {
		for _, idx := range applyIdx {
			ch := upserts[idx]

			st, err := s.applyUpsert(ctx, tx, userID, sourceID, ch)
			if err != nil {
				s.logger.Error("Failed to apply upsert", "error", err, "source_change_id", ch.SourceChangeID,
					"schema", ch.Schema, "table", ch.Table, "pk", ch.PK)
				return nil, fmt.Errorf("failed to apply upsert for change %d: %w", ch.SourceChangeID, err)
			}
			statusesByIdx[idx] = &st
		}
	}

	for _, m := range mats {
		if err := s.applyBusinessProjectionBestEffort(ctx, tx, userID, m.change, m.deleted, m.attemptedVersion); err != nil {
			s.logger.Warn("Business projection failed; sync still applied",
				"error", err, "schema", m.change.Schema, "table", m.change.Table, "pk", m.change.PK,
				"server_version", m.attemptedVersion, "source_change_id", m.change.SourceChangeID)
		}
	}

	statuses := make([]ChangeUploadStatus, 0, len(upserts))
	for i, st := range statusesByIdx {
		if st == nil {
			statuses = append(statuses, statusInternalError(upserts[i].SourceChangeID, fmt.Errorf("missing status for scid")))
			continue
		}
		statuses = append(statuses, *st)
	}

	return statuses, nil
}

// processDeletes processes DELETE operations with SAVEPOINTs
func (s *SyncService) processDeletes(
	ctx context.Context, tx pgx.Tx, userID, sourceID string, deletes []ChangeUpload,
	useBatchedApply bool,
) ([]ChangeUploadStatus, error) {
	statusesByIdx := make([]*ChangeUploadStatus, len(deletes))
	applyIdx := make([]int, 0, len(deletes))

	for i := range deletes {
		change := &deletes[i]

		// Validate change (PK should already be converted by this point)
		if err := s.validateChange(change); err != nil {
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

		applyIdx = append(applyIdx, i)
	}

	type matItem struct {
		change           ChangeUpload
		deleted          bool
		attemptedVersion int64
	}
	var mats []matItem

	if useBatchedApply && len(applyIdx) > 1 {
		outcomes, err := s.applyDeletesSidecarBatched(ctx, tx, userID, sourceID, deletes, applyIdx)
		if err != nil {
			return nil, err
		}
		if len(outcomes) != len(applyIdx) {
			return nil, fmt.Errorf("batched delete apply outcomes mismatch: got=%d want=%d", len(outcomes), len(applyIdx))
		}
		for i, idx := range applyIdx {
			ch := deletes[idx]
			o := outcomes[i]

			var st ChangeUploadStatus
			switch o.code {
			case 0, 3:
				st = statusAppliedIdempotent(ch.SourceChangeID)
			case 1:
				st = statusApplied(ch.SourceChangeID, o.newServerVer64)
				mats = append(mats, matItem{change: ch, deleted: true, attemptedVersion: o.newServerVer64})
			case 2:
				serverRow, fetchErr := s.fetchServerRowJSON(ctx, tx, userID, ch.Schema, ch.Table, ch.PK)
				if fetchErr != nil {
					if errors.Is(fetchErr, pgx.ErrNoRows) {
						st = statusAppliedIdempotent(ch.SourceChangeID)
					} else {
						return nil, fmt.Errorf("failed to fetch server row for conflict: %w", fetchErr)
					}
				} else {
					st = statusConflict(ch.SourceChangeID, serverRow)
				}
			default:
				return nil, fmt.Errorf("unknown apply delete code %d", o.code)
			}
			statusesByIdx[idx] = &st
		}
	} else {
		for _, idx := range applyIdx {
			ch := deletes[idx]

			st, err := s.applyDelete(ctx, tx, userID, sourceID, ch)
			if err != nil {
				return nil, fmt.Errorf("failed to apply delete for change %d: %w", ch.SourceChangeID, err)
			}
			statusesByIdx[idx] = &st
		}
	}

	for _, m := range mats {
		if err := s.applyBusinessProjectionBestEffort(ctx, tx, userID, m.change, m.deleted, m.attemptedVersion); err != nil {
			s.logger.Warn("Business projection failed; sync still applied",
				"error", err, "schema", m.change.Schema, "table", m.change.Table, "pk", m.change.PK,
				"server_version", m.attemptedVersion, "source_change_id", m.change.SourceChangeID)
		}
	}

	statuses := make([]ChangeUploadStatus, 0, len(deletes))
	for i, st := range statusesByIdx {
		if st == nil {
			statuses = append(statuses, statusInternalError(deletes[i].SourceChangeID, fmt.Errorf("missing status for scid")))
			continue
		}
		statuses = append(statuses, *st)
	}

	return statuses, nil
}
