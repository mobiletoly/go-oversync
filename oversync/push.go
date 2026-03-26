// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

type PushValidationError struct {
	Message string
}

func (e *PushValidationError) Error() string {
	return e.Message
}

type PushConflictError struct {
	Message  string
	Conflict *PushConflictDetails
}

func (e *PushConflictError) Error() string {
	return e.Message
}

type pushPreparedRow struct {
	schema         string
	table          string
	op             string
	keyColumn      string
	keyType        string
	keyString      string
	keyValue       any
	keyJSON        string
	baseRowVersion int64
	payload        []byte
	payloadColumns []string
	inputOrder     int
}

type rowStateSnapshot struct {
	rowVersion int64
	deleted    bool
}

type indexedRowStateSnapshot struct {
	rowStateSnapshot
	found bool
}

func (s *SyncService) preparePushRows(rows []PushRequestRow) ([]pushPreparedRow, error) {
	return s.preparePushRowsWithOptions(rows, false)
}

func (s *SyncService) preparePushRowsPreservingInput(rows []PushRequestRow) ([]pushPreparedRow, error) {
	return s.preparePushRowsWithOptions(rows, true)
}

func (s *SyncService) preparePushRowsWithOptions(rows []PushRequestRow, preserveInputOrder bool) ([]pushPreparedRow, error) {
	prepared := make([]pushPreparedRow, 0, len(rows))
	seenTargets := make(map[string]struct{}, len(rows))

	for i, row := range rows {
		schemaName := strings.ToLower(strings.TrimSpace(row.Schema))
		if schemaName == "" {
			schemaName = "public"
		}
		tableName := strings.ToLower(strings.TrimSpace(row.Table))
		op := strings.ToUpper(strings.TrimSpace(row.Op))

		if !isValidSchemaName(schemaName) {
			return nil, &PushValidationError{Message: fmt.Sprintf("invalid schema name %q", row.Schema)}
		}
		if !isValidTableName(tableName) {
			return nil, &PushValidationError{Message: fmt.Sprintf("invalid table name %q", row.Table)}
		}
		if !s.IsTableRegistered(schemaName, tableName) {
			return nil, &PushValidationError{Message: fmt.Sprintf("table %s.%s is not registered", schemaName, tableName)}
		}
		switch op {
		case OpInsert, OpUpdate, OpDelete:
		default:
			return nil, &PushValidationError{Message: fmt.Sprintf("invalid op %q", row.Op)}
		}

		normalizedKey, err := s.normalizeVisibleSyncKey(schemaName, tableName, row.Key)
		if err != nil {
			return nil, err
		}

		targetKey := schemaName + "\x00" + tableName + "\x00" + normalizedKey.keyJSON
		if _, exists := seenTargets[targetKey]; exists {
			return nil, &PushValidationError{Message: fmt.Sprintf("duplicate target row in push request: %s.%s %s", schemaName, tableName, normalizedKey.keyString)}
		}
		seenTargets[targetKey] = struct{}{}

		var payload []byte
		var payloadColumns []string
		if op == OpDelete {
			if len(row.Payload) != 0 {
				return nil, &PushValidationError{Message: fmt.Sprintf("DELETE for %s.%s must not include payload", schemaName, tableName)}
			}
		} else {
			if len(row.Payload) == 0 {
				return nil, &PushValidationError{Message: fmt.Sprintf("%s for %s.%s requires payload", op, schemaName, tableName)}
			}

			var payloadObj map[string]any
			if err := json.Unmarshal(row.Payload, &payloadObj); err != nil || payloadObj == nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s must be a JSON object", schemaName, tableName)}
			}
			if _, ok := payloadObj[syncScopeColumnName]; ok {
				return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s must not include hidden scope column %q", schemaName, tableName, syncScopeColumnName)}
			}
			if err := normalizePayloadVisibleSyncKey(payloadObj, normalizedKey, schemaName, tableName); err != nil {
				return nil, err
			}

			for col := range payloadObj {
				if !isValidColumnName(strings.ToLower(col)) {
					return nil, &PushValidationError{Message: fmt.Sprintf("payload for %s.%s contains invalid column %q", schemaName, tableName, col)}
				}
				payloadColumns = append(payloadColumns, strings.ToLower(col))
			}
			if err := s.normalizePushPayloadBinaryFields(schemaName, tableName, payloadObj); err != nil {
				return nil, err
			}
			sort.Strings(payloadColumns)

			payloadRaw, err := json.Marshal(payloadObj)
			if err != nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("marshal payload for %s.%s: %v", schemaName, tableName, err)}
			}
			payload, err = canonicalJSON(payloadRaw)
			if err != nil {
				return nil, &PushValidationError{Message: fmt.Sprintf("canonicalize payload for %s.%s: %v", schemaName, tableName, err)}
			}
		}

		prepared = append(prepared, pushPreparedRow{
			schema:         schemaName,
			table:          tableName,
			op:             op,
			keyColumn:      normalizedKey.column,
			keyType:        normalizedKey.keyType,
			keyString:      normalizedKey.keyString,
			keyValue:       normalizedKey.dbValue,
			keyJSON:        normalizedKey.keyJSON,
			baseRowVersion: row.BaseRowVersion,
			payload:        payload,
			payloadColumns: payloadColumns,
			inputOrder:     i,
		})
	}

	if !preserveInputOrder {
		sort.SliceStable(prepared, func(i, j int) bool {
			opRank := func(op string) int {
				if op == OpDelete {
					return 1
				}
				return 0
			}
			if opRank(prepared[i].op) != opRank(prepared[j].op) {
				return opRank(prepared[i].op) < opRank(prepared[j].op)
			}

			orderI := s.tableOrderIndex(prepared[i].schema, prepared[i].table)
			orderJ := s.tableOrderIndex(prepared[j].schema, prepared[j].table)
			if prepared[i].op == OpDelete {
				if orderI != orderJ {
					return orderI > orderJ
				}
			} else if orderI != orderJ {
				return orderI < orderJ
			}
			return prepared[i].inputOrder < prepared[j].inputOrder
		})
	}

	return prepared, nil
}

func (s *SyncService) tableOrderIndex(schemaName, tableName string) int {
	if s == nil || s.discoveredSchema == nil {
		return 0
	}
	if idx, ok := s.discoveredSchema.OrderIdx[Key(schemaName, tableName)]; ok {
		return idx
	}
	return 0
}

func loadRowStateSnapshot(ctx context.Context, tx pgx.Tx, userID, schemaName, tableName, keyJSON string) (*rowStateSnapshot, error) {
	var state rowStateSnapshot
	err := tx.QueryRow(ctx, `
		SELECT row_version, deleted
		FROM sync.row_state
		WHERE user_id = $1
		  AND schema_name = $2
		  AND table_name = $3
		  AND key_json = $4
	`, userID, schemaName, tableName, keyJSON).Scan(&state.rowVersion, &state.deleted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load row state for %s.%s %s: %w", schemaName, tableName, keyJSON, err)
	}
	return &state, nil
}

func loadRowStateSnapshots(ctx context.Context, tx pgx.Tx, userID string, rows []pushPreparedRow) ([]indexedRowStateSnapshot, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	schemaNames := make([]string, len(rows))
	tableNames := make([]string, len(rows))
	keyJSONs := make([]string, len(rows))
	for i, row := range rows {
		schemaNames[i] = row.schema
		tableNames[i] = row.table
		keyJSONs[i] = row.keyJSON
	}

	queryRows, err := tx.Query(ctx, `
		WITH requested AS (
			SELECT ord::bigint, schema_name, table_name, key_json
			FROM unnest($2::text[], $3::text[], $4::text[]) WITH ORDINALITY
				AS req(schema_name, table_name, key_json, ord)
		)
		SELECT req.ord, rs.row_version, rs.deleted
		FROM requested AS req
		LEFT JOIN sync.row_state AS rs
		  ON rs.user_id = $1
		 AND rs.schema_name = req.schema_name
		 AND rs.table_name = req.table_name
		 AND rs.key_json = req.key_json
		ORDER BY req.ord
	`, userID, schemaNames, tableNames, keyJSONs)
	if err != nil {
		return nil, fmt.Errorf("bulk load row state snapshots: %w", err)
	}
	defer queryRows.Close()

	states := make([]indexedRowStateSnapshot, len(rows))
	for queryRows.Next() {
		var (
			ord        int64
			rowVersion *int64
			deleted    *bool
		)
		if err := queryRows.Scan(&ord, &rowVersion, &deleted); err != nil {
			return nil, fmt.Errorf("scan bulk row state snapshot: %w", err)
		}
		idx := int(ord) - 1
		if idx < 0 || idx >= len(states) {
			return nil, fmt.Errorf("bulk row state snapshot returned invalid ordinal %d", ord)
		}
		if rowVersion == nil || deleted == nil {
			continue
		}
		states[idx] = indexedRowStateSnapshot{
			rowStateSnapshot: rowStateSnapshot{
				rowVersion: *rowVersion,
				deleted:    *deleted,
			},
			found: true,
		}
	}
	if queryRows.Err() != nil {
		return nil, fmt.Errorf("iterate bulk row state snapshots: %w", queryRows.Err())
	}
	return states, nil
}

func (s *SyncService) pushConflictError(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow, state *rowStateSnapshot, message string) error {
	var (
		serverRowVersion int64
		serverRowDeleted bool
		serverRow        json.RawMessage
	)
	if state != nil {
		serverRowVersion = state.rowVersion
		serverRowDeleted = state.deleted
	}
	if state != nil && !state.deleted {
		var err error
		serverRow, err = s.loadCurrentServerRowPayload(ctx, tx, ownerUserID, row)
		if err != nil {
			return err
		}
	}
	return &PushConflictError{
		Message: message,
		Conflict: &PushConflictDetails{
			Schema:           row.schema,
			Table:            row.table,
			Key:              SyncKey{row.keyColumn: row.keyString},
			Op:               row.op,
			BaseRowVersion:   row.baseRowVersion,
			ServerRowVersion: serverRowVersion,
			ServerRowDeleted: serverRowDeleted,
			ServerRow:        serverRow,
		},
	}
}

func (s *SyncService) loadCurrentServerRowPayload(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow) (json.RawMessage, error) {
	tableIdent := pgx.Identifier{row.schema, row.table}.Sanitize()
	keyColumnIdent := pgx.Identifier{row.keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()

	var payload []byte
	err := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT to_jsonb(src) - '_sync_scope_id'
		FROM %s AS src
		WHERE %s = $1
		  AND %s = $2
	`, tableIdent, keyColumnIdent, ownerColumnIdent), row.keyValue, ownerUserID).Scan(&payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load current server row payload for %s.%s %s: %w", row.schema, row.table, row.keyString, err)
	}

	canonicalPayload, err := s.canonicalizeWirePayload(row.schema, row.table, payload)
	if err != nil {
		return nil, fmt.Errorf("canonicalize current server row payload for %s.%s %s: %w", row.schema, row.table, row.keyString, err)
	}
	return canonicalPayload, nil
}

func (s *SyncService) validatePushRowConflict(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow, state *rowStateSnapshot) error {
	switch row.op {
	case OpInsert:
		if state == nil {
			if row.baseRowVersion != 0 {
				return s.pushConflictError(ctx, tx, ownerUserID, row, nil, fmt.Sprintf("insert conflict on %s.%s %s: expected missing row at version 0, got %d", row.schema, row.table, row.keyString, row.baseRowVersion))
			}
			return nil
		}
		if row.baseRowVersion != state.rowVersion {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, fmt.Sprintf("insert conflict on %s.%s %s: expected base_row_version %d, got %d", row.schema, row.table, row.keyString, state.rowVersion, row.baseRowVersion))
		}
		if !state.deleted {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, fmt.Sprintf("insert conflict on %s.%s %s: row already exists at version %d", row.schema, row.table, row.keyString, state.rowVersion))
		}
	case OpUpdate, OpDelete:
		if state == nil || state.deleted {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, fmt.Sprintf("%s conflict on %s.%s %s: row does not exist", strings.ToLower(row.op), row.schema, row.table, row.keyString))
		}
		if row.baseRowVersion != state.rowVersion {
			return s.pushConflictError(ctx, tx, ownerUserID, row, state, fmt.Sprintf("%s conflict on %s.%s %s: expected version %d, got %d", strings.ToLower(row.op), row.schema, row.table, row.keyString, state.rowVersion, row.baseRowVersion))
		}
	}
	return nil
}

func applyPreparedPushRow(ctx context.Context, tx pgx.Tx, ownerUserID string, row pushPreparedRow) error {
	tableIdent := pgx.Identifier{row.schema, row.table}.Sanitize()
	keyColumnIdent := pgx.Identifier{row.keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()

	switch row.op {
	case OpDelete:
		stmt := fmt.Sprintf(`DELETE FROM %s WHERE %s = $1 AND %s = $2`, tableIdent, keyColumnIdent, ownerColumnIdent)
		if _, err := tx.Exec(ctx, stmt, row.keyValue, ownerUserID); err != nil {
			return fmt.Errorf("delete %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		return nil
	case OpInsert:
		payloadWithOwner, err := injectOwnerUserIDPayload(row.payload, ownerUserID)
		if err != nil {
			return fmt.Errorf("inject owner into insert payload for %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		columnList := quotedInsertColumnList(row.payloadColumns)
		stmt := fmt.Sprintf(`
			INSERT INTO %s (%s)
			SELECT %s
			FROM jsonb_populate_record(NULL::%s, $1::jsonb)
		`, tableIdent, columnList, columnList, tableIdent)
		if _, err := tx.Exec(ctx, stmt, payloadWithOwner); err != nil {
			return fmt.Errorf("insert %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		return nil
	case OpUpdate:
		setClauses := make([]string, 0, len(row.payloadColumns))
		for _, col := range row.payloadColumns {
			colIdent := pgx.Identifier{col}.Sanitize()
			setClauses = append(setClauses, fmt.Sprintf("%s = src.%s", colIdent, colIdent))
		}
		stmt := fmt.Sprintf(`
			UPDATE %s AS dst
			SET %s
			FROM (SELECT * FROM jsonb_populate_record(NULL::%s, $1::jsonb)) AS src
			WHERE dst.%s = $2
			  AND dst.%s = $3
		`, tableIdent, strings.Join(setClauses, ", "), tableIdent, keyColumnIdent, ownerColumnIdent)
		if _, err := tx.Exec(ctx, stmt, row.payload, row.keyValue, ownerUserID); err != nil {
			return fmt.Errorf("update %s.%s %s: %w", row.schema, row.table, row.keyString, err)
		}
		return nil
	default:
		return &PushValidationError{Message: fmt.Sprintf("unsupported op %q", row.op)}
	}
}

func batchPreparedPushRows(rows []pushPreparedRow) [][]pushPreparedRow {
	if len(rows) == 0 {
		return nil
	}

	batches := make([][]pushPreparedRow, 0, len(rows))
	start := 0
	for i := 1; i <= len(rows); i++ {
		if i < len(rows) && canBatchPreparedPushRows(rows[i-1], rows[i]) {
			continue
		}
		batches = append(batches, rows[start:i])
		start = i
	}
	return batches
}

func canBatchPreparedPushRows(a, b pushPreparedRow) bool {
	return a.schema == b.schema &&
		a.table == b.table &&
		a.op == b.op &&
		a.keyType == b.keyType &&
		a.keyColumn == b.keyColumn &&
		sameStrings(a.payloadColumns, b.payloadColumns)
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func applyPreparedPushRowBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) == 1 {
		return applyPreparedPushRow(ctx, tx, ownerUserID, rows[0])
	}

	switch rows[0].op {
	case OpDelete:
		return applyPreparedDeleteBatch(ctx, tx, ownerUserID, rows)
	case OpInsert:
		return applyPreparedInsertBatch(ctx, tx, ownerUserID, rows)
	case OpUpdate:
		return applyPreparedUpdateBatch(ctx, tx, ownerUserID, rows)
	default:
		return &PushValidationError{Message: fmt.Sprintf("unsupported op %q", rows[0].op)}
	}
}

func applyPreparedDeleteBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	tableIdent := pgx.Identifier{rows[0].schema, rows[0].table}.Sanitize()
	keyColumnIdent := pgx.Identifier{rows[0].keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()

	stmt := fmt.Sprintf(`DELETE FROM %s WHERE %s = $1 AND %s = ANY($2)`, tableIdent, ownerColumnIdent, keyColumnIdent)
	if _, err := tx.Exec(ctx, stmt, ownerUserID, syncKeyArray(rows)); err != nil {
		return fmt.Errorf("delete batch %s.%s (%d rows): %w", rows[0].schema, rows[0].table, len(rows), err)
	}
	return nil
}

func applyPreparedInsertBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	tableIdent := pgx.Identifier{rows[0].schema, rows[0].table}.Sanitize()
	columnList := quotedInsertColumnList(rows[0].payloadColumns)
	payloadArray, err := marshalPayloadJSONArray(rows, ownerUserID)
	if err != nil {
		return fmt.Errorf("marshal insert batch payload for %s.%s: %w", rows[0].schema, rows[0].table, err)
	}

	stmt := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s
		FROM jsonb_populate_recordset(NULL::%s, $1::jsonb)
	`, tableIdent, columnList, columnList, tableIdent)
	if _, err := tx.Exec(ctx, stmt, payloadArray); err != nil {
		return fmt.Errorf("insert batch %s.%s (%d rows): %w", rows[0].schema, rows[0].table, len(rows), err)
	}
	return nil
}

func applyPreparedUpdateBatch(ctx context.Context, tx pgx.Tx, ownerUserID string, rows []pushPreparedRow) error {
	tableIdent := pgx.Identifier{rows[0].schema, rows[0].table}.Sanitize()
	keyColumnIdent := pgx.Identifier{rows[0].keyColumn}.Sanitize()
	ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
	payloadArray, err := marshalPayloadJSONArray(rows, "")
	if err != nil {
		return fmt.Errorf("marshal update batch payload for %s.%s: %w", rows[0].schema, rows[0].table, err)
	}

	setClauses := make([]string, 0, len(rows[0].payloadColumns))
	for _, col := range rows[0].payloadColumns {
		colIdent := pgx.Identifier{col}.Sanitize()
		setClauses = append(setClauses, fmt.Sprintf("%s = src.%s", colIdent, colIdent))
	}
	stmt := fmt.Sprintf(`
		UPDATE %s AS dst
		SET %s
		FROM jsonb_populate_recordset(NULL::%s, $1::jsonb) AS src
		WHERE dst.%s = src.%s
		  AND dst.%s = $2
	`, tableIdent, strings.Join(setClauses, ", "), tableIdent, keyColumnIdent, keyColumnIdent, ownerColumnIdent)
	if _, err := tx.Exec(ctx, stmt, payloadArray, ownerUserID); err != nil {
		return fmt.Errorf("update batch %s.%s (%d rows): %w", rows[0].schema, rows[0].table, len(rows), err)
	}
	return nil
}

func marshalPayloadJSONArray(rows []pushPreparedRow, ownerUserID string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, row := range rows {
		if i > 0 {
			buf.WriteByte(',')
		}
		payload := row.payload
		if ownerUserID != "" {
			var err error
			payload, err = injectOwnerUserIDPayload(row.payload, ownerUserID)
			if err != nil {
				return nil, err
			}
		}
		buf.Write(payload)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func quotedColumnList(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, col := range columns {
		quoted = append(quoted, pgx.Identifier{col}.Sanitize())
	}
	return strings.Join(quoted, ", ")
}

func quotedInsertColumnList(columns []string) string {
	insertColumns := make([]string, 0, len(columns)+1)
	insertColumns = append(insertColumns, syncScopeColumnName)
	insertColumns = append(insertColumns, columns...)
	return quotedColumnList(insertColumns)
}

func (s *SyncService) loadCommittedBundle(ctx context.Context, tx pgx.Tx, userID string, bundleSeq int64) (*Bundle, error) {
	var bundle Bundle
	if err := tx.QueryRow(ctx, `
		SELECT bundle_seq, source_id, source_bundle_id, row_count, bundle_hash
		FROM sync.bundle_log
		WHERE user_id = $1
		  AND bundle_seq = $2
	`, userID, bundleSeq).Scan(&bundle.BundleSeq, &bundle.SourceID, &bundle.SourceBundleID, &bundle.RowCount, &bundle.BundleHash); err != nil {
		return nil, fmt.Errorf("load bundle_log %d: %w", bundleSeq, err)
	}

	rows, err := tx.Query(ctx, `
		SELECT schema_name, table_name, key_json, op, row_version, payload
		FROM sync.bundle_rows
		WHERE user_id = $1
		  AND bundle_seq = $2
		ORDER BY row_ordinal
	`, userID, bundleSeq)
	if err != nil {
		return nil, fmt.Errorf("query bundle_rows %d: %w", bundleSeq, err)
	}
	defer rows.Close()

	for rows.Next() {
		var row BundleRow
		var keyJSON string
		if err := rows.Scan(&row.Schema, &row.Table, &keyJSON, &row.Op, &row.RowVersion, &row.Payload); err != nil {
			return nil, fmt.Errorf("scan bundle_rows %d: %w", bundleSeq, err)
		}
		row.Key, err = decodeSyncKeyJSON(keyJSON)
		if err != nil {
			return nil, fmt.Errorf("decode bundle row key %d: %w", bundleSeq, err)
		}
		row.Payload, err = s.canonicalizeWirePayload(row.Schema, row.Table, row.Payload)
		if err != nil {
			return nil, fmt.Errorf("canonicalize bundle row payload %d for %s.%s: %w", bundleSeq, row.Schema, row.Table, err)
		}
		bundle.Rows = append(bundle.Rows, row)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate bundle_rows %d: %w", bundleSeq, rows.Err())
	}
	return &bundle, nil
}
