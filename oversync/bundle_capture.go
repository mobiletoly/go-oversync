// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const (
	registeredTableCaptureTriggerName = "oversync_bundle_capture_row"
	registeredTableOwnerGuardTrigger  = "oversync_bundle_owner_guard"
)

// BundleSource identifies one server-side committed bundle source.
type BundleSource struct {
	SourceID       string
	SourceBundleID int64
}

type capturedBundleEvent struct {
	ordinal    int64
	schemaName string
	tableName  string
	op         string
	keyJSON    string
	payload    []byte
}

type normalizedBundleRow struct {
	firstOrdinal int64
	schemaName   string
	tableName    string
	keyJSON      string
	op           string
	payload      []byte
}

type bundleAccumulator struct {
	firstOrdinal int64
	firstOp      string
	lastOp       string
	lastPayload  []byte
}

func quoteSQLLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func (s *SyncService) installRegisteredTableCaptureTriggers(ctx context.Context) error {
	if s == nil || s.pool == nil || s.config == nil || len(s.config.RegisteredTables) == 0 {
		return nil
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, syncBootstrapLockKey); err != nil {
			return fmt.Errorf("acquire sync bootstrap lock for capture triggers: %w", err)
		}
		for _, table := range s.config.RegisteredTables {
			keyColumns := table.normalizedSyncKeyColumns()
			if len(keyColumns) != 1 {
				return fmt.Errorf("registered table %s requires exactly one sync key column to install capture trigger", table.normalizedKey())
			}

			tableIdent := pgx.Identifier{table.normalizedSchema(), table.normalizedTable()}.Sanitize()
			stmt := fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, registeredTableCaptureTriggerName, tableIdent)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("drop capture trigger for %s: %w", table.normalizedKey(), err)
			}
			stmt = fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, registeredTableOwnerGuardTrigger, tableIdent)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("drop owner guard trigger for %s: %w", table.normalizedKey(), err)
			}

			stmt = fmt.Sprintf(
				`CREATE TRIGGER %s BEFORE INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.enforce_registered_row_owner()`,
				registeredTableOwnerGuardTrigger,
				tableIdent,
			)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("create owner guard trigger for %s: %w", table.normalizedKey(), err)
			}

			stmt = fmt.Sprintf(
				`CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.capture_registered_row_change(%s)`,
				registeredTableCaptureTriggerName,
				tableIdent,
				quoteSQLLiteral(keyColumns[0]),
			)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("create capture trigger for %s: %w", table.normalizedKey(), err)
			}
		}
		return nil
	})
}

func reserveUserBundleSeq(ctx context.Context, tx pgx.Tx, userID string) (int64, error) {
	var bundleSeq int64
	if err := tx.QueryRow(ctx, `
		UPDATE sync.user_state
		SET next_bundle_seq = next_bundle_seq + 1,
			updated_at = now()
		WHERE user_id = $1
		RETURNING next_bundle_seq - 1
	`, userID).Scan(&bundleSeq); err != nil {
		return 0, fmt.Errorf("reserve user bundle_seq: %w", err)
	}
	return bundleSeq, nil
}

func (s *SyncService) WithinSyncBundle(
	ctx context.Context,
	actor Actor,
	source BundleSource,
	fn func(tx pgx.Tx) error,
) (err error) {
	if err := actor.validate(false); err != nil {
		return err
	}
	if strings.TrimSpace(source.SourceID) == "" {
		return fmt.Errorf("bundle source_id is required")
	}
	if source.SourceBundleID <= 0 {
		return fmt.Errorf("bundle source_bundle_id must be > 0")
	}
	if fn == nil {
		return fmt.Errorf("bundle callback is required")
	}

	done, err := s.beginOperation()
	if err != nil {
		return err
	}
	defer done()

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return err
	}
	defer releaseConn()

	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if err := ensureScopeStateExistsWithExec(ctx, tx, actor.UserID); err != nil {
			return err
		}
		scopeState, err := loadScopeStateForUpdate(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}
		scopeState, err = expireInitializationLeaseIfNeeded(ctx, tx, scopeState)
		if err != nil {
			return err
		}
		switch scopeState.State {
		case scopeStateInitialized:
		case scopeStateInitializing:
			return &ScopeInitializingError{UserID: actor.UserID, LeaseExpiresAt: scopeState.LeaseExpiresAt}
		default:
			return &ScopeUninitializedError{UserID: actor.UserID}
		}
		if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
			return fmt.Errorf("defer bundle constraints: %w", err)
		}
		if err := ensureUserStatePresent(ctx, tx, actor.UserID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('oversync.bundle_user_id', $1, true)`, actor.UserID); err != nil {
			return fmt.Errorf("set bundle user_id: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('oversync.bundle_source_id', $1, true)`, source.SourceID); err != nil {
			return fmt.Errorf("set bundle source_id: %w", err)
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('oversync.bundle_source_bundle_id', $1, true)`, strconv.FormatInt(source.SourceBundleID, 10)); err != nil {
			return fmt.Errorf("set bundle source_bundle_id: %w", err)
		}

		if err := fn(tx); err != nil {
			return err
		}
		if _, err := s.finalizeCapturedBundle(ctx, tx, actor, source); err != nil {
			return err
		}
		return nil
	})
}

func (s *SyncService) finalizeCapturedBundle(ctx context.Context, tx pgx.Tx, actor Actor, source BundleSource) (*Bundle, error) {
	var txid int64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txid); err != nil {
		return nil, fmt.Errorf("read current txid for bundle capture: %w", err)
	}

	events, err := loadCapturedBundleEvents(ctx, tx, txid, actor.UserID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}

	rows := normalizeCapturedBundleEvents(events)
	if len(rows) == 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_capture_stage WHERE txid = $1 AND user_id = $2`, txid, actor.UserID); err != nil {
			return nil, fmt.Errorf("clear empty captured bundle stage rows: %w", err)
		}
		return nil, nil
	}

	bundleSeq, err := reserveUserBundleSeq(ctx, tx, actor.UserID)
	if err != nil {
		return nil, err
	}

	rowCount := len(rows)
	var byteCount int64
	for _, row := range rows {
		byteCount += int64(len(row.schemaName) + len(row.tableName) + len(row.keyJSON) + len(row.op) + len(row.payload))
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_id, bundle_seq, source_id, source_bundle_id, row_count, byte_count, committed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, now())
	`, actor.UserID, bundleSeq, source.SourceID, source.SourceBundleID, rowCount, byteCount); err != nil {
		return nil, fmt.Errorf("insert bundle_log row: %w", err)
	}

	bundleRows := make([]BundleRow, 0, len(rows))
	for _, row := range rows {
		key, err := decodeSyncKeyJSON(row.keyJSON)
		if err != nil {
			return nil, fmt.Errorf("decode bundle row key: %w", err)
		}
		bundleRow := BundleRow{
			Schema:     row.schemaName,
			Table:      row.tableName,
			Key:        key,
			Op:         row.op,
			RowVersion: bundleSeq,
		}
		if row.op != OpDelete {
			bundleRow.Payload, err = s.canonicalizeWirePayload(row.schemaName, row.tableName, row.payload)
			if err != nil {
				return nil, fmt.Errorf("canonicalize bundle row payload for %s.%s: %w", row.schemaName, row.tableName, err)
			}
		}
		bundleRows = append(bundleRows, bundleRow)
	}

	bundleHash, err := computeCommittedBundleHash(bundleRows)
	if err != nil {
		return nil, fmt.Errorf("compute committed bundle hash: %w", err)
	}

	if err := persistNormalizedBundleRows(ctx, tx, actor.UserID, bundleSeq, rows); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sync.bundle_log
		SET bundle_hash = $3
		WHERE user_id = $1 AND bundle_seq = $2
	`, actor.UserID, bundleSeq, bundleHash); err != nil {
		return nil, fmt.Errorf("persist bundle_log hash: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_capture_stage WHERE txid = $1 AND user_id = $2`, txid, actor.UserID); err != nil {
		return nil, fmt.Errorf("delete captured bundle stage rows: %w", err)
	}
	return &Bundle{
		BundleSeq:      bundleSeq,
		SourceID:       source.SourceID,
		SourceBundleID: source.SourceBundleID,
		RowCount:       int64(len(bundleRows)),
		BundleHash:     bundleHash,
		Rows:           bundleRows,
	}, nil
}

func computeCommittedBundleHash(rows []BundleRow) (string, error) {
	logicalRows := make([]map[string]any, 0, len(rows))
	for i, row := range rows {
		payloadValue := any(nil)
		if row.Op != OpDelete && len(row.Payload) > 0 {
			if err := json.Unmarshal(row.Payload, &payloadValue); err != nil {
				return "", fmt.Errorf("decode payload for %s.%s row %d: %w", row.Schema, row.Table, i, err)
			}
		}
		logicalRows = append(logicalRows, map[string]any{
			"row_ordinal": i,
			"schema":      row.Schema,
			"table":       row.Table,
			"key":         row.Key,
			"op":          row.Op,
			"row_version": row.RowVersion,
			"payload":     payloadValue,
		})
	}
	raw, err := json.Marshal(logicalRows)
	if err != nil {
		return "", fmt.Errorf("marshal logical bundle rows: %w", err)
	}
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return "", fmt.Errorf("canonicalize logical bundle rows: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func loadCapturedBundleEvents(ctx context.Context, tx pgx.Tx, txid int64, userID string) ([]capturedBundleEvent, error) {
	rows, err := tx.Query(ctx, `
		SELECT capture_id, schema_name, table_name, op, key_json, payload
		FROM sync.bundle_capture_stage
		WHERE txid = $1
		  AND user_id = $2
		ORDER BY capture_id
	`, txid, userID)
	if err != nil {
		return nil, fmt.Errorf("query captured bundle stage rows: %w", err)
	}
	defer rows.Close()

	events := make([]capturedBundleEvent, 0)
	for rows.Next() {
		var event capturedBundleEvent
		var payload []byte
		if err := rows.Scan(&event.ordinal, &event.schemaName, &event.tableName, &event.op, &event.keyJSON, &payload); err != nil {
			return nil, fmt.Errorf("scan captured bundle stage row: %w", err)
		}
		keyJSON, err := canonicalJSON([]byte(event.keyJSON))
		if err != nil {
			return nil, fmt.Errorf("canonicalize captured key_json for %s.%s: %w", event.schemaName, event.tableName, err)
		}
		event.keyJSON = string(keyJSON)
		if payload != nil {
			canonicalPayload, err := canonicalJSON(payload)
			if err != nil {
				return nil, fmt.Errorf("canonicalize captured payload for %s.%s: %w", event.schemaName, event.tableName, err)
			}
			event.payload = append([]byte(nil), canonicalPayload...)
		}
		events = append(events, event)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("iterate captured bundle stage rows: %w", rows.Err())
	}
	return events, nil
}

func normalizeCapturedBundleEvents(events []capturedBundleEvent) []normalizedBundleRow {
	accumulators := make(map[string]*bundleAccumulator, len(events))
	for _, event := range events {
		key := event.schemaName + "\x00" + event.tableName + "\x00" + event.keyJSON
		acc := accumulators[key]
		if acc == nil {
			acc = &bundleAccumulator{
				firstOrdinal: event.ordinal,
				firstOp:      event.op,
			}
			accumulators[key] = acc
		}
		acc.lastOp = event.op
		if event.payload != nil {
			acc.lastPayload = append(acc.lastPayload[:0], event.payload...)
		} else {
			acc.lastPayload = nil
		}
	}

	rows := make([]normalizedBundleRow, 0, len(accumulators))
	for key, acc := range accumulators {
		parts := strings.SplitN(key, "\x00", 3)
		if len(parts) != 3 {
			continue
		}

		row := normalizedBundleRow{
			firstOrdinal: acc.firstOrdinal,
			schemaName:   parts[0],
			tableName:    parts[1],
			keyJSON:      parts[2],
		}

		switch acc.lastOp {
		case OpDelete:
			if acc.firstOp == OpInsert {
				continue
			}
			row.op = OpDelete
		default:
			if acc.firstOp == OpInsert {
				row.op = OpInsert
			} else {
				row.op = OpUpdate
			}
			row.payload = append([]byte(nil), acc.lastPayload...)
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].firstOrdinal == rows[j].firstOrdinal {
			if rows[i].schemaName == rows[j].schemaName {
				if rows[i].tableName == rows[j].tableName {
					return rows[i].keyJSON < rows[j].keyJSON
				}
				return rows[i].tableName < rows[j].tableName
			}
			return rows[i].schemaName < rows[j].schemaName
		}
		return rows[i].firstOrdinal < rows[j].firstOrdinal
	})
	return rows
}

func persistNormalizedBundleRows(ctx context.Context, tx pgx.Tx, userID string, bundleSeq int64, rows []normalizedBundleRow) error {
	rowOrdinals := make([]int32, len(rows))
	schemaNames := make([]string, len(rows))
	tableNames := make([]string, len(rows))
	keyJSONs := make([]string, len(rows))
	ops := make([]string, len(rows))
	hasPayload := make([]bool, len(rows))
	payloadTexts := make([]string, len(rows))
	deletedFlags := make([]bool, len(rows))
	hasPayloadHash := make([]bool, len(rows))
	payloadHashHex := make([]string, len(rows))

	for i, row := range rows {
		rowOrdinals[i] = int32(i + 1)
		schemaNames[i] = row.schemaName
		tableNames[i] = row.tableName
		keyJSONs[i] = row.keyJSON
		ops[i] = row.op
		deletedFlags[i] = row.op == OpDelete

		if row.op != OpDelete {
			hasPayload[i] = true
			payloadTexts[i] = string(row.payload)

			sum := sha256.Sum256(row.payload)
			hasPayloadHash[i] = true
			payloadHashHex[i] = fmt.Sprintf("%x", sum[:])
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_rows (
			user_id, bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		)
		SELECT
			$1,
			$2,
			rows.row_ordinal,
			rows.schema_name,
			rows.table_name,
			rows.key_json,
			rows.op,
			$2,
			CASE WHEN rows.has_payload THEN rows.payload_text::jsonb ELSE NULL END
		FROM unnest(
			$3::int4[],
			$4::text[],
			$5::text[],
			$6::text[],
			$7::text[],
			$8::bool[],
			$9::text[]
		) AS rows(row_ordinal, schema_name, table_name, key_json, op, has_payload, payload_text)
	`, userID, bundleSeq, rowOrdinals, schemaNames, tableNames, keyJSONs, ops, hasPayload, payloadTexts); err != nil {
		return fmt.Errorf("bulk insert bundle_rows: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.row_state (
			user_id, schema_name, table_name, key_json, row_version, deleted, bundle_seq, payload_hash, updated_at
		)
		SELECT
			$1,
			rows.schema_name,
			rows.table_name,
			rows.key_json,
			$2,
			rows.deleted,
			$2,
			CASE WHEN rows.has_payload_hash THEN decode(rows.payload_hash_hex, 'hex') ELSE NULL END,
			now()
		FROM unnest(
			$3::text[],
			$4::text[],
			$5::text[],
			$6::bool[],
			$7::bool[],
			$8::text[]
		) AS rows(schema_name, table_name, key_json, deleted, has_payload_hash, payload_hash_hex)
		ON CONFLICT (user_id, schema_name, table_name, key_json) DO UPDATE
		SET row_version = EXCLUDED.row_version,
			deleted = EXCLUDED.deleted,
			bundle_seq = EXCLUDED.bundle_seq,
			payload_hash = EXCLUDED.payload_hash,
			updated_at = now()
	`, userID, bundleSeq, schemaNames, tableNames, keyJSONs, deletedFlags, hasPayloadHash, payloadHashHex); err != nil {
		return fmt.Errorf("bulk upsert row_state: %w", err)
	}

	return nil
}

func decodeSyncKeyJSON(raw string) (SyncKey, error) {
	var key SyncKey
	if err := json.Unmarshal([]byte(raw), &key); err != nil {
		return nil, err
	}
	return key, nil
}
