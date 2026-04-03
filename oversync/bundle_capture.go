// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
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
	ordinal  int64
	userPK   int64
	tableID  int32
	opCode   int16
	keyBytes []byte
	payload  []byte
}

type normalizedBundleRow struct {
	firstOrdinal int64
	tableID      int32
	keyBytes     []byte
	opCode       int16
	payloadDB    []byte
}

type committedBundleStorageRow struct {
	tableID     int32
	keyBytes    []byte
	opCode      int16
	payloadWire []byte
}

type bundleAccumulator struct {
	firstOrdinal int64
	firstOpCode  int16
	lastOpCode   int16
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
			info, ok := s.registeredTableInfo[table.normalizedKey()]
			if !ok {
				return fmt.Errorf("registered table %s is missing runtime metadata for trigger installation", table.normalizedKey())
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
				`CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION sync.capture_registered_row_change(%s, %s, %s)`,
				registeredTableCaptureTriggerName,
				tableIdent,
				quoteSQLLiteral(keyColumns[0]),
				quoteSQLLiteral(strconv.Itoa(int(info.syncKeyKind))),
				quoteSQLLiteral(strconv.Itoa(int(info.tableID))),
			)
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("create capture trigger for %s: %w", table.normalizedKey(), err)
			}
		}
		return nil
	})
}

func reserveUserBundleSeq(ctx context.Context, tx pgx.Tx, userPK int64) (int64, error) {
	var bundleSeq int64
	if err := tx.QueryRow(ctx, `
		UPDATE sync.user_state
		SET next_bundle_seq = next_bundle_seq + 1
		WHERE user_pk = $1
		RETURNING next_bundle_seq - 1
	`, userPK).Scan(&bundleSeq); err != nil {
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
		expectedSourceBundleID, maxCommittedSourceBundleID, err := loadNextExpectedSourceBundleIDForUpdate(ctx, tx, scopeState.UserPK, actor.UserID, source.SourceID)
		if err != nil {
			return err
		}
		switch {
		case source.SourceBundleID < expectedSourceBundleID:
			return &SourceTupleHistoryPrunedError{
				UserID:                         actor.UserID,
				SourceID:                       source.SourceID,
				SourceBundleID:                 source.SourceBundleID,
				MaxCommittedSourceBundleIDHint: maxCommittedSourceBundleID,
			}
		case source.SourceBundleID > expectedSourceBundleID:
			return &SourceSequenceOutOfOrderError{
				UserID:   actor.UserID,
				SourceID: source.SourceID,
				Expected: expectedSourceBundleID,
				Actual:   source.SourceBundleID,
			}
		}
		if err := setBundleTxContext(ctx, tx, bundleTxContext{
			UserID:         actor.UserID,
			UserPK:         scopeState.UserPK,
			SourceID:       source.SourceID,
			SourceBundleID: source.SourceBundleID,
		}); err != nil {
			return err
		}

		if err := fn(tx); err != nil {
			return err
		}
		bundle, err := s.finalizeCapturedBundle(ctx, tx, actor, scopeState.UserPK, source)
		if err != nil {
			return err
		}
		if bundle != nil {
			if err := activateSourceState(ctx, tx, scopeState.UserPK, actor.UserID, source.SourceID, source.SourceBundleID); err != nil {
				return err
			}
			if err := s.applyRetentionPolicyForUser(ctx, tx, scopeState.UserPK); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SyncService) finalizeCapturedBundle(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, source BundleSource) (*Bundle, error) {
	var txid int64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&txid); err != nil {
		return nil, fmt.Errorf("read current txid for bundle capture: %w", err)
	}

	events, err := loadCapturedBundleEvents(ctx, tx, txid, userPK)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}

	rows := normalizeCapturedBundleEvents(events)
	if len(rows) == 0 {
		if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_capture_stage WHERE txid = $1 AND user_pk = $2`, txid, userPK); err != nil {
			return nil, fmt.Errorf("clear empty captured bundle stage rows: %w", err)
		}
		return nil, nil
	}

	bundleSeq, err := reserveUserBundleSeq(ctx, tx, userPK)
	if err != nil {
		return nil, err
	}

	storageRows := make([]committedBundleStorageRow, 0, len(rows))
	bundleRows := make([]BundleRow, 0, len(rows))
	for _, row := range rows {
		info, err := s.tableInfoForID(row.tableID)
		if err != nil {
			return nil, err
		}
		key, err := wireSyncKeyFromBytes(info, row.keyBytes)
		if err != nil {
			return nil, fmt.Errorf("decode bundle row key for table_id %d: %w", row.tableID, err)
		}
		op, err := opStringFromCode(row.opCode)
		if err != nil {
			return nil, err
		}
		bundleRow := BundleRow{
			Schema:     info.schemaName,
			Table:      info.tableName,
			Key:        key,
			Op:         op,
			RowVersion: bundleSeq,
		}
		var payloadWire []byte
		if row.opCode != opCodeDelete {
			payloadWire, err = s.canonicalizeWirePayload(info.schemaName, info.tableName, row.payloadDB)
			if err != nil {
				return nil, fmt.Errorf("canonicalize bundle row payload for %s.%s: %w", info.schemaName, info.tableName, err)
			}
			bundleRow.Payload = payloadWire
		}
		storageRows = append(storageRows, committedBundleStorageRow{
			tableID:     row.tableID,
			keyBytes:    append([]byte(nil), row.keyBytes...),
			opCode:      row.opCode,
			payloadWire: append([]byte(nil), payloadWire...),
		})
		bundleRows = append(bundleRows, bundleRow)
	}

	bundleHash, byteCount, err := computeCommittedBundleHash(bundleRows)
	if err != nil {
		return nil, fmt.Errorf("compute committed bundle hash: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_pk, bundle_seq, source_id, source_bundle_id, row_count, byte_count, bundle_hash, committed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
	`, userPK, bundleSeq, source.SourceID, source.SourceBundleID, len(bundleRows), byteCount, bundleHash); err != nil {
		return nil, fmt.Errorf("insert bundle_log row: %w", err)
	}

	if err := persistCommittedBundleRows(ctx, tx, userPK, bundleSeq, storageRows); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_capture_stage WHERE txid = $1 AND user_pk = $2`, txid, userPK); err != nil {
		return nil, fmt.Errorf("delete captured bundle stage rows: %w", err)
	}
	return &Bundle{
		BundleSeq:      bundleSeq,
		SourceID:       source.SourceID,
		SourceBundleID: source.SourceBundleID,
		RowCount:       int64(len(bundleRows)),
		BundleHash:     renderBundleHash(bundleHash),
		Rows:           bundleRows,
	}, nil
}

func renderBundleHash(bundleHash []byte) string {
	return hex.EncodeToString(bundleHash)
}

func computeCommittedBundleHash(rows []BundleRow) ([]byte, int64, error) {
	logicalRows := make([]map[string]any, 0, len(rows))
	for i, row := range rows {
		payloadValue := any(nil)
		if row.Op != OpDelete && len(row.Payload) > 0 {
			if err := json.Unmarshal(row.Payload, &payloadValue); err != nil {
				return nil, 0, fmt.Errorf("decode payload for %s.%s row %d: %w", row.Schema, row.Table, i, err)
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
		return nil, 0, fmt.Errorf("marshal logical bundle rows: %w", err)
	}
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return nil, 0, fmt.Errorf("canonicalize logical bundle rows: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return sum[:], int64(len(canonical)), nil
}

func loadCapturedBundleEvents(ctx context.Context, tx pgx.Tx, txid int64, userPK int64) ([]capturedBundleEvent, error) {
	rows, err := tx.Query(ctx, `
		SELECT capture_ordinal, user_pk, table_id, op_code, key_bytes, payload_db
		FROM sync.bundle_capture_stage
		WHERE txid = $1
		  AND user_pk = $2
		ORDER BY capture_ordinal
	`, txid, userPK)
	if err != nil {
		return nil, fmt.Errorf("query captured bundle stage rows: %w", err)
	}
	defer rows.Close()

	events := make([]capturedBundleEvent, 0)
	for rows.Next() {
		var event capturedBundleEvent
		var payload []byte
		if err := rows.Scan(&event.ordinal, &event.userPK, &event.tableID, &event.opCode, &event.keyBytes, &payload); err != nil {
			return nil, fmt.Errorf("scan captured bundle stage row: %w", err)
		}
		if payload != nil {
			canonicalPayload, err := canonicalJSON(payload)
			if err != nil {
				return nil, fmt.Errorf("canonicalize captured payload for table_id %d: %w", event.tableID, err)
			}
			event.payload = append([]byte(nil), canonicalPayload...)
		}
		event.keyBytes = append([]byte(nil), event.keyBytes...)
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
		key := string(appendInt32BigEndian(append([]byte(nil), event.keyBytes...), event.tableID))
		acc := accumulators[key]
		if acc == nil {
			acc = &bundleAccumulator{
				firstOrdinal: event.ordinal,
				firstOpCode:  event.opCode,
			}
			accumulators[key] = acc
		}
		acc.lastOpCode = event.opCode
		if event.payload != nil {
			acc.lastPayload = append(acc.lastPayload[:0], event.payload...)
		} else {
			acc.lastPayload = nil
		}
	}

	rows := make([]normalizedBundleRow, 0, len(accumulators))
	for _, event := range events {
		key := string(appendInt32BigEndian(append([]byte(nil), event.keyBytes...), event.tableID))
		acc, ok := accumulators[key]
		if !ok {
			continue
		}
		delete(accumulators, key)

		row := normalizedBundleRow{
			firstOrdinal: acc.firstOrdinal,
			tableID:      event.tableID,
			keyBytes:     append([]byte(nil), event.keyBytes...),
		}

		switch acc.lastOpCode {
		case opCodeDelete:
			if acc.firstOpCode == opCodeInsert {
				continue
			}
			row.opCode = opCodeDelete
		default:
			if acc.firstOpCode == opCodeInsert {
				row.opCode = opCodeInsert
			} else {
				row.opCode = opCodeUpdate
			}
			row.payloadDB = append([]byte(nil), acc.lastPayload...)
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].firstOrdinal == rows[j].firstOrdinal {
			if rows[i].tableID == rows[j].tableID {
				return bytes.Compare(rows[i].keyBytes, rows[j].keyBytes) < 0
			}
			return rows[i].tableID < rows[j].tableID
		}
		return rows[i].firstOrdinal < rows[j].firstOrdinal
	})
	return rows
}

func persistCommittedBundleRows(ctx context.Context, tx pgx.Tx, userPK, bundleSeq int64, rows []committedBundleStorageRow) error {
	rowOrdinals := make([]int64, len(rows))
	tableIDs := make([]int32, len(rows))
	keyBytes := make([][]byte, len(rows))
	opCodes := make([]int16, len(rows))
	hasPayload := make([]bool, len(rows))
	payloadTexts := make([]string, len(rows))
	deletedFlags := make([]bool, len(rows))

	for i, row := range rows {
		rowOrdinals[i] = int64(i + 1)
		tableIDs[i] = row.tableID
		keyBytes[i] = append([]byte(nil), row.keyBytes...)
		opCodes[i] = row.opCode
		deletedFlags[i] = row.opCode == opCodeDelete
		if row.opCode != opCodeDelete {
			hasPayload[i] = true
			payloadTexts[i] = string(row.payloadWire)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_rows (
			user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire
		)
		SELECT
			$1,
			$2,
			rows.row_ordinal,
			rows.table_id,
			rows.key_bytes,
			rows.op_code,
			CASE WHEN rows.has_payload THEN rows.payload_text::json ELSE NULL END
		FROM unnest(
			$3::int8[],
			$4::int4[],
			$5::bytea[],
			$6::int2[],
			$7::bool[],
			$8::text[]
		) AS rows(row_ordinal, table_id, key_bytes, op_code, has_payload, payload_text)
	`, userPK, bundleSeq, rowOrdinals, tableIDs, keyBytes, opCodes, hasPayload, payloadTexts); err != nil {
		return fmt.Errorf("bulk insert bundle_rows: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.row_state (
			user_pk, table_id, key_bytes, bundle_seq, deleted
		)
		SELECT
			$1,
			rows.table_id,
			rows.key_bytes,
			$2,
			rows.deleted
		FROM unnest(
			$3::int4[],
			$4::bytea[],
			$5::bool[]
		) AS rows(table_id, key_bytes, deleted)
		ON CONFLICT (user_pk, table_id, key_bytes) DO UPDATE
		SET bundle_seq = EXCLUDED.bundle_seq,
			deleted = EXCLUDED.deleted
	`, userPK, bundleSeq, tableIDs, keyBytes, deletedFlags); err != nil {
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
