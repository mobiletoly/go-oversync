// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

type committedPushBundle struct {
	BundleSeq            int64
	SourceID             string
	SourceBundleID       int64
	PlannedRowCount      int64
	RowCount             int64
	ByteCount            int64
	BundleHash           string
	CanonicalRequestHash string
	AlreadyCommitted     bool
}

const (
	maxCommittedPushBundleRows  int64 = 100000
	maxCommittedPushBundleBytes int64 = 64 << 20
)

type stagedPushBundleRow struct {
	SchemaName string
	TableName  string
	KeyJSON    string
	LocalPK    string
	WireKey    oversync.SyncKey
	Op         string
	RowVersion int64
	Payload    sql.NullString
}

// stageAuthoritativePushRows persists the single complete response before the
// existing atomic SQLite settlement applies it. There is no remote receipt or
// incremental committed-row continuation: an interrupted response is retried
// as the same complete source tuple.
func (c *Client) stageAuthoritativePushRows(ctx context.Context, committed *committedPushBundle, rows []oversync.PushRequestRow) error {
	return c.completeWork(ctx, "push response stage", func(ctx context.Context) error { return c.stageAuthoritativePushRowsSlice(ctx, committed, rows) })
}

func (c *Client) stageAuthoritativePushRowsSlice(ctx context.Context, committed *committedPushBundle, rows []oversync.PushRequestRow) error {
	if err := validateCommittedPushBundleHardLimits(committed); err != nil {
		return err
	}
	if int64(len(rows)) != committed.RowCount {
		return fmt.Errorf("authoritative response row count does not match committed descriptor")
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin authoritative push stage: %w", err)
	}
	defer tx.Rollback()
	if _, err := c.validateAuthoritativePushAgainstOutbox(ctx, tx, committed); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("clear authoritative push stage: %w", err)
	}
	for ordinal, row := range rows {
		if row.Schema != c.schemaForTable(row.Table) {
			return fmt.Errorf("authoritative push row schema does not match admitted contract")
		}
		keyJSON, _, err := c.bundleRowKeyToLocalKey(tx, row.Table, row.Key)
		if err != nil {
			return err
		}
		unit := applyUnit("push-stage", committed.BundleSeq, committed.BundleHash)
		charge, err := c.stageWorkspaceCharge(unit, int64(len(row.Payload)), row.Schema, row.Table, keyJSON, row.Op)
		if err != nil {
			return err
		}
		if err := c.checkAggregateWorkspace(ctx, tx, unit, nil, nil, charge); err != nil {
			return err
		}
		if err := c.reserveSyncWork(ctx, unit, 8); err != nil {
			return err
		}
		payload := sql.NullString{}
		if row.Op != oversync.OpDelete {
			payload = sql.NullString{String: string(row.Payload), Valid: true}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO _sync_push_stage(bundle_seq,row_ordinal,schema_name,table_name,key_json,op,row_version,payload)
			VALUES(?,?,?,?,?,?,?,?)
		`, committed.BundleSeq, ordinal, row.Schema, row.Table, keyJSON, row.Op, row.BaseRowVersion, nullStringValue(payload)); err != nil {
			return fmt.Errorf("stage authoritative push row %d: %w", ordinal, err)
		}
	}
	if _, count, complete, err := c.inspectReusablePushStageInTx(ctx, tx, committed); err != nil || !complete || count != committed.RowCount {
		if err != nil {
			return fmt.Errorf("validate authoritative push stage: %w", err)
		}
		return fmt.Errorf("authoritative push stage is incomplete")
	}
	return tx.Commit()
}

func (c *Client) inspectReusablePushStage(ctx context.Context, committed *committedPushBundle) (*int64, int64, bool, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, false, fmt.Errorf("failed to begin reusable push stage validation: %w", err)
	}
	defer tx.Rollback()
	after, count, complete, err := c.inspectReusablePushStageInTx(ctx, tx, committed)
	if err != nil {
		return nil, 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, false, fmt.Errorf("failed to commit reusable push stage validation: %w", err)
	}
	return after, count, complete, nil
}

func (c *Client) inspectReusablePushStageInTx(ctx context.Context, tx sqliteTransaction, committed *committedPushBundle) (*int64, int64, bool, error) {
	if err := validateCommittedPushBundleHardLimits(committed); err != nil {
		return nil, 0, false, err
	}
	verifier, err := c.loadCommittedBundleStageVerifierInTx(ctx, tx, committed)
	if err != nil {
		return nil, 0, false, err
	}
	return verifier.result()
}

type committedBundleStageVerifier struct {
	client      *Client
	committed   *committedPushBundle
	hasher      *protocolhash.CommittedBundleHasher
	seenEffects map[string]struct{}
	count       int64
}

func newCommittedBundleStageVerifier(client *Client, committed *committedPushBundle) *committedBundleStageVerifier {
	return &committedBundleStageVerifier{
		client: client, committed: committed,
		hasher: protocolhash.NewCommittedBundleHasher(), seenEffects: make(map[string]struct{}),
	}
}

func (v *committedBundleStageVerifier) add(row oversync.BundleRow, ordinal int64) error {
	if ordinal != v.count {
		return fmt.Errorf("push stage row ordinals are not contiguous")
	}
	if v.count >= v.committed.RowCount || v.count >= maxCommittedPushBundleRows {
		return fmt.Errorf("push stage contains more rows than the committed descriptor or protocol hard limit")
	}
	if err := v.client.validateBundleRowForBundle(&row, v.committed.BundleSeq); err != nil {
		return fmt.Errorf("push stage row %d is invalid: %w", ordinal, err)
	}
	canonicalKey, err := jcs.Marshal(row.Key)
	if err != nil {
		return fmt.Errorf("push stage row %d key is invalid: %w", ordinal, err)
	}
	effectID := row.Schema + "\x00" + row.Table + "\x00" + string(canonicalKey)
	if _, duplicate := v.seenEffects[effectID]; duplicate {
		return fmt.Errorf("push stage contains duplicate effect keys")
	}
	v.seenEffects[effectID] = struct{}{}
	if err := v.hasher.Add(protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload}); err != nil {
		return fmt.Errorf("push stage row %d cannot be hashed: %w", ordinal, err)
	}
	if v.hasher.CurrentByteCount() > v.committed.ByteCount || v.hasher.CurrentByteCount() > maxCommittedPushBundleBytes {
		return fmt.Errorf("push stage exceeds the committed byte descriptor or protocol hard limit")
	}
	v.count++
	return nil
}

func (v *committedBundleStageVerifier) result() (*int64, int64, bool, error) {
	if v.count == v.committed.RowCount && v.count > 0 {
		hash, byteCount, err := v.hasher.Sum()
		if err != nil {
			return nil, 0, false, fmt.Errorf("failed to finalize push stage hash: %w", err)
		}
		if hash != v.committed.BundleHash || byteCount != v.committed.ByteCount {
			return nil, 0, false, fmt.Errorf("complete push stage does not match the committed descriptor")
		}
		return nil, v.count, true, nil
	}
	if v.count == 0 {
		return nil, 0, false, nil
	}
	if v.hasher.CurrentByteCount() >= v.committed.ByteCount {
		return nil, 0, false, fmt.Errorf("partial push stage cannot fit the remaining committed byte descriptor")
	}
	after := v.count - 1
	return &after, v.count, false, nil
}

func (c *Client) loadCommittedBundleStageVerifierInTx(ctx context.Context, tx sqliteTransaction, committed *committedPushBundle) (*committedBundleStageVerifier, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		FROM _sync_push_stage
		ORDER BY bundle_seq, row_ordinal
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query reusable push stage: %w", err)
	}
	defer rows.Close()
	verifier := newCommittedBundleStageVerifier(c, committed)
	for rows.Next() {
		var bundleSeq, ordinal, rowVersion int64
		var schemaName, tableName, keyJSON, op string
		var payload sql.NullString
		if err := rows.Scan(&bundleSeq, &ordinal, &schemaName, &tableName, &keyJSON, &op, &rowVersion, &payload); err != nil {
			return nil, fmt.Errorf("failed to scan reusable push stage: %w", err)
		}
		if bundleSeq != committed.BundleSeq {
			return nil, fmt.Errorf("push stage contains rows for a different committed bundle")
		}
		_, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return nil, err
		}
		bundleRow := oversync.BundleRow{Schema: schemaName, Table: tableName, Key: wireKey, Op: op, RowVersion: rowVersion}
		if payload.Valid {
			bundleRow.Payload = json.RawMessage(payload.String)
		}
		if err := verifier.add(bundleRow, ordinal); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate reusable push stage: %w", err)
	}
	rows.Close()
	return verifier, nil
}

func (c *Client) validateAuthoritativePushAgainstOutbox(ctx context.Context, tx sqliteTransaction, committed *committedPushBundle) (*outboxBundleRecord, error) {
	if committed == nil {
		return nil, fmt.Errorf("committed push bundle metadata is required")
	}
	if err := validateCommittedPushBundleHardLimits(committed); err != nil {
		return nil, err
	}
	current, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, err
	}
	if current.State != outboxStatePrepared {
		return nil, fmt.Errorf("committed response requires a prepared outbox")
	}
	if current.SourceID != c.sourceID || current.SourceID != committed.SourceID || current.SourceBundleID != committed.SourceBundleID {
		return nil, fmt.Errorf("committed receipt source tuple does not match frozen outbox")
	}
	if current.RowCount != committed.PlannedRowCount || current.RowCount <= 0 {
		return nil, fmt.Errorf("committed planned_row_count does not match frozen outbox")
	}
	if current.CanonicalRequestHash != committed.CanonicalRequestHash {
		return nil, fmt.Errorf("committed canonical_request_hash does not match frozen outbox")
	}
	if committed.BundleSeq <= 0 || committed.RowCount <= 0 || committed.ByteCount <= 2 || !protocolhash.IsCanonicalHash(committed.BundleHash) {
		return nil, fmt.Errorf("committed receipt descriptor is invalid")
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT row_ordinal, schema_name, table_name, wire_key_json, op, base_row_version, wire_payload
		FROM _sync_outbox_rows
		WHERE source_bundle_id = ?
		ORDER BY row_ordinal
	`, current.SourceBundleID)
	if err != nil {
		return nil, fmt.Errorf("failed to query frozen outbox receipt rows: %w", err)
	}
	defer rows.Close()
	hasher := protocolhash.NewGenerationPinnedPushRequestHasher(strconv.FormatInt(int64(c.schemaGenerationProof.ID), 10), c.schemaGenerationProof.Fingerprint)
	var count int64
	for rows.Next() {
		var ordinal, baseRowVersion int64
		var schemaName, tableName, wireKeyJSON, op string
		var wirePayload sql.NullString
		if err := rows.Scan(&ordinal, &schemaName, &tableName, &wireKeyJSON, &op, &baseRowVersion, &wirePayload); err != nil {
			return nil, fmt.Errorf("failed to scan frozen outbox receipt row: %w", err)
		}
		if ordinal != count {
			return nil, fmt.Errorf("frozen outbox row ordinals are not contiguous")
		}
		key, payload, err := c.validateFrozenPushRequestRow(tx, schemaName, tableName, wireKeyJSON, op, baseRowVersion, wirePayload)
		if err != nil {
			return nil, fmt.Errorf("frozen outbox row %d is invalid: %w", ordinal, err)
		}
		if err := hasher.Add(protocolhash.PushRow{Schema: schemaName, Table: tableName, Key: key, Op: op, BaseRowVersion: baseRowVersion, Payload: payload}); err != nil {
			return nil, fmt.Errorf("failed to reconstruct frozen outbox request hash: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate frozen outbox receipt rows: %w", err)
	}
	if count != current.RowCount {
		return nil, fmt.Errorf("frozen outbox row count does not match planned_row_count")
	}
	hash, _, err := hasher.Sum()
	if err != nil {
		return nil, fmt.Errorf("failed to finalize frozen outbox request hash: %w", err)
	}
	if hash != current.CanonicalRequestHash {
		return nil, fmt.Errorf("frozen outbox request hash does not match durable Q39")
	}
	return current, nil
}

func (c *Client) validateFrozenPushRequestRow(tx tableInfoQueryer, schemaName, tableName, wireKeyJSON, op string, baseRowVersion int64, wirePayload sql.NullString) (oversync.SyncKey, []byte, error) {
	if schemaName != c.schemaForTable(tableName) {
		return nil, nil, fmt.Errorf("schema does not match the fixed client generation")
	}
	if baseRowVersion < 0 {
		return nil, nil, fmt.Errorf("base_row_version must be non-negative")
	}
	keyObject, err := jcs.DecodeObjectLossless([]byte(wireKeyJSON))
	if err != nil || len(keyObject) != 1 {
		return nil, nil, fmt.Errorf("wire key must be an exact single-key object")
	}
	_, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, wireKeyJSON)
	if err != nil {
		return nil, nil, err
	}
	switch op {
	case oversync.OpDelete:
		if wirePayload.Valid {
			return nil, nil, fmt.Errorf("DELETE wire payload must be NULL")
		}
		return wireKey, nil, nil
	case oversync.OpInsert, oversync.OpUpdate:
		if !wirePayload.Valid {
			return nil, nil, fmt.Errorf("%s wire payload must be present", op)
		}
	default:
		return nil, nil, fmt.Errorf("unsupported operation %q", op)
	}
	payload := []byte(wirePayload.String)
	payloadObject, err := jcs.DecodeObjectLossless(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("wire payload must be an exact JSON object: %w", err)
	}
	keyColumns, err := c.syncKeyColumnsForTable(tableName)
	if err != nil || len(keyColumns) != 1 {
		return nil, nil, fmt.Errorf("table must declare one sync key")
	}
	keyName := strings.ToLower(keyColumns[0])
	payloadKey, ok := payloadObject[keyName]
	if !ok {
		payloadKey, ok = payloadObject[keyColumns[0]]
	}
	wireKeyValue, keyOK := wireKey[keyName]
	keyCanonical, keyErr := jcs.Marshal(wireKeyValue)
	payloadCanonical, payloadErr := jcs.Marshal(payloadKey)
	if !ok || !keyOK || keyErr != nil || payloadErr != nil || string(keyCanonical) != string(payloadCanonical) {
		return nil, nil, fmt.Errorf("wire payload key does not match wire key")
	}
	return wireKey, payload, nil
}

func (c *Client) clearPushStage(ctx context.Context) error {
	if _, err := c.db.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return fmt.Errorf("failed to clear push staging: %w", err)
	}
	return nil
}

func (c *Client) computeStagedPushBundleHash(ctx context.Context, bundleSeq int64) (string, int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, fmt.Errorf("failed to begin staged bundle hash transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT row_ordinal, schema_name, table_name, key_json, op, row_version, payload
		FROM _sync_push_stage
		WHERE bundle_seq = ?
		ORDER BY row_ordinal
	`, bundleSeq)
	if err != nil {
		return "", 0, fmt.Errorf("failed to query staged bundle rows for hash: %w", err)
	}
	defer rows.Close()

	logicalRows := make([]protocolhash.BundleRow, 0)
	for rows.Next() {
		var (
			rowOrdinal int64
			schemaName string
			tableName  string
			keyJSON    string
			op         string
			rowVersion int64
			payload    sql.NullString
		)
		if err := rows.Scan(&rowOrdinal, &schemaName, &tableName, &keyJSON, &op, &rowVersion, &payload); err != nil {
			return "", 0, fmt.Errorf("failed to scan staged bundle row for hash: %w", err)
		}

		_, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return "", 0, err
		}
		logicalRows = append(logicalRows, protocolhash.BundleRow{
			Schema: schemaName, Table: tableName, Key: wireKey, Op: op,
			RowVersion: rowVersion,
			Payload: func() []byte {
				if payload.Valid {
					return []byte(payload.String)
				}
				return nil
			}(),
		})
	}
	if err := rows.Err(); err != nil {
		return "", 0, fmt.Errorf("failed to iterate staged bundle rows for hash: %w", err)
	}

	hash, byteCount, err := protocolhash.CommittedBundle(logicalRows)
	if err != nil {
		return "", 0, fmt.Errorf("failed to canonicalize staged logical bundle rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", 0, fmt.Errorf("failed to commit staged bundle hash transaction: %w", err)
	}
	return hash, byteCount, nil
}
