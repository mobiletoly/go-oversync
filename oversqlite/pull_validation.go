// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	maxPullResponseEnvelopes    = 5000
	maxPullResponseLogicalBytes = int64(128 << 20)
)

func validatePullResponseLogicalLimits(bundles []oversync.Bundle) error {
	if len(bundles) > maxPullResponseEnvelopes {
		return fmt.Errorf("pull response envelope count exceeds the protocol hard limit %d", maxPullResponseEnvelopes)
	}
	var aggregateBytes int64
	for index := range bundles {
		if err := validatePullBundleLogicalLimits(&bundles[index]); err != nil {
			return fmt.Errorf("invalid pull bundle %d: %w", index, err)
		}
		var err error
		aggregateBytes, err = checkedAddInt64(aggregateBytes, bundles[index].ByteCount)
		if err != nil {
			return fmt.Errorf("pull response aggregate logical byte count overflow: %w", err)
		}
		if aggregateBytes > maxPullResponseLogicalBytes {
			return fmt.Errorf("pull response aggregate logical byte count exceeds the protocol hard limit %d", maxPullResponseLogicalBytes)
		}
	}
	return nil
}

func validatePullBundleLogicalLimits(bundle *oversync.Bundle) error {
	if bundle == nil {
		return fmt.Errorf("bundle is missing")
	}
	if bundle.RowCount < 0 || bundle.RowCount > maxCommittedPushBundleRows {
		return fmt.Errorf("bundle row count exceeds the protocol hard limit %d", maxCommittedPushBundleRows)
	}
	if len(bundle.Rows) > int(maxCommittedPushBundleRows) {
		return fmt.Errorf("bundle actual row count exceeds the protocol hard limit %d", maxCommittedPushBundleRows)
	}
	if bundle.ByteCount < 2 || bundle.ByteCount > maxCommittedPushBundleBytes {
		return fmt.Errorf("bundle byte_count exceeds the protocol hard limit %d or is below the empty-array minimum", maxCommittedPushBundleBytes)
	}
	return nil
}

func (c *Client) validatePullResponse(resp *oversync.PullResponse, afterBundleSeq int64) error {
	if resp == nil {
		return fmt.Errorf("pull response missing body")
	}
	if err := validatePullResponseLogicalLimits(resp.Bundles); err != nil {
		return err
	}
	if resp.StableBundleSeq < 0 {
		return fmt.Errorf("pull response stable_bundle_seq %d must be non-negative", resp.StableBundleSeq)
	}
	if resp.StableBundleSeq < afterBundleSeq {
		return fmt.Errorf("pull response stable_bundle_seq %d is behind requested after_bundle_seq %d", resp.StableBundleSeq, afterBundleSeq)
	}
	if len(resp.Bundles) > 0 && resp.StableBundleSeq == 0 {
		return fmt.Errorf("pull response missing stable_bundle_seq for non-empty bundle list")
	}

	prevBundleSeq := afterBundleSeq
	for idx := range resp.Bundles {
		bundle := &resp.Bundles[idx]
		if err := validateBundle(bundle); err != nil {
			return fmt.Errorf("invalid pull bundle %d: %w", idx, err)
		}
		for rowIndex := range bundle.Rows {
			if err := c.validateBundleRowForBundle(&bundle.Rows[rowIndex], bundle.BundleSeq); err != nil {
				return fmt.Errorf("invalid pull bundle %d row %d: %w", idx, rowIndex, err)
			}
		}
		expectedBundleSeq, err := checkedAddInt64(prevBundleSeq, 1)
		if err != nil {
			return fmt.Errorf("pull response canonical bundle sequence overflow after %d: %w", prevBundleSeq, err)
		}
		if bundle.BundleSeq != expectedBundleSeq {
			return fmt.Errorf("pull response canonical bundle sequence %d does not follow expected sequence %d", bundle.BundleSeq, expectedBundleSeq)
		}
		if bundle.BundleSeq > resp.StableBundleSeq {
			return fmt.Errorf("pull response bundle_seq %d exceeds stable_bundle_seq %d", bundle.BundleSeq, resp.StableBundleSeq)
		}
		prevBundleSeq = bundle.BundleSeq
	}
	if len(resp.Bundles) == 0 && resp.HasMore {
		return fmt.Errorf("pull response canonical bundle sequence is empty while has_more is true")
	}
	if prevBundleSeq >= resp.StableBundleSeq && resp.HasMore {
		return fmt.Errorf("pull response has_more is true at stable_bundle_seq %d", resp.StableBundleSeq)
	}
	return nil
}

func validateSnapshotSession(resp *oversync.SnapshotSession) error {
	if resp == nil {
		return fmt.Errorf("snapshot session response missing body")
	}
	if resp.SnapshotID == "" {
		return fmt.Errorf("snapshot session response missing snapshot_id")
	}
	if resp.SnapshotBundleSeq < 0 {
		return fmt.Errorf("snapshot session snapshot_bundle_seq %d must be non-negative", resp.SnapshotBundleSeq)
	}
	if resp.RowCount < 0 {
		return fmt.Errorf("snapshot session row_count %d must be non-negative", resp.RowCount)
	}
	if resp.RowCount > 0 && resp.SnapshotBundleSeq == 0 {
		return fmt.Errorf("snapshot session missing snapshot_bundle_seq for non-empty row set")
	}
	if resp.ByteCount < 0 {
		return fmt.Errorf("snapshot session byte_count %d must be non-negative", resp.ByteCount)
	}
	if resp.RowCount == 0 && resp.ByteCount != 0 {
		return fmt.Errorf("empty snapshot session must declare byte_count 0")
	}
	if resp.RowCount > 0 && resp.ByteCount == 0 {
		return fmt.Errorf("non-empty snapshot session must declare a positive byte_count")
	}
	if strings.TrimSpace(resp.ExpiresAt) == "" {
		return fmt.Errorf("snapshot session response missing expires_at")
	}
	if len(resp.SnapshotHash) != 64 || strings.Trim(resp.SnapshotHash, "0123456789abcdef") != "" {
		return fmt.Errorf("snapshot session hash must be lowercase SHA-256")
	}
	return nil
}

func validateSnapshotChunkResponse(resp *oversync.SnapshotChunkResponse, snapshotID string, snapshotBundleSeq int64, afterRowOrdinal int64, maxRows int, maxBytes int64) error {
	return validateSnapshotChunkResponseWithExpectedByteCount(resp, snapshotID, snapshotBundleSeq, afterRowOrdinal, maxRows, maxBytes, nil)
}

func validateSnapshotChunkResponseWithExpectedByteCount(resp *oversync.SnapshotChunkResponse, snapshotID string, snapshotBundleSeq int64, afterRowOrdinal int64, maxRows int, maxBytes int64, expectedByteCount *int64) error {
	if resp == nil {
		return fmt.Errorf("snapshot chunk response missing body")
	}
	if resp.SnapshotID != snapshotID {
		return fmt.Errorf("snapshot chunk response snapshot_id %q does not match requested %q", resp.SnapshotID, snapshotID)
	}
	if resp.SnapshotBundleSeq != snapshotBundleSeq {
		return fmt.Errorf("snapshot chunk response snapshot_bundle_seq %d does not match session %d", resp.SnapshotBundleSeq, snapshotBundleSeq)
	}
	if len(resp.Rows) > 0 && resp.SnapshotBundleSeq == 0 {
		return fmt.Errorf("snapshot chunk response missing snapshot_bundle_seq for non-empty row set")
	}
	if len(resp.Rows) > maxRows {
		return fmt.Errorf("snapshot chunk row count %d exceeds effective max_rows %d", len(resp.Rows), maxRows)
	}
	if resp.ByteCount < 0 || resp.ByteCount > maxBytes {
		return fmt.Errorf("snapshot chunk byte_count %d exceeds effective max_bytes %d or is negative", resp.ByteCount, maxBytes)
	}
	if len(resp.Rows) == 0 && resp.ByteCount != 0 {
		return fmt.Errorf("empty snapshot chunk must declare byte_count 0")
	}
	if len(resp.Rows) > 0 && resp.ByteCount == 0 {
		return fmt.Errorf("non-empty snapshot chunk must declare a positive byte_count")
	}
	expectedOrdinal, err := checkedAddInt64(afterRowOrdinal, int64(len(resp.Rows)))
	if err != nil {
		return fmt.Errorf("snapshot chunk next ordinal overflow: %w", err)
	}
	if resp.NextRowOrdinal != expectedOrdinal {
		return fmt.Errorf("snapshot chunk response next_row_ordinal %d does not match expected %d", resp.NextRowOrdinal, expectedOrdinal)
	}
	if resp.HasMore && len(resp.Rows) == 0 {
		return fmt.Errorf("snapshot chunk response with has_more=true must include at least one row")
	}
	var computedByteCount int64
	for idx := range resp.Rows {
		row := &resp.Rows[idx]
		if err := validateSnapshotRow(row); err != nil {
			return fmt.Errorf("invalid snapshot row %d: %w", idx, err)
		}
		if row.RowVersion > snapshotBundleSeq {
			return fmt.Errorf("invalid snapshot row %d: row_version %d exceeds snapshot_bundle_seq %d", idx, row.RowVersion, snapshotBundleSeq)
		}
		rowBytes, err := snapshotRowEncodedByteCount(row)
		if err != nil {
			return fmt.Errorf("invalid snapshot row %d encoding: %w", idx, err)
		}
		computedByteCount, err = checkedAddInt64(computedByteCount, rowBytes)
		if err != nil {
			return fmt.Errorf("snapshot chunk computed byte count overflow: %w", err)
		}
	}
	if expectedByteCount != nil {
		computedByteCount = *expectedByteCount
	}
	if resp.ByteCount != computedByteCount {
		return fmt.Errorf("snapshot chunk byte_count %d does not match computed %d", resp.ByteCount, computedByteCount)
	}
	return nil
}

func validateBundle(bundle *oversync.Bundle) error {
	if err := validatePullBundleLogicalLimits(bundle); err != nil {
		return err
	}
	if bundle.BundleSeq <= 0 {
		return fmt.Errorf("bundle_seq %d must be positive", bundle.BundleSeq)
	}
	if err := validateSourceID(bundle.SourceID); err != nil {
		return fmt.Errorf("bundle source_id is invalid: %w", err)
	}
	if bundle.SourceBundleID <= 0 {
		return fmt.Errorf("bundle source_bundle_id %d must be positive", bundle.SourceBundleID)
	}
	if bundle.Rows == nil {
		return fmt.Errorf("bundle rows must be present")
	}
	if bundle.RowCount != int64(len(bundle.Rows)) {
		return fmt.Errorf("bundle row_count %d does not match rows length %d", bundle.RowCount, len(bundle.Rows))
	}
	for idx := range bundle.Rows {
		row := &bundle.Rows[idx]
		if err := validateBundleRow(row); err != nil {
			return fmt.Errorf("invalid bundle row %d: %w", idx, err)
		}
	}
	hasher := protocolhash.NewCommittedBundleHasher()
	for _, row := range bundle.Rows {
		if err := hasher.Add(protocolhash.BundleRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, RowVersion: row.RowVersion, Payload: row.Payload}); err != nil {
			return fmt.Errorf("bundle committed stream is invalid: %w", err)
		}
		if hasher.CurrentByteCount() > maxCommittedPushBundleBytes {
			return fmt.Errorf("bundle computed byte_count exceeds the protocol hard limit %d", maxCommittedPushBundleBytes)
		}
	}
	hash, byteCount, err := hasher.Sum()
	if err != nil {
		return fmt.Errorf("bundle committed stream is invalid: %w", err)
	}
	if bundle.ByteCount != byteCount {
		return fmt.Errorf("bundle byte_count %d does not match computed %d", bundle.ByteCount, byteCount)
	}
	if bundle.BundleHash != hash {
		return fmt.Errorf("bundle_hash does not match committed rows")
	}
	if bundle.CanonicalRequestHash != "" {
		return fmt.Errorf("pull bundle must omit canonical_request_hash")
	}
	return nil
}

func validateBundleRow(row *oversync.BundleRow) error {
	if strings.TrimSpace(row.Schema) == "" {
		return fmt.Errorf("row schema must be non-empty")
	}
	if strings.TrimSpace(row.Table) == "" {
		return fmt.Errorf("row table must be non-empty")
	}
	if len(row.Key) == 0 {
		return fmt.Errorf("row key must be non-empty")
	}
	if row.RowVersion <= 0 {
		return fmt.Errorf("row_version %d must be positive", row.RowVersion)
	}
	switch row.Op {
	case oversync.OpInsert, oversync.OpUpdate:
		if len(row.Payload) == 0 {
			return fmt.Errorf("%s row payload must be present", strings.ToLower(row.Op))
		}
	case oversync.OpDelete:
		if len(row.Payload) != 0 {
			return fmt.Errorf("delete row payload must be omitted")
		}
	default:
		return fmt.Errorf("unsupported row op %q", row.Op)
	}
	return nil
}

func (c *Client) validateBundleRowForBundle(row *oversync.BundleRow, bundleSeq int64) error {
	if err := validateBundleRow(row); err != nil {
		return err
	}
	if row.RowVersion != bundleSeq {
		return fmt.Errorf("row_version %d does not match bundle_seq %d", row.RowVersion, bundleSeq)
	}
	if row.Schema != c.schemaForTable(row.Table) {
		return fmt.Errorf("row schema %s does not match client schema %s", row.Schema, c.schemaForTable(row.Table))
	}
	keyColumns, ok := c.keyByTable[row.Table]
	if !ok || len(keyColumns) != 1 {
		return fmt.Errorf("row table %s is not configured with one sync key", row.Table)
	}
	keyName := strings.ToLower(keyColumns[0])
	if len(row.Key) != 1 {
		return fmt.Errorf("row key must contain exactly the configured sync key")
	}
	keyValue, ok := row.Key[keyName]
	if !ok {
		return fmt.Errorf("row key does not contain configured sync key %s", keyName)
	}
	if row.Op == oversync.OpDelete {
		return nil
	}
	payload, err := jcs.DecodeObjectLossless(row.Payload)
	if err != nil {
		return fmt.Errorf("row payload is invalid: %w", err)
	}
	payloadKey, ok := payload[keyName]
	if !ok {
		return fmt.Errorf("row payload does not contain configured sync key %s", keyName)
	}
	wireKey, err := jcs.Marshal(keyValue)
	if err != nil {
		return fmt.Errorf("row key is invalid: %w", err)
	}
	wirePayloadKey, err := jcs.Marshal(payloadKey)
	if err != nil {
		return fmt.Errorf("row payload key is invalid: %w", err)
	}
	if !bytes.Equal(wireKey, wirePayloadKey) {
		return fmt.Errorf("row key does not match payload key")
	}
	return nil
}

func validateSnapshotRow(row *oversync.SnapshotRow) error {
	if strings.TrimSpace(row.Schema) == "" {
		return fmt.Errorf("row schema must be non-empty")
	}
	if strings.TrimSpace(row.Table) == "" {
		return fmt.Errorf("row table must be non-empty")
	}
	if len(row.Key) == 0 {
		return fmt.Errorf("row key must be non-empty")
	}
	if row.RowVersion <= 0 {
		return fmt.Errorf("row_version %d must be positive", row.RowVersion)
	}
	if len(row.Payload) == 0 {
		return fmt.Errorf("snapshot row payload must be present")
	}
	return nil
}

func snapshotRowEncodedByteCount(row *oversync.SnapshotRow) (int64, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return 0, err
	}
	return int64(len(encoded)), nil
}

func (c *Client) validateSnapshotRowForSession(row *oversync.SnapshotRow, snapshotBundleSeq int64) error {
	if err := validateSnapshotRow(row); err != nil {
		return err
	}
	if row.RowVersion > snapshotBundleSeq {
		return fmt.Errorf("row_version %d exceeds snapshot_bundle_seq %d", row.RowVersion, snapshotBundleSeq)
	}
	if row.Schema != c.schemaForTable(row.Table) {
		return fmt.Errorf("snapshot row schema %s does not match client schema %s", row.Schema, c.schemaForTable(row.Table))
	}
	keyColumns, ok := c.keyByTable[row.Table]
	if !ok || len(keyColumns) != 1 {
		return fmt.Errorf("snapshot row table %s is not configured with one sync key", row.Table)
	}
	keyName := strings.ToLower(keyColumns[0])
	if len(row.Key) != 1 {
		return fmt.Errorf("snapshot row key must contain exactly the configured sync key")
	}
	keyValue, ok := row.Key[keyName]
	if !ok {
		return fmt.Errorf("snapshot row key does not contain configured sync key %s", keyName)
	}
	payload, err := jcs.DecodeObjectLossless(row.Payload)
	if err != nil {
		return fmt.Errorf("snapshot row payload is invalid: %w", err)
	}
	payloadKey, ok := payload[keyName]
	if !ok {
		return fmt.Errorf("snapshot row payload does not contain configured sync key %s", keyName)
	}
	wireKey, err := jcs.Marshal(keyValue)
	if err != nil {
		return fmt.Errorf("snapshot row key is invalid: %w", err)
	}
	wirePayloadKey, err := jcs.Marshal(payloadKey)
	if err != nil {
		return fmt.Errorf("snapshot row payload key is invalid: %w", err)
	}
	if !bytes.Equal(wireKey, wirePayloadKey) {
		return fmt.Errorf("snapshot row key does not match payload key")
	}
	return nil
}

func (c *Client) validateStagedSnapshotForApply(ctx context.Context, tx sqliteTransaction, session *oversync.SnapshotSession) error {
	var duplicateOrdinal int64
	err := tx.QueryRowContext(ctx, `
		SELECT MIN(row_ordinal)
		FROM _sync_snapshot_stage
		WHERE snapshot_id = ?
		GROUP BY schema_name, table_name, key_json
		HAVING COUNT(*) > 1
		ORDER BY MIN(row_ordinal)
		LIMIT 1
	`, session.SnapshotID).Scan(&duplicateOrdinal)
	if err == nil {
		return fmt.Errorf("duplicate snapshot row identity at row %d", duplicateOrdinal)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to validate staged snapshot identity uniqueness: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
		FROM _sync_snapshot_stage
		WHERE snapshot_id = ?
		ORDER BY row_ordinal
	`, session.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to query staged snapshot for final validation: %w", err)
	}
	defer rows.Close()

	var rowCount, byteCount int64
	for rows.Next() {
		var (
			ordinal    int64
			schemaName string
			tableName  string
			keyJSON    string
			rowVersion int64
			payload    string
		)
		if err := rows.Scan(&ordinal, &schemaName, &tableName, &keyJSON, &rowVersion, &payload); err != nil {
			return fmt.Errorf("failed to scan staged snapshot for final validation: %w", err)
		}
		expectedOrdinal, err := checkedAddInt64(rowCount, 1)
		if err != nil {
			return fmt.Errorf("staged snapshot row ordinal overflow: %w", err)
		}
		if ordinal != expectedOrdinal {
			return fmt.Errorf("staged snapshot row ordinal %d does not match expected %d", ordinal, expectedOrdinal)
		}
		_, wireKey, err := c.decodeDirtyKeyForPush(tx, tableName, keyJSON)
		if err != nil {
			return fmt.Errorf("invalid staged snapshot key at row %d: %w", ordinal, err)
		}
		canonicalKeyJSON, _, err := c.bundleRowKeyToLocalKey(tx, tableName, wireKey)
		if err != nil {
			return fmt.Errorf("invalid staged snapshot key at row %d: %w", ordinal, err)
		}
		if canonicalKeyJSON != keyJSON {
			return fmt.Errorf("invalid staged snapshot key at row %d: key_json is not canonical", ordinal)
		}
		row := oversync.SnapshotRow{
			Schema: schemaName, Table: tableName, Key: wireKey,
			RowVersion: rowVersion, Payload: json.RawMessage(payload),
		}
		if err := c.validateSnapshotRowForSession(&row, session.SnapshotBundleSeq); err != nil {
			return fmt.Errorf("invalid staged snapshot row %d: %w", ordinal, err)
		}
		if c.discovery == nil {
			return fmt.Errorf("staged snapshot validation requires an admitted discovery contract")
		}
		_, encodedBytes, err := oversync.CompactGroupsForSnapshotRowsAt(c.discovery.SchemaGeneration, []oversync.SnapshotRow{row}, ordinal)
		if err != nil {
			return fmt.Errorf("encode staged snapshot row %d: %w", ordinal, err)
		}
		byteCount, err = checkedAddInt64(byteCount, encodedBytes)
		if err != nil {
			return fmt.Errorf("staged snapshot byte count overflow: %w", err)
		}
		rowCount = expectedOrdinal
		if rowCount > session.RowCount {
			return fmt.Errorf("staged snapshot row count exceeds declared session row_count")
		}
		if byteCount > session.ByteCount {
			return fmt.Errorf("staged snapshot byte count exceeds declared session byte_count")
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate staged snapshot for final validation: %w", err)
	}
	if rowCount != session.RowCount {
		return fmt.Errorf("staged snapshot row count %d does not match expected row_count %d", rowCount, session.RowCount)
	}
	if byteCount != session.ByteCount {
		return fmt.Errorf("staged snapshot byte count %d does not match expected byte_count %d", byteCount, session.ByteCount)
	}
	return nil
}
