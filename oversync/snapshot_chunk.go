// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/jcs"
)

// snapshotChunkSizer measures the same JSON as the final compact encoder without
// retaining or re-encoding earlier rows. groupsBytes excludes the outer [] and
// includes all group/tuple separators. Its zero value sizes an empty rows array.
// Each input is the one-row group from CompactGroupsForSnapshotRowsAt.
type snapshotChunkSizer struct {
	groupsBytes    int64
	lastTableIndex int
	hasRows        bool
}

func (s *snapshotChunkSizer) addRow(group CompactRowGroup) error {
	var value any = group
	if s.hasRows && s.lastTableIndex == group.TableIndex {
		value = group.Rows[0]
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	additional := int64(len(encoded))
	if s.hasRows {
		additional++ // Comma between tuples or between groups.
	}
	if additional > math.MaxInt64-s.groupsBytes {
		return fmt.Errorf("snapshot decoded byte count overflows")
	}
	s.groupsBytes += additional
	s.lastTableIndex = group.TableIndex
	s.hasRows = true
	return nil
}

func (s snapshotChunkSizer) decodedBytes(nextRowOrdinal, logicalBytes int64, hasMore bool) (int64, error) {
	// Use the real envelope so tags, decimal widths and true/false are identical
	// to the HTTP body. No payload is visited here.
	envelope, err := json.Marshal(CompactSnapshotChunkResponse{
		Rows: []CompactRowGroup{}, NextRowOrdinal: nextRowOrdinal,
		LogicalByteCount: logicalBytes, HasMore: hasMore,
	})
	if err != nil {
		return 0, err
	}
	if int64(len(envelope)) > math.MaxInt64-s.groupsBytes {
		return 0, fmt.Errorf("snapshot decoded byte count overflows")
	}
	return int64(len(envelope)) + s.groupsBytes, nil
}

// The first row may fit only with has_more:true (one byte shorter than false).
// Look ahead before reporting the exact minimum body required for a retry.
func fitFirstSnapshotChunkRow(rows pgx.Rows, sizer snapshotChunkSizer, ordinal, logicalBytes, maxBytes int64) (bool, error) {
	moreRows := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("determine compact snapshot chunk continuation: %w", err)
	}
	requiredBytes, err := sizer.decodedBytes(ordinal, logicalBytes, moreRows)
	if err != nil {
		return false, fmt.Errorf("encode minimal compact snapshot chunk: %w", err)
	}
	if requiredBytes > maxBytes {
		return false, &SnapshotChunkTooSmallError{RequiredDecodedBodyBytes: requiredBytes}
	}
	return moreRows, nil
}

// Validate caller targets before accessing the session, then clamp them to the
// admitted service limits. Presence and ordinal checks retain their precedence.
func (s *SyncService) snapshotChunkLimits(snapshotID string, afterRowOrdinal int64, maxRows int, maxBytes int64) (int, int64, error) {
	if snapshotID == "" {
		return 0, 0, &SnapshotChunkInvalidError{Message: "snapshot_id must be provided"}
	}
	if afterRowOrdinal < 0 {
		return 0, 0, &SnapshotChunkInvalidError{Message: "after_row_ordinal must be >= 0"}
	}
	if maxRows <= 0 {
		return 0, 0, &SnapshotChunkInvalidError{Message: "max_rows must be > 0"}
	}
	if maxBytes <= 0 {
		return 0, 0, &SnapshotChunkInvalidError{Message: "max_bytes must be > 0"}
	}
	return min(maxRows, s.maxRowsPerSnapshotChunk()), min(maxBytes, s.maxBytesPerSnapshotChunk()), nil
}

func (s *SyncService) selectSnapshotChunkRows(ctx context.Context, queryRows pgx.Rows, selection generationSelection, resp *SnapshotChunkResponse, effectiveMaxRows int, effectiveMaxBytes, physicalStoredPayloadLimit int64, observation *snapshotChunkObservation) error {
	wireGeneration := generationDescriptor(selection.view.contract)
	var sizer snapshotChunkSizer
	chunkRows := make([]SnapshotRow, 0, effectiveMaxRows)
	var chunkBytes int64
	hasMore := false
	nextRowOrdinal := resp.NextRowOrdinal
	expectedRowOrdinal := resp.NextRowOrdinal + 1
	for queryRows.Next() {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("iterate snapshot chunk rows: %w", err)
		}
		rowOrdinal := expectedRowOrdinal
		row, wireByteCount, err := s.scanSnapshotChunkRow(queryRows, selection, resp.SnapshotBundleSeq, rowOrdinal, physicalStoredPayloadLimit)
		if err != nil {
			return err
		}
		expectedRowOrdinal++
		rowGroups, rowLogicalBytes, err := CompactGroupsForSnapshotRowsAt(wireGeneration, []SnapshotRow{row}, rowOrdinal)
		if err != nil || rowLogicalBytes != wireByteCount {
			return projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot logical byte count is invalid")
		}
		candidateRetainedRows := int64(len(chunkRows) + 1)
		if rowLogicalBytes > math.MaxInt64-chunkBytes {
			return projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot rows cannot be compact encoded")
		}
		candidateLogicalBytes := chunkBytes + rowLogicalBytes
		if err := sizer.addRow(rowGroups[0]); err != nil {
			return fmt.Errorf("encode compact snapshot chunk candidate: %w", err)
		}
		// Size against the longer final-chunk representation. If the row is not
		// final, has_more:true can only make the emitted response smaller.
		candidateBodyBytes, err := sizer.decodedBytes(rowOrdinal, candidateLogicalBytes, false)
		if err != nil {
			return fmt.Errorf("encode compact snapshot chunk candidate: %w", err)
		}
		candidateRetainedBytes := candidateLogicalBytes
		if candidateRetainedRows > observation.retainedRowsHighWater {
			observation.retainedRowsHighWater = candidateRetainedRows
		}
		if candidateRetainedBytes > observation.retainedBytesHighWater {
			observation.retainedBytesHighWater = candidateRetainedBytes
		}
		if len(chunkRows) >= effectiveMaxRows || candidateBodyBytes > effectiveMaxBytes {
			hasMore = true
			if len(chunkRows) == 0 {
				moreRows, err := fitFirstSnapshotChunkRow(queryRows, sizer, rowOrdinal, candidateLogicalBytes, effectiveMaxBytes)
				if err != nil {
					return err
				}
				hasMore = moreRows
				chunkRows = append(chunkRows, row)
				chunkBytes = candidateLogicalBytes
				nextRowOrdinal = rowOrdinal
				break
			}
			break
		}
		chunkRows = append(chunkRows, row)
		chunkBytes = candidateLogicalBytes
		observation.selectedRowsHighWater = int64(len(chunkRows))
		observation.selectedBytesHighWater = chunkBytes
		nextRowOrdinal = rowOrdinal
	}
	if err := queryRows.Err(); err != nil {
		return fmt.Errorf("iterate snapshot chunk rows: %w", err)
	}

	resp.Rows = chunkRows
	resp.NextRowOrdinal = nextRowOrdinal
	resp.HasMore = hasMore
	resp.ByteCount = chunkBytes
	return nil
}

// Scan and validate even the lookahead row, preserving corruption detection and
// error precedence before applying the chunk's row and byte limits.
func (s *SyncService) scanSnapshotChunkRow(queryRows pgx.Rows, selection generationSelection, snapshotBundleSeq, expectedRowOrdinal, physicalStoredPayloadLimit int64) (row SnapshotRow, wireByteCount int64, err error) {
	var (
		tableID          int32
		keyBytes         []byte
		rowOrdinal       int64
		keyByteCount     int64
		payloadByteCount int64
	)
	if err := queryRows.Scan(&rowOrdinal, &tableID, &keyByteCount, &keyBytes, &row.RowVersion, &payloadByteCount, &row.Payload, &wireByteCount); err != nil {
		return row, 0, fmt.Errorf("scan snapshot chunk row: %w", err)
	}
	if s.snapshotHooks != nil && s.snapshotHooks.afterSnapshotChunkRowScan != nil {
		s.snapshotHooks.afterSnapshotChunkRowScan(snapshotPayloadScanObservation{
			ReportedKeyByteCount: keyByteCount, ReportedPayloadByteCount: payloadByteCount,
			KeyMaterialized: keyBytes != nil, PayloadMaterialized: row.Payload != nil,
			MaterializedKeyBytes: len(keyBytes), MaterializedPayloadBytes: len(row.Payload),
		})
	}
	if keyByteCount < 0 || keyByteCount > s.maxBytesPerSnapshotRow() || payloadByteCount < 0 || payloadByteCount > physicalStoredPayloadLimit || keyBytes == nil || row.Payload == nil {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot row payload exceeds its bounded representation")
	}
	if rowOrdinal != expectedRowOrdinal {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot row ordinals are invalid")
	}
	info, err := s.tableInfoForID(tableID)
	if err != nil {
		return row, 0, err
	}
	visibleTable, visible := selection.view.tables[Key(info.schemaName, info.tableName)]
	if !visible || visibleTable.tableID != tableID {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot table is not visible to the generation")
	}
	if info.schemaName != visibleTable.contract.Schema || info.tableName != visibleTable.contract.Table {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot table identity is invalid")
	}
	row.Schema = info.schemaName
	row.Table = info.tableName
	row.Key, err = wireSyncKeyFromBytes(info, keyBytes)
	if err != nil {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot key is invalid")
	}
	if row.RowVersion <= 0 || row.RowVersion > snapshotBundleSeq || wireByteCount <= 0 {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot row metadata is invalid")
	}
	payloadObject, err := jcs.DecodeObjectLossless(row.Payload)
	if err != nil || len(payloadObject) != len(visibleTable.contract.Columns) {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot payload is invalid")
	}
	for _, column := range visibleTable.contract.Columns {
		value, ok := payloadObject[column.Name]
		if !ok || validateGenerationWireValue(column, value) != nil {
			return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot payload is invalid")
		}
	}
	if len(visibleTable.contract.SyncKeyColumns) != 1 {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot key contract is invalid")
	}
	keyName := visibleTable.contract.SyncKeyColumns[0]
	keyCanonical, keyErr := jcs.Marshal(row.Key[keyName])
	payloadCanonical, payloadErr := jcs.Marshal(payloadObject[keyName])
	if keyErr != nil || payloadErr != nil || string(keyCanonical) != string(payloadCanonical) {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot key does not match payload")
	}
	expandedBytes, err := ExpandedRowDecodedBytes(PushRequestRow{
		Schema: row.Schema, Table: row.Table, Key: row.Key,
		Op: OpInsert, BaseRowVersion: row.RowVersion, Payload: row.Payload,
	})
	if err != nil || expandedBytes > s.maxBytesPerSnapshotRow() {
		return row, 0, projectionError(GenerationProjectionIntegrityError, "snapshot", "stored snapshot row exceeds max_row_decoded_bytes")
	}
	return row, wireByteCount, nil
}
