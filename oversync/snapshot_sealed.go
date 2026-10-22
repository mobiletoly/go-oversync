package oversync

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Hash the immutable materialization in ordinal order, retaining one bounded row.
// The caller owns the coherent source fence and publishes counts/hash atomically.
func (s *SyncService) sealSnapshotRows(ctx context.Context, tx pgx.Tx, selection generationSelection, source, id string, boundary int64, recovery *SnapshotRecovery, count, size int64) (string, error) {
	generation := generationDescriptor(selection.view.contract)
	h, err := NewSnapshotHashWriter(source, generation, id, boundary, recovery)
	if err != nil {
		return "", err
	}
	physicalLimit, err := physicalStoredWirePayloadByteLimit(s.maxBytesPerSnapshotRow())
	if err != nil {
		return "", err
	}
	rows, err := tx.Query(ctx, `SELECT row_ordinal,table_id,octet_length(key_bytes),
 CASE WHEN octet_length(key_bytes)<=$2 THEN key_bytes END,bundle_seq,
 octet_length(payload_wire::text),CASE WHEN octet_length(payload_wire::text)<=$3 THEN payload_wire END,wire_byte_count
 FROM sync.snapshot_session_rows WHERE snapshot_id=$1::uuid ORDER BY row_ordinal`, id, s.maxBytesPerSnapshotRow(), physicalLimit)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var ordinal int64
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		ordinal++
		row, logical, err := s.scanSnapshotChunkRow(rows, selection, boundary, ordinal, physicalLimit)
		if err != nil {
			return "", err
		}
		groups, computed, err := CompactGroupsForSnapshotRowsAt(generation, []SnapshotRow{row}, ordinal)
		if err != nil || computed != logical {
			return "", fmt.Errorf("snapshot hash logical accounting mismatch")
		}
		group := groups[0]
		keys := make([]any, len(generation.Tables[group.TableIndex].SyncKeyColumns))
		for i, key := range generation.Tables[group.TableIndex].SyncKeyColumns {
			keys[i] = row.Key[key]
		}
		if err := h.Add(group.TableIndex, keys, row.RowVersion, row.Payload); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return h.Sum(count, size)
}
