package oversync

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRecoverySnapshot_BoundaryAndPendingOutcome(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		proof := testSchemaGenerationProofForService(f.svc)
		pending, err := f.svc.PushOneShot(ctx, f.writer, proof, f.request(t, 1, f.userRow(uuid.New(), "sealed")))
		require.NoError(t, err)
		before := f.count(t, ctx, `SELECT count(*) FROM sync.snapshot_sessions`)
		_, err = f.svc.CreateSnapshotSessionWithRequest(ctx, f.writer, proof, &SnapshotSessionCreateRequest{Recovery: &SnapshotRecovery{MinimumBundleSeq: 1}})
		outcomeCode(t, err, "push_outcome_pending")
		require.Equal(t, before, f.count(t, ctx, `SELECT count(*) FROM sync.snapshot_sessions`))
		f.ack(t, ctx, f.writer, pending.Outcome)
		_, err = f.svc.CreateSnapshotSessionWithRequest(ctx, f.writer, proof, &SnapshotSessionCreateRequest{Recovery: &SnapshotRecovery{MinimumBundleSeq: 2}})
		outcomeCode(t, err, "recovery_floor_ahead")
		require.Equal(t, before, f.count(t, ctx, `SELECT count(*) FROM sync.snapshot_sessions`))
		session, err := f.svc.CreateSnapshotSessionWithRequest(ctx, f.writer, proof, &SnapshotSessionCreateRequest{Recovery: &SnapshotRecovery{MinimumBundleSeq: 1}})
		require.NoError(t, err)
		require.EqualValues(t, 1, session.SnapshotBundleSeq)
		require.Equal(t, &SnapshotRecovery{MinimumBundleSeq: 1}, session.Recovery)
		require.Len(t, session.SnapshotHash, 64)
		_, err = f.svc.GetSnapshotChunk(ctx, f.reader, proof, session.SnapshotID, 0, 1, 16384)
		var forbidden *SnapshotSessionForbiddenError
		require.ErrorAs(t, err, &forbidden)
		otherScope := f.writer
		otherScope.UserID = "other-scope"
		_, err = f.svc.GetSnapshotChunk(ctx, otherScope, proof, session.SnapshotID, 0, 1, 16384)
		require.Error(t, err)
		otherGeneration := proof
		otherGeneration.Fingerprint = strings.Repeat("0", 64)
		_, err = f.svc.GetSnapshotChunk(ctx, f.writer, otherGeneration, session.SnapshotID, 0, 1, 16384)
		require.Error(t, err)
		var hash string
		var floor int64
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT snapshot_hash,recovery_minimum_bundle_seq FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid`, session.SnapshotID).Scan(&hash, &floor))
		require.Equal(t, session.SnapshotHash, hash)
		require.EqualValues(t, 1, floor)
		// A later authoritative write cannot change the sealed session or T.
		next, err := f.svc.PushOneShot(ctx, f.writer, proof, f.request(t, 2, f.userRow(uuid.New(), "later")))
		require.NoError(t, err)
		f.ack(t, ctx, f.writer, next.Outcome)
		chunk, err := f.svc.GetSnapshotChunk(ctx, f.writer, proof, session.SnapshotID, 0, 10, 16384)
		require.NoError(t, err)
		require.Len(t, chunk.Rows, 1)
		require.EqualValues(t, 1, chunk.SnapshotBundleSeq)
	})
}

func TestRecoverySnapshot_StreamingHash(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		proof := testSchemaGenerationProofForService(f.svc)
		empty, err := f.svc.CreateSnapshotSessionWithRequest(ctx, f.reader, proof, &SnapshotSessionCreateRequest{Recovery: &SnapshotRecovery{MinimumBundleSeq: 0}})
		require.NoError(t, err)
		require.Zero(t, empty.RowCount)
		require.Zero(t, empty.ByteCount)
		view, err := f.svc.resolveRequestGeneration(ctx, f.reader, proof)
		require.NoError(t, err)
		generation := generationDescriptor(view.contract)
		emptyHash, err := NewSnapshotHashWriter(f.reader.SourceID, generation, empty.SnapshotID, 0, empty.Recovery)
		require.NoError(t, err)
		digest, err := emptyHash.Sum(0, 0)
		require.NoError(t, err)
		require.Equal(t, digest, empty.SnapshotHash)
		page, err := f.svc.GetSnapshotChunk(ctx, f.reader, proof, empty.SnapshotID, 0, 1, 16384)
		require.NoError(t, err)
		require.Empty(t, page.Rows)
		require.False(t, page.HasMore)
		f.query(t, ctx, `UPDATE sync.snapshot_sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE snapshot_id=$1::uuid`, empty.SnapshotID)
		_, err = f.svc.GetSnapshotChunk(ctx, f.reader, proof, empty.SnapshotID, 0, 1, 16384)
		var expired *SnapshotSessionExpiredError
		require.ErrorAs(t, err, &expired)
		for i := int64(1); i <= 3; i++ {
			r, e := f.svc.PushOneShot(ctx, f.writer, proof, f.request(t, i, f.userRow(uuid.New(), "row")))
			require.NoError(t, e)
			f.ack(t, ctx, f.writer, r.Outcome)
		}
		// The ordinary retention owner actually pruned history. Recovery still
		// materializes the complete current projection, including older rows.
		require.EqualValues(t, 1, f.count(t, ctx, `SELECT count(*) FROM sync.bundle_log`))
		require.EqualValues(t, 3, f.count(t, ctx, `SELECT min(bundle_seq) FROM sync.bundle_log`))
		session, err := f.svc.CreateSnapshotSessionWithRequest(ctx, f.reader, proof, &SnapshotSessionCreateRequest{Recovery: &SnapshotRecovery{MinimumBundleSeq: 3}})
		require.NoError(t, err)
		for _, pageSize := range []int{1, 2, 10} {
			h, e := NewSnapshotHashWriter(f.reader.SourceID, generation, session.SnapshotID, session.SnapshotBundleSeq, session.Recovery)
			require.NoError(t, e)
			var cursor int64
			for {
				page, e := f.svc.GetSnapshotChunk(ctx, f.reader, proof, session.SnapshotID, cursor, pageSize, 16384)
				require.NoError(t, e)
				groups, _, e := CompactGroupsForSnapshotRowsAt(generation, page.Rows, cursor+1)
				require.NoError(t, e)
				for _, g := range groups {
					for _, row := range g.Rows {
						require.NoError(t, h.Add(g.TableIndex, row.KeyValues, row.Version, row.Payload))
					}
				}
				cursor = page.NextRowOrdinal
				if !page.HasMore {
					break
				}
			}
			digest, e := h.Sum(session.RowCount, session.ByteCount)
			require.NoError(t, e)
			require.Equal(t, session.SnapshotHash, digest)
		}
		raw, e := json.Marshal(CompactSnapshotSessionResponse{SnapshotID: session.SnapshotID, SnapshotBundleSeq: session.SnapshotBundleSeq, RowCount: session.RowCount, LogicalByteCount: session.ByteCount, ExpiresAt: session.ExpiresAt, SnapshotHash: session.SnapshotHash, Recovery: session.Recovery})
		require.NoError(t, e)
		var decoded CompactSnapshotSessionResponse
		require.NoError(t, json.Unmarshal(raw, &decoded))
		require.NoError(t, ValidateSnapshotRecoveryEcho(decoded, session.Recovery))
	})
}
