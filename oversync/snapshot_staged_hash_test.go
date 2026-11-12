package oversync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// readBackSnapshotHash is the seal that session creation computed before hashing while staging: it
// reads every staged row back in row order and hashes what was stored. Kept as the oracle for the
// staged hash.
func readBackSnapshotHash(ctx context.Context, s *SyncService, tx pgx.Tx, selection generationSelection, source, id string, boundary int64, recovery *SnapshotRecovery, count, size int64) (string, error) {
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

// servedSnapshotHash recomputes the hash the client verifies, over the rows the session serves.
func servedSnapshotHash(t *testing.T, f *snapshotSessionFixture, session *SnapshotSession, pageSize int) string {
	t.Helper()
	proof := testSchemaGenerationProofForService(f.svc)
	view, err := f.svc.resolveRequestGeneration(f.ctx, f.reader, proof)
	require.NoError(t, err)
	generation := generationDescriptor(view.contract)
	h, err := NewSnapshotHashWriter(f.reader.SourceID, generation, session.SnapshotID, session.SnapshotBundleSeq, session.Recovery)
	require.NoError(t, err)
	var cursor int64
	for {
		page, err := f.svc.GetSnapshotChunk(f.ctx, f.reader, proof, session.SnapshotID, cursor, pageSize, 1<<20)
		require.NoError(t, err)
		groups, _, err := CompactGroupsForSnapshotRowsAt(generation, page.Rows, cursor+1)
		require.NoError(t, err)
		for _, group := range groups {
			for _, row := range group.Rows {
				require.NoError(t, h.Add(group.TableIndex, row.KeyValues, row.Version, row.Payload))
			}
		}
		cursor = page.NextRowOrdinal
		if !page.HasMore {
			break
		}
	}
	digest, err := h.Sum(session.RowCount, session.ByteCount)
	require.NoError(t, err)
	return digest
}

func readBackHashForSession(t *testing.T, f *snapshotSessionFixture, session *SnapshotSession) string {
	t.Helper()
	proof := testSchemaGenerationProofForService(f.svc)
	view, err := f.svc.resolveRequestGeneration(f.ctx, f.reader, proof)
	require.NoError(t, err)
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(f.ctx) }()
	digest, err := readBackSnapshotHash(f.ctx, f.svc, tx, generationSelection{proof: proof, view: view}, f.reader.SourceID,
		session.SnapshotID, session.SnapshotBundleSeq, session.Recovery, session.RowCount, session.ByteCount)
	require.NoError(t, err)
	return digest
}

// Creating a session must not read its staged rows back: the hash is accumulated while staging.
func TestSnapshotSession_CreationDoesNotReadStagedRowsBack(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "hash_no_readback", snapshotSessionFixtureOptions{})
	for i := int64(1); i <= 5; i++ {
		fixture.pushUser(t, i, uuid.New(), fmt.Sprintf("user-%d", i))
	}
	scans := 0
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotChunkRowScan: func(snapshotPayloadScanObservation) { scans++ }}
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.EqualValues(t, 5, session.RowCount)
	require.Zero(t, scans, "session creation read staged rows back")

	// Serving still validates every stored row it returns.
	_, err = fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, 10, 1<<20)
	require.NoError(t, err)
	require.Equal(t, 5, scans)
	fixture.svc.snapshotHooks = nil
}

// The hash accumulated while staging must equal the hash of the staged rows read back in order and
// the hash the client computes over the served rows, across tables, types and batch boundaries.
func TestSnapshotSession_HashAccumulatedWhileStagingMatchesStagedRows(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "hash_matches", snapshotSessionFixtureOptions{
		registeredTables: []RegisteredTable{
			{Table: "users", SyncKeyColumns: []string{"id"}},
			{Table: "posts", SyncKeyColumns: []string{"id"}},
			{Table: "files", SyncKeyColumns: []string{"id"}},
		},
		// Small batches: several COPY batches per table, and rows read past the byte limit are re-read.
		snapshotMaterializationBatchRows:  3,
		snapshotMaterializationBatchBytes: 400,
		maxBytesPerSnapshotRow:            400,
	})

	empty, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Zero(t, empty.RowCount)
	require.Equal(t, readBackHashForSession(t, fixture, empty), empty.SnapshotHash)

	names := []string{"Ada", "Zoë", "名前", "emoji 🎉", `quote " and \ backslash`, "tab\tnew\nline", "x" + string(rune(0x2028)) + "y"}
	var rows []PushRequestRow
	var users []uuid.UUID
	for i, name := range names {
		id := uuid.New()
		users = append(users, id)
		payload, err := json.Marshal(map[string]any{"id": id.String(), "name": name, "email": fmt.Sprintf("u%d@example.com", i)})
		require.NoError(t, err)
		rows = append(rows, PushRequestRow{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": id.String()}, Op: OpInsert, Payload: payload})
	}
	for i := 0; i < 5; i++ {
		id := uuid.New()
		payload, err := json.Marshal(map[string]any{"id": id.String(), "title": fmt.Sprintf("post %d", i), "content": names[i%len(names)], "author_id": users[i%len(users)].String()})
		require.NoError(t, err)
		rows = append(rows, PushRequestRow{Schema: fixture.schemaName, Table: "posts", Key: SyncKey{"id": id.String()}, Op: OpInsert, Payload: payload})
	}
	for i := 0; i < 4; i++ {
		id := uuid.New()
		data := make([]byte, 16+i*40)
		for j := range data {
			data[j] = byte(j*31 + i)
		}
		payload, err := json.Marshal(map[string]any{"id": id.String(), "name": fmt.Sprintf("file-%d", i), "data": base64.StdEncoding.EncodeToString(data)})
		require.NoError(t, err)
		rows = append(rows, PushRequestRow{Schema: fixture.schemaName, Table: "files", Key: SyncKey{"id": id.String()}, Op: OpInsert, Payload: payload})
	}
	_, err = pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, 1, rows)
	require.NoError(t, err)

	for _, recovery := range []*SnapshotRecovery{nil, {MinimumBundleSeq: 1}} {
		session, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc),
			&SnapshotSessionCreateRequest{Recovery: recovery})
		require.NoError(t, err)
		require.EqualValues(t, len(rows), session.RowCount)
		require.Equal(t, readBackHashForSession(t, fixture, session), session.SnapshotHash, "read-back hash (recovery=%v)", recovery != nil)
		for _, pageSize := range []int{1, 4, 100} {
			require.Equal(t, session.SnapshotHash, servedSnapshotHash(t, fixture, session, pageSize), "served hash, page size %d", pageSize)
		}
	}
}
