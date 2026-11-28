package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func snapshotChunkSyntheticRows(count, width int) []SnapshotRow {
	rows := make([]SnapshotRow, count)
	for i := range rows {
		key := fmt.Sprintf("row-%08d", i)
		payload, _ := json.Marshal(map[string]any{"id": key, "name": strings.Repeat("x", width)})
		rows[i] = SnapshotRow{Schema: "main", Table: "users", Key: SyncKey{"id": key}, RowVersion: 1, Payload: payload}
	}
	return rows
}

// Retain the pre-fix candidate-sizing algorithm as a reproducible baseline.
// The per-row validation outside this hot path and final HTTP encode are not timed.
func legacySnapshotChunkSizing(generation SchemaGenerationCapabilities, rows []SnapshotRow) (int64, error) {
	chunkRows := make([]SnapshotRow, 0, len(rows))
	var size int64
	for i, row := range rows {
		if _, _, err := CompactGroupsForSnapshotRowsAt(generation, []SnapshotRow{row}, int64(i+1)); err != nil {
			return 0, err
		}
		candidate := append(append([]SnapshotRow(nil), chunkRows...), row)
		groups, logical, err := CompactGroupsForSnapshotRowsAt(generation, candidate, 1)
		if err != nil {
			return 0, err
		}
		body, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: int64(i + 1), LogicalByteCount: logical, HasMore: false})
		if err != nil {
			return 0, err
		}
		size = int64(len(body))
		chunkRows = append(chunkRows, row)
	}
	return size, nil
}

func BenchmarkSnapshotChunkSizing(b *testing.B) {
	for _, width := range []int{640, 3900} {
		for _, count := range []int{250, 500, 1000} {
			rows := snapshotChunkSyntheticRows(count, width)
			for _, algorithm := range []struct {
				name string
				size func(SchemaGenerationCapabilities, []SnapshotRow) (int64, error)
			}{{"Legacy", legacySnapshotChunkSizing}, {"Incremental", incrementalSnapshotChunkSizing}} {
				b.Run(fmt.Sprintf("%s/rows=%d/width=%d", algorithm.name, count, width), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						size, err := algorithm.size(compactTestGeneration(), rows)
						if err != nil || size > 4<<20 {
							b.Fatalf("size=%d err=%v", size, err)
						}
					}
				})
			}
		}
	}
}

func incrementalSnapshotChunkSizing(generation SchemaGenerationCapabilities, rows []SnapshotRow) (int64, error) {
	var sizer snapshotChunkSizer
	var logical, size int64
	for i, row := range rows {
		groups, rowBytes, err := CompactGroupsForSnapshotRowsAt(generation, []SnapshotRow{row}, int64(i+1))
		if err != nil {
			return 0, err
		}
		logical += rowBytes
		if err := sizer.addRow(groups[0]); err != nil {
			return 0, err
		}
		size, err = sizer.decodedBytes(int64(i+1), logical, false)
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

func TestSnapshotChunkSizer_ExactEncoderParity(t *testing.T) {
	generation := compactTestGeneration()
	// Include a two-digit table index and repeat a table non-adjacently.
	for i := 2; i <= 10; i++ {
		generation.Tables = append(generation.Tables, SchemaGenerationTableCapabilities{Schema: "main", Table: fmt.Sprintf("table%d", i), SyncKeyColumns: []string{"id"}})
	}
	rows := []SnapshotRow{
		{Schema: "main", Table: "users", Key: SyncKey{"id": "<&>\u2028\u2029\n\"\\雪"}, RowVersion: 9, Payload: json.RawMessage(`{ "id": "<&>\u2028\u2029", "n": 9007199254740991, "d": 1.25e-7, "v": null }`)},
		{Schema: "main", Table: "users", Key: SyncKey{"id": "next"}, RowVersion: 10, Payload: json.RawMessage(`{"html":"<>&","escaped":"\u003c","newline":"\n"}`)},
		{Schema: "main", Table: "projects", Key: SyncKey{"project_id": json.Number("9007199254740991"), "tenant_id": "тест"}, RowVersion: 99, Payload: json.RawMessage(`{"nested":{"v":[1,true,null]},"n":-0.0}`)},
		{Schema: "main", Table: "table10", Key: SyncKey{"id": "ten"}, RowVersion: 100, Payload: json.RawMessage(`{}`)},
		{Schema: "main", Table: "users", Key: SyncKey{"id": "back"}, RowVersion: math.MaxInt64, Payload: json.RawMessage(`{"last":true}`)},
	}
	for _, first := range []int64{1, 8, 98, 998, math.MaxInt64 - 10} {
		var sizer snapshotChunkSizer
		for count := 0; count <= len(rows); count++ {
			if count > 0 {
				group, _, err := CompactGroupsForSnapshotRowsAt(generation, rows[count-1:count], first+int64(count)-1)
				require.NoError(t, err)
				require.NoError(t, sizer.addRow(group[0]))
			}
			groups, logical, err := CompactGroupsForSnapshotRowsAt(generation, rows[:count], first)
			require.NoError(t, err)
			for _, more := range []bool{false, true} {
				ordinal := first + int64(count) - 1
				body, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: ordinal, LogicalByteCount: logical, HasMore: more})
				require.NoError(t, err)
				size, err := sizer.decodedBytes(ordinal, logical, more)
				require.NoError(t, err)
				require.Equal(t, int64(len(body)), size, "first=%d count=%d more=%t", first, count, more)
			}
		}
	}
}

func TestSnapshotChunkSizer_ThousandRowsAndLinearAllocation(t *testing.T) {
	for _, width := range []int{640, 3900} {
		rows := snapshotChunkSyntheticRows(1000, width)
		generation := compactTestGeneration()
		groups, logical, err := CompactGroupsForSnapshotRowsAt(generation, rows, 1)
		require.NoError(t, err)
		body, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: 1000, LogicalByteCount: logical})
		require.NoError(t, err)
		size, err := incrementalSnapshotChunkSizing(generation, rows)
		require.NoError(t, err)
		require.Equal(t, int64(len(body)), size)
		require.Less(t, size, int64(4<<20))
		measure := func(rows []SnapshotRow) float64 {
			return testing.AllocsPerRun(1, func() { _, err := incrementalSnapshotChunkSizing(generation, rows); require.NoError(t, err) })
		}
		small, large := measure(rows[:250]), measure(rows)
		// Four times the data permits six times the allocations; a prefix
		// re-encode grows roughly sixteen-fold and fails this structural gate.
		require.Less(t, large, small*6, "width=%d small=%f large=%f", width, small, large)
	}
}

func TestSnapshotChunk_ThousandRowsFourMiB(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "chunk_perf", snapshotSessionFixtureOptions{maxRowsPerSnapshotChunk: 1000})
	for batch := 0; batch < 11; batch++ {
		count := 100
		if batch == 10 {
			count = 1
		}
		rows := make([]PushRequestRow, count)
		for i := range rows {
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", batch*100+i)
			payload, err := json.Marshal(map[string]any{"id": id, "name": strings.Repeat("x", 3900), "email": id + "@example.com"})
			require.NoError(t, err)
			rows[i] = PushRequestRow{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": id}, Op: OpInsert, Payload: payload}
		}
		_, err := pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, int64(batch+1), rows)
		require.NoError(t, err)
		acknowledgeRetainedOutcomeForTest(t, fixture.ctx, fixture.svc, fixture.writer, testSchemaGenerationProofForService(fixture.svc))
	}
	proof := testSchemaGenerationProofForService(fixture.svc)
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, proof)
	require.NoError(t, err)
	require.Equal(t, int64(1001), session.RowCount)
	ctx, cancel := context.WithTimeout(fixture.ctx, 10*time.Second)
	defer cancel()
	started := time.Now()
	chunk, err := fixture.svc.GetSnapshotChunk(ctx, fixture.reader, proof, session.SnapshotID, 0, 1000, 4<<20)
	require.NoError(t, err)
	t.Logf("1000-row 4MiB chunk selection: %s", time.Since(started))
	require.Len(t, chunk.Rows, 1000)
	require.Equal(t, int64(1000), chunk.NextRowOrdinal)
	require.True(t, chunk.HasMore)
	contract, ok := fixture.svc.schemaGenerationRegistry.Generation(proof.ID)
	require.True(t, ok)
	generation := generationDescriptor(contract)
	groups, logical, err := CompactGroupsForSnapshotRowsAt(generation, chunk.Rows, 1)
	require.NoError(t, err)
	require.Equal(t, logical, chunk.ByteCount)
	body, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: 1000, LogicalByteCount: logical, HasMore: true})
	require.NoError(t, err)
	require.LessOrEqual(t, len(body), 4<<20)
	request := httptest.NewRequest(http.MethodGet, "/sync/snapshot-sessions/"+session.SnapshotID+"?max_rows=1000&max_bytes=4194304", nil)
	request.SetPathValue("snapshot_id", session.SnapshotID)
	setTestSchemaGenerationHeaders(request, fixture.svc)
	request = request.WithContext(ContextWithActor(ctx, fixture.reader))
	recorder := httptest.NewRecorder()
	NewHTTPSyncHandlers(fixture.svc, fixture.svc.logger).HandleGetSnapshotChunk(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, body, recorder.Body.Bytes())
	last, err := fixture.svc.GetSnapshotChunk(ctx, fixture.reader, proof, session.SnapshotID, 1000, 1000, 4<<20)
	require.NoError(t, err)
	require.Len(t, last.Rows, 1)
	require.False(t, last.HasMore)
	require.Equal(t, int64(1001), last.NextRowOrdinal)
	require.Equal(t, session.ByteCount, chunk.ByteCount+last.ByteCount)
	metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	require.Equal(t, int64(1001), metrics.ChunkRetainedRowsHighWater)
	require.Equal(t, int64(1000), metrics.ChunkRowsHighWater)

	// A buffered database result must still observe cancellation during selection.
	cancelCtx, cancelSelection := context.WithCancel(fixture.ctx)
	defer cancelSelection()
	scans := 0
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterSnapshotChunkRowScan: func(snapshotPayloadScanObservation) {
		scans++
		if scans == 3 {
			cancelSelection()
		}
	}}
	_, err = fixture.svc.GetSnapshotChunk(cancelCtx, fixture.reader, proof, session.SnapshotID, 0, 1000, 4<<20)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 3, scans)
	require.Empty(t, fixture.svc.snapshotChunkPermits)
	fixture.svc.snapshotHooks = nil
	retry, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 0, 1000, 4<<20)
	require.NoError(t, err)
	require.Equal(t, chunk, retry)
}

func TestSnapshotChunk_ExactByteBoundaries(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "chunk_exact", snapshotSessionFixtureOptions{})
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		payload, err := json.Marshal(map[string]any{"id": id, "name": "<>&\u2028\u2029雪\n\"\\", "email": "exact@example.com"})
		require.NoError(t, err)
		_, err = pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, int64(i+1), []PushRequestRow{{
			Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": id}, Op: OpInsert, Payload: payload,
		}})
		require.NoError(t, err)
		acknowledgeRetainedOutcomeForTest(t, fixture.ctx, fixture.svc, fixture.writer, testSchemaGenerationProofForService(fixture.svc))
	}
	proof := testSchemaGenerationProofForService(fixture.svc)
	session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, proof)
	require.NoError(t, err)
	full, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 0, 10, 4<<20)
	require.NoError(t, err)
	contract, ok := fixture.svc.schemaGenerationRegistry.Generation(proof.ID)
	require.True(t, ok)
	for _, after := range []int{0, 1, 3} {
		for count := 1; count <= len(full.Rows)-after; count++ {
			groups, logical, err := CompactGroupsForSnapshotRowsAt(generationDescriptor(contract), full.Rows[after:after+count], int64(after+1))
			require.NoError(t, err)
			body, err := json.Marshal(CompactSnapshotChunkResponse{Rows: groups, NextRowOrdinal: int64(after + count), LogicalByteCount: logical, HasMore: false})
			require.NoError(t, err)
			for _, delta := range []int{-1, 0, 1} {
				t.Run(fmt.Sprintf("after=%d/count=%d/delta=%d", after, count, delta), func(t *testing.T) {
					chunk, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, int64(after), 10, int64(len(body)+delta))
					more := after+count < len(full.Rows)
					want := count
					if delta < 0 && !(count == 1 && more) {
						want--
					}
					if want == 0 {
						var tooSmall *SnapshotChunkTooSmallError
						require.ErrorAs(t, err, &tooSmall)
						require.Equal(t, int64(len(body)), tooSmall.RequiredDecodedBodyBytes)
						return
					}
					require.NoError(t, err)
					require.Equal(t, full.Rows[after:after+want], chunk.Rows)
					require.Equal(t, int64(after+want), chunk.NextRowOrdinal)
					require.Equal(t, after+want < len(full.Rows), chunk.HasMore)
					wireGroups, wireLogical, err := CompactGroupsForSnapshotRowsAt(generationDescriptor(contract), chunk.Rows, int64(after+1))
					require.NoError(t, err)
					require.Equal(t, wireLogical, chunk.ByteCount)
					wire, err := json.Marshal(CompactSnapshotChunkResponse{Rows: wireGroups, NextRowOrdinal: chunk.NextRowOrdinal, LogicalByteCount: chunk.ByteCount, HasMore: chunk.HasMore})
					require.NoError(t, err)
					require.LessOrEqual(t, len(wire), len(body)+delta)
				})
			}
		}
	}
	empty, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, proof, session.SnapshotID, 4, 10, 4<<20)
	require.NoError(t, err)
	require.Empty(t, empty.Rows)
	require.False(t, empty.HasMore)
	require.Zero(t, empty.ByteCount)
	require.Equal(t, int64(4), empty.NextRowOrdinal)
}
