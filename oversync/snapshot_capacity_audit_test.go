//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func BenchmarkAuditSnapshotAdmission(b *testing.B) {
	requireAuditBenchmarkProfile(b, auditBenchmarkProfileTop)
	if b.N != 1 {
		b.Fatalf("run with -benchtime=1x; b.N=%d", b.N)
	}
	for _, requestCount := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("build_requests_%d", requestCount), func(b *testing.B) {
			benchmarkAuditSnapshotBuildAdmission(b, requestCount)
		})
	}
	for _, requestCount := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("chunk_requests_%d_max_budget", requestCount), func(b *testing.B) {
			benchmarkAuditSnapshotChunkAdmission(b, requestCount)
		})
	}
}

func benchmarkAuditSnapshotBuildAdmission(b *testing.B, requestCount int) {
	fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("snapshot_build_capacity_%d", requestCount))
	ctx, cancel := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
	defer cancel()
	if err := auditBenchmarkSeedSnapshotRows(ctx, fixture, 0); err != nil {
		b.Fatal(err)
	}
	acceptedCount := min(requestCount, defaultMaxConcurrentSnapshotBuilds)
	reached, release := make(chan struct{}, acceptedCount), make(chan struct{})
	fixture.svc.snapshotHooks = &snapshotTestHooks{afterBuildPermit: func(context.Context) error { reached <- struct{}{}; <-release; return nil }}
	results := make(chan error, requestCount)
	var wg sync.WaitGroup
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	highWater := startAuditGoMemoryHighWater()
	b.ResetTimer()
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			if err == nil {
				_ = fixture.svc.DeleteSnapshotSession(context.Background(), fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID)
			}
			results <- err
		}()
	}
	for i := 0; i < acceptedCount; i++ {
		<-reached
	}
	syncProbeElapsed := benchmarkAuditSnapshotAdmissionSyncProbe(b, fixture)
	close(release)
	wg.Wait()
	b.StopTimer()
	maxHeap, maxInUse := highWater.finish()
	close(results)
	accepted, rejected := 0, 0
	for err := range results {
		var capacity *SnapshotCapacityError
		if err == nil {
			accepted++
		} else if errors.As(err, &capacity) {
			rejected++
		} else {
			b.Fatal(err)
		}
	}
	if accepted != acceptedCount || rejected != requestCount-acceptedCount {
		b.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}
	metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	b.ReportMetric(float64(accepted), "accepted_builds")
	b.ReportMetric(float64(rejected), "rejected_builds")
	b.ReportMetric(float64(metrics.BuildHighWater), "active_builds_high_water")
	b.ReportMetric(float64(before.HeapAlloc), "baseline_heap_B")
	b.ReportMetric(float64(maxHeap), "peak_heap_B")
	b.ReportMetric(float64(maxHeap-before.HeapAlloc), "peak_heap_delta_B")
	b.ReportMetric(float64(maxInUse-before.HeapInuse), "peak_heap_inuse_delta_B")
	b.ReportMetric(float64(syncProbeElapsed), "sync_probe_ns")
}

func benchmarkAuditSnapshotChunkAdmission(b *testing.B, requestCount int) {
	fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("snapshot_chunk_capacity_%d", requestCount))
	snapshotID, err := seedAuditMaximumBudgetSnapshot(fixture)
	if err != nil {
		b.Fatal(err)
	}
	acceptedCount := min(requestCount, defaultMaxConcurrentSnapshotChunkRequests)
	release := make(chan struct{})
	reached := make(chan struct{}, acceptedCount)
	writers := make([]*blockingSnapshotResponseWriter, requestCount)
	results := make(chan int, requestCount)
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
	var wg sync.WaitGroup
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	highWater := startAuditGoMemoryHighWater()
	b.ResetTimer()
	for i := 0; i < requestCount; i++ {
		writers[i] = newBlockingSnapshotResponseWriter(release)
		writers[i].onStarted = func() { reached <- struct{}{} }
		writers[i].discardBody = true
		wg.Add(1)
		go func(writer *blockingSnapshotResponseWriter) {
			defer wg.Done()
			request := snapshotRequestWithActor(http.MethodGet, "/sync/snapshot-sessions/"+snapshotID+"?max_rows=5000&max_bytes=16777216", snapshotID, fixture.reader, fixture.svc)
			handlers.HandleGetSnapshotChunk(writer, request)
			results <- writer.StatusCode()
		}(writers[i])
	}
	for i := 0; i < acceptedCount; i++ {
		select {
		case <-reached:
		case <-time.After(fixture.operationTimeout):
			b.Fatal("snapshot response writer did not reach the blocked write")
		}
	}
	syncProbeElapsed := benchmarkAuditSnapshotAdmissionSyncProbe(b, fixture)
	close(release)
	wg.Wait()
	b.StopTimer()
	maxHeap, maxInUse := highWater.finish()
	close(results)
	accepted, rejected := 0, 0
	for status := range results {
		if status == http.StatusOK {
			accepted++
		} else if status == http.StatusTooManyRequests {
			rejected++
		} else {
			b.Fatalf("unexpected chunk status %d", status)
		}
	}
	if accepted != acceptedCount || rejected != requestCount-acceptedCount {
		b.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}
	metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	b.ReportMetric(float64(accepted), "accepted_chunks")
	b.ReportMetric(float64(rejected), "rejected_chunks")
	b.ReportMetric(float64(metrics.ChunkHighWater), "active_chunks_high_water")
	b.ReportMetric(float64(metrics.ChunkRowsHighWater), "chunk_rows_high_water")
	b.ReportMetric(float64(metrics.ChunkBytesHighWater), "chunk_bytes_high_water")
	b.ReportMetric(float64(metrics.ChunkRetainedRowsHighWater), "chunk_retained_rows_high_water")
	b.ReportMetric(float64(metrics.ChunkRetainedBytesHighWater), "chunk_retained_bytes_high_water")
	b.ReportMetric(float64(before.HeapAlloc), "baseline_heap_B")
	b.ReportMetric(float64(maxHeap), "peak_heap_B")
	b.ReportMetric(float64(maxHeap-before.HeapAlloc), "peak_heap_delta_B")
	b.ReportMetric(float64(maxInUse-before.HeapInuse), "peak_heap_inuse_delta_B")
	b.ReportMetric(float64(syncProbeElapsed), "sync_probe_ns")
}

func benchmarkAuditSnapshotAdmissionSyncProbe(b *testing.B, fixture *auditDatabaseBenchmarkFixture) time.Duration {
	b.Helper()
	ctx, cancel := context.WithTimeout(fixture.ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	if err := fixture.svc.WithinSyncBundle(ctx, fixture.writer, BundleSource{
		SourceID:       fixture.writer.SourceID,
		SourceBundleID: 1,
	}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.users (id, name, email)
			VALUES ($1, 'snapshot-admission-probe', 'snapshot-admission-probe@example.com')
		`, pgx.Identifier{fixture.schemaName}.Sanitize()), uuid.NewSHA1(uuid.NameSpaceOID, []byte("snapshot-admission-probe\x00"+fixture.writer.UserID)))
		return err
	}); err != nil {
		b.Fatalf("ordinary push probe while snapshot requests are admitted: %v", err)
	}
	if _, err := fixture.svc.ProcessPull(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 10, 0); err != nil {
		b.Fatalf("ordinary pull probe while snapshot requests are admitted: %v", err)
	}
	return time.Since(started)
}

func seedAuditMaximumBudgetSnapshot(fixture *auditDatabaseBenchmarkFixture) (string, error) {
	ctx, cancel := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
	defer cancel()
	if err := auditBenchmarkSeedSnapshotRows(ctx, fixture, 0); err != nil {
		return "", err
	}
	userPK, err := lookupUserPK(ctx, fixture.pool, fixture.reader.UserID)
	if err != nil {
		return "", err
	}
	tableID, err := fixture.svc.tableIDForTable(fixture.schemaName, "users")
	if err != nil {
		return "", err
	}
	snapshotID := uuid.NewString()
	proof := testSchemaGenerationProofForService(fixture.svc)
	payload := json.RawMessage(`{"blob":"` + strings.Repeat("x", 3000) + `"}`)
	const rowCount = 5001
	wireCounts := make([]int64, rowCount)
	var byteCount int64
	for i := 0; i < rowCount; i++ {
		id := auditBenchmarkUUID(91, i)
		wire, err := json.Marshal(SnapshotRow{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": id.String()}, RowVersion: 1, Payload: payload})
		if err != nil {
			return "", err
		}
		wireCounts[i] = int64(len(wire))
		byteCount += wireCounts[i]
	}
	if _, err = fixture.pool.Exec(ctx, `INSERT INTO sync.snapshot_sessions(snapshot_id,user_pk,source_id,schema_generation_id,schema_generation_fingerprint,snapshot_bundle_seq,row_count,byte_count,expires_at) VALUES($1::uuid,$2,$3,$4,$5,1,$6,$7,now()+interval '15 minutes')`, snapshotID, userPK, fixture.reader.SourceID, proof.ID, proof.Fingerprint, rowCount, byteCount); err != nil {
		return "", err
	}
	written, err := fixture.pool.CopyFrom(ctx, pgx.Identifier{"sync", "snapshot_session_rows"}, []string{"snapshot_id", "row_ordinal", "table_id", "key_bytes", "bundle_seq", "payload_wire", "wire_byte_count"}, pgx.CopyFromSlice(rowCount, func(i int) ([]any, error) {
		id := auditBenchmarkUUID(91, i)
		return []any{snapshotID, int64(i + 1), tableID, append([]byte(nil), id[:]...), int64(1), string(payload), wireCounts[i]}, nil
	}))
	if err != nil {
		return "", err
	}
	if written != rowCount {
		return "", fmt.Errorf("wrote %d rows, want %d", written, rowCount)
	}
	return snapshotID, nil
}
