//go:build oversync_audit

package oversync

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const auditCleanupRowsEnvironment = "OVERSYNC_AUDIT_CLEANUP_ROWS"

type auditGoMemoryHighWater struct {
	mu        sync.Mutex
	maxHeap   uint64
	maxInUse  uint64
	stop      chan struct{}
	completed chan struct{}
}

type auditContainerMemoryHighWater struct {
	mu       sync.Mutex
	baseline uint64
	max      uint64
	err      error
	cancel   context.CancelFunc
	done     chan struct{}
}

func startAuditContainerMemoryHighWater(containerID string) (*auditContainerMemoryHighWater, error) {
	highWater := &auditContainerMemoryHighWater{done: make(chan struct{})}
	if containerID == "" {
		close(highWater.done)
		return highWater, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	highWater.cancel = cancel
	command := exec.CommandContext(ctx, "docker", "stats", "--format", "{{.MemUsage}}", containerID)
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open docker stats output: %w", err)
	}
	if err := command.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start docker stats: %w", err)
	}

	ready := make(chan struct{})
	var readyOnce sync.Once
	go func() {
		defer close(highWater.done)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if !strings.ContainsAny(scanner.Text(), "0123456789") {
				continue
			}
			value, parseErr := parseDockerMemoryUsage(scanner.Text())
			if parseErr != nil {
				highWater.mu.Lock()
				highWater.err = parseErr
				highWater.mu.Unlock()
				readyOnce.Do(func() { close(ready) })
				cancel()
				break
			}
			highWater.mu.Lock()
			if highWater.baseline == 0 {
				highWater.baseline = value
			}
			if value > highWater.max {
				highWater.max = value
			}
			highWater.mu.Unlock()
			readyOnce.Do(func() { close(ready) })
		}
		if scanErr := scanner.Err(); scanErr != nil && ctx.Err() == nil {
			highWater.mu.Lock()
			highWater.err = fmt.Errorf("scan docker stats: %w", scanErr)
			highWater.mu.Unlock()
		}
		if waitErr := command.Wait(); waitErr != nil && ctx.Err() == nil {
			highWater.mu.Lock()
			highWater.err = fmt.Errorf("wait for docker stats: %w", waitErr)
			highWater.mu.Unlock()
		}
		readyOnce.Do(func() { close(ready) })
	}()

	select {
	case <-ready:
		highWater.mu.Lock()
		startErr := highWater.err
		baseline := highWater.baseline
		highWater.mu.Unlock()
		if startErr != nil {
			cancel()
			<-highWater.done
			return nil, startErr
		}
		if baseline == 0 {
			cancel()
			<-highWater.done
			return nil, fmt.Errorf("docker stats ended before reporting container memory")
		}
		return highWater, nil
	case <-time.After(5 * time.Second):
		cancel()
		<-highWater.done
		return nil, fmt.Errorf("docker stats did not report container memory within 5s")
	}
}

func parseDockerMemoryUsage(line string) (uint64, error) {
	valueWithUnit := strings.TrimSpace(strings.SplitN(line, "/", 2)[0])
	numericStart := 0
	for numericStart < len(valueWithUnit) && (valueWithUnit[numericStart] < '0' || valueWithUnit[numericStart] > '9') {
		numericStart++
	}
	numericEnd := numericStart
	for numericEnd < len(valueWithUnit) {
		ch := valueWithUnit[numericEnd]
		if (ch < '0' || ch > '9') && ch != '.' {
			break
		}
		numericEnd++
	}
	if numericEnd == numericStart || numericEnd == len(valueWithUnit) {
		return 0, fmt.Errorf("parse docker memory usage %q", line)
	}
	value, err := strconv.ParseFloat(valueWithUnit[numericStart:numericEnd], 64)
	if err != nil {
		return 0, fmt.Errorf("parse docker memory value %q: %w", line, err)
	}
	unit := strings.TrimSpace(valueWithUnit[numericEnd:])
	multiplier := float64(0)
	switch unit {
	case "B":
		multiplier = 1
	case "KiB":
		multiplier = 1 << 10
	case "MiB":
		multiplier = 1 << 20
	case "GiB":
		multiplier = 1 << 30
	default:
		return 0, fmt.Errorf("unsupported docker memory unit %q in %q", unit, line)
	}
	return uint64(value * multiplier), nil
}

func (h *auditContainerMemoryHighWater) finish() (uint64, uint64, error) {
	if h.cancel != nil {
		h.cancel()
	}
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.baseline, h.max, h.err
}

func TestAuditSnapshotDockerMemoryUsageParser(t *testing.T) {
	tests := map[string]uint64{
		"512B / 1GiB":           512,
		"1.5KiB / 1GiB":         1536,
		"32.25MiB / 32GiB":      33_816_576,
		"1.25GiB / 32GiB":       1_342_177_280,
		"\x1b[H46.86MiB / 8GiB": 49_136_271,
	}
	for input, expected := range tests {
		actual, err := parseDockerMemoryUsage(input)
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}
		if actual != expected {
			t.Fatalf("parse %q = %d, want %d", input, actual, expected)
		}
	}
	if _, err := parseDockerMemoryUsage("unknown"); err == nil {
		t.Fatal("expected malformed docker memory usage to fail")
	}
}

func startAuditGoMemoryHighWater() *auditGoMemoryHighWater {
	highWater := &auditGoMemoryHighWater{stop: make(chan struct{}), completed: make(chan struct{})}
	go func() {
		defer close(highWater.completed)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			highWater.mu.Lock()
			if stats.HeapAlloc > highWater.maxHeap {
				highWater.maxHeap = stats.HeapAlloc
			}
			if stats.HeapInuse > highWater.maxInUse {
				highWater.maxInUse = stats.HeapInuse
			}
			highWater.mu.Unlock()
			select {
			case <-highWater.stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return highWater
}

func (h *auditGoMemoryHighWater) finish() (uint64, uint64) {
	close(h.stop)
	<-h.completed
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxHeap, h.maxInUse
}

func seedAuditExpiredSnapshotSession(
	ctx context.Context,
	fixture *auditDatabaseBenchmarkFixture,
	rowCount int,
) (string, int64, error) {
	userPK, err := lookupUserPK(ctx, fixture.pool, fixture.writer.UserID)
	if err != nil {
		return "", 0, err
	}
	tableID, err := fixture.svc.tableIDForTable(fixture.schemaName, "users")
	if err != nil {
		return "", 0, err
	}
	snapshotID := uuid.NewString()
	proof := testSchemaGenerationProofForService(fixture.svc)
	payload := `{"email":"snapshot@example.com","id":"00000000-0000-0000-0000-000000000000","name":"` + strings.Repeat("x", 150) + `"}`
	byteCount := int64(len(payload) * rowCount)
	if _, err := fixture.pool.Exec(ctx, `
		INSERT INTO sync.snapshot_sessions (
			snapshot_id, user_pk, source_id,
			schema_generation_id, schema_generation_fingerprint,
			snapshot_bundle_seq, row_count, byte_count, expires_at
		) VALUES ($1::uuid, $2, $3, $4, $5, 0, $6, $7, now() - interval '1 minute')
	`, snapshotID, userPK, fixture.writer.SourceID, proof.ID, proof.Fingerprint, rowCount, byteCount); err != nil {
		return "", 0, fmt.Errorf("insert expired snapshot session: %w", err)
	}
	if rowCount == 0 {
		return snapshotID, byteCount, nil
	}
	written, err := fixture.pool.CopyFrom(
		ctx,
		pgx.Identifier{"sync", "snapshot_session_rows"},
		[]string{"snapshot_id", "row_ordinal", "table_id", "key_bytes", "bundle_seq", "payload_wire", "wire_byte_count"},
		pgx.CopyFromSlice(rowCount, func(index int) ([]any, error) {
			id := auditBenchmarkUUID(0, index)
			keyBytes := append([]byte(nil), id[:]...)
			wireBytes := int64(len(payload) + 128)
			return []any{snapshotID, int64(index + 1), tableID, keyBytes, int64(0), payload, wireBytes}, nil
		}),
	)
	if err != nil {
		return "", 0, fmt.Errorf("COPY expired snapshot rows: %w", err)
	}
	if written != int64(rowCount) {
		return "", 0, fmt.Errorf("COPY expired snapshot rows wrote %d, want %d", written, rowCount)
	}
	return snapshotID, byteCount, nil
}

func BenchmarkAuditDatabaseSnapshotBoundedCleanup(b *testing.B) {
	rawRows := os.Getenv(auditCleanupRowsEnvironment)
	if rawRows == "" {
		b.Skipf("set %s to 0, 1000, 100000, or 1000000", auditCleanupRowsEnvironment)
	}
	rowCount, err := strconv.Atoi(rawRows)
	if err != nil || (rowCount != 0 && rowCount != 1_000 && rowCount != 100_000 && rowCount != 1_000_000) {
		b.Fatalf("%s must be 0, 1000, 100000, or 1000000, got %q", auditCleanupRowsEnvironment, rawRows)
	}
	if rowCount >= 100_000 && strings.EqualFold(os.Getenv("GITHUB_ACTIONS"), "true") {
		b.Fatalf("heavy snapshot cleanup benchmark is local-only and must not run in GitHub Actions")
	}
	if b.N != 1 {
		b.Fatalf("run this audit benchmark with -benchtime=1x; b.N=%d", b.N)
	}

	fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("cleanup_%d", rowCount))
	activeSession, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	if err != nil {
		b.Fatalf("create unexpired cleanup-control snapshot: %v", err)
	}
	seedCtx, cancelSeed := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
	snapshotID, payloadBytes, err := seedAuditExpiredSnapshotSession(seedCtx, fixture, rowCount)
	cancelSeed()
	if err != nil {
		auditBenchmarkFatalOperation(b, "seed expired snapshot session", fixture.operationTimeout, err)
	}
	postgresMemory, err := startAuditContainerMemoryHighWater(fixture.postgresContainerID)
	if err != nil {
		b.Fatalf("start PostgreSQL container memory sampler: %v", err)
	}

	countRows := func() (int64, int64) {
		var sessions, rows int64
		if err := fixture.pool.QueryRow(fixture.ctx, `
			SELECT
				(SELECT COUNT(*) FROM sync.snapshot_sessions WHERE snapshot_id = $1::uuid),
				(SELECT COUNT(*) FROM sync.snapshot_session_rows WHERE snapshot_id = $1::uuid)
		`, snapshotID).Scan(&sessions, &rows); err != nil {
			b.Fatalf("count snapshot cleanup rows: %v", err)
		}
		return sessions, rows
	}

	time.Sleep(500 * time.Millisecond)
	idleSessions, idleRows := countRows()
	b.Logf("expired snapshot after idle period sessions=%d rows=%d", idleSessions, idleRows)

	var tableBytes int64
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT pg_total_relation_size('sync.snapshot_session_rows')`).Scan(&tableBytes); err != nil {
		b.Fatalf("measure snapshot row table bytes: %v", err)
	}
	var walStart string
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&walStart); err != nil {
		b.Fatalf("read starting WAL LSN: %v", err)
	}

	var maxGrantedLocks atomic.Int64
	stopLocks := make(chan struct{})
	locksDone := make(chan struct{})
	go func() {
		defer close(locksDone)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			var locks int64
			if err := fixture.pool.QueryRow(context.Background(), `
				SELECT COUNT(*)
				FROM pg_locks
				WHERE granted
				  AND relation IN ('sync.snapshot_sessions'::regclass, 'sync.snapshot_session_rows'::regclass)
			`).Scan(&locks); err == nil {
				for {
					current := maxGrantedLocks.Load()
					if locks <= current || maxGrantedLocks.CompareAndSwap(current, locks) {
						break
					}
				}
			}
			select {
			case <-stopLocks:
				return
			case <-ticker.C:
			}
		}
	}()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	highWater := startAuditGoMemoryHighWater()
	b.ResetTimer()
	type cleanupRunResult struct {
		err              error
		batchCount       int
		maxBatchDuration time.Duration
		elapsed          time.Duration
		tableBytesDuring int64
		walBytesDuring   float64
	}
	expectedBatches := (rowCount + fixture.svc.config.SnapshotCleanupBatchRows - 1) / fixture.svc.config.SnapshotCleanupBatchRows
	midpointBatch := max(1, (expectedBatches+1)/2)
	cleanupBegan := make(chan struct{})
	cleanupDone := make(chan cleanupRunResult, 1)
	go func() {
		close(cleanupBegan)
		started := time.Now()
		result := cleanupRunResult{}
		for {
			batchStarted := time.Now()
			cleanupCtx, cancelCleanup := context.WithTimeout(fixture.ctx, fixture.svc.config.SnapshotCleanupBatchTimeout)
			batch, cleanupErr := fixture.svc.cleanupSnapshotBatch(cleanupCtx)
			cancelCleanup()
			batchDuration := time.Since(batchStarted)
			if batchDuration > result.maxBatchDuration {
				result.maxBatchDuration = batchDuration
			}
			if cleanupErr != nil {
				result.err = cleanupErr
				break
			}
			if batch.candidateSessions == 0 {
				break
			}
			result.batchCount++
			if result.batchCount == midpointBatch {
				if measureErr := fixture.pool.QueryRow(fixture.ctx, `
					SELECT pg_total_relation_size('sync.snapshot_session_rows'),
					       pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)
				`, walStart).Scan(&result.tableBytesDuring, &result.walBytesDuring); measureErr != nil {
					result.err = fmt.Errorf("measure midpoint cleanup storage: %w", measureErr)
					break
				}
			}
		}
		result.elapsed = time.Since(started)
		cleanupDone <- result
	}()
	<-cleanupBegan

	syncProbeStarted := time.Now()
	activeChunk, syncProbeErr := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), activeSession.SnapshotID, 0, 1, defaultBytesPerSnapshotChunk)
	if syncProbeErr == nil && (len(activeChunk.Rows) != 0 || activeChunk.HasMore) {
		syncProbeErr = fmt.Errorf("unexpired frozen empty session changed during cleanup")
	}
	probeRowID := auditBenchmarkUUID(77, 0)
	if syncProbeErr == nil {
		tableIdent := pgx.Identifier{fixture.schemaName, "users"}.Sanitize()
		syncProbeErr = fixture.svc.WithinSyncBundle(fixture.ctx, fixture.writer, BundleSource{SourceID: fixture.writer.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
			_, execErr := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(_sync_scope_id,id,name,email) VALUES($1,$2,'Cleanup probe','cleanup-probe@example.com')`, tableIdent), fixture.writer.UserID, probeRowID)
			return execErr
		})
	}
	if syncProbeErr == nil {
		pull, pullErr := fixture.svc.ProcessPull(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 10, 0)
		if pullErr != nil {
			syncProbeErr = pullErr
		} else if len(pull.Bundles) != 1 || len(pull.Bundles[0].Rows) != 1 {
			syncProbeErr = fmt.Errorf("cleanup sync probe pulled %d bundles", len(pull.Bundles))
		}
	}
	syncProbeElapsed := time.Since(syncProbeStarted)
	cleanupResult := <-cleanupDone
	err = cleanupResult.err
	batchCount := cleanupResult.batchCount
	maxBatchDuration := cleanupResult.maxBatchDuration
	cleanupElapsed := cleanupResult.elapsed
	b.StopTimer()
	close(stopLocks)
	<-locksDone
	if err != nil {
		auditBenchmarkFatalOperation(b, "bounded cleanup expired snapshot session", fixture.operationTimeout, err)
	}
	if syncProbeErr != nil {
		auditBenchmarkFatalOperation(b, "ordinary sync during snapshot cleanup", fixture.operationTimeout, syncProbeErr)
	}
	maxHeap, maxInUse := highWater.finish()
	postgresBaseline, postgresPeak, postgresMemoryErr := postgresMemory.finish()
	if postgresMemoryErr != nil {
		b.Fatalf("sample PostgreSQL container memory: %v", postgresMemoryErr)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	remainingSessions, remainingRows := countRows()
	if remainingSessions != 0 || remainingRows != 0 {
		b.Fatalf("snapshot cleanup left sessions=%d rows=%d", remainingSessions, remainingRows)
	}
	var activeSessionCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM sync.snapshot_sessions WHERE snapshot_id=$1::uuid`, activeSession.SnapshotID).Scan(&activeSessionCount); err != nil {
		b.Fatalf("count unexpired cleanup-control snapshot: %v", err)
	}
	if activeSessionCount != 1 {
		b.Fatalf("cleanup removed unexpired cleanup-control snapshot")
	}
	activeChunkAfter, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), activeSession.SnapshotID, 0, 1, defaultBytesPerSnapshotChunk)
	if err != nil {
		b.Fatalf("read unexpired cleanup-control snapshot after cleanup: %v", err)
	}
	if len(activeChunkAfter.Rows) != 0 || activeChunkAfter.HasMore {
		b.Fatalf("unexpired frozen empty session changed after cleanup")
	}
	var walBytes float64
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)`, walStart).Scan(&walBytes); err != nil {
		b.Fatalf("measure cleanup WAL bytes: %v", err)
	}
	var tableBytesAfter int64
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT pg_total_relation_size('sync.snapshot_session_rows')`).Scan(&tableBytesAfter); err != nil {
		b.Fatalf("measure snapshot row table bytes after cleanup: %v", err)
	}

	peakHeapDelta := uint64(0)
	if maxHeap > before.HeapAlloc {
		peakHeapDelta = maxHeap - before.HeapAlloc
	}
	peakInUseDelta := uint64(0)
	if maxInUse > before.HeapInuse {
		peakInUseDelta = maxInUse - before.HeapInuse
	}
	b.ReportMetric(float64(rowCount), "cleanup_rows")
	b.ReportMetric(float64(batchCount), "cleanup_batches")
	b.ReportMetric(float64(maxBatchDuration.Nanoseconds()), "max_batch_ns")
	metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
	b.ReportMetric(float64(metrics.CleanupDeletedRowsHighWater), "cleanup_rows_high_water")
	b.ReportMetric(float64(metrics.CleanupCandidateSessionsHighWater), "cleanup_sessions_high_water")
	b.ReportMetric(float64(payloadBytes), "payload_bytes")
	b.ReportMetric(float64(tableBytes), "table_bytes_before")
	b.ReportMetric(float64(cleanupResult.tableBytesDuring), "table_bytes_during")
	b.ReportMetric(float64(tableBytesAfter), "table_bytes_after")
	b.ReportMetric(cleanupResult.walBytesDuring, "wal_bytes_during")
	b.ReportMetric(walBytes, "wal_bytes_after")
	b.ReportMetric(float64(maxGrantedLocks.Load()), "granted_locks_high_water")
	b.ReportMetric(float64(peakHeapDelta), "peak_heap_delta_B")
	b.ReportMetric(float64(peakInUseDelta), "peak_heap_inuse_delta_B")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "total_alloc_B")
	b.ReportMetric(float64(postgresBaseline), "postgres_container_baseline_B")
	b.ReportMetric(float64(postgresPeak), "postgres_container_peak_B")
	b.ReportMetric(float64(syncProbeElapsed.Nanoseconds()), "sync_probe_ns")
	b.Logf(
		"snapshot_cleanup_bounded rows=%d cleanup=%s batches=%d max_batch=%s idle_remaining_sessions=%d idle_remaining_rows=%d table_bytes_before=%d table_bytes_during=%d table_bytes_after=%d wal_bytes_during=%.0f wal_bytes_after=%.0f max_granted_locks=%d baseline_heap=%d peak_heap=%d total_alloc_delta=%d",
		rowCount,
		cleanupElapsed,
		batchCount,
		maxBatchDuration,
		idleSessions,
		idleRows,
		tableBytes,
		cleanupResult.tableBytesDuring,
		tableBytesAfter,
		cleanupResult.walBytesDuring,
		walBytes,
		maxGrantedLocks.Load(),
		before.HeapAlloc,
		maxHeap,
		after.TotalAlloc-before.TotalAlloc,
	)
}
