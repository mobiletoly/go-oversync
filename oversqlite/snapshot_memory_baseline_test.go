package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

const (
	oversqliteMemoryBaselineRowsEnvironment     = "OVERSQLITE_MEMORY_BASELINE_ROWS"
	oversqliteMemoryBaselineRowBytesEnvironment = "OVERSQLITE_MEMORY_BASELINE_ROW_BYTES"
)

type goMemoryHighWater struct {
	mu        sync.Mutex
	maxHeap   uint64
	maxInUse  uint64
	stop      chan struct{}
	completed chan struct{}
}

func startGoMemoryHighWater() *goMemoryHighWater {
	highWater := &goMemoryHighWater{stop: make(chan struct{}), completed: make(chan struct{})}
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

func (h *goMemoryHighWater) finish() (uint64, uint64) {
	close(h.stop)
	<-h.completed
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxHeap, h.maxInUse
}

func TestOversqliteSnapshotApplyCursorStructuralBound(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	sourcePath := filepath.Join(filepath.Dir(currentFile), "pull.go")
	file, err := parser.ParseFile(token.NewFileSet(), sourcePath, nil, 0)
	require.NoError(t, err)

	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "installStagedSnapshotRowsInTx" {
			target = fn
			break
		}
	}
	require.NotNil(t, target)

	rowsNextCalls := 0
	stagedRowSliceAllocations := 0
	ast.Inspect(target.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Next" {
			rowsNextCalls++
		}
		if len(call.Args) > 0 {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "make" {
				if _, ok := call.Args[0].(*ast.ArrayType); ok {
					stagedRowSliceAllocations++
				}
			}
		}
		return true
	})
	require.Equal(t, 1, rowsNextCalls, "staged apply must retain its single database cursor loop")
	require.Zero(t, stagedRowSliceAllocations, "staged apply must not allocate a snapshot-row slice")
	t.Log("max_applied_in_memory_rows=1 source=single database/sql Rows cursor with scalar scan variables")
}

func BenchmarkOversqliteSnapshotStageApplyBaseline(b *testing.B) {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("GITHUB_ACTIONS")), "true") {
		b.Fatalf("local-heavy Oversqlite snapshot benchmark is forbidden when GITHUB_ACTIONS=true; refusing before fixture or database setup")
	}
	rawRows := os.Getenv(oversqliteMemoryBaselineRowsEnvironment)
	if rawRows == "" {
		b.Skipf("set %s to 10000, 100000, or 1000000", oversqliteMemoryBaselineRowsEnvironment)
	}
	rowCount, err := strconv.Atoi(rawRows)
	if err != nil || (rowCount != 10_000 && rowCount != 100_000 && rowCount != 1_000_000) {
		b.Fatalf("%s must be 10000, 100000, or 1000000, got %q", oversqliteMemoryBaselineRowsEnvironment, rawRows)
	}
	rawRowBytes := os.Getenv(oversqliteMemoryBaselineRowBytesEnvironment)
	rowBytes, err := strconv.Atoi(rawRowBytes)
	if err != nil || (rowBytes != 256 && rowBytes != 1024) {
		b.Fatalf("%s must be 256 or 1024, got %q", oversqliteMemoryBaselineRowBytesEnvironment, rawRowBytes)
	}
	if b.N != 1 {
		b.Fatalf("run this audit benchmark with -benchtime=1x; b.N=%d", b.N)
	}

	ctx := context.Background()
	databasePath := filepath.Join(b.TempDir(), "oversqlite-memory-baseline.sqlite")
	db, err := sql.Open("sqlite3", databasePath)
	require.NoError(b, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	b.Cleanup(func() { require.NoError(b, db.Close()) })
	_, err = db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)`)
	require.NoError(b, err)
	_, err = db.Exec(`PRAGMA journal_mode = WAL`)
	require.NoError(b, err)

	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	config.RetryPolicy = &RetryPolicy{Enabled: false}
	client, err := newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, client.Close()) })
	client.sourceIDGenerator = func() string { return "memory-baseline-source" }
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, map[string]string{"error": "not_found"}), nil
		}
	})}
	require.NoError(b, client.open(ctx))
	_, err = client.Attach(ctx, "memory-baseline-user")
	require.NoError(b, err)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	baselineRSS := currentProcessPeakRSSBytes()
	if baselineRSS == 0 {
		b.Fatal("current process RSS is unavailable on this platform")
	}
	highWater := startGoMemoryHighWater()

	stageStarted := time.Now()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(b, err)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO _sync_snapshot_stage (
			snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
		) VALUES (?, ?, 'main', 'users', ?, ?, ?)
	`)
	require.NoError(b, err)
	var stagedTextBytes, declaredWireBytes int64
	for ordinal := 1; ordinal <= rowCount; ordinal++ {
		id := fmt.Sprintf("user-%09d", ordinal)
		keyJSON, payload, wireBytes := memoryBaselineSnapshotRow(b, id, rowBytes)
		_, err = stmt.ExecContext(ctx, "snapshot-memory-baseline", ordinal, keyJSON, ordinal, payload)
		if err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			b.Fatalf("stage row %d: %v", ordinal, err)
		}
		stagedTextBytes += int64(len("main") + len("users") + len(keyJSON) + len(payload))
		declaredWireBytes += wireBytes
	}
	require.NoError(b, stmt.Close())
	require.NoError(b, tx.Commit())
	stageElapsed := time.Since(stageStarted)

	b.ResetTimer()
	applyStarted := time.Now()
	require.NoError(b, client.markCheckpointRecoveryRequiredLocked(ctx, "explicit_rebuild"))
	options := snapshotApplyOptions{}
	options.PinnedGuard, err = client.pinSnapshotApplyGuard(ctx, options)
	require.NoError(b, err)
	err = client.applyStagedSnapshotLocked(ctx, &oversync.SnapshotSession{
		SnapshotID:        "snapshot-memory-baseline",
		SnapshotBundleSeq: 1,
		RowCount:          int64(rowCount),
		ByteCount:         declaredWireBytes,
	}, options)
	applyElapsed := time.Since(applyStarted)
	b.StopTimer()
	require.NoError(b, err)

	maxHeap, maxHeapInUse := highWater.finish()
	peakRSS := currentProcessPeakRSSBytes()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	var appliedRows, stagedRows int64
	require.NoError(b, db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&appliedRows))
	require.NoError(b, db.QueryRow(`SELECT COUNT(*) FROM _sync_snapshot_stage`).Scan(&stagedRows))
	require.Equal(b, int64(rowCount), appliedRows)
	require.Zero(b, stagedRows)

	peakHeapDelta := uint64(0)
	if maxHeap > before.HeapAlloc {
		peakHeapDelta = maxHeap - before.HeapAlloc
	}
	peakInUseDelta := uint64(0)
	if maxHeapInUse > before.HeapInuse {
		peakInUseDelta = maxHeapInUse - before.HeapInuse
	}
	b.ReportMetric(float64(rowCount), "staged_rows")
	b.ReportMetric(float64(stagedTextBytes), "staged_text_bytes")
	b.ReportMetric(float64(declaredWireBytes), "declared_wire_bytes")
	b.ReportMetric(float64(rowBytes), "encoded_row_bytes")
	b.ReportMetric(1, "stage_batches")
	b.ReportMetric(1, "apply_cursor_passes")
	transferStats := client.SnapshotTransferDiagnostics()
	b.ReportMetric(float64(transferStats.SessionsCreated), "http_snapshot_sessions")
	b.ReportMetric(float64(transferStats.ChunksFetched), "http_snapshot_chunks")
	b.ReportMetric(float64(transferStats.MaxLiveStagedApplyRows), "max_live_staged_apply_rows")
	b.ReportMetric(float64(transferStats.MaxLiveStagedApplyTextBytes), "max_live_staged_apply_text_bytes")
	b.ReportMetric(float64(transferStats.MaxAppliedInMemoryRows), "max_applied_in_memory_rows")
	b.ReportMetric(float64(peakHeapDelta), "peak_heap_delta_B")
	b.ReportMetric(float64(peakInUseDelta), "peak_heap_inuse_delta_B")
	b.ReportMetric(float64(baselineRSS), "baseline_process_rss_B")
	b.ReportMetric(float64(peakRSS), "peak_process_rss_B")
	b.ReportMetric(float64(peakRSS-baselineRSS), "peak_process_rss_delta_B")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "total_alloc_B")
	b.ReportMetric(float64(stageElapsed.Milliseconds()), "stage_ms")
	b.ReportMetric(float64(applyElapsed.Milliseconds()), "apply_ms")
	b.Logf(
		"snapshot_memory_baseline rows=%d encoded_row_bytes=%d declared_wire_bytes=%d staged_text_bytes=%d stage=%s apply=%s baseline_heap=%d peak_heap=%d peak_heap_inuse=%d baseline_process_rss=%d peak_process_rss=%d peak_process_rss_delta=%d total_alloc_delta=%d sqlite_bytes=%d sqlite_wal_bytes=%d http_sessions=%d http_chunks=%d stage_batches=1 apply_cursor_passes=1 max_live_staged_apply_rows=%d max_live_staged_apply_text_bytes=%d max_applied_in_memory_rows=%d",
		rowCount,
		rowBytes,
		declaredWireBytes,
		stagedTextBytes,
		stageElapsed,
		applyElapsed,
		before.HeapAlloc,
		maxHeap,
		maxHeapInUse,
		baselineRSS,
		peakRSS,
		peakRSS-baselineRSS,
		after.TotalAlloc-before.TotalAlloc,
		fileSize(databasePath),
		fileSize(databasePath+"-wal"),
		transferStats.SessionsCreated,
		transferStats.ChunksFetched,
		transferStats.MaxLiveStagedApplyRows,
		transferStats.MaxLiveStagedApplyTextBytes,
		transferStats.MaxAppliedInMemoryRows,
	)
}

func memoryBaselineSnapshotRow(tb testing.TB, id string, targetBytes int) (keyJSON string, payload string, wireBytes int64) {
	tb.Helper()
	keyJSON = fmt.Sprintf(`{"id":%q}`, id)
	makePayload := func(name string) string {
		return fmt.Sprintf(`{"id":%q,"name":%q,"email":%q}`, id, name, id+"@example.com")
	}
	payload = makePayload("")
	encoded := func(payload string) []byte {
		row := oversync.SnapshotRow{
			Schema: "main", Table: "users", Key: oversync.SyncKey{"id": id},
			RowVersion: 1, Payload: json.RawMessage(payload),
		}
		raw, err := json.Marshal(row)
		require.NoError(tb, err)
		return raw
	}
	base := encoded(payload)
	if len(base) > targetBytes {
		tb.Fatalf("baseline row fixed encoding %d exceeds requested %d bytes", len(base), targetBytes)
	}
	payload = makePayload(strings.Repeat("n", targetBytes-len(base)))
	raw := encoded(payload)
	if len(raw) != targetBytes {
		tb.Fatalf("baseline encoded row is %d bytes, expected %d", len(raw), targetBytes)
	}
	return keyJSON, payload, int64(len(raw))
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
