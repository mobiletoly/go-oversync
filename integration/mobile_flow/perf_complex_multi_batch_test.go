//go:build perf

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

const defaultPerfDatabaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"

const perfProcessWorkerEnvironment = "OVERSYNC_PERF_PROCESS_WORKER"

func TestMain(m *testing.M) {
	if os.Getenv(perfProcessWorkerEnvironment) == "1" && os.Getenv(processWorkerEnvironment) == "1" {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		if err := runProcessWorker(ctx, os.Stdin); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type stageMetricsCollector struct {
	mu      sync.Mutex
	byStage map[string]*stageMetricSummary
}

type stageMetricSummary struct {
	Operation    string
	Stage        string
	Samples      int64
	Total        time.Duration
	Max          time.Duration
	TotalCount   int64
	ErrorSamples int64
}

type perfStatusCounter struct {
	handler     http.Handler
	unavailable atomic.Int64
}

type perfStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *perfStatusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *perfStatusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func (w *perfStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (c *perfStatusCounter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status := &perfStatusWriter{ResponseWriter: w}
	c.handler.ServeHTTP(status, r)
	if status.status == http.StatusServiceUnavailable {
		c.unavailable.Add(1)
	}
}

type perfResourcePeaks struct {
	poolAcquired int32
	poolTotal    int32
	heapAlloc    uint64
}

func samplePerfResources(server *serverpkg.TestServer, stop <-chan struct{}) <-chan perfResourcePeaks {
	finished := make(chan perfResourcePeaks, 1)
	go func() {
		var peaks perfResourcePeaks
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		sample := func() {
			pool := server.Pool.Stat()
			if acquired := pool.AcquiredConns(); acquired > peaks.poolAcquired {
				peaks.poolAcquired = acquired
			}
			if total := pool.TotalConns(); total > peaks.poolTotal {
				peaks.poolTotal = total
			}
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			if memory.HeapAlloc > peaks.heapAlloc {
				peaks.heapAlloc = memory.HeapAlloc
			}
		}
		for {
			sample()
			select {
			case <-stop:
				finished <- peaks
				return
			case <-ticker.C:
			}
		}
	}()
	return finished
}

func newStageMetricsCollector() *stageMetricsCollector {
	return &stageMetricsCollector{
		byStage: make(map[string]*stageMetricSummary),
	}
}

func (c *stageMetricsCollector) ObserveStage(_ context.Context, timing oversync.StageTiming) {
	key := timing.Operation + "\x00" + timing.Stage

	c.mu.Lock()
	defer c.mu.Unlock()

	summary := c.byStage[key]
	if summary == nil {
		summary = &stageMetricSummary{
			Operation: timing.Operation,
			Stage:     timing.Stage,
		}
		c.byStage[key] = summary
	}
	summary.Samples++
	summary.Total += timing.Duration
	summary.TotalCount += int64(timing.Count)
	if timing.Duration > summary.Max {
		summary.Max = timing.Duration
	}
	if timing.Error {
		summary.ErrorSamples++
	}
}

func (c *stageMetricsCollector) snapshot() []stageMetricSummary {
	c.mu.Lock()
	defer c.mu.Unlock()

	summaries := make([]stageMetricSummary, 0, len(c.byStage))
	for _, summary := range c.byStage {
		summaries = append(summaries, *summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Total != summaries[j].Total {
			return summaries[i].Total > summaries[j].Total
		}
		if summaries[i].Samples != summaries[j].Samples {
			return summaries[i].Samples > summaries[j].Samples
		}
		if summaries[i].Operation != summaries[j].Operation {
			return summaries[i].Operation < summaries[j].Operation
		}
		return summaries[i].Stage < summaries[j].Stage
	})
	return summaries
}

func TestPerfComplexMultiBatchParallel(t *testing.T) {
	parallelUsers := envInt("OVERSYNC_PERF_PARALLEL", 30)
	concurrency := envInt("OVERSYNC_PERF_CONCURRENCY", parallelUsers)
	verify := envBool("OVERSYNC_PERF_VERIFY", false)
	timeout := envDuration("OVERSYNC_PERF_TIMEOUT", 20*time.Minute)
	databaseURL := strings.TrimSpace(os.Getenv("OVERSYNC_PERF_DATABASE_URL"))
	if databaseURL == "" {
		databaseURL = defaultPerfDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	perfDatabaseURL, cleanupDatabase := createIsolatedPerfDatabase(t, ctx, databaseURL)
	defer cleanupDatabase()

	logWriter := io.Writer(io.Discard)
	if envBool("OVERSYNC_PERF_LOG", false) {
		logWriter = os.Stdout
	}
	logger := slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelWarn}))
	stageMetrics := newStageMetricsCollector()

	testServer, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		DatabaseURL:  perfDatabaseURL,
		JWTSecret:    "perf-test-secret",
		Logger:       logger,
		AppName:      "mobile-flow-perf",
		StageMetrics: stageMetrics,
	})
	if err != nil {
		t.Fatalf("start test server: %v", err)
	}
	defer testServer.Close()
	statusCounter := &perfStatusCounter{handler: testServer.Handler}
	measuredServer := httptest.NewServer(statusCounter)
	defer measuredServer.Close()

	baseCfg := &config.Config{
		ServerURL:    measuredServer.URL,
		DatabaseURL:  perfDatabaseURL,
		JWTSecret:    "perf-test-secret",
		EnableVerify: verify,
		PreserveDB:   verify,
		Logger:       logger,
	}
	t.Setenv(perfProcessWorkerEnvironment, "1")

	stopSampling := make(chan struct{})
	resourcePeaks := samplePerfResources(testServer, stopSampling)
	start := time.Now()
	runErr := runParallelSimulation(ctx, baseCfg, "complex-multi-batch", parallelUsers, concurrency)
	duration := time.Since(start)
	close(stopSampling)
	peaks := <-resourcePeaks
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Logf("read process peak RSS: %v", err)
	}

	t.Logf("scenario=complex-multi-batch parallel=%d concurrency=%d verify=%t projection_slots=%d duration=%s http_503=%d pool_acquired_peak=%d pool_total_peak=%d heap_alloc_sampled_peak_bytes=%d process_peak_rss_raw=%d process_peak_rss_goos=%s",
		parallelUsers, concurrency, verify, envInt("OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", 4), duration,
		statusCounter.unavailable.Load(), peaks.poolAcquired, peaks.poolTotal, peaks.heapAlloc, usage.Maxrss, runtime.GOOS)

	for idx, summary := range stageMetrics.snapshot() {
		if idx >= 12 {
			break
		}
		avg := time.Duration(0)
		if summary.Samples > 0 {
			avg = time.Duration(int64(summary.Total) / summary.Samples)
		}
		t.Logf(
			"stage[%02d] op=%s stage=%s total=%s avg=%s max=%s samples=%d total_count=%d errors=%d",
			idx+1,
			summary.Operation,
			summary.Stage,
			summary.Total,
			avg,
			summary.Max,
			summary.Samples,
			summary.TotalCount,
			summary.ErrorSamples,
		)
	}
	if runErr != nil {
		t.Fatalf("run parallel simulation: %v", runErr)
	}
}

func createIsolatedPerfDatabase(t *testing.T, ctx context.Context, databaseURL string) (string, func()) {
	t.Helper()

	adminURL, perfURL, dbName := buildPerfDatabaseURLs(t, databaseURL)

	adminDB, err := sql.Open("postgres", adminURL)
	if err != nil {
		t.Fatalf("open admin postgres connection: %v", err)
	}

	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		t.Fatalf("ping admin postgres connection: %v", err)
	}

	if _, err := adminDB.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(dbName)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create perf database %s: %v", dbName, err)
	}
	_ = adminDB.Close()

	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		adminDB, err := sql.Open("postgres", adminURL)
		if err != nil {
			t.Logf("open admin postgres connection for cleanup: %v", err)
			return
		}
		defer adminDB.Close()

		if _, err := adminDB.ExecContext(cleanupCtx, `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1
			  AND pid <> pg_backend_pid()
		`, dbName); err != nil {
			t.Logf("terminate perf database backends: %v", err)
		}
		if _, err := adminDB.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS "+pq.QuoteIdentifier(dbName)); err != nil {
			t.Logf("drop perf database %s: %v", dbName, err)
		}
	}

	return perfURL, cleanup
}

func buildPerfDatabaseURLs(t *testing.T, databaseURL string) (adminURL string, perfURL string, dbName string) {
	t.Helper()

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}

	dbName = fmt.Sprintf("oversync_perf_%d", time.Now().UnixNano())

	adminParsed := *parsed
	adminParsed.Path = "/postgres"

	perfParsed := *parsed
	perfParsed.Path = "/" + dbName

	return adminParsed.String(), perfParsed.String(), dbName
}

func envBool(key string, defaultValue bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}

	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

func envInt(key string, defaultValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}

func envDuration(key string, defaultValue time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}
