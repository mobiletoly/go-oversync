//go:build perf

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	serverpkg "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

const defaultPerfDatabaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"

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

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))
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

	baseCfg := &config.Config{
		ServerURL:    testServer.URL(),
		DatabaseURL:  perfDatabaseURL,
		JWTSecret:    "perf-test-secret",
		EnableVerify: verify,
		PreserveDB:   verify,
		Logger:       logger,
	}

	start := time.Now()
	if err := runParallelSimulation(ctx, baseCfg, "complex-multi-batch", parallelUsers); err != nil {
		t.Fatalf("run parallel simulation: %v", err)
	}
	duration := time.Since(start)

	t.Logf("scenario=complex-multi-batch parallel=%d verify=%t duration=%s", parallelUsers, verify, duration)

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
