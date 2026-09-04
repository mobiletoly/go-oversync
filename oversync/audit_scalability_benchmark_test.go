//go:build oversync_audit

package oversync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	auditScalabilityFullScaleEnv = "OVERSYNC_AUDIT_FULL_SCALE"
	auditScalabilityWarmups      = 5
	auditScalabilitySamples      = 10
)

type auditScalabilityOperationSample struct {
	duration       time.Duration
	allocatedBytes uint64
	mallocs        uint64
}

// These are fixed-sample capacity profiles rather than adaptive Go microbenchmarks.
// Invoke them with -benchtime=1x so each named profile performs exactly five warmups
// and ten measured samples. Scales above the smallest point additionally require
// OVERSYNC_AUDIT_FULL_SCALE=1.
func BenchmarkAuditScalabilityConcurrentPushes(b *testing.B) {
	if os.Getenv(auditScalabilityFullScaleEnv) != "" && strings.EqualFold(os.Getenv("GITHUB_ACTIONS"), "true") {
		b.Fatalf("full-scale audit benchmarks are local-only and must not run in GitHub Actions")
	}
	topologies := []struct {
		name           string
		distinctScopes bool
	}{
		{name: "same_scope"},
		{name: "distinct_scopes", distinctScopes: true},
	}
	for _, topology := range topologies {
		for _, workerCount := range []int{1, 8, 32, 128} {
			b.Run(fmt.Sprintf("%s/workers_%d", topology.name, workerCount), func(b *testing.B) {
				auditRequireScalabilityProfile(b, workerCount, 1)
				fixture := newAuditDatabaseBenchmarkFixture(
					b,
					fmt.Sprintf("concurrent_%s_%d", topology.name, workerCount),
				)
				actors := auditScalabilityActors(b, fixture, workerCount, topology.distinctScopes)

				for round := 0; round < auditScalabilityWarmups; round++ {
					auditRunConcurrentPushRound(b, fixture, actors, round)
				}
				beforePool := fixture.pool.Stat()
				var beforeMemory runtime.MemStats
				runtime.ReadMemStats(&beforeMemory)
				b.ResetTimer()
				measuredStart := time.Now()
				latencies := make([]time.Duration, 0, workerCount*auditScalabilitySamples)
				roundDurations := make([]time.Duration, 0, auditScalabilitySamples)
				for round := auditScalabilityWarmups; round < auditScalabilityWarmups+auditScalabilitySamples; round++ {
					roundStarted := time.Now()
					roundLatencies := auditRunConcurrentPushRound(b, fixture, actors, round)
					roundDurations = append(roundDurations, time.Since(roundStarted))
					latencies = append(latencies, roundLatencies...)
				}
				measuredElapsed := time.Since(measuredStart)
				b.StopTimer()
				var afterMemory runtime.MemStats
				runtime.ReadMemStats(&afterMemory)

				afterPool := fixture.pool.Stat()
				operationCount := float64(workerCount * auditScalabilitySamples)
				auditReportScalabilityMetrics(
					b,
					latencies,
					roundDurations,
					operationCount,
					measuredElapsed,
					"pushes/s",
				)
				auditReportScalabilityMemoryDelta(b, beforeMemory, afterMemory, operationCount, "push")
				b.ReportMetric(float64(afterPool.MaxConns()), "pool-max-conns")
				b.ReportMetric(
					float64((afterPool.EmptyAcquireWaitTime()-beforePool.EmptyAcquireWaitTime()).Nanoseconds())/operationCount,
					"pool-wait-ns/push",
				)
				b.ReportMetric(
					float64(afterPool.EmptyAcquireCount()-beforePool.EmptyAcquireCount())/operationCount,
					"empty-acquires/push",
				)
				if acquired := fixture.pool.Stat().AcquiredConns(); acquired != 0 {
					b.Fatalf("concurrent push profile leaked %d acquired pool connections", acquired)
				}
			})
		}
	}
}

func BenchmarkAuditScalabilityWatchSubscribers(b *testing.B) {
	modes := []struct {
		name           string
		reconnectChurn bool
	}{
		{name: "slow_clients"},
		{name: "reconnect_churn", reconnectChurn: true},
	}
	for _, subscriberCount := range []int{100, 1_000, 10_000} {
		for _, mode := range modes {
			b.Run(fmt.Sprintf("subscribers_%d/%s", subscriberCount, mode.name), func(b *testing.B) {
				auditRequireScalabilityProfile(b, subscriberCount, 100)
				runtime.GC()
				var beforeMemory runtime.MemStats
				runtime.ReadMemStats(&beforeMemory)
				beforeGoroutines := runtime.NumGoroutine()

				hub := newBundleChangeHub()
				subscribers := auditCreateWatchProfileSubscribers(hub, subscriberCount)
				b.Cleanup(func() {
					auditUnsubscribeWatchProfileSubscribers(subscribers)
				})
				runtime.GC()
				var afterSetupMemory runtime.MemStats
				runtime.ReadMemStats(&afterSetupMemory)
				afterSetupGoroutines := runtime.NumGoroutine()
				churnPerRound := 0
				if mode.reconnectChurn {
					churnPerRound = subscriberCount / 100
					if churnPerRound < 1 {
						churnPerRound = 1
					}
				}

				lastBundleSeq := int64(0)
				for round := 0; round < auditScalabilityWarmups; round++ {
					lastBundleSeq++
					auditRunWatchProfileRound(b, hub, subscribers, lastBundleSeq, round, churnPerRound)
				}
				var afterWarmupMemory runtime.MemStats
				runtime.ReadMemStats(&afterWarmupMemory)
				b.ResetTimer()
				measuredStart := time.Now()
				latencies := make([]time.Duration, 0, auditScalabilitySamples)
				for round := 0; round < auditScalabilitySamples; round++ {
					lastBundleSeq++
					latencies = append(
						latencies,
						auditRunWatchProfileRound(b, hub, subscribers, lastBundleSeq, round, churnPerRound),
					)
				}
				measuredElapsed := time.Since(measuredStart)
				b.StopTimer()
				var afterMeasuredMemory runtime.MemStats
				runtime.ReadMemStats(&afterMeasuredMemory)

				auditAssertSlowSubscribersBounded(b, subscribers, lastBundleSeq)
				cleanupStart := time.Now()
				auditUnsubscribeWatchProfileSubscribers(subscribers)
				cleanupElapsed := time.Since(cleanupStart)
				if remaining := hub.subscriberCount(1); remaining != 0 {
					b.Fatalf("watch profile cleanup left %d subscribers", remaining)
				}
				subscribers = nil

				eventCount := float64(subscriberCount * auditScalabilitySamples)
				auditReportScalabilityMetrics(b, latencies, latencies, eventCount, measuredElapsed, "events/s")
				auditReportScalabilityMemoryDelta(
					b,
					afterWarmupMemory,
					afterMeasuredMemory,
					eventCount,
					"event",
				)
				b.ReportMetric(1, "buffer-slots/subscriber")
				reconnectCount := churnPerRound * auditScalabilitySamples
				b.ReportMetric(float64(reconnectCount), "reconnects")
				if reconnectCount > 0 && afterMeasuredMemory.TotalAlloc >= afterWarmupMemory.TotalAlloc {
					b.ReportMetric(
						float64(afterMeasuredMemory.TotalAlloc-afterWarmupMemory.TotalAlloc)/float64(reconnectCount),
						"workload-alloc-bytes/reconnect",
					)
				}
				if cleanupElapsed > 0 {
					b.ReportMetric(float64(subscriberCount)/cleanupElapsed.Seconds(), "disconnects/s")
				}
				if afterSetupMemory.HeapAlloc >= beforeMemory.HeapAlloc {
					b.ReportMetric(
						float64(afterSetupMemory.HeapAlloc-beforeMemory.HeapAlloc)/float64(subscriberCount),
						"heap-bytes/subscriber",
					)
				}
				b.ReportMetric(float64(afterSetupGoroutines-beforeGoroutines), "subscriber-goroutines")

				runtime.GC()
				deadline := time.Now().Add(time.Second)
				for runtime.NumGoroutine() > beforeGoroutines && time.Now().Before(deadline) {
					runtime.Gosched()
					time.Sleep(time.Millisecond)
				}
				var afterCleanupMemory runtime.MemStats
				runtime.ReadMemStats(&afterCleanupMemory)
				b.ReportMetric(
					float64(int64(afterCleanupMemory.HeapAlloc)-int64(beforeMemory.HeapAlloc)),
					"post-cleanup-heap-bytes",
				)
				b.ReportMetric(
					float64(runtime.NumGoroutine()-beforeGoroutines),
					"post-cleanup-goroutines",
				)
			})
		}
	}
}

func BenchmarkAuditScalabilityBootstrapRegisteredTables(b *testing.B) {
	for _, tableCount := range []int{1, 10, 100, 1_000} {
		b.Run(fmt.Sprintf("tables_%d", tableCount), func(b *testing.B) {
			auditRequireScalabilityProfile(b, tableCount, 1)
			fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("bootstrap_%d", tableCount))
			if err := fixture.svc.Close(context.Background()); err != nil {
				b.Fatalf("close bootstrap setup service: %v", err)
			}
			if err := resetTestSyncSchema(fixture.ctx, fixture.pool); err != nil {
				b.Fatalf("reset sync schema before bootstrap profile: %v", err)
			}
			registeredTables := auditCreateBootstrapProfileTables(b, fixture, tableCount)
			config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registeredTables), AppName: fmt.Sprintf("audit-scalability-bootstrap-%d", tableCount),
				RegisteredTables:               registeredTables,
				RegisteredTableCatalogRevision: 1,
			}

			latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
				return auditRunBootstrapProfileSample(b, fixture, config, measured)
			})

			measuredElapsed := auditDurationSum(latencies)
			workUnits := float64(tableCount * auditScalabilitySamples)
			auditReportScalabilityMetrics(
				b,
				latencies,
				latencies,
				workUnits,
				measuredElapsed,
				"tables/s",
			)
			auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, "table")
		})
	}
}

type auditPopulatedBootstrapScenario struct {
	name         string
	scopeCount   int
	rowsPerScope int
	historyShape string
	pristine     bool
}

type auditPopulatedBootstrapFixture struct {
	ctx       context.Context
	setupPool *pgxpool.Pool
	pool      *pgxpool.Pool
	tracer    *adoptionQueryTracer
	config    *ServiceConfig
	scenario  auditPopulatedBootstrapScenario
	totalRows int
	spoolDir  string
}

type auditTemporaryUsage struct {
	bytes int64
	files int64
}

type auditPopulatedBootstrapEvidence struct {
	postgresTemp auditTemporaryUsage
	spoolBytes   int64
	spoolCreated int64
	spoolBefore  auditTemporaryUsage
	spoolAfter   auditTemporaryUsage
}

func BenchmarkAuditScalabilityBootstrapPopulatedScopes(b *testing.B) {
	scenarios := []auditPopulatedBootstrapScenario{
		{name: "coherent_small", scopeCount: 10, rowsPerScope: 50, historyShape: "retained"},
		{name: "coherent_pruned_medium", scopeCount: 100, rowsPerScope: 500, historyShape: "pruned"},
		{name: "coherent_mixed_medium", scopeCount: 100, rowsPerScope: 500, historyShape: "mixed"},
		{name: "coherent_reference", scopeCount: 500, rowsPerScope: 700, historyShape: "retained_pruned"},
		{name: "pristine_adoption_medium", scopeCount: 100, rowsPerScope: 500, historyShape: "pristine", pristine: true},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			auditRequireScalabilityProfile(b, scenario.scopeCount, 10)
			fixture := newAuditPopulatedBootstrapFixture(b, scenario)
			var queryTotals adoptionQueryCounts
			var evidenceTotals auditPopulatedBootstrapEvidence
			latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
				sample, counts, evidence := fixture.runBootstrapSample(b, measured)
				if measured {
					queryTotals.total += counts.total
					queryTotals.business += counts.business
					queryTotals.scopeEnvelope += counts.scopeEnvelope
					queryTotals.currentRow += counts.currentRow
					queryTotals.currentRowStatements += counts.currentRowStatements
					queryTotals.history += counts.history
					queryTotals.historyStatements += counts.historyStatements
					queryTotals.persistence += counts.persistence
					evidenceTotals.postgresTemp.bytes += evidence.postgresTemp.bytes
					evidenceTotals.postgresTemp.files += evidence.postgresTemp.files
					evidenceTotals.spoolBytes += evidence.spoolBytes
					evidenceTotals.spoolCreated += evidence.spoolCreated
					evidenceTotals.spoolBefore.bytes += evidence.spoolBefore.bytes
					evidenceTotals.spoolBefore.files += evidence.spoolBefore.files
					evidenceTotals.spoolAfter.bytes += evidence.spoolAfter.bytes
					evidenceTotals.spoolAfter.files += evidence.spoolAfter.files
				}
				return sample
			})

			measuredElapsed := auditDurationSum(latencies)
			rowWorkUnits := float64(fixture.totalRows * auditScalabilitySamples)
			scopeWorkUnits := float64(scenario.scopeCount * auditScalabilitySamples)
			auditReportScalabilityMetrics(b, latencies, latencies, rowWorkUnits, measuredElapsed, "rows/s")
			b.ReportMetric(scopeWorkUnits/measuredElapsed.Seconds(), "scopes/s")
			auditReportScalabilityAllocations(b, allocatedBytes, mallocs, rowWorkUnits, "row")
			b.ReportMetric(float64(queryTotals.total)/auditScalabilitySamples, "total-queries/op")
			b.ReportMetric(float64(queryTotals.business)/auditScalabilitySamples, "business-queries/op")
			b.ReportMetric(float64(queryTotals.scopeEnvelope)/auditScalabilitySamples, "scope-envelope-queries/op")
			b.ReportMetric(float64(queryTotals.currentRow)/auditScalabilitySamples, "current-row-queries/op")
			b.ReportMetric(float64(queryTotals.currentRowStatements)/auditScalabilitySamples, "current-row-statements/op")
			b.ReportMetric(float64(queryTotals.history)/auditScalabilitySamples, "history-queries/op")
			b.ReportMetric(float64(queryTotals.historyStatements)/auditScalabilitySamples, "history-statements/op")
			b.ReportMetric(float64(queryTotals.persistence)/auditScalabilitySamples, "persistence-queries/op")
			b.ReportMetric(float64(fixture.totalRows), "observed-business-rows/op")
			b.ReportMetric(float64(scenario.scopeCount), "observed-scopes/op")
			b.ReportMetric(float64(evidenceTotals.postgresTemp.bytes)/auditScalabilitySamples, "postgres-temp-bytes/op")
			b.ReportMetric(float64(evidenceTotals.postgresTemp.files)/auditScalabilitySamples, "postgres-temp-files/op")
			b.ReportMetric(float64(evidenceTotals.spoolBytes)/auditScalabilitySamples, "owned-spool-bytes/op")
			b.ReportMetric(float64(evidenceTotals.spoolCreated)/auditScalabilitySamples, "owned-spool-files/op")
			b.ReportMetric(float64(evidenceTotals.spoolBefore.files)/auditScalabilitySamples, "owned-spool-files-before/op")
			b.ReportMetric(float64(evidenceTotals.spoolAfter.files)/auditScalabilitySamples, "owned-spool-files-after/op")
			b.ReportMetric(float64(evidenceTotals.spoolAfter.bytes)/auditScalabilitySamples, "owned-spool-bytes-after/op")
		})
	}
}

func BenchmarkAuditScalabilityBootstrapExistingLayout(b *testing.B) {
	scenarios := []auditPopulatedBootstrapScenario{
		{name: "empty", scopeCount: 0, rowsPerScope: 0, historyShape: "retained"},
		{name: "reference_500_scopes_350000_rows", scopeCount: 500, rowsPerScope: 700, historyShape: "retained_pruned"},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			auditRequireScalabilityProfile(b, scenario.scopeCount, 0)
			b.StopTimer()
			fixture := newAuditPopulatedBootstrapFixture(b, scenario)
			fixture.tracer.businessSchema = fixture.config.RegisteredTables[0].Schema
			var totalAllocated, totalMallocs uint64
			var totalDuration time.Duration
			var totalQueries int
			for range b.N {
				sample, counts, evidence := fixture.runBootstrapSample(b, true)
				if counts.business != 0 || counts.scopeEnvelope != 0 || counts.currentRow != 0 || counts.history != 0 || counts.persistence != 0 {
					b.Fatalf("existing attachment queried row data or persistence state: %+v", counts)
				}
				if counts.explicitDataLocks != 0 || counts.dml != 0 || counts.ddl != 0 {
					b.Fatalf("existing attachment locked or mutated data: %+v", counts)
				}
				if len(counts.unknown) != 0 {
					b.Fatalf("unclassified existing-attachment SQL: %q", counts.unknown)
				}
				if evidence.spoolCreated != 0 || evidence.spoolBytes != 0 || evidence.spoolAfter.files != 0 || evidence.spoolAfter.bytes != 0 {
					b.Fatalf("existing attachment used adoption spools: %+v", evidence)
				}
				if evidence.postgresTemp != (auditTemporaryUsage{}) {
					b.Fatalf("existing attachment created PostgreSQL temporary files: %+v", evidence.postgresTemp)
				}
				totalAllocated += sample.allocatedBytes
				totalMallocs += sample.mallocs
				totalDuration += sample.duration
				totalQueries += counts.total
			}
			operations := float64(b.N)
			b.ReportMetric(float64(totalDuration.Nanoseconds())/operations, "attach-ns/op")
			b.ReportMetric(float64(totalAllocated)/operations, "allocated-bytes/op")
			b.ReportMetric(float64(totalMallocs)/operations, "mallocs/op")
			b.ReportMetric(float64(totalQueries)/operations, "total-queries/op")
			b.ReportMetric(0, "owned-spool-bytes/op")
			b.ReportMetric(0, "owned-spool-files/op")
			b.ReportMetric(0, "postgres-temp-bytes/op")
			b.ReportMetric(0, "postgres-temp-files/op")
		})
	}
}

func newAuditPopulatedBootstrapFixture(
	b *testing.B,
	scenario auditPopulatedBootstrapScenario,
) *auditPopulatedBootstrapFixture {
	b.Helper()
	base := newAuditDatabaseBenchmarkFixture(b, "populated_"+scenario.name)
	if err := base.svc.Close(context.Background()); err != nil {
		b.Fatalf("close populated benchmark setup service: %v", err)
	}
	if err := resetTestSyncSchema(base.ctx, base.pool); err != nil {
		b.Fatalf("reset sync schema for populated benchmark: %v", err)
	}

	schemaName := fmt.Sprintf(
		"audit_populated_%s_%d",
		scenario.name,
		managedIntegrationDatabaseSequence.Add(1),
	)
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	if _, err := base.pool.Exec(base.ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent)); err != nil {
		b.Fatalf("create populated benchmark schema: %v", err)
	}
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, base.pool, schemaName); err != nil {
			b.Errorf("drop populated benchmark schema: %v", err)
		}
	})

	const tableCount = 8
	registeredTables := make([]RegisteredTable, tableCount)
	for tableIndex := range tableCount {
		tableName := fmt.Sprintf("table_%02d", tableIndex)
		tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
		if _, err := base.pool.Exec(base.ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				_sync_scope_id TEXT NOT NULL,
				id TEXT NOT NULL,
				payload TEXT NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)
		`, tableIdent)); err != nil {
			b.Fatalf("create populated benchmark table %d: %v", tableIndex, err)
		}
		registeredTables[tableIndex] = RegisteredTable{
			Schema:         schemaName,
			Table:          tableName,
			SyncKeyColumns: []string{"id"},
		}
	}

	for tableIndex := range tableCount {
		rowsForTable := scenario.rowsPerScope / tableCount
		if tableIndex < scenario.rowsPerScope%tableCount {
			rowsForTable++
		}
		tableName := fmt.Sprintf("table_%02d", tableIndex)
		tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
		if _, err := base.pool.Exec(base.ctx, fmt.Sprintf(`
			INSERT INTO %s (_sync_scope_id, id, payload)
			SELECT
				'scope-' || lpad(scope_index::text, 6, '0'),
				'key-%02d-' || lpad(row_index::text, 6, '0'),
				repeat('p', 64) || '-' || scope_index::text || '-' || row_index::text
			FROM generate_series(0, $1 - 1) AS scopes(scope_index)
			CROSS JOIN generate_series(0, $2 - 1) AS rows(row_index)
		`, tableIdent, tableIndex), scenario.scopeCount, rowsForTable); err != nil {
			b.Fatalf("seed populated benchmark table %d: %v", tableIndex, err)
		}
	}

	config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registeredTables), AppName: "oversync-audit-populated-" + scenario.name,
		RegisteredTables:               registeredTables,
		RegisteredTableCatalogRevision: 1,
	}
	if !scenario.pristine {
		setupService, err := newTestRuntimeService(base.pool, config, integrationTestLogger(slog.LevelWarn))
		if err != nil {
			b.Fatalf("create populated benchmark setup service: %v", err)
		}
		if err := setupService.Bootstrap(base.ctx); err != nil {
			b.Fatalf("bootstrap populated benchmark setup: %v", err)
		}
		if scenario.historyShape == "mixed" {
			auditSeedPopulatedBenchmarkTombstones(b, base.ctx, setupService, schemaName, scenario.scopeCount)
		}
		if err := setupService.Close(context.Background()); err != nil {
			b.Fatalf("close coherent populated benchmark setup service: %v", err)
		}
		switch scenario.historyShape {
		case "pruned":
			auditPrunePopulatedBenchmarkBaselines(b, base.ctx, base.pool, false)
		case "retained_pruned":
			auditPrunePopulatedBenchmarkBaselines(b, base.ctx, base.pool, true)
		}
	}

	tracer := &adoptionQueryTracer{}
	poolConfig, err := pgxpool.ParseConfig(base.pool.Config().ConnString())
	if err != nil {
		b.Fatalf("parse populated benchmark pool config: %v", err)
	}
	poolConfig.ConnConfig.Tracer = tracer
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "oversync-populated-bootstrap-benchmark"
	pool, err := pgxpool.NewWithConfig(base.ctx, poolConfig)
	if err != nil {
		b.Fatalf("create populated benchmark traced pool: %v", err)
	}
	if err := pool.Ping(base.ctx); err != nil {
		pool.Close()
		b.Fatalf("ping populated benchmark traced pool: %v", err)
	}
	b.Cleanup(pool.Close)
	totalRows := 0
	for _, table := range registeredTables {
		var tableRows int
		if err := base.pool.QueryRow(base.ctx, fmt.Sprintf(
			"SELECT count(*) FROM %s",
			pgx.Identifier{table.Schema, table.Table}.Sanitize(),
		)).Scan(&tableRows); err != nil {
			b.Fatalf("count observed populated benchmark rows for %s.%s: %v", table.Schema, table.Table, err)
		}
		totalRows += tableRows
	}
	if scenario.historyShape == "mixed" && totalRows != 49_900 {
		b.Fatalf("mixed populated benchmark must observe 49,900 business rows after tombstones, got %d", totalRows)
	}

	return &auditPopulatedBootstrapFixture{
		ctx:       base.ctx,
		setupPool: base.pool,
		pool:      pool,
		tracer:    tracer,
		config:    config,
		scenario:  scenario,
		totalRows: totalRows,
		spoolDir:  b.TempDir(),
	}
}

func auditSeedPopulatedBenchmarkTombstones(
	b *testing.B,
	ctx context.Context,
	service *SyncService,
	schemaName string,
	scopeCount int,
) {
	b.Helper()
	for scopeIndex := range scopeCount {
		actor := Actor{
			UserID:   fmt.Sprintf("scope-%06d", scopeIndex),
			SourceID: fmt.Sprintf("benchmark-writer-%06d", scopeIndex),
		}
		row := PushRequestRow{
			Schema:         schemaName,
			Table:          "table_00",
			Key:            SyncKey{"id": "key-00-000000"},
			Op:             OpDelete,
			BaseRowVersion: 1,
		}
		requestHash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(service), []PushRequestRow{row})
		if err != nil {
			b.Fatalf("hash populated benchmark tombstone %d: %v", scopeIndex, err)
		}
		response, err := service.PushOneShot(ctx, actor, testSchemaGenerationProofForService(service), &OneShotPushRequest{
			SourceBundleID:       1,
			CanonicalRequestHash: requestHash,
			Rows:                 []PushRequestRow{row},
		})

		if err != nil {
			b.Fatalf("direct populated benchmark tombstone %d: %v", scopeIndex, err)
		}
		if response.Bundle == nil {
			b.Fatalf("direct populated benchmark tombstone %d returned no bundle", scopeIndex)
		}
	}
}

func auditPrunePopulatedBenchmarkBaselines(
	b *testing.B,
	ctx context.Context,
	pool *pgxpool.Pool,
	halfOnly bool,
) {
	b.Helper()
	condition := "TRUE"
	if halfOnly {
		condition = "users.user_pk % 2 = 0"
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		DELETE FROM sync.bundle_log AS bundles
		USING sync.user_state AS users
		WHERE bundles.user_pk = users.user_pk
		  AND bundles.bundle_seq = 1
		  AND %s
	`, condition)); err != nil {
		b.Fatalf("delete populated benchmark baseline history: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		UPDATE sync.user_state AS users
		SET retained_bundle_floor = 1
		WHERE %s
	`, condition)); err != nil {
		b.Fatalf("advance populated benchmark retained floor: %v", err)
	}
}

func (f *auditPopulatedBootstrapFixture) runBootstrapSample(
	b *testing.B,
	measured bool,
) (auditScalabilityOperationSample, adoptionQueryCounts, auditPopulatedBootstrapEvidence) {
	b.Helper()
	if f.scenario.pristine {
		if err := resetTestSyncSchema(f.ctx, f.setupPool); err != nil {
			b.Fatalf("reset pristine populated benchmark sync schema: %v", err)
		}
	}
	config := *f.config
	service, err := newTestRuntimeService(f.pool, &config, integrationTestLogger(slog.LevelWarn))
	if err != nil {
		b.Fatalf("create populated benchmark service: %v", err)
	}
	service.adoptionSpoolDir = f.spoolDir
	var evidence auditPopulatedBootstrapEvidence
	createdSpools := make(map[string]struct{})
	var spoolCleanupErr error
	if measured {
		evidence.spoolBefore = auditOwnedDirectoryUsage(b, f.spoolDir)
		service.adoptionHooks = &adoptionTestHooks{
			onSpoolCreated: func(_ int, path string) {
				createdSpools[path] = struct{}{}
				evidence.spoolCreated++
			},
			onSpoolRemoved: func(_ int, path string, bytes int64, cleanupErr error) {
				delete(createdSpools, path)
				evidence.spoolBytes += bytes
				spoolCleanupErr = errors.Join(spoolCleanupErr, cleanupErr)
			},
		}
	}
	var beforePostgres auditTemporaryUsage
	if measured {
		beforePostgres = f.postgresTempUsage(b)
	}
	f.tracer.reset()
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	err = service.Bootstrap(f.ctx)
	elapsed := time.Since(startedAt)
	counts := f.tracer.counts()
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
		afterPostgres := f.postgresTempUsage(b)
		evidence.postgresTemp, err = auditTemporaryUsageDelta(beforePostgres, afterPostgres)
		if err != nil {
			b.Fatalf("calculate PostgreSQL temporary usage delta: %v", err)
		}
		evidence.spoolAfter = auditOwnedDirectoryUsage(b, f.spoolDir)
		if spoolCleanupErr != nil {
			b.Fatalf("clean populated benchmark owned spool: %v", spoolCleanupErr)
		}
		if len(createdSpools) != 0 || evidence.spoolAfter.files != 0 || evidence.spoolAfter.bytes != 0 {
			b.Fatalf("populated benchmark leaked owned spools: tracked=%d files=%d bytes=%d", len(createdSpools), evidence.spoolAfter.files, evidence.spoolAfter.bytes)
		}
	}
	if closeErr := service.Close(context.Background()); closeErr != nil {
		b.Fatalf("close populated benchmark service: %v", closeErr)
	}
	if err != nil {
		b.Fatalf("bootstrap populated benchmark sample: %v", err)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory), counts, evidence
}

func (f *auditPopulatedBootstrapFixture) postgresTempUsage(b *testing.B) auditTemporaryUsage {
	b.Helper()
	if _, err := f.pool.Exec(f.ctx, `SELECT pg_stat_force_next_flush()`); err != nil {
		b.Fatalf("flush populated benchmark PostgreSQL statistics: %v", err)
	}
	var usage auditTemporaryUsage
	if err := f.pool.QueryRow(f.ctx, `
		SELECT temp_bytes, temp_files
		FROM pg_stat_database
		WHERE datname = current_database()
	`).Scan(&usage.bytes, &usage.files); err != nil {
		b.Fatalf("read populated benchmark PostgreSQL temporary usage: %v", err)
	}
	return usage
}

func auditTemporaryUsageDelta(before, after auditTemporaryUsage) (auditTemporaryUsage, error) {
	if after.bytes < before.bytes || after.files < before.files {
		return auditTemporaryUsage{}, fmt.Errorf("temporary usage counters moved backwards: before=%+v after=%+v", before, after)
	}
	return auditTemporaryUsage{bytes: after.bytes - before.bytes, files: after.files - before.files}, nil
}

func TestAuditTemporaryUsageDelta(t *testing.T) {
	delta, err := auditTemporaryUsageDelta(
		auditTemporaryUsage{bytes: 10, files: 2},
		auditTemporaryUsage{bytes: 42, files: 5},
	)
	if err != nil {
		t.Fatalf("calculate temporary usage delta: %v", err)
	}
	if delta != (auditTemporaryUsage{bytes: 32, files: 3}) {
		t.Fatalf("unexpected temporary usage delta: %+v", delta)
	}
	if _, err := auditTemporaryUsageDelta(
		auditTemporaryUsage{bytes: 42, files: 5},
		auditTemporaryUsage{bytes: 10, files: 2},
	); err == nil {
		t.Fatal("expected backwards counters to be rejected")
	}
}

func auditOwnedDirectoryUsage(b *testing.B, dir string) auditTemporaryUsage {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatalf("read populated benchmark owned spool directory: %v", err)
	}
	usage := auditTemporaryUsage{files: int64(len(entries))}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			b.Fatalf("stat populated benchmark owned spool %s: %v", entry.Name(), err)
		}
		usage.bytes += info.Size()
	}
	return usage
}

func BenchmarkAuditScalabilityExpiredSessionCleanup(b *testing.B) {
	cleanupKinds := []string{"snapshot"}
	for _, sessionCount := range []int{100, 10_000} {
		for _, cleanupKind := range cleanupKinds {
			b.Run(fmt.Sprintf("%s/sessions_%d", cleanupKind, sessionCount), func(b *testing.B) {
				auditRequireScalabilityProfile(b, sessionCount, 100)
				fixture := newAuditDatabaseBenchmarkFixture(
					b,
					fmt.Sprintf("cleanup_%s_%d", cleanupKind, sessionCount),
				)
				userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.writer.UserID)
				if err != nil {
					b.Fatalf("lookup cleanup profile user: %v", err)
				}
				var tableID int32
				if err := fixture.pool.QueryRow(fixture.ctx, `
					SELECT table_id
					FROM sync.table_catalog
					WHERE schema_name = $1 AND table_name = 'users'
				`, fixture.schemaName).Scan(&tableID); err != nil {
					b.Fatalf("lookup cleanup profile table: %v", err)
				}

				latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
					return auditRunExpiredCleanupProfileSample(
						b,
						fixture,
						cleanupKind,
						sessionCount,
						userPK,
						tableID,
						measured,
					)
				})

				measuredElapsed := auditDurationSum(latencies)
				workUnits := float64(sessionCount * auditScalabilitySamples)
				auditReportScalabilityMetrics(
					b,
					latencies,
					latencies,
					workUnits,
					measuredElapsed,
					"sessions/s",
				)
				auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, "session")
			})
		}
	}
}

func BenchmarkAuditScalabilityRetentionPruning(b *testing.B) {
	const retainedBundles = 10
	for _, historyCount := range []int{100, 10_000} {
		b.Run(fmt.Sprintf("history_%d", historyCount), func(b *testing.B) {
			auditRequireScalabilityProfile(b, historyCount, 100)
			fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("pruning_%d", historyCount))
			fixture.svc.config.RetainedBundlesPerUser = retainedBundles
			fixture.svc.config.RetentionPruneBatchSize = int64(historyCount)
			userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.writer.UserID)
			if err != nil {
				b.Fatalf("lookup pruning profile user: %v", err)
			}
			var tableID int32
			if err := fixture.pool.QueryRow(fixture.ctx, `
				SELECT table_id
				FROM sync.table_catalog
				WHERE schema_name = $1 AND table_name = 'users'
			`, fixture.schemaName).Scan(&tableID); err != nil {
				b.Fatalf("lookup pruning profile table: %v", err)
			}

			latencies, allocatedBytes, mallocs := auditRunFixedScalabilitySamples(b, func(measured bool) auditScalabilityOperationSample {
				return auditRunPruningProfileSample(
					b,
					fixture,
					historyCount,
					retainedBundles,
					userPK,
					tableID,
					measured,
				)
			})

			measuredElapsed := auditDurationSum(latencies)
			workUnits := float64((historyCount - retainedBundles) * auditScalabilitySamples)
			auditReportScalabilityMetrics(
				b,
				latencies,
				latencies,
				workUnits,
				measuredElapsed,
				"bundles/s",
			)
			auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, "bundle")
		})
	}
}

type auditConcurrentPushResult struct {
	duration time.Duration
	err      error
}

func auditScalabilityActors(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	workerCount int,
	distinctScopes bool,
) []Actor {
	b.Helper()
	actors := make([]Actor, workerCount)
	for worker := range workerCount {
		userID := fixture.writer.UserID
		if distinctScopes {
			userID = fmt.Sprintf("%s-scope-%04d", fixture.writer.UserID, worker)
		}
		actor := Actor{UserID: userID, SourceID: fmt.Sprintf("worker-%04d", worker)}
		response, err := fixture.svc.Connect(fixture.ctx, actor, testSchemaGenerationProofForService(fixture.svc), &ConnectRequest{HasLocalPendingRows: false})
		if err != nil {
			b.Fatalf("initialize concurrency actor %d: %v", worker, err)
		}
		if response.Resolution != "initialize_empty" && response.Resolution != "remote_authoritative" {
			b.Fatalf("initialize concurrency actor %d returned %q", worker, response.Resolution)
		}
		actors[worker] = actor
	}
	return actors
}

func auditRunConcurrentPushRound(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	actors []Actor,
	round int,
) []time.Duration {
	b.Helper()
	results := make(chan auditConcurrentPushResult, len(actors))
	start := make(chan struct{})
	roundCtx, cancelRound := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
	defer cancelRound()
	var workers sync.WaitGroup
	workers.Add(len(actors))
	for worker, actor := range actors {
		go func() {
			defer workers.Done()
			<-start
			startedAt := time.Now()
			rowSeed := int64(round*len(actors) + worker + 1)
			_, err := auditBenchmarkPush(
				roundCtx,
				fixture.svc,
				actor,
				int64(round+1),
				auditBenchmarkRows(fixture.schemaName, rowSeed, 1),
			)
			results <- auditConcurrentPushResult{duration: time.Since(startedAt), err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	latencies := make([]time.Duration, 0, len(actors))
	for result := range results {
		if result.err != nil {
			b.Fatalf("concurrent push round %d failed: %v", round, result.err)
		}
		latencies = append(latencies, result.duration)
	}
	return latencies
}

func auditUnsubscribeWatchProfileSubscribers(subscribers []auditWatchProfileSubscriber) {
	for _, subscriber := range subscribers {
		if subscriber.unsubscribe != nil {
			subscriber.unsubscribe()
		}
	}
}

type auditWatchProfileSubscriber struct {
	events      <-chan BundleChangeEvent
	unsubscribe func()
	slow        bool
}

func auditCreateWatchProfileSubscribers(hub *bundleChangeHub, subscriberCount int) []auditWatchProfileSubscriber {
	subscribers := make([]auditWatchProfileSubscriber, subscriberCount)
	for index := range subscriberCount {
		events, unsubscribe := hub.subscribe(context.Background(), 1, 0)
		subscribers[index] = auditWatchProfileSubscriber{
			events:      events,
			unsubscribe: unsubscribe,
			slow:        index%10 == 0,
		}
	}
	return subscribers
}

func auditRunWatchProfileRound(
	b *testing.B,
	hub *bundleChangeHub,
	subscribers []auditWatchProfileSubscriber,
	bundleSeq int64,
	round int,
	churnCount int,
) time.Duration {
	b.Helper()
	startedAt := time.Now()
	roundTimer := time.NewTimer(30 * time.Second)
	defer roundTimer.Stop()
	for offset := 0; offset < churnCount; offset++ {
		index := (round*churnCount + offset) % len(subscribers)
		subscribers[index].unsubscribe()
		events, unsubscribe := hub.subscribe(context.Background(), 1, bundleSeq-1)
		subscribers[index].events = events
		subscribers[index].unsubscribe = unsubscribe
	}
	hub.publish(1, BundleChangeEvent{BundleSeq: bundleSeq})
	for index := range subscribers {
		if subscribers[index].slow {
			continue
		}
		var event BundleChangeEvent
		var ok bool
		select {
		case event, ok = <-subscribers[index].events:
		case <-roundTimer.C:
			b.Fatalf("timed out waiting for fast watch subscriber %d during profile", index)
		}
		if !ok {
			b.Fatalf("fast watch subscriber %d closed during profile", index)
		}
		if event.BundleSeq != bundleSeq {
			b.Fatalf("fast watch subscriber %d received bundle_seq %d, want %d", index, event.BundleSeq, bundleSeq)
		}
	}
	return time.Since(startedAt)
}

func auditAssertSlowSubscribersBounded(
	b *testing.B,
	subscribers []auditWatchProfileSubscriber,
	wantBundleSeq int64,
) {
	b.Helper()
	for index := range subscribers {
		if !subscribers[index].slow {
			continue
		}
		if buffered := len(subscribers[index].events); buffered != 1 {
			b.Fatalf("slow watch subscriber %d buffered %d events, want exactly 1", index, buffered)
		}
		event, ok := <-subscribers[index].events
		if !ok {
			b.Fatalf("slow watch subscriber %d closed during profile", index)
		}
		if event.BundleSeq != wantBundleSeq {
			b.Fatalf("slow watch subscriber %d retained bundle_seq %d, want latest %d", index, event.BundleSeq, wantBundleSeq)
		}
	}
}

func auditCreateBootstrapProfileTables(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	tableCount int,
) []RegisteredTable {
	b.Helper()
	schemaName := fmt.Sprintf("audit_scale_bootstrap_%d_%d", tableCount, managedIntegrationDatabaseSequence.Add(1))
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	if _, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schemaIdent)); err != nil {
		b.Fatalf("create bootstrap profile schema: %v", err)
	}
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, fixture.pool, schemaName); err != nil {
			b.Errorf("drop bootstrap profile schema: %v", err)
		}
	})

	registeredTables := make([]RegisteredTable, tableCount)
	err := pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		var batch pgx.Batch
		for index := range tableCount {
			tableName := fmt.Sprintf("table_%04d", index)
			tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
			batch.Queue(fmt.Sprintf(`
				CREATE TABLE %s (
					_sync_scope_id TEXT NOT NULL,
					id UUID NOT NULL,
					payload TEXT NOT NULL,
					PRIMARY KEY (_sync_scope_id, id)
				)
			`, tableIdent))
			registeredTables[index] = RegisteredTable{
				Schema:         schemaName,
				Table:          tableName,
				SyncKeyColumns: []string{"id"},
			}
		}
		results := tx.SendBatch(fixture.ctx, &batch)
		for index := range tableCount {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("create registered table %d: %w", index, err)
			}
		}
		return results.Close()
	})
	if err != nil {
		b.Fatalf("create bootstrap profile tables: %v", err)
	}
	return registeredTables
}

func auditRunBootstrapProfileSample(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	config *ServiceConfig,
	measured bool,
) auditScalabilityOperationSample {
	b.Helper()
	if err := resetTestSyncSchema(fixture.ctx, fixture.pool); err != nil {
		b.Fatalf("reset sync schema for bootstrap sample: %v", err)
	}
	service, err := newTestRuntimeService(fixture.pool, config, integrationTestLogger(slog.LevelWarn))
	if err != nil {
		b.Fatalf("create bootstrap sample service: %v", err)
	}
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	err = service.Bootstrap(fixture.ctx)
	elapsed := time.Since(startedAt)
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
	}
	if closeErr := service.Close(context.Background()); closeErr != nil {
		b.Fatalf("close bootstrap sample service: %v", closeErr)
	}
	if err != nil {
		b.Fatalf("bootstrap sample failed: %v", err)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory)
}

func auditRunExpiredCleanupProfileSample(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	cleanupKind string,
	sessionCount int,
	userPK int64,
	tableID int32,
	measured bool,
) auditScalabilityOperationSample {
	b.Helper()
	if err := auditSeedExpiredSessions(fixture.ctx, fixture.pool, cleanupKind, sessionCount, userPK, tableID, testSchemaGenerationProofForService(fixture.svc)); err != nil {
		b.Fatalf("seed %s cleanup sample: %v", cleanupKind, err)
	}
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	var err error
	switch cleanupKind {
	case "push":
		b.Fatalf("direct pushes do not create expiring server sessions")
	case "snapshot":
		for {
			var result snapshotCleanupBatchResult
			result, err = fixture.svc.cleanupSnapshotBatch(fixture.ctx)
			if err != nil || result.candidateSessions == 0 {
				break
			}
		}
	default:
		b.Fatalf("unknown cleanup profile kind %q", cleanupKind)
	}
	elapsed := time.Since(startedAt)
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
	}
	if err != nil {
		b.Fatalf("cleanup %s sessions: %v", cleanupKind, err)
	}

	var remaining int64
	query := `
		SELECT
			(SELECT COUNT(*) FROM sync.snapshot_sessions) +
			(SELECT COUNT(*) FROM sync.snapshot_session_rows)
	`
	if err := fixture.pool.QueryRow(fixture.ctx, query).Scan(&remaining); err != nil {
		b.Fatalf("count remaining %s sessions: %v", cleanupKind, err)
	}
	if remaining != 0 {
		b.Fatalf("cleanup left %d %s session or staged-row records", remaining, cleanupKind)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory)
}

func auditSeedExpiredSessions(
	ctx context.Context,
	pool *pgxpool.Pool,
	cleanupKind string,
	sessionCount int,
	userPK int64,
	tableID int32,
	proof SchemaGenerationProof,
) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id,
				replaced_by_source_id, retirement_reason
			)
			SELECT $1, 'audit-expired-' || item::text, 'active', 0, '', ''
			FROM generate_series(1, $2) AS item
			ON CONFLICT (user_pk, source_id) DO NOTHING
		`, userPK, sessionCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.source_generation_bindings (
				user_pk, source_id, schema_generation_id, schema_generation_fingerprint
			)
			SELECT $1, 'audit-expired-' || item::text, $3, $4
			FROM generate_series(1, $2) AS item
			ON CONFLICT (user_pk, source_id) DO NOTHING
		`, userPK, sessionCount, proof.ID, proof.Fingerprint); err != nil {
			return err
		}
		switch cleanupKind {
		case "snapshot":
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.snapshot_sessions (
					snapshot_id, user_pk, source_id,
					schema_generation_id, schema_generation_fingerprint,
					snapshot_bundle_seq,
					row_count, byte_count, expires_at
				)
				SELECT md5('audit-expired-snapshot-' || item::text)::uuid,
					$1,
					'audit-expired-' || item::text,
					$3,
					$4,
					0,
					1,
					2,
					now() - interval '1 second'
				FROM generate_series(1, $2) AS item
			`, userPK, sessionCount, proof.ID, proof.Fingerprint); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO sync.snapshot_session_rows (
					snapshot_id, row_ordinal, table_id, key_bytes,
					bundle_seq, payload_wire, wire_byte_count
				)
				SELECT md5('audit-expired-snapshot-' || item::text)::uuid,
					0,
					$1,
					decode(md5('audit-expired-snapshot-key-' || item::text), 'hex'),
					0,
					'{}'::json,
					2
				FROM generate_series(1, $2) AS item
			`, tableID, sessionCount); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown cleanup profile kind %q", cleanupKind)
		}
		return nil
	})
}

func auditRunPruningProfileSample(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	historyCount int,
	retainedBundles int,
	userPK int64,
	tableID int32,
	measured bool,
) auditScalabilityOperationSample {
	b.Helper()
	if err := auditSeedPruningHistory(fixture.ctx, fixture.pool, historyCount, userPK, tableID); err != nil {
		b.Fatalf("seed pruning sample: %v", err)
	}
	var beforeMemory runtime.MemStats
	if measured {
		runtime.ReadMemStats(&beforeMemory)
		b.StartTimer()
	}
	startedAt := time.Now()
	err := pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		return fixture.svc.applyRetentionPolicyForUser(fixture.ctx, tx, userPK)
	})
	elapsed := time.Since(startedAt)
	var afterMemory runtime.MemStats
	if measured {
		b.StopTimer()
		runtime.ReadMemStats(&afterMemory)
	}
	if err != nil {
		b.Fatalf("apply retention policy: %v", err)
	}

	var retainedFloor, remaining int64
	if err := fixture.pool.QueryRow(fixture.ctx, `
		SELECT users.retained_bundle_floor, COUNT(bundle.bundle_seq)
		FROM sync.user_state AS users
		LEFT JOIN sync.bundle_log AS bundle ON bundle.user_pk = users.user_pk
		WHERE users.user_pk = $1
		GROUP BY users.retained_bundle_floor
	`, userPK).Scan(&retainedFloor, &remaining); err != nil {
		b.Fatalf("verify pruning sample: %v", err)
	}
	if wantFloor := int64(historyCount - retainedBundles); retainedFloor != wantFloor {
		b.Fatalf("retained floor = %d, want %d", retainedFloor, wantFloor)
	}
	if remaining != int64(retainedBundles) {
		b.Fatalf("retained bundle count = %d, want %d", remaining, retainedBundles)
	}
	return auditScalabilitySampleWithMemory(elapsed, measured, beforeMemory, afterMemory)
}

func auditScalabilitySampleWithMemory(
	duration time.Duration,
	measured bool,
	before runtime.MemStats,
	after runtime.MemStats,
) auditScalabilityOperationSample {
	sample := auditScalabilityOperationSample{duration: duration}
	if !measured {
		return sample
	}
	if after.TotalAlloc >= before.TotalAlloc {
		sample.allocatedBytes = after.TotalAlloc - before.TotalAlloc
	}
	if after.Mallocs >= before.Mallocs {
		sample.mallocs = after.Mallocs - before.Mallocs
	}
	return sample
}

func auditSeedPruningHistory(
	ctx context.Context,
	q interface {
		Begin(context.Context) (pgx.Tx, error)
	},
	historyCount int,
	userPK int64,
	tableID int32,
) error {
	return pgx.BeginFunc(ctx, q, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM sync.bundle_log WHERE user_pk = $1`, userPK); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE sync.user_state
			SET next_bundle_seq = $2 + 1,
				retained_bundle_floor = 0
			WHERE user_pk = $1
		`, userPK, historyCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id,
				replaced_by_source_id, retirement_reason
			)
			VALUES ($1, 'audit-prune', 'active', $2, '', '')
			ON CONFLICT (user_pk, source_id) DO UPDATE
			SET state = 'active',
				max_committed_source_bundle_id = EXCLUDED.max_committed_source_bundle_id,
				replaced_by_source_id = '',
				retirement_reason = ''
		`, userPK, historyCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.bundle_log (
				user_pk, bundle_seq, source_id, source_bundle_id,
				row_count, byte_count, bundle_hash
			)
			SELECT $1, item, 'audit-prune', item, 1, 2, decode(md5(item::text), 'hex')
			FROM generate_series(1, $2) AS item
		`, userPK, historyCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.bundle_rows (
				user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire
			)
			SELECT $1, item, 1, $3, decode(md5('key-' || item::text), 'hex'), 1, '{}'::json
			FROM generate_series(1, $2) AS item
		`, userPK, historyCount, tableID); err != nil {
			return err
		}
		return nil
	})
}

func auditRequireScalabilityProfile(b *testing.B, scale, smallestScale int) {
	b.Helper()
	if b.N != 1 {
		b.Fatalf("fixed-sample scalability profiles require -benchtime=1x (benchmark N=%d)", b.N)
	}
	if scale > smallestScale && strings.EqualFold(os.Getenv("GITHUB_ACTIONS"), "true") {
		b.Fatalf("full-scale audit benchmarks are local-only and must not run in GitHub Actions")
	}
	if scale > smallestScale && os.Getenv(auditScalabilityFullScaleEnv) != "1" {
		b.Skipf("scale %d requires %s=1", scale, auditScalabilityFullScaleEnv)
	}
}

func auditRunFixedScalabilitySamples(
	b *testing.B,
	runSample func(measured bool) auditScalabilityOperationSample,
) ([]time.Duration, uint64, uint64) {
	b.Helper()
	b.StopTimer()
	for range auditScalabilityWarmups {
		runSample(false)
	}
	b.ResetTimer()
	b.StopTimer()
	latencies := make([]time.Duration, 0, auditScalabilitySamples)
	var allocatedBytes uint64
	var mallocs uint64
	for range auditScalabilitySamples {
		sample := runSample(true)
		latencies = append(latencies, sample.duration)
		allocatedBytes += sample.allocatedBytes
		mallocs += sample.mallocs
	}
	return latencies, allocatedBytes, mallocs
}

func auditReportScalabilityAllocations(
	b *testing.B,
	allocatedBytes uint64,
	mallocs uint64,
	workUnits float64,
	unit string,
) {
	b.Helper()
	if workUnits <= 0 {
		b.Fatalf("scalability allocation normalization requires positive work units, got %f", workUnits)
	}
	b.ReportMetric(float64(allocatedBytes)/workUnits, fmt.Sprintf("alloc-bytes/%s", unit))
	b.ReportMetric(float64(mallocs)/workUnits, fmt.Sprintf("allocs/%s", unit))
}

func auditReportScalabilityMemoryDelta(
	b *testing.B,
	before runtime.MemStats,
	after runtime.MemStats,
	workUnits float64,
	unit string,
) {
	b.Helper()
	var allocatedBytes uint64
	if after.TotalAlloc >= before.TotalAlloc {
		allocatedBytes = after.TotalAlloc - before.TotalAlloc
	}
	var mallocs uint64
	if after.Mallocs >= before.Mallocs {
		mallocs = after.Mallocs - before.Mallocs
	}
	auditReportScalabilityAllocations(b, allocatedBytes, mallocs, workUnits, unit)
}

func auditReportScalabilityMetrics(
	b *testing.B,
	latencies []time.Duration,
	reproducibilitySamples []time.Duration,
	workUnits float64,
	measuredElapsed time.Duration,
	throughputUnit string,
) {
	b.Helper()
	if len(latencies) == 0 {
		b.Fatal("scalability profile produced no latency samples")
	}
	if len(reproducibilitySamples) == 0 {
		b.Fatal("scalability profile produced no reproducibility samples")
	}
	ordered := append([]time.Duration(nil), latencies...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	mean := auditDurationMean(ordered)
	b.ReportMetric(float64(mean.Nanoseconds()), "ns/op")
	b.ReportMetric(float64(auditDurationPercentile(ordered, 0.50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(auditDurationPercentile(ordered, 0.95).Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(auditDurationPercentile(ordered, 0.99).Nanoseconds()), "p99-ns")
	b.ReportMetric(auditDurationCoefficientOfVariation(reproducibilitySamples)*100, "cv-percent")
	b.ReportMetric(float64(len(ordered)), "samples")
	b.ReportMetric(float64(len(reproducibilitySamples)), "cv-samples")
	if measuredElapsed > 0 {
		b.ReportMetric(workUnits/measuredElapsed.Seconds(), throughputUnit)
	}
}

func auditDurationPercentile(ordered []time.Duration, percentile float64) time.Duration {
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

func auditDurationMean(samples []time.Duration) time.Duration {
	return auditDurationSum(samples) / time.Duration(len(samples))
}

func auditDurationSum(samples []time.Duration) time.Duration {
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	return total
}

func auditDurationCoefficientOfVariation(samples []time.Duration) float64 {
	mean := float64(auditDurationMean(samples))
	if mean == 0 {
		return 0
	}
	var squaredDifferenceSum float64
	for _, sample := range samples {
		difference := float64(sample) - mean
		squaredDifferenceSum += difference * difference
	}
	return math.Sqrt(squaredDifferenceSum/float64(len(samples))) / mean
}
