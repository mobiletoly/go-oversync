//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	auditProfileWarmupSamples   = 5
	auditProfileMeasuredSamples = 10
)

type auditProfileTracer struct {
	operations atomic.Int64
	mu         sync.Mutex
	lastSQL    string
}

func (t *auditProfileTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.operations.Add(1)
	t.mu.Lock()
	t.lastSQL = data.SQL
	t.mu.Unlock()
	return ctx
}

func (*auditProfileTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *auditProfileTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	t.operations.Add(1)
	return ctx
}

func (*auditProfileTracer) TraceCopyFromEnd(context.Context, *pgx.Conn, pgx.TraceCopyFromEndData) {}

func (t *auditProfileTracer) reset() {
	t.operations.Store(0)
	t.mu.Lock()
	t.lastSQL = ""
	t.mu.Unlock()
}

func (t *auditProfileTracer) latestSQL() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastSQL
}

type auditProfileFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	svc        *SyncService
	tracer     *auditProfileTracer
	schemaName string
	writer     Actor
	reader     Actor
}

func newAuditProfileFixture(t *testing.T, maxConns int32) *auditProfileFixture {
	t.Helper()

	ctx := context.Background()
	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	tracer := &auditProfileTracer{}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err)
	poolConfig.MaxConns = maxConns
	poolConfig.MinConns = 0
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "oversync-audit-phase6-profile"
	poolConfig.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	var svc *SyncService
	schemaName := ""
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if svc != nil {
			if err := svc.Close(cleanupCtx); err != nil {
				t.Errorf("close audit profile service: %v", err)
			}
		}
		if schemaName != "" {
			if err := dropTestSchema(cleanupCtx, pool, schemaName); err != nil {
				t.Errorf("drop audit profile schema: %v", err)
			}
		}
		if !managed {
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed audit profile database: %v", err)
			}
		}
		pool.Close()
	})
	require.NoError(t, pool.Ping(ctx))
	require.NoError(t, resetTestSyncSchema(ctx, pool))

	schemaName = "audit_profile_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	svc = newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "oversync-audit-phase6-profile",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	userID := "audit-profile-user-" + schemaName
	writer := Actor{UserID: userID, SourceID: "writer"}
	mustInitializeEmptyScope(t, ctx, svc, userID, writer.SourceID)
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, svc, userID, reader.SourceID)

	return &auditProfileFixture{
		ctx:        ctx,
		pool:       pool,
		svc:        svc,
		tracer:     tracer,
		schemaName: schemaName,
		writer:     writer,
		reader:     reader,
	}
}

type auditProfileSample struct {
	duration          time.Duration
	queries           int64
	walBytes          float64
	allocatedBytes    uint64
	mallocs           uint64
	poolAcquires      int64
	poolAcquireTime   time.Duration
	emptyAcquireCount int64
	emptyAcquireTime  time.Duration
}

type auditPoolCounters struct {
	acquires    int64
	acquireTime time.Duration
	emptyCount  int64
	emptyWait   time.Duration
}

func auditProfilePoolCounters(pool *pgxpool.Pool) auditPoolCounters {
	stats := pool.Stat()
	return auditPoolCounters{
		acquires:    stats.AcquireCount(),
		acquireTime: stats.AcquireDuration(),
		emptyCount:  stats.EmptyAcquireCount(),
		emptyWait:   stats.EmptyAcquireWaitTime(),
	}
}

func auditProfileCurrentWAL(t *testing.T, fixture *auditProfileFixture) string {
	t.Helper()
	var lsn string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn))
	return lsn
}

func auditProfileWALDifference(t *testing.T, fixture *auditProfileFixture, after, before string) float64 {
	t.Helper()
	var bytes float64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::float8`, after, before).Scan(&bytes))
	return bytes
}

func measureAuditProfileOperation(t *testing.T, fixture *auditProfileFixture, operation func() error) auditProfileSample {
	t.Helper()

	beforeWAL := auditProfileCurrentWAL(t, fixture)
	var beforeMem runtime.MemStats
	runtime.ReadMemStats(&beforeMem)
	beforePool := auditProfilePoolCounters(fixture.pool)
	fixture.tracer.reset()

	startedAt := time.Now()
	err := operation()
	duration := time.Since(startedAt)
	require.NoError(t, err)
	queryCount := fixture.tracer.operations.Load()
	afterPool := auditProfilePoolCounters(fixture.pool)
	var afterMem runtime.MemStats
	runtime.ReadMemStats(&afterMem)
	afterWAL := auditProfileCurrentWAL(t, fixture)

	return auditProfileSample{
		duration:          duration,
		queries:           queryCount,
		walBytes:          auditProfileWALDifference(t, fixture, afterWAL, beforeWAL),
		allocatedBytes:    afterMem.TotalAlloc - beforeMem.TotalAlloc,
		mallocs:           afterMem.Mallocs - beforeMem.Mallocs,
		poolAcquires:      afterPool.acquires - beforePool.acquires,
		poolAcquireTime:   afterPool.acquireTime - beforePool.acquireTime,
		emptyAcquireCount: afterPool.emptyCount - beforePool.emptyCount,
		emptyAcquireTime:  afterPool.emptyWait - beforePool.emptyWait,
	}
}

func auditProfilePercentile(sorted []float64, percentile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func auditProfileMeanAndCV(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	mean := sum / float64(len(values))
	if mean == 0 {
		return mean, 0
	}
	var squared float64
	for _, value := range values {
		delta := value - mean
		squared += delta * delta
	}
	return mean, math.Sqrt(squared/float64(len(values))) / mean
}

func logAuditProfileSamples(t *testing.T, operation string, samples []auditProfileSample) {
	t.Helper()
	require.Len(t, samples, auditProfileMeasuredSamples)

	durations := make([]float64, len(samples))
	queries := make([]float64, len(samples))
	walBytes := make([]float64, len(samples))
	allocated := make([]float64, len(samples))
	mallocs := make([]float64, len(samples))
	poolAcquires := make([]float64, len(samples))
	poolAcquireTime := make([]float64, len(samples))
	emptyAcquireCount := make([]float64, len(samples))
	emptyAcquireTime := make([]float64, len(samples))
	for i, sample := range samples {
		durations[i] = float64(sample.duration)
		queries[i] = float64(sample.queries)
		walBytes[i] = sample.walBytes
		allocated[i] = float64(sample.allocatedBytes)
		mallocs[i] = float64(sample.mallocs)
		poolAcquires[i] = float64(sample.poolAcquires)
		poolAcquireTime[i] = float64(sample.poolAcquireTime)
		emptyAcquireCount[i] = float64(sample.emptyAcquireCount)
		emptyAcquireTime[i] = float64(sample.emptyAcquireTime)
	}
	sort.Float64s(durations)
	meanDuration, durationCV := auditProfileMeanAndCV(durations)
	meanQueries, _ := auditProfileMeanAndCV(queries)
	meanWAL, _ := auditProfileMeanAndCV(walBytes)
	meanAllocated, _ := auditProfileMeanAndCV(allocated)
	meanMallocs, _ := auditProfileMeanAndCV(mallocs)
	meanPoolAcquires, _ := auditProfileMeanAndCV(poolAcquires)
	meanPoolAcquireTime, _ := auditProfileMeanAndCV(poolAcquireTime)
	meanEmptyCount, _ := auditProfileMeanAndCV(emptyAcquireCount)
	meanEmptyTime, _ := auditProfileMeanAndCV(emptyAcquireTime)

	t.Logf(
		"AUDIT_PROFILE operation=%s warmups=%d samples=%d p50=%s p95=%s p99=%s mean=%s cv=%.4f queries_mean=%.1f wal_bytes_mean=%.0f alloc_bytes_mean=%.0f mallocs_mean=%.0f pool_acquires_mean=%.1f pool_acquire_time_mean=%s empty_acquires_mean=%.1f empty_wait_mean=%s",
		operation,
		auditProfileWarmupSamples,
		len(samples),
		time.Duration(auditProfilePercentile(durations, 0.50)),
		time.Duration(auditProfilePercentile(durations, 0.95)),
		time.Duration(auditProfilePercentile(durations, 0.99)),
		time.Duration(meanDuration),
		durationCV,
		meanQueries,
		meanWAL,
		meanAllocated,
		meanMallocs,
		meanPoolAcquires,
		time.Duration(meanPoolAcquireTime),
		meanEmptyCount,
		time.Duration(meanEmptyTime),
	)
}

func auditProfileMaxRSS(t *testing.T) int64 {
	t.Helper()
	var usage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	return usage.Maxrss
}

func TestAuditProfileMetrics_RepresentativeOperations(t *testing.T) {
	fixture := newAuditProfileFixture(t, 4)
	const totalSamples = auditProfileWarmupSamples + auditProfileMeasuredSamples

	pushSamples := make([]auditProfileSample, 0, auditProfileMeasuredSamples)
	for i := 0; i < totalSamples; i++ {
		sourceBundleID := int64(i + 1)
		rows := auditBenchmarkRows(fixture.schemaName, sourceBundleID, 10)
		sample := measureAuditProfileOperation(t, fixture, func() error {
			_, err := auditBenchmarkPush(fixture.ctx, fixture.svc, fixture.writer, sourceBundleID, rows)
			return err
		})
		if i >= auditProfileWarmupSamples {
			pushSamples = append(pushSamples, sample)
		}
	}
	logAuditProfileSamples(t, "push_10_rows", pushSamples)

	for sourceBundleID := int64(totalSamples + 1); sourceBundleID <= 20; sourceBundleID++ {
		_, err := auditBenchmarkPush(
			fixture.ctx,
			fixture.svc,
			fixture.writer,
			sourceBundleID,
			auditBenchmarkRows(fixture.schemaName, sourceBundleID, 10),
		)
		require.NoError(t, err)
	}

	pullSamples := make([]auditProfileSample, 0, auditProfileMeasuredSamples)
	for i := 0; i < totalSamples; i++ {
		sample := measureAuditProfileOperation(t, fixture, func() error {
			response, err := fixture.svc.ProcessPull(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 20, 20)
			if err == nil && len(response.Bundles) != 20 {
				return fmt.Errorf("pull returned %d bundles, want 20", len(response.Bundles))
			}
			return err
		})
		if i >= auditProfileWarmupSamples {
			pullSamples = append(pullSamples, sample)
		}
	}
	logAuditProfileSamples(t, "pull_20_bundles_200_rows", pullSamples)

	snapshotSamples := make([]auditProfileSample, 0, auditProfileMeasuredSamples)
	for i := 0; i < totalSamples; i++ {
		var snapshotID string
		sample := measureAuditProfileOperation(t, fixture, func() error {
			snapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			if err == nil {
				snapshotID = snapshot.SnapshotID
				if snapshot.RowCount != 200 {
					return fmt.Errorf("snapshot rows=%d, want 200", snapshot.RowCount)
				}
			}
			return err
		})
		require.NoError(t, fixture.svc.DeleteSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), snapshotID))
		if i >= auditProfileWarmupSamples {
			snapshotSamples = append(snapshotSamples, sample)
		}
	}
	logAuditProfileSamples(t, "snapshot_200_rows", snapshotSamples)

	statusSamples := make([]auditProfileSample, 0, auditProfileMeasuredSamples)
	for i := 0; i < totalSamples; i++ {
		sample := measureAuditProfileOperation(t, fixture, func() error {
			_, err := fixture.svc.GetStatus(fixture.ctx)
			return err
		})
		if i >= auditProfileWarmupSamples {
			statusSamples = append(statusSamples, sample)
		}
	}
	logAuditProfileSamples(t, "status_20_bundles", statusSamples)

	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf(
		"AUDIT_RESOURCE goos=%s peak_rss_raw=%d heap_alloc=%d heap_sys=%d total_alloc=%d mallocs=%d goroutines=%d",
		runtime.GOOS,
		auditProfileMaxRSS(t),
		memory.HeapAlloc,
		memory.HeapSys,
		memory.TotalAlloc,
		memory.Mallocs,
		runtime.NumGoroutine(),
	)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
}

func TestAuditProfileMetrics_QueryPlans(t *testing.T) {
	fixture := newAuditProfileFixture(t, 4)
	for sourceBundleID := int64(1); sourceBundleID <= 20; sourceBundleID++ {
		_, err := auditBenchmarkPush(
			fixture.ctx,
			fixture.svc,
			fixture.writer,
			sourceBundleID,
			auditBenchmarkRows(fixture.schemaName, sourceBundleID, 10),
		)
		require.NoError(t, err)
	}
	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.writer.UserID)
	require.NoError(t, err)

	plans := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name: "pull_bundle_page",
			query: `SELECT bundle_seq FROM sync.bundle_log
				WHERE user_pk = $1 AND bundle_seq > $2 AND bundle_seq > $3 AND bundle_seq <= $4
				ORDER BY bundle_seq LIMIT $5`,
			args: []any{userPK, int64(0), int64(0), int64(20), 21},
		},
		{
			name:  "snapshot_row_state",
			query: `SELECT table_id, key_bytes, bundle_seq FROM sync.row_state WHERE user_pk = $1 AND deleted = FALSE`,
			args:  []any{userPK},
		},
		{
			name: "snapshot_business_rows",
			query: fmt.Sprintf(`SELECT CAST(src.id AS text), to_jsonb(src) - '_sync_scope_id'
				FROM %s AS src WHERE src._sync_scope_id = $1 ORDER BY CAST(src.id AS text)`,
				pgx.Identifier{fixture.schemaName, "users"}.Sanitize()),
			args: []any{fixture.writer.UserID},
		},
		{
			name:  "status_bundle_aggregates",
			query: `SELECT COUNT(*), COALESCE(SUM(byte_count), 0) FROM sync.bundle_log`,
		},
	}

	for _, plan := range plans {
		t.Run(plan.name, func(t *testing.T) {
			var raw []byte
			explain := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + plan.query
			require.NoError(t, fixture.pool.QueryRow(fixture.ctx, explain, plan.args...).Scan(&raw))
			require.True(t, json.Valid(raw), "invalid JSON plan: %s", raw)
			var decoded []map[string]any
			require.NoError(t, json.Unmarshal(raw, &decoded))
			require.Len(t, decoded, 1)
			t.Logf("AUDIT_PLAN name=%s json=%s", plan.name, raw)
		})
	}
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
}

func auditSeedStatusBundleCardinality(t *testing.T, fixture *auditProfileFixture, target int64) {
	t.Helper()

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.writer.UserID)
	require.NoError(t, err)
	var current int64
	require.NoError(t, fixture.pool.QueryRow(
		fixture.ctx,
		`SELECT COALESCE(MAX(bundle_seq), 0) FROM sync.bundle_log WHERE user_pk = $1`,
		userPK,
	).Scan(&current))
	require.GreaterOrEqual(t, target, current)
	if target == current {
		return
	}

	require.NoError(t, pgx.BeginFunc(fixture.ctx, fixture.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(fixture.ctx, `
			UPDATE sync.user_state
			SET next_bundle_seq = $2 + 1,
				retained_bundle_floor = 0
			WHERE user_pk = $1
		`, userPK, target); err != nil {
			return err
		}
		if _, err := tx.Exec(fixture.ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id,
				replaced_by_source_id, retirement_reason
			)
			VALUES ($1, 'audit-status-scale', 'active', $2, '', '')
			ON CONFLICT (user_pk, source_id) DO UPDATE
			SET state = 'active',
				max_committed_source_bundle_id = EXCLUDED.max_committed_source_bundle_id,
				replaced_by_source_id = '',
				retirement_reason = ''
		`, userPK, target); err != nil {
			return err
		}
		_, err := tx.Exec(fixture.ctx, `
			INSERT INTO sync.bundle_log (
				user_pk, bundle_seq, source_id, source_bundle_id,
				row_count, byte_count, bundle_hash, canonical_request_hash
			)
			SELECT $1, item, 'audit-status-scale', item, 0, 128,
				decode(md5('audit-status-scale-' || item::text), 'hex'),
				'audit-status-scale-' || item::text
			FROM generate_series($3 + 1, $2) AS item
		`, userPK, target, current)
		return err
	}))
}

func TestAuditProfileMetrics_StatusScaling(t *testing.T) {
	fixture := newAuditProfileFixture(t, 4)
	const totalSamples = auditProfileWarmupSamples + auditProfileMeasuredSamples

	for _, bundleCount := range []int64{0, 1_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("bundles_%d", bundleCount), func(t *testing.T) {
			auditSeedStatusBundleCardinality(t, fixture, bundleCount)

			fixture.tracer.reset()
			status, err := fixture.svc.GetStatus(fixture.ctx)
			require.NoError(t, err)
			require.Equal(t, bundleCount, status.CommittedBundleCount)
			require.Equal(t, bundleCount*128, status.CommittedBundleBytes)
			require.EqualValues(t, 1, fixture.tracer.operations.Load())
			statusSQL := fixture.tracer.latestSQL()
			require.Contains(t, statusSQL, "SELECT COUNT(*) FROM sync.bundle_log")
			require.Contains(t, statusSQL, "SELECT COALESCE(SUM(byte_count), 0) FROM sync.bundle_log")

			samples := make([]auditProfileSample, 0, auditProfileMeasuredSamples)
			for i := 0; i < totalSamples; i++ {
				sample := measureAuditProfileOperation(t, fixture, func() error {
					measuredStatus, statusErr := fixture.svc.GetStatus(fixture.ctx)
					if statusErr == nil && measuredStatus.CommittedBundleCount != bundleCount {
						return fmt.Errorf(
							"status committed bundle count=%d, want %d",
							measuredStatus.CommittedBundleCount,
							bundleCount,
						)
					}
					return statusErr
				})
				if i >= auditProfileWarmupSamples {
					samples = append(samples, sample)
				}
			}
			logAuditProfileSamples(t, fmt.Sprintf("status_%d_bundles", bundleCount), samples)

			var raw []byte
			explain := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statusSQL
			require.NoError(t, fixture.pool.QueryRow(fixture.ctx, explain).Scan(&raw))
			require.True(t, json.Valid(raw), "invalid JSON plan: %s", raw)
			t.Logf("AUDIT_STATUS_PLAN bundles=%d json=%s", bundleCount, raw)
		})
	}
}

func TestAuditProfileMetrics_PoolAndAdvisoryLockWait(t *testing.T) {
	fixture := newAuditProfileFixture(t, 4)
	held := make([]*pgxpool.Conn, 0, 4)
	t.Cleanup(func() {
		for _, conn := range held {
			conn.Release()
		}
	})
	for range 4 {
		conn, err := fixture.pool.Acquire(fixture.ctx)
		require.NoError(t, err)
		held = append(held, conn)
	}
	poolBefore := auditProfilePoolCounters(fixture.pool)
	canceledBefore := fixture.pool.Stat().CanceledAcquireCount()
	poolWaitCtx, cancelPoolWait := context.WithTimeout(fixture.ctx, 100*time.Millisecond)
	poolStarted := time.Now()
	_, err := fixture.pool.Acquire(poolWaitCtx)
	poolWait := time.Since(poolStarted)
	cancelPoolWait()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	for _, conn := range held {
		conn.Release()
	}
	held = nil
	poolAfter := auditProfilePoolCounters(fixture.pool)
	t.Logf(
		"AUDIT_WAIT kind=pool duration=%s canceled_delta=%d empty_count_delta=%d empty_wait_delta=%s",
		poolWait,
		fixture.pool.Stat().CanceledAcquireCount()-canceledBefore,
		poolAfter.emptyCount-poolBefore.emptyCount,
		poolAfter.emptyWait-poolBefore.emptyWait,
	)

	lock := holdAuditScopeLock(t, fixture.ctx, fixture.pool, fixture.writer.UserID)
	row := auditBenchmarkRows(fixture.schemaName, 1, 1)[0]
	hash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(fixture.svc), []PushRequestRow{row})
	require.NoError(t, err)
	request := &OneShotPushRequest{SourceBundleID: 1, CanonicalRequestHash: hash, Rows: []PushRequestRow{row}}
	lockWaitCtx, cancelLockWait := context.WithTimeout(fixture.ctx, 100*time.Millisecond)
	lockStarted := time.Now()
	_, err = fixture.svc.PushOneShot(lockWaitCtx, fixture.writer, testSchemaGenerationProofForService(fixture.svc), request)

	lockWait := time.Since(lockStarted)
	cancelLockWait()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	lock.release(t, fixture.ctx)

	retry, err := pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{row})
	require.NoError(t, err)
	require.NotNil(t, retry)
	t.Logf("AUDIT_WAIT kind=advisory_lock duration=%s retry=success", lockWait)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
}
