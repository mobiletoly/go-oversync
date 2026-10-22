//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const auditResourceSafetyPressureCount = 12

type auditResourceSafetyFixture struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	svc        *SyncService
	schemaName string
	userID     string
	writer     Actor
	reader     Actor
}

func newAuditResourceSafetyFixture(
	t *testing.T,
	scenario string,
	maxConns int32,
	configure func(*ServiceConfig),
) *auditResourceSafetyFixture {
	t.Helper()

	ctx := context.Background()
	databaseURL, managed := provisionIntegrationTestDatabase(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "audit_resource_" + strings.ReplaceAll(scenario, "_", "") + "_" + suffix
	if maxConns <= 0 {
		maxConns = 4
	}
	pool := newAuditConfiguredPool(t, ctx, databaseURL, "audit-resource-"+scenario+"-"+suffix, maxConns)
	require.NoError(t, resetTestSyncSchema(ctx, pool))
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestSchema(cleanupCtx, pool, schemaName); err != nil {
			t.Errorf("drop audit resource-safety schema: %v", err)
		}
		if !managed {
			if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
				t.Errorf("reset caller-managed audit resource-safety database: %v", err)
			}
		}
	})

	config := &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "audit-resource-safety-" + scenario,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}
	if configure != nil {
		configure(config)
	}
	svc := newBootstrappedIntegrationService(t, ctx, pool, config, integrationTestLogger(slog.LevelWarn))
	userID := "audit-resource-" + scenario + "-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	mustInitializeEmptyScope(t, ctx, svc, userID, writer.SourceID)
	mustInitializeEmptyScope(t, ctx, svc, userID, reader.SourceID)

	return &auditResourceSafetyFixture{
		ctx:        ctx,
		pool:       pool,
		svc:        svc,
		schemaName: schemaName,
		userID:     userID,
		writer:     writer,
		reader:     reader,
	}
}

func (f *auditResourceSafetyFixture) userRow(t *testing.T, id uuid.UUID, name string) PushRequestRow {
	t.Helper()

	payload, err := json.Marshal(map[string]string{
		"id":    id.String(),
		"name":  name,
		"email": strings.ToLower(name) + "@example.com",
	})
	require.NoError(t, err)
	return PushRequestRow{
		Schema:         f.schemaName,
		Table:          "users",
		Key:            SyncKey{"id": id.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        payload,
	}
}

func (f *auditResourceSafetyFixture) pushRows(
	t *testing.T,
	actor Actor,
	sourceBundleID int64,
	rows []PushRequestRow,
) *Bundle {
	t.Helper()

	bundle, err := pushRowsOneShot(t, f.ctx, f.svc, actor, sourceBundleID, rows)
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func (f *auditResourceSafetyFixture) count(t *testing.T, query string, args ...any) int64 {
	t.Helper()

	var count int64
	require.NoError(t, f.pool.QueryRow(f.ctx, query, args...).Scan(&count))
	return count
}

func TestAuditResourceSafety_RequestBodySizeCharacterization(t *testing.T) {
	fixture, initializationID := newAuditPushHTTPFixture(t)
	body := strings.Repeat(" ", 16<<10) + auditCompactPushBody(t, fixture, initializationID)

	startedAt := time.Now()
	recorder := runAuditPushRequest(t, fixture, body, "application/json")
	elapsed := time.Since(startedAt)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Less(t, elapsed, 5*time.Second)
	var relation *string
	require.NoError(t, fixture.pool.QueryRow(context.Background(), `SELECT to_regclass('sync.push_sessions')::text`).Scan(&relation))
	require.Nil(t, relation)
	requireAuditMetadataIntegrity(t, context.Background(), fixture.pool)
	t.Logf("accepted direct request body bytes=%d elapsed=%s", len(body), elapsed)
}

func TestAuditResourceSafety_DirectRequestLimitRejectsWithoutPersistence(t *testing.T) {
	ctx := context.Background()
	fixture := newDirectPushFixture(t, ctx, directPushFixtureOptions{maxRowsPerBundle: 1})

	startedAt := time.Now()
	_, err := fixture.push(t, ctx, fixture.writer, 1, []PushRequestRow{fixture.userRow(uuid.New(), "Alpha"), fixture.userRow(uuid.New(), "Bravo")})

	elapsed := time.Since(startedAt)

	var limitErr *PushInvalidError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "push request row count exceeds bounds", limitErr.Message)
	require.Less(t, elapsed, time.Second)
	require.Zero(t, fixture.businessUserCount(t, ctx))
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	t.Logf("rejected direct rows=2 configured_max=1 elapsed=%s", elapsed)
}

func TestAuditResourceSafety_DirectPushAndSnapshotSessionPressureExpiresAndCleansUp(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "session_pressure", 4, nil)
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")
	fixture.pushRows(t, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(t, uuid.New(), "Seed"),
	})

	for i := 0; i < auditResourceSafetyPressureCount; i++ {
		actor := Actor{UserID: fixture.userID, SourceID: fmt.Sprintf("direct-%02d", i)}
		resolveConnectForPush(t, fixture.ctx, fixture.svc, actor, false)
		fixture.pushRows(t, actor, 1, []PushRequestRow{
			fixture.userRow(t, uuid.New(), fmt.Sprintf("Direct%02d", i)),
		})

		_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.NoError(t, err)
	}

	require.Equal(t, int64(auditResourceSafetyPressureCount), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`))
	require.Equal(t, int64(auditResourceSafetyPressureCount*(auditResourceSafetyPressureCount+3)/2), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)

	_, err := fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at = now() - interval '1 second'`)
	require.NoError(t, err)

	startedAt := time.Now()
	postCleanupSnapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`) == 1 &&
			fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`) == int64(auditResourceSafetyPressureCount+1)
	}, 5*time.Second, 10*time.Millisecond)
	elapsed := time.Since(startedAt)

	require.Equal(t, int64(1), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`))
	require.Equal(t, int64(auditResourceSafetyPressureCount+1), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	require.Equal(t, int64(auditResourceSafetyPressureCount+1), postCleanupSnapshot.RowCount)
	require.Less(t, elapsed, 5*time.Second)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf(
		"created direct pushes and lazily reclaimed snapshot_sessions=%d elapsed=%s",
		auditResourceSafetyPressureCount,
		elapsed,
	)
}

func TestAuditResourceSafety_RetainedStorageAndFetchChunksStayWithinConfiguredBounds(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "retained_storage", 4, func(config *ServiceConfig) {
		config.RetainedBundlesPerUser = 2
		config.RetentionPruneBatchSize = 100
		config.MaxRowsPerSnapshotChunk = 2
		config.DefaultRowsPerSnapshotChunk = 2
	})
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")

	var latest *Bundle
	for sourceBundleID := int64(1); sourceBundleID <= 4; sourceBundleID++ {
		rowCount := 1
		if sourceBundleID == 4 {
			rowCount = 3
		}
		rows := make([]PushRequestRow, 0, rowCount)
		for rowIndex := 0; rowIndex < rowCount; rowIndex++ {
			rows = append(rows, fixture.userRow(
				t,
				uuid.New(),
				fmt.Sprintf("Bundle%dRow%d", sourceBundleID, rowIndex+1),
			))
		}
		latest = fixture.pushRows(t, fixture.writer, sourceBundleID, rows)
		if sourceBundleID < 4 {
			acknowledgeRetainedOutcomeForTest(t, fixture.ctx, fixture.svc, fixture.writer, testSchemaGenerationProofForService(fixture.svc))
		}
	}
	require.NotNil(t, latest)
	require.Equal(t, int64(4), latest.BundleSeq)

	var retainedFloor, retainedBundleCount, firstRetainedBundle, lastRetainedBundle int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT users.retained_bundle_floor,
			COUNT(bundle.bundle_seq),
			COALESCE(MIN(bundle.bundle_seq), 0),
			COALESCE(MAX(bundle.bundle_seq), 0)
		FROM sync.user_state AS users
		LEFT JOIN sync.bundle_log AS bundle ON bundle.user_pk = users.user_pk
		WHERE users.user_id = $1
		GROUP BY users.user_pk
	`, fixture.userID).Scan(
		&retainedFloor,
		&retainedBundleCount,
		&firstRetainedBundle,
		&lastRetainedBundle,
	))
	require.Equal(t, int64(2), retainedFloor)
	require.Equal(t, int64(2), retainedBundleCount)
	require.Equal(t, int64(3), firstRetainedBundle)
	require.Equal(t, int64(4), lastRetainedBundle)
	require.Equal(t, int64(4), fixture.count(t, `SELECT COUNT(*) FROM sync.bundle_rows`))

	pull, err := fixture.svc.ProcessPull(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), retainedFloor, 100, 0)
	require.NoError(t, err)
	require.Len(t, pull.Bundles, 2)
	require.Equal(t, int64(3), pull.Bundles[0].BundleSeq)
	require.Equal(t, int64(4), pull.Bundles[1].BundleSeq)

	snapshot, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, int64(6), snapshot.RowCount)
	require.Equal(t, int64(6), fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	snapshotPage, err := fixture.svc.GetSnapshotChunk(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), snapshot.SnapshotID, 0, 100, defaultBytesPerSnapshotChunk)
	require.NoError(t, err)
	require.Len(t, snapshotPage.Rows, 2)
	require.True(t, snapshotPage.HasMore)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf(
		"retained_floor=%d retained_bundles=%d retained_bundle_rows=%d direct_pull_bundles=%d snapshot_rows=%d snapshot_page_rows=%d",
		retainedFloor,
		retainedBundleCount,
		fixture.count(t, `SELECT COUNT(*) FROM sync.bundle_rows`),
		len(pull.Bundles),
		snapshot.RowCount,
		len(snapshotPage.Rows),
	)
}

func TestAuditResourceSafety_SnapshotSessionLimitRejectsAtomicallyWithBoundedState(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "snapshot_limit", 2, func(config *ServiceConfig) {
		config.MaxRowsPerSnapshotSession = 1
	})
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")
	fixture.pushRows(t, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(t, uuid.New(), "Alpha"),
		fixture.userRow(t, uuid.New(), "Bravo"),
	})

	startedAt := time.Now()
	_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	elapsed := time.Since(startedAt)

	var limitErr *SnapshotSessionLimitExceededError
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, "row_count", limitErr.Dimension)
	require.Equal(t, int64(2), limitErr.Actual)
	require.Equal(t, int64(1), limitErr.Limit)
	require.Zero(t, fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_sessions`))
	require.Zero(t, fixture.count(t, `SELECT COUNT(*) FROM sync.snapshot_session_rows`))
	require.Zero(t, fixture.pool.Stat().AcquiredConns())
	require.Less(t, elapsed, 5*time.Second)
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf("snapshot rows=%d configured_max=%d rejected atomically in %s after current full materialization", limitErr.Actual, limitErr.Limit, elapsed)
}

func TestAuditResourceSafety_PoolExhaustionCancelsAndRetryRecovers(t *testing.T) {
	fixture := newAuditResourceSafetyFixture(t, "pool_exhaustion", 2, nil)
	mustInitializeEmptyScope(t, fixture.ctx, fixture.svc, fixture.userID, "initializer")
	fixture.pushRows(t, fixture.writer, 1, []PushRequestRow{
		fixture.userRow(t, uuid.New(), "Alpha"),
	})

	first, err := fixture.pool.Acquire(fixture.ctx)
	require.NoError(t, err)
	firstHeld := true
	t.Cleanup(func() {
		if firstHeld {
			first.Release()
		}
	})
	second, err := fixture.pool.Acquire(fixture.ctx)
	require.NoError(t, err)
	secondHeld := true
	t.Cleanup(func() {
		if secondHeld {
			second.Release()
		}
	})
	require.Equal(t, int32(2), fixture.pool.Stat().MaxConns())
	require.Equal(t, int32(2), fixture.pool.Stat().AcquiredConns())
	canceledBefore := fixture.pool.Stat().CanceledAcquireCount()

	blockedCtx, cancelBlocked := context.WithTimeout(fixture.ctx, 200*time.Millisecond)
	startedAt := time.Now()
	_, err = fixture.svc.ProcessPull(blockedCtx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 1, 0)
	elapsed := time.Since(startedAt)
	cancelBlocked()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	canceledAfter := fixture.pool.Stat().CanceledAcquireCount()
	require.GreaterOrEqual(t, canceledAfter, canceledBefore+1)
	require.Less(t, elapsed, 2*time.Second)

	firstHeld = false
	first.Release()
	retryCtx, cancelRetry := context.WithTimeout(fixture.ctx, 2*time.Second)
	retry, err := fixture.svc.ProcessPull(retryCtx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 1, 0)
	cancelRetry()
	require.NoError(t, err)
	require.NotNil(t, retry)
	require.Len(t, retry.Bundles, 1)

	secondHeld = false
	second.Release()
	require.Zero(t, fixture.pool.Stat().AcquiredConns())
	requireAuditMetadataIntegrity(t, fixture.ctx, fixture.pool)
	t.Logf(
		"pool max_conns=2 canceled_acquire_delta=%d exhausted acquire after %s and recovered on one-slot release",
		canceledAfter-canceledBefore,
		elapsed,
	)
}
