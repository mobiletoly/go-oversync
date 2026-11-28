//go:build !oversync_audit

package oversync

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSnapshotSessions_TotalLimitsRollBackAfterEarlierPageWasStaged(t *testing.T) {
	tests := []struct {
		name      string
		scenario  string
		dimension string
		setLimit  func(*SyncService, int64)
	}{
		{
			name:      "row count",
			scenario:  "stagedrows",
			dimension: "row_count",
			setLimit: func(service *SyncService, _ int64) {
				service.config.MaxRowsPerSnapshotSession = 1
			},
		},
		{
			name:      "byte count",
			scenario:  "stagedbytes",
			dimension: "logical_byte_count",
			setLimit: func(service *SyncService, firstRowWireBytes int64) {
				service.config.MaxBytesPerSnapshotSession = firstRowWireBytes
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSnapshotSessionFixture(t, tc.scenario, snapshotSessionFixtureOptions{
				snapshotMaterializationBatchRows: 1,
			})
			fixture.pushUser(t, 1, uuid.New(), "Alpha")
			fixture.pushUser(t, 2, uuid.New(), "Bravo")

			prior, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			require.NoError(t, err)
			var firstRowWireBytes int64
			require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
				SELECT wire_byte_count
				FROM sync.snapshot_session_rows
				WHERE snapshot_id=$1::uuid AND row_ordinal=1
			`, prior.SnapshotID).Scan(&firstRowWireBytes))
			priorSessions := fixture.snapshotSessionCountForUser(t)
			priorRows := fixture.snapshotSessionRowCountForUser(t)

			tc.setLimit(fixture.svc, firstRowWireBytes)
			copyCalls := 0
			fixture.svc.snapshotHooks = &snapshotTestHooks{beforeSnapshotCopy: func(context.Context) error {
				copyCalls++
				return nil
			}}
			_, err = fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
			var limit *SnapshotSessionLimitExceededError
			require.ErrorAs(t, err, &limit)
			require.Equal(t, tc.dimension, limit.Dimension)
			require.Equal(t, 1, copyCalls, "the first bounded page must have been copied before the later page exceeded the total limit")
			require.Equal(t, priorSessions, fixture.snapshotSessionCountForUser(t))
			require.Equal(t, priorRows, fixture.snapshotSessionRowCountForUser(t))
		})
	}
}

func TestSnapshotCleanup_TwoInstancesDoNotDoubleCountDeletedRows(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "cleanupnodouble", snapshotSessionFixtureOptions{
		snapshotCleanupInterval:      time.Hour,
		snapshotCleanupBatchRows:     2,
		snapshotCleanupBatchSessions: 2,
	})
	fixture.pushUser(t, 1, uuid.New(), "Alpha")
	for i := 0; i < 4; i++ {
		_, err := fixture.svc.CreateSnapshotSession(fixture.ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
		require.NoError(t, err)
	}
	second, err := newTestRuntimeService(fixture.pool, fixture.svc.config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, second.Bootstrap(fixture.ctx))
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	fixture.svc.stopSnapshotCleanupWorker()
	second.stopSnapshotCleanupWorker()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, fixture.svc.waitSnapshotCleanupWorker(drainCtx))
	require.NoError(t, second.waitSnapshotCleanupWorker(drainCtx))
	cancelDrain()
	_, err = fixture.pool.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second'`)
	require.NoError(t, err)

	results := make(chan snapshotCleanupBatchResult, 2)
	errors := make(chan error, 2)
	for _, service := range []*SyncService{fixture.svc, second} {
		go func(service *SyncService) {
			result, cleanupErr := service.cleanupSnapshotBatch(fixture.ctx)
			results <- result
			errors <- cleanupErr
		}(service)
	}
	require.NoError(t, <-errors)
	require.NoError(t, <-errors)
	first, secondResult := <-results, <-results
	require.Equal(t, int64(4), first.deletedRows+secondResult.deletedRows)
	require.Equal(t, int64(4), first.deletedSessions+secondResult.deletedSessions)
	require.Equal(t, 0, fixture.countRows(t, `SELECT count(*) FROM sync.snapshot_sessions`))
	require.Equal(t, 0, fixture.countRows(t, `SELECT count(*) FROM sync.snapshot_session_rows`))
}

func TestSnapshotCleanup_CloseDrainsActiveBatchAndStopsFutureCleanup(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "cleanupclose", snapshotSessionFixtureOptions{
		snapshotCleanupInterval:     time.Hour,
		snapshotCleanupBatchTimeout: 30 * time.Second,
	})
	session := fixture.createSessionWithUser(t, uuid.New(), "Alpha")
	blocker, err := fixture.pool.Begin(fixture.ctx)
	require.NoError(t, err)
	blockerReleased := false
	defer func() {
		if !blockerReleased {
			_ = blocker.Rollback(context.Background())
		}
	}()
	_, err = blocker.Exec(fixture.ctx, `LOCK TABLE sync.snapshot_sessions IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	_, err = blocker.Exec(fixture.ctx, `UPDATE sync.snapshot_sessions SET expires_at=now()-interval '1 second' WHERE snapshot_id=$1::uuid`, session.SnapshotID)
	require.NoError(t, err)

	workerDone := fixture.svc.snapshotCleanupDone
	require.NotNil(t, workerDone)
	fixture.svc.requestSnapshotCleanup("close_proof")
	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := fixture.pool.QueryRow(fixture.ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname=current_database()
				  AND pid<>pg_backend_pid()
				  AND wait_event_type='Lock'
				  AND query LIKE '%FROM sync.snapshot_sessions%'
				  AND query LIKE '%FOR UPDATE SKIP LOCKED%'
			)
		`).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "cleanup worker never entered the blocked session-selection query")

	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, fixture.svc.Close(closeCtx))
	select {
	case <-workerDone:
	default:
		t.Fatal("cleanup worker did not close its done channel")
	}
	require.Eventually(t, func() bool {
		var active bool
		queryErr := fixture.pool.QueryRow(fixture.ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname=current_database()
				  AND pid<>pg_backend_pid()
				  AND query LIKE '%FROM sync.snapshot_sessions%'
				  AND query LIKE '%FOR UPDATE SKIP LOCKED%'
			)
		`).Scan(&active)
		return queryErr == nil && !active
	}, 5*time.Second, 10*time.Millisecond, "cleanup query remained active after Close")
	require.NoError(t, blocker.Commit(context.Background()))
	blockerReleased = true
	require.Equal(t, 1, fixture.snapshotSessionCount(t, session.SnapshotID))
	require.Equal(t, 1, fixture.snapshotSessionRowCount(t, session.SnapshotID))

	afterClose := fixture.svc.snapshotRuntimeMetricsSnapshot()
	fixture.svc.requestSnapshotCleanup("after_close")
	require.Equal(t, afterClose.CleanupRuns, fixture.svc.snapshotRuntimeMetricsSnapshot().CleanupRuns)
	require.Equal(t, 1, fixture.snapshotSessionCount(t, session.SnapshotID))
	require.Equal(t, 1, fixture.snapshotSessionRowCount(t, session.SnapshotID))
}
