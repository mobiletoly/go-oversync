package schemaevolution

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRolloutCleanup_NaturalExpiryBlocksUntilAuditedDeletion(t *testing.T) {
	requireRolloutQualification(t)
	ctx := context.Background()
	var committed tablePush
	pool, _, a, ledger := rolloutServingFixtureShape(t, 1, 1, func(pool *pgxpool.Pool, s *oversync.SyncService) {
		_, err := CreateScope(ctx, pool, "scope")
		require.NoError(t, err)
		committed = tablePushRequest(t, s, 5)
		_, err = s.PushOneShot(ctx, committed.actor, committed.proof, &committed.request)
		require.NoError(t, err)
		_, err = s.CreateSnapshotSession(ctx, committed.actor, committed.proof)
		require.NoError(t, err)
	})
	defer a.Close()
	qualification := digest([]byte(t.Name()))
	cfg, err := RolloutTableConfiguration(RolloutTableLedger, ledger, qualification)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	next := *ledger
	next.Entries = append([]oversync.GenerationLifecycle(nil), ledger.Entries...)
	next.Revision++
	next.Entries[0].State = "retired"
	next.Entries[0].RetiredRevision = next.Revision
	target, err := RolloutTableConfiguration(RolloutTableGenerationRetired, &next, qualification)
	require.NoError(t, err)
	retirement := rolloutTestPlan(t, a, target, "retire_generation")
	retirement.Retiring = ledger.Entries[0].Proof
	report, err := RolloutReportReadOnly(ctx, pool, retirement)
	require.NoError(t, err)
	require.Contains(t, report.Blockers, "staging_not_empty")
	var expires time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(expires_at) FROM sync.snapshot_sessions`).Scan(&expires))
	// The TTL was declared before session creation. No stored expiry is rewritten.
	require.Eventually(t, func() bool { return time.Now().After(expires) }, 6*time.Second, 20*time.Millisecond)
	report, err = RolloutReportReadOnly(ctx, pool, retirement)
	require.NoError(t, err)
	require.Contains(t, report.Blockers, "staging_not_empty")
	before := tableAuthoritativeDigest(t, pool)
	cleanup := rolloutTestPlan(t, a, cfg, "cleanup_staging")
	cleanup.MaxRows = 2
	cleanupReport, err := RolloutReportReadOnly(ctx, pool, cleanup)
	require.NoError(t, err)
	require.Empty(t, cleanupReport.Blockers)
	a.Close()
	result := runRolloutBatches(t, pool, RolloutExecution{Plan: cleanup, Report: cleanupReport, TargetStage: RolloutTableLedger, TargetLedger: ledger, QualificationSHA256: qualification})
	require.Equal(t, "completed", result.State)
	require.Zero(t, result.DeletedRows)
	require.Equal(t, before, tableAuthoritativeDigest(t, pool))
	var batches int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM app_evolution.rollout_batches WHERE operation_id=$1`, cleanup.ID).Scan(&batches))
	require.Greater(t, batches, 1)
	report, err = RolloutReportReadOnly(ctx, pool, retirement)
	require.NoError(t, err)
	require.Empty(t, report.Blockers)
	a, err = OpenClosedRolloutAuthority(ctx, pool)
	require.NoError(t, err)
	defer a.Close()
	_, err = a.ApplyRollout(ctx, retirement, report, target, nil)
	require.NoError(t, err)
	// Direct push has no uncommitted server staging; its atomic commit advances
	// the authoritative watermark exactly once.
	var last int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT max_committed_source_bundle_id FROM sync.source_state WHERE source_id=$1`, committed.actor.SourceID).Scan(&last))
	require.EqualValues(t, 1, last)
}
