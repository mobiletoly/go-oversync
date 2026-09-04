//go:build oversync_audit

package oversync

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newAuditPrunedHistoryFixture(t *testing.T) (*auditDatabaseContractFixture, int64, int64) {
	t.Helper()
	ctx := context.Background()
	fixture := newAuditDatabaseContractFixture(t, ctx, "boundaries", auditDatabaseContractOptions{
		retainedBundlesPerUser: 1,
	})
	for sourceBundleID := int64(1); sourceBundleID <= 3; sourceBundleID++ {
		mustPushUserBundle(
			t,
			ctx,
			fixture.svc,
			fixture.writer,
			fixture.schemaName,
			sourceBundleID,
			uuid.New(),
			"Boundary",
		)
	}

	var retainedFloor, currentBundleSeq int64
	require.NoError(t, fixture.pool.QueryRow(ctx, `
		SELECT retained_bundle_floor, next_bundle_seq - 1
		FROM sync.user_state
		WHERE user_id = $1
	`, fixture.writer.UserID).Scan(&retainedFloor, &currentBundleSeq))
	require.Equal(t, int64(2), retainedFloor)
	require.Equal(t, int64(3), currentBundleSeq)
	return fixture, retainedFloor, currentBundleSeq
}

func TestAuditPullCheckpointBoundaries(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		fixture, retainedFloor, _ := newAuditPrunedHistoryFixture(t)
		_, err := fixture.svc.ProcessPull(context.Background(), fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, 10, 0)
		var prunedErr *HistoryPrunedError
		require.ErrorAs(t, err, &prunedErr)
		require.Equal(t, retainedFloor, prunedErr.RetainedFloor)
	})

	t.Run("floor_minus_one", func(t *testing.T) {
		fixture, retainedFloor, _ := newAuditPrunedHistoryFixture(t)
		_, err := fixture.svc.ProcessPull(context.Background(), fixture.reader, testSchemaGenerationProofForService(fixture.svc), retainedFloor-1, 10, 0)
		var prunedErr *HistoryPrunedError
		require.ErrorAs(t, err, &prunedErr)
		require.Equal(t, retainedFloor, prunedErr.RetainedFloor)
	})

	t.Run("floor", func(t *testing.T) {
		fixture, retainedFloor, currentBundleSeq := newAuditPrunedHistoryFixture(t)
		response, err := fixture.svc.ProcessPull(context.Background(), fixture.reader, testSchemaGenerationProofForService(fixture.svc), retainedFloor, 10, 0)
		require.NoError(t, err)
		require.Equal(t, currentBundleSeq, response.StableBundleSeq)
		require.Len(t, response.Bundles, 1)
		require.Equal(t, currentBundleSeq, response.Bundles[0].BundleSeq)
	})

	t.Run("current", func(t *testing.T) {
		fixture, _, currentBundleSeq := newAuditPrunedHistoryFixture(t)
		response, err := fixture.svc.ProcessPull(context.Background(), fixture.reader, testSchemaGenerationProofForService(fixture.svc), currentBundleSeq, 10, 0)
		require.NoError(t, err)
		require.Equal(t, currentBundleSeq, response.StableBundleSeq)
		require.Empty(t, response.Bundles)
	})

	t.Run("future_checkpoint", func(t *testing.T) {
		fixture, _, currentBundleSeq := newAuditPrunedHistoryFixture(t)
		_, err := fixture.svc.ProcessPull(context.Background(), fixture.reader, testSchemaGenerationProofForService(fixture.svc), currentBundleSeq+10, 10, 0)
		var aheadErr *CheckpointAheadError
		require.ErrorAs(t, err, &aheadErr)
		require.Equal(t, "checkpoint", aheadErr.Field)
		require.Equal(t, currentBundleSeq, aheadErr.CurrentBundleSeq)
	})

	t.Run("future_target", func(t *testing.T) {
		fixture, _, currentBundleSeq := newAuditPrunedHistoryFixture(t)
		_, err := fixture.svc.ProcessPull(
			context.Background(),
			fixture.reader, testSchemaGenerationProofForService(fixture.svc),

			currentBundleSeq,
			10,
			currentBundleSeq+10)

		var aheadErr *CheckpointAheadError
		require.ErrorAs(t, err, &aheadErr)
		require.Equal(t, "target", aheadErr.Field)
		require.Equal(t, currentBundleSeq, aheadErr.CurrentBundleSeq)
	})
}

func TestAuditSnapshotAfterPruningReconstructsAuthoritativeState(t *testing.T) {
	fixture, _, currentBundleSeq := newAuditPrunedHistoryFixture(t)
	ctx := context.Background()

	session, err := fixture.svc.CreateSnapshotSession(ctx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
	require.NoError(t, err)
	require.Equal(t, currentBundleSeq, session.SnapshotBundleSeq)
	require.Equal(t, int64(3), session.RowCount)

	rows, snapshotBundleSeq := collectSnapshotChunkRows(
		t,
		ctx,
		fixture.svc,
		fixture.reader,
		session.SnapshotID,
		2,
	)
	require.Equal(t, currentBundleSeq, snapshotBundleSeq)
	require.Len(t, rows, 3)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}
