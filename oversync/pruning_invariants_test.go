package oversync

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRetentionPolicy_PrunesCommittedHistoryButPreservesCurrentStateAndSourceWatermark(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "retention_prune_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "retention-prune-invariants-test",
		RetainedBundlesPerUser:    1,
		RetentionPruneBatchSize:   100,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
	}, logger)

	userID := "retention-prune-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}

	row1 := uuid.New()
	row2 := uuid.New()
	row3 := uuid.New()

	bundle1 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "One")
	bundle2 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Two")
	bundle3 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 3, row3, "Three")

	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)

	var nextBundleSeq int64
	var retainedFloor int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_pk = $1
	`, userPK).Scan(&nextBundleSeq, &retainedFloor))
	require.Equal(t, int64(4), nextBundleSeq)
	require.Equal(t, int64(2), retainedFloor)

	var bundleLogCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_pk = $1
	`, userPK).Scan(&bundleLogCount))
	require.Equal(t, 1, bundleLogCount)

	var bundleRowsCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_rows
		WHERE user_pk = $1
	`, userPK).Scan(&bundleRowsCount))
	require.Equal(t, 1, bundleRowsCount)

	var prunedBundleLogCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq <= $2
	`, userPK, retainedFloor).Scan(&prunedBundleLogCount))
	require.Zero(t, prunedBundleLogCount)

	var prunedBundleRowsCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_rows
		WHERE user_pk = $1
		  AND bundle_seq <= $2
	`, userPK, retainedFloor).Scan(&prunedBundleRowsCount))
	require.Zero(t, prunedBundleRowsCount)

	info, err := svc.syncKeyInfoForTable(schemaName, "users")
	require.NoError(t, err)
	keyBytes1, _, err := encodeKeyBytes(info.syncKeyType, row1.String())
	require.NoError(t, err)
	var row1BundleSeq int64
	var row1Deleted bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT bundle_seq, deleted
		FROM sync.row_state
		WHERE user_pk = $1
		  AND table_id = $2
		  AND key_bytes = $3
	`, userPK, info.tableID, keyBytes1).Scan(&row1BundleSeq, &row1Deleted))
	require.Equal(t, bundle1.BundleSeq, row1BundleSeq)
	require.False(t, row1Deleted)

	var liveUserCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_pk = $1
		  AND table_id = $2
		  AND deleted = FALSE
	`, userPK, info.tableID).Scan(&liveUserCount))
	require.Equal(t, 3, liveUserCount)

	var businessUserCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM `+schemaName+`.users
		WHERE _sync_scope_id = $1
	`, userID).Scan(&businessUserCount))
	require.Equal(t, 3, businessUserCount)

	var maxCommittedSourceBundleID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT max_committed_source_bundle_id
		FROM sync.source_state
		WHERE user_pk = $1
		  AND source_id = $2
	`, userPK, writer.SourceID).Scan(&maxCommittedSourceBundleID))
	require.Equal(t, int64(3), maxCommittedSourceBundleID)

	_, err = svc.GetCommittedBundleRows(ctx, reader, bundle2.BundleSeq, nil, 10)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, retainedFloor, prunedErr.RetainedFloor)

	_, err = svc.ProcessPull(ctx, reader, 1, 10, 0)
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, retainedFloor, prunedErr.RetainedFloor)

	resp, err := svc.GetCommittedBundleRows(ctx, reader, bundle3.BundleSeq, nil, 10)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Rows, 1)
	require.Equal(t, bundle3.BundleSeq, resp.BundleSeq)
	require.Equal(t, bundle3.BundleSeq, resp.Rows[0].RowVersion)
	require.Equal(t, schemaName, resp.Rows[0].Schema)
	require.Equal(t, "users", resp.Rows[0].Table)
}
