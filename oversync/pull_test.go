package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func mustPushUserBundle(t *testing.T, ctx context.Context, svc *SyncService, actor Actor, schemaName string, sourceBundleID int64, rowID uuid.UUID, name string) *Bundle {
	t.Helper()

	bundle, err := pushRowsViaSession(t, ctx, svc, actor, sourceBundleID, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"%s","email":"%s@example.com"}`, rowID, name, strings.ToLower(name))),
	}})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func mustDeleteUserBundle(t *testing.T, ctx context.Context, svc *SyncService, actor Actor, schemaName string, sourceBundleID int64, rowID uuid.UUID, baseRowVersion int64) *Bundle {
	t.Helper()

	bundle, err := pushRowsViaSession(t, ctx, svc, actor, sourceBundleID, []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpDelete,
		BaseRowVersion: baseRowVersion,
	}})
	require.NoError(t, err)
	require.NotNil(t, bundle)
	return bundle
}

func TestProcessPull_FreezesStableBundleSeqAndPagesByBundleCount(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_bundles_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "pull-bundles-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "pull-bundles-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}

	row1 := uuid.New()
	row2 := uuid.New()
	row3 := uuid.New()
	row4 := uuid.New()

	resp1 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "One")
	resp2 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Two")
	resp3 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 3, row3, "Three")
	require.Equal(t, int64(1), resp1.BundleSeq)
	require.Equal(t, int64(2), resp2.BundleSeq)
	require.Equal(t, int64(3), resp3.BundleSeq)

	page1, err := svc.ProcessPull(ctx, reader, 0, 1, 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), page1.StableBundleSeq)
	require.Len(t, page1.Bundles, 1)
	require.Equal(t, int64(1), page1.Bundles[0].BundleSeq)
	require.True(t, page1.HasMore)

	resp4 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 4, row4, "Four")
	require.Equal(t, int64(4), resp4.BundleSeq)

	page2, err := svc.ProcessPull(ctx, reader, page1.Bundles[0].BundleSeq, 1, page1.StableBundleSeq)
	require.NoError(t, err)
	require.Equal(t, page1.StableBundleSeq, page2.StableBundleSeq)
	require.Len(t, page2.Bundles, 1)
	require.Equal(t, int64(2), page2.Bundles[0].BundleSeq)
	require.True(t, page2.HasMore)

	page3, err := svc.ProcessPull(ctx, reader, page2.Bundles[0].BundleSeq, 5, page1.StableBundleSeq)
	require.NoError(t, err)
	require.Equal(t, page1.StableBundleSeq, page3.StableBundleSeq)
	require.Len(t, page3.Bundles, 1)
	require.Equal(t, int64(3), page3.Bundles[0].BundleSeq)
	require.False(t, page3.HasMore)
}

func TestProcessPull_RejectsCheckpointBelowRetainedBundleFloor(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_pruned_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "pull-pruned-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
		},
	}, logger)

	userID := "pull-pruned-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	row1 := uuid.New()
	row2 := uuid.New()

	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "One")
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Two")

	_, err := pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = 2 WHERE user_id = $1`, userID)
	require.NoError(t, err)

	_, err = svc.ProcessPull(ctx, reader, 1, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, int64(1), prunedErr.ProvidedSeq)
	require.Equal(t, int64(2), prunedErr.RetainedFloor)
}

func TestGetStatus_ReportsRetentionAndHistoryPrunedVisibility(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "status-visibility-test",
	}, logger)

	var prunedBefore int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT CASE WHEN is_called THEN last_value ELSE 0 END::bigint
		FROM sync.history_pruned_error_seq
	`).Scan(&prunedBefore))

	userID := "status-visibility-user-" + strings.ReplaceAll(uuid.New().String(), "-", "")
	const (
		latestBundleSeq = int64(1_000_000)
		retainedFloor   = int64(900_000)
		retainedWindow  = latestBundleSeq - retainedFloor
	)
	_, err := pool.Exec(ctx, `
		INSERT INTO sync.user_state (user_id, next_bundle_seq, retained_bundle_floor)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id) DO UPDATE
		SET next_bundle_seq = EXCLUDED.next_bundle_seq,
			retained_bundle_floor = EXCLUDED.retained_bundle_floor
	`, userID, latestBundleSeq+1, retainedFloor)
	require.NoError(t, err)

	_, err = svc.ProcessPull(ctx, Actor{UserID: userID, SourceID: "reader"}, retainedFloor-1, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)

	status, err := svc.GetStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, latestBundleSeq, status.LatestBundleSeqMax)
	require.Equal(t, retainedFloor, status.RetainedBundleFloorMax)
	require.Equal(t, retainedWindow, status.RetainedBundleWindowMax)
	require.GreaterOrEqual(t, status.RetainedBundleFloorMax, status.RetainedBundleFloorMin)
	require.GreaterOrEqual(t, status.RetainedBundleWindowMax, status.RetainedBundleWindowMin)
	require.Equal(t, prunedBefore+1, status.HistoryPrunedErrorCount)
}
