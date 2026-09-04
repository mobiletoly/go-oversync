package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func mustPushUserBundle(t *testing.T, ctx context.Context, svc *SyncService, actor Actor, schemaName string, sourceBundleID int64, rowID uuid.UUID, name string) *Bundle {
	t.Helper()

	bundle, err := pushRowsOneShot(t, ctx, svc, actor, sourceBundleID, []PushRequestRow{{
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

func TestProcessPull_FreezesStableBundleSeqAndPagesByBundleCount(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_bundles_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-bundles-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
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
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)
	require.Equal(t, int64(1), resp1.BundleSeq)
	require.Equal(t, int64(2), resp2.BundleSeq)
	require.Equal(t, int64(3), resp3.BundleSeq)

	page1, err := svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), 0, 1, 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), page1.StableBundleSeq)
	require.Len(t, page1.Bundles, 1)
	require.Equal(t, int64(1), page1.Bundles[0].BundleSeq)
	require.NotContains(t, string(page1.Bundles[0].Rows[0].Payload), `"_sync_scope_id"`)
	require.True(t, page1.HasMore)

	resp4 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 4, row4, "Four")
	require.Equal(t, int64(4), resp4.BundleSeq)

	page2, err := svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), page1.Bundles[0].BundleSeq, 1, page1.StableBundleSeq)
	require.NoError(t, err)
	require.Equal(t, page1.StableBundleSeq, page2.StableBundleSeq)
	require.Len(t, page2.Bundles, 1)
	require.Equal(t, int64(2), page2.Bundles[0].BundleSeq)
	require.True(t, page2.HasMore)

	page3, err := svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), page2.Bundles[0].BundleSeq, 5, page1.StableBundleSeq)
	require.NoError(t, err)
	require.Equal(t, page1.StableBundleSeq, page3.StableBundleSeq)
	require.Len(t, page3.Bundles, 1)
	require.Equal(t, int64(3), page3.Bundles[0].BundleSeq)
	require.False(t, page3.HasMore)
}

func TestProcessPull_RejectsMissingRetainedCanonicalEnvelope(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "pull_gap_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}),
		RegisteredTables:  []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}, RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	userID := "pull-gap-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	for sourceBundleID := int64(1); sourceBundleID <= 3; sourceBundleID++ {
		mustPushUserBundle(t, ctx, service, writer, schemaName, sourceBundleID, uuid.New(), fmt.Sprintf("User%d", sourceBundleID))
	}
	mustInitializeEmptyScope(t, ctx, service, userID, reader.SourceID)
	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `DELETE FROM sync.bundle_log WHERE user_pk=$1 AND bundle_seq=2`, userPK)
	require.NoError(t, err)

	_, err = service.ProcessPull(ctx, reader, testSchemaGenerationProofForService(service), 0, 10, 0)
	var integrity *GenerationProjectionError
	require.ErrorAs(t, err, &integrity)
	require.Equal(t, GenerationProjectionIntegrityError, integrity.Code)
}

func TestProcessPull_RejectsCheckpointBelowRetainedBundleFloor(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_pruned_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-pruned-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "pull-pruned-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	row1 := uuid.New()
	row2 := uuid.New()

	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "One")
	mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Two")
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)

	_, err := pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = 2 WHERE user_id = $1`, userID)
	require.NoError(t, err)

	_, err = svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), 1, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, int64(1), prunedErr.ProvidedSeq)
	require.Equal(t, int64(2), prunedErr.RetainedFloor)
}

func TestProcessPull_CheckpointAndTargetBoundaryMatrix(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_boundaries_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-boundaries-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "pull-boundaries-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	for sourceBundleID := int64(1); sourceBundleID <= 3; sourceBundleID++ {
		mustPushUserBundle(t, ctx, svc, writer, schemaName, sourceBundleID, uuid.New(), fmt.Sprintf("User%d", sourceBundleID))
	}
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)
	_, err := pool.Exec(ctx, `UPDATE sync.user_state SET retained_bundle_floor = 2 WHERE user_id = $1`, userID)
	require.NoError(t, err)

	tests := []struct {
		name       string
		after      int64
		target     int64
		stable     int64
		bundleSeqs []int64
		errType    any
	}{
		{name: "zero is pruned", after: 0, errType: (*HistoryPrunedError)(nil)},
		{name: "floor minus one is pruned", after: 1, errType: (*HistoryPrunedError)(nil)},
		{name: "floor succeeds", after: 2, stable: 3, bundleSeqs: []int64{3}},
		{name: "current succeeds empty", after: 3, stable: 3},
		{name: "future checkpoint is ahead", after: 4, errType: (*CheckpointAheadError)(nil)},
		{name: "target below floor is pruned", after: 2, target: 1, errType: (*HistoryPrunedError)(nil)},
		{name: "target at floor succeeds empty", after: 2, target: 2, stable: 2},
		{name: "target current succeeds", after: 2, target: 3, stable: 3, bundleSeqs: []int64{3}},
		{name: "future target is ahead", after: 3, target: 4, errType: (*CheckpointAheadError)(nil)},
		{name: "target below checkpoint is invalid", after: 3, target: 2, errType: (*InvalidPullRequestError)(nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), test.after, 10, test.target)
			if test.errType != nil {
				require.Error(t, err)
				switch test.errType.(type) {
				case *HistoryPrunedError:
					var target *HistoryPrunedError
					require.ErrorAs(t, err, &target)
				case *CheckpointAheadError:
					var target *CheckpointAheadError
					require.ErrorAs(t, err, &target)
				case *InvalidPullRequestError:
					var target *InvalidPullRequestError
					require.ErrorAs(t, err, &target)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.stable, response.StableBundleSeq)
			var actualSeqs []int64
			for _, bundle := range response.Bundles {
				actualSeqs = append(actualSeqs, bundle.BundleSeq)
			}
			require.Equal(t, test.bundleSeqs, actualSeqs)
		})
	}
}

func TestHTTPSyncHandlers_HandlePullMapsCheckpointBoundaryErrors(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	logger := integrationTestLogger(slog.LevelWarn)
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_http_boundaries_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-http-boundaries-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	actor := Actor{UserID: "pull-http-user-" + suffix, SourceID: "reader"}
	writer := Actor{UserID: actor.UserID, SourceID: "writer"}
	for sourceBundleID := int64(1); sourceBundleID <= 3; sourceBundleID++ {
		mustPushUserBundle(t, ctx, svc, writer, schemaName, sourceBundleID, uuid.New(), fmt.Sprintf("HTTPUser%d", sourceBundleID))
	}
	mustInitializeEmptyScope(t, ctx, svc, actor.UserID, actor.SourceID)
	handlers := NewHTTPSyncHandlers(svc, logger)

	tests := []struct {
		name       string
		query      string
		statusCode int
		errorCode  string
	}{
		{name: "future checkpoint", query: "after_bundle_seq=4", statusCode: http.StatusConflict, errorCode: "checkpoint_ahead"},
		{name: "future target", query: "after_bundle_seq=3&target_bundle_seq=4", statusCode: http.StatusConflict, errorCode: "checkpoint_ahead"},
		{name: "target below checkpoint", query: "after_bundle_seq=3&target_bundle_seq=2", statusCode: http.StatusBadRequest, errorCode: "invalid_request"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/sync/pull?"+test.query, nil)
			setTestSchemaGenerationHeaders(request, svc)
			request = request.WithContext(ContextWithActor(request.Context(), actor))
			recorder := httptest.NewRecorder()
			handlers.HandlePull(recorder, request)
			require.Equal(t, test.statusCode, recorder.Code)
			if test.errorCode == "" {
				return
			}
			var response ErrorResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, test.errorCode, response.Error)
		})
	}
}

func TestGetStatus_ReportsRetentionAndHistoryPrunedVisibility(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "status_visibility_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), AppName: "status-visibility-test",
		RegisteredTables:               []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
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
	mustInitializeEmptyScope(t, ctx, svc, userID, "reader")

	_, err = svc.ProcessPull(ctx, Actor{UserID: userID, SourceID: "reader"}, testSchemaGenerationProofForService(svc), retainedFloor-1, 10, 0)
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

func TestProcessPull_RepeatableReadSnapshotDoesNotMixWithConcurrentPrune(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "pull_repeatable_prune_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "pull-repeatable-prune-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)

	userID := "pull-repeatable-prune-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "writer"}
	reader := Actor{UserID: userID, SourceID: "reader"}
	row1 := uuid.New()
	row2 := uuid.New()

	mustPushUserBundle(t, ctx, svc, writer, schemaName, 1, row1, "One")
	bundle2 := mustPushUserBundle(t, ctx, svc, writer, schemaName, 2, row2, "Two")
	mustInitializeEmptyScope(t, ctx, svc, reader.UserID, reader.SourceID)

	tx, err := svc.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	retainedState, err := loadRetainedHistoryStateByUserID(ctx, tx, userID)
	require.NoError(t, err)
	require.NotNil(t, retainedState)
	require.Equal(t, int64(0), retainedState.RetainedFloor)

	userPK, err := lookupUserPK(ctx, pool, userID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = 2
		WHERE user_pk = $1
	`, userPK)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		DELETE FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq <= 2
	`, userPK)
	require.NoError(t, err)

	page, err := svc.processPullQuerier(ctx, tx, userID, 1, 10, 0)
	require.NoError(t, err)
	require.Equal(t, bundle2.BundleSeq, page.StableBundleSeq)
	require.Len(t, page.Bundles, 1)
	require.Equal(t, bundle2.BundleSeq, page.Bundles[0].BundleSeq)

	_, err = svc.ProcessPull(ctx, reader, testSchemaGenerationProofForService(svc), 1, 10, 0)
	var prunedErr *HistoryPrunedError
	require.ErrorAs(t, err, &prunedErr)
	require.Equal(t, int64(2), prunedErr.RetainedFloor)
}

func TestRecordHistoryPrunedError_OverridesReadOnlySerializableSessionDefaults(t *testing.T) {
	ctx := context.Background()
	basePool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaName := "history_pruned_modes_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, basePool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), basePool, schemaName) })
	_ = newBootstrappedIntegrationService(t, ctx, basePool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), AppName: "history-pruned-mutation-mode-bootstrap",
		RegisteredTables:               []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	config, err := pgxpool.ParseConfig(basePool.Config().ConnString())
	require.NoError(t, err)
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE`)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))

	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}}), AppName: "history-pruned-mutation-mode-test",
		RegisteredTables:               []RegisteredTable{{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}}},
		RegisteredTableCatalogRevision: 1}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	require.NoError(t, service.recordHistoryPrunedError(ctx))
}
