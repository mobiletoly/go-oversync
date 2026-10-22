package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func newConnectTestService(t *testing.T, ctx context.Context) (*SyncService, string) {
	t.Helper()

	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "connect_lifecycle_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "connect-lifecycle-test",
		InitializationLeaseTTL: 10 * time.Minute,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	return svc, schemaName
}

func requireScopeStateForUser(t *testing.T, ctx context.Context, svc *SyncService, userID string) string {
	t.Helper()

	var stateCode int16
	require.NoError(t, svc.pool.QueryRow(ctx, `
		SELECT ss.state_code
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&stateCode))
	state, err := scopeStateNameFromCode(stateCode)
	require.NoError(t, err)
	return state
}

func TestConnect_InitializeEmptyMarksScopeInitialized(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)

	userID := "connect-empty-" + uuid.NewString()
	resp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", resp.Resolution)

	state := requireScopeStateForUser(t, ctx, svc, userID)
	require.Equal(t, scopeStateInitialized, state)

	session, err := svc.CreateSnapshotSession(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc))
	require.NoError(t, err)
	require.Equal(t, int64(0), session.RowCount)
	require.Equal(t, int64(0), session.SnapshotBundleSeq)
}

func TestConnect_InitializeLocalSameSourceReconnectAndCommit(t *testing.T) {
	ctx := context.Background()
	svc, schemaName := newConnectTestService(t, ctx)

	userID := "connect-local-" + uuid.NewString()
	firstResp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", firstResp.Resolution)
	require.NotEmpty(t, firstResp.InitializationID)

	var nextBundleSeq int64
	require.NoError(t, svc.pool.QueryRow(ctx, `
		SELECT next_bundle_seq
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&nextBundleSeq))
	require.Equal(t, int64(1), nextBundleSeq)

	secondResp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", secondResp.Resolution)
	require.Equal(t, firstResp.InitializationID, secondResp.InitializationID)

	rowID := uuid.NewString()
	pushRows := []PushRequestRow{{
		Schema: schemaName, Table: "users", Key: SyncKey{"id": rowID}, Op: OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(fmt.Sprintf(`{"id":"%s","name":"Alpha","email":"alpha@example.com"}`, rowID)),
	}}
	commitResp, err := svc.PushOneShot(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &OneShotPushRequest{
		SourceBundleID:       1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, svc, pushRows),
		InitializationID:     firstResp.InitializationID,
		Rows:                 pushRows,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), commitResp.Bundle.BundleSeq)

	state := requireScopeStateForUser(t, ctx, svc, userID)
	require.Equal(t, scopeStateInitialized, state)
}

func TestConnect_InitializingSameSourceWithoutPendingRowsTransitionsToInitializeEmpty(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)

	userID := "connect-abandon-" + uuid.NewString()
	firstResp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", firstResp.Resolution)

	secondResp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", secondResp.Resolution)
	require.Empty(t, secondResp.InitializationID)

	var (
		stateCode        int16
		initializedBySrc string
	)
	require.NoError(t, svc.pool.QueryRow(ctx, `
		SELECT ss.state_code, ss.initialized_by_source_id
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&stateCode, &initializedBySrc))
	state, err := scopeStateNameFromCode(stateCode)
	require.NoError(t, err)
	require.Equal(t, scopeStateInitialized, state)
	require.Equal(t, "device-a", initializedBySrc)
}

func TestConnect_RemoteAuthoritativeAfterPriorInitialization(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)

	userID := "connect-remote-" + uuid.NewString()
	_, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)

	resp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-b"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", resp.Resolution)
}

func TestConnect_ExpiredInitializerCannotPush(t *testing.T) {
	ctx := context.Background()
	svc, schemaName := newConnectTestService(t, ctx)

	userID := "connect-expired-" + uuid.NewString()
	resp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", resp.Resolution)

	pushRows := []PushRequestRow{{
		Schema:         schemaName,
		Table:          "users",
		Key:            SyncKey{"id": "1b7b0f6a-37c0-4d28-b62d-6d575fa37dd7"},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload:        json.RawMessage(`{"id":"1b7b0f6a-37c0-4d28-b62d-6d575fa37dd7","name":"Late","email":"late@example.com"}`),
	}}

	_, err = svc.pool.Exec(ctx, `
		UPDATE sync.scope_state
		SET lease_expires_at = now() - interval '1 second'
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID)
	require.NoError(t, err)

	_, err = svc.PushOneShot(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &OneShotPushRequest{
		SourceBundleID:       1,
		CanonicalRequestHash: mustCanonicalPushRequestHash(t, svc, pushRows),
		InitializationID:     resp.InitializationID,
		Rows:                 pushRows,
	})

	var expiredErr *InitializationExpiredError
	require.ErrorAs(t, err, &expiredErr)
}

func TestConnect_ReadPathsRejectBeforeInitialization(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)

	userID := "connect-uninitialized-" + uuid.NewString()
	_, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	_, err = svc.ProcessPull(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), 0, 10, 0)
	var initializingErr *ScopeInitializingError
	require.ErrorAs(t, err, &initializingErr)

	_, err = svc.CreateSnapshotSession(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc))
	require.ErrorAs(t, err, &initializingErr)
}

func TestConnect_ConcurrentFirstInitializerRaceHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)

	userID := "connect-race-" + uuid.NewString()
	type result struct {
		sourceID string
		resp     *ConnectResponse
		err      error
	}

	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, sourceID := range []string{"device-a", "device-b"} {
		wg.Add(1)
		go func(sourceID string) {
			defer wg.Done()
			<-start
			resp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: sourceID}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
			results <- result{sourceID: sourceID, resp: resp, err: err}
		}(sourceID)
	}

	close(start)
	wg.Wait()
	close(results)

	initializeLocalCount := 0
	retryLaterCount := 0
	for result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.resp)
		switch result.resp.Resolution {
		case "initialize_local":
			initializeLocalCount++
		case "retry_later":
			retryLaterCount++
		default:
			t.Fatalf("unexpected race resolution for %s: %s", result.sourceID, result.resp.Resolution)
		}
	}
	require.Equal(t, 1, initializeLocalCount)
	require.Equal(t, 1, retryLaterCount)
}

func TestHandleConnect_SuccessAndRetryLaterMappings(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)
	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))

	userID := "connect-handler-" + uuid.NewString()
	successRec := doJSONHandlerRequest(
		t,
		svc,
		handlers.HandleConnect,
		http.MethodPost,
		"/sync/connect",
		Actor{UserID: userID, SourceID: "device-a"},
		ConnectRequest{HasLocalPendingRows: true},
		nil,
	)
	require.Equal(t, http.StatusOK, successRec.Code)

	var successResp ConnectResponse
	require.NoError(t, json.Unmarshal(successRec.Body.Bytes(), &successResp))
	require.Equal(t, "initialize_local", successResp.Resolution)
	require.NotEmpty(t, successResp.InitializationID)

	retryRec := doJSONHandlerRequest(
		t,
		svc,
		handlers.HandleConnect,
		http.MethodPost,
		"/sync/connect",
		Actor{UserID: userID, SourceID: "device-b"},
		ConnectRequest{HasLocalPendingRows: true},
		nil,
	)
	require.Equal(t, http.StatusOK, retryRec.Code)

	var retryResp ConnectResponse
	require.NoError(t, json.Unmarshal(retryRec.Body.Bytes(), &retryResp))
	require.Equal(t, "retry_later", retryResp.Resolution)
	require.NotZero(t, retryResp.RetryAfterSec)
}

func TestHandleConnect_ExpiredInitializerLeaseMapsToFreshInitializeLocal(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)
	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))

	userID := "connect-handler-stale-" + uuid.NewString()
	firstResp, err := svc.Connect(ctx, Actor{UserID: userID, SourceID: "device-a"}, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	require.NoError(t, err)
	require.Equal(t, "initialize_local", firstResp.Resolution)

	_, err = svc.pool.Exec(ctx, `
		UPDATE sync.scope_state
		SET lease_expires_at = now() - interval '1 second'
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID)
	require.NoError(t, err)

	rec := doJSONHandlerRequest(
		t,
		svc,
		handlers.HandleConnect,
		http.MethodPost,
		"/sync/connect",
		Actor{UserID: userID, SourceID: "device-b"},
		ConnectRequest{HasLocalPendingRows: true},
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp ConnectResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "initialize_local", resp.Resolution)
	require.NotEmpty(t, resp.InitializationID)
	require.NotEqual(t, firstResp.InitializationID, resp.InitializationID)
}

func TestHandleConnect_InvalidRequestMappings(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)
	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))

	connectInvalidRec := doJSONHandlerRequest(
		t,
		svc,
		handlers.HandleConnect,
		http.MethodPost,
		"/sync/connect",
		Actor{UserID: "connect-handler-invalid-" + uuid.NewString()},
		ConnectRequest{},
		nil,
	)
	require.Equal(t, http.StatusUnauthorized, connectInvalidRec.Code)
	require.Equal(t, "authentication_failed", decodeErrorResponse(t, connectInvalidRec).Error)

	req := httptest.NewRequest(http.MethodPost, "/sync/connect", strings.NewReader(`{"source_id":`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	setTestSchemaGenerationHeaders(req, svc)
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: "connect-handler-badjson-" + uuid.NewString(), SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	handlers.HandleConnect(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid_request", decodeErrorResponse(t, rec).Error)
}

func TestHandleConnect_RejectsBodySourceIDInsteadOfAliasingLegacyInput(t *testing.T) {
	ctx := context.Background()
	svc, _ := newConnectTestService(t, ctx)
	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))

	userID := "connect-source-truth-" + uuid.NewString()
	req := httptest.NewRequest(http.MethodPost, "/sync/connect", strings.NewReader(`{"source_id":"wrong-source","has_local_pending_rows":true}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(ProtocolVersionHeader, SyncProtocolVersion)
	setTestSchemaGenerationHeaders(req, svc)
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: userID, SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	handlers.HandleConnect(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid_request", decodeErrorResponse(t, rec).Error)

	var initializerSourceID string
	err := svc.pool.QueryRow(ctx, `
		SELECT ss.initializer_source_id
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&initializerSourceID)
	require.ErrorIs(t, err, pgx.ErrNoRows)
}
