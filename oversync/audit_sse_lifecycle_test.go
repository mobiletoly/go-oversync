//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const auditSSESubscriberCount = 64

func TestAuditSSELifecycle_BoundedSubscriberPressureCoalescesAndCleansUp(t *testing.T) {
	ctx := context.Background()
	fixture := newBundleChangeWatchFixture(t, true)
	actor := fixture.actor("audit-sse-pressure", "reader")
	mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
	userPK := fixture.userPK(t, actor)

	type subscription struct {
		cancel context.CancelFunc
		events <-chan BundleChangeEvent
	}
	subscriptions := make([]subscription, 0, auditSSESubscriberCount)
	for range auditSSESubscriberCount {
		subCtx, cancel := context.WithCancel(ctx)
		events, err := fixture.svc.SubscribeBundleChanges(subCtx, actor, testSchemaGenerationProofForService(fixture.svc), 0)
		require.NoError(t, err)
		subscriptions = append(subscriptions, subscription{cancel: cancel, events: events})
	}
	require.Equal(t, auditSSESubscriberCount, fixture.svc.bundleChangeSubscriberCount(userPK))

	const lastBundleSeq = int64(256)
	for bundleSeq := int64(1); bundleSeq <= lastBundleSeq; bundleSeq++ {
		fixture.svc.ensureBundleChangeHub().publish(userPK, BundleChangeEvent{BundleSeq: bundleSeq})
	}
	for _, sub := range subscriptions {
		event := receiveBundleChangeEvent(t, sub.events)
		require.Equal(t, lastBundleSeq, event.BundleSeq, "a capacity-one subscriber must retain the newest wakeup")
	}

	for _, sub := range subscriptions {
		sub.cancel()
	}
	require.Eventually(t, func() bool {
		return fixture.svc.bundleChangeSubscriberCount(userPK) == 0
	}, 2*time.Second, 10*time.Millisecond)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	t.Log("OS-AUD-011 characterization: each subscriber is bounded to one coalesced event, but the service exposes no subscriber quota")
}

func TestAuditSSELifecycle_SlowHTTPClientsReleaseSubscriptionsOnDisconnect(t *testing.T) {
	ctx := context.Background()
	fixture := newBundleChangeWatchFixture(t, true)
	fixture.svc.config.BundleChangeWatch.HeartbeatInterval = 10 * time.Millisecond
	actor := fixture.actor("audit-sse-slow", "reader")
	mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
	userPK := fixture.userPK(t, actor)
	server := newWatchHTTPTestServer(t, fixture.svc, actor)

	const slowClientCount = 16
	responses := make([]*http.Response, 0, slowClientCount)
	for range slowClientCount {
		response, err := server.Client().Get(server.URL + "?after_bundle_seq=0")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		responses = append(responses, response)
	}
	require.Eventually(t, func() bool {
		return fixture.svc.bundleChangeSubscriberCount(userPK) == slowClientCount
	}, 2*time.Second, 10*time.Millisecond)

	for _, response := range responses {
		require.NoError(t, response.Body.Close())
	}
	require.Eventually(t, func() bool {
		return fixture.svc.bundleChangeSubscriberCount(userPK) == 0
	}, 2*time.Second, 10*time.Millisecond)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	t.Log("OS-AUD-011 characterization: disconnected slow clients clean up, but active SSE writes have no configured write deadline")
}

func TestAuditSSELifecycle_DuplicateListenersCoalesceNotificationsAndReleasePoolSlots(t *testing.T) {
	ctx := context.Background()
	fixture := newBundleChangeWatchFixture(t, true)
	actor := fixture.actor("audit-sse-duplicate-listener", "reader")
	mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
	userPK := fixture.userPK(t, actor)

	cancelFirst := startBundleChangeListenerForTest(t, fixture.svc)
	require.Eventually(t, func() bool {
		fixture.svc.mu.RLock()
		defer fixture.svc.mu.RUnlock()
		return fixture.svc.bundleChangeListenerRunning
	}, 2*time.Second, 10*time.Millisecond)
	require.ErrorContains(t, fixture.svc.RunBundleChangeListener(ctx), "already running")

	subCtx, cancelSubscription := context.WithCancel(ctx)
	events, err := fixture.svc.SubscribeBundleChanges(subCtx, actor, testSchemaGenerationProofForService(fixture.svc), 0)
	require.NoError(t, err)
	bundle := fixture.pushOne(t, actor, 1, "DuplicateListener")
	event := receiveBundleChangeEvent(t, events)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
	assertNoWatchEvent(t, events)

	cancelSubscription()
	cancelFirst()
	require.Eventually(t, func() bool {
		return fixture.svc.bundleChangeSubscriberCount(userPK) == 0 && fixture.pool.Stat().AcquiredConns() == 0
	}, 2*time.Second, 10*time.Millisecond)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
	t.Log("a service admits one listener, while subscriber sequence filtering coalesces duplicate notifications")
}

func TestAuditSSELifecycle_ListenerReconnectsAfterBackendTermination(t *testing.T) {
	ctx := context.Background()
	fixture := newBundleChangeWatchFixture(t, true)
	actor := fixture.actor("audit-sse-reconnect", "reader")
	mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
	userPK := fixture.userPK(t, actor)

	startBundleChangeListenerForTest(t, fixture.svc)
	firstPID := requireAuditBundleChangeListenerPID(t, ctx, fixture)
	subCtx, cancelSubscription := context.WithCancel(ctx)
	t.Cleanup(cancelSubscription)
	events, err := fixture.svc.SubscribeBundleChanges(subCtx, actor, testSchemaGenerationProofForService(fixture.svc), 0)
	require.NoError(t, err)

	var terminated bool
	require.NoError(t, fixture.pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, firstPID).Scan(&terminated))
	require.True(t, terminated)
	require.Eventually(t, func() bool {
		return fixture.pool.Stat().AcquiredConns() == 0
	}, time.Second, 5*time.Millisecond, "listener must release the killed connection before retrying")

	bundle := fixture.pushOne(t, actor, 1, "ReconnectCatchUp")
	event := receiveBundleChangeEvent(t, events)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
	secondPID := requireAuditBundleChangeListenerPID(t, ctx, fixture)
	require.NotEqual(t, firstPID, secondPID)

	cancelSubscription()
	require.Eventually(t, func() bool {
		return fixture.svc.bundleChangeSubscriberCount(userPK) == 0
	}, 2*time.Second, 10*time.Millisecond)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}

func TestAuditSSELifecycle_MalformedNotificationsAreIgnoredWithoutStoppingListener(t *testing.T) {
	ctx := context.Background()
	fixture := newBundleChangeWatchFixture(t, true)
	actor := fixture.actor("audit-sse-malformed", "reader")
	mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
	userPK := fixture.userPK(t, actor)

	subCtx, cancelSubscription := context.WithCancel(ctx)
	t.Cleanup(cancelSubscription)
	events, err := fixture.svc.SubscribeBundleChanges(subCtx, actor, testSchemaGenerationProofForService(fixture.svc), 0)
	require.NoError(t, err)
	invalidPayloads := []string{
		`{`,
		`{}`,
		fmt.Sprintf(`{"user_pk":%d,"bundle_seq":0}`, userPK),
		`{"user_pk":0,"bundle_seq":1}`,
	}
	for _, payload := range invalidPayloads {
		require.Error(t, fixture.svc.publishBundleChangeNotification(payload))
	}
	assertNoWatchEvent(t, events)

	startBundleChangeListenerForTest(t, fixture.svc)
	_ = requireAuditBundleChangeListenerPID(t, ctx, fixture)
	_, err = fixture.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, fixture.svc.effectiveBundleChangeWatchConfig().NotifyChannel, `{`)
	require.NoError(t, err)
	assertNoWatchEvent(t, events)

	bundle := fixture.pushOne(t, actor, 1, "AfterMalformedNotify")
	event := receiveBundleChangeEvent(t, events)
	require.Equal(t, bundle.BundleSeq, event.BundleSeq)
	cancelSubscription()
	require.Eventually(t, func() bool {
		return fixture.svc.bundleChangeSubscriberCount(userPK) == 0
	}, 2*time.Second, 10*time.Millisecond)
	requireAuditMetadataIntegrity(t, ctx, fixture.pool)
}

func TestAuditSSELifecycle_ShutdownAndClosedHTTPMappings(t *testing.T) {
	ctx := context.Background()
	fixture := newBundleChangeWatchFixture(t, true)
	actor := fixture.actor("audit-sse-lifecycle", "writer")
	mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
	handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))

	done, err := fixture.svc.beginOperation()
	require.NoError(t, err)
	defer done()
	closeErr := make(chan error, 1)
	go func() {
		closeErr <- fixture.svc.Close(ctx)
	}()
	require.Eventually(t, func() bool {
		state, inFlight, _ := fixture.svc.lifecycleSnapshot()
		return state == serviceLifecycleShuttingDown && inFlight == 1
	}, time.Second, 5*time.Millisecond)

	assertAuditLifecycleHTTPError(t, handlers, actor, "create", http.StatusServiceUnavailable, "service_not_ready")
	assertAuditLifecycleHTTPError(t, handlers, actor, "watch", http.StatusServiceUnavailable, "service_not_ready")
	done()
	require.NoError(t, <-closeErr)

	assertAuditLifecycleHTTPError(t, handlers, actor, "create", http.StatusServiceUnavailable, "service_not_ready")
	assertAuditLifecycleHTTPError(t, handlers, actor, "watch", http.StatusServiceUnavailable, "service_not_ready")
	t.Log("shutting-down and closed requests both map to stable service-not-ready responses")
}

func TestAuditSSELifecycle_StatusAndHealthDatabaseFailuresHaveStableErrors(t *testing.T) {
	fixture := newBundleChangeWatchFixture(t, true)
	failurePool, err := pgxpool.New(context.Background(), fixture.pool.Config().ConnString())
	require.NoError(t, err)
	t.Cleanup(failurePool.Close)
	failureService, err := newTestRuntimeService(failurePool, fixture.svc.config, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = failureService.Close(context.Background()) })
	handlers := NewHTTPSyncHandlers(failureService, integrationTestLogger(slog.LevelWarn))
	failurePool.Close()

	tests := []struct {
		name      string
		handle    func(http.ResponseWriter, *http.Request)
		status    int
		errorCode string
	}{
		{name: "status", handle: handlers.HandleStatus, status: http.StatusOK},
		{name: "health", handle: handlers.HandleHealth, status: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			test.handle(recorder, httptest.NewRequest(http.MethodGet, "/sync/"+test.name, nil))
			require.Equal(t, test.status, recorder.Code)
			var status StatusResponse
			require.NoError(t, json.NewDecoder(recorder.Body).Decode(&status))
			require.Equal(t, "unhealthy", status.Status)
		})
	}
	requireAuditMetadataIntegrity(t, context.Background(), fixture.pool)
}

func TestAuditSSELifecycle_ResponseEncodingAndWriteFailuresAreContained(t *testing.T) {
	t.Run("encoding_failure", func(t *testing.T) {
		logs := &recordingSlogHandler{}
		handlers := NewHTTPSyncHandlers(nil, slog.New(logs))
		recorder := httptest.NewRecorder()

		handlers.writeJSON(recorder, make(chan int), "audit encode failed")

		require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
		require.True(t, logs.hasMessage("audit encode failed"))
	})

	t.Run("response_write_failure", func(t *testing.T) {
		logs := &recordingSlogHandler{}
		handlers := NewHTTPSyncHandlers(nil, slog.New(logs))
		writer := &auditFailingResponseWriter{writeErr: errors.New("audit forced response write failure")}

		handlers.writeJSON(writer, map[string]bool{"ok": true}, "audit write failed")

		require.Equal(t, "application/json", writer.Header().Get("Content-Type"))
		require.True(t, logs.hasMessage("audit write failed"))
	})
}

func TestAuditExpectedLifecycle_RequestsAfterCloseReturnStableUnavailable(t *testing.T) {
	for _, operation := range []string{"create", "watch"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			fixture := newBundleChangeWatchFixture(t, true)
			actor := fixture.actor("audit-expected-closed", "writer")
			mustInitializeEmptyScope(t, ctx, fixture.svc, actor.UserID, actor.SourceID)
			handlers := NewHTTPSyncHandlers(fixture.svc, integrationTestLogger(slog.LevelWarn))
			require.NoError(t, fixture.svc.Close(ctx))

			assertAuditLifecycleHTTPError(t, handlers, actor, operation, http.StatusServiceUnavailable, "service_not_ready")
		})
	}
}

type auditFailingResponseWriter struct {
	header   http.Header
	writeErr error
	status   int
}

func (w *auditFailingResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *auditFailingResponseWriter) Write([]byte) (int, error) {
	return 0, w.writeErr
}

func (w *auditFailingResponseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}

func requireAuditBundleChangeListenerPID(t *testing.T, ctx context.Context, fixture *bundleChangeWatchFixture) int32 {
	t.Helper()
	return requireAuditBundleChangeListenerPIDForPool(t, ctx, fixture.pool)
}

func requireAuditBundleChangeListenerPIDForPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int32 {
	t.Helper()
	var pid int32
	require.Eventually(t, func() bool {
		err := pool.QueryRow(ctx, `
			SELECT pid
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND backend_type = 'client backend'
			  AND query LIKE 'LISTEN %'
			ORDER BY backend_start DESC
			LIMIT 1
		`).Scan(&pid)
		return err == nil && pid > 0
	}, 2*time.Second, 10*time.Millisecond)
	return pid
}

func assertAuditLifecycleHTTPError(
	t *testing.T,
	handlers *HTTPSyncHandlers,
	actor Actor,
	operation string,
	wantStatus int,
	wantCode string,
) {
	t.Helper()
	var request *http.Request
	recorder := httptest.NewRecorder()
	switch operation {
	case "create":
		request = httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(`{}`))
		request.Header.Set("Content-Type", "application/json")
		request = request.WithContext(ContextWithActor(request.Context(), actor))
	case "watch":
		request = httptest.NewRequest(http.MethodGet, "/sync/watch?after_bundle_seq=0", nil)
		request = request.WithContext(ContextWithActor(request.Context(), actor))
	default:
		t.Fatalf("unsupported lifecycle HTTP operation %q", operation)
	}
	setTestSchemaGenerationHeaders(request, handlers.service)
	if operation == "create" {
		handlers.HandlePush(recorder, request)
	} else {
		handlers.HandleWatch(recorder, request)
	}
	require.Equal(t, wantStatus, recorder.Code)
	require.Equal(t, wantCode, decodeErrorResponse(t, recorder).Error)
}
