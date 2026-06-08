package oversync

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newWatchHTTPTestServer(t *testing.T, svc *SyncService, actor Actor) *httptest.Server {
	t.Helper()
	handlers := NewHTTPSyncHandlers(svc, integrationTestLogger(slog.LevelWarn))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(ContextWithActor(r.Context(), actor))
		handlers.HandleWatch(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}

func readSSEUntil(t *testing.T, body io.Reader, contains string) string {
	t.Helper()
	reader := bufio.NewReader(body)
	var b strings.Builder
	deadline := time.After(2 * time.Second)
	for {
		lineCh := make(chan string, 1)
		errCh := make(chan error, 1)
		go func() {
			line, err := reader.ReadString('\n')
			if err != nil {
				errCh <- err
				return
			}
			lineCh <- line
		}()
		select {
		case line := <-lineCh:
			b.WriteString(line)
			if strings.Contains(b.String(), contains) {
				return b.String()
			}
		case err := <-errCh:
			t.Fatalf("read SSE stream: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for SSE content %q; got %q", contains, b.String())
		}
	}
}

func TestHTTPSyncHandlers_HandleWatchRequiresAuthenticatedActor(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-auth-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled: true,
		},
	}, slog.Default())
	require.NoError(t, err)
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/sync/watch", nil)
	rec := httptest.NewRecorder()
	h.HandleWatch(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "authentication_failed", decodeErrorResponse(t, rec).Error)
}

func TestHTTPSyncHandlers_HandleWatchRejectsInvalidAfterBundleSeq(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-invalid-after-handler-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled: true,
		},
	}, slog.Default())
	require.NoError(t, err)
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/sync/watch?after_bundle_seq=-1", nil)
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: "user", SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	h.HandleWatch(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid_request", decodeErrorResponse(t, rec).Error)
}

func TestHTTPSyncHandlers_HandleWatchRejectsDisabledFeature(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{AppName: "watch-disabled-handler-test"}, slog.Default())
	require.NoError(t, err)
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/sync/watch", nil)
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: "user", SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	h.HandleWatch(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "bundle_change_watch_disabled", decodeErrorResponse(t, rec).Error)
}

func TestHTTPSyncHandlers_HandleWatchRejectsForbiddenActor(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-forbidden-handler-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled: true,
		},
	}, slog.Default())
	require.NoError(t, err)
	h := NewHTTPSyncHandlersWithConfig(svc, slog.Default(), HTTPSyncHandlersConfig{
		BundleChangeWatchAllowed: func(context.Context, Actor) bool {
			return false
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/sync/watch", nil)
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: "user", SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	h.HandleWatch(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Equal(t, "bundle_change_watch_forbidden", decodeErrorResponse(t, rec).Error)
}

func TestHTTPSyncHandlers_HandleWatchStreamsImmediateBundleEvent(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("http-immediate", "device-a")
	bundle := f.pushOne(t, actor, 1, "http-immediate")
	server := newWatchHTTPTestServer(t, f.svc, actor)

	resp, err := http.Get(server.URL + "/sync/watch?after_bundle_seq=0")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")
	stream := readSSEUntil(t, resp.Body, `"bundle_seq":`)
	require.Contains(t, stream, "event: bundle\n")
	require.Contains(t, stream, `"bundle_seq":`+strconvFormatInt(bundle.BundleSeq))
}

func TestHTTPSyncHandlers_HandleWatchSendsHeartbeatAndCleansUp(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	f.svc.config.BundleChangeWatch.HeartbeatInterval = 10 * time.Millisecond
	actor := f.actor("http-heartbeat", "device-a")
	mustInitializeEmptyScope(t, f.ctx, f.svc, actor.UserID, actor.SourceID)
	userPK := f.userPK(t, actor)
	server := newWatchHTTPTestServer(t, f.svc, actor)

	resp, err := http.Get(server.URL + "/sync/watch?after_bundle_seq=0")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")
	stream := readSSEUntil(t, resp.Body, ": heartbeat")
	require.Contains(t, stream, ": heartbeat\n")
	require.Equal(t, 1, f.svc.bundleChangeSubscriberCount(userPK))

	_ = resp.Body.Close()
	waitForBundleChangeSubscriberCount(t, f.svc.ensureBundleChangeHub(), userPK, 0)
}

func TestHTTPSyncHandlers_HandleWatchUnregistersSubscriberOnWriteFailure(t *testing.T) {
	f := newBundleChangeWatchFixture(t, true)
	actor := f.actor("http-write-fail", "device-a")
	f.pushOne(t, actor, 1, "http-write-fail")
	userPK := f.userPK(t, actor)
	h := NewHTTPSyncHandlers(f.svc, integrationTestLogger(slog.LevelWarn))

	req := httptest.NewRequest(http.MethodGet, "/sync/watch?after_bundle_seq=0", nil)
	req = req.WithContext(ContextWithActor(req.Context(), actor))
	writer := &failingWatchResponseWriter{header: make(http.Header)}
	h.HandleWatch(writer, req)

	require.True(t, writer.writeCount > 0)
	waitForBundleChangeSubscriberCount(t, f.svc.ensureBundleChangeHub(), userPK, 0)
}

type failingWatchResponseWriter struct {
	header     http.Header
	statusCode int
	writeCount int
}

func (w *failingWatchResponseWriter) Header() http.Header {
	return w.header
}

func (w *failingWatchResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *failingWatchResponseWriter) Write([]byte) (int, error) {
	w.writeCount++
	return 0, errors.New("write failed")
}

func (w *failingWatchResponseWriter) Flush() {}

func strconvFormatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}
