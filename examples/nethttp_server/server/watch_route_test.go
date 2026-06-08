package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func watchRouteTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWatchRoute_UsesAuthenticatedActor(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{Logger: watchRouteTestLogger()})
	require.NoError(t, err)
	defer ts.Close()

	resp, err := http.Get(ts.URL() + "/sync/watch")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestWatchRoute_ServerAdvertisesCapabilityWhenEnabled(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{Logger: watchRouteTestLogger()})
	require.NoError(t, err)
	defer ts.Close()

	token, err := ts.GenerateToken("watch-capability-user", time.Hour)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodGet, ts.URL()+"/sync/capabilities", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(oversync.SourceIDHeader, "device-a")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var caps oversync.CapabilitiesResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&caps))
	require.True(t, caps.Features["bundle_change_watch"])
}

func TestWatchRoute_WakesAfterPeerPush(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{Logger: watchRouteTestLogger()})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	userID := "watch-peer-route-" + uuid.NewString()
	_, _ = newUsersClientForTest(t, ts, userID)
	resp, cancel := openWatchRouteForTest(t, ts, userID, 0)
	defer cancel()
	defer resp.Body.Close()

	writerDB, writerClient := newUsersClientForTest(t, ts, userID)
	rowID := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = writerDB.ExecContext(ctx, `
		INSERT INTO users (id, name, email, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, rowID, "Route Peer Ada", "route.peer.ada@example.com", now, now)
	require.NoError(t, err)
	report, err := writerClient.Sync(ctx)
	require.NoError(t, err)
	require.Equal(t, oversqlite.PushOutcomeCommitted, report.PushOutcome)

	stream := readWatchRouteEventForTest(t, resp.Body)
	require.Contains(t, stream, "event: bundle")
	require.Contains(t, stream, `"bundle_seq":`)
}

func TestWatchRoute_WakesAfterServerOriginatedWrite(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{Logger: watchRouteTestLogger()})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	userID := "watch-server-route-" + uuid.NewString()
	_, _ = newUsersClientForTest(t, ts, userID)
	resp, cancel := openWatchRouteForTest(t, ts, userID, 0)
	defer cancel()
	defer resp.Body.Close()

	rowID := uuid.New()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{Logger: watchRouteTestLogger()})
	result, err := scopeMgr.ExecWrite(ctx, userID, oversync.ScopeWriteOptions{WriterID: "watch-route-admin"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, rowID, "Route Server Ada", "route.server.ada@example.com")
		return err
	})
	require.NoError(t, err)
	require.NotNil(t, result.Bundle)

	stream := readWatchRouteEventForTest(t, resp.Body)
	require.Contains(t, stream, "event: bundle")
	require.Contains(t, stream, `"bundle_seq":`)
}

func TestWatchRoute_DisconnectCleanupDoesNotLeakSubscriber(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{Logger: watchRouteTestLogger()})
	require.NoError(t, err)
	defer ts.Close()

	userID := "watch-cleanup-route-" + uuid.NewString()
	_, _ = newUsersClientForTest(t, ts, userID)
	resp, cancel := openWatchRouteForTest(t, ts, userID, 0)

	waitForWatchSubscriberCountForTest(t, ts, userID, 1)
	_ = resp.Body.Close()
	cancel()
	waitForWatchSubscriberCountForTest(t, ts, userID, 0)
}

func openWatchRouteForTest(t *testing.T, ts *TestServer, userID string, afterBundleSeq int64) (*http.Response, context.CancelFunc) {
	t.Helper()
	token, err := ts.GenerateToken(userID, time.Hour)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/sync/watch?after_bundle_seq="+strconv.FormatInt(afterBundleSeq, 10), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(oversync.SourceIDHeader, "watch-route-source")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp, cancel
}

func readWatchRouteEventForTest(t *testing.T, body io.Reader) string {
	t.Helper()
	reader := bufio.NewReader(body)
	var stream strings.Builder
	for {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		stream.WriteString(line)
		if line == "\n" && strings.Contains(stream.String(), "event: bundle") {
			return stream.String()
		}
	}
}

func waitForWatchSubscriberCountForTest(t *testing.T, ts *TestServer, userID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := ts.SyncService.BundleChangeSubscriberCountForTest(context.Background(), userID)
		require.NoError(t, err)
		if got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := ts.SyncService.BundleChangeSubscriberCountForTest(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
