package oversqlite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig_BundleChangeWatchDefaultsOff(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})

	require.Equal(t, BundleChangeWatchOff, cfg.BundleChangeWatchMode)
	require.Zero(t, cfg.WatchFallbackInterval)
}

func TestWatchRemoteChanges_ParsesBundleEvent(t *testing.T) {
	events := make(chan oversync.BundleChangeEvent, 1)

	err := parseBundleChangeWatchStream(context.Background(), strings.NewReader("event: bundle\ndata: {\"bundle_seq\":7}\n\n"), events)

	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 7, (<-events).BundleSeq)
}

func TestWatchRemoteChanges_IgnoresHeartbeatComments(t *testing.T) {
	events := make(chan oversync.BundleChangeEvent, 1)

	err := parseBundleChangeWatchStream(context.Background(), strings.NewReader(": heartbeat\n\n"), events)

	require.ErrorIs(t, err, io.EOF)
	require.Empty(t, events)
}

func TestWatchRemoteChanges_RejectsMalformedBundleData(t *testing.T) {
	events := make(chan oversync.BundleChangeEvent, 1)

	err := parseBundleChangeWatchStream(context.Background(), strings.NewReader("event: bundle\ndata: nope\n\n"), events)

	require.Error(t, err)
	require.Contains(t, err.Error(), "decode bundle watch event")
}

func TestWatchRemoteChanges_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan oversync.BundleChangeEvent)

	err := parseBundleChangeWatchStream(ctx, strings.NewReader("event: bundle\ndata: {\"bundle_seq\":7}\n\n"), events)

	require.ErrorIs(t, err, context.Canceled)
}

func TestStart_DoesNotCallWatchWhenClientWatchOffEvenIfServerAdvertises(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.config.BackoffMin = time.Millisecond
	client.config.BackoffMax = 2 * time.Millisecond

	var watchRequests atomic.Int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	time.Sleep(15 * time.Millisecond)
	cancel()

	require.Zero(t, watchRequests.Load())
}

func TestStart_DoesNotCallWatchWhenServerDoesNotAdvertise(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	var watchRequests atomic.Int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": false}}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	time.Sleep(15 * time.Millisecond)
	cancel()

	require.Zero(t, watchRequests.Load())
}

func TestStart_UsesWatchWhenClientAutoAndServerAdvertises(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	watchRequest := make(chan *http.Request, 1)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			watchRequest <- r
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	defer cancel()

	select {
	case req := <-watchRequest:
		require.Equal(t, "/sync/watch", req.URL.Path)
		require.Equal(t, "0", req.URL.Query().Get("after_bundle_seq"))
		require.Equal(t, "bundle-device", req.Header.Get(oversync.SourceIDHeader))
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for /sync/watch request")
	}
}

func TestStart_WatchSetupWaitsForStartupSyncContention(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.PauseUploads()

	var pullRequests atomic.Int64
	watchRequest := make(chan *http.Request, 1)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			watchRequest <- r
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		case "/sync/pull":
			pullRequests.Add(1)
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	client.writeMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	time.Sleep(15 * time.Millisecond)
	require.Zero(t, pullRequests.Load())
	client.writeMu.Unlock()
	defer cancel()

	select {
	case req := <-watchRequest:
		require.Equal(t, "/sync/watch", req.URL.Path)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for /sync/watch request after startup contention cleared")
	}
	require.Zero(t, pullRequests.Load())
}

func TestStart_WatchReconnectUsesCurrentSourceIDAfterRotation(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	client, db := newBundleClientWithConfig(t, cfg, usersTestDDL)

	attachment, err := loadAttachmentState(context.Background(), db)
	require.NoError(t, err)
	attachment.CurrentSourceID = "fresh-device"
	require.NoError(t, persistAttachmentState(context.Background(), db, attachment))
	client.sourceID = "stale-device"

	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "/sync/watch", r.URL.Path)
		require.Equal(t, "fresh-device", r.Header.Get(oversync.SourceIDHeader))
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}

	resp, err := client.openBundleChangeWatch(context.Background(), 0)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestStart_WatchBundleEventTriggersExistingRemoteSync(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)

	pullRequests := make(chan struct{}, 1)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/watch":
			body := "event: bundle\ndata: {\"bundle_seq\":1}\n\n"
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		case "/sync/pull":
			select {
			case pullRequests <- struct{}{}:
			default:
			}
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	err := client.watchRemoteChanges(context.Background(), 0, time.Hour)

	require.ErrorIs(t, err, io.EOF)
	select {
	case <-pullRequests:
	default:
		t.Fatal("expected bundle event to trigger PullToStable")
	}
}

func TestStart_WatchDisconnectReconnectsWithBackoff(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	watchRequests := make(chan *http.Request, 2)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			watchRequests <- r
			if len(watchRequests) == 1 {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
			}
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	defer cancel()

	select {
	case <-watchRequests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first /sync/watch request")
	}
	select {
	case <-watchRequests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reconnect /sync/watch request")
	}
}

func TestStart_WatchSetupFailureStillPolls(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	watchRequests := make(chan struct{}, 1)
	pullRequests := make(chan struct{}, 1)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			select {
			case watchRequests <- struct{}{}:
			default:
			}
			return errorJSONResponse(http.StatusServiceUnavailable, oversync.ErrorResponse{Error: "watch_unavailable"}), nil
		case "/sync/pull":
			select {
			case pullRequests <- struct{}{}:
			default:
			}
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	defer cancel()

	select {
	case <-watchRequests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for /sync/watch request")
	}
	select {
	case <-pullRequests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fallback pull after watch setup failure")
	}
}

func TestStart_WatchForbiddenSwitchesToPolling(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	var watchRequests atomic.Int64
	pullRequests := make(chan struct{}, 2)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusForbidden, oversync.ErrorResponse{
				Error:   "bundle_change_watch_forbidden",
				Message: "Bundle change watch is not enabled for this client",
			}), nil
		case "/sync/pull":
			select {
			case pullRequests <- struct{}{}:
			default:
			}
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	defer cancel()

	select {
	case <-pullRequests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for polling fallback after forbidden watch")
	}
	time.Sleep(25 * time.Millisecond)
	require.EqualValues(t, 1, watchRequests.Load())
}

func TestStart_WatchFallbackPollingUsesDefaultInterval(t *testing.T) {
	cfg := &Config{BackoffMax: 17 * time.Millisecond}
	require.Equal(t, 17*time.Millisecond, (&Client{config: cfg}).normalizedWatchFallbackInterval())

	cfg.BackoffMax = 0
	require.Equal(t, 60*time.Second, (&Client{config: cfg}).normalizedWatchFallbackInterval())
}

func TestStart_WatchFallbackPollingUsesConfiguredInterval(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pullRequests := make(chan struct{}, 1)
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/watch":
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		case "/sync/pull":
			select {
			case pullRequests <- struct{}{}:
			default:
			}
			cancel()
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	err := client.watchRemoteChanges(ctx, 0, 5*time.Millisecond)

	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe))
	select {
	case <-pullRequests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fallback pull")
	}
}

func TestStart_WatchRespectsPausedDownloads(t *testing.T) {
	cfg := DefaultConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.PauseDownloads()

	var watchRequests atomic.Int64
	var pullRequests atomic.Int64
	client.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return jsonResponse(oversync.CapabilitiesResponse{Features: map[string]bool{"bundle_change_watch": true}}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
		case "/sync/pull":
			pullRequests.Add(1)
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(ctx))
	time.Sleep(30 * time.Millisecond)
	cancel()

	require.Zero(t, watchRequests.Load())
	require.Zero(t, pullRequests.Load())
}
