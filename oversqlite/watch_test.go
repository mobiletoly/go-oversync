package oversqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig_BundleChangeWatchDefaultsOff(t *testing.T) {
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())

	require.Equal(t, BundleChangeWatchOff, cfg.BundleChangeWatchMode)
	require.Zero(t, cfg.WatchFallbackInterval)
}

func TestWatchRemoteChanges_ParsesBundleEvent(t *testing.T) {
	events := make(chan oversync.BundleChangeEvent, 1)

	err := parseBundleChangeWatchStream(context.Background(), strings.NewReader("event: bundle\ndata: {\"bundle_seq\":7}\n\n"), events)

	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 7, (<-events).BundleSeq)
}

func TestWatchRemoteChanges_ConflatesBurstToLatestBundle(t *testing.T) {
	events := make(chan oversync.BundleChangeEvent, 1)
	stream := "event: bundle\ndata: {\"bundle_seq\":7}\n\n" +
		"event: bundle\ndata: {\"bundle_seq\":8}\n\n" +
		"event: bundle\ndata: {\"bundle_seq\":9}\n\n"

	err := parseBundleChangeWatchStream(context.Background(), strings.NewReader(stream), events)

	require.ErrorIs(t, err, io.EOF)
	require.Len(t, events, 1)
	require.EqualValues(t, 9, (<-events).BundleSeq)
}

func TestWatchRemoteChanges_RejectsLegacySourceFields(t *testing.T) {
	events := make(chan oversync.BundleChangeEvent, 1)
	err := parseBundleChangeWatchStream(context.Background(), strings.NewReader("event: bundle\ndata: {\"bundle_seq\":7,\"source_id\":\"legacy\"}\n\n"), events)
	require.ErrorContains(t, err, "exactly bundle_seq")
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

func TestWatchRemoteChanges_RejectsOversizedInputs(t *testing.T) {
	data := strings.Repeat("x", bundleChangeWatchMaxEventBytes/2)
	tests := []struct {
		name    string
		stream  string
		wantErr error
	}{
		{
			name:    "line",
			stream:  strings.Repeat("x", bundleChangeWatchMaxLineBytes+1) + "\n",
			wantErr: errBundleChangeWatchLineTooLarge,
		},
		{
			name:    "event across bounded lines",
			stream:  "event: bundle\ndata: " + data + "\ndata: " + data + "\n\n",
			wantErr: errBundleChangeWatchEventTooLarge,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			events := make(chan oversync.BundleChangeEvent, 1)
			err := parseBundleChangeWatchStream(context.Background(), strings.NewReader(testCase.stream), events)

			require.ErrorIs(t, err, testCase.wantErr)
			require.Empty(t, events)
		})
	}
}

func TestWatchRemoteChanges_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan oversync.BundleChangeEvent)

	err := parseBundleChangeWatchStream(ctx, strings.NewReader("event: bundle\ndata: {\"bundle_seq\":7}\n\n"), events)

	require.ErrorIs(t, err, context.Canceled)
}

type hostileWatchBody struct {
	err error
}

func (b *hostileWatchBody) Read([]byte) (int, error) { return 0, b.err }
func (b *hostileWatchBody) Close() error             { return nil }

type joiningWatchBody struct {
	data    []byte
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *joiningWatchBody) Read(dst []byte) (int, error) {
	if len(b.data) != 0 {
		n := copy(dst, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	b.once.Do(func() { close(b.blocked) })
	<-b.release
	return 0, io.EOF
}

func (b *joiningWatchBody) Close() error { return nil }

func TestClient_WatchAttemptJoinsParserBeforeReturn(t *testing.T) {
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = &RetryPolicy{Enabled: false}
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	body := &joiningWatchBody{
		data:    []byte("event: bundle\ndata: {\"bundle_seq\":1}\n\n"),
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
	primaryErr := errors.New("primary pull failure")
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/sync/watch":
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}, nil
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion}), nil
		case "/sync/pull":
			return nil, primaryErr
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}
	done := make(chan error, 1)
	go func() { done <- client.watchRemoteChanges(context.Background(), 0, time.Hour) }()
	<-body.blocked
	select {
	case err := <-done:
		t.Fatalf("watch returned before its parser joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(body.release)
	select {
	case err := <-done:
		var pullErr *bundleChangeWatchPullError
		require.ErrorAs(t, err, &pullErr)
		require.ErrorContains(t, err, "pull request")
	case <-time.After(time.Second):
		t.Fatal("watch did not return after parser release")
	}
}

type lockedWatchLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedWatchLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedWatchLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestWatchAwareDownloaderLoop_RedactsStreamReaderErrorLog(t *testing.T) {
	const hostileText = "HOSTILE_WATCH_READER_SECRET"
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	cfg.WatchFallbackInterval = time.Hour
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	var logs lockedWatchLogBuffer
	client.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/watch":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       &hostileWatchBody{err: errors.New(hostileText)},
			}, nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.watchAwareDownloaderLoop(ctx)
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "bundle change watch stream stopped")
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch loop to stop")
	}

	logOutput := logs.String()
	require.Contains(t, logOutput, "error_type=*errors.errorString")
	require.NotContains(t, logOutput, hostileText)
}

func TestWatchAwareDownloaderLoop_RedactsFallbackPullTransportErrorLog(t *testing.T) {
	const hostileText = "private-source-sentinel connection reset by peer"
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = &RetryPolicy{Enabled: false}
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	cfg.WatchFallbackInterval = time.Hour
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	var logs lockedWatchLogBuffer
	client.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/watch":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{
				ProtocolVersion: requiredProtocolVersion,
				WatchEnabled:    true,
			}), nil
		case "/sync/pull":
			return nil, phase3RetryableHostileError{}
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.watchAwareDownloaderLoop(ctx)
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "bundle change watch fallback pull failed")
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch loop to stop")
	}

	logOutput := logs.String()
	require.NotContains(t, logOutput, hostileText)
}

func TestStart_DoesNotCallWatchWhenClientWatchOffEvenIfServerAdvertises(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	client.config.BackoffMin = time.Millisecond
	client.config.BackoffMax = 2 * time.Millisecond

	var watchRequests atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)

	var watchRequests atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: false}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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

func TestStart_UsesCachedDisabledWatchCapabilityWithoutRefresh(t *testing.T) {
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.discovery.WatchEnabled = false

	var watchRequests atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.discovery.WatchEnabled = true

	watchRequest := make(chan *http.Request, 1)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
		case "/sync/watch":
			watchRequest <- r
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		case "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.discovery.WatchEnabled = true
	client.PauseUploads()

	var pullRequests atomic.Int64
	watchRequest := make(chan *http.Request, 1)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
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
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
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
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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

func TestWatchRemoteChanges_ConflatesBurstWhileOnePullIsActive(t *testing.T) {
	client, _ := newBundleClient(t, "main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, usersTestDDL)
	watchReader, watchWriter := io.Pipe()
	firstPullStarted := make(chan struct{})
	releaseFirstPull := make(chan struct{})
	var pullRequests atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/watch":
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: watchReader}, nil
		case "/sync/pull":
			requestNumber := pullRequests.Add(1)
			if requestNumber == 1 {
				close(firstPullStarted)
				select {
				case <-releaseFirstPull:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
		default:
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{Error: "not_found"}), nil
		}
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.watchRemoteChanges(ctx, 0, time.Hour) }()
	require.NoError(t, writeWatchEvent(watchWriter, 1))
	select {
	case <-firstPullStarted:
	case <-ctx.Done():
		t.Fatal("timed out waiting for first watch-triggered pull")
	}
	for bundleSeq := int64(2); bundleSeq <= 100; bundleSeq++ {
		require.NoError(t, writeWatchEvent(watchWriter, bundleSeq))
	}
	close(releaseFirstPull)
	require.Eventually(t, func() bool { return pullRequests.Load() == 2 }, time.Second, time.Millisecond)
	require.NoError(t, watchWriter.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)
	case <-ctx.Done():
		t.Fatal("watch did not finish after stream close")
	}
	require.EqualValues(t, 2, pullRequests.Load(), "a burst during one active pull must schedule only one trailing pull")
}

func writeWatchEvent(w io.Writer, bundleSeq int64) error {
	_, err := fmt.Fprintf(w, "event: bundle\ndata: {\"bundle_seq\":%d}\n\n", bundleSeq)
	return err
}

func TestStart_WatchDisconnectReconnectsWithBackoff(t *testing.T) {
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.discovery.WatchEnabled = true

	watchRequests := make(chan *http.Request, 2)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
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
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.discovery.WatchEnabled = true

	watchRequests := make(chan struct{}, 1)
	pullRequests := make(chan struct{}, 1)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
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
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.discovery.WatchEnabled = true

	var watchRequests atomic.Int64
	pullRequests := make(chan struct{}, 2)
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
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
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	cfg := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, testSchemaGenerationProof())
	cfg.RetryPolicy = newTestRetryPolicy()
	cfg.BundleChangeWatchMode = BundleChangeWatchAuto
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 10 * time.Millisecond
	client, _ := newBundleClientWithConfig(t, cfg, usersTestDDL)
	client.PauseDownloads()

	var watchRequests atomic.Int64
	var pullRequests atomic.Int64
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, WatchEnabled: true}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			return errorJSONResponse(http.StatusInternalServerError, oversync.ErrorResponse{Error: "unexpected_watch"}), nil
		case "/sync/pull":
			pullRequests.Add(1)
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
