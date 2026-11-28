package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCancellationBarrier_ReachedPhasesJoinBeforeDispatch(t *testing.T) {
	for _, phase := range []struct{ mode, method, path string }{
		{"push", "POST", "/sync/push"},
		{"resolve", "POST", "/sync/push-outcomes/resolve"},
		{"ack", "POST", "/sync/push-outcomes/ack"},
		{"snapshot", "GET", "/sync/snapshot-sessions/owned/page"},
	} {
		t.Run(phase.mode, func(t *testing.T) {
			var calls atomic.Int64
			stop := make(chan struct{})
			handler := cancellationFixtureHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }), stop, true)
			control := httptest.NewRecorder()
			handler.ServeHTTP(control, httptest.NewRequest("GET", "/__test/block?path="+phase.mode, nil))
			require.Equal(t, http.StatusNoContent, control.Code)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(phase.method, phase.path, nil).WithContext(ctx))
				close(done)
			}()
			require.Eventually(t, func() bool { return handler.(*cancellationBarrier).blocked.Load() == 1 }, time.Second, time.Millisecond)
			require.Zero(t, calls.Load())
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancelled fixture request did not join")
			}
			require.Zero(t, calls.Load())
		})
	}
}

func TestCancellationBarrier_CommittedReplyAndShutdownAreDistinct(t *testing.T) {
	var committed atomic.Int64
	stop := make(chan struct{})
	handler := cancellationFixtureHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		committed.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("terminal evidence"))
	}), stop, true)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/__test/block?path=push-response", nil))
	reply := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { handler.ServeHTTP(reply, httptest.NewRequest("POST", "/sync/push", nil)); close(done) }()
	require.Eventually(t, func() bool { return handler.(*cancellationBarrier).blocked.Load() == 1 }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, committed.Load())
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("authority shutdown did not release committed-reply barrier")
	}
	require.Empty(t, reply.Body.String(), "a lost committed reply must not reach the caller")
}

func TestCancellationBarrier_DefaultSecondPushRegressionAndInvalidControl(t *testing.T) {
	var calls atomic.Int64
	stop := make(chan struct{})
	handler := cancellationFixtureHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }), stop, false)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/sync/push", nil))
	require.EqualValues(t, 1, calls.Load())
	close(stop)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/sync/push", nil))
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 1, handler.(*cancellationBarrier).blocked.Load())
	controlled := cancellationFixtureHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), stop, true)
	reply := httptest.NewRecorder()
	controlled.ServeHTTP(reply, httptest.NewRequest("GET", "/__test/block?path=unknown", nil))
	require.Equal(t, http.StatusBadRequest, reply.Code)
	require.Equal(t, "none", controlled.(*cancellationBarrier).mode.Load())
}

func TestQualificationReadiness_PrivatePublication(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(root, 0700))
	path := filepath.Join(root, "ready.json")
	t.Setenv("OVERSYNC_QUALIFICATION_READY_FILE", path)
	t.Setenv("OVERSYNC_QUALIFICATION_READY_NONCE", strings.Repeat("1", 32))
	t.Setenv("OVERSQLITE_PROCESS_OWNER", "oversqlite-"+strings.Repeat("2", 32))
	t.Setenv("JWT_SECRET", "never-publish-this-secret")
	generation := map[string]any{"schema_generation": map[string]any{"id": 1, "fingerprint": strings.Repeat("a", 64)}}
	require.NoError(t, publishQualificationReadiness("categories", "http://127.0.0.1:1234", "owned_database", generation, nil))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var value map[string]any
	require.NoError(t, json.Unmarshal(data, &value))
	require.Equal(t, "categories", value["profile"])
	require.Equal(t, "owned_database", value["database"])
	require.Equal(t, strings.Repeat("1", 32), value["nonce"])
	require.Equal(t, float64(os.Getpid()), value["pid"])
	require.Equal(t, float64(syscall.Getpgrp()), value["pgid"])
	require.Equal(t, value["nonce"], value["reset_domain"])
	require.Len(t, value["fixture_sha256"], 64)
	require.NotContains(t, string(data), os.Getenv("JWT_SECRET"))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	require.Error(t, publishQualificationReadiness("activity", "http://127.0.0.1:9999", "other", generation, nil))
	unchanged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, unchanged)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1, "atomic publication must remove its temporary name")
}

func TestQualificationReadiness_RejectUnsafeOrPartialPublication(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(root, 0700))
	path := filepath.Join(root, "ready.json")
	t.Setenv("OVERSYNC_QUALIFICATION_READY_FILE", path)
	t.Setenv("OVERSYNC_QUALIFICATION_READY_NONCE", "")
	t.Setenv("OVERSQLITE_PROCESS_OWNER", "oversqlite-"+strings.Repeat("2", 32))
	require.Error(t, publishQualificationReadiness("categories", "url", "database", map[string]any{}, nil))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	require.Error(t, writeQualificationReadiness("relative.json", map[string]any{}))
	public := filepath.Join(root, "public")
	require.NoError(t, os.Mkdir(public, 0755))
	require.Error(t, writeQualificationReadiness(filepath.Join(public, "ready.json"), map[string]any{}))
	link := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(root, link))
	require.Error(t, writeQualificationReadiness(filepath.Join(link, "ready.json"), map[string]any{}))
	require.Error(t, writeQualificationReadiness(path, map[string]any{"unencodable": make(chan int)}))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "partial JSON must never be published")
	sentinel := filepath.Join(root, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("unrelated"), 0600))
	require.NoError(t, os.Symlink(sentinel, path))
	require.Error(t, writeQualificationReadiness(path, map[string]any{}))
	unchanged, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "unrelated", string(unchanged))
}
