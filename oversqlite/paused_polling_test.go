// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type syncLoopBackoffObserver struct {
	upload       chan struct{}
	download     chan struct{}
	uploadOnce   sync.Once
	downloadOnce sync.Once
	mu           sync.Mutex
	records      []string
}

func (o *syncLoopBackoffObserver) Write(message []byte) (int, error) {
	o.mu.Lock()
	o.records = append(o.records, string(message))
	o.mu.Unlock()
	if bytes.Contains(message, []byte("upload loop blocked by lifecycle state")) {
		o.uploadOnce.Do(func() { close(o.upload) })
	}
	if bytes.Contains(message, []byte("download loop blocked by lifecycle state")) {
		o.downloadOnce.Do(func() { close(o.download) })
	}
	return len(message), nil
}

func (o *syncLoopBackoffObserver) observedLogs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.records...)
}

func TestBackgroundLoops_StopDuringFailureBackoff(t *testing.T) {
	for _, loop := range []struct {
		name string
		run  func(*Client, context.Context)
		wait func(*syncLoopBackoffObserver) <-chan struct{}
	}{
		{"upload", (*Client).uploaderLoop, func(o *syncLoopBackoffObserver) <-chan struct{} { return o.upload }},
		{"download", (*Client).downloaderLoop, func(o *syncLoopBackoffObserver) <-chan struct{} { return o.download }},
	} {
		t.Run(loop.name, func(t *testing.T) {
			observer := &syncLoopBackoffObserver{
				upload:   make(chan struct{}),
				download: make(chan struct{}),
			}
			client := &Client{
				config: &Config{BackoffMin: time.Hour, BackoffMax: time.Hour},
				logger: slog.New(slog.NewTextHandler(observer, nil)),
			}
			loopCtx, cancelLoops := context.WithCancel(context.Background())
			defer cancelLoops()
			done := make(chan struct{})
			client.loopCancel = cancelLoops
			client.loopDone = done
			go func() {
				loop.run(client, loopCtx)
				close(done)
			}()
			select {
			case <-loop.wait(observer):
			case <-time.After(time.Second):
				t.Fatalf("%s loop did not reach failure backoff; logs=%v", loop.name, observer.observedLogs())
			}
			stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
			defer cancelStop()
			require.NoError(t, client.Stop(stopCtx))
		})
	}
}

func TestPausedPollingDownloader_WaitsAndCancels(t *testing.T) {
	client := &Client{config: &Config{BackoffMin: time.Hour}}
	atomic.StoreInt32(&client.downloadPaused, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.downloaderLoop(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("paused downloader returned before cancellation")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("paused downloader did not stop promptly after cancellation")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&client.downloadPaused))
}

func TestStart_RestartsAfterParentContextCancellation(t *testing.T) {
	observer := &syncLoopBackoffObserver{upload: make(chan struct{}), download: make(chan struct{})}
	client := &Client{
		config: &Config{BackoffMin: time.Millisecond, BackoffMax: 2 * time.Millisecond},
		logger: slog.New(slog.NewTextHandler(observer, nil)),
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	require.NoError(t, client.Start(firstCtx))
	select {
	case <-observer.upload:
	case <-time.After(time.Second):
		t.Fatal("first uploader did not poll")
	}
	client.loopMu.Lock()
	firstDone := client.loopDone
	client.loopMu.Unlock()
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first loop pair did not exit")
	}
	before := len(observer.observedLogs())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	require.NoError(t, client.Start(secondCtx))
	require.Eventually(t, func() bool { return len(observer.observedLogs()) > before }, time.Second, time.Millisecond)
	require.NoError(t, client.Stop(context.Background()))
}
