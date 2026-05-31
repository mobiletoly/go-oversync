// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcquireUserUploadConnReleaseSkipsErrorLogWhenConnectionClosed(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	logs := &recordingSlogHandler{}
	svc := &SyncService{pool: pool, logger: slog.New(logs)}

	conn, releaseConn, err := svc.acquireUserUploadConn(ctx, "closed-release-user")
	require.NoError(t, err)
	require.NoError(t, conn.Conn().Close(ctx))

	releaseConn()

	require.False(t, logs.hasLevel(slog.LevelError), "closed connection release should not log an error")
	require.True(t, logs.hasMessage("Skipped releasing user advisory lock because connection is already closed"))
}

type recordingSlogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingSlogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *recordingSlogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordingSlogHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordingSlogHandler) hasLevel(level slog.Level) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.Level == level {
			return true
		}
	}
	return false
}

func (h *recordingSlogHandler) hasMessage(message string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.Message == message {
			return true
		}
	}
	return false
}
