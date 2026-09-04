package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

type convergentTraceTransport struct {
	base   http.RoundTripper
	mu     sync.Mutex
	pushes int
	pulls  int
}

func (t *convergentTraceTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	if request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
		t.pushes++
	}
	if request.Method == http.MethodGet && request.URL.Path == "/sync/pull" {
		t.pulls++
	}
	t.mu.Unlock()
	return t.base.RoundTrip(request)
}

func TestMobileFlow_DirectSyncDrainsSeveralAdmittedBatchesOnce(t *testing.T) {
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range serverpkg.SchemaGenerationsForBusinessSchema("business")[0].Tables {
		if table.Table == "users" {
			generation.Tables = append(generation.Tables, table)
		}
	}
	server, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		JWTSecret:         "direct-sync-multi-batch-secret",
		SchemaGenerations: []oversync.SchemaGeneration{generation},
		MaxRowsPerBundle:  2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("start isolated PostgreSQL server: %v", err)
	}
	defer func() {
		if err := server.CloseWithError(); err != nil {
			t.Errorf("clean isolated PostgreSQL server: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	userID := "direct-sync-" + uuid.NewString()
	writerDB, writer := newNetHTTPWatchClient(t, server, userID, false)
	followerDB, follower := newNetHTTPWatchClient(t, server, userID, false)
	trace := &convergentTraceTransport{base: http.DefaultTransport}
	writer.HTTP = &http.Client{Transport: trace}
	var rowIDs []string
	for i := 0; i < 5; i++ {
		id := uuid.NewString()
		rowIDs = append(rowIDs, id)
		stamp := time.Now().UTC().Format(time.RFC3339Nano)
		_, err := writerDB.ExecContext(ctx, `INSERT INTO users (id,name,email,created_at,updated_at) VALUES (?,?,?,?,?)`,
			id, fmt.Sprintf("Direct %d", i), fmt.Sprintf("direct%d@example.com", i), stamp, stamp)
		if err != nil {
			t.Fatalf("insert local row %d: %v", i, err)
		}
	}
	// This is the only writer Sync call. Neither a caller drain nor a retry loop assists it.
	report, err := writer.Sync(ctx)
	if err != nil {
		t.Fatalf("one direct Sync call: %v", err)
	}
	if report.PushOutcome != oversqlite.PushOutcomeCommitted {
		t.Fatalf("push outcome = %s", report.PushOutcome)
	}
	if report.Status.Pending.PendingRowCount != 0 {
		t.Fatalf("pending rows = %d", report.Status.Pending.PendingRowCount)
	}
	trace.mu.Lock()
	pushes, pulls := trace.pushes, trace.pulls
	trace.mu.Unlock()
	if pushes != 3 || pulls != 1 {
		t.Fatalf("writer trace: pushes=%d pulls=%d; want 3 and 1", pushes, pulls)
	}
	if _, err := follower.PullToStable(ctx); err != nil {
		t.Fatalf("follower pull: %v", err)
	}
	for i, id := range rowIDs {
		var name string
		if err := followerDB.QueryRowContext(ctx, `SELECT name FROM users WHERE id=?`, id).Scan(&name); err != nil {
			t.Fatalf("follower row %d: %v", i, err)
		}
		if want := fmt.Sprintf("Direct %d", i); name != want {
			t.Fatalf("follower row %d name = %q, want %q", i, name, want)
		}
	}
	var count int
	if err := writerDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("dirty rows after direct Sync = %d", count)
	}
	if err := writerDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("outbox rows after direct Sync = %d", count)
	}
}
