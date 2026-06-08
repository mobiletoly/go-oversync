package main

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	serverpkg "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

func TestMobileFlow_WatchPeerPushConverges(t *testing.T) {
	ts, err := serverpkg.NewTestServer(&serverpkg.ServerConfig{
		JWTSecret: "mobile-flow-watch-peer-push-secret",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("start nethttp test server: %v", err)
	}
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	userID := "watch-peer-push-" + uuid.NewString()
	watchDB, watchClient := newNetHTTPWatchClient(t, ts, userID, true)
	_, writerClient := newNetHTTPWatchClient(t, ts, userID, false)

	startCtx, stopWatchClient := context.WithCancel(ctx)
	defer stopWatchClient()
	if err := watchClient.Start(startCtx); err != nil {
		t.Fatalf("start watch client: %v", err)
	}

	rowID := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := writerClient.DB.ExecContext(ctx, `
		INSERT INTO users (id, name, email, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, rowID, "Watch Ada", "watch.ada@example.com", now, now); err != nil {
		t.Fatalf("insert writer row: %v", err)
	}
	syncReport, err := writerClient.Sync(ctx)
	if err != nil {
		t.Fatalf("writer sync: %v", err)
	}
	if syncReport.PushOutcome != oversqlite.PushOutcomeCommitted {
		t.Fatalf("expected writer push to commit, got %s", syncReport.PushOutcome)
	}

	assertWatchClientReceivesUser(t, ctx, watchDB, rowID, "Watch Ada")
}

func newNetHTTPWatchClient(t *testing.T, ts *serverpkg.TestServer, userID string, watch bool) (*sql.DB, *oversqlite.Client) {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "client.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create users table: %v", err)
	}

	token, err := ts.GenerateToken(userID, time.Hour)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
	})
	cfg.BackoffMin = 5 * time.Millisecond
	cfg.BackoffMax = 50 * time.Millisecond
	cfg.WatchFallbackInterval = 30 * time.Second
	if watch {
		cfg.BundleChangeWatchMode = oversqlite.BundleChangeWatchAuto
	}

	client, err := oversqlite.NewClient(db, ts.URL(), func(context.Context) (string, error) {
		return token, nil
	}, cfg)
	if err != nil {
		t.Fatalf("create oversqlite client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := client.Open(context.Background()); err != nil {
		t.Fatalf("open oversqlite client: %v", err)
	}
	attachResult, err := client.Attach(context.Background(), userID)
	if err != nil {
		t.Fatalf("attach oversqlite client: %v", err)
	}
	if attachResult.Status != oversqlite.AttachStatusConnected {
		t.Fatalf("expected attached client, got %s", attachResult.Status)
	}

	return db, client
}

func assertWatchClientReceivesUser(t *testing.T, ctx context.Context, db *sql.DB, rowID, wantName string) {
	t.Helper()

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var gotName string
		err := db.QueryRowContext(ctx, `SELECT name FROM users WHERE id = ?`, rowID).Scan(&gotName)
		if err == nil {
			if gotName != wantName {
				t.Fatalf("watch client row name = %q, want %q", gotName, wantName)
			}
			return
		}
		if err != sql.ErrNoRows {
			t.Fatalf("query watch client row: %v", err)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for watch client row: %v", ctx.Err())
		case <-deadline.C:
			t.Fatalf("timed out waiting for watch client row %s", rowID)
		case <-ticker.C:
		}
	}
}
