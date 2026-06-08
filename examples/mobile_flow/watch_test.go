package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

type watchRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn watchRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func watchJSONResponse(v any) *http.Response {
	body, _ := json.Marshal(v)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func newWatchMobileClient(t *testing.T, cfg *oversqlite.Config, watchSupported bool, watchRequests *atomic.Int64) *oversqlite.Client {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)`); err != nil {
		t.Fatalf("create users table: %v", err)
	}
	client, err := oversqlite.NewClient(db, "http://mobile-flow-watch.test", func(context.Context) (string, error) {
		return "token", nil
	}, cfg)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	client.HTTP = &http.Client{Transport: watchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/capabilities":
			return watchJSONResponse(oversync.CapabilitiesResponse{
				Features: map[string]bool{
					"connect_lifecycle":   true,
					"bundle_change_watch": watchSupported,
				},
			}), nil
		case "/sync/connect":
			return watchJSONResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		case "/sync/pull":
			return watchJSONResponse(oversync.PullResponse{StableBundleSeq: 0, HasMore: false}), nil
		case "/sync/watch":
			watchRequests.Add(1)
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				_ = pw.Close()
			}()
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
		default:
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: http.NoBody}, nil
		}
	})}
	if err := client.Open(context.Background()); err != nil {
		t.Fatalf("open client: %v", err)
	}
	if result, err := client.Attach(context.Background(), "mobile-watch-user"); err != nil || result.Status != oversqlite.AttachStatusConnected {
		t.Fatalf("attach client result=%#v err=%v", result, err)
	}
	return client
}

func TestMobileFlow_WatchDisabledClientDoesNotOpenStream(t *testing.T) {
	cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	var watchRequests atomic.Int64
	client := newWatchMobileClient(t, cfg, true, &watchRequests)

	ctx, cancel := context.WithCancel(context.Background())
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	cancel()

	if got := watchRequests.Load(); got != 0 {
		t.Fatalf("expected default-off client to avoid /sync/watch, got %d requests", got)
	}
}

func TestMobileFlow_WatchAutoOpensStreamWhenAdvertised(t *testing.T) {
	cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.BundleChangeWatchMode = oversqlite.BundleChangeWatchAuto
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 2 * time.Millisecond
	var watchRequests atomic.Int64
	client := newWatchMobileClient(t, cfg, true, &watchRequests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start client: %v", err)
	}

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if watchRequests.Load() > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for /sync/watch request")
		case <-ticker.C:
		}
	}
}
