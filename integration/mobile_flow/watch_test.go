package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

type watchRoundTripFunc func(*http.Request) (*http.Response, error)

var watchTestDescriptor = func() oversync.SchemaGenerationCapabilities {
	columns := []oversync.GenerationColumn{
		{Name: "email", WireType: oversync.GenerationWireTypeText},
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{{Schema: "business", Table: "users", Columns: columns}}}
	registry, err := oversync.CompileSchemaGenerationRegistry([]oversync.RegisteredTable{{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}}}, []oversync.SchemaGeneration{generation})
	if err != nil {
		panic(err)
	}
	compiled, _ := registry.Generation(1)
	table := oversync.SchemaGenerationTableCapabilities{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}, ForeignKeys: []oversync.SchemaGenerationForeignKeyCapabilities{}}
	for _, column := range columns {
		table.Columns = append(table.Columns, oversync.SchemaGenerationColumnCapabilities{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable})
	}
	return oversync.SchemaGenerationCapabilities{ID: "1", Fingerprint: compiled.Fingerprint(), Tables: []oversync.SchemaGenerationTableCapabilities{table}}
}()

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
	client := openWatchClient(t, filepath.Join(t.TempDir(), "client.sqlite"), "http://mobile-flow-watch.test", func(context.Context) (string, error) {
		return "token", nil
	}, cfg, `CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, email TEXT NOT NULL)`)
	t.Cleanup(func() { _ = client.Close() })
	client.HTTP = &http.Client{Transport: watchRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/sync/generations/1":
			return watchJSONResponse(oversync.GenerationDiscoveryResponse{
				ProtocolVersion: "v1", SchemaGeneration: watchTestDescriptor, WatchEnabled: watchSupported,
				Limits: oversync.WireLimits{
					MaxPushRows: 10000, MaxPushDecodedBytes: 16 << 20,
					MaxPushOutcomeDecodedBytes: 16*1024*1024 + 8192,
					MaxPullBundles:             5000, MaxPullDecodedBytes: 32 << 20,
					MaxSnapshotRowsPerChunk: 1, MaxSnapshotDecodedBytesPerChunk: 1024,
					MaxSnapshotRows: 50000, MaxSnapshotLogicalBytes: 128 << 20, MaxRowDecodedBytes: 1024,
				},
			}), nil
		case "/sync/connect":
			return watchJSONResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		case "/sync/pull":
			return watchJSONResponse(oversync.PullResponse{StableBundleSeq: 0, Bundles: []oversync.Bundle{}, HasMore: false}), nil
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
	if result, err := client.Attach(context.Background(), "mobile-watch-user"); err != nil || result.Status != oversqlite.AttachStatusConnected {
		t.Fatalf("attach client result=%#v err=%v", result, err)
	}
	return client
}

type watchFreshMigrator struct{ ddl string }

func (m watchFreshMigrator) InstalledVersion(ctx context.Context, reader oversqlite.LocalMigrationReader) (int64, error) {
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m watchFreshMigrator) Migrate(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
	if _, err := tx.ExecContext(ctx, m.ddl); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=1`)
	return err
}

func openWatchClient(t *testing.T, path, baseURL string, token func(context.Context) (string, error), cfg *oversqlite.Config, ddl string) *oversqlite.Client {
	t.Helper()
	plan := oversqlite.UpgradePlan{PlanID: "mobile-watch-fresh-v1", Mode: oversqlite.UpgradeModeFresh, DatabaseID: cfg.DatabaseID, ServiceID: cfg.ServiceID, TargetGenerationID: cfg.GenerationID, TargetVersion: 1, Migrator: watchFreshMigrator{ddl: ddl}}
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{DatabasePath: path, Plan: &plan, BaseURL: baseURL, Token: token, TargetClientConfig: cfg})
	if err != nil {
		t.Fatalf("create opening: %v", err)
	}
	result, err := opening.Advance(context.Background())
	if err != nil || result.Ready == nil {
		t.Fatalf("advance opening: result=%#v err=%v", result, err)
	}
	return result.Ready
}

func TestMobileFlow_WatchDisabledClientDoesNotOpenStream(t *testing.T) {
	cfg := oversqlite.DefaultConfig(1, []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.DatabaseID, cfg.ServiceID = "watch-client", "watch-service"
	cfg.LocalVersion = 1
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
	cfg := oversqlite.DefaultConfig(1, []oversqlite.SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
	cfg.DatabaseID, cfg.ServiceID = "watch-client", "watch-service"
	cfg.LocalVersion = 1
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
