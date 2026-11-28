package simulator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	serverpkg "github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

func TestMobileApp_OnLaunch_PersistsOpenLifecycleStateWithoutAttachedRestore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbFile := filepath.Join(t.TempDir(), "mobile.db")

	app, err := newMobileApp(&mobileAppConfig{
		DatabaseFile: dbFile,
		ServerURL:    "http://example.invalid",
		UserID:       "user-test",
		DeviceName:   "Test Device",
		JWTSecret:    "test-secret",
		OversqliteConfig: &oversqlite.Config{

			Tables:       managedSyncTables(),
			GenerationID: 1, DatabaseID: "mobile-flow", ServiceID: "mobile-flow-service",
			UploadLimit:   1,
			DownloadLimit: 1,
		},
		PreserveDB: true, // keep the DB until Close() to allow assertions
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("newMobileApp: %v", err)
	}
	defer app.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.onLaunch(ctx); err != nil {
		t.Fatalf("OnLaunch: %v", err)
	}

	// Stop background loops immediately (no network needed for this test).
	cancel()

	var (
		sourceID         string
		bindingState     string
		attachedStateCnt int
	)
	if err := app.db.QueryRowContext(context.Background(), `
		SELECT current_source_id, binding_state
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(&sourceID, &bindingState); err != nil {
		t.Fatalf("expected lifecycle state row after launch: %v", err)
	}
	if sourceID == "" {
		t.Fatalf("expected non-empty internal source_id after launch")
	}
	if bindingState != "anonymous" {
		t.Fatalf("unexpected binding_state: got %q want anonymous", bindingState)
	}
	if err := app.db.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM _sync_attachment_state
		WHERE binding_state = 'attached'
	`).Scan(&attachedStateCnt); err != nil {
		t.Fatalf("count attached _sync_attachment_state rows: %v", err)
	}
	if attachedStateCnt != 0 {
		t.Fatalf("expected no attached _sync_attachment_state rows on first launch, got %d", attachedStateCnt)
	}

}

func TestMobileApp_Close_ReleasesClientOwnershipForRestart(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbFile := filepath.Join(t.TempDir(), "mobile.db")
	cfg := &mobileAppConfig{
		DatabaseFile: dbFile,
		ServerURL:    "http://example.invalid",
		UserID:       "user-test",
		DeviceName:   "Test Device",
		JWTSecret:    "test-secret",
		OversqliteConfig: &oversqlite.Config{

			Tables:       managedSyncTables(),
			GenerationID: 1, DatabaseID: "mobile-flow", ServiceID: "mobile-flow-service",
			UploadLimit:   1,
			DownloadLimit: 1,
		},
		PreserveDB: true,
		Logger:     logger,
	}

	app, err := newMobileApp(cfg)
	if err != nil {
		t.Fatalf("newMobileApp (first): %v", err)
	}
	if err := app.close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	restarted, err := newMobileApp(cfg)
	if err != nil {
		t.Fatalf("newMobileApp (restart): %v", err)
	}
	defer restarted.close()
}

func TestMobileApp_SessionTokensDoNotDependOnSourceIdentity(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	session := NewSession("user-test", "test-secret", logger)
	if err := session.SignIn("user-test"); err != nil {
		t.Fatalf("SignIn: %v", err)
	}
	if token, err := session.GetToken(); err != nil || token == "" {
		t.Fatalf("expected token refresh to remain available after source rotation, token=%q err=%v", token, err)
	}
}

func TestMobileApp_OnDetach_KeepsOfflineWritesPending(t *testing.T) {
	t.Parallel()
	proof := mobileFlowSchemaGenerationProof()
	descriptor := oversync.SchemaGenerationCapabilities{ID: "1", Fingerprint: proof.Fingerprint}
	for _, table := range serverpkg.SchemaGenerationsForBusinessSchema("business")[0].Tables {
		capability := oversync.SchemaGenerationTableCapabilities{
			Schema: table.Schema, Table: table.Table, SyncKeyColumns: []string{"id"},
			ForeignKeys: []oversync.SchemaGenerationForeignKeyCapabilities{},
		}
		for _, column := range table.Columns {
			capability.Columns = append(capability.Columns, oversync.SchemaGenerationColumnCapabilities{
				Name: column.Name, WireType: column.WireType, Nullable: column.Nullable,
			})
		}
		for _, fk := range table.ForeignKeys {
			capability.ForeignKeys = append(capability.ForeignKeys, oversync.SchemaGenerationForeignKeyCapabilities{
				Columns: fk.Columns, ReferencedSchema: fk.ReferencedSchema, ReferencedTable: fk.ReferencedTable,
				ReferencedColumns: fk.ReferencedColumns, OnDelete: fk.OnDelete, OnUpdate: fk.OnUpdate,
			})
		}
		descriptor.Tables = append(descriptor.Tables, capability)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sync/generations/1":
			_ = json.NewEncoder(w).Encode(oversync.GenerationDiscoveryResponse{
				ProtocolVersion:  oversync.SyncProtocolVersion,
				SchemaGeneration: descriptor,
				Limits:           oversync.WireLimits{MaxPushRows: 10000, MaxPushDecodedBytes: 16 << 20, MaxPushOutcomeDecodedBytes: 16*1024*1024 + 8192, MaxPullBundles: 5000, MaxPullDecodedBytes: 32 << 20, MaxSnapshotRowsPerChunk: 1, MaxSnapshotDecodedBytesPerChunk: 1024, MaxSnapshotRows: 10000, MaxSnapshotLogicalBytes: 16 << 20, MaxRowDecodedBytes: 1024},
			})
		case "/sync/connect":
			_ = json.NewEncoder(w).Encode(oversync.ConnectResponse{Resolution: "initialize_empty"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbFile := filepath.Join(t.TempDir(), "mobile.db")

	app, err := newMobileApp(&mobileAppConfig{
		DatabaseFile: dbFile,
		ServerURL:    server.URL,
		UserID:       "user-test",
		DeviceName:   "Test Device",
		JWTSecret:    "test-secret",
		OversqliteConfig: &oversqlite.Config{

			Tables:       managedSyncTables(),
			GenerationID: proof.ID, DatabaseID: "mobile-flow", ServiceID: "mobile-flow-service",
			UploadLimit:   1,
			DownloadLimit: 1,
		},
		PreserveDB: true,
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("newMobileApp: %v", err)
	}
	defer app.close()

	ctx := context.Background()
	if err := app.onSignIn(ctx, "user-test"); err != nil {
		t.Fatalf("OnSignIn: %v", err)
	}
	if err := app.onDetach(ctx); err != nil {
		t.Fatalf("OnDetach: %v", err)
	}
	if err := app.createUserWithID(ctx, "offline-user", "Offline", "offline@example.com"); err != nil {
		t.Fatalf("createUserWithID: %v", err)
	}

	pendingCount, err := app.pendingChangesCount(ctx)
	if err != nil {
		t.Fatalf("pendingChangesCount: %v", err)
	}
	if pendingCount != 1 {
		t.Fatalf("unexpected pending count after detached offline write: got %d want 1", pendingCount)
	}
	if app.ui.GetPendingBadge() != 1 {
		t.Fatalf("unexpected pending badge after detached offline write: got %d want 1", app.ui.GetPendingBadge())
	}
	if app.ui.GetBanner() != "Offline mode. Sign in to sync." {
		t.Fatalf("unexpected banner after detached offline write: got %q", app.ui.GetBanner())
	}
}
