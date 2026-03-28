package simulator

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/mobiletoly/go-oversync/oversqlite"
)

func TestMobileApp_OnLaunch_PersistsOpenLifecycleStateWithoutAttachedRestore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbFile := filepath.Join(t.TempDir(), "mobile.db")

	app, err := NewMobileApp(&MobileAppConfig{
		DatabaseFile: dbFile,
		ServerURL:    "http://example.invalid",
		UserID:       "user-test",
		SourceID:     "device-test",
		DeviceName:   "Test Device",
		JWTSecret:    "test-secret",
		OversqliteConfig: &oversqlite.Config{
			Schema:        "business",
			Tables:        managedSyncTables(),
			UploadLimit:   1,
			DownloadLimit: 1,
		},
		PreserveDB: true, // keep the DB until Close() to allow assertions
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("NewMobileApp: %v", err)
	}
	defer app.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.OnLaunch(ctx); err != nil {
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
		SELECT source_id, binding_state
		FROM _sync_lifecycle_state
		WHERE singleton_key = 1
	`).Scan(&sourceID, &bindingState); err != nil {
		t.Fatalf("expected lifecycle state row after launch: %v", err)
	}
	if sourceID != app.config.SourceID {
		t.Fatalf("unexpected source_id: got %q want %q", sourceID, app.config.SourceID)
	}
	if bindingState != "anonymous" {
		t.Fatalf("unexpected binding_state: got %q want anonymous", bindingState)
	}
	if err := app.db.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM _sync_client_state
	`).Scan(&attachedStateCnt); err != nil {
		t.Fatalf("count _sync_client_state: %v", err)
	}
	if attachedStateCnt != 0 {
		t.Fatalf("expected no attached _sync_client_state rows on first launch, got %d", attachedStateCnt)
	}

}

func TestMobileApp_Close_ReleasesClientOwnershipForRestart(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbFile := filepath.Join(t.TempDir(), "mobile.db")
	cfg := &MobileAppConfig{
		DatabaseFile: dbFile,
		ServerURL:    "http://example.invalid",
		UserID:       "user-test",
		SourceID:     "device-test",
		DeviceName:   "Test Device",
		JWTSecret:    "test-secret",
		OversqliteConfig: &oversqlite.Config{
			Schema:        "business",
			Tables:        managedSyncTables(),
			UploadLimit:   1,
			DownloadLimit: 1,
		},
		PreserveDB: true,
		Logger:     logger,
	}

	app, err := NewMobileApp(cfg)
	if err != nil {
		t.Fatalf("NewMobileApp (first): %v", err)
	}
	if err := app.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	restarted, err := NewMobileApp(cfg)
	if err != nil {
		t.Fatalf("NewMobileApp (restart): %v", err)
	}
	defer restarted.Close()
}

func TestMobileApp_SyncRotatedSourceIdentityUpdatesConfigAndSession(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := &MobileApp{
		config: &MobileAppConfig{
			SourceID: "device-original",
		},
		session: NewSession("user-test", "device-original", "test-secret", logger),
		logger:  logger,
	}

	if err := app.session.SignIn("user-test", "device-original"); err != nil {
		t.Fatalf("SignIn: %v", err)
	}
	app.syncRotatedSourceIdentity("device-rotated")

	if got := app.config.SourceID; got != "device-rotated" {
		t.Fatalf("unexpected config source_id: got %q want %q", got, "device-rotated")
	}
	if got := app.session.GetSourceID(); got != "device-rotated" {
		t.Fatalf("unexpected session source_id: got %q want %q", got, "device-rotated")
	}
	if token, err := app.session.GetToken(); err != nil || token == "" {
		t.Fatalf("expected token refresh to remain available after source rotation, token=%q err=%v", token, err)
	}
}
