package simulator

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/mobiletoly/go-oversync/oversqlite"
)

func TestMobileApp_OnLaunch_BootstrapsClientInfoOnRestore(t *testing.T) {
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
			Schema: "business",
			Tables: []oversqlite.SyncTable{
				{TableName: "users"},
			},
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

	var sourceID string
	if err := app.db.QueryRowContext(context.Background(), `
		SELECT source_id FROM _sync_client_info WHERE user_id = ?
	`, app.config.UserID).Scan(&sourceID); err != nil {
		t.Fatalf("expected _sync_client_info row after restore bootstrap: %v", err)
	}
	if sourceID != app.config.SourceID {
		t.Fatalf("unexpected source_id: got %q want %q", sourceID, app.config.SourceID)
	}

	if _, err := app.GetLastServerSeqSeen(context.Background()); err != nil {
		t.Fatalf("GetLastServerSeqSeen: %v", err)
	}
}
