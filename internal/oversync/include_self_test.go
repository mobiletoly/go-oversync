package oversync

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

// TestIncludeSelfParameter verifies that the include_self parameter
// allows clients to download their own changes for recovery scenarios
func TestIncludeSelfParameter(t *testing.T) {
	ctx := context.Background()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Setup database connection
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:password@localhost:5432/clisync_test?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	// Create sync service
	config := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "include-self-test",
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "public", Table: "note"},
		},
		DisableAutoMigrateFKs: true,
	}

	service, err := oversync.NewSyncService(pool, config, logger)
	require.NoError(t, err)
	defer service.Close()

	// Clean up and initialize
	_, err = pool.Exec(ctx, "TRUNCATE TABLE sync.sync_row_meta, sync.sync_state, sync.server_change_log")
	require.NoError(t, err)

	// Test scenario: Client uploads changes, then needs to recover them
	userID := "recovery-test-user"
	sourceID := "device-123"

	// Insert test changes for the client
	for i := 1; i <= 3; i++ {
		pkUUID := "aaaaaaaa-bbbb-cccc-dddd-00000000000" + string(rune('0'+i))
		_, err = pool.Exec(ctx, `
			INSERT INTO sync.server_change_log (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
			VALUES ($1, 'public', 'note', 'INSERT', $2::uuid, $3::jsonb, $4, $5, $6)`,
			userID, pkUUID,
			`{"id": "`+pkUUID+`", "title": "Note `+string(rune('0'+i))+`", "content": "Content `+string(rune('0'+i))+`"}`,
			sourceID, i, int64(i)) // Use i as server_version for test
		require.NoError(t, err)
	}

	// Test 1: Normal download (includeSelf=false) should exclude own changes
	normalDownload, err := service.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 100, "", false, 0)
	require.NoError(t, err)
	require.Len(t, normalDownload.Changes, 0, "Normal download should exclude own changes")
	require.False(t, normalDownload.HasMore)

	// Test 2: Recovery download (includeSelf=true) should include own changes
	recoveryDownload, err := service.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 100, "", true, 0)
	require.NoError(t, err)
	require.Len(t, recoveryDownload.Changes, 3, "Recovery download should include own changes")
	require.False(t, recoveryDownload.HasMore)

	// Verify the changes are in correct order
	t.Logf("Recovery download returned %d changes:", len(recoveryDownload.Changes))
	for i, change := range recoveryDownload.Changes {
		t.Logf("  Change %d: ServerID=%d, SourceID=%s, Op=%s, Table=%s", i, change.ServerID, change.SourceID, change.Op, change.TableName)
		require.Equal(t, sourceID, change.SourceID, "All changes should be from the same source")
		require.Equal(t, "INSERT", change.Op)
		require.Equal(t, "note", change.TableName)
	}

	// Test 3: Partial recovery (includeSelf=true with after parameter)
	// Use the first change's server_id as the "after" value
	firstServerID := recoveryDownload.Changes[0].ServerID
	partialRecovery, err := service.ProcessDownloadWindowed(ctx, userID, sourceID, firstServerID, 100, "", true, 0)
	require.NoError(t, err)
	require.Len(t, partialRecovery.Changes, 2, "Should get the remaining 2 changes")
	require.Equal(t, recoveryDownload.Changes[1].ServerID, partialRecovery.Changes[0].ServerID)
	require.Equal(t, recoveryDownload.Changes[2].ServerID, partialRecovery.Changes[1].ServerID)

	// Test 4: Add changes from another source and verify filtering still works
	otherSourceID := "device-456"
	pkUUID := "aaaaaaaa-bbbb-cccc-dddd-000000000004"
	_, err = pool.Exec(ctx, `
		INSERT INTO sync.server_change_log (user_id, schema_name, table_name, op, pk_uuid, payload, source_id, source_change_id, server_version)
		VALUES ($1, 'public', 'note', 'INSERT', $2::uuid, $3::jsonb, $4, $5, $6)`,
		userID, pkUUID,
		`{"id": "`+pkUUID+`", "title": "Other Source Note", "content": "From different device"}`,
		otherSourceID, 1, int64(4)) // Use 4 as server_version for test
	require.NoError(t, err)

	// Normal download should now see the other source's change
	normalDownload2, err := service.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 100, "", false, 0)
	require.NoError(t, err)
	require.Len(t, normalDownload2.Changes, 1, "Should see other source's change")
	require.Equal(t, otherSourceID, normalDownload2.Changes[0].SourceID)

	// Recovery download should see all changes (own + others)
	recoveryDownload2, err := service.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 100, "", true, 0)
	require.NoError(t, err)
	require.Len(t, recoveryDownload2.Changes, 4, "Should see all changes including own")

	t.Logf("✅ Include self parameter test passed")
	t.Logf("✅ Normal download excludes own changes: %d changes", len(normalDownload.Changes))
	t.Logf("✅ Recovery download includes own changes: %d changes", len(recoveryDownload.Changes))
	t.Logf("✅ Mixed scenario works correctly: normal=%d, recovery=%d", len(normalDownload2.Changes), len(recoveryDownload2.Changes))

	// Test 5: Quick HTTP API verification
	testIncludeSelfHTTP(t, service, userID, sourceID)
}

// testAuthenticator implements UserAuthenticator for testing
type testAuthenticator struct{}

func (a *testAuthenticator) GetUserID(r *http.Request) (string, error) {
	return r.Header.Get("X-User-ID"), nil
}

func (a *testAuthenticator) GetSourceID(r *http.Request) (string, error) {
	return r.Header.Get("X-Source-ID"), nil
}

// testIncludeSelfHTTP verifies the HTTP API works with include_self parameter
func testIncludeSelfHTTP(t *testing.T, service *oversync.SyncService, userID, sourceID string) {
	// Create HTTP handlers
	auth := &testAuthenticator{}
	handlers := oversync.NewHTTPSyncHandlers(service, auth, slog.Default())

	// Test HTTP API with include_self=true
	req := httptest.NewRequest("GET", "/download?after=0&limit=100&include_self=true", nil)
	req.Header.Set("X-User-ID", userID)
	req.Header.Set("X-Source-ID", sourceID)

	w := httptest.NewRecorder()
	handlers.HandleDownload(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var resp oversync.DownloadResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	require.Greater(t, len(resp.Changes), 0, "HTTP API should return own changes with include_self=true")

	t.Logf("✅ HTTP API verification: include_self=true returned %d changes", len(resp.Changes))
}
