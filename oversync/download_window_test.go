package oversync

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestDownloadWindowed_WindowUntilFreezesPages(t *testing.T) {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "business_dw_" + suffix
	userID := "test-user-download-window-" + suffix
	sourceID := "test-device-download-window-" + suffix

	err = resetTestBusinessSchema(ctx, pool, schemaName)
	require.NoError(t, err)
	defer func() {
		_ = dropTestSchema(ctx, pool, schemaName)
	}()
	defer func() {
		_ = cleanupSyncUser(ctx, pool, userID)
	}()

	config := &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "integration-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users"},
			{Schema: schemaName, Table: "posts"},
		},
	}

	svc, err := NewSyncService(pool, config, logger)
	require.NoError(t, err)
	defer svc.Close()

	// Seed 3 changes so we can page with limit=2.
	userChanges, _ := createUserChanges(3, schemaName, sourceID)
	uploadResp, err := svc.ProcessUpload(ctx, userID, sourceID, &UploadRequest{Changes: userChanges})
	require.NoError(t, err)
	require.True(t, uploadResp.Accepted)
	for _, st := range uploadResp.Statuses {
		require.Equal(t, StApplied, st.Status)
	}

	page1, err := svc.ProcessDownloadWindowed(ctx, userID, sourceID, 0, 2, "", true, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(page1.Changes))
	require.True(t, page1.HasMore)
	require.Greater(t, page1.WindowUntil, int64(0))

	// Insert a new change after we have a frozen window (this must not appear in page 2 when using until=WindowUntil).
	pk4 := uuid.New().String()
	payload4, err := json.Marshal(map[string]any{
		"id":    pk4,
		"name":  "User 4",
		"email": "user4@test.com",
	})
	require.NoError(t, err)

	change4 := ChangeUpload{
		SourceChangeID: 4,
		Schema:         schemaName,
		Table:          "users",
		Op:             OpInsert,
		PK:             pk4,
		Payload:        payload4,
	}

	uploadResp2, err := svc.ProcessUpload(ctx, userID, sourceID, &UploadRequest{Changes: []ChangeUpload{change4}})
	require.NoError(t, err)
	require.True(t, uploadResp2.Accepted)
	require.Greater(t, uploadResp2.HighestServerSeq, page1.WindowUntil)
	require.Equal(t, 1, len(uploadResp2.Statuses))
	require.Equal(t, StApplied, uploadResp2.Statuses[0].Status)

	page2, err := svc.ProcessDownloadWindowed(ctx, userID, sourceID, page1.NextAfter, 2, "", true, page1.WindowUntil)
	require.NoError(t, err)
	require.Equal(t, page1.WindowUntil, page2.WindowUntil)
	require.False(t, page2.HasMore)
	require.Equal(t, 1, len(page2.Changes))
	require.NotEqual(t, pk4, page2.Changes[0].PK)

	// Next page with a fresh window should now include the new change.
	page3, err := svc.ProcessDownloadWindowed(ctx, userID, sourceID, page2.NextAfter, 10, "", true, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(page3.Changes))
	require.Equal(t, pk4, page3.Changes[0].PK)
}
