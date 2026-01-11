package oversync

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestProcessUpload_ConflictWithoutServerRowIsInternalError(t *testing.T) {
	ctx := context.Background()

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@localhost:5432/clisync_example?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	svc, err := NewSyncService(pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "conflict-missing-row-test",
		RegisteredTables: []RegisteredTable{
			{Schema: "public", Table: "t"},
		},
	}, logger)
	require.NoError(t, err)
	defer svc.Close()

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	userID := "test-conflict-user-" + suffix
	sourceID := "test-conflict-src-" + suffix

	pk := uuid.New().String()
	req := &UploadRequest{
		Changes: []ChangeUpload{
			{
				SourceChangeID: 1,
				Schema:         "public",
				Table:          "t",
				Op:             OpUpdate,
				PK:             pk,
				ServerVersion:  1, // non-zero: no ensureMeta, so server_row won't exist
				Payload:        []byte(`{"id":"` + pk + `","x":1}`),
			},
		},
	}

	resp, err := svc.ProcessUpload(ctx, userID, sourceID, req)
	require.NoError(t, err)
	require.True(t, resp.Accepted)
	require.Len(t, resp.Statuses, 1)

	st := resp.Statuses[0]
	require.Equal(t, StInvalid, st.Status)
	require.Equal(t, ReasonInternalError, st.Invalid["reason"])
	require.Nil(t, st.ServerRow)
}
