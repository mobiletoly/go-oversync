package server

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestWithinSyncBundle_ServerOriginatedWritePullsToRealClient(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{
		JWTSecret: "real-server-e2e-secret",
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	suffix := uuid.NewString()
	userID := "e2e-server-originated-user-" + suffix
	deviceID := "device-e2e-a-" + suffix

	token, err := ts.GenerateToken(userID, deviceID, time.Hour)
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), "client.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`)
	require.NoError(t, err)

	client, err := oversqlite.NewClient(
		db,
		ts.URL(),
		func(context.Context) (string, error) { return token, nil },
		oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
			{TableName: "users", SyncKeyColumnName: "id"},
		}),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()

	_, err = client.Open(ctx, deviceID)
	require.NoError(t, err)

	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	rowID := uuid.New()
	err = ts.SyncService.WithinSyncBundle(
		ctx,
		oversync.Actor{UserID: userID, SourceID: "server-admin"},
		oversync.BundleSource{SourceID: "server-admin", SourceBundleID: 1},
		func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, rowID, "Server Ada", "server.ada@example.com")
			return err
		},
	)
	require.NoError(t, err)

	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	var (
		name      string
		email     string
		createdAt string
		updatedAt string
	)
	require.NoError(t, db.QueryRow(`
		SELECT name, email, created_at, updated_at
		FROM users
		WHERE id = ?
	`, rowID.String()).Scan(&name, &email, &createdAt, &updatedAt))
	require.Equal(t, "Server Ada", name)
	require.Equal(t, "server.ada@example.com", email)
	require.NotEmpty(t, createdAt)
	require.NotEmpty(t, updatedAt)

	var lastBundleSeqSeen int64
	require.NoError(t, db.QueryRow(`
		SELECT last_bundle_seq_seen
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(&lastBundleSeqSeen))

	var latestBundleSeq int64
	require.NoError(t, ts.Pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(bundle_seq), 0)
		FROM sync.bundle_log bl
		INNER JOIN sync.user_state us
			ON us.user_pk = bl.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&latestBundleSeq))
	require.Equal(t, latestBundleSeq, lastBundleSeqSeen)

	var rowCount int
	require.NoError(t, ts.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM sync.bundle_log bl
		INNER JOIN sync.user_state us
			ON us.user_pk = bl.user_pk
		WHERE us.user_id = $1
		  AND bl.source_id = $2
	`, userID, "server-admin").Scan(&rowCount))
	require.Equal(t, 1, rowCount)

	var storedName string
	require.NoError(t, ts.Pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT name
		FROM %s.users
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, "business"), userID, rowID).Scan(&storedName))
	require.Equal(t, "Server Ada", storedName)
}
