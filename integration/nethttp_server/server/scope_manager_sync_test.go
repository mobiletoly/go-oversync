package server

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func defaultConfigForTest(t *testing.T, schema string, tables []oversqlite.SyncTable) *oversqlite.Config {
	t.Helper()
	config := oversqlite.DefaultConfig(1, tables)
	config.DatabaseID, config.ServiceID = "scope-client", schema
	config.LocalVersion = 1
	return config
}

// Each fixture generation describes its complete local table set.
func scopeClientGeneration(tables ...string) []oversync.SchemaGeneration {
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range SchemaGenerationsForBusinessSchema("business")[0].Tables {
		for _, name := range tables {
			if name == table.Table {
				generation.Tables = append(generation.Tables, table)
			}
		}
	}
	return []oversync.SchemaGeneration{generation}
}

type scopeFreshMigrator struct{ ddl string }

func (m scopeFreshMigrator) InstalledVersion(ctx context.Context, reader oversqlite.LocalMigrationReader) (int64, error) {
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m scopeFreshMigrator) Migrate(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
	if _, err := tx.ExecContext(ctx, m.ddl); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=1`)
	return err
}

func newReadyOnlyTestClient(t *testing.T, path, baseURL string, token func(context.Context) (string, error), config *oversqlite.Config, ddl string) *oversqlite.Client {
	t.Helper()
	plan := &oversqlite.UpgradePlan{
		PlanID: "nethttp-fresh-v1", Mode: oversqlite.UpgradeModeFresh,
		DatabaseID: config.DatabaseID, ServiceID: config.ServiceID,
		TargetGenerationID: config.GenerationID, TargetVersion: 1,
		Migrator: scopeFreshMigrator{ddl: ddl},
	}
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
		DatabasePath: path, Plan: plan, BaseURL: baseURL, Token: token,
		TargetClientConfig: config,
	})
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	return result.Ready
}

func newUsersClientForTest(t *testing.T, ts *TestServer, userID string) (*sql.DB, *oversqlite.Client) {
	t.Helper()

	ctx := context.Background()
	token, err := ts.GenerateToken(userID, time.Hour)
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), userID+".sqlite")
	ddl := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`
	client := newReadyOnlyTestClient(t, dbPath, ts.URL(), func(context.Context) (string, error) { return token, nil },
		defaultConfigForTest(t, "business", []oversqlite.SyncTable{
			{TableName: "users", SyncKeyColumnName: "id"},
		}), ddl)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	return client.Database(), client
}

func newUsersAndPostsClientForTest(t *testing.T, ts *TestServer, userID string) (*sql.DB, *oversqlite.Client) {
	t.Helper()

	ctx := context.Background()
	token, err := ts.GenerateToken(userID, time.Hour)
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), userID+"-posts.sqlite")
	ddl := `
		CREATE TABLE users (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE TABLE posts (
			id TEXT PRIMARY KEY NOT NULL,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED
		)
	`
	client := newReadyOnlyTestClient(t, dbPath, ts.URL(), func(context.Context) (string, error) { return token, nil },
		defaultConfigForTest(t, "business", []oversqlite.SyncTable{
			{TableName: "users", SyncKeyColumnName: "id"},
			{TableName: "posts", SyncKeyColumnName: "id"},
		}), ddl)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	return client.Database(), client
}

func TestScopeManager_ServerOriginatedWritesPullToRealClient(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{
		JWTSecret:         "scope-manager-e2e-secret",
		SchemaGenerations: scopeClientGeneration("users"),
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	scopeID := "e2e-scope-manager-" + uuid.NewString()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{})
	rowID := uuid.New()

	firstResult, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest(),
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, rowID, "Server Ada", "server.ada@example.com")
		return err
	})
	require.NoError(t, err)
	require.True(t, firstResult.AutoInitialized)
	require.Equal(t, int64(1), firstResult.Bundle.SourceBundleID)

	db, client := newUsersClientForTest(t, ts, scopeID)
	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID.String()).Scan(&name))
	require.Equal(t, "Server Ada", name)

	secondResult, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest(),
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `
			UPDATE business.users
			SET name = $3
			WHERE _sync_scope_id = $1
			  AND id = $2
		`, scopeID, rowID, "Server Ada Updated")
		return err
	})
	require.NoError(t, err)
	require.False(t, secondResult.AutoInitialized)
	require.Equal(t, int64(2), secondResult.Bundle.SourceBundleID)

	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, rowID.String()).Scan(&name))
	require.Equal(t, "Server Ada Updated", name)
}

func TestScopeManager_ServerOriginatedWritesRemainScopeIsolatedForRealClients(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{
		JWTSecret:         "scope-manager-isolation-secret",
		SchemaGenerations: scopeClientGeneration("users"),
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{})
	scopeA := "e2e-scope-a-" + uuid.NewString()
	scopeB := "e2e-scope-b-" + uuid.NewString()
	rowID := uuid.New()

	dbA, clientA := newUsersClientForTest(t, ts, scopeA)
	dbB, clientB := newUsersClientForTest(t, ts, scopeB)
	_, err = clientA.PullToStable(ctx)
	require.NoError(t, err)
	_, err = clientB.PullToStable(ctx)
	require.NoError(t, err)

	_, err = scopeMgr.ExecWrite(ctx, scopeA, oversync.ScopeWriteOptions{
		WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest(),
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, rowID, "Only A", "only-a@example.com")
		return err
	})
	require.NoError(t, err)

	_, err = clientA.PullToStable(ctx)
	require.NoError(t, err)
	_, err = clientB.PullToStable(ctx)
	require.NoError(t, err)

	var countA, countB int
	require.NoError(t, dbA.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, rowID.String()).Scan(&countA))
	require.NoError(t, dbB.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, rowID.String()).Scan(&countB))
	require.Equal(t, 1, countA)
	require.Equal(t, 0, countB)
}

func TestScopeManager_FailedServerWriteDoesNotReachRealClient(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{
		JWTSecret:         "scope-manager-failure-secret",
		SchemaGenerations: scopeClientGeneration("users"),
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	scopeID := "e2e-scope-failed-" + uuid.NewString()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{})
	db, client := newUsersClientForTest(t, ts, scopeID)
	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	rowID := uuid.New()
	writeErr := fmt.Errorf("synthetic failure")
	_, err = scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest(),
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, rowID, "Should Roll Back", "rollback@example.com"); err != nil {
			return err
		}
		return writeErr
	})
	require.ErrorIs(t, err, writeErr)

	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, rowID.String()).Scan(&count))
	require.Equal(t, 0, count)
}

func TestScopeManager_ServerOriginatedWritesFollowNormalPullFlowOverTime(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{
		JWTSecret:         "scope-manager-over-time-secret",
		SchemaGenerations: scopeClientGeneration("users", "posts"),
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	scopeID := "e2e-scope-over-time-" + uuid.NewString()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{})

	userID := uuid.New()
	postID := uuid.New()

	firstResult, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest(),
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, userID, "Timeline User", "timeline.user@example.com"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO business.posts (id, title, content, author_id)
			VALUES ($1, $2, $3, $4)
		`, postID, "Initial Post", "hello world", userID)
		return err
	})
	require.NoError(t, err)
	require.True(t, firstResult.AutoInitialized)

	db, client := newUsersAndPostsClientForTest(t, ts, scopeID)
	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	var userCount, postCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, userID.String()).Scan(&userCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM posts WHERE id = ?`, postID.String()).Scan(&postCount))
	require.Equal(t, 1, userCount)
	require.Equal(t, 1, postCount)

	secondResult, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel", RetryableWriteOptions: retryableWriteOptionsForTest(),
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE business.users
			SET name = $3
			WHERE _sync_scope_id = $1
			  AND id = $2
		`, scopeID, userID, "Timeline User Updated"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE business.posts
			SET title = $3
			WHERE _sync_scope_id = $1
			  AND id = $2
		`, scopeID, postID, "Updated Post")
		return err
	})
	require.NoError(t, err)
	require.False(t, secondResult.AutoInitialized)
	require.Greater(t, secondResult.Bundle.BundleSeq, firstResult.Bundle.BundleSeq)

	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	var userName, postTitle string
	require.NoError(t, db.QueryRow(`SELECT name FROM users WHERE id = ?`, userID.String()).Scan(&userName))
	require.NoError(t, db.QueryRow(`SELECT title FROM posts WHERE id = ?`, postID.String()).Scan(&postTitle))
	require.Equal(t, "Timeline User Updated", userName)
	require.Equal(t, "Updated Post", postTitle)

	_, err = client.PullToStable(ctx)
	require.NoError(t, err)

	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, userID.String()).Scan(&userCount))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM posts WHERE id = ?`, postID.String()).Scan(&postCount))
	require.Equal(t, 1, userCount)
	require.Equal(t, 1, postCount)
}
