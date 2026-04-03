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

func newUsersClientForTest(t *testing.T, ts *TestServer, userID string) (*sql.DB, *oversqlite.Client) {
	t.Helper()

	ctx := context.Background()
	token, err := ts.GenerateToken(userID, time.Hour)
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), userID+".sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

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
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	require.NoError(t, client.Open(ctx))
	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	return db, client
}

func newUsersAndPostsClientForTest(t *testing.T, ts *TestServer, userID string) (*sql.DB, *oversqlite.Client) {
	t.Helper()

	ctx := context.Background()
	token, err := ts.GenerateToken(userID, time.Hour)
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), userID+"-posts.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`
		CREATE TABLE users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE TABLE posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT NULL,
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
			{TableName: "posts", SyncKeyColumnName: "id"},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	require.NoError(t, client.Open(ctx))
	connectResult, err := client.Attach(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, oversqlite.AttachStatusConnected, connectResult.Status)

	return db, client
}

func TestScopeManager_ServerOriginatedWritesPullToRealClient(t *testing.T) {
	ts, err := NewTestServer(&ServerConfig{
		JWTSecret: "scope-manager-e2e-secret",
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	scopeID := "e2e-scope-manager-" + uuid.NewString()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{})
	rowID := uuid.New()

	firstResult, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel",
	}, func(tx pgx.Tx) error {
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
		WriterID: "admin-panel",
	}, func(tx pgx.Tx) error {
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
		JWTSecret: "scope-manager-isolation-secret",
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
		WriterID: "admin-panel",
	}, func(tx pgx.Tx) error {
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
		JWTSecret: "scope-manager-failure-secret",
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
		WriterID: "admin-panel",
	}, func(tx pgx.Tx) error {
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
		JWTSecret: "scope-manager-over-time-secret",
	})
	require.NoError(t, err)
	defer ts.Close()

	ctx := context.Background()
	scopeID := "e2e-scope-over-time-" + uuid.NewString()
	scopeMgr := oversync.NewScopeManager(ts.SyncService, oversync.ScopeManagerConfig{})

	userID := uuid.New()
	postID := uuid.New()

	firstResult, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
		WriterID: "admin-panel",
	}, func(tx pgx.Tx) error {
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
		WriterID: "admin-panel",
	}, func(tx pgx.Tx) error {
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
