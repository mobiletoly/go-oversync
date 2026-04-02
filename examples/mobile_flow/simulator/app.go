package simulator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

var verboseLog = false

// mobileAppConfig holds configuration for a mobile app instance.
type mobileAppConfig struct {
	DatabaseFile     string
	ServerURL        string
	UserID           string
	DeviceID         string
	DeviceName       string
	JWTSecret        string
	OversqliteConfig *oversqlite.Config
	PreserveDB       bool // Preserve database file after app shutdown
	Logger           *slog.Logger
}

// MobileApp represents a simulated mobile application
type MobileApp struct {
	config *mobileAppConfig
	logger *slog.Logger

	db     *sql.DB
	client *oversqlite.Client
	stmts  mobileAppStatements

	session *Session
	ui      *UISimulator
	sync    *SyncManager

	isRunning bool
	mu        sync.RWMutex
}

type mobileAppStatements struct {
	createUser *sql.Stmt
	createPost *sql.Stmt
	updateUser *sql.Stmt
	updatePost *sql.Stmt
	deleteUser *sql.Stmt
	deletePost *sql.Stmt
}

func newMobileApp(config *mobileAppConfig) (*MobileApp, error) {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	app := &MobileApp{
		config: config,
		logger: logger,
	}
	if err := app.initializeDatabase(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	app.session = NewSession(config.UserID, config.JWTSecret, logger)
	app.ui = NewUISimulator(logger)
	app.sync = NewSyncManager(app, logger)

	return app, nil
}

// initializeDatabase sets up the SQLite database with oversqlite
func (app *MobileApp) initializeDatabase() error {
	// Open SQLite database with robust single-writer configuration
	db, err := sql.Open("sqlite3", app.config.DatabaseFile+"?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000&_txlock=immediate")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Force single connection to prevent SQLite locking issues in parallel execution
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Configure SQLite for robust concurrent access
	_, _ = db.Exec(`PRAGMA busy_timeout = 5000`) // wait up to 5s on locks
	_, _ = db.Exec(`PRAGMA journal_mode = WAL`)
	_, _ = db.Exec(`PRAGMA synchronous = NORMAL`)
	_, _ = db.Exec(`PRAGMA foreign_keys = ON`)

	app.db = db
	if err := app.createBusinessTables(); err != nil {
		return fmt.Errorf("failed to create business tables: %w", err)
	}
	if err := app.prepareStatements(); err != nil {
		return fmt.Errorf("failed to prepare business statements: %w", err)
	}

	client, err := oversqlite.NewClient(
		db,
		app.config.ServerURL,
		app.tokenFunc,
		app.config.OversqliteConfig,
	)
	if err != nil {
		return fmt.Errorf("failed to create oversqlite client: %w", err)
	}
	app.client = client

	if verboseLog {
		app.logger.Info("Database initialized successfully",
			"file", app.config.DatabaseFile,
			"user_id", app.config.UserID)
	}

	return nil
}

func (app *MobileApp) prepareStatements() error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}

	ctx := context.Background()
	var err error

	if app.stmts.createUser, err = app.db.PrepareContext(ctx, `
		INSERT INTO users (id, name, email, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)`); err != nil {
		return err
	}
	if app.stmts.createPost, err = app.db.PrepareContext(ctx, `
		INSERT INTO posts (id, title, content, author_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`); err != nil {
		return err
	}
	if app.stmts.updateUser, err = app.db.PrepareContext(ctx, `
		UPDATE users SET name = ?, email = ?, updated_at = ?
		WHERE id = ?`); err != nil {
		return err
	}
	if app.stmts.updatePost, err = app.db.PrepareContext(ctx, `
		UPDATE posts SET title = ?, content = ?, updated_at = ?
		WHERE id = ?`); err != nil {
		return err
	}
	if app.stmts.deletePost, err = app.db.PrepareContext(ctx, `DELETE FROM posts WHERE id = ?`); err != nil {
		return err
	}
	if app.stmts.deleteUser, err = app.db.PrepareContext(ctx, `DELETE FROM users WHERE id = ?`); err != nil {
		return err
	}

	return nil
}

func (app *MobileApp) closeStatements() error {
	var firstErr error
	closeStmt := func(name string, stmt *sql.Stmt) {
		if stmt == nil {
			return
		}
		if err := stmt.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s statement: %w", name, err)
		}
	}

	closeStmt("createUser", app.stmts.createUser)
	closeStmt("createPost", app.stmts.createPost)
	closeStmt("updateUser", app.stmts.updateUser)
	closeStmt("updatePost", app.stmts.updatePost)
	closeStmt("deleteUser", app.stmts.deleteUser)
	closeStmt("deletePost", app.stmts.deletePost)
	app.stmts = mobileAppStatements{}
	return firstErr
}

// createBusinessTables creates the local business tables that will be synced
func (app *MobileApp) createBusinessTables() error {
	// Create local users table (SQLite)
	createUsersSQL := `
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`
	if _, err := app.db.Exec(createUsersSQL); err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Create local posts table (SQLite)
	createPostsSQL := `
		CREATE TABLE IF NOT EXISTS posts (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			author_id TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id)
		)`

	if _, err := app.db.Exec(createPostsSQL); err != nil {
		return fmt.Errorf("failed to create posts table: %w", err)
	}

	createCategoriesSQL := `
		CREATE TABLE IF NOT EXISTS categories (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			parent_id TEXT,
			FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
		)`
	if _, err := app.db.Exec(createCategoriesSQL); err != nil {
		return fmt.Errorf("failed to create categories table: %w", err)
	}

	createTeamsSQL := `
		CREATE TABLE IF NOT EXISTS teams (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			captain_member_id TEXT,
			FOREIGN KEY (captain_member_id) REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
		)`
	if _, err := app.db.Exec(createTeamsSQL); err != nil {
		return fmt.Errorf("failed to create teams table: %w", err)
	}

	createTeamMembersSQL := `
		CREATE TABLE IF NOT EXISTS team_members (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			team_id TEXT NOT NULL,
			FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
		)`
	if _, err := app.db.Exec(createTeamMembersSQL); err != nil {
		return fmt.Errorf("failed to create team_members table: %w", err)
	}

	// Create local files table (SQLite) with BLOB primary key
	createFilesSQL := `
		CREATE TABLE IF NOT EXISTS files (
			id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
			name TEXT NOT NULL,
			data BLOB
		)`
	if _, err := app.db.Exec(createFilesSQL); err != nil {
		return fmt.Errorf("failed to create files table: %w", err)
	}

	// Create local file_reviews table (SQLite) with BLOB primary key and foreign key
	createFileReviewsSQL := `
		CREATE TABLE IF NOT EXISTS file_reviews (
			id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),
			file_id BLOB NOT NULL,
			review TEXT NOT NULL,
			FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
		)`
	if _, err := app.db.Exec(createFileReviewsSQL); err != nil {
		return fmt.Errorf("failed to create file_reviews table: %w", err)
	}

	app.logger.Debug("Business tables created successfully")
	return nil
}

// tokenFunc provides JWT tokens for oversqlite client
func (app *MobileApp) tokenFunc(ctx context.Context) (string, error) {
	if !app.session.IsActive() {
		return "", fmt.Errorf("no active session")
	}

	token, err := app.session.GetToken()
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}
	return token, nil
}

// onLaunch simulates app launch lifecycle.
func (app *MobileApp) onLaunch(ctx context.Context) error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.isRunning {
		return fmt.Errorf("app is already running")
	}
	if verboseLog {
		app.logger.Info("🚀 App launching", "device", app.config.DeviceName)
	}
	app.ui.SetBanner("Starting up...")

	err := app.client.Open(ctx)
	if err != nil {
		app.logger.Error("Failed to open local oversqlite lifecycle", "error", err)
		app.ui.SetBanner("Setup failed")
		return fmt.Errorf("open lifecycle failed: %w", err)
	}
	if verboseLog {
		app.logger.Info("Opened local oversqlite lifecycle")
	}

	// Try to restore session
	if app.session.CanRestore() && app.client != nil && app.client.UserID == app.session.GetUserID() {
		if verboseLog {
			app.logger.Info("Restoring previous session")
		}
		if err := app.session.Restore(); err != nil {
			app.logger.Warn("Failed to restore session", "error", err)
			app.ui.SetBanner("Session expired. Sign in to sync.")
			app.ui.SetPendingBadge(app.getPendingCount())
			return nil
		}

		if verboseLog {
			app.logger.Info("Session restored successfully", "user_id", app.session.GetUserID())
		}

		if err := app.connectLifecycle(ctx, app.session.GetUserID()); err != nil {
			app.logger.Error("Failed to connect oversqlite after session restore", "error", err)
			app.ui.SetBanner("Setup failed")
			return fmt.Errorf("connect after restore failed: %w", err)
		}

		if err := app.sync.Start(ctx); err != nil {
			app.logger.Error("Failed to start sync", "error", err)
			app.ui.SetBanner("Sync failed to start")
			return err
		}

		app.ui.SetBanner("Syncing...")
		app.ui.SetPendingBadge(app.getPendingCount())
	} else {
		app.ui.SetBanner("Offline mode. Sign in to sync.")
		app.ui.SetPendingBadge(app.getPendingCount())
	}

	app.isRunning = true
	if verboseLog {
		app.logger.Info("✅ App launched successfully")
	}

	return nil
}

// onSignIn simulates user sign-in.
func (app *MobileApp) onSignIn(ctx context.Context, userID string) error {
	if verboseLog {
		app.logger.Info("👤 User signing in", "user_id", userID)
	}

	// Create new session
	if err := app.session.SignIn(userID); err != nil {
		app.ui.SetBanner("Sign in failed")
		return fmt.Errorf("sign in failed: %w", err)
	}

	if verboseLog {
		app.ui.SetBanner("Setting up sync...")
	}
	err := app.client.Open(ctx)
	if err != nil {
		app.logger.Error("Failed to open local oversqlite lifecycle", "error", err)
		app.ui.SetBanner("Setup failed")
		return fmt.Errorf("open lifecycle failed: %w", err)
	}
	if verboseLog {
		app.logger.Info("Opened local oversqlite lifecycle")
	}
	if err := app.connectLifecycle(ctx, userID); err != nil {
		app.logger.Error("Failed to connect oversqlite lifecycle", "error", err)
		app.ui.SetBanner("Setup failed")
		return fmt.Errorf("connect lifecycle failed: %w", err)
	}

	if err := app.sync.Start(ctx); err != nil {
		app.logger.Error("Failed to start sync after sign in", "error", err)
		app.ui.SetBanner("Sync failed to start")
		return err
	}

	app.ui.SetBanner("Syncing...")
	app.ui.SetPendingBadge(app.getPendingCount())

	if verboseLog {
		app.logger.Info("✅ User signed in successfully", "user_id", userID)
	}
	return nil
}

// signIn is a convenience method that wraps onSignIn.
func (app *MobileApp) signIn(ctx context.Context, userID string) error {
	return app.onSignIn(ctx, userID)
}

// onDetach leaves the attached sync session while keeping offline local edits enabled.
func (app *MobileApp) onDetach(ctx context.Context) error {
	app.logger.Info("👋 Detaching sync session")

	app.sync.Stop()
	if app.client != nil {
		detachResult, err := app.client.Detach(ctx)
		if err != nil {
			return fmt.Errorf("detach failed: %w", err)
		}
		if detachResult.Outcome == oversqlite.DetachOutcomeBlockedUnsyncedData {
			return fmt.Errorf("detach blocked by %d pending sync rows", detachResult.PendingRowCount)
		}
	}
	app.session.Detach()

	app.ui.SetBanner("Offline mode. Sign in to sync.")
	app.ui.SetPendingBadge(app.getPendingCount())

	app.logger.Info("✅ Detached sync session successfully")
	return nil
}

func (app *MobileApp) connectLifecycle(ctx context.Context, userID string) error {
	const maxAttempts = 8

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result, err := app.client.Attach(ctx, userID)
		if err == nil {
			switch result.Status {
			case oversqlite.AttachStatusConnected:
				return nil
			case oversqlite.AttachStatusRetryLater:
				waitFor := result.RetryAfter
				if waitFor <= 0 {
					waitFor = 250 * time.Millisecond
				}
				if verboseLog {
					app.logger.Info("Connect retry requested", "attempt", attempt, "retry_after", waitFor)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(waitFor):
					continue
				}
			default:
				return fmt.Errorf("unexpected connect status %q", result.Status)
			}
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return err
	}

	return fmt.Errorf("connect lifecycle did not reach connected status after %d attempts", maxAttempts)
}

func (app *MobileApp) createUser(name, email string) (string, error) {
	userID := uuid.New().String()
	return app.insertUserWithID(context.Background(), userID, name, email)
}

func (app *MobileApp) createUserWithContext(ctx context.Context, userID, name, email string) error {
	_, err := app.insertUserWithID(ctx, userID, name, email)
	return err
}

func (app *MobileApp) createUserWithID(ctx context.Context, userID, name, email string) error {
	_, err := app.insertUserWithID(ctx, userID, name, email)
	return err
}

// insertUserWithID is the internal implementation for creating users.
func (app *MobileApp) insertUserWithID(ctx context.Context, userID, name, email string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := app.stmts.createUser.ExecContext(ctx, userID, name, email, now, now)

	if err != nil {
		return "", fmt.Errorf("failed to create user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("👤 User created", "id", userID, "name", name)

	return userID, nil
}

func (app *MobileApp) createUserTx(tx *sql.Tx, userID, name, email string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	stmt := tx.StmtContext(context.Background(), app.stmts.createUser)
	defer stmt.Close()
	_, err := stmt.ExecContext(context.Background(), userID, name, email, now, now)
	if err != nil {
		return fmt.Errorf("failed to create user (tx): %w", err)
	}
	return nil
}

func (app *MobileApp) createPost(title, content, authorID string) (string, error) {
	postID := uuid.New().String()
	return app.insertPostWithID(context.Background(), postID, authorID, title, content)
}

func (app *MobileApp) createPostWithID(ctx context.Context, postID, authorID, title, content string) error {
	_, err := app.insertPostWithID(ctx, postID, authorID, title, content)
	return err
}

// insertPostWithID is the internal implementation for creating posts.
func (app *MobileApp) insertPostWithID(ctx context.Context, postID, authorID, title, content string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := app.stmts.createPost.ExecContext(ctx, postID, title, content, authorID, now, now)

	if err != nil {
		return "", fmt.Errorf("failed to create post: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	//app.logger.Info("📝 Post created", "id", postID, "title", title, "author_id", authorID)

	return postID, nil
}

func (app *MobileApp) createPostTx(tx *sql.Tx, postID, authorID, title, content string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	stmt := tx.StmtContext(context.Background(), app.stmts.createPost)
	defer stmt.Close()
	_, err := stmt.ExecContext(context.Background(), postID, title, content, authorID, now, now)
	if err != nil {
		return fmt.Errorf("failed to create post (tx): %w", err)
	}
	return nil
}

func (app *MobileApp) updateUser(userID, name, email string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := app.stmts.updateUser.Exec(name, email, now, userID)

	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("👤 User updated", "id", userID, "name", name)

	return nil
}

func (app *MobileApp) updatePost(postID, title, content string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := app.stmts.updatePost.Exec(title, content, now, postID)
	if err != nil {
		return fmt.Errorf("failed to update post: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	//app.logger.Info("✏️ Post updated", "id", postID, "title", title)

	return nil
}

func (app *MobileApp) deletePost(postID string) error {
	_, err := app.stmts.deletePost.Exec(postID)
	if err != nil {
		return fmt.Errorf("failed to delete post: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	//app.logger.Info("🗑️ Post deleted", "id", postID)

	return nil
}

func (app *MobileApp) deleteUser(userID string) error {
	_, err := app.stmts.deleteUser.Exec(userID)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("🗑️ User deleted", "id", userID)

	return nil
}

func (app *MobileApp) deleteUserWithContext(ctx context.Context, userID string) error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}

	_, err := app.stmts.deleteUser.ExecContext(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("🗑️ User deleted", "user_id", userID)
	return nil
}

// getPendingCount returns the number of locally dirty rows waiting for bundle push.
func (app *MobileApp) getPendingCount() int {
	if app.db == nil {
		return 0
	}

	var count int
	err := app.db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

func (app *MobileApp) pendingChangesCount(ctx context.Context) (int, error) {
	if app.db == nil {
		return 0, fmt.Errorf("database not initialized")
	}

	var count int
	err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&count)
	if err != nil {
		return 0, nil
	}
	return count, nil
}

// close cleans up the mobile app.
func (app *MobileApp) close() error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if verboseLog && app.isRunning {
		app.logger.Info("🔄 App shutting down")
	}

	if app.sync != nil {
		app.sync.Stop()
	}

	if app.client != nil {
		app.client.Stop(context.Background())
		if err := app.client.Close(); err != nil {
			return fmt.Errorf("failed to close oversqlite client: %w", err)
		}
		app.client = nil
	}
	if err := app.closeStatements(); err != nil {
		return err
	}

	if app.db != nil {
		if err := app.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
		app.db = nil
	}

	// Clean up database file (unless preservation is requested)
	if app.config.DatabaseFile != ":memory:" && !app.config.PreserveDB {
		os.Remove(app.config.DatabaseFile)
	} else if app.config.PreserveDB && app.config.DatabaseFile != ":memory:" {
		if verboseLog {
			app.logger.Info("📁 Database file preserved for manual inspection", "path", app.config.DatabaseFile)
		}
	}

	app.isRunning = false
	if verboseLog {
		app.logger.Info("✅ App shutdown complete")
	}

	return nil
}

func (app *MobileApp) database() *sql.DB {
	return app.db
}

func (app *MobileApp) currentClient() *oversqlite.Client {
	return app.client
}

func (app *MobileApp) currentSession() *Session {
	return app.session
}

func (app *MobileApp) currentUI() *UISimulator {
	return app.ui
}

func (app *MobileApp) pauseSync() {
	if app.client != nil {
		app.client.PauseUploads()
		app.client.PauseDownloads()
	}
}

func (app *MobileApp) resumeSync() {
	if app.client != nil {
		app.client.ResumeUploads()
		app.client.ResumeDownloads()
	}
}

func (app *MobileApp) stopSync() {
	if app.sync != nil {
		app.sync.Stop()
	}
}

func (app *MobileApp) startSync(ctx context.Context) error {
	if app.sync != nil {
		return app.sync.Start(ctx)
	}
	return nil
}

// rebuildKeepSource rebuilds local state from the authoritative server snapshot.
func (app *MobileApp) rebuildKeepSource(ctx context.Context) error {
	if app.client == nil {
		return fmt.Errorf("no oversqlite client available")
	}

	app.logger.Info("🔄 Starting rebuild")
	app.ui.SetBanner("Downloading data...")

	report, err := app.client.Rebuild(ctx)
	if err != nil {
		app.ui.SetBanner("Download failed")
		return fmt.Errorf("rebuild failed: %w", err)
	}
	if verboseLog {
		app.logger.Info("Rebuild completed", "outcome", report.Outcome, "restored", report.Restore != nil)
	}

	app.ui.SetBanner("Data downloaded")
	app.logger.Info("✅ Rebuild completed successfully")

	return nil
}
func (app *MobileApp) resetSnapshotTransferDiagnostics() {
	if app.client != nil {
		app.client.ResetSnapshotTransferDiagnostics()
	}
}

func (app *MobileApp) snapshotTransferDiagnostics() oversqlite.SnapshotTransferStats {
	if app.client == nil {
		return oversqlite.SnapshotTransferStats{}
	}
	return app.client.SnapshotTransferDiagnostics()
}

func (app *MobileApp) resetPushTransferDiagnostics() {
	if app.client != nil {
		app.client.ResetPushTransferDiagnostics()
	}
}

func (app *MobileApp) pushTransferDiagnostics() oversqlite.PushTransferStats {
	if app.client == nil {
		return oversqlite.PushTransferStats{}
	}
	return app.client.PushTransferDiagnostics()
}

// pushPending uploads the full current dirty set and refreshes the local UI state.
func (app *MobileApp) pushPending(ctx context.Context) error {
	if app.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// Perform synchronous upload with retry logic (increased delays for high-load scenarios)
	const maxRetries = 5
	const retryDelay = 500 * time.Millisecond

	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		var report oversqlite.PushReport
		report, err = app.client.PushPending(ctx)
		if err == nil {
			if verboseLog {
				app.logger.Info("Upload finished", "outcome", report.Outcome)
			}
			break
		}

		if attempt < maxRetries {
			// Exponential backoff with jitter for high-load scenarios
			backoffDelay := time.Duration(attempt) * retryDelay
			app.logger.Warn("Upload attempt failed, retrying", "attempt", attempt, "backoff_ms", backoffDelay.Milliseconds(), "error", err.Error())
			time.Sleep(backoffDelay)
			continue
		}

		return fmt.Errorf("upload failed after %d attempts: %w", maxRetries, err)
	}

	// Refresh UI state after upload
	app.ui.SetPendingBadge(app.getPendingCount())

	return nil
}

// pullToStable performs one bundle-era pull session and returns the number of
// newly applied bundles along with the latest durable bundle checkpoint.
func (app *MobileApp) pullToStable(ctx context.Context) (applied int, nextAfter int64, err error) {
	if app.client == nil {
		return 0, 0, fmt.Errorf("client not initialized")
	}

	// Perform synchronous download with retry logic (increased delays for high-load scenarios)
	const maxRetries = 5
	const retryDelay = 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		before, err := app.lastServerSeqSeen(ctx)
		if err != nil {
			return 0, 0, err
		}
		var report oversqlite.RemoteSyncReport
		report, err = app.client.PullToStable(ctx)
		if err == nil {
			nextAfter, err = app.lastServerSeqSeen(ctx)
			if err != nil {
				return 0, 0, err
			}
			if report.Outcome == oversqlite.RemoteSyncOutcomeSkippedPaused {
				applied = 0
			} else if nextAfter > before {
				applied = int(nextAfter - before)
			} else {
				applied = 0
			}
			break
		}
		if attempt < maxRetries {
			// Exponential backoff with jitter for high-load scenarios
			backoffDelay := time.Duration(attempt) * retryDelay
			app.logger.Warn("Download attempt failed, retrying", "attempt", attempt, "backoff_ms", backoffDelay.Milliseconds(), "error", err.Error())
			time.Sleep(backoffDelay)
			continue
		}
		return 0, 0, fmt.Errorf("download failed after %d attempts: %w", maxRetries, err)
	}

	// Refresh UI state after download (if any changes were applied)
	if applied > 0 {
		app.ui.SetPendingBadge(app.getPendingCount())
	}

	return applied, nextAfter, nil
}

// GetDatabasePath returns the path to the SQLite database file
func (app *MobileApp) GetDatabasePath() string {
	return app.config.DatabaseFile
}

func (app *MobileApp) lastServerSeqSeen(ctx context.Context) (int64, error) {
	var lastSeq int64
	err := app.db.QueryRowContext(ctx, `
		SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1
	`).Scan(&lastSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get last bundle seq seen: %w", err)
	}
	return lastSeq, nil
}

func (app *MobileApp) userCount(ctx context.Context) (int, error) {
	if app.db == nil {
		return 0, fmt.Errorf("database not initialized")
	}

	var count int
	err := app.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count users: %w", err)
	}
	return count, nil
}

func (app *MobileApp) hasUser(ctx context.Context, userID string) (bool, error) {
	if app.db == nil {
		return false, fmt.Errorf("database not initialized")
	}

	var count int
	err := app.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check user existence: %w", err)
	}
	return count > 0, nil
}
