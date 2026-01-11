package simulator

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// MobileAppConfig holds configuration for a mobile app instance
type MobileAppConfig struct {
	DatabaseFile     string
	ServerURL        string
	UserID           string
	SourceID         string
	DeviceName       string
	JWTSecret        string
	OversqliteConfig *oversqlite.Config
	PreserveDB       bool // Preserve database file after app shutdown
	Logger           *slog.Logger
}

// MobileApp represents a simulated mobile application
type MobileApp struct {
	config *MobileAppConfig
	logger *slog.Logger

	db     *sql.DB
	client *oversqlite.Client

	session *Session
	ui      *UISimulator
	sync    *SyncManager

	isRunning bool
	mu        sync.RWMutex
}

func NewMobileApp(config *MobileAppConfig) (*MobileApp, error) {
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

	app.session = NewSession(config.UserID, config.SourceID, config.JWTSecret, logger)
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

	client, err := oversqlite.NewClient(
		db,
		app.config.ServerURL,
		app.config.UserID,
		app.config.SourceID,
		app.tokenFunc,
		app.config.OversqliteConfig,
	)
	if err != nil {
		return fmt.Errorf("failed to create oversqlite client: %w", err)
	}
	app.client = client

	app.logger.Info("Database initialized successfully",
		"file", app.config.DatabaseFile,
		"user_id", app.config.UserID,
		"source_id", app.config.SourceID)

	return nil
}

// createBusinessTables creates the local business tables that will be synced
func (app *MobileApp) createBusinessTables() error {
	// Create local users table (SQLite)
	createUsersSQL := `
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
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
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			FOREIGN KEY (author_id) REFERENCES users(id)
		)`

	if _, err := app.db.Exec(createPostsSQL); err != nil {
		return fmt.Errorf("failed to create posts table: %w", err)
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

// OnLaunch simulates app launch lifecycle
func (app *MobileApp) OnLaunch(ctx context.Context) error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if app.isRunning {
		return fmt.Errorf("app is already running")
	}
	app.logger.Info("🚀 App launching", "device", app.config.DeviceName)
	app.ui.SetBanner("Starting up...")

	// Try to restore session
	if app.session.CanRestore() {
		app.logger.Info("Restoring previous session")
		if err := app.session.Restore(); err != nil {
			app.logger.Warn("Failed to restore session", "error", err)
			app.ui.SetBanner("Session expired. Sign in to sync.")
			app.ui.SetPendingBadge(app.getPendingCount())
			return nil
		}

		app.logger.Info("Session restored successfully", "user_id", app.session.GetUserID())

		// Ensure the local oversqlite client is bootstrapped before starting background sync loops.
		// Under high parallelism, background loops can tick before SignIn/Bootstrap runs and hit
		// sql.ErrNoRows on _sync_client_info.
		if err := app.client.Bootstrap(ctx, false); err != nil {
			app.logger.Error("Failed to bootstrap client after session restore", "error", err)
			app.ui.SetBanner("Setup failed")
			return fmt.Errorf("bootstrap after restore failed: %w", err)
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
	app.logger.Info("✅ App launched successfully")

	return nil
}

// OnSignIn simulates user sign-in
func (app *MobileApp) OnSignIn(ctx context.Context, userID string) error {
	app.logger.Info("👤 User signing in", "user_id", userID)

	// Create new session
	if err := app.session.SignIn(userID, app.config.SourceID); err != nil {
		app.ui.SetBanner("Sign in failed")
		return fmt.Errorf("sign in failed: %w", err)
	}

	app.ui.SetBanner("Setting up sync...")
	if err := app.client.Bootstrap(ctx, false); err != nil {
		app.logger.Error("Failed to bootstrap client", "error", err)
		app.ui.SetBanner("Setup failed")
		return fmt.Errorf("bootstrap failed: %w", err)
	}

	if err := app.sync.Start(ctx); err != nil {
		app.logger.Error("Failed to start sync after sign in", "error", err)
		app.ui.SetBanner("Sync failed to start")
		return err
	}

	app.ui.SetBanner("Syncing...")
	app.ui.SetPendingBadge(app.getPendingCount())

	app.logger.Info("✅ User signed in successfully", "user_id", userID)
	return nil
}

// SignIn is a convenience method that wraps OnSignIn
func (app *MobileApp) SignIn(ctx context.Context, userID string) error {
	return app.OnSignIn(ctx, userID)
}

// OnSignOut simulates user sign-out
func (app *MobileApp) OnSignOut(ctx context.Context) error {
	app.logger.Info("👋 User signing out")

	app.sync.Stop()
	app.session.SignOut()

	app.ui.SetBanner("Offline mode. Sign in to sync.")
	app.ui.SetPendingBadge(app.getPendingCount())

	app.logger.Info("✅ User signed out successfully")
	return nil
}

// CreateUser creates a new user record
func (app *MobileApp) CreateUser(name, email string) (string, error) {
	userID := uuid.New().String()
	return app.createUserWithID(context.Background(), userID, name, email)
}

// CreateUserWithContext creates a new user record with context and specified ID
func (app *MobileApp) CreateUserWithContext(ctx context.Context, userID, name, email string) error {
	_, err := app.createUserWithID(ctx, userID, name, email)
	return err
}

// CreateUserWithID creates a new user record with specified ID
func (app *MobileApp) CreateUserWithID(ctx context.Context, userID, name, email string) error {
	_, err := app.createUserWithID(ctx, userID, name, email)
	return err
}

// CreateUserWithIDReturningID creates a new user record with specified ID and returns the ID
func (app *MobileApp) CreateUserWithIDReturningID(userID, name, email string) (string, error) {
	return app.createUserWithID(context.Background(), userID, name, email)
}

// createUserWithID is the internal implementation for creating users
func (app *MobileApp) createUserWithID(ctx context.Context, userID, name, email string) (string, error) {
	now := time.Now().Unix()

	_, err := app.db.ExecContext(ctx, `
		INSERT INTO users (id, name, email, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)`,
		userID, name, email, now, now)

	if err != nil {
		return "", fmt.Errorf("failed to create user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("👤 User created", "id", userID, "name", name)

	return userID, nil
}

// CreateUserTx inserts a user row within an existing transaction
func (app *MobileApp) CreateUserTx(tx *sql.Tx, userID, name, email string) error {
	now := time.Now().Unix()
	_, err := tx.Exec(`
        INSERT INTO users (id, name, email, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)`,
		userID, name, email, now, now)
	if err != nil {
		return fmt.Errorf("failed to create user (tx): %w", err)
	}
	return nil
}

// CreatePost creates a new post record
func (app *MobileApp) CreatePost(title, content, authorID string) (string, error) {
	postID := uuid.New().String()
	return app.createPostWithID(context.Background(), postID, authorID, title, content)
}

// CreatePostWithID creates a new post record with specified ID and author
func (app *MobileApp) CreatePostWithID(ctx context.Context, postID, authorID, title, content string) error {
	_, err := app.createPostWithID(ctx, postID, authorID, title, content)
	return err
}

// createPostWithID is the internal implementation for creating posts
func (app *MobileApp) createPostWithID(ctx context.Context, postID, authorID, title, content string) (string, error) {
	now := time.Now().Unix()

	_, err := app.db.ExecContext(ctx, `
		INSERT INTO posts (id, title, content, author_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		postID, title, content, authorID, now, now)

	if err != nil {
		return "", fmt.Errorf("failed to create post: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	//app.logger.Info("📝 Post created", "id", postID, "title", title, "author_id", authorID)

	return postID, nil
}

// CreatePostTx inserts a post row within an existing transaction
func (app *MobileApp) CreatePostTx(tx *sql.Tx, postID, authorID, title, content string) error {
	now := time.Now().Unix()
	_, err := tx.Exec(`
        INSERT INTO posts (id, title, content, author_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)`,
		postID, title, content, authorID, now, now)
	if err != nil {
		return fmt.Errorf("failed to create post (tx): %w", err)
	}
	return nil
}

// UpdateUser updates an existing user record
func (app *MobileApp) UpdateUser(userID, name, email string) error {
	now := time.Now().Unix()

	_, err := app.db.Exec(`
		UPDATE users SET name = ?, email = ?, updated_at = ?
		WHERE id = ?`,
		name, email, now, userID)

	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("👤 User updated", "id", userID, "name", name)

	return nil
}

// UpdatePost updates an existing post record
func (app *MobileApp) UpdatePost(postID, title, content string) error {
	now := time.Now().Unix()

	_, err := app.db.Exec(`
		UPDATE posts SET title = ?, content = ?, updated_at = ?
		WHERE id = ?`,
		title, content, now, postID)
	if err != nil {
		return fmt.Errorf("failed to update post: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("✏️ Post updated", "id", postID, "title", title)

	return nil
}

// DeletePost deletes a post record
func (app *MobileApp) DeletePost(postID string) error {
	_, err := app.db.Exec(`DELETE FROM posts WHERE id = ?`, postID)
	if err != nil {
		return fmt.Errorf("failed to delete post: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("🗑️ Post deleted", "id", postID)

	return nil
}

// DeleteUser deletes a user record
func (app *MobileApp) DeleteUser(userID string) error {
	_, err := app.db.Exec(`DELETE FROM users WHERE id = ?`, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("🗑️ User deleted", "id", userID)

	return nil
}

// DeleteUserWithContext deletes a user record with context
func (app *MobileApp) DeleteUserWithContext(ctx context.Context, userID string) error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}

	_, err := app.db.ExecContext(ctx, `DELETE FROM users WHERE id = ?`, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	app.ui.SetPendingBadge(app.getPendingCount())
	app.logger.Info("🗑️ User deleted", "user_id", userID)
	return nil
}

// getPendingCount returns the number of pending sync operations
func (app *MobileApp) getPendingCount() int {
	if app.db == nil {
		return 0
	}

	var count int
	err := app.db.QueryRow(`SELECT COUNT(*) FROM _sync_pending`).Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

// GetPendingChangesCount returns the number of pending sync operations (public method)
func (app *MobileApp) GetPendingChangesCount(ctx context.Context) (int, error) {
	if app.db == nil {
		return 0, fmt.Errorf("database not initialized")
	}

	var count int
	err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_pending`).Scan(&count)
	if err != nil {
		return 0, nil
	}
	return count, nil
}

// Close cleans up the mobile app
func (app *MobileApp) Close() error {
	app.mu.Lock()
	defer app.mu.Unlock()

	if !app.isRunning {
		return nil
	}

	app.logger.Info("🔄 App shutting down")

	if app.sync != nil {
		app.sync.Stop()
	}

	if app.client != nil {
		app.client.Stop(context.Background())
	}

	if app.db != nil {
		app.db.Close()
	}

	// Clean up database file (unless preservation is requested)
	if app.config.DatabaseFile != ":memory:" && !app.config.PreserveDB {
		os.Remove(app.config.DatabaseFile)
	} else if app.config.PreserveDB && app.config.DatabaseFile != ":memory:" {
		app.logger.Info("📁 Database file preserved for manual inspection", "path", app.config.DatabaseFile)
	}

	app.isRunning = false
	app.logger.Info("✅ App shutdown complete")

	return nil
}

// GetDatabase returns the SQLite database connection
func (app *MobileApp) GetDatabase() *sql.DB {
	return app.db
}

// GetClient returns the oversqlite client
func (app *MobileApp) GetClient() *oversqlite.Client {
	return app.client
}

// GetSession returns the session manager
func (app *MobileApp) GetSession() *Session {
	return app.session
}

// GetUI returns the UI simulator
func (app *MobileApp) GetUI() *UISimulator {
	return app.ui
}

// ResetApplyMode forces _sync_client_info.apply_mode back to 0 for this user.
// This ensures local change-tracking triggers are active even if a previous
// download transaction left apply_mode=1 due to an interrupted flow.
func (app *MobileApp) ResetApplyMode(ctx context.Context) error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}
	_, err := app.db.ExecContext(ctx, `UPDATE _sync_client_info SET apply_mode = 0 WHERE user_id = ?`, app.config.UserID)
	return err
}

// PauseSync pauses both uploads and downloads in the oversqlite client
func (app *MobileApp) PauseSync() {
	if app.client != nil {
		app.client.PauseUploads()
		app.client.PauseDownloads()
	}
}

// ResumeSync resumes both uploads and downloads in the oversqlite client
func (app *MobileApp) ResumeSync() {
	if app.client != nil {
		app.client.ResumeUploads()
		app.client.ResumeDownloads()
	}
}

// StopSync stops background sync loops (uploader/downloader)
func (app *MobileApp) StopSync() {
	if app.sync != nil {
		app.sync.Stop()
	}
}

// StartSync starts background sync loops
func (app *MobileApp) StartSync(ctx context.Context) error {
	if app.sync != nil {
		return app.sync.Start(ctx)
	}
	return nil
}

// IsRunning returns whether the app is currently running
func (app *MobileApp) IsRunning() bool {
	app.mu.RLock()
	defer app.mu.RUnlock()
	return app.isRunning
}

// PerformHydration downloads all user data from the server
func (app *MobileApp) PerformHydration(ctx context.Context) error {
	if app.client == nil {
		return fmt.Errorf("no oversqlite client available")
	}

	app.logger.Info("🔄 Starting hydration")
	app.ui.SetBanner("Downloading data...")

	if err := app.client.Hydrate(ctx); err != nil {
		app.ui.SetBanner("Download failed")
		return fmt.Errorf("hydration failed: %w", err)
	}

	app.ui.SetBanner("Data downloaded")
	app.logger.Info("✅ Hydration completed successfully")

	return nil
}

// PerformRecoveryHydration downloads all user data including own changes (for recovery)
func (app *MobileApp) PerformRecoveryHydration(ctx context.Context) error {
	if app.client == nil {
		return fmt.Errorf("no oversqlite client available")
	}

	app.logger.Info("🔄 Starting recovery hydration (include_self=true)")
	app.ui.SetBanner("Recovering data...")

	if err := app.client.HydrateWithSelf(ctx); err != nil {
		app.ui.SetBanner("Recovery failed")
		return fmt.Errorf("recovery hydration failed: %w", err)
	}

	app.ui.SetBanner("Data recovered")
	app.logger.Info("✅ Recovery hydration completed successfully")

	return nil
}

// PerformSyncUpload performs a synchronous upload operation and refreshes UI
func (app *MobileApp) PerformSyncUpload(ctx context.Context) error {
	if app.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// Perform synchronous upload with retry logic (increased delays for high-load scenarios)
	const maxRetries = 5
	const retryDelay = 500 * time.Millisecond

	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = app.client.UploadOnce(ctx)
		if err == nil {
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

// PerformSyncDownloadUntil performs a synchronous download with a **server-side ceiling** (until watermark).
// This ensures the server does not return rows beyond the specified `until` server_id,
// which prevents paging from skipping older rows when new changes are being appended concurrently.
//
// Why this exists:
// In parallel test scenarios a phone may capture an upload watermark W at Stage 2.
// If the client downloads without passing `until=W`, the server may interleave rows > W,
// the client's last_server_seq_seen will jump forward, and earlier rows <= W may be skipped.
// This helper attempts to call an extended download method on the oversqlite client that
// accepts an `until` parameter. If it's not available, it falls back to the legacy DownloadOnce.
//
// The method tries several optional interfaces to avoid breaking builds with older clients:
//  1. DownloadOnceWindowed(ctx, limit, until, includeSelf, schema)
//  2. DownloadOnceUntil(ctx, limit, until)
//  3. DownloadOnceWithQuery(ctx, limit, map[string]string)  // adds query params such as until/include_self
//
// If none of the above are implemented by the client, it logs a warning and falls back to DownloadOnce.
func (app *MobileApp) PerformSyncDownloadUntil(ctx context.Context, limit int, until int64) (applied int, nextAfter int64, err error) {
	if app.client == nil {
		return 0, 0, fmt.Errorf("client not initialized")
	}

	// Optional interfaces the oversqlite client may implement
	type dlWindowed interface {
		DownloadOnceWindowed(ctx context.Context, limit int, until int64, includeSelf bool, schema string) (int, int64, error)
	}
	type dlUntil interface {
		DownloadOnceUntil(ctx context.Context, limit int, until int64) (int, int64, error)
	}
	type dlWithQuery interface {
		DownloadOnceWithQuery(ctx context.Context, limit int, query map[string]string) (int, int64, error)
	}

	const maxRetries = 5
	const retryDelay = 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		switch c := any(app.client).(type) {
		case dlWindowed:
			applied, nextAfter, err = c.DownloadOnceWindowed(ctx, limit, until, false /* includeSelf */, "" /* schema: default */)
		case dlUntil:
			applied, nextAfter, err = c.DownloadOnceUntil(ctx, limit, until)
		case dlWithQuery:
			q := map[string]string{
				"until":        fmt.Sprintf("%d", until),
				"include_self": "false",
			}
			applied, nextAfter, err = c.DownloadOnceWithQuery(ctx, limit, q)
		default:
			app.logger.Warn("Download ceiling not supported by client; falling back to legacy download",
				"until", until, "limit", limit)
			applied, nextAfter, err = app.client.DownloadOnce(ctx, limit)
		}

		if err == nil {
			break
		}
		if attempt < maxRetries {
			backoff := time.Duration(attempt) * retryDelay
			app.logger.Warn("DownloadUntil attempt failed, retrying",
				"attempt", attempt, "backoff_ms", backoff.Milliseconds(), "error", err.Error(), "until", until)
			time.Sleep(backoff)
			continue
		}
		return 0, 0, fmt.Errorf("download (until=%d) failed after %d attempts: %w", until, maxRetries, err)
	}

	// Refresh UI state after download (if any changes were applied)
	if applied > 0 {
		app.ui.SetPendingBadge(app.getPendingCount())
	}

	return applied, nextAfter, nil
}

// PerformSyncDownload performs a synchronous download operation and refreshes UI
func (app *MobileApp) PerformSyncDownload(ctx context.Context, limit int) (applied int, nextAfter int64, err error) {
	if app.client == nil {
		return 0, 0, fmt.Errorf("client not initialized")
	}

	// Perform synchronous download with retry logic (increased delays for high-load scenarios)
	const maxRetries = 5
	const retryDelay = 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		applied, nextAfter, err = app.client.DownloadOnce(ctx, limit)
		if err == nil {
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

// GetLastServerSeqSeen returns the last server sequence number seen by this client
func (app *MobileApp) GetLastServerSeqSeen(ctx context.Context) (int64, error) {
	var lastSeq int64
	err := app.db.QueryRowContext(ctx, `
		SELECT last_server_seq_seen FROM _sync_client_info WHERE user_id = ?
	`, app.config.UserID).Scan(&lastSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get last server seq seen: %w", err)
	}
	return lastSeq, nil
}

// GetCurrentWindowUntil returns the latest known server watermark from the client_info row.
// This is updated from upload/download responses and is intended for windowed paging (`until`).
func (app *MobileApp) GetCurrentWindowUntil(ctx context.Context) (int64, error) {
	var until int64
	err := app.db.QueryRowContext(ctx, `
		SELECT current_window_until FROM _sync_client_info WHERE user_id = ?
	`, app.config.UserID).Scan(&until)
	if err != nil {
		return 0, fmt.Errorf("failed to get current window until: %w", err)
	}
	return until, nil
}

// SyncDownloadToWatermark downloads pages until the server-side ceiling `until` is reached.
// It repeatedly calls PerformSyncDownloadUntil until nextAfter >= until or a page returns 0.
func (app *MobileApp) SyncDownloadToWatermark(ctx context.Context, limit int, until int64) (totalApplied int, lastAfter int64, err error) {
	var applied int
	var nextAfter int64
	for {
		applied, nextAfter, err = app.PerformSyncDownloadUntil(ctx, limit, until)
		if err != nil {
			return totalApplied, lastAfter, err
		}
		totalApplied += applied
		lastAfter = nextAfter
		if applied == 0 || nextAfter >= until {
			break
		}
	}
	return totalApplied, lastAfter, nil
}

// GetUserCount returns the number of users in the database
func (app *MobileApp) GetUserCount(ctx context.Context) (int, error) {
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

// HasUser checks if a user with the given ID exists in the database
func (app *MobileApp) HasUser(ctx context.Context, userID string) (bool, error) {
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

// ResetSyncState resets the sync state to download all changes from the beginning
func (app *MobileApp) ResetSyncState(ctx context.Context) error {
	if app.db == nil {
		return fmt.Errorf("database not initialized")
	}

	_, err := app.db.ExecContext(ctx, "UPDATE _sync_client_info SET last_server_seq_seen = 0 WHERE user_id = ?", app.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to reset sync state: %w", err)
	}

	app.logger.Info("🔄 Reset sync state to download all changes", "user_id", app.config.UserID)
	return nil
}
