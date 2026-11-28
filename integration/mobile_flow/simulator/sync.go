package simulator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/mobiletoly/go-oversync/oversqlite"
)

// SyncManager manages the upload and download sync loops
type SyncManager struct {
	app    *MobileApp
	logger *slog.Logger

	// Sync loops
	uploader   *Uploader
	downloader *Downloader

	// Control
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// NewSyncManager creates a new sync manager
func NewSyncManager(app *MobileApp, logger *slog.Logger) *SyncManager {
	return &SyncManager{
		app:    app,
		logger: logger,
	}
}

// Start begins the sync loops
func (sm *SyncManager) Start(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.running {
		return nil
	}

	if verboseLog {
		sm.logger.Info("🔄 Starting sync loops")
	}

	syncCtx, cancel := context.WithCancel(ctx)
	sm.cancel = cancel

	sm.uploader = NewUploader(sm.app, sm.logger)
	sm.downloader = NewDownloader(sm.app, sm.logger)
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.uploader.Run(syncCtx)
	}()

	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		sm.downloader.Run(syncCtx)
	}()

	sm.running = true
	if verboseLog {
		sm.logger.Info("✅ Sync loops started")
	}

	return nil
}

// Stop stops the sync loops
func (sm *SyncManager) Stop() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.running {
		return
	}

	if verboseLog {
		sm.logger.Info("🛑 Stopping sync loops")
	}

	if sm.cancel != nil {
		sm.cancel()
	}

	sm.wg.Wait()

	sm.running = false
	sm.cancel = nil
	sm.uploader = nil
	sm.downloader = nil

	if verboseLog {
		sm.logger.Info("✅ Sync loops stopped")
	}
}

// IsRunning returns whether sync loops are running
func (sm *SyncManager) IsRunning() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.running
}

// TriggerUpload signals the uploader to wake up
func (sm *SyncManager) TriggerUpload() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.running && sm.uploader != nil {
		sm.uploader.TriggerWake()
	}
}

// TriggerDownload signals the downloader to wake up
func (sm *SyncManager) TriggerDownload() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.running && sm.downloader != nil {
		sm.downloader.TriggerWake()
	}
}

// Uploader handles uploading local changes to the server
type Uploader struct {
	app    *MobileApp
	logger *slog.Logger
	wake   chan struct{}
}

// NewUploader creates a new uploader
func NewUploader(app *MobileApp, logger *slog.Logger) *Uploader {
	return &Uploader{
		app:    app,
		logger: logger,
		wake:   make(chan struct{}, 1),
	}
}

// Run runs the upload loop
func (u *Uploader) Run(ctx context.Context) {
	u.logger.Debug("📤 Upload loop started")
	defer u.logger.Debug("📤 Upload loop stopped")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.runOnce(ctx)
		case <-u.wake:
			u.runOnce(ctx)
		}
	}
}

// runOnce performs one upload cycle
func (u *Uploader) runOnce(ctx context.Context) {
	client, ok := backgroundSyncClient(u.app, u.logger)
	if !ok {
		return
	}

	report, err := client.PushPending(ctx)
	if err != nil {
		handleBackgroundSyncError(u.app, u.logger, "Upload failed", err)
		return
	}
	if report.Outcome == oversqlite.PushOutcomeSkippedPaused {
		return
	}

	u.app.currentUI().SetBanner("All changes saved")
}

// TriggerWake wakes up the uploader
func (u *Uploader) TriggerWake() {
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

// Downloader handles downloading changes from the server
type Downloader struct {
	app    *MobileApp
	logger *slog.Logger
	wake   chan struct{}
}

// NewDownloader creates a new downloader
func NewDownloader(app *MobileApp, logger *slog.Logger) *Downloader {
	return &Downloader{
		app:    app,
		logger: logger,
		wake:   make(chan struct{}, 1),
	}
}

// Run runs the download loop
func (d *Downloader) Run(ctx context.Context) {
	d.logger.Debug("📥 Download loop started")
	defer d.logger.Debug("📥 Download loop stopped")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runOnce(ctx)
		case <-d.wake:
			d.runOnce(ctx)
		}
	}
}

// runOnce performs one download cycle
func (d *Downloader) runOnce(ctx context.Context) {
	client, ok := backgroundSyncClient(d.app, d.logger)
	if !ok {
		return
	}

	report, err := client.PullToStable(ctx)
	if err != nil {
		handleBackgroundSyncError(d.app, d.logger, "Download failed", err)
		return
	}
	if report.Outcome == oversqlite.RemoteSyncOutcomeSkippedPaused {
		return
	}

	d.app.currentUI().SetBanner("All changes saved")
}

// TriggerWake wakes up the downloader
func (d *Downloader) TriggerWake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func backgroundSyncClient(app *MobileApp, logger *slog.Logger) (*oversqlite.Client, bool) {
	if !app.currentUI().IsOnline() {
		return nil, false
	}

	if !app.currentSession().IsActive() {
		return nil, false
	}

	client := app.currentClient()
	if client == nil {
		logger.Error("No oversqlite client available")
		return nil, false
	}
	return client, true
}

func handleBackgroundSyncError(app *MobileApp, logger *slog.Logger, message string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if isIgnorableBackgroundSyncError(err) {
		return
	}
	logger.Error(message, "error", err)
	if isAuthError(err) {
		app.currentUI().SetBanner("Session expired. Sign in.")
	}
}

// isAuthError checks if an error is authentication-related
func isAuthError(err error) bool {
	errStr := err.Error()
	return contains(errStr, "401") || contains(errStr, "403") ||
		contains(errStr, "unauthorized") || contains(errStr, "forbidden")
}

func isIgnorableBackgroundSyncError(err error) bool {
	if oversqlite.IsExpectedSyncContention(err) {
		return true
	}
	var pendingReplayErr *oversqlite.PendingPushReplayError
	return errors.As(err, &pendingReplayErr)
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || (len(s) > len(substr) &&
			(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
				containsAt(s, substr))))
}

// containsAt checks if string contains substring at any position
func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
