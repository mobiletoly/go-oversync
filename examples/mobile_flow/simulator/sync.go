package simulator

import (
	"context"
	"log/slog"
	"sync"
	"time"
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

	sm.logger.Info("🔄 Starting sync loops")

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
	sm.logger.Info("✅ Sync loops started")

	return nil
}

// Stop stops the sync loops
func (sm *SyncManager) Stop() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.running {
		return
	}

	sm.logger.Info("🛑 Stopping sync loops")

	if sm.cancel != nil {
		sm.cancel()
	}

	sm.wg.Wait()

	sm.running = false
	sm.cancel = nil
	sm.uploader = nil
	sm.downloader = nil

	sm.logger.Info("✅ Sync loops stopped")
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
	if !u.app.GetUI().IsOnline() {
		return
	}

	if !u.app.GetSession().IsActive() {
		return
	}
	client := u.app.GetClient()
	if client == nil {
		u.logger.Error("No oversqlite client available")
		return
	}

	if err := client.UploadOnce(ctx); err != nil {
		u.logger.Error("Upload failed", "error", err)
		if isAuthError(err) {
			u.app.GetUI().SetBanner("Session expired. Sign in.")
		}
		return
	}

	u.app.GetUI().SetBanner("All changes saved")
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
	if !d.app.GetUI().IsOnline() {
		return
	}

	if !d.app.GetSession().IsActive() {
		return
	}
	client := d.app.GetClient()
	if client == nil {
		d.logger.Error("No oversqlite client available")
		return
	}

	_, _, err := client.DownloadOnce(ctx, 1000)
	if err != nil {
		d.logger.Error("Download failed", "error", err)
		if isAuthError(err) {
			d.app.GetUI().SetBanner("Session expired. Sign in.")
		}
		return
	}

	d.app.GetUI().SetBanner("All changes saved")
}

// TriggerWake wakes up the downloader
func (d *Downloader) TriggerWake() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// isAuthError checks if an error is authentication-related
func isAuthError(err error) bool {
	errStr := err.Error()
	return contains(errStr, "401") || contains(errStr, "403") ||
		contains(errStr, "unauthorized") || contains(errStr, "forbidden")
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
