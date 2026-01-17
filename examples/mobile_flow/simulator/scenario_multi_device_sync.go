package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// MultiDeviceSyncScenario tests a complex multi-device scenario for the SAME user:
// - Sign in with user A and device 1
// - Sign in with user A and device 2
// - Sync device 1, sync device 2
// - For device 1 let's add person record
// - For device 2 let's add person record
// - Sync device 1, sync device 2
// - Let's remove first record for device 1
// - Let's sync device 1
// - Verify the deleted record doesn't get restored (this reproduces the KMP bug)
type MultiDeviceSyncScenario struct {
	*BaseScenario
	device2App *MobileApp // Second device for testing multi-device sync
	device1ID  string     // Device 1 identifier
	device2ID  string     // Device 2 identifier
	userA      string     // User A identifier (same user on both devices)
	personAID  string     // Person record ID created by device 1
	personBID  string     // Person record ID created by device 2
}

// NewMultiDeviceSyncScenario creates a new multi-device sync scenario
func NewMultiDeviceSyncScenario(simulator *Simulator) Scenario {
	return &MultiDeviceSyncScenario{
		BaseScenario: NewBaseScenario(simulator, "multi-device-sync"),
	}
}

func (s *MultiDeviceSyncScenario) Name() string {
	return "multi-device-sync"
}

func (s *MultiDeviceSyncScenario) Description() string {
	return "Single-user ulti-device sync: Test delete operations and sync window conflicts"
}

// Setup creates two mobile apps representing different devices for the same user
func (s *MultiDeviceSyncScenario) Setup(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🔧 Setting up multi-device sync scenario for SAME user")

	// Prefer simulator-provided user/source IDs (used by parallel runner), but keep a random fallback for
	// interactive runs where this scenario doesn't have a dedicated ScenarioConfig entry.
	if s.config != nil && s.config.UserID != "" && s.config.UserID != "user-unknown" {
		s.userA = s.config.UserID
	} else {
		// Generate a single user identifier
		s.userA = "userA-" + uuid.New().String()[:8]
	}
	logger.Info("👤 Generated user ID", "userA", s.userA)

	// Generate device identifiers (both for the same user)
	sourceBase := fmt.Sprintf("device-%s", s.userA)
	if s.config != nil && s.config.SourceID != "" && s.config.SourceID != "device-unknown-001" {
		sourceBase = s.config.SourceID
	}
	s.device1ID = sourceBase + "-d1"
	s.device2ID = sourceBase + "-d2"
	logger.Info("📱 Generated device IDs", "device1_id", s.device1ID, "device2_id", s.device2ID)

	// Create device 1 for user A
	device1Config := &config.ScenarioConfig{UserID: s.userA}
	device1App, err := s.createDeviceApp(device1Config, s.device1ID, "Device 1 (User A)")
	if err != nil {
		return fmt.Errorf("failed to create device 1 app: %w", err)
	}
	s.app = device1App
	s.simulator.currentApp = s.app // enable sqlite path reporting for parallel runs

	// Create device 2 for the SAME user A
	device2Config := &config.ScenarioConfig{UserID: s.userA}
	device2App, err := s.createDeviceApp(device2Config, s.device2ID, "Device 2 (User A)")
	if err != nil {
		return fmt.Errorf("failed to create device 2 app: %w", err)
	}
	s.device2App = device2App

	// Launch both apps
	if err := s.app.OnLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch device 1 app: %w", err)
	}
	if err := s.device2App.OnLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch device 2 app: %w", err)
	}

	logger.Info("✅ Multi-device sync scenario setup complete")
	return nil
}

// createDeviceApp creates a mobile app for a specific device
func (s *MultiDeviceSyncScenario) createDeviceApp(scenarioConfig *config.ScenarioConfig, deviceID, deviceName string) (*MobileApp, error) {
	simCfg := s.simulator.GetConfig()

	// Create unique database file path using device ID and timestamp to prevent collisions
	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", deviceID, scenarioConfig.UserID, time.Now().UnixNano()))

	// Create oversqlite config
	oversqliteConfig := &oversqlite.Config{
		Schema: "business",
		Tables: []oversqlite.SyncTable{
			{TableName: "users"},
			{TableName: "posts"},
			{TableName: "files"},
			{TableName: "file_reviews"},
		},
		UploadLimit:   100, // Standard batch size
		DownloadLimit: 100, // Standard batch size
	}

	// Create mobile app config
	appConfig := &MobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        simCfg.ServerURL,
		UserID:           scenarioConfig.UserID,
		SourceID:         deviceID,
		DeviceName:       deviceName,
		JWTSecret:        simCfg.JWTSecret,
		OversqliteConfig: oversqliteConfig,
		PreserveDB:       simCfg.PreserveDB,
		Logger:           s.simulator.GetLogger(),
	}

	// Create mobile app
	app, err := NewMobileApp(appConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create mobile app: %w", err)
	}

	return app, nil
}

// Execute runs the multi-device sync scenario
func (s *MultiDeviceSyncScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("🎯 Starting Multi-Device Sync Scenario (SAME user)")
	logger.Info("📊 Strategy: Two devices under same user; test delete + sync window interactions")

	// Step 1: Sign in with user A on device 1
	logger.Info("👤 Step 1: Device 1 - Sign in with User A")
	err := s.app.SignIn(ctx, s.userA)
	if err != nil {
		return fmt.Errorf("failed to sign in user A on device 1: %w", err)
	}

	// Step 2: Sign in with user A on device 2
	logger.Info("👤 Step 2: Device 2 - Sign in with User A")
	err = s.device2App.SignIn(ctx, s.userA)
	if err != nil {
		return fmt.Errorf("failed to sign in user A on device 2: %w", err)
	}

	// Step 3: Sync device 1
	logger.Info("🔄 Step 3: Sync Device 1 (User A)")
	err = s.app.PerformHydration(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync device 1: %w", err)
	}

	// Step 4: Sync device 2
	logger.Info("🔄 Step 4: Sync Device 2 (User A)")
	err = s.device2App.PerformHydration(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync device 2: %w", err)
	}

	// Step 5: Device 1 adds person record
	logger.Info("📱 Step 5: Device 1 creates person record")
	s.personAID = uuid.New().String()
	err = s.app.CreateUserWithContext(ctx, s.personAID, "Person A (Device 1)", "persona@example.com")
	if err != nil {
		return fmt.Errorf("failed to create person on device 1: %w", err)
	}

	// Step 6: Device 2 adds person record
	logger.Info("📱 Step 6: Device 2 creates person record")
	s.personBID = uuid.New().String()
	err = s.device2App.CreateUserWithContext(ctx, s.personBID, "Person B (Device 2)", "personb@example.com")
	if err != nil {
		return fmt.Errorf("failed to create person on device 2: %w", err)
	}

	// Step 7: Sync device 1 (upload person A)
	logger.Info("🔄 Step 7: Sync Device 1 (upload person A)")
	err = s.app.PerformSyncUpload(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync device 1: %w", err)
	}
	// Post-upload download loop to mirror mobile app full sync
	limit := 500
	for {
		applied, _, err := s.app.PerformSyncDownload(ctx, limit)
		if err != nil {
			return fmt.Errorf("post-upload download (device 1) failed: %w", err)
		}
		if applied < limit {
			break
		}
	}

	// Step 8: Sync device 2 (upload person B)
	logger.Info("🔄 Step 8: Sync Device 2 (upload person B)")
	err = s.device2App.PerformSyncUpload(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync device 2: %w", err)
	}
	// Post-upload download loop to mirror mobile app full sync
	limit = 500
	for {
		applied, _, err := s.device2App.PerformSyncDownload(ctx, limit)
		if err != nil {
			return fmt.Errorf("post-upload download (device 2) failed: %w", err)
		}
		if applied < limit {
			break
		}
	}

	// Step 9: Delete person A record (this is the critical test)
	logger.Info("🗑️ Step 9: User A deletes person A record")
	err = s.app.DeleteUserWithContext(ctx, s.personAID)
	if err != nil {
		return fmt.Errorf("failed to delete person A: %w", err)
	}

	// Verify the record is deleted locally
	hasPersonA, err := s.app.HasUser(ctx, s.personAID)
	if err != nil {
		return fmt.Errorf("failed to check if person A exists: %w", err)
	}
	if hasPersonA {
		return fmt.Errorf("person A should be deleted locally but still exists")
	}
	logger.Info("✅ Person A deleted locally")

	// Step 10: Sync user A (upload delete operation)
	logger.Info("🔄 Step 10: Sync User A (upload delete operation)")
	err = s.app.PerformSyncUpload(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync user A after delete: %w", err)
	}
	// Post-upload download loop after delete (critical to mirror mobile app full sync)
	limit = 500
	for {
		applied, _, err := s.app.PerformSyncDownload(ctx, limit)
		if err != nil {
			return fmt.Errorf("post-delete download (device 1) failed: %w", err)
		}
		if applied < limit {
			break
		}
	}

	// Verify the record is still deleted after sync
	hasPersonAAfterSync, err := s.app.HasUser(ctx, s.personAID)
	if err != nil {
		return fmt.Errorf("failed to check if person A exists after sync: %w", err)
	}
	if hasPersonAAfterSync {
		logger.Error("🚨 BUG REPRODUCED: Person A was restored after sync!")
		logger.Error("This indicates the sync window strategy is incorrectly restoring deleted records")
		return fmt.Errorf("SYNC BUG: Deleted record was restored after sync")
	}

	logger.Info("✅ Person A remains deleted after sync - no bug detected")

	// Final convergence: ensure Device 2 pulls the deletion as in full sync flows
	logger.Info("🔄 Final convergence sync: Device 2 downloads deletions")
	limit = 500
	for {
		applied, _, err := s.device2App.PerformSyncDownload(ctx, limit)
		if err != nil {
			return fmt.Errorf("final convergence download (device 2) failed: %w", err)
		}
		if applied < limit {
			break
		}
	}

	// Verify both devices have the same expected records at the end
	logger.Info("✅ Verifying both devices have expected records for SAME user")
	err = s.verifyDataConsistency(ctx, logger)
	if err != nil {
		return fmt.Errorf("data consistency verification failed: %w", err)
	}

	logger.Info("🎉 Multi-Device Sync Scenario completed successfully")
	logger.Info("✅ Both devices have identical records as expected")
	return nil
}

// verifyDataConsistency checks the final state after delete operations
func (s *MultiDeviceSyncScenario) verifyDataConsistency(ctx context.Context, logger *slog.Logger) error {
	// Get user count from device 1
	device1Count, err := s.app.GetUserCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get user count from device 1: %w", err)
	}

	// Get user count from device 2
	device2Count, err := s.device2App.GetUserCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get user count from device 2: %w", err)
	}

	logger.Info("📊 Final user counts (same user)", "device1", device1Count, "device2", device2Count)

	// Device 1 should have 1 user (person B) after its deletion of person A is synced
	if device1Count != 1 {
		return fmt.Errorf("Device 1 should have 1 user after delete, got %d", device1Count)
	}

	// Device 2 should have 1 user (person B)
	if device2Count != 1 {
		return fmt.Errorf("Device 2 should have 1 user, got %d", device2Count)
	}

	// Verify person A is deleted on device 1
	hasPersonA, err := s.app.HasUser(ctx, s.personAID)
	if err != nil {
		return fmt.Errorf("failed to check if person A exists: %w", err)
	}
	if hasPersonA {
		return fmt.Errorf("person A should be deleted but still exists on device 1")
	}

	// Verify person B still exists on both devices
	hasPersonB1, err := s.app.HasUser(ctx, s.personBID)
	if err != nil {
		return fmt.Errorf("failed to check if person B exists on device 1: %w", err)
	}
	hasPersonB2, err := s.device2App.HasUser(ctx, s.personBID)
	if err != nil {
		return fmt.Errorf("failed to check if person B exists on device 2: %w", err)
	}
	if !hasPersonB1 || !hasPersonB2 {
		return fmt.Errorf("person B should exist on both devices (d1=%v, d2=%v)", hasPersonB1, hasPersonB2)
	}

	logger.Info("✅ Data consistency verification passed - delete operation worked correctly (same user)")
	return nil
}

// Verify performs server-side verification of the single-user multi-device sync scenario
func (s *MultiDeviceSyncScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil // Verification disabled
	}

	logger := s.simulator.GetLogger()
	logger.Info("🔍 Verifying Multi-Device Sync Scenario (same user)")

	// For the same user, we expect 1 user total on the server (person B)
	userCount, err := verifier.CountActualBusinessRecords(ctx, "business.users", s.userA)
	if err != nil {
		return fmt.Errorf("failed to count actual business users for user %s: %w", s.userA, err)
	}

	logger.Info("📊 Server user count", "user", s.userA, "count", userCount)

	if userCount != 1 {
		return fmt.Errorf("expected 1 user on server for %s (person B only), found %d", s.userA, userCount)
	}

	logger.Info("✅ Server verification passed - delete operation synced correctly (same user)")
	return nil
}

// Cleanup cleans up resources for both devices
func (s *MultiDeviceSyncScenario) Cleanup(ctx context.Context) error {
	var err1, err2 error

	if s.app != nil {
		err1 = s.app.Close()
	}

	if s.device2App != nil {
		err2 = s.device2App.Close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}
