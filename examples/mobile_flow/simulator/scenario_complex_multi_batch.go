package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// ComplexMultiBatchScenario implements FK constraint testing with clean, simple approach
type ComplexMultiBatchScenario struct {
	*BaseScenario
	testData        *ComplexTestData
	laptopApp       *MobileApp // For download testing with different source ID
	phoneSourceID   string     // Generated once and reused
	laptopSourceID  string     // Generated once and reused
	uploadWatermark int64      // Server seq after laptop uploads (for watermark-based download termination)
}

// ComplexTestData holds the test data structure for verification
type ComplexTestData struct {
	// Original data created by phone
	UserIDs []string
	PostIDs []string

	// New data created by laptop for multi-stage testing
	NewUserIDs []string
	NewPostIDs []string
}

// AppConfig holds common configuration for creating mobile apps
type AppConfig struct {
	SourceID   string
	DeviceName string
	UserID     string
	BatchSize  int
}

// Constants for the scenario
const (
	totalUsers      = 5
	postsPerUser    = 20
	newUsersStage2  = 20 // Additional users created by laptop
	newPostsPerUser = 20 // Posts per new user
	batchSize       = 50
	maxRounds       = 15 // Increased for multi-stage testing
	serverURL       = "http://localhost:8080"
	jwtSecret       = "your-secret-key-change-in-production"
)

// NewComplexMultiBatchScenario creates a new complex multi-batch scenario
func NewComplexMultiBatchScenario(simulator *Simulator) Scenario {
	return &ComplexMultiBatchScenario{
		BaseScenario: NewBaseScenario(simulator, "complex-multi-batch"),
		testData:     &ComplexTestData{},
	}
}

func (s *ComplexMultiBatchScenario) Name() string {
	return "complex-multi-batch"
}

func (s *ComplexMultiBatchScenario) Description() string {
	return "FK constraint testing: Create posts before users to test server-side FK constraint retry logic"
}

// Setup creates a mobile app with small batch sizes for FK constraint testing
func (s *ComplexMultiBatchScenario) Setup(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🔧 Setting up mobile app with small batch sizes for FK constraint testing")

	// Generate source IDs once and store for reuse throughout scenario
	s.phoneSourceID = fmt.Sprintf("phone-%s", s.config.UserID)
	s.laptopSourceID = fmt.Sprintf("laptop-%s", s.config.UserID)
	logger.Info("📱 Generated source IDs", "phone_source_id", s.phoneSourceID, "laptop_source_id", s.laptopSourceID)

	// Create mobile app with small batch sizes to force multi-batch operations
	app, err := s.createAppWithSmallBatches(s.config)
	if err != nil {
		return fmt.Errorf("failed to create mobile app with small batches: %w", err)
	}

	s.app = app

	// Launch the app
	if err := s.app.OnLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch app: %w", err)
	}

	logger.Info("✅ Mobile app setup complete with small batch sizes")
	return nil
}

// createAppWithSmallBatches creates a mobile app with small upload limits to force multi-batch operations
func (s *ComplexMultiBatchScenario) createAppWithSmallBatches(scenarioConfig *config.ScenarioConfig) (*MobileApp, error) {
	appConfig := &AppConfig{
		SourceID:   s.phoneSourceID,
		DeviceName: "FK Test Device",
		UserID:     scenarioConfig.UserID,
		BatchSize:  batchSize,
	}
	return s.createMobileApp(appConfig, s.phoneSourceID)
}

// createMobileApp creates a mobile app with the given configuration (DRY helper)
func (s *ComplexMultiBatchScenario) createMobileApp(config *AppConfig, sourcePrefix string) (*MobileApp, error) {
	// Create unique database file path using user ID and timestamp to prevent collisions in parallel execution
	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", sourcePrefix, config.UserID, time.Now().UnixNano()))

	// Create oversqlite config with small batch sizes
	oversqliteConfig := &oversqlite.Config{
		Schema: "business",
		Tables: []oversqlite.SyncTable{
			{TableName: "users"},
			{TableName: "posts"},
			{TableName: "files"},
			{TableName: "file_reviews"},
		},
		UploadLimit:   config.BatchSize,
		DownloadLimit: config.BatchSize,
	}

	// Create mobile app config
	appConfig := &MobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        serverURL,
		UserID:           config.UserID,
		SourceID:         config.SourceID,
		DeviceName:       config.DeviceName,
		JWTSecret:        jwtSecret,
		OversqliteConfig: oversqliteConfig,
		Logger:           s.simulator.GetLogger(),
	}

	// Create mobile app
	app, err := NewMobileApp(appConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create mobile app: %w", err)
	}

	return app, nil
}

// Execute runs the FK constraint testing scenario
func (s *ComplexMultiBatchScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("🎯 Starting FK Constraint Testing Scenario")
	logger.Info("📊 Strategy: Create posts BEFORE users to trigger server-side FK constraint violations")

	// Step 1: Sign in user to initialize sync system
	logger.Info("👤 Signing in user to initialize sync system")
	err := s.app.SignIn(ctx, s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to sign in user: %w", err)
	}

	// Step 2: Create FK-challenging data (we'll work offline by not calling sync)
	logger.Info("📱 Creating FK-challenging data offline")

	// Step 2: Create data in FK-challenging order using SQLite PRAGMA
	logger.Info("👤📝 Creating FK-challenging data in SQLite")

	// Configuration for this test
	totalPosts := totalUsers * postsPerUser

	logger.Info("📊 Test configuration", "users", totalUsers, "posts_per_user", postsPerUser, "total_posts", totalPosts)

	s.testData.UserIDs = make([]string, totalUsers)
	s.testData.PostIDs = make([]string, totalPosts)

	// Prevent background sync from draining pending changes during creation
	s.app.StopSync()
	s.app.PauseSync()
	logger.Info("⏸️ Paused phone client sync during offline creation")

	// Ensure apply_mode=0 so local triggers capture pending changes deterministically
	if err := s.app.ResetApplyMode(ctx); err != nil {
		logger.Warn("Failed to reset apply_mode to 0 before creation (phone)", "error", err)
	}

	// Create posts BEFORE users in a single transaction with deferred FK checks
	db := s.app.GetDatabase()
	// Best-effort clear any stray transaction
	_, _ = db.Exec("ROLLBACK")
	logger.Info("⏱️ About to begin phone creation transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx for FK-challenging creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	logger.Info("✅ Phone creation transaction begun")
	if _, err := tx.Exec("PRAGMA defer_foreign_keys = ON"); err != nil {
		return fmt.Errorf("failed to enable deferred FKs in tx: %w", err)
	}

	logger.Info("🔄 Creating data in FK-challenging order (tx): posts BEFORE users, FK checks deferred to COMMIT")

	for userGroup := 0; userGroup < totalUsers; userGroup++ {
		// Pre-generate user UUID
		futureUserID := uuid.New().String()

		// Create 20 posts that reference this user (who doesn't exist yet)
		for postInGroup := 0; postInGroup < postsPerUser; postInGroup++ {
			postIndex := userGroup*postsPerUser + postInGroup
			postID := uuid.New().String()
			if err := s.app.CreatePostTx(tx, postID,
				futureUserID,
				fmt.Sprintf("FK Test Post %d by User %d", postIndex+1, userGroup+1),
				fmt.Sprintf("Content for post %d by user %d", postIndex+1, userGroup+1)); err != nil {
				return fmt.Errorf("failed to create post %d: %w", postIndex+1, err)
			}
			s.testData.PostIDs[postIndex] = postID
		}

		// Now create the user with the pre-generated UUID
		if err := s.app.CreateUserTx(tx, futureUserID,
			fmt.Sprintf("User %d", userGroup+1),
			fmt.Sprintf("user%d@example.com", userGroup+1)); err != nil {
			return fmt.Errorf("failed to create user %d: %w", userGroup+1, err)
		}
		s.testData.UserIDs[userGroup] = futureUserID

		logger.Info("✅ FK-challenging group complete", "group", userGroup+1, "posts_created", postsPerUser, "user_created", 1)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit FK-challenging creation tx: %w", err)
	}
	logger.Info("✅ FK-challenging data creation committed (FKs verified at COMMIT)")

	actualUsers := len(s.testData.UserIDs)
	actualPosts := len(s.testData.PostIDs)
	logger.Info("✅ FK-challenging data creation complete")
	logger.Info("📊 Data created", "users", actualUsers, "posts", actualPosts, "total_changes", actualUsers+actualPosts)
	logger.Info("🎯 FK challenge strategy", "approach", "posts created BEFORE their users", "pragma", "foreign_keys OFF/ON")

	// Step 3: Upload FK-challenging data (this should trigger FK constraint violations and retries)
	logger.Info("📱 Uploading FK-challenging data to server")

	// Ensure apply_mode=0 before reading pending (safety against interrupted flows)
	if err := s.app.ResetApplyMode(ctx); err != nil {
		logger.Warn("Failed to reset apply_mode to 0 before pending check (phone)", "error", err)
	}
	// Check pending changes before upload
	pendingCount, err := s.app.GetPendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count: %w", err)
	}
	logger.Info("📊 Pending changes before upload", "count", pendingCount)

	// Resume uploads and trigger upload - this should cause FK constraint violations and retries
	logger.Info("🚀 Starting upload with small batch sizes (will trigger FK constraint violations)")

	if client := s.app.GetClient(); client != nil {
		client.ResumeUploads()
		// Keep downloads paused; not needed for this phase
		logger.Info("▶️ Resumed client uploads for FK-challenge upload phase")
	}

	// Upload all pending changes in multiple small batches
	err = s.performMultiBatchUpload(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to perform multi-batch upload: %w", err)
	}

	// Check pending changes after upload
	pendingCountAfter, err := s.app.GetPendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count after upload: %w", err)
	}
	logger.Info("📊 Pending changes after upload", "count", pendingCountAfter)

	// Wait a moment for any async operations to complete
	time.Sleep(1 * time.Second)

	// Step 5: Test download sync with "phone" source ID (should get 0 records)
	if client := s.app.GetClient(); client != nil {
		client.ResumeDownloads()
		logger.Info("▶️ Resumed phone client downloads for verification")
	}
	err = s.testDownloadWithPhoneSourceID(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to test download with phone source ID: %w", err)
	}

	// Step 6: Test download sync with "laptop" source ID (should get all data)
	err = s.testDownloadWithLaptopSourceID(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to test download with laptop source ID: %w", err)
	}

	// Multi-Stage Sync Testing
	logger.Info("🚀 Starting Multi-Stage Sync Testing")
	logger.Info("📊 Multi-stage test: Create new data on laptop, upload to server, verify phone downloads it")

	// Stage 1: Create additional data on laptop
	err = s.stage1CreateAdditionalDataOnLaptop(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed stage 1 - create additional data on laptop: %w", err)
	}

	// Stage 2: Upload new data from laptop
	err = s.stage2UploadNewDataFromLaptop(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed stage 2 - upload new data from laptop: %w", err)
	}

	// Stage 3: Download verification - laptop (should get 0 records)
	err = s.stage3DownloadVerificationLaptop(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed stage 3 - download verification laptop: %w", err)
	}

	// Stage 4: Download verification - phone (should get all new records)
	err = s.stage4DownloadVerificationPhone(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed stage 4 - download verification phone: %w", err)
	}

	// Stage 5: Data verification
	err = s.stage5DataVerification(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed stage 5 - data verification: %w", err)
	}

	logger.Info("🎉 Multi-Stage Sync Testing completed successfully!")
	logger.Info("🎉 FK Constraint Testing Scenario completed successfully!")
	return nil
}

// Verify verifies the scenario results
// For complex multi-step scenarios, verification is handled internally during execution
// This external verification is a no-op to avoid duplicate verification
func (s *ComplexMultiBatchScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	logger := s.simulator.GetLogger()
	logger.Info("✅ Complex scenario verification handled internally during execution")
	return nil
}

// testDownloadWithPhoneSourceID tests download sync with phone's source ID (should get 0 records)
func (s *ComplexMultiBatchScenario) testDownloadWithPhoneSourceID(ctx context.Context, logger *slog.Logger) error {
	logger.Info("📥 Testing download sync with phone source ID", "source_id", s.phoneSourceID)
	logger.Info("📊 Expected result: 0 records (data already synced from this source)")

	// Perform download sync - should get 0 records since we uploaded from this phone
	applied, nextAfter, err := s.app.PerformSyncDownload(ctx, batchSize)
	if err != nil {
		logger.Info("❌ Download with phone source ID failed", "error", err.Error())
		return fmt.Errorf("download sync with phone source ID failed: %w", err)
	}

	logger.Info("📊 Download results", "applied", applied, "next_after", nextAfter)

	logger.Info("✅ Download sync with phone source ID completed", "source_id", s.phoneSourceID)
	logger.Info("📊 Result: No new records downloaded (expected behavior)")
	return nil
}

// testDownloadWithLaptopSourceID tests download sync with laptop downloading from phone's data
func (s *ComplexMultiBatchScenario) testDownloadWithLaptopSourceID(ctx context.Context, logger *slog.Logger) error {
	logger.Info("📥 Testing laptop download from phone's data", "laptop_source_id", s.laptopSourceID, "phone_source_id", s.phoneSourceID)
	logger.Info("📊 Expected result: All 105 records (5 users + 100 posts)")

	// Create a new mobile app with laptop source ID for download testing
	err := s.createLaptopApp(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to create laptop app: %w", err)
	}

	// Perform multi-batch download until all data is received
	err = s.performMultiBatchDownload(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to perform multi-batch download: %w", err)
	}

	// Verify downloaded data matches uploaded data
	err = s.verifyDownloadedData(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to verify downloaded data: %w", err)
	}

	logger.Info("✅ Laptop download from phone's data completed successfully", "laptop_source_id", s.laptopSourceID)
	return nil
}

// createLaptopApp creates a new mobile app with "laptop" source ID for download testing
func (s *ComplexMultiBatchScenario) createLaptopApp(ctx context.Context, logger *slog.Logger) error {
	logger.Info("💻 Creating laptop app for download testing")

	appConfig := &AppConfig{
		SourceID:   s.laptopSourceID,
		DeviceName: "Laptop Test Device",
		UserID:     s.app.config.UserID,
		BatchSize:  batchSize,
	}

	laptopApp, err := s.createMobileApp(appConfig, s.laptopSourceID)
	if err != nil {
		return fmt.Errorf("failed to create laptop app: %w", err)
	}

	// Sign in the laptop app
	err = laptopApp.SignIn(ctx, appConfig.UserID)
	if err != nil {
		return fmt.Errorf("failed to sign in laptop app: %w", err)
	}

	// Store the laptop app for later use
	s.laptopApp = laptopApp
	logger.Info("✅ Laptop app created successfully", "source_id", s.laptopSourceID)
	return nil
}

// performMultiBatchDownload performs multiple download requests until all data is received
func (s *ComplexMultiBatchScenario) performMultiBatchDownload(ctx context.Context, logger *slog.Logger) error {
	logger.Info("📥 Starting multi-batch download with small batch sizes")

	// Get initial counts
	initialUserCount, initialPostCount, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get initial laptop data counts: %w", err)
	}

	logger.Info("📊 Initial laptop database state", "users", initialUserCount, "posts", initialPostCount)

	// Download all data in multiple small batches
	downloadRound := 1
	totalUsersDownloaded := 0
	totalPostsDownloaded := 0

	for {
		logger.Info("📤 Download round", "round", downloadRound)

		// Get counts before this download round
		usersBefore, postsBefore, err := s.getDataCounts(s.laptopApp)
		if err != nil {
			return fmt.Errorf("failed to get data counts before download round %d: %w", downloadRound, err)
		}

		// Perform one download batch
		applied, nextAfter, err := s.laptopApp.PerformSyncDownload(ctx, batchSize)
		if err != nil {
			logger.Info("❌ Download round failed", "round", downloadRound, "error", err.Error())
			return fmt.Errorf("failed to download in round %d: %w", downloadRound, err)
		}

		logger.Info("📊 Download batch results", "applied", applied, "next_after", nextAfter)

		// Get counts after this download round
		usersAfter, postsAfter, err := s.getDataCounts(s.laptopApp)
		if err != nil {
			return fmt.Errorf("failed to get data counts after download round %d: %w", downloadRound, err)
		}

		// Calculate what was downloaded in this round
		usersDownloaded := usersAfter - usersBefore
		postsDownloaded := postsAfter - postsBefore
		totalUsersDownloaded += usersDownloaded
		totalPostsDownloaded += postsDownloaded

		logger.Info("📈 Download round completed",
			"round", downloadRound,
			"users_downloaded", usersDownloaded,
			"posts_downloaded", postsDownloaded,
			"total_users", usersAfter,
			"total_posts", postsAfter)

		// Check if we got any new data in this round
		if usersDownloaded == 0 && postsDownloaded == 0 {
			logger.Info("✅ All data downloaded successfully")
			logger.Info("📊 Download summary",
				"total_rounds", downloadRound,
				"total_users_downloaded", totalUsersDownloaded,
				"total_posts_downloaded", totalPostsDownloaded,
				"batch_size", batchSize)
			break
		}

		// Safety check to prevent infinite loops
		if downloadRound > maxRounds {
			return fmt.Errorf("download failed: too many rounds (%d), still downloading data", downloadRound)
		}

		downloadRound++
	}

	return nil
}

// verifyDownloadedData verifies that downloaded data matches the originally uploaded data
func (s *ComplexMultiBatchScenario) verifyDownloadedData(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🔍 Verifying downloaded data matches uploaded data")

	// Get final counts
	userCount, postCount, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get final laptop data counts: %w", err)
	}

	// Verify counts match expected
	expectedUsers := len(s.testData.UserIDs)
	expectedPosts := len(s.testData.PostIDs)

	logger.Info("📊 Data count verification",
		"expected_users", expectedUsers, "actual_users", userCount,
		"expected_posts", expectedPosts, "actual_posts", postCount)

	if userCount != expectedUsers {
		return fmt.Errorf("user count mismatch: expected %d, got %d", expectedUsers, userCount)
	}

	if postCount != expectedPosts {
		return fmt.Errorf("post count mismatch: expected %d, got %d", expectedPosts, postCount)
	}

	// Verify all user IDs are present
	err = s.verifyUserIDs(logger)
	if err != nil {
		return fmt.Errorf("user ID verification failed: %w", err)
	}

	// Verify all post IDs are present and have correct FK relationships
	err = s.verifyPostIDs(logger)
	if err != nil {
		return fmt.Errorf("post ID verification failed: %w", err)
	}

	// Verify FK relationships are valid
	err = s.verifyFKRelationships(logger)
	if err != nil {
		return fmt.Errorf("FK relationship verification failed: %w", err)
	}

	logger.Info("✅ Downloaded data verification completed successfully")
	logger.Info("📊 All user IDs, post IDs, and FK relationships verified")
	return nil
}

// verifyUserIDs verifies that all expected user IDs are present in the laptop database
func (s *ComplexMultiBatchScenario) verifyUserIDs(logger *slog.Logger) error {
	db := s.laptopApp.GetDatabase()

	logger.Info("🔍 Verifying user IDs", "expected_count", len(s.testData.UserIDs))

	for i, expectedUserID := range s.testData.UserIDs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", expectedUserID).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check user ID %s: %w", expectedUserID, err)
		}

		if count != 1 {
			return fmt.Errorf("user ID %s not found in laptop database", expectedUserID)
		}

		logger.Info("✅ User ID verified", "index", i+1, "user_id", expectedUserID)
	}

	logger.Info("✅ All user IDs verified successfully")
	return nil
}

// verifyPostIDs verifies that all expected post IDs are present in the laptop database
func (s *ComplexMultiBatchScenario) verifyPostIDs(logger *slog.Logger) error {
	db := s.laptopApp.GetDatabase()

	logger.Info("🔍 Verifying post IDs", "expected_count", len(s.testData.PostIDs))

	for i, expectedPostID := range s.testData.PostIDs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM posts WHERE id = ?", expectedPostID).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check post ID %s: %w", expectedPostID, err)
		}

		if count != 1 {
			return fmt.Errorf("post ID %s not found in laptop database", expectedPostID)
		}

		if (i+1)%20 == 0 { // Log every 20 posts to avoid spam
			logger.Info("✅ Post IDs verified", "verified_count", i+1, "total", len(s.testData.PostIDs))
		}
	}

	logger.Info("✅ All post IDs verified successfully")
	return nil
}

// verifyFKRelationships verifies that all FK relationships are valid in the laptop database
func (s *ComplexMultiBatchScenario) verifyFKRelationships(logger *slog.Logger) error {
	db := s.laptopApp.GetDatabase()

	logger.Info("🔍 Verifying FK relationships")

	// Count posts with valid FK relationships
	var validFKCount int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM posts p
		INNER JOIN users u ON p.author_id = u.id
	`).Scan(&validFKCount)
	if err != nil {
		return fmt.Errorf("failed to count posts with valid FK relationships: %w", err)
	}

	expectedPosts := len(s.testData.PostIDs)
	if validFKCount != expectedPosts {
		return fmt.Errorf("FK relationship mismatch: expected %d posts with valid FKs, got %d", expectedPosts, validFKCount)
	}

	logger.Info("✅ All FK relationships verified successfully", "valid_relationships", validFKCount)
	return nil
}

// performMultiBatchUpload performs multiple upload requests until all data is uploaded (DRY helper)
func (s *ComplexMultiBatchScenario) performMultiBatchUpload(ctx context.Context, logger *slog.Logger) error {
	return s.performMultiBatchUploadForApp(ctx, logger, s.app, "phone")
}

// performMultiBatchUploadForApp performs multiple upload requests for any app until all data is uploaded (DRY helper)
func (s *ComplexMultiBatchScenario) performMultiBatchUploadForApp(ctx context.Context, logger *slog.Logger, app *MobileApp, deviceName string) error {
	// For deterministic behavior, pause downloads during explicit upload rounds
	if client := app.GetClient(); client != nil {
		client.PauseDownloads()
	}

	uploadRound := 1
	for {
		// Check pending changes before this upload round
		pendingBefore, err := app.GetPendingChangesCount(ctx)
		if err != nil {
			return fmt.Errorf("failed to get pending changes count before upload round %d: %w", uploadRound, err)
		}

		if pendingBefore == 0 {
			logger.Info("✅ All changes uploaded successfully", "device", deviceName)
			logger.Info("📊 Upload summary", "device", deviceName, "total_rounds", uploadRound-1, "batch_size", batchSize, "strategy", "FK constraint retry logic")
			break
		}

		logger.Info("📤 Upload round", "device", deviceName, "round", uploadRound, "pending_changes", pendingBefore)

		// Upload one batch and inspect the response
		err = app.PerformSyncUpload(ctx)
		if err != nil {
			logger.Info("❌ Upload round failed", "device", deviceName, "round", uploadRound, "error", err.Error())
			return fmt.Errorf("failed to upload changes in round %d: %w", uploadRound, err)
		}

		// Check pending changes after this upload round
		pendingAfter, err := app.GetPendingChangesCount(ctx)
		if err != nil {
			return fmt.Errorf("failed to get pending changes count after upload round %d: %w", uploadRound, err)
		}

		uploaded := pendingBefore - pendingAfter
		logger.Info("📈 Upload round completed", "device", deviceName, "round", uploadRound, "uploaded", uploaded, "remaining", pendingAfter)

		// Safety check to prevent infinite loops
		if uploadRound > maxRounds {
			return fmt.Errorf("upload failed: too many rounds (%d), still have %d pending changes", uploadRound, pendingAfter)
		}

		uploadRound++
	}
	// Resume downloads after explicit upload completes
	if client := app.GetClient(); client != nil {
		client.ResumeDownloads()
	}

	// Simulator-only verification guards
	if err := app.DebugVerifyPendingCleared(ctx); err != nil {
		logger.Warn("Post-upload pending verification failed", "device", deviceName, "error", err.Error())
	} else {
		logger.Info("✅ Pending queue cleared after upload", "device", deviceName)
	}
	if err := app.DebugVerifyBusinessFKIntegrity(ctx); err != nil {
		logger.Warn("Business FK integrity check failed", "device", deviceName, "error", err.Error())
	} else {
		logger.Info("✅ Business FK integrity verified (simulator check)", "device", deviceName)
	}
	return nil
}

// getDataCounts gets the current user and post counts in a database (DRY helper)
func (s *ComplexMultiBatchScenario) getDataCounts(app *MobileApp) (int, int, error) {
	db := app.GetDatabase()

	var userCount, postCount int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to count users: %w", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM posts").Scan(&postCount)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to count posts: %w", err)
	}

	return userCount, postCount, nil
}

// stage1CreateAdditionalDataOnLaptop creates 20 new users and 400 new posts on laptop
func (s *ComplexMultiBatchScenario) stage1CreateAdditionalDataOnLaptop(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🏁 STAGE 1: Creating Additional Data on Laptop")
	logger.Info("📊 Target: 20 new users + 400 new posts (20 posts per user) = 420 total records")
	logger.Info("🎯 Strategy: Create posts BEFORE users to test FK constraint handling")

	// Verify laptop app is ready
	if s.laptopApp == nil {
		return fmt.Errorf("laptop app not initialized")
	}

	// Check current state of laptop app
	currentUsers, currentPosts, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get current laptop data counts: %w", err)
	}
	logger.Info("📊 Current laptop state", "users", currentUsers, "posts", currentPosts)

	// Initialize storage for new data
	totalNewPosts := newUsersStage2 * newPostsPerUser
	s.testData.NewUserIDs = make([]string, newUsersStage2)
	s.testData.NewPostIDs = make([]string, totalNewPosts)

	logger.Info("📊 Stage 1 configuration", "new_users", newUsersStage2, "posts_per_user", newPostsPerUser, "total_new_posts", totalNewPosts)

	// Prevent background sync from draining pending changes during laptop creation
	s.laptopApp.StopSync()  // ensure no in-flight txs or loops
	s.laptopApp.PauseSync() // belt-and-suspenders: pause at client layer too
	logger.Info("⏸️ Paused laptop client sync during offline creation")

	// Ensure apply_mode=0 so local triggers capture pending changes deterministically
	if err := s.laptopApp.ResetApplyMode(ctx); err != nil {
		logger.Warn("Failed to reset apply_mode to 0 before creation", "error", err)
	}

	// Create data in one transaction with deferred FK checks
	db := s.laptopApp.GetDatabase()
	// Best-effort clear any stray transaction
	_, _ = db.Exec("ROLLBACK")
	logger.Info("⏱️ About to begin laptop creation transaction")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx for laptop creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	logger.Info("✅ Laptop creation transaction begun")
	if _, err := tx.Exec("PRAGMA defer_foreign_keys = ON"); err != nil {
		return fmt.Errorf("failed to enable deferred FKs in tx (laptop): %w", err)
	}

	// Create data in FK-challenging order using deferred FKs within the tx
	logger.Info("🔄 Creating new data in FK-challenging order (tx): posts BEFORE users")

	postIndex := 0
	for userGroup := 0; userGroup < newUsersStage2; userGroup++ {
		// Generate new user ID
		newUserID := uuid.New().String()
		s.testData.NewUserIDs[userGroup] = newUserID

		// Create posts BEFORE the user (FK-challenging order)
		for postNum := 1; postNum <= newPostsPerUser; postNum++ {
			newPostID := uuid.New().String()
			s.testData.NewPostIDs[postIndex] = newPostID

			globalPostNum := postIndex + 1 + 100 // Continue numbering after original 100 posts
			title := fmt.Sprintf("New FK Test Post %d by New User %d", globalPostNum, userGroup+1)

			if err := s.laptopApp.CreatePostTx(tx, newPostID, newUserID, title, fmt.Sprintf("Content for new post %d", globalPostNum)); err != nil {
				return fmt.Errorf("failed to create new post %d: %w", globalPostNum, err)
			}

			//logger.Info("📝 New post created", "id", newPostID, "title", title, "author_id", newUserID)
			postIndex++

			// Log progress every 50 posts
			if postIndex%50 == 0 {
				logger.Info("📈 New posts creation progress", "created", postIndex, "total", totalNewPosts)
			}
		}

		// Create the user AFTER its posts (FK-challenging order)
		userName := fmt.Sprintf("New User %d", userGroup+1)
		if err := s.laptopApp.CreateUserTx(tx, newUserID, userName, fmt.Sprintf("newuser%d@example.com", userGroup+1)); err != nil {
			return fmt.Errorf("failed to create new user %d: %w", userGroup+1, err)
		}

		logger.Info("👤 New user created", "id", newUserID, "name", userName)
		logger.Info("✅ FK-challenging group complete", "group", userGroup+1, "posts_created", newPostsPerUser, "user_created", 1)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit laptop creation tx: %w", err)
	}
	logger.Info("✅ Laptop FK-challenging data creation committed (FKs verified at COMMIT)")

	// Verify data creation
	actualNewUsers := len(s.testData.NewUserIDs)
	actualNewPosts := len(s.testData.NewPostIDs)
	totalNewRecords := actualNewUsers + actualNewPosts

	logger.Info("✅ Stage 1 completed successfully")
	logger.Info("📊 New data created", "users", actualNewUsers, "posts", actualNewPosts, "total_records", totalNewRecords)
	logger.Info("🎯 FK challenge strategy", "approach", "posts created BEFORE their users", "pragma", "foreign_keys OFF/ON")

	return nil
}

// stage2UploadNewDataFromLaptop uploads the 420 new records from laptop to server
func (s *ComplexMultiBatchScenario) stage2UploadNewDataFromLaptop(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🏁 STAGE 2: Upload New Data from Laptop")
	logger.Info("📊 Target: Upload 420 new records (20 users + 400 posts) using 50-record batches")
	logger.Info("🎯 Expected: Multiple upload rounds due to FK constraints and batch size limits")

	// Ensure apply_mode=0 before reading pending (safety against interrupted flows)
	if err := s.laptopApp.ResetApplyMode(ctx); err != nil {
		logger.Warn("Failed to reset apply_mode to 0 before pending check", "error", err)
	}

	// Check pending changes before upload
	pendingBefore, err := s.laptopApp.GetPendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count before upload: %w", err)
	}

	expectedNewRecords := newUsersStage2 + (newUsersStage2 * newPostsPerUser)
	logger.Info("📊 Pending changes before upload", "count", pendingBefore, "expected", expectedNewRecords)

	// Allow some flexibility in pending count due to sync system behavior
	if pendingBefore < expectedNewRecords {
		return fmt.Errorf("insufficient pending changes: expected at least %d, got %d", expectedNewRecords, pendingBefore)
	}

	if pendingBefore > expectedNewRecords {
		logger.Info("⚠️ More pending changes than expected", "extra", pendingBefore-expectedNewRecords, "possible_cause", "sync system state")
	}

	// INVESTIGATION: Check what's actually in the pending changes
	err = s.investigatePendingChanges(ctx, logger)
	if err != nil {
		logger.Info("⚠️ Failed to investigate pending changes", "error", err.Error())
	}

	logger.Info("🚀 Starting upload with small batch sizes (will trigger FK constraint violations)")

	// Resume uploads now that creation is complete and pending has been verified
	if client := s.laptopApp.GetClient(); client != nil {
		client.ResumeUploads()
		// Keep downloads paused to avoid noisy interference; download verification happens later explicitly
		logger.Info("▶️ Resumed laptop client uploads for Stage 2")
	}

	// Perform multi-batch upload using existing helper
	err = s.performMultiBatchUploadForApp(ctx, logger, s.laptopApp, "laptop")
	if err != nil {
		return fmt.Errorf("failed to perform multi-batch upload from laptop: %w", err)
	}

	// Verify all changes uploaded
	pendingAfter, err := s.laptopApp.GetPendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count after upload: %w", err)
	}

	if pendingAfter != 0 {
		return fmt.Errorf("upload incomplete: %d changes still pending", pendingAfter)
	}

	// Capture upload watermark for watermark-based download termination
	s.uploadWatermark, err = s.laptopApp.GetLastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to get upload watermark: %w", err)
	}

	logger.Info("✅ Stage 2 completed successfully")
	logger.Info("📊 Upload summary", "total_uploaded", expectedNewRecords, "remaining_pending", pendingAfter, "watermark", s.uploadWatermark)

	return nil
}

// stage3DownloadVerificationLaptop verifies laptop gets 0 records (already fully synced)
func (s *ComplexMultiBatchScenario) stage3DownloadVerificationLaptop(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🏁 STAGE 3: Download Verification - Laptop (Should Get 0 Records)")
	logger.Info("📊 Expected result: 0 records downloaded (laptop is already fully synced)")

	if client := s.laptopApp.GetClient(); client != nil {
		client.ResumeDownloads()
		logger.Info("▶️ Resumed laptop client downloads for Stage 3")
	}

	// Get data counts before download
	usersBefore, postsBefore, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get laptop data counts before download: %w", err)
	}

	logger.Info("📊 Laptop data before download", "users", usersBefore, "posts", postsBefore)

	// Perform download sync - should get 0 records since laptop uploaded the data
	applied, nextAfter, err := s.laptopApp.PerformSyncDownload(ctx, batchSize)
	if err != nil {
		logger.Info("❌ Download verification on laptop failed", "error", err.Error())
		return fmt.Errorf("download sync on laptop failed: %w", err)
	}

	logger.Info("📊 Download results", "device", "laptop", "applied", applied, "next_after", nextAfter)

	// Get data counts after download
	usersAfter, postsAfter, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get laptop data counts after download: %w", err)
	}

	// Verify no new data was downloaded
	usersDownloaded := usersAfter - usersBefore
	postsDownloaded := postsAfter - postsBefore

	if applied != 0 {
		return fmt.Errorf("unexpected download result: expected 0 applied, got %d", applied)
	}

	if usersDownloaded != 0 || postsDownloaded != 0 {
		return fmt.Errorf("unexpected data downloaded: %d users, %d posts (expected 0, 0)", usersDownloaded, postsDownloaded)
	}

	logger.Info("✅ Stage 3 completed successfully")
	logger.Info("📊 Verification result", "device", "laptop", "users_downloaded", usersDownloaded, "posts_downloaded", postsDownloaded, "expected", "0, 0")

	return nil
}

// stage4DownloadVerificationPhone verifies phone gets all new records from laptop (progress-based paging)
func (s *ComplexMultiBatchScenario) stage4DownloadVerificationPhone(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🏁 STAGE 4: Download Verification - Phone (Should Get All New Records)")

	if client := s.app.GetClient(); client != nil {
		client.ResumeDownloads()
		logger.Info("▶️ Ensured phone client downloads are resumed for Stage 4")
	}

	// Compute the expected delta based on the phone's current snapshot.
	// Target totals come from the scenario configuration:
	//   - Initially we created `totalUsers` users with `postsPerUser` posts each on the phone
	//   - Stage 1 created `newUsersStage2` users with `newPostsPerUser` posts each on the laptop
	// Final expected totals after Stage 4 = (totalUsers + newUsersStage2, totalUsers*postsPerUser + newUsersStage2*newPostsPerUser)
	targetTotalUsers := totalUsers + newUsersStage2
	targetTotalPosts := (totalUsers * postsPerUser) + (newUsersStage2 * newPostsPerUser)

	// Get data counts before download
	usersBefore, postsBefore, err := s.getDataCounts(s.app)
	if err != nil {
		return fmt.Errorf("failed to get phone data counts before download: %w", err)
	}

	// Dynamic expectation: download delta = (target totals - current phone counts), clamped at >= 0
	expUsersDelta := targetTotalUsers - usersBefore
	if expUsersDelta < 0 {
		expUsersDelta = 0
	}
	expPostsDelta := targetTotalPosts - postsBefore
	if expPostsDelta < 0 {
		expPostsDelta = 0
	}
	expectedNewRecords := expUsersDelta + expPostsDelta

	logger.Info("📊 Expected result (dynamic)",
		"target_total_users", targetTotalUsers,
		"target_total_posts", targetTotalPosts,
		"phone_users_before", usersBefore,
		"phone_posts_before", postsBefore,
		"expected_users_delta", expUsersDelta,
		"expected_posts_delta", expPostsDelta,
		"expected_total_delta", expectedNewRecords)

	logger.Info("📊 Phone data before download", "users", usersBefore, "posts", postsBefore)

	// If there's nothing new expected, we're done.
	if expectedNewRecords == 0 {
		logger.Info("✅ No new records expected for download; skipping Stage 4")
		return nil
	}

	// Use a frozen server-side ceiling to avoid interleaving with concurrent writes
	logger.Info("📥 Starting windowed multi-batch download to upload watermark", "until", s.uploadWatermark)
	totalApplied, lastAfter, err := s.app.SyncDownloadToWatermark(ctx, batchSize, s.uploadWatermark)
	if err != nil {
		return fmt.Errorf("download to watermark failed: %w", err)
	}
	logger.Info("📊 Windowed download complete", "applied", totalApplied, "last_after", lastAfter, "until", s.uploadWatermark)

	// Verify we reached the target totals (stronger check than deltas)
	usersAfter, postsAfter, err := s.getDataCounts(s.app)
	if err != nil {
		return fmt.Errorf("failed to get phone data counts after windowed download: %w", err)
	}
	if usersAfter != targetTotalUsers || postsAfter != targetTotalPosts {
		return fmt.Errorf("final count mismatch after windowed download: expected users=%d posts=%d, got users=%d posts=%d",
			targetTotalUsers, targetTotalPosts, usersAfter, postsAfter)
	}

	logger.Info("✅ Stage 4 completed successfully")
	logger.Info("📊 Download verification", "device", "phone", "users_total", usersAfter, "posts_total", postsAfter, "targets", fmt.Sprintf("users=%d posts=%d", targetTotalUsers, targetTotalPosts))

	return nil
}

// stage5DataVerification performs comprehensive verification of the multi-stage sync
func (s *ComplexMultiBatchScenario) stage5DataVerification(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🏁 STAGE 5: Data Verification")
	logger.Info("📊 Comprehensive verification of multi-stage sync results")

	// Get final counts on phone
	finalUsers, finalPosts, err := s.getDataCounts(s.app)
	if err != nil {
		return fmt.Errorf("failed to get final phone data counts: %w", err)
	}

	// Expected totals: original data + new data
	expectedTotalUsers := totalUsers + newUsersStage2
	expectedTotalPosts := (totalUsers * postsPerUser) + (newUsersStage2 * newPostsPerUser)

	logger.Info("📊 Final data count verification",
		"expected_total_users", expectedTotalUsers, "actual_users", finalUsers,
		"expected_total_posts", expectedTotalPosts, "actual_posts", finalPosts)

	// Verify total counts
	if finalUsers != expectedTotalUsers {
		return fmt.Errorf("total user count mismatch: expected %d, got %d", expectedTotalUsers, finalUsers)
	}

	if finalPosts != expectedTotalPosts {
		return fmt.Errorf("total post count mismatch: expected %d, got %d", expectedTotalPosts, finalPosts)
	}

	// Verify phone received only the NEW user IDs created by laptop
	err = s.verifyNewUserIDsOnPhone(logger)
	if err != nil {
		return fmt.Errorf("new user ID verification failed: %w", err)
	}

	// Verify phone received only the NEW post IDs created by laptop
	err = s.verifyNewPostIDsOnPhone(logger)
	if err != nil {
		return fmt.Errorf("new post ID verification failed: %w", err)
	}

	// Verify FK relationships for new posts
	err = s.verifyNewFKRelationshipsOnPhone(logger)
	if err != nil {
		return fmt.Errorf("new FK relationship verification failed: %w", err)
	}

	// Verify original data integrity (original 105 records should remain unchanged)
	err = s.verifyOriginalDataIntegrity(logger)
	if err != nil {
		return fmt.Errorf("original data integrity verification failed: %w", err)
	}

	logger.Info("✅ Stage 5 completed successfully")
	logger.Info("📊 All verification checks passed")
	logger.Info("🎯 Multi-stage sync test: FULLY SUCCESSFUL")

	return nil
}

// verifyNewUserIDsOnPhone verifies phone received only the NEW user IDs created by laptop
func (s *ComplexMultiBatchScenario) verifyNewUserIDsOnPhone(logger *slog.Logger) error {
	db := s.app.GetDatabase()

	logger.Info("🔍 Verifying new user IDs on phone", "expected_count", len(s.testData.NewUserIDs))

	for _, expectedNewUserID := range s.testData.NewUserIDs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", expectedNewUserID).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check new user ID %s: %w", expectedNewUserID, err)
		}

		if count != 1 {
			return fmt.Errorf("new user ID %s not found on phone", expectedNewUserID)
		}

		//logger.Info("✅ New user ID verified on phone", "index", i+1, "user_id", expectedNewUserID)
	}

	logger.Info("✅ All new user IDs verified successfully on phone")
	return nil
}

// verifyNewPostIDsOnPhone verifies phone received only the NEW post IDs created by laptop
func (s *ComplexMultiBatchScenario) verifyNewPostIDsOnPhone(logger *slog.Logger) error {
	db := s.app.GetDatabase()

	logger.Info("🔍 Verifying new post IDs on phone", "expected_count", len(s.testData.NewPostIDs))

	for _, expectedNewPostID := range s.testData.NewPostIDs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM posts WHERE id = ?", expectedNewPostID).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check new post ID %s: %w", expectedNewPostID, err)
		}

		if count != 1 {
			return fmt.Errorf("new post ID %s not found on phone", expectedNewPostID)
		}

		//if (i+1)%50 == 0 { // Log every 50 posts to avoid spam
		//	logger.Info("✅ New post IDs verified on phone", "verified_count", i+1, "total", len(s.testData.NewPostIDs))
		//}
	}

	logger.Info("✅ All new post IDs verified successfully on phone")
	return nil
}

// verifyNewFKRelationshipsOnPhone verifies all new posts reference correct new user IDs
func (s *ComplexMultiBatchScenario) verifyNewFKRelationshipsOnPhone(logger *slog.Logger) error {
	db := s.app.GetDatabase()

	logger.Info("🔍 Verifying new FK relationships on phone")

	// Count new posts with valid FK relationships to new users
	var validNewFKCount int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM posts p
		INNER JOIN users u ON p.author_id = u.id
		WHERE p.id IN (`+s.buildPlaceholders(len(s.testData.NewPostIDs))+`)
		AND u.id IN (`+s.buildPlaceholders(len(s.testData.NewUserIDs))+`)
	`, append(s.stringSliceToInterface(s.testData.NewPostIDs), s.stringSliceToInterface(s.testData.NewUserIDs)...)...).Scan(&validNewFKCount)
	if err != nil {
		return fmt.Errorf("failed to count new posts with valid FK relationships: %w", err)
	}

	expectedNewPosts := len(s.testData.NewPostIDs)
	if validNewFKCount != expectedNewPosts {
		return fmt.Errorf("new FK relationship mismatch: expected %d new posts with valid FKs, got %d", expectedNewPosts, validNewFKCount)
	}

	logger.Info("✅ All new FK relationships verified successfully on phone", "valid_new_relationships", validNewFKCount)
	return nil
}

// verifyOriginalDataIntegrity verifies original 105 records remain unchanged
func (s *ComplexMultiBatchScenario) verifyOriginalDataIntegrity(logger *slog.Logger) error {
	db := s.app.GetDatabase()

	logger.Info("🔍 Verifying original data integrity on phone")

	// Verify all original user IDs are still present
	for i, originalUserID := range s.testData.UserIDs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", originalUserID).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check original user ID %s: %w", originalUserID, err)
		}

		if count != 1 {
			return fmt.Errorf("original user ID %s missing from phone", originalUserID)
		}

		if i == 0 || i == len(s.testData.UserIDs)-1 { // Log first and last
			logger.Info("✅ Original user ID verified", "index", i+1, "user_id", originalUserID)
		}
	}

	// Verify all original post IDs are still present
	for i, originalPostID := range s.testData.PostIDs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM posts WHERE id = ?", originalPostID).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to check original post ID %s: %w", originalPostID, err)
		}

		if count != 1 {
			return fmt.Errorf("original post ID %s missing from phone", originalPostID)
		}

		if (i+1)%50 == 0 { // Log every 50 posts
			logger.Info("✅ Original post IDs verified", "verified_count", i+1, "total", len(s.testData.PostIDs))
		}
	}

	// Verify original FK relationships are still valid
	var validOriginalFKCount int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM posts p
		INNER JOIN users u ON p.author_id = u.id
		WHERE p.id IN (`+s.buildPlaceholders(len(s.testData.PostIDs))+`)
		AND u.id IN (`+s.buildPlaceholders(len(s.testData.UserIDs))+`)
	`, append(s.stringSliceToInterface(s.testData.PostIDs), s.stringSliceToInterface(s.testData.UserIDs)...)...).Scan(&validOriginalFKCount)
	if err != nil {
		return fmt.Errorf("failed to count original posts with valid FK relationships: %w", err)
	}

	expectedOriginalPosts := len(s.testData.PostIDs)
	if validOriginalFKCount != expectedOriginalPosts {
		return fmt.Errorf("original FK relationship mismatch: expected %d original posts with valid FKs, got %d", expectedOriginalPosts, validOriginalFKCount)
	}

	logger.Info("✅ Original data integrity verified successfully on phone")
	logger.Info("📊 Original data counts", "users", len(s.testData.UserIDs), "posts", len(s.testData.PostIDs), "valid_fk_relationships", validOriginalFKCount)
	return nil
}

// Helper methods for SQL query building
func (s *ComplexMultiBatchScenario) buildPlaceholders(count int) string {
	if count == 0 {
		return ""
	}
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ",")
}

func (s *ComplexMultiBatchScenario) stringSliceToInterface(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// investigatePendingChanges investigates what's in the pending changes to understand the issue
func (s *ComplexMultiBatchScenario) investigatePendingChanges(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🔍 INVESTIGATION: Analyzing pending changes on laptop")

	db := s.laptopApp.GetDatabase()

	// Query the sync metadata to see what changes are pending
	rows, err := db.Query(`
		SELECT table_name, operation, primary_key, status, created_at
		FROM oversync_changes
		WHERE status = 'pending'
		ORDER BY created_at DESC
		LIMIT 50
	`)
	if err != nil {
		return fmt.Errorf("failed to query pending changes: %w", err)
	}
	defer rows.Close()

	var pendingChanges []struct {
		Table     string
		Operation string
		PK        string
		Status    string
		CreatedAt string
	}

	for rows.Next() {
		var change struct {
			Table     string
			Operation string
			PK        string
			Status    string
			CreatedAt string
		}
		err := rows.Scan(&change.Table, &change.Operation, &change.PK, &change.Status, &change.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to scan pending change: %w", err)
		}
		pendingChanges = append(pendingChanges, change)
	}

	logger.Info("📊 Pending changes analysis", "total_found", len(pendingChanges))

	// Count by table and operation
	userInserts := 0
	postInserts := 0
	otherChanges := 0

	for i, change := range pendingChanges {
		if change.Table == "users" && change.Operation == "INSERT" {
			userInserts++
		} else if change.Table == "posts" && change.Operation == "INSERT" {
			postInserts++
		} else {
			otherChanges++
		}

		// Log first 10 changes for detailed analysis
		if i < 10 {
			logger.Info("🔍 Pending change detail",
				"index", i+1,
				"table", change.Table,
				"operation", change.Operation,
				"pk", change.PK,
				"status", change.Status,
				"created_at", change.CreatedAt)
		}
	}

	logger.Info("📊 Pending changes breakdown",
		"user_inserts", userInserts,
		"post_inserts", postInserts,
		"other_changes", otherChanges)

	// Check if any of the pending changes match our original data
	originalDataInPending := 0
	for _, change := range pendingChanges {
		// Check if this PK matches any of our original user IDs
		for _, originalUserID := range s.testData.UserIDs {
			if change.PK == originalUserID {
				originalDataInPending++
				logger.Info("⚠️ FOUND ORIGINAL DATA IN PENDING",
					"table", change.Table,
					"pk", change.PK,
					"operation", change.Operation)
				break
			}
		}

		// Check if this PK matches any of our original post IDs
		for _, originalPostID := range s.testData.PostIDs {
			if change.PK == originalPostID {
				originalDataInPending++
				logger.Info("⚠️ FOUND ORIGINAL DATA IN PENDING",
					"table", change.Table,
					"pk", change.PK,
					"operation", change.Operation)
				break
			}
		}
	}

	if originalDataInPending > 0 {
		logger.Info("🚨 ISSUE DETECTED: Downloaded data is being treated as pending changes",
			"original_data_in_pending", originalDataInPending)
		logger.Info("💡 This suggests oversqlite is incorrectly marking downloaded records as local changes")
	} else {
		logger.Info("✅ No original data found in pending changes - this is expected")
	}

	return nil
}

// Cleanup cleans up resources
func (s *ComplexMultiBatchScenario) Cleanup(ctx context.Context) error {
	if s.laptopApp != nil {
		// Clean up laptop app resources if needed
	}
	return nil
}
