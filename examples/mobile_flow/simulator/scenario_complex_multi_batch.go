package simulator

import (
	"context"
	"database/sql"
	"encoding/hex"
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
	stableBundleSeq int64      // Durable server bundle checkpoint after laptop uploads
}

// ComplexTestData holds the test data structure for verification
type ComplexTestData struct {
	// Original data created by phone
	UserIDs   []string
	PostIDs   []string
	FileIDs   []string
	ReviewIDs []string

	// New data created by laptop for multi-stage testing
	NewUserIDs   []string
	NewPostIDs   []string
	NewFileIDs   []string
	NewReviewIDs []string

	ExpectedUsers   map[string]expectedUserState
	ExpectedPosts   map[string]expectedPostState
	ExpectedFiles   map[string]expectedFileState
	ExpectedReviews map[string]expectedReviewState
}

// AppConfig holds common configuration for creating mobile apps
type AppConfig struct {
	SourceID          string
	DeviceName        string
	UserID            string
	PullLimit         int
	PushLimit         int
	SnapshotChunkRows int
}

type deferredFKCreationSpec struct {
	label          string
	userCount      int
	postsPerUser   int
	postLogEvery   int
	postNumberBase int
	buildUser      func(userOrdinal int) (name, email string)
	buildPost      func(globalPostNum, userOrdinal int) (title, content string)
}

type expectedUserState struct {
	Name   string
	Email  string
	Exists bool
}

type expectedPostState struct {
	AuthorID string
	Title    string
	Content  string
	Exists   bool
}

type expectedFileState struct {
	Name    string
	DataHex string
	Exists  bool
}

type expectedReviewState struct {
	FileID string
	Review string
	Exists bool
}

// Constants for the scenario
const (
	totalUsers        = 5
	postsPerUser      = 20
	newUsersStage2    = 20 // Additional users created by laptop
	newPostsPerUser   = 20 // Posts per new user
	pullLimit         = 50
	pushLimit         = 40
	snapshotChunkRows = 25
	maxRounds         = 15 // Increased for multi-stage testing
)

// NewComplexMultiBatchScenario creates a new complex multi-batch scenario
func NewComplexMultiBatchScenario(simulator *Simulator) Scenario {
	return &ComplexMultiBatchScenario{
		BaseScenario: NewBaseScenario(simulator, "complex-multi-batch"),
		testData: &ComplexTestData{
			ExpectedUsers:   make(map[string]expectedUserState),
			ExpectedPosts:   make(map[string]expectedPostState),
			ExpectedFiles:   make(map[string]expectedFileState),
			ExpectedReviews: make(map[string]expectedReviewState),
		},
	}
}

func (s *ComplexMultiBatchScenario) Name() string {
	return "complex-multi-batch"
}

func (s *ComplexMultiBatchScenario) Description() string {
	return "FK-heavy multi-batch stress test with mixed inserts, updates, deletes, and two-device convergence"
}

// Setup creates a mobile app with a small pull limit for convergence testing.
func (s *ComplexMultiBatchScenario) Setup(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	if verboseLog {
		logger.Info("🔧 Setting up mobile app with incremental pull limits for high-volume sync testing")
	}

	// Generate source IDs once and store for reuse throughout scenario
	s.phoneSourceID = fmt.Sprintf("phone-%s", s.config.UserID)
	s.laptopSourceID = fmt.Sprintf("laptop-%s", s.config.UserID)
	if verboseLog {
		logger.Info("📱 Generated source IDs", "phone_source_id", s.phoneSourceID, "laptop_source_id", s.laptopSourceID)
	}

	// Create a mobile app tuned for a large local dirty set plus incremental pulls.
	app, err := s.createAppWithSmallBatches(s.config)
	if err != nil {
		return fmt.Errorf("failed to create mobile app for complex sync scenario: %w", err)
	}

	s.app = app
	s.simulator.currentApp = s.app // enable sqlite path reporting for parallel runs

	// Launch the app
	if err := s.app.onLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch app: %w", err)
	}

	if verboseLog {
		logger.Info("✅ Mobile app setup complete")
	}
	return nil
}

// createAppWithSmallBatches creates a mobile app with intentionally small push/pull chunk sizes.
func (s *ComplexMultiBatchScenario) createAppWithSmallBatches(scenarioConfig *config.ScenarioConfig) (*MobileApp, error) {
	appConfig := &AppConfig{
		SourceID:          s.phoneSourceID,
		DeviceName:        "FK Test Device",
		UserID:            scenarioConfig.UserID,
		PullLimit:         pullLimit,
		PushLimit:         pushLimit,
		SnapshotChunkRows: snapshotChunkRows,
	}
	return s.createMobileApp(appConfig, s.phoneSourceID)
}

// createMobileApp creates a mobile app with the given configuration (DRY helper)
func (s *ComplexMultiBatchScenario) createMobileApp(config *AppConfig, sourcePrefix string) (*MobileApp, error) {
	simCfg := s.simulator.GetConfig()

	// Create unique database file path using user ID and timestamp to prevent collisions in parallel execution
	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", sourcePrefix, config.UserID, time.Now().UnixNano()))

	// Force both push-session chunking and incremental pull replay so the scenario exercises
	// multi-chunk upload/download paths under a large FK-connected working set.
	oversqliteConfig := &oversqlite.Config{
		Schema:            "business",
		Tables:            managedSyncTables(),
		UploadLimit:       config.PushLimit,
		DownloadLimit:     config.PullLimit,
		SnapshotChunkRows: config.SnapshotChunkRows,
	}

	// Create mobile app config
	appConfig := &mobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        simCfg.ServerURL,
		UserID:           config.UserID,
		SourceID:         config.SourceID,
		DeviceName:       config.DeviceName,
		JWTSecret:        simCfg.JWTSecret,
		OversqliteConfig: oversqliteConfig,
		PreserveDB:       simCfg.PreserveDB,
		Logger:           s.simulator.GetLogger(),
	}

	// Create mobile app
	app, err := newMobileApp(appConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create mobile app: %w", err)
	}

	return app, nil
}

// Execute runs the FK constraint testing scenario
func (s *ComplexMultiBatchScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	if verboseLog {
		logger.Info("🎯 Starting FK Constraint Testing Scenario")
		logger.Info("📊 Strategy: Create posts BEFORE users to trigger server-side FK constraint violations")
	}

	// Step 1: Sign in user to initialize sync system
	if verboseLog {
		logger.Info("👤 Signing in user to initialize sync system")
	}
	err := s.app.signIn(ctx, s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to sign in user: %w", err)
	}

	// Step 2: Create FK-challenging data (we'll work offline by not calling sync)
	if verboseLog {
		logger.Info("📱 Creating FK-challenging data offline")
	}

	// Step 2: Create data in FK-challenging order using SQLite PRAGMA
	if verboseLog {
		logger.Info("👤📝 Creating FK-challenging data in SQLite")
	}

	if verboseLog {
		logger.Info("📊 Test configuration", "users", totalUsers, "posts_per_user", postsPerUser, "total_posts", totalUsers*postsPerUser)
	}

	s.testData.UserIDs, s.testData.PostIDs, err = s.createDeferredFKData(ctx, logger, s.app, deferredFKCreationSpec{
		label:          "phone",
		userCount:      totalUsers,
		postsPerUser:   postsPerUser,
		postNumberBase: 0,
		buildUser: func(userOrdinal int) (string, string) {
			return fmt.Sprintf("User %d", userOrdinal), fmt.Sprintf("user%d@example.com", userOrdinal)
		},
		buildPost: func(globalPostNum, userOrdinal int) (string, string) {
			return fmt.Sprintf("FK Test Post %d by User %d", globalPostNum, userOrdinal),
				fmt.Sprintf("Content for post %d by user %d", globalPostNum, userOrdinal)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create phone FK-challenging data: %w", err)
	}
	s.recordDeferredFKCreatedData(s.testData.UserIDs, s.testData.PostIDs, deferredFKCreationSpec{
		userCount:      totalUsers,
		postsPerUser:   postsPerUser,
		postNumberBase: 0,
		buildUser: func(userOrdinal int) (string, string) {
			return fmt.Sprintf("User %d", userOrdinal), fmt.Sprintf("user%d@example.com", userOrdinal)
		},
		buildPost: func(globalPostNum, userOrdinal int) (string, string) {
			return fmt.Sprintf("FK Test Post %d by User %d", globalPostNum, userOrdinal),
				fmt.Sprintf("Content for post %d by user %d", globalPostNum, userOrdinal)
		},
	})
	s.testData.FileIDs, s.testData.ReviewIDs, err = s.createBlobBatch(s.app, "phone-seed", totalUsers)
	if err != nil {
		return fmt.Errorf("failed to create phone blob batch: %w", err)
	}
	if err := s.rebalancePendingForDeferredFKGroups(ctx, logger, s.app, "phone-seed", s.testData.UserIDs, s.testData.PostIDs, postsPerUser); err != nil {
		return fmt.Errorf("failed to rebalance phone pending queue: %w", err)
	}

	actualUsers := len(s.testData.UserIDs)
	actualPosts := len(s.testData.PostIDs)
	if verboseLog {
		logger.Info("✅ FK-challenging data creation complete")
		logger.Info("📊 Data created", "users", actualUsers, "posts", actualPosts, "total_changes", actualUsers+actualPosts)
		logger.Info("🎯 FK challenge strategy", "approach", "posts created BEFORE their users", "pragma", "foreign_keys OFF/ON")
	}

	// Step 3: Upload FK-challenging data (this should trigger FK constraint violations and retries)
	if verboseLog {
		logger.Info("📱 Uploading FK-challenging data to server")
	}

	// Check pending changes before upload
	pendingCount, err := s.app.pendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count: %w", err)
	}
	if verboseLog {
		logger.Info("📊 Pending changes before upload", "count", pendingCount)
	}

	// Resume uploads and trigger upload - this should cause FK constraint violations and retries
	if verboseLog {
		logger.Info("🚀 Starting bundle push for the full local dirty set")
	}

	if client := s.app.currentClient(); client != nil {
		client.ResumeUploads()
		// Keep downloads paused; not needed for this phase
		if verboseLog {
			logger.Info("▶️ Resumed client uploads for FK-challenge upload phase")
		}
	}

	// Push all pending changes for this stage.
	err = s.performMultiBatchUpload(ctx, logger)
	if err != nil {
		return fmt.Errorf("failed to push pending changes: %w", err)
	}

	// Check pending changes after upload
	pendingCountAfter, err := s.app.pendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count after upload: %w", err)
	}
	if verboseLog {
		logger.Info("📊 Pending changes after upload", "count", pendingCountAfter)
	}

	// Step 5: Test download sync with "phone" source ID (should get 0 records)
	if client := s.app.currentClient(); client != nil {
		client.ResumeDownloads()
		if verboseLog {
			logger.Info("▶️ Resumed phone client downloads for verification")
		}
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
	if verboseLog {
		logger.Info("🚀 Starting Multi-Stage Sync Testing")
		logger.Info("📊 Multi-stage test: Create new data on laptop, upload to server, verify phone downloads it")
	}

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

	if verboseLog {
		logger.Info("🎉 Multi-Stage Sync Testing completed successfully!")
		logger.Info("🎉 FK Constraint Testing Scenario completed successfully!")
	}
	return nil
}

// Verify verifies the scenario results
// For complex multi-step scenarios, verification is handled internally during execution
// This external verification is a no-op to avoid duplicate verification
func (s *ComplexMultiBatchScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	logger := s.simulator.GetLogger()
	if verboseLog {
		logger.Info("✅ Complex scenario verification handled internally during execution")
	}
	return nil
}

// testDownloadWithPhoneSourceID tests download sync with phone's source ID (should get 0 records)
func (s *ComplexMultiBatchScenario) testDownloadWithPhoneSourceID(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("📥 Testing download sync with phone source ID", "source_id", s.phoneSourceID)
		logger.Info("📊 Expected result: 0 records (data already synced from this source)")
	}

	// Perform download sync - should get 0 records since we uploaded from this phone
	applied, nextAfter, err := s.app.pullToStable(ctx)
	if err != nil {
		logger.Warn("❌ Download with phone source ID failed", "error", err.Error())
		return fmt.Errorf("download sync with phone source ID failed: %w", err)
	}

	if verboseLog {
		logger.Info("📊 Download results", "applied", applied, "next_after", nextAfter)
		logger.Info("✅ Download sync with phone source ID completed", "source_id", s.phoneSourceID)
		logger.Info("📊 Result: No new records downloaded (expected behavior)")
	}
	return nil
}

// testDownloadWithLaptopSourceID tests download sync with laptop downloading from phone's data
func (s *ComplexMultiBatchScenario) testDownloadWithLaptopSourceID(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("📥 Testing laptop download from phone's data", "laptop_source_id", s.laptopSourceID, "phone_source_id", s.phoneSourceID)
		logger.Info("📊 Expected result: All 105 records (5 users + 100 posts)")
	}

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

	if verboseLog {
		logger.Info("✅ Laptop download from phone's data completed successfully", "laptop_source_id", s.laptopSourceID)
	}
	return nil
}

// createLaptopApp creates a new mobile app with "laptop" source ID for download testing
func (s *ComplexMultiBatchScenario) createLaptopApp(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("💻 Creating laptop app for download testing")
	}

	appConfig := &AppConfig{
		SourceID:          s.laptopSourceID,
		DeviceName:        "Laptop Test Device",
		UserID:            s.app.config.UserID,
		PullLimit:         pullLimit,
		PushLimit:         pushLimit,
		SnapshotChunkRows: snapshotChunkRows,
	}

	laptopApp, err := s.createMobileApp(appConfig, s.laptopSourceID)
	if err != nil {
		return fmt.Errorf("failed to create laptop app: %w", err)
	}

	// Sign in the laptop app
	err = laptopApp.signIn(ctx, appConfig.UserID)
	if err != nil {
		return fmt.Errorf("failed to sign in laptop app: %w", err)
	}

	// Store the laptop app for later use
	s.laptopApp = laptopApp
	if verboseLog {
		logger.Info("✅ Laptop app created successfully", "source_id", s.laptopSourceID)
	}
	return nil
}

// performMultiBatchDownload performs initial hydrate on a fresh device and proves chunked
// snapshot transfer was actually used.
func (s *ComplexMultiBatchScenario) performMultiBatchDownload(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("📥 Starting fresh-device hydrate with forced chunked snapshot transfer")
	}

	// Get initial counts
	initialUserCount, initialPostCount, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get initial laptop data counts: %w", err)
	}

	if verboseLog {
		logger.Info("📊 Initial laptop database state", "users", initialUserCount, "posts", initialPostCount)
	}

	s.laptopApp.resetSnapshotTransferDiagnostics()
	if err := s.laptopApp.rebuildKeepSource(ctx); err != nil {
		logger.Warn("❌ Hydrate failed", "error", err.Error())
		return fmt.Errorf("failed to hydrate laptop from chunked snapshot: %w", err)
	}
	snapshotStats := s.laptopApp.snapshotTransferDiagnostics()
	if snapshotStats.ChunksFetched <= 1 {
		return fmt.Errorf("expected laptop hydrate to fetch more than one snapshot chunk, got %d", snapshotStats.ChunksFetched)
	}

	usersAfter, postsAfter, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get laptop data counts after hydrate: %w", err)
	}

	usersDownloaded := usersAfter - initialUserCount
	postsDownloaded := postsAfter - initialPostCount

	if verboseLog {
		logger.Info("✅ Chunked hydrate completed",
			"snapshot_sessions", snapshotStats.SessionsCreated,
			"snapshot_chunks", snapshotStats.ChunksFetched,
			"users_downloaded", usersDownloaded,
			"posts_downloaded", postsDownloaded,
			"total_users", usersAfter,
			"total_posts", postsAfter,
			"snapshot_chunk_rows", snapshotChunkRows)
	}
	return nil
}

// verifyDownloadedData verifies that downloaded data matches the originally uploaded data
func (s *ComplexMultiBatchScenario) verifyDownloadedData(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("🔍 Verifying downloaded data matches uploaded data")
	}

	if err := s.verifyExpectedBusinessState(s.laptopApp, logger, "laptop"); err != nil {
		return fmt.Errorf("laptop state verification failed: %w", err)
	}

	if verboseLog {
		logger.Info("✅ Downloaded data verification completed successfully")
		logger.Info("📊 Laptop state matches original uploaded dataset")
	}
	return nil
}

// performMultiBatchUpload performs explicit upload rounds until all staged data is uploaded.
func (s *ComplexMultiBatchScenario) performMultiBatchUpload(ctx context.Context, logger *slog.Logger) error {
	return s.performMultiBatchUploadForApp(ctx, logger, s.app, "phone")
}

// performMultiBatchUploadForApp performs explicit upload rounds for any app until dirty state clears.
func (s *ComplexMultiBatchScenario) performMultiBatchUploadForApp(ctx context.Context, logger *slog.Logger, app *MobileApp, deviceName string) error {
	// For deterministic behavior, pause downloads during explicit upload rounds
	if client := app.currentClient(); client != nil {
		client.PauseDownloads()
	}

	uploadRound := 1
	for {
		// Check pending changes before this upload round
		pendingBefore, err := app.pendingChangesCount(ctx)
		if err != nil {
			return fmt.Errorf("failed to get pending changes count before upload round %d: %w", uploadRound, err)
		}
		outboundCount, _, _, err := s.getPushRecoveryStateCounts(ctx, app)
		if err != nil {
			return fmt.Errorf("failed to inspect push recovery state before upload round %d: %w", uploadRound, err)
		}

		if pendingBefore == 0 && outboundCount == 0 {
			if verboseLog {
				logger.Info("✅ All changes uploaded successfully", "device", deviceName)
				logger.Info("📊 Upload summary", "device", deviceName, "total_rounds", uploadRound-1, "pull_limit", pullLimit, "push_limit", pushLimit)
			}
			break
		}

		if verboseLog {
			logger.Info("📤 Upload round", "device", deviceName, "round", uploadRound, "pending_changes", pendingBefore, "outbound_rows", outboundCount)
		}

		// Push the current dirty set and inspect the response.
		err = app.pushPending(ctx)
		if err != nil {
			logger.Warn("❌ Upload round failed", "device", deviceName, "round", uploadRound, "error", err.Error())
			return fmt.Errorf("failed to upload changes in round %d: %w", uploadRound, err)
		}

		// Check pending changes after this upload round
		pendingAfter, err := app.pendingChangesCount(ctx)
		if err != nil {
			return fmt.Errorf("failed to get pending changes count after upload round %d: %w", uploadRound, err)
		}

		uploaded := pendingBefore - pendingAfter
		if verboseLog {
			logger.Info("📈 Upload round completed", "device", deviceName, "round", uploadRound, "uploaded", uploaded, "remaining", pendingAfter)
		}

		// Safety check to prevent infinite loops
		if uploadRound > maxRounds {
			return fmt.Errorf("upload failed: too many rounds (%d), still have %d pending changes", uploadRound, pendingAfter)
		}

		uploadRound++
	}
	// Resume downloads after explicit upload completes
	if client := app.currentClient(); client != nil {
		client.ResumeDownloads()
	}

	// Simulator-only verification guards
	if err := app.debugVerifyPendingCleared(ctx); err != nil {
		logger.Warn("Post-upload pending verification failed", "device", deviceName, "error", err.Error())
	} else {
		if verboseLog {
			logger.Info("✅ Pending queue cleared after upload", "device", deviceName)
		}
	}
	if err := app.debugVerifyBusinessFKIntegrity(ctx); err != nil {
		logger.Warn("Business FK integrity check failed", "device", deviceName, "error", err.Error())
	} else {
		if verboseLog {
			logger.Info("✅ Business FK integrity verified (simulator check)", "device", deviceName)
		}
	}
	return nil
}

// getDataCounts gets the current user and post counts in a database (DRY helper)
func (s *ComplexMultiBatchScenario) getDataCounts(app *MobileApp) (int, int, error) {
	db := app.database()

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

// stage1CreateAdditionalDataOnLaptop creates new data and mixed CRUD churn on laptop
func (s *ComplexMultiBatchScenario) stage1CreateAdditionalDataOnLaptop(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("🏁 STAGE 1: Creating Additional Data on Laptop")
		logger.Info("📊 Target: 20 new users + 400 new posts, then deterministic updates and deletes")
		logger.Info("🎯 Strategy: Keep FK-hostile inserts, then add hot-row churn and cleanup deletes")
	}

	// Verify laptop app is ready
	if s.laptopApp == nil {
		return fmt.Errorf("laptop app not initialized")
	}

	// Check current state of laptop app
	currentUsers, currentPosts, err := s.getDataCounts(s.laptopApp)
	if err != nil {
		return fmt.Errorf("failed to get current laptop data counts: %w", err)
	}
	if verboseLog {
		logger.Info("📊 Current laptop state", "users", currentUsers, "posts", currentPosts)
	}

	if verboseLog {
		logger.Info("📊 Stage 1 configuration", "new_users", newUsersStage2, "posts_per_user", newPostsPerUser, "total_new_posts", newUsersStage2*newPostsPerUser)
	}

	s.testData.NewUserIDs, s.testData.NewPostIDs, err = s.createDeferredFKData(ctx, logger, s.laptopApp, deferredFKCreationSpec{
		label:          "laptop",
		userCount:      newUsersStage2,
		postsPerUser:   newPostsPerUser,
		postLogEvery:   100,
		postNumberBase: 100,
		buildUser: func(userOrdinal int) (string, string) {
			return fmt.Sprintf("New User %d", userOrdinal), fmt.Sprintf("newuser%d@example.com", userOrdinal)
		},
		buildPost: func(globalPostNum, userOrdinal int) (string, string) {
			return fmt.Sprintf("New FK Test Post %d by New User %d", globalPostNum, userOrdinal),
				fmt.Sprintf("Content for new post %d", globalPostNum)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create laptop FK-challenging data: %w", err)
	}
	s.recordDeferredFKCreatedData(s.testData.NewUserIDs, s.testData.NewPostIDs, deferredFKCreationSpec{
		userCount:      newUsersStage2,
		postsPerUser:   newPostsPerUser,
		postNumberBase: 100,
		buildUser: func(userOrdinal int) (string, string) {
			return fmt.Sprintf("New User %d", userOrdinal), fmt.Sprintf("newuser%d@example.com", userOrdinal)
		},
		buildPost: func(globalPostNum, userOrdinal int) (string, string) {
			return fmt.Sprintf("New FK Test Post %d by New User %d", globalPostNum, userOrdinal),
				fmt.Sprintf("Content for new post %d", globalPostNum)
		},
	})
	s.testData.NewFileIDs, s.testData.NewReviewIDs, err = s.createBlobBatch(s.laptopApp, "laptop-seed", newUsersStage2)
	if err != nil {
		return fmt.Errorf("failed to create laptop blob batch: %w", err)
	}

	if err := s.applyMixedCRUDOnLaptop(ctx, logger); err != nil {
		return fmt.Errorf("failed to apply mixed CRUD workload on laptop: %w", err)
	}
	if err := s.rebalancePendingForDeferredFKGroups(ctx, logger, s.laptopApp, "laptop-mixed", s.testData.NewUserIDs, s.testData.NewPostIDs, newPostsPerUser); err != nil {
		return fmt.Errorf("failed to rebalance laptop pending queue: %w", err)
	}

	actualNewUsers := len(s.testData.NewUserIDs)
	actualNewPosts := len(s.testData.NewPostIDs)
	totalNewRecords := actualNewUsers + actualNewPosts

	if verboseLog {
		logger.Info("✅ Stage 1 completed successfully")
		logger.Info("📊 Offline workload created", "new_users", actualNewUsers, "new_posts", actualNewPosts, "created_records", totalNewRecords, "expected_final_users", s.expectedUserCount(), "expected_final_posts", s.expectedPostCount())
		logger.Info("🎯 Stress profile", "mix", "fk-hostile inserts + deterministic updates + deletes", "pragma", "foreign_keys OFF/ON")
	}

	return nil
}

func (s *ComplexMultiBatchScenario) createDeferredFKData(ctx context.Context, logger *slog.Logger, app *MobileApp, spec deferredFKCreationSpec) ([]string, []string, error) {
	totalPosts := spec.userCount * spec.postsPerUser
	userIDs := make([]string, spec.userCount)
	postIDs := make([]string, totalPosts)

	app.stopSync()
	app.pauseSync()
	if verboseLog {
		logger.Info("⏸️ Paused client sync during offline creation", "device", spec.label)
	}

	db := app.database()
	_, _ = db.Exec("ROLLBACK")
	if verboseLog {
		logger.Info("⏱️ About to begin creation transaction", "device", spec.label)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin %s creation tx: %w", spec.label, err)
	}
	defer func() { _ = tx.Rollback() }()

	if verboseLog {
		logger.Info("✅ Creation transaction begun", "device", spec.label)
		logger.Info("🔄 Creating data in FK-challenging order (tx): posts BEFORE users", "device", spec.label)
	}

	if _, err := tx.Exec("PRAGMA defer_foreign_keys = ON"); err != nil {
		return nil, nil, fmt.Errorf("failed to enable deferred FKs in tx (%s): %w", spec.label, err)
	}

	postIndex := 0
	for userGroup := 0; userGroup < spec.userCount; userGroup++ {
		userOrdinal := userGroup + 1
		userID := uuid.New().String()

		for postInGroup := 0; postInGroup < spec.postsPerUser; postInGroup++ {
			postID := uuid.New().String()
			globalPostNum := spec.postNumberBase + postIndex + 1
			title, content := spec.buildPost(globalPostNum, userOrdinal)

			if err := app.createPostTx(tx, postID, userID, title, content); err != nil {
				return nil, nil, fmt.Errorf("failed to create post %d on %s: %w", globalPostNum, spec.label, err)
			}

			postIDs[postIndex] = postID
			postIndex++

			if spec.postLogEvery > 0 && postIndex%spec.postLogEvery == 0 && verboseLog {
				logger.Info("📈 Posts creation progress", "device", spec.label, "created", postIndex, "total", totalPosts)
			}
		}

		name, email := spec.buildUser(userOrdinal)
		if err := app.createUserTx(tx, userID, name, email); err != nil {
			return nil, nil, fmt.Errorf("failed to create user %d on %s: %w", userOrdinal, spec.label, err)
		}
		userIDs[userGroup] = userID

		if verboseLog {
			logger.Info("✅ FK-challenging group complete", "device", spec.label, "group", userOrdinal, "posts_created", spec.postsPerUser, "user_created", 1)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("failed to commit %s creation tx: %w", spec.label, err)
	}
	if verboseLog {
		logger.Info("✅ FK-challenging data creation committed (FKs verified at COMMIT)", "device", spec.label)
	}

	return userIDs, postIDs, nil
}

func (s *ComplexMultiBatchScenario) recordDeferredFKCreatedData(userIDs, postIDs []string, spec deferredFKCreationSpec) {
	for userGroup, userID := range userIDs {
		userOrdinal := userGroup + 1
		name, email := spec.buildUser(userOrdinal)
		s.testData.ExpectedUsers[userID] = expectedUserState{
			Name:   name,
			Email:  email,
			Exists: true,
		}

		for postInGroup := 0; postInGroup < spec.postsPerUser; postInGroup++ {
			postIndex := userGroup*spec.postsPerUser + postInGroup
			globalPostNum := spec.postNumberBase + postIndex + 1
			title, content := spec.buildPost(globalPostNum, userOrdinal)
			s.testData.ExpectedPosts[postIDs[postIndex]] = expectedPostState{
				AuthorID: userID,
				Title:    title,
				Content:  content,
				Exists:   true,
			}
		}
	}
}

func (s *ComplexMultiBatchScenario) applyMixedCRUDOnLaptop(ctx context.Context, logger *slog.Logger) error {
	const (
		originalUserUpdates     = 2
		newUserUpdates          = 3
		originalPostUpdates     = 8
		newPostUpdates          = 20
		deletedOriginalPosts    = 5
		deletedAdditionalPosts  = 8
		deletedTailNewUserCount = 2
	)

	if verboseLog {
		logger.Info("🧪 Applying deterministic mixed CRUD on laptop")
	}

	for i := 0; i < originalUserUpdates; i++ {
		userID := s.testData.UserIDs[i]
		if err := s.applyUserUpdate(s.laptopApp, userID,
			fmt.Sprintf("User %d Laptop Rev 1", i+1),
			fmt.Sprintf("user%d+laptop-rev1@example.com", i+1)); err != nil {
			return err
		}
	}
	if err := s.applyUserUpdate(s.laptopApp, s.testData.UserIDs[0], "User 1 Laptop Rev 2", "user1+laptop-rev2@example.com"); err != nil {
		return err
	}

	for i := 0; i < newUserUpdates; i++ {
		userID := s.testData.NewUserIDs[i]
		if err := s.applyUserUpdate(s.laptopApp, userID,
			fmt.Sprintf("New User %d Laptop Rev 1", i+1),
			fmt.Sprintf("newuser%d+laptop-rev1@example.com", i+1)); err != nil {
			return err
		}
	}
	if err := s.applyUserUpdate(s.laptopApp, s.testData.NewUserIDs[1], "New User 2 Laptop Rev 2", "newuser2+laptop-rev2@example.com"); err != nil {
		return err
	}

	for i := 0; i < originalPostUpdates; i++ {
		postID := s.testData.PostIDs[i]
		if err := s.applyPostUpdate(s.laptopApp, postID,
			fmt.Sprintf("Original Post %d Laptop Rev 1", i+1),
			fmt.Sprintf("Original post %d updated on laptop during offline stress", i+1)); err != nil {
			return err
		}
	}
	if err := s.applyPostUpdate(s.laptopApp, s.testData.PostIDs[0], "Original Post 1 Laptop Rev 2", "Original post 1 received a second offline edit on laptop"); err != nil {
		return err
	}

	for i := 0; i < newPostUpdates; i++ {
		postID := s.testData.NewPostIDs[i]
		if err := s.applyPostUpdate(s.laptopApp, postID,
			fmt.Sprintf("New Post %d Laptop Rev 1", i+1),
			fmt.Sprintf("New post %d updated on laptop during offline stress", i+1)); err != nil {
			return err
		}
	}
	if err := s.applyPostUpdate(s.laptopApp, s.testData.NewPostIDs[1], "New Post 2 Laptop Rev 2", "New post 2 received a second offline edit on laptop"); err != nil {
		return err
	}

	for i := 0; i < deletedOriginalPosts; i++ {
		if err := s.applyPostDelete(s.laptopApp, s.testData.PostIDs[i]); err != nil {
			return err
		}
	}

	for i := 0; i < deletedAdditionalPosts; i++ {
		if err := s.applyPostDelete(s.laptopApp, s.testData.NewPostIDs[100+i]); err != nil {
			return err
		}
	}

	for offset := 0; offset < deletedTailNewUserCount; offset++ {
		userGroup := len(s.testData.NewUserIDs) - 1 - offset
		for _, postID := range s.newUserPostIDs(userGroup) {
			if err := s.applyPostDelete(s.laptopApp, postID); err != nil {
				return err
			}
		}
		if err := s.applyUserDelete(ctx, s.laptopApp, s.testData.NewUserIDs[userGroup]); err != nil {
			return err
		}
	}

	if verboseLog {
		logger.Info("✅ Mixed CRUD workload applied",
			"original_user_updates", originalUserUpdates+1,
			"new_user_updates", newUserUpdates+1,
			"original_post_updates", originalPostUpdates+1,
			"new_post_updates", newPostUpdates+1,
			"deleted_original_posts", deletedOriginalPosts,
			"deleted_additional_new_posts", deletedAdditionalPosts,
			"deleted_new_users", deletedTailNewUserCount,
			"expected_final_users", s.expectedUserCount(),
			"expected_final_posts", s.expectedPostCount(),
			"expected_final_files", s.expectedFileCount(),
			"expected_final_reviews", s.expectedReviewCount())
	}

	return nil
}

func (s *ComplexMultiBatchScenario) applyUserUpdate(app *MobileApp, userID, name, email string) error {
	if err := app.updateUser(userID, name, email); err != nil {
		return fmt.Errorf("failed to update user %s: %w", userID, err)
	}
	s.testData.ExpectedUsers[userID] = expectedUserState{Name: name, Email: email, Exists: true}
	return nil
}

func (s *ComplexMultiBatchScenario) applyPostUpdate(app *MobileApp, postID, title, content string) error {
	if err := app.updatePost(postID, title, content); err != nil {
		return fmt.Errorf("failed to update post %s: %w", postID, err)
	}
	state := s.testData.ExpectedPosts[postID]
	state.Title = title
	state.Content = content
	state.Exists = true
	s.testData.ExpectedPosts[postID] = state
	return nil
}

func (s *ComplexMultiBatchScenario) applyPostDelete(app *MobileApp, postID string) error {
	if err := app.deletePost(postID); err != nil {
		return fmt.Errorf("failed to delete post %s: %w", postID, err)
	}
	state := s.testData.ExpectedPosts[postID]
	state.Exists = false
	s.testData.ExpectedPosts[postID] = state
	return nil
}

func (s *ComplexMultiBatchScenario) applyUserDelete(ctx context.Context, app *MobileApp, userID string) error {
	if err := app.deleteUserWithContext(ctx, userID); err != nil {
		return fmt.Errorf("failed to delete user %s: %w", userID, err)
	}
	state := s.testData.ExpectedUsers[userID]
	state.Exists = false
	s.testData.ExpectedUsers[userID] = state
	return nil
}

func (s *ComplexMultiBatchScenario) newUserPostIDs(userGroup int) []string {
	start := userGroup * newPostsPerUser
	end := start + newPostsPerUser
	return s.testData.NewPostIDs[start:end]
}

func (s *ComplexMultiBatchScenario) rebalancePendingForDeferredFKGroups(ctx context.Context, logger *slog.Logger, app *MobileApp, label string, userIDs, postIDs []string, postsPerGroup int) error {
	if len(userIDs) == 0 || len(postIDs) == 0 || postsPerGroup <= 0 {
		return nil
	}

	db := app.database()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin pending rebalance tx for %s: %w", label, err)
	}
	defer func() { _ = tx.Rollback() }()

	reordered := 0
	prefixPosts := postsPerGroup / 2
	if prefixPosts < 1 {
		prefixPosts = 1
	}

	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	step := time.Millisecond
	seq := 0

	for groupIdx, userID := range userIDs {
		start := groupIdx * postsPerGroup
		end := start + postsPerGroup
		if end > len(postIDs) {
			end = len(postIDs)
		}
		groupPosts := postIDs[start:end]

		for _, postID := range groupPosts[:min(prefixPosts, len(groupPosts))] {
			changed, err := setDirtyRowOrder(ctx, tx, "posts", postID, seq, base.Add(time.Duration(seq)*step))
			if err != nil {
				return fmt.Errorf("failed to rebalance post %s for %s: %w", postID, label, err)
			}
			if changed {
				reordered++
				seq++
			}
		}

		changed, err := setDirtyRowOrder(ctx, tx, "users", userID, seq, base.Add(time.Duration(seq)*step))
		if err != nil {
			return fmt.Errorf("failed to rebalance user %s for %s: %w", userID, label, err)
		}
		if changed {
			reordered++
			seq++
		}

		for _, postID := range groupPosts[min(prefixPosts, len(groupPosts)):] {
			changed, err := setDirtyRowOrder(ctx, tx, "posts", postID, seq, base.Add(time.Duration(seq)*step))
			if err != nil {
				return fmt.Errorf("failed to rebalance trailing post %s for %s: %w", postID, label, err)
			}
			if changed {
				reordered++
				seq++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit pending rebalance for %s: %w", label, err)
	}

	if verboseLog {
		logger.Info("🧭 Rebalanced dirty-row order for FK progress", "label", label, "groups", len(userIDs), "reordered_entries", reordered, "prefix_posts_before_user", prefixPosts)
	}
	return nil
}

func setDirtyRowOrder(ctx context.Context, tx *sql.Tx, tableName, pkUUID string, dirtyOrdinal int, updatedAt time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE _sync_dirty_rows
		SET dirty_ordinal = ?, updated_at = ?
		WHERE table_name = ? AND key_json = json_object('id', ?)
	`, dirtyOrdinal, updatedAt.UTC().Format("2006-01-02T15:04:05.000Z"), tableName, pkUUID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// stage2UploadNewDataFromLaptop uploads the 420 new records from laptop to server
func (s *ComplexMultiBatchScenario) stage2UploadNewDataFromLaptop(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("🏁 STAGE 2: Upload New Data from Laptop")
		logger.Info("📊 Target: Push the laptop mixed-CRUD session as a bundle-backed dirty set")
		logger.Info("🎯 Expected: Upload rounds continue only if retries are needed, not to drain artificial batch limits")
	}

	// Check pending changes before upload
	pendingBefore, err := s.laptopApp.pendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count before upload: %w", err)
	}

	if verboseLog {
		logger.Info("📊 Pending changes before upload", "count", pendingBefore, "expected_final_users", s.expectedUserCount(), "expected_final_posts", s.expectedPostCount())
	}

	if pendingBefore == 0 {
		return fmt.Errorf("expected pending changes on laptop before upload, got 0")
	}

	if verboseLog {
		logger.Info("🚀 Starting laptop chunked push-session upload")
	}

	// Resume uploads now that creation is complete and pending has been verified
	if client := s.laptopApp.currentClient(); client != nil {
		client.ResumeUploads()
		// Keep downloads paused to avoid noisy interference; download verification happens later explicitly
		if verboseLog {
			logger.Info("▶️ Resumed laptop client uploads for Stage 2")
		}
	}

	s.laptopApp.resetPushTransferDiagnostics()
	if err := s.verifyChunkedPushRestartRecovery(ctx, logger); err != nil {
		return fmt.Errorf("failed chunked push restart recovery verification: %w", err)
	}

	// Verify all changes uploaded
	pendingAfter, err := s.laptopApp.pendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count after upload: %w", err)
	}

	if pendingAfter != 0 {
		return fmt.Errorf("upload incomplete: %d changes still pending", pendingAfter)
	}

	// Capture the durable bundle checkpoint after laptop uploads.
	s.stableBundleSeq, err = s.laptopApp.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to get stable bundle seq after upload: %w", err)
	}

	if verboseLog {
		logger.Info("✅ Stage 2 completed successfully")
		logger.Info("📊 Upload summary",
			"remaining_pending", pendingAfter,
			"stable_bundle_seq", s.stableBundleSeq,
			"push_sessions", s.laptopApp.pushTransferDiagnostics().SessionsCreated,
			"push_chunks", s.laptopApp.pushTransferDiagnostics().ChunksUploaded,
			"committed_bundle_chunks", s.laptopApp.pushTransferDiagnostics().CommittedBundleChunksRead,
			"expected_final_users", s.expectedUserCount(),
			"expected_final_posts", s.expectedPostCount())
	}

	return nil
}

func (s *ComplexMultiBatchScenario) verifyChunkedPushRestartRecovery(ctx context.Context, logger *slog.Logger) error {
	const preCommitCrashMessage = "simulated crash after chunk upload before push commit"
	const replayCrashMessage = "simulated crash after committed bundle fetch before local replay"

	client := s.laptopApp.currentClient()
	if client == nil {
		return fmt.Errorf("laptop client not initialized")
	}
	client.SetBeforePushCommitHook(func(context.Context) error {
		return fmt.Errorf(preCommitCrashMessage)
	})

	_, err := client.PushPending(ctx)
	if err == nil || !strings.Contains(err.Error(), preCommitCrashMessage) {
		return fmt.Errorf("expected simulated pre-commit crash, got %v", err)
	}

	pushStatsBeforeCommitRestart := s.laptopApp.pushTransferDiagnostics()
	if pushStatsBeforeCommitRestart.ChunksUploaded <= 1 {
		return fmt.Errorf("expected laptop push to upload more than one chunk before pre-commit restart, got %d", pushStatsBeforeCommitRestart.ChunksUploaded)
	}
	if pushStatsBeforeCommitRestart.CommittedBundleChunksRead != 0 {
		return fmt.Errorf("expected no committed bundle fetch before pre-commit restart, got %d chunks", pushStatsBeforeCommitRestart.CommittedBundleChunksRead)
	}

	outboundCount, stagedCount, dirtyCount, err := s.getPushRecoveryStateCounts(ctx, s.laptopApp)
	if err != nil {
		return err
	}
	if outboundCount == 0 {
		return fmt.Errorf("expected outbound push snapshot rows to remain after simulated pre-commit crash")
	}
	if stagedCount != 0 {
		return fmt.Errorf("expected no staged committed bundle rows after simulated pre-commit crash, got %d", stagedCount)
	}
	if dirtyCount != 0 {
		return fmt.Errorf("expected only outbound snapshot rows after simulated pre-commit crash, got dirty_rows=%d", dirtyCount)
	}

	if verboseLog {
		logger.Info("♻️ Restarting laptop after simulated pre-commit crash",
			"outbound_rows", outboundCount,
			"staged_rows", stagedCount,
			"dirty_rows", dirtyCount,
			"push_chunks", pushStatsBeforeCommitRestart.ChunksUploaded,
			"committed_bundle_chunks", pushStatsBeforeCommitRestart.CommittedBundleChunksRead)
	}

	restartedLaptop, err := s.restartApp(ctx, s.laptopApp, "laptop")
	if err != nil {
		return err
	}
	s.laptopApp = restartedLaptop

	client = s.laptopApp.currentClient()
	if client == nil {
		return fmt.Errorf("restarted laptop client not initialized")
	}
	client.ResumeUploads()
	s.laptopApp.resetPushTransferDiagnostics()
	client.SetBeforePushReplayHook(func(context.Context) error {
		return fmt.Errorf(replayCrashMessage)
	})
	_, err = client.PushPending(ctx)
	if err == nil || !strings.Contains(err.Error(), replayCrashMessage) {
		return fmt.Errorf("expected simulated pre-replay crash after pre-commit restart, got %v", err)
	}

	pushStatsBeforeReplayRestart := s.laptopApp.pushTransferDiagnostics()
	if pushStatsBeforeReplayRestart.SessionsCreated != 1 {
		return fmt.Errorf("expected one fresh push session after pre-commit restart, got %d", pushStatsBeforeReplayRestart.SessionsCreated)
	}
	if pushStatsBeforeReplayRestart.ChunksUploaded <= 1 {
		return fmt.Errorf("expected restarted laptop push to re-upload more than one chunk from zero, got %d", pushStatsBeforeReplayRestart.ChunksUploaded)
	}
	if pushStatsBeforeReplayRestart.CommittedBundleChunksRead <= 1 {
		return fmt.Errorf("expected restarted laptop replay fetch to read more than one committed bundle chunk before replay restart, got %d", pushStatsBeforeReplayRestart.CommittedBundleChunksRead)
	}

	outboundCount, stagedCount, dirtyCount, err = s.getPushRecoveryStateCounts(ctx, s.laptopApp)
	if err != nil {
		return err
	}
	if outboundCount == 0 {
		return fmt.Errorf("expected outbound push snapshot rows to remain after simulated pre-replay crash")
	}
	if stagedCount == 0 {
		return fmt.Errorf("expected staged committed bundle rows to remain after simulated pre-replay crash")
	}
	if dirtyCount != 0 {
		return fmt.Errorf("expected no live dirty rows after simulated pre-replay crash without newer local writes, got %d", dirtyCount)
	}

	if verboseLog {
		logger.Info("♻️ Restarting laptop after simulated pre-replay crash",
			"outbound_rows", outboundCount,
			"staged_rows", stagedCount,
			"dirty_rows", dirtyCount,
			"push_sessions", pushStatsBeforeReplayRestart.SessionsCreated,
			"push_chunks", pushStatsBeforeReplayRestart.ChunksUploaded,
			"committed_bundle_chunks", pushStatsBeforeReplayRestart.CommittedBundleChunksRead)
	}

	restartedLaptop, err = s.restartApp(ctx, s.laptopApp, "laptop")
	if err != nil {
		return err
	}
	s.laptopApp = restartedLaptop
	if client = s.laptopApp.currentClient(); client != nil {
		client.ResumeUploads()
	}
	s.laptopApp.resetPushTransferDiagnostics()
	if err := s.performMultiBatchUploadForApp(ctx, logger, s.laptopApp, "laptop-restart-replay"); err != nil {
		return fmt.Errorf("failed to replay committed laptop push after restart: %w", err)
	}

	outboundCount, stagedCount, dirtyCount, err = s.getPushRecoveryStateCounts(ctx, s.laptopApp)
	if err != nil {
		return err
	}
	if outboundCount != 0 || stagedCount != 0 || dirtyCount != 0 {
		return fmt.Errorf("expected restart replay to clear outbound=%d staged=%d dirty=%d", outboundCount, stagedCount, dirtyCount)
	}

	postCommitRestart, err := s.restartApp(ctx, s.laptopApp, "laptop")
	if err != nil {
		return err
	}
	s.laptopApp = postCommitRestart
	lastSeen, err := s.laptopApp.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to load laptop checkpoint after post-commit restart: %w", err)
	}
	if lastSeen <= 0 {
		return fmt.Errorf("expected durable bundle checkpoint after post-commit restart, got %d", lastSeen)
	}
	if _, _, err := s.laptopApp.pullToStable(ctx); err != nil {
		return fmt.Errorf("post-commit restart pull failed: %w", err)
	}
	return nil
}

func (s *ComplexMultiBatchScenario) getPushRecoveryStateCounts(ctx context.Context, app *MobileApp) (outboundCount int, stagedCount int, dirtyCount int, err error) {
	db := app.database()
	if db == nil {
		return 0, 0, 0, fmt.Errorf("database not initialized")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&outboundCount); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to count _sync_outbox_rows rows: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to count _sync_push_stage rows: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to count _sync_dirty_rows rows: %w", err)
	}
	return outboundCount, stagedCount, dirtyCount, nil
}

func (s *ComplexMultiBatchScenario) restartApp(ctx context.Context, app *MobileApp, label string) (*MobileApp, error) {
	if app == nil {
		return nil, fmt.Errorf("%s app is not initialized", label)
	}
	originalPreserveDB := app.config.PreserveDB
	app.config.PreserveDB = true
	cfg := *app.config
	cfg.PreserveDB = originalPreserveDB
	if err := app.close(); err != nil {
		return nil, fmt.Errorf("failed to close %s app for restart: %w", label, err)
	}
	restarted, err := newMobileApp(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to recreate %s app after restart: %w", label, err)
	}
	if err := restarted.onLaunch(ctx); err != nil {
		return nil, fmt.Errorf("failed to relaunch %s app after restart: %w", label, err)
	}
	restarted.stopSync()
	restarted.pauseSync()
	return restarted, nil
}

// stage3DownloadVerificationLaptop verifies the laptop checkpoint already matches the stable
// bundle published by its own Stage 2 upload, without issuing another network pull.
func (s *ComplexMultiBatchScenario) stage3DownloadVerificationLaptop(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("🏁 STAGE 3: Checkpoint Verification - Laptop")
		logger.Info("📊 Expected result: laptop durable checkpoint already equals the Stage 2 stable bundle")
	}

	lastSeen, err := s.laptopApp.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to get laptop stable checkpoint: %w", err)
	}
	if lastSeen != s.stableBundleSeq {
		return fmt.Errorf("unexpected laptop checkpoint: expected stable bundle seq %d, got %d", s.stableBundleSeq, lastSeen)
	}

	if verboseLog {
		logger.Info("✅ Stage 3 completed successfully")
		logger.Info("📊 Verification result", "device", "laptop", "stable_bundle_seq", s.stableBundleSeq, "last_seen_bundle_seq", lastSeen)
	}

	return nil
}

// stage4DownloadVerificationPhone verifies phone gets all new records from laptop (progress-based paging)
func (s *ComplexMultiBatchScenario) stage4DownloadVerificationPhone(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("🏁 STAGE 4: Download Verification - Phone (Should Get All New Records)")
	}

	if client := s.app.currentClient(); client != nil {
		client.ResumeDownloads()
		if verboseLog {
			logger.Info("▶️ Ensured phone client downloads are resumed for Stage 4")
		}
	}

	// Compute the expected delta based on the phone's current snapshot.
	// Target totals come from the scenario configuration:
	//   - Initially we created `totalUsers` users with `postsPerUser` posts each on the phone
	//   - Stage 1 created `newUsersStage2` users with `newPostsPerUser` posts each on the laptop
	// Final expected totals after Stage 4 come from the tracked final state after laptop mixed CRUD.
	targetTotalUsers := s.expectedUserCount()
	targetTotalPosts := s.expectedPostCount()

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

	if verboseLog {
		logger.Info("📊 Expected result (dynamic)",
			"target_total_users", targetTotalUsers,
			"target_total_posts", targetTotalPosts,
			"phone_users_before", usersBefore,
			"phone_posts_before", postsBefore,
			"expected_users_delta", expUsersDelta,
			"expected_posts_delta", expPostsDelta,
			"expected_total_delta", expectedNewRecords)

		logger.Info("📊 Phone data before download", "users", usersBefore, "posts", postsBefore)
	}

	// If there's nothing new expected, we're done.
	if expectedNewRecords == 0 {
		if verboseLog {
			logger.Info("✅ No new records expected for download; skipping Stage 4")
		}
		return nil
	}
	if s.simulator.verifier == nil {
		return fmt.Errorf("complex-multi-batch requires database verification access to force prune fallback")
	}

	if _, err := s.simulator.verifier.pool.Exec(ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = $2
		WHERE user_id = $1
	`, s.config.UserID, s.stableBundleSeq); err != nil {
		return fmt.Errorf("failed to advance retained bundle floor for prune fallback: %w", err)
	}
	if err := s.verifyServerHistoryPrunedState(ctx, logger); err != nil {
		return fmt.Errorf("failed to verify pruned retained history state: %w", err)
	}

	s.app.resetSnapshotTransferDiagnostics()
	if verboseLog {
		logger.Info("📥 Starting bundle pull to the stable upload checkpoint with forced prune fallback", "stable_bundle_seq", s.stableBundleSeq, "snapshot_chunk_rows", snapshotChunkRows)
	}
	totalApplied, lastAfter, err := s.app.pullToStable(ctx)
	if err != nil {
		return fmt.Errorf("bundle pull to stable checkpoint failed: %w", err)
	}
	snapshotStats := s.app.snapshotTransferDiagnostics()
	if snapshotStats.ChunksFetched <= 1 {
		return fmt.Errorf("expected phone prune fallback to fetch more than one snapshot chunk, got %d", snapshotStats.ChunksFetched)
	}
	if verboseLog {
		logger.Info("📊 Bundle pull complete", "applied", totalApplied, "last_after", lastAfter, "stable_bundle_seq", s.stableBundleSeq, "snapshot_sessions", snapshotStats.SessionsCreated, "snapshot_chunks", snapshotStats.ChunksFetched)
	}

	// Verify we reached the target totals (stronger check than deltas)
	usersAfter, postsAfter, err := s.getDataCounts(s.app)
	if err != nil {
		return fmt.Errorf("failed to get phone data counts after bundle pull: %w", err)
	}
	if usersAfter != targetTotalUsers || postsAfter != targetTotalPosts {
		return fmt.Errorf("final count mismatch after bundle pull: expected users=%d posts=%d, got users=%d posts=%d",
			targetTotalUsers, targetTotalPosts, usersAfter, postsAfter)
	}
	lastSeen, err := s.app.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to get phone stable checkpoint after prune fallback: %w", err)
	}
	if lastSeen != s.stableBundleSeq {
		return fmt.Errorf("unexpected phone checkpoint after prune fallback: expected stable bundle seq %d, got %d", s.stableBundleSeq, lastSeen)
	}

	if verboseLog {
		logger.Info("✅ Stage 4 completed successfully")
		logger.Info("📊 Download verification", "device", "phone", "users_total", usersAfter, "posts_total", postsAfter, "targets", fmt.Sprintf("users=%d posts=%d", targetTotalUsers, targetTotalPosts), "last_seen_bundle_seq", lastSeen)
	}

	return nil
}

func (s *ComplexMultiBatchScenario) verifyServerHistoryPrunedState(ctx context.Context, logger *slog.Logger) error {
	if s.simulator.verifier == nil {
		return fmt.Errorf("database verifier is required for retained-history verification")
	}

	retentionState, err := s.simulator.verifier.GetUserRetentionState(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if retentionState.RetainedBundleFloor != s.stableBundleSeq {
		return fmt.Errorf("expected retained bundle floor %d, got %d", s.stableBundleSeq, retentionState.RetainedBundleFloor)
	}

	bundlesBeforeDelete, err := s.simulator.verifier.CountCommittedBundlesAtOrBelowRetainedFloor(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if bundlesBeforeDelete == 0 {
		return fmt.Errorf("expected committed bundles at or below retained floor %d before deletion", retentionState.RetainedBundleFloor)
	}

	rowsBeforeDelete, err := s.simulator.verifier.CountCommittedBundleRowsAtOrBelowRetainedFloor(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if rowsBeforeDelete == 0 {
		return fmt.Errorf("expected committed bundle rows at or below retained floor %d before deletion", retentionState.RetainedBundleFloor)
	}

	sourceStateCountBefore, err := s.simulator.verifier.CountSourceStateRows(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if sourceStateCountBefore == 0 {
		return fmt.Errorf("expected source_state rows to survive retained-history pruning setup")
	}

	if verboseLog {
		logger.Info("🗜️ Pruning retained history on the server",
			"retained_bundle_floor", retentionState.RetainedBundleFloor,
			"bundle_log_rows_before", bundlesBeforeDelete,
			"bundle_row_rows_before", rowsBeforeDelete,
			"source_state_rows", sourceStateCountBefore)
	}

	if err := s.simulator.verifier.DeleteCommittedHistoryAtOrBelowRetainedFloor(ctx, s.config.UserID); err != nil {
		return err
	}

	bundlesAfterDelete, err := s.simulator.verifier.CountCommittedBundlesAtOrBelowRetainedFloor(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if bundlesAfterDelete != 0 {
		return fmt.Errorf("expected no committed bundles at or below retained floor after deletion, got %d", bundlesAfterDelete)
	}

	rowsAfterDelete, err := s.simulator.verifier.CountCommittedBundleRowsAtOrBelowRetainedFloor(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if rowsAfterDelete != 0 {
		return fmt.Errorf("expected no committed bundle rows at or below retained floor after deletion, got %d", rowsAfterDelete)
	}

	sourceStateCountAfter, err := s.simulator.verifier.CountSourceStateRows(ctx, s.config.UserID)
	if err != nil {
		return err
	}
	if sourceStateCountAfter != sourceStateCountBefore {
		return fmt.Errorf("expected source_state row count to remain %d after history deletion, got %d", sourceStateCountBefore, sourceStateCountAfter)
	}

	expectedLiveCounts := map[string]int{
		"business.users":        s.expectedUserCount(),
		"business.posts":        s.expectedPostCount(),
		"business.files":        s.expectedFileCount(),
		"business.file_reviews": s.expectedReviewCount(),
	}
	for tableName, expectedCount := range expectedLiveCounts {
		rowStateCount, err := s.simulator.verifier.CountUserRecords(ctx, tableName, s.config.UserID)
		if err != nil {
			return fmt.Errorf("failed to count row_state-backed records in %s after retained-history deletion: %w", tableName, err)
		}
		if rowStateCount != expectedCount {
			return fmt.Errorf("unexpected live row_state count in %s after retained-history deletion: expected %d, got %d", tableName, expectedCount, rowStateCount)
		}
	}

	if verboseLog {
		logger.Info("✅ Retained-history deletion verified on the server",
			"retained_bundle_floor", retentionState.RetainedBundleFloor,
			"bundle_log_rows_after", bundlesAfterDelete,
			"bundle_row_rows_after", rowsAfterDelete,
			"source_state_rows", sourceStateCountAfter)
	}
	return nil
}

// stage5DataVerification performs comprehensive verification of the multi-stage sync
func (s *ComplexMultiBatchScenario) stage5DataVerification(ctx context.Context, logger *slog.Logger) error {
	if verboseLog {
		logger.Info("🏁 STAGE 5: Data Verification")
		logger.Info("📊 Comprehensive verification of multi-stage sync results")
	}

	// Get final counts on phone
	finalUsers, finalPosts, err := s.getDataCounts(s.app)
	if err != nil {
		return fmt.Errorf("failed to get final phone data counts: %w", err)
	}

	// Expected totals: original data + new data
	expectedTotalUsers := s.expectedUserCount()
	expectedTotalPosts := s.expectedPostCount()

	if verboseLog {
		logger.Info("📊 Final data count verification",
			"expected_total_users", expectedTotalUsers, "actual_users", finalUsers,
			"expected_total_posts", expectedTotalPosts, "actual_posts", finalPosts)
	}

	// Verify total counts
	if finalUsers != expectedTotalUsers {
		return fmt.Errorf("total user count mismatch: expected %d, got %d", expectedTotalUsers, finalUsers)
	}

	if finalPosts != expectedTotalPosts {
		return fmt.Errorf("total post count mismatch: expected %d, got %d", expectedTotalPosts, finalPosts)
	}

	if err := s.verifyExpectedBusinessState(s.laptopApp, logger, "laptop"); err != nil {
		return fmt.Errorf("laptop final state verification failed: %w", err)
	}
	if err := s.verifyExpectedBusinessState(s.app, logger, "phone"); err != nil {
		return fmt.Errorf("phone final state verification failed: %w", err)
	}

	if verboseLog {
		logger.Info("✅ Stage 5 completed successfully")
		logger.Info("📊 All verification checks passed")
		logger.Info("🎯 Multi-stage sync test: FULLY SUCCESSFUL")
	}

	return nil
}

func (s *ComplexMultiBatchScenario) expectedUserCount() int {
	count := 0
	for _, state := range s.testData.ExpectedUsers {
		if state.Exists {
			count++
		}
	}
	return count
}

func (s *ComplexMultiBatchScenario) expectedPostCount() int {
	count := 0
	for _, state := range s.testData.ExpectedPosts {
		if state.Exists {
			count++
		}
	}
	return count
}

func (s *ComplexMultiBatchScenario) expectedFileCount() int {
	count := 0
	for _, state := range s.testData.ExpectedFiles {
		if state.Exists {
			count++
		}
	}
	return count
}

func (s *ComplexMultiBatchScenario) expectedReviewCount() int {
	count := 0
	for _, state := range s.testData.ExpectedReviews {
		if state.Exists {
			count++
		}
	}
	return count
}

func (s *ComplexMultiBatchScenario) verifyExpectedBusinessState(app *MobileApp, logger *slog.Logger, device string) error {
	actualUsers, err := s.loadActualUsers(app.database())
	if err != nil {
		return fmt.Errorf("failed to load users on %s: %w", device, err)
	}
	actualPosts, err := s.loadActualPosts(app.database())
	if err != nil {
		return fmt.Errorf("failed to load posts on %s: %w", device, err)
	}
	actualFiles, err := s.loadActualFiles(app.database())
	if err != nil {
		return fmt.Errorf("failed to load files on %s: %w", device, err)
	}
	actualReviews, err := s.loadActualReviews(app.database())
	if err != nil {
		return fmt.Errorf("failed to load file reviews on %s: %w", device, err)
	}

	if verboseLog {
		logger.Info("🔍 Verifying expected business state",
			"device", device,
			"expected_users", s.expectedUserCount(),
			"actual_users", len(actualUsers),
			"expected_posts", s.expectedPostCount(),
			"actual_posts", len(actualPosts),
			"expected_files", s.expectedFileCount(),
			"actual_files", len(actualFiles),
			"expected_reviews", s.expectedReviewCount(),
			"actual_reviews", len(actualReviews))
	}

	if err := s.compareExpectedUsers(device, actualUsers); err != nil {
		return err
	}
	if err := s.compareExpectedPosts(device, actualPosts); err != nil {
		return err
	}
	if err := s.compareExpectedFiles(device, actualFiles); err != nil {
		return err
	}
	if err := s.compareExpectedReviews(device, actualReviews); err != nil {
		return err
	}
	if err := s.verifyActualFKIntegrity(device, actualUsers, actualPosts, actualFiles, actualReviews); err != nil {
		return err
	}

	if verboseLog {
		logger.Info("✅ Expected business state verified", "device", device, "users", len(actualUsers), "posts", len(actualPosts), "files", len(actualFiles), "reviews", len(actualReviews))
	}
	return nil
}

func (s *ComplexMultiBatchScenario) loadActualUsers(db *sql.DB) (map[string]expectedUserState, error) {
	rows, err := db.Query(`SELECT id, name, email FROM users`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make(map[string]expectedUserState)
	for rows.Next() {
		var id, name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			return nil, err
		}
		users[id] = expectedUserState{Name: name, Email: email, Exists: true}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

func (s *ComplexMultiBatchScenario) loadActualPosts(db *sql.DB) (map[string]expectedPostState, error) {
	rows, err := db.Query(`SELECT id, author_id, title, content FROM posts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	posts := make(map[string]expectedPostState)
	for rows.Next() {
		var id, authorID, title, content string
		if err := rows.Scan(&id, &authorID, &title, &content); err != nil {
			return nil, err
		}
		posts[id] = expectedPostState{AuthorID: authorID, Title: title, Content: content, Exists: true}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return posts, nil
}

func (s *ComplexMultiBatchScenario) loadActualFiles(db *sql.DB) (map[string]expectedFileState, error) {
	rows, err := db.Query(`SELECT lower(hex(id)), name, lower(hex(data)) FROM files`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files := make(map[string]expectedFileState)
	for rows.Next() {
		var idHex, name, dataHex string
		if err := rows.Scan(&idHex, &name, &dataHex); err != nil {
			return nil, err
		}
		files[idHex] = expectedFileState{Name: name, DataHex: dataHex, Exists: true}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return files, nil
}

func (s *ComplexMultiBatchScenario) loadActualReviews(db *sql.DB) (map[string]expectedReviewState, error) {
	rows, err := db.Query(`SELECT lower(hex(id)), lower(hex(file_id)), review FROM file_reviews`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	reviews := make(map[string]expectedReviewState)
	for rows.Next() {
		var idHex, fileIDHex, review string
		if err := rows.Scan(&idHex, &fileIDHex, &review); err != nil {
			return nil, err
		}
		reviews[idHex] = expectedReviewState{FileID: fileIDHex, Review: review, Exists: true}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return reviews, nil
}

func (s *ComplexMultiBatchScenario) compareExpectedUsers(device string, actual map[string]expectedUserState) error {
	if len(actual) != s.expectedUserCount() {
		return fmt.Errorf("user count mismatch on %s: expected %d, got %d", device, s.expectedUserCount(), len(actual))
	}

	for id, expected := range s.testData.ExpectedUsers {
		actualState, exists := actual[id]
		if !expected.Exists {
			if exists {
				return fmt.Errorf("unexpected deleted user %s present on %s", id, device)
			}
			continue
		}
		if !exists {
			return fmt.Errorf("expected user %s missing on %s", id, device)
		}
		if actualState.Name != expected.Name || actualState.Email != expected.Email {
			return fmt.Errorf("user state mismatch on %s for %s: expected (%q, %q), got (%q, %q)",
				device, id, expected.Name, expected.Email, actualState.Name, actualState.Email)
		}
	}

	for id := range actual {
		expected, known := s.testData.ExpectedUsers[id]
		if !known || !expected.Exists {
			return fmt.Errorf("unexpected live user %s on %s", id, device)
		}
	}
	return nil
}

func (s *ComplexMultiBatchScenario) compareExpectedPosts(device string, actual map[string]expectedPostState) error {
	if len(actual) != s.expectedPostCount() {
		return fmt.Errorf("post count mismatch on %s: expected %d, got %d", device, s.expectedPostCount(), len(actual))
	}

	for id, expected := range s.testData.ExpectedPosts {
		actualState, exists := actual[id]
		if !expected.Exists {
			if exists {
				return fmt.Errorf("unexpected deleted post %s present on %s", id, device)
			}
			continue
		}
		if !exists {
			return fmt.Errorf("expected post %s missing on %s", id, device)
		}
		if actualState.AuthorID != expected.AuthorID || actualState.Title != expected.Title || actualState.Content != expected.Content {
			return fmt.Errorf("post state mismatch on %s for %s: expected author=%q title=%q content=%q, got author=%q title=%q content=%q",
				device, id, expected.AuthorID, expected.Title, expected.Content, actualState.AuthorID, actualState.Title, actualState.Content)
		}
	}

	for id := range actual {
		expected, known := s.testData.ExpectedPosts[id]
		if !known || !expected.Exists {
			return fmt.Errorf("unexpected live post %s on %s", id, device)
		}
	}
	return nil
}

func (s *ComplexMultiBatchScenario) compareExpectedFiles(device string, actual map[string]expectedFileState) error {
	if len(actual) != s.expectedFileCount() {
		return fmt.Errorf("file count mismatch on %s: expected %d, got %d", device, s.expectedFileCount(), len(actual))
	}

	for id, expected := range s.testData.ExpectedFiles {
		actualState, exists := actual[id]
		if !expected.Exists {
			if exists {
				return fmt.Errorf("unexpected deleted file %s present on %s", id, device)
			}
			continue
		}
		if !exists {
			return fmt.Errorf("expected file %s missing on %s", id, device)
		}
		if actualState.Name != expected.Name || actualState.DataHex != expected.DataHex {
			return fmt.Errorf("file state mismatch on %s for %s: expected (%q, %q), got (%q, %q)",
				device, id, expected.Name, expected.DataHex, actualState.Name, actualState.DataHex)
		}
	}

	for id := range actual {
		expected, known := s.testData.ExpectedFiles[id]
		if !known || !expected.Exists {
			return fmt.Errorf("unexpected live file %s on %s", id, device)
		}
	}
	return nil
}

func (s *ComplexMultiBatchScenario) compareExpectedReviews(device string, actual map[string]expectedReviewState) error {
	if len(actual) != s.expectedReviewCount() {
		return fmt.Errorf("file review count mismatch on %s: expected %d, got %d", device, s.expectedReviewCount(), len(actual))
	}

	for id, expected := range s.testData.ExpectedReviews {
		actualState, exists := actual[id]
		if !expected.Exists {
			if exists {
				return fmt.Errorf("unexpected deleted file review %s present on %s", id, device)
			}
			continue
		}
		if !exists {
			return fmt.Errorf("expected file review %s missing on %s", id, device)
		}
		if actualState.FileID != expected.FileID || actualState.Review != expected.Review {
			return fmt.Errorf("file review state mismatch on %s for %s: expected file=%q review=%q, got file=%q review=%q",
				device, id, expected.FileID, expected.Review, actualState.FileID, actualState.Review)
		}
	}

	for id := range actual {
		expected, known := s.testData.ExpectedReviews[id]
		if !known || !expected.Exists {
			return fmt.Errorf("unexpected live file review %s on %s", id, device)
		}
	}
	return nil
}

func (s *ComplexMultiBatchScenario) verifyActualFKIntegrity(
	device string,
	actualUsers map[string]expectedUserState,
	actualPosts map[string]expectedPostState,
	actualFiles map[string]expectedFileState,
	actualReviews map[string]expectedReviewState,
) error {
	for postID, post := range actualPosts {
		if _, ok := actualUsers[post.AuthorID]; !ok {
			return fmt.Errorf("post %s on %s references missing user %s", postID, device, post.AuthorID)
		}
	}
	for reviewID, review := range actualReviews {
		if _, ok := actualFiles[review.FileID]; !ok {
			return fmt.Errorf("file review %s on %s references missing file %s", reviewID, device, review.FileID)
		}
	}
	return nil
}

func (s *ComplexMultiBatchScenario) createBlobBatch(app *MobileApp, label string, groups int) ([]string, []string, error) {
	fileIDs := make([]string, 0, groups*2)
	reviewIDs := make([]string, 0, groups*2)

	for group := 0; group < groups; group++ {
		for variant, suffix := range []string{"A", "B"} {
			fileUUID := uuid.New()
			fileBytes, err := fileUUID.MarshalBinary()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal file UUID for %s group %d: %w", label, group, err)
			}
			fileHex := hex.EncodeToString(fileBytes)
			dataUUID := uuid.New()
			dataHex := strings.ReplaceAll(dataUUID.String(), "-", "")
			fileName := fmt.Sprintf("Blob File %s %s-%d", suffix, label, group)
			if _, err := app.db.Exec(`INSERT INTO files (id, name, data) VALUES (?, ?, x'`+dataHex+`')`, fileBytes, fileName); err != nil {
				return nil, nil, fmt.Errorf("failed to insert file %s %s-%d: %w", suffix, label, group, err)
			}
			s.testData.ExpectedFiles[fileHex] = expectedFileState{Name: fileName, DataHex: dataHex, Exists: true}
			fileIDs = append(fileIDs, fileHex)

			reviewUUID := uuid.New()
			reviewBytes, err := reviewUUID.MarshalBinary()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal review UUID for %s group %d: %w", label, group, err)
			}
			reviewHex := hex.EncodeToString(reviewBytes)
			reviewText := fmt.Sprintf("Blob Review %s %s-%d", suffix, label, group)
			if _, err := app.db.Exec(`INSERT INTO file_reviews (id, file_id, review) VALUES (?, ?, ?)`, reviewBytes, fileBytes, reviewText); err != nil {
				return nil, nil, fmt.Errorf("failed to insert file review %s %s-%d: %w", suffix, label, group, err)
			}
			s.testData.ExpectedReviews[reviewHex] = expectedReviewState{FileID: fileHex, Review: reviewText, Exists: true}
			reviewIDs = append(reviewIDs, reviewHex)

			_ = variant
		}
	}

	return fileIDs, reviewIDs, nil
}

// Cleanup cleans up resources
func (s *ComplexMultiBatchScenario) Cleanup(ctx context.Context) error {
	var err1, err2 error

	if s.app != nil {
		err1 = s.app.close()
	}

	if s.laptopApp != nil {
		err2 = s.laptopApp.close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}
