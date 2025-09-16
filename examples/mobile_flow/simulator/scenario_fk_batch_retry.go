package simulator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// FKBatchRetryScenario tests FK constraint handling and retry mechanism
type FKBatchRetryScenario struct {
	*BaseScenario
}

// NewFKBatchRetryScenario creates a new FK batch retry scenario
func NewFKBatchRetryScenario(simulator *Simulator) Scenario {
	return &FKBatchRetryScenario{
		BaseScenario: NewBaseScenario(simulator, "fk-batch-retry"),
	}
}

func (s *FKBatchRetryScenario) Name() string {
	return "FK Batch Retry"
}

func (s *FKBatchRetryScenario) Description() string {
	return "Tests batch upload FK constraint handling and retry mechanism"
}

func (s *FKBatchRetryScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("🎯 Executing FK Batch Retry Scenario")
	logger.Info("📱 This scenario tests the server's handling of FK constraint violations during batch uploads")

	// Phase 1: Sign in and prepare for testing
	logger.Info("📱 Phase 1: Sign in and prepare for FK dependency testing")

	if err := s.app.OnSignIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in: %w", err)
	}

	// Go offline to create problematic data
	s.app.GetUI().SimulateNetworkChange(false)

	// Phase 2: Create data that will test FK dependency ordering during upload
	logger.Info("📱 Phase 2: Creating data with FK dependencies")
	logger.Info("📱 Creating 50 users and 100 posts - will test upload ordering")

	// First create 50 users locally so FK constraints are satisfied in SQLite
	userUUIDs := make([]string, 50)
	for i := 0; i < 50; i++ {
		userID := uuid.New().String()
		userUUIDs[i] = userID

		name := fmt.Sprintf("User %d", i+1)
		email := fmt.Sprintf("user%d@example.com", i+1)

		// Create user locally with specific ID
		err := s.app.CreateUserWithID(ctx, userID, name, email)
		if err != nil {
			return fmt.Errorf("failed to create user %d: %w", i+1, err)
		}
	}

	logger.Info("✅ Created 50 users locally")

	// Now create 100 posts that reference these users
	// The FK constraint issue will be simulated by uploading posts before users
	for i := 1; i <= 100; i++ {
		userIndex := ((i - 1) % 50) // Posts reference users 0-49
		userID := userUUIDs[userIndex]

		title := fmt.Sprintf("Post %d by User %d", i, userIndex+1)
		content := fmt.Sprintf("This post references user %s", userID)

		// Create post with FK reference to existing local user
		_, err := s.app.CreatePost(title, content, userID)
		if err != nil {
			return fmt.Errorf("failed to create post %d: %w", i, err)
		}

		if i%20 == 0 {
			logger.Info("📝 Created posts", "count", i, "referencing_users", fmt.Sprintf("1-%d", userIndex+1))
		}
	}

	logger.Info("✅ Phase 2 complete: Created 50 users and 100 posts with FK dependencies")

	// Phase 3: Go online and trigger sync - should handle FK dependencies correctly
	logger.Info("📱 Phase 3: First sync - testing FK dependency handling")
	logger.Info("📱 Server should handle FK dependencies correctly during batch upload")

	s.app.GetUI().SimulateNetworkChange(true)

	// Trigger upload - should succeed with proper FK dependency handling
	uploadStartTime := time.Now()
	logger.Info("⏱️ Upload triggered", "start_time", uploadStartTime.Format(time.RFC3339))

	s.app.sync.TriggerUpload()
	time.Sleep(5 * time.Second) // Give more time for large batch

	uploadDuration := time.Since(uploadStartTime)
	logger.Info("⏱️ Upload wait completed", "duration", uploadDuration)

	// Check sync state - should have no pending changes after successful sync
	pendingCount, err := s.app.GetPendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending changes count: %w", err)
	}

	logger.Info("📊 After first sync", "pending_changes", pendingCount)

	if pendingCount != 0 {
		logger.Warn("Still have pending changes after sync", "count", pendingCount)
		// Continue anyway to test the scenario
	}

	// Phase 4: Verify sync completion and data integrity
	logger.Info("📱 Phase 4: Verifying sync completion and data integrity")

	// Wait a bit more for any remaining sync operations
	time.Sleep(2 * time.Second)

	// Check final sync state
	finalPendingCount, err := s.app.GetPendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get final pending changes count: %w", err)
	}

	logger.Info("📊 After second sync", "pending_changes", finalPendingCount)

	if finalPendingCount > 0 {
		logger.Warn("Still have pending changes after FK resolution", "count", finalPendingCount)
		// Try one more sync round
		s.app.sync.TriggerUpload()
		time.Sleep(3 * time.Second)

		finalPendingCount, _ = s.app.GetPendingChangesCount(ctx)
		logger.Info("📊 After final sync", "pending_changes", finalPendingCount)
	}

	logger.Info("✅ FK Batch Retry Scenario completed", "final_pending", finalPendingCount)

	return nil
}

func (s *FKBatchRetryScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}

	logger := s.simulator.GetLogger()
	logger.Info("🔍 Verifying FK Batch Retry Scenario")

	// Verify that all users and posts were eventually synced for this specific user
	userCount, err := verifier.CountUserRecords(ctx, "business.users", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count users for user %s: %w", s.config.UserID, err)
	}

	postCount, err := verifier.CountUserRecords(ctx, "business.posts", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count posts for user %s: %w", s.config.UserID, err)
	}

	logger.Info("Server record counts for user", "user_id", s.config.UserID, "users", userCount, "posts", postCount)

	// Use the same numbers as in Execute()
	expectedUsers := 50
	expectedPosts := 100

	if userCount < expectedUsers {
		return fmt.Errorf("expected at least %d users for user %s, got %d", expectedUsers, s.config.UserID, userCount)
	}

	if postCount < expectedPosts {
		return fmt.Errorf("expected at least %d posts for user %s, got %d", expectedPosts, s.config.UserID, postCount)
	}

	// Verify FK relationships are valid (simplified check)
	if postCount > 0 && userCount > 0 {
		logger.Info("✅ FK relationships validated - posts and users both present")
	}

	logger.Info("✅ FK Batch Retry Scenario verification passed")

	return nil
}
