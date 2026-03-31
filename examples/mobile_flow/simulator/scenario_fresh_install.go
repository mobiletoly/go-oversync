package simulator

import (
	"context"
	"fmt"
)

// FreshInstallScenario simulates a fresh app installation
type FreshInstallScenario struct {
	*BaseScenario
}

// NewFreshInstallScenario creates a new fresh install scenario
func NewFreshInstallScenario(simulator *Simulator) Scenario {
	return &FreshInstallScenario{
		BaseScenario: NewBaseScenario(simulator, "fresh-install"),
	}
}

func (s *FreshInstallScenario) Name() string {
	return s.config.Name
}

func (s *FreshInstallScenario) Description() string {
	return s.config.Description
}

func (s *FreshInstallScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("🎯 Executing Fresh Install Scenario")

	// Phase 1: Offline usage (guest mode)
	logger.Info("📱 Phase 1: Offline usage (guest mode)")
	s.app.currentUI().SimulateNetworkChange(false) // Go offline

	// Create some data while offline
	userIDs := make([]string, 0, s.config.InitialRecords)
	for i := 0; i < s.config.InitialRecords; i++ {
		userID, err := s.app.createUser(
			fmt.Sprintf("User %d", i+1),
			fmt.Sprintf("user%d@example.com", i+1),
		)
		if err != nil {
			return fmt.Errorf("failed to create user %d: %w", i+1, err)
		}
		userIDs = append(userIDs, userID)

		// Create a post for this user
		_, err = s.app.createPost(
			fmt.Sprintf("Post by User %d", i+1),
			fmt.Sprintf("This is content from user %d created offline", i+1),
			userID,
		)
		if err != nil {
			return fmt.Errorf("failed to create post for user %d: %w", i+1, err)
		}
	}

	// Perform some updates while offline
	for i := 0; i < s.config.UpdateOperations && i < len(userIDs); i++ {
		err := s.app.updateUser(
			userIDs[i],
			fmt.Sprintf("Updated User %d", i+1),
			fmt.Sprintf("updated.user%d@example.com", i+1),
		)
		if err != nil {
			return fmt.Errorf("failed to update user %d: %w", i+1, err)
		}
	}

	// Perform some deletes while offline (delete posts first to avoid FK constraint)
	for i := 0; i < s.config.DeleteOperations && i < len(userIDs); i++ {
		userToDelete := userIDs[len(userIDs)-1-i]

		// First delete posts by this user to avoid foreign key constraint
		_, err := s.app.database().Exec(`DELETE FROM posts WHERE author_id = ?`, userToDelete)
		if err != nil {
			return fmt.Errorf("failed to delete posts for user: %w", err)
		}

		// Then delete the user
		err = s.app.deleteUser(userToDelete)
		if err != nil {
			return fmt.Errorf("failed to delete user: %w", err)
		}
	}

	logger.Info("✅ Phase 1 complete: Created data offline",
		"users", s.config.InitialRecords,
		"updates", s.config.UpdateOperations,
		"deletes", s.config.DeleteOperations)

	// Phase 2: Sign in and sync
	logger.Info("📱 Phase 2: Sign in and first sync")

	// Go back online first
	s.app.currentUI().SimulateNetworkChange(true)

	// Sign in (this will bootstrap the client)
	if err := s.app.onSignIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in: %w", err)
	}

	// Use explicit sync calls here so the scenario remains deterministic even if
	// the background downloader wakes up before the uploader.
	s.app.stopSync()
	logger.Info("⏳ Uploading initial dirty set explicitly...")
	if err := s.app.pushPending(ctx); err != nil {
		return fmt.Errorf("failed to push initial dirty set: %w", err)
	}

	logger.Info("✅ Phase 2 complete: First sync finished")

	// Phase 3: Verify sync state
	logger.Info("📱 Phase 3: Verify sync state")

	// Check UI state
	uiState := s.app.currentUI().GetUIState()
	logger.Info("UI State",
		"banner", uiState.Banner,
		"pending", uiState.PendingBadge,
		"online", uiState.IsOnline,
		"conflicts", len(uiState.Conflicts))

	if uiState.PendingBadge > 0 {
		logger.Warn("Still have pending changes", "count", uiState.PendingBadge)
	}

	logger.Info("✅ Fresh Install Scenario completed successfully")

	return nil
}

func (s *FreshInstallScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil // Verification disabled
	}

	logger := s.simulator.GetLogger()
	logger.Info("🔍 Verifying Fresh Install Scenario")

	// Verify that data was synced to server
	expectedUsers := s.config.InitialRecords - s.config.DeleteOperations
	expectedPosts := s.config.InitialRecords - s.config.DeleteOperations // Posts are deleted when users are deleted

	userCount, err := verifier.CountUserRecords(ctx, "business.users", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count users for user %s: %w", s.config.UserID, err)
	}

	postCount, err := verifier.CountUserRecords(ctx, "business.posts", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count posts for user %s: %w", s.config.UserID, err)
	}

	logger.Info("Server record counts for user",
		"user_id", s.config.UserID,
		"users", userCount,
		"expected_users", expectedUsers,
		"posts", postCount,
		"expected_posts", expectedPosts)

	if userCount < expectedUsers {
		return fmt.Errorf("expected at least %d users for user %s, got %d", expectedUsers, s.config.UserID, userCount)
	}

	if postCount < expectedPosts {
		return fmt.Errorf("expected at least %d posts for user %s, got %d", expectedPosts, s.config.UserID, postCount)
	}

	// Verify sync metadata
	changeCount, err := verifier.CountSyncChanges(ctx, s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count sync changes: %w", err)
	}

	logger.Info("Sync metadata", "changes", changeCount)

	if changeCount == 0 {
		return fmt.Errorf("expected sync changes to be recorded")
	}

	logger.Info("✅ Fresh Install Scenario verification passed")

	return nil
}
