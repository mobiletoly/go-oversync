package simulator

import (
	"context"
	"fmt"
)

// NormalUsageScenario simulates normal app usage with established sync
type NormalUsageScenario struct {
	*BaseScenario
}

// NewNormalUsageScenario creates a new normal usage scenario
func NewNormalUsageScenario(simulator *Simulator) Scenario {
	return &NormalUsageScenario{
		BaseScenario: NewBaseScenario(simulator, "normal-usage"),
	}
}

func (s *NormalUsageScenario) Name() string {
	return s.config.Name
}

func (s *NormalUsageScenario) Description() string {
	return s.config.Description
}

func (s *NormalUsageScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()

	logger.Info("🎯 Executing Normal Usage Scenario")

	// Sign in immediately (established user)
	if err := s.app.OnSignIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in: %w", err)
	}
	s.app.StopSync()

	// Simulate normal CRUD operations with immediate sync
	logger.Info("📱 Performing normal CRUD operations")

	userIDs := make([]string, 0, s.config.InitialRecords)

	// Create records
	for i := 0; i < s.config.InitialRecords; i++ {
		userID, err := s.app.CreateUser(
			fmt.Sprintf("Normal User %d", i+1),
			fmt.Sprintf("normal.user%d@example.com", i+1),
		)
		if err != nil {
			return fmt.Errorf("failed to create user %d: %w", i+1, err)
		}
		userIDs = append(userIDs, userID)
	}

	// Update records
	for i := 0; i < s.config.UpdateOperations && i < len(userIDs); i++ {
		err := s.app.UpdateUser(
			userIDs[i],
			fmt.Sprintf("Updated Normal User %d", i+1),
			fmt.Sprintf("updated.normal.user%d@example.com", i+1),
		)
		if err != nil {
			return fmt.Errorf("failed to update user %d: %w", i+1, err)
		}
	}

	// Delete records
	for i := 0; i < s.config.DeleteOperations && i < len(userIDs); i++ {
		err := s.app.DeleteUser(userIDs[len(userIDs)-1-i])
		if err != nil {
			return fmt.Errorf("failed to delete user: %w", err)
		}
	}

	if err := s.app.PushPending(ctx); err != nil {
		return fmt.Errorf("failed to push normal usage changes: %w", err)
	}

	logger.Info("✅ Normal Usage Scenario completed successfully")

	return nil
}

func (s *NormalUsageScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}

	logger := s.simulator.GetLogger()
	logger.Info("🔍 Verifying Normal Usage Scenario")

	// Similar verification to fresh install
	expectedUsers := s.config.InitialRecords - s.config.DeleteOperations

	userCount, err := verifier.CountUserRecords(ctx, "business.users", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to count users for user %s: %w", s.config.UserID, err)
	}

	logger.Info("Server record counts for user", "user_id", s.config.UserID, "users", userCount, "expected", expectedUsers)

	if userCount < expectedUsers {
		return fmt.Errorf("expected at least %d users for user %s, got %d", expectedUsers, s.config.UserID, userCount)
	}

	logger.Info("✅ Normal Usage Scenario verification passed")

	return nil
}
