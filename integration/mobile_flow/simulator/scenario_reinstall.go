package simulator

import (
	"context"
	"fmt"
)

// ReinstallScenario simulates app reinstallation with data restoration.
type ReinstallScenario struct {
	*BaseScenario
	recordIDs []string
}

// NewReinstallScenario creates a new reinstall scenario.
func NewReinstallScenario(simulator *Simulator) Scenario {
	return &ReinstallScenario{
		BaseScenario: NewBaseScenario(simulator, "reinstall"),
	}
}

func (s *ReinstallScenario) Name() string {
	return s.config.Name
}

func (s *ReinstallScenario) Description() string {
	return s.config.Description
}

func (s *ReinstallScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Executing Reinstall Scenario")

	if err := s.app.signIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in original app: %w", err)
	}

	s.recordIDs = s.recordIDs[:0]
	for idx := 0; idx < 3; idx++ {
		recordID, err := s.app.createUser(
			fmt.Sprintf("Reinstall User %d", idx+1),
			fmt.Sprintf("reinstall-user-%d@example.com", idx+1),
		)
		if err != nil {
			return fmt.Errorf("failed to create reinstall seed user %d: %w", idx+1, err)
		}
		s.recordIDs = append(s.recordIDs, recordID)
	}

	if err := s.app.pushPending(ctx); err != nil {
		return fmt.Errorf("failed to push seeded reinstall data: %w", err)
	}
	if _, _, err := s.app.pullToStable(ctx); err != nil {
		return fmt.Errorf("failed to stabilize original app after seed push: %w", err)
	}

	if err := s.app.close(); err != nil {
		return fmt.Errorf("failed to close original app before reinstall: %w", err)
	}
	s.app = nil

	reinstalled, err := s.simulator.CreateMobileApp(s.config)
	if err != nil {
		return fmt.Errorf("failed to create reinstalled app: %w", err)
	}
	s.app = reinstalled
	s.simulator.currentApp = s.app

	if err := s.app.onLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch reinstalled app: %w", err)
	}
	if err := s.app.signIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in reinstalled app: %w", err)
	}
	if err := s.app.rebuildKeepSource(ctx); err != nil {
		return fmt.Errorf("failed to rebuild after reinstall: %w", err)
	}

	count, err := s.app.userCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to count restored users after reinstall: %w", err)
	}
	if count != len(s.recordIDs) {
		return fmt.Errorf("expected %d restored users after reinstall, got %d", len(s.recordIDs), count)
	}
	for _, recordID := range s.recordIDs {
		ok, err := s.app.hasUser(ctx, recordID)
		if err != nil {
			return fmt.Errorf("failed to verify restored user %s: %w", recordID, err)
		}
		if !ok {
			return fmt.Errorf("restored app is missing expected user %s", recordID)
		}
	}

	logger.Info("✅ Reinstall Scenario completed successfully", "restored_users", len(s.recordIDs))
	return nil
}

func (s *ReinstallScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	serverCount, err := verifier.CountUserRecords(ctx, "business.users", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to verify reinstall server state: %w", err)
	}
	if serverCount != len(s.recordIDs) {
		return fmt.Errorf("expected %d server users after reinstall, got %d", len(s.recordIDs), serverCount)
	}
	return nil
}
