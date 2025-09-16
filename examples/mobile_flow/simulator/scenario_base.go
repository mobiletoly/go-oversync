package simulator

import (
	"context"
	"fmt"

	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
)

// Scenario represents a test scenario that can be executed
type Scenario interface {
	Name() string
	Description() string
	Setup(ctx context.Context) error
	Execute(ctx context.Context) error
	Verify(ctx context.Context, verifier *DatabaseVerifier) error
	Cleanup(ctx context.Context) error
}

// BaseScenario provides common functionality for all scenarios
type BaseScenario struct {
	simulator *Simulator
	config    *config.ScenarioConfig
	app       *MobileApp
}

// NewBaseScenario creates a new base scenario
func NewBaseScenario(simulator *Simulator, scenarioName string) *BaseScenario {
	return &BaseScenario{
		simulator: simulator,
		config:    config.GetScenarioConfig(scenarioName),
	}
}

// SetUserConfig overrides user configuration for parallel testing
func (bs *BaseScenario) SetUserConfig(userID, sourceID string) {
	if bs.config != nil {
		bs.config.UserID = userID
		bs.config.SourceID = sourceID
	}
}

// Setup performs common setup for scenarios
func (bs *BaseScenario) Setup(ctx context.Context) error {
	// Create mobile app for this scenario
	app, err := bs.simulator.CreateMobileApp(bs.config)
	if err != nil {
		return fmt.Errorf("failed to create mobile app: %w", err)
	}

	bs.app = app

	// Launch the app
	if err := bs.app.OnLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch app: %w", err)
	}

	return nil
}

// Cleanup performs common cleanup for scenarios
func (bs *BaseScenario) Cleanup(ctx context.Context) error {
	if bs.app != nil {
		if err := bs.app.Close(); err != nil {
			return fmt.Errorf("failed to close app: %w", err)
		}
	}
	return nil
}
