package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// Simulator represents the mobile app simulator
type Simulator struct {
	config *config.Config
	logger *slog.Logger

	// Current simulation state
	currentApp *MobileApp
	verifier   *DatabaseVerifier
	reporter   *Reporter

	// Scenario management
	scenarios map[string]Scenario
}

// NewSimulator creates a new mobile flow simulator
func NewSimulator(cfg *config.Config) (*Simulator, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Create database verifier if enabled
	var verifier *DatabaseVerifier
	if cfg.EnableVerify {
		var err error
		verifier, err = NewDatabaseVerifier(cfg.DatabaseURL, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create database verifier: %w", err)
		}
	}

	// Create reporter
	reporter := NewReporter(cfg.OutputFile, logger)

	sim := &Simulator{
		config:    cfg,
		logger:    logger,
		verifier:  verifier,
		reporter:  reporter,
		scenarios: make(map[string]Scenario),
	}

	// Register all scenarios
	sim.registerScenarios()

	return sim, nil
}

// NewSimulatorWithUserConfig creates a new simulator with specific user configuration for parallel testing
func NewSimulatorWithUserConfig(cfg *config.Config, userID, sourceID string) (*Simulator, error) {
	sim, err := NewSimulator(cfg)
	if err != nil {
		return nil, err
	}

	// Override user configuration for all scenarios
	sim.SetUserConfig(userID, sourceID)

	return sim, nil
}

// NewSimulatorWithUserConfigAndVerifier creates a new simulator with specific user configuration and shared verifier
func NewSimulatorWithUserConfigAndVerifier(cfg *config.Config, userID, sourceID string, sharedVerifier *DatabaseVerifier) (*Simulator, error) {
	// Disable verification in config since we'll use the shared verifier
	cfgCopy := *cfg
	cfgCopy.EnableVerify = false

	sim, err := NewSimulator(&cfgCopy)
	if err != nil {
		return nil, err
	}

	// Override user configuration for all scenarios
	sim.SetUserConfig(userID, sourceID)

	// Use shared verifier (never owned by this simulator)
	sim.verifier = sharedVerifier

	return sim, nil
}

// Close cleans up simulator resources
func (s *Simulator) Close() error {
	if s.currentApp != nil {
		s.currentApp.Close()
	}

	// Close verifier only if it was created by NewSimulator (not shared)
	// If verifier was passed from outside via NewSimulatorWithUserConfigAndVerifier, don't close it
	if s.verifier != nil && s.config.EnableVerify {
		s.verifier.Close()
	}

	return s.reporter.Close()
}

// RunScenario executes a specific scenario
func (s *Simulator) RunScenario(ctx context.Context, scenarioName string) error {
	scenario, exists := s.scenarios[scenarioName]
	if !exists {
		return fmt.Errorf("unknown scenario: %s", scenarioName)
	}

	if verboseLog {
		s.logger.Info("Starting scenario", "name", scenarioName, "description", scenario.Description())
	}

	// Start timing
	startTime := time.Now()

	// Create scenario report
	report := s.reporter.StartScenario(scenarioName, scenario.Description())

	// Setup phase
	if verboseLog {
		s.logger.Info("Setting up scenario", "name", scenarioName)
	}
	if err := scenario.Setup(ctx); err != nil {
		report.SetError(fmt.Errorf("setup failed: %w", err))
		return err
	}

	// Execute phase
	if verboseLog {
		s.logger.Info("Executing scenario", "name", scenarioName)
	}
	if err := scenario.Execute(ctx); err != nil {
		report.SetError(fmt.Errorf("execution failed: %w", err))
		return err
	}

	// Verify phase
	if s.verifier != nil {
		if verboseLog {
			s.logger.Info("Verifying scenario", "name", scenarioName)
		}
		if err := scenario.Verify(ctx, s.verifier); err != nil {
			report.SetError(fmt.Errorf("verification failed: %w", err))
			return err
		}
	}

	// Cleanup phase
	if verboseLog {
		s.logger.Info("Cleaning up scenario", "name", scenarioName)
	}
	if err := scenario.Cleanup(ctx); err != nil {
		s.logger.Warn("Cleanup failed", "name", scenarioName, "error", err)
		// Don't fail the scenario for cleanup errors
	}

	// Complete report
	duration := time.Since(startTime)
	report.SetDuration(duration)
	report.SetSuccess()

	if verboseLog {
		s.logger.Info("Scenario completed successfully",
			"name", scenarioName,
			"duration", duration.String())
	}

	return nil
}

// registerScenarios registers all available scenarios
func (s *Simulator) registerScenarios() {
	// Use the centralized registry to get all scenarios
	for _, scenarioName := range GetAvailableScenarios() {
		scenario := GetScenario(s, scenarioName)
		if scenario != nil {
			s.scenarios[scenarioName] = scenario
		}
	}
}

// CreateMobileApp creates a new mobile app instance for simulation
func (s *Simulator) CreateMobileApp(scenarioConfig *config.ScenarioConfig) (*MobileApp, error) {
	// Create database file path based on user and source ID
	safeUserID := strings.ReplaceAll(scenarioConfig.UserID, "-", "_")
	safeSourceID := strings.ReplaceAll(scenarioConfig.SourceID, "-", "_")

	var dbFile string
	if scenarioConfig.CleanDatabase {
		// Create a fresh database file with timestamp for scenarios that need clean state
		safeName := strings.ReplaceAll(scenarioConfig.Name, " ", "_")
		safeName = strings.ReplaceAll(safeName, "/", "_")
		dbFile = filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db",
			safeName, safeSourceID, time.Now().UnixNano()))
		s.logger.Debug("Creating clean database", "file", dbFile)
	} else {
		// Use persistent database file for scenario continuity
		dbFile = filepath.Join("/tmp", fmt.Sprintf("mobile_flow_persistent_%s_%s.db",
			safeUserID, safeSourceID))
		s.logger.Debug("Using persistent database for continuity", "file", dbFile)
	}

	// Create oversqlite config - local tables sync to server business schema
	oversqliteConfig := &oversqlite.Config{
		Schema: "business", // Server schema where data will be synced
		Tables: []oversqlite.SyncTable{ // Local SQLite table names
			{TableName: "users"},
			{TableName: "posts"},
			{TableName: "files"},
			{TableName: "file_reviews"},
		},
		UploadLimit:   200,
		DownloadLimit: 1000,
		BackoffMin:    1 * time.Second,
		BackoffMax:    60 * time.Second,
	}

	// Create mobile app
	app, err := NewMobileApp(&MobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        s.config.ServerURL,
		UserID:           scenarioConfig.UserID,
		SourceID:         scenarioConfig.SourceID,
		DeviceName:       scenarioConfig.DeviceName,
		JWTSecret:        s.config.JWTSecret,
		OversqliteConfig: oversqliteConfig,
		PreserveDB:       s.config.PreserveDB,
		Logger:           s.logger,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create mobile app: %w", err)
	}

	// Store current app reference (don't close previous app automatically)
	s.currentApp = app

	return app, nil
}

// GetLogger returns the simulator logger
func (s *Simulator) GetLogger() *slog.Logger {
	return s.logger
}

// GetCurrentApp returns the current mobile app instance
func (s *Simulator) GetCurrentApp() *MobileApp {
	return s.currentApp
}

// GetConfig returns the simulator configuration
func (s *Simulator) GetConfig() *config.Config {
	return s.config
}

// GetReporter returns the scenario reporter
func (s *Simulator) GetReporter() *Reporter {
	return s.reporter
}

// SetUserConfig overrides user configuration for parallel testing
func (s *Simulator) SetUserConfig(userID, sourceID string) {
	// Update scenario configurations to use unique user/source IDs
	for _, scenario := range s.scenarios {
		if configurable, ok := scenario.(interface{ SetUserConfig(string, string) }); ok {
			configurable.SetUserConfig(userID, sourceID)
		}
	}
}
