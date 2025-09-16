package config

import (
	"log/slog"
	"time"
)

// Config holds all configuration for the mobile flow simulator
type Config struct {
	// Server connection
	ServerURL   string
	DatabaseURL string // For verification queries
	JWTSecret   string // Server JWT secret

	// Simulation settings
	EnableVerify bool
	OutputFile   string
	PreserveDB   bool // Preserve SQLite database files for manual inspection

	// Timing settings (realistic mobile app behavior)
	SyncInterval    time.Duration
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	JitterRange     time.Duration
	UploadBatchSize int
	DownloadLimit   int

	// Database settings
	SQLiteFile  string
	WALMode     bool
	BusyTimeout time.Duration

	// Authentication settings
	TokenExpiry time.Duration

	// Logging
	Logger *slog.Logger
}

// DefaultConfig returns a configuration with sensible defaults for mobile simulation
func DefaultConfig() *Config {
	return &Config{
		ServerURL:    "http://localhost:8080",
		DatabaseURL:  "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable",
		EnableVerify: true,

		// Realistic mobile timing
		SyncInterval:    2 * time.Second,
		BackoffBase:     1 * time.Second,
		BackoffMax:      60 * time.Second,
		JitterRange:     300 * time.Millisecond,
		UploadBatchSize: 200,
		DownloadLimit:   1000,

		// SQLite settings matching mobile best practices
		SQLiteFile:  ":memory:", // Will be overridden per scenario
		WALMode:     true,
		BusyTimeout: 5 * time.Second,

		// Auth settings (match server default)
		JWTSecret:   "your-secret-key-change-in-production",
		TokenExpiry: 24 * time.Hour,

		Logger: slog.Default(),
	}
}

// ScenarioConfig holds configuration specific to a scenario
type ScenarioConfig struct {
	Name        string
	Description string

	// User and device settings
	UserID     string
	SourceID   string
	DeviceName string

	// Scenario-specific settings
	OfflineMode   bool
	ConflictMode  bool
	MultiDevice   bool
	CleanDatabase bool

	// Data generation
	InitialRecords   int
	UpdateOperations int
	DeleteOperations int

	// Timing overrides
	CustomTiming map[string]time.Duration
}

// GetScenarioConfig returns configuration for a specific scenario
func GetScenarioConfig(scenarioName string) *ScenarioConfig {
	configs := map[string]*ScenarioConfig{
		"fresh-install": {
			Name:             "Fresh Install",
			Description:      "Simulates a fresh app installation with offline usage and first sync",
			UserID:           "user-fresh-install",
			SourceID:         "device-fresh-001",
			DeviceName:       "iPhone Fresh",
			OfflineMode:      true,
			CleanDatabase:    true,
			InitialRecords:   5,
			UpdateOperations: 3,
			DeleteOperations: 1,
		},
		"normal-usage": {
			Name:             "Normal Usage",
			Description:      "Simulates normal app usage with established sync",
			UserID:           "user-normal-usage",
			SourceID:         "device-normal-001",
			DeviceName:       "iPhone Normal",
			OfflineMode:      false,
			CleanDatabase:    false,
			InitialRecords:   10,
			UpdateOperations: 8,
			DeleteOperations: 2,
		},
		"reinstall": {
			Name:             "Reinstall/Recovery",
			Description:      "Simulates app reinstall with data recovery using include_self=true",
			UserID:           "user-reinstall",
			SourceID:         "device-reinstall-001",
			DeviceName:       "iPhone Reinstall",
			OfflineMode:      false,
			CleanDatabase:    true,
			InitialRecords:   15,
			UpdateOperations: 5,
			DeleteOperations: 2,
		},
		"device-replacement": {
			Name:             "Device Replacement",
			Description:      "Simulates user switching to a new device",
			UserID:           "user-device-replacement",
			SourceID:         "device-replacement-002", // Different device ID
			DeviceName:       "iPhone New",
			OfflineMode:      false,
			CleanDatabase:    true,
			InitialRecords:   12,
			UpdateOperations: 4,
			DeleteOperations: 1,
		},
		"offline-online": {
			Name:             "Offline/Online Transitions",
			Description:      "Simulates network connectivity changes and pending sync",
			UserID:           "user-offline-online",
			SourceID:         "device-offline-001",
			DeviceName:       "iPhone Offline",
			OfflineMode:      true,
			CleanDatabase:    true, // Use clean database for this scenario
			InitialRecords:   8,
			UpdateOperations: 12,
			DeleteOperations: 3,
		},
		"conflicts": {
			Name:             "Multi-Device Conflicts",
			Description:      "Simulates concurrent edits from multiple devices",
			UserID:           "user-conflicts",
			SourceID:         "device-conflicts-001",
			DeviceName:       "iPhone Conflicts",
			OfflineMode:      false,
			ConflictMode:     true,
			MultiDevice:      true,
			CleanDatabase:    true, // Use clean database for this scenario
			InitialRecords:   6,
			UpdateOperations: 10,
			DeleteOperations: 2,
		},
		"user-switch": {
			Name:             "User Switch",
			Description:      "Simulates multiple users using the same device",
			UserID:           "user-switch-a",
			SourceID:         "device-switch-001",
			DeviceName:       "Shared iPhone",
			OfflineMode:      false,
			CleanDatabase:    true,
			InitialRecords:   4,
			UpdateOperations: 6,
			DeleteOperations: 1,
		},
		"fk-batch-retry": {
			Name:             "FK Batch Retry",
			Description:      "Tests batch upload FK constraint handling and retry mechanism",
			UserID:           "fk-test-user",
			SourceID:         "device-fk-test",
			DeviceName:       "iPhone FK Test",
			OfflineMode:      false,
			CleanDatabase:    true,
			InitialRecords:   50, // 50 users
			UpdateOperations: 0,  // No updates needed
			DeleteOperations: 0,  // No deletes needed
		},
		"complex-multi-batch": {
			Name:             "Complex Multi-Batch",
			Description:      "Comprehensive stress test with complex offline operations, multi-batch uploads, FK constraints, and full hydration validation",
			UserID:           "user-complex-multi-batch",
			SourceID:         "device-complex-001",
			DeviceName:       "iPhone Complex Test",
			OfflineMode:      true,
			CleanDatabase:    true,
			InitialRecords:   60, // 60 users (forces multiple batches)
			UpdateOperations: 30, // 30 users updated multiple times each
			DeleteOperations: 10, // 10 users deleted
		},
		"files-sync": {
			Name:             "Files Sync",
			Description:      "Tests synchronization of files and file_reviews tables with BLOB primary keys and foreign key relationships",
			UserID:           "user-files-sync",
			SourceID:         "device-files-001",
			DeviceName:       "iPhone Files Test",
			OfflineMode:      false,
			CleanDatabase:    true,
			InitialRecords:   4, // 4 files
			UpdateOperations: 0, // No updates needed for this test
			DeleteOperations: 0, // No deletes needed for this test
		},
	}

	config := configs[scenarioName]
	if config == nil {
		// Return default config for unknown scenarios
		return &ScenarioConfig{
			Name:        "Unknown",
			Description: "Unknown scenario",
			UserID:      "user-unknown",
			SourceID:    "device-unknown-001",
			DeviceName:  "Unknown Device",
		}
	}

	return config
}
