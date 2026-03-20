// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/simulator"
)

func main() {
	// Parse command line flags
	var (
		scenarioFlag   = flag.String("scenario", "", "Scenario to run (fresh-install, normal-usage, reinstall, device-replacement, offline-online, conflicts, user-switch, bundle-fk-atomicity, complex-multi-batch, multi-device-sync, multi-device-complex, all)")
		verifyFlag     = flag.Bool("verify", true, "Enable database verification")
		outputFlag     = flag.String("output", "", "Output report file (JSON)")
		verboseFlag    = flag.Bool("verbose", false, "Enable verbose logging")
		serverFlag     = flag.String("server", "http://127.0.0.1:8080", "Server URL")
		dbFlag         = flag.String("db", "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable", "Database URL for verification")
		jwtSecretFlag  = flag.String("jwt-secret", "", "JWT secret for local token generation (defaults to env JWT_SECRET, else server default)")
		parallelFlag   = flag.Int("parallel", 1, "Number of parallel users to simulate (1-100)")
		cleanupFlag    = flag.Bool("cleanup", true, "Clean up server database before starting")
		preserveDBFlag = flag.Bool("preserve-db", false, "Preserve SQLite database files for manual inspection")
	)
	flag.Parse()

	logLevel := slog.LevelInfo
	if *verboseFlag {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	// Ensure libraries using slog.Default() (e.g., oversqlite) share the same handler + level.
	slog.SetDefault(logger)

	if *parallelFlag < 1 || *parallelFlag > 500 {
		log.Fatalf("Parallel users must be between 1 and 500, got: %d", *parallelFlag)
	}

	jwtSecret := *jwtSecretFlag
	if jwtSecret == "" {
		jwtSecret = os.Getenv("JWT_SECRET")
	}
	if jwtSecret == "" {
		jwtSecret = "your-secret-key-change-in-production" // Match nethttp_server default
	}

	cfg := &config.Config{
		ServerURL:    *serverFlag,
		DatabaseURL:  *dbFlag,
		JWTSecret:    jwtSecret,
		EnableVerify: *verifyFlag,
		OutputFile:   *outputFlag,
		PreserveDB:   *preserveDBFlag || *verifyFlag, // Auto-preserve when verification is enabled
		Logger:       logger,
	}

	ctx := context.Background()

	// Clean up server database if requested
	if *cleanupFlag {
		logger.Info("🧹 Cleaning up server database before starting...")
		if err := cleanupServerDatabase(ctx, cfg); err != nil {
			logger.Warn("Failed to cleanup server database", "error", err)
		} else {
			logger.Info("✅ Server database cleaned up successfully")
		}
	}

	// Check if parallel execution is requested
	if *parallelFlag > 1 {
		logger.Info("🚀 Starting parallel multi-user simulation", "users", *parallelFlag, "scenario", *scenarioFlag)
		err := runParallelSimulation(ctx, cfg, *scenarioFlag, *parallelFlag)
		if err != nil {
			log.Fatalf("Parallel simulation failed: %v", err)
		}
		return
	}

	// Single user mode
	sim, err := simulator.NewSimulator(cfg)
	if err != nil {
		log.Fatalf("Failed to create simulator: %v", err)
	}
	defer sim.Close()

	// Interactive mode if no scenario specified
	if *scenarioFlag == "" {
		runInteractiveMode(ctx, sim)
		return
	}

	if err := runScenario(ctx, sim, *scenarioFlag); err != nil {
		log.Fatalf("Scenario failed: %v", err)
	}
	fmt.Println("🎉 Mobile flow simulation completed successfully!")
}

func runInteractiveMode(ctx context.Context, sim *simulator.Simulator) {
	fmt.Println("🚀 Mobile Flow Simulator - Interactive Mode")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Println("Available scenarios:")
	fmt.Println("1. Fresh Install        - Clean app, offline usage, first sync")
	fmt.Println("2. Normal Usage         - Established user, regular sync operations")
	fmt.Println("3. Reinstall/Recovery   - Same user/device, clean DB, data recovery")
	fmt.Println("4. Device Replacement   - Same user, new device, data migration")
	fmt.Println("5. Offline/Online       - Network transitions, pending changes")
	fmt.Println("6. Multi-Device Conflicts - Concurrent edits, conflict resolution")
	fmt.Println("7. User Switch          - Multiple users, database isolation")
	fmt.Println("8. Multi-Device Sync    - Two devices sync scenario with ordering fix")
	fmt.Println("9. Bundle FK Atomicity  - Bundle checkpoints plus self-ref/cycle/cascade FK graphs")
	fmt.Println("10. Multi-Device Complex - Long mixed ops across two devices, converge")
	fmt.Println("11. Run All Scenarios   - Complete test suite")
	fmt.Println("12. Exit")
	fmt.Println()

	for {
		fmt.Print("Select scenario (1-12): ")
		var choice string
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			runScenario(ctx, sim, "fresh-install")
		case "2":
			runScenario(ctx, sim, "normal-usage")
		case "3":
			runScenario(ctx, sim, "reinstall")
		case "4":
			runScenario(ctx, sim, "device-replacement")
		case "5":
			runScenario(ctx, sim, "offline-online")
		case "6":
			runScenario(ctx, sim, "conflicts")
		case "7":
			runScenario(ctx, sim, "user-switch")
		case "8":
			runScenario(ctx, sim, "multi-device-sync")
		case "9":
			runScenario(ctx, sim, "bundle-fk-atomicity")
		case "10":
			runScenario(ctx, sim, "multi-device-complex")
		case "11":
			runScenario(ctx, sim, "all")
		case "12":
			fmt.Println("👋 Goodbye!")
			return
		default:
			fmt.Println("❌ Invalid choice. Please select 1-12.")
		}
		fmt.Println()
	}
}

func runScenario(ctx context.Context, sim *simulator.Simulator, scenarioName string) error {
	if scenarioName == "all" {
		scenarios := []string{
			"fresh-install", "normal-usage", "reinstall",
			"device-replacement", "offline-online", "conflicts", "user-switch",
			"bundle-fk-atomicity", "complex-multi-batch", "multi-device-sync", "multi-device-complex",
		}

		fmt.Printf("🎯 Running all %d scenarios...\n\n", len(scenarios))

		for i, scenario := range scenarios {
			fmt.Printf("📋 [%d/%d] Running scenario: %s\n", i+1, len(scenarios), scenario)
			if err := sim.RunScenario(ctx, scenario); err != nil {
				return fmt.Errorf("scenario %s failed: %w", scenario, err)
			}
			fmt.Printf("✅ [%d/%d] Scenario %s completed successfully\n\n", i+1, len(scenarios), scenario)
		}

		return nil
	}

	fmt.Printf("🎯 Running scenario: %s\n", scenarioName)
	return sim.RunScenario(ctx, scenarioName)
}

// runParallelSimulation runs scenarios for multiple users in parallel
func runParallelSimulation(ctx context.Context, baseCfg *config.Config, scenarioName string, numUsers int) error {
	startTime := time.Now()
	baseCfg.Logger.Info("🚀 Starting parallel multi-user simulation",
		"users", numUsers,
		"scenario", scenarioName,
		"server", baseCfg.ServerURL)

	// Create a shared database verifier to avoid connection pool exhaustion
	var sharedVerifier *simulator.DatabaseVerifier
	if baseCfg.EnableVerify {
		var err error
		sharedVerifier, err = simulator.NewDatabaseVerifier(baseCfg.DatabaseURL, baseCfg.Logger)
		if err != nil {
			return fmt.Errorf("failed to create shared database verifier: %w", err)
		}
		defer sharedVerifier.Close()
	}

	// Channel to collect results
	type userResult struct {
		userID       string
		duration     time.Duration
		err          error
		sqliteDBPath string
	}

	results := make(chan userResult, numUsers)
	var wg sync.WaitGroup

	for i := 1; i <= numUsers; i++ {
		wg.Add(1)
		go func(userIndex int) {
			defer wg.Done()

			userID := fmt.Sprintf("parallel-user-%03d", userIndex)
			userStartTime := time.Now()

			// Create user-specific configuration with shared verifier
			userCfg := &config.Config{
				ServerURL:    baseCfg.ServerURL,
				DatabaseURL:  baseCfg.DatabaseURL,
				JWTSecret:    baseCfg.JWTSecret,
				EnableVerify: false,              // Disable individual verifiers - we'll use the shared one
				OutputFile:   "",                 // No individual output files for parallel users
				PreserveDB:   baseCfg.PreserveDB, // Preserve database files if requested
				Logger:       baseCfg.Logger,
			}

			// Create user-specific simulator with parallel user config
			deviceID := fmt.Sprintf("device-%03d", userIndex)
			sim, err := simulator.NewSimulatorWithUserConfigAndVerifier(userCfg, userID, deviceID, sharedVerifier)
			if err != nil {
				results <- userResult{userID: userID, duration: 0, err: fmt.Errorf("failed to create simulator: %w", err)}
				return
			}
			defer sim.Close()

			// Run the scenario
			baseCfg.Logger.Info("⏱️ Starting scenario for user", "user_id", userID, "scenario", scenarioName, "start_time", userStartTime.Format(time.RFC3339))
			err = runScenarioForUser(ctx, sim, scenarioName, userID)
			duration := time.Since(userStartTime)

			// Get SQLite database path for reporting
			sqliteDBPath := ""
			if app := sim.GetCurrentApp(); app != nil {
				sqliteDBPath = app.GetDatabasePath()
			}

			if err != nil {
				baseCfg.Logger.Error("⏱️ Scenario failed for user", "user_id", userID, "duration", duration, "error", err)
			}

			results <- userResult{userID: userID, duration: duration, err: err, sqliteDBPath: sqliteDBPath}
		}(i)
	}

	// Wait for all simulations to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and report results
	var successCount, failureCount int
	var totalDuration time.Duration
	var errors []string
	var userReports []simulator.UserVerificationReport

	// Create report generator
	reportGenerator := simulator.NewReportGenerator(sharedVerifier)

	for result := range results {
		totalDuration += result.duration
		if result.err != nil {
			failureCount++
			errors = append(errors, fmt.Sprintf("%s: %v", result.userID, result.err))
			baseCfg.Logger.Error("❌ User simulation failed", "user", result.userID, "error", result.err)
		} else {
			successCount++
			baseCfg.Logger.Info("✅ User simulation completed", "user", result.userID, "duration", result.duration)
		}

		// Generate comprehensive report for this user (only if verification is enabled)
		if baseCfg.EnableVerify && sharedVerifier != nil {
			expectedCounts := map[string]int{
				"business.users": 50,  // Expected users per user
				"business.posts": 100, // Expected posts per user
			}

			userReport, err := reportGenerator.GenerateUserReport(ctx, result.userID, scenarioName, result.sqliteDBPath, expectedCounts, result.duration)
			if err != nil {
				baseCfg.Logger.Warn("Failed to generate user report", "user_id", result.userID, "error", err)
				// Create minimal report with error
				userReport = &simulator.UserVerificationReport{
					UserID:             result.userID,
					ScenarioName:       scenarioName,
					SQLiteDatabasePath: result.sqliteDBPath,
					VerificationPassed: false,
					Errors:             []string{fmt.Sprintf("Report generation failed: %v", err)},
					Duration:           result.duration.String(),
				}
			}

			// Fold scenario execution result into the verification summary to keep the report consistent
			// with the parallel runner's success/failure counts.
			if result.err != nil {
				userReport.VerificationPassed = false
				userReport.Errors = append(userReport.Errors, fmt.Sprintf("Scenario failed: %v", result.err))
			}
			userReports = append(userReports, *userReport)
		}
	}

	// Final report
	totalTime := time.Since(startTime)
	avgDuration := totalDuration / time.Duration(numUsers)

	baseCfg.Logger.Info("🎉 Parallel simulation completed",
		"total_users", numUsers,
		"successful", successCount,
		"failed", failureCount,
		"total_time", totalTime,
		"avg_user_time", avgDuration,
		"scenario", scenarioName)

	// Generate comprehensive test report
	var verificationFailures int
	if baseCfg.EnableVerify && len(userReports) > 0 {
		testName := fmt.Sprintf("parallel_%dusers", numUsers)
		completeReport := reportGenerator.GenerateCompleteReport(testName, scenarioName, userReports, totalTime)

		// Save report to file
		if err := reportGenerator.SaveReportToFile(completeReport, ""); err != nil {
			baseCfg.Logger.Warn("Failed to save test report", "error", err)
		}

		// Print summary
		fmt.Printf("\n📊 COMPREHENSIVE TEST REPORT\n")
		fmt.Printf("═══════════════════════════════════════════════════════════════\n")
		fmt.Printf("🧪 Test: %s\n", completeReport.TestName)
		fmt.Printf("📋 Scenario: %s\n", completeReport.ScenarioName)
		fmt.Printf("👥 Users: %d total, %d successful, %d failed\n",
			completeReport.TotalUsers, completeReport.SuccessfulUsers, completeReport.FailedUsers)
		fmt.Printf("⏱️  Duration: %s total, %s average per user\n",
			completeReport.TotalDuration, completeReport.AverageUserTime)

		fmt.Printf("\n📊 POSTGRESQL TOTALS:\n")
		for table, count := range completeReport.Summary.PostgreSQLTotals {
			fmt.Printf("  %s: %d records\n", table, count)
		}

		fmt.Printf("\n📊 SQLITE TOTALS:\n")
		for table, count := range completeReport.Summary.SQLiteTotals {
			fmt.Printf("  %s: %d records\n", table, count)
		}

		fmt.Printf("\n💾 SQLITE DATABASE PATHS:\n")
		for i, path := range completeReport.Summary.DatabasePaths {
			if i < 10 { // Show first 10 paths
				fmt.Printf("  %s\n", path)
			} else if i == 10 {
				fmt.Printf("  ... and %d more databases\n", len(completeReport.Summary.DatabasePaths)-10)
				break
			}
		}

		fmt.Printf("\n📈 Total Records Verified: %d\n", completeReport.Summary.TotalRecords)
		fmt.Printf("═══════════════════════════════════════════════════════════════\n")

		verificationFailures = completeReport.FailedUsers
	}

	if failureCount > 0 {
		baseCfg.Logger.Error("❌ Some simulations failed", "failures", errors)
		return fmt.Errorf("%d out of %d users failed", failureCount, numUsers)
	}

	if verificationFailures > 0 {
		return fmt.Errorf("%d out of %d users failed verification", verificationFailures, numUsers)
	}

	fmt.Printf("\n🎉 All %d users completed successfully!\n", numUsers)
	fmt.Printf("📊 Total time: %v\n", totalTime)
	fmt.Printf("📊 Average time per user: %v\n", avgDuration)

	return nil
}

// runScenarioForUser runs a specific scenario for a single user
func runScenarioForUser(ctx context.Context, sim *simulator.Simulator, scenarioName, userID string) error {
	if scenarioName == "all" {
		// For parallel execution with "all", we should run just one comprehensive scenario
		// Running all scenarios sequentially per user causes interference and is not practical
		// Instead, run the most comprehensive scenario that tests all functionality
		fmt.Printf("🎯 Running comprehensive scenario for user %s: complex-multi-batch\n", userID)
		return sim.RunScenario(ctx, "complex-multi-batch")
	}

	// Run single scenario
	return sim.RunScenario(ctx, scenarioName)
}

// cleanupServerDatabase cleans up sync and business tables on the server
func cleanupServerDatabase(ctx context.Context, cfg *config.Config) error {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin cleanup transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		TRUNCATE TABLE
			business.file_reviews,
			business.files,
			business.posts,
			business.users,
			business.categories,
			business.team_members,
			business.teams
		RESTART IDENTITY CASCADE
	`); err != nil {
		return fmt.Errorf("failed to truncate business tables: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		TRUNCATE TABLE
			sync.bundle_capture_stage,
			sync.applied_pushes,
			sync.bundle_rows,
			sync.bundle_log,
			sync.row_state,
			sync.user_state
		RESTART IDENTITY CASCADE
	`); err != nil {
		return fmt.Errorf("failed to truncate sync tables: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit cleanup transaction: %w", err)
	}
	return nil
}
