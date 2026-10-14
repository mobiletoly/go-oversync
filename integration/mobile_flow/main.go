// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/integration/mobile_flow/simulator"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if os.Getenv(processWorkerEnvironment) == "1" {
		if err := runProcessWorker(ctx, os.Stdin); err != nil {
			log.Fatalf("User worker failed: %v", err)
		}
		return
	}
	// Parse command line flags
	var (
		scenarioFlag    = flag.String("scenario", "", "Scenario to run (fresh-install, normal-usage, reinstall, device-replacement, offline-online, conflicts, user-switch, bundle-fk-atomicity, complex-multi-batch, multi-device-sync, multi-device-complex, files-sync, typed-rows, watch-peer-push, watch-server-originated, watch-reconnect-catch-up, watch-default-off, watch-server-unsupported-fallback, watch-idle-cleanup, watch-many-clients-converge, watch-all, all)")
		verifyFlag      = flag.Bool("verify", true, "Enable database verification")
		outputFlag      = flag.String("output", "", "Output report file (JSON)")
		verboseFlag     = flag.Bool("verbose", false, "Enable verbose logging")
		serverFlag      = flag.String("server", "http://127.0.0.1:8080", "Server URL")
		dbFlag          = flag.String("db", "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable", "Database URL for verification")
		jwtSecretFlag   = flag.String("jwt-secret", "", "JWT secret for local token generation (defaults to env JWT_SECRET, else server default)")
		parallelFlag    = flag.Int("parallel", 1, "Total users to simulate (1-500)")
		concurrencyFlag = flag.Int("concurrency", 0, "Maximum concurrently active users (0 = --parallel)")
		parallelMode    = flag.String("parallel-mode", "process", "Parallel user execution: process or goroutine (SDK stress)")
		preserveDBFlag  = flag.Bool("preserve-db", false, "Preserve SQLite database files for manual inspection")
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
	concurrency, err := normalizeSimulationConcurrency(*parallelFlag, *concurrencyFlag)
	if err != nil {
		log.Fatalf("Invalid concurrency: %v", err)
	}
	if *parallelMode != "process" && *parallelMode != "goroutine" {
		log.Fatalf("Invalid parallel mode %q: use process or goroutine", *parallelMode)
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

	// Check if parallel execution is requested
	if *parallelFlag > 1 {
		logger.Info("🚀 Starting parallel multi-user simulation", "users", *parallelFlag, "concurrency", concurrency, "scenario", *scenarioFlag)
		err := runParallelSimulationMode(ctx, cfg, *scenarioFlag, *parallelFlag, concurrency, *parallelMode)
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
	fmt.Println("11. Files Sync          - BLOB primary key sync")
	fmt.Println("12. Typed Rows          - Nullable, numeric, timestamp, and BLOB typed row sync")
	fmt.Println("13. Watch Peer Push     - Watch wake-up after peer push")
	fmt.Println("14. Watch Server Write  - Watch wake-up after ScopeManager write")
	fmt.Println("15. Watch Reconnect     - Watch reconnect catches up after missed push")
	fmt.Println("16. Watch Default Off   - Default clients avoid /sync/watch")
	fmt.Println("17. Watch Unsupported   - Watch-auto falls back to polling")
	fmt.Println("18. Watch Idle Cleanup  - Idle watcher unregisters on cancel")
	fmt.Println("19. Watch Many Clients  - Many watchers converge")
	fmt.Println("20. Run Watch Scenarios - Real watch scenarios over the configured server")
	fmt.Println("21. Run All Scenarios   - Complete test suite")
	fmt.Println("22. Exit")
	fmt.Println()

	for {
		fmt.Print("Select scenario (1-22): ")
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
			runScenario(ctx, sim, "files-sync")
		case "12":
			runScenario(ctx, sim, "typed-rows")
		case "13":
			runScenario(ctx, sim, "watch-peer-push")
		case "14":
			runScenario(ctx, sim, "watch-server-originated")
		case "15":
			runScenario(ctx, sim, "watch-reconnect-catch-up")
		case "16":
			runScenario(ctx, sim, "watch-default-off")
		case "17":
			runScenario(ctx, sim, "watch-server-unsupported-fallback")
		case "18":
			runScenario(ctx, sim, "watch-idle-cleanup")
		case "19":
			runScenario(ctx, sim, "watch-many-clients-converge")
		case "20":
			runScenario(ctx, sim, "watch-all")
		case "21":
			runScenario(ctx, sim, "all")
		case "22":
			fmt.Println("👋 Goodbye!")
			return
		default:
			fmt.Println("❌ Invalid choice. Please select 1-22.")
		}
		fmt.Println()
	}
}

func runScenario(ctx context.Context, sim *simulator.Simulator, scenarioName string) error {
	if scenarioName == "watch-all" {
		scenarios := []string{
			"watch-peer-push",
			"watch-server-originated",
			"watch-reconnect-catch-up",
			"watch-default-off",
			"watch-server-unsupported-fallback",
			"watch-idle-cleanup",
			"watch-many-clients-converge",
		}

		fmt.Printf("🎯 Running all %d watch scenarios...\n\n", len(scenarios))
		for i, scenario := range scenarios {
			fmt.Printf("📋 [%d/%d] Running watch scenario: %s\n", i+1, len(scenarios), scenario)
			if err := sim.RunScenario(ctx, scenario); err != nil {
				return fmt.Errorf("watch scenario %s failed: %w", scenario, err)
			}
			fmt.Printf("✅ [%d/%d] Watch scenario %s completed successfully\n\n", i+1, len(scenarios), scenario)
		}
		return nil
	}

	if scenarioName == "all" {
		scenarios := []string{
			"fresh-install", "normal-usage", "reinstall",
			"device-replacement", "offline-online", "conflicts", "user-switch",
			"bundle-fk-atomicity", "complex-multi-batch", "multi-device-sync", "multi-device-complex",
			"files-sync",
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

func normalizeSimulationConcurrency(numUsers, requested int) (int, error) {
	if numUsers < 1 {
		return 0, fmt.Errorf("total users must be positive")
	}
	if requested < 0 {
		return 0, fmt.Errorf("must be non-negative")
	}
	if requested == 0 {
		return numUsers, nil
	}
	if requested > numUsers {
		return 0, fmt.Errorf("must not exceed total users (%d)", numUsers)
	}
	return requested, nil
}

func runBoundedUserWorkers(numUsers, concurrency int, run func(int)) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for userIndex := range jobs {
				run(userIndex)
			}
		}()
	}
	for userIndex := 1; userIndex <= numUsers; userIndex++ {
		jobs <- userIndex
	}
	close(jobs)
	wg.Wait()
}

// runParallelSimulation runs scenarios for multiple users with bounded active-user concurrency.
func runParallelSimulation(ctx context.Context, baseCfg *config.Config, scenarioName string, numUsers, requestedConcurrency int) error {
	return runParallelSimulationMode(ctx, baseCfg, scenarioName, numUsers, requestedConcurrency, "process")
}

func runParallelSimulationMode(ctx context.Context, baseCfg *config.Config, scenarioName string, numUsers, requestedConcurrency int, mode string) (resultErr error) {
	if mode != "process" && mode != "goroutine" {
		return fmt.Errorf("invalid parallel mode %q: use process or goroutine", mode)
	}
	concurrency, err := normalizeSimulationConcurrency(numUsers, requestedConcurrency)
	if err != nil {
		return fmt.Errorf("invalid simulation concurrency: %w", err)
	}
	startTime := time.Now()
	runID := fmt.Sprintf("%d-%d", os.Getpid(), startTime.UnixNano())
	baseCfg.Logger.Info("🚀 Starting parallel multi-user simulation",
		"users", numUsers,
		"concurrency", concurrency,
		"parallel_mode", mode,
		"scenario", scenarioName,
		"run_id", runID,
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
	var processes *processUserRunner
	if mode == "process" {
		processes, err = newProcessUserRunner()
		if err != nil {
			return err
		}
		defer func() {
			if resultErr == nil {
				resultErr = processes.cleanup()
			} else {
				baseCfg.Logger.Info("Retained worker results", "directory", processes.directory)
			}
		}()
	}

	results := make(chan simulationUserResult, numUsers)
	go func() {
		runBoundedUserWorkers(numUsers, concurrency, func(userIndex int) {
			userID := fmt.Sprintf("parallel-%s-user-%03d", runID, userIndex)
			userStartTime := time.Now()

			deviceID := fmt.Sprintf("parallel-%s-device-%03d", runID, userIndex)
			baseCfg.Logger.Info("⏱️ Starting scenario for user", "user_id", userID, "scenario", scenarioName, "start_time", userStartTime.Format(time.RFC3339))
			var result simulationUserResult
			if processes != nil {
				result = processes.run(ctx, baseCfg, scenarioName, userID, deviceID, userIndex)
			} else {
				result = runSimulationUser(ctx, baseCfg, scenarioName, userID, deviceID, sharedVerifier)
			}
			results <- result
		})
		close(results)
	}()

	successCount, failureCount, totalDuration, errors, userReports := collectParallelResults(ctx, baseCfg, scenarioName, sharedVerifier, results)

	// Final report
	totalTime := time.Since(startTime)
	avgDuration := totalDuration / time.Duration(numUsers)

	baseCfg.Logger.Info("🎉 Parallel simulation completed",
		"total_users", numUsers,
		"concurrency", concurrency,
		"successful", successCount,
		"failed", failureCount,
		"total_time", totalTime,
		"avg_user_time", avgDuration,
		"scenario", scenarioName)

	verificationFailures := finishParallelVerification(baseCfg, scenarioName, numUsers, userReports, totalTime, sharedVerifier)

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
