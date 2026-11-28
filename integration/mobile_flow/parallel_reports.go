package main

import (
	"context"
	"fmt"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/integration/mobile_flow/simulator"
)

func collectParallelResults(ctx context.Context, baseCfg *config.Config, scenarioName string, sharedVerifier *simulator.DatabaseVerifier, results <-chan simulationUserResult) (int, int, time.Duration, []string, []simulator.UserVerificationReport) {
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
			// For other scenarios, these keys select tables to include in the report.
			// The complex scenario also has a fixed final state that we can verify.
			expectedCounts := map[string]int{"business.users": 0, "business.posts": 0}
			if scenarioName == "complex-multi-batch" {
				expectedCounts = map[string]int{
					"business.users":        23,
					"business.posts":        447,
					"business.files":        50,
					"business.file_reviews": 50,
				}
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
			if scenarioName == "complex-multi-batch" {
				verifyComplexMultiBatchCounts(userReport, expectedCounts)
			}
			userReports = append(userReports, *userReport)
		}
	}

	return successCount, failureCount, totalDuration, errors, userReports
}

func verifyComplexMultiBatchCounts(report *simulator.UserVerificationReport, expected map[string]int) {
	for table, want := range expected {
		postgresCount, postgresPresent := report.PostgreSQLResults.BusinessTableCounts[table]
		if !postgresPresent || postgresCount != want {
			report.VerificationPassed = false
			report.Errors = append(report.Errors, fmt.Sprintf("PostgreSQL %s: expected %d, got %d (present=%t)", table, want, postgresCount, postgresPresent))
		}
		sqliteTable := table[len("business."):]
		sqliteCount, sqlitePresent := report.SQLiteResults.TableCounts[sqliteTable]
		if !sqlitePresent || sqliteCount != want {
			report.VerificationPassed = false
			report.Errors = append(report.Errors, fmt.Sprintf("SQLite %s: expected %d, got %d (present=%t)", sqliteTable, want, sqliteCount, sqlitePresent))
		}
	}
	if report.SQLiteResults.PendingChanges != 0 {
		report.VerificationPassed = false
		report.Errors = append(report.Errors, fmt.Sprintf("SQLite has %d pending changes", report.SQLiteResults.PendingChanges))
	}
}

func finishParallelVerification(baseCfg *config.Config, scenarioName string, numUsers int, userReports []simulator.UserVerificationReport, totalTime time.Duration, sharedVerifier *simulator.DatabaseVerifier) int {
	reportGenerator := simulator.NewReportGenerator(sharedVerifier)
	// Generate comprehensive test report
	var verificationFailures int
	if baseCfg.EnableVerify && len(userReports) > 0 {
		testName := fmt.Sprintf("parallel_%dusers", numUsers)
		completeReport := reportGenerator.GenerateCompleteReport(testName, scenarioName, userReports, totalTime)

		// Save report to file
		if err := reportGenerator.SaveReportToFile(completeReport, ""); err != nil {
			baseCfg.Logger.Warn("Failed to save test report", "error", err)
		}

		printParallelVerificationReport(completeReport)

		verificationFailures = completeReport.FailedUsers
	}

	return verificationFailures
}

func printParallelVerificationReport(completeReport *simulator.TestReport) {
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

	fmt.Printf("\n📈 PostgreSQL business records counted (including incomplete users): %d\n", completeReport.Summary.TotalRecords)
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
}
