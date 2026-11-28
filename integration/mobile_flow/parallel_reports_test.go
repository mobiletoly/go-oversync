package main

import (
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/integration/mobile_flow/simulator"
)

func TestVerifyComplexMultiBatchCounts_CompleteAndPartial(t *testing.T) {
	expected := map[string]int{
		"business.users":        23,
		"business.posts":        447,
		"business.files":        50,
		"business.file_reviews": 50,
	}
	complete := simulator.UserVerificationReport{
		VerificationPassed: true,
		PostgreSQLResults: simulator.PostgreSQLResults{BusinessTableCounts: map[string]int{
			"business.users": 23, "business.posts": 447, "business.files": 50, "business.file_reviews": 50,
		}},
		SQLiteResults: simulator.SQLiteResults{TableCounts: map[string]int{
			"users": 23, "posts": 447, "files": 50, "file_reviews": 50,
		}},
	}
	verifyComplexMultiBatchCounts(&complete, expected)
	if !complete.VerificationPassed || len(complete.Errors) != 0 {
		t.Fatalf("complete user failed verification: %+v", complete)
	}

	partial := complete
	partial.PostgreSQLResults.BusinessTableCounts = map[string]int{
		"business.users": 5, "business.posts": 100, "business.files": 10, "business.file_reviews": 10,
	}
	partial.SQLiteResults.TableCounts = map[string]int{
		"users": 5, "posts": 100, "files": 10, "file_reviews": 10,
	}
	verifyComplexMultiBatchCounts(&partial, expected)
	if partial.VerificationPassed || len(partial.Errors) != 8 {
		t.Fatalf("partial user incorrectly passed: %+v", partial)
	}
	if !strings.Contains(strings.Join(partial.Errors, "\n"), "business.file_reviews") {
		t.Fatalf("blob-backed table mismatch missing: %v", partial.Errors)
	}

	missing := complete
	missing.SQLiteResults.TableCounts = map[string]int{"users": 23, "posts": 447, "files": 50}
	missing.SQLiteResults.PendingChanges = 1
	verifyComplexMultiBatchCounts(&missing, expected)
	if missing.VerificationPassed || len(missing.Errors) != 2 {
		t.Fatalf("missing table or pending change incorrectly passed: %+v", missing)
	}

	complete.UserID, partial.UserID = "complete", "partial"
	report := simulator.NewReportGenerator(nil).GenerateCompleteReport("load", "complex-multi-batch", []simulator.UserVerificationReport{complete, partial}, time.Second)
	if report.SuccessfulUsers != 1 || report.FailedUsers != 1 || report.Summary.TotalRecords != 695 {
		t.Fatalf("aggregate should count partial rows but fail that user: %+v", report)
	}
}
