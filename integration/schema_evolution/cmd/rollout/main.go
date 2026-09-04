// Command rollout executes fixed application-owned phases under closed authority.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	schemaevolution "github.com/mobiletoly/go-oversync/integration/schema_evolution"
)

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func run() (int, error) {
	flags := flag.NewFlagSet("rollout", flag.ContinueOnError)
	planPath := flags.String("plan", "", "bounded JSON operation and target selection")
	reportPath := flags.String("report", "", "reviewed JSON report")
	operation := flags.String("operation-id", "", "immutable operation ID")
	format := flags.String("format", "json", "json or text report")
	if len(os.Args) < 2 {
		return 1, fmt.Errorf("usage: rollout report|apply|status|resume --plan artifact.json --report report.json --operation-id ID")
	}
	mode := os.Args[1]
	if mode != "report" && mode != "apply" && mode != "status" && mode != "resume" {
		return 1, fmt.Errorf("unknown rollout command")
	}
	if err := flags.Parse(os.Args[2:]); err != nil {
		return 1, err
	}
	if flags.NArg() != 0 || (*format != "json" && *format != "text") {
		return 1, fmt.Errorf("invalid rollout arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return 1, fmt.Errorf("DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return 1, fmt.Errorf("invalid DATABASE_URL")
	}
	defer pool.Close()
	output := json.NewEncoder(os.Stdout)
	output.SetIndent("", "  ")
	if mode == "status" {
		p, r, result, err := schemaevolution.RolloutStatus(ctx, pool, *operation)
		if err != nil {
			return 1, err
		}
		return 0, output.Encode(struct {
			Plan   schemaevolution.RolloutPlan
			Report schemaevolution.RolloutReport
			Result schemaevolution.RolloutResult
		}{p, r, result})
	}
	input, err := loadExecution(ctx, pool, mode, *planPath, *reportPath, *operation)
	if err != nil {
		return 1, err
	}
	if mode == "report" {
		return emitReport(ctx, pool, output, input, *format)
	}
	result, err := schemaevolution.ExecuteTableRollout(ctx, pool, input, mode == "resume")
	if err != nil {
		return 1, err
	}
	return 0, output.Encode(result)
}

func loadExecution(ctx context.Context, pool *pgxpool.Pool, mode, planPath, reportPath, operation string) (schemaevolution.RolloutExecution, error) {
	if mode == "resume" {
		return schemaevolution.ResumeRolloutExecution(ctx, pool, operation)
	}
	var input schemaevolution.RolloutExecution
	if err := readInput(planPath, &input); err != nil {
		return input, err
	}
	input.RetainTargetSelection()
	if mode == "apply" {
		return input, readInput(reportPath, &input.Report)
	}
	return input, nil
}

func emitReport(ctx context.Context, pool *pgxpool.Pool, output *json.Encoder, input schemaevolution.RolloutExecution, format string) (int, error) {
	report, err := schemaevolution.RolloutReportReadOnly(ctx, pool, input.Plan)
	if err != nil {
		return 1, err
	}
	if format == "text" {
		fmt.Printf("Version: %d\nPlan: %s\nState: %s\nComplete: %t\nDatabase: %s\nObserved: %s\nDeployment: %d\nLedger: %d\nCatalog: %d %s\nTargets: %v\nPolicy eligible: %t\nBackup: %s\nRollback holds: %v\nCounts: %+v\nRetained floors: %+v\nEstimated rows: %d\nLimits: %+v\n", report.Version, report.PlanDigest, report.StateDigest, report.Complete, report.Database, report.ObservedAt.Format(time.RFC3339Nano), report.Deployment.Revision, report.Ledger.Revision, report.CatalogRevision, report.CatalogFingerprint, report.TargetOperations, report.PolicyEligible, report.BackupEvidenceSHA256, report.RollbackHolds, report.Counts, report.RetainedFloors, report.EstimatedRows, report.Limits)
		for _, member := range report.Members {
			fmt.Printf("Member: %s role=%s login=%t privileged=%t backends=%d\n", member.Name, member.Role, member.Login, member.Privileged, member.Backends)
		}
		for _, entry := range report.CatalogEntries {
			fmt.Printf("Catalog entry: %d %s.%s state=%s retired_revision=%d\n", entry.TableID, entry.Schema, entry.Table, entry.State, entry.RetiredRevision)
		}
		for _, predicate := range report.Predicates {
			fmt.Printf("Predicate: %s complete=%t passed=%t source=%s\n", predicate.Name, predicate.Complete, predicate.Passed, predicate.Source)
		}
		for _, dependency := range report.DependencyInventory {
			fmt.Printf("Dependency: %s %s disposition=%s\n", dependency.Kind, dependency.Identity, dependency.Disposition)
		}
		if report.Recovery != nil {
			fmt.Printf("Recovery: state=%s cut=%s verification=%s\n", report.Recovery.State, report.Recovery.CutSHA256, report.Recovery.VerificationSHA256)
		}
		fmt.Printf("Blockers: %v\n", report.Blockers)
	} else if err = output.Encode(report); err != nil {
		return 1, err
	}
	if len(report.Blockers) != 0 {
		return 2, nil
	}
	return 0, nil
}

func readInput(path string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return schemaevolution.ReadRolloutArtifact(f, target)
}
