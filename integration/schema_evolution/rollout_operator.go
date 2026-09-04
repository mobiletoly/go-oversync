package schemaevolution

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// RolloutExecution is an operator artifact, not a portable schema language. The
// only schema target here is the compiled integration authority's table fixture.
// Resume consumes these exact target bytes and the original stored operation.
type RolloutExecution struct {
	Plan                RolloutPlan
	Report              RolloutReport
	TargetStage         RolloutTableStage
	ColumnStage         RolloutColumnStage
	TargetLedger        *oversync.GenerationLedger
	QualificationSHA256 string
}

// RolloutTargetSelection contains only inputs to the fixed application factories.
// Its enclosing immutable plan and TargetSHA256 bind the exact compiled target.
type RolloutTargetSelection struct {
	TargetStage         RolloutTableStage
	ColumnStage         RolloutColumnStage
	TargetLedger        *oversync.GenerationLedger
	QualificationSHA256 string
}

func (in *RolloutExecution) RetainTargetSelection() {
	in.Plan.TargetSelection = &RolloutTargetSelection{in.TargetStage, in.ColumnStage, in.TargetLedger, in.QualificationSHA256}
}

func ResumeRolloutExecution(ctx context.Context, pool *pgxpool.Pool, id string) (RolloutExecution, error) {
	p, r, _, err := RolloutStatus(ctx, pool, id)
	if err != nil {
		return RolloutExecution{}, err
	}
	if p.TargetSelection == nil {
		return RolloutExecution{}, fmt.Errorf("stored_cli_target_required")
	}
	target := p.TargetSelection
	return RolloutExecution{Plan: p, Report: r, TargetStage: target.TargetStage, ColumnStage: target.ColumnStage, TargetLedger: target.TargetLedger, QualificationSHA256: target.QualificationSHA256}, nil
}

func ReadRolloutLedger(ctx context.Context, pool *pgxpool.Pool) (*oversync.GenerationLedger, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var ledger *oversync.GenerationLedger
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error { var err error; ledger, err = readGenerationLedger(ctx, tx); return err })
	return ledger, err
}

func OpenClosedRolloutAuthority(ctx context.Context, pool *pgxpool.Pool) (*Authority, error) {
	a, err := OpenAuthority(ctx, DrainProof{pool: pool})
	if err != nil {
		return nil, err
	}
	if err = requireRolloutAuthority(ctx, a.conn); err != nil {
		a.Close()
		return nil, err
	}
	return a, nil
}

func ExecuteTableRollout(ctx context.Context, pool *pgxpool.Pool, input RolloutExecution, resume bool) (RolloutResult, error) {
	var result RolloutResult
	if resume {
		p, report, _, err := RolloutStatus(ctx, pool, input.Plan.ID)
		if err != nil {
			return result, err
		}
		expected, err := RolloutPlanDigest(input.Plan)
		if err != nil {
			return result, err
		}
		stored, err := RolloutPlanDigest(p)
		if err != nil || expected != stored {
			return result, fmt.Errorf("rollout_resume_identity_conflict")
		}
		input.Plan = p
		input.Report = report
	}
	var target *oversync.ServiceConfig
	var err error
	if input.ColumnStage != 0 {
		target, err = RolloutColumnConfiguration(input.ColumnStage, input.TargetLedger, input.QualificationSHA256)
	} else {
		target, err = RolloutTableConfiguration(input.TargetStage, input.TargetLedger, input.QualificationSHA256)
	}
	if err != nil {
		return result, err
	}
	// Reconcile a committed phase before constructing its obsolete private
	// runtime. A table retirement may already have advanced the catalog.
	requestedHash, err := validateRolloutRequest(input.Plan, input.Report, target)
	if err != nil {
		return result, err
	}
	if stored, storedReport, done, statusErr := RolloutStatus(ctx, pool, input.Plan.ID); statusErr == nil && done.State == "completed" {
		storedHash, err := RolloutPlanDigest(stored)
		if err != nil || storedHash != requestedHash || !reflect.DeepEqual(storedReport, input.Report) {
			return result, fmt.Errorf("rollout_operation_identity_conflict")
		}
		return done, nil
	}
	private, cleanup, err := openPrivateRolloutRuntime(ctx, pool, input)
	if err != nil {
		return result, err
	}
	defer cleanup()

	a, err := OpenClosedRolloutAuthority(ctx, pool)
	if err != nil {
		return result, err
	}
	defer a.Close()
	return a.ApplyRollout(ctx, input.Plan, input.Report, target, private)
}

func openPrivateRolloutRuntime(ctx context.Context, pool *pgxpool.Pool, input RolloutExecution) (*oversync.SyncService, func(), error) {
	var err error
	if input.Plan.Action == "retire_tables" || input.Plan.Action == "prune_table_state" || input.Plan.Action == "expire_history" || input.Plan.Action == "cleanup_staging" {
		var current oversync.DeploymentContract
		if err = pool.QueryRow(ctx, `SELECT contract FROM app_evolution.deployment WHERE singleton`).Scan(&current); err != nil {
			return nil, func() {}, err
		}
		var cfg *oversync.ServiceConfig
		if input.ColumnStage != 0 {
			cfg, err = RolloutColumnConfiguration(RolloutColumnStage(current.Revision), current.GenerationLedger, current.QualificationSHA256)
		} else {
			cfg, err = RolloutTableConfiguration(RolloutTableStage(current.Revision), current.GenerationLedger, current.QualificationSHA256)
		}
		if err != nil {
			return nil, func() {}, err
		}
		cfg.Readiness.DisableBackgroundWorkers = true
		if input.Plan.Action == "expire_history" {
			cfg.RetainedBundlesPerUser = input.Plan.RetentionWindow
			cfg.RetentionPruneBatchSize = int64(input.Plan.MaxRows)
		}
		if err = WithCatalogRetirement(cfg, input.Plan); err != nil {
			return nil, func() {}, err
		}
		pair, err := oversync.NewReadinessPoolPair(ctx, pool.Config().Copy(), 2)
		if err != nil {
			return nil, func() {}, err
		}

		cfg.Readiness.PoolPair = pair
		private, err := oversync.NewRuntimeService(pair.ExecutionPool(), cfg, slog.New(slog.DiscardHandler))
		if err != nil {
			pair.Close()
			return nil, func() {}, err
		}

		if err = private.Bootstrap(ctx); err != nil {
			_ = private.Close(context.Background())
			pair.Close()
			return nil, func() {}, err
		}
		return private, func() { _ = private.Close(context.Background()); pair.Close() }, nil
	}
	return nil, func() {}, nil
}
