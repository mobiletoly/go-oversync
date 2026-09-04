package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

type RolloutResult struct {
	Duration          time.Duration
	AuthorityLockWait time.Duration
	OperationID       string
	State             string
	DeletedRows       int64
	Catalog           *oversync.CatalogRetirementResult
	Cleanup           *oversync.StagingCleanupBatch
}

// ApplyRollout executes one bounded phase under the already drained authority.
// The application constructs target from its code-first schema definitions.
// Retrying an ambiguous result with the same ID reconciles before further work.
func (a *Authority) ApplyRollout(ctx context.Context, p RolloutPlan, report RolloutReport, target *oversync.ServiceConfig, private *oversync.SyncService) (RolloutResult, error) {
	var result RolloutResult
	if a == nil || a.conn == nil {
		return result, fmt.Errorf("rollout_authority_required")
	}
	planDigest, err := validateRolloutRequest(p, report, target)
	if err != nil {
		return result, err
	}
	ctx, bounded := context.WithTimeout(ctx, 60*time.Second)
	defer bounded()
	completed, err := a.reserveRollout(ctx, p, planDigest, report, &result)
	if err != nil || completed {
		return result, err
	}
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	ctx, queryCancel := context.WithTimeout(ctx, p.QueryTimeout)
	defer queryCancel()
	started := time.Now()
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		completed, _, err := loadRolloutAttempt(ctx, tx, p, planDigest, &result)
		if err != nil || completed {
			return err
		}
		live, err := inspectRollout(ctx, tx, p)
		if err != nil {
			return err
		}
		if len(live.Blockers) != 0 {
			return fmt.Errorf("rollout_blocked: %v", live.Blockers)
		}
		// A failed first attempt has no committed mutation and must still match
		// the reviewed report. Later prune batches reconcile their own row counts.
		var committedBatches int
		if err = tx.QueryRow(ctx, `SELECT batches FROM app_evolution.rollout_operations WHERE operation_id=$1`, p.ID).Scan(&committedBatches); err != nil {
			return err
		}
		if committedBatches == 0 && live.StateDigest != report.StateDigest {
			return fmt.Errorf("rollout_report_stale")
		}
		result.OperationID = p.ID
		result.State = "completed"
		if err = executeRolloutPhase(ctx, tx, p, target, private, &result); err != nil {
			return err
		}
		result.Duration = time.Since(started)
		result.AuthorityLockWait = a.lockWait
		bytes, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if len(bytes) > p.MaxEvidenceBytes {
			return fmt.Errorf("rollout_audit_capacity")
		}
		if err = recordRolloutBatch(ctx, tx, p, bytes); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE app_evolution.rollout_operations SET state=$2,deleted_rows=$3,result=$4,completed_at=CASE WHEN $2='completed' THEN transaction_timestamp() ELSE NULL END WHERE operation_id=$1`, p.ID, result.State, result.DeletedRows, bytes)
		return err
	})
	return result, err
}

// Reserve intent in a separate transaction before executing any destructive phase.
// A rollback or lost connection leaves the original immutable deadline and plan
// available to status/resume; it never creates a fresh retry budget.
func (a *Authority) reserveRollout(ctx context.Context, p RolloutPlan, planDigest string, report RolloutReport, result *RolloutResult) (bool, error) {
	completed := false
	err := pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		var exists bool
		var err error
		completed, exists, err = loadRolloutAttempt(ctx, tx, p, planDigest, result)
		if err != nil || completed {
			return err
		}
		if exists {
			var storedReport RolloutReport
			if err = tx.QueryRow(ctx, `SELECT report FROM app_evolution.rollout_operations WHERE operation_id=$1`, p.ID).Scan(&storedReport); err != nil {
				return err
			}
			expectedBytes, err := json.Marshal(report)
			if err != nil {
				return err
			}
			storedBytes, err := json.Marshal(storedReport)
			if err != nil {
				return err
			}
			if digest(expectedBytes) != digest(storedBytes) {
				return fmt.Errorf("rollout_report_identity_conflict")
			}
			return reserveRolloutAttempt(ctx, tx, p)
		}
		live, err := inspectRollout(ctx, tx, p)
		if err != nil {
			return err
		}
		if len(live.Blockers) != 0 || live.StateDigest != report.StateDigest {
			return fmt.Errorf("rollout_report_stale_or_blocked")
		}
		_, err = tx.Exec(ctx, `INSERT INTO app_evolution.rollout_operations(operation_id,digest,plan,report,state) VALUES($1,$2,$3,$4,'running')`, p.ID, planDigest, p, report)
		if err != nil {
			return err
		}
		return reserveRolloutAttempt(ctx, tx, p)
	})
	return completed, err
}

func loadRolloutAttempt(ctx context.Context, tx pgx.Tx, p RolloutPlan, digest string, result *RolloutResult) (bool, bool, error) {
	var storedDigest, state string
	var storedPlan RolloutPlan
	var bytes []byte
	err := tx.QueryRow(ctx, `SELECT digest,state,plan,result FROM app_evolution.rollout_operations WHERE operation_id=$1 FOR UPDATE`, p.ID).Scan(&storedDigest, &state, &storedPlan, &bytes)
	if err == pgx.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	recomputed, identityErr := RolloutPlanDigest(storedPlan)
	if identityErr != nil || recomputed != digest || storedDigest != digest {
		return false, false, fmt.Errorf("rollout_operation_identity_conflict")
	}
	if len(bytes) != 0 && string(bytes) != "null" {
		if err = json.Unmarshal(bytes, result); err != nil {
			return false, false, fmt.Errorf("rollout_result_corrupt")
		}
	}
	return state == "completed", true, nil
}

func executeRolloutPhase(ctx context.Context, tx pgx.Tx, p RolloutPlan, target *oversync.ServiceConfig, private *oversync.SyncService, result *RolloutResult) error {
	switch p.Action {
	case "prepare":
		if wrapper, ok := target.Readiness.Adapter.(rolloutLedgerAdapter); ok {
			if base, ok := wrapper.base.(rolloutTableAdapter); ok && base.stage == RolloutTableLedger {
				return publishRolloutTarget(ctx, tx, target)
			}
		}
		return prepareRolloutColumn(ctx, tx, p, target)
	case "retire_generation":
		return retireRolloutGeneration(ctx, tx, p, target)
	case "enable_generation":
		return enableRolloutGeneration(ctx, tx, p, target)
	case "cleanup_staging":
		return cleanupRolloutStaging(ctx, tx, p, private, result)
	case "expire_history":
		return expireRolloutHistory(ctx, tx, p, private, result)
	case "contract":
		if p.Contract != nil && *p.Contract == personContractIdentity() {
			return contractRolloutPerson(ctx, tx, p, target)
		}
		return contractRolloutNotes(ctx, tx, p, target)
	case "retire_tables":
		if private == nil {
			return fmt.Errorf("private_retirement_runtime_required")
		}
		batch, err := private.RetireRegisteredTables(ctx, tx)
		if err != nil {
			return err
		}
		expectedPlanDigest, digestErr := RolloutPlanDigest(p)
		if digestErr != nil || batch.OperationID != p.ID || batch.PlanDigest != expectedPlanDigest {
			return fmt.Errorf("retirement_operation_mismatch")
		}
		result.Catalog = &batch
		return publishRolloutTarget(ctx, tx, target)
	case "prune_table_state":
		if private == nil {
			return fmt.Errorf("private_retirement_runtime_required")
		}
		batch, err := private.PruneRetiredTableState(ctx, tx)
		if err != nil {
			return err
		}
		expectedPlanDigest, digestErr := RolloutPlanDigest(p)
		if digestErr != nil || batch.OperationID != p.ID || batch.PlanDigest != expectedPlanDigest {
			return fmt.Errorf("retirement_operation_mismatch")
		}
		result.DeletedRows += batch.DeletedRows
		result.Catalog = &batch
		var remaining bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.row_state WHERE table_id=ANY($1::int[]))`, p.TableIDs).Scan(&remaining); err != nil {
			return err
		}
		if remaining {
			result.State = "running"
		}
		return nil
	default:
		return fmt.Errorf("rollout_action_requires_application_driver")
	}
}

func retireRolloutGeneration(ctx context.Context, tx pgx.Tx, p RolloutPlan, target *oversync.ServiceConfig) error {
	if p.LedgerRevision == int64(^uint64(0)>>1) {
		return fmt.Errorf("generation_revision_exhausted")
	}
	tag, err := tx.Exec(ctx, `UPDATE app_evolution.generation_ledger SET state='retired',retired_revision=$3 WHERE generation_id=$1 AND fingerprint=$2 AND state='supported'`, p.Retiring.ID, p.Retiring.Fingerprint, p.LedgerRevision+1)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retirement_generation_changed")
	}
	if _, err = tx.Exec(ctx, `UPDATE app_evolution.generation_ledger_meta SET revision=revision+1 WHERE singleton AND revision=$1`, p.LedgerRevision); err != nil {
		return err
	}
	return publishRolloutTarget(ctx, tx, target)
}

func publishRolloutTarget(ctx context.Context, tx pgx.Tx, cfg *oversync.ServiceConfig) error {
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return err
	}
	ledger, err := readGenerationLedger(ctx, tx)
	if err != nil {
		return err
	}
	if err = oversync.ValidateGenerationLedger(ledger, registry); err != nil {
		return err
	}
	if !reflect.DeepEqual(ledger, cfg.Readiness.Expected.GenerationLedger) {
		return fmt.Errorf("target_generation_ledger_mismatch")
	}
	var enabled []oversync.GenerationEnablement
	for _, g := range cfg.Readiness.Generations {
		enabled = append(enabled, oversync.GenerationEnablement{Requirement: g, DeploymentRevision: cfg.Readiness.Expected.Revision, Enabled: true})
	}
	tag, err := tx.Exec(ctx, `UPDATE app_evolution.deployment SET revision=$1,contract=$2,generations=$3 WHERE singleton AND revision<$1`, cfg.Readiness.Expected.Revision, cfg.Readiness.Expected, enabled)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rollout_target_revision_conflict")
	}
	evidence, err := cfg.Readiness.Adapter.Inspect(ctx, tx)
	if err != nil {
		return err
	}
	return oversync.ValidateApplicationReadiness(cfg.Readiness, registry, evidence)
}

// RolloutStatus is read-only and preserves the exact stored plan for Resume.
func RolloutStatus(ctx context.Context, r oversync.DatabaseReadinessReader, id string) (RolloutPlan, RolloutReport, RolloutResult, error) {
	var p RolloutPlan
	var report RolloutReport
	var result RolloutResult
	if id == "" {
		return p, report, result, fmt.Errorf("operation_id_required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := r.QueryRow(ctx, `SELECT plan,report,COALESCE(result,jsonb_build_object('OperationID',operation_id,'State',state,'DeletedRows',deleted_rows)) FROM app_evolution.rollout_operations WHERE operation_id=$1`, id).Scan(&p, &report, &result)
	return p, report, result, err
}

func enableRolloutGeneration(ctx context.Context, tx pgx.Tx, p RolloutPlan, target *oversync.ServiceConfig) error {
	if p.LedgerRevision == int64(^uint64(0)>>1) {
		return fmt.Errorf("generation_revision_exhausted")
	}
	var supported int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM app_evolution.generation_ledger WHERE state='supported'`).Scan(&supported); err != nil {
		return err
	}
	if supported >= 2 {
		return fmt.Errorf("generation_support_capacity")
	}
	tag, err := tx.Exec(ctx, `UPDATE app_evolution.generation_ledger SET state='supported' WHERE generation_id=$1 AND fingerprint=$2 AND state='allocated'`, p.Activating.ID, p.Activating.Fingerprint)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("activation_generation_changed")
	}
	tag, err = tx.Exec(ctx, `UPDATE app_evolution.generation_ledger_meta SET revision=revision+1 WHERE singleton AND revision=$1`, p.LedgerRevision)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("generation_ledger_changed")
	}
	return publishRolloutTarget(ctx, tx, target)
}

func contractRolloutNotes(ctx context.Context, tx pgx.Tx, p RolloutPlan, target *oversync.ServiceConfig) error {
	identity := notesContractIdentity()
	if p.Contract == nil || *p.Contract != identity {
		return fmt.Errorf("exact_contract_migration_required")
	}
	if target.Readiness.Expected.Revision != int64(RolloutTableContracted) || target.RegisteredTableCatalogRevision != 3 {
		return fmt.Errorf("contract_target_mismatch")
	}
	var retired, historyEmpty, stateEmpty bool
	if err := tx.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM sync.table_catalog WHERE table_id=1 AND schema_name='main' AND table_name='legacy_notes' AND state_code=2),
 NOT EXISTS(SELECT 1 FROM sync.bundle_rows WHERE table_id=1),
 NOT EXISTS(SELECT 1 FROM sync.row_state WHERE table_id=1)`).Scan(&retired, &historyEmpty, &stateEmpty); err != nil {
		return err
	}
	if !retired || !historyEmpty || !stateEmpty {
		return fmt.Errorf("contract_retirement_incomplete")
	}
	data, err := rolloutSource.ReadFile(identity.Filename)
	if err != nil {
		return err
	}
	if digest(data) != identity.SHA256 {
		return fmt.Errorf("contract_migration_changed")
	}
	if _, err = tx.Exec(ctx, string(data)); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app_evolution.migrations(version,identity,state,started_at,completed_at) VALUES($1,$2,'completed',transaction_timestamp(),transaction_timestamp())`, identity.Version, identity); err != nil {
		return err
	}
	v, err := validateRolloutGraph(ctx, tx)
	if err != nil {
		return err
	}
	if v.Violations != 0 {
		return fmt.Errorf("postcontract_graph_invalid")
	}
	if err = publishContractCertificate(ctx, tx, p.ID, target.Readiness.Expected, v); err != nil {
		return err
	}

	return publishRolloutTarget(ctx, tx, target)
}

func reserveRolloutAttempt(ctx context.Context, tx pgx.Tx, p RolloutPlan) error {
	tag, err := tx.Exec(ctx, `UPDATE app_evolution.rollout_operations SET attempts=attempts+1 WHERE operation_id=$1 AND attempts<$2 AND batches<$3 AND transaction_timestamp()<$4`, p.ID, p.MaxAttempts, p.MaxBatches, p.Deadline)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rollout_budget_exhausted")
	}
	return nil
}
func recordRolloutBatch(ctx context.Context, tx pgx.Tx, p RolloutPlan, outcome []byte) error {
	var batch int
	err := tx.QueryRow(ctx, `UPDATE app_evolution.rollout_operations SET batches=batches+1,evidence_bytes=evidence_bytes+$2 WHERE operation_id=$1 AND batches<$3 AND evidence_bytes+$2<=$4 RETURNING batches`, p.ID, len(outcome), p.MaxBatches, p.MaxEvidenceBytes).Scan(&batch)
	if err != nil {
		return fmt.Errorf("rollout_batch_or_evidence_budget: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO app_evolution.rollout_batches(operation_id,batch_number,outcome) VALUES($1,$2,$3)`, p.ID, batch, outcome)
	return err
}

func expireRolloutHistory(ctx context.Context, tx pgx.Tx, p RolloutPlan, private *oversync.SyncService, result *RolloutResult) error {
	if private == nil {
		return fmt.Errorf("private_retirement_runtime_required")
	}
	batch, err := private.ExpireRetiredTableHistory(ctx, tx)
	if err != nil {
		return err
	}
	expected, err := RolloutPlanDigest(p)
	if err != nil {
		return err
	}
	if batch.OperationID != p.ID || batch.PlanDigest != expected {
		return fmt.Errorf("retirement_operation_mismatch")
	}
	result.Catalog = &batch
	var remaining bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.bundle_rows WHERE table_id=ANY($1::int[]))`, p.TableIDs).Scan(&remaining); err != nil {
		return err
	}
	if remaining {
		result.State = "running"
	}
	return nil
}

func publishContractCertificate(ctx context.Context, tx pgx.Tx, operationID string, d oversync.DeploymentContract, v oversync.InvariantValidation) error {
	cert := oversync.BackfillCertificate{Requirement: v.Requirement, MigrationDigest: oversync.MigrationManifestDigest(d.Migrations), TransitionRevision: d.TransitionRevision, TransitionPhase: d.TransitionPhase, CoverageWatermark: v.CoverageWatermark, ValidatedAt: time.Now().UTC(), ResultSHA256: v.ResultSHA256}
	_, err := tx.Exec(ctx, `INSERT INTO app_evolution.rollout_certificates(deployment_revision,operation_id,certificate) VALUES($1,$2,$3)`, d.Revision, operationID, cert)
	return err
}

func validateRolloutRequest(p RolloutPlan, report RolloutReport, target *oversync.ServiceConfig) (string, error) {
	planDigest, err := RolloutPlanDigest(p)
	if err != nil {
		return "", err
	}
	targetDigest, err := RolloutTargetDigest(target)
	if err != nil {
		return "", err
	}
	if targetDigest != p.TargetSHA256 {
		return "", fmt.Errorf("rollout_target_mismatch")
	}
	stateDigest, err := rolloutReportStateDigest(report, p.MaxEvidenceBytes)
	if err != nil || stateDigest != report.StateDigest || !report.Complete || !rolloutPredicatesComplete(report.Predicates, p.Action) || len(report.Blockers) != 0 || report.PlanDigest != planDigest {
		return "", fmt.Errorf("complete_unblocked_report_required")
	}
	return planDigest, nil
}

func cleanupRolloutStaging(ctx context.Context, tx pgx.Tx, p RolloutPlan, private *oversync.SyncService, result *RolloutResult) error {
	if private == nil {
		return fmt.Errorf("private_cleanup_runtime_required")
	}
	batch, err := private.CleanupExpiredStaging(ctx, tx)
	if err != nil {
		return err
	}
	if batch.Permit.PlanID != p.ID {
		return fmt.Errorf("cleanup_operation_mismatch")
	}
	result.Cleanup = &batch
	for _, session := range batch.Sessions {
		result.DeletedRows += session.DeletedRows
	}
	var remaining bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.snapshot_sessions)`).Scan(&remaining); err != nil {
		return err
	}
	if remaining {
		result.State = "running"
	}
	return nil
}
