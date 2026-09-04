package schemaevolution

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

//go:embed rollout_control.sql
var rolloutControlSQL string

// PrepareGenerationLedger installs the first ledger only under the existing
// drained application authority. Repeated calls validate the exact active set;
// omission never retires a generation. It does not reopen admission.
func (a *Authority) PrepareGenerationLedger(ctx context.Context, cfg *oversync.ServiceConfig) (*oversync.GenerationLedger, error) {
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	var ledger *oversync.GenerationLedger
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, rolloutControlSQL); err != nil {
			return err
		}
		var initialized bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.generation_ledger_meta)`).Scan(&initialized); err != nil {
			return err
		}
		if !initialized {
			ids := registry.GenerationIDs()
			for i, id := range ids {
				contract, _ := registry.Generation(id)
				if err := insertGeneration(ctx, tx, contract, int64(i+1), "supported"); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.generation_ledger_meta VALUES(true,$1,$2,$3)`, len(ids), ids[len(ids)-1], oversync.MaxGenerationLedgerEntries); err != nil {
				return err
			}
		}
		ledger, err = readGenerationLedger(ctx, tx)
		if err != nil {
			return err
		}
		return oversync.ValidateGenerationLedger(ledger, registry)
	})
	return ledger, err
}

func requireRolloutAuthority(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	var valid bool
	err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b
 WHERE m.singleton AND b.singleton AND m.requested AND b.closed AND m.epoch=b.revision)
 AND EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
 AND classid=1869833075 AND objid=6 AND objsubid=2 AND mode='ExclusiveLock' AND granted)`).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("rollout_authority_required")
	}
	return nil
}

func insertGeneration(ctx context.Context, tx pgx.Tx, contract oversync.SchemaGenerationContract, revision int64, state string) error {
	// Tables is an independently owned canonical ordered declaration from the
	// compiler, including sync keys. Lifecycle facts are not part of its proof.
	_, err := tx.Exec(ctx, `INSERT INTO app_evolution.generation_ledger(generation_id,fingerprint,declaration,allocated_revision,state) VALUES($1,$2,$3,$4,$5)`, contract.ID(), contract.Fingerprint(), contract.Tables(), revision, state)
	return err
}

// AllocateGeneration permanently reserves a new monotonic ID. Failed/abandoned
// application plans cannot reclaim it. Activation requires a later closed rollout.
func (a *Authority) AllocateGeneration(ctx context.Context, expectedRevision int64, tables []oversync.RegisteredTable, declaration oversync.SchemaGeneration) (*oversync.GenerationLedger, error) {
	registry, err := oversync.CompileSchemaGenerationRegistry(tables, []oversync.SchemaGeneration{declaration})
	if err != nil {
		return nil, err
	}
	contract, _ := registry.Generation(declaration.ID)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var result *oversync.GenerationLedger
	err = pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		var revision int64
		var maximum, capacity, count int
		if err := tx.QueryRow(ctx, `SELECT revision,maximum_id,capacity,(SELECT count(*) FROM app_evolution.generation_ledger) FROM app_evolution.generation_ledger_meta WHERE singleton FOR UPDATE`).Scan(&revision, &maximum, &capacity, &count); err != nil {
			return err
		}
		if revision != expectedRevision || int(declaration.ID) <= maximum || count >= capacity || revision == int64(^uint64(0)>>1) {
			return fmt.Errorf("generation_allocation_conflict_or_capacity")
		}
		if err := insertGeneration(ctx, tx, contract, revision+1, "allocated"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE app_evolution.generation_ledger_meta SET revision=revision+1,maximum_id=$1 WHERE singleton`, declaration.ID); err != nil {
			return err
		}
		result, err = readGenerationLedger(ctx, tx)
		return err
	})
	return result, err
}

func readGenerationLedger(ctx context.Context, r oversync.DatabaseReadinessReader) (*oversync.GenerationLedger, error) {
	if err := validateGenerationLedgerGuards(ctx, r); err != nil {
		return nil, err
	}
	ledger := &oversync.GenerationLedger{}
	var capacity int
	var maximum oversync.SchemaGenerationID
	if err := r.QueryRow(ctx, `SELECT revision,maximum_id,capacity FROM app_evolution.generation_ledger_meta WHERE singleton`).Scan(&ledger.Revision, &maximum, &capacity); err != nil {
		return nil, err
	}
	if capacity < 1 || capacity > oversync.MaxGenerationLedgerEntries {
		return nil, fmt.Errorf("generation_ledger_capacity_invalid")
	}
	rows, err := r.Query(ctx, `SELECT generation_id,fingerprint,state,allocated_revision,COALESCE(retired_revision,0),CASE WHEN octet_length(declaration::text)<=1048576 THEN declaration END FROM app_evolution.generation_ledger ORDER BY generation_id LIMIT $1`, capacity+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	declarationBytes := 0
	for rows.Next() {
		var entry oversync.GenerationLifecycle
		var declaration []byte
		if err := rows.Scan(&entry.Proof.ID, &entry.Proof.Fingerprint, &entry.State, &entry.AllocatedRevision, &entry.RetiredRevision, &declaration); err != nil {
			return nil, err
		}
		declarationBytes += len(declaration)
		if declarationBytes > 16<<20 {
			return nil, fmt.Errorf("generation_archive_capacity")
		}
		if err := validateArchivedGeneration(entry.Proof, declaration); err != nil {
			return nil, err
		}
		ledger.Entries = append(ledger.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ledger.Entries) == 0 || len(ledger.Entries) > capacity || ledger.Entries[len(ledger.Entries)-1].Proof.ID != maximum {
		return nil, fmt.Errorf("generation_ledger_incomplete_or_capacity")
	}
	return ledger, nil
}

func validateGenerationLedgerGuards(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	for _, guard := range []struct {
		table, trigger, function string
	}{
		{"generation_ledger", "generation_identity_guard", "guard_generation_identity"},
		{"generation_ledger_meta", "generation_meta_guard", "guard_generation_meta"},
	} {
		var enabled, body string
		var publicWrite bool
		err := r.QueryRow(ctx, `SELECT t.tgenabled::text,p.prosrc,
 has_table_privilege('public',c.oid,'INSERT') OR has_table_privilege('public',c.oid,'UPDATE')
 OR has_table_privilege('public',c.oid,'DELETE') OR has_table_privilege('public',c.oid,'TRUNCATE')
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_trigger t ON t.tgrelid=c.oid AND t.tgname=$2 AND NOT t.tgisinternal
 JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace
 WHERE n.nspname='app_evolution' AND c.relname=$1 AND pn.nspname='app_evolution' AND p.proname=$3`, guard.table, guard.trigger, guard.function).Scan(&enabled, &body, &publicWrite)
		if err != nil {
			return fmt.Errorf("generation_ledger_guard_missing: %w", err)
		}
		expected, err := rolloutFunctionBody(guard.function)
		if err != nil {
			return err
		}
		if enabled != "O" || body != expected || publicWrite {
			return fmt.Errorf("generation_ledger_guard_invalid")
		}
	}
	return nil
}

func rolloutFunctionBody(name string) (string, error) {
	prefix := "CREATE OR REPLACE FUNCTION app_evolution." + name + "()"
	start := strings.Index(rolloutControlSQL, prefix)
	if start < 0 {
		return "", fmt.Errorf("rollout_guard_definition_missing")
	}
	open := strings.Index(rolloutControlSQL[start:], "$$")
	if open < 0 {
		return "", fmt.Errorf("rollout_guard_definition_missing")
	}
	start += open + 2
	end := strings.Index(rolloutControlSQL[start:], "$$;")
	if end < 0 {
		return "", fmt.Errorf("rollout_guard_definition_missing")
	}
	return rolloutControlSQL[start : start+end], nil
}

// AbandonGeneration permanently closes an unused reservation. It never changes
// the supported set or makes the ID available for another declaration.
func (a *Authority) AbandonGeneration(ctx context.Context, expectedRevision int64, proof oversync.SchemaGenerationProof) (*oversync.GenerationLedger, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var ledger *oversync.GenerationLedger
	err := pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := requireRolloutAuthority(ctx, tx); err != nil {
			return err
		}
		var revision int64
		if err := tx.QueryRow(ctx, `SELECT revision FROM app_evolution.generation_ledger_meta WHERE singleton FOR UPDATE`).Scan(&revision); err != nil {
			return err
		}
		if revision != expectedRevision {
			return fmt.Errorf("generation_ledger_changed")
		}
		changed, err := tx.Exec(ctx, `UPDATE app_evolution.generation_ledger SET state='abandoned',retired_revision=$3 WHERE generation_id=$1 AND fingerprint=$2 AND state='allocated'`, proof.ID, proof.Fingerprint, revision+1)
		if err != nil {
			return err
		}
		if changed.RowsAffected() != 1 {
			return fmt.Errorf("generation_reservation_mismatch")
		}
		if _, err = tx.Exec(ctx, `UPDATE app_evolution.generation_ledger_meta SET revision=revision+1 WHERE singleton`); err != nil {
			return err
		}
		ledger, err = readGenerationLedger(ctx, tx)
		return err
	})
	return ledger, err
}

// WithGenerationLedger binds a freshly read ledger to an application configuration.
// Startup and per-operation admission still use the original application's locks.
func WithGenerationLedger(cfg *oversync.ServiceConfig, ledger *oversync.GenerationLedger) error {
	if cfg == nil || cfg.Readiness == nil || cfg.Readiness.Adapter == nil {
		return fmt.Errorf("readiness_required")
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return err
	}
	if err = oversync.ValidateGenerationLedger(ledger, registry); err != nil {
		return err
	}
	copy := *ledger
	copy.Entries = append([]oversync.GenerationLifecycle(nil), ledger.Entries...)
	cfg.Readiness.Expected.GenerationLedger = &copy
	cfg.Readiness.Adapter = rolloutLedgerAdapter{base: cfg.Readiness.Adapter, revision: copy.Revision}
	return nil
}

type rolloutLedgerAdapter struct {
	base       oversync.ReadinessAdapter
	revision   int64
	retirement *RolloutPlan
}

func (a rolloutLedgerAdapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	e, err := a.base.Inspect(ctx, r)
	if err != nil {
		return e, err
	}
	actual, err := readGenerationLedger(ctx, r)
	if err != nil {
		return e, err
	}
	if !reflect.DeepEqual(e.Deployment.GenerationLedger, actual) {
		return e, fmt.Errorf("deployment_generation_ledger_mismatch")
	}
	return e, nil
}

func (a rolloutLedgerAdapter) Admit(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	base, ok := a.base.(oversync.OperationAdmissionAdapter)
	if !ok {
		return fmt.Errorf("rollout_admission_required")
	}
	if err := base.Admit(ctx, r); err != nil {
		return err
	}
	var revision int64
	if err := r.QueryRow(ctx, `SELECT revision FROM app_evolution.generation_ledger_meta WHERE singleton`).Scan(&revision); err != nil {
		return err
	}
	if revision != a.revision {
		return fmt.Errorf("stale_generation_ledger")
	}
	return nil
}

func (a rolloutLedgerAdapter) ValidateCatalogReadiness(ctx context.Context, r oversync.DatabaseReadinessReader, revision int64, fingerprint string) error {
	if base, ok := a.base.(oversync.CatalogReadinessAdapter); ok {
		return base.ValidateCatalogReadiness(ctx, r, revision, fingerprint)
	}
	return nil
}

func (a rolloutLedgerAdapter) AuthorizeStagingCleanup(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.StagingCleanupPermit, error) {
	if a.retirement != nil && a.retirement.Action == "cleanup_staging" {
		p, _, err := a.authorizePrivateRollout(ctx, r)
		if err != nil {
			return oversync.StagingCleanupPermit{}, err
		}
		return oversync.StagingCleanupPermit{Epoch: p.Epoch, DeploymentRevision: p.DeploymentRevision, CatalogRevision: p.CatalogRevision, Worker: "rollout-authority", PlanID: p.ID, RegistryFingerprint: p.RegistryFingerprint, CatalogFingerprint: p.CatalogFingerprint, Deadline: p.Deadline, QueryTimeout: p.QueryTimeout, MaxSessions: min(64, p.MaxRows), MaxRows: p.MaxRows, MaxEvidenceBytes: p.MaxEvidenceBytes}, nil
	}
	if base, ok := a.base.(oversync.StagingCleanupAdapter); ok {
		return base.AuthorizeStagingCleanup(ctx, r)
	}
	return oversync.StagingCleanupPermit{}, fmt.Errorf("staging_cleanup_not_authorized")
}

// WithCatalogRetirement installs the exact private operation capability. The
// durable reservation and live database fences are checked in the mutation's
// transaction; constructing this configuration is not itself authorization.
func WithCatalogRetirement(cfg *oversync.ServiceConfig, p RolloutPlan) error {
	if _, err := RolloutPlanDigest(p); err != nil {
		return err
	}
	if cfg == nil || cfg.Readiness == nil || !cfg.Readiness.DisableBackgroundWorkers {
		return fmt.Errorf("private_retirement_runtime_required")
	}
	a, ok := cfg.Readiness.Adapter.(rolloutLedgerAdapter)
	if !ok {
		return fmt.Errorf("generation_ledger_adapter_required")
	}
	if p.Action != "retire_tables" && p.Action != "prune_table_state" && p.Action != "expire_history" && p.Action != "cleanup_staging" {
		return fmt.Errorf("catalog_operation_required")
	}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	var immutable RolloutPlan
	if err = json.Unmarshal(b, &immutable); err != nil {
		return err
	}
	a.retirement = &immutable
	cfg.Readiness.Adapter = a
	return nil
}

func (a rolloutLedgerAdapter) AuthorizeCatalogRetirement(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.CatalogRetirementPermit, error) {
	var permit oversync.CatalogRetirementPermit
	p, expected, err := a.authorizePrivateRollout(ctx, r)
	if err != nil {
		return permit, err
	}
	if p.Action == "cleanup_staging" {
		return permit, fmt.Errorf("catalog_operation_required")
	}
	permit = oversync.CatalogRetirementPermit{OperationID: p.ID, PlanDigest: expected, Epoch: p.Epoch, DeploymentRevision: p.DeploymentRevision, CatalogRevision: p.CatalogRevision, RegistryFingerprint: p.RegistryFingerprint, CatalogFingerprint: p.CatalogFingerprint, TableIDs: p.TableIDs, Deadline: p.Deadline, MaxRows: p.MaxRows, Prune: p.Action == "prune_table_state", ExpireHistory: p.Action == "expire_history", RetentionWindow: p.RetentionWindow}
	return permit, nil
}

// Historical declarations are checked against their original fingerprint without
// inspecting today's contracted physical schema.
func validateArchivedGeneration(proof oversync.SchemaGenerationProof, raw []byte) error {
	var tables []oversync.GenerationTableContract
	if len(raw) == 0 || json.Unmarshal(raw, &tables) != nil || len(tables) == 0 {
		return fmt.Errorf("generation_archive_invalid")
	}
	var registered []oversync.RegisteredTable
	generation := oversync.SchemaGeneration{ID: proof.ID}
	for _, table := range tables {
		registered = append(registered, oversync.RegisteredTable{Schema: table.Schema, Table: table.Table, SyncKeyColumns: table.SyncKeyColumns})
		generation.Tables = append(generation.Tables, oversync.GenerationTable{Schema: table.Schema, Table: table.Table, Columns: table.Columns, ForeignKeys: table.ForeignKeys})
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	if err != nil {
		return fmt.Errorf("generation_archive_invalid: %w", err)
	}
	compiled, _ := registry.Generation(proof.ID)
	if compiled.Fingerprint() != proof.Fingerprint || !reflect.DeepEqual(compiled.Tables(), tables) {
		return fmt.Errorf("generation_archive_identity_mismatch")
	}
	return nil
}

func (a rolloutLedgerAdapter) authorizePrivateRollout(ctx context.Context, r oversync.DatabaseReadinessReader) (RolloutPlan, string, error) {
	if a.retirement == nil {
		return RolloutPlan{}, "", fmt.Errorf("catalog_retirement_not_authorized")
	}
	if err := requireRolloutAuthority(ctx, r); err != nil {
		return RolloutPlan{}, "", err
	}
	p := *a.retirement
	expected, err := RolloutPlanDigest(p)
	if err != nil {
		return RolloutPlan{}, "", err
	}
	var stored RolloutPlan
	var storedDigest, state string
	if err = r.QueryRow(ctx, `SELECT plan,digest,state FROM app_evolution.rollout_operations WHERE operation_id=$1`, p.ID).Scan(&stored, &storedDigest, &state); err != nil {
		return RolloutPlan{}, "", err
	}
	actual, err := RolloutPlanDigest(stored)
	if err != nil || actual != expected || storedDigest != expected || state != "running" {
		return RolloutPlan{}, "", fmt.Errorf("catalog_operation_identity_mismatch")
	}
	report, err := inspectRollout(ctx, r, p)
	if err != nil {
		return RolloutPlan{}, "", err
	}
	if len(report.Blockers) != 0 {
		return RolloutPlan{}, "", fmt.Errorf("catalog_retirement_blocked: %v", report.Blockers)
	}
	return p, expected, nil
}
