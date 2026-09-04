package schemaevolution

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

type RolloutColumnStage int

const (
	RolloutColumnLedger RolloutColumnStage = 5 + iota
	RolloutColumnGenerationRetired
	RolloutColumnContracted
)

func personContractIdentity() oversync.MigrationIdentity {
	path := "rollout/migrations/0005_contract_person.sql"
	b, _ := rolloutSource.ReadFile(path)
	return oversync.MigrationIdentity{Version: 5, Filename: path, SHA256: digest(b), Transactional: true, Class: "contract"}
}

const contractedPersonConstraint = `CHECK (((name IS NOT NULL) AND (new_label IS NOT NULL) AND ((name COLLATE "C") ~ '^[A-Za-z0-9_]+$'::text) AND ((new_label COLLATE "C") ~ '^[A-Za-z0-9_]+$'::text)))|true`

func RolloutColumnConfiguration(stage RolloutColumnStage, ledger *oversync.GenerationLedger, qualification string) (*oversync.ServiceConfig, error) {
	if stage < RolloutColumnLedger || stage > RolloutColumnContracted || !rolloutDigestPattern.MatchString(qualification) {
		return nil, fmt.Errorf("invalid_rollout_column_target")
	}
	cfg, err := Configuration(4, int64(stage), true)
	if err != nil {
		return nil, err
	}
	cfg.BundleChangeWatch.Enabled = true

	if stage >= RolloutColumnGenerationRetired {
		cfg.SchemaGenerations = cfg.SchemaGenerations[1:]
		cfg.Readiness.Generations = cfg.Readiness.Generations[1:]
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	cfg.Readiness.Expected.RegistryFingerprint = registry.Fingerprint()
	cfg.Readiness.Expected.ApplicationCodeSHA256 = rolloutSourceDigest()
	cfg.Readiness.Expected.QualificationSHA256 = qualification
	if stage == RolloutColumnContracted {
		cfg.Readiness.Expected.Migrations = append(cfg.Readiness.Expected.Migrations, personContractIdentity())
		cfg.Readiness.Expected.Mechanisms = append(expectedMechanisms(1, ""), oversync.MechanismIdentity{Kind: "constraint", Identity: "main.upgrade_people.person_encoding", Definition: contractedPersonConstraint})
		cfg.Readiness.Expected.TransitionRevision = "contracted-person-v1"
		cfg.Readiness.Expected.TransitionPhase = "rollout_contracted"
		cfg.Readiness.Generations[0].Backfills = []oversync.BackfillRequirement{contractedPersonRequirement()}
	}
	cfg.Readiness.Adapter = rolloutColumnAdapter{stage: stage, ledgerAdapter: ledgerAdapter{target: true, revision: int64(stage), registry: registry.Fingerprint()}}
	if err = WithGenerationLedger(cfg, ledger); err != nil {
		return nil, err
	}
	return cfg, nil
}

type rolloutColumnAdapter struct {
	stage RolloutColumnStage
	ledgerAdapter
}

func (a rolloutColumnAdapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	if a.stage != RolloutColumnContracted {
		e, err := readRolloutDeploymentEvidence(ctx, r)
		if err != nil {
			return e, err
		}
		var cert oversync.BackfillCertificate
		if err = r.QueryRow(ctx, `SELECT certificate FROM app_evolution.rollout_certificates WHERE deployment_revision<=$1 ORDER BY deployment_revision DESC LIMIT 1`, int64(a.stage)).Scan(&cert); err != nil {
			return e, err
		}
		v, err := validateAllRows(ctx, r)
		if err != nil {
			return e, err
		}
		e.Certificates = []oversync.BackfillCertificate{cert}
		e.Validations = []oversync.InvariantValidation{v}
		return e, nil
	}
	e, err := readRolloutDeploymentEvidence(ctx, r)
	if err != nil {
		return e, err
	}
	var absent bool
	if err = r.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='main.upgrade_people'::regclass AND attname='legacy_name' AND NOT attisdropped)
 AND to_regprocedure('app_evolution.transition_person()') IS NULL AND to_regprocedure('app_evolution.convert_person(text,text,text,boolean)') IS NULL
 AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='main.upgrade_people'::regclass AND NOT tgisinternal AND tgname NOT IN ('aa_app_write_guard','oversync_bundle_capture_row','oversync_bundle_owner_guard','oversync_registered_truncate_guard'))`).Scan(&absent); err != nil {
		return e, err
	}
	if !absent {
		return e, fmt.Errorf("person_contract_inventory_mismatch")
	}
	var cert oversync.BackfillCertificate
	if err = r.QueryRow(ctx, `SELECT certificate FROM app_evolution.rollout_certificates WHERE deployment_revision=$1`, int64(a.stage)).Scan(&cert); err != nil {
		return e, err
	}
	v, err := validateContractedPeople(ctx, r)
	if err != nil {
		return e, err
	}
	e.Certificates = []oversync.BackfillCertificate{cert}
	e.Validations = []oversync.InvariantValidation{v}
	return e, nil
}

func readRolloutDeploymentEvidence(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var e oversync.ReadinessEvidence
	if err := r.QueryRow(ctx, `SELECT contract,generations FROM app_evolution.deployment WHERE singleton`).Scan(&e.Deployment, &e.Generations); err != nil {
		return e, err
	}
	rows, err := r.Query(ctx, `SELECT identity,state,started_at,COALESCE(completed_at,'0001-01-01'::timestamptz) FROM app_evolution.migrations ORDER BY version LIMIT 7`)
	if err != nil {
		return e, err
	}
	for rows.Next() {
		var m oversync.MigrationEvidence
		if err = rows.Scan(&m.Identity, &m.State, &m.StartedAt, &m.CompletedAt); err != nil {
			rows.Close()
			return e, err
		}
		e.Migrations = append(e.Migrations, m)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return e, err
	}
	for _, m := range e.Deployment.Mechanisms {
		actual, err := readTableMechanism(ctx, r, m)
		if err != nil {
			return e, err
		}
		e.Mechanisms = append(e.Mechanisms, actual)
	}
	return e, nil
}
func contractedPersonRequirement() oversync.BackfillRequirement {
	return oversync.BackfillRequirement{Job: "contracted-person", Revision: "1", AlgorithmSHA256: rolloutSourceDigest(), ValidationRevision: "surviving-encoding-v1"}
}
func validateContractedPeople(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.InvariantValidation, error) {
	result := oversync.InvariantValidation{Requirement: contractedPersonRequirement(), ResultSHA256: digest([]byte("surviving-encoding-v1:complete"))}
	if err := r.QueryRow(ctx, `SELECT COALESCE(max(sequence),0) FROM app_evolution.scope_inventory`).Scan(&result.CoverageWatermark); err != nil {
		return result, err
	}
	if err := r.QueryRow(ctx, `SELECT count(*) FROM sync.scope_state WHERE state_code=1`).Scan(&result.Violations); err != nil {
		return result, err
	}
	var scope *string
	var key *uuid.UUID
	for {
		rows, err := r.Query(ctx, `SELECT p._sync_scope_id,p.id,
 p.name IS NOT NULL AND p.new_label IS NOT NULL AND p.name COLLATE "C" ~ '^[A-Za-z0-9_]+$' AND p.new_label COLLATE "C" ~ '^[A-Za-z0-9_]+$'
 AND EXISTS(SELECT 1 FROM app_evolution.scope_inventory i WHERE i.scope_id=p._sync_scope_id AND NOT i.deleted)
 FROM main.upgrade_people p WHERE $1::text IS NULL OR (p._sync_scope_id,p.id)>($1,$2::uuid)
 ORDER BY p._sync_scope_id,p.id LIMIT 128`, scope, key)
		if err != nil {
			return result, err
		}
		count := 0
		for rows.Next() {
			var nextScope string
			var nextKey uuid.UUID
			var valid bool
			if err = rows.Scan(&nextScope, &nextKey, &valid); err != nil {
				rows.Close()
				return result, err
			}
			count++
			scope = &nextScope
			key = &nextKey
			if !valid {
				result.Violations++
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return result, err
		}
		if count < 128 || result.Violations > 0 {
			return result, nil
		}
	}
}

func contractRolloutPerson(ctx context.Context, tx pgx.Tx, p RolloutPlan, target *oversync.ServiceConfig) error {
	if p.Contract == nil || *p.Contract != personContractIdentity() {
		return fmt.Errorf("exact_contract_migration_required")
	}
	ledger, err := readGenerationLedger(ctx, tx)
	if err != nil {
		return err
	}
	if len(ledger.Entries) < 2 || ledger.Entries[0].Proof.ID != 1 || ledger.Entries[0].State != "retired" {
		return fmt.Errorf("person_generation_retirement_required")
	}
	if target.Readiness.Expected.Revision != int64(RolloutColumnContracted) || len(target.SchemaGenerations) != 1 || target.SchemaGenerations[0].ID != 3 {
		return fmt.Errorf("person_contract_target_mismatch")
	}
	data, err := rolloutSource.ReadFile(p.Contract.Filename)
	if err != nil {
		return err
	}
	if digest(data) != p.Contract.SHA256 {
		return fmt.Errorf("contract_migration_changed")
	}
	if _, err = tx.Exec(ctx, string(data)); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO app_evolution.migrations(version,identity,state,started_at,completed_at) VALUES($1,$2,'completed',transaction_timestamp(),transaction_timestamp())`, p.Contract.Version, p.Contract); err != nil {
		return err
	}
	v, err := validateContractedPeople(ctx, tx)
	if err != nil {
		return err
	}
	if v.Violations != 0 {
		return fmt.Errorf("contracted_person_invalid")
	}
	if err = publishContractCertificate(ctx, tx, p.ID, target.Readiness.Expected, v); err != nil {
		return err
	}
	return publishRolloutTarget(ctx, tx, target)
}

func prepareRolloutColumn(ctx context.Context, tx pgx.Tx, p RolloutPlan, target *oversync.ServiceConfig) error {
	if target.Readiness.Expected.Revision != int64(RolloutColumnLedger) || target.Readiness.Expected.TransitionPhase != "coexistence" {
		return fmt.Errorf("column_preparation_target_required")
	}
	// The old certificate still identifies its original job. A new maintenance
	// epoch does not erase that provenance, but needs fresh full-population evidence.
	var cert maintenanceCertificate
	if err := tx.QueryRow(ctx, `SELECT certificate FROM app_evolution.certificates ORDER BY certificate_id DESC LIMIT 1`).Scan(&cert); err != nil {
		return err
	}
	var matching bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.jobs WHERE job=$1 AND epoch=$2 AND census=$3 AND state='validated' AND algorithm=$4)
 AND NOT EXISTS(SELECT 1 FROM app_evolution.row_work) AND NOT EXISTS(SELECT 1 FROM app_evolution.batches WHERE state='pending')`, JobID, cert.Epoch, cert.Census, backfillRequirement.AlgorithmSHA256).Scan(&matching); err != nil {
		return err
	}
	if !matching || cert.TransitionPhase != "coexistence" {
		return fmt.Errorf("column_predecessor_not_certified")
	}
	v, err := validateAllRows(ctx, tx)
	if err != nil {
		return err
	}
	if v.Violations != 0 {
		return fmt.Errorf("column_rollout_population_invalid")
	}
	if err = publishContractCertificate(ctx, tx, p.ID, target.Readiness.Expected, v); err != nil {
		return err
	}
	return publishRolloutTarget(ctx, tx, target)
}
