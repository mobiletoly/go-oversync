package schemaevolution

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

//go:embed rollout_*.go rollout_control.sql rollout/migrations/*.sql cmd/rollout/*.go cmd/rollout-member/*.go
var rolloutSource embed.FS

func rolloutSourceDigest() string {
	var paths []string
	_ = fs.WalkDir(rolloutSource, ".", func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.IsDir() && !strings.HasSuffix(path, "_test.go") {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	var b strings.Builder
	for _, path := range paths {
		data, _ := rolloutSource.ReadFile(path)
		fmt.Fprintf(&b, "%d:%s%d:%s", len(path), path, len(data), data)
	}
	b.WriteString(tableSourceDigest())
	return digest([]byte(b.String()))
}

type RolloutTableStage int

const (
	RolloutTableLedger RolloutTableStage = 6 + iota
	RolloutTableGenerationRetired
	RolloutTableIdentityRetired
	RolloutTableContracted
)

// RolloutTableConfiguration supplies the integration authority's exact successive
// targets. Qualification is an explicit digest of executed evidence, never a
// library default. Each target requires fresh Bootstrap after publication.
func RolloutTableConfiguration(stage RolloutTableStage, ledger *oversync.GenerationLedger, qualification string) (*oversync.ServiceConfig, error) {
	if stage < RolloutTableLedger || stage > RolloutTableContracted || !rolloutDigestPattern.MatchString(qualification) {
		return nil, fmt.Errorf("invalid_rollout_table_target")
	}
	cfg, err := TableEvolutionConfiguration(TablesServing)
	if err != nil {
		return nil, err
	}
	cfg.BundleChangeWatch.Enabled = true

	if stage >= RolloutTableGenerationRetired {
		cfg.SchemaGenerations = cfg.SchemaGenerations[1:]
		cfg.Readiness.Generations = cfg.Readiness.Generations[1:]
	}
	if stage >= RolloutTableIdentityRetired {
		var active []oversync.RegisteredTable
		for _, table := range cfg.RegisteredTables {
			if table.Table != "legacy_notes" {
				active = append(active, table)
			}
		}
		cfg.RegisteredTables = active
		cfg.RegisteredTableCatalogRevision = 3
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	cfg.Readiness.Expected.Revision = int64(stage)
	cfg.Readiness.Expected.RegistryFingerprint = registry.Fingerprint()
	cfg.Readiness.Expected.ApplicationCodeSHA256 = rolloutSourceDigest()
	cfg.Readiness.Expected.QualificationSHA256 = qualification
	cfg.Readiness.Expected.TransitionPhase = tablePhaseName(TablesServing)
	if stage == RolloutTableContracted {
		cfg.Readiness.Expected.TransitionPhase = "rollout_contracted"
		cfg.Readiness.Expected.TransitionRevision = "retired-notes-v1"
		cfg.Readiness.Generations[0].Backfills = []oversync.BackfillRequirement{rolloutGraphRequirement()}
		var mechanisms []oversync.MechanismIdentity
		for _, m := range cfg.Readiness.Expected.Mechanisms {
			if m.Identity != "main.legacy_notes.aa_app_write_guard" {
				mechanisms = append(mechanisms, m)
			}
		}
		cfg.Readiness.Expected.Mechanisms = mechanisms
		cfg.Readiness.Expected.Migrations = append(cfg.Readiness.Expected.Migrations, notesContractIdentity())
	}
	cfg.Readiness.Adapter = rolloutTableAdapter{stage: stage, registry: registry.Fingerprint()}
	if err = WithGenerationLedger(cfg, ledger); err != nil {
		return nil, err
	}
	return cfg, nil
}

func notesContractIdentity() oversync.MigrationIdentity {
	path := "rollout/migrations/0004_contract_notes.sql"
	b, _ := rolloutSource.ReadFile(path)
	return oversync.MigrationIdentity{Version: 4, Filename: path, SHA256: digest(b), Transactional: true, Class: "contract"}
}

func retiredNotesCatalogFingerprint() string {
	entries := []any{[]any{"1", "main", "legacy_notes", "id", "uuid", "retired", "1", "3"}, []any{"2", "main", "people", "id", "uuid", "active", "1", nil}, []any{"3", "main", "attachments", "id", "uuid", "active", "2", nil}, []any{"4", "main", "projects", "id", "uuid", "active", "2", nil}}
	b, _ := json.Marshal([]any{"oversync.registered-table-catalog.v1", "3", "5", entries})
	return digest(b)
}

type rolloutTableAdapter struct {
	stage              RolloutTableStage
	deploymentRevision int64
	registry           string
}

func (a rolloutTableAdapter) Admit(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	return (ledgerAdapter{revision: a.revision(), registry: a.registry}).Admit(ctx, r)
}
func (a rolloutTableAdapter) ValidateCatalogReadiness(ctx context.Context, r oversync.DatabaseReadinessReader, revision int64, fp string) error {
	wantRevision := int64(2)
	wantFP := tableCatalogFingerprint(2)
	if a.stage >= RolloutTableIdentityRetired {
		wantRevision = 3
		wantFP = retiredNotesCatalogFingerprint()
	}
	if revision != wantRevision || fp != wantFP {
		return fmt.Errorf("rollout_table_catalog_mismatch")
	}
	return nil
}
func (a rolloutTableAdapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var e oversync.ReadinessEvidence
	if err := r.QueryRow(ctx, `SELECT contract,generations FROM app_evolution.deployment WHERE singleton`).Scan(&e.Deployment, &e.Generations); err != nil {
		return e, err
	}
	if e.Deployment.Revision != a.revision() || e.Deployment.RegistryFingerprint != a.registry {
		return e, fmt.Errorf("rollout_table_deployment_mismatch")
	}
	rows, err := r.Query(ctx, `SELECT identity,state,started_at,COALESCE(completed_at,'0001-01-01'::timestamptz) FROM app_evolution.migrations ORDER BY version LIMIT 6`)
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
	if err = validateTableMechanismInventory(ctx, r, TablesServing); err != nil {
		return e, err
	}
	cert, validation, err := a.graphEvidence(ctx, r)
	if err != nil {
		return e, err
	}
	e.Certificates = []oversync.BackfillCertificate{cert}
	e.Validations = []oversync.InvariantValidation{validation}
	if a.stage == RolloutTableContracted {
		var absent bool
		if err = r.QueryRow(ctx, `SELECT to_regclass('main.legacy_notes') IS NULL`).Scan(&absent); err != nil {
			return e, err
		}
		if !absent {
			return e, fmt.Errorf("contract_relation_still_present")
		}
	}
	return e, nil
}

func rolloutGraphRequirement() oversync.BackfillRequirement {
	return oversync.BackfillRequirement{Job: "rollout-surviving-graph", Revision: "1", AlgorithmSHA256: rolloutSourceDigest(), ValidationRevision: "retired-notes-v1"}
}
func validateRolloutGraph(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.InvariantValidation, error) {
	v, err := validateTablePopulation(ctx, r, false)
	if err != nil {
		return v, err
	}
	v.Requirement = rolloutGraphRequirement()
	v.ResultSHA256 = digest([]byte("retired-notes-v1:" + v.ResultSHA256))
	return v, nil
}
func (a rolloutTableAdapter) graphEvidence(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.BackfillCertificate, oversync.InvariantValidation, error) {
	var cert oversync.BackfillCertificate
	var validation oversync.InvariantValidation
	if a.stage == RolloutTableContracted {
		if err := r.QueryRow(ctx, `SELECT certificate FROM app_evolution.rollout_certificates WHERE deployment_revision=$1`, int64(a.stage)).Scan(&cert); err != nil {
			return cert, validation, err
		}
		v, err := validateRolloutGraph(ctx, r)
		return cert, v, err
	}
	if err := r.QueryRow(ctx, `SELECT certificate FROM app_evolution.table_run WHERE singleton AND phase='serving' AND source_digest=$1`, tableSourceDigest()).Scan(&cert); err != nil {
		return cert, validation, err
	}
	v, err := validateTablePopulation(ctx, r, false)
	return cert, v, err
}

func (a rolloutTableAdapter) revision() int64 {
	if a.deploymentRevision != 0 {
		return a.deploymentRevision
	}
	return int64(a.stage)
}
