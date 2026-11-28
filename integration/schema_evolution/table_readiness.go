package schemaevolution

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/mobiletoly/go-oversync/oversync"
)

//go:embed table_evolution.go table_readiness.go table_worker.go table_cleanup.go table_population.go table_validation.go table_repair.go table_evolution/*.sql table_evolution/migrations/*.sql maintenance.go migrations.go control.sql
var tableSource embed.FS

func tableSourceDigest() string {
	var paths []string
	_ = fs.WalkDir(tableSource, ".", func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	var framed strings.Builder
	for _, path := range paths {
		b, _ := tableSource.ReadFile(path)
		fmt.Fprintf(&framed, "%d:%s%d:%s", len(path), path, len(b), b)
	}
	framed.WriteString(applicationSourceDigest("application-deployment-v1"))
	return digest([]byte(framed.String()))
}

//go:embed table_evolution/qualification.json
var tableQualification []byte

func tableQualificationDigest() string { return digest(tableQualification) }
func tableBackfillRequirement() oversync.BackfillRequirement {
	return oversync.BackfillRequirement{Job: "table-population", Revision: "1", AlgorithmSHA256: tableSourceDigest(), ValidationRevision: "table-population-v2"}
}

func tableMechanisms(phase TablePhase) []oversync.MechanismIdentity {
	result := []oversync.MechanismIdentity{expectedMechanisms(1, "")[0], expectedMechanisms(1, "")[2]}
	names := []string{"people", "legacy_notes"}
	if phase >= TablesAppend {
		names = append(names, "projects", "attachments")
	}
	for _, name := range names {
		result = append(result, oversync.MechanismIdentity{Kind: "trigger", Identity: "main." + name + ".aa_app_write_guard", Definition: "62|O|app_evolution.guard_write()||0||"})
	}
	if phase >= TablesBackfill {
		result = append(result,
			oversync.MechanismIdentity{Kind: "table_fk", Identity: "main.people.people_project", Definition: "FOREIGN KEY (_sync_scope_id, project_id) REFERENCES main.projects(_sync_scope_id, id) ON DELETE SET NULL (project_id) DEFERRABLE INITIALLY DEFERRED|true"},
			oversync.MechanismIdentity{Kind: "table_fk", Identity: "main.attachments.attachment_person", Definition: "FOREIGN KEY (_sync_scope_id, person_id) REFERENCES main.people(_sync_scope_id, id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED|true"})
	}
	return result
}

type tableAdapter struct {
	phase    TablePhase
	revision int64
	registry string
}

func (a tableAdapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var e oversync.ReadinessEvidence
	if err := r.QueryRow(ctx, `SELECT contract,generations FROM app_evolution.deployment WHERE singleton`).Scan(&e.Deployment, &e.Generations); err != nil {
		return e, err
	}
	if e.Deployment.Revision != int64(a.phase) || e.Deployment.RegistryFingerprint != a.registry {
		return e, fmt.Errorf("table deployment changed")
	}
	rows, err := r.Query(ctx, `SELECT identity,state,started_at,COALESCE(completed_at,'0001-01-01'::timestamptz) FROM app_evolution.migrations ORDER BY version LIMIT 5`)
	if err != nil {
		return e, err
	}
	for rows.Next() {
		var m oversync.MigrationEvidence
		if err := rows.Scan(&m.Identity, &m.State, &m.StartedAt, &m.CompletedAt); err != nil {
			rows.Close()
			return e, err
		}
		e.Migrations = append(e.Migrations, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return e, err
	}
	for _, m := range e.Deployment.Mechanisms {
		actual, err := readTableMechanism(ctx, r, m)
		if err != nil {
			return e, err
		}
		e.Mechanisms = append(e.Mechanisms, actual)
	}
	if err := validateTableMechanismInventory(ctx, r, a.phase); err != nil {
		return e, err
	}
	if a.phase == TablesServing {
		var cert oversync.BackfillCertificate
		var initial bool
		if err := r.QueryRow(ctx, `SELECT certificate,phase='populated' FROM app_evolution.table_run WHERE singleton AND phase IN ('populated','serving') AND source_digest=$1`, tableSourceDigest()).Scan(&cert, &initial); err != nil {
			return e, err
		}
		// Certification alone cannot relax completion checks. Only the durable
		// first reopen permits ordinary coexistence to change the populated rows.
		validation, err := validateTablePopulation(ctx, r, initial)
		if err != nil {
			return e, err
		}
		e.Certificates = []oversync.BackfillCertificate{cert}
		e.Validations = []oversync.InvariantValidation{validation}
	}
	return e, nil
}

func readTableMechanism(ctx context.Context, r oversync.DatabaseReadinessReader, m oversync.MechanismIdentity) (oversync.MechanismIdentity, error) {
	if m.Kind != "table_fk" {
		return readMechanism(ctx, r, m)
	}
	parts := strings.Split(m.Identity, ".")
	if len(parts) != 3 {
		return m, fmt.Errorf("invalid FK identity")
	}
	err := r.QueryRow(ctx, `SELECT pg_get_constraintdef(oid)||'|'||convalidated::text FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2`, parts[0]+"."+parts[1], parts[2]).Scan(&m.Definition)
	return m, err
}

func validateTableMechanismInventory(ctx context.Context, r oversync.DatabaseReadinessReader, phase TablePhase) error {
	var unexpected, fks int
	err := r.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='main' AND NOT t.tgisinternal AND t.tgname NOT IN ('aa_app_write_guard','oversync_bundle_capture_row','oversync_bundle_owner_guard','oversync_registered_truncate_guard'))+
 (SELECT count(*) FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='main' AND r.rulename<>'_RETURN'),
 (SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='main' AND c.contype='f')`).Scan(&unexpected, &fks)
	if err != nil {
		return err
	}
	want := 0
	if phase >= TablesBackfill {
		want = 2
	}
	if unexpected != 0 || fks != want {
		return fmt.Errorf("table mechanism inventory mismatch")
	}
	return nil
}

func (a tableAdapter) ValidateCatalogReadiness(ctx context.Context, r oversync.DatabaseReadinessReader, revision int64, fp string) error {
	if revision != a.revision || fp != tableCatalogFingerprint(a.revision) {
		return fmt.Errorf("table target catalog mismatch")
	}
	return nil
}

func (a tableAdapter) Admit(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	if a.phase == TablesCleanup || a.phase == TablesAppend {
		return fmt.Errorf("maintenance_in_progress")
	}
	if a.phase == TablesBackfill {
		if _, ok := ctx.Value(migrationContextKey{}).(migrationCapability); !ok {
			return fmt.Errorf("maintenance_in_progress")
		}
	}
	return (ledgerAdapter{revision: int64(a.phase), registry: a.registry}).Admit(ctx, r)
}

func (a tableAdapter) AuthorizeStagingCleanup(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.StagingCleanupPermit, error) {
	var permit oversync.StagingCleanupPermit
	if a.phase != TablesCleanup {
		return permit, fmt.Errorf("cleanup phase required")
	}
	if err := lockAdmission(ctx, r, false); err != nil {
		return permit, err
	}
	c, ok := ctx.Value(migrationContextKey{}).(migrationCapability)
	if !ok {
		return permit, fmt.Errorf("cleanup capability required")
	}
	if err := verifyWorker(ctx, r, c); err != nil {
		return permit, err
	}
	run, err := loadTableRun(ctx, r)
	if err != nil {
		return permit, err
	}
	if err := run.check(c, "cleanup"); err != nil {
		return permit, err
	}
	var closed bool
	var revision int64
	var registry string
	if err := r.QueryRow(ctx, `SELECT b.closed,d.revision,d.contract->>'RegistryFingerprint' FROM app_evolution.barrier b CROSS JOIN app_evolution.deployment d WHERE b.singleton AND d.singleton AND b.revision=$1`, c.epoch).Scan(&closed, &revision, &registry); err != nil {
		return permit, err
	}
	if !closed || revision != int64(a.phase) || registry != a.registry {
		return permit, fmt.Errorf("cleanup deployment mismatch")
	}
	return oversync.StagingCleanupPermit{Epoch: c.epoch, Worker: c.owner.String(), PlanID: run.ID.String(), DeploymentRevision: revision, RegistryFingerprint: registry, CatalogRevision: 1, CatalogFingerprint: tableCatalogFingerprint(1), Deadline: run.Bounds.Deadline, QueryTimeout: run.Bounds.QueryTimeout, MaxSessions: run.Bounds.CleanupSessions, MaxRows: run.Bounds.CleanupRows, MaxEvidenceBytes: run.Bounds.CleanupBytes}, nil
}
