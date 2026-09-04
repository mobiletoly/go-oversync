package schemaevolution

import (
	"context"
	_ "embed"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Qualification evidence identifies the executed focused qualification artifact.
// Bootstrap compares identities; it cannot re-prove application behavior.
//
//go:embed qualification-evidence.json
var qualificationEvidence []byte

const JobID = "person-encoding"
const JobRevision = "3"
const TransitionRevision = "person-encoding-v1"

var backfillRequirement = oversync.BackfillRequirement{Job: JobID, Revision: JobRevision, AlgorithmSHA256: applicationSourceDigest("backfill-sql-v3"), ValidationRevision: "encoding-invariant-v1"}

func Configuration(through, revision int64, target bool) (*oversync.ServiceConfig, error) {
	if through < 1 || through > 4 || revision < 1 || (target && through != 4) {
		return nil, fmt.Errorf("invalid deployment boundary")
	}
	table := oversync.RegisteredTable{Schema: "main", Table: "upgrade_people", SyncKeyColumns: []string{"id"}}
	old := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{{Schema: "main", Table: "upgrade_people", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}, {Name: "legacy_name", WireType: oversync.GenerationWireTypeText}}}}}
	next := oversync.SchemaGeneration{ID: 3, Tables: []oversync.GenerationTable{{Schema: "main", Table: "upgrade_people", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}, {Name: "name", WireType: oversync.GenerationWireTypeText}, {Name: "new_label", WireType: oversync.GenerationWireTypeText}}}}}
	cfg := &oversync.ServiceConfig{AppName: "schema-evolution", RegisteredTables: []oversync.RegisteredTable{table}, SchemaGenerations: []oversync.SchemaGeneration{old}, RegisteredTableCatalogRevision: 1, ReservedServerSourceIDs: []string{"schema-evolution-backfill", "schema-evolution-writer"}}
	if target {
		cfg.SchemaGenerations = append(cfg.SchemaGenerations, next)
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return nil, err
	}
	manifest, err := Manifest(migrationFiles)
	if err != nil {
		return nil, err
	}
	phase := "baseline"
	if through >= 2 {
		phase = "maintaining"
	}
	if through == 4 {
		phase = "coexistence"
	}
	mechanisms := expectedMechanisms(through, phase)
	d := oversync.DeploymentContract{WriteRejections: []oversync.ApplicationWriteRejection{{SQLState: "P6001", Category: "transition_input_invalid"}, {SQLState: "P6002", Category: "transition_input_conflict"}}, AuthorityID: "mobile-flow-postgresql", Revision: revision, RegistryFingerprint: registry.Fingerprint(), Migrations: manifest[:through], TransitionRevision: TransitionRevision, TransitionPhase: phase, Mechanisms: mechanisms, ApplicationCodeSHA256: applicationSourceDigest("application-deployment-v1"), QualificationSHA256: digest(qualificationEvidence)}
	readiness := &oversync.ReadinessConfig{AuthorityLock: authorityLock, Expected: d}
	for _, id := range registry.GenerationIDs() {
		contract, _ := registry.Generation(id)
		g := oversync.GenerationReadiness{Proof: oversync.SchemaGenerationProof{ID: id, Fingerprint: contract.Fingerprint()}, MinimumMigrationVersion: 1}
		if id == 3 {
			g.MinimumMigrationVersion = 4
			g.Backfills = []oversync.BackfillRequirement{backfillRequirement}
		}
		readiness.Generations = append(readiness.Generations, g)
	}
	readiness.Adapter = ledgerAdapter{target: target, revision: revision, registry: registry.Fingerprint()}
	cfg.Readiness = readiness
	return cfg, nil
}

func expectedMechanisms(through int64, phase string) []oversync.MechanismIdentity {
	control, _ := migrationFiles.ReadFile("control.sql")
	guard := strings.Split(string(control), "$$")[1]
	result := []oversync.MechanismIdentity{
		{Kind: "function", Identity: "app_evolution.guard_write()", Definition: guard + "\nplpgsql|trigger|v|false|false|false|u|"},
		{Kind: "trigger", Identity: "main.upgrade_people.aa_app_write_guard", Definition: "62|O|app_evolution.guard_write()||0||"},
		{Kind: "trigger", Identity: "app_evolution.scope_inventory.aa_app_scope_guard", Definition: "62|O|app_evolution.guard_write()||0||"},
	}
	if through < 2 {
		return result
	}
	sql, _ := migrationFiles.ReadFile("migrations/0002_expand.sql")
	bodies := strings.Split(string(sql), "$$")
	result = append(result,
		oversync.MechanismIdentity{Kind: "function", Identity: "app_evolution.convert_person(text,text,text,boolean)", Definition: bodies[1] + "\nplpgsql|text[]|i|false|false|false|u|"},
		oversync.MechanismIdentity{Kind: "function", Identity: "app_evolution.transition_person()", Definition: bodies[3] + "\nplpgsql|trigger|v|false|false|false|u|"},
		oversync.MechanismIdentity{Kind: "trigger", Identity: "main.upgrade_people.zz_app_person_transition", Definition: "23|O|app_evolution.transition_person()|" + hex.EncodeToString(append([]byte(phase), 0)) + "|0||"},
	)
	if through == 4 {
		// pg_get_constraintdef's exact PostgreSQL 16 rendering is checked below.
		result = append(result, oversync.MechanismIdentity{Kind: "constraint", Identity: "main.upgrade_people.person_encoding", Definition: encodingConstraintDefinition + "|true"})
	}
	return result
}

const encodingConstraintDefinition = `CHECK (((legacy_name IS NOT NULL) AND (name IS NOT NULL) AND (new_label IS NOT NULL) AND ((name COLLATE "C") ~ '^[A-Za-z0-9_]+$'::text) AND ((new_label COLLATE "C") ~ '^[A-Za-z0-9_]+$'::text) AND (legacy_name = ((name || '|'::text) || new_label))))`

type ledgerAdapter struct {
	target   bool
	revision int64
	registry string
}

func (a ledgerAdapter) Inspect(ctx context.Context, r oversync.DatabaseReadinessReader) (oversync.ReadinessEvidence, error) {
	var e oversync.ReadinessEvidence
	if err := r.QueryRow(ctx, `SELECT contract,generations FROM app_evolution.deployment WHERE singleton`).Scan(&e.Deployment, &e.Generations); err != nil {
		return e, err
	}
	rows, err := r.Query(ctx, `SELECT identity,state,started_at,completed_at,failed_at FROM app_evolution.migrations ORDER BY version LIMIT 10001`)
	if err != nil {
		return e, err
	}
	for rows.Next() {
		var m oversync.MigrationEvidence
		var completedAt, failedAt pgtype.Timestamptz
		if err := rows.Scan(&m.Identity, &m.State, &m.StartedAt, &completedAt, &failedAt); err != nil {
			rows.Close()
			return e, err
		}
		if completedAt.Valid {
			m.CompletedAt = completedAt.Time
		}
		if failedAt.Valid {
			m.FailedAt = failedAt.Time
		}
		e.Migrations = append(e.Migrations, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return e, err
	}
	if len(e.Deployment.Mechanisms) > 0 {
		var extras int
		if err := r.QueryRow(ctx, `SELECT count(*) FROM (
 SELECT 1 FROM pg_trigger WHERE tgrelid='main.upgrade_people'::regclass AND NOT tgisinternal AND tgname NOT IN ('zz_app_person_transition','oversync_bundle_capture_row','oversync_bundle_owner_guard','oversync_registered_truncate_guard','aa_app_write_guard')
 UNION ALL
 SELECT 1 FROM pg_trigger WHERE tgrelid='app_evolution.scope_inventory'::regclass AND tgname NOT IN ('aa_app_scope_guard')
) unexpected`).Scan(&extras); err != nil {
			return e, err
		}
		if extras != 0 {
			return e, &oversync.ReadinessError{Category: oversync.TransitionMechanismMismatch}
		}
	}
	for _, expected := range e.Deployment.Mechanisms {
		actual, err := readMechanism(ctx, r, expected)
		if err != nil {
			return e, &oversync.ReadinessError{Category: oversync.TransitionMechanismMismatch, Err: err}
		}
		e.Mechanisms = append(e.Mechanisms, actual)
	}
	if a.target {
		if err := verifyCertificateFence(ctx, r); err != nil {
			return e, err
		}
		var cert oversync.BackfillCertificate
		err = r.QueryRow(ctx, `SELECT certificate FROM app_evolution.certificates ORDER BY certificate_id DESC LIMIT 1`).Scan(&cert)
		if err != nil {
			return e, &oversync.ReadinessError{Category: oversync.BackfillIncomplete, Err: err}
		}
		e.Certificates = []oversync.BackfillCertificate{cert}
		validation, err := validateAllRows(ctx, r)
		if err != nil {
			return e, err
		}
		e.Validations = []oversync.InvariantValidation{validation}
		var unresolved int64
		err = r.QueryRow(ctx, `SELECT (SELECT count(*) FROM app_evolution.row_work)+(SELECT count(*) FROM app_evolution.batches WHERE state='pending')+(SELECT count(*) FROM app_evolution.jobs WHERE state<>'validated')`).Scan(&unresolved)
		if err != nil {
			return e, err
		}
		if unresolved != 0 {
			return e, &oversync.ReadinessError{Category: oversync.BackfillIncomplete}
		}
	}
	return e, nil
}

func readMechanism(ctx context.Context, r oversync.DatabaseReadinessReader, m oversync.MechanismIdentity) (oversync.MechanismIdentity, error) {
	var err error
	switch m.Kind {
	case "function":
		err = r.QueryRow(ctx, `SELECT p.prosrc || E'\n' || l.lanname || '|' || p.prorettype::regtype::text || '|' || p.provolatile::text || '|' || p.prosecdef::text::boolean::text || '|' || p.proisstrict::text::boolean::text || '|' || p.proleakproof::text::boolean::text || '|' || p.proparallel::text || '|' || COALESCE(array_to_string(p.proconfig,','),'') FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=$1::regprocedure`, m.Identity).Scan(&m.Definition)
		// PostgreSQL bool text is true/false; use those exact expected values.
	case "trigger":
		err = r.QueryRow(ctx, `SELECT tgtype::text||'|'||tgenabled::text||'|'||tgfoid::regprocedure::text||'|'||encode(tgargs,'hex')||'|'||tgconstraint::text||'|'||tgattr::text||'|'||COALESCE(pg_get_expr(tgqual,tgrelid),'') FROM pg_trigger WHERE tgrelid=$1::regclass AND tgname=$2 AND NOT tgisinternal`, strings.Join(strings.Split(m.Identity, ".")[:2], "."), strings.Split(m.Identity, ".")[2]).Scan(&m.Definition)
	case "constraint":
		err = r.QueryRow(ctx, `SELECT pg_get_constraintdef(oid)||'|'||convalidated::text FROM pg_constraint WHERE conrelid='main.upgrade_people'::regclass AND conname='person_encoding'`).Scan(&m.Definition)
	default:
		return m, fmt.Errorf("unknown mechanism kind")
	}
	return m, err
}

func (a *Authority) Publish(ctx context.Context, cfg *oversync.ServiceConfig) error {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx = bounded

	if a.conn == nil || cfg.Readiness == nil {
		return fmt.Errorf("authority/config unavailable")
	}
	d := cfg.Readiness.Expected
	var position int64
	if err := a.conn.QueryRow(ctx, `SELECT COALESCE(max(version),0) FROM app_evolution.migrations WHERE state='completed'`).Scan(&position); err != nil {
		return err
	}
	if len(d.Migrations) == 0 || d.Migrations[len(d.Migrations)-1].Version != position {
		return fmt.Errorf("deployment migration mismatch")
	}
	var enabled []oversync.GenerationEnablement
	for _, g := range cfg.Readiness.Generations {
		enabled = append(enabled, oversync.GenerationEnablement{Requirement: g, DeploymentRevision: d.Revision, Enabled: true})
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return err
	}
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO app_evolution.deployment(singleton,revision,contract,generations) VALUES(true,$1,$2,$3) ON CONFLICT(singleton) DO UPDATE SET revision=excluded.revision,contract=excluded.contract,generations=excluded.generations WHERE app_evolution.deployment.revision<excluded.revision`, d.Revision, d, enabled)
		if err != nil {
			return err
		}
		_ = tag // An exact lost-response retry is validated against durable evidence below.
		evidence, err := cfg.Readiness.Adapter.Inspect(ctx, tx)
		if err != nil {
			return err
		}
		return oversync.ValidateApplicationReadiness(cfg.Readiness, registry, evidence)
	})
}

// Reopen occurs only after every fresh service reports the prepared exact
// deployment. The controller must also fence automatic restarts of old binaries.
func ReopenServing(ctx context.Context, pool *pgxpool.Pool, services []*oversync.SyncService) error {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx = bounded
	if err := validateRecoveryAdmission(ctx, pool, nil); err != nil {
		return err
	}

	if len(services) == 0 {
		return fmt.Errorf("fresh serving membership required")
	}
	var d oversync.DeploymentContract
	if err := pool.QueryRow(ctx, `SELECT contract FROM app_evolution.deployment WHERE singleton`).Scan(&d); err != nil {
		return err
	}
	for _, service := range services {
		report := service.Readiness()
		if !report.Ready || report.DeploymentRevision != d.Revision || report.RegistryFingerprint != d.RegistryFingerprint {
			return fmt.Errorf("fresh process deployment mismatch")
		}
	}
	if d.TransitionPhase == "maintaining" || d.TransitionPhase == "tables_cleanup" || d.TransitionPhase == "tables_append" || d.TransitionPhase == "tables_backfill" {
		return fmt.Errorf("maintenance must remain closed until post-switch certification")
	}
	return reopenMaintenance(ctx, pool, d)

}
