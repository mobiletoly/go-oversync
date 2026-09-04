package schemaevolution

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// TablePhase describes one immutable deployment within the same closed window.
type TablePhase int

const (
	TablesBaseline TablePhase = iota + 1
	TablesCleanup
	TablesAppend
	TablesBackfill
	TablesServing
)

//go:embed table_evolution/*.sql table_evolution/migrations/*.sql
var tableFiles embed.FS

func tablePhaseName(p TablePhase) string {
	return map[TablePhase]string{TablesBaseline: "tables_baseline", TablesCleanup: "tables_cleanup", TablesAppend: "tables_append", TablesBackfill: "tables_backfill", TablesServing: "tables_serving"}[p]
}
func tableMigrationPosition(p TablePhase) int {
	if p >= TablesBackfill {
		return 3
	}
	if p >= TablesAppend {
		return 2
	}
	return 1
}

func tableManifest() []oversync.MigrationIdentity {
	result := make([]oversync.MigrationIdentity, 0, 3)
	for i, name := range []string{"0001_people.sql", "0002_expand.sql", "0003_foreign_keys.sql"} {
		path := "table_evolution/migrations/" + name
		data, _ := tableFiles.ReadFile(path)
		class := "expand"
		if i == 2 {
			class = "constraint"
		}
		result = append(result, oversync.MigrationIdentity{Version: int64(i + 1), Filename: path, SHA256: digest(data), Transactional: true, Class: class})
	}
	return result
}

// ApplyTableEvolution reuses the application authority lock and ledger. It does
// not run the separate transition column fixture's migrations or its version-4 policy.
func (a *Authority) ApplyTableEvolution(ctx context.Context, phase TablePhase) error {
	if a == nil || a.conn == nil || tablePhaseName(phase) == "" {
		return fmt.Errorf("table authority/phase required")
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if phase >= TablesAppend {
		if err := requireTablePhaseAdvance(ctx, a.conn, phase); err != nil {
			return err
		}
	}
	for _, m := range tableManifest()[:tableMigrationPosition(phase)] {
		if err := a.applyTableMigration(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (a *Authority) applyTableMigration(ctx context.Context, m oversync.MigrationIdentity) error {
	var stored oversync.MigrationIdentity
	var state string
	err := a.conn.QueryRow(ctx, `SELECT identity,state FROM app_evolution.migrations WHERE version=$1`, m.Version).Scan(&stored, &state)
	if err == nil {
		if stored != m {
			return fmt.Errorf("table migration checksum drift")
		}
		if state == "completed" {
			return nil
		}
	} else if err != pgx.ErrNoRows {
		return err
	}
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if m.Version == 1 {
			data, _ := tableFiles.ReadFile("table_evolution/control.sql")
			if _, err := tx.Exec(ctx, string(data)); err != nil {
				return err
			}
		}
		data, err := fs.ReadFile(tableFiles, m.Filename)
		if err != nil {
			return err
		}
		if digest(data) != m.SHA256 {
			return fmt.Errorf("table migration bytes changed")
		}
		if _, err = tx.Exec(ctx, string(data)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO app_evolution.migrations(version,identity,state,started_at,completed_at) VALUES($1,$2,'completed',transaction_timestamp(),transaction_timestamp())`, m.Version, m)
		return err
	})
}

func requireTablePhaseAdvance(ctx context.Context, r oversync.DatabaseReadinessReader, p TablePhase) error {
	run, err := loadTableRun(ctx, r)
	if err != nil {
		return err
	}
	if run.Source != tableSourceDigest() || !time.Now().Before(run.Bounds.Deadline) {
		return fmt.Errorf("table phase advance source/deadline mismatch")
	}
	if err := requireEmptyTableStaging(ctx, r); err != nil {
		return err
	}
	var closed, requested, workerStopped bool
	var phase string
	if err := r.QueryRow(ctx, `SELECT b.closed,m.requested,NOT EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=m.worker_pid AND l.locktype='advisory' AND l.classid=1869833075 AND l.objid=7 AND l.objsubid=2 AND l.granted),r.phase FROM app_evolution.barrier b CROSS JOIN app_evolution.maintenance m CROSS JOIN app_evolution.table_run r WHERE b.singleton AND m.singleton AND r.singleton AND b.revision=m.epoch AND r.epoch=m.epoch`).Scan(&closed, &requested, &workerStopped, &phase); err != nil {
		return err
	}
	if !closed || !requested || !workerStopped {
		return fmt.Errorf("closed drain and stopped table worker required")
	}
	if phase != "cleaned" && phase != "populated" {
		return fmt.Errorf("table cleanup incomplete")
	}
	if p >= TablesBackfill {
		var revision int64
		var fp string
		if err := r.QueryRow(ctx, `SELECT catalog_revision,catalog_fingerprint FROM sync.meta WHERE singleton_key`).Scan(&revision, &fp); err != nil {
			return err
		}
		if revision != 2 || fp != tableCatalogFingerprint(2) {
			return fmt.Errorf("append parent before installing FK")
		}
	}
	return nil
}

func tableCatalogFingerprint(revision int64) string {
	entries := []any{[]any{"1", "main", "legacy_notes", "id", "uuid", "active", "1", nil}, []any{"2", "main", "people", "id", "uuid", "active", "1", nil}}
	if revision == 2 {
		entries = append(entries, []any{"3", "main", "attachments", "id", "uuid", "active", "2", nil}, []any{"4", "main", "projects", "id", "uuid", "active", "2", nil})
	}
	data, _ := json.Marshal([]any{"oversync.registered-table-catalog.v1", strconv.FormatInt(revision, 10), strconv.Itoa(len(entries) + 1), entries})
	return digest(data)
}

// TableEvolutionConfiguration is a complete code-first old/target fixture.
// Every phase has a distinct deployment revision; only final serving enables g2.
func TableEvolutionConfiguration(phase TablePhase) (*oversync.ServiceConfig, error) {
	if tablePhaseName(phase) == "" {
		return nil, fmt.Errorf("unknown table phase")
	}
	id := oversync.GenerationColumn{Name: "id", WireType: oversync.GenerationWireTypeUUID}
	people := oversync.GenerationTable{Schema: "main", Table: "people", Columns: []oversync.GenerationColumn{id, {Name: "name", WireType: oversync.GenerationWireTypeText}}}
	notes := oversync.GenerationTable{Schema: "main", Table: "legacy_notes", Columns: []oversync.GenerationColumn{id, {Name: "body", WireType: oversync.GenerationWireTypeText}}}
	registered := []oversync.RegisteredTable{{Schema: "main", Table: "people", SyncKeyColumns: []string{"id"}}, {Schema: "main", Table: "legacy_notes", SyncKeyColumns: []string{"id"}}}
	gens := []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{people, notes}}}
	revision := int64(1)
	if phase >= TablesAppend {
		revision = 2
		for _, name := range []string{"projects", "attachments"} {
			registered = append(registered, oversync.RegisteredTable{Schema: "main", Table: name, SyncKeyColumns: []string{"id"}})
		}
	}
	if phase == TablesServing {
		people.Columns = append(append([]oversync.GenerationColumn(nil), people.Columns...), oversync.GenerationColumn{Name: "project_id", WireType: oversync.GenerationWireTypeUUID, Nullable: true})
		people.ForeignKeys = []oversync.GenerationForeignKey{tableFK("project_id", "projects", oversync.GenerationForeignKeySetNull)}
		gens = append(gens, oversync.SchemaGeneration{ID: 2, Tables: []oversync.GenerationTable{people, {Schema: "main", Table: "projects", Columns: []oversync.GenerationColumn{id, {Name: "title", WireType: oversync.GenerationWireTypeText}}}, {Schema: "main", Table: "attachments", Columns: []oversync.GenerationColumn{id, {Name: "person_id", WireType: oversync.GenerationWireTypeUUID, Nullable: true}, {Name: "body", WireType: oversync.GenerationWireTypeText}}, ForeignKeys: []oversync.GenerationForeignKey{tableFK("person_id", "people", oversync.GenerationForeignKeyCascade)}}}})
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, gens)
	if err != nil {
		return nil, err
	}
	d := oversync.DeploymentContract{AuthorityID: "mobile-flow-postgresql", Revision: int64(phase), RegistryFingerprint: registry.Fingerprint(), Migrations: tableManifest()[:tableMigrationPosition(phase)], TransitionRevision: "additive-tables-v1", TransitionPhase: tablePhaseName(phase), Mechanisms: tableMechanisms(phase), ApplicationCodeSHA256: tableSourceDigest(), QualificationSHA256: tableQualificationDigest()}
	cfg := &oversync.ServiceConfig{AppName: "table-evolution", RegisteredTables: registered, RegisteredTableCatalogRevision: revision, SchemaGenerations: gens, ReservedServerSourceIDs: []string{"schema-evolution-backfill", "schema-evolution-writer"}}
	r := &oversync.ReadinessConfig{Expected: d, AuthorityLock: authorityLock, DisableBackgroundWorkers: phase != TablesBaseline && phase != TablesServing, Adapter: tableAdapter{phase: phase, revision: revision, registry: registry.Fingerprint()}}
	for _, id := range registry.GenerationIDs() {
		contract, _ := registry.Generation(id)
		g := oversync.GenerationReadiness{Proof: oversync.SchemaGenerationProof{ID: id, Fingerprint: contract.Fingerprint()}, MinimumMigrationVersion: int64(tableMigrationPosition(phase))}
		if id == 2 {
			g.Backfills = []oversync.BackfillRequirement{tableBackfillRequirement()}
		}
		r.Generations = append(r.Generations, g)
	}
	cfg.Readiness = r
	return cfg, nil
}

func tableFK(column, parent string, action oversync.GenerationForeignKeyAction) oversync.GenerationForeignKey {
	return oversync.GenerationForeignKey{Columns: []string{column}, ReferencedSchema: "main", ReferencedTable: parent, ReferencedColumns: []string{"id"}, OnDelete: action, OnUpdate: oversync.GenerationForeignKeyNoAction}
}

// ReopenTableServing admits only a baseline or fully validated final deployment.
// Intermediate catalog/cleanup/backfill runtimes can never reopen the barrier.
func ReopenTableServing(ctx context.Context, pool *pgxpool.Pool, services []*oversync.SyncService) error {
	var d oversync.DeploymentContract
	if err := pool.QueryRow(ctx, `SELECT contract FROM app_evolution.deployment WHERE singleton`).Scan(&d); err != nil {
		return err
	}
	phase := TablePhase(d.Revision)
	if (phase != TablesBaseline && phase != TablesServing) || d.TransitionPhase != tablePhaseName(phase) {
		return fmt.Errorf("table intermediate phase cannot reopen")
	}
	return ReopenServing(ctx, pool, services)
}

func verifyTableReopen(ctx context.Context, tx pgx.Tx, d oversync.DeploymentContract) error {
	cfg, err := TableEvolutionConfiguration(TablesServing)
	if err != nil {
		return err
	}
	if d.Revision != cfg.Readiness.Expected.Revision || d.RegistryFingerprint != cfg.Readiness.Expected.RegistryFingerprint {
		return fmt.Errorf("table serving deployment mismatch")
	}
	e, err := cfg.Readiness.Adapter.Inspect(ctx, tx)
	if err != nil {
		return err
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return err
	}
	if err := oversync.ValidateApplicationReadiness(cfg.Readiness, registry, e); err != nil {
		return err
	}
	var revision int64
	var fp, policy string
	if err := tx.QueryRow(ctx, `SELECT catalog_revision,catalog_fingerprint FROM sync.meta WHERE singleton_key FOR SHARE`).Scan(&revision, &fp); err != nil {
		return err
	}
	if err := cfg.Readiness.Adapter.(tableAdapter).ValidateCatalogReadiness(ctx, tx, revision, fp); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, `SELECT client_policy FROM app_evolution.maintenance WHERE singleton`).Scan(&policy); err != nil {
		return err
	}
	if policy == "" {
		return fmt.Errorf("preserving clients required")
	}
	// This transaction also opens admission in reopenMaintenance. A failed
	// reopen rolls this marker back, so startup continues to check completion.
	_, err = tx.Exec(ctx, `UPDATE app_evolution.table_run SET phase='serving' WHERE singleton AND phase='populated'`)
	return err
}

// GrantTableRuntime grants attach/DML permissions after provisioning. It does
// not grant DDL or private cleanup/population evidence authority to serving roles.
func (a *Authority) GrantTableRuntime(ctx context.Context, role string) error {
	if a == nil || a.conn == nil || role == "" {
		return fmt.Errorf("table runtime role and authority required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r := pgx.Identifier{role}.Sanitize()
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		statements := []string{`REVOKE TRIGGER ON ALL TABLES IN SCHEMA main FROM ` + r, `REVOKE TRIGGER ON ALL TABLES IN SCHEMA main FROM PUBLIC`, `REVOKE SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON ALL TABLES IN SCHEMA main FROM ` + r, `GRANT USAGE ON SCHEMA main,sync,app_evolution TO ` + r, `GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA sync TO ` + r, `GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA sync,app_evolution TO ` + r, `GRANT SELECT ON ALL TABLES IN SCHEMA app_evolution TO ` + r, `GRANT INSERT ON app_evolution.scope_inventory TO ` + r, `GRANT UPDATE(deleted) ON app_evolution.scope_inventory TO ` + r}
		for _, statement := range statements {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return err
			}
		}
		rows, err := tx.Query(ctx, `SELECT schema_name,table_name FROM sync.table_catalog WHERE state_code=1 ORDER BY table_id`)
		if err != nil {
			return err
		}
		type runtimeTable struct{ schema, table string }
		var active []runtimeTable
		for rows.Next() {
			var table runtimeTable
			if err = rows.Scan(&table.schema, &table.table); err != nil {
				rows.Close()
				return err
			}
			active = append(active, table)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		for _, table := range active {
			if _, err = tx.Exec(ctx, `GRANT SELECT,INSERT,UPDATE,DELETE ON `+pgx.Identifier{table.schema, table.table}.Sanitize()+` TO `+r); err != nil {
				return err
			}
		}
		// Undo any previous broad grants, including those from an older fixture.
		if _, err := tx.Exec(ctx, `REVOKE ALL ON sync.meta,sync.table_catalog FROM `+r); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `GRANT SELECT ON sync.meta,sync.table_catalog TO `+r); err != nil {
			return err
		}
		if err := validateRuntimeRole(ctx, tx, role); err != nil {
			return err
		}
		return validateTableCatalogPrivileges(ctx, tx, role)
	})
}

func validateTableCatalogPrivileges(ctx context.Context, tx pgx.Tx, role string) error {
	var unsafe bool
	err := tx.QueryRow(ctx, `WITH RECURSIVE reachable(oid) AS (
 SELECT oid FROM pg_roles WHERE rolname=$1
 UNION SELECT m.roleid FROM pg_auth_members m JOIN reachable r ON m.member=r.oid
 ) SELECT EXISTS(SELECT 1 FROM reachable r CROSS JOIN pg_class c
 WHERE c.oid IN ('sync.meta'::regclass,'sync.table_catalog'::regclass)
 AND (has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
 OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE')))`, role).Scan(&unsafe)
	if err != nil {
		return err
	}
	if unsafe {
		return fmt.Errorf("runtime role has reachable catalog provisioning privileges")
	}
	return nil
}
