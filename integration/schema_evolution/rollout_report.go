package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

type RolloutCounts struct{ SnapshotSessions, CaptureRows, RetainedRows, RowState int64 }

type RolloutPredicateEvidence struct {
	Name     string
	Source   string
	Complete bool
	Passed   bool
}

type RolloutDependencyEvidence struct {
	Kind        string
	Identity    string
	Disposition string
}

type RolloutMemberEvidence struct {
	Name, Role        string
	Login, Privileged bool
	Backends          int
}

type RolloutRetainedFloorEvidence struct {
	ScopeCount, Minimum, Maximum int64
}

type RolloutCatalogEvidence struct {
	TableID         int32
	Schema, Table   string
	State           string
	RetiredRevision int64
}

type RolloutLimits struct {
	MaxRows, MaxBatches, MaxAttempts, MaxEvidenceBytes int
	QueryTimeout                                       time.Duration
}

type RolloutRecoveryEvidence struct {
	State              string
	CutSHA256          string
	VerificationSHA256 string
}

type RolloutReport struct {
	Version              int
	PlanDigest           string
	StateDigest          string
	ObservedAt           time.Time
	Complete             bool
	Blockers             []string
	Counts               RolloutCounts
	Deployment           oversync.DeploymentContract
	Ledger               *oversync.GenerationLedger
	WriterRoles          []RolloutRoleEvidence
	Members              []RolloutMemberEvidence
	Database             string
	CatalogRevision      int64
	CatalogFingerprint   string
	TargetOperations     []string
	PolicyEligible       bool
	DependencyInventory  []RolloutDependencyEvidence
	BackupEvidenceSHA256 string
	RollbackHolds        []string
	TransitionInvariants []string
	EstimatedRows        int64
	Limits               RolloutLimits
	Predicates           []RolloutPredicateEvidence
	Recovery             *RolloutRecoveryEvidence `json:",omitempty"`
	RetainedFloors       RolloutRetainedFloorEvidence
	CatalogEntries       []RolloutCatalogEvidence
}

// RolloutReportReadOnly makes no ledger, cleanup, DDL or business mutations.
// A complete report is observational; apply must recheck under the authority.
func RolloutReportReadOnly(ctx context.Context, pool *pgxpool.Pool, p RolloutPlan) (RolloutReport, error) {
	var result RolloutReport
	if _, err := RolloutPlanDigest(p); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		result, err = inspectRollout(ctx, tx, p)
		return err
	})
	return result, err
}

func inspectRollout(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan) (RolloutReport, error) {
	result := RolloutReport{
		Version:          1,
		Blockers:         []string{},
		TargetOperations: []string{p.Action},
		RollbackHolds:    slices.Clone(p.Policy.RollbackHolds),
		Limits:           RolloutLimits{MaxRows: p.MaxRows, MaxBatches: p.MaxBatches, MaxAttempts: p.MaxAttempts, MaxEvidenceBytes: p.MaxEvidenceBytes, QueryTimeout: p.QueryTimeout},
	}
	if p.Policy.BackupEvidence != "" {
		result.BackupEvidenceSHA256 = p.Policy.BackupEvidence
	}
	var err error
	result.PlanDigest, err = RolloutPlanDigest(p)
	if err != nil {
		return result, err
	}
	var database, clientPolicy string
	var requested, closed bool
	var epoch, barrier, catalogRevision int64
	var catalogFingerprint string
	err = r.QueryRow(ctx, `SELECT current_database(),transaction_timestamp(),m.requested,m.epoch,m.client_policy,b.closed,b.revision,d.contract,s.catalog_revision,s.catalog_fingerprint FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b CROSS JOIN app_evolution.deployment d CROSS JOIN sync.meta s WHERE m.singleton AND b.singleton AND d.singleton AND s.singleton_key`).Scan(&database, &result.ObservedAt, &requested, &epoch, &clientPolicy, &closed, &barrier, &result.Deployment, &catalogRevision, &catalogFingerprint)
	if err != nil {
		return result, err
	}
	if clientPolicy == "" || clientPolicy != p.Policy.ClientAdmissionPolicy {
		result.Blockers = append(result.Blockers, "client_admission_policy_mismatch")
	}
	if database != p.Database || result.Deployment.AuthorityID != p.AuthorityID {
		result.Blockers = append(result.Blockers, "authority_mismatch")
	}
	if !requested || !closed || epoch != barrier {
		result.Blockers = append(result.Blockers, "maintenance_drain_incomplete")
	}
	if epoch != p.Epoch || result.Deployment.Revision != p.DeploymentRevision || result.Deployment.RegistryFingerprint != p.RegistryFingerprint || catalogRevision != p.CatalogRevision || catalogFingerprint != p.CatalogFingerprint {
		result.Blockers = append(result.Blockers, "deployment_or_catalog_changed")
	}
	if !result.ObservedAt.Before(p.Deadline) {
		result.Blockers = append(result.Blockers, "plan_deadline_exceeded")
	}
	if err = p.Policy.eligible(result.ObservedAt); err != nil {
		result.Blockers = append(result.Blockers, err.Error())
	}
	result.Database = database
	result.CatalogRevision = catalogRevision
	result.CatalogFingerprint = catalogFingerprint
	result.TransitionInvariants = []string{"maintenance_requested", "admission_closed", "epoch_bound", "deployment_bound", "catalog_bound"}
	if p.Action == "retire_tables" {
		result.TransitionInvariants = append(result.TransitionInvariants, "table_writes_"+p.TableWritePolicy)
	}
	addRolloutPredicate(&result, "identity", "database:app_evolution.maintenance+barrier+deployment,sync.meta", len(result.Blockers) == 0)
	result.PolicyEligible = p.Policy.eligible(result.ObservedAt) == nil
	addRolloutPredicate(&result, "policy", "plan:RetirementPolicy,database:app_evolution.maintenance.client_policy", result.PolicyEligible && clientPolicy == p.Policy.ClientAdmissionPolicy)
	addRolloutPredicate(&result, "backup_rollback", "plan:RetirementPolicy.BackupEvidence+RollbackHolds", rolloutDigestPattern.MatchString(p.Policy.BackupEvidence) && len(p.Policy.RollbackHolds) == 0)
	addRolloutPredicate(&result, "transition_invariants", "database:app_evolution.maintenance+barrier+deployment,sync.meta", requested && closed && epoch == barrier)
	result.Ledger, err = readGenerationLedger(ctx, r)
	if err != nil {
		return result, err
	}
	if result.Ledger.Revision != p.LedgerRevision {
		result.Blockers = append(result.Blockers, "generation_ledger_changed")
	}
	addRolloutPredicate(&result, "generation_ledger", "database:app_evolution.generation_ledger+generation_ledger_meta+guards", result.Ledger.Revision == p.LedgerRevision)
	before := len(result.Blockers)
	if err = inspectRolloutFence(ctx, r, p, &result); err != nil {
		return result, err
	}
	addRolloutPredicate(&result, "membership_writer_fence", "database:pg_roles+pg_stat_activity+pg_locks+pg_prepared_xacts", len(result.Blockers) == before)
	before = len(result.Blockers)
	if err = inspectRolloutReferences(ctx, r, p, &result); err != nil {
		return result, err
	}
	addRolloutPredicate(&result, "sessions_capture_retention_catalog", "database:sync.snapshot_sessions+bundle_capture_stage+bundle_rows+row_state+table_catalog", len(result.Blockers) == before)
	before = len(result.Blockers)
	if err = inspectRolloutDependencies(ctx, r, p, &result); err != nil {
		return result, err
	}
	addRolloutPredicate(&result, "dependency_inventory", "database:pg_inherits+pg_trigger+pg_rewrite+pg_depend+pg_policy+pg_constraint+pg_roles", len(result.Blockers) == before)
	if p.Action == "restore" || p.Action == "reopen" {
		before = len(result.Blockers)
		if err = inspectRecoveryEvidence(ctx, r, p, &result); err != nil {
			return result, err
		}
		addRolloutPredicate(&result, "recovery", "database:app_evolution.recovery_gate+plan:recovery digests", len(result.Blockers) == before)
	}
	result.EstimatedRows = result.Counts.RetainedRows + result.Counts.RowState + result.Counts.CaptureRows
	addRolloutPredicate(&result, "estimates_limits", "database:bounded counts,plan:finite limits", true)
	slices.SortFunc(result.Predicates, func(a, b RolloutPredicateEvidence) int { return strings.Compare(a.Name, b.Name) })
	result.Complete = rolloutPredicatesComplete(result.Predicates, p.Action)
	if !result.Complete {
		result.Blockers = append(result.Blockers, "report_scan_incomplete")
	}
	slices.Sort(result.Blockers)
	result.Blockers = slices.Compact(result.Blockers)
	result.StateDigest, err = rolloutReportStateDigest(result, p.MaxEvidenceBytes)
	if err != nil {
		return result, err
	}

	return result, nil
}

func addRolloutPredicate(report *RolloutReport, name, source string, passed bool) {
	report.Predicates = append(report.Predicates, RolloutPredicateEvidence{Name: name, Source: source, Complete: source != "", Passed: passed})
}

func rolloutPredicatesComplete(predicates []RolloutPredicateEvidence, action string) bool {
	required := []string{"backup_rollback", "dependency_inventory", "estimates_limits", "generation_ledger", "identity", "membership_writer_fence", "policy", "sessions_capture_retention_catalog", "transition_invariants"}
	if action == "restore" || action == "reopen" {
		required = append(required, "recovery")
		slices.Sort(required)
	}
	if len(predicates) != len(required) {
		return false
	}
	for i, predicate := range predicates {
		if predicate.Name != required[i] || predicate.Source == "" || !predicate.Complete {
			return false
		}
	}
	return true
}

type RolloutRoleEvidence struct {
	Name       string
	Login      bool
	Privileged bool
	Backends   int64
}

// Inventory every role that can modify application, sync, or control tables,
// including inherited grants. The pinned authority credential is the trusted
// operator; all other writer credentials must be disabled and disconnected.
func inspectWriterRoles(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, report *RolloutReport) error {
	rows, err := r.Query(ctx, `WITH writers AS (
 SELECT role.oid FROM pg_roles role WHERE
 role.rolsuper OR role.rolcreaterole OR role.rolcreatedb OR role.rolreplication OR role.rolbypassrls
 OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname IN ('main','sync','app_evolution') AND c.relkind IN ('r','p')
 AND (c.relowner=role.oid OR has_table_privilege(role.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER') OR has_any_column_privilege(role.oid,c.oid,'INSERT,UPDATE')))
 OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname IN ('main','sync','app_evolution') AND CASE WHEN c.relkind='S' THEN has_sequence_privilege(role.oid,c.oid,'USAGE,UPDATE') ELSE false END)
 OR EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname IN ('main','sync','app_evolution') AND has_schema_privilege(role.oid,n.oid,'CREATE'))
 OR EXISTS(SELECT 1 FROM pg_proc f JOIN pg_namespace n ON n.oid=f.pronamespace WHERE n.nspname IN ('main','sync','app_evolution') AND f.prosecdef AND f.prorettype<>'trigger'::regtype AND has_function_privilege(role.oid,f.oid,'EXECUTE'))
 )
 SELECT rolname,rolcanlogin,rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls,
 (SELECT count(*) FROM pg_stat_activity a WHERE a.usename=p.rolname)
 FROM pg_roles p WHERE rolname<>current_user AND EXISTS(SELECT 1 FROM writers w WHERE pg_has_role(p.oid,w.oid,'MEMBER'))
 ORDER BY rolname LIMIT 65`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var role RolloutRoleEvidence
		if err := rows.Scan(&role.Name, &role.Login, &role.Privileged, &role.Backends); err != nil {
			return err
		}
		report.WriterRoles = append(report.WriterRoles, role)
		if (role.Login || role.Backends != 0) && !expectedReopenRole(p, role.Name) {
			report.Blockers = append(report.Blockers, "unfenced_database_writer")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(report.WriterRoles) > 64 {
		return fmt.Errorf("writer_role_inventory_capacity")
	}
	return nil
}

func inspectRolloutFence(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, report *RolloutReport) error {
	if err := inspectWriterRoles(ctx, r, p, report); err != nil {
		return err
	}
	for _, member := range p.Members {
		var login, privileged bool
		var backends int
		err := r.QueryRow(ctx, `SELECT rolcanlogin,rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls,
 (SELECT count(*) FROM pg_stat_activity WHERE usename=$1) FROM pg_roles WHERE rolname=$1`, member.Role).Scan(&login, &privileged, &backends)
		if err != nil {
			return fmt.Errorf("member_fence_evidence_unavailable: %w", err)
		}
		if privileged || (login || backends != 0) && p.Action != "reopen" {
			report.Blockers = append(report.Blockers, "writer_fence_incomplete")
		}
		report.Members = append(report.Members, RolloutMemberEvidence{Name: member.Name, Role: member.Role, Login: login, Privileged: privileged, Backends: backends})
	}
	var workers, prepared int
	if err := r.QueryRow(ctx, `SELECT (SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=1869833075 AND objid=7 AND objsubid=2 AND granted),(SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database())`).Scan(&workers, &prepared); err != nil {
		return err
	}
	if workers != 0 || prepared != 0 {
		report.Blockers = append(report.Blockers, "writer_transactions_not_stopped")
	}
	return nil
}

func inspectRolloutReferences(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, report *RolloutReport) error {
	generation := int32(0)
	if p.Action == "retire_generation" {
		generation = int32(p.Retiring.ID)
	}
	err := r.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM sync.snapshot_sessions WHERE $1::int=0 OR schema_generation_id=$1),
 (SELECT count(*) FROM sync.bundle_capture_stage),
 (SELECT count(*) FROM sync.bundle_rows WHERE table_id=ANY($2::int[])),
 (SELECT count(*) FROM sync.row_state WHERE table_id=ANY($2::int[]))`, generation, p.TableIDs).Scan(&report.Counts.SnapshotSessions, &report.Counts.CaptureRows, &report.Counts.RetainedRows, &report.Counts.RowState)
	if err != nil {
		return err
	}
	if err = r.QueryRow(ctx, `SELECT count(*),COALESCE(min(retained_bundle_floor),0),COALESCE(max(retained_bundle_floor),0) FROM sync.user_state`).Scan(&report.RetainedFloors.ScopeCount, &report.RetainedFloors.Minimum, &report.RetainedFloors.Maximum); err != nil {
		return err
	}
	if p.Action != "cleanup_staging" && report.Counts.SnapshotSessions != 0 {
		report.Blockers = append(report.Blockers, "staging_not_empty")
	}
	if report.Counts.CaptureRows != 0 {
		report.Blockers = append(report.Blockers, "capture_not_empty")
	}
	if (p.Action == "prune_table_state" || p.Action == "contract") && report.Counts.RetainedRows != 0 {
		report.Blockers = append(report.Blockers, "retained_history_requires_table_state")
	}
	if p.Action == "retire_tables" || p.Action == "prune_table_state" || p.Action == "contract" || p.Action == "expire_history" {
		if err := inspectRetiringTableReferences(ctx, r, p, report); err != nil {
			return err
		}
	}
	if p.Action == "contract" && len(p.Columns) > 0 {
		var referenced bool
		if err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM app_evolution.generation_ledger g CROSS JOIN LATERAL jsonb_array_elements(g.declaration) t CROSS JOIN LATERAL jsonb_array_elements(t->'Columns') c WHERE g.state='supported' AND t->>'Schema'='main' AND t->>'Table'='upgrade_people' AND c->>'Name'='legacy_name')`).Scan(&referenced); err != nil {
			return err
		}
		if referenced {
			report.Blockers = append(report.Blockers, "supported_generation_references_column")
		}
	}
	if p.Action == "contract" && report.Counts.RowState != 0 {
		report.Blockers = append(report.Blockers, "contract_row_state_not_pruned")
	}
	if p.Action == "retire_generation" {
		checkRetiringProof(p, report)
	}
	return nil
}

func checkRetiringProof(p RolloutPlan, r *RolloutReport) {
	active := 0
	found := false
	for _, entry := range r.Ledger.Entries {
		if entry.State == "supported" {
			active++
		}
		if entry.Proof == p.Retiring && entry.State == "supported" {
			found = true
		}
	}
	if !found {
		r.Blockers = append(r.Blockers, "retiring_generation_not_supported")
	}
	if active <= 1 {
		r.Blockers = append(r.Blockers, "last_supported_generation")
	}
}

func inspectRetiringTableReferences(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, report *RolloutReport) error {
	var count, references int
	state := 2
	if p.Action == "retire_tables" {
		state = 1
	}
	err := r.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM sync.table_catalog WHERE table_id=ANY($1::int[]) AND state_code=$2),
 (SELECT count(*) FROM app_evolution.generation_ledger g CROSS JOIN LATERAL jsonb_array_elements(g.declaration) d
 JOIN sync.table_catalog t ON t.schema_name=d->>'Schema' AND t.table_name=d->>'Table'
 WHERE g.state='supported' AND t.table_id=ANY($1::int[]))`, p.TableIDs, state).Scan(&count, &references)
	if err != nil {
		return err
	}
	if count != len(p.TableIDs) {
		report.Blockers = append(report.Blockers, "retiring_table_lifecycle_mismatch")
	}
	if references != 0 {
		report.Blockers = append(report.Blockers, "supported_generation_references_table")
	}
	rows, err := r.Query(ctx, `SELECT table_id,schema_name,table_name,CASE state_code WHEN 1 THEN 'active' WHEN 2 THEN 'retired' ELSE 'unknown' END,COALESCE(retired_revision,0) FROM sync.table_catalog WHERE table_id=ANY($1::int[]) ORDER BY table_id LIMIT 4097`, p.TableIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var entry RolloutCatalogEvidence
		if err = rows.Scan(&entry.TableID, &entry.Schema, &entry.Table, &entry.State, &entry.RetiredRevision); err != nil {
			return err
		}
		report.CatalogEntries = append(report.CatalogEntries, entry)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(report.CatalogEntries) > 4096 {
		return fmt.Errorf("catalog_evidence_capacity")
	}
	return nil
}

func inspectRecoveryEvidence(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, report *RolloutReport) error {
	var evidence RolloutRecoveryEvidence
	err := r.QueryRow(ctx, `SELECT state,COALESCE(cut_sha256,''),COALESCE(verification_sha256,'') FROM app_evolution.recovery_gate WHERE singleton`).Scan(&evidence.State, &evidence.CutSHA256, &evidence.VerificationSHA256)
	if err == pgx.ErrNoRows {
		if p.Action == "restore" || p.RecoveryCutSHA256 != "" || p.RecoveryResultSHA256 != "" {
			report.Blockers = append(report.Blockers, "recovery_cut_not_armed")
		}
		return nil
	}
	if err != nil {
		return err
	}
	report.Recovery = &evidence
	if evidence.State != "verified" {
		report.Blockers = append(report.Blockers, "recovery_completion_missing")
		return nil
	}
	if p.Action == "reopen" && (p.RecoveryCutSHA256 != evidence.CutSHA256 || p.RecoveryResultSHA256 != evidence.VerificationSHA256) {
		report.Blockers = append(report.Blockers, "recovery_reopen_identity_mismatch")
	}
	return nil
}

func inspectRolloutDependencies(ctx context.Context, r oversync.DatabaseReadinessReader, p RolloutPlan, report *RolloutReport) error {
	if p.Action != "retire_tables" && p.Action != "prune_table_state" && p.Action != "contract" {
		return nil
	}
	rows, err := r.Query(ctx, `WITH RECURSIVE selected AS (
 SELECT c.oid FROM sync.table_catalog t JOIN pg_namespace n ON n.nspname=t.schema_name
 JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=t.table_name WHERE t.table_id=ANY($1::int[])
), targets(oid) AS (
 SELECT oid FROM selected UNION SELECT i.inhrelid FROM pg_inherits i JOIN targets t ON t.oid=i.inhparent
), inventory(kind,identity) AS (
 SELECT 'partition',n.nspname||'.'||c.relname FROM targets t JOIN pg_class c ON c.oid=t.oid
 JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relispartition
 UNION
 SELECT 'trigger',n.nspname||'.'||c.relname||'.'||g.tgname FROM targets t JOIN pg_class c ON c.oid=t.oid
 JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_trigger g ON g.tgrelid=c.oid WHERE NOT g.tgisinternal
 UNION
 SELECT 'policy',n.nspname||'.'||c.relname||'.'||p.polname FROM targets t JOIN pg_class c ON c.oid=t.oid
 JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_policy p ON p.polrelid=c.oid
 UNION
 SELECT 'view',vn.nspname||'.'||v.relname FROM targets t JOIN pg_depend d ON d.refobjid=t.oid
 JOIN pg_rewrite rw ON rw.oid=d.objid JOIN pg_class v ON v.oid=rw.ev_class JOIN pg_namespace vn ON vn.oid=v.relnamespace
 WHERE v.oid<>t.oid AND v.relkind IN ('v','m')
 UNION
 SELECT 'function',pn.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')' FROM targets t
 JOIN pg_depend d ON d.refobjid=t.oid AND d.classid='pg_proc'::regclass JOIN pg_proc p ON p.oid=d.objid JOIN pg_namespace pn ON pn.oid=p.pronamespace
 UNION
 SELECT CASE WHEN cr.oid=ANY(ARRAY(SELECT oid FROM targets)) AND cf.oid=ANY(ARRAY(SELECT oid FROM targets)) THEN 'foreign_key_closed_set' ELSE 'foreign_key_external' END,
 n.nspname||'.'||c.relname||'.'||f.conname FROM pg_constraint f JOIN pg_class c ON c.oid=f.conrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_class cr ON cr.oid=f.conrelid JOIN pg_class cf ON cf.oid=f.confrelid
 WHERE f.contype='f' AND (f.conrelid=ANY(ARRAY(SELECT oid FROM targets)) OR f.confrelid=ANY(ARRAY(SELECT oid FROM targets)))
 UNION
	SELECT 'writer_role',r.rolname FROM pg_roles r WHERE r.rolname<>current_user
	AND NOT (r.rolname LIKE 'pg\_%' ESCAPE '\' AND NOT r.rolcanlogin) AND EXISTS(SELECT 1 FROM targets t WHERE
 has_table_privilege(r.oid,t.oid,'INSERT') OR has_table_privilege(r.oid,t.oid,'UPDATE')
 OR has_table_privilege(r.oid,t.oid,'DELETE') OR has_table_privilege(r.oid,t.oid,'TRUNCATE')
 OR has_table_privilege(r.oid,t.oid,'REFERENCES') OR has_any_column_privilege(r.oid,t.oid,'INSERT') OR has_any_column_privilege(r.oid,t.oid,'UPDATE'))
) SELECT kind,identity FROM inventory ORDER BY kind,identity LIMIT 1025`, p.TableIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var item RolloutDependencyEvidence
		if err = rows.Scan(&item.Kind, &item.Identity); err != nil {
			return err
		}
		switch item.Kind {
		case "partition":
			item.Disposition = "retire_with_root"
		case "trigger":
			if strings.HasSuffix(item.Identity, ".oversync_bundle_capture_row") || strings.HasSuffix(item.Identity, ".oversync_bundle_owner_guard") || strings.HasSuffix(item.Identity, ".oversync_registered_truncate_guard") {
				item.Disposition = "remove_managed_atomically"
			} else if p.Action == "contract" && strings.HasSuffix(item.Identity, ".aa_app_write_guard") {
				item.Disposition = "remove_with_exact_contract"
			} else if p.Action == "contract" {
				item.Disposition = "unresolved"
				report.Blockers = append(report.Blockers, "contract_dependency_unresolved")
			} else {
				item.Disposition = "retain_readonly_until_contract"
			}
		case "writer_role":
			if p.Action == "retire_tables" {
				item.Disposition = "revoke_dml_atomically"
			} else {
				item.Disposition = "writes_must_already_be_denied"
				report.Blockers = append(report.Blockers, "retired_table_write_privilege_remains")
			}
		case "foreign_key_closed_set":
			item.Disposition = "retain_with_closed_set"
		case "foreign_key_external":
			item.Disposition = "unresolved"
			report.Blockers = append(report.Blockers, "retiring_table_external_foreign_key")
		case "view", "function", "policy":
			if p.Action == "contract" {
				item.Disposition = "unresolved"
				report.Blockers = append(report.Blockers, "contract_dependency_unresolved")
			} else {
				item.Disposition = "retain_readonly_until_contract"
			}
		default:
			item.Disposition = "retain_readonly_until_contract"
		}
		report.DependencyInventory = append(report.DependencyInventory, item)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(report.DependencyInventory) > 1024 {
		return fmt.Errorf("dependency_inventory_capacity")
	}
	return nil
}

func rolloutReportStateDigest(report RolloutReport, maxBytes int) (string, error) {
	report.ObservedAt = time.Time{}
	report.StateDigest = ""
	b, err := json.Marshal(report)
	if err != nil {
		return "", err
	}
	if len(b) > maxBytes {
		return "", fmt.Errorf("report_evidence_capacity")
	}
	return digest(b), nil
}

func expectedReopenRole(p RolloutPlan, role string) bool {
	if p.Action != "reopen" {
		return false
	}
	for _, member := range p.Members {
		if member.Role == role {
			return true
		}
	}
	return false
}
