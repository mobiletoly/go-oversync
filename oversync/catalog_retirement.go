package oversync

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// CatalogRetirementAdapter grants a private operation only while holding the
// application's closed admission/authority fence through the entire transaction.
// It must validate policy, dependency disposition, backup/rollback holds, writer
// exclusion, and the exact immutable operation identity in the supplied transaction.
type CatalogRetirementAdapter interface {
	AuthorizeCatalogRetirement(context.Context, DatabaseReadinessReader) (CatalogRetirementPermit, error)
}

type CatalogRetirementPermit struct {
	OperationID                                string
	PlanDigest                                 string
	Epoch, DeploymentRevision, CatalogRevision int64
	RegistryFingerprint, CatalogFingerprint    string
	TableIDs                                   []int32
	Deadline                                   time.Time
	Prune                                      bool
	ExpireHistory                              bool
	RetentionWindow                            int64
	MaxRows                                    int
}

// CatalogRetirementResult is provisional until the caller records it in its
// operation ledger and commits this same transaction. No method commits for it.
type CatalogRetirementResult struct {
	OperationID           string
	PlanDigest            string
	ScopeUserPK           int64
	CatalogRevision       int64
	CatalogFingerprint    string
	TableIDs              []int32
	DeletedRows           int64
	PreviousRetainedFloor int64
	RetainedFloor         int64
	ExpiredBundles        int64
	ExpiredRows           int64
}

func (s *SyncService) authorizeCatalogRetirement(ctx context.Context, tx pgx.Tx) (CatalogRetirementPermit, error) {
	var zero CatalogRetirementPermit
	if s == nil || tx == nil || s.config == nil || s.config.Readiness == nil || !s.config.Readiness.DisableBackgroundWorkers {
		return zero, fmt.Errorf("private_retirement_runtime_required")
	}
	a, ok := s.config.Readiness.Adapter.(CatalogRetirementAdapter)
	if !ok {
		return zero, fmt.Errorf("catalog_retirement_not_authorized")
	}
	if err := attestOriginSession(ctx, tx); err != nil {
		return zero, err
	}
	r := &readinessReader{ctx: ctx, tx: tx, remaining: s.config.Readiness.MaxQueries}
	defer r.close()
	p, err := a.AuthorizeCatalogRetirement(ctx, r)
	if err != nil {
		return zero, err
	}
	p.TableIDs = slices.Clone(p.TableIDs)
	if !readinessSHA256.MatchString(p.PlanDigest) || p.OperationID == "" || p.Epoch < 1 || !time.Now().Before(p.Deadline) || len(p.TableIDs) == 0 || len(p.TableIDs) > 4096 || p.MaxRows < 1 || p.MaxRows > 4096 {
		return zero, fmt.Errorf("invalid_retirement_permit")
	}
	if p.DeploymentRevision != s.config.Readiness.Expected.Revision || p.RegistryFingerprint != s.schemaGenerationRegistry.Fingerprint() {
		return zero, fmt.Errorf("retirement_deployment_mismatch")
	}
	for i, id := range p.TableIDs {
		if id < 1 || (i > 0 && p.TableIDs[i-1] >= id) {
			return zero, fmt.Errorf("retirement_table_ids_not_sorted_unique")
		}
	}
	return p, nil
}

// RetireRegisteredTables atomically changes catalog lifecycle, managed triggers,
// and metadata. The caller must roll back on any error and audit before commit.
// Neither business rows nor retained history are removed. Fresh Bootstrap is
// required after commit; no runtime maps are published by this operation.
func (s *SyncService) RetireRegisteredTables(ctx context.Context, tx pgx.Tx) (CatalogRetirementResult, error) {
	var result CatalogRetirementResult
	if s == nil || s.config == nil || s.config.Readiness == nil {
		return result, fmt.Errorf("private_retirement_runtime_required")
	}
	done, err := s.beginLocalOperation()
	if err != nil {
		return result, err
	}
	defer done()
	ctx, bounded := context.WithTimeout(ctx, s.config.Readiness.InspectionTimeout)
	defer bounded()
	p, err := s.authorizeCatalogRetirement(ctx, tx)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	if p.Prune || p.ExpireHistory {
		return result, fmt.Errorf("retirement_permit_is_for_pruning")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, syncBootstrapLockKey); err != nil {
		return result, err
	}
	metadata, entries, err := readRegisteredTableCatalog(ctx, tx, true)
	if err != nil {
		return result, err
	}
	if err = checkRetirementCatalog(p, metadata, entries); err != nil {
		return result, err
	}
	if metadata.catalogRevision == math.MaxInt64 {
		return result, fmt.Errorf("catalog_revision_exhausted")
	}
	if err = validateRegisteredTableCatalogAppendDrain(ctx, tx); err != nil {
		return result, err
	}
	if err = lockRegisteredTables(ctx, tx, registeredTableDeclarationsForCatalogEntries(activeRegisteredTableCatalogEntries(entries))); err != nil {
		return result, err
	}
	if err = s.validateManagedLayout(ctx, tx); err != nil {
		return result, err
	}
	retired, err := s.selectRetiringEntries(entries, p.TableIDs)
	if err != nil {
		return result, err
	}
	if err = denyRetiredTableWrites(ctx, tx, retired); err != nil {
		return result, err
	}
	metadata.catalogRevision++
	if err = retireCatalogEntries(ctx, tx, entries, retired, metadata.catalogRevision); err != nil {
		return result, err
	}
	metadata.catalogFingerprint, err = registeredTableCatalogFingerprint(metadata, entries)
	if err != nil {
		return result, err
	}
	if err = publishRegisteredTableCatalogMetadata(ctx, tx, metadata); err != nil {
		return result, err
	}
	if err = s.validateRetiredCatalogTarget(ctx, tx, metadata, entries); err != nil {
		return result, err
	}
	return CatalogRetirementResult{OperationID: p.OperationID, PlanDigest: p.PlanDigest, CatalogRevision: metadata.catalogRevision, CatalogFingerprint: metadata.catalogFingerprint, TableIDs: p.TableIDs}, nil
}

func denyRetiredTableWrites(ctx context.Context, tx pgx.Tx, entries []registeredTableCatalogEntry) error {
	for _, entry := range entries {
		identity := pgx.Identifier{entry.schemaName, entry.tableName}.Sanitize()
		if _, err := tx.Exec(ctx, `REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON `+identity+` FROM PUBLIC`); err != nil {
			return err
		}
		columns, err := retirementColumnList(ctx, tx, identity)
		if err != nil {
			return err
		}
		if columns != "" {
			if _, err = tx.Exec(ctx, `REVOKE INSERT (`+columns+`) ON `+identity+` FROM PUBLIC`); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `REVOKE UPDATE (`+columns+`) ON `+identity+` FROM PUBLIC`); err != nil {
				return err
			}
		}
		rows, err := tx.Query(ctx, `SELECT rolname FROM pg_roles WHERE rolname<>current_user
 AND NOT (rolname LIKE 'pg\_%' ESCAPE '\' AND NOT rolcanlogin) AND (
 has_table_privilege(oid,$1,'INSERT') OR has_table_privilege(oid,$1,'UPDATE')
 OR has_table_privilege(oid,$1,'DELETE') OR has_table_privilege(oid,$1,'TRUNCATE')
 OR has_table_privilege(oid,$1,'REFERENCES') OR has_any_column_privilege(oid,$1,'INSERT')
 OR has_any_column_privilege(oid,$1,'UPDATE')) ORDER BY rolname LIMIT 257`, identity)
		if err != nil {
			return err
		}
		var roles []string
		for rows.Next() {
			var role string
			if err = rows.Scan(&role); err != nil {
				rows.Close()
				return err
			}
			roles = append(roles, role)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		if len(roles) > 256 {
			return fmt.Errorf("retired_table_writer_inventory_capacity")
		}
		for _, role := range roles {
			if _, err = tx.Exec(ctx, `REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON `+identity+` FROM `+pgx.Identifier{role}.Sanitize()); err != nil {
				return err
			}
			if columns != "" {
				if _, err = tx.Exec(ctx, `REVOKE INSERT (`+columns+`) ON `+identity+` FROM `+pgx.Identifier{role}.Sanitize()); err != nil {
					return err
				}
				if _, err = tx.Exec(ctx, `REVOKE UPDATE (`+columns+`) ON `+identity+` FROM `+pgx.Identifier{role}.Sanitize()); err != nil {
					return err
				}
			}
		}
		var unsafeRoles []string
		if err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(rolname ORDER BY rolname),'{}') FROM pg_roles WHERE rolname<>current_user
 AND NOT (rolname LIKE 'pg\_%' ESCAPE '\' AND NOT rolcanlogin) AND (
 has_table_privilege(oid,$1,'INSERT') OR has_table_privilege(oid,$1,'UPDATE')
 OR has_table_privilege(oid,$1,'DELETE') OR has_table_privilege(oid,$1,'TRUNCATE')
 OR has_table_privilege(oid,$1,'REFERENCES') OR has_any_column_privilege(oid,$1,'INSERT')
	 OR has_any_column_privilege(oid,$1,'UPDATE'))`, identity).Scan(&unsafeRoles); err != nil {
			return err
		}
		if len(unsafeRoles) != 0 {
			return fmt.Errorf("retired_table_write_privilege_remains: %v", unsafeRoles)
		}
	}
	return nil
}

func retirementColumnList(ctx context.Context, tx pgx.Tx, identity string) (string, error) {
	rows, err := tx.Query(ctx, `SELECT attname FROM pg_attribute WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum`, identity)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err = rows.Scan(&column); err != nil {
			return "", err
		}
		columns = append(columns, pgx.Identifier{column}.Sanitize())
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(columns, ","), nil
}

func checkRetirementCatalog(p CatalogRetirementPermit, m registeredTableCatalogMetadata, entries []registeredTableCatalogEntry) error {
	if err := validateLoadedRegisteredTableCatalog(m, entries); err != nil {
		return err
	}
	if p.CatalogRevision != m.catalogRevision || p.CatalogFingerprint != m.catalogFingerprint {
		return fmt.Errorf("retirement_catalog_changed")
	}
	return nil
}

func (s *SyncService) selectRetiringEntries(entries []registeredTableCatalogEntry, ids []int32) ([]registeredTableCatalogEntry, error) {
	var selected []registeredTableCatalogEntry
	for _, entry := range entries {
		if !slices.Contains(ids, entry.tableID) {
			continue
		}
		if entry.stateCode != registeredTableCatalogStateActive {
			return nil, fmt.Errorf("table_not_active")
		}
		for _, id := range s.schemaGenerationRegistry.GenerationIDs() {
			contract, _ := s.schemaGenerationRegistry.Generation(id)
			for _, table := range contract.Tables() {
				if table.Schema == entry.schemaName && table.Table == entry.tableName {
					return nil, fmt.Errorf("supported_generation_references_table")
				}
			}
		}
		selected = append(selected, entry)
	}
	if len(selected) != len(ids) {
		return nil, fmt.Errorf("unknown_retirement_table")
	}
	return selected, nil
}

func retireCatalogEntries(ctx context.Context, tx pgx.Tx, entries, selected []registeredTableCatalogEntry, revision int64) error {
	for _, entry := range selected {
		tag, err := tx.Exec(ctx, `UPDATE sync.table_catalog SET state_code=2,retired_revision=$2 WHERE table_id=$1 AND state_code=1`, entry.tableID, revision)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("catalog_retirement_raced")
		}
		if err = removeRetiredTriggers(ctx, tx, entry); err != nil {
			return err
		}
		for i := range entries {
			if entries[i].tableID == entry.tableID {
				entries[i].stateCode = registeredTableCatalogStateRetired
				entries[i].retiredRevision = &revision
			}
		}
	}
	return nil
}

func removeRetiredTriggers(ctx context.Context, tx pgx.Tx, entry registeredTableCatalogEntry) error {
	table := RegisteredTable{Schema: entry.schemaName, Table: entry.tableName, SyncKeyColumns: []string{entry.syncKeyColumn}}
	targets, err := loadRegisteredTableTriggerTargetInventory(ctx, tx, []RegisteredTable{table})
	if err != nil {
		return err
	}
	for _, name := range []string{registeredTableOwnerGuardTrigger, registeredTableCaptureTriggerName} {
		if _, err = tx.Exec(ctx, `DROP TRIGGER `+pgx.Identifier{name}.Sanitize()+` ON `+pgx.Identifier{entry.schemaName, entry.tableName}.Sanitize()); err != nil {
			return err
		}
	}
	for _, target := range targets {
		if _, err = tx.Exec(ctx, `DROP TRIGGER `+pgx.Identifier{registeredTableTruncateGuardTrigger}.Sanitize()+` ON `+pgx.Identifier{target.targetSchema, target.targetTable}.Sanitize()); err != nil {
			return err
		}
	}
	return nil
}

func (s *SyncService) validateRetiredCatalogTarget(ctx context.Context, tx pgx.Tx, m registeredTableCatalogMetadata, entries []registeredTableCatalogEntry) error {
	tables := registeredTableDeclarationsForCatalogEntries(activeRegisteredTableCatalogEntries(entries))
	candidate, err := s.prepareCatalogBootstrapCandidateForTables(ctx, tx, tables)
	if err != nil {
		return err
	}
	candidate.catalogMetadata = m
	if err = candidate.applyCatalogEntries(entries); err != nil {
		return err
	}
	if err = candidate.discoverSchemaRelationships(ctx, tx); err != nil {
		return err
	}
	return candidate.validateManagedLayout(ctx, tx)
}

// PruneRetiredTableState removes only row state after whole-bundle history no
// longer references the selected IDs. It never advances floors or source state.
// Application authorization must also prove physical contraction eligibility.
func (s *SyncService) PruneRetiredTableState(ctx context.Context, tx pgx.Tx) (CatalogRetirementResult, error) {
	var result CatalogRetirementResult
	if s == nil || s.config == nil || s.config.Readiness == nil {
		return result, fmt.Errorf("private_retirement_runtime_required")
	}
	done, err := s.beginLocalOperation()
	if err != nil {
		return result, err
	}
	defer done()
	ctx, bounded := context.WithTimeout(ctx, s.config.Readiness.InspectionTimeout)
	defer bounded()
	p, err := s.authorizeCatalogRetirement(ctx, tx)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithDeadline(ctx, p.Deadline)
	defer cancel()
	if !p.Prune || p.ExpireHistory {
		return result, fmt.Errorf("prune_permit_required")
	}
	metadata, entries, err := readRegisteredTableCatalog(ctx, tx, true)
	if err != nil {
		return result, err
	}
	if err = checkRetirementCatalog(p, metadata, entries); err != nil {
		return result, err
	}
	if err = validateRegisteredTableCatalogAppendDrain(ctx, tx); err != nil {
		return result, err
	}
	for _, id := range p.TableIDs {
		if int(id) > len(entries) || entries[id-1].tableID != id || entries[id-1].stateCode != registeredTableCatalogStateRetired {
			return result, fmt.Errorf("prune_requires_retired_identity")
		}
	}
	var referenced bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.bundle_rows WHERE table_id=ANY($1::int[]))`, p.TableIDs).Scan(&referenced); err != nil {
		return result, err
	}
	if referenced {
		return result, fmt.Errorf("retained_history_requires_table_state")
	}
	var userPK int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(min(user_pk),0) FROM sync.row_state WHERE table_id=ANY($1::int[])`, p.TableIDs).Scan(&userPK); err != nil {
		return result, err
	}
	var lockedUserPK int64
	if userPK != 0 {
		if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_pk=$1 FOR UPDATE`, userPK).Scan(&lockedUserPK); err != nil {
			return result, err
		}
	}
	tag, err := tx.Exec(ctx, `WITH doomed AS (SELECT ctid FROM sync.row_state WHERE user_pk=$3 AND table_id=ANY($1::int[]) ORDER BY table_id,key_bytes LIMIT $2 FOR UPDATE) DELETE FROM sync.row_state r USING doomed d WHERE r.ctid=d.ctid`, p.TableIDs, p.MaxRows, userPK)
	if err != nil {
		return result, err
	}
	return CatalogRetirementResult{OperationID: p.OperationID, PlanDigest: p.PlanDigest, CatalogRevision: metadata.catalogRevision, CatalogFingerprint: metadata.catalogFingerprint, TableIDs: p.TableIDs, ScopeUserPK: userPK, DeletedRows: tag.RowsAffected()}, nil
}
