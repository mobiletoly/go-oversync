package oversync

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ExpireRetiredTableHistory applies an explicitly authorized existing whole-prefix
// retention policy to one scope. It never selects individual bundle rows. The
// caller audits and commits this transaction, then retries row-state pruning.
func (s *SyncService) ExpireRetiredTableHistory(ctx context.Context, tx pgx.Tx) (CatalogRetirementResult, error) {
	var result CatalogRetirementResult
	if s == nil || s.config == nil || s.config.Readiness == nil {
		return result, fmt.Errorf("private_retirement_runtime_required")
	}
	done, err := s.beginLocalOperation()
	if err != nil {
		return result, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, s.config.Readiness.InspectionTimeout)
	defer cancel()
	p, err := s.authorizeCatalogRetirement(ctx, tx)
	if err != nil {
		return result, err
	}
	if !p.ExpireHistory || p.RetentionWindow < 1 || p.RetentionWindow != s.retainedBundlesPerUser() || s.retentionPruneBatchSize() > int64(p.MaxRows) {
		return result, fmt.Errorf("history_expiry_policy_mismatch")
	}
	ctx, deadline := context.WithDeadline(ctx, p.Deadline)
	defer deadline()
	metadata, entries, err := readRegisteredTableCatalog(ctx, tx, true)
	if err != nil {
		return result, err
	}
	if err = checkRetirementCatalog(p, metadata, entries); err != nil {
		return result, err
	}
	for _, id := range p.TableIDs {
		if int(id) > len(entries) || entries[id-1].stateCode != registeredTableCatalogStateRetired {
			return result, fmt.Errorf("history_expiry_requires_retired_identity")
		}
	}
	if err = validateRegisteredTableCatalogAppendDrain(ctx, tx); err != nil {
		return result, err
	}
	var userPK int64
	if err = tx.QueryRow(ctx, `SELECT COALESCE(min(user_pk),0) FROM sync.bundle_rows WHERE table_id=ANY($1::int[])`, p.TableIDs).Scan(&userPK); err != nil {
		return result, err
	}
	result = CatalogRetirementResult{OperationID: p.OperationID, PlanDigest: p.PlanDigest, CatalogRevision: metadata.catalogRevision, CatalogFingerprint: metadata.catalogFingerprint, TableIDs: p.TableIDs, ScopeUserPK: userPK}
	if userPK == 0 {
		return result, nil
	}
	var before int64
	if err = tx.QueryRow(ctx, `SELECT retained_bundle_floor FROM sync.user_state WHERE user_pk=$1 FOR UPDATE`, userPK).Scan(&before); err != nil {
		return result, err
	}
	var nextBundle int64
	if err = tx.QueryRow(ctx, `SELECT next_bundle_seq FROM sync.user_state WHERE user_pk=$1`, userPK).Scan(&nextBundle); err != nil {
		return result, err
	}
	targetFloor := nextBundle - 1 - p.RetentionWindow
	if targetFloor > before+s.retentionPruneBatchSize() {
		targetFloor = before + s.retentionPruneBatchSize()
	}
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM sync.bundle_log WHERE user_pk=$1 AND bundle_seq<=$2),(SELECT count(*) FROM sync.bundle_rows WHERE user_pk=$1 AND bundle_seq<=$2)`, userPK, targetFloor).Scan(&result.ExpiredBundles, &result.ExpiredRows); err != nil {
		return result, err
	}
	if result.ExpiredRows > int64(p.MaxRows) {
		return result, fmt.Errorf("history_prefix_row_budget_exceeded")
	}
	if err = s.applyRetentionPolicyForUser(ctx, tx, userPK); err != nil {
		return result, err
	}
	var after int64
	if err = tx.QueryRow(ctx, `SELECT retained_bundle_floor FROM sync.user_state WHERE user_pk=$1`, userPK).Scan(&after); err != nil {
		return result, err
	}
	if after == before {
		return result, fmt.Errorf("history_retention_still_required")
	}
	result.PreviousRetainedFloor = before
	result.RetainedFloor = after
	return result, nil
}
