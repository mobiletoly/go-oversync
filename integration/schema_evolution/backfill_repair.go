package schemaevolution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Repair explicitly authorizes one bounded replacement for a proven conversion
// failure. The new immutable operation is durable before execution. Repeating
// this call reconciles that operation; changing its approval fails closed.
func (w *Worker) Repair(ctx context.Context, failed uuid.UUID, key uuid.UUID, replacement string) error {
	if w.Pool == nil || w.Manager == nil || w.Capacity == nil {
		return fmt.Errorf("worker dependencies required")
	}
	if err := w.Bounds.validate(); err != nil {
		return err
	}
	bounded, cancel := w.repairQueryContext(ctx)
	lock, err := acquireSessionLock(bounded, w.Pool, workerLock)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, releaseCancel := w.repairQueryContext(context.WithoutCancel(ctx))
		defer releaseCancel()
		releaseSessionLockContext(releaseCtx, lock, workerLock)
	}()
	bounded, cancel = w.repairQueryContext(ctx)
	capability, err := claimWorker(bounded, lock)
	cancel()
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, migrationContextKey{}, capability)
	bounded, cancel = w.repairQueryContext(ctx)
	err = w.resumeJob(bounded)
	cancel()
	if err != nil {
		return err
	}
	defer w.pause(ctx, "repair_stopped")
	if !time.Now().Before(w.Bounds.Deadline) {
		return fmt.Errorf("run_deadline_expired")
	}
	bounded, cancel = w.repairQueryContext(ctx)
	err = w.reconcileMetrics(bounded)
	cancel()
	if err != nil {
		return err
	}
	bounded, cancel = w.repairQueryContext(ctx)
	err = w.checkCapacity(bounded)
	cancel()
	if err != nil {
		return err
	}
	done, err := w.prepareRepair(ctx, failed, key, replacement)
	if err != nil || done {
		return err
	}
	w.signal("after_repair_descriptor")
	bounded, cancel = w.repairQueryContext(ctx)
	defer cancel()
	_, err = w.step(bounded)
	return err
}

func (w *Worker) repairQueryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, minDeadline(time.Now().Add(w.Bounds.QueryTimeout), w.Bounds.Deadline))
}

func (w *Worker) prepareRepair(ctx context.Context, failed, key uuid.UUID, replacement string) (done bool, err error) {
	bounded, cancel := context.WithDeadline(ctx, minDeadline(time.Now().Add(w.Bounds.QueryTimeout), w.Bounds.Deadline))
	defer cancel()
	err = pgx.BeginTxFunc(bounded, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := fenceWorkerMetadata(bounded, tx); err != nil {
			return err
		}
		var prior BatchDescriptor
		var state string
		err := tx.QueryRow(bounded, `SELECT b.spec,b.state FROM app_evolution.repair_evidence e JOIN app_evolution.batches b ON b.operation_id=e.repair_operation WHERE e.failed_operation=$1 AND e.is_current`, failed).Scan(&prior, &state)
		if err == nil {
			if prior.Repair == nil || prior.Repair.Key != key || prior.Repair.Replacement != replacement {
				return fmt.Errorf("immutable repair approval changed")
			}
			if state == "pending" && !time.Now().Before(prior.ValidUntil) {
				return w.replaceExpiredRepairTx(bounded, tx, prior)
			}
			done = state != "pending"
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return w.createRepairTx(bounded, tx, failed, key, replacement)
	})
	return done, err
}

func (w *Worker) createRepairTx(ctx context.Context, tx pgx.Tx, failed, key uuid.UUID, replacement string) error {
	old, failureDigest, category, err := loadRepairFailure(ctx, tx, failed, key, "pending")
	if err != nil {
		return err
	}
	if err := validateRepairReplacement(ctx, tx, replacement); err != nil {
		return err
	}
	d := old
	d.Repair = &RowRepair{FailedOperation: failed, Key: key, Replacement: replacement}
	d.OperationID = uuid.New()
	d.ValidUntil = w.descriptorValidUntil()
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(ordinal),0)+1 FROM app_evolution.batches`).Scan(&d.Ordinal); err != nil {
		return err
	}
	encoded, err := d.CanonicalBytes()
	if err != nil {
		return err
	}
	if len(encoded) > w.Bounds.Bytes || int64(len(encoded)) > w.Bounds.PendingBytes {
		return fmt.Errorf("repair_descriptor_bytes_exceeded")
	}
	// Preserve the failed descriptor and its diagnostic evidence. Its captured
	// business write is proven rolled back, so it consumed no canonical history.
	if _, err := tx.Exec(ctx, `UPDATE app_evolution.batches SET state='repair_pending',metrics_complete=true WHERE operation_id=$1`, failed); err != nil {
		return err
	}
	if err := persistReservedDescriptorTx(ctx, tx, d, encoded); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.repair_evidence(repair_operation,failed_operation,attempt_no,is_current,row_key,input_digest,category) VALUES($1,$2,1,true,$3,$4,$5)`, d.OperationID, failed, key, failureDigest, category); err != nil {
		return err
	}
	return checkJobEvidenceCapacity(ctx, tx)
}

func (w *Worker) replaceExpiredRepairTx(ctx context.Context, tx pgx.Tx, expired BatchDescriptor) error {
	if expired.Repair == nil || time.Now().Before(expired.ValidUntil) {
		return fmt.Errorf("repair replacement requires an expired repair descriptor")
	}
	if !time.Now().Before(w.Bounds.Deadline) {
		return fmt.Errorf("run_deadline_expired")
	}
	old, failureDigest, category, err := loadRepairFailure(ctx, tx, expired.Repair.FailedOperation, expired.Repair.Key, "repair_pending")
	if err != nil {
		return err
	}
	if expired.Repair.Replacement == "" {
		return fmt.Errorf("immutable repair approval is missing")
	}
	if err := validateRepairReplacement(ctx, tx, expired.Repair.Replacement); err != nil {
		return err
	}
	for _, row := range expired.Rows {
		if _, err := verifySelectedRow(ctx, tx, expired, row); err != nil {
			return err
		}
	}
	var attempt int64
	if err := tx.QueryRow(ctx, `SELECT attempt_no FROM app_evolution.repair_evidence WHERE repair_operation=$1 AND is_current FOR UPDATE`, expired.OperationID).Scan(&attempt); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE app_evolution.batches SET state='expired_reselect',metrics_complete=true,history_bytes=0,bundles=0 WHERE operation_id=$1 AND state='pending'`, expired.OperationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("expired repair descriptor state changed")
	}
	tag, err = tx.Exec(ctx, `UPDATE app_evolution.repair_evidence SET is_current=false WHERE repair_operation=$1 AND is_current`, expired.OperationID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("expired repair evidence state changed")
	}
	next := old
	next.Repair = &RowRepair{FailedOperation: expired.Repair.FailedOperation, Key: expired.Repair.Key, Replacement: expired.Repair.Replacement}
	next.OperationID = uuid.New()
	next.ValidUntil = w.descriptorValidUntil()
	if !time.Now().Before(next.ValidUntil) {
		return fmt.Errorf("run_deadline_expired")
	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(ordinal),0)+1 FROM app_evolution.batches`).Scan(&next.Ordinal); err != nil {
		return err
	}
	encoded, err := next.CanonicalBytes()
	if err != nil {
		return err
	}
	if len(encoded) > w.Bounds.Bytes || int64(len(encoded)) > w.Bounds.PendingBytes {
		return fmt.Errorf("repair_descriptor_bytes_exceeded")
	}
	if err := persistReservedDescriptorTx(ctx, tx, next, encoded); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.repair_evidence(repair_operation,failed_operation,attempt_no,is_current,row_key,input_digest,category) VALUES($1,$2,$3,true,$4,$5,$6)`, next.OperationID, expired.Repair.FailedOperation, attempt+1, expired.Repair.Key, failureDigest, category); err != nil {
		return err
	}
	return checkJobEvidenceCapacity(ctx, tx)
}

func (w *Worker) applyRepair(ctx context.Context, tx oversync.DatabaseWriteTx, d BatchDescriptor) error {
	if !time.Now().Before(w.Bounds.Deadline) {
		return fmt.Errorf("run_deadline_expired")
	}
	deleted, err := lockDescriptorScope(ctx, tx, d)
	if err != nil {
		return err
	}
	if deleted {
		return fmt.Errorf("repair_scope_deleted")
	}
	if err := permitWorkerTransaction(ctx, tx); err != nil {
		return err
	}
	found := false
	for _, row := range d.Rows {
		if _, err := verifySelectedRow(ctx, tx, d, row); err != nil {
			return err
		}
		if row.Key == d.Repair.Key {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("repair_row_not_selected")
	}
	var originalState string
	if err := tx.QueryRow(ctx, `SELECT state FROM app_evolution.batches WHERE operation_id=$1 FOR UPDATE`, d.Repair.FailedOperation).Scan(&originalState); err != nil {
		return err
	}
	if originalState != "repair_pending" {
		return fmt.Errorf("repair_outcome_changed")
	}
	tag, err := tx.Exec(ctx, `UPDATE main.upgrade_people SET legacy_name=$3 WHERE _sync_scope_id=$1 AND id=$2`, d.Scope, d.Repair.Key, d.Repair.Replacement)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("repair_row_disappeared")
	}
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT (`+targetInvariant+`) FROM main.upgrade_people WHERE _sync_scope_id=$1 AND id=$2`, d.Scope, d.Repair.Key).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("repair_invariant_failed")
	}
	if _, err := tx.Exec(ctx, `UPDATE app_evolution.batches SET state='repaired' WHERE operation_id=$1`, d.Repair.FailedOperation); err != nil {
		return err
	}
	if err := resolveRowWork(ctx, tx, d, SelectedRow{Key: d.Repair.Key}); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM app_evolution.worker_transactions WHERE backend=pg_backend_pid()`); err != nil {
		return err
	}
	w.signal("inside_repair")
	// Reselect the original interval under fresh source identities, including any
	// other invalid survivors. Do not advance past rows in the failed batch.
	return recordOutcome(ctx, tx, d, batchOutcome{Scanned: 1, Changed: 1, Stale: 1})
}

func loadRepairFailure(ctx context.Context, tx pgx.Tx, failed, key uuid.UUID, expectedState string) (BatchDescriptor, string, string, error) {
	var state string
	var old BatchDescriptor
	var raw []byte
	var hash string
	if err := tx.QueryRow(ctx, `SELECT spec,descriptor,hash,state FROM app_evolution.batches WHERE operation_id=$1 FOR UPDATE`, failed).Scan(&old, &raw, &hash, &state); err != nil {
		return BatchDescriptor{}, "", "", err
	}
	encoded, err := old.CanonicalBytes()
	if err != nil {
		return BatchDescriptor{}, "", "", err
	}
	if digest(encoded) != hash || digest(raw) != hash || state != expectedState || old.Repair != nil {
		return BatchDescriptor{}, "", "", fmt.Errorf("repair requires exact pending failed descriptor")
	}
	if _, err := lockDescriptorScope(ctx, tx, old); err != nil {
		return BatchDescriptor{}, "", "", err
	}
	var failureDigest, category string
	if err := tx.QueryRow(ctx, `SELECT input_digest,category FROM app_evolution.row_work WHERE job=$1 AND incarnation=$2 AND row_key=$3`, JobID, old.Incarnation, key).Scan(&failureDigest, &category); err != nil {
		return BatchDescriptor{}, "", "", err
	}
	if category != "invalid_historical_input" {
		return BatchDescriptor{}, "", "", fmt.Errorf("repair requires confirmed conversion failure")
	}
	matched := false
	for _, row := range old.Rows {
		if row.Key == key && row.InputDigest == failureDigest {
			matched = true
		}
		if _, err := verifySelectedRow(ctx, tx, old, row); err != nil {
			return BatchDescriptor{}, "", "", err
		}
	}
	if !matched {
		return BatchDescriptor{}, "", "", fmt.Errorf("repair failure identity mismatch")
	}

	return old, failureDigest, category, nil
}

func validateRepairReplacement(ctx context.Context, tx pgx.Tx, replacement string) error {
	if len(replacement) == 0 || len(replacement) > 4096 {
		return fmt.Errorf("invalid repair replacement size")
	}
	var converted []string
	if err := tx.QueryRow(ctx, `SELECT app_evolution.convert_person($1,NULL,NULL,true)`, replacement).Scan(&converted); err != nil {
		return err
	}

	return nil
}
