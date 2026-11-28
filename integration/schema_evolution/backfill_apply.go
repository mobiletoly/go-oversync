package schemaevolution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mobiletoly/go-oversync/oversync"
)

type batchOutcome struct {
	Scanned, Changed, Skipped, Stale int64
	Deleted                          bool
}

type applicationTx interface {
	oversync.DatabaseReadinessReader
}

func lockDescriptorScope(ctx context.Context, tx applicationTx, d BatchDescriptor) (bool, error) {
	if err := lockAdmission(ctx, tx, false); err != nil {
		return false, err
	}
	var phase, revision string
	var closed bool
	if err := tx.QueryRow(ctx, `SELECT closed FROM app_evolution.barrier WHERE singleton FOR SHARE`).Scan(&closed); err != nil {
		return false, err
	}
	if !closed {
		return false, fmt.Errorf("maintenance_requires_closed_admission")
	}
	capability, ok := ctx.Value(migrationContextKey{}).(migrationCapability)
	if !ok || capability.epoch != d.Epoch {
		return false, fmt.Errorf("maintenance_epoch_changed")
	}
	if err := verifyWorker(ctx, tx, capability); err != nil {
		return false, err
	}
	var epoch int64
	var census string
	var bounds Bounds
	if err := tx.QueryRow(ctx, `SELECT epoch,census,bounds FROM app_evolution.jobs WHERE job=$1`, d.Job).Scan(&epoch, &census, &bounds); err != nil {
		return false, err
	}
	if epoch != d.Epoch || census != d.Census || d.SQLDigest != digest([]byte(backfillSQL)) || d.BoundsDigest != digest(encodeJSON(bounds)) || d.Algorithm != backfillRequirement.AlgorithmSHA256 {
		return false, fmt.Errorf("descriptor_plan_changed")
	}
	for _, expected := range expectedMechanisms(3, "maintaining") {
		actual, err := readMechanism(ctx, tx, expected)
		if err != nil {
			return false, err
		}
		if actual != expected {
			return false, fmt.Errorf("transition_mechanism_changed")
		}
	}
	if err := tx.QueryRow(ctx, `SELECT contract->>'TransitionPhase',contract->>'TransitionRevision' FROM app_evolution.deployment WHERE singleton`).Scan(&phase, &revision); err != nil {
		return false, err
	}
	if phase != d.Phase || revision != d.TransitionRevision {
		return false, fmt.Errorf("transition_identity_changed")
	}
	var deleted bool
	err := tx.QueryRow(ctx, `SELECT deleted FROM app_evolution.scope_inventory WHERE scope_id=$1 AND incarnation=$2 AND sequence=$3 FOR SHARE`, d.Scope, d.Incarnation, d.ScopeSequence).Scan(&deleted)
	if err != nil {
		return false, err
	}
	if !deleted {
		if err := requireSettledScope(ctx, tx, d.Scope); err != nil {
			return false, err
		}
	}
	return deleted, nil
}

func (w *Worker) apply(ctx context.Context, tx oversync.DatabaseWriteTx, d BatchDescriptor) error {
	if !time.Now().Before(w.Bounds.Deadline) {
		return fmt.Errorf("run_deadline_expired")
	}
	deleted, err := lockDescriptorScope(ctx, tx, d)
	if err != nil {
		return err
	}
	if deleted {
		return fmt.Errorf("descriptor_scope_deleted")
	}
	if err := permitWorkerTransaction(ctx, tx); err != nil {
		return err
	}
	outcome := batchOutcome{Deleted: deleted}
	for _, row := range d.Rows {
		outcome.Scanned++
		if deleted {
			outcome.Skipped++
			continue
		}
		disposition, err := applySQLRow(ctx, tx, d, row)
		if err != nil {
			var sqlErr *pgconn.PgError
			if errors.As(err, &sqlErr) && sqlErr.Code == "P6001" {
				return &conversionFailure{Row: row, Category: "invalid_historical_input"}
			}
			return err
		}
		switch disposition {
		case "changed":
			outcome.Changed++
		case "valid":
			outcome.Skipped++
		case "stale":
			outcome.Stale++
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM app_evolution.worker_transactions WHERE backend=pg_backend_pid()`); err != nil {
		return err
	}
	w.signal("inside_callback")
	return recordOutcome(ctx, tx, d, outcome)
}

const targetInvariant = `name IS NOT NULL AND new_label IS NOT NULL AND name COLLATE "C" ~ '^[A-Za-z0-9_]+$' AND new_label COLLATE "C" ~ '^[A-Za-z0-9_]+$' AND legacy_name=name||'|'||new_label`
const backfillSQL = `UPDATE main.upgrade_people SET (name,new_label)=((app_evolution.convert_person(legacy_name,NULL,NULL,true))[1],(app_evolution.convert_person(legacy_name,NULL,NULL,true))[2]) WHERE _sync_scope_id=$1 AND id=$2 AND NOT (` + targetInvariant + `)`

func applySQLRow(ctx context.Context, tx oversync.DatabaseWriteTx, d BatchDescriptor, row SelectedRow) (string, error) {
	valid, err := verifySelectedRow(ctx, tx, d, row)
	if err != nil {
		return "", err
	}
	if valid {
		return "valid", resolveRowWork(ctx, tx, d, row)
	}
	tag, err := tx.Exec(ctx, backfillSQL, d.Scope, row.Key)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", fmt.Errorf("selected_row_not_updated")
	}
	return "changed", resolveRowWork(ctx, tx, d, row)
}

func verifySelectedRow(ctx context.Context, tx applicationTx, d BatchDescriptor, row SelectedRow) (bool, error) {
	var legacy, version string
	var valid bool
	err := tx.QueryRow(ctx, `SELECT legacy_name,xmin::text,(`+targetInvariant+`) FROM main.upgrade_people WHERE _sync_scope_id=$1 AND id=$2 FOR UPDATE`, d.Scope, row.Key).Scan(&legacy, &version, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("selected_row_disappeared")
	}
	if err != nil {
		return false, err
	}
	if version != row.Version || digest([]byte(legacy)) != row.InputDigest {
		return false, fmt.Errorf("selected_source_changed")
	}
	return valid, nil
}

func resolveRowWork(ctx context.Context, tx oversync.DatabaseWriteTx, d BatchDescriptor, row SelectedRow) error {
	_, err := tx.Exec(ctx, `DELETE FROM app_evolution.row_work WHERE job=$1 AND incarnation=$2 AND row_key=$3`, d.Job, d.Incarnation, row.Key)
	return err
}

func recordOutcome(ctx context.Context, tx oversync.DatabaseWriteTx, d BatchDescriptor, outcome batchOutcome) error {
	state := "completed"
	if outcome.Stale > 0 {
		state = "reselect"
	}
	tag, err := tx.Exec(ctx, `UPDATE app_evolution.batches SET state=$2,outcome=$3,metrics_complete=$4 WHERE operation_id=$1 AND state='pending'`, d.OperationID, state, outcome, outcome.Changed == 0)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("batch_outcome_identity_changed")
	}
	_, err = tx.Exec(ctx, `UPDATE app_evolution.jobs SET scanned=scanned+$2,changed=changed+$3,skipped=skipped+$4,stale=stale+$5 WHERE job=$1`, d.Job, outcome.Scanned, outcome.Changed, outcome.Skipped, outcome.Stale)
	if err != nil {
		return err
	}
	if outcome.Stale > 0 {
		return checkJobEvidenceCapacity(ctx, tx)
	}
	if len(d.Rows) == 0 {
		return fmt.Errorf("empty descriptor")
	}
	_, err = tx.Exec(ctx, `UPDATE app_evolution.jobs SET cursor_sequence=$2,cursor_key=$3 WHERE job=$1`, d.Job, d.ScopeSequence, d.Rows[len(d.Rows)-1].Key)
	if err != nil {
		return err
	}
	return checkJobEvidenceCapacity(ctx, tx)
}

// checkpointNoop revalidates after ExecWrite's entire transaction rolled back.
// It emits no registered DML and never claims durable scope initialization.
func (w *Worker) checkpointNoop(ctx context.Context, d BatchDescriptor) error {
	return w.checkpointCurrentRows(ctx, d, false)
}

func (w *Worker) checkpointCurrentRows(ctx context.Context, d BatchDescriptor, expired bool) error {
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		deleted, err := lockDescriptorScope(ctx, tx, d)
		if err != nil {
			return err
		}
		outcome := batchOutcome{Deleted: deleted}
		for _, row := range d.Rows {
			outcome.Scanned++
			stale, err := checkpointRow(ctx, tx, d, row, deleted)
			if err != nil {
				return err
			}
			if stale {
				if !expired {
					return fmt.Errorf("noop_invariant_not_satisfied")
				}
				outcome.Stale++
			} else {
				outcome.Skipped++
			}
		}
		w.signal("inside_noop_followup")
		if err := recordOutcome(ctx, tx, d, outcome); err != nil {
			return err
		}
		if expired && outcome.Stale > 0 {
			_, err := tx.Exec(ctx, `UPDATE app_evolution.batches SET state='expired_reselect' WHERE operation_id=$1`, d.OperationID)
			if err != nil {
				return err
			}
			return checkJobEvidenceCapacity(ctx, tx)
		}
		return nil
	})
}

// Expiry cannot renew an identity. A terminal application outcome survives
// receipt pruning. Otherwise validate current rows; unresolved work is retired
// as a conditional attempt and reselected with a new UUID and fresh preparation.
func (w *Worker) reconcileExpired(ctx context.Context, d BatchDescriptor) error {
	if d.Repair != nil {
		_, err := w.prepareRepair(ctx, d.Repair.FailedOperation, d.Repair.Key, d.Repair.Replacement)
		return err
	}
	var state string
	if err := w.Pool.QueryRow(ctx, `SELECT state FROM app_evolution.batches WHERE operation_id=$1`, d.OperationID).Scan(&state); err != nil {
		return err
	}
	if state != "pending" {
		return nil
	}
	return w.checkpointCurrentRows(ctx, d, true)
}

// checkpointRow locks the current business row but writes only application
// progress. An invalid survivor stays unresolved after exact source verification.
func checkpointRow(ctx context.Context, tx pgx.Tx, d BatchDescriptor, row SelectedRow, deleted bool) (bool, error) {
	if deleted {
		return false, fmt.Errorf("selected_scope_disappeared")
	}
	valid, err := verifySelectedRow(ctx, tx, d, row)
	if err != nil {
		return false, err
	}
	return !valid, nil
}

type conversionFailure struct {
	Row      SelectedRow
	Category string
}

func (e *conversionFailure) Error() string { return e.Category }
