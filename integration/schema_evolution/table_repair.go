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

// reserveTableAttempt persists rate/attempt accounting before entering ExecWrite.
// A crash consumes an attempt and cannot reset pacing on restart.
func (w *TableWorker) reserveTableAttempt(ctx context.Context, d tableDescriptor) error {
	for {
		var wait time.Duration
		err := w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
			if err := verifyTableDescriptor(ctx, tx, d, run); err != nil {
				return err
			}
			var attempts int
			var next, now time.Time
			if err := tx.QueryRow(ctx, `SELECT b.attempts,r.next_batch_at,clock_timestamp() FROM app_evolution.table_batches b CROSS JOIN app_evolution.table_run r WHERE b.operation_id=$1 AND r.singleton`, d.Operation).Scan(&attempts, &next, &now); err != nil {
				return err
			}
			if attempts >= run.Bounds.Retries {
				return fmt.Errorf("table attempt budget exhausted; reconcile before explicit repair")
			}
			if next.After(now) {
				wait = next.Sub(now)
				return nil
			}
			interval := max(run.Bounds.Interval, time.Duration((int64(run.Bounds.Bytes)*int64(time.Second)+run.Bounds.MaxHistoryBytesPerSecond-1)/run.Bounds.MaxHistoryBytesPerSecond))
			if _, err := tx.Exec(ctx, `UPDATE app_evolution.table_batches SET attempts=attempts+1 WHERE operation_id=$1`, d.Operation); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE app_evolution.table_run SET next_batch_at=$1 WHERE singleton`, now.Add(interval))
			return err
		})
		if err != nil {
			return err
		}
		if wait == 0 {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *TableWorker) recordTableFailure(ctx context.Context, d tableDescriptor, cause error) error {
	if cause == nil {
		return nil
	}
	var unknown *oversync.CommitOutcomeUnknownError
	var unconfirmed *oversync.WriteRollbackUnconfirmedError
	if errors.As(cause, &unknown) || errors.As(cause, &unconfirmed) {
		return cause
	}
	// No payload or SQL diagnostic is copied into evidence. Original errors return
	// to the controlling application; the durable category is bounded and stable.
	err := w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
		if err := verifyTableDescriptor(ctx, tx, d, run); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE app_evolution.table_batches SET state='failed',error='apply_rolled_back' WHERE operation_id=$1 AND state='pending'`, d.Operation)
		return err
	})
	return errors.Join(cause, err)
}

// RepairTableBatch explicitly retries the same frozen transformation after an
// operational defect has been corrected. It never changes selected keys or
// deadlines. Old evidence and reservations remain charged. It cannot repair a
// different conversion algorithm; that requires a separately accepted plan.
func (w *TableWorker) RepairTableBatch(ctx context.Context, operation uuid.UUID) error {
	return w.withWorker(ctx, "cleaned", func(ctx context.Context, _ tableRun) error {
		return w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
			return repairTableBatchTx(ctx, tx, run, operation)
		})
	})
}

func repairTableBatchTx(ctx context.Context, tx pgx.Tx, run tableRun, operation uuid.UUID) error {
	var d tableDescriptor
	var state, hash string
	var replaced bool
	if err := tx.QueryRow(ctx, `SELECT descriptor,state,hash,EXISTS(SELECT 1 FROM app_evolution.table_batches x WHERE x.replaces=b.operation_id) FROM app_evolution.table_batches b WHERE operation_id=$1 FOR UPDATE`, operation).Scan(&d, &state, &hash, &replaced); err != nil {
		return err
	}
	if replaced {
		return nil
	}
	if state != "failed" && state != "pending" {
		return fmt.Errorf("table repair requires unresolved work")
	}
	if hash != digest(tableJSON(d)) || d.Plan != run.ID || d.Source != run.Source || !time.Now().Before(d.Deadline) {
		return fmt.Errorf("table repair identity/deadline mismatch")
	}
	// claimWorker's exclusive admission fence has joined predecessor operations.
	// An absent receipt AND absent atomic applied checkpoint now prove rollback.
	var receipt bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.scope_write_receipts WHERE operation_id=$1)`, d.Operation).Scan(&receipt); err != nil {
		return err
	}
	if receipt {
		return fmt.Errorf("table repair cannot discard retained committed evidence")
	}
	if run.History+int64(run.Bounds.Bytes) > run.Bounds.MaxHistoryBytes || run.Bundles+1 > run.Bounds.MaxBundles {
		return fmt.Errorf("table repair capacity exhausted")
	}
	if _, err := tx.Exec(ctx, `UPDATE app_evolution.table_batches SET state='replaced',error=COALESCE(error,'rollback_reconciled') WHERE operation_id=$1`, operation); err != nil {
		return err
	}
	d.Operation = uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.table_batches(operation_id,epoch,descriptor,hash,evidence_reserve,history_reserve,replaces) VALUES($1,$2,$3,$4,$5,$6,$7)`, d.Operation, d.Epoch, d, digest(tableJSON(d)), len(tableJSON(d))+1024, run.Bounds.Bytes, operation); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE app_evolution.table_run SET history_bytes=history_bytes+$1,bundles=bundles+1 WHERE singleton`, run.Bounds.Bytes)
	return err
}
