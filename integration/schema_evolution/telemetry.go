package schemaevolution

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Telemetry contains durable outcomes only. EstimatedRemainingRows is explicitly
// an estimate while bounded batches make progress. No row/scope IDs are metric labels.
type Telemetry struct {
	Scanned, Changed, ValidSkipped, Stale, Failed, Unresolved, Completed int64
	EstimatedRemainingRows                                               int64
	HistoryBytes, Bundles                                                int64
	State, PauseReason                                                   string
	Retries                                                              int64
	Duration, LockWait                                                   time.Duration
	LastCapacity                                                         *Capacity
}

func (w *Worker) Telemetry(ctx context.Context) (Telemetry, error) {
	bounded, cancel := context.WithTimeout(ctx, w.Bounds.QueryTimeout)
	defer cancel()
	var result Telemetry
	err := w.Pool.QueryRow(bounded, `SELECT scanned,changed,skipped,stale,state,pause_reason,history_bytes,bundles,retries,duration_ns,lock_wait_ns,last_capacity FROM app_evolution.jobs WHERE job=$1`, JobID).Scan(&result.Scanned, &result.Changed, &result.ValidSkipped, &result.Stale, &result.State, &result.PauseReason, &result.HistoryBytes, &result.Bundles, &result.Retries, &result.Duration, &result.LockWait, &result.LastCapacity)
	if err != nil {
		return result, err
	}
	err = w.Pool.QueryRow(bounded, `SELECT count(*)+(SELECT count(*) FROM app_evolution.batches WHERE job=$1 AND state='pending'),count(*) FILTER(WHERE category<>'stale_input') FROM app_evolution.row_work WHERE job=$1`, JobID).Scan(&result.Unresolved, &result.Failed)
	if err != nil {
		return result, err
	}
	err = w.Pool.QueryRow(bounded, `SELECT count(*) FROM app_evolution.batches WHERE job=$1 AND state='completed'`, JobID).Scan(&result.Completed)
	if err != nil {
		return result, err
	}
	err = w.Pool.QueryRow(bounded, `SELECT count(*) FROM main.upgrade_people WHERE name IS NULL OR new_label IS NULL OR legacy_name IS DISTINCT FROM name||'|'||new_label`).Scan(&result.EstimatedRemainingRows)
	return result, err
}

// Capture bytes are measured from core's finalized canonical bundle, including
// trigger fanout. Application accounting survives later receipt/history pruning.
// An unobservable post-crash measurement pauses a required capacity policy.
func (w *Worker) reconcileMetrics(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, w.Bounds.QueryTimeout)
	defer cancel()
	rows, err := w.Pool.Query(bounded, `SELECT operation_id FROM app_evolution.batches WHERE job=$1 AND state<>'pending' AND NOT metrics_complete ORDER BY ordinal LIMIT 100`, JobID)
	if err != nil {
		return err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := w.recordMetrics(bounded, id); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) recordMetrics(ctx context.Context, id uuid.UUID) error {
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := fenceWorkerMetadata(ctx, tx); err != nil {
			return err
		}
		var complete bool
		if err := tx.QueryRow(ctx, `SELECT metrics_complete FROM app_evolution.batches WHERE operation_id=$1 FOR UPDATE`, id).Scan(&complete); err != nil {
			return err
		}
		if complete {
			return nil
		}
		var count, bytes int64
		err := tx.QueryRow(ctx, `SELECT count(*),COALESCE(sum(l.byte_count),0) FROM sync.scope_write_receipts r JOIN sync.bundle_log l ON l.user_pk=r.user_pk AND l.bundle_seq=r.committed_bundle_seq WHERE r.operation_id=$1 AND r.writer_id='schema-evolution-backfill' AND r.receipt_state='completed'`, id).Scan(&count, &bytes)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("canonical_history_measurement_unavailable")
		}
		if _, err := tx.Exec(ctx, `UPDATE app_evolution.batches SET metrics_complete=true,history_bytes=$2,bundles=1 WHERE operation_id=$1`, id, bytes); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE app_evolution.jobs SET history_bytes=history_bytes+$2,bundles=bundles+1 WHERE job=$1`, JobID, bytes)
		if err != nil {
			return err
		}
		return checkJobEvidenceCapacity(ctx, tx)
	})
}

// Operational observations are separate from transactional row outcome counters.
// LockWait measures coordinator acquisition; Duration includes throttling and retries.
func (w *Worker) observe(ctx context.Context, field string, value int64) error {
	bounded, cancel := context.WithTimeout(ctx, w.Bounds.QueryTimeout)
	defer cancel()
	var statement string
	switch field {
	case "retries":
		statement = `UPDATE app_evolution.jobs SET retries=retries+$2 WHERE job=$1`
	case "duration_ns":
		statement = `UPDATE app_evolution.jobs SET duration_ns=duration_ns+$2 WHERE job=$1`
	case "lock_wait_ns":
		statement = `UPDATE app_evolution.jobs SET lock_wait_ns=lock_wait_ns+$2 WHERE job=$1`
	default:
		return fmt.Errorf("unknown observation")
	}
	return w.updateEvidence(bounded, statement, JobID, value)
}

func (w *Worker) observeDuration(parent context.Context, started time.Time) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	_ = w.observe(ctx, "duration_ns", time.Since(started).Nanoseconds())
}
