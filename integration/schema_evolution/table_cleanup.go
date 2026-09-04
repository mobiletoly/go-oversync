package schemaevolution

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Cleanup runs exclusively against the old registration before any business DDL.
// Expiry is checked by PostgreSQL; elapsed TTL alone never counts as removal.
func (w *TableWorker) Cleanup(ctx context.Context) error {
	return w.withWorker(ctx, "cleanup", func(ctx context.Context, run tableRun) error {
		if run.Phase == "cleaned" {
			return w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, _ tableRun) error { return requireEmptyTableStaging(ctx, tx) })
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			operation := uuid.New()
			done, err := w.cleanupBatch(ctx, operation)
			if err != nil {
				return err
			}
			w.signal("cleanup_after_commit")
			if done {
				return nil
			}
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	})
}

func (w *TableWorker) cleanupBatch(ctx context.Context, operation uuid.UUID) (bool, error) {
	done := false
	err := w.transaction(ctx, func(ctx context.Context, tx pgx.Tx, run tableRun) error {
		var prior oversync.StagingCleanupBatch
		err := tx.QueryRow(ctx, `SELECT result FROM app_evolution.table_cleanup WHERE operation_id=$1 AND epoch=$2`, operation, run.Epoch).Scan(&prior)
		if err == nil {
			return nil
		}
		if err != pgx.ErrNoRows {
			return err
		}
		if run.Batches >= int64(run.Bounds.MaxCleanupBatches) {
			return fmt.Errorf("cleanup batch budget exhausted")
		}
		result, err := w.Service.CleanupExpiredStaging(ctx, tx)
		if err != nil {
			return err
		}
		// An empty poll is not a mutating batch, does not grow evidence and cannot
		// exhaust batch capacity simply because a disconnected device's TTL is pending.
		if len(result.Sessions) > 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO app_evolution.table_cleanup(operation_id,epoch,result) VALUES($1,$2,$3)`, operation, run.Epoch, result); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE app_evolution.table_run SET batches=batches+1 WHERE singleton`); err != nil {
				return err
			}
		}
		var snapshots, capture bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.snapshot_sessions),EXISTS(SELECT 1 FROM sync.bundle_capture_stage)`).Scan(&snapshots, &capture); err != nil {
			return err
		}
		if capture {
			return fmt.Errorf("capture residue is not expired staging")
		}
		done = !snapshots
		if done {
			_, err = tx.Exec(ctx, `UPDATE app_evolution.table_run SET phase='cleaned' WHERE singleton`)
		}
		w.signal("cleanup_before_commit")
		return err
	})
	return done, err
}

func requireEmptyTableStaging(ctx context.Context, r oversync.DatabaseReadinessReader) error {
	var pending bool
	if err := r.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.snapshot_sessions) OR EXISTS(SELECT 1 FROM sync.bundle_capture_stage)`).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return fmt.Errorf("table staging is not empty")
	}
	return nil
}
