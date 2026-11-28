package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// TableBounds extends the existing immutable maintenance budget with staging limits.
type TableBounds struct {
	Bounds
	CleanupSessions, CleanupRows, CleanupBytes, MaxCleanupBatches int
}

func (b TableBounds) validate() error {
	if err := b.Bounds.validate(); err != nil {
		return err
	}
	if b.CleanupSessions < 1 || b.CleanupSessions > 64 || b.CleanupRows < 1 || b.CleanupRows > 4096 || b.CleanupBytes < 1024 || b.CleanupBytes > 1<<20 || b.MaxCleanupBatches < 1 || b.MaxCleanupBatches > 100000 {
		return fmt.Errorf("invalid table cleanup bounds")
	}
	return nil
}

type tableRun struct {
	Epoch                                       int64
	ID                                          uuid.UUID
	Bounds                                      TableBounds
	Source                                      string
	Watermark                                   int64
	Phase                                       string
	Batches, Evidence, History, Bundles, Cursor int64
	CensusComplete                              bool
	Validation                                  tableValidationCursor
}

func loadTableRun(ctx context.Context, r oversync.DatabaseReadinessReader) (tableRun, error) {
	var run tableRun
	err := r.QueryRow(ctx, `SELECT epoch,plan_id,bounds,source_digest,watermark,phase,batches,evidence_bytes,history_bytes,bundles,census_cursor,census_complete,validation_cursor FROM app_evolution.table_run WHERE singleton`).Scan(&run.Epoch, &run.ID, &run.Bounds, &run.Source, &run.Watermark, &run.Phase, &run.Batches, &run.Evidence, &run.History, &run.Bundles, &run.Cursor, &run.CensusComplete, &run.Validation)
	return run, err
}

func (r tableRun) check(c migrationCapability, phase string) error {
	if err := r.Bounds.validate(); err != nil {
		return err
	}
	if r.Epoch != c.epoch || r.Source != tableSourceDigest() || r.Phase != phase {
		return fmt.Errorf("table plan/phase drift")
	}
	if !time.Now().Before(r.Bounds.Deadline) {
		return fmt.Errorf("table deadline exhausted")
	}
	return nil
}

// ActivateTableEvolution freezes the plan before cleanup. No restart can widen
// limits, replace source/SQL identities or capture a new scope watermark.
func (a *Authority) ActivateTableEvolution(ctx context.Context, id uuid.UUID, b TableBounds, capacity Capacity) error {
	if id == uuid.Nil {
		return fmt.Errorf("table plan identity required")
	}
	if err := b.validate(); err != nil {
		return err
	}
	if !time.Now().Before(b.Deadline) || !capacity.Available || capacity.FreeDiskBytes < b.MinFreeDiskBytes {
		return fmt.Errorf("table activation deadline/capacity unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, b.QueryTimeout)
	defer cancel()
	return pgx.BeginTxFunc(ctx, a.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := lockAdmission(ctx, tx, true); err != nil {
			return err
		}
		var epoch int64
		var closed, requested bool
		if err := tx.QueryRow(ctx, `SELECT b.closed,m.requested,m.epoch FROM app_evolution.barrier b CROSS JOIN app_evolution.maintenance m WHERE b.singleton AND m.singleton AND b.revision=m.epoch`).Scan(&closed, &requested, &epoch); err != nil {
			return err
		}
		if !closed || !requested {
			return fmt.Errorf("completed table drain required")
		}
		_, err := tx.Exec(ctx, `INSERT INTO app_evolution.table_run(singleton,epoch,plan_id,bounds,source_digest,watermark) SELECT true,$1,$2,$3,$4,COALESCE(max(sequence),0) FROM app_evolution.scope_inventory ON CONFLICT DO NOTHING`, epoch, id, b, tableSourceDigest())
		if err != nil {
			return err
		}
		run, err := loadTableRun(ctx, tx)
		if err != nil {
			return err
		}
		if run.Epoch != epoch || run.ID != id || run.Source != tableSourceDigest() || digest(encodeJSON(run.Bounds)) != digest(encodeJSON(b)) {
			return fmt.Errorf("immutable table plan changed")
		}
		return checkTableCapacity(ctx, tx, run)
	})
}

// TableWorker uses the same ownership/admission locks as the column worker.
// Only one maintenance worker can own either kind of work in an epoch.
type TableWorker struct {
	Pool     *pgxpool.Pool
	Service  *oversync.SyncService
	Capacity func(context.Context) (Capacity, error)
	boundary func(string)
}

func (w *TableWorker) signal(point string) {
	if w.boundary != nil {
		w.boundary(point)
	}
}

func (w *TableWorker) withWorker(ctx context.Context, phase string, fn func(context.Context, tableRun) error) error {
	if w.Pool == nil || w.Service == nil || w.Capacity == nil {
		return fmt.Errorf("table worker dependencies required")
	}
	expectedPhase := TablesCleanup
	if phase == "cleaned" {
		expectedPhase = TablesBackfill
	}
	if report := w.Service.Readiness(); !report.Ready || report.DeploymentRevision != int64(expectedPhase) {
		return fmt.Errorf("table worker runtime phase mismatch")
	}
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	run, err := loadTableRun(readCtx, w.Pool)
	readCancel()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, run.Bounds.Deadline)
	defer cancel()
	lockCtx, lockCancel := context.WithTimeout(ctx, run.Bounds.QueryTimeout)
	lock, err := acquireSessionLock(lockCtx, w.Pool, workerLock)
	lockCancel()
	if err != nil {
		return err
	}
	defer releaseSessionLock(lock, workerLock)
	c, err := claimWorker(ctx, lock)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, migrationContextKey{}, c)
	if (phase == "cleanup" && run.Phase == "cleaned") || (phase == "cleaned" && run.Phase == "populated") {
		phase = run.Phase
	}
	if err := run.check(c, phase); err != nil {
		return err
	}
	if err := w.checkCapacity(ctx, run); err != nil {
		return err
	}
	return fn(ctx, run)
}

func (w *TableWorker) checkCapacity(ctx context.Context, run tableRun) error {
	bounded, cancel := context.WithTimeout(ctx, run.Bounds.QueryTimeout)
	defer cancel()
	capacity, err := w.Capacity(bounded)
	if err != nil {
		return err
	}
	if !capacity.Available || capacity.FreeDiskBytes < run.Bounds.MinFreeDiskBytes || capacity.RetentionRemaining < run.Bounds.MinRetentionRemaining || capacity.Lag > run.Bounds.MaxLag || capacity.HistoryBytesPerSecond > run.Bounds.MaxHistoryBytesPerSecond || capacity.BundlesPerSecond > run.Bounds.MaxBundlesPerSecond {
		return fmt.Errorf("table worker capacity unavailable")
	}
	return nil
}

func (w *TableWorker) transaction(ctx context.Context, fn func(context.Context, pgx.Tx, tableRun) error) error {
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	run, err := loadTableRun(readCtx, w.Pool)
	readCancel()
	if err != nil {
		return err
	}
	ctx, stop := context.WithDeadline(ctx, run.Bounds.Deadline)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, run.Bounds.QueryTimeout)
	defer cancel()
	if err := w.checkCapacity(ctx, run); err != nil {
		return err
	}
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := fenceWorkerMetadata(ctx, tx); err != nil {
			return err
		}
		// Serialize budget reservations and metadata against all worker operations.
		var ignored bool
		if err := tx.QueryRow(ctx, `SELECT singleton FROM app_evolution.table_run WHERE singleton FOR UPDATE`).Scan(&ignored); err != nil {
			return err
		}
		current, err := loadTableRun(ctx, tx)
		if err != nil {
			return err
		}
		c, _ := ctx.Value(migrationContextKey{}).(migrationCapability)
		if err := current.check(c, run.Phase); err != nil {
			return err
		}
		if err := fn(ctx, tx, current); err != nil {
			return err
		}
		return checkTableCapacity(ctx, tx, current)
	})
}

func checkTableCapacity(ctx context.Context, r oversync.DatabaseReadinessReader, run tableRun) error {
	var bytes int64
	if err := r.QueryRow(ctx, `SELECT COALESCE(sum(pg_column_size(t)),0)+(SELECT COALESCE(sum(evidence_reserve),0) FROM app_evolution.table_batches) FROM (
 SELECT to_jsonb(r) AS value FROM app_evolution.table_run r
 UNION ALL SELECT to_jsonb(c) FROM app_evolution.table_cleanup c
 UNION ALL SELECT to_jsonb(s) FROM app_evolution.table_scopes s
 UNION ALL SELECT to_jsonb(b) FROM app_evolution.table_batches b) t`).Scan(&bytes); err != nil {
		return err
	}
	if bytes > run.Bounds.MaxEvidenceBytes {
		return fmt.Errorf("table evidence capacity exhausted")
	}
	return nil
}

func tableJSON(v any) []byte { b, _ := json.Marshal(v); return b }
