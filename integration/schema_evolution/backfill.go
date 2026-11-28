package schemaevolution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

type Bounds struct {
	Deadline                                                        time.Time
	MaxEvidenceBytes, MaxHistoryBytes, MaxBundles, MinFreeDiskBytes int64
	Rows                                                            int
	Bytes                                                           int
	ScopePage                                                       int
	PendingBytes                                                    int64
	ErrorBytes                                                      int
	Retries                                                         int
	Interval                                                        time.Duration
	QueryTimeout                                                    time.Duration
	MaxHistoryBytesPerSecond                                        int64
	MaxBundlesPerSecond                                             int64
	MaxLag                                                          time.Duration
	MinRetentionRemaining                                           time.Duration
}

func DefaultBounds() Bounds {
	return Bounds{Rows: 100, Bytes: 1 << 20, ScopePage: 100, PendingBytes: 64 << 20, ErrorBytes: 1024, Retries: 3, Interval: time.Second, QueryTimeout: 30 * time.Second, MaxHistoryBytesPerSecond: 1 << 20, MaxBundlesPerSecond: 1, MaxLag: time.Hour, MinRetentionRemaining: 24 * time.Hour}
}
func (b Bounds) validate() error {
	if b.Deadline.IsZero() || b.MaxEvidenceBytes < 1 || b.MaxHistoryBytes < int64(b.Bytes) || b.MaxBundles < 1 || b.MinFreeDiskBytes < 1 {
		return fmt.Errorf("explicit total run limits required")
	}
	if b.Rows < 1 || b.Rows > 100 || b.Bytes < 1 || b.Bytes > 1<<20 || b.ScopePage < 1 || b.ScopePage > 1000 || b.PendingBytes < int64(b.Bytes) || b.ErrorBytes < 64 || b.ErrorBytes > 4096 || b.Retries < 1 || b.Retries > 3 || b.Interval < time.Second || b.QueryTimeout <= 0 || b.QueryTimeout > 30*time.Second || b.MaxHistoryBytesPerSecond < 1 || b.MaxBundlesPerSecond < 1 || b.MaxLag <= 0 || b.MinRetentionRemaining <= 0 {
		return fmt.Errorf("invalid backfill bounds")
	}
	return nil
}

type Capacity struct {
	FreeDiskBytes                           int64
	Available                               bool
	HistoryBytesPerSecond, BundlesPerSecond int64
	Lag, RetentionRemaining                 time.Duration
}
type Worker struct {
	Pool    *pgxpool.Pool
	Manager *oversync.ScopeManager
	Bounds  Bounds
	// Capacity must include application-observed consumer lag and retention headroom;
	// unavailable measurements stop scheduling. Core does not know consumer lag.
	Capacity func(context.Context) (Capacity, error)
	// Private deterministic fault boundaries are used by real process-death tests.
	boundary func(string)
}

func (w *Worker) signal(name string) {
	if w.boundary != nil {
		w.boundary(name)
	}
}

// Activate freezes the finite scope watermark while the application lifecycle
// barrier is closed. Sequence allocation and scope creation use that same guard.
func (a *Authority) Activate(ctx context.Context, bounds Bounds, capacity Capacity) error {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ctx = bounded

	if err := bounds.validate(); err != nil {
		return err
	}
	if !time.Now().Before(bounds.Deadline) || !capacity.Available || capacity.FreeDiskBytes < bounds.MinFreeDiskBytes {
		return fmt.Errorf("activation deadline or disk capacity unavailable")
	}
	var d oversync.DeploymentContract
	if err := a.conn.QueryRow(ctx, `SELECT contract FROM app_evolution.deployment WHERE singleton`).Scan(&d); err != nil {
		return err
	}
	if d.TransitionPhase != "maintaining" {
		return fmt.Errorf("maintaining transition required")
	}
	if err := a.initializeCensus(ctx, bounds); err != nil {
		return err
	}
	for {
		done, err := a.captureCensusPage(ctx, bounds)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}

}

// Run owns one coordinator and one in-flight descriptor. Cancellation returns
// only after current database work stops; resume reconciles durable evidence.
func (w *Worker) Run(ctx context.Context) error {
	if w.Pool == nil || w.Manager == nil || w.Capacity == nil {
		return fmt.Errorf("worker dependencies required")
	}
	if err := w.Bounds.validate(); err != nil {
		return err
	}
	started := time.Now()
	lock, err := acquireSessionLock(ctx, w.Pool, workerLock)
	if err != nil {
		return err
	}
	defer releaseSessionLock(lock, workerLock)
	capability, err := claimWorker(ctx, lock)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, migrationContextKey{}, capability)
	if err := w.observe(ctx, "lock_wait_ns", time.Since(started).Nanoseconds()); err != nil {
		return err
	}
	defer w.observeDuration(ctx, started)
	if err := w.resumeJob(ctx); err != nil {
		return err
	}
	defer func() { w.pause(ctx, "worker_stopped") }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.reconcileMetrics(ctx); err != nil {
			w.pause(ctx, "metrics_unavailable")
			return err
		}
		if !time.Now().Before(w.Bounds.Deadline) {
			err := w.reconcileRunExpiry(ctx)
			w.pause(ctx, "run_deadline_expired")
			return errors.Join(fmt.Errorf("run_deadline_expired"), err)
		}
		if err := w.checkCapacity(ctx); err != nil {
			w.pause(ctx, "capacity")
			return err
		}
		done, err := w.schedulingAttempt(ctx)
		if errors.Is(err, errScopePageYield) {
			continue
		}
		if err != nil {
			w.pause(ctx, "attempt_failed")
			return err
		}
		if done {
			return nil
		}
		timer := time.NewTimer(w.Bounds.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *Worker) pause(parent context.Context, reason string) {
	ctx, cancel := context.WithDeadline(context.WithoutCancel(parent), minDeadline(time.Now().Add(5*time.Second), w.Bounds.Deadline))
	defer cancel()
	_ = w.updateEvidence(ctx, `UPDATE app_evolution.jobs SET state='paused',pause_reason=CASE WHEN pause_reason='' THEN $2 ELSE pause_reason END WHERE job=$1 AND state='running'`, JobID, reason)
}

func (w *Worker) checkCapacity(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, w.Bounds.QueryTimeout)
	defer cancel()
	c, err := w.Capacity(bounded)
	if err != nil {
		return err
	}
	if err := w.updateEvidence(bounded, `UPDATE app_evolution.jobs SET last_capacity=$2 WHERE job=$1`, JobID, c); err != nil {
		return err
	}
	if !c.Available || c.FreeDiskBytes < w.Bounds.MinFreeDiskBytes || c.HistoryBytesPerSecond >= w.Bounds.MaxHistoryBytesPerSecond || c.BundlesPerSecond >= w.Bounds.MaxBundlesPerSecond || c.Lag > w.Bounds.MaxLag || c.RetentionRemaining < w.Bounds.MinRetentionRemaining {
		return fmt.Errorf("capacity unavailable or exceeded")
	}
	return nil
}

func (w *Worker) schedulingAttempt(ctx context.Context) (bool, error) {
	for attempt := 0; attempt < w.Bounds.Retries; attempt++ {
		bounded, cancel := context.WithDeadline(ctx, minDeadline(time.Now().Add(w.Bounds.QueryTimeout), w.Bounds.Deadline))
		done, err := w.step(bounded)
		cancel()
		if err == nil {
			return done, nil
		}
		if !transientBackfillError(err) || attempt+1 == w.Bounds.Retries {
			return false, err
		}
		if err := w.observe(ctx, "retries", 1); err != nil {
			return false, err
		}
		// Retries consume the same rate budget and keep the exact descriptor/deadline.
		timer := time.NewTimer(w.Bounds.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
		if err := w.checkCapacity(ctx); err != nil {
			return false, err
		}
	}
	return false, fmt.Errorf("retry budget exhausted")
}

func transientBackfillError(err error) bool {
	var unknown *oversync.CommitOutcomeUnknownError
	var initializing *oversync.ScopeInitializingError
	return errors.As(err, &unknown) || errors.As(err, &initializing) || errors.Is(err, context.DeadlineExceeded)
}

func (w *Worker) step(ctx context.Context) (bool, error) {
	descriptor, err := w.pendingDescriptor(ctx)
	if err != nil {
		return false, err
	}
	if descriptor == nil {
		descriptor, err = w.prepareNext(ctx)
		if err != nil {
			return false, err
		}
		if descriptor == nil {
			return true, nil
		}
	}
	w.signal("before_apply")
	if !time.Now().Before(descriptor.ValidUntil) {
		return false, w.reconcileExpired(ctx, *descriptor)
	}
	opts, err := descriptor.WriteOptions()
	if err != nil {
		return false, err
	}
	opts.MaxBytesPerBundle = w.Bounds.Bytes
	_, err = w.Manager.ExecWrite(ctx, descriptor.Scope, opts, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		if descriptor.Repair != nil {
			return w.applyRepair(ctx, tx, *descriptor)
		}
		return w.apply(ctx, tx, *descriptor)
	})
	if err == nil {
		w.signal("after_commit")
		return false, w.reconcileMetrics(ctx)
	}
	var unconfirmed *oversync.WriteRollbackUnconfirmedError
	if errors.As(err, &unconfirmed) {
		return false, err
	}
	var conversion *conversionFailure
	if errors.As(err, &conversion) {
		failedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.Bounds.QueryTimeout)
		defer cancel()
		recordErr := w.updateEvidence(failedCtx, `INSERT INTO app_evolution.row_work(job,incarnation,row_key,input_digest,category) VALUES($1,$2,$3,$4,$5) ON CONFLICT(job,incarnation,row_key) DO UPDATE SET input_digest=excluded.input_digest,category=excluded.category`, JobID, descriptor.Incarnation, conversion.Row.Key, conversion.Row.InputDigest, conversion.Category)
		return false, errors.Join(err, recordErr)
	}
	var noop *oversync.ScopeWriteNoCapturedChangesError
	if errors.As(err, &noop) {
		return false, w.checkpointNoop(ctx, *descriptor)
	}
	var expired *oversync.OperationHistoryExpiredError
	var pruned *oversync.OperationHistoryUnavailableError
	if errors.As(err, &expired) || errors.As(err, &pruned) {
		return false, w.reconcileExpired(ctx, *descriptor)
	}
	return false, err
}

// CreateScope serializes sequence allocation with activation/final validation.
// It commits inventory before any registered insertion/import may use the scope.
func CreateScope(ctx context.Context, pool *pgxpool.Pool, scope string) (uuid.UUID, error) {
	if scope == "" {
		return uuid.Nil, fmt.Errorf("scope_id_required")
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = bounded
	incarnation := uuid.New()
	err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := lockAdmission(ctx, tx, false); err != nil {
			return err
		}
		var closed bool
		if err := tx.QueryRow(ctx, `SELECT closed OR (SELECT requested FROM app_evolution.maintenance WHERE singleton) FROM app_evolution.barrier WHERE singleton`).Scan(&closed); err != nil {
			return err
		}
		if closed {
			return fmt.Errorf("application_admission_closed")
		}
		return createScopeInventoryRow(ctx, tx, scope, incarnation)
	})
	return incarnation, err
}

func (w *Worker) resumeJob(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, w.Bounds.QueryTimeout)
	defer cancel()
	ctx = bounded
	var persisted Bounds
	if err := w.Pool.QueryRow(ctx, `SELECT bounds FROM app_evolution.jobs WHERE job=$1 AND revision=$2 AND algorithm=$3 AND enumeration_policy='keyset-incarnation-v1' AND preparation_policy='sql-v3'`, JobID, JobRevision, backfillRequirement.AlgorithmSHA256).Scan(&persisted); err != nil {
		return err
	}
	if err := verifyCensus(ctx, w.Pool); err != nil {
		return err
	}
	if digest(encodeJSON(persisted)) != digest(encodeJSON(w.Bounds)) {
		return fmt.Errorf("immutable job bounds mismatch")
	}
	if err := w.updateEvidence(ctx, `UPDATE app_evolution.jobs SET state='running',pause_reason='' WHERE job=$1 AND state IN ('paused','running')`, JobID); err != nil {
		return err
	}
	return nil
}

// Expiry never grants fresh execution. Only existing receipt/checkpoint evidence
// may be reconciled, with a finite cleanup budget even for a Background caller.
func (w *Worker) reconcileRunExpiry(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, w.Bounds.QueryTimeout)
	defer cancel()
	d, err := w.pendingDescriptor(bounded)
	if err != nil || d == nil {
		return err
	}
	return w.reconcileExpired(bounded, *d)
}
