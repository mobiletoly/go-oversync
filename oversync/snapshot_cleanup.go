package oversync

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type snapshotCleanupBatchResult struct {
	candidateSessions int64
	deletedRows       int64
	deletedSessions   int64
	oldestExpiryAge   time.Duration
}

func (s *SyncService) startSnapshotCleanupWorker() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycle != serviceLifecycleRunning || s.bootstrapReadiness != bootstrapReadinessReady {
		return
	}
	s.snapshotCleanupMu.Lock()
	defer s.snapshotCleanupMu.Unlock()
	if s.snapshotCleanupDone != nil {
		return
	}
	if s.snapshotHooks != nil && s.snapshotHooks.beforeSnapshotCleanupWorkerPublish != nil {
		s.snapshotHooks.beforeSnapshotCleanupWorkerPublish()
	}
	ctx, cancel := context.WithCancel(withSyncMaintenanceContext(context.Background()))
	done := make(chan struct{})
	s.snapshotCleanupCancel = cancel
	s.snapshotCleanupDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.config.SnapshotCleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case reason := <-s.snapshotCleanupTrigger:
				s.runSnapshotCleanup(ctx, reason)
			case <-ticker.C:
				s.runSnapshotCleanup(ctx, "periodic")
			}
		}
	}()
}

func (s *SyncService) waitSnapshotCleanupWorker(ctx context.Context) error {
	s.snapshotCleanupMu.Lock()
	done := s.snapshotCleanupDone
	s.snapshotCleanupMu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for snapshot cleanup worker: %w", ctx.Err())
	}
}

func (s *SyncService) stopSnapshotCleanupWorker() {
	s.snapshotCleanupMu.Lock()
	if s.snapshotCleanupCancel != nil {
		s.snapshotCleanupCancel()
	}
	s.snapshotCleanupMu.Unlock()
}

func (s *SyncService) requestSnapshotCleanup(reason string) {
	if s == nil || s.snapshotCleanupTrigger == nil {
		return
	}
	select {
	case s.snapshotCleanupTrigger <- reason:
	default:
	}
}

func (s *SyncService) runSnapshotCleanup(ctx context.Context, reason string) {
	startedAt := time.Now()
	s.snapshotMetrics.cleanupRuns.Add(1)
	activeRuns := s.snapshotMetrics.activeCleanupRuns.Add(1)
	observeAtomicHighWater(&s.snapshotMetrics.cleanupRunHighWater, activeRuns)
	defer s.snapshotMetrics.activeCleanupRuns.Add(-1)

	var totals snapshotCleanupBatchResult
	batchCount := 0
	outcome := "completed"
	for batch := 0; batch < s.config.SnapshotCleanupMaxBatchesPerRun; batch++ {
		batchCount++
		s.snapshotMetrics.cleanupBatches.Add(1)
		batchStartedAt := time.Now()
		batchCtx, cancel := context.WithTimeout(ctx, s.config.SnapshotCleanupBatchTimeout)
		result, err := s.cleanupSnapshotBatch(batchCtx)
		cancel()
		batchDuration := time.Since(batchStartedAt)
		observeAtomicHighWater(&s.snapshotMetrics.cleanupBatchDurationHighWater, batchDuration.Nanoseconds())
		observeAtomicHighWater(&s.snapshotMetrics.cleanupOldestExpiryAgeHighWater, result.oldestExpiryAge.Nanoseconds())
		observeAtomicHighWater(&s.snapshotMetrics.cleanupCandidateSessionsHighWater, result.candidateSessions)
		observeAtomicHighWater(&s.snapshotMetrics.cleanupDeletedRowsHighWater, result.deletedRows)
		observeAtomicHighWater(&s.snapshotMetrics.cleanupDeletedSessionsHighWater, result.deletedSessions)
		if err != nil {
			outcome = "failed"
			if ctx.Err() != nil {
				outcome = "cancelled"
				s.snapshotMetrics.cleanupCancellations.Add(1)
			} else {
				s.snapshotMetrics.cleanupFailures.Add(1)
				s.logger.Warn("Snapshot cleanup batch failed", "reason", reason, "batch", batch+1, "error", err)
			}
			s.logger.Info("Snapshot cleanup batch completed", "reason", reason, "batch", batch+1, "outcome", outcome, "duration", batchDuration, "candidate_sessions", result.candidateSessions, "deleted_rows", result.deletedRows, "deleted_sessions", result.deletedSessions, "oldest_selected_expiry_age", result.oldestExpiryAge)
			s.logger.Info("Snapshot cleanup run completed", "reason", reason, "outcome", outcome, "duration", time.Since(startedAt), "batch_count", batchCount, "candidate_sessions", totals.candidateSessions, "deleted_rows", totals.deletedRows, "deleted_sessions", totals.deletedSessions, "oldest_selected_expiry_age", totals.oldestExpiryAge, "active_run_high_water", s.snapshotMetrics.cleanupRunHighWater.Load())
			return
		}
		totals.candidateSessions += result.candidateSessions
		totals.deletedRows += result.deletedRows
		totals.deletedSessions += result.deletedSessions
		if result.oldestExpiryAge > totals.oldestExpiryAge {
			totals.oldestExpiryAge = result.oldestExpiryAge
		}
		s.logger.Info("Snapshot cleanup batch completed", "reason", reason, "batch", batch+1, "outcome", outcome, "duration", batchDuration, "candidate_sessions", result.candidateSessions, "deleted_rows", result.deletedRows, "deleted_sessions", result.deletedSessions, "oldest_selected_expiry_age", result.oldestExpiryAge)
		if result.candidateSessions == 0 || (result.deletedRows == 0 && result.deletedSessions == 0) {
			break
		}
	}
	s.logger.Info("Snapshot cleanup run completed", "reason", reason, "outcome", outcome, "duration", time.Since(startedAt), "batch_count", batchCount, "candidate_sessions", totals.candidateSessions, "deleted_rows", totals.deletedRows, "deleted_sessions", totals.deletedSessions, "oldest_selected_expiry_age", totals.oldestExpiryAge, "active_run_high_water", s.snapshotMetrics.cleanupRunHighWater.Load())
}

func (s *SyncService) cleanupSnapshotBatch(ctx context.Context) (result snapshotCleanupBatchResult, err error) {
	err = withOriginTransaction(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		if s.config.Readiness != nil {
			if adapter, ok := s.config.Readiness.Adapter.(OperationAdmissionAdapter); ok {
				if err := adapter.Admit(ctx, tx); err != nil {
					return err
				}
			}
		}

		rows, err := tx.Query(ctx, `
			SELECT snapshot_id::text, expires_at, transaction_timestamp()
			FROM sync.snapshot_sessions
			WHERE expires_at <= transaction_timestamp()
			ORDER BY expires_at, snapshot_id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		`, s.config.SnapshotCleanupBatchSessions)
		if err != nil {
			return fmt.Errorf("select expired snapshot sessions: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			var expiresAt, transactionTime time.Time
			if err := rows.Scan(&id, &expiresAt, &transactionTime); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
			if expiryAge := transactionTime.Sub(expiresAt); expiryAge > result.oldestExpiryAge {
				result.oldestExpiryAge = expiryAge
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		result.candidateSessions = int64(len(ids))
		if len(ids) == 0 {
			return nil
		}

		tag, err := tx.Exec(ctx, `
			WITH doomed AS (
				SELECT ctid
				FROM sync.snapshot_session_rows
				WHERE snapshot_id = ANY($1::uuid[])
				ORDER BY snapshot_id, row_ordinal
				LIMIT $2
			)
			DELETE FROM sync.snapshot_session_rows AS row
			USING doomed
			WHERE row.ctid = doomed.ctid
		`, ids, s.config.SnapshotCleanupBatchRows)
		if err != nil {
			return fmt.Errorf("delete expired snapshot rows: %w", err)
		}
		result.deletedRows = tag.RowsAffected()
		tag, err = tx.Exec(ctx, `
			DELETE FROM sync.snapshot_sessions AS session
			WHERE session.snapshot_id = ANY($1::uuid[])
			  AND NOT EXISTS (SELECT 1 FROM sync.snapshot_session_rows AS row WHERE row.snapshot_id=session.snapshot_id)
		`, ids)
		if err != nil {
			return fmt.Errorf("delete empty expired snapshot sessions: %w", err)
		}
		result.deletedSessions = tag.RowsAffected()
		return nil
	})
	if err == nil {
		observeAtomicHighWater(&s.snapshotMetrics.cleanupCandidateSessionsHighWater, result.candidateSessions)
		observeAtomicHighWater(&s.snapshotMetrics.cleanupDeletedRowsHighWater, result.deletedRows)
		observeAtomicHighWater(&s.snapshotMetrics.cleanupDeletedSessionsHighWater, result.deletedSessions)
		observeAtomicHighWater(&s.snapshotMetrics.cleanupOldestExpiryAgeHighWater, result.oldestExpiryAge.Nanoseconds())
	}
	return result, err
}
