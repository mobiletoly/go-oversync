// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	scopeReceiptCleanupInterval         = time.Minute
	scopeReceiptCleanupBatchSize        = 500
	scopeReceiptCleanupMaxBatchesPerRun = 4
	scopeReceiptCleanupBatchTimeout     = 5 * time.Second
)

func (s *SyncService) startScopeReceiptCleanupWorker() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycle != serviceLifecycleRunning || s.bootstrapReadiness != bootstrapReadinessReady {
		return
	}
	s.scopeReceiptCleanupMu.Lock()
	defer s.scopeReceiptCleanupMu.Unlock()
	if s.scopeReceiptCleanupDone != nil {
		return
	}
	ctx, cancel := context.WithCancel(withSyncMaintenanceContext(context.Background()))
	done := make(chan struct{})
	logger := s.logger
	s.scopeReceiptCleanupCancel = cancel
	s.scopeReceiptCleanupDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(scopeReceiptCleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case reason := <-s.scopeReceiptCleanupTrigger:
				s.runScopeReceiptCleanup(ctx, logger, reason)
			case <-ticker.C:
				s.runScopeReceiptCleanup(ctx, logger, "periodic")
			}
		}
	}()
}

func (s *SyncService) stopScopeReceiptCleanupWorker() {
	s.scopeReceiptCleanupMu.Lock()
	if s.scopeReceiptCleanupCancel != nil {
		s.scopeReceiptCleanupCancel()
	}
	s.scopeReceiptCleanupMu.Unlock()
}

func (s *SyncService) waitScopeReceiptCleanupWorker(ctx context.Context) error {
	s.scopeReceiptCleanupMu.Lock()
	done := s.scopeReceiptCleanupDone
	s.scopeReceiptCleanupMu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for scope receipt cleanup worker: %w", ctx.Err())
	}
}

func (s *SyncService) waitSyncMaintenanceWorkers(ctx context.Context) error {
	if err := s.waitSnapshotCleanupWorker(ctx); err != nil {
		return err
	}
	return s.waitScopeReceiptCleanupWorker(ctx)
}

func (s *SyncService) requestScopeReceiptCleanup(reason string) {
	if s == nil || s.scopeReceiptCleanupTrigger == nil {
		return
	}
	select {
	case s.scopeReceiptCleanupTrigger <- reason:
	default:
	}
}

func (s *SyncService) runScopeReceiptCleanup(ctx context.Context, logger *slog.Logger, reason string) {
	startedAt := time.Now()
	var deleted int64
	outcome := "completed"
	batches := 0
	for batch := 0; batch < scopeReceiptCleanupMaxBatchesPerRun; batch++ {
		batches++
		batchCtx, cancel := context.WithTimeout(ctx, scopeReceiptCleanupBatchTimeout)
		batchDeleted, err := s.cleanupScopeReceiptBatch(batchCtx)
		cancel()
		if err != nil {
			outcome = "failed"
			if ctx.Err() != nil {
				outcome = "cancelled"
			} else {
				logger.Warn("Scope receipt cleanup batch failed", "reason", reason, "batch", batch+1, "error", err)
			}
			logger.Info("Scope receipt cleanup run completed", "reason", reason, "outcome", outcome, "duration", time.Since(startedAt), "batch_count", batches, "deleted_receipts", deleted)
			return
		}
		deleted += batchDeleted
		if batchDeleted < scopeReceiptCleanupBatchSize {
			break
		}
	}
	logger.Info("Scope receipt cleanup run completed", "reason", reason, "outcome", outcome, "duration", time.Since(startedAt), "batch_count", batches, "deleted_receipts", deleted)
}

func (s *SyncService) cleanupScopeReceiptBatch(ctx context.Context) (int64, error) {
	var deleted int64
	err := withOriginTransaction(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		if s.config.Readiness != nil {
			if adapter, ok := s.config.Readiness.Adapter.(OperationAdmissionAdapter); ok {
				if err := adapter.Admit(ctx, tx); err != nil {
					return err
				}
			}
		}

		tag, err := tx.Exec(ctx, `
			WITH cleanup_clock AS MATERIALIZED (
				SELECT clock_timestamp() AS cleanup_time
			), candidates AS MATERIALIZED (
				SELECT receipt.ctid
				FROM sync.scope_write_receipts AS receipt
				CROSS JOIN cleanup_clock
				WHERE receipt.receipt_state = 'completed'
				  AND receipt.receipt_expires_at <= cleanup_clock.cleanup_time
				ORDER BY receipt.receipt_expires_at, receipt.user_pk, receipt.operation_id
				LIMIT $1
				FOR UPDATE OF receipt SKIP LOCKED
			)
			DELETE FROM sync.scope_write_receipts AS receipt
			USING candidates
			WHERE receipt.ctid = candidates.ctid
		`, scopeReceiptCleanupBatchSize)
		if err != nil {
			return fmt.Errorf("delete expired scope write receipts: %w", err)
		}
		deleted = tag.RowsAffected()
		return nil
	})
	return deleted, err
}
