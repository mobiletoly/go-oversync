package oversync

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *SyncService) applyRetentionPolicyForUser(ctx context.Context, tx pgx.Tx, userPK int64) error {
	if s == nil {
		return nil
	}
	retainedWindow := s.retainedBundlesPerUser()
	if retainedWindow <= 0 {
		return nil
	}

	state, err := loadRetainedHistoryStateByUserPK(ctx, tx, userPK)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}

	latestBundleSeq := state.highestBundleSeq()
	targetFloor := latestBundleSeq - retainedWindow
	if targetFloor < 0 {
		targetFloor = 0
	}
	if targetFloor <= state.RetainedFloor {
		return nil
	}

	if batchSize := s.retentionPruneBatchSize(); batchSize > 0 && targetFloor-state.RetainedFloor > batchSize {
		targetFloor = state.RetainedFloor + batchSize
	}

	if _, err := tx.Exec(ctx, `
		UPDATE sync.user_state
		SET retained_bundle_floor = $2
		WHERE user_pk = $1
	`, userPK, targetFloor); err != nil {
		return fmt.Errorf("advance retained bundle floor for user_pk %d: %w", userPK, err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq <= $2
	`, userPK, targetFloor); err != nil {
		return fmt.Errorf("delete pruned bundle history for user_pk %d: %w", userPK, err)
	}
	return nil
}
