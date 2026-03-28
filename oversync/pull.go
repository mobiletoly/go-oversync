// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *SyncService) ProcessPull(
	ctx context.Context,
	actor Actor,
	afterBundleSeq int64,
	maxBundles int,
	targetBundleSeq int64,
) (resp *PullResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	totalStart := s.stageStart()
	defer func() {
		s.observeStageErr(ctx, "pull", "total", totalStart, maxBundles, 0, err)
	}()
	if err := actor.validate(false); err != nil {
		return nil, err
	}
	if maxBundles <= 0 {
		maxBundles = defaultPullBundlesPerRequest
	}
	if maxBundles > defaultMaxBundlesPerPull {
		maxBundles = defaultMaxBundlesPerPull
	}
	if afterBundleSeq < 0 {
		return nil, fmt.Errorf("after_bundle_seq must be >= 0")
	}
	if targetBundleSeq < 0 {
		return nil, fmt.Errorf("target_bundle_seq must be >= 0")
	}

	var txResp *PullResponse
	txStart := s.stageStart()
	err = pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var txErr error
		txResp, txErr = s.processPullQuerier(ctx, tx, actor.UserID, afterBundleSeq, maxBundles, targetBundleSeq)
		return txErr
	})
	s.observeStageErr(ctx, "pull", "transaction", txStart, maxBundles, 0, err)
	if err != nil {
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			if recordErr := s.recordHistoryPrunedError(ctx); recordErr != nil {
				s.logger.Warn("Failed to record history_pruned event", "error", recordErr, "user_id", actor.UserID, "source_id", actor.SourceID)
			}
		}
		return nil, err
	}
	return txResp, nil
}

func (s *SyncService) processPullQuerier(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	afterBundleSeq int64,
	maxBundles int,
	targetBundleSeq int64,
) (*PullResponse, error) {
	if err := requireScopeInitializedQuerier(ctx, tx, userID); err != nil {
		return nil, err
	}
	stageStart := s.stageStart()
	if err := enforceRetainedBundleFloorQuerier(ctx, tx, userID, afterBundleSeq); err != nil {
		s.observeStageErr(ctx, "pull", "retained_floor_check", stageStart, 1, 0, err)
		return nil, err
	}
	s.observeStage(ctx, "pull", "retained_floor_check", stageStart, 1, 0, false)

	stableBundleSeq := targetBundleSeq
	if stableBundleSeq <= 0 {
		stageStart = s.stageStart()
		var err error
		stableBundleSeq, err = userHighestBundleSeqQuerier(ctx, tx, userID)
		s.observeStageErr(ctx, "pull", "resolve_stable_bundle_seq", stageStart, 1, 0, err)
		if err != nil {
			return nil, err
		}
	} else {
		s.observeStage(ctx, "pull", "resolve_stable_bundle_seq", s.stageStart(), 1, 0, false)
	}
	if afterBundleSeq >= stableBundleSeq {
		return &PullResponse{
			StableBundleSeq: stableBundleSeq,
			Bundles:         []Bundle{},
			HasMore:         false,
		}, nil
	}

	stageStart = s.stageStart()
	rows, err := tx.Query(ctx, `
		SELECT bundle_seq
		FROM sync.bundle_log
		WHERE user_id = $1
		  AND bundle_seq > $2
		  AND bundle_seq <= $3
		ORDER BY bundle_seq
		LIMIT $4
	`, userID, afterBundleSeq, stableBundleSeq, maxBundles+1)
	if err != nil {
		s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, maxBundles, 0, err)
		return nil, fmt.Errorf("query bundle pull page: %w", err)
	}
	defer rows.Close()

	bundleSeqs := make([]int64, 0, maxBundles+1)
	for rows.Next() {
		var bundleSeq int64
		if err := rows.Scan(&bundleSeq); err != nil {
			s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, err)
			return nil, fmt.Errorf("scan pulled bundle sequence: %w", err)
		}
		bundleSeqs = append(bundleSeqs, bundleSeq)
	}
	if err := rows.Err(); err != nil {
		s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, err)
		return nil, fmt.Errorf("iterate pulled bundle sequences: %w", err)
	}
	s.observeStage(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, false)

	hasMore := false
	if len(bundleSeqs) > maxBundles {
		hasMore = true
		bundleSeqs = bundleSeqs[:maxBundles]
	}

	bundles := make([]Bundle, 0, len(bundleSeqs))
	stageStart = s.stageStart()
	for _, bundleSeq := range bundleSeqs {
		bundle, err := s.loadCommittedBundle(ctx, tx, userID, bundleSeq)
		if err != nil {
			s.observeStageErr(ctx, "pull", "load_bundles", stageStart, len(bundles), 0, err)
			return nil, err
		}
		bundles = append(bundles, *bundle)
	}
	s.observeStage(ctx, "pull", "load_bundles", stageStart, len(bundles), 0, false)

	return &PullResponse{
		StableBundleSeq: stableBundleSeq,
		Bundles:         bundles,
		HasMore:         hasMore,
	}, nil
}

func userHighestBundleSeqQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string) (int64, error) {
	var maxSeq int64
	err := q.QueryRow(ctx, `
		SELECT GREATEST(
			COALESCE((SELECT MAX(bundle_seq) FROM sync.bundle_log WHERE user_id = @user_id), 0),
			COALESCE((SELECT next_bundle_seq - 1 FROM sync.user_state WHERE user_id = @user_id), 0)
		)
	`, pgx.NamedArgs{"user_id": userID}).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("query user highest bundle seq: %w", err)
	}
	return maxSeq, nil
}

func enforceRetainedBundleFloorQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string, providedSeq int64) error {
	var floor int64
	if err := q.QueryRow(ctx, `
		SELECT COALESCE(retained_bundle_floor, 0)
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&floor); err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return fmt.Errorf("query retained bundle floor: %w", err)
	}
	if providedSeq < floor {
		return &HistoryPrunedError{
			UserID:        userID,
			ProvidedSeq:   providedSeq,
			RetainedFloor: floor,
		}
	}
	return nil
}

func (s *SyncService) recordHistoryPrunedError(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `SELECT nextval('sync.history_pruned_error_seq')`)
	if err != nil {
		return fmt.Errorf("record history_pruned event: %w", err)
	}
	return nil
}
