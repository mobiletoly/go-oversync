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
	proof SchemaGenerationProof,
	afterBundleSeq int64,
	maxBundles int,
	targetBundleSeq int64,
) (resp *PullResponse, err error) {
	if err := rejectRetryableCallbackContext(ctx, "ProcessPull"); err != nil {
		return nil, err
	}
	if err := s.validateClientActor(actor, true); err != nil {
		return nil, err
	}
	if err := validateSchemaGenerationProof(proof); err != nil {
		return nil, err
	}
	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	releaseOperation := done
	defer releaseHistoryProjection(&releaseOperation)
	view, err := s.resolveRequestGeneration(ctx, actor, proof)
	if err != nil {
		return nil, err
	}
	totalStart := s.stageStart()
	defer func() {
		s.observeStageErr(ctx, "pull", "total", totalStart, maxBundles, 0, err)
	}()
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
	if _, err := s.requireExactSourceGenerationBinding(ctx, actor, proof); err != nil {
		return nil, err
	}
	selection, err := selectionFromResolvedGeneration(proof, view)
	if err != nil {
		return nil, err
	}
	var releaseProjection func()
	defer releaseHistoryProjection(&releaseProjection)

	var txResp *PullResponse
	txStart := s.stageStart()
	err = withOriginTransaction(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly, DeferrableMode: pgx.NotDeferrable}, func(tx pgx.Tx) error {
		releaseHistoryProjection(&releaseProjection)
		stableBundleSeq, bundleSeqs, hasMore, txErr := s.planPullPage(ctx, tx, actor.UserID, afterBundleSeq, maxBundles, targetBundleSeq)
		if txErr != nil {
			return txErr
		}
		releaseProjection, txErr = s.acquireHistoryProjection(ctx)
		if txErr != nil {
			return txErr
		}
		txResp, txErr = s.projectPullPageForGeneration(ctx, tx, selection, actor.UserID, stableBundleSeq, bundleSeqs, hasMore)
		return txErr
	})
	s.observeStageErr(ctx, "pull", "transaction", txStart, maxBundles, 0, err)
	if err != nil {
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			if recordErr := s.recordHistoryPrunedError(ctx); recordErr != nil {
				s.logger.Warn("Failed to record history_pruned event", "error_type", fmt.Sprintf("%T", recordErr))
			}
		}
		return nil, err
	}
	transferHistoryProjectionForHTTP(ctx, &txResp.historyProjectionRelease, &releaseProjection, &releaseOperation)
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
	stableBundleSeq, bundleSeqs, hasMore, err := s.planPullPage(ctx, tx, userID, afterBundleSeq, maxBundles, targetBundleSeq)
	if err != nil {
		return nil, err
	}
	bundles := make([]Bundle, 0, len(bundleSeqs))
	stageStart := s.stageStart()
	for _, bundleSeq := range bundleSeqs {
		bundle, err := s.loadCommittedBundle(ctx, tx, userID, bundleSeq)
		if err != nil {
			s.observeStageErr(ctx, "pull", "load_bundles", stageStart, len(bundles), 0, err)
			return nil, err
		}
		bundles = append(bundles, *bundle)
	}
	s.observeStage(ctx, "pull", "load_bundles", stageStart, len(bundles), 0, false)
	return &PullResponse{StableBundleSeq: stableBundleSeq, Bundles: bundles, HasMore: hasMore}, nil
}

func (s *SyncService) planPullPage(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	afterBundleSeq int64,
	maxBundles int,
	targetBundleSeq int64,
) (int64, []int64, bool, error) {
	if err := requireScopeInitializedQuerier(ctx, tx, userID); err != nil {
		return 0, nil, false, err
	}
	retainedState, err := loadRetainedHistoryStateByUserID(ctx, tx, userID)
	if err != nil {
		return 0, nil, false, err
	}
	if retainedState == nil {
		retainedState = &retainedHistoryState{NextBundleSeq: 1}
	}
	stageStart := s.stageStart()
	if err := enforcePullSequenceBoundary(userID, "checkpoint", afterBundleSeq, *retainedState); err != nil {
		s.observeStageErr(ctx, "pull", "retained_floor_check", stageStart, 1, 0, err)
		return 0, nil, false, err
	}
	if targetBundleSeq > 0 {
		if err := enforcePullSequenceBoundary(userID, "target", targetBundleSeq, *retainedState); err != nil {
			s.observeStageErr(ctx, "pull", "retained_floor_check", stageStart, 1, 0, err)
			return 0, nil, false, err
		}
		if targetBundleSeq < afterBundleSeq {
			err := &InvalidPullRequestError{Message: fmt.Sprintf(
				"target_bundle_seq %d must be greater than or equal to after_bundle_seq %d",
				targetBundleSeq,
				afterBundleSeq,
			)}
			s.observeStageErr(ctx, "pull", "retained_floor_check", stageStart, 1, 0, err)
			return 0, nil, false, err
		}
	}
	s.observeStage(ctx, "pull", "retained_floor_check", stageStart, 1, 0, false)

	stableBundleSeq := targetBundleSeq
	if stableBundleSeq <= 0 {
		stageStart = s.stageStart()
		stableBundleSeq = retainedState.highestBundleSeq()
		s.observeStage(ctx, "pull", "resolve_stable_bundle_seq", stageStart, 1, 0, false)
	} else {
		s.observeStage(ctx, "pull", "resolve_stable_bundle_seq", s.stageStart(), 1, 0, false)
	}
	if afterBundleSeq >= stableBundleSeq {
		return stableBundleSeq, []int64{}, false, nil
	}

	stageStart = s.stageStart()
	rows, err := tx.Query(ctx, `
		SELECT bundle_seq
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq > $2
		  AND bundle_seq > $3
		  AND bundle_seq <= $4
		ORDER BY bundle_seq
		LIMIT $5
	`, retainedState.UserPK, afterBundleSeq, retainedState.RetainedFloor, stableBundleSeq, maxBundles+1)
	if err != nil {
		s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, maxBundles, 0, err)
		return 0, nil, false, fmt.Errorf("query bundle pull page: %w", err)
	}
	defer rows.Close()

	bundleSeqs := make([]int64, 0, maxBundles+1)
	expectedBundleSeq := afterBundleSeq + 1
	for rows.Next() {
		var bundleSeq int64
		if err := rows.Scan(&bundleSeq); err != nil {
			s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, err)
			return 0, nil, false, fmt.Errorf("scan pulled bundle sequence: %w", err)
		}
		if bundleSeq != expectedBundleSeq {
			return 0, nil, false, projectionError(GenerationProjectionIntegrityError, "pull", fmt.Sprintf("retained canonical bundle sequence %d is missing", expectedBundleSeq))
		}
		bundleSeqs = append(bundleSeqs, bundleSeq)
		expectedBundleSeq++
	}
	if err := rows.Err(); err != nil {
		s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, err)
		return 0, nil, false, fmt.Errorf("iterate pulled bundle sequences: %w", err)
	}
	s.observeStage(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, false)

	hasMore := false
	if len(bundleSeqs) > maxBundles {
		hasMore = true
		bundleSeqs = bundleSeqs[:maxBundles]
	} else if expectedBundleSeq <= stableBundleSeq {
		return 0, nil, false, projectionError(GenerationProjectionIntegrityError, "pull", fmt.Sprintf("retained canonical bundle sequence %d is missing", expectedBundleSeq))
	}

	return stableBundleSeq, bundleSeqs, hasMore, nil
}

func (s *SyncService) recordHistoryPrunedError(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return nil
	}
	err := withOriginTransaction(ctx, s.pool, syncMutationTxOptions(), func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `SELECT nextval('sync.history_pruned_error_seq')`)
		return err
	})
	if err != nil {
		return fmt.Errorf("record history_pruned event: %w", err)
	}
	return nil
}
