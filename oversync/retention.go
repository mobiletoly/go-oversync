// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var errHistoryPruned = errors.New("requested checkpoint is older than retained history")

type HistoryPrunedError struct {
	UserID        string
	ProvidedSeq   int64
	RetainedFloor int64
}

func (e *HistoryPrunedError) Error() string {
	return fmt.Sprintf(
		"requested checkpoint %d is older than retained history floor %d for user %s",
		e.ProvidedSeq,
		e.RetainedFloor,
		e.UserID,
	)
}

func (e *HistoryPrunedError) Is(target error) bool {
	return target == errHistoryPruned
}

type retainedHistoryState struct {
	UserPK        int64
	NextBundleSeq int64
	RetainedFloor int64
}

func (s retainedHistoryState) highestBundleSeq() int64 {
	if s.NextBundleSeq <= 0 {
		return 0
	}
	return s.NextBundleSeq - 1
}

func loadRetainedHistoryStateByUserID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string) (*retainedHistoryState, error) {
	var state retainedHistoryState
	err := q.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_id = $1
	`, userID).Scan(&state.UserPK, &state.NextBundleSeq, &state.RetainedFloor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query retained history state for %q: %w", userID, err)
	}
	return &state, nil
}

func loadRetainedHistoryStateByUserPK(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userPK int64) (*retainedHistoryState, error) {
	var state retainedHistoryState
	err := q.QueryRow(ctx, `
		SELECT user_pk, next_bundle_seq, retained_bundle_floor
		FROM sync.user_state
		WHERE user_pk = $1
	`, userPK).Scan(&state.UserPK, &state.NextBundleSeq, &state.RetainedFloor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query retained history state for user_pk %d: %w", userPK, err)
	}
	return &state, nil
}

func enforceRetainedBundleFloor(userID string, providedSeq int64, retainedFloor int64) error {
	if providedSeq <= 0 {
		return nil
	}
	if providedSeq <= retainedFloor {
		return &HistoryPrunedError{
			UserID:        userID,
			ProvidedSeq:   providedSeq,
			RetainedFloor: retainedFloor,
		}
	}
	return nil
}
