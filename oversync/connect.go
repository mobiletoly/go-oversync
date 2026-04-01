// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *SyncService) Connect(ctx context.Context, actor Actor, req *ConnectRequest) (_ *ConnectResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	if err := actor.validate(true); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, &ConnectInvalidError{Message: "connect request is required"}
	}

	conn, releaseConn, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer releaseConn()

	var resp *ConnectResponse
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if err := ensureScopeStateExistsWithExec(ctx, tx, actor.UserID); err != nil {
			return err
		}
		state, err := loadScopeStateForUpdate(ctx, tx, actor.UserID)
		if err != nil {
			return err
		}
		state, err = expireInitializationLeaseIfNeeded(ctx, tx, state)
		if err != nil {
			return err
		}

		switch state.State {
		case scopeStateInitialized:
			resp = &ConnectResponse{Resolution: "remote_authoritative"}
			return nil
		case scopeStateUninitialized:
			if req.HasLocalPendingRows {
				next, err := transitionScopeToInitializing(ctx, tx, actor.UserID, actor.SourceID, s.initializationLeaseTTL())
				if err != nil {
					return err
				}
				resp = &ConnectResponse{
					Resolution:       "initialize_local",
					InitializationID: next.InitializationID,
					LeaseExpiresAt:   next.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
				}
				return nil
			}
			if err := transitionScopeToInitialized(ctx, tx, actor.UserID, actor.SourceID); err != nil {
				return err
			}
			resp = &ConnectResponse{Resolution: "initialize_empty"}
			return nil
		case scopeStateInitializing:
			if state.InitializerSourceID == actor.SourceID {
				if !req.HasLocalPendingRows {
					if err := transitionScopeToInitialized(ctx, tx, actor.UserID, actor.SourceID); err != nil {
						return err
					}
					resp = &ConnectResponse{Resolution: "initialize_empty"}
					return nil
				}
				refreshedUntil := time.Now().UTC().Add(s.initializationLeaseTTL())
				if _, err := tx.Exec(ctx, `
					UPDATE sync.scope_state
					SET lease_expires_at = $2
					WHERE user_pk = $1
				`, state.UserPK, refreshedUntil); err != nil {
					return fmt.Errorf("refresh initialization lease during reconnect: %w", err)
				}
				resp = &ConnectResponse{
					Resolution:       "initialize_local",
					InitializationID: state.InitializationID,
					LeaseExpiresAt:   refreshedUntil.UTC().Format(time.RFC3339Nano),
				}
				return nil
			}
			retryAfter := 1
			if state.LeaseExpiresAt.After(time.Now().UTC()) {
				retryAfter = int(time.Until(state.LeaseExpiresAt).Seconds())
				if retryAfter < 1 {
					retryAfter = 1
				}
			}
			resp = &ConnectResponse{
				Resolution:     "retry_later",
				RetryAfterSec:  retryAfter,
				LeaseExpiresAt: state.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
			}
			return nil
		default:
			return fmt.Errorf("unexpected scope state %q for user %s", state.State, actor.UserID)
		}
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
