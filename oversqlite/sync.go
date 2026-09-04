// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

type preparedUploadChange struct {
	Table        string
	Op           string
	LocalPK      string
	LocalPayload sql.NullString
}

func nextSyncLoopBackoffAfterError(err error, current, min, max time.Duration) time.Duration {
	if IsLifecyclePreconditionError(err) {
		return min
	}
	next := current * 2
	if next < min {
		return min
	}
	if next > max {
		return max
	}
	return next
}

func waitForSyncLoopDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// uploaderLoop runs the push loop with backoff.
func (c *Client) uploaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomic.LoadInt32(&c.uploadPaused) == 1 {
			if !waitForSyncLoopDelay(ctx, backoff) {
				return
			}
			continue
		}

		if _, err := c.PushPending(ctx); err != nil {
			if c.stopForRetiredGeneration(err) {
				return
			}
			if isProtocolVersionMismatch(err) {
				c.logger.Error("upload loop stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", err))
				return
			}
			if isTerminalProtocolRepresentationError(err) {
				c.logger.Error("upload loop stopped by terminal protocol representation error", "error_type", fmt.Sprintf("%T", err))
				return
			}
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("upload loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				if !waitForSyncLoopDelay(ctx, backoff) {
					return
				}
				continue
			}
			if !waitForSyncLoopDelay(ctx, backoff) {
				return
			}
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		backoff = c.config.BackoffMin
		if !waitForSyncLoopDelay(ctx, backoff) {
			return
		}
	}
}

// downloaderLoop runs the pull loop with backoff.
func (c *Client) downloaderLoop(ctx context.Context) {
	backoff := c.config.BackoffMin
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if atomic.LoadInt32(&c.downloadPaused) == 1 {
			if !waitForSyncLoopDelay(ctx, backoff) {
				return
			}
			continue
		}

		before, err := c.LastBundleSeqSeen(ctx)
		if err != nil {
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				if !waitForSyncLoopDelay(ctx, backoff) {
					return
				}
				continue
			}
			if !waitForSyncLoopDelay(ctx, backoff) {
				return
			}
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		if _, err := c.PullToStable(ctx); err != nil {
			if c.stopForRetiredGeneration(err) {
				return
			}
			if c.probeRetiredGenerationWhileDownloadBlocked(ctx, before, err) {
				return
			}
			if isProtocolVersionMismatch(err) {
				c.logger.Error("download loop stopped by protocol version mismatch", "error_type", fmt.Sprintf("%T", err))
				return
			}
			if isTerminalProtocolRepresentationError(err) {
				c.logger.Error("download loop stopped by terminal protocol representation error", "error_type", fmt.Sprintf("%T", err))
				return
			}
			if IsLifecyclePreconditionError(err) {
				c.logger.Warn("download loop blocked by lifecycle state", "error", err)
				backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
				if !waitForSyncLoopDelay(ctx, backoff) {
					return
				}
				continue
			}
			if !waitForSyncLoopDelay(ctx, backoff) {
				return
			}
			backoff = nextSyncLoopBackoffAfterError(err, backoff, c.config.BackoffMin, c.config.BackoffMax)
			continue
		}

		backoff = c.config.BackoffMin
		after, seqErr := c.LastBundleSeqSeen(ctx)
		if seqErr != nil || after == before {
			if !waitForSyncLoopDelay(ctx, backoff) {
				return
			}
		}
	}
}

// probeRetiredGenerationWhileDownloadBlocked lets an automatic downloader
// observe the terminal generation response even when local intent correctly
// prevents applying an incremental pull. The response is never applied here:
// this is only the ordinary pull endpoint's retirement signal, and every
// non-retirement result leaves the dirty-state rejection in force.
func (c *Client) probeRetiredGenerationWhileDownloadBlocked(ctx context.Context, afterBundleSeq int64, pullErr error) bool {
	var dirty *DirtyStateRejectedError
	if !errors.As(pullErr, &dirty) {
		return false
	}
	maxBundles := c.config.DownloadLimit
	if maxBundles <= 0 {
		maxBundles = 1000
	}
	_, err := c.sendPullRequest(ctx, afterBundleSeq, maxBundles, 0)
	return c.stopForRetiredGeneration(err)
}

func (c *Client) serializeExistingRowInTx(ctx context.Context, tx sqliteTransaction, tableName, pk string) (json.RawMessage, bool, error) {
	payload, err := c.serializeRowInTx(ctx, tx, tableName, pk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to serialize current row for %s.%s: %w", tableName, pk, err)
	}
	return payload, true, nil
}

func liveMatchesUploadedIntent(change preparedUploadChange, livePayload json.RawMessage, liveExists bool) (bool, error) {
	if change.Op == oversync.OpDelete {
		return !liveExists, nil
	}
	if !liveExists {
		return false, nil
	}
	return canonicalPayloadEqual(
		sql.NullString{String: string(livePayload), Valid: true},
		change.LocalPayload,
	)
}

func reconcileAppliedUploadState(change preparedUploadChange, pendingExists bool, pendingMatches bool, livePayload json.RawMessage, liveExists bool, liveMatches bool) (string, sql.NullString, bool, bool) {
	switch change.Op {
	case oversync.OpDelete:
		if liveExists {
			return oversync.OpInsert, sql.NullString{String: string(livePayload), Valid: true}, false, true
		}
		return "", sql.NullString{}, true, false
	default:
		if !liveExists {
			return oversync.OpDelete, sql.NullString{}, true, true
		}
		if pendingExists && !pendingMatches {
			return oversync.OpUpdate, sql.NullString{String: string(livePayload), Valid: true}, false, true
		}
		if !liveMatches {
			return oversync.OpUpdate, sql.NullString{String: string(livePayload), Valid: true}, false, true
		}
		return "", sql.NullString{}, false, false
	}
}

func canonicalPayloadEqual(left sql.NullString, right sql.NullString) (bool, error) {
	if left.Valid != right.Valid {
		return false, nil
	}
	if !left.Valid {
		return true, nil
	}
	leftNorm, err := canonicalizeJSON(left.String)
	if err != nil {
		return false, err
	}
	if left.String == right.String {
		return true, nil
	}
	rightNorm, err := canonicalizeJSON(right.String)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftNorm, rightNorm), nil
}

func canonicalizeJSON(raw string) ([]byte, error) {
	normalized, err := jcs.Canonicalize([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("failed to normalize JSON payload: %w", err)
	}
	return normalized, nil
}

func nullStringValue(value sql.NullString) any {
	if value.Valid {
		return value.String
	}
	return nil
}

// processPayloadForUpload converts locally stored JSON payloads into the wire format expected by
// bundle-shaped push requests. BLOB values are converted from hex to base64 to match JSON transport.
func (c *Client) processPayloadForUpload(tableName, payloadStr string) (json.RawMessage, error) {
	payloadData, err := jcs.DecodeObject([]byte(payloadStr))
	if err != nil {
		return nil, fmt.Errorf("failed to parse payload JSON: %w", err)
	}

	if err := c.validateAndUnwrapCapturedPayload(tableName, payloadData); err != nil {
		return nil, err
	}
	if err := c.processPayloadWithDeclaredCodecs(tableName, payloadData); err != nil {
		return nil, err
	}
	return jcs.Marshal(payloadData)
}
