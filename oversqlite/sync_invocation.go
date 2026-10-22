// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
)

const maxInteractivePushAttempts = 64

type SyncIncompleteReason string

const (
	SyncIncompleteRecoveryReplanLimit   SyncIncompleteReason = "recovery_replan_limit"
	SyncIncompletePushAttemptLimit      SyncIncompleteReason = "push_attempt_limit"
	SyncIncompleteNoProgress            SyncIncompleteReason = "no_progress"
	SyncIncompleteUploadsPaused         SyncIncompleteReason = "uploads_paused"
	SyncIncompleteRecoveryExchangeLimit SyncIncompleteReason = "recovery_exchange_limit"
)

// SyncIncompleteError leaves all confirmed work and outstanding local intent durable.
type SyncIncompleteError struct {
	Reason               SyncIncompleteReason
	DirtyCount           int
	OutboundCount        int
	ConfirmedBundleCount int
	PushAttempts         int
}

func (e *SyncIncompleteError) Error() string {
	return fmt.Sprintf("sync incomplete (%s): dirty=%d outbound=%d confirmed=%d attempts=%d",
		e.Reason, e.DirtyCount, e.OutboundCount, e.ConfirmedBundleCount, e.PushAttempts)
}

type syncInvocationKey struct{}

type confirmedPushKey struct {
	sourceID       string
	sourceBundleID int64
}

type syncInvocation struct {
	pushAttempts         int
	recoveryReplans      int
	recoveryStarted      bool
	snapshotReplacements int
	recoveryExchanges    int
	confirmed            map[confirmedPushKey]struct{}
}

func invocationFromContext(ctx context.Context) *syncInvocation {
	invocation, _ := ctx.Value(syncInvocationKey{}).(*syncInvocation)
	return invocation
}

func (i *syncInvocation) recordCommitted(sourceID string, sourceBundleID int64) {
	if i == nil {
		return
	}
	if i.confirmed == nil {
		i.confirmed = make(map[confirmedPushKey]struct{})
	}
	i.confirmed[confirmedPushKey{sourceID, sourceBundleID}] = struct{}{}
}

func (i *syncInvocation) pushOutcome() PushOutcome {
	if len(i.confirmed) > 0 {
		return PushOutcomeCommitted
	}
	return PushOutcomeNoChange
}

func (c *Client) incompleteSync(ctx context.Context, reason SyncIncompleteReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dirty, err := c.pendingChangeCount(ctx)
	if err != nil {
		return err
	}
	outbound, err := c.pendingPushOutboundCount(ctx)
	if err != nil {
		return err
	}
	i := invocationFromContext(ctx)
	return &SyncIncompleteError{
		Reason: reason, DirtyCount: dirty, OutboundCount: outbound,
		ConfirmedBundleCount: len(i.confirmed), PushAttempts: i.pushAttempts,
	}
}

// pendingWorkIdentity streams the durable queue through a read transaction. It never
// materializes the whole dirty backlog in memory merely to detect a no-op pass.
func (c *Client) pendingWorkIdentity(ctx context.Context) ([32]byte, error) {
	var zero [32]byte
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	hash := sha256.New()
	encode := json.NewEncoder(hash)
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return zero, err
	}
	source, err := loadSourceState(ctx, tx, c.sourceID)
	if err != nil {
		return zero, err
	}
	if err := encode.Encode([]any{c.sourceID, outbox, source}); err != nil {
		return zero, err
	}
	for _, query := range []string{
		`SELECT schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal
		 FROM _sync_dirty_rows ORDER BY schema_name, table_name, key_json`,
		`SELECT schema_name, table_name, key_json, op, base_row_version, local_payload, row_ordinal
		 FROM _sync_outbox_rows ORDER BY source_bundle_id, row_ordinal`,
	} {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return zero, err
		}
		for rows.Next() {
			var schema, table, key, op string
			var base, ordinal int64
			var payload sql.NullString
			if err := rows.Scan(&schema, &table, &key, &op, &base, &payload, &ordinal); err != nil {
				rows.Close()
				return zero, err
			}
			if err := encode.Encode([]any{schema, table, key, op, base, payload, ordinal}); err != nil {
				rows.Close()
				return zero, err
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return zero, err
		}
		rows.Close()
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	var identity [32]byte
	copy(identity[:], hash.Sum(nil))
	return identity, nil
}
