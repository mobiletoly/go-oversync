// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Pending intent is every dirty row plus every frozen or rejected outbox row that no newer dirty row
// replaced. The server refuses a retired source, so that intent can never settle under it. An explicit
// Rebuild out of source recovery keeps it in place while the authoritative snapshot is staged and moves
// it onto the installed snapshot inside the final writer. By then the server has accepted the
// replacement source and so certified that the retired source holds no unacknowledged outcome.

// sourceRecoveryIntentKey marks that explicit rotated rebuild, the one snapshot owner allowed to hold
// pending intent. Every other snapshot path keeps the strict clean gate.
type sourceRecoveryIntentKey struct{}

func withSourceRecoveryIntent(ctx context.Context) context.Context {
	return context.WithValue(ctx, sourceRecoveryIntentKey{}, true)
}

func sourceRecoveryOwnsPendingIntent(ctx context.Context, q queryRower) (bool, error) {
	if ctx.Value(sourceRecoveryIntentKey{}) == nil {
		return false, nil
	}
	var owns bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM _sync_operation_state WHERE singleton_key=1 AND kind=?)`, operationKindSourceRecovery).Scan(&owns)
	return owns, err
}

// PendingIntentUnsatisfiableError reports that a source-recovery Rebuild could not apply pending local
// edits to the authoritative snapshot, for example because the snapshot no longer holds a parent row a
// pending edit references. Nothing was changed. DiscardPendingIntentForRecovery is the explicit exit
// after user consent; the client never drops such edits on its own.
type PendingIntentUnsatisfiableError struct {
	Tables []string
	Cause  error
}

func (e *PendingIntentUnsatisfiableError) Error() string {
	return "pending local edits in " + strings.Join(e.Tables, ",") + " cannot be applied to the authoritative snapshot; call DiscardPendingIntentForRecovery only after explicit user consent"
}

func (e *PendingIntentUnsatisfiableError) Unwrap() error { return e.Cause }

// PendingIntentDiscardUnavailableError reports why DiscardPendingIntentForRecovery refused to run.
type PendingIntentDiscardUnavailableError struct{ Reason string }

func (e *PendingIntentDiscardUnavailableError) Error() string {
	return "pending intent cannot be discarded: " + e.Reason
}

// DiscardedIntentRow identifies one pending edit DiscardPendingIntentForRecovery dropped. KeyJSON is the
// canonical local sync key object and Op is INSERT, UPDATE or DELETE.
type DiscardedIntentRow struct {
	Schema  string
	Table   string
	KeyJSON string
	Op      string
}

// DiscardedPendingIntent lists every pending edit DiscardPendingIntentForRecovery dropped.
type DiscardedPendingIntent struct {
	Rows []DiscardedIntentRow
}

type pendingRecoveryIntent struct {
	schema, table, key, op string
	payload                sql.NullString
	baseRowVersion         int64
	base                   historicalBase
}

// madeFromInstalledState reports whether the installed snapshot still holds the server state this edit
// was made from. A row that is absent on both sides has no server change to lose, whatever its
// tombstone version is.
func (i *pendingRecoveryIntent) madeFromInstalledState(live bool, installedVersion int64) bool {
	if live {
		return i.base.Kind == "present" && i.baseRowVersion == installedVersion
	}
	return i.base.Kind == "absent" || i.base.Kind == "deleted"
}

// A dirty row is the newer application write for its key, so it replaces the frozen capture.
const pendingRecoveryIntentQuery = `SELECT schema_name,table_name,key_json,op,payload,base_row_version,base_kind,base_payload,base_provenance FROM (
 SELECT schema_name,table_name,key_json,op,payload,base_row_version,base_kind,base_payload,base_provenance FROM _sync_dirty_rows
 UNION ALL
 SELECT o.schema_name,o.table_name,o.key_json,o.op,o.local_payload,o.base_row_version,o.base_kind,o.base_payload,o.base_provenance FROM _sync_outbox_rows o
 WHERE NOT EXISTS(SELECT 1 FROM _sync_dirty_rows d
   WHERE (d.schema_name,d.table_name,d.key_json)=(o.schema_name,o.table_name,o.key_json))
) WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1`

func nextPendingRecoveryIntent(ctx context.Context, tx sqliteTransaction, after pendingRecoveryIntent) (*pendingRecoveryIntent, error) {
	var next pendingRecoveryIntent
	err := tx.QueryRowContext(ctx, pendingRecoveryIntentQuery, after.schema, after.table, after.key).Scan(&next.schema, &next.table, &next.key, &next.op, &next.payload,
		&next.baseRowVersion, &next.base.Kind, &next.base.Payload, &next.base.Provenance)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read pending recovery intent: %w", err)
	}
	return &next, nil
}

// carryPendingIntentOntoInstalledSnapshot rebases every pending edit onto the snapshot the caller just
// installed and verified, then releases the retired source's frozen request. An edit of a row the
// snapshot holds becomes an UPDATE or DELETE based on the installed version. An edit of a row the
// snapshot lacks becomes an INSERT, and a DELETE of such a row is already satisfied. The local value is
// written back to the business row, and the next ordinary push uploads it as the replacement source's
// first bundle.
//
// That rebase is sound only while the snapshot still holds the server state the edit was made from.
// When another device changed or removed the row in the meantime, or created the key the edit inserts,
// the edit keeps its original base instead. The server then refuses its upload as an ordinary conflict
// and the configured resolver decides, exactly as for any other stale edit. Rebasing it here would
// overwrite that change without asking.
func (c *Client) carryPendingIntentOntoInstalledSnapshot(ctx context.Context, tx sqliteTransaction, cache *txStmtCache) error {
	var cursor pendingRecoveryIntent
	requeued := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		intent, err := nextPendingRecoveryIntent(ctx, tx, cursor)
		if err != nil {
			return err
		}
		if intent == nil {
			break
		}
		cursor = *intent
		carried, err := c.carryPendingRecoveryIntent(ctx, tx, cache, intent)
		if err != nil {
			return err
		}
		if carried {
			requeued++
		}
	}
	if err := releaseRetiredSourceUploadInTx(ctx, tx); err != nil {
		return err
	}
	if requeued == 0 {
		return nil
	}
	return c.requireCarriedIntentForeignKeys(ctx, tx)
}

func (c *Client) carryPendingRecoveryIntent(ctx context.Context, tx sqliteTransaction, cache *txStmtCache, intent *pendingRecoveryIntent) (bool, error) {
	var version int64
	var kind string
	if err := tx.QueryRowContext(ctx, `SELECT row_version,server_kind FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?`, intent.schema, intent.table, intent.key).Scan(&version, &kind); err != nil {
		return false, fmt.Errorf("installed snapshot left no authoritative observation for pending intent in %s: %w", intent.table, err)
	}
	live := kind == "live"
	current := intent.madeFromInstalledState(live, version)
	requeue := func(op string, payload sql.NullString) error {
		if current {
			return c.rebaseDirtyIntentInTx(ctx, tx, intent.schema, intent.table, intent.key, op, version, payload)
		}
		err := c.requeueDirtyIntentInTx(ctx, tx, intent.schema, intent.table, intent.key, op, intent.baseRowVersion, payload, intent.base)
		var unavailable *ResolutionBaseUnavailableError
		if errors.As(err, &unavailable) {
			// Without its original base the edit could only be uploaded over the changed row.
			return &PendingIntentUnsatisfiableError{Tables: []string{intent.table}, Cause: err}
		}
		return err
	}
	localPK, _, err := c.decodeDirtyKeyForPush(tx, intent.table, intent.key)
	if err != nil {
		return false, err
	}
	// The unique-apply plan of the installation is finished; carried rows are written directly.
	raw := context.WithValue(ctx, uniqueRawContextKey{}, true)
	if intent.op == oversync.OpDelete {
		if !live {
			// The authoritative state already lacks the row the application deleted.
			_, err := tx.ExecContext(ctx, `DELETE FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?`, intent.schema, intent.table, intent.key)
			return false, err
		}
		if err := requeue(oversync.OpDelete, sql.NullString{}); err != nil {
			return false, err
		}
		return true, unsatisfiableConstraint(intent.table, c.deleteLocalRowInTxUsing(raw, cache, tx, intent.table, localPK))
	}
	if !intent.payload.Valid {
		return false, fmt.Errorf("pending %s intent for %s has no local payload", intent.op, intent.table)
	}
	op := intent.op
	if current {
		op = oversync.OpInsert
		if live {
			op = oversync.OpUpdate
		}
	}
	if err := requeue(op, intent.payload); err != nil {
		return false, err
	}
	payload, err := jcs.DecodeObject([]byte(intent.payload.String))
	if err != nil {
		return false, err
	}
	return true, unsatisfiableConstraint(intent.table, c.upsertLocalRowInTxUsing(raw, cache, tx, intent.table, payload))
}

func unsatisfiableConstraint(table string, err error) error {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrConstraint {
		return &PendingIntentUnsatisfiableError{Tables: []string{table}, Cause: err}
	}
	return err
}

// Deferred enforcement would only fail the commit; name the tables so the caller can act.
func (c *Client) requireCarriedIntentForeignKeys(ctx context.Context, tx sqliteTransaction) error {
	var violating []string
	for _, table := range c.config.Tables {
		var violations int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pragma_foreign_key_check(?)`, table.TableName).Scan(&violations); err != nil {
			return fmt.Errorf("failed to verify carried intent foreign keys for %s: %w", table.TableName, err)
		}
		if violations != 0 {
			violating = append(violating, table.TableName)
		}
	}
	if len(violating) == 0 {
		return nil
	}
	sort.Strings(violating)
	return &PendingIntentUnsatisfiableError{Tables: violating}
}

// requireNoRebuildBlockingDecision refuses a rebuild while an explicit decision is pending. The rebuild
// out of source recovery admits a retained application rejection: the rejected request's intent is
// pending intent that the carry requeues, and the carry releases the rejection with that request. A
// deferred row conflict still blocks every rebuild before any network request.
func (c *Client) requireNoRebuildBlockingDecision(ctx context.Context, q queryRower, carriesIntent bool) error {
	if !carriesIntent {
		return c.requireNoPendingConflict(ctx, q)
	}
	pending, err := readPendingConflict(ctx, q)
	if err != nil {
		return err
	}
	if pending != nil {
		return &ConflictNeedsResolutionError{Pending: *pending}
	}
	return nil
}

// releaseRetiredSourceUploadInTx releases the frozen request, its journal, any acknowledgment
// obligation and any retained application rejection bound to a source the server has replaced.
// Replacement is admitted only while that source holds no unacknowledged outcome, so none of them can
// be needed again. The rejection selected only that request: its intent is requeued or discarded by the
// caller, and a stale edit is refused again under the replacement source as an ordinary conflict.
func releaseRetiredSourceUploadInTx(ctx context.Context, tx sqliteTransaction) error {
	for _, statement := range []string{`DELETE FROM _sync_upload_journal WHERE singleton_key=1`, `DELETE FROM _sync_outbox_rows`, `DELETE FROM _sync_push_stage`, `UPDATE _sync_push_rejection_state SET pending='' WHERE singleton_key=1`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("failed to release retired source upload: %w", err)
		}
	}
	return persistOutboxBundle(ctx, tx, &outboxBundleRecord{})
}

// settleUploadUnderRetiringSourceLocked gives the old source one ordinary chance to account for an
// unsettled upload before rotation. A retained outcome settles and acknowledges exactly as PushPending
// would. When the server refuses the source instead, the request stays frozen and the rotated rebuild
// carries its intent forward.
func (c *Client) settleUploadUnderRetiringSourceLocked(ctx context.Context) error {
	frozen, err := c.frozenUploadMayResume(ctx)
	if err != nil {
		return err
	}
	if frozen {
		_, err := c.pushPendingLockedWithRecovery(ctx, 0, false)
		var refused *SourceRecoveryRequiredError
		var settled *RebuildRequiredError
		if err == nil || errors.As(err, &refused) || errors.As(err, &settled) {
			return nil
		}
		return err
	}
	// A terminal refusal cannot improve under this source. The obligation stays durable and is released
	// only once the server accepts the replacement, which proves nothing waits on it.
	if err := c.acknowledgeUploadJournal(ctx); err != nil && !isTerminalRefusalUnderRetiringSource(err) {
		return err
	}
	return nil
}

func isTerminalRefusalUnderRetiringSource(err error) bool {
	var refused *SourceRecoveryRequiredError
	if errors.As(err, &refused) {
		return true
	}
	var status *retryHTTPError
	if !errors.As(err, &status) {
		return false
	}
	switch status.StatusCode {
	case 401, 403, 408, 429:
		return false
	}
	return status.StatusCode >= 400 && status.StatusCode <= 499
}

// DiscardPendingIntentForRecovery permanently drops the pending local edits that Rebuild would
// otherwise carry across a source recovery, and reports exactly which rows were dropped.
//
// The ordinary exit from SourceRecoveryRequiredError is Rebuild alone: it installs the authoritative
// snapshot under a replacement source and re-uploads every pending edit. Call this only when Rebuild
// fails with PendingIntentUnsatisfiableError, or when the user explicitly chooses to abandon unsynced
// edits. It is destructive, must follow explicit user consent and never runs implicitly. Business rows
// keep their local values until the following Rebuild replaces them with authoritative state.
//
// While an upload is still unsettled it requires a previous Rebuild attempt, because only the server's
// acceptance of the replacement source proves that the upload was never applied.
func (c *Client) DiscardPendingIntentForRecovery(ctx context.Context) (result DiscardedPendingIntent, resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return result, err
	}
	defer c.writeMu.Unlock()
	if err := c.ensureConnectedSessionLocked(ctx, "DiscardPendingIntentForRecovery()"); err != nil {
		return result, err
	}
	if err := requireRecoveryIntentDiscardable(ctx, c.db); err != nil {
		return result, err
	}
	if err := c.clearPushPlanForLifecycle(ctx, false); err != nil {
		return result, err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return result, err
	}
	defer tx.rollbackOnReturn(&resultErr, "pending intent discard")
	if err := requireRecoveryIntentDiscardable(ctx, tx); err != nil {
		return result, err
	}
	var cursor pendingRecoveryIntent
	for {
		if err := ctx.Err(); err != nil {
			return DiscardedPendingIntent{}, err
		}
		intent, err := nextPendingRecoveryIntent(ctx, tx, cursor)
		if err != nil {
			return DiscardedPendingIntent{}, err
		}
		if intent == nil {
			break
		}
		cursor = *intent
		result.Rows = append(result.Rows, DiscardedIntentRow{Schema: intent.schema, Table: intent.table, KeyJSON: intent.key, Op: intent.op})
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_dirty_rows`); err != nil {
		return DiscardedPendingIntent{}, err
	}
	if err := releaseRetiredSourceUploadInTx(ctx, tx); err != nil {
		return DiscardedPendingIntent{}, err
	}
	// The dropped request was the subject of any retained conflict observation.
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_conflict_state SET pending='' WHERE singleton_key=1`); err != nil {
		return DiscardedPendingIntent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DiscardedPendingIntent{}, err
	}
	return result, nil
}

// A frozen request or acknowledgment obligation may only be released once the server has accepted the
// replacement source, which the staged snapshot proves. Before that the server could still hold an
// outcome for it, and losing the request would leave that outcome unresolvable.
func requireRecoveryIntentDiscardable(ctx context.Context, q queryRower) error {
	operation, err := loadOperationState(ctx, q)
	if err != nil {
		return err
	}
	if operation.Kind != operationKindSourceRecovery {
		return &PendingIntentDiscardUnavailableError{Reason: "source recovery is not required"}
	}
	journal, err := loadUploadJournal(ctx, q)
	if err != nil {
		return err
	}
	if journal != nil && operation.StagedSnapshotID == "" {
		return &PendingIntentDiscardUnavailableError{Reason: "run Rebuild first; an unsettled upload may be released only after the server accepts the replacement source"}
	}
	return nil
}
