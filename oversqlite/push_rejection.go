package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

// PushApplicationRejectionError comes only from a validated terminal rejection.
type PushApplicationRejectionError struct {
	Operation string
	Status    int
	Category  string
	Message   string
}

func (e *PushApplicationRejectionError) Error() string {
	if e == nil {
		return "push request was rejected"
	}
	return fmt.Sprintf("%s was rejected: HTTP %d error=%s", e.Operation, e.Status, e.Category)
}

func (e *PushApplicationRejectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &retryHTTPError{Operation: e.Operation, StatusCode: e.Status}
}

// PendingPushRejection identifies one exact definitively rejected frozen request.
type PendingPushRejection struct {
	ID             string
	Revision       int64
	SourceID       string
	SourceBundleID int64
	RequestHash    string
	Generation     oversync.SchemaGenerationProof
	Operation      string
	Status         int
	Category       string
	Message        string
}

// PushRejectionNeedsResolutionError preserves the frozen request until retry or discard.
type PushRejectionNeedsResolutionError struct{ Pending PendingPushRejection }

func (*PushRejectionNeedsResolutionError) Error() string {
	return "push rejection needs explicit retry or discard"
}

func (e *PushRejectionNeedsResolutionError) Unwrap() error {
	return &PushApplicationRejectionError{Operation: e.Pending.Operation, Status: e.Pending.Status, Category: e.Pending.Category, Message: e.Pending.Message}
}

// StalePushRejectionResolutionError means the selected request or local revision changed.
type StalePushRejectionResolutionError struct{}

func (*StalePushRejectionResolutionError) Error() string {
	return "push rejection decision is stale; inspect pending intent again"
}

func ensurePushRejectionMetadata(ctx context.Context, e execer) error {
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS _sync_push_rejection_state(singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), pending TEXT NOT NULL DEFAULT '', decision TEXT NOT NULL DEFAULT '')`,
		`INSERT INTO _sync_push_rejection_state(singleton_key) VALUES(1) ON CONFLICT DO NOTHING`,
	} {
		if _, err := e.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func readPendingPushRejection(ctx context.Context, q queryRower) (*PendingPushRejection, error) {
	var raw string
	var revision int64
	if err := q.QueryRowContext(ctx, `SELECT r.pending,c.revision FROM _sync_push_rejection_state r CROSS JOIN _sync_conflict_state c WHERE r.singleton_key=1 AND c.singleton_key=1`).Scan(&raw, &revision); err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var pending PendingPushRejection
	if err := json.Unmarshal([]byte(raw), &pending); err != nil {
		return nil, fmt.Errorf("invalid durable push rejection: %w", err)
	}
	pending.Revision = revision
	if pending.ID == "" || pending.SourceID == "" || pending.SourceBundleID < 1 || !validSchemaGenerationFingerprint(pending.RequestHash) || pending.Generation.ID < 1 || !validSchemaGenerationFingerprint(pending.Generation.Fingerprint) || pending.Operation == "" || pending.Status != 200 || pending.Category == "" {
		return nil, fmt.Errorf("invalid durable push rejection identity")
	}
	return &pending, nil
}

// PendingPushRejection inspects a definitive application-level push rejection.
func (c *Client) PendingPushRejection(ctx context.Context) (*PendingPushRejection, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return nil, err
	}
	defer c.writeMu.Unlock()
	pending, err := readPendingPushRejection(ctx, c.db)
	if err != nil || pending == nil {
		return pending, err
	}
	if err := c.guardPushRejection(ctx, c.db, pending); err != nil {
		return nil, err
	}
	return pending, nil
}

func (c *Client) preservePushRejection(ctx context.Context, snapshot *pushOutboundSnapshot, rejected *PushApplicationRejectionError) (*PendingPushRejection, error) {
	if snapshot == nil || rejected == nil {
		return nil, fmt.Errorf("push rejection evidence is incomplete")
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if conflict, err := readPendingConflict(ctx, tx); err != nil {
		return nil, err
	} else if conflict != nil {
		return nil, fmt.Errorf("push conflict already requires resolution")
	}
	if previous, err := readPendingPushRejection(ctx, tx); err != nil {
		return nil, err
	} else if previous != nil {
		return previous, nil
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, err
	}
	if outbox.State != outboxStatePrepared || outbox.SourceBundleID != snapshot.SourceBundleID || outbox.SourceID != c.sourceID {
		return nil, &StalePushRejectionResolutionError{}
	}
	pending := PendingPushRejection{
		ID: uuid.NewString(), SourceID: outbox.SourceID, SourceBundleID: outbox.SourceBundleID,
		RequestHash: outbox.CanonicalRequestHash, Generation: c.schemaGenerationProof,
		Operation: rejected.Operation, Status: rejected.Status, Category: rejected.Category, Message: rejected.Message,
	}
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM _sync_conflict_state WHERE singleton_key=1`).Scan(&pending.Revision); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(pending)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_push_rejection_state SET pending=? WHERE singleton_key=1`, string(encoded)); err != nil {
		return nil, err
	}
	if err := retainRejectedUploadInTx(ctx, tx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &pending, nil
}

// RetryPushRejection disposes the rejected operation and requeues preserved intent.
// The next upload uses a successor identity only after its acknowledgment.
func (c *Client) RetryPushRejection(ctx context.Context, selected PendingPushRejection) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	snapshot, err := c.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return &StalePushRejectionResolutionError{}
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := c.guardPushRejection(ctx, tx, &selected); err != nil {
		return err
	}
	if err := c.requeueSnapshotRowsInTx(ctx, tx, snapshot, nil); err != nil {
		return err
	}
	if err := recordPushRejectionDecision(ctx, tx, &selected, "retry"); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, selected.SourceBundleID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DiscardPushRejection definitively abandons the selected whole frozen request
// while preserving every newer dirty row.
func (c *Client) DiscardPushRejection(ctx context.Context, selected PendingPushRejection) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	if err := c.guardPushRejection(ctx, c.db, &selected); err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := c.guardPushRejection(ctx, tx, &selected); err != nil {
		return err
	}
	if err := recordPushRejectionDecision(ctx, tx, &selected, "discard"); err != nil {
		return err
	}
	if err := c.deletePushOutboundSnapshotInTx(ctx, tx, selected.SourceBundleID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) guardPushRejection(ctx context.Context, q queryRower, selected *PendingPushRejection) error {
	current, err := readPendingPushRejection(ctx, q)
	if err != nil {
		return err
	}
	outbox, err := loadOutboxBundle(ctx, q)
	if err != nil {
		return err
	}
	if selected == nil || current == nil || *current != *selected || outbox.State != outboxStateRejected || outbox.SourceID != selected.SourceID || outbox.SourceBundleID != selected.SourceBundleID || outbox.CanonicalRequestHash != selected.RequestHash || selected.Generation != c.schemaGenerationProof {
		return &StalePushRejectionResolutionError{}
	}
	return nil
}

func recordPushRejectionDecision(ctx context.Context, e execer, pending *PendingPushRejection, action string) error {
	encoded, err := json.Marshal(struct {
		ID, RequestHash, Action string
		Revision                int64
	}{pending.ID, pending.RequestHash, action, pending.Revision})
	if err != nil {
		return err
	}
	_, err = e.ExecContext(ctx, `UPDATE _sync_push_rejection_state SET pending='',decision=? WHERE singleton_key=1`, string(encoded))
	return err
}

func clearPushRejectionForUpgradeDecision(ctx context.Context, tx sqliteTransaction, requestHash, action string) error {
	pending, err := readPendingPushRejection(ctx, tx)
	if err != nil || pending == nil {
		return err
	}
	if pending.RequestHash != requestHash {
		return &StalePushRejectionResolutionError{}
	}
	return recordPushRejectionDecision(ctx, tx, pending, action)
}

func pushApplicationRejection(err error) *PushApplicationRejectionError {
	var rejected *PushApplicationRejectionError
	if errors.As(err, &rejected) {
		return rejected
	}
	var coverage *PushEffectCoverageError
	if errors.As(err, &coverage) {
		return &PushApplicationRejectionError{Operation: coverage.Operation, Status: coverage.Status, Category: "push_effect_coverage_incomplete", Message: coverage.Message}
	}
	return nil
}
