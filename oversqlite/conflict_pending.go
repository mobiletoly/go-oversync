package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

// DeferConflict preserves the frozen request for an explicit later decision.
type DeferConflict struct{}

func (DeferConflict) isMergeResult() {}

// PendingConflict identifies a definitively rejected request. Revision changes
// on every dirty-row mutation, including a change back to the same value.
type PendingConflict struct {
	ID                string
	Revision          int64
	BaselineRevision  int64
	SourceID          string
	SourceBundleID    int64
	RequestHash       string
	Generation        oversync.SchemaGenerationProof
	Conflict          ConflictContext
	Response          oversync.PushConflictResponse
	NewerLocalOp      string
	NewerLocalPayload json.RawMessage
}

// ConflictNeedsResolutionError never acknowledges or discards pending intent.
type ConflictNeedsResolutionError struct{ Pending PendingConflict }

func (*ConflictNeedsResolutionError) Error() string { return "push conflict needs explicit resolution" }
func (e *ConflictNeedsResolutionError) Unwrap() error {
	return &PushConflictError{Response: e.Pending.Response}
}

// StaleConflictResolutionError means the selected request or local revision changed.
type StaleConflictResolutionError struct{}

func (*StaleConflictResolutionError) Error() string {
	return "conflict decision is stale; inspect pending intent again"
}

func ensureConflictMetadata(ctx context.Context, e execer) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS _sync_conflict_state(singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(revision)='integer' AND revision>=0 AND revision<9223372036854775807), baseline_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(baseline_revision)='integer' AND baseline_revision>=0 AND baseline_revision<9223372036854775807), pending TEXT NOT NULL DEFAULT '', decision TEXT NOT NULL DEFAULT '')`,
		`INSERT INTO _sync_conflict_state(singleton_key) VALUES(1) ON CONFLICT DO NOTHING`,
	}
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		body := `UPDATE _sync_conflict_state SET revision=revision+1 WHERE singleton_key=1;`
		if event != "INSERT" {
			body += projectionDeleteSQL("desired", "OLD")
		}
		if event != "DELETE" {
			body += relationshipProjectionInsertSQL("desired", "NEW.payload", "(SELECT revision FROM _sync_conflict_state WHERE singleton_key=1)", "NEW.op!='DELETE'")
		}
		statements = append(statements, fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS _sync_conflict_revision_%s AFTER %s ON _sync_dirty_rows BEGIN %s END`, event, event, body))
	}
	for _, stmt := range statements {
		if _, err := e.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func readPendingConflict(ctx context.Context, q queryRower) (*PendingConflict, error) {
	var raw string
	var revision, baselineRevision int64
	if err := q.QueryRowContext(ctx, `SELECT pending,revision,baseline_revision FROM _sync_conflict_state WHERE singleton_key=1`).Scan(&raw, &revision, &baselineRevision); err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, fmt.Errorf("invalid durable conflict: %w", err)
	}
	if _, err := completeConflictResponse(fields["Response"]); err != nil {
		return nil, fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	var p PendingConflict
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, fmt.Errorf("invalid durable conflict: %w", err)
	}
	if p.ID == "" || p.RequestHash == "" || p.SourceBundleID < 1 || !coherentConflict(p.Response.Conflict) {
		return nil, fmt.Errorf("invalid durable conflict identity")
	}
	if err := validateRetainedConflictEnvelope(p.Conflict, p.Response.Conflict); err != nil {
		return nil, fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	p.Revision = revision
	p.BaselineRevision = baselineRevision
	// RawMessage's nil value round-trips through durable JSON as the bytes
	// "null". Resolution uses an absent payload to distinguish tombstones and
	// missing rows from live rows, just as the initial HTTP conflict path does.
	p.Conflict.ServerRowPayload = normalizeOptionalJSON(p.Conflict.ServerRowPayload)
	return &p, nil
}

// PendingConflict inspects durable work without contacting the server. A nil
// result means no definitive conflict has been recorded, not that an outbox is empty.
func (c *Client) PendingConflict(ctx context.Context) (*PendingConflict, error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return nil, err
	}
	defer c.writeMu.Unlock()
	p, err := readPendingConflict(ctx, c.db)
	if err != nil || p == nil {
		return p, err
	}
	snapshot, err := c.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, &StaleConflictResolutionError{}
	}
	row, err := findConflictingSnapshotRow(snapshot, p.Response.Conflict)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, &StaleConflictResolutionError{}
	}
	if err := c.validateAuthoritativeConflictRow(ctx, p.Response.Conflict, row); err != nil {
		return nil, fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	trusted, err := c.trustedConflictContext(ctx, row, p.Response.Conflict)
	if err != nil {
		return nil, fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	if err := validateRetainedConflictContext(p.Conflict, trusted); err != nil {
		return nil, fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	p.Conflict = trusted
	var payload sql.NullString
	err = c.db.QueryRowContext(ctx, `SELECT op,payload FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?`, row.SchemaName, row.TableName, row.KeyJSON).Scan(&p.NewerLocalOp, &payload)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if payload.Valid {
		wire, err := c.processPayloadForUpload(row.TableName, payload.String)
		if err != nil {
			return nil, err
		}
		p.NewerLocalPayload = wire
	}
	return p, nil
}

func (c *Client) preserveConflict(ctx context.Context, snapshot *pushOutboundSnapshot, response *PushConflictError, conflict ConflictContext) (*PendingConflict, error) {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return nil, err
	}
	if outbox.State != outboxStatePrepared || outbox.SourceBundleID != snapshot.SourceBundleID {
		return nil, &StaleConflictResolutionError{}
	}
	previous, err := readPendingConflict(ctx, tx)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		return previous, nil
	}
	p := PendingConflict{ID: uuid.NewString(), SourceID: outbox.SourceID, SourceBundleID: outbox.SourceBundleID, RequestHash: outbox.CanonicalRequestHash, Generation: c.schemaGenerationProof, Conflict: conflict, Response: response.Response}
	if err := tx.QueryRowContext(ctx, `SELECT revision,baseline_revision FROM _sync_conflict_state WHERE singleton_key=1`).Scan(&p.Revision, &p.BaselineRevision); err != nil {
		return nil, err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE _sync_conflict_state SET pending=? WHERE singleton_key=1`, string(data)); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &p, nil
}

// ResolveConflict applies a deliberate decision to this exact pending request
// and local revision. It performs no remote write; call PushPending afterward.
func (c *Client) ResolveConflict(ctx context.Context, selected PendingConflict, result MergeResult) (resultErr error) {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	ctx = context.WithValue(ctx, syncInvocationKey{}, &syncInvocation{})
	defer func() { resultErr = c.withSyncErrorProgress(ctx, resultErr) }()
	if _, err := inspectUpgradeConstructionGate(ctx, c.db, c.schemaGenerationProof); err != nil {
		return err
	}
	current, err := readPendingConflict(ctx, c.db)
	if err != nil {
		return err
	}
	if current == nil || current.ID != selected.ID || current.Revision != selected.Revision || current.BaselineRevision != selected.BaselineRevision {
		return &StaleConflictResolutionError{}
	}
	unit := applyUnit("conflict-buffers", selected.SourceBundleID, selected.RequestHash)
	token, err := c.reserveWorkspace(ctx, unit, workspaceCharge{64, 256 * 1024})
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, workspaceBufferKey{}, &workspaceBufferScope{client: c, token: token, unit: unit, base: workspaceCharge{64, 256 * 1024}})
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, c.releaseWorkspace(cleanup, token))
	}()
	snapshot, err := c.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return &StaleConflictResolutionError{}
	}
	row, err := findConflictingSnapshotRow(snapshot, current.Response.Conflict)
	if err != nil {
		return err
	}
	if row == nil {
		return &StaleConflictResolutionError{}
	}
	if err := c.validateAuthoritativeConflictRow(ctx, current.Response.Conflict, row); err != nil {
		return fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	trusted, err := c.trustedConflictContext(ctx, row, current.Response.Conflict)
	if err != nil {
		return fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	if err := validateRetainedConflictContext(current.Conflict, trusted); err != nil {
		return fmt.Errorf("invalid durable conflict evidence: %w", err)
	}
	current.Conflict = trusted
	snapshot.conflict = current
	return c.applyExplicitConflictResult(ctx, snapshot, row, trusted, &PushConflictError{Response: current.Response}, result)
}

func validateRetainedConflictEnvelope(retained ConflictContext, authoritative *oversync.PushConflictDetails) error {
	if authoritative == nil || retained.Schema != authoritative.Schema || retained.Table != authoritative.Table || retained.LocalOp != authoritative.Op || retained.BaseRowVersion != authoritative.BaseRowVersion || retained.ServerRowVersion != authoritative.ServerRowVersion || retained.ServerRowDeleted != authoritative.ServerRowDeleted {
		return fmt.Errorf("retained conflict envelope disagrees with authoritative response")
	}
	same, err := syncKeysEqual(retained.Key, authoritative.Key)
	if err != nil || !same {
		return fmt.Errorf("retained conflict key disagrees with authoritative response")
	}
	equal, err := optionalJSONEqual(retained.ServerRowPayload, authoritative.ServerRow)
	if err != nil || !equal {
		return fmt.Errorf("retained server row disagrees with authoritative response")
	}
	return nil
}

func validateRetainedConflictContext(retained, trusted ConflictContext) error {
	retainedJSON, err := json.Marshal(retained)
	if err != nil {
		return err
	}
	trustedJSON, err := json.Marshal(trusted)
	if err != nil {
		return err
	}
	equal, err := jcs.Equal(retainedJSON, trustedJSON)
	if err != nil || !equal {
		return fmt.Errorf("retained conflict context disagrees with frozen request")
	}
	return nil
}

func optionalJSONEqual(left, right json.RawMessage) (bool, error) {
	left = normalizeOptionalJSON(left)
	right = normalizeOptionalJSON(right)
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right), nil
	}
	return jcs.Equal(left, right)
}

// RetryConflict clears only the definitive conflict observation, retaining the
// exact frozen request for another ordinary, bounded upload attempt.
func (c *Client) RetryConflict(ctx context.Context, selected PendingConflict) error {
	if err := c.tryBeginSyncOperation(); err != nil {
		return err
	}
	defer c.writeMu.Unlock()
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := c.guardConflictDecision(ctx, tx, &selected); err != nil {
		return err
	}
	if err := recordConflictDecision(ctx, tx, &selected, DeferConflict{}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Client) guardConflictDecision(ctx context.Context, tx sqliteTransaction, p *PendingConflict) error {
	if p == nil {
		return nil
	} // An upgrade-only deferred request has its own journal.
	current, err := readPendingConflict(ctx, tx)
	if err != nil {
		return err
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if current == nil || current.ID != p.ID || current.Revision != p.Revision || current.BaselineRevision != p.BaselineRevision || outbox.State != outboxStatePrepared || outbox.SourceID != p.SourceID || outbox.SourceBundleID != p.SourceBundleID || outbox.CanonicalRequestHash != p.RequestHash || p.Generation != c.schemaGenerationProof {
		return &StaleConflictResolutionError{}
	}
	return nil
}

func recordConflictDecision(ctx context.Context, tx sqliteTransaction, p *PendingConflict, result MergeResult) error {
	if p == nil {
		return nil
	}
	data, err := json.Marshal(struct {
		ID       string
		Revision int64
		Action   string
		Result   MergeResult
	}{p.ID, p.Revision, fmt.Sprintf("%T", result), result})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_conflict_state SET pending='',decision=? WHERE singleton_key=1`, string(data))
	return err
}

func invokeConflictResolver(resolver Resolver, conflict ConflictContext) (result MergeResult) {
	result = DeferConflict{}
	defer func() {
		if recover() != nil {
			result = DeferConflict{}
		}
	}()
	if resolver != nil {
		key := make(oversync.SyncKey, len(conflict.Key))
		for column, value := range conflict.Key {
			key[column] = value
		}
		conflict.Key = key
		conflict.LocalPayload = append(json.RawMessage(nil), conflict.LocalPayload...)
		conflict.ServerRowPayload = append(json.RawMessage(nil), conflict.ServerRowPayload...)
		result = resolver.Resolve(conflict)
	}
	if result == nil {
		return DeferConflict{}
	}
	return result
}

func (c *Client) requireNoPendingConflict(ctx context.Context, q queryRower) error {
	p, err := readPendingConflict(ctx, q)
	if err != nil {
		return err
	}
	if p != nil {
		return &ConflictNeedsResolutionError{Pending: *p}
	}
	rejection, err := readPendingPushRejection(ctx, q)
	if err != nil {
		return err
	}
	if rejection != nil {
		return &PushRejectionNeedsResolutionError{Pending: *rejection}
	}
	return nil
}

// The upgrade journal is the decision authority while the old client is frozen.
// Clearing an ordinary observation never clears or rebases its request.
func clearConflictForUpgradeDecision(ctx context.Context, tx sqliteTransaction, requestHash, action string) error {
	p, err := readPendingConflict(ctx, tx)
	if err != nil {
		return err
	}
	if p == nil {
		return nil
	}
	outbox, err := loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if p.RequestHash != requestHash || outbox.CanonicalRequestHash != requestHash || outbox.SourceID != p.SourceID || outbox.SourceBundleID != p.SourceBundleID {
		return &StaleConflictResolutionError{}
	}
	data, err := json.Marshal(struct{ ID, RequestHash, Action string }{p.ID, requestHash, action})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _sync_conflict_state SET pending='',decision=? WHERE singleton_key=1`, string(data))
	return err
}
