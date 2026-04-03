package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	attachmentBindingAnonymous = "anonymous"
	attachmentBindingAttached  = "attached"

	operationKindNone           = "none"
	operationKindRemoteReplace  = "remote_replace"
	operationKindSourceRecovery = "source_recovery"

	outboxStateNone            = "none"
	outboxStatePrepared        = "prepared"
	outboxStateCommittedRemote = "committed_remote"
)

type attachmentStateRecord struct {
	CurrentSourceID         string
	BindingState            string
	AttachedUserID          string
	SchemaName              string
	LastBundleSeqSeen       int64
	RebuildRequired         bool
	PendingInitializationID string
}

type sourceStateRecord struct {
	SourceID           string
	NextSourceBundleID int64
	ReplacedBySourceID string
}

type operationStateRecord struct {
	Kind                string
	TargetUserID        string
	Reason              string
	ReplacementSourceID string
	StagedSnapshotID    string
	SnapshotBundleSeq   int64
	SnapshotRowCount    int64
}

type outboxBundleRecord struct {
	State                string
	SourceID             string
	SourceBundleID       int64
	CanonicalRequestHash string
	RowCount             int64
	InitializationID     string
	RemoteBundleHash     string
	RemoteBundleSeq      int64
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func loadAttachmentState(ctx context.Context, q queryRower) (*attachmentStateRecord, error) {
	var (
		rec             attachmentStateRecord
		rebuildRequired int
	)
	if err := q.QueryRowContext(ctx, `
		SELECT current_source_id, binding_state, attached_user_id, schema_name, last_bundle_seq_seen, rebuild_required, pending_initialization_id
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(
		&rec.CurrentSourceID,
		&rec.BindingState,
		&rec.AttachedUserID,
		&rec.SchemaName,
		&rec.LastBundleSeqSeen,
		&rebuildRequired,
		&rec.PendingInitializationID,
	); err != nil {
		return nil, fmt.Errorf("failed to load attachment state: %w", err)
	}
	rec.RebuildRequired = rebuildRequired == 1
	if strings.TrimSpace(rec.BindingState) == "" {
		rec.BindingState = attachmentBindingAnonymous
	}
	return &rec, nil
}

func persistAttachmentState(ctx context.Context, e execer, rec *attachmentStateRecord) error {
	if rec == nil {
		return fmt.Errorf("attachment state is required")
	}
	rebuildRequired := 0
	if rec.RebuildRequired {
		rebuildRequired = 1
	}
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_attachment_state
		SET current_source_id = ?,
			binding_state = ?,
			attached_user_id = ?,
			schema_name = ?,
			last_bundle_seq_seen = ?,
			rebuild_required = ?,
			pending_initialization_id = ?
		WHERE singleton_key = 1
	`, rec.CurrentSourceID, rec.BindingState, rec.AttachedUserID, rec.SchemaName, rec.LastBundleSeqSeen, rebuildRequired, rec.PendingInitializationID); err != nil {
		return fmt.Errorf("failed to persist attachment state: %w", err)
	}
	return nil
}

func loadOperationState(ctx context.Context, q queryRower) (*operationStateRecord, error) {
	var rec operationStateRecord
	if err := q.QueryRowContext(ctx, `
		SELECT kind, target_user_id, reason, replacement_source_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count
		FROM _sync_operation_state
		WHERE singleton_key = 1
	`).Scan(&rec.Kind, &rec.TargetUserID, &rec.Reason, &rec.ReplacementSourceID, &rec.StagedSnapshotID, &rec.SnapshotBundleSeq, &rec.SnapshotRowCount); err != nil {
		return nil, fmt.Errorf("failed to load operation state: %w", err)
	}
	if strings.TrimSpace(rec.Kind) == "" {
		rec.Kind = operationKindNone
	}
	return &rec, nil
}

func persistOperationState(ctx context.Context, e execer, rec *operationStateRecord) error {
	if rec == nil {
		return fmt.Errorf("operation state is required")
	}
	if strings.TrimSpace(rec.Kind) == "" {
		rec.Kind = operationKindNone
	}
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_operation_state
		SET kind = ?,
			target_user_id = ?,
			reason = ?,
			replacement_source_id = ?,
			staged_snapshot_id = ?,
			snapshot_bundle_seq = ?,
			snapshot_row_count = ?
		WHERE singleton_key = 1
	`, rec.Kind, rec.TargetUserID, rec.Reason, rec.ReplacementSourceID, rec.StagedSnapshotID, rec.SnapshotBundleSeq, rec.SnapshotRowCount); err != nil {
		return fmt.Errorf("failed to persist operation state: %w", err)
	}
	return nil
}

func ensureSourceState(ctx context.Context, e execer, sourceID string) error {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return nil
	}
	if _, err := e.ExecContext(ctx, `
		INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
		VALUES(?, 1, '')
		ON CONFLICT(source_id) DO NOTHING
	`, sourceID); err != nil {
		return fmt.Errorf("failed to ensure source state for %s: %w", sourceID, err)
	}
	return nil
}

func loadSourceState(ctx context.Context, q queryRower, sourceID string) (*sourceStateRecord, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return nil, fmt.Errorf("sourceID must be provided")
	}
	var rec sourceStateRecord
	if err := q.QueryRowContext(ctx, `
		SELECT source_id, next_source_bundle_id, replaced_by_source_id
		FROM _sync_source_state
		WHERE source_id = ?
	`, sourceID).Scan(&rec.SourceID, &rec.NextSourceBundleID, &rec.ReplacedBySourceID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load source state: %w", err)
	}
	return &rec, nil
}

func updateSourceNextBundleID(ctx context.Context, e execer, sourceID string, nextSourceBundleID int64) error {
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_source_state
		SET next_source_bundle_id = CASE
			WHEN next_source_bundle_id < ? THEN ?
			ELSE next_source_bundle_id
		END
		WHERE source_id = ?
	`, nextSourceBundleID, nextSourceBundleID, sourceID); err != nil {
		return fmt.Errorf("failed to update source floor: %w", err)
	}
	return nil
}

func markSourceReplaced(ctx context.Context, e execer, sourceID, replacedBySourceID string) error {
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_source_state
		SET replaced_by_source_id = ?
		WHERE source_id = ?
	`, replacedBySourceID, sourceID); err != nil {
		return fmt.Errorf("failed to mark source replacement: %w", err)
	}
	return nil
}

func loadOutboxBundle(ctx context.Context, q queryRower) (*outboxBundleRecord, error) {
	var rec outboxBundleRecord
	if err := q.QueryRowContext(ctx, `
		SELECT state, source_id, source_bundle_id, canonical_request_hash, row_count, initialization_id, remote_bundle_hash, remote_bundle_seq
		FROM _sync_outbox_bundle
		WHERE singleton_key = 1
	`).Scan(
		&rec.State,
		&rec.SourceID,
		&rec.SourceBundleID,
		&rec.CanonicalRequestHash,
		&rec.RowCount,
		&rec.InitializationID,
		&rec.RemoteBundleHash,
		&rec.RemoteBundleSeq,
	); err != nil {
		return nil, fmt.Errorf("failed to load outbox bundle: %w", err)
	}
	if strings.TrimSpace(rec.State) == "" {
		rec.State = outboxStateNone
	}
	return &rec, nil
}

func persistOutboxBundle(ctx context.Context, e execer, rec *outboxBundleRecord) error {
	if rec == nil {
		return fmt.Errorf("outbox bundle is required")
	}
	if strings.TrimSpace(rec.State) == "" {
		rec.State = outboxStateNone
	}
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_outbox_bundle
		SET state = ?,
			source_id = ?,
			source_bundle_id = ?,
			canonical_request_hash = ?,
			row_count = ?,
			initialization_id = ?,
			remote_bundle_hash = ?,
			remote_bundle_seq = ?
		WHERE singleton_key = 1
	`, rec.State, rec.SourceID, rec.SourceBundleID, rec.CanonicalRequestHash, rec.RowCount, rec.InitializationID, rec.RemoteBundleHash, rec.RemoteBundleSeq); err != nil {
		return fmt.Errorf("failed to persist outbox bundle: %w", err)
	}
	return nil
}

func clearOutboxBundle(ctx context.Context, e execer) error {
	return persistOutboxBundle(ctx, e, &outboxBundleRecord{})
}

func loadApplyMode(ctx context.Context, q queryRower) (int, error) {
	var applyMode int
	if err := q.QueryRowContext(ctx, `
		SELECT apply_mode
		FROM _sync_apply_state
		WHERE singleton_key = 1
	`).Scan(&applyMode); err != nil {
		return 0, fmt.Errorf("failed to load apply state: %w", err)
	}
	return applyMode, nil
}

func setApplyMode(ctx context.Context, e execer, enabled bool) error {
	value := 0
	if enabled {
		value = 1
	}
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_apply_state
		SET apply_mode = ?
		WHERE singleton_key = 1
	`, value); err != nil {
		return fmt.Errorf("failed to persist apply state: %w", err)
	}
	return nil
}
