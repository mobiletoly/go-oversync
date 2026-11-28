package oversqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/sessionid"
)

const (
	attachmentBindingAnonymous = "anonymous"
	attachmentBindingAttached  = "attached"

	operationKindNone           = "none"
	operationKindRemoteReplace  = "remote_replace"
	operationKindSourceRecovery = "source_recovery"

	outboxStateNone     = "none"
	outboxStatePrepared = "prepared"
	outboxStateRejected = "rejected" // Retained policy evidence; never dispatchable.
)

type attachmentStateRecord struct {
	AttachmentEpoch                 int64
	CurrentSourceID                 string
	BindingState                    string
	AttachedUserID                  string
	SchemaName                      string
	LastBundleSeqSeen               int64
	RebuildRequired                 bool
	PendingInitializationID         string
	InitializationReconnectRequired bool
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
	SnapshotTransfer    string
}

type outboxBundleRecord struct {
	State                string
	SourceID             string
	SourceBundleID       int64
	CanonicalRequestHash string
	RowCount             int64
	InitializationID     string
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
		SELECT attachment_epoch, current_source_id, binding_state, attached_user_id, schema_name, last_bundle_seq_seen, rebuild_required, pending_initialization_id, initialization_reconnect_required
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(
		&rec.AttachmentEpoch,
		&rec.CurrentSourceID,
		&rec.BindingState,
		&rec.AttachedUserID,
		&rec.SchemaName,
		&rec.LastBundleSeqSeen,
		&rebuildRequired,
		&rec.PendingInitializationID,
		&rec.InitializationReconnectRequired,
	); err != nil {
		return nil, fmt.Errorf("failed to load attachment state: %w", err)
	}
	rec.RebuildRequired = rebuildRequired == 1
	if err := validateOptionalSourceID(rec.CurrentSourceID); err != nil {
		return nil, fmt.Errorf("persisted attachment source id is invalid: %w", err)
	}
	if err := sessionid.ValidateOptional(rec.PendingInitializationID); err != nil {
		return nil, fmt.Errorf("persisted attachment initialization id is invalid: %w", err)
	}
	if strings.TrimSpace(rec.BindingState) == "" {
		rec.BindingState = attachmentBindingAnonymous
	}
	return &rec, nil
}

func persistAttachmentState(ctx context.Context, e execer, rec *attachmentStateRecord, epochAlreadyAdvanced ...bool) error {
	if rec == nil {
		return fmt.Errorf("attachment state is required")
	}
	if err := validateOptionalSourceID(rec.CurrentSourceID); err != nil {
		return fmt.Errorf("attachment source id is invalid: %w", err)
	}
	if err := sessionid.ValidateOptional(rec.PendingInitializationID); err != nil {
		return fmt.Errorf("attachment initialization id is invalid: %w", err)
	}
	rebuildRequired := 0
	if rec.RebuildRequired {
		rebuildRequired = 1
	}
	alreadyAdvanced := len(epochAlreadyAdvanced) > 0 && epochAlreadyAdvanced[0]
	if _, err := e.ExecContext(ctx, `
		UPDATE _sync_attachment_state
		SET attachment_epoch = attachment_epoch + CASE WHEN NOT ? AND (current_source_id <> ? OR binding_state <> ? OR attached_user_id <> ?) THEN 1 ELSE 0 END,
			current_source_id = ?,
			binding_state = ?,
			attached_user_id = ?,
			schema_name = ?,
			last_bundle_seq_seen = ?,
			rebuild_required = ?,
			pending_initialization_id = ?, initialization_reconnect_required=?
		WHERE singleton_key = 1
	`, alreadyAdvanced, rec.CurrentSourceID, rec.BindingState, rec.AttachedUserID, rec.CurrentSourceID, rec.BindingState, rec.AttachedUserID, rec.SchemaName, rec.LastBundleSeqSeen, rebuildRequired, rec.PendingInitializationID, rec.InitializationReconnectRequired); err != nil {
		return fmt.Errorf("failed to persist attachment state: %w", err)
	}
	return bindLocalBaselineContext(ctx, e)
}

// Explicit replacement advances the incarnation even if the binding repeats.
// SQLite's checked integer column rejects overflow in the same transaction.
func advanceAttachmentEpoch(ctx context.Context, e execer) error {
	_, err := e.ExecContext(ctx, `UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1 WHERE singleton_key=1`)
	if err != nil {
		return fmt.Errorf("attachment incarnation cannot advance: %w", err)
	}
	return nil
}

func loadOperationState(ctx context.Context, q queryRower) (*operationStateRecord, error) {
	var rec operationStateRecord
	if err := q.QueryRowContext(ctx, `
		SELECT kind, target_user_id, reason, replacement_source_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count, snapshot_transfer
		FROM _sync_operation_state
		WHERE singleton_key = 1
	`).Scan(&rec.Kind, &rec.TargetUserID, &rec.Reason, &rec.ReplacementSourceID, &rec.StagedSnapshotID, &rec.SnapshotBundleSeq, &rec.SnapshotRowCount, &rec.SnapshotTransfer); err != nil {
		return nil, fmt.Errorf("failed to load operation state: %w", err)
	}
	if strings.TrimSpace(rec.Kind) == "" {
		rec.Kind = operationKindNone
	}
	if err := validateOptionalSourceID(rec.ReplacementSourceID); err != nil {
		return nil, fmt.Errorf("persisted operation replacement source id is invalid: %w", err)
	}
	return &rec, nil
}

func persistOperationState(ctx context.Context, e execer, rec *operationStateRecord) error {
	if rec == nil {
		return fmt.Errorf("operation state is required")
	}
	if err := validateOptionalSourceID(rec.ReplacementSourceID); err != nil {
		return fmt.Errorf("operation replacement source id is invalid: %w", err)
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
			snapshot_row_count = ?, snapshot_transfer = ?
		WHERE singleton_key = 1
	`, rec.Kind, rec.TargetUserID, rec.Reason, rec.ReplacementSourceID, rec.StagedSnapshotID, rec.SnapshotBundleSeq, rec.SnapshotRowCount, rec.SnapshotTransfer); err != nil {
		return fmt.Errorf("failed to persist operation state: %w", err)
	}
	return nil
}

func ensureSourceState(ctx context.Context, e execer, sourceID string) error {
	if err := validateOptionalSourceID(sourceID); err != nil {
		return err
	}
	if sourceID == "" {
		return nil
	}
	if _, err := e.ExecContext(ctx, `
		INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
		VALUES(?, 1, '')
		ON CONFLICT(source_id) DO NOTHING
	`, sourceID); err != nil {
		return fmt.Errorf("failed to ensure source state: %w", err)
	}
	return nil
}

func loadSourceState(ctx context.Context, q queryRower, sourceID string) (*sourceStateRecord, error) {
	if err := validateSourceID(sourceID); err != nil {
		return nil, err
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
	if err := validateSourceID(rec.SourceID); err != nil {
		return nil, fmt.Errorf("persisted source state id is invalid: %w", err)
	}
	if err := validateOptionalSourceID(rec.ReplacedBySourceID); err != nil {
		return nil, fmt.Errorf("persisted replacement source id is invalid: %w", err)
	}
	return &rec, nil
}

func updateSourceNextBundleID(ctx context.Context, e execer, sourceID string, nextSourceBundleID int64) error {
	if err := validateSourceID(sourceID); err != nil {
		return err
	}
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
	if err := validateSourceID(sourceID); err != nil {
		return err
	}
	if err := validateSourceID(replacedBySourceID); err != nil {
		return err
	}
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
		SELECT state, source_id, source_bundle_id, canonical_request_hash, row_count, initialization_id
		FROM _sync_outbox_bundle
		WHERE singleton_key = 1
	`).Scan(
		&rec.State,
		&rec.SourceID,
		&rec.SourceBundleID,
		&rec.CanonicalRequestHash,
		&rec.RowCount,
		&rec.InitializationID,
	); err != nil {
		return nil, fmt.Errorf("failed to load outbox bundle: %w", err)
	}
	if strings.TrimSpace(rec.State) == "" {
		rec.State = outboxStateNone
	}
	if err := validateOptionalSourceID(rec.SourceID); err != nil {
		return nil, fmt.Errorf("persisted outbox source id is invalid: %w", err)
	}
	if err := sessionid.ValidateOptional(rec.InitializationID); err != nil {
		return nil, fmt.Errorf("persisted outbox initialization id is invalid: %w", err)
	}
	return &rec, nil
}

func persistOutboxBundle(ctx context.Context, e execer, rec *outboxBundleRecord) error {
	if rec == nil {
		return fmt.Errorf("outbox bundle is required")
	}
	if err := validateOptionalSourceID(rec.SourceID); err != nil {
		return fmt.Errorf("outbox source id is invalid: %w", err)
	}
	if err := sessionid.ValidateOptional(rec.InitializationID); err != nil {
		return fmt.Errorf("outbox initialization id is invalid: %w", err)
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
			initialization_id = ?
		WHERE singleton_key = 1
	`, rec.State, rec.SourceID, rec.SourceBundleID, rec.CanonicalRequestHash, rec.RowCount, rec.InitializationID); err != nil {
		return fmt.Errorf("failed to persist outbox bundle: %w", err)
	}
	return nil
}

func clearOutboxBundle(ctx context.Context, e execer) error {
	if tx, ok := e.(sqliteTransaction); ok {
		j, err := loadUploadJournal(ctx, tx)
		if err != nil {
			return err
		}
		if j != nil && j.Phase != "ack_pending" {
			return &UploadJournalIntegrityError{"cannot release an unsettled frozen upload"}
		}
	}
	return persistOutboxBundle(ctx, e, &outboxBundleRecord{})
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
