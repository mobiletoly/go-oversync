package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

const (
	sourceStateActive   = "active"
	sourceStateReserved = "reserved"
	sourceStateRetired  = "retired"
)

type SnapshotSessionInvalidError struct {
	Message string
}

func (e *SnapshotSessionInvalidError) Error() string { return e.Message }

type SourceReplacementInvalidError struct {
	Message string
}

func (e *SourceReplacementInvalidError) Error() string { return e.Message }

type SourceRetiredError struct {
	UserID             string
	SourceID           string
	ReplacedBySourceID string
}

func (e *SourceRetiredError) Error() string {
	return "source is retired"
}

type sourceStateRow struct {
	UserPK                     int64
	SourceID                   string
	State                      string
	MaxCommittedSourceBundleID int64
	ReplacedBySourceID         string
	RetirementReason           string
}

func loadSourceStateRow(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, forUpdate bool) (*sourceStateRow, error) {
	if err := sourceid.Validate(sourceID); err != nil {
		return nil, fmt.Errorf("source_state lookup source_id is invalid: %w", err)
	}
	query := `
		SELECT user_pk, source_id, state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		FROM sync.source_state
		WHERE user_pk = $1
		  AND source_id = $2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var row sourceStateRow
	if err := tx.QueryRow(ctx, query, userPK, sourceID).Scan(
		&row.UserPK,
		&row.SourceID,
		&row.State,
		&row.MaxCommittedSourceBundleID,
		&row.ReplacedBySourceID,
		&row.RetirementReason,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query source_state: %w", err)
	}
	if err := sourceid.Validate(row.SourceID); err != nil {
		return nil, fmt.Errorf("persisted source_state source_id is invalid: %w", err)
	}
	if err := sourceid.ValidateOptional(row.ReplacedBySourceID); err != nil {
		return nil, fmt.Errorf("persisted source_state replacement source_id is invalid: %w", err)
	}
	return &row, nil
}

func reserveSourceState(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string) error {
	if err := sourceid.Validate(sourceID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.source_state (
			user_pk, source_id, state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		) VALUES ($1, $2, $3, 0, '', '')
	`, userPK, sourceID, sourceStateReserved); err != nil {
		return fmt.Errorf("insert reserved source_state: %w", err)
	}
	return nil
}

func retireSourceState(ctx context.Context, tx pgx.Tx, userPK int64, sourceID, replacedBySourceID, reason string) error {
	if err := sourceid.Validate(sourceID); err != nil {
		return err
	}
	if err := sourceid.Validate(replacedBySourceID); err != nil {
		return err
	}
	if err := requireNoPendingPushOutcome(ctx, tx, userPK, sourceID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.source_state (
			user_pk, source_id, state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
		) VALUES ($1, $2, $3, 0, $4, $5)
		ON CONFLICT (user_pk, source_id) DO UPDATE
		SET state = EXCLUDED.state,
			replaced_by_source_id = EXCLUDED.replaced_by_source_id,
			retirement_reason = EXCLUDED.retirement_reason
	`, userPK, sourceID, sourceStateRetired, replacedBySourceID, reason); err != nil {
		return fmt.Errorf("retire source_state: %w", err)
	}
	return nil
}

func activateSourceState(ctx context.Context, tx pgx.Tx, userPK int64, userID, sourceID string, sourceBundleID int64) error {
	if err := sourceid.Validate(sourceID); err != nil {
		return err
	}
	state, err := loadSourceStateRow(ctx, tx, userPK, sourceID, true)
	if err != nil {
		return err
	}
	if state == nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO sync.source_state (
				user_pk, source_id, state, max_committed_source_bundle_id, replaced_by_source_id, retirement_reason
			) VALUES ($1, $2, $3, $4, '', '')
		`, userPK, sourceID, sourceStateActive, sourceBundleID); err != nil {
			return fmt.Errorf("insert active source_state: %w", err)
		}
		return nil
	}
	if state.State == sourceStateRetired {
		return &SourceRetiredError{
			UserID:             userID,
			SourceID:           sourceID,
			ReplacedBySourceID: state.ReplacedBySourceID,
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sync.source_state
		SET state = $3,
			max_committed_source_bundle_id = $4,
			replaced_by_source_id = '',
			retirement_reason = ''
		WHERE user_pk = $1
		  AND source_id = $2
	`, userPK, sourceID, sourceStateActive, sourceBundleID); err != nil {
		return fmt.Errorf("activate source_state: %w", err)
	}
	return nil
}
