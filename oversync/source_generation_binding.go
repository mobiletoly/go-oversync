// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type sourceGenerationBindingRow struct {
	generationID SchemaGenerationID
	fingerprint  string
}

func loadSourceGenerationBindingRow(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, forUpdate bool) (*sourceGenerationBindingRow, error) {
	query := `SELECT schema_generation_id, schema_generation_fingerprint FROM sync.source_generation_bindings WHERE user_pk=$1 AND source_id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var id int32
	var row sourceGenerationBindingRow
	if err := tx.QueryRow(ctx, query, userPK, sourceID).Scan(&id, &row.fingerprint); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load source generation binding: %w", err)
	}
	row.generationID = SchemaGenerationID(id)
	return &row, nil
}

func validateBindingRow(row *sourceGenerationBindingRow, requested SchemaGenerationID) error {
	if row.generationID < 1 {
		return sourceGenerationError(SourceGenerationStateCorrupt, requested, 0, "schema_generation_id", "source generation state is corrupt")
	}
	if !isLowerHexFingerprint(row.fingerprint) {
		return sourceGenerationError(SourceGenerationStateCorrupt, requested, row.generationID, "schema_generation_fingerprint", "source generation state is corrupt")
	}
	return nil
}

func requireBindingPair(row *sourceGenerationBindingRow, proof SchemaGenerationProof) error {
	if err := validateBindingRow(row, proof.ID); err != nil {
		return err
	}
	if proof.ID < row.generationID {
		return sourceGenerationError(SchemaGenerationDowngradeForbidden, proof.ID, row.generationID, "", "schema generation downgrade is forbidden")
	}
	if proof.ID != row.generationID || proof.Fingerprint != row.fingerprint {
		return sourceGenerationError(SourceGenerationConflict, proof.ID, row.generationID, "", "source is bound to another schema generation")
	}
	return nil
}

func (s *SyncService) admitSourceGenerationBinding(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, proof SchemaGenerationProof) error {
	return s.admitSourceGenerationBindingWithState(ctx, tx, userPK, sourceID, proof, sourceStateActive)
}

func (s *SyncService) admitReplacementSourceBinding(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, proof SchemaGenerationProof) error {
	return s.admitSourceGenerationBindingWithState(ctx, tx, userPK, sourceID, proof, sourceStateReserved)
}

func (s *SyncService) admitSourceGenerationBindingWithState(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, proof SchemaGenerationProof, state string) error {
	rows, err := tx.Query(ctx, `SELECT source_id FROM sync.source_generation_bindings WHERE user_pk=$1 ORDER BY source_id COLLATE "C" LIMIT $2`, userPK, s.config.MaxClientSourcesPerUser+1)
	if err != nil {
		return fmt.Errorf("inspect source binding admission: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ignored string
		if err := rows.Scan(&ignored); err != nil {
			return fmt.Errorf("scan source binding admission: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate source binding admission: %w", err)
	}
	if count >= s.config.MaxClientSourcesPerUser {
		return sourceGenerationError(SourceGenerationBindingLimitExceeded, proof.ID, 0, "", "source binding admission is unavailable")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sync.source_state (user_pk,source_id,state,max_committed_source_bundle_id,replaced_by_source_id,retirement_reason) VALUES ($1,$2,$3,0,'','')`, userPK, sourceID, state); err != nil {
		return fmt.Errorf("insert source state for generation binding: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sync.source_generation_bindings (user_pk,source_id,schema_generation_id,schema_generation_fingerprint) VALUES ($1,$2,$3,$4)`, userPK, sourceID, proof.ID, proof.Fingerprint); err != nil {
		return fmt.Errorf("insert source generation binding: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sync.push_source_progress(user_pk,source_id) VALUES($1,$2)`, userPK, sourceID); err != nil {
		return fmt.Errorf("retain client source ownership: %w", err)
	}
	return nil
}

func (s *SyncService) createOrConfirmSourceGenerationBinding(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, proof SchemaGenerationProof) error {
	source, err := loadSourceStateRow(ctx, tx, userPK, actor.SourceID, true)
	if err != nil {
		return err
	}
	binding, err := loadSourceGenerationBindingRow(ctx, tx, userPK, actor.SourceID, true)
	if err != nil {
		return err
	}
	if source == nil && binding == nil {
		return s.admitSourceGenerationBinding(ctx, tx, userPK, actor.SourceID, proof)
	}
	if source == nil {
		if err := validateBindingRow(binding, proof.ID); err != nil {
			return err
		}
		return sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, binding.generationID, "source_state", "source generation state is corrupt")
	}
	progress, err := loadPushProgress(ctx, tx, userPK, actor.SourceID)
	if err != nil {
		return err
	}
	if binding == nil {
		if !progress.present {
			return sourceGenerationError(SourceGenerationConflict, proof.ID, 0, "source_id", "source_id belongs to a trusted server writer")
		}
		return sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "source_generation_binding", "source generation state is corrupt")
	}
	if !progress.present {
		return sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "source_id", "client source ownership is missing")
	}
	if err := requireBindingPair(binding, proof); err != nil {
		return err
	}
	if source.State == sourceStateRetired {
		return &SourceRetiredError{UserID: actor.UserID, SourceID: actor.SourceID, ReplacedBySourceID: source.ReplacedBySourceID}
	}
	return nil
}

func requireExactSourceGenerationBindingInTx(ctx context.Context, tx pgx.Tx, userPK int64, userID, sourceID string, proof SchemaGenerationProof) error {
	source, err := requireExactSourceGenerationBindingStateInTx(ctx, tx, userPK, sourceID, proof, true)
	if err != nil {
		return err
	}
	if source.State == sourceStateRetired {
		return &SourceRetiredError{UserID: userID, SourceID: sourceID, ReplacedBySourceID: source.ReplacedBySourceID}
	}
	return nil
}

// requireExactSourceGenerationBindingStateInTx validates the immutable source
// and generation identity without applying current source lifecycle policy.
// Replay uses the read-only form because an authenticated retained commit may
// be read after the source has retired; new writes use the locking form and
// apply lifecycle policy only after committed-history classification.
func requireExactSourceGenerationBindingStateInTx(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string, proof SchemaGenerationProof, forUpdate bool) (*sourceStateRow, error) {
	source, err := loadSourceStateRow(ctx, tx, userPK, sourceID, forUpdate)
	if err != nil {
		return nil, err
	}
	binding, err := loadSourceGenerationBindingRow(ctx, tx, userPK, sourceID, forUpdate)
	if err != nil {
		return nil, err
	}
	if source == nil && binding == nil {
		return nil, sourceGenerationError(SourceGenerationUnbound, proof.ID, 0, "", "source has not connected")
	}
	if binding != nil {
		if err := validateBindingRow(binding, proof.ID); err != nil {
			return nil, err
		}
	}
	if source == nil {
		return nil, sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, binding.generationID, "source_state", "source generation state is corrupt")
	}
	if binding == nil {
		return nil, sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "source_generation_binding", "source generation state is corrupt")
	}
	if err := requireBindingPair(binding, proof); err != nil {
		return nil, err
	}
	return source, nil
}
