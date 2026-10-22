// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// PushOutcomeError is nonterminal protocol admission evidence. It must never be
// converted into a rejected business outcome.
type PushOutcomeError struct{ Code string }

func (e *PushOutcomeError) Error() string { return e.Code }

func outcomeError(code string) error { return &PushOutcomeError{Code: code} }

type RetainedPushOutcome struct {
	Outcome                  *PushOutcome
	encoded                  []byte
	historyProjectionRelease func()
}

type pushSourceProgress struct {
	present                               bool
	acknowledged                          int64
	generationID                          *int32
	fingerprint, requestHash, outcomeHash *string
}

func requireNoPendingPushOutcome(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sync.push_outcomes WHERE user_pk=$1 AND source_id=$2)`, userPK, sourceID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return outcomeError("push_outcome_pending")
	}
	return nil
}

// lockPushSource follows the established application admission -> scope/user ->
// source -> generation binding order. No reservation row is upgraded to exclusive.
func lockPushSource(ctx context.Context, tx pgx.Tx, actor Actor) (int64, error) {
	var userPK int64
	if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id=$1 FOR UPDATE`, actor.UserID).Scan(&userPK); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, sourceGenerationError(SourceGenerationUnbound, 0, 0, "", "source has not connected")
		}
		return 0, err
	}
	return userPK, nil
}

func loadPushProgress(ctx context.Context, tx pgx.Tx, userPK int64, source string) (*pushSourceProgress, error) {
	p := &pushSourceProgress{}
	err := tx.QueryRow(ctx, `SELECT acknowledged_sequence,acknowledged_generation_id,acknowledged_generation_fingerprint,acknowledged_request_hash,acknowledged_outcome_hash FROM sync.push_source_progress WHERE user_pk=$1 AND source_id=$2 FOR UPDATE`, userPK, source).Scan(&p.acknowledged, &p.generationID, &p.fingerprint, &p.requestHash, &p.outcomeHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	p.present = err == nil
	return p, err
}

// requireServerWriteSource runs before receipt replay or callbacks. The existing
// upload advisory lock plus this scope row serialize ownership with Connect and
// source lifecycle; reservation locks keep their established key-share mode.
func requireServerWriteSource(ctx context.Context, tx pgx.Tx, userPK int64, sourceID string) error {
	var locked int64
	if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_pk=$1 FOR UPDATE`, userPK).Scan(&locked); err != nil {
		return err
	}
	if _, err := loadSourceStateRow(ctx, tx, userPK, sourceID, true); err != nil {
		return err
	}
	binding, err := loadSourceGenerationBindingRow(ctx, tx, userPK, sourceID, true)
	if err != nil {
		return err
	}
	progress, err := loadPushProgress(ctx, tx, userPK, sourceID)
	if err != nil {
		return err
	}
	if binding != nil || progress.present {
		return sourceGenerationError(SourceGenerationConflict, 0, 0, "source_id", "source_id belongs to client uploads")
	}
	return nil
}
func loadRetainedPushOutcome(ctx context.Context, tx pgx.Tx, userPK int64, source string) (*RetainedPushOutcome, error) {
	var raw []byte
	err := tx.QueryRow(ctx, `SELECT envelope FROM sync.push_outcomes WHERE user_pk=$1 AND source_id=$2`, userPK, source).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var outcome PushOutcome
	if err := json.Unmarshal(raw, &outcome); err != nil {
		return nil, fmt.Errorf("retained outcome is corrupt: %w", err)
	}
	return &RetainedPushOutcome{Outcome: &outcome, encoded: raw}, nil
}
func sameOutcomeOperation(outcome *PushOutcome, proof SchemaGenerationProof, req PushOutcomeResolveRequest) bool {
	if outcome.GenerationID != canonicalSchemaGenerationID(proof.ID) || outcome.GenerationFingerprint != proof.Fingerprint || outcome.SourceBundleID != req.SourceBundleID || outcome.CanonicalRequestHash != req.CanonicalRequestHash {
		return false
	}
	if outcome.InitializationID == nil || req.InitializationID == nil {
		return outcome.InitializationID == nil && req.InitializationID == nil
	}
	return *outcome.InitializationID == *req.InitializationID
}
func classifyPushOperation(p *pushSourceProgress, retained *RetainedPushOutcome, proof SchemaGenerationProof, req PushOutcomeResolveRequest) error {
	if req.SourceBundleID <= p.acknowledged {
		return outcomeError("push_outcome_acknowledged")
	}
	if retained != nil {
		if req.SourceBundleID != retained.Outcome.SourceBundleID {
			return outcomeError("push_outcome_pending")
		}
		if !sameOutcomeOperation(retained.Outcome, proof, req) {
			return outcomeError("push_outcome_mismatch")
		}
		return nil
	}
	if p.acknowledged == math.MaxInt64 || req.SourceBundleID != p.acknowledged+1 {
		return outcomeError("source_sequence_out_of_order")
	}
	return nil
}
func newPushOutcome(actor Actor, proof SchemaGenerationProof, req PushOutcomeResolveRequest, status string) *PushOutcome {
	return &PushOutcome{SourceID: actor.SourceID, GenerationID: canonicalSchemaGenerationID(proof.ID), GenerationFingerprint: proof.Fingerprint, SourceBundleID: req.SourceBundleID, CanonicalRequestHash: req.CanonicalRequestHash, InitializationID: req.InitializationID, Status: status}
}
func (s *SyncService) encodePushOutcome(outcome *PushOutcome, view *schemaGenerationView) (*RetainedPushOutcome, error) {
	hash, err := HashPushOutcome(*outcome, generationDescriptor(view.contract))
	if err != nil {
		return nil, err
	}
	outcome.OutcomeHash = hash
	raw, err := json.Marshal(outcome)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > s.config.MaxPushOutcomeDecodedBytes {
		return nil, outcomeError("push_outcome_too_large")
	}
	var checked PushOutcome
	if err := json.Unmarshal(raw, &checked); err != nil {
		return nil, fmt.Errorf("invalid generated outcome: %w", err)
	}
	return &RetainedPushOutcome{Outcome: outcome, encoded: raw}, nil
}
func persistPushOutcome(ctx context.Context, tx pgx.Tx, userPK int64, retained *RetainedPushOutcome) error {
	o := retained.Outcome
	if _, err := tx.Exec(ctx, `INSERT INTO sync.push_source_progress(user_pk,source_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, userPK, o.SourceID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO sync.push_outcomes(user_pk,source_id,source_bundle_id,generation_id,generation_fingerprint,request_hash,initialization_id,outcome_hash,envelope) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, userPK, o.SourceID, o.SourceBundleID, o.GenerationID, o.GenerationFingerprint, o.CanonicalRequestHash, o.InitializationID, o.OutcomeHash, retained.encoded)
	if err != nil {
		return fmt.Errorf("persist exact terminal outcome: %w", err)
	}
	return nil
}

// PushOutcomeStorage reports logical retained evidence, independently of history.
// PhysicalBytes includes this store's heap/index/TOAST allocation; WAL remains a
// database-wide measurement, never a global capacity promise.
type PushOutcomeStorage struct {
	Outstanding   int64
	PayloadBytes  int64
	OldestAge     time.Duration
	PhysicalBytes int64
}

func (s *SyncService) PushOutcomeStorage(ctx context.Context) (PushOutcomeStorage, error) {
	done, err := s.beginOperation(ctx)
	if err != nil {
		return PushOutcomeStorage{}, err
	}
	defer done()
	var result PushOutcomeStorage
	var age float64
	err = s.pool.QueryRow(ctx, `SELECT count(*),COALESCE(sum(octet_length(envelope)),0)::bigint,COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0)::float8 FROM sync.push_outcomes`).Scan(&result.Outstanding, &result.PayloadBytes, &age)
	if err != nil {
		return result, err
	}
	result.OldestAge = time.Duration(age * float64(time.Second))
	err = s.pool.QueryRow(ctx, `SELECT pg_total_relation_size('sync.push_outcomes')+pg_total_relation_size('sync.push_source_progress')+pg_total_relation_size('sync.push_protocol_store')`).Scan(&result.PhysicalBytes)
	return result, err
}

func lockPushIdentity(ctx context.Context, tx pgx.Tx, userPK int64, source string, proof SchemaGenerationProof) (*sourceStateRow, error) {
	state, err := loadSourceStateRow(ctx, tx, userPK, source, true)
	if err != nil {
		return nil, err
	}
	binding, err := loadSourceGenerationBindingRow(ctx, tx, userPK, source, true)
	if err != nil {
		return nil, err
	}
	if state == nil && binding == nil {
		return nil, sourceGenerationError(SourceGenerationUnbound, proof.ID, 0, "", "source has not connected")
	}
	if state == nil || binding == nil {
		return nil, sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "", "source generation state is corrupt")
	}
	if err := validateBindingRow(binding, proof.ID); err != nil {
		return nil, err
	}
	return state, nil
}

type pushOperationAdmission struct {
	userPK   int64
	retained *RetainedPushOutcome
}

func (s *SyncService) admitPushOperation(ctx context.Context, tx pgx.Tx, actor Actor, proof SchemaGenerationProof, req PushOutcomeResolveRequest, kind ScopeTransactionKind) (*pushOperationAdmission, error) {
	if err := s.admitScopeTransaction(ctx, tx, ScopeTransactionOperation{ScopeID: actor.UserID, SourceID: actor.SourceID, Kind: kind}); err != nil {
		return nil, err
	}
	if err := s.configureUploadTx(ctx, tx); err != nil {
		return nil, err
	}
	userPK, err := lockPushSource(ctx, tx, actor)
	if err != nil {
		return nil, err
	}
	state, err := lockPushIdentity(ctx, tx, userPK, actor.SourceID, proof)
	if err != nil {
		return nil, err
	}
	progress, err := loadPushProgress(ctx, tx, userPK, actor.SourceID)
	if err != nil {
		return nil, err
	}
	if !progress.present {
		return nil, sourceGenerationError(SourceGenerationStateCorrupt, proof.ID, 0, "source_id", "client source ownership is missing")
	}
	retained, err := loadRetainedPushOutcome(ctx, tx, userPK, actor.SourceID)
	if err != nil {
		return nil, err
	}
	if err := classifyPushOperation(progress, retained, proof, req); err != nil {
		return nil, err
	}
	if retained == nil {
		if _, err := requireExactSourceGenerationBindingStateInTx(ctx, tx, userPK, actor.SourceID, proof, true); err != nil {
			return nil, err
		}
		if state.State == sourceStateRetired {
			return nil, &SourceRetiredError{UserID: actor.UserID, SourceID: actor.SourceID, ReplacedBySourceID: state.ReplacedBySourceID}
		}
	}
	return &pushOperationAdmission{userPK: userPK, retained: retained}, nil
}
