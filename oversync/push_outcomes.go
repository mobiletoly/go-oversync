// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *SyncService) validateOutcomeActor(ctx context.Context, actor Actor, proof SchemaGenerationProof) error {
	if err := rejectRetryableCallbackContext(ctx, "push outcome"); err != nil {
		return err
	}
	if err := s.validateClientActor(actor, true); err != nil {
		return err
	}
	return validateSchemaGenerationProof(proof)
}

// ResolvePushOutcome either returns immutable terminal evidence or durably
// cancels the exact next tuple under the same source lock used by PushOneShot.
func (s *SyncService) ResolvePushOutcome(ctx context.Context, actor Actor, proof SchemaGenerationProof, req PushOutcomeResolveRequest) (_ *RetainedPushOutcome, err error) {
	if err = s.validateOutcomeActor(ctx, actor, proof); err != nil {
		return nil, err
	}
	if err = validOperationTuple(req.SourceBundleID, req.CanonicalRequestHash, req.InitializationID); err != nil {
		return nil, &PushInvalidError{Message: err.Error()}
	}
	done, err := s.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	releaseOperation := done
	defer releaseHistoryProjection(&releaseOperation)
	releaseProjection, err := s.acquireHistoryProjection(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseHistoryProjection(&releaseProjection)
	conn, release, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	defer release()
	var result *RetainedPushOutcome
	err = runRetryableOwnedTransaction(ctx, conn, func(tx pgx.Tx) error {
		result = nil
		admission, err := s.admitPushOperation(ctx, tx, actor, proof, req, ScopeTransactionPushResolve)
		if err != nil {
			return err
		}
		result = admission.retained
		if result != nil {
			return nil
		}
		view, err := s.resolveRequestGeneration(ctx, actor, proof)
		if err != nil {
			return err
		}
		result, err = s.encodePushOutcome(newPushOutcome(actor, proof, req, "cancelled"), view)
		if err != nil {
			return err
		}
		return persistPushOutcome(ctx, tx, admission.userPK, result)
	})
	if err != nil {
		return nil, err
	}
	transferHistoryProjectionForHTTP(ctx, &result.historyProjectionRelease, &releaseProjection, &releaseOperation)
	return result, nil
}

// AcknowledgePushOutcome preserves the original binding in a compact latest-ack
// fence. Exact duplicate matching precedes mutable generation/source admission.
func (s *SyncService) AcknowledgePushOutcome(ctx context.Context, actor Actor, proof SchemaGenerationProof, req PushOutcomeAckRequest) error {
	if err := s.validateOutcomeActor(ctx, actor, proof); err != nil {
		return err
	}
	if err := validOperationTuple(req.SourceBundleID, req.CanonicalRequestHash, nil); err != nil {
		return &PushInvalidError{Message: err.Error()}
	}
	if !isLowerHexFingerprint(req.OutcomeHash) {
		return &PushInvalidError{Message: "invalid outcome hash"}
	}
	done, err := s.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()
	conn, release, err := s.acquireUserUploadConn(ctx, actor.UserID)
	if err != nil {
		return err
	}
	defer release()
	return runRetryableOwnedTransaction(ctx, conn, func(tx pgx.Tx) error {
		return s.ackPushOutcomeInTx(ctx, tx, actor, proof, req)
	})
}
func exactLatestAcknowledgment(p *pushSourceProgress, proof SchemaGenerationProof, req PushOutcomeAckRequest) bool {
	return p.generationID != nil && p.fingerprint != nil && p.requestHash != nil && p.outcomeHash != nil && *p.generationID == int32(proof.ID) && *p.fingerprint == proof.Fingerprint && *p.requestHash == req.CanonicalRequestHash && *p.outcomeHash == req.OutcomeHash
}

func (s *SyncService) terminalPushRejection(err error) *PushOutcomeRejection {
	classified := s.classifyApplicationWriteError(err)
	var conflict *PushConflictError
	if errors.As(classified, &conflict) && conflict.Conflict != nil {
		detail := *conflict.Conflict
		if len(detail.Op) > 0 {
			detail.Op = strings.ToLower(detail.Op[:1])
		}
		return &PushOutcomeRejection{Code: "push_conflict", Conflict: &detail}
	}
	var rejected *ApplicationWriteRejectedError
	if errors.As(classified, &rejected) {
		return &PushOutcomeRejection{Code: rejected.Category}
	}
	return nil
}
func rollbackPushBusiness(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT push_business`); err != nil {
		return fmt.Errorf("rollback push business savepoint: %w", err)
	}
	_, err := tx.Exec(ctx, `RELEASE SAVEPOINT push_business`)
	return err
}

func (s *SyncService) ackPushOutcomeInTx(ctx context.Context, tx pgx.Tx, actor Actor, proof SchemaGenerationProof, req PushOutcomeAckRequest) error {
	if err := s.admitScopeTransaction(ctx, tx, ScopeTransactionOperation{ScopeID: actor.UserID, SourceID: actor.SourceID, Kind: ScopeTransactionPushAck}); err != nil {
		return err
	}
	if err := s.configureUploadTx(ctx, tx); err != nil {
		return err
	}
	userPK, err := lockPushSource(ctx, tx, actor)
	if err != nil {
		return err
	}
	progress, err := loadPushProgress(ctx, tx, userPK, actor.SourceID)
	if err != nil {
		return err
	}
	if req.SourceBundleID < progress.acknowledged {
		return outcomeError("push_outcome_acknowledged")
	}
	if req.SourceBundleID == progress.acknowledged {
		if exactLatestAcknowledgment(progress, proof, req) {
			return nil
		}
		return outcomeError("push_outcome_mismatch")
	}
	// Retained evidence pins its binding. Ordinary lifecycle cannot destroy it.
	if _, err := requireExactSourceGenerationBindingStateInTx(ctx, tx, userPK, actor.SourceID, proof, true); err != nil {
		return err
	}
	outcome, err := loadRetainedPushOutcome(ctx, tx, userPK, actor.SourceID)
	if err != nil {
		return err
	}
	if outcome == nil {
		return outcomeError("push_outcome_mismatch")
	}
	o := outcome.Outcome
	if o.SourceBundleID != req.SourceBundleID || o.CanonicalRequestHash != req.CanonicalRequestHash || o.OutcomeHash != req.OutcomeHash || o.GenerationID != canonicalSchemaGenerationID(proof.ID) || o.GenerationFingerprint != proof.Fingerprint {
		return outcomeError("push_outcome_mismatch")
	}
	if _, err := tx.Exec(ctx, `UPDATE sync.push_source_progress SET acknowledged_sequence=$3,acknowledged_generation_id=$4,acknowledged_generation_fingerprint=$5,acknowledged_request_hash=$6,acknowledged_outcome_hash=$7 WHERE user_pk=$1 AND source_id=$2`, userPK, actor.SourceID, req.SourceBundleID, int32(proof.ID), proof.Fingerprint, req.CanonicalRequestHash, req.OutcomeHash); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM sync.push_outcomes WHERE user_pk=$1 AND source_id=$2`, userPK, actor.SourceID)
	return err
}
