// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

const (
	SchemaGenerationIDHeader          = "Oversync-Schema-Generation-ID"
	SchemaGenerationFingerprintHeader = "Oversync-Schema-Generation-Fingerprint"
)

// SchemaGenerationProof identifies one exact, compiled schema generation.
type SchemaGenerationProof struct {
	ID          SchemaGenerationID
	Fingerprint string
}

type SchemaGenerationRequestErrorCode string

const (
	SchemaGenerationMissing              SchemaGenerationRequestErrorCode = "missing_schema_generation"
	SchemaGenerationIDInvalid            SchemaGenerationRequestErrorCode = "invalid_schema_generation_id"
	SchemaGenerationUnknown              SchemaGenerationRequestErrorCode = "unknown_schema_generation"
	SchemaGenerationFingerprintMissing   SchemaGenerationRequestErrorCode = "missing_schema_generation_fingerprint"
	SchemaGenerationFingerprintMalformed SchemaGenerationRequestErrorCode = "malformed_schema_generation_fingerprint"
	SchemaGenerationFingerprintMismatch  SchemaGenerationRequestErrorCode = "schema_generation_fingerprint_mismatch"
	SchemaGenerationUnsupported          SchemaGenerationRequestErrorCode = "unsupported_schema_generation"
	SchemaGenerationRetired              SchemaGenerationRequestErrorCode = "retired_schema_generation"
)

type SchemaGenerationRequestError struct {
	Code         SchemaGenerationRequestErrorCode
	Field        string
	GenerationID SchemaGenerationID
	Message      string
}

func (e *SchemaGenerationRequestError) Error() string {
	if e == nil || e.Message == "" {
		return "schema generation request is invalid"
	}
	return e.Message
}

type SourceGenerationErrorCode string

const (
	SourceGenerationUnbound              SourceGenerationErrorCode = "source_generation_unbound"
	SourceGenerationConflict             SourceGenerationErrorCode = "source_generation_conflict"
	SchemaGenerationDowngradeForbidden   SourceGenerationErrorCode = "schema_generation_downgrade_forbidden"
	SourceGenerationStateCorrupt         SourceGenerationErrorCode = "corrupt_source_generation_state"
	SourceGenerationBindingLimitExceeded SourceGenerationErrorCode = "source_binding_limit_exceeded"
)

type SourceGenerationError struct {
	Code                  SourceGenerationErrorCode
	RequestedGenerationID SchemaGenerationID
	BoundGenerationID     SchemaGenerationID
	Field                 string
	Message               string
}

func (e *SourceGenerationError) Error() string {
	if e == nil || e.Message == "" {
		return "source generation state is invalid"
	}
	return e.Message
}

type SessionGenerationErrorCode string

const (
	SessionGenerationMismatch     SessionGenerationErrorCode = "session_generation_mismatch"
	SessionGenerationStateCorrupt SessionGenerationErrorCode = "corrupt_session_generation_state"
)

type SessionGenerationError struct {
	Code                  SessionGenerationErrorCode
	SessionKind           string
	RequestedGenerationID SchemaGenerationID
	StoredGenerationID    SchemaGenerationID
	Field                 string
	Message               string
}

func (e *SessionGenerationError) Error() string {
	if e == nil || e.Message == "" {
		return "session generation state is invalid"
	}
	return e.Message
}

type CommittedPushStateErrorCode string

const CommittedPushStateCorrupt CommittedPushStateErrorCode = "corrupt_committed_push_state"

type CommittedPushStateError struct {
	Code    CommittedPushStateErrorCode
	Field   string
	Message string
}

func (e *CommittedPushStateError) Error() string {
	if e == nil || e.Message == "" {
		return "committed push state is invalid"
	}
	return e.Message
}

type GenerationErrorResponse struct {
	Error       string `json:"error"`
	Message     string `json:"message"`
	Retryable   bool   `json:"retryable"`
	Remediation string `json:"remediation"`
}

type schemaGenerationView struct {
	contract        SchemaGenerationContract
	tables          map[string]generationTableView
	canonicalTables map[string]canonicalProjectionTable
	byID            []generationTableView
	dataPlaneReady  bool
}

type canonicalProjectionTable struct {
	syncKeyColumn string
	syncKeyType   string
}

// generationSelection is the non-context capability passed across each
// generation-sensitive data-plane boundary. The view is immutable after
// Bootstrap publishes it.
type generationSelection struct {
	proof SchemaGenerationProof
	view  *schemaGenerationView
}

type generationTableView struct {
	contract GenerationTableContract
	tableID  int32
}

func validateSchemaGenerationProof(proof SchemaGenerationProof) error {
	if proof.ID == 0 {
		return &SchemaGenerationRequestError{Code: SchemaGenerationMissing, Field: "schema_generation_id", Message: "schema generation is required"}
	}
	if proof.ID < 1 {
		return &SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Field: "schema_generation_id", Message: "schema generation ID is invalid"}
	}
	if proof.Fingerprint == "" {
		return &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMissing, Field: "schema_generation_fingerprint", GenerationID: proof.ID, Message: "schema generation fingerprint is required"}
	}
	if !isLowerHexFingerprint(proof.Fingerprint) {
		return &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMalformed, Field: "schema_generation_fingerprint", GenerationID: proof.ID, Message: "schema generation fingerprint is malformed"}
	}
	return nil
}

func parseSchemaGenerationID(value string) (SchemaGenerationID, error) {
	if value == "" {
		return 0, &SchemaGenerationRequestError{Code: SchemaGenerationMissing, Field: "schema_generation_id", Message: "schema generation is required"}
	}
	if value[0] < '1' || value[0] > '9' {
		return 0, &SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Field: "schema_generation_id", Message: "schema generation ID is invalid"}
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, &SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Field: "schema_generation_id", Message: "schema generation ID is invalid"}
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed < 1 {
		return 0, &SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Field: "schema_generation_id", Message: "schema generation ID is invalid"}
	}
	return SchemaGenerationID(parsed), nil
}

func isLowerHexFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if !((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')) {
			return false
		}
	}
	return true
}

func canonicalSchemaGenerationID(id SchemaGenerationID) string {
	return strconv.FormatInt(int64(id), 10)
}

func (s *SyncService) resolveRequestGeneration(ctx context.Context, actor Actor, proof SchemaGenerationProof) (_ *schemaGenerationView, resultErr error) {
	defer func() { s.observeGenerationProof(actor, proof, resultErr) }()
	if err := validateSchemaGenerationProof(proof); err != nil {
		return nil, err
	}
	contract, ok := s.schemaGenerationRegistry.Generation(proof.ID)
	if !ok {
		if err := s.retiredGenerationError(proof); err != nil {
			return nil, err
		}
		binding, err := s.loadActorSourceGenerationState(ctx, actor, proof.ID)
		if err != nil {
			return nil, err
		}
		if binding == nil {
			return nil, &SchemaGenerationRequestError{Code: SchemaGenerationUnknown, GenerationID: proof.ID, Message: "schema generation is unknown"}
		}
		if err := validateStoredSourceGeneration(binding, proof.ID); err != nil {
			return nil, err
		}
		switch {
		case proof.ID < binding.generationID:
			return nil, sourceGenerationError(SchemaGenerationDowngradeForbidden, proof.ID, binding.generationID, "", "schema generation downgrade is forbidden")
		case proof.ID == binding.generationID && proof.Fingerprint != binding.fingerprint:
			return nil, sourceGenerationError(SourceGenerationConflict, proof.ID, binding.generationID, "", "source is bound to another schema generation")
		case proof.ID == binding.generationID:
			return nil, &SchemaGenerationRequestError{Code: SchemaGenerationUnsupported, GenerationID: proof.ID, Message: "schema generation is no longer supported"}
		default:
			return nil, &SchemaGenerationRequestError{Code: SchemaGenerationUnknown, GenerationID: proof.ID, Message: "schema generation is unknown"}
		}
	}
	if contract.Fingerprint() != proof.Fingerprint {
		return nil, &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMismatch, Field: "schema_generation_fingerprint", GenerationID: proof.ID, Message: "schema generation fingerprint does not match"}
	}
	s.mu.RLock()
	view := s.generationViews[proof.ID]
	s.mu.RUnlock()
	if view == nil {
		return nil, fmt.Errorf("schema generation %d runtime view is unavailable", proof.ID)
	}
	return view, nil
}

type sourceGenerationState struct {
	userPK             int64
	generationID       SchemaGenerationID
	fingerprint        string
	bound              bool
	sourceState        bool
	sourceLifecycle    string
	replacedBySourceID string
}

func (s *SyncService) loadActorSourceGenerationState(ctx context.Context, actor Actor, requestedID SchemaGenerationID) (*sourceGenerationState, error) {
	var state sourceGenerationState
	var generationID int32
	err := s.pool.QueryRow(ctx, `
		SELECT u.user_pk,
		       source.source_id IS NOT NULL,
		       binding.source_id IS NOT NULL,
		       COALESCE(binding.schema_generation_id, 0),
		       COALESCE(binding.schema_generation_fingerprint, ''),
		       COALESCE(source.state, ''),
		       COALESCE(source.replaced_by_source_id, '')
		FROM sync.user_state u
		LEFT JOIN sync.source_state source ON source.user_pk=u.user_pk AND source.source_id=$2
		LEFT JOIN sync.source_generation_bindings binding ON binding.user_pk=u.user_pk AND binding.source_id=$2
		WHERE u.user_id=$1
	`, actor.UserID, actor.SourceID).Scan(&state.userPK, &state.sourceState, &state.bound, &generationID, &state.fingerprint, &state.sourceLifecycle, &state.replacedBySourceID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load source generation binding: %w", err)
	}
	state.generationID = SchemaGenerationID(generationID)
	if state.bound {
		if err := validateStoredSourceGeneration(&state, requestedID); err != nil {
			return nil, err
		}
	}
	if state.sourceState != state.bound {
		field := "source_state"
		if state.sourceState {
			field = "source_generation_binding"
		}
		return nil, sourceGenerationError(SourceGenerationStateCorrupt, requestedID, state.generationID, field, "source generation state is corrupt")
	}
	if !state.bound {
		return nil, nil
	}
	return &state, nil
}

func validateStoredSourceGeneration(state *sourceGenerationState, requestedID SchemaGenerationID) error {
	if state == nil {
		return nil
	}
	if state.generationID < 1 {
		return sourceGenerationError(SourceGenerationStateCorrupt, requestedID, 0, "schema_generation_id", "source generation state is corrupt")
	}
	if !isLowerHexFingerprint(state.fingerprint) {
		return sourceGenerationError(SourceGenerationStateCorrupt, requestedID, state.generationID, "schema_generation_fingerprint", "source generation state is corrupt")
	}
	return nil
}

func sourceGenerationError(code SourceGenerationErrorCode, requested, bound SchemaGenerationID, field, message string) error {
	return &SourceGenerationError{Code: code, RequestedGenerationID: requested, BoundGenerationID: bound, Field: field, Message: message}
}

func (s *SyncService) requireExactSourceGenerationBinding(ctx context.Context, actor Actor, proof SchemaGenerationProof) (*sourceGenerationState, error) {
	state, err := s.loadActorSourceGenerationState(ctx, actor, proof.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, sourceGenerationError(SourceGenerationUnbound, proof.ID, 0, "", "source has not connected")
	}
	if proof.ID < state.generationID {
		return nil, sourceGenerationError(SchemaGenerationDowngradeForbidden, proof.ID, state.generationID, "", "schema generation downgrade is forbidden")
	}
	if proof.ID != state.generationID || proof.Fingerprint != state.fingerprint {
		return nil, sourceGenerationError(SourceGenerationConflict, proof.ID, state.generationID, "", "source is bound to another schema generation")
	}
	if state.sourceLifecycle == sourceStateRetired {
		return nil, &SourceRetiredError{UserID: actor.UserID, SourceID: actor.SourceID, ReplacedBySourceID: state.replacedBySourceID}
	}
	return state, nil
}

func (s *SyncService) authorizeBoundGeneration(ctx context.Context, actor Actor, proof SchemaGenerationProof) (*schemaGenerationView, error) {
	view, err := s.resolveRequestGeneration(ctx, actor, proof)
	if err != nil {
		return nil, err
	}
	if _, err := s.requireExactSourceGenerationBinding(ctx, actor, proof); err != nil {
		return nil, err
	}
	return view, nil
}

func (s *SyncService) authorizeGenerationSelection(ctx context.Context, actor Actor, proof SchemaGenerationProof) (generationSelection, error) {
	view, err := s.authorizeBoundGeneration(ctx, actor, proof)
	if err != nil {
		return generationSelection{}, err
	}
	if !view.dataPlaneReady {
		return generationSelection{}, &SchemaGenerationRequestError{Code: SchemaGenerationUnsupported, GenerationID: proof.ID, Message: "schema generation data plane is not supported"}
	}
	return generationSelection{proof: proof, view: view}, nil
}

func selectionFromResolvedGeneration(proof SchemaGenerationProof, view *schemaGenerationView) (generationSelection, error) {
	if view == nil {
		return generationSelection{}, fmt.Errorf("schema generation runtime view is unavailable")
	}
	if !view.dataPlaneReady {
		return generationSelection{}, &SchemaGenerationRequestError{Code: SchemaGenerationUnsupported, GenerationID: proof.ID, Message: "schema generation data plane is not supported"}
	}
	return generationSelection{proof: proof, view: view}, nil
}
