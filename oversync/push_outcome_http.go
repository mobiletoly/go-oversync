// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"errors"
	"net/http"
)

// Parse-only proof admission is deliberate: latest duplicate ack must remain
// available after retirement/rebinding without a retired generation decoder.
func (h *HTTPSyncHandlers) requireOutcomeActor(w http.ResponseWriter, r *http.Request) (Actor, SchemaGenerationProof, bool) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodPost)
	if !ok {
		return Actor{}, SchemaGenerationProof{}, false
	}
	proof, err := parseSchemaGenerationProofHeaders(r.Header)
	if err != nil {
		h.writeGenerationError(w, r, err)
		return Actor{}, SchemaGenerationProof{}, false
	}
	if err := h.service.requestReadinessError(); err != nil {
		h.writeServiceUnavailable(w, r, err)
		return Actor{}, SchemaGenerationProof{}, false
	}
	if err := validateJSONRequestContentType(r, false); err != nil {
		h.writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error(), r)
		return Actor{}, SchemaGenerationProof{}, false
	}
	return actor, proof, true
}
func (h *HTTPSyncHandlers) HandlePushOutcomeResolve(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireOutcomeActor(w, r)
	if !ok {
		return
	}
	var req PushOutcomeResolveRequest
	if err := decodeBoundedJSONRequest(r, &req, OutcomeControlBodyLimit); err != nil {
		h.writeBoundedRequestFailure(w, r, err, "Invalid outcome resolution request")
		return
	}
	result, err := h.service.ResolvePushOutcome(retainHistoryProjectionForHTTP(r.Context()), actor, proof, req)
	if err != nil {
		h.writeCompactPushError(w, r, actor.SourceID, err)
		return
	}
	defer releaseHistoryProjection(&result.historyProjectionRelease)
	h.writeJSONBytes(w, r, http.StatusOK, result.encoded)
}
func (h *HTTPSyncHandlers) HandlePushOutcomeAck(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireOutcomeActor(w, r)
	if !ok {
		return
	}
	var req PushOutcomeAckRequest
	if err := decodeBoundedJSONRequest(r, &req, OutcomeControlBodyLimit); err != nil {
		h.writeBoundedRequestFailure(w, r, err, "Invalid outcome acknowledgment request")
		return
	}
	if err := h.service.AcknowledgePushOutcome(r.Context(), actor, proof, req); err != nil {
		h.writeCompactPushError(w, r, actor.SourceID, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *HTTPSyncHandlers) writePushOutcomeError(w http.ResponseWriter, r *http.Request, err error) bool {
	var outcome *PushOutcomeError
	if !errors.As(err, &outcome) {
		return false
	}
	status := http.StatusConflict
	if outcome.Code == "push_outcome_acknowledged" {
		status = http.StatusGone
	}
	if outcome.Code == "push_outcome_too_large" {
		status = http.StatusRequestEntityTooLarge
	}
	h.writeError(w, status, outcome.Code, outcome.Code, r)
	return true
}
