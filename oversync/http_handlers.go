// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

// HTTPSyncHandlers provides HTTP handlers for the two-way sync API
type HTTPSyncHandlers struct {
	service *SyncService
	logger  *slog.Logger
}

type chunkQueryParams struct {
	afterRowOrdinal *int64
	maxRows         int
}

// NewHTTPSyncHandlers creates a new instance of sync handlers
func NewHTTPSyncHandlers(service *SyncService, logger *slog.Logger) *HTTPSyncHandlers {
	return &HTTPSyncHandlers{
		service: service,
		logger:  logger,
	}
}

func actorFromRequest(r *http.Request) (Actor, error) {
	actor, ok := ActorFromContext(r.Context())
	if !ok {
		return Actor{}, errors.New("authenticated actor not found in request context")
	}
	if err := actor.validate(true); err != nil {
		return Actor{}, err
	}
	return actor, nil
}

func (h *HTTPSyncHandlers) requireActorForMethod(w http.ResponseWriter, r *http.Request, method string) (Actor, bool) {
	if r.Method != method {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only "+method+" method is allowed")
		return Actor{}, false
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return Actor{}, false
	}

	return actor, true
}

func parseChunkQueryParams(r *http.Request, defaultMaxRows int) (chunkQueryParams, error) {
	params := chunkQueryParams{maxRows: defaultMaxRows}

	if afterStr := r.URL.Query().Get("after_row_ordinal"); afterStr != "" {
		parsed, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			return chunkQueryParams{}, errors.New("after_row_ordinal must be an integer")
		}
		params.afterRowOrdinal = &parsed
	}

	if limitStr := r.URL.Query().Get("max_rows"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil {
			return chunkQueryParams{}, errors.New("max_rows must be an integer")
		}
		params.maxRows = parsed
	}

	return params, nil
}

func (h *HTTPSyncHandlers) writeJSON(w http.ResponseWriter, response any, encodeErrorMessage string, logAttrs ...any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		attrs := append([]any{"error", err}, logAttrs...)
		h.logger.Error(encodeErrorMessage, attrs...)
	}
}

func (h *HTTPSyncHandlers) HandleCreatePushSession(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}

	var req PushSessionCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "Failed to parse push session request")
		return
	}

	response, err := h.service.CreatePushSession(r.Context(), actor, &req)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *PushSessionInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "push_session_invalid", invalidErr.Error())
			return
		}
		var uninitializedErr *ScopeUninitializedError
		if errors.As(err, &uninitializedErr) {
			h.writeError(w, http.StatusConflict, "scope_uninitialized", uninitializedErr.Error())
			return
		}
		var initializingErr *ScopeInitializingError
		if errors.As(err, &initializingErr) {
			h.writeError(w, http.StatusConflict, "scope_initializing", initializingErr.Error())
			return
		}
		var staleErr *InitializationStaleError
		if errors.As(err, &staleErr) {
			h.writeError(w, http.StatusConflict, "initialization_stale", staleErr.Error())
			return
		}
		var expiredErr *InitializationExpiredError
		if errors.As(err, &expiredErr) {
			h.writeError(w, http.StatusGone, "initialization_expired", expiredErr.Error())
			return
		}
		var prunedErr *SourceTupleHistoryPrunedError
		if errors.As(err, &prunedErr) {
			h.writeError(w, http.StatusConflict, "history_pruned", prunedErr.Error())
			return
		}
		var sequenceErr *SourceSequenceOutOfOrderError
		if errors.As(err, &sequenceErr) {
			h.writeError(w, http.StatusConflict, "source_sequence_out_of_order", sequenceErr.Error())
			return
		}
		var retiredErr *SourceRetiredError
		if errors.As(err, &retiredErr) {
			h.writeSourceRetired(w, retiredErr)
			return
		}
		h.logger.Error("Failed to create push session", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
		h.writeError(w, http.StatusInternalServerError, "push_session_create_failed", "Failed to create push session")
		return
	}

	h.writeJSON(w, response, "Failed to encode push session response", "user_id", actor.UserID, "source_id", actor.SourceID)
}

func (h *HTTPSyncHandlers) HandlePushSessionChunk(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}
	pushID := r.PathValue("push_id")

	var req PushSessionChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "Failed to parse push chunk request")
		return
	}

	response, err := h.service.UploadPushChunk(r.Context(), actor, pushID, &req)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *PushChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "push_chunk_invalid", invalidErr.Error())
			return
		}
		var outOfOrderErr *PushChunkOutOfOrderError
		if errors.As(err, &outOfOrderErr) {
			h.writeError(w, http.StatusConflict, "push_chunk_out_of_order", outOfOrderErr.Error())
			return
		}
		var notFoundErr *PushSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "push_session_not_found", notFoundErr.Error())
			return
		}
		var expiredErr *PushSessionExpiredError
		if errors.As(err, &expiredErr) {
			h.writeError(w, http.StatusGone, "push_session_expired", expiredErr.Error())
			return
		}
		var initExpiredErr *InitializationExpiredError
		if errors.As(err, &initExpiredErr) {
			h.writeError(w, http.StatusGone, "initialization_expired", initExpiredErr.Error())
			return
		}
		var staleErr *InitializationStaleError
		if errors.As(err, &staleErr) {
			h.writeError(w, http.StatusConflict, "initialization_stale", staleErr.Error())
			return
		}
		var forbiddenErr *PushSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "push_session_forbidden", forbiddenErr.Error())
			return
		}
		h.logger.Error("Failed to upload push chunk", "error", err, "user_id", actor.UserID, "push_id", pushID)
		h.writeError(w, http.StatusInternalServerError, "push_chunk_failed", "Failed to upload push chunk")
		return
	}

	h.writeJSON(w, response, "Failed to encode push chunk response", "user_id", actor.UserID, "push_id", pushID)
}

func (h *HTTPSyncHandlers) HandleCommitPushSession(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}
	pushID := r.PathValue("push_id")

	response, err := h.service.CommitPushSession(r.Context(), actor, pushID)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *PushCommitInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "push_commit_invalid", invalidErr.Error())
			return
		}
		var conflictErr *PushConflictError
		if errors.As(err, &conflictErr) {
			h.writePushConflict(w, conflictErr)
			return
		}
		var notFoundErr *PushSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "push_session_not_found", notFoundErr.Error())
			return
		}
		var expiredErr *PushSessionExpiredError
		if errors.As(err, &expiredErr) {
			h.writeError(w, http.StatusGone, "push_session_expired", expiredErr.Error())
			return
		}
		var initExpiredErr *InitializationExpiredError
		if errors.As(err, &initExpiredErr) {
			h.writeError(w, http.StatusGone, "initialization_expired", initExpiredErr.Error())
			return
		}
		var staleErr *InitializationStaleError
		if errors.As(err, &staleErr) {
			h.writeError(w, http.StatusConflict, "initialization_stale", staleErr.Error())
			return
		}
		var forbiddenErr *PushSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "push_session_forbidden", forbiddenErr.Error())
			return
		}
		var sequenceErr *SourceSequenceChangedError
		if errors.As(err, &sequenceErr) {
			h.writeError(w, http.StatusConflict, "source_sequence_changed", sequenceErr.Error())
			return
		}
		var retiredErr *SourceRetiredError
		if errors.As(err, &retiredErr) {
			h.writeSourceRetired(w, retiredErr)
			return
		}
		h.logger.Error("Failed to commit push session", "error", err, "user_id", actor.UserID, "push_id", pushID)
		h.writeError(w, http.StatusInternalServerError, "push_session_commit_failed", "Failed to commit push session")
		return
	}

	h.writeJSON(w, response, "Failed to encode push session commit response", "user_id", actor.UserID, "push_id", pushID)
}

func (h *HTTPSyncHandlers) HandleConnect(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}

	var req ConnectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "Failed to parse connect request")
		return
	}

	response, err := h.service.Connect(r.Context(), actor, &req)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *ConnectInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "connect_invalid", invalidErr.Error())
			return
		}
		h.logger.Error("Failed to resolve connect lifecycle", "error", err, "user_id", actor.UserID)
		h.writeError(w, http.StatusInternalServerError, "connect_failed", "Failed to resolve connect lifecycle")
		return
	}

	h.writeJSON(w, response, "Failed to encode connect response", "user_id", actor.UserID)
}

func (h *HTTPSyncHandlers) HandleGetCommittedBundleRows(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}

	bundleSeq, err := strconv.ParseInt(r.PathValue("bundle_seq"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "committed_bundle_chunk_invalid", "bundle_seq must be an integer")
		return
	}

	queryParams, err := parseChunkQueryParams(r, h.service.defaultRowsPerCommittedBundleChunk())
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "committed_bundle_chunk_invalid", err.Error())
		return
	}

	response, err := h.service.GetCommittedBundleRows(r.Context(), actor, bundleSeq, queryParams.afterRowOrdinal, queryParams.maxRows)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *CommittedBundleChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "committed_bundle_chunk_invalid", invalidErr.Error())
			return
		}
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			h.writeError(w, http.StatusConflict, "history_pruned", prunedErr.Error())
			return
		}
		var notFoundErr *CommittedBundleNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "committed_bundle_not_found", notFoundErr.Error())
			return
		}
		h.logger.Error("Failed to get committed bundle rows", "error", err, "user_id", actor.UserID, "bundle_seq", bundleSeq)
		h.writeError(w, http.StatusInternalServerError, "committed_bundle_rows_failed", "Failed to fetch committed bundle rows")
		return
	}

	h.writeJSON(w, response, "Failed to encode committed bundle rows response", "user_id", actor.UserID, "bundle_seq", bundleSeq)
}

func (h *HTTPSyncHandlers) HandleDeletePushSession(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodDelete)
	if !ok {
		return
	}
	pushID := r.PathValue("push_id")

	if err := h.service.DeletePushSession(r.Context(), actor, pushID); err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *PushChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "push_chunk_invalid", invalidErr.Error())
			return
		}
		var notFoundErr *PushSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "push_session_not_found", notFoundErr.Error())
			return
		}
		var expiredErr *PushSessionExpiredError
		if errors.As(err, &expiredErr) {
			h.writeError(w, http.StatusGone, "push_session_expired", expiredErr.Error())
			return
		}
		var forbiddenErr *PushSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "push_session_forbidden", forbiddenErr.Error())
			return
		}
		h.logger.Error("Failed to delete push session", "error", err, "user_id", actor.UserID, "push_id", pushID)
		h.writeError(w, http.StatusInternalServerError, "push_session_delete_failed", "Failed to delete push session")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// HandlePull processes bundle pull requests.
func (h *HTTPSyncHandlers) HandlePull(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}

	afterBundleSeq := int64(0)
	if afterStr := r.URL.Query().Get("after_bundle_seq"); afterStr != "" {
		parsedAfter, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after_bundle_seq must be an integer")
			return
		}
		if parsedAfter < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after_bundle_seq must be >= 0")
			return
		}
		afterBundleSeq = parsedAfter
	}

	maxBundles := defaultPullBundlesPerRequest
	if limitStr := r.URL.Query().Get("max_bundles"); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "max_bundles must be an integer")
			return
		}
		if parsedLimit < 1 || parsedLimit > defaultMaxBundlesPerPull {
			h.writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("max_bundles must be between 1 and %d", defaultMaxBundlesPerPull))
			return
		}
		maxBundles = parsedLimit
	}

	targetBundleSeq := int64(0)
	if targetStr := r.URL.Query().Get("target_bundle_seq"); targetStr != "" {
		parsedTarget, err := strconv.ParseInt(targetStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "target_bundle_seq must be an integer")
			return
		}
		if parsedTarget < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "target_bundle_seq must be >= 0")
			return
		}
		targetBundleSeq = parsedTarget
	}

	response, err := h.service.ProcessPull(r.Context(), actor, afterBundleSeq, maxBundles, targetBundleSeq)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			h.writeError(w, http.StatusConflict, "history_pruned", prunedErr.Error())
			return
		}
		var uninitializedErr *ScopeUninitializedError
		if errors.As(err, &uninitializedErr) {
			h.writeError(w, http.StatusConflict, "scope_uninitialized", uninitializedErr.Error())
			return
		}
		var initializingErr *ScopeInitializingError
		if errors.As(err, &initializingErr) {
			h.writeError(w, http.StatusConflict, "scope_initializing", initializingErr.Error())
			return
		}
		h.logger.Error("Failed to process pull", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
		h.writeError(w, http.StatusInternalServerError, "pull_failed", "Failed to process pull")
		return
	}

	h.writeJSON(w, response, "Failed to encode pull response", "user_id", actor.UserID, "source_id", actor.SourceID)
}

// HandleCreateSnapshotSession creates one frozen snapshot session for chunked hydrate/recover.
func (h *HTTPSyncHandlers) HandleCreateSnapshotSession(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}

	var req *SnapshotSessionCreateRequest
	if r.Body != nil {
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var parsed SnapshotSessionCreateRequest
		if err := decoder.Decode(&parsed); err != nil {
			if !errors.Is(err, io.EOF) {
				h.writeError(w, http.StatusBadRequest, "snapshot_session_invalid", "Failed to parse snapshot session request")
				return
			}
		} else {
			var trailing any
			if err := decoder.Decode(&trailing); err != nil && !errors.Is(err, io.EOF) {
				h.writeError(w, http.StatusBadRequest, "snapshot_session_invalid", "Failed to parse snapshot session request")
				return
			}
			req = &parsed
		}
	}

	response, err := h.service.CreateSnapshotSessionWithRequest(r.Context(), actor, req)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *SnapshotSessionInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "snapshot_session_invalid", invalidErr.Error())
			return
		}
		var replacementErr *SourceReplacementInvalidError
		if errors.As(err, &replacementErr) {
			h.writeError(w, http.StatusConflict, "source_replacement_invalid", replacementErr.Error())
			return
		}
		var retiredErr *SourceRetiredError
		if errors.As(err, &retiredErr) {
			h.writeSourceRetired(w, retiredErr)
			return
		}
		var uninitializedErr *ScopeUninitializedError
		if errors.As(err, &uninitializedErr) {
			h.writeError(w, http.StatusConflict, "scope_uninitialized", uninitializedErr.Error())
			return
		}
		var initializingErr *ScopeInitializingError
		if errors.As(err, &initializingErr) {
			h.writeError(w, http.StatusConflict, "scope_initializing", initializingErr.Error())
			return
		}
		h.logger.Error("Failed to create snapshot session", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
		h.writeError(w, http.StatusInternalServerError, "snapshot_session_create_failed", "Failed to create snapshot session")
		return
	}

	h.writeJSON(w, response, "Failed to encode snapshot session response", "user_id", actor.UserID, "source_id", actor.SourceID)
}

func (h *HTTPSyncHandlers) writeSourceRetired(w http.ResponseWriter, retiredErr *SourceRetiredError) {
	if retiredErr == nil {
		h.writeError(w, http.StatusConflict, "source_retired", "source is retired")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)

	response := SourceRetiredResponse{
		Error:              "source_retired",
		Message:            retiredErr.Error(),
		SourceID:           retiredErr.SourceID,
		ReplacedBySourceID: retiredErr.ReplacedBySourceID,
	}
	_ = json.NewEncoder(w).Encode(response)

	h.logger.Debug("HTTP source retired response",
		"status_code", http.StatusConflict,
		"error_code", "source_retired",
		"source_id", retiredErr.SourceID,
		"replaced_by_source_id", retiredErr.ReplacedBySourceID,
	)
}

// HandleGetSnapshotChunk returns one chunk of rows from a frozen snapshot session.
func (h *HTTPSyncHandlers) HandleGetSnapshotChunk(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}

	snapshotID := r.PathValue("snapshot_id")
	afterRowOrdinal := int64(0)
	queryParams, err := parseChunkQueryParams(r, h.service.defaultRowsPerSnapshotChunk())
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", err.Error())
		return
	}
	if queryParams.afterRowOrdinal != nil {
		afterRowOrdinal = *queryParams.afterRowOrdinal
	}

	response, err := h.service.GetSnapshotChunk(r.Context(), actor, snapshotID, afterRowOrdinal, queryParams.maxRows)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *SnapshotChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", invalidErr.Error())
			return
		}
		var notFoundErr *SnapshotSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "snapshot_session_not_found", notFoundErr.Error())
			return
		}
		var expiredErr *SnapshotSessionExpiredError
		if errors.As(err, &expiredErr) {
			h.writeError(w, http.StatusGone, "snapshot_session_expired", expiredErr.Error())
			return
		}
		var forbiddenErr *SnapshotSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "snapshot_session_forbidden", forbiddenErr.Error())
			return
		}
		h.logger.Error("Failed to get snapshot chunk", "error", err, "user_id", actor.UserID, "snapshot_id", snapshotID)
		h.writeError(w, http.StatusInternalServerError, "snapshot_chunk_failed", "Failed to get snapshot chunk")
		return
	}

	h.writeJSON(w, response, "Failed to encode snapshot chunk response", "user_id", actor.UserID, "snapshot_id", snapshotID)
}

// HandleDeleteSnapshotSession deletes an existing frozen snapshot session.
func (h *HTTPSyncHandlers) HandleDeleteSnapshotSession(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.requireActorForMethod(w, r, http.MethodDelete)
	if !ok {
		return
	}

	snapshotID := r.PathValue("snapshot_id")
	if err := h.service.DeleteSnapshotSession(r.Context(), actor, snapshotID); err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		var invalidErr *SnapshotChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", invalidErr.Error())
			return
		}
		var notFoundErr *SnapshotSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "snapshot_session_not_found", notFoundErr.Error())
			return
		}
		var forbiddenErr *SnapshotSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "snapshot_session_forbidden", forbiddenErr.Error())
			return
		}
		h.logger.Error("Failed to delete snapshot session", "error", err, "user_id", actor.UserID, "snapshot_id", snapshotID)
		h.writeError(w, http.StatusInternalServerError, "snapshot_session_delete_failed", "Failed to delete snapshot session")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleStatus returns the current lifecycle and operability status snapshot.
func (h *HTTPSyncHandlers) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}
	response, err := h.service.GetStatus(r.Context())
	if err != nil {
		h.logger.Error("Failed to get service status", "error", err)
		h.writeError(w, http.StatusInternalServerError, "status_failed", "Failed to get service status")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleHealth returns a readiness-oriented health response derived from the service status snapshot.
func (h *HTTPSyncHandlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}
	response, err := h.service.GetStatus(r.Context())
	if err != nil {
		h.logger.Error("Failed to get health status", "error", err)
		h.writeError(w, http.StatusInternalServerError, "health_failed", "Failed to get health status")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if response.Status == "unhealthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(w).Encode(response)
}

// HandleCapabilities returns the current sync capabilities surface.
func (h *HTTPSyncHandlers) HandleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}
	response := h.service.GetCapabilities()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// writeError writes a standardized error response
func (h *HTTPSyncHandlers) writeError(w http.ResponseWriter, statusCode int, errorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	errorResponse := ErrorResponse{
		Error:   errorCode,
		Message: message,
	}
	json.NewEncoder(w).Encode(errorResponse)

	h.logger.Debug("HTTP error response",
		"status_code", statusCode,
		"error_code", errorCode,
		"message", message)
}

func (h *HTTPSyncHandlers) writePushConflict(w http.ResponseWriter, conflictErr *PushConflictError) {
	if conflictErr == nil {
		h.writeError(w, http.StatusConflict, "push_conflict", "push conflict")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)

	response := PushConflictResponse{
		Error:    "push_conflict",
		Message:  conflictErr.Error(),
		Conflict: conflictErr.Conflict,
	}
	_ = json.NewEncoder(w).Encode(response)

	h.logger.Debug("HTTP push conflict response",
		"status_code", http.StatusConflict,
		"error_code", "push_conflict",
		"message", conflictErr.Error())
}
