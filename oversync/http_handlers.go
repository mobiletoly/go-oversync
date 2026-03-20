// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

// HTTPSyncHandlers provides HTTP handlers for the two-way sync API
type HTTPSyncHandlers struct {
	service *SyncService
	logger  *slog.Logger
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
	return actor, nil
}

func (h *HTTPSyncHandlers) HandleCreatePushSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
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
		h.logger.Error("Failed to create push session", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
		h.writeError(w, http.StatusInternalServerError, "push_session_create_failed", "Failed to create push session")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode push session response", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
	}
}

func (h *HTTPSyncHandlers) HandlePushSessionChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}
	pushID := strings.TrimSpace(r.PathValue("push_id"))

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
		var forbiddenErr *PushSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "push_session_forbidden", forbiddenErr.Error())
			return
		}
		h.logger.Error("Failed to upload push chunk", "error", err, "user_id", actor.UserID, "push_id", pushID)
		h.writeError(w, http.StatusInternalServerError, "push_chunk_failed", "Failed to upload push chunk")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode push chunk response", "error", err, "user_id", actor.UserID, "push_id", pushID)
	}
}

func (h *HTTPSyncHandlers) HandleCommitPushSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}
	pushID := strings.TrimSpace(r.PathValue("push_id"))

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
			h.writeError(w, http.StatusConflict, "push_conflict", conflictErr.Error())
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
		h.logger.Error("Failed to commit push session", "error", err, "user_id", actor.UserID, "push_id", pushID)
		h.writeError(w, http.StatusInternalServerError, "push_session_commit_failed", "Failed to commit push session")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode push session commit response", "error", err, "user_id", actor.UserID, "push_id", pushID)
	}
}

func (h *HTTPSyncHandlers) HandleGetCommittedBundleRows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	bundleSeq, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("bundle_seq")), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "committed_bundle_chunk_invalid", "bundle_seq must be an integer")
		return
	}

	var afterRowOrdinal *int64
	if afterStr := r.URL.Query().Get("after_row_ordinal"); afterStr != "" {
		parsed, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "committed_bundle_chunk_invalid", "after_row_ordinal must be an integer")
			return
		}
		afterRowOrdinal = &parsed
	}

	maxRows := h.service.defaultRowsPerCommittedBundleChunk()
	if limitStr := r.URL.Query().Get("max_rows"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "committed_bundle_chunk_invalid", "max_rows must be an integer")
			return
		}
		maxRows = parsed
	}

	response, err := h.service.GetCommittedBundleRows(r.Context(), actor, bundleSeq, afterRowOrdinal, maxRows)
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
		var notFoundErr *CommittedBundleNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "committed_bundle_not_found", notFoundErr.Error())
			return
		}
		h.logger.Error("Failed to get committed bundle rows", "error", err, "user_id", actor.UserID, "bundle_seq", bundleSeq)
		h.writeError(w, http.StatusInternalServerError, "committed_bundle_rows_failed", "Failed to fetch committed bundle rows")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode committed bundle rows response", "error", err, "user_id", actor.UserID, "bundle_seq", bundleSeq)
	}
}

func (h *HTTPSyncHandlers) HandleDeletePushSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only DELETE method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}
	pushID := strings.TrimSpace(r.PathValue("push_id"))

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
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
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
		h.logger.Error("Failed to process pull", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
		h.writeError(w, http.StatusInternalServerError, "pull_failed", "Failed to process pull")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode pull response", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
	}
}

// HandleCreateSnapshotSession creates one frozen snapshot session for chunked hydrate/recover.
func (h *HTTPSyncHandlers) HandleCreateSnapshotSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	response, err := h.service.CreateSnapshotSession(r.Context(), actor)
	if err != nil {
		if errors.Is(err, errServiceShuttingDown) {
			h.writeError(w, http.StatusServiceUnavailable, "service_unavailable", "Sync service is shutting down")
			return
		}
		h.logger.Error("Failed to create snapshot session", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
		h.writeError(w, http.StatusInternalServerError, "snapshot_session_create_failed", "Failed to create snapshot session")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode snapshot session response", "error", err, "user_id", actor.UserID, "source_id", actor.SourceID)
	}
}

// HandleGetSnapshotChunk returns one chunk of rows from a frozen snapshot session.
func (h *HTTPSyncHandlers) HandleGetSnapshotChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	snapshotID := strings.TrimSpace(r.PathValue("snapshot_id"))
	afterRowOrdinal := int64(0)
	if afterStr := r.URL.Query().Get("after_row_ordinal"); afterStr != "" {
		parsed, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", "after_row_ordinal must be an integer")
			return
		}
		afterRowOrdinal = parsed
	}

	maxRows := h.service.defaultRowsPerSnapshotChunk()
	if limitStr := r.URL.Query().Get("max_rows"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", "max_rows must be an integer")
			return
		}
		maxRows = parsed
	}

	response, err := h.service.GetSnapshotChunk(r.Context(), actor, snapshotID, afterRowOrdinal, maxRows)
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

	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode snapshot chunk response", "error", err, "user_id", actor.UserID, "snapshot_id", snapshotID)
	}
}

// HandleDeleteSnapshotSession deletes an existing frozen snapshot session.
func (h *HTTPSyncHandlers) HandleDeleteSnapshotSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only DELETE method is allowed")
		return
	}

	actor, err := actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	snapshotID := strings.TrimSpace(r.PathValue("snapshot_id"))
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
