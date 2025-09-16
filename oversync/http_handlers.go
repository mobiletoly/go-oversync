// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
)

// ClientAuthenticator extracts both user and device identity from HTTP requests
// Implementations should validate auth (e.g., JWT) and provide both identifiers.
type ClientAuthenticator interface {
	GetUserID(r *http.Request) (string, error)
	GetSourceID(r *http.Request) (string, error)
}

// HTTPSyncHandlers provides HTTP handlers for the two-way sync API
type HTTPSyncHandlers struct {
	service       *SyncService
	authenticator ClientAuthenticator
	logger        *slog.Logger
}

// NewHTTPSyncHandlers creates a new instance of sync handlers
func NewHTTPSyncHandlers(service *SyncService, authenticator ClientAuthenticator, logger *slog.Logger) *HTTPSyncHandlers {
	return &HTTPSyncHandlers{
		service:       service,
		authenticator: authenticator,
		logger:        logger,
	}
}

// HandleUpload processes batch upload requests with conflict resolution
func (h *HTTPSyncHandlers) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST method is allowed")
		return
	}

	userID, err := h.authenticator.GetUserID(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	sourceID, err := h.authenticator.GetSourceID(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	// Parse upload request
	var uploadReq UploadRequest
	if err := json.NewDecoder(r.Body).Decode(&uploadReq); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "Failed to parse upload request")
		return
	}

	// Process the upload with user and device context
	response, err := h.service.ProcessUpload(r.Context(), userID, sourceID, &uploadReq)
	if err != nil {
		h.logger.Error("Failed to process upload", "error", err, "source_id", sourceID)
		h.writeError(w, http.StatusInternalServerError, "upload_failed", "Failed to process upload")
		return
	}

	//h.logger.Info("Successfully processed upload",
	//	"source_id", sourceID,
	//	"changes_count", len(uploadReq.Changes),
	//	"highest_server_seq", response.HighestServerSeq)

	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode upload response", "error", err, "source_id", sourceID)
	}
}

// HandleDownload processes download requests
func (h *HTTPSyncHandlers) HandleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}

	userID, err := h.authenticator.GetUserID(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	sourceID, err := h.authenticator.GetSourceID(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}

	// Parse and validate query parameters
	after := int64(0)
	if afterStr := r.URL.Query().Get("after"); afterStr != "" {
		parsedAfter, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after must be an integer")
			return
		}
		if parsedAfter < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after must be >= 0")
			return
		}
		after = parsedAfter
	}

	limit := 100
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "limit must be an integer")
			return
		}
		if parsedLimit < 1 || parsedLimit > 1000 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and 1000")
			return
		}
		limit = parsedLimit
	}

	// Get optional schema filter and validate if provided
	schemaFilter := r.URL.Query().Get("schema")
	if schemaFilter != "" {
		if !isValidSchemaName(schemaFilter) {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "invalid schema name")
			return
		}
	}

	// Get optional includeSelf flag (for client recovery scenarios)
	includeSelf := r.URL.Query().Get("include_self") == "true"

	// Optional frozen upper bound for paging window
	until := int64(0)
	if untilStr := r.URL.Query().Get("until"); untilStr != "" {
		v, err := strconv.ParseInt(untilStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "until must be an integer")
			return
		}
		if v < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "until must be >= 0")
			return
		}
		until = v
	}

	// Process the download with user context (windowed)
	response, err := h.service.ProcessDownloadWindowed(r.Context(), userID, sourceID, after, limit, schemaFilter, includeSelf, until)
	if err != nil {
		h.logger.Error("Failed to process download", "error", err, "user_id", userID, "source_id", sourceID)
		h.writeError(w, http.StatusInternalServerError, "download_failed", "Failed to process download")
		return
	}

	//h.logger.Debug("Successfully processed download",
	//	"user_id", userID,
	//	"source_id", sourceID,
	//	"after", after,
	//	"until", until,
	//	"limit", limit,
	//	"include_self", includeSelf,
	//	"changes_count", len(response.Changes),
	//	"has_more", response.HasMore)

	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode download response", "error", err, "user_id", userID, "source_id", sourceID)
	}
}

// HandleListFailures lists materializer failures for the authenticated user
func (h *HTTPSyncHandlers) HandleListFailures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}
	userID, err := h.authenticator.GetUserID(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}
	schema := r.URL.Query().Get("schema")
	table := r.URL.Query().Get("table")
	limit := 100
	if ls := r.URL.Query().Get("limit"); ls != "" {
		if v, e := strconv.Atoi(ls); e == nil && v > 0 {
			limit = v
		}
	}
	rows, err := h.service.ListMaterializeFailures(r.Context(), userID, schema, table, limit)
	if err != nil {
		h.logger.Error("List failures error", "error", err)
		h.writeError(w, http.StatusInternalServerError, "list_failures_failed", "Failed to list failures")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rows)
}

// HandleRetryFailure retries a single failure by id for the authenticated user
func (h *HTTPSyncHandlers) HandleRetryFailure(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only POST method is allowed")
		return
	}
	userID, err := h.authenticator.GetUserID(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error())
		return
	}
	// Accept id via query (?id=) or JSON body {"id": n}
	var id int64
	if qs := r.URL.Query().Get("id"); qs != "" {
		if v, e := strconv.ParseInt(qs, 10, 64); e == nil {
			id = v
		}
	}
	if id == 0 {
		var body struct {
			ID int64 `json:"id"`
		}
		if e := json.NewDecoder(r.Body).Decode(&body); e == nil {
			id = body.ID
		}
	}
	if id <= 0 {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "missing or invalid id")
		return
	}
	status, err := h.service.RetryMaterializeFailure(r.Context(), userID, id)
	if err != nil {
		h.logger.Error("Retry failure error", "error", err, "id", id)
		h.writeError(w, http.StatusInternalServerError, "retry_failed", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// HandleSchemaVersion returns the current schema version
func (h *HTTPSyncHandlers) HandleSchemaVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed")
		return
	}
	response := SchemaVersionResponse{
		Version: h.service.GetSchemaVersion(),
	}
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
