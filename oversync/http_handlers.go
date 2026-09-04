// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

// HTTPSyncHandlers provides HTTP handlers for the two-way sync API
type HTTPSyncHandlers struct {
	service                  *SyncService
	logger                   *slog.Logger
	bundleChangeWatchAllowed func(context.Context, Actor) bool
}

// HTTPSyncHandlersConfig configures HTTP-level sync handler policy.
type HTTPSyncHandlersConfig struct {
	// BundleChangeWatchAllowed optionally gates the SSE watch endpoint per actor/source.
	// Nil allows watch for every actor when the service-level watch feature is enabled.
	BundleChangeWatchAllowed func(context.Context, Actor) bool
}

type chunkQueryParams struct {
	afterRowOrdinal *int64
	maxRows         int
}

type boundedBodyError struct {
	code string
	err  error
}

func (e *boundedBodyError) Error() string { return e.err.Error() }

func decodeBoundedJSONRequest(r *http.Request, destination any, decodedLimit int64) error {
	decoded, err := readBoundedRequestBody(r, decodedLimit)
	if err != nil {
		return err
	}
	return decodeStrictJSONRequest(bytes.NewReader(decoded), destination, true)
}

func readBoundedRequestBody(r *http.Request, decodedLimit int64) ([]byte, error) {
	if decodedLimit < 1 {
		return nil, &boundedBodyError{code: "decoded_body_too_large", err: errors.New("decoded body limit is invalid")}
	}
	encoding, err := validateRequestContentEncoding(r)
	if err != nil {
		return nil, err
	}
	encoded, err := io.ReadAll(io.LimitReader(r.Body, decodedLimit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) > decodedLimit {
		return nil, &boundedBodyError{code: "encoded_body_too_large", err: errors.New("encoded request body exceeds limit")}
	}
	decoded := encoded
	if encoding == "gzip" {
		reader := bytes.NewReader(encoded)
		zipped, gzipErr := gzip.NewReader(reader)
		if gzipErr != nil {
			return nil, &boundedBodyError{code: "invalid_gzip", err: gzipErr}
		}
		zipped.Multistream(false)
		decoded, err = io.ReadAll(io.LimitReader(zipped, decodedLimit+1))
		closeErr := zipped.Close()
		if err != nil || closeErr != nil {
			if err == nil {
				err = closeErr
			}
			return nil, &boundedBodyError{code: "invalid_gzip", err: err}
		}
		if int64(len(decoded)) > decodedLimit {
			return nil, &boundedBodyError{code: "decoded_body_too_large", err: errors.New("decoded request body exceeds limit")}
		}
		if reader.Len() != 0 {
			return nil, &boundedBodyError{code: "invalid_gzip", err: errors.New("gzip request contains trailing data")}
		}
	}
	if int64(len(decoded)) > decodedLimit {
		return nil, &boundedBodyError{code: "decoded_body_too_large", err: errors.New("decoded request body exceeds limit")}
	}
	return decoded, nil
}

func validateRequestContentEncoding(r *http.Request) (string, error) {
	values := exactHeaderValues(r.Header, "Content-Encoding")
	if len(values) == 0 {
		return "identity", nil
	}
	if len(values) != 1 || values[0] == "" || strings.Contains(values[0], ",") {
		return "", &boundedBodyError{code: "unsupported_content_encoding", err: errors.New("Content-Encoding must be absent, identity, or gzip")}
	}
	switch {
	case strings.EqualFold(values[0], "identity"):
		return "identity", nil
	case strings.EqualFold(values[0], "gzip"):
		return "gzip", nil
	default:
		return "", &boundedBodyError{code: "unsupported_content_encoding", err: errors.New("Content-Encoding must be absent, identity, or gzip")}
	}
}

func (h *HTTPSyncHandlers) writeBoundedRequestFailure(w http.ResponseWriter, r *http.Request, err error, invalidMessage string) {
	var bodyErr *boundedBodyError
	if !errors.As(err, &bodyErr) {
		h.writeError(w, http.StatusBadRequest, "invalid_request", invalidMessage, r)
		return
	}
	status := http.StatusBadRequest
	switch bodyErr.code {
	case "unsupported_media_type", "unsupported_content_encoding":
		status = http.StatusUnsupportedMediaType
	case "encoded_body_too_large", "decoded_body_too_large":
		status = http.StatusRequestEntityTooLarge
	}
	h.writeError(w, status, bodyErr.code, bodyErr.Error(), r)
}

func validateJSONRequestContentType(r *http.Request, allowAbsent bool) error {
	values := exactHeaderValues(r.Header, "Content-Type")
	if len(values) == 0 && allowAbsent {
		return nil
	}
	if len(values) != 1 {
		return &boundedBodyError{code: "unsupported_media_type", err: errors.New("Content-Type must be exactly application/json")}
	}
	mediaType, params, err := mime.ParseMediaType(values[0])
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return &boundedBodyError{code: "unsupported_media_type", err: errors.New("Content-Type must be application/json")}
	}
	for name, value := range params {
		if !strings.EqualFold(name, "charset") || !strings.EqualFold(value, "utf-8") {
			return &boundedBodyError{code: "unsupported_media_type", err: errors.New("application/json parameters must describe UTF-8")}
		}
	}
	return nil
}

func rejectUnknownQueryParams(r *http.Request, allowed ...string) error {
	allow := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allow[name] = struct{}{}
	}
	for name := range r.URL.Query() {
		if _, ok := allow[name]; !ok {
			return fmt.Errorf("unknown query parameter %q", name)
		}
	}
	return nil
}

func optionalScalarQueryParam(r *http.Request, name string) (string, bool, error) {
	values, present := r.URL.Query()[name]
	if !present {
		return "", false, nil
	}
	if len(values) != 1 || values[0] == "" {
		return "", true, fmt.Errorf("%s must be provided exactly once with a non-empty value", name)
	}
	return values[0], true, nil
}

func decodeStrictJSONRequest(body io.Reader, destination any, disallowUnknownFields bool) error {
	raw, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if err := jcs.ValidateStructure(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if disallowUnknownFields {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request contains more than one JSON value")
		}
		return err
	}
	return nil
}

// NewHTTPSyncHandlers creates a new instance of sync handlers
func NewHTTPSyncHandlers(service *SyncService, logger *slog.Logger) *HTTPSyncHandlers {
	return NewHTTPSyncHandlersWithConfig(service, logger, HTTPSyncHandlersConfig{})
}

// NewHTTPSyncHandlersWithConfig creates a new instance of sync handlers with HTTP policy hooks.
func NewHTTPSyncHandlersWithConfig(service *SyncService, logger *slog.Logger, config HTTPSyncHandlersConfig) *HTTPSyncHandlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &HTTPSyncHandlers{
		service:                  service,
		logger:                   logger,
		bundleChangeWatchAllowed: config.BundleChangeWatchAllowed,
	}
}

func (h *HTTPSyncHandlers) actorFromRequest(r *http.Request) (Actor, error) {
	actor, ok := ActorFromContext(r.Context())
	if !ok {
		return Actor{}, errors.New("authenticated actor not found in request context")
	}
	if err := h.service.validateClientActor(actor, true); err != nil {
		return Actor{}, err
	}
	return actor, nil
}

func (h *HTTPSyncHandlers) requireActorForMethod(w http.ResponseWriter, r *http.Request, method string) (Actor, bool) {
	if r.Method != method {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only "+method+" method is allowed", r)
		return Actor{}, false
	}

	actor, err := h.actorFromRequest(r)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, "authentication_failed", err.Error(), r)
		return Actor{}, false
	}

	return actor, true
}

func exactHeaderValues(header http.Header, name string) []string {
	var values []string
	for key, entries := range header {
		if strings.EqualFold(key, name) {
			values = append(values, entries...)
		}
	}
	return values
}

func parseSchemaGenerationProofHeaders(header http.Header) (SchemaGenerationProof, error) {
	idValues := exactHeaderValues(header, SchemaGenerationIDHeader)
	if len(idValues) == 0 {
		return SchemaGenerationProof{}, &SchemaGenerationRequestError{Code: SchemaGenerationMissing, Field: "schema_generation_id", Message: "schema generation is required"}
	}
	if len(idValues) != 1 {
		return SchemaGenerationProof{}, &SchemaGenerationRequestError{Code: SchemaGenerationIDInvalid, Field: "schema_generation_id", Message: "schema generation ID header is invalid"}
	}
	id, err := parseSchemaGenerationID(idValues[0])
	if err != nil {
		return SchemaGenerationProof{}, err
	}
	fingerprintValues := exactHeaderValues(header, SchemaGenerationFingerprintHeader)
	if len(fingerprintValues) == 0 {
		return SchemaGenerationProof{}, &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMissing, Field: "schema_generation_fingerprint", GenerationID: id, Message: "schema generation fingerprint is required"}
	}
	if len(fingerprintValues) != 1 || !isLowerHexFingerprint(fingerprintValues[0]) {
		return SchemaGenerationProof{}, &SchemaGenerationRequestError{Code: SchemaGenerationFingerprintMalformed, Field: "schema_generation_fingerprint", GenerationID: id, Message: "schema generation fingerprint is malformed"}
	}
	return SchemaGenerationProof{ID: id, Fingerprint: fingerprintValues[0]}, nil
}

func (h *HTTPSyncHandlers) requireActorAndProofForMethod(w http.ResponseWriter, r *http.Request, method string) (Actor, SchemaGenerationProof, bool) {
	actor, ok := h.requireActorForMethod(w, r, method)
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
	// Resolve the configured pair before endpoint-specific path, query, or body
	// parsing. Each direct operation repeats resolution inside its lifecycle
	// boundary and then applies its binding or session rule.
	if _, err := h.service.resolveRequestGeneration(r.Context(), actor, proof); err != nil {
		if !h.writeGenerationError(w, r, err) {
			h.writeError(w, http.StatusInternalServerError, "schema_generation_resolution_failed", "Failed to resolve schema generation", r)
		}
		return Actor{}, SchemaGenerationProof{}, false
	}
	return actor, proof, true
}

func (h *HTTPSyncHandlers) writeGenerationError(w http.ResponseWriter, r *http.Request, err error) bool {
	code, message := "", "schema generation request failed"
	status, remediation := http.StatusConflict, "upgrade_or_reconfigure_client"
	var requestErr *SchemaGenerationRequestError
	var sourceErr *SourceGenerationError
	var sessionErr *SessionGenerationError
	var committedErr *CommittedPushStateError
	var projectionErr *GenerationProjectionError
	var coverageErr *PushEffectCoverageError
	switch {
	case errors.As(err, &requestErr):
		code, message = string(requestErr.Code), requestErr.Message
		switch requestErr.Code {
		case SchemaGenerationMissing:
			status, remediation = http.StatusBadRequest, "provide_schema_generation"
		case SchemaGenerationIDInvalid:
			status, remediation = http.StatusBadRequest, "correct_schema_generation_id"
		case SchemaGenerationFingerprintMissing:
			status, remediation = http.StatusBadRequest, "provide_schema_generation_fingerprint"
		case SchemaGenerationFingerprintMalformed:
			status, remediation = http.StatusBadRequest, "correct_schema_generation_fingerprint"
		case SchemaGenerationUnsupported:
			status = http.StatusGone
		case SchemaGenerationRetired:
			status, remediation = http.StatusUpgradeRequired, "upgrade_application"
		}
	case errors.As(err, &sourceErr):
		code, message = string(sourceErr.Code), sourceErr.Message
		switch sourceErr.Code {
		case SourceGenerationUnbound:
			remediation = "connect_source"
		case SourceGenerationConflict:
			remediation = "rotate_source"
		case SchemaGenerationDowngradeForbidden:
			remediation = "keep_or_upgrade_generation"
		case SourceGenerationStateCorrupt:
			status, remediation = http.StatusInternalServerError, "contact_operator"
		case SourceGenerationBindingLimitExceeded:
			remediation = "contact_operator"
		}
	case errors.As(err, &sessionErr):
		code, message = string(sessionErr.Code), sessionErr.Message
		if sessionErr.Code == SessionGenerationStateCorrupt {
			status, remediation = http.StatusInternalServerError, "contact_operator"
		} else {
			remediation = "restart_session_with_matching_generation"
		}
	case errors.As(err, &committedErr):
		code, message, status, remediation = string(committedErr.Code), committedErr.Message, http.StatusInternalServerError, "contact_operator"
	case errors.As(err, &coverageErr):
		code, message, status, remediation = "push_effect_coverage_incomplete", coverageErr.Error(), http.StatusUnprocessableEntity, "correct_server_write_logic"
	case errors.As(err, &projectionErr):
		code, message = string(projectionErr.Code), projectionErr.Error()
		switch projectionErr.Code {
		case GenerationHistoryUnavailable:
			status, remediation = http.StatusConflict, "rebuild_from_snapshot"
		case BundleLimitExceeded:
			status, remediation = http.StatusRequestEntityTooLarge, "reduce_request"
		case HistoryProjectionCapacityExceeded:
			w.Header().Set("Retry-After", "1")
			status, remediation = http.StatusServiceUnavailable, "retry"
			h.writeJSONStatus(w, r, status, GenerationErrorResponse{Error: code, Message: message, Retryable: true, Remediation: remediation})
			return true
		default:
			status, remediation = http.StatusInternalServerError, "contact_operator"
		}
	default:
		return false
	}
	h.writeJSONStatus(w, r, status, GenerationErrorResponse{Error: code, Message: message, Retryable: false, Remediation: remediation})
	return true
}

func (h *HTTPSyncHandlers) writeGenerationOrSourceLifecycleError(w http.ResponseWriter, r *http.Request, sourceID string, err error) bool {
	if h.writeGenerationError(w, r, err) {
		return true
	}
	var retiredErr *SourceRetiredError
	if errors.As(err, &retiredErr) {
		h.writeSourceRetired(w, r, sourceID, retiredErr)
		return true
	}
	return false
}

func (h *HTTPSyncHandlers) writeJSONStatus(w http.ResponseWriter, r *http.Request, status int, response any) {
	h.writeBoundedJSON(w, r, status, response, generationControlResponseLimit, "Failed to encode bounded error response")
}

func (h *HTTPSyncHandlers) isBundleChangeWatchAllowed(ctx context.Context, actor Actor) bool {
	if h == nil || h.bundleChangeWatchAllowed == nil {
		return true
	}
	return h.bundleChangeWatchAllowed(ctx, actor)
}

func parseChunkQueryParams(r *http.Request, defaultMaxRows int) (chunkQueryParams, error) {
	params := chunkQueryParams{maxRows: defaultMaxRows}

	afterStr, present, err := optionalScalarQueryParam(r, "after_row_ordinal")
	if err != nil {
		return chunkQueryParams{}, err
	}
	if present {
		parsed, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			return chunkQueryParams{}, errors.New("after_row_ordinal must be an integer")
		}
		params.afterRowOrdinal = &parsed
	}

	limitStr, present, err := optionalScalarQueryParam(r, "max_rows")
	if err != nil {
		return chunkQueryParams{}, err
	}
	if present {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil {
			return chunkQueryParams{}, errors.New("max_rows must be an integer")
		}
		params.maxRows = parsed
	}

	return params, nil
}

func (h *HTTPSyncHandlers) writeJSON(w http.ResponseWriter, response any, encodeErrorMessage string) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error(encodeErrorMessage, "error_type", fmt.Sprintf("%T", err))
	}
}

func acceptsGzip(r *http.Request) bool {
	explicit := false
	explicitAccepted := false
	wildcardAccepted := false
	for _, value := range exactHeaderValues(r.Header, "Accept-Encoding") {
		for _, token := range strings.Split(value, ",") {
			parts := strings.Split(token, ";")
			name := strings.ToLower(strings.TrimSpace(parts[0]))
			quality := 1.0
			for _, parameter := range parts[1:] {
				pair := strings.SplitN(strings.TrimSpace(parameter), "=", 2)
				if len(pair) == 2 && strings.EqualFold(pair[0], "q") {
					parsed, err := strconv.ParseFloat(pair[1], 64)
					if err != nil || parsed < 0 || parsed > 1 {
						quality = 0
					} else {
						quality = parsed
					}
				}
			}
			switch name {
			case "gzip":
				explicit = true
				explicitAccepted = quality > 0
			case "*":
				wildcardAccepted = quality > 0
			}
		}
	}
	if explicit {
		return explicitAccepted
	}
	return wildcardAccepted
}

func (h *HTTPSyncHandlers) writeBoundedJSON(w http.ResponseWriter, r *http.Request, status int, response any, limit int64, encodeErrorMessage string) {
	body, err := json.Marshal(response)
	if err != nil || int64(len(body)) > limit {
		h.logger.Error(encodeErrorMessage, "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "projection_response_encoding_failed", "Failed to encode bounded response", r)
		return
	}
	h.writeJSONBytes(w, r, status, body)
}

func (h *HTTPSyncHandlers) writeJSONBytes(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	if r != nil && len(body) >= 2048 && acceptsGzip(r) {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		writeErr := func() error {
			if _, err := writer.Write(body); err != nil {
				return err
			}
			return writer.Close()
		}()
		if writeErr == nil && compressed.Len() < len(body) {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
			w.WriteHeader(status)
			_, _ = w.Write(compressed.Bytes())
			return
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (h *HTTPSyncHandlers) writeServiceUnavailable(w http.ResponseWriter, r *http.Request, err error) bool {
	if !errors.Is(err, errServiceShuttingDown) && !errors.Is(err, errServiceNotReady) {
		return false
	}
	h.writeError(w, http.StatusServiceUnavailable, "service_not_ready", "Sync service is unavailable; retry later", r)
	return true
}

func (h *HTTPSyncHandlers) writePushTransactionFailure(w http.ResponseWriter, r *http.Request, err error) bool {
	var applicationErr *ApplicationWriteRejectedError
	if errors.As(err, &applicationErr) {
		h.writeError(w, http.StatusUnprocessableEntity, applicationErr.Category, "Application rejected the write; resolve the pending intent before retrying", r)
		return true
	}
	var unknownErr *CommitOutcomeUnknownError
	if errors.As(err, &unknownErr) {
		h.writeError(
			w,
			http.StatusServiceUnavailable,
			"commit_outcome_unknown",
			"Push transaction commit outcome is unknown; retry the same complete push with the same source tuple",
			r,
		)
		return true
	}
	return h.writeServiceUnavailable(w, r, err)
}

// HandlePush receives one bounded compact v1 bundle. The compatibility-free
// public response is emitted only after the whole authoritative effect is
// available. The legacy staging implementation is removed in P2; this handler
// owns the new HTTP boundary and retained logical validation now.
func (h *HTTPSyncHandlers) HandlePush(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}
	if err := validateJSONRequestContentType(r, false); err != nil {
		h.writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error(), r)
		return
	}
	var request CompactPushRequest
	if err := decodeBoundedJSONRequest(r, &request, int64(h.service.config.MaxBytesPerBundle)+historyEnvelopeAllowance); err != nil {
		h.writeBoundedRequestFailure(w, r, err, "Failed to parse push envelope")
		return
	}
	view, err := h.service.resolveRequestGeneration(r.Context(), actor, proof)
	if err != nil {
		h.writeGenerationError(w, r, err)
		return
	}
	rows, err := ExpandCompactPushRows(generationDescriptor(view.contract), request.Rows)
	if err != nil || request.SourceBundleID < 1 {
		if err == nil {
			err = errors.New("source_bundle_id must be positive")
		}
		h.writeError(w, http.StatusBadRequest, "push_invalid", err.Error(), r)
		return
	}
	requestHash, err := computeGenerationPinnedPushRequestHash(proof, rows)
	if err != nil || requestHash != request.CanonicalRequestHash {
		h.writeError(w, http.StatusBadRequest, "push_invalid", "canonical_request_hash does not match compact logical rows", r)
		return
	}

	pushed, err := h.service.PushOneShot(retainHistoryProjectionForHTTP(r.Context()), actor, proof, &OneShotPushRequest{
		SourceBundleID:       request.SourceBundleID,
		CanonicalRequestHash: request.CanonicalRequestHash,
		InitializationID:     request.InitializationID,
		Rows:                 rows,
	})
	if err != nil {
		h.writeCompactPushError(w, r, actor.SourceID, err)
		return
	}
	defer releaseHistoryProjection(&pushed.historyProjectionRelease)
	authoritative := make([]PushRequestRow, 0, len(pushed.Bundle.Rows))
	for _, row := range pushed.Bundle.Rows {
		authoritative = append(authoritative, PushRequestRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, BaseRowVersion: row.RowVersion, Payload: row.Payload})
	}
	groups, err := CompactGroupsForRows(generationDescriptor(view.contract), authoritative)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "push_failed", "Failed to encode authoritative compact rows", r)
		return
	}
	h.writeBoundedJSON(w, r, http.StatusOK, CompactPushResponse{BundleSeq: pushed.Bundle.BundleSeq, BundleHash: pushed.Bundle.BundleHash, Rows: groups}, int64(h.service.config.MaxBytesPerBundle)+historyEnvelopeAllowance, "Failed to encode compact push response")
}

func (h *HTTPSyncHandlers) writeCompactPushError(w http.ResponseWriter, r *http.Request, sourceID string, err error) {
	var replayMismatchErr *PushReplayMismatchError
	if errors.As(err, &replayMismatchErr) {
		h.writeError(w, http.StatusConflict, "push_replay_mismatch", replayMismatchErr.Error(), r)
		return
	}
	var projectionErr *GenerationProjectionError
	if errors.As(err, &projectionErr) && projectionErr.Code == BundleProjectionTooLarge {
		h.writeError(w, http.StatusUnprocessableEntity, "bundle_projection_too_large", projectionErr.Error(), r)
		return
	}
	if errors.As(err, &projectionErr) && projectionErr.Code == BundleLimitExceeded && projectionErr.Operation == "push" {
		h.writeError(w, http.StatusBadRequest, "push_invalid", projectionErr.Error(), r)
		return
	}
	if h.writeGenerationOrSourceLifecycleError(w, r, sourceID, err) || h.writePushTransactionFailure(w, r, err) {
		return
	}
	var conflictErr *PushConflictError
	if errors.As(err, &conflictErr) {
		h.writePushConflict(w, r, conflictErr)
		return
	}
	var invalidErr *PushInvalidError
	var validationErr *PushValidationError
	switch {
	case errors.As(err, &invalidErr), errors.As(err, &validationErr):
		h.writeError(w, http.StatusBadRequest, "push_invalid", err.Error(), r)
	case errors.As(err, new(*SourceTupleHistoryPrunedError)):
		h.writeError(w, http.StatusConflict, "history_pruned", err.Error(), r)
	case errors.As(err, new(*SourceSequenceOutOfOrderError)):
		h.writeError(w, http.StatusConflict, "source_sequence_out_of_order", err.Error(), r)
	case errors.As(err, new(*SourceSequenceChangedError)):
		h.writeError(w, http.StatusConflict, "source_sequence_changed", err.Error(), r)
	case errors.As(err, new(*ScopeUninitializedError)):
		h.writeError(w, http.StatusConflict, "scope_uninitialized", err.Error(), r)
	case errors.As(err, new(*ScopeInitializingError)):
		h.writeError(w, http.StatusConflict, "scope_initializing", err.Error(), r)
	case errors.As(err, new(*InitializationStaleError)):
		h.writeError(w, http.StatusConflict, "initialization_stale", err.Error(), r)
	case errors.As(err, new(*InitializationExpiredError)):
		h.writeError(w, http.StatusGone, "initialization_expired", err.Error(), r)
	default:
		h.logger.Error("Failed to process compact push", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "push_failed", "Failed to process push", r)
	}
}

func (h *HTTPSyncHandlers) HandleConnect(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}

	if err := validateJSONRequestContentType(r, false); err != nil {
		h.writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error(), r)
		return
	}
	var req ConnectRequest
	if err := decodeBoundedJSONRequest(r, &req, generationControlResponseLimit); err != nil {
		h.writeBoundedRequestFailure(w, r, err, "Failed to parse connect request")
		return
	}

	response, err := h.service.Connect(r.Context(), actor, proof, &req)
	if err != nil {
		if h.writeGenerationOrSourceLifecycleError(w, r, actor.SourceID, err) {
			return
		}
		if h.writeServiceUnavailable(w, r, err) {
			return
		}
		var invalidErr *ConnectInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "connect_invalid", invalidErr.Error(), r)
			return
		}
		h.logger.Error("Failed to resolve connect lifecycle", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "connect_failed", "Failed to resolve connect lifecycle", r)
		return
	}

	h.writeBoundedJSON(w, r, http.StatusOK, response, generationControlResponseLimit, "Failed to encode connect response")
}

// HandlePull processes bundle pull requests.
func (h *HTTPSyncHandlers) HandlePull(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}
	if err := rejectUnknownQueryParams(r, "after_bundle_seq", "max_bundles", "target_bundle_seq"); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}

	afterBundleSeq := int64(0)
	afterStr, present, err := optionalScalarQueryParam(r, "after_bundle_seq")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}
	if present {
		parsedAfter, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after_bundle_seq must be an integer", r)
			return
		}
		if parsedAfter < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after_bundle_seq must be >= 0", r)
			return
		}
		afterBundleSeq = parsedAfter
	}

	maxBundles := defaultPullBundlesPerRequest
	limitStr, present, err := optionalScalarQueryParam(r, "max_bundles")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}
	if present {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "max_bundles must be an integer", r)
			return
		}
		if parsedLimit < 1 || parsedLimit > defaultMaxBundlesPerPull {
			h.writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("max_bundles must be between 1 and %d", defaultMaxBundlesPerPull), r)
			return
		}
		maxBundles = parsedLimit
	}

	targetBundleSeq := int64(0)
	targetStr, present, err := optionalScalarQueryParam(r, "target_bundle_seq")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}
	if present {
		parsedTarget, err := strconv.ParseInt(targetStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "target_bundle_seq must be an integer", r)
			return
		}
		if parsedTarget < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "target_bundle_seq must be >= 0", r)
			return
		}
		targetBundleSeq = parsedTarget
	}

	response, err := h.service.ProcessPull(retainHistoryProjectionForHTTP(r.Context()), actor, proof, afterBundleSeq, maxBundles, targetBundleSeq)
	if err != nil {
		if h.writeGenerationOrSourceLifecycleError(w, r, actor.SourceID, err) {
			return
		}
		if h.writeServiceUnavailable(w, r, err) {
			return
		}
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			h.writeError(w, http.StatusConflict, "history_pruned", prunedErr.Error(), r)
			return
		}
		var aheadErr *CheckpointAheadError
		if errors.As(err, &aheadErr) {
			h.writeError(w, http.StatusConflict, "checkpoint_ahead", aheadErr.Error(), r)
			return
		}
		var invalidErr *InvalidPullRequestError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "invalid_request", invalidErr.Error(), r)
			return
		}
		var unrepresentableErr *CommittedBundleUnrepresentableError
		if errors.As(err, &unrepresentableErr) {
			h.writeError(w, http.StatusInternalServerError, "committed_bundle_unrepresentable", unrepresentableErr.Error(), r)
			return
		}
		var uninitializedErr *ScopeUninitializedError
		if errors.As(err, &uninitializedErr) {
			h.writeError(w, http.StatusConflict, "scope_uninitialized", uninitializedErr.Error(), r)
			return
		}
		var initializingErr *ScopeInitializingError
		if errors.As(err, &initializingErr) {
			h.writeError(w, http.StatusConflict, "scope_initializing", initializingErr.Error(), r)
			return
		}
		h.logger.Error("Failed to process pull", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "pull_failed", "Failed to process pull", r)
		return
	}
	defer releaseHistoryProjection(&response.historyProjectionRelease)
	view, err := h.service.resolveRequestGeneration(r.Context(), actor, proof)
	if err != nil {
		h.writeGenerationError(w, r, err)
		return
	}
	compact := CompactPullResponse{StableBundleSeq: response.StableBundleSeq, HasMore: response.HasMore, Bundles: make([]CompactBundle, 0, len(response.Bundles))}
	for _, bundle := range response.Bundles {
		rows, encodeErr := CompactGroupsForBundleRows(generationDescriptor(view.contract), bundle.Rows)
		if encodeErr != nil {
			h.writeError(w, http.StatusInternalServerError, "committed_bundle_unrepresentable", "Committed bundle cannot be represented as compact rows", r)
			return
		}
		compact.Bundles = append(compact.Bundles, CompactBundle{
			BundleSeq: bundle.BundleSeq, SourceID: bundle.SourceID, SourceBundleID: bundle.SourceBundleID,
			BundleHash: bundle.BundleHash, Rows: rows,
		})
	}
	h.writeBoundedJSON(w, r, http.StatusOK, compact, h.service.config.MaxBytesPerPullResponse, "Failed to encode compact pull response")
}

// HandleWatch streams metadata-only bundle change wakeups as Server-Sent Events.
func (h *HTTPSyncHandlers) HandleWatch(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}
	if err := rejectUnknownQueryParams(r, "after_bundle_seq"); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}
	if h.service != nil && h.service.bundleChangeWatchEnabled() && !h.isBundleChangeWatchAllowed(r.Context(), actor) {
		h.writeError(w, http.StatusForbidden, "bundle_change_watch_forbidden", "Bundle change watch is not enabled for this client", r)
		return
	}

	afterBundleSeq := int64(0)
	afterStr, present, err := optionalScalarQueryParam(r, "after_bundle_seq")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}
	if present {
		parsedAfter, err := strconv.ParseInt(afterStr, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after_bundle_seq must be an integer", r)
			return
		}
		if parsedAfter < 0 {
			h.writeError(w, http.StatusBadRequest, "invalid_request", "after_bundle_seq must be >= 0", r)
			return
		}
		afterBundleSeq = parsedAfter
	}

	subCtx, cancel := context.WithCancel(r.Context())
	defer cancel()
	events, err := h.service.SubscribeBundleChanges(subCtx, actor, proof, afterBundleSeq)
	if err != nil {
		h.writeWatchSetupError(w, r, err, actor)
		return
	}

	cfg := h.service.effectiveBundleChangeWatchConfig()
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultBundleChangeHeartbeatInterval
	}

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := flushSSE(w); err != nil {
		return
	}

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case event, open := <-events:
			if !open {
				return
			}
			if r.Context().Err() != nil {
				return
			}
			if err := writeSSEBundleEvent(w, event); err != nil {
				return
			}
			if err := flushSSE(w); err != nil {
				return
			}
		case <-heartbeat.C:
			if r.Context().Err() != nil {
				return
			}
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			if err := flushSSE(w); err != nil {
				return
			}
		}
	}
}

func (h *HTTPSyncHandlers) writeWatchSetupError(w http.ResponseWriter, r *http.Request, err error, actor Actor) {
	if h.writeGenerationOrSourceLifecycleError(w, r, actor.SourceID, err) {
		return
	}
	if errors.Is(err, errBundleChangeWatchDisabled) {
		h.writeError(w, http.StatusServiceUnavailable, "bundle_change_watch_disabled", "Bundle change watch is disabled", r)
		return
	}
	if h.writeServiceUnavailable(w, r, err) {
		return
	}
	var uninitializedErr *ScopeUninitializedError
	if errors.As(err, &uninitializedErr) {
		h.writeError(w, http.StatusConflict, "scope_uninitialized", uninitializedErr.Error(), r)
		return
	}
	var initializingErr *ScopeInitializingError
	if errors.As(err, &initializingErr) {
		h.writeError(w, http.StatusConflict, "scope_initializing", initializingErr.Error(), r)
		return
	}
	h.logger.Error("Failed to subscribe bundle change watch", "error_type", fmt.Sprintf("%T", err))
	h.writeError(w, http.StatusInternalServerError, "bundle_change_watch_failed", "Failed to subscribe bundle change watch", r)
}

func writeSSEBundleEvent(w io.Writer, event BundleChangeEvent) error {
	payload, err := json.Marshal(struct {
		BundleSeq int64 `json:"bundle_seq"`
	}{BundleSeq: event.BundleSeq})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: bundle\ndata: %s\n\n", payload); err != nil {
		return err
	}
	return nil
}

func flushSSE(w http.ResponseWriter) error {
	if err := http.NewResponseController(w).Flush(); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
	return nil
}

// HandleCreateSnapshotSession creates one frozen snapshot session for chunked hydrate/recover.
func (h *HTTPSyncHandlers) HandleCreateSnapshotSession(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodPost)
	if !ok {
		return
	}
	if err := rejectUnknownQueryParams(r); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}
	if err := validateJSONRequestContentType(r, r.ContentLength == 0); err != nil {
		h.writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error(), r)
		return
	}

	var req *SnapshotSessionCreateRequest
	if r.Body != nil {
		defer r.Body.Close()
		raw, err := readBoundedRequestBody(r, generationControlResponseLimit)
		if err != nil {
			h.writeBoundedRequestFailure(w, r, err, "Failed to parse snapshot session request")
			return
		}
		if len(bytes.TrimSpace(raw)) > 0 {
			var parsed SnapshotSessionCreateRequest
			if err := decodeStrictJSONRequest(bytes.NewReader(raw), &parsed, true); err != nil {
				h.writeError(w, http.StatusBadRequest, "invalid_request", "Failed to parse snapshot session request", r)
				return
			}
			req = &parsed
		}
	}

	response, err := h.service.CreateSnapshotSessionWithRequest(r.Context(), actor, proof, req)
	if err != nil {
		if h.writeGenerationOrSourceLifecycleError(w, r, actor.SourceID, err) {
			return
		}
		if h.writeServiceUnavailable(w, r, err) {
			return
		}
		var invalidErr *SnapshotSessionInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "snapshot_session_invalid", invalidErr.Error(), r)
			return
		}
		var capacityErr *SnapshotCapacityError
		if errors.As(err, &capacityErr) {
			w.Header().Set("Retry-After", "1")
			h.writeError(w, http.StatusTooManyRequests, "snapshot_build_capacity", capacityErr.Error(), r)
			return
		}
		var limitErr *SnapshotSessionLimitExceededError
		if errors.As(err, &limitErr) {
			h.writeSnapshotSessionLimitExceeded(w, r, limitErr)
			return
		}
		var replacementErr *SourceReplacementInvalidError
		if errors.As(err, &replacementErr) {
			h.writeError(w, http.StatusConflict, "source_replacement_invalid", replacementErr.Error(), r)
			return
		}
		var uninitializedErr *ScopeUninitializedError
		if errors.As(err, &uninitializedErr) {
			h.writeError(w, http.StatusConflict, "scope_uninitialized", uninitializedErr.Error(), r)
			return
		}
		var initializingErr *ScopeInitializingError
		if errors.As(err, &initializingErr) {
			h.writeError(w, http.StatusConflict, "scope_initializing", initializingErr.Error(), r)
			return
		}
		h.logger.Error("Failed to create snapshot session", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "snapshot_session_create_failed", "Failed to create snapshot session", r)
		return
	}

	h.writeBoundedJSON(w, r, http.StatusOK, CompactSnapshotSessionResponse{
		SnapshotID:        response.SnapshotID,
		SnapshotBundleSeq: response.SnapshotBundleSeq,
		RowCount:          response.RowCount,
		LogicalByteCount:  response.ByteCount,
		ExpiresAt:         response.ExpiresAt,
	}, h.service.maxBytesPerSnapshotChunk(), "Failed to encode compact snapshot session response")
}

func (h *HTTPSyncHandlers) writeSnapshotSessionLimitExceeded(w http.ResponseWriter, r *http.Request, limitErr *SnapshotSessionLimitExceededError) {
	h.writeBoundedJSON(w, r, http.StatusConflict, SnapshotSessionLimitResponse{
		Error:     "snapshot_session_limit_exceeded",
		Message:   limitErr.Error(),
		Dimension: limitErr.Dimension,
		Actual:    limitErr.Actual,
		Limit:     limitErr.Limit,
	}, generationControlResponseLimit, "Failed to encode snapshot session limit response")
}

func (h *HTTPSyncHandlers) writeSourceRetired(w http.ResponseWriter, r *http.Request, requestSourceID string, retiredErr *SourceRetiredError) {
	if retiredErr == nil || sourceid.Validate(requestSourceID) != nil ||
		sourceid.Validate(retiredErr.SourceID) != nil || retiredErr.SourceID != requestSourceID ||
		sourceid.ValidateOptional(retiredErr.ReplacedBySourceID) != nil {
		h.logger.Error("Rejected invalid source retired state")
		h.writeError(w, http.StatusInternalServerError, "source_retired_invalid", "Source retirement state is invalid", r)
		return
	}

	response := SourceRetiredResponse{
		Error:              "source_retired",
		Message:            "source is retired",
		SourceID:           requestSourceID,
		ReplacedBySourceID: retiredErr.ReplacedBySourceID,
	}
	h.writeBoundedJSON(w, r, http.StatusConflict, response, generationControlResponseLimit, "Failed to encode source retired response")

	h.logger.Debug("HTTP source retired response",
		"status_code", http.StatusConflict,
		"error_code", "source_retired",
	)
}

// HandleGetSnapshotChunk returns one chunk of rows from a frozen snapshot session.
func (h *HTTPSyncHandlers) HandleGetSnapshotChunk(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodGet)
	if !ok {
		return
	}
	if err := rejectUnknownQueryParams(r, "after_row_ordinal", "max_rows", "max_bytes"); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}

	snapshotID := r.PathValue("snapshot_id")
	afterRowOrdinal := int64(0)
	queryParams, err := parseChunkQueryParams(r, h.service.defaultRowsPerSnapshotChunk())
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", err.Error(), r)
		return
	}
	if queryParams.afterRowOrdinal != nil {
		afterRowOrdinal = *queryParams.afterRowOrdinal
	}
	maxBytes := h.service.defaultBytesPerSnapshotChunk()
	raw, present, err := optionalScalarQueryParam(r, "max_bytes")
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", err.Error(), r)
		return
	}
	if present {
		maxBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", "max_bytes must be an integer", r)
			return
		}
	}

	response, release, err := h.service.getSnapshotChunkWithLease(r.Context(), actor, proof, snapshotID, afterRowOrdinal, queryParams.maxRows, maxBytes)
	if err != nil {
		if h.writeGenerationOrSourceLifecycleError(w, r, actor.SourceID, err) {
			return
		}
		if h.writeServiceUnavailable(w, r, err) {
			return
		}
		var invalidErr *SnapshotChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", invalidErr.Error(), r)
			return
		}
		var capacityErr *SnapshotCapacityError
		if errors.As(err, &capacityErr) {
			w.Header().Set("Retry-After", "1")
			h.writeError(w, http.StatusTooManyRequests, "snapshot_chunk_capacity", capacityErr.Error(), r)
			return
		}
		var tooSmallErr *SnapshotChunkTooSmallError
		if errors.As(err, &tooSmallErr) {
			h.writeSnapshotChunkTooSmall(w, r, tooSmallErr)
			return
		}
		var notFoundErr *SnapshotSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "snapshot_session_not_found", notFoundErr.Error(), r)
			return
		}
		var expiredErr *SnapshotSessionExpiredError
		if errors.As(err, &expiredErr) {
			h.writeError(w, http.StatusGone, "snapshot_session_expired", expiredErr.Error(), r)
			return
		}
		var forbiddenErr *SnapshotSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "snapshot_session_forbidden", forbiddenErr.Error(), r)
			return
		}
		h.logger.Error("Failed to get snapshot chunk", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "snapshot_chunk_failed", "Failed to get snapshot chunk", r)
		return
	}
	defer release()
	view, err := h.service.resolveRequestGeneration(r.Context(), actor, proof)
	if err != nil {
		h.writeGenerationError(w, r, err)
		return
	}
	groups, logicalByteCount, err := CompactGroupsForSnapshotRowsAt(generationDescriptor(view.contract), response.Rows, afterRowOrdinal+1)
	if err != nil || logicalByteCount != response.ByteCount {
		h.logger.Error("Failed to project compact snapshot chunk", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "snapshot_chunk_failed", "Failed to project compact snapshot chunk", r)
		return
	}
	h.writeBoundedJSON(w, r, http.StatusOK, CompactSnapshotChunkResponse{
		Rows:             groups,
		NextRowOrdinal:   response.NextRowOrdinal,
		LogicalByteCount: logicalByteCount,
		HasMore:          response.HasMore,
	}, h.service.maxBytesPerSnapshotChunk(), "Failed to encode compact snapshot chunk response")
}

func (h *HTTPSyncHandlers) writeSnapshotChunkTooSmall(w http.ResponseWriter, r *http.Request, err *SnapshotChunkTooSmallError) {
	h.writeBoundedJSON(w, r, http.StatusBadRequest, ErrorResponse{Error: "snapshot_chunk_too_small", Message: err.Error(), RequiredDecodedBodyBytes: err.RequiredDecodedBodyBytes}, generationControlResponseLimit, "Failed to encode snapshot chunk limit response")
}

// HandleDeleteSnapshotSession deletes an existing frozen snapshot session.
func (h *HTTPSyncHandlers) HandleDeleteSnapshotSession(w http.ResponseWriter, r *http.Request) {
	actor, proof, ok := h.requireActorAndProofForMethod(w, r, http.MethodDelete)
	if !ok {
		return
	}
	if err := rejectUnknownQueryParams(r); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), r)
		return
	}

	snapshotID := r.PathValue("snapshot_id")
	if err := h.service.DeleteSnapshotSession(r.Context(), actor, proof, snapshotID); err != nil {
		if h.writeGenerationOrSourceLifecycleError(w, r, actor.SourceID, err) {
			return
		}
		if h.writeServiceUnavailable(w, r, err) {
			return
		}
		var invalidErr *SnapshotChunkInvalidError
		if errors.As(err, &invalidErr) {
			h.writeError(w, http.StatusBadRequest, "snapshot_chunk_invalid", invalidErr.Error(), r)
			return
		}
		var notFoundErr *SnapshotSessionNotFoundError
		if errors.As(err, &notFoundErr) {
			h.writeError(w, http.StatusNotFound, "snapshot_session_not_found", notFoundErr.Error(), r)
			return
		}
		var forbiddenErr *SnapshotSessionForbiddenError
		if errors.As(err, &forbiddenErr) {
			h.writeError(w, http.StatusForbidden, "snapshot_session_forbidden", forbiddenErr.Error(), r)
			return
		}
		h.logger.Error("Failed to delete snapshot session", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "snapshot_session_delete_failed", "Failed to delete snapshot session", r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleStatus returns the current lifecycle and operability status snapshot.
func (h *HTTPSyncHandlers) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed", r)
		return
	}
	response, err := h.service.GetStatus(r.Context())
	if err != nil {
		h.logger.Error("Failed to get service status", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "status_failed", "Failed to get service status", r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleHealth returns a readiness-oriented health response derived from the service status snapshot.
func (h *HTTPSyncHandlers) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Only GET method is allowed", r)
		return
	}
	response, err := h.service.GetStatus(r.Context())
	if err != nil {
		h.logger.Error("Failed to get health status", "error_type", fmt.Sprintf("%T", err))
		h.writeError(w, http.StatusInternalServerError, "health_failed", "Failed to get health status", r)
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

// writeError writes a standardized error response
func (h *HTTPSyncHandlers) writeError(w http.ResponseWriter, statusCode int, errorCode, message string, requests ...*http.Request) {
	errorResponse := ErrorResponse{
		Error:   errorCode,
		Message: message,
	}
	body, err := json.Marshal(errorResponse)
	if err != nil || len(body) > generationControlResponseLimit {
		body = []byte(`{"error":"response_encoding_failed","message":"Failed to encode error response"}`)
	}
	var request *http.Request
	if len(requests) > 0 {
		request = requests[0]
	}
	h.writeJSONBytes(w, request, statusCode, body)

	h.logger.Debug("HTTP error response",
		"status_code", statusCode,
		"error_code", errorCode,
		"message", message)
}

func (h *HTTPSyncHandlers) writePushConflict(w http.ResponseWriter, r *http.Request, conflictErr *PushConflictError) {
	if conflictErr == nil {
		h.writeError(w, http.StatusConflict, "push_conflict", "push conflict", r)
		return
	}
	defer releaseHistoryProjection(&conflictErr.historyProjectionRelease)

	response := PushConflictResponse{
		Error:    "push_conflict",
		Message:  conflictErr.Error(),
		Conflict: conflictErr.Conflict,
	}
	limit := int64(h.service.config.MaxBytesPerBundle) + historyEnvelopeAllowance
	h.writeBoundedJSON(w, r, http.StatusConflict, response, limit, "Failed to encode push conflict response")

	h.logger.Debug("HTTP push conflict response",
		"status_code", http.StatusConflict,
		"error_code", "push_conflict",
		"message", conflictErr.Error())
}
