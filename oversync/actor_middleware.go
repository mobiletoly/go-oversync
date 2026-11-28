// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/sourceid"
)

const SourceIDHeader = "Oversync-Source-ID"

// ActorMiddlewareConfig configures the standard HTTP actor middleware.
type ActorMiddlewareConfig struct {
	UserIDFromContext       func(context.Context) (string, error)
	ReservedServerSourceIDs []string
}

// ActorMiddleware injects the runtime actor from trusted auth context plus sync source header.
func ActorMiddleware(cfg ActorMiddlewareConfig) func(http.Handler) http.Handler {
	if cfg.UserIDFromContext == nil {
		panic("oversync.ActorMiddleware requires UserIDFromContext")
	}
	if err := validateReservedServerSourceIDs(cfg.ReservedServerSourceIDs); err != nil {
		panic("oversync.ActorMiddleware has invalid ReservedServerSourceIDs: " + err.Error())
	}
	reserved := make(map[string]struct{}, len(cfg.ReservedServerSourceIDs))
	for _, sourceID := range cfg.ReservedServerSourceIDs {
		reserved[sourceID] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, err := cfg.UserIDFromContext(r.Context())
			if err != nil || !validAuthenticatedUserID(userID) {
				writeMiddlewareError(w, http.StatusUnauthorized, "authentication_failed", "authenticated user_id not found in request context")
				return
			}

			if strings.Contains(r.URL.Path, "/sync/") {
				if err := ValidateProtocolHeader(r.Header); err != nil {
					writeMiddlewareError(w, http.StatusUpgradeRequired, "protocol_version_mismatch", "Require exactly Oversync-Protocol-Version: v1")
					return
				}
			}

			sourceValues := exactHeaderValues(r.Header, SourceIDHeader)
			if len(sourceValues) != 1 {
				writeMiddlewareError(w, http.StatusBadRequest, "invalid_request", SourceIDHeader+" header must appear exactly once")
				return
			}
			sourceID := sourceValues[0]
			if err := sourceid.Validate(sourceID); err != nil {
				writeMiddlewareError(w, http.StatusBadRequest, "invalid_request", SourceIDHeader+" header must be a non-empty visible ASCII token")
				return
			}
			if _, rejected := reserved[sourceID]; rejected {
				writeMiddlewareError(w, http.StatusBadRequest, "invalid_request", SourceIDHeader+" header must be a non-empty visible ASCII token")
				return
			}

			actor := Actor{
				UserID:   userID,
				SourceID: sourceID,
			}
			next.ServeHTTP(w, r.WithContext(ContextWithActor(r.Context(), actor)))
		})
	}
}

func validAuthenticatedUserID(value string) bool {
	if value == "" || len(value) > 64*1024 || strings.TrimSpace(value) == "" {
		return false
	}
	for _, character := range value {
		if character < ' ' || character == '\u007f' {
			return false
		}
	}
	return true
}

func writeMiddlewareError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Error:   code,
		Message: message,
	})
}
