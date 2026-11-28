// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"net/http"
)

// ReadinessGuard must wrap the application's sync routes before authentication
// and proof decoding. Direct API operations retain their own admission gate.
func (s *SyncService) ReadinessGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admission := &operationAdmission{service: s}
		ctx := context.WithValue(r.Context(), operationAdmissionKey{}, admission)
		done, err := s.beginOperation(ctx)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"service_not_ready","message":"Sync service is unavailable; retry later"}`))
			return
		}
		defer done()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
