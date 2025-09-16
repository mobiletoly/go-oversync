// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/mobiletoly/go-oversync/oversync"
)

// Server represents the HTTP server for the sync API
type Server struct {
	service *oversync.SyncService
	auth    *oversync.JWTAuth
	logger  *slog.Logger
	mux     *http.ServeMux
}

// NewServer creates a new server instance
func NewServer(service *oversync.SyncService, jwtAuth *oversync.JWTAuth, logger *slog.Logger) *Server {
	server := &Server{
		service: service,
		auth:    jwtAuth,
		logger:  logger,
		mux:     http.NewServeMux(),
	}

	server.setupRoutes()
	return server
}

// setupRoutes configures the HTTP routes
func (s *Server) setupRoutes() {
	// Create sync handlers
	syncHandlers := oversync.NewHTTPSyncHandlers(s.service, s.auth, s.logger)

	// Two-way sync endpoints
	uploadHandler := s.auth.Middleware(http.HandlerFunc(syncHandlers.HandleUpload))
	downloadHandler := s.auth.Middleware(http.HandlerFunc(syncHandlers.HandleDownload))
	schemaHandler := s.auth.Middleware(http.HandlerFunc(syncHandlers.HandleSchemaVersion))
	s.mux.Handle("POST /sync/upload", uploadHandler)
	s.mux.Handle("GET /sync/download", downloadHandler)
	s.mux.Handle("GET /sync/schema-version", schemaHandler)

	// Admin-like endpoints (scoped to authenticated user's data)
	listFailures := s.auth.Middleware(http.HandlerFunc(syncHandlers.HandleListFailures))
	retryFailure := s.auth.Middleware(http.HandlerFunc(syncHandlers.HandleRetryFailure))
	s.mux.Handle("GET /admin/materialize-failures", listFailures)
	s.mux.Handle("POST /admin/materialize-failures/retry", retryFailure)

	// Health check endpoint (no auth required)
	s.mux.HandleFunc("GET /health", s.handleHealth)
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// writeError writes an error response
func (s *Server) writeError(w http.ResponseWriter, statusCode int, errorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	errorResponse := map[string]string{
		"error":   errorCode,
		"message": message,
	}

	json.NewEncoder(w).Encode(errorResponse)

	s.logger.Debug("HTTP error response",
		"status_code", statusCode,
		"error_code", errorCode,
		"message", message)
}
