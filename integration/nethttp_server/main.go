// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/internal/serverenv"
)

func main() {
	// Example-server variables (OVERSYNC_*) are read by the host, never by oversync.
	envSettings, err := serverenv.Load(os.Getenv)
	if err != nil {
		log.Fatalf("Invalid environment: %v", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: envSettings.LogLevel}))

	// Database connection string
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"
	}

	// Setup JWT secret
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "your-secret-key-change-in-production"
		logger.Warn("Using default JWT secret - change in production!")
	}

	// Setup server components using shared logic
	serverConfig := &server.ServerConfig{
		DatabaseURL: databaseURL,
		JWTSecret:   jwtSecret,
		Logger:      logger,
		AppName:     "nethttp-server-example",
	}

	components, err := server.SetupServer(serverConfig)
	if err != nil {
		log.Fatalf("Failed to setup server: %v", err)
	}
	defer components.Close()

	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}
	httpServer := newHTTPServer(addr, components.Handler)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		components.Logger.Info("Starting two-way sync server", "addr", httpServer.Addr)
		components.Logger.Info("Two-way sync endpoints available at:")
		components.Logger.Info("  POST /sync/push          - Commit or replay one complete compact push")
		components.Logger.Info("  GET  /sync/pull          - Pull committed bundles from server")
		components.Logger.Info("  GET  /sync/watch         - Watch metadata-only bundle change wake-ups")
		components.Logger.Info("  POST /sync/snapshot-sessions               - Create frozen chunked snapshot session")
		components.Logger.Info("  GET  /sync/snapshot-sessions/{snapshot_id} - Fetch one snapshot chunk")
		components.Logger.Info("  DEL  /sync/snapshot-sessions/{snapshot_id} - Delete snapshot session")
		components.Logger.Info("  GET  /sync/generations/{generation_id} - Discover the admitted wire contract")
		components.Logger.Info("  GET  /syncx/status       - Service lifecycle and bundle visibility status")
		components.Logger.Info("  GET  /syncx/health       - Readiness health check")
		components.Logger.Info("  POST /dummy-signin       - Dummy signin to obtain JWT (user)")
		components.Logger.Info("  POST /test/retention-floor - Test helper to raise retained bundle floor for one user")
		components.Logger.Info("")
		components.Logger.Info("Authentication: JWT Bearer token required")
		components.Logger.Info("Sync source: send Oversync-Source-ID on authenticated /sync/* requests")
		components.Logger.Info("Example: Authorization: Bearer <jwt-token>")

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	case err := <-serverErr:
		components.Logger.Error("HTTP server stopped unexpectedly", "error", err)
	}

	components.Logger.Info("Shutting down server...")

	// Create a deadline for shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Withdraw readiness and cancel watches before waiting for HTTP handlers.
	if err := components.SyncService.Close(shutdownCtx); err != nil {
		components.Logger.Error("Sync service drain did not complete", "error", err)
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		components.Logger.Error("HTTP server shutdown did not complete", "error", err)
	}

	components.Logger.Info("Server exited")
}

// newHTTPServer bounds slow uploads of a maximum-size push with ReadTimeout.
// WriteTimeout stays zero because GET /sync/watch is a long-lived server-sent
// event stream.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       120 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}
