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

	"github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
)

func main() {
	// Set up structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Enable DEBUG logging to see duplicate detection
	}))

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
	logger.Info("DEBUG: Server JWT secret", "secret", jwtSecret, "length", len(jwtSecret))

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

	// Create HTTP server with timeouts suitable for large batch uploads
	httpServer := &http.Server{
		Addr:         ":8080",
		Handler:      components.Handler,
		ReadTimeout:  120 * time.Second, // Increased for large batch uploads
		WriteTimeout: 120 * time.Second, // Increased for large batch processing
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		components.Logger.Info("Starting two-way sync server", "addr", httpServer.Addr)
		components.Logger.Info("Two-way sync endpoints available at:")
		components.Logger.Info("  POST /sync/upload        - Upload changes with conflict resolution")
		components.Logger.Info("  GET  /sync/download      - Download changes from server")
		components.Logger.Info("  GET  /sync/schema-version - Get current schema version")
		components.Logger.Info("  POST /dummy-signin       - Dummy signin to obtain JWT (user/device)")
		components.Logger.Info("")
		components.Logger.Info("Authentication: JWT Bearer token required")
		components.Logger.Info("Example: Authorization: Bearer <jwt-token>")

		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	components.Logger.Info("Shutting down server...")

	// Create a deadline for shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown server gracefully
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	components.Logger.Info("Server exited")
}
