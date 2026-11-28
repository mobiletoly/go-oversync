// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	sc "github.com/mobiletoly/go-oversync/examples/samplesync_server/server"
	"github.com/mobiletoly/go-oversync/internal/serverenv"
)

// shutdownTimeout bounds the drain on SIGTERM: readiness is withdrawn first,
// then in-flight requests finish.
const shutdownTimeout = 30 * time.Second

func main() {
	// Example-server variables (OVERSYNC_*) are read by the host, never by oversync.
	envSettings, err := serverenv.Load(os.Getenv)
	if err != nil {
		slog.Error("invalid environment", "error", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: envSettings.LogLevel}))

	cfg := &sc.ServerConfig{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		JWTSecret:   os.Getenv("JWT_SECRET"),
		Logger:      logger,
		AppName:     "samplesync-server",
	}

	comps, err := sc.SetupServer(cfg)
	if err != nil {
		logger.Error("server setup failed", "error", err)
		os.Exit(1)
	}
	defer comps.Close()

	addr := ":8080"
	if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}

	logger.Info("Samplesync server listening", "addr", addr)
	logger.Info("Endpoints:")
	logger.Info("  POST /sync/push           - Commit or replay one complete compact push")
	logger.Info("  GET  /sync/pull           - Pull committed bundles from server")
	logger.Info("  POST /sync/snapshot-sessions               - Create frozen chunked snapshot session")
	logger.Info("  GET  /sync/snapshot-sessions/{snapshot_id} - Fetch one snapshot chunk")
	logger.Info("  DEL  /sync/snapshot-sessions/{snapshot_id} - Delete snapshot session")
	logger.Info("  GET  /sync/generations/{generation_id} - Discover the admitted wire contract")
	logger.Info("  GET  /syncx/status        - Service lifecycle and bundle visibility status")
	logger.Info("  GET  /syncx/health        - Readiness health check")
	logger.Info("  POST /dummy-signin        - Dummy signin to obtain JWT (user)")
	logger.Info("Sync source: send Oversync-Source-ID on authenticated /sync/* requests")

	server := newHTTPServer(addr, comps.Handler)
	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	case err := <-serverErr:
		logger.Error("http server failed", "error", err)
	}

	logger.Info("Shutting down server")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	// Withdraw readiness and cancel watches before waiting for HTTP handlers.
	if err := comps.SyncService.Close(shutdownCtx); err != nil {
		logger.Error("sync service drain did not complete", "error", err)
	}
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown did not complete", "error", err)
	}
	logger.Info("Server exited")
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// ReadTimeout bounds a slow upload of a maximum-size push, which would
		// otherwise hold the shared push-body budget.
		ReadTimeout:    120 * time.Second,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 1 << 20,
		// WriteTimeout remains zero because /sync/watch is a long-lived SSE stream.
	}
}
