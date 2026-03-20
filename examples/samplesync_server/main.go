// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"net/http"
	"os"

	sc "github.com/mobiletoly/go-oversync/examples/samplesync_server/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

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
	logger.Info("  POST /sync/push-sessions  - Create chunked push session")
	logger.Info("  POST /sync/push-sessions/{push_id}/chunks - Upload push chunk")
	logger.Info("  POST /sync/push-sessions/{push_id}/commit - Commit staged push")
	logger.Info("  GET  /sync/committed-bundles/{bundle_seq}/rows - Fetch committed bundle rows")
	logger.Info("  GET  /sync/pull           - Pull committed bundles from server")
	logger.Info("  POST /sync/snapshot-sessions               - Create frozen chunked snapshot session")
	logger.Info("  GET  /sync/snapshot-sessions/{snapshot_id} - Fetch one snapshot chunk")
	logger.Info("  DEL  /sync/snapshot-sessions/{snapshot_id} - Delete snapshot session")
	logger.Info("  GET  /sync/capabilities   - Get protocol capabilities and limits")
	logger.Info("  GET  /status              - Service lifecycle and bundle visibility status")
	logger.Info("  GET  /health              - Readiness health check")
	logger.Info("  POST /dummy-signin        - Dummy signin to obtain JWT (user/device)")

	// Create HTTP server with custom timeout settings
	server := &http.Server{
		Addr:    addr,
		Handler: comps.Handler,
	}

	if err := server.ListenAndServe(); err != nil {
		logger.Error("http server failed", "error", err)
		os.Exit(1)
	}
}
