// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

// schema-evolution-authority is the permanent test-only live authority for SQLiteNow's
// additive schema-evolution qualification journeys.
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
)

func main() {
	mode, err := parseMode(os.Getenv("SCHEMA_EVOLUTION_AUTHORITY_MODE"))
	if err != nil {
		log.Fatal(err)
	}
	controlToken := os.Getenv("SCHEMA_EVOLUTION_CONTROL_TOKEN")
	if controlToken == "" {
		log.Fatal("SCHEMA_EVOLUTION_CONTROL_TOKEN is required")
	}
	config, err := newFixtureConfig(mode)
	if err != nil {
		log.Fatalf("configure schema-evolution authority: %v", err)
	}
	if err := validateFixtureOnly(config); err != nil {
		log.Fatalf("validate schema-evolution authority: %v", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"
	}
	gate := &targetGate{target: config.targetGeneration}
	components, err := server.SetupServer(&server.ServerConfig{
		DatabaseURL: databaseURL, JWTSecret: os.Getenv("JWT_SECRET"), Logger: logger,
		AppName: authorityID + "/" + string(mode), BusinessSchema: schemaName,
		ApplicationSchemaPrepared: true, RegisteredTables: config.registeredTables,
		SchemaGenerations: config.generations, RegisteredTableCatalogRevision: 1,
		Readiness:                 config.readiness,
		BaselineReadinessManifest: baselineReadinessManifest(mode),
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
			return prepareApplication(ctx, pool, config)
		},
		SyncAdmissionMiddleware: gate.middleware,
	})
	if err != nil {
		log.Fatalf("setup schema-evolution authority: %v", err)
	}
	defer components.Close()

	addr := os.Getenv("SCHEMA_EVOLUTION_AUTHORITY_ADDR")
	if addr == "" {
		addr = ":8082"
	}
	httpServer := &http.Server{
		Addr:        addr,
		Handler:     authorityHandler(components, config, controlToken, gate, &faultController{}, newRequestCounters()),
		ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 60 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("schema-evolution authority ready", "addr", addr, "mode", mode, "target_generation", config.targetGeneration)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	case err := <-serverErr:
		logger.Error("schema-evolution authority stopped", "error", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = components.SyncService.Close(ctx)
	_ = httpServer.Shutdown(ctx)
}
