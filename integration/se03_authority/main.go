// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

// se03-authority is a test-only two-generation HTTP authority for SQLiteNow
// SE-03 qualification. It is intentionally separate from the ordinary
// one-generation business example server.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversync"
)

const (
	authorityID = "se03-notes-v1-v2"
	appName     = "se03-authority"
	schemaName  = "se03"
)

//go:embed baseline-readiness.json
var baselineReadinessManifest []byte

func registeredTables() []oversync.RegisteredTable {
	return []oversync.RegisteredTable{{Schema: schemaName, Table: "notes", SyncKeyColumns: []string{"id"}}}
}

func schemaGenerations() []oversync.SchemaGeneration {
	id := oversync.GenerationColumn{Name: "id", WireType: oversync.GenerationWireTypeUUID}
	title := oversync.GenerationColumn{Name: "title", WireType: oversync.GenerationWireTypeText}
	details := oversync.GenerationColumn{Name: "details", WireType: oversync.GenerationWireTypeText, Nullable: true}
	return []oversync.SchemaGeneration{
		{ID: 1, Tables: []oversync.GenerationTable{{Schema: schemaName, Table: "notes", Columns: []oversync.GenerationColumn{id, title}}}},
		{ID: 2, Tables: []oversync.GenerationTable{{Schema: schemaName, Table: "notes", Columns: []oversync.GenerationColumn{id, title, details}}}},
	}
}

func prepareApplication(ctx context.Context, pool *pgxpool.Pool, _ string) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS %s;
		CREATE TABLE IF NOT EXISTS %s.notes (
			_sync_scope_id text NOT NULL,
			id uuid NOT NULL,
			title text NOT NULL,
			details text NULL,
			PRIMARY KEY (_sync_scope_id, id)
		)
	`, pgx.Identifier{schemaName}.Sanitize(), pgx.Identifier{schemaName}.Sanitize()))
	return err
}

type generationIdentity struct {
	ID          int64  `json:"id"`
	Fingerprint string `json:"fingerprint"`
}

type authorityStatus struct {
	AuthorityID     string               `json:"authority_id"`
	Application     string               `json:"application"`
	Schema          string               `json:"schema"`
	PostgresVersion string               `json:"postgres_version"`
	Generations     []generationIdentity `json:"generations"`
	ManagedTables   []string             `json:"managed_tables"`
	LocalOnlyTables []string             `json:"local_only_tables"`
	Upgrade         map[string]any       `json:"upgrade"`
}

type controlRequest struct {
	Action      string  `json:"action"`
	UserID      string  `json:"user_id"`
	ID          string  `json:"id,omitempty"`
	Title       string  `json:"title,omitempty"`
	Details     *string `json:"details,omitempty"`
	SessionKind string  `json:"session_kind,omitempty"`
	SessionID   string  `json:"session_id,omitempty"`
}

type controlNote struct {
	ID      string  `json:"id"`
	Title   string  `json:"title"`
	Details *string `json:"details"`
}

type controlSession struct {
	Kind       string    `json:"kind"`
	SessionID  string    `json:"session_id"`
	SourceID   string    `json:"source_id"`
	Generation int32     `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type controlInspection struct {
	Action          string           `json:"action"`
	UserID          string           `json:"user_id"`
	UserStateExists bool             `json:"user_state_exists"`
	NextBundleSeq   int64            `json:"next_bundle_seq,omitempty"`
	ScopeStateCode  *int16           `json:"scope_state_code,omitempty"`
	Notes           []controlNote    `json:"notes"`
	Sessions        []controlSession `json:"sessions"`
}

func statusFor(ctx context.Context, components *server.ServerComponents) (authorityStatus, error) {
	registry, err := oversync.CompileSchemaGenerationRegistry(registeredTables(), schemaGenerations())
	if err != nil {
		return authorityStatus{}, err
	}
	identities := make([]generationIdentity, 0, 2)
	for _, id := range []oversync.SchemaGenerationID{1, 2} {
		contract, ok := registry.Generation(id)
		if !ok {
			return authorityStatus{}, fmt.Errorf("generation %d missing", id)
		}
		identities = append(identities, generationIdentity{ID: int64(id), Fingerprint: contract.Fingerprint()})
	}
	var version string
	if err := components.Pool.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		return authorityStatus{}, err
	}
	return authorityStatus{
		AuthorityID: authorityID, Application: appName, Schema: schemaName,
		PostgresVersion: version, Generations: identities,
		ManagedTables: []string{"se03.notes"}, LocalOnlyTables: []string{"drafts"},
		Upgrade: map[string]any{"from_generation": 1, "to_generation": 2, "added_nullable_columns": []string{"notes.details"}},
	}, nil
}

func authorityHandler(components *server.ServerComponents, controlToken string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /se03/status", func(w http.ResponseWriter, r *http.Request) {
		status, err := statusFor(r.Context(), components)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("POST /se03/fault", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Endpoint string `json:"endpoint"`
			Status   int    `json:"status"`
			Count    int    `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err := components.InjectFailure(request.Endpoint, request.Status, request.Count); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": request.Endpoint, "status": request.Status, "count": request.Count})
	})
	if controlToken != "" {
		mux.HandleFunc("POST /se03/control", func(w http.ResponseWriter, r *http.Request) {
			supplied := r.Header.Get("X-SE03-Control-Token")
			if subtle.ConstantTimeCompare([]byte(supplied), []byte(controlToken)) != 1 {
				http.Error(w, "control token required", http.StatusUnauthorized)
				return
			}
			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			var request controlRequest
			if err := decoder.Decode(&request); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			var (
				response any
				err      error
			)
			switch request.Action {
			case "seed":
				response, err = seedScope(r.Context(), components, request)
			case "inspect":
				response, err = inspectScope(r.Context(), components.Pool, request.UserID)
			case "expire_sessions":
				response, err = expireSessions(r.Context(), components.Pool, request)
			default:
				http.Error(w, "action must be seed, inspect, or expire_sessions", http.StatusBadRequest)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(response)
		})
	}
	mux.Handle("/", components.Handler)
	return server.BrowserTestCORSMiddleware(mux)
}

func seedScope(ctx context.Context, components *server.ServerComponents, request controlRequest) (any, error) {
	if request.UserID == "" || request.ID == "" || request.Title == "" {
		return nil, fmt.Errorf("seed requires user_id, id, and title")
	}
	rowID, err := uuid.Parse(request.ID)
	if err != nil {
		return nil, fmt.Errorf("seed id must be a UUID: %w", err)
	}
	operationBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode seed identity: %w", err)
	}
	manager := oversync.NewScopeManager(components.SyncService, oversync.ScopeManagerConfig{})
	result, err := manager.ExecWrite(ctx, request.UserID, oversync.ScopeWriteOptions{
		WriterID: "se03-control",
		RetryableWriteOptions: oversync.RetryableWriteOptions{
			OperationID:         uuid.New(),
			OperationHash:       sha256.Sum256(operationBody),
			OperationValidUntil: time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond),
		},
		RequiredEffectTables: []oversync.RegisteredEffectTable{{Schema: schemaName, Table: "notes"}},
	}, func(ctx context.Context, tx oversync.DatabaseWriteTx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO se03.notes (id, title, details)
			VALUES ($1, $2, $3)
			ON CONFLICT (_sync_scope_id, id) DO UPDATE
			SET title=EXCLUDED.title, details=EXCLUDED.details
		`, rowID, request.Title, request.Details)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("seed scope: %w", err)
	}
	return map[string]any{
		"action": "seed", "user_id": request.UserID, "id": request.ID,
		"bundle_seq": result.Bundle.BundleSeq, "row_count": result.Bundle.RowCount,
		"auto_initialized": result.AutoInitialized,
	}, nil
}

func inspectScope(ctx context.Context, pool *pgxpool.Pool, userID string) (controlInspection, error) {
	if userID == "" {
		return controlInspection{}, fmt.Errorf("inspect requires user_id")
	}
	inspection := controlInspection{
		Action: "inspect", UserID: userID, Notes: []controlNote{}, Sessions: []controlSession{},
	}
	var stateCode *int16
	err := pool.QueryRow(ctx, `
		SELECT users.next_bundle_seq, scope.state_code
		FROM sync.user_state AS users
		LEFT JOIN sync.scope_state AS scope USING (user_pk)
		WHERE users.user_id=$1
	`, userID).Scan(&inspection.NextBundleSeq, &stateCode)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return controlInspection{}, fmt.Errorf("inspect user state: %w", err)
	}
	if err == nil {
		inspection.UserStateExists = true
		inspection.ScopeStateCode = stateCode
	}
	notes, err := pool.Query(ctx, `
		SELECT id::text, title, details
		FROM se03.notes
		WHERE _sync_scope_id=$1
		ORDER BY id
	`, userID)
	if err != nil {
		return controlInspection{}, fmt.Errorf("inspect notes: %w", err)
	}
	defer notes.Close()
	for notes.Next() {
		var note controlNote
		if err := notes.Scan(&note.ID, &note.Title, &note.Details); err != nil {
			return controlInspection{}, fmt.Errorf("inspect note: %w", err)
		}
		inspection.Notes = append(inspection.Notes, note)
	}
	if err := notes.Err(); err != nil {
		return controlInspection{}, fmt.Errorf("inspect notes: %w", err)
	}
	sessions, err := pool.Query(ctx, `
		SELECT 'snapshot'::text, snapshot.snapshot_id, snapshot.source_id,
		snapshot.schema_generation_id, snapshot.expires_at
		FROM sync.snapshot_sessions AS snapshot JOIN sync.user_state AS users USING (user_pk)
		WHERE users.user_id=$1
		ORDER BY snapshot.snapshot_id
	`, userID)
	if err != nil {
		return controlInspection{}, fmt.Errorf("inspect sessions: %w", err)
	}
	defer sessions.Close()
	for sessions.Next() {
		var session controlSession
		if err := sessions.Scan(&session.Kind, &session.SessionID, &session.SourceID, &session.Generation, &session.ExpiresAt); err != nil {
			return controlInspection{}, fmt.Errorf("inspect session: %w", err)
		}
		inspection.Sessions = append(inspection.Sessions, session)
	}
	if err := sessions.Err(); err != nil {
		return controlInspection{}, fmt.Errorf("inspect sessions: %w", err)
	}
	return inspection, nil
}

func expireSessions(ctx context.Context, pool *pgxpool.Pool, request controlRequest) (any, error) {
	if request.UserID == "" && request.SessionID == "" {
		return nil, fmt.Errorf("expire_sessions requires user_id or session_id")
	}
	if request.SessionKind != "" && request.SessionKind != "snapshot" {
		return nil, fmt.Errorf("session_kind must be snapshot when supplied")
	}
	if request.SessionID != "" {
		if _, err := uuid.Parse(request.SessionID); err != nil {
			return nil, fmt.Errorf("session_id must be a UUID: %w", err)
		}
	}
	query := `
		UPDATE sync.snapshot_sessions AS session
		SET expires_at=transaction_timestamp()
		WHERE ($1='' OR session.user_pk IN (
			SELECT user_pk FROM sync.user_state WHERE user_id=$1
		)) AND ($2='' OR session.snapshot_id=$2::uuid)
	`
	tag, err := pool.Exec(ctx, query, request.UserID, request.SessionID)
	if err != nil {
		return nil, fmt.Errorf("expire snapshot sessions: %w", err)
	}
	counts := map[string]int64{"snapshot": tag.RowsAffected()}
	return map[string]any{
		"action": "expire_sessions", "user_id": request.UserID,
		"session_id": request.SessionID, "expired": counts,
	}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"
	}
	components, err := server.SetupServer(&server.ServerConfig{
		DatabaseURL: databaseURL, JWTSecret: os.Getenv("JWT_SECRET"), Logger: logger,
		AppName: appName, BusinessSchema: schemaName, ApplicationSchemaPrepared: true,
		RegisteredTables: registeredTables(), SchemaGenerations: schemaGenerations(),
		RegisteredTableCatalogRevision: 1, BeforeBootstrap: prepareApplication,
		BaselineReadinessManifest: baselineReadinessManifest,
	})
	if err != nil {
		log.Fatalf("setup SE-03 authority: %v", err)
	}
	defer components.Close()

	addr := os.Getenv("SE03_AUTHORITY_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	httpServer := &http.Server{Addr: addr, Handler: authorityHandler(components, os.Getenv("SE03_CONTROL_TOKEN")), ReadTimeout: 120 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 60 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("SE-03 authority ready", "addr", addr, "authority_id", authorityID, "generations", []int{1, 2})
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	case err := <-serverErr:
		logger.Error("SE-03 authority stopped", "error", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = components.SyncService.Close(ctx)
	_ = httpServer.Shutdown(ctx)
}
