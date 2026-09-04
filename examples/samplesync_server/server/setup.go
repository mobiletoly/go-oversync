package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/exampleauth"
	"github.com/mobiletoly/go-oversync/internal/syncroute"
	"github.com/mobiletoly/go-oversync/oversync"
)

type ServerConfig struct {
	DatabaseURL                        string
	JWTSecret                          string
	Logger                             *slog.Logger
	AppName                            string
	ReservedServerSourceIDs            []string
	MaxConcurrentSnapshotBuilds        int
	MaxConcurrentSnapshotChunkRequests int
}

const maxLoggedBodyBytes = 100_000

type ServerComponents struct {
	Pool        *pgxpool.Pool
	SyncService *oversync.SyncService
	Auth        *exampleauth.TokenAuth
	Handler     http.Handler
	Logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
}

func (sc *ServerComponents) Close() {
	if sc.SyncService != nil {
		_ = sc.SyncService.Close(context.Background())
	}
	if sc.Pool != nil {
		sc.Pool.Close()
	}
	if sc.cancel != nil {
		sc.cancel()
	}
}

func SetupServer(config *ServerConfig) (*ServerComponents, error) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	dsn := config.DatabaseURL
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable"
	}
	appName := config.AppName
	if appName == "" {
		appName = "samplesync-server"
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		cancel()
		return nil, err
	}
	poolCfg.MaxConns = 20
	poolCfg.MinConns = 2
	poolCfg.MaxConnIdleTime = time.Minute * 30
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	if err := InitializeApplicationTables(ctx, pool, logger); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	svcCfg := &oversync.ServiceConfig{
		SchemaGenerations: []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{
			{Schema: "business", Table: "comment", Columns: []oversync.GenerationColumn{{Name: "comment", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "created_at", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "id", WireType: oversync.GenerationWireTypeUUID, Nullable: false}, {Name: "person_id", WireType: oversync.GenerationWireTypeUUID, Nullable: false}, {Name: "tags", WireType: oversync.GenerationWireTypeText, Nullable: true}}, ForeignKeys: []oversync.GenerationForeignKey{{Columns: []string{"person_id"}, ReferencedSchema: "business", ReferencedTable: "person", ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeyCascade, OnUpdate: oversync.GenerationForeignKeyNoAction}}},
			{Schema: "business", Table: "person", Columns: []oversync.GenerationColumn{{Name: "birth_date", WireType: oversync.GenerationWireTypeText, Nullable: true}, {Name: "created_at", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "email", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "first_name", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "id", WireType: oversync.GenerationWireTypeUUID, Nullable: false}, {Name: "is_active", WireType: oversync.GenerationWireTypeBoolean, Nullable: false}, {Name: "last_name", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "notes", WireType: oversync.GenerationWireTypeText, Nullable: true}, {Name: "phone", WireType: oversync.GenerationWireTypeText, Nullable: true}, {Name: "score", WireType: oversync.GenerationWireTypeFloat64, Nullable: true}, {Name: "ssn", WireType: oversync.GenerationWireTypeInt64, Nullable: true}, {Name: "updated_at", WireType: oversync.GenerationWireTypeText, Nullable: false}}},
			{Schema: "business", Table: "person_address", Columns: []oversync.GenerationColumn{{Name: "address_type", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "city", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "country", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "created_at", WireType: oversync.GenerationWireTypeText, Nullable: false}, {Name: "id", WireType: oversync.GenerationWireTypeUUID, Nullable: false}, {Name: "is_primary", WireType: oversync.GenerationWireTypeBoolean, Nullable: false}, {Name: "person_id", WireType: oversync.GenerationWireTypeUUID, Nullable: false}, {Name: "postal_code", WireType: oversync.GenerationWireTypeText, Nullable: true}, {Name: "state", WireType: oversync.GenerationWireTypeText, Nullable: true}, {Name: "street", WireType: oversync.GenerationWireTypeText, Nullable: false}}, ForeignKeys: []oversync.GenerationForeignKey{{Columns: []string{"person_id"}, ReferencedSchema: "business", ReferencedTable: "person", ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeyCascade, OnUpdate: oversync.GenerationForeignKeyNoAction}}},
		}}},
		AppName:                            appName,
		MaxConcurrentSnapshotBuilds:        config.MaxConcurrentSnapshotBuilds,
		MaxConcurrentSnapshotChunkRequests: config.MaxConcurrentSnapshotChunkRequests,
		BundleChangeWatch: oversync.BundleChangeWatchConfig{
			Enabled: true,
		},
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "business", Table: "person", SyncKeyColumns: []string{"id"}},
			{Schema: "business", Table: "person_address", SyncKeyColumns: []string{"id"}},
			{Schema: "business", Table: "comment", SyncKeyColumns: []string{"id"}},
		},
		ReservedServerSourceIDs:        append([]string(nil), config.ReservedServerSourceIDs...),
		RegisteredTableCatalogRevision: 1,
	}
	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(
		svcCfg.MaxConcurrentSnapshotBuilds,
		svcCfg.MaxConcurrentSnapshotChunkRequests,
	)
	if err != nil {
		pool.Close()
		cancel()
		return nil, err
	}
	svcCfg.MaxConcurrentSnapshotBuilds = builds
	svcCfg.MaxConcurrentSnapshotChunkRequests = chunks
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("OVERSYNC_LOG_STAGE_TIMINGS"))); v == "1" || v == "true" || v == "yes" {
		svcCfg.LogStageTimings = true
	}

	if err := prepareBaselineReadiness(ctx, pool, svcCfg); err != nil {
		cancel()
		pool.Close()
		return nil, err
	}
	syncService, err := oversync.NewRuntimeService(pool, svcCfg, logger)
	if err != nil {
		pool.Close()
		cancel()
		return nil, err
	}
	if err := syncService.Bootstrap(ctx); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}
	go func() {
		if err := syncService.RunBundleChangeListener(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("sync bundle change listener stopped", "error", err)
		}
	}()

	jwtSecret := config.JWTSecret
	if jwtSecret == "" {
		jwtSecret = "dev-secret"
	}
	auth := exampleauth.New(jwtSecret)
	actorMiddleware := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
		UserIDFromContext: func(ctx context.Context) (string, error) {
			userID, ok := exampleauth.UserIDFromContext(ctx)
			if !ok || userID == "" {
				return "", fmt.Errorf("authenticated user_id not found")
			}
			return userID, nil
		},
		ReservedServerSourceIDs: append([]string(nil), config.ReservedServerSourceIDs...),
	})

	syncHandlers := oversync.NewHTTPSyncHandlers(syncService, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /syncx/health", syncHandlers.HandleHealth)
	mux.HandleFunc("GET /syncx/status", syncHandlers.HandleStatus)
	mux.Handle("POST /dummy-signin", sampleSyncDummySigninHandler(auth, logger))
	withSyncActor := func(next http.Handler) http.Handler {
		return auth.Middleware(actorMiddleware(next))
	}
	mux.Handle("POST /sync/push", LoggingMiddleware(true, withSyncActor(http.HandlerFunc(syncHandlers.HandlePush)), logger))
	mux.Handle("POST /sync/connect", LoggingMiddleware(true, withSyncActor(http.HandlerFunc(syncHandlers.HandleConnect)), logger))
	mux.Handle("GET /sync/pull", LoggingMiddleware(true, withSyncActor(http.HandlerFunc(syncHandlers.HandlePull)), logger))
	// Keep SSE responses off the body-capturing logger so http.Flusher remains available.
	mux.Handle("GET /sync/watch", withSyncActor(http.HandlerFunc(syncHandlers.HandleWatch)))
	mux.Handle("POST /sync/snapshot-sessions", withSyncActor(http.HandlerFunc(syncHandlers.HandleCreateSnapshotSession)))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", withSyncActor(http.HandlerFunc(syncHandlers.HandleGetSnapshotChunk)))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", withSyncActor(http.HandlerFunc(syncHandlers.HandleDeleteSnapshotSession)))
	generationHandler := withSyncActor(http.HandlerFunc(syncHandlers.HandleGeneration))
	mux.Handle("GET /sync/generations/{generation_id}", generationHandler)
	mux.Handle("GET /sync/generations/", generationHandler)

	return &ServerComponents{
		Pool:        pool,
		SyncService: syncService,
		Auth:        auth,
		Handler:     BrowserTestCORSMiddleware(sampleReadinessGuard(syncService, mux, generationHandler)),
		Logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

func sampleSyncDummySigninHandler(auth *exampleauth.TokenAuth, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type req struct{ User, Password string }
		type resp struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
			User      string `json:"user"`
		}
		var rr req
		if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request"})
			return
		}
		if rr.User == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "user_required"})
			return
		}
		const tokenExpiresInSeconds int64 = 180
		token, err := auth.GenerateToken(rr.User, time.Duration(tokenExpiresInSeconds)*time.Second)
		if err != nil {
			logger.Error("failed to generate dummy JWT", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "token_error"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp{Token: token, ExpiresIn: tokenExpiresInSeconds, User: rr.User})
	})
}

func configuredSnapshotConcurrencyFromEnv(builds, chunks int) (int, int, error) {
	parse := func(name string, current int) (int, error) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return current, nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return 0, fmt.Errorf("%s must be a positive integer", name)
		}
		return parsed, nil
	}

	var err error
	builds, err = parse("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", builds)
	if err != nil {
		return 0, 0, err
	}
	chunks, err = parse("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", chunks)
	if err != nil {
		return 0, 0, err
	}
	return builds, chunks, nil
}

// BrowserTestCORSMiddleware keeps the example server usable from browser-based demos.
// It is intentionally permissive because this binary is a local sample server.
func BrowserTestCORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, "+oversync.SourceIDHeader+", "+oversync.SchemaGenerationIDHeader+", "+oversync.SchemaGenerationFingerprintHeader)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Max-Age", "600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// LoggingMiddleware logs request/response bodies for development
func LoggingMiddleware(enable bool, next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !enable {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		var bodyLog string
		if r.Method == http.MethodPost && r.ContentLength > 0 && r.ContentLength <= maxLoggedBodyBytes {
			if bodyBytes, err := io.ReadAll(r.Body); err == nil {
				bodyLog = string(bodyBytes)
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			} else {
				bodyLog = fmt.Sprintf("read body error: %v", err)
			}
		}
		logger.Info("HTTP Request", "method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery, "body", bodyLog)
		rw := &respCapture{ResponseWriter: w, status: http.StatusOK, limit: maxLoggedBodyBytes}
		next.ServeHTTP(rw, r)
		fields := map[string]any{"status": rw.status, "duration": time.Since(start).String()}
		if (r.URL.Path == "/sync/pull" || r.URL.Path == "/sync/push") && len(rw.buf) > 0 && !rw.truncated {
			fields["body"] = string(rw.buf)
		}
		// Extra structured logs for dev: push/download summaries and user id
		actor, _ := oversync.ActorFromContext(r.Context())
		userID := actor.UserID
		if r.URL.Path == "/sync/push" && len(rw.buf) > 0 {
			logger.Info("Whole-request push summary", "user", userID, "status", rw.status)
		}
		if r.URL.Path == "/sync/pull" && len(rw.buf) > 0 {
			type pullResp struct {
				Bundles         []any `json:"bundles"`
				HasMore         bool  `json:"has_more"`
				StableBundleSeq int64 `json:"stable_bundle_seq"`
			}
			var pr pullResp
			if json.Unmarshal(rw.buf, &pr) == nil {
				logger.Info("Pull summary", "user", userID, "bundles", len(pr.Bundles), "has_more", pr.HasMore, "stable_bundle_seq", pr.StableBundleSeq)
			}
		}
		//logger.Info("HTTP Response", "path", r.URL.Path, "resp", fields)
	})
}

type respCapture struct {
	http.ResponseWriter
	status    int
	limit     int
	buf       []byte
	truncated bool
}

func (w *respCapture) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }
func (w *respCapture) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	written := b[:n]
	remaining := w.limit - len(w.buf)
	if remaining > 0 {
		captured := len(written)
		if captured > remaining {
			captured = remaining
		}
		w.buf = append(w.buf, written[:captured]...)
	}
	if len(written) > remaining {
		w.truncated = true
	}
	return n, err
}

func sampleReadinessGuard(service *oversync.SyncService, next, generation http.Handler) http.Handler {
	guarded := service.ReadinessGuard(next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if syncroute.IsRootGenerationDiscoveryPath(r) {
			generation.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/sync/") {
			guarded.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
