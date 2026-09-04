package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/exampleauth"
	"github.com/mobiletoly/go-oversync/internal/syncroute"
	"github.com/mobiletoly/go-oversync/oversync"
)

// ServerConfig holds configuration for the server
type ServerConfig struct {
	Readiness *oversync.ReadinessConfig
	// ApplicationSchemaPrepared skips the static demo initializer for migrated applications.
	ApplicationSchemaPrepared bool
	DatabaseURL               string
	JWTSecret                 string
	Logger                    *slog.Logger
	AppName                   string
	BusinessSchema            string
	// SchemaGenerations optionally replaces the integration server's default
	// immutable generation declaration. It is useful for applications and
	// conformance fixtures that provision more than one fixed client view.
	SchemaGenerations []oversync.SchemaGeneration
	// RegisteredTables optionally replaces the reference table catalog. Tests
	// and examples use it to exercise production handlers with a compact
	// application schema; production callers should provide their real catalog.
	RegisteredTables               []oversync.RegisteredTable
	RegisteredTableCatalogRevision int64
	ReservedServerSourceIDs        []string
	StageMetrics                   oversync.StageMetricsRecorder
	// MaxRowsPerBundle optionally narrows the admitted push row limit in isolated tests.
	MaxRowsPerBundle                   int
	InitializationLeaseTTL             time.Duration
	MaxConcurrentSnapshotBuilds        int
	MaxConcurrentSnapshotChunkRequests int
	MaxConcurrentHistoryProjections    int
	EnableEmptyFirstDeferral           bool
	EmptyFirstDeferralDuration         time.Duration
	// BeforeBootstrap is a test/conformance hook that runs after the application
	// schema exists and before Oversync observes it. Production callers should
	// leave it nil.
	BeforeBootstrap func(context.Context, *pgxpool.Pool, string) error
	// BaselineReadinessManifest overrides the ordinary integration server's
	// static readiness identity for a dedicated static-schema authority.
	BaselineReadinessManifest []byte
	// SyncAdmissionMiddleware installs application-authenticated runtime support
	// policy independently of the immutable schema-generation contract.
	SyncAdmissionMiddleware func(http.Handler) http.Handler
}

// ServerComponents holds the initialized server components
type ServerComponents struct {
	Pool            *pgxpool.Pool
	ReadinessPools  *oversync.ReadinessPoolPair
	SyncService     *oversync.SyncService
	Auth            *exampleauth.TokenAuth
	Handler         http.Handler
	Logger          *slog.Logger
	BusinessSchema  string
	FailureInjector *failureInjector
	ctx             context.Context
	cancel          context.CancelFunc
}

// TestServer represents a running test server instance
type TestServer struct {
	*ServerComponents
	HTTPServer   *httptest.Server
	dropDatabase func() error
}

type emptyFirstDeferralCandidate struct {
	sourceID  string
	expiresAt time.Time
}

type emptyFirstDeferralController struct {
	mu         sync.Mutex
	duration   time.Duration
	candidates map[string]emptyFirstDeferralCandidate
}

const (
	FailureEndpointConnect        = "connect"
	FailureEndpointPush           = "push"
	FailureEndpointPull           = "pull"
	FailureEndpointSnapshotCreate = "snapshot_create"
	FailureEndpointSnapshotChunk  = "snapshot_chunk"
)

// RegisteredTablesForBusinessSchema returns the integration server's sync table set.
func RegisteredTablesForBusinessSchema(businessSchema string) []oversync.RegisteredTable {
	if strings.TrimSpace(businessSchema) == "" {
		businessSchema = "business"
	}
	return []oversync.RegisteredTable{
		{Schema: businessSchema, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "categories", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "teams", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "team_members", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "file_reviews", SyncKeyColumns: []string{"id"}},
		{Schema: businessSchema, Table: "typed_rows", SyncKeyColumns: []string{"id"}},
	}
}

func SchemaGenerationsForBusinessSchema(schema string) []oversync.SchemaGeneration {
	if strings.TrimSpace(schema) == "" {
		schema = "business"
	}
	c := func(name string, wire oversync.GenerationWireType, nullable bool) oversync.GenerationColumn {
		return oversync.GenerationColumn{Name: name, WireType: wire, Nullable: nullable}
	}
	fk := func(column, table string, onDelete oversync.GenerationForeignKeyAction) oversync.GenerationForeignKey {
		return oversync.GenerationForeignKey{Columns: []string{column}, ReferencedSchema: schema, ReferencedTable: table, ReferencedColumns: []string{"id"}, OnDelete: onDelete, OnUpdate: oversync.GenerationForeignKeyNoAction}
	}
	return []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{
		{Schema: schema, Table: "categories", Columns: []oversync.GenerationColumn{c("id", oversync.GenerationWireTypeUUID, false), c("name", oversync.GenerationWireTypeText, false), c("parent_id", oversync.GenerationWireTypeUUID, true)}, ForeignKeys: []oversync.GenerationForeignKey{fk("parent_id", "categories", oversync.GenerationForeignKeyCascade)}},
		{Schema: schema, Table: "file_reviews", Columns: []oversync.GenerationColumn{c("file_id", oversync.GenerationWireTypeUUID, false), c("id", oversync.GenerationWireTypeUUID, false), c("review", oversync.GenerationWireTypeText, false)}, ForeignKeys: []oversync.GenerationForeignKey{fk("file_id", "files", oversync.GenerationForeignKeyCascade)}},
		{Schema: schema, Table: "files", Columns: []oversync.GenerationColumn{c("data", oversync.GenerationWireTypeBytes, false), c("id", oversync.GenerationWireTypeUUID, false), c("name", oversync.GenerationWireTypeText, false)}},
		{Schema: schema, Table: "posts", Columns: []oversync.GenerationColumn{c("author_id", oversync.GenerationWireTypeUUID, true), c("content", oversync.GenerationWireTypeText, false), c("created_at", oversync.GenerationWireTypeText, false), c("id", oversync.GenerationWireTypeUUID, false), c("title", oversync.GenerationWireTypeText, false), c("updated_at", oversync.GenerationWireTypeText, false)}, ForeignKeys: []oversync.GenerationForeignKey{fk("author_id", "users", oversync.GenerationForeignKeyNoAction)}},
		{Schema: schema, Table: "team_members", Columns: []oversync.GenerationColumn{c("id", oversync.GenerationWireTypeUUID, false), c("name", oversync.GenerationWireTypeText, false), c("team_id", oversync.GenerationWireTypeUUID, false)}, ForeignKeys: []oversync.GenerationForeignKey{fk("team_id", "teams", oversync.GenerationForeignKeyCascade)}},
		{Schema: schema, Table: "teams", Columns: []oversync.GenerationColumn{c("captain_member_id", oversync.GenerationWireTypeUUID, true), c("id", oversync.GenerationWireTypeUUID, false), c("name", oversync.GenerationWireTypeText, false)}, ForeignKeys: []oversync.GenerationForeignKey{fk("captain_member_id", "team_members", oversync.GenerationForeignKeyNoAction)}},
		{Schema: schema, Table: "typed_rows", Columns: []oversync.GenerationColumn{c("count_value", oversync.GenerationWireTypeInt64, true), c("created_at", oversync.GenerationWireTypeText, true), c("data", oversync.GenerationWireTypeBytes, true), c("enabled_flag", oversync.GenerationWireTypeBoolean, false), c("exact_amount", oversync.GenerationWireTypeDecimal, true), c("float4_value", oversync.GenerationWireTypeFloat64, true), c("id", oversync.GenerationWireTypeUUID, false), c("medium_count", oversync.GenerationWireTypeInt64, true), c("name", oversync.GenerationWireTypeText, false), c("note", oversync.GenerationWireTypeText, true), c("rating", oversync.GenerationWireTypeFloat64, true), c("small_count", oversync.GenerationWireTypeInt64, true)}},
		{Schema: schema, Table: "users", Columns: []oversync.GenerationColumn{c("created_at", oversync.GenerationWireTypeText, false), c("email", oversync.GenerationWireTypeText, false), c("id", oversync.GenerationWireTypeUUID, false), c("name", oversync.GenerationWireTypeText, false), c("updated_at", oversync.GenerationWireTypeText, false)}},
	}}}
}

// SchemaGenerationProofForBusinessSchema returns the fixed proof matching the
// integration server's generation declaration.
func SchemaGenerationProofForBusinessSchema(schema string) (oversync.SchemaGenerationProof, error) {
	registered := RegisteredTablesForBusinessSchema(schema)
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, SchemaGenerationsForBusinessSchema(schema))
	if err != nil {
		return oversync.SchemaGenerationProof{}, err
	}
	contract, ok := registry.Generation(1)
	if !ok {
		return oversync.SchemaGenerationProof{}, fmt.Errorf("reference schema generation 1 is unavailable")
	}
	return oversync.SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}, nil
}

type failureInjector struct {
	mu    sync.Mutex
	rules map[string]*failureRule
}

type failureRule struct {
	statusCode int
	remaining  int
	body       []byte
}

// SetupServer initializes all server components (database, sync service, handlers)
// This is the shared logic used by both main() and tests
func SetupServer(config *ServerConfig) (*ServerComponents, error) {
	ctx, cancel := context.WithCancel(context.Background())
	injector := &failureInjector{rules: make(map[string]*failureRule)}

	// Use default logger if not provided
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug, // Enable DEBUG logging to see duplicate detection
		}))
	}

	// Use default database URL if not provided
	databaseURL := config.DatabaseURL
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"
	}

	// Use default app name if not provided
	appName := config.AppName
	if appName == "" {
		appName = "nethttp-server-example"
	}

	businessSchema := strings.TrimSpace(config.BusinessSchema)
	if businessSchema == "" {
		businessSchema = "business"
	}

	// Create database connection pool with configuration for parallel load
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		cancel()
		return nil, err
	}

	// Configure connection pool for parallel testing. Allow environment overrides so
	// higher-parallelism mobile_flow runs can be profiled without code changes.
	maxConns, minConns, err := configuredPoolSizeFromEnv()
	if err != nil {
		cancel()
		return nil, err
	}
	poolConfig.MaxConns = maxConns
	poolConfig.MinConns = minConns
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Minute * 30

	logger.Info("Configured PostgreSQL pool", "max_conns", poolConfig.MaxConns, "min_conns", poolConfig.MinConns)

	var readinessPools *oversync.ReadinessPoolPair
	var pool *pgxpool.Pool
	if config.Readiness != nil {
		if _, ok := config.Readiness.Adapter.(oversync.OperationAdmissionAdapter); ok {
			readinessPools, err = oversync.NewReadinessPoolPair(ctx, poolConfig, 2)
			if err != nil {
				cancel()
				return nil, err
			}
			pool = readinessPools.ExecutionPool()
			config.Readiness.PoolPair = readinessPools
			config.Readiness.AdmissionPool = nil
		}
	}
	if pool == nil {
		pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	// Initialize application tables
	if err := initializeConfiguredApplication(ctx, pool, logger, businessSchema, config.ApplicationSchemaPrepared); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}
	if config.BeforeBootstrap != nil {
		if err := config.BeforeBootstrap(ctx, pool, businessSchema); err != nil {
			pool.Close()
			cancel()
			return nil, fmt.Errorf("before bootstrap: %w", err)
		}
	}

	// Configure sync service with registered tables and handlers
	schemaGenerations := config.SchemaGenerations
	if len(schemaGenerations) == 0 {
		schemaGenerations = SchemaGenerationsForBusinessSchema(businessSchema)
	}
	registeredTables := config.RegisteredTables
	if len(registeredTables) == 0 {
		registeredTables = RegisteredTablesForBusinessSchema(businessSchema)
	}
	serviceConfig := &oversync.ServiceConfig{
		SchemaGenerations:                  schemaGenerations,
		AppName:                            appName,
		StageMetrics:                       config.StageMetrics,
		MaxRowsPerBundle:                   config.MaxRowsPerBundle,
		InitializationLeaseTTL:             config.InitializationLeaseTTL,
		MaxConcurrentSnapshotBuilds:        config.MaxConcurrentSnapshotBuilds,
		MaxConcurrentSnapshotChunkRequests: config.MaxConcurrentSnapshotChunkRequests,
		MaxConcurrentHistoryProjections:    config.MaxConcurrentHistoryProjections,
		BundleChangeWatch: oversync.BundleChangeWatchConfig{
			Enabled: true,
		},
		RegisteredTables:               registeredTables,
		ReservedServerSourceIDs:        append([]string(nil), config.ReservedServerSourceIDs...),
		RegisteredTableCatalogRevision: 1,
	}
	if config.RegisteredTableCatalogRevision != 0 {
		serviceConfig.RegisteredTableCatalogRevision = config.RegisteredTableCatalogRevision
	}
	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(
		serviceConfig.MaxConcurrentSnapshotBuilds,
		serviceConfig.MaxConcurrentSnapshotChunkRequests,
	)
	if err != nil {
		pool.Close()
		cancel()
		return nil, err
	}
	serviceConfig.MaxConcurrentSnapshotBuilds = builds
	serviceConfig.MaxConcurrentSnapshotChunkRequests = chunks
	historyProjections, err := configuredHistoryProjectionConcurrencyFromEnv(serviceConfig.MaxConcurrentHistoryProjections)
	if err != nil {
		pool.Close()
		cancel()
		return nil, err
	}
	serviceConfig.MaxConcurrentHistoryProjections = historyProjections
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("OVERSYNC_LOG_STAGE_TIMINGS"))); v == "1" || v == "true" || v == "yes" {
		serviceConfig.LogStageTimings = true
	}

	registeredKeys := make([]string, 0, len(serviceConfig.RegisteredTables))
	for _, table := range serviceConfig.RegisteredTables {
		registeredKeys = append(registeredKeys, table.Schema+"."+table.Table)
	}
	logger.Info("Registering bundle-captured sync tables", "tables", registeredKeys)

	// Build runtime service, then bootstrap bundle-capture schema and topology explicitly.
	serviceConfig.Readiness = config.Readiness
	if serviceConfig.Readiness == nil {
		manifest := baselineReadinessManifest
		if len(config.BaselineReadinessManifest) != 0 {
			manifest = config.BaselineReadinessManifest
		}
		if err := prepareBaselineReadinessManifest(ctx, pool, serviceConfig, manifest); err != nil {
			pool.Close()
			cancel()
			return nil, err
		}
	}
	syncService, err := oversync.NewRuntimeService(pool, serviceConfig, logger)
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
	// Setup example auth
	jwtSecret := config.JWTSecret
	if jwtSecret == "" {
		jwtSecret = "your-secret-key-change-in-production"
		logger.Warn("Using default JWT secret - change in production!")
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

	// Create sync handlers
	syncHandlers := oversync.NewHTTPSyncHandlers(syncService, logger)
	connectHandler := http.HandlerFunc(syncHandlers.HandleConnect)
	if config.EnableEmptyFirstDeferral {
		deferralDuration := config.EmptyFirstDeferralDuration
		if deferralDuration <= 0 {
			deferralDuration = time.Second
		}
		connectHandler = wrapConnectWithEmptyFirstDeferral(connectHandler, pool, deferralDuration)
	}

	// Create HTTP handler
	mux := http.NewServeMux()

	// Add unauthenticated auxiliary syncx endpoints
	mux.HandleFunc("GET /syncx/health", syncHandlers.HandleHealth)
	mux.HandleFunc("GET /syncx/status", syncHandlers.HandleStatus)
	mux.HandleFunc("POST /test/retention-floor", func(w http.ResponseWriter, r *http.Request) {
		type request struct {
			UserID              string `json:"user_id"`
			RetainedBundleFloor int64  `json:"retained_bundle_floor"`
		}
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request", "message": "invalid JSON"})
			return
		}
		if strings.TrimSpace(req.UserID) == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request", "message": "user_id required"})
			return
		}
		if req.RetainedBundleFloor < 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request", "message": "retained_bundle_floor must be non-negative"})
			return
		}
		tag, err := pool.Exec(r.Context(), `
			UPDATE sync.user_state
			SET retained_bundle_floor = $2
			WHERE user_id = $1
		`, req.UserID, req.RetainedBundleFloor)
		if err != nil {
			logger.Error("failed to update retained bundle floor", "error", err, "user_id", req.UserID, "retained_bundle_floor", req.RetainedBundleFloor)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "retention_floor_update_failed", "message": err.Error()})
			return
		}
		if tag.RowsAffected() == 0 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "user_state_not_found", "message": "no sync user_state row exists for user_id"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":                "ok",
			"user_id":               req.UserID,
			"retained_bundle_floor": req.RetainedBundleFloor,
		})
	})
	mux.HandleFunc("GET /test/watch-subscribers", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request", "message": "user_id required"})
			return
		}
		count, err := syncService.BundleChangeSubscriberCountForTest(r.Context(), userID)
		if err != nil {
			logger.Error("failed to inspect watch subscribers", "error", err, "user_id", userID)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "watch_subscribers_failed", "message": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user_id":            userID,
			"subscriber_count":   count,
			"process_local_only": true,
		})
	})

	// Dummy signin endpoint returns a JWT for the provided user; any password accepted.
	mux.HandleFunc("POST /dummy-signin", func(w http.ResponseWriter, r *http.Request) {
		type signinReq struct {
			User     string `json:"user"`
			Password string `json:"password"`
		}
		type signinResp struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
			User      string `json:"user"`
		}
		var req signinReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request", "message": "invalid JSON"})
			return
		}
		if req.User == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request", "message": "user required"})
			return
		}
		// Any password accepted; generate JWT valid for 5 minutes.
		tok, err := auth.GenerateToken(req.User, 5*time.Minute)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "token_error", "message": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(signinResp{Token: tok, ExpiresIn: int64(300), User: req.User})
		logger.Info("Generated dummy JWT", "user", req.User)
	})

	// Register sync endpoints with enhanced logging
	withSyncActor := func(next http.Handler) http.Handler {
		return auth.Middleware(actorMiddleware(next))
	}
	mux.Handle("POST /sync/connect", LoggingMiddleware(false, injectFailureMiddleware(injector, FailureEndpointConnect, withSyncActor(connectHandler)), logger))
	mux.Handle("POST /sync/push", LoggingMiddleware(false, injectFailureMiddleware(injector, FailureEndpointPush, withSyncActor(http.HandlerFunc(syncHandlers.HandlePush))), logger))
	mux.Handle("GET /sync/pull", LoggingMiddleware(false, injectFailureMiddleware(injector, FailureEndpointPull, withSyncActor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		syncHandlers.HandlePull(w, r)
		logger.Info("🔥 PULL: Pull handler completed")
	}))), logger))
	mux.Handle("GET /sync/watch", LoggingMiddleware(false, withSyncActor(http.HandlerFunc(syncHandlers.HandleWatch)), logger))
	mux.Handle("POST /sync/snapshot-sessions", LoggingMiddleware(false, injectFailureMiddleware(injector, FailureEndpointSnapshotCreate, withSyncActor(http.HandlerFunc(syncHandlers.HandleCreateSnapshotSession))), logger))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", LoggingMiddleware(false, injectFailureMiddleware(injector, FailureEndpointSnapshotChunk, withSyncActor(http.HandlerFunc(syncHandlers.HandleGetSnapshotChunk))), logger))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", LoggingMiddleware(false, withSyncActor(http.HandlerFunc(syncHandlers.HandleDeleteSnapshotSession)), logger))
	generationHandler := withSyncActor(http.HandlerFunc(syncHandlers.HandleGeneration))
	mux.Handle("GET /sync/generations/{generation_id}", generationHandler)
	mux.Handle("GET /sync/generations/", generationHandler)

	return &ServerComponents{
		Pool:            pool,
		ReadinessPools:  readinessPools,
		SyncService:     syncService,
		Auth:            auth,
		Handler:         BrowserTestCORSMiddleware(guardSyncRoutes(syncService, mux, generationHandler, config.SyncAdmissionMiddleware)),
		Logger:          logger,
		BusinessSchema:  businessSchema,
		FailureInjector: injector,
		ctx:             ctx,
		cancel:          cancel,
	}, nil
}

func injectFailureMiddleware(injector *failureInjector, endpoint string, next http.Handler) http.Handler {
	if injector == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if injector.maybeRespond(endpoint, w) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

func wrapConnectWithEmptyFirstDeferral(next http.Handler, pool *pgxpool.Pool, duration time.Duration) http.HandlerFunc {
	controller := &emptyFirstDeferralController{
		duration:   duration,
		candidates: make(map[string]emptyFirstDeferralCandidate),
	}
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(oversync.ErrorResponse{Error: "invalid_request", Message: "failed to read connect request"})
			return
		}
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))

		var req oversync.ConnectRequest
		if err := json.Unmarshal(body, &req); err != nil {
			next.ServeHTTP(w, r)
			return
		}

		actor, ok := oversync.ActorFromContext(r.Context())
		if !ok {
			next.ServeHTTP(w, r)
			return
		}

		if req.HasLocalPendingRows {
			controller.clear(actor.UserID)
			next.ServeHTTP(w, r)
			return
		}

		state, err := loadHarnessScopeState(r.Context(), pool, actor.UserID)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		if state != "UNINITIALIZED" {
			next.ServeHTTP(w, r)
			return
		}

		now := time.Now().UTC()
		retryLater, expiresAt := controller.decide(actor.UserID, actor.SourceID, now)
		if retryLater {
			writeEmptyFirstRetryLater(w, expiresAt, now)
			return
		}
		controller.clear(actor.UserID)
		next.ServeHTTP(w, r)
	}
}

func loadHarnessScopeState(ctx context.Context, pool *pgxpool.Pool, userID string) (string, error) {
	if pool == nil {
		return "", fmt.Errorf("pool is required")
	}
	var state string
	err := pool.QueryRow(ctx, `
		SELECT CASE ss.state_code
			WHEN 0 THEN 'UNINITIALIZED'
			WHEN 1 THEN 'INITIALIZING'
			WHEN 2 THEN 'INITIALIZED'
			ELSE 'UNKNOWN'
		END
		FROM sync.scope_state ss
		JOIN sync.user_state us ON us.user_pk = ss.user_pk
		WHERE us.user_id = $1
	`, userID).Scan(&state)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "UNINITIALIZED", nil
		}
		return "", err
	}
	return state, nil
}

func writeEmptyFirstRetryLater(w http.ResponseWriter, expiresAt time.Time, now time.Time) {
	retryAfter := int(math.Ceil(expiresAt.Sub(now).Seconds()))
	if retryAfter < 1 {
		retryAfter = 1
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(&oversync.ConnectResponse{
		Resolution:     "retry_later",
		RetryAfterSec:  retryAfter,
		LeaseExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano),
	})
}

func (c *emptyFirstDeferralController) clear(userID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.candidates, userID)
}

func (c *emptyFirstDeferralController) decide(userID, sourceID string, now time.Time) (bool, time.Time) {
	if c == nil {
		return false, time.Time{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	candidate, ok := c.candidates[userID]
	if !ok {
		expiresAt := now.Add(c.duration)
		c.candidates[userID] = emptyFirstDeferralCandidate{
			sourceID:  sourceID,
			expiresAt: expiresAt,
		}
		return true, expiresAt
	}
	if candidate.expiresAt.After(now) {
		return true, candidate.expiresAt
	}
	if candidate.sourceID == sourceID {
		delete(c.candidates, userID)
		return false, time.Time{}
	}
	expiresAt := now.Add(c.duration)
	c.candidates[userID] = emptyFirstDeferralCandidate{
		sourceID:  sourceID,
		expiresAt: expiresAt,
	}
	return true, expiresAt
}

func configuredPoolSizeFromEnv() (maxConns int32, minConns int32, err error) {
	maxConns = 50
	minConns = 5

	if v := strings.TrimSpace(os.Getenv("OVERSYNC_DB_POOL_MAX_CONNS")); v != "" {
		parsed, parseErr := strconv.Atoi(v)
		if parseErr != nil || parsed < 1 {
			return 0, 0, fmt.Errorf("OVERSYNC_DB_POOL_MAX_CONNS must be a positive integer")
		}
		maxConns = int32(parsed)
	}
	if v := strings.TrimSpace(os.Getenv("OVERSYNC_DB_POOL_MIN_CONNS")); v != "" {
		parsed, parseErr := strconv.Atoi(v)
		if parseErr != nil || parsed < 0 {
			return 0, 0, fmt.Errorf("OVERSYNC_DB_POOL_MIN_CONNS must be a non-negative integer")
		}
		minConns = int32(parsed)
	}
	if minConns > maxConns {
		return 0, 0, fmt.Errorf("OVERSYNC_DB_POOL_MIN_CONNS cannot exceed OVERSYNC_DB_POOL_MAX_CONNS")
	}
	return maxConns, minConns, nil
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

func configuredHistoryProjectionConcurrencyFromEnv(current int) (int, error) {
	value := strings.TrimSpace(os.Getenv("OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS"))
	if value == "" {
		return current, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS must be a positive integer")
	}
	return parsed, nil
}

// Close shuts down the server components and cleans up resources
func (sc *ServerComponents) Close() {
	if sc.SyncService != nil {
		_ = sc.SyncService.Close(context.Background())
	}
	if sc.ReadinessPools != nil {
		sc.ReadinessPools.Close()
	} else if sc.Pool != nil {
		sc.Pool.Close()
	}
	if sc.cancel != nil {
		sc.cancel()
	}
}

// BrowserTestCORSMiddleware keeps the integration server usable from browser-based smoke tests
// without affecting auth semantics. It intentionally allows cross-origin access because this
// binary is a local integration test server, not a production deployment template.
func BrowserTestCORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", strings.Join([]string{
			"Authorization",
			"Content-Encoding",
			"Content-Type",
			oversync.SourceIDHeader,
			oversync.SchemaGenerationIDHeader,
			oversync.SchemaGenerationFingerprintHeader,
		}, ", "))
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Max-Age", "600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// NewTestServer creates a new test server instance using the shared server setup
func NewTestServer(config *ServerConfig) (*TestServer, error) {
	resolvedConfig := *config
	var dropDatabase func() error
	if resolvedConfig.DatabaseURL == "" {
		var err error
		resolvedConfig.DatabaseURL, dropDatabase, err = createExampleTestDatabase(
			context.Background(),
			exampleTestDatabaseURL(),
		)
		if err != nil {
			return nil, err
		}
	}
	components, err := SetupServer(&resolvedConfig)
	if err != nil {
		if dropDatabase != nil {
			err = errors.Join(err, dropDatabase())
		}
		return nil, err
	}

	// Create test HTTP server
	httpServer := httptest.NewServer(components.Handler)

	return &TestServer{
		ServerComponents: components,
		HTTPServer:       httpServer,
		dropDatabase:     dropDatabase,
	}, nil
}

func createExampleTestDatabase(ctx context.Context, baseDatabaseURL string) (string, func() error, error) {
	parsed, err := url.Parse(baseDatabaseURL)
	if err != nil {
		return "", nil, fmt.Errorf("parse example test database URL: %w", err)
	}
	databaseName := "oversync_nethttp_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if name := os.Getenv("OVERSYNC_SMOKE_DATABASE_NAME"); name != "" {
		if err := validateSmokeDatabaseName(name); err != nil {
			return "", nil, err
		}
		databaseName = name
	}
	adminURL := *parsed
	adminURL.Path = "/postgres"
	adminConn, err := pgx.Connect(ctx, adminURL.String())
	if err != nil {
		return "", nil, fmt.Errorf("connect to create example test database: %w", err)
	}
	if _, err := adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		_ = adminConn.Close(ctx)
		return "", nil, fmt.Errorf("create example test database %s: %w", databaseName, err)
	}
	_ = adminConn.Close(ctx)

	testURL := *parsed
	testURL.Path = "/" + databaseName
	drop := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return dropExampleTestDatabase(cleanupCtx, baseDatabaseURL, databaseName)
	}
	return testURL.String(), drop, nil
}

// Close shuts down the test server and cleans up resources
func (ts *TestServer) Close() {
	_ = ts.CloseWithError()
}

// CloseWithError lets focused harnesses fail when their owned database cleanup fails.
func (ts *TestServer) CloseWithError() error {
	if ts.HTTPServer != nil {
		ts.HTTPServer.Close()
	}
	if ts.ServerComponents != nil {
		ts.ServerComponents.Close()
	}
	if ts.dropDatabase != nil {
		if err := ts.dropDatabase(); err != nil {
			return err
		}
		ts.dropDatabase = nil
	}
	return nil
}

func validateSmokeDatabaseName(name string) error {
	const prefix = "oversync_smoke_"
	suffix := strings.TrimPrefix(name, prefix)
	if !strings.HasPrefix(name, prefix) || len(suffix) != 32 || strings.Trim(suffix, "0123456789abcdef") != "" {
		return errors.New("OVERSYNC_SMOKE_DATABASE_NAME must be oversync_smoke_ followed by 32 lowercase hex digits")
	}
	return nil
}

// CleanupSmokeDatabase removes only the exact run-owned database after a child timeout.
// It never falls back to the normal integration database or enumerates databases.
func CleanupSmokeDatabase(ctx context.Context) error {
	name := os.Getenv("OVERSYNC_SMOKE_DATABASE_NAME")
	if err := validateSmokeDatabaseName(name); err != nil {
		return err
	}
	baseURL := os.Getenv("OVERSYNC_EXAMPLE_TEST_DATABASE_URL")
	if baseURL == "" {
		return errors.New("OVERSYNC_EXAMPLE_TEST_DATABASE_URL is required")
	}
	return dropExampleTestDatabase(ctx, baseURL, name)
}

func dropExampleTestDatabase(ctx context.Context, baseURL, name string) error {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return errors.New("parse example test database URL for cleanup")
	}
	parsed.Path = "/postgres"
	conn, err := pgx.Connect(ctx, parsed.String())
	if err != nil {
		return fmt.Errorf("connect to clean run-owned database: %w", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}

// URL returns the base URL of the test server
func (ts *TestServer) URL() string {
	return ts.HTTPServer.URL
}

// GenerateToken generates a JWT token for testing.
func (ts *TestServer) GenerateToken(userID string, duration time.Duration) (string, error) {
	return ts.Auth.GenerateToken(userID, duration)
}

func (ts *TestServer) InjectFailure(endpoint string, statusCode, count int) {
	if ts == nil || ts.FailureInjector == nil {
		return
	}
	body, _ := json.Marshal(oversync.ErrorResponse{
		Error:   "injected_failure",
		Message: fmt.Sprintf("injected %s failure", endpoint),
	})
	ts.FailureInjector.set(endpoint, statusCode, count, body)
}

func (ts *TestServer) InjectJSONFailure(endpoint string, statusCode, count int, body any) {
	if ts == nil || ts.FailureInjector == nil {
		return
	}
	raw, _ := json.Marshal(body)
	ts.FailureInjector.set(endpoint, statusCode, count, raw)
}

// InjectFailure configures one deterministic transport failure on a standalone
// integration server. The endpoint must be one of the exported FailureEndpoint*
// constants; count <= 0 clears the rule.
func (sc *ServerComponents) InjectFailure(endpoint string, statusCode, count int) error {
	if sc == nil || sc.FailureInjector == nil {
		return fmt.Errorf("failure injector is unavailable")
	}
	if !validFailureEndpoint(endpoint) {
		return fmt.Errorf("unsupported failure endpoint %q", endpoint)
	}
	if count > 0 && (statusCode < 400 || statusCode > 599) {
		return fmt.Errorf("failure status must be in 400..599")
	}
	body, _ := json.Marshal(oversync.ErrorResponse{
		Error:   "injected_failure",
		Message: fmt.Sprintf("injected %s failure", endpoint),
	})
	sc.FailureInjector.set(endpoint, statusCode, count, body)
	return nil
}

func validFailureEndpoint(endpoint string) bool {
	switch endpoint {
	case FailureEndpointConnect,
		FailureEndpointPush,
		FailureEndpointPull,
		FailureEndpointSnapshotCreate,
		FailureEndpointSnapshotChunk:
		return true
	default:
		return false
	}
}

// LoggingMiddleware logs all HTTP requests with detailed information
func LoggingMiddleware(enableLogging bool, next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !enableLogging {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()

		// Log incoming request
		authHeader := r.Header.Get("Authorization")
		authInfo := "none"
		if authHeader != "" {
			if len(authHeader) > 20 {
				authInfo = authHeader[:20] + "..."
			} else {
				authInfo = authHeader
			}
		}

		// Read and log request body for POST requests
		var bodyLog string
		if r.Method == "POST" && r.ContentLength > 0 && r.ContentLength < 10000 {
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				bodyLog = fmt.Sprintf("Error reading body: %v", err)
			} else {
				bodyLog = string(bodyBytes)
				// Restore body for the actual handler
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			}
		}

		logger.Info("HTTP Request",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"remote_addr", r.RemoteAddr,
			"user_agent", r.Header.Get("User-Agent"),
			"auth_header", authInfo,
			"content_length", r.ContentLength,
			"body", bodyLog,
		)

		// Create a response writer wrapper to capture status code and body
		wrapped := &responseWriter{ResponseWriter: w, statusCode: 200}

		// Call the next handler
		next.ServeHTTP(wrapped, r)

		// Log response with body for download requests
		duration := time.Since(start)
		responseLog := map[string]interface{}{
			"method":   r.Method,
			"path":     r.URL.Path,
			"status":   wrapped.statusCode,
			"duration": duration.String(),
		}

		// Log response body for pull requests to debug empty responses
		if r.URL.Path == "/sync/pull" && len(wrapped.body) > 0 && len(wrapped.body) < 5000 {
			responseLog["response_body"] = string(wrapped.body)
		}

		logger.Info("HTTP Response", "response", responseLog)
	})
}

func (f *failureInjector) set(endpoint string, statusCode, count int, body []byte) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if count <= 0 {
		delete(f.rules, endpoint)
		return
	}
	f.rules[endpoint] = &failureRule{
		statusCode: statusCode,
		remaining:  count,
		body:       append([]byte(nil), body...),
	}
}

func (f *failureInjector) maybeRespond(endpoint string, w http.ResponseWriter) bool {
	if f == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	rule := f.rules[endpoint]
	if rule == nil || rule.remaining <= 0 {
		return false
	}
	rule.remaining--
	if rule.remaining == 0 {
		delete(f.rules, endpoint)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(rule.statusCode)
	_, _ = w.Write(rule.body)
	return true
}

// responseWriter wraps http.ResponseWriter to capture status code and body
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	body       []byte
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(data []byte) (int, error) {
	// Capture response body for logging
	rw.body = append(rw.body, data...)
	return rw.ResponseWriter.Write(data)
}

func initializeConfiguredApplication(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger, schema string, prepared bool) error {
	if prepared {
		return nil
	}
	return InitializeApplicationTables(ctx, pool, logger, schema)
}

func guardSyncRoutes(service *oversync.SyncService, next, generation http.Handler, application ...func(http.Handler) http.Handler) http.Handler {
	dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if syncroute.IsRootGenerationDiscoveryPath(r) {
			generation.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
	admitted := http.Handler(dispatch)
	if len(application) > 0 && application[0] != nil {
		admitted = application[0](admitted)
	}
	guarded := service.ReadinessGuard(admitted)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if syncroute.IsRootGenerationDiscoveryPath(r) {
			admitted.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/sync/") {
			guarded.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func exampleTestDatabaseURL() string {
	if value := os.Getenv("OVERSYNC_EXAMPLE_TEST_DATABASE_URL"); value != "" {
		return value
	}
	return "postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable"
}
