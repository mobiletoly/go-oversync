package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// ServerConfig holds configuration for the server
type ServerConfig struct {
	DatabaseURL    string
	JWTSecret      string
	Logger         *slog.Logger
	AppName        string
	BusinessSchema string
}

// ServerComponents holds the initialized server components
type ServerComponents struct {
	Pool           *pgxpool.Pool
	SyncService    *oversync.SyncService
	JWTAuth        *oversync.JWTAuth
	Handler        http.Handler
	Logger         *slog.Logger
	BusinessSchema string
	ctx            context.Context
	cancel         context.CancelFunc
}

// TestServer represents a running test server instance
type TestServer struct {
	*ServerComponents
	HTTPServer *httptest.Server
}

// SetupServer initializes all server components (database, sync service, handlers)
// This is the shared logic used by both main() and tests
func SetupServer(config *ServerConfig) (*ServerComponents, error) {
	ctx, cancel := context.WithCancel(context.Background())

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

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		cancel()
		return nil, err
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	// Initialize application tables
	if err := InitializeApplicationTables(ctx, pool, logger, businessSchema); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	// Configure sync service with registered tables and handlers
	serviceConfig := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   appName,
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: businessSchema, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: businessSchema, Table: "posts", SyncKeyColumns: []string{"id"}},
			{Schema: businessSchema, Table: "categories"},
			{Schema: businessSchema, Table: "teams"},
			{Schema: businessSchema, Table: "team_members"},
			{Schema: businessSchema, Table: "files", SyncKeyColumns: []string{"id"}},
			{Schema: businessSchema, Table: "file_reviews", SyncKeyColumns: []string{"id"}},
		},
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("OVERSYNC_LOG_STAGE_TIMINGS"))); v == "1" || v == "true" || v == "yes" {
		serviceConfig.LogStageTimings = true
	}

	logger.Info("Registering bundle-captured sync tables",
		"users_key", serviceConfig.RegisteredTables[0].Schema+"."+serviceConfig.RegisteredTables[0].Table,
		"posts_key", serviceConfig.RegisteredTables[1].Schema+"."+serviceConfig.RegisteredTables[1].Table,
		"files_key", serviceConfig.RegisteredTables[5].Schema+"."+serviceConfig.RegisteredTables[5].Table,
	)

	// Build runtime service, then bootstrap bundle-capture schema and topology explicitly.
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
	// Setup JWT authentication
	jwtSecret := config.JWTSecret
	if jwtSecret == "" {
		jwtSecret = "your-secret-key-change-in-production"
		logger.Warn("Using default JWT secret - change in production!")
	}
	jwtAuth := oversync.NewJWTAuth(jwtSecret)

	// Create sync handlers
	syncHandlers := oversync.NewHTTPSyncHandlers(syncService, logger)

	// Create HTTP handler
	mux := http.NewServeMux()

	// Add health/status endpoints
	mux.HandleFunc("GET /health", syncHandlers.HandleHealth)
	mux.HandleFunc("GET /status", syncHandlers.HandleStatus)
	mux.HandleFunc("POST /test/reset", func(w http.ResponseWriter, r *http.Request) {
		if err := resetExampleDatabase(r.Context(), pool, syncService, logger, businessSchema); err != nil {
			logger.Error("failed to reset example database", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "reset_failed", "message": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"schema": businessSchema,
		})
	})
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

	// Dummy signin endpoint returns a JWT for provided user/device; any password accepted
	mux.HandleFunc("POST /dummy-signin", func(w http.ResponseWriter, r *http.Request) {
		type signinReq struct {
			User     string `json:"user"`
			Password string `json:"password"`
			Device   string `json:"device"`
		}
		type signinResp struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
			User      string `json:"user"`
			Device    string `json:"device"`
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
		if req.Device == "" {
			req.Device = "device-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		}
		// Any password accepted; generate JWT valid for 5 minutes
		tok, err := jwtAuth.GenerateToken(req.User, req.Device, 5*time.Minute)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "token_error", "message": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(signinResp{Token: tok, ExpiresIn: int64(300), User: req.User, Device: req.Device})
		logger.Info("Generated dummy JWT", "user", req.User, "device", req.Device)
	})

	// Register sync endpoints with enhanced logging
	mux.Handle("POST /sync/push-sessions", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCreatePushSession)), logger))
	mux.Handle("POST /sync/push-sessions/{push_id}/chunks", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandlePushSessionChunk)), logger))
	mux.Handle("POST /sync/push-sessions/{push_id}/commit", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCommitPushSession)), logger))
	mux.Handle("DELETE /sync/push-sessions/{push_id}", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleDeletePushSession)), logger))
	mux.Handle("GET /sync/committed-bundles/{bundle_seq}/rows", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleGetCommittedBundleRows)), logger))
	mux.Handle("GET /sync/pull", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		syncHandlers.HandlePull(w, r)
		logger.Info("🔥 PULL: Pull handler completed")
	})), logger))
	mux.Handle("POST /sync/snapshot-sessions", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCreateSnapshotSession)), logger))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleGetSnapshotChunk)), logger))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleDeleteSnapshotSession)), logger))
	mux.Handle("GET /sync/capabilities", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCapabilities)), logger))

	return &ServerComponents{
		Pool:           pool,
		SyncService:    syncService,
		JWTAuth:        jwtAuth,
		Handler:        BrowserTestCORSMiddleware(mux),
		Logger:         logger,
		BusinessSchema: businessSchema,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

func resetExampleDatabase(ctx context.Context, pool *pgxpool.Pool, syncService *oversync.SyncService, logger *slog.Logger, businessSchema string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(businessSchema) == "" {
		businessSchema = "business"
	}
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		businessSchemaIdent := qualifiedSchema(businessSchema)
		if _, err := tx.Exec(ctx, `DROP SCHEMA IF EXISTS sync CASCADE`); err != nil {
			return fmt.Errorf("drop sync schema: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, businessSchemaIdent)); err != nil {
			return fmt.Errorf("drop business schema: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := InitializeApplicationTables(ctx, pool, logger, businessSchema); err != nil {
		return fmt.Errorf("reinitialize application tables: %w", err)
	}
	if err := syncService.Bootstrap(ctx); err != nil {
		return fmt.Errorf("rebootstrap sync service: %w", err)
	}
	logger.Info("example database reset complete", "schema", businessSchema)
	return nil
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

// Close shuts down the server components and cleans up resources
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

// BrowserTestCORSMiddleware keeps the example server usable from browser-based smoke tests
// without affecting auth semantics. It intentionally allows cross-origin access because this
// binary is a local demo/test server, not a production deployment template.
func BrowserTestCORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
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
	components, err := SetupServer(config)
	if err != nil {
		return nil, err
	}

	// Create test HTTP server
	httpServer := httptest.NewServer(components.Handler)

	return &TestServer{
		ServerComponents: components,
		HTTPServer:       httpServer,
	}, nil
}

// Close shuts down the test server and cleans up resources
func (ts *TestServer) Close() {
	if ts.HTTPServer != nil {
		ts.HTTPServer.Close()
	}
	ts.ServerComponents.Close()
}

// URL returns the base URL of the test server
func (ts *TestServer) URL() string {
	return ts.HTTPServer.URL
}

// GenerateToken generates a JWT token for testing
func (ts *TestServer) GenerateToken(userID, deviceID string, duration time.Duration) (string, error) {
	return ts.JWTAuth.GenerateToken(userID, deviceID, duration)
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
