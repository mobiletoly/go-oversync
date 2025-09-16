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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// ServerConfig holds configuration for the server
type ServerConfig struct {
	DatabaseURL string
	JWTSecret   string
	Logger      *slog.Logger
	AppName     string
}

// ServerComponents holds the initialized server components
type ServerComponents struct {
	Pool        *pgxpool.Pool
	SyncService *oversync.SyncService
	JWTAuth     *oversync.JWTAuth
	Handler     http.Handler
	Logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
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

	// Create database connection pool with configuration for parallel load
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		cancel()
		return nil, err
	}

	// Configure connection pool for parallel testing
	poolConfig.MaxConns = 50 // Allow up to 50 concurrent connections
	poolConfig.MinConns = 5  // Keep minimum 5 connections open
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Minute * 30

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
	if err := InitializeApplicationTables(ctx, pool, logger); err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	// Configure sync service with registered tables and handlers
	serviceConfig := &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   appName,
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "business", Table: "users", Handler: &UsersTableHandler{logger: logger}},
			{Schema: "business", Table: "posts", Handler: &PostsTableHandler{logger: logger}},
			{Schema: "business", Table: "files", Handler: &FilesTableHandler{logger: logger}},
			{Schema: "business", Table: "file_reviews", Handler: &FileReviewsTableHandler{logger: logger}},
		},
		DisableAutoMigrateFKs: false, // Enable automatic FK migration to deferrable
	}

	logger.Info("DEBUG: Registering table handlers",
		"users_handler", serviceConfig.RegisteredTables[0].Handler != nil,
		"users_key", serviceConfig.RegisteredTables[0].Schema+"."+serviceConfig.RegisteredTables[0].Table,
		"posts_key", serviceConfig.RegisteredTables[1].Schema+"."+serviceConfig.RegisteredTables[1].Table,
		"files_key", serviceConfig.RegisteredTables[2].Schema+"."+serviceConfig.RegisteredTables[2].Table,
	)

	// Initialize sync service
	syncService, err := oversync.NewSyncService(pool, serviceConfig, logger)
	if err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	// Table handlers are now registered automatically from ServiceConfig

	// Setup JWT authentication
	jwtSecret := config.JWTSecret
	if jwtSecret == "" {
		jwtSecret = "your-secret-key-change-in-production"
		logger.Warn("Using default JWT secret - change in production!")
	}
	jwtAuth := oversync.NewJWTAuth(jwtSecret)

	// Create sync handlers
	syncHandlers := oversync.NewHTTPSyncHandlers(syncService, jwtAuth, logger)

	// Create HTTP handler
	mux := http.NewServeMux()

	// Add health check endpoint
	mux.HandleFunc("GET /health", HandleHealth)

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
	mux.Handle("POST /sync/upload", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		syncHandlers.HandleUpload(w, r)
		logger.Info("🔥 UPLOAD: Upload handler completed")
	})), logger))
	mux.Handle("GET /sync/download", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		syncHandlers.HandleDownload(w, r)
		logger.Info("🔥 DOWNLOAD: Download handler completed")
	})), logger))
	mux.Handle("GET /sync/schema-version", LoggingMiddleware(false, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleSchemaVersion)), logger))

	return &ServerComponents{
		Pool:        pool,
		SyncService: syncService,
		JWTAuth:     jwtAuth,
		Handler:     mux,
		Logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Close shuts down the server components and cleans up resources
func (sc *ServerComponents) Close() {
	if sc.SyncService != nil {
		sc.SyncService.Close()
	}
	if sc.Pool != nil {
		sc.Pool.Close()
	}
	if sc.cancel != nil {
		sc.cancel()
	}
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

		// Log response body for download requests to debug empty responses
		if r.URL.Path == "/sync/download" && len(wrapped.body) > 0 && len(wrapped.body) < 5000 {
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

// HandleHealth provides a simple health check endpoint
func HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "healthy", "service": "go-oversync-nethttp-example"}`))
}
