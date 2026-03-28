package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

type ServerConfig struct {
	DatabaseURL string
	JWTSecret   string
	Logger      *slog.Logger
	AppName     string
}

type ServerComponents struct {
	Pool        *pgxpool.Pool
	SyncService *oversync.SyncService
	JWTAuth     *oversync.JWTAuth
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
		MaxSupportedSchemaVersion: 1,
		AppName:                   appName,
		RegisteredTables: []oversync.RegisteredTable{
			{Schema: "business", Table: "person", SyncKeyColumns: []string{"id"}},
			{Schema: "business", Table: "person_address", SyncKeyColumns: []string{"id"}},
			{Schema: "business", Table: "comment", SyncKeyColumns: []string{"id"}},
		},
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("OVERSYNC_LOG_STAGE_TIMINGS"))); v == "1" || v == "true" || v == "yes" {
		svcCfg.LogStageTimings = true
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

	jwtSecret := config.JWTSecret
	if jwtSecret == "" {
		jwtSecret = "dev-secret"
	}
	jwtAuth := oversync.NewJWTAuth(jwtSecret)

	syncHandlers := oversync.NewHTTPSyncHandlers(syncService, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", syncHandlers.HandleHealth)
	mux.HandleFunc("GET /status", syncHandlers.HandleStatus)
	mux.HandleFunc("POST /dummy-signin", func(w http.ResponseWriter, r *http.Request) {
		type req struct{ User, Password, Device string }
		type resp struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
			User      string `json:"user"`
			Device    string `json:"device"`
		}
		var rr req
		if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request"})
			return
		}
		if rr.User == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "user_required"})
			return
		}
		if rr.Device == "" {
			rr.Device = "device-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		}
		// TODO (any username/password accepted for now)
		tok, err := jwtAuth.GenerateToken(rr.User, rr.Device, 10*time.Minute)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "token_error"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp{Token: tok, ExpiresIn: 600, User: rr.User, Device: rr.Device})
	})
	mux.Handle("POST /sync/push-sessions", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCreatePushSession)), logger))
	mux.Handle("POST /sync/connect", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleConnect)), logger))
	mux.Handle("POST /sync/push-sessions/{push_id}/chunks", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandlePushSessionChunk)), logger))
	mux.Handle("POST /sync/push-sessions/{push_id}/commit", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCommitPushSession)), logger))
	mux.Handle("DELETE /sync/push-sessions/{push_id}", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleDeletePushSession)), logger))
	mux.Handle("GET /sync/committed-bundles/{bundle_seq}/rows", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleGetCommittedBundleRows)), logger))
	mux.Handle("GET /sync/pull", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandlePull)), logger))
	mux.Handle("POST /sync/snapshot-sessions", jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCreateSnapshotSession)))
	mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleGetSnapshotChunk)))
	mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleDeleteSnapshotSession)))
	mux.Handle("GET /sync/capabilities", jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleCapabilities)))

	return &ServerComponents{
		Pool:        pool,
		SyncService: syncService,
		JWTAuth:     jwtAuth,
		Handler:     BrowserTestCORSMiddleware(mux),
		Logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// BrowserTestCORSMiddleware keeps the example server usable from browser-based demos.
// It is intentionally permissive because this binary is a local sample server.
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

// LoggingMiddleware logs request/response bodies for development
func LoggingMiddleware(enable bool, next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !enable {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		var bodyLog string
		if r.Method == http.MethodPost && r.ContentLength > 0 && r.ContentLength < 100_000 {
			if bodyBytes, err := io.ReadAll(r.Body); err == nil {
				bodyLog = string(bodyBytes)
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			} else {
				bodyLog = fmt.Sprintf("read body error: %v", err)
			}
		}
		logger.Info("HTTP Request", "method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery, "body", bodyLog)
		rw := &respCapture{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		fields := map[string]any{"status": rw.status, "duration": time.Since(start).String()}
		if (r.URL.Path == "/sync/pull" || strings.HasPrefix(r.URL.Path, "/sync/push-sessions") || strings.HasPrefix(r.URL.Path, "/sync/committed-bundles/")) && len(rw.buf) > 0 && len(rw.buf) < 100_000 {
			fields["body"] = string(rw.buf)
		}
		// Extra structured logs for dev: push/download summaries and user id
		actor, _ := oversync.ActorFromContext(r.Context())
		userID := actor.UserID
		if strings.HasPrefix(r.URL.Path, "/sync/push-sessions") && len(rw.buf) > 0 {
			logger.Info("Push session summary", "user", userID, "path", r.URL.Path, "status", rw.status)
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
	status int
	buf    []byte
}

func (w *respCapture) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }
func (w *respCapture) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	return w.ResponseWriter.Write(b)
}
