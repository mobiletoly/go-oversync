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
		sc.SyncService.Close()
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
			{Schema: "business", Table: "person", Handler: &PersonHandler{logger: logger}},
			{Schema: "business", Table: "person_address", Handler: &PersonAddressHandler{logger: logger}},
			{Schema: "business", Table: "comment", Handler: &CommentHandler{logger: logger}},
		},
		DisableAutoMigrateFKs: false,
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("OVERSYNC_LOG_STAGE_TIMINGS"))); v == "1" || v == "true" || v == "yes" {
		svcCfg.LogStageTimings = true
	}

	syncService, err := oversync.NewSyncService(pool, svcCfg, logger)
	if err != nil {
		pool.Close()
		cancel()
		return nil, err
	}

	jwtSecret := config.JWTSecret
	if jwtSecret == "" {
		jwtSecret = "dev-secret"
	}
	jwtAuth := oversync.NewJWTAuth(jwtSecret)

	syncHandlers := oversync.NewHTTPSyncHandlers(syncService, jwtAuth, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
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
	mux.Handle("POST /sync/upload", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleUpload)), logger, jwtAuth))
	mux.Handle("GET /sync/download", LoggingMiddleware(true, jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleDownload)), logger, jwtAuth))
	mux.Handle("GET /sync/schema-version", jwtAuth.Middleware(http.HandlerFunc(syncHandlers.HandleSchemaVersion)))

	return &ServerComponents{Pool: pool, SyncService: syncService, JWTAuth: jwtAuth, Handler: mux, Logger: logger, ctx: ctx, cancel: cancel}, nil
}

// LoggingMiddleware logs request/response bodies for development
func LoggingMiddleware(enable bool, next http.Handler, logger *slog.Logger, jwt *oversync.JWTAuth) http.Handler {
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
		if (r.URL.Path == "/sync/download" || r.URL.Path == "/sync/upload") && len(rw.buf) > 0 && len(rw.buf) < 100_000 {
			fields["body"] = string(rw.buf)
		}
		// Extra structured logs for dev: upload/download summaries and user id
		userID, _ := jwt.GetUserID(r)
		if r.URL.Path == "/sync/upload" && len(rw.buf) > 0 {
			type upSt struct {
				Status  string         `json:"status"`
				Invalid map[string]any `json:"invalid,omitempty"`
			}
			type upResp struct {
				Accepted bool   `json:"accepted"`
				Statuses []upSt `json:"statuses"`
			}
			var ur upResp
			err := json.Unmarshal(rw.buf, &ur)
			if err != nil {
				logger.Error("Failed to unmarshal upload response", "error", err)
			} else {
				var applied, conflict, invalid, materr int
				reasons := map[string]int{}
				for _, s := range ur.Statuses {
					switch s.Status {
					case "applied":
						applied++
					case "conflict":
						conflict++
					case "invalid":
						invalid++
						if s.Invalid != nil {
							if rv, ok := s.Invalid["reason"].(string); ok {
								reasons[rv]++
							}
						}
					case "materialize_error":
						materr++
					}
				}
				logger.Info("Upload summary",
					"user", userID,
					"accepted", ur.Accepted,
					"applied", applied,
					"conflict", conflict,
					"invalid", invalid,
					"materialize_error", materr,
					"invalid_reasons", reasons,
				)
			}
		}
		if r.URL.Path == "/sync/download" && len(rw.buf) > 0 {
			type dlResp struct {
				Changes   []any `json:"changes"`
				HasMore   bool  `json:"has_more"`
				NextAfter int64 `json:"next_after"`
			}
			var dr dlResp
			if json.Unmarshal(rw.buf, &dr) == nil {
				logger.Info("Download summary", "user", userID, "changes", len(dr.Changes), "has_more", dr.HasMore, "next_after", dr.NextAfter)
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
