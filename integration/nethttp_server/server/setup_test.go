package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestBrowserTestCORSMiddleware_GzipPushPreflight(t *testing.T) {
	called := false
	handler := BrowserTestCORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusTeapot)
	}))
	request := httptest.NewRequest(http.MethodOptions, "/sync/push", nil)
	request.Header.Set("Origin", "http://localhost:9876")
	request.Header.Set("Access-Control-Request-Method", http.MethodPost)
	request.Header.Set("Access-Control-Request-Headers", "authorization,content-encoding,content-type,oversync-source-id,oversync-schema-generation-id,oversync-schema-generation-fingerprint")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusNoContent, response.Code)
	require.False(t, called, "preflight must not reach the sync handler")
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
	allowedHeaders := strings.Split(strings.ToLower(response.Header().Get("Access-Control-Allow-Headers")), ", ")
	for _, requestedHeader := range strings.Split(request.Header.Get("Access-Control-Request-Headers"), ",") {
		require.Contains(t, allowedHeaders, requestedHeader)
	}
	require.Contains(t, strings.Split(response.Header().Get("Access-Control-Allow-Methods"), ", "), http.MethodPost)
}

func TestGuardSyncRoutes_DiscoveryBypassesOnlyOperationalReadiness(t *testing.T) {
	service := &oversync.SyncService{}
	service.WithdrawReadiness(oversync.DeploymentContractMismatch)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	applicationConstructions := 0
	application := func(http.Handler) http.Handler {
		applicationConstructions++
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	}
	handler := guardSyncRoutes(service, next, next, application)
	require.Equal(t, 1, applicationConstructions)

	for _, test := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{name: "discovery", method: http.MethodGet, path: "/sync/generations/1", status: http.StatusTeapot},
		{name: "maximum generation", method: http.MethodGet, path: "/sync/generations/2147483647", status: http.StatusTeapot},
		{name: "bare missing generation", method: http.MethodGet, path: "/sync/generations", status: http.StatusTeapot},
		{name: "missing generation", method: http.MethodGet, path: "/sync/generations/", status: http.StatusTeapot},
		{name: "leading zero", method: http.MethodGet, path: "/sync/generations/01", status: http.StatusTeapot},
		{name: "overflow", method: http.MethodGet, path: "/sync/generations/2147483648", status: http.StatusTeapot},
		{name: "suffix", method: http.MethodGet, path: "/sync/generations/1/other", status: http.StatusTeapot},
		{name: "encoded digit", method: http.MethodGet, path: "/sync/generations/%31", status: http.StatusTeapot},
		{name: "wrong method", method: http.MethodPost, path: "/sync/generations/1", status: http.StatusServiceUnavailable},
		{name: "data plane", method: http.MethodGet, path: "/sync/pull", status: http.StatusServiceUnavailable},
		{name: "non sync", method: http.MethodGet, path: "/other", status: http.StatusNoContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			require.Equal(t, test.status, response.Code)
		})
	}

	readyHandler := guardSyncRoutes(&oversync.SyncService{}, next, next, application)
	response := httptest.NewRecorder()
	readyHandler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sync/pull", nil))
	require.Equal(t, http.StatusTeapot, response.Code)

	passthrough := guardSyncRoutes(service, next, next)
	response = httptest.NewRecorder()
	passthrough.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sync/generations/1", nil))
	require.Equal(t, http.StatusNoContent, response.Code)
}

func TestGuardSyncRoutes_DispatchesMalformedDiscoveryBeforeServeMux(t *testing.T) {
	service := &oversync.SyncService{}
	service.WithdrawReadiness(oversync.DeploymentContractMismatch)
	handlers := oversync.NewHTTPSyncHandlers(service, nil)
	withActor := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
		UserIDFromContext: func(context.Context) (string, error) { return "routing-user", nil },
	})
	generation := withActor(http.HandlerFunc(handlers.HandleGeneration))
	mux := http.NewServeMux()
	mux.Handle("GET /sync/generations/{generation_id}", generation)
	mux.Handle("GET /sync/generations/", generation)
	mux.HandleFunc("GET /wrong/sync/generations/1", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	clientAdmission := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer routing-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	handler := guardSyncRoutes(service, mux, generation, clientAdmission)

	for _, path := range []string{
		"/sync/generations",
		"/sync/generations//1",
		"/sync/generations/./1",
		"/sync/generations/%2e/1",
		"/sync/generations/%31",
		"/sync/generations/",
		"/sync/generations/01",
		"/sync/generations/2147483648",
		"/sync/generations/1/other",
	} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("Authorization", "Bearer routing-token")
			request.Header.Set(oversync.SourceIDHeader, "routing-source")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			require.Empty(t, response.Header().Get("Location"))
			require.Contains(t, response.Body.String(), `"error":"invalid_generation_discovery_request"`)
		})
	}

	for _, path := range []string{"/sync/generations", "/sync/generations//1"} {
		unauthenticated := httptest.NewRecorder()
		handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusUnauthorized, unauthenticated.Code, path)
	}

	wrongMethod := httptest.NewRecorder()
	handler.ServeHTTP(wrongMethod, httptest.NewRequest(http.MethodPost, "/sync/generations/1", nil))
	require.Equal(t, http.StatusServiceUnavailable, wrongMethod.Code)

	wrongPrefix := httptest.NewRecorder()
	handler.ServeHTTP(wrongPrefix, httptest.NewRequest(http.MethodGet, "/wrong/sync/generations/1", nil))
	require.Equal(t, http.StatusNoContent, wrongPrefix.Code)
}

func TestGenerationDiscoveryPrefixRoutesMalformedSuffixToHandler(t *testing.T) {
	mux := http.NewServeMux()
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	mux.Handle("GET /sync/generations/{generation_id}", handler)
	mux.Handle("GET /sync/generations/", handler)

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/sync/generations/1/other", nil))
	require.Equal(t, http.StatusBadRequest, response.Code)
}

func TestSetupServer_AdoptsPopulatedRowsBeforeBootstrap(t *testing.T) {
	userID := "nethttp-adopt-" + uuid.NewString()
	rowID := uuid.New()
	ts, err := NewTestServer(&ServerConfig{
		Logger: slog.New(slog.DiscardHandler),
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, schema string) error {
			tableIdent := pgx.Identifier{schema, "users"}.Sanitize()
			_, err := pool.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (_sync_scope_id, id, name, email)
				VALUES ($1, $2, 'Existing', 'existing@example.com')
			`, tableIdent), userID, rowID)
			return err
		},
	})
	require.NoError(t, err)
	t.Cleanup(ts.Close)

	actor := oversync.Actor{UserID: userID, SourceID: "nethttp-reader"}
	proof, err := SchemaGenerationProofForBusinessSchema(ts.BusinessSchema)
	require.NoError(t, err)
	connect, err := ts.SyncService.Connect(context.Background(), actor, proof, &oversync.ConnectRequest{})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connect.Resolution)
	pull, err := ts.SyncService.ProcessPull(context.Background(), actor, proof, 0, 10, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), pull.StableBundleSeq)
	require.Len(t, pull.Bundles, 1)
	require.Len(t, pull.Bundles[0].Rows, 1)
	require.Equal(t, rowID.String(), pull.Bundles[0].Rows[0].Key["id"])
}

func TestBaselineReadinessManifest_PersistsAttachesAndRejectsChangedIdentity(t *testing.T) {
	ctx := context.Background()
	databaseURL, dropDatabase, err := createExampleTestDatabase(ctx, exampleTestDatabaseURL())
	require.NoError(t, err)
	defer dropDatabase()
	pool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	newConfig := func() *oversync.ServiceConfig {
		registered := []oversync.RegisteredTable{{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}}}
		generations := []oversync.SchemaGeneration{{ID: 1, Tables: []oversync.GenerationTable{{Schema: "business", Table: "users", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}}}}}
		return &oversync.ServiceConfig{RegisteredTables: registered, SchemaGenerations: generations}
	}
	readEvidence := func() oversync.ReadinessEvidence {
		var evidence oversync.ReadinessEvidence
		require.NoError(t, pool.QueryRow(ctx, `SELECT evidence FROM app_baseline.readiness WHERE singleton`).Scan(&evidence))
		return evidence
	}
	validate := func(cfg *oversync.ServiceConfig, evidence oversync.ReadinessEvidence) error {
		registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
		require.NoError(t, err)
		return oversync.ValidateApplicationReadiness(cfg.Readiness, registry, evidence)
	}

	first := newConfig()
	require.NoError(t, PrepareBaselineReadiness(ctx, pool, first))
	persisted := readEvidence()
	require.NoError(t, validate(first, persisted))

	restarted := newConfig()
	require.NoError(t, PrepareBaselineReadiness(ctx, pool, restarted))
	require.NoError(t, validate(restarted, readEvidence()))

	changed := bytes.Replace(baselineReadinessManifest, []byte(`"id": "nethttp-server"`), []byte(`"id": "nethttp-server-v2"`), 1)
	require.NotEqual(t, baselineReadinessManifest, changed)
	changedConfig := newConfig()
	require.NoError(t, prepareBaselineReadinessManifest(ctx, pool, changedConfig, changed))
	var readinessErr *oversync.ReadinessError
	require.ErrorAs(t, validate(changedConfig, readEvidence()), &readinessErr)
	require.Equal(t, oversync.DeploymentContractMismatch, readinessErr.Category)
}

func TestRegisteredTablesForBusinessSchema_UsesBusinessRichV0Contract(t *testing.T) {
	tables := RegisteredTablesForBusinessSchema("custom_business")

	require.Equal(t, []oversync.RegisteredTable{
		{Schema: "custom_business", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "categories", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "teams", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "team_members", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "files", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "file_reviews", SyncKeyColumns: []string{"id"}},
		{Schema: "custom_business", Table: "typed_rows", SyncKeyColumns: []string{"id"}},
	}, tables)
	for _, table := range tables {
		require.NotContains(t, table.Table, "blob_")
	}
}

func TestRegisteredTablesForBusinessSchema_DefaultsToBusinessSchema(t *testing.T) {
	tables := RegisteredTablesForBusinessSchema("")

	require.NotEmpty(t, tables)
	for _, table := range tables {
		require.Equal(t, "business", table.Schema)
	}
}

func TestConfiguredPoolSizeFromEnv_Defaults(t *testing.T) {
	t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "")
	t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "")

	maxConns, minConns, err := configuredPoolSizeFromEnv()
	require.NoError(t, err)
	require.Equal(t, int32(50), maxConns)
	require.Equal(t, int32(5), minConns)
}

func TestConfiguredPoolSizeFromEnv_Overrides(t *testing.T) {
	t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "120")
	t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "12")

	maxConns, minConns, err := configuredPoolSizeFromEnv()
	require.NoError(t, err)
	require.Equal(t, int32(120), maxConns)
	require.Equal(t, int32(12), minConns)
}

func TestConfiguredPoolSizeFromEnv_RejectsInvalidValues(t *testing.T) {
	t.Run("non-positive max", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "0")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})

	t.Run("negative min", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "-1")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})

	t.Run("min exceeds max", func(t *testing.T) {
		t.Setenv("OVERSYNC_DB_POOL_MAX_CONNS", "10")
		t.Setenv("OVERSYNC_DB_POOL_MIN_CONNS", "11")
		_, _, err := configuredPoolSizeFromEnv()
		require.Error(t, err)
	})
}

func TestConfiguredSnapshotConcurrencyFromEnv(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "8")
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", "12")

	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(0, 0)
	require.NoError(t, err)
	require.Equal(t, 8, builds)
	require.Equal(t, 12, chunks)

	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "0")
	_, _, err = configuredSnapshotConcurrencyFromEnv(0, 0)
	require.ErrorContains(t, err, "positive integer")
}

func TestConfiguredHistoryProjectionConcurrencyFromEnv(t *testing.T) {
	t.Run("keeps service default selection when unset", func(t *testing.T) {
		t.Setenv("OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", "")
		value, err := configuredHistoryProjectionConcurrencyFromEnv(0)
		require.NoError(t, err)
		require.Zero(t, value)
	})

	t.Run("accepts positive override", func(t *testing.T) {
		t.Setenv("OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", "10")
		value, err := configuredHistoryProjectionConcurrencyFromEnv(4)
		require.NoError(t, err)
		require.Equal(t, 10, value)
	})

	t.Run("rejects non-positive override", func(t *testing.T) {
		t.Setenv("OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS", "0")
		_, err := configuredHistoryProjectionConcurrencyFromEnv(4)
		require.ErrorContains(t, err, "positive integer")
	})
}
