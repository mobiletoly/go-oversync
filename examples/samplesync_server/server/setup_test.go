package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/internal/exampleauth"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestSampleReadinessGuard_DiscoveryNamespaceBypassesReadiness(t *testing.T) {
	service := &oversync.SyncService{}
	service.WithdrawReadiness(oversync.DeploymentContractMismatch)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := sampleReadinessGuard(service, next, next)

	for _, test := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{name: "discovery", method: http.MethodGet, path: "/sync/generations/1", status: http.StatusNoContent},
		{name: "bare missing generation", method: http.MethodGet, path: "/sync/generations", status: http.StatusNoContent},
		{name: "missing generation", method: http.MethodGet, path: "/sync/generations/", status: http.StatusNoContent},
		{name: "leading zero", method: http.MethodGet, path: "/sync/generations/01", status: http.StatusNoContent},
		{name: "overflow", method: http.MethodGet, path: "/sync/generations/2147483648", status: http.StatusNoContent},
		{name: "suffix", method: http.MethodGet, path: "/sync/generations/1/other", status: http.StatusNoContent},
		{name: "encoded digit", method: http.MethodGet, path: "/sync/generations/%31", status: http.StatusNoContent},
		{name: "wrong method", method: http.MethodPost, path: "/sync/generations/1", status: http.StatusServiceUnavailable},
		{name: "data plane", method: http.MethodGet, path: "/sync/pull", status: http.StatusServiceUnavailable},
		{name: "wrong discovery prefix", method: http.MethodGet, path: "/wrong/sync/generations/1", status: http.StatusNoContent},
		{name: "non sync", method: http.MethodGet, path: "/other", status: http.StatusNoContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			require.Equal(t, test.status, response.Code)
		})
	}
}

func TestSampleReadinessGuard_DispatchesAuthenticatedMalformedDiscoveryBeforeServeMux(t *testing.T) {
	const secret = "sample-route-test-secret"
	auth := exampleauth.New(secret)
	token, err := auth.GenerateToken("sample-user", time.Hour)
	require.NoError(t, err)
	service := &oversync.SyncService{}
	service.WithdrawReadiness(oversync.DeploymentContractMismatch)
	handlers := oversync.NewHTTPSyncHandlers(service, nil)
	actors := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
		UserIDFromContext: func(ctx context.Context) (string, error) {
			userID, _ := exampleauth.UserIDFromContext(ctx)
			return userID, nil
		},
	})
	generation := auth.Middleware(actors(http.HandlerFunc(handlers.HandleGeneration)))
	mux := http.NewServeMux()
	mux.Handle("GET /sync/generations/{generation_id}", generation)
	mux.Handle("GET /sync/generations/", generation)
	handler := sampleReadinessGuard(service, mux, generation)

	for _, path := range []string{"/sync/generations", "/sync/generations//1", "/sync/generations/./1"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set(oversync.SourceIDHeader, "sample-source")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Empty(t, response.Header().Get("Location"))
		require.Contains(t, response.Body.String(), `"error":"invalid_generation_discovery_request"`)
	}

	for _, path := range []string{"/sync/generations", "/sync/generations//1"} {
		unauthenticated := httptest.NewRecorder()
		handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusUnauthorized, unauthenticated.Code, path)
	}
}

func TestBrowserTestCORSMiddlewareAllowsGenerationProofHeaders(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := BrowserTestCORSMiddleware(next)
	request := httptest.NewRequest(http.MethodOptions, "/sync/connect", nil)
	request.Header.Set("Origin", "https://example.test")
	request.Header.Set("Access-Control-Request-Headers", "authorization, oversync-source-id")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusNoContent, response.Code)
	require.Contains(t, response.Header().Get("Access-Control-Allow-Headers"), oversync.SchemaGenerationIDHeader)
	require.Contains(t, response.Header().Get("Access-Control-Allow-Headers"), oversync.SchemaGenerationFingerprintHeader)
}

func TestConfiguredSnapshotConcurrencyFromEnv(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "12")
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", "6")

	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(8, 4)
	require.NoError(t, err)
	require.Equal(t, 12, builds)
	require.Equal(t, 6, chunks)
}

func TestConfiguredSnapshotConcurrencyFromEnv_PreservesConfigWithoutOverrides(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "")
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS", "")

	builds, chunks, err := configuredSnapshotConcurrencyFromEnv(11, 7)
	require.NoError(t, err)
	require.Equal(t, 11, builds)
	require.Equal(t, 7, chunks)
}

func TestConfiguredSnapshotConcurrencyFromEnv_RejectsInvalidValues(t *testing.T) {
	t.Setenv("OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS", "0")

	_, _, err := configuredSnapshotConcurrencyFromEnv(8, 4)
	require.ErrorContains(t, err, "OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS must be a positive integer")
}

func TestResponseCapture_BoundsBufferedBody(t *testing.T) {
	recorder := httptest.NewRecorder()
	capture := &respCapture{
		ResponseWriter: recorder,
		status:         200,
		limit:          maxLoggedBodyBytes,
	}
	body := bytes.Repeat([]byte("x"), maxLoggedBodyBytes*3)

	n, err := capture.Write(body)
	require.NoError(t, err)
	require.Equal(t, len(body), n)
	require.Equal(t, body, recorder.Body.Bytes())
	require.Len(t, capture.buf, maxLoggedBodyBytes)
	require.True(t, capture.truncated)
}

func TestDummySigninHandlerPreservesExactUserIdentity(t *testing.T) {
	auth := exampleauth.New("exact-identity-test-secret")
	handler := sampleSyncDummySigninHandler(auth, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, userID := range []string{"test-user", " test-user "} {
		body, err := json.Marshal(map[string]string{"user": userID, "password": "any"})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/dummy-signin", bytes.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code)

		var result struct {
			Token string `json:"token"`
			User  string `json:"user"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Equal(t, userID, result.User)
		claims, err := auth.ValidateToken(result.Token)
		require.NoError(t, err)
		require.Equal(t, userID, claims.Subject)
		require.NotNil(t, claims.ExpiresAt)
		require.Greater(t, time.Until(claims.ExpiresAt.Time), time.Duration(0))
	}
}
