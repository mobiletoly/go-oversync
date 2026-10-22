package schemaevolution

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/internal/exampleauth"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestMemberSyncRoutes_DispatchDiscoveryBeforeServeMuxAndGuardOtherSyncTraffic(t *testing.T) {
	const secret = "member-route-test-secret"
	auth := exampleauth.New(secret)
	token, err := auth.GenerateToken("member-user", time.Hour)
	require.NoError(t, err)
	authorization := "Bearer " + token
	service := &oversync.SyncService{}
	service.WithdrawReadiness(oversync.DeploymentContractMismatch)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /wrong/sync/generations/1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := registerMemberSyncRoutes(mux, service, LocalMemberConfig{
		JWTSecret:    secret,
		ClientTokens: []string{authorization},
	})

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
			request.Header.Set("Authorization", authorization)
			request.Header.Set(oversync.SourceIDHeader, "member-source")
			request.Header.Set(oversync.ProtocolVersionHeader, oversync.SyncProtocolVersion)
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

func TestMemberSyncRoutes_RegisterDurableOutcomeHandlers(t *testing.T) {
	mux := http.NewServeMux()
	registerMemberSyncRoutes(mux, &oversync.SyncService{}, LocalMemberConfig{})
	for _, path := range []string{"/sync/push-outcomes/resolve", "/sync/push-outcomes/ack"} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		_, pattern := mux.Handler(request)
		require.Equal(t, "POST "+path, pattern)
	}
}
