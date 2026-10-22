package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestRecoveryFixtureHandler_BrowserControlRoutes(t *testing.T) {
	// A preflight or invalid request must never reach the database or delegate.
	authority := &TestServer{ServerComponents: &ServerComponents{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("control request reached ordinary handler")
	})}}
	handler := RecoveryFixtureHandler(authority)
	for _, action := range []string{"state", "prune", "retire", "historical_bad", "historical_good", "post_snapshot_write"} {
		t.Run(action, func(t *testing.T) {
			path := "/__recovery-fixture/" + action
			request := httptest.NewRequest(http.MethodOptions, path, nil)
			request.Header.Set("Origin", "http://localhost:9876")
			request.Header.Set("Access-Control-Request-Method", http.MethodPost)
			request.Header.Set("Access-Control-Request-Headers", "authorization, content-type, "+oversync.ProtocolVersionHeader)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusNoContent, response.Code)
			require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
			for _, header := range []string{"Authorization", "Content-Type", oversync.ProtocolVersionHeader} {
				require.Contains(t, response.Header().Get("Access-Control-Allow-Headers"), header)
			}
			require.Contains(t, response.Header().Get("Access-Control-Allow-Methods"), http.MethodPost)
			for _, body := range []string{`{}`, `{"scope":"test","unknown":true}`, `{`} {
				response = httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
				require.Equal(t, http.StatusBadRequest, response.Code)
				require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
			}
		})
	}
}

func TestRecoveryFixtureHandler_OrdinaryRouteDelegates(t *testing.T) {
	var observed *http.Request
	authority := &TestServer{ServerComponents: &ServerComponents{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = r
		w.Header().Set("X-Fixture-Delegate", "reached")
		w.WriteHeader(http.StatusAccepted)
	})}}
	request := httptest.NewRequest(http.MethodPost, "/sync/pull", strings.NewReader(`{"opaque":"body"}`))
	response := httptest.NewRecorder()
	RecoveryFixtureHandler(authority).ServeHTTP(response, request)
	require.Same(t, request, observed)
	require.Equal(t, http.StatusAccepted, response.Code)
	require.Equal(t, "reached", response.Header().Get("X-Fixture-Delegate"))
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
}
