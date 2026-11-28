package oversync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecoveryProtocol_HeaderAuthenticationPrecedence(t *testing.T) {
	h := NewHTTPSyncHandlers(&SyncService{}, nil)
	request := httptest.NewRequest(http.MethodGet, "/sync/generations/1", nil)
	response := httptest.NewRecorder()
	_, ok := h.requireActorForMethod(response, request, http.MethodGet)
	if ok || response.Code != http.StatusUnauthorized {
		t.Fatal("authentication must precede header diagnostic")
	}
	actor := Actor{UserID: "fixture-user", SourceID: "550e8400-e29b-41d4-a716-446655440000"}
	request = request.WithContext(ContextWithActor(context.Background(), actor))
	response = httptest.NewRecorder()
	_, ok = h.requireActorForMethod(response, request, http.MethodGet)
	if ok || response.Code != http.StatusUpgradeRequired {
		t.Fatal("missing protocol must fail before service admission")
	}
	request.Header["Oversync-Protocol-Version"] = []string{"v1"}
	request.Header["oversync-protocol-version"] = []string{"v1"}
	response = httptest.NewRecorder()
	_, ok = h.requireActorForMethod(response, request, http.MethodGet)
	if ok || response.Code != 426 {
		t.Fatal("folded duplicate protocol accepted")
	}
}

func TestRecoveryProtocol_ComposedHeaderPrecedence(t *testing.T) {
	for _, c := range []struct {
		name, path, user, protocol, source string
		status                             int
	}{
		{"auth-first", "/sync/pull", "", "", "", 401},
		{"protocol-before-missing-source", "/sync/pull", "user", "", "", 426},
		{"protocol-before-invalid-source", "/sync/pull", "user", "v2", " invalid", 426},
		{"source-after-protocol", "/sync/pull", "user", "v1", " invalid", 400},
		{"admitted", "/sync/pull", "user", "v1", "source-a", 204},
		{"base-prefix", "/api/v1/sync/pull", "user", "", "", 426},
		{"operational-exempt", "/syncx/health", "user", "", "source-a", 204},
	} {
		t.Run(c.name, func(t *testing.T) {
			handler := ActorMiddleware(ActorMiddlewareConfig{UserIDFromContext: func(context.Context) (string, error) { return c.user, nil }})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
			r := httptest.NewRequest(http.MethodGet, c.path, nil)
			if c.protocol != "" {
				r.Header.Set(ProtocolVersionHeader, c.protocol)
			}
			if c.source != "" {
				r.Header.Set(SourceIDHeader, c.source)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != c.status {
				t.Fatalf("status %d want %d", w.Code, c.status)
			}
		})
	}
}
