package oversync

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActorMiddleware_InjectsActorFromContextAndHeader(t *testing.T) {
	middleware := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(ctx context.Context) (string, error) {
			return " user-1 ", nil
		},
	})

	var got Actor
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ok bool
		got, ok = ActorFromContext(r.Context())
		require.True(t, ok)
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	req.Header.Set(SourceIDHeader, "source-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, Actor{UserID: " user-1 ", SourceID: "source-1"}, got)
}

func TestActorMiddleware_FailsClosedWhenUserIDCannotBeResolved(t *testing.T) {
	middleware := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(ctx context.Context) (string, error) {
			return "", errors.New("missing principal")
		},
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not run")
	}))

	req := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
	req.Header.Set(SourceIDHeader, "source-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "authentication_failed", decodeErrorResponse(t, rec).Error)
}

func TestActorMiddleware_FailsClosedWhenSourceHeaderIsMissingOrInvalid(t *testing.T) {
	middleware := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(ctx context.Context) (string, error) {
			return "user-1", nil
		},
	})

	for _, tc := range []struct {
		name        string
		headerValue string
	}{
		{name: "missing"},
		{name: "blank", headerValue: "   "},
		{name: "leading space", headerValue: " source-1"},
		{name: "trailing space", headerValue: "source-1 "},
		{name: "control", headerValue: "source\x01id"},
		{name: "unicode", headerValue: "söurce"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("next handler should not run")
			}))

			req := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
			if tc.headerValue != "" {
				req.Header.Set(SourceIDHeader, tc.headerValue)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, "invalid_request", decodeErrorResponse(t, rec).Error)
		})
	}
}

func TestActorMiddleware_RejectsDuplicateAndCaseCollidingSourceHeaders(t *testing.T) {
	middleware := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext: func(context.Context) (string, error) { return "user-1", nil },
	})
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("duplicate source headers must not reach the handler")
	}))
	for _, tc := range []struct {
		name    string
		headers http.Header
	}{
		{name: "duplicate values", headers: http.Header{SourceIDHeader: {"source-1", "source-1"}}},
		{name: "case-colliding names", headers: http.Header{SourceIDHeader: {"source-1"}, strings.ToLower(SourceIDHeader): {"source-1"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/sync/pull", nil)
			req.Header = tc.headers
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, "invalid_request", decodeErrorResponse(t, rec).Error)
		})
	}
}

func TestActorMiddleware_RejectsReservedServerSourceWithoutEnumeration(t *testing.T) {
	middleware := ActorMiddleware(ActorMiddlewareConfig{
		UserIDFromContext:       func(context.Context) (string, error) { return "user-1", nil },
		ReservedServerSourceIDs: []string{"server-writer"},
	})
	handler := middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("reserved server source must not reach the handler")
	}))
	req := httptest.NewRequest(http.MethodPost, "/sync/push", nil)
	req.Header.Set(SourceIDHeader, "server-writer")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	response := decodeErrorResponse(t, rec)
	require.Equal(t, "invalid_request", response.Error)
	require.NotContains(t, response.Message, "reserved")
	require.NotContains(t, response.Message, "server-writer")
}
