package oversync

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
	req.Header.Set(SourceIDHeader, " source-1 ")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, Actor{UserID: "user-1", SourceID: "source-1"}, got)
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
