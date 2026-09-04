package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/exampleauth"
	"github.com/mobiletoly/go-oversync/internal/syncroute"
	"github.com/mobiletoly/go-oversync/oversync"
)

func memberAdmissionDigest(input LocalMemberConfig) (string, error) {
	if input.JWTSecret == "" || input.ClientPolicy == "" || len(input.ClientTokens) < 1 || len(input.ClientTokens) > 64 {
		return "", fmt.Errorf("qualified_member_client_admission_required")
	}
	tokens := slices.Clone(input.ClientTokens)
	slices.Sort(tokens)
	for i, token := range tokens {
		if token == "" || i > 0 && token == tokens[i-1] {
			return "", fmt.Errorf("invalid_member_client_admission")
		}
	}
	b, err := json.Marshal(struct {
		Policy         string
		Tokens         []string
		VerifierSHA256 string
	}{input.ClientPolicy, tokens, digest([]byte(input.JWTSecret))})
	if err != nil {
		return "", err
	}
	return digest(b), nil
}

func registerMemberSyncRoutes(mux *http.ServeMux, s *oversync.SyncService, input LocalMemberConfig) http.Handler {
	auth := exampleauth.New(input.JWTSecret)
	actors := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{ReservedServerSourceIDs: []string{"schema-evolution-writer", "schema-evolution-backfill"}, UserIDFromContext: func(ctx context.Context) (string, error) {
		id, ok := exampleauth.UserIDFromContext(ctx)
		if !ok || id == "" {
			return "", fmt.Errorf("authenticated_user_required")
		}
		return id, nil
	}})
	allowed := func(r *http.Request) bool { return slices.Contains(input.ClientTokens, r.Header.Get("Authorization")) }
	authenticate := func(next http.Handler) http.Handler {
		return auth.Middleware(ClientRuntimeGuard(allowed, actors(next)))
	}
	handlers := oversync.NewHTTPSyncHandlers(s, nil)
	routes := []struct {
		pattern string
		handler http.HandlerFunc
	}{
		{"GET /sync/generations/{generation_id}", handlers.HandleGeneration},
		{"GET /sync/generations/", handlers.HandleGeneration},
		{"POST /sync/connect", handlers.HandleConnect}, {"POST /sync/push", handlers.HandlePush}, {"GET /sync/pull", handlers.HandlePull},
		{"GET /sync/watch", handlers.HandleWatch}, {"POST /sync/snapshot-sessions", handlers.HandleCreateSnapshotSession},
		{"GET /sync/snapshot-sessions/{snapshot_id}", handlers.HandleGetSnapshotChunk}, {"DELETE /sync/snapshot-sessions/{snapshot_id}", handlers.HandleDeleteSnapshotSession},
	}
	for _, route := range routes {
		mux.Handle(route.pattern, route.handler)
	}
	dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if syncroute.IsRootGenerationDiscoveryPath(r) {
			handlers.HandleGeneration(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
	authenticated := authenticate(dispatch)
	operational := s.ReadinessGuard(authenticated)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if syncroute.IsRootGenerationDiscoveryPath(r) {
			authenticated.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/sync/") {
			operational.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
