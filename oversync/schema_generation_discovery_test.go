package oversync

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func discoveryTestService(t *testing.T) *SyncService {
	t.Helper()
	tables := []RegisteredTable{{Schema: "app", Table: "docs", SyncKeyColumns: []string{"id"}}}
	registry, err := CompileSchemaGenerationRegistry(tables, testSchemaGenerationsForRegisteredTables(tables))
	require.NoError(t, err)
	return &SyncService{config: &ServiceConfig{AppName: "discovery-test"}, schemaGenerationRegistry: registry, logger: slog.Default()}
}

func TestGenerationDiscoveryDirectReadOnly(t *testing.T) {
	s := discoveryTestService(t)
	actor := Actor{UserID: "user", SourceID: "unbound-source"}
	response, err := s.GetGeneration(context.Background(), actor, 1)
	require.NoError(t, err)
	contract, _ := s.schemaGenerationRegistry.Generation(1)
	require.Equal(t, contract.Fingerprint(), response.SchemaGeneration.Fingerprint)
	require.Equal(t, "1", response.SchemaGeneration.ID)
	require.Equal(t, SyncProtocolVersion, response.ProtocolVersion)
	require.Equal(t, "docs", response.SchemaGeneration.Tables[0].Table)
	require.Zero(t, s.inFlightOps)
	// No pool or generation view exists: lookup cannot touch binding, quota,
	// readiness, progress or session tables, or depend on a bound generation.
	response.SchemaGeneration.Tables[0].Columns[0].Name = "mutated"
	again, err := s.GetGeneration(context.Background(), Actor{UserID: "other", SourceID: "bound-source"}, 1)
	require.NoError(t, err)
	require.NotEqual(t, "mutated", again.SchemaGeneration.Tables[0].Columns[0].Name)
	for _, actor := range []Actor{{SourceID: "s"}, {UserID: "u"}, {UserID: "u", SourceID: "bad source"}} {
		_, err := s.GetGeneration(context.Background(), actor, 1)
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.GetGeneration(ctx, actor, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGenerationDiscoveryIgnoresOperationalReadinessButHonorsShutdown(t *testing.T) {
	s := discoveryTestService(t)
	actor := Actor{UserID: "user", SourceID: "source"}
	s.readinessWithdrawn = true
	s.bootstrapReadiness = bootstrapReadinessNotReady
	_, err := s.GetGeneration(context.Background(), actor, 1)
	require.NoError(t, err)
	require.Zero(t, s.inFlightOps)

	s.lifecycle = serviceLifecycleShuttingDown
	_, err = s.GetGeneration(context.Background(), actor, 1)
	require.ErrorIs(t, err, errServiceShuttingDown)
	require.Zero(t, s.inFlightOps)

	s.lifecycle = serviceLifecycleClosed
	_, err = s.GetGeneration(context.Background(), actor, 1)
	require.ErrorContains(t, err, "closed")
	require.Zero(t, s.inFlightOps)
}

func TestGenerationDiscoveryHTTP(t *testing.T) {
	for _, tc := range []struct {
		name, id, source, proof string
		status                  int
		code                    string
	}{
		{"unbound", "1", "source", "", 200, ""},
		{"missing source", "1", "", "", 400, "invalid_request"},
		{"invalid source", "1", "bad source", "", 400, "invalid_request"},
		{"leading zero", "01", "source", "", 400, "invalid_generation_discovery_request"},
		{"percent encoded digit", "%31", "source", "", 400, "invalid_generation_discovery_request"},
		{"overflow", "2147483648", "source", "", 400, "invalid_generation_discovery_request"},
		{"missing ID", "", "source", "", 400, "invalid_generation_discovery_request"},
		{"unexpected suffix", "1/other", "source", "", 400, "invalid_generation_discovery_request"},
		{"ID proof supplied", "1", "source", SchemaGenerationIDHeader, 400, "invalid_generation_discovery_request"},
		{"fingerprint supplied", "1", "source", SchemaGenerationFingerprintHeader, 400, "invalid_generation_discovery_request"},
		{"unknown", "2", "source", "", 409, "unknown_schema_generation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := discoveryTestService(t)
			h := NewHTTPSyncHandlers(s, slog.Default())
			mux := http.NewServeMux()
			middleware := ActorMiddleware(ActorMiddlewareConfig{UserIDFromContext: func(context.Context) (string, error) { return "user", nil }})
			generationHandler := middleware(http.HandlerFunc(h.HandleGeneration))
			mux.Handle("GET /sync/generations/{generation_id}", generationHandler)
			mux.Handle("GET /sync/generations/", generationHandler)
			req := httptest.NewRequest(http.MethodGet, "/sync/generations/"+tc.id, nil)
			req.Header.Set(SourceIDHeader, tc.source)
			if tc.proof != "" {
				req.Header.Set(tc.proof, "")
			}
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			require.Equal(t, tc.status, rec.Code, rec.Body.String())
			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			if tc.status == 200 {
				require.Len(t, body, 4)
			} else {
				require.Equal(t, tc.code, body["error"])
			}
			if tc.code == "invalid_generation_discovery_request" || tc.code == "unknown_schema_generation" {
				require.Equal(t, false, body["retryable"])
				require.NotEmpty(t, body["remediation"])
				require.NotContains(t, body, "schema_generation_fingerprint")
			}
		})
	}
}

func TestGenerationDiscoveryHTTPLeaseSpansResponseWrite(t *testing.T) {
	s := discoveryTestService(t)
	h := NewHTTPSyncHandlers(s, slog.Default())
	middleware := ActorMiddleware(ActorMiddlewareConfig{UserIDFromContext: func(context.Context) (string, error) { return "user", nil }})
	handler := middleware(http.HandlerFunc(h.HandleGeneration))
	req := httptest.NewRequest(http.MethodGet, "/sync/generations/1", nil)
	req.SetPathValue("generation_id", "1")
	req.Header.Set(SourceIDHeader, "source")
	writer := newBlockingDiscoveryWriter()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(writer, req)
		close(done)
	}()
	<-writer.started
	require.Equal(t, 1, s.inFlightOps)
	close(writer.release)
	<-done
	require.Zero(t, s.inFlightOps)
}

type blockingDiscoveryWriter struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingDiscoveryWriter() *blockingDiscoveryWriter {
	return &blockingDiscoveryWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingDiscoveryWriter) Header() http.Header { return w.header }
func (w *blockingDiscoveryWriter) WriteHeader(int)     {}
func (w *blockingDiscoveryWriter) Write(body []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(body), nil
}

func TestGenerationDiscoveryRetirementPrecedesRegistration(t *testing.T) {
	s := discoveryTestService(t)
	contract, _ := s.schemaGenerationRegistry.Generation(1)
	s.config.Readiness = &ReadinessConfig{}
	s.config.Readiness.Expected.GenerationLedger = &GenerationLedger{Entries: []GenerationLifecycle{{Proof: SchemaGenerationProof{ID: 1, Fingerprint: contract.Fingerprint()}, State: "retired"}, {Proof: SchemaGenerationProof{ID: 2}, State: "retired"}}}
	for _, id := range []SchemaGenerationID{1, 2} {
		_, err := s.GetGeneration(context.Background(), Actor{UserID: "u", SourceID: "s"}, id)
		var generationErr *SchemaGenerationRequestError
		require.ErrorAs(t, err, &generationErr)
		require.Equal(t, SchemaGenerationRetired, generationErr.Code)
	}
}

func TestGenerationDiscoveryHTTPPrefixMountedMux(t *testing.T) {
	s := discoveryTestService(t)
	h := NewHTTPSyncHandlers(s, slog.Default())
	inner := http.NewServeMux()
	middleware := ActorMiddleware(ActorMiddlewareConfig{UserIDFromContext: func(context.Context) (string, error) { return "user", nil }})
	handler := middleware(http.HandlerFunc(h.HandleGeneration))
	inner.Handle("GET /sync/generations/{generation_id}", handler)
	inner.Handle("GET /sync/generations/", handler)
	outer := http.NewServeMux()
	outer.Handle("/host/", http.StripPrefix("/host", inner))

	req := httptest.NewRequest(http.MethodGet, "/host/sync/generations/1", nil)
	req.Header.Set(SourceIDHeader, "source")
	rec := httptest.NewRecorder()
	outer.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestGenerationControlResponseSizeBoundary(t *testing.T) {
	s := discoveryTestService(t)
	require.NoError(t, s.validateGenerationResponseSizes())
	tables := []RegisteredTable{{Schema: "app", Table: "docs", SyncKeyColumns: []string{"id"}}}
	_, err := CompileSchemaGenerationRegistry(tables, []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{
		Schema: "app", Table: "docs", Columns: []GenerationColumn{
			{Name: "id", WireType: GenerationWireTypeText},
			{Name: strings.Repeat("x", generationControlResponseLimit*2), WireType: GenerationWireTypeText},
		},
	}}}})
	require.Error(t, err)
}
