package oversync

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadiness_EvidenceFailures(t *testing.T) {
	registry, err := CompileSchemaGenerationRegistry(runtimeServiceTestRegisteredTables(), testSchemaGenerationsForRegisteredTables(runtimeServiceTestRegisteredTables()))
	require.NoError(t, err)
	base := baselineTestReadiness(registry)
	m := MigrationIdentity{Version: 1, Filename: "migrations/0001_people.sql", SHA256: strings.Repeat("a", 64), Transactional: true, Class: "expand"}
	base.Expected.Migrations = []MigrationIdentity{m}
	evidence := ReadinessEvidence{Deployment: base.Expected, Migrations: []MigrationEvidence{{Identity: m, State: "completed", StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0)}}}
	for _, g := range base.Generations {
		evidence.Generations = append(evidence.Generations, GenerationEnablement{Requirement: g, DeploymentRevision: 1, Enabled: true})
	}
	require.NoError(t, validateReadinessEvidence(base, evidence))
	tests := []struct {
		name     string
		category ReadinessCategory
		change   func(*ReadinessEvidence)
	}{
		{"missing", MigrationMissing, func(e *ReadinessEvidence) { e.Migrations = nil }},
		{"ahead", MigrationAhead, func(e *ReadinessEvidence) { e.Migrations = append(e.Migrations, e.Migrations[0]) }},
		{"filename", MigrationChecksumMismatch, func(e *ReadinessEvidence) { e.Migrations[0].Identity.Filename = "renamed.sql" }},
		{"checksum", MigrationChecksumMismatch, func(e *ReadinessEvidence) { e.Migrations[0].Identity.SHA256 = strings.Repeat("A", 64) }},
		{"partial", MigrationIncomplete, func(e *ReadinessEvidence) { e.Migrations[0].State = "started" }},
		{"deployment", DeploymentContractMismatch, func(e *ReadinessEvidence) { e.Deployment.Revision++ }},
		{"mechanism", TransitionMechanismMismatch, func(e *ReadinessEvidence) { e.Mechanisms = []MechanismIdentity{{Kind: "trigger", Identity: "missing"}} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := evidence
			e.Migrations = append([]MigrationEvidence(nil), evidence.Migrations...)
			test.change(&e)
			var failure *ReadinessError
			require.ErrorAs(t, validateReadinessEvidence(base, e), &failure)
			require.Equal(t, test.category, failure.Category)
		})
	}
}

func TestReadiness_BootstrapFailClosedAndHTTPGuard(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	require.NoError(t, resetTestBusinessSchema(ctx, pool, "readiness"))
	tables := []RegisteredTable{{Schema: "readiness", Table: "users", SyncKeyColumns: []string{"id"}}, {Schema: "readiness", Table: "posts", SyncKeyColumns: []string{"id"}}, {Schema: "readiness", Table: "files", SyncKeyColumns: []string{"id"}}, {Schema: "readiness", Table: "file_reviews", SyncKeyColumns: []string{"id"}}}
	cfg := &ServiceConfig{RegisteredTables: tables, SchemaGenerations: testSchemaGenerationsForRegisteredTables(tables), RegisteredTableCatalogRevision: 1}
	service, err := NewRuntimeService(pool, cfg, nil)
	require.NoError(t, err)
	require.Error(t, service.Bootstrap(ctx))
	require.False(t, service.Readiness().Ready)
	var exists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&exists))
	require.False(t, exists)
	called := false
	handler := service.ReadinessGuard(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("POST", "/sync/push", nil))
	require.Equal(t, 503, recorder.Code)
	require.Contains(t, recorder.Body.String(), "service_not_ready")
	require.False(t, called)
	_, err = service.beginOperation()
	require.ErrorIs(t, err, errServiceNotReady)
	service.config.Readiness = baselineTestReadiness(service.schemaGenerationRegistry)
	service.config.Readiness.Adapter = readinessAdapterFunc(func(context.Context, DatabaseReadinessReader) (ReadinessEvidence, error) {
		return ReadinessEvidence{}, errors.New("private evidence failure")
	})
	require.Error(t, service.Bootstrap(ctx))
	require.Equal(t, ReadinessEvidenceUnavailable, service.Readiness().FailureCategory)
	service.config.Readiness = baselineTestReadiness(service.schemaGenerationRegistry)
	require.NoError(t, service.Bootstrap(ctx))
	require.True(t, service.Readiness().Ready)
	service.WithdrawReadiness(DeploymentContractMismatch)
	require.False(t, service.Readiness().Ready)
	require.ErrorIs(t, service.Bootstrap(ctx), errServiceNotReady)
	require.NoError(t, service.Close(ctx))
}

func TestReadiness_ApplicationRejectionHTTP(t *testing.T) {
	recorder := httptest.NewRecorder()
	handler := &HTTPSyncHandlers{logger: slog.New(slog.DiscardHandler)}
	require.True(t, handler.writePushTransactionFailure(recorder, nil, &ApplicationWriteRejectedError{Category: "transition_input_invalid"}))
	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	require.Contains(t, recorder.Body.String(), "transition_input_invalid")
	require.Contains(t, recorder.Body.String(), "pending intent")
}
