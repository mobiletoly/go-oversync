package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHTTPSyncHandlers_WritePushTransactionFailurePreservesCommitAmbiguity(t *testing.T) {
	h := NewHTTPSyncHandlers(nil, slog.Default())
	rec := httptest.NewRecorder()

	handled := h.writePushTransactionFailure(rec, nil, &CommitOutcomeUnknownError{Err: errors.New("lost commit response")})

	require.True(t, handled)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	response := decodeErrorResponse(t, rec)
	require.Equal(t, "commit_outcome_unknown", response.Error)
	require.Equal(t, "Push transaction commit outcome is unknown; retry the same complete push with the same source tuple", response.Message)
}

func TestNewHTTPSyncHandlersWithConfig_DefaultsNilLogger(t *testing.T) {
	h := NewHTTPSyncHandlersWithConfig(nil, nil, HTTPSyncHandlersConfig{})
	recorder := httptest.NewRecorder()

	require.NotPanics(t, func() {
		h.writeError(recorder, http.StatusBadRequest, "test_error", "test error")
	})
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestSyncService_GenerationDiscoveryModel(t *testing.T) {
	tables := []RegisteredTable{
		{Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
		{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
	}
	registry, err := CompileSchemaGenerationRegistry(tables, testSchemaGenerationsForRegisteredTables(tables))
	require.NoError(t, err)
	contract, _ := registry.Generation(registry.GenerationIDs()[0])
	svc := &SyncService{config: &ServiceConfig{
		MaxRowsPerBundle:           250,
		MaxBytesPerBundle:          4096,
		MaxBytesPerPullResponse:    8192,
		MaxRowsPerSnapshotChunk:    45,
		MaxBytesPerSnapshotChunk:   5678,
		MaxRowsPerSnapshotSession:  890,
		MaxBytesPerSnapshotSession: 98765,
		MaxBytesPerSnapshotRow:     1234,
	}, logger: slog.Default()}

	response := svc.generationDiscoveryResponse(contract, true)
	require.Equal(t, SyncProtocolVersion, response.ProtocolVersion)
	require.Equal(t, generationDescriptor(contract), response.SchemaGeneration)
	require.True(t, response.WatchEnabled)
	require.Equal(t, WireLimits{
		MaxPushRows:                     250,
		MaxPushDecodedBytes:             4096 + historyEnvelopeAllowance,
		MaxPullBundles:                  defaultMaxBundlesPerPull,
		MaxPullDecodedBytes:             8192,
		MaxSnapshotRowsPerChunk:         45,
		MaxSnapshotDecodedBytesPerChunk: 5678,
		MaxSnapshotRows:                 890,
		MaxSnapshotLogicalBytes:         98765,
		MaxRowDecodedBytes:              1234,
	}, response.Limits)
}

func TestHTTPSyncHandlers_HandleGenerationRequiresAuthentication(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			AppName:                            "handler-test",
			MaxConcurrentSnapshotBuilds:        defaultMaxConcurrentSnapshotBuilds,
			MaxConcurrentSnapshotChunkRequests: defaultMaxConcurrentSnapshotChunkRequests,
			DefaultBytesPerSnapshotChunk:       defaultBytesPerSnapshotChunk,
			MaxBytesPerSnapshotChunk:           defaultMaxBytesPerSnapshotChunk,
			MaxBytesPerSnapshotRow:             defaultMaxBytesPerSnapshotRow,
			RegisteredTableCatalogRevision:     1,
		},
		logger: slog.Default(),
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/sync/generations/1", nil)
	rec := httptest.NewRecorder()

	h.HandleGeneration(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHTTPSyncHandlers_GenerationDiscoveryAppliesActorWatchPolicy(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			AppName:                            "handler-watch-policy-test",
			MaxConcurrentSnapshotBuilds:        defaultMaxConcurrentSnapshotBuilds,
			MaxConcurrentSnapshotChunkRequests: defaultMaxConcurrentSnapshotChunkRequests,
			DefaultBytesPerSnapshotChunk:       defaultBytesPerSnapshotChunk,
			MaxBytesPerSnapshotChunk:           defaultMaxBytesPerSnapshotChunk,
			MaxBytesPerSnapshotRow:             defaultMaxBytesPerSnapshotRow,
			BundleChangeWatch: BundleChangeWatchConfig{
				Enabled: true,
			},
			RegisteredTableCatalogRevision: 1,
		},
		logger: slog.Default(),
	}
	h := NewHTTPSyncHandlersWithConfig(svc, slog.Default(), HTTPSyncHandlersConfig{
		BundleChangeWatchAllowed: func(_ context.Context, actor Actor) bool {
			return actor.SourceID != "blocked-device"
		},
	})

	require.True(t, h.isBundleChangeWatchAllowed(context.Background(), Actor{SourceID: "allowed-device"}))
	require.False(t, h.isBundleChangeWatchAllowed(context.Background(), Actor{SourceID: "blocked-device"}))
}

func TestParseChunkQueryParams(t *testing.T) {
	t.Run("defaults when query params are absent", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/sync/chunks", nil)

		params, err := parseChunkQueryParams(req, 25)

		require.NoError(t, err)
		require.Nil(t, params.afterRowOrdinal)
		require.Equal(t, 25, params.maxRows)
	})

	t.Run("parses explicit params", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/sync/chunks?after_row_ordinal=7&max_rows=11", nil)

		params, err := parseChunkQueryParams(req, 25)

		require.NoError(t, err)
		require.NotNil(t, params.afterRowOrdinal)
		require.EqualValues(t, 7, *params.afterRowOrdinal)
		require.Equal(t, 11, params.maxRows)
	})

	t.Run("does not trim query tokens", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/sync/chunks?after_row_ordinal=%207", nil)

		_, err := parseChunkQueryParams(req, 25)

		require.EqualError(t, err, "after_row_ordinal must be an integer")
	})

	t.Run("reports invalid max rows", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/sync/chunks?max_rows=many", nil)

		_, err := parseChunkQueryParams(req, 25)

		require.EqualError(t, err, "max_rows must be an integer")
	})
}

func TestOptionalScalarQueryParam_RejectsEmptyAndRepeatedValuesForEveryScalar(t *testing.T) {
	for _, name := range []string{
		"after_bundle_seq",
		"max_bundles",
		"target_bundle_seq",
		"after_row_ordinal",
		"max_rows",
		"max_bytes",
	} {
		name := name
		t.Run(name, func(t *testing.T) {
			tests := []struct {
				name        string
				rawQuery    string
				wantValue   string
				wantPresent bool
				wantError   bool
			}{
				{name: "omitted uses default", wantPresent: false},
				{name: "valid", rawQuery: name + "=7", wantValue: "7", wantPresent: true},
				{name: "empty", rawQuery: name + "=", wantPresent: true, wantError: true},
				{name: "repeated", rawQuery: name + "=7&" + name + "=8", wantPresent: true, wantError: true},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					req := httptest.NewRequest(http.MethodGet, "/sync/test?"+test.rawQuery, nil)

					value, present, err := optionalScalarQueryParam(req, name)

					require.Equal(t, test.wantPresent, present)
					if test.wantError {
						require.EqualError(t, err, name+" must be provided exactly once with a non-empty value")
						return
					}
					require.NoError(t, err)
					require.Equal(t, test.wantValue, value)
				})
			}
		})
	}
}

func TestParseChunkQueryParams_DefaultsApplyOnlyWhenKeysAreAbsent(t *testing.T) {
	for _, rawQuery := range []string{
		"after_row_ordinal=",
		"after_row_ordinal=1&after_row_ordinal=2",
		"max_rows=",
		"max_rows=1&max_rows=2",
	} {
		req := httptest.NewRequest(http.MethodGet, "/sync/chunks?"+rawQuery, nil)

		_, err := parseChunkQueryParams(req, 25)

		require.Error(t, err, rawQuery)
	}
}

func TestHTTPSyncHandlers_HandleHealthUsesStatusCodeForUnhealthyService(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{AppName: "health-test",
			RegisteredTableCatalogRevision: 1},
		logger:    slog.Default(),
		lifecycle: serviceLifecycleClosed,
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/syncx/health", nil)
	rec := httptest.NewRecorder()
	h.HandleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "unhealthy" || resp.Lifecycle != "closed" {
		t.Fatalf("unexpected health response: %#v", resp)
	}
}

func TestHTTPSyncHandlers_HandleHealthReportsWithdrawnReadiness(t *testing.T) {
	svc := &SyncService{
		config:             &ServiceConfig{AppName: "withdrawn-health-test", RegisteredTableCatalogRevision: 1},
		logger:             slog.Default(),
		lifecycle:          serviceLifecycleRunning,
		bootstrapReadiness: bootstrapReadinessReady,
	}
	svc.WithdrawReadiness(DeploymentContractMismatch)

	status, err := svc.GetStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, "unhealthy", status.Status)
	require.False(t, status.AcceptingOperations)

	h := NewHTTPSyncHandlers(svc, slog.Default())
	req := httptest.NewRequest(http.MethodGet, "/syncx/health", nil)
	rec := httptest.NewRecorder()
	h.HandleHealth(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHTTPSyncHandlers_HandleStatusReturnsLifecycleSnapshot(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
			{Schema: "business", Table: "users"},
		}), AppName: "status-test",
			RegisteredTables: []RegisteredTable{
				{Schema: "business", Table: "users"},
			},
			RegisteredTableCatalogRevision: 1,
		},
		logger:    slog.Default(),
		lifecycle: serviceLifecycleRunning,
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/syncx/status", nil)
	rec := httptest.NewRecorder()
	h.HandleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp StatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "healthy" || resp.Lifecycle != "running" {
		t.Fatalf("unexpected status response: %#v", resp)
	}
	if len(resp.RegisteredTables) != 1 || resp.RegisteredTables[0] != "business.users" {
		t.Fatalf("unexpected registered tables: %#v", resp.RegisteredTables)
	}
}

func TestHTTPSyncHandlers_HandleCreateSnapshotSessionRequiresAuthenticatedActor(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{AppName: "snapshot-auth-test",
			RegisteredTableCatalogRevision: 1},
		logger: slog.Default(),
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", nil)
	rec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", rec.Code)
	}
}

func TestHTTPSyncHandlers_HandleCreateSnapshotSessionRejectsMalformedBody(t *testing.T) {
	fixture := newSnapshotSessionFixture(t, "invalid_body", snapshotSessionFixtureOptions{})
	h := NewHTTPSyncHandlers(fixture.svc, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", bytes.NewBufferString(`{"source_replacement":`))
	req.Header.Set("Content-Type", "application/json")
	setTestSchemaGenerationHeaders(req, fixture.svc)
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: "snapshot-invalid-body-user", SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	errResp := decodeErrorResponse(t, rec)
	require.Equal(t, "invalid_request", errResp.Error)
}

func TestHTTPSyncHandlers_HandleCreateSnapshotSessionErrorMappings(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_handler_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
	}), AppName: "snapshot-handler-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, logger)
	h := NewHTTPSyncHandlers(svc, slog.Default())

	userID := "snapshot-handler-user-" + suffix
	oldSourceID := "writer-handler-old"
	firstNewSourceID := "writer-handler-new-a"
	secondNewSourceID := "writer-handler-new-b"
	otherOldSourceID := "writer-handler-old-b"

	oldWriter := Actor{UserID: userID, SourceID: oldSourceID}
	otherWriter := Actor{UserID: userID, SourceID: otherOldSourceID}
	mustPushUserBundle(t, ctx, svc, oldWriter, schemaName, 1, uuid.New(), "Alpha")
	mustPushUserBundle(t, ctx, svc, otherWriter, schemaName, 1, uuid.New(), "Bravo")

	initialReq := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", bytes.NewBufferString(fmt.Sprintf(`{
		"source_replacement": {
			"previous_source_id": %q,
			"new_source_id": %q,
			"reason": "history_pruned"
		}
	}`, oldSourceID, firstNewSourceID)))
	initialReq.Header.Set("Content-Type", "application/json")
	setTestSchemaGenerationHeaders(initialReq, svc)
	initialReq = initialReq.WithContext(ContextWithActor(initialReq.Context(), oldWriter))
	initialRec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(initialRec, initialReq)
	require.Equal(t, http.StatusOK, initialRec.Code)

	retiredReq := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", bytes.NewBufferString(fmt.Sprintf(`{
		"source_replacement": {
			"previous_source_id": %q,
			"new_source_id": %q,
			"reason": "history_pruned"
		}
	}`, oldSourceID, secondNewSourceID)))
	retiredReq.Header.Set("Content-Type", "application/json")
	setTestSchemaGenerationHeaders(retiredReq, svc)
	retiredReq = retiredReq.WithContext(ContextWithActor(retiredReq.Context(), oldWriter))
	retiredRec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(retiredRec, retiredReq)
	require.Equal(t, http.StatusConflict, retiredRec.Code)
	retiredResp := decodeErrorResponse(t, retiredRec)
	require.Equal(t, "source_retired", retiredResp.Error)

	replacementReq := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", bytes.NewBufferString(fmt.Sprintf(`{
		"source_replacement": {
			"previous_source_id": %q,
			"new_source_id": %q,
			"reason": "history_pruned"
		}
	}`, otherOldSourceID, firstNewSourceID)))
	replacementReq.Header.Set("Content-Type", "application/json")
	setTestSchemaGenerationHeaders(replacementReq, svc)
	replacementReq = replacementReq.WithContext(ContextWithActor(replacementReq.Context(), otherWriter))
	replacementRec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(replacementRec, replacementReq)
	require.Equal(t, http.StatusConflict, replacementRec.Code)
	replacementResp := decodeErrorResponse(t, replacementRec)
	require.Equal(t, "source_replacement_invalid", replacementResp.Error)
}
