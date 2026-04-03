package oversync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSyncService_GetCapabilities(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			MaxSupportedSchemaVersion:          3,
			AppName:                            "test-app",
			MaxRowsPerBundle:                   250,
			MaxBytesPerBundle:                  4096,
			MaxRowsPerPushChunk:                321,
			PushSessionTTL:                     17 * time.Second,
			DefaultRowsPerCommittedBundleChunk: 45,
			MaxRowsPerCommittedBundleChunk:     67,
			MaxRowsPerSnapshotSession:          890,
			MaxBytesPerSnapshotSession:         98765,
			RegisteredTables: []RegisteredTable{
				{Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
				{Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
			},
		},
		logger: slog.Default(),
	}

	caps := svc.GetCapabilities()
	if caps.ProtocolVersion != SyncProtocolVersion {
		t.Fatalf("expected protocol version %q, got %q", SyncProtocolVersion, caps.ProtocolVersion)
	}
	if caps.SchemaVersion != 3 {
		t.Fatalf("expected schema version 3, got %d", caps.SchemaVersion)
	}
	if caps.AppName != "test-app" {
		t.Fatalf("expected app name test-app, got %q", caps.AppName)
	}
	if len(caps.RegisteredTables) != 2 || caps.RegisteredTables[0] != "business.posts" || caps.RegisteredTables[1] != "business.users" {
		t.Fatalf("unexpected registered tables: %#v", caps.RegisteredTables)
	}
	if len(caps.RegisteredTableSpecs) != 2 {
		t.Fatalf("expected registered table specs, got %#v", caps.RegisteredTableSpecs)
	}
	if caps.RegisteredTableSpecs[0].Schema != "business" || caps.RegisteredTableSpecs[0].Table != "posts" || len(caps.RegisteredTableSpecs[0].SyncKeyColumns) != 1 || caps.RegisteredTableSpecs[0].SyncKeyColumns[0] != "id" {
		t.Fatalf("unexpected first registered table spec: %#v", caps.RegisteredTableSpecs[0])
	}
	if !caps.Features["bundle_pull"] || !caps.Features["snapshot_chunking"] || !caps.Features["capabilities_endpoint"] {
		t.Fatalf("expected core capability flags to be enabled: %#v", caps.Features)
	}
	if !caps.Features["history_pruned_errors"] {
		t.Fatalf("expected retention capability flags to be enabled: %#v", caps.Features)
	}
	if !caps.Features["bundle_push"] || !caps.Features["structured_sync_keys"] {
		t.Fatalf("expected new contract preview capability flags to be enabled: %#v", caps.Features)
	}
	if !caps.Features["push_session_chunking"] || !caps.Features["committed_bundle_row_fetch"] {
		t.Fatalf("expected push-session capability flags to be enabled: %#v", caps.Features)
	}
	if caps.Features["push_sessions"] {
		t.Fatalf("expected legacy push_sessions capability flag to be absent: %#v", caps.Features)
	}
	if caps.BundleLimits == nil ||
		caps.BundleLimits.MaxRowsPerBundle != 250 ||
		caps.BundleLimits.MaxBytesPerBundle != 4096 ||
		caps.BundleLimits.MaxBundlesPerPull != defaultMaxBundlesPerPull ||
		caps.BundleLimits.DefaultRowsPerPushChunk != defaultRowsPerPushChunk ||
		caps.BundleLimits.MaxRowsPerPushChunk != 321 ||
		caps.BundleLimits.PushSessionTTLSeconds != 17 ||
		caps.BundleLimits.DefaultRowsPerCommittedBundleChunk != 45 ||
		caps.BundleLimits.MaxRowsPerCommittedBundleChunk != 67 ||
		caps.BundleLimits.DefaultRowsPerSnapshotChunk != defaultRowsPerSnapshotChunk ||
		caps.BundleLimits.MaxRowsPerSnapshotChunk != defaultMaxRowsPerSnapshotChunk ||
		caps.BundleLimits.SnapshotSessionTTLSeconds != int(defaultSnapshotSessionTTL.Seconds()) ||
		caps.BundleLimits.MaxRowsPerSnapshotSession != 890 ||
		caps.BundleLimits.MaxBytesPerSnapshotSession != 98765 {
		t.Fatalf("unexpected bundle limits: %#v", caps.BundleLimits)
	}
}

func TestHTTPSyncHandlers_HandleCapabilities(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			MaxSupportedSchemaVersion: 2,
			AppName:                   "handler-test",
		},
		logger: slog.Default(),
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/sync/capabilities", nil)
	rec := httptest.NewRecorder()

	h.HandleCapabilities(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp CapabilitiesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ProtocolVersion != SyncProtocolVersion {
		t.Fatalf("expected protocol version %q, got %q", SyncProtocolVersion, resp.ProtocolVersion)
	}
	if resp.SchemaVersion != 2 {
		t.Fatalf("expected schema version 2, got %d", resp.SchemaVersion)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"sync_key_type"`)) {
		t.Fatalf("expected capabilities response to omit visible sync key type, got %s", rec.Body.String())
	}
}

func TestHTTPSyncHandlers_HandleHealthUsesStatusCodeForUnhealthyService(t *testing.T) {
	svc := &SyncService{
		config:    &ServiceConfig{AppName: "health-test"},
		logger:    slog.Default(),
		lifecycle: serviceLifecycleClosed,
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
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

func TestHTTPSyncHandlers_HandleStatusReturnsLifecycleSnapshot(t *testing.T) {
	svc := &SyncService{
		config: &ServiceConfig{
			AppName: "status-test",
			RegisteredTables: []RegisteredTable{
				{Schema: "business", Table: "users"},
			},
		},
		logger:    slog.Default(),
		lifecycle: serviceLifecycleRunning,
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
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
		config: &ServiceConfig{AppName: "snapshot-auth-test"},
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
	svc := &SyncService{
		config: &ServiceConfig{AppName: "snapshot-invalid-body-test"},
		logger: slog.Default(),
	}
	h := NewHTTPSyncHandlers(svc, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/sync/snapshot-sessions", bytes.NewBufferString(`{"source_replacement":`))
	req = req.WithContext(ContextWithActor(req.Context(), Actor{UserID: "snapshot-invalid-body-user", SourceID: "device-a"}))
	rec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	errResp := decodeErrorResponse(t, rec)
	require.Equal(t, "snapshot_session_invalid", errResp.Error)
}

func TestHTTPSyncHandlers_HandleCreateSnapshotSessionErrorMappings(t *testing.T) {
	ctx := context.Background()
	logger := integrationTestLogger(slog.LevelWarn)
	pool := newIntegrationTestPool(t, ctx)

	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	schemaName := "snapshot_handler_" + suffix
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schemaName))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schemaName) })

	svc := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "snapshot-handler-test",
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		},
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
	replacementReq = replacementReq.WithContext(ContextWithActor(replacementReq.Context(), otherWriter))
	replacementRec := httptest.NewRecorder()
	h.HandleCreateSnapshotSession(replacementRec, replacementReq)
	require.Equal(t, http.StatusConflict, replacementRec.Code)
	replacementResp := decodeErrorResponse(t, replacementRec)
	require.Equal(t, "source_replacement_invalid", replacementResp.Error)
}
