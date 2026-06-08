package oversync

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewRuntimeService_ConstructsWithoutBootstrapping(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		MaxSupportedSchemaVersion: 2,
		AppName:                   "runtime-only-test",
		RegisteredTables: []RegisteredTable{
			{Schema: "business", Table: "users"},
		},
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected service instance")
	}
	if svc.discoveredSchema != nil {
		t.Fatalf("expected runtime constructor to avoid topology discovery")
	}
	if got := svc.GetSchemaVersion(); got != 2 {
		t.Fatalf("expected schema version 2, got %d", got)
	}
}

func TestBundleChangeWatchConfig_DefaultsDisabled(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{AppName: "watch-defaults-test"}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	cfg := svc.effectiveBundleChangeWatchConfig()
	if cfg.Enabled {
		t.Fatal("expected bundle change watch to default disabled")
	}
	if cfg.NotifyChannel != defaultBundleChangeNotifyChannel {
		t.Fatalf("expected default notify channel %q, got %q", defaultBundleChangeNotifyChannel, cfg.NotifyChannel)
	}
	if cfg.HeartbeatInterval != defaultBundleChangeHeartbeatInterval {
		t.Fatalf("expected default heartbeat %s, got %s", defaultBundleChangeHeartbeatInterval, cfg.HeartbeatInterval)
	}
	if svc.GetCapabilities().Features["bundle_change_watch"] {
		t.Fatalf("expected disabled watch capability, got %#v", svc.GetCapabilities().Features)
	}
}

func TestBundleChangeWatchConfig_DefaultHeartbeatWhenEnabled(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-enabled-defaults-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled: true,
		},
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	cfg := svc.effectiveBundleChangeWatchConfig()
	if !cfg.Enabled {
		t.Fatal("expected bundle change watch to be enabled")
	}
	if cfg.NotifyChannel != defaultBundleChangeNotifyChannel {
		t.Fatalf("expected default notify channel %q, got %q", defaultBundleChangeNotifyChannel, cfg.NotifyChannel)
	}
	if cfg.HeartbeatInterval != defaultBundleChangeHeartbeatInterval {
		t.Fatalf("expected default heartbeat %s, got %s", defaultBundleChangeHeartbeatInterval, cfg.HeartbeatInterval)
	}
	if !svc.GetCapabilities().Features["bundle_change_watch"] {
		t.Fatalf("expected enabled watch capability, got %#v", svc.GetCapabilities().Features)
	}
}

func TestBundleChangeWatchConfig_RejectsEnabledNonPositiveHeartbeat(t *testing.T) {
	_, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-invalid-heartbeat-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled:           true,
			HeartbeatInterval: -time.Second,
		},
	}, slog.Default())
	if err == nil {
		t.Fatal("expected constructor error")
	}
	if !strings.Contains(err.Error(), "heartbeat interval must be positive") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBundleChangeWatchConfig_RejectsInvalidNotifyChannel(t *testing.T) {
	_, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "watch-invalid-channel-test",
		BundleChangeWatch: BundleChangeWatchConfig{
			Enabled:       true,
			NotifyChannel: strings.Repeat("x", 64),
		},
	}, slog.Default())
	if err == nil {
		t.Fatal("expected constructor error")
	}
	if !strings.Contains(err.Error(), "notify channel") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSyncService_BootstrapRequiresPool(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "bootstrap-no-pool-test",
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	err = svc.Bootstrap(context.Background())
	if err == nil {
		t.Fatal("expected bootstrap to fail without pool")
	}
}

func TestSyncService_CloseWaitsForInflightOperations(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{AppName: "close-test"}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	done, err := svc.beginOperation()
	if err != nil {
		t.Fatalf("unexpected beginOperation error: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- svc.Close(context.Background())
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("close returned before in-flight operation drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if status, err := svc.GetStatus(context.Background()); err != nil {
		t.Fatalf("unexpected status error: %v", err)
	} else {
		if status.Lifecycle != "shutting_down" {
			t.Fatalf("expected shutting_down lifecycle, got %q", status.Lifecycle)
		}
		if status.AcceptingOperations {
			t.Fatalf("expected service to reject new operations while shutting down")
		}
		if status.Status != "unhealthy" {
			t.Fatalf("expected unhealthy status while shutting down, got %q", status.Status)
		}
	}

	if _, err := svc.CreatePushSession(context.Background(), Actor{UserID: "user", SourceID: "device"}, nil); err != errServiceShuttingDown {
		t.Fatalf("expected errServiceShuttingDown, got %v", err)
	}

	done()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("unexpected close error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not finish after in-flight operation drained")
	}

	if status, err := svc.GetStatus(context.Background()); err != nil {
		t.Fatalf("unexpected status error: %v", err)
	} else if status.Lifecycle != "closed" {
		t.Fatalf("expected closed lifecycle, got %q", status.Lifecycle)
	}
}

func TestSyncService_GetStatusHealthyByDefault(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "status-test",
		RegisteredTables: []RegisteredTable{
			{Schema: "business", Table: "users"},
		},
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	status, err := svc.GetStatus(context.Background())
	if err != nil {
		t.Fatalf("unexpected status error: %v", err)
	}
	if status.Status != "healthy" {
		t.Fatalf("expected healthy status, got %q", status.Status)
	}
	if status.Lifecycle != "running" {
		t.Fatalf("expected running lifecycle, got %q", status.Lifecycle)
	}
	if !status.AcceptingOperations {
		t.Fatalf("expected service to accept operations")
	}
	if status.UserStateRetentionFloorAheadCount != 0 {
		t.Fatalf("expected zero invariant violations, got %#v", status)
	}
}

func TestSyncService_GetCapabilities_DoesNotAdvertiseVisibleSyncKeyType(t *testing.T) {
	svc, err := NewRuntimeService(nil, &ServiceConfig{
		AppName: "capabilities-test",
		RegisteredTables: []RegisteredTable{
			{Schema: "business", Table: "docs", SyncKeyColumns: []string{"doc_id"}},
		},
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	payload, err := json.Marshal(svc.GetCapabilities())
	if err != nil {
		t.Fatalf("marshal capabilities: %v", err)
	}
	if string(payload) == "" {
		t.Fatal("expected non-empty capabilities payload")
	}
	if strings.Contains(string(payload), `"sync_key_type"`) || strings.Contains(string(payload), `"_sync_scope_id"`) {
		t.Fatalf("expected hidden-owner design to stay out of capabilities payload, got %s", payload)
	}
}

func TestSyncService_DoesNotExposeRuntimeTopologyRefreshAPI(t *testing.T) {
	if _, ok := reflect.TypeOf(&SyncService{}).MethodByName("RefreshTopology"); ok {
		t.Fatal("expected runtime topology refresh to remain non-public; restart-only behavior is the supported contract")
	}
}

func TestSyncService_UploadLockTimeoutMillis_DefaultDisabled(t *testing.T) {
	svc := &SyncService{config: &ServiceConfig{}}
	if ms, ok := svc.uploadLockTimeoutMillis(); ok || ms != 0 {
		t.Fatalf("expected upload lock timeout to be disabled by default, got ms=%d ok=%t", ms, ok)
	}
}

func TestSyncService_UploadLockTimeoutMillis_Configured(t *testing.T) {
	svc := &SyncService{config: &ServiceConfig{UploadLockTimeout: 2500 * time.Millisecond}}
	ms, ok := svc.uploadLockTimeoutMillis()
	if !ok {
		t.Fatal("expected upload lock timeout to be enabled")
	}
	if ms != 2500 {
		t.Fatalf("expected 2500ms, got %d", ms)
	}
}

func TestSyncService_UploadLockTimeoutMillis_RoundsSubMillisecondUp(t *testing.T) {
	svc := &SyncService{config: &ServiceConfig{UploadLockTimeout: 500 * time.Microsecond}}
	ms, ok := svc.uploadLockTimeoutMillis()
	if !ok {
		t.Fatal("expected upload lock timeout to be enabled")
	}
	if ms != 1 {
		t.Fatalf("expected sub-millisecond timeout to round up to 1ms, got %d", ms)
	}
}
