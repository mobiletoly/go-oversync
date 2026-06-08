package simulator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	serverpkg "github.com/mobiletoly/go-oversync/examples/nethttp_server/server"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

const watchScenarioFallbackInterval = 30 * time.Second

type watchScenarioHarness struct {
	serverURL  string
	base       *BaseScenario
	userID     string
	watcherApp *MobileApp
	writerApp  *MobileApp
	cancel     context.CancelFunc
}

func newWatchScenarioHarness(base *BaseScenario) *watchScenarioHarness {
	return &watchScenarioHarness{base: base}
}

func (h *watchScenarioHarness) setupTwoDevices(ctx context.Context) error {
	return h.setupTwoDevicesWithWatcherMode(ctx, true)
}

func (h *watchScenarioHarness) setupTwoDevicesWithWatcherMode(ctx context.Context, watcherWatch bool) error {
	logger := h.base.simulator.GetLogger()
	h.userID = watchScenarioUserID(h.base.config, "watch-user")
	logger.Info("🔧 Setting up watch scenario devices", "user_id", h.userID)

	watcherApp, err := h.createApp("watcher", "Watch Device", watcherWatch)
	if err != nil {
		return fmt.Errorf("create watcher app: %w", err)
	}
	h.watcherApp = watcherApp
	h.base.app = watcherApp
	h.base.simulator.currentApp = watcherApp

	writerApp, err := h.createApp("writer", "Writer Device", false)
	if err != nil {
		return fmt.Errorf("create writer app: %w", err)
	}
	h.writerApp = writerApp

	if err := h.watcherApp.onLaunch(ctx); err != nil {
		return fmt.Errorf("launch watcher app: %w", err)
	}
	if err := h.writerApp.onLaunch(ctx); err != nil {
		return fmt.Errorf("launch writer app: %w", err)
	}
	if err := signInWithoutSimulatorSync(ctx, h.watcherApp, h.userID); err != nil {
		return fmt.Errorf("sign in watcher app: %w", err)
	}
	if err := signInWithoutSimulatorSync(ctx, h.writerApp, h.userID); err != nil {
		return fmt.Errorf("sign in writer app: %w", err)
	}
	return nil
}

func (h *watchScenarioHarness) createApp(deviceSuffix, deviceName string, watch bool) (*MobileApp, error) {
	deviceID := "watch-" + deviceSuffix
	if h.base.config != nil && h.base.config.DeviceID != "" && h.base.config.DeviceID != "device-unknown-001" {
		deviceID = h.base.config.DeviceID + "-" + deviceSuffix
	}
	return h.base.simulator.newScenarioMobileApp(scenarioMobileAppOptions{
		UserID:           h.userID,
		DeviceID:         deviceID,
		DeviceName:       deviceName,
		DBNamePrefix:     deviceID,
		ServerURL:        h.serverURL,
		OversqliteConfig: watchScenarioOversqliteConfig(watch),
	})
}

func (h *watchScenarioHarness) startWatcher(ctx context.Context) error {
	if h.watcherApp == nil || h.watcherApp.client == nil {
		return fmt.Errorf("watcher app is not initialized")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	return h.watcherApp.client.Start(watchCtx)
}

func (h *watchScenarioHarness) stopWatcher() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
}

func (h *watchScenarioHarness) cleanup(context.Context) error {
	h.stopWatcher()
	var firstErr error
	if h.writerApp != nil {
		if err := h.writerApp.close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close writer app: %w", err)
		}
		h.writerApp = nil
	}
	if h.watcherApp != nil {
		if err := h.watcherApp.close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close watcher app: %w", err)
		}
		h.watcherApp = nil
	}
	return firstErr
}

type WatchPeerPushScenario struct {
	*BaseScenario
	harness *watchScenarioHarness
	rowID   string
}

func NewWatchPeerPushScenario(simulator *Simulator) Scenario {
	base := NewBaseScenario(simulator, "watch-peer-push")
	return &WatchPeerPushScenario{
		BaseScenario: base,
		harness:      newWatchScenarioHarness(base),
	}
}

func (s *WatchPeerPushScenario) Name() string { return "watch-peer-push" }

func (s *WatchPeerPushScenario) Description() string {
	return "Watch-enabled client wakes and pulls after a peer device pushes"
}

func (s *WatchPeerPushScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchPeerPushScenario) Setup(ctx context.Context) error {
	return s.harness.setupTwoDevices(ctx)
}

func (s *WatchPeerPushScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch peer-push scenario")
	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("start watch-enabled client: %w", err)
	}

	s.rowID = uuid.NewString()
	if err := s.harness.writerApp.createUserWithContext(ctx, s.rowID, "Watch Peer Ada", "watch.peer.ada@example.com"); err != nil {
		return fmt.Errorf("create peer row: %w", err)
	}
	report, err := s.harness.writerApp.client.Sync(ctx)
	if err != nil {
		return fmt.Errorf("writer sync: %w", err)
	}
	if report.PushOutcome != oversqlite.PushOutcomeCommitted {
		return fmt.Errorf("writer push outcome = %s, want %s", report.PushOutcome, oversqlite.PushOutcomeCommitted)
	}

	if err := waitForLocalUserState(ctx, s.harness.watcherApp.db, s.rowID, "Watch Peer Ada", report.Status.LastBundleSeqSeen, 5*time.Second); err != nil {
		return err
	}
	logger.Info("✅ Watch peer-push scenario converged", "row_id", s.rowID)
	return nil
}

func (s *WatchPeerPushScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	return verifier.VerifyBusinessRecordsExist(ctx, "business.users", s.harness.userID, []string{s.rowID})
}

func (s *WatchPeerPushScenario) Cleanup(ctx context.Context) error {
	return s.harness.cleanup(ctx)
}

type WatchServerOriginatedScenario struct {
	*BaseScenario
	harness *watchScenarioHarness
	rowID   uuid.UUID
}

func NewWatchServerOriginatedScenario(simulator *Simulator) Scenario {
	base := NewBaseScenario(simulator, "watch-server-originated")
	return &WatchServerOriginatedScenario{
		BaseScenario: base,
		harness:      newWatchScenarioHarness(base),
	}
}

func (s *WatchServerOriginatedScenario) Name() string { return "watch-server-originated" }

func (s *WatchServerOriginatedScenario) Description() string {
	return "Watch-enabled client wakes and pulls after a ScopeManager server-originated write"
}

func (s *WatchServerOriginatedScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchServerOriginatedScenario) Setup(ctx context.Context) error {
	return s.harness.setupTwoDevices(ctx)
}

func (s *WatchServerOriginatedScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch server-originated scenario")
	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("start watch-enabled client: %w", err)
	}

	service, pool, err := newWatchScenarioWriteService(ctx, s.simulator.config, logger)
	if err != nil {
		return err
	}
	defer pool.Close()
	defer func() { _ = service.Close(context.Background()) }()

	s.rowID = uuid.New()
	scopeMgr := oversync.NewScopeManager(service, oversync.ScopeManagerConfig{Logger: logger})
	result, err := scopeMgr.ExecWrite(ctx, s.harness.userID, oversync.ScopeWriteOptions{WriterID: "mobile-flow-server-originated"}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO business.users (id, name, email)
			VALUES ($1, $2, $3)
		`, s.rowID, "Watch Server Ada", "watch.server.ada@example.com")
		return err
	})
	if err != nil {
		return fmt.Errorf("server-originated write: %w", err)
	}
	if result == nil || result.Bundle == nil {
		return fmt.Errorf("server-originated write produced no committed bundle")
	}

	if err := waitForLocalUserState(ctx, s.harness.watcherApp.db, s.rowID.String(), "Watch Server Ada", result.Bundle.BundleSeq, 5*time.Second); err != nil {
		return err
	}
	logger.Info("✅ Watch server-originated scenario converged", "row_id", s.rowID.String(), "bundle_seq", result.Bundle.BundleSeq)
	return nil
}

func (s *WatchServerOriginatedScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	return verifier.VerifyBusinessRecordsExist(ctx, "business.users", s.harness.userID, []string{s.rowID.String()})
}

func (s *WatchServerOriginatedScenario) Cleanup(ctx context.Context) error {
	return s.harness.cleanup(ctx)
}

type WatchReconnectCatchUpScenario struct {
	*BaseScenario
	harness *watchScenarioHarness
	rowID   string
}

func NewWatchReconnectCatchUpScenario(simulator *Simulator) Scenario {
	base := NewBaseScenario(simulator, "watch-reconnect-catch-up")
	return &WatchReconnectCatchUpScenario{
		BaseScenario: base,
		harness:      newWatchScenarioHarness(base),
	}
}

func (s *WatchReconnectCatchUpScenario) Name() string { return "watch-reconnect-catch-up" }

func (s *WatchReconnectCatchUpScenario) Description() string {
	return "Watch-enabled client reconnects after missing a peer push and catches up through after_bundle_seq"
}

func (s *WatchReconnectCatchUpScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchReconnectCatchUpScenario) Setup(ctx context.Context) error {
	return s.harness.setupTwoDevices(ctx)
}

func (s *WatchReconnectCatchUpScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch reconnect catch-up scenario")
	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("start first watch connection: %w", err)
	}
	s.harness.stopWatcher()

	s.rowID = uuid.NewString()
	if err := s.harness.writerApp.createUserWithContext(ctx, s.rowID, "Watch Reconnect Ada", "watch.reconnect.ada@example.com"); err != nil {
		return fmt.Errorf("create reconnect row: %w", err)
	}
	report, err := s.harness.writerApp.client.Sync(ctx)
	if err != nil {
		return fmt.Errorf("writer sync while watcher disconnected: %w", err)
	}
	if report.PushOutcome != oversqlite.PushOutcomeCommitted {
		return fmt.Errorf("writer push outcome = %s, want %s", report.PushOutcome, oversqlite.PushOutcomeCommitted)
	}

	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("restart watch connection: %w", err)
	}
	if err := waitForLocalUserState(ctx, s.harness.watcherApp.db, s.rowID, "Watch Reconnect Ada", report.Status.LastBundleSeqSeen, 5*time.Second); err != nil {
		return err
	}
	logger.Info("✅ Watch reconnect catch-up scenario converged", "row_id", s.rowID)
	return nil
}

func (s *WatchReconnectCatchUpScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	return verifier.VerifyBusinessRecordsExist(ctx, "business.users", s.harness.userID, []string{s.rowID})
}

func (s *WatchReconnectCatchUpScenario) Cleanup(ctx context.Context) error {
	return s.harness.cleanup(ctx)
}

type WatchDefaultOffScenario struct {
	*BaseScenario
	harness *watchScenarioHarness
	proxy   *watchScenarioProxy
	rowID   string
}

func NewWatchDefaultOffScenario(simulator *Simulator) Scenario {
	base := NewBaseScenario(simulator, "watch-default-off")
	return &WatchDefaultOffScenario{
		BaseScenario: base,
		harness:      newWatchScenarioHarness(base),
	}
}

func (s *WatchDefaultOffScenario) Name() string { return "watch-default-off" }

func (s *WatchDefaultOffScenario) Description() string {
	return "Default client configuration does not open /sync/watch even when the server advertises it"
}

func (s *WatchDefaultOffScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchDefaultOffScenario) Setup(ctx context.Context) error {
	proxy, err := newWatchScenarioProxy(s.simulator.config.ServerURL, nil)
	if err != nil {
		return err
	}
	s.proxy = proxy
	s.harness.serverURL = proxy.URL()
	return s.harness.setupTwoDevicesWithWatcherMode(ctx, false)
}

func (s *WatchDefaultOffScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch default-off scenario")
	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("start default-off client: %w", err)
	}

	s.rowID = uuid.NewString()
	if err := s.harness.writerApp.createUserWithContext(ctx, s.rowID, "Watch Default Off Ada", "watch.default.off@example.com"); err != nil {
		return fmt.Errorf("create default-off row: %w", err)
	}
	report, err := s.harness.writerApp.client.Sync(ctx)
	if err != nil {
		return fmt.Errorf("writer sync: %w", err)
	}
	if err := waitForLocalUserState(ctx, s.harness.watcherApp.db, s.rowID, "Watch Default Off Ada", report.Status.LastBundleSeqSeen, 5*time.Second); err != nil {
		return err
	}
	if got := s.proxy.WatchRequests(); got != 0 {
		return fmt.Errorf("default-off client opened /sync/watch %d times", got)
	}
	logger.Info("✅ Watch default-off scenario converged without opening /sync/watch", "row_id", s.rowID)
	return nil
}

func (s *WatchDefaultOffScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	return verifier.VerifyBusinessRecordsExist(ctx, "business.users", s.harness.userID, []string{s.rowID})
}

func (s *WatchDefaultOffScenario) Cleanup(ctx context.Context) error {
	if s.proxy != nil {
		defer s.proxy.Close()
	}
	return s.harness.cleanup(ctx)
}

type WatchServerUnsupportedFallbackScenario struct {
	*BaseScenario
	harness *watchScenarioHarness
	proxy   *watchScenarioProxy
	rowID   string
}

func NewWatchServerUnsupportedFallbackScenario(simulator *Simulator) Scenario {
	base := NewBaseScenario(simulator, "watch-server-unsupported-fallback")
	return &WatchServerUnsupportedFallbackScenario{
		BaseScenario: base,
		harness:      newWatchScenarioHarness(base),
	}
}

func (s *WatchServerUnsupportedFallbackScenario) Name() string {
	return "watch-server-unsupported-fallback"
}

func (s *WatchServerUnsupportedFallbackScenario) Description() string {
	return "Watch-auto client falls back to polling when capabilities do not advertise bundle_change_watch"
}

func (s *WatchServerUnsupportedFallbackScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchServerUnsupportedFallbackScenario) Setup(ctx context.Context) error {
	advertiseWatch := false
	proxy, err := newWatchScenarioProxy(s.simulator.config.ServerURL, &advertiseWatch)
	if err != nil {
		return err
	}
	s.proxy = proxy
	s.harness.serverURL = proxy.URL()
	return s.harness.setupTwoDevicesWithWatcherMode(ctx, true)
}

func (s *WatchServerUnsupportedFallbackScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch unsupported-server fallback scenario")
	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("start watch-auto client: %w", err)
	}

	s.rowID = uuid.NewString()
	if err := s.harness.writerApp.createUserWithContext(ctx, s.rowID, "Watch Fallback Ada", "watch.fallback@example.com"); err != nil {
		return fmt.Errorf("create fallback row: %w", err)
	}
	report, err := s.harness.writerApp.client.Sync(ctx)
	if err != nil {
		return fmt.Errorf("writer sync: %w", err)
	}
	if err := waitForLocalUserState(ctx, s.harness.watcherApp.db, s.rowID, "Watch Fallback Ada", report.Status.LastBundleSeqSeen, 5*time.Second); err != nil {
		return err
	}
	if got := s.proxy.WatchRequests(); got != 0 {
		return fmt.Errorf("unsupported-server fallback opened /sync/watch %d times", got)
	}
	logger.Info("✅ Watch unsupported-server fallback converged through polling", "row_id", s.rowID)
	return nil
}

func (s *WatchServerUnsupportedFallbackScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	return verifier.VerifyBusinessRecordsExist(ctx, "business.users", s.harness.userID, []string{s.rowID})
}

func (s *WatchServerUnsupportedFallbackScenario) Cleanup(ctx context.Context) error {
	if s.proxy != nil {
		defer s.proxy.Close()
	}
	return s.harness.cleanup(ctx)
}

type WatchIdleCleanupScenario struct {
	*BaseScenario
	harness *watchScenarioHarness
}

func NewWatchIdleCleanupScenario(simulator *Simulator) Scenario {
	base := NewBaseScenario(simulator, "watch-idle-cleanup")
	return &WatchIdleCleanupScenario{
		BaseScenario: base,
		harness:      newWatchScenarioHarness(base),
	}
}

func (s *WatchIdleCleanupScenario) Name() string { return "watch-idle-cleanup" }

func (s *WatchIdleCleanupScenario) Description() string {
	return "Idle watch subscriber unregisters after client cancellation without a data event"
}

func (s *WatchIdleCleanupScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchIdleCleanupScenario) Setup(ctx context.Context) error {
	return s.harness.setupTwoDevices(ctx)
}

func (s *WatchIdleCleanupScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch idle cleanup scenario")
	if err := s.harness.startWatcher(ctx); err != nil {
		return fmt.Errorf("start idle watch client: %w", err)
	}
	if err := waitForRemoteWatchSubscriberCount(ctx, s.simulator.config.ServerURL, s.harness.userID, 1, 5*time.Second); err != nil {
		return err
	}
	s.harness.stopWatcher()
	if err := waitForRemoteWatchSubscriberCount(ctx, s.simulator.config.ServerURL, s.harness.userID, 0, 5*time.Second); err != nil {
		return err
	}
	logger.Info("✅ Watch idle cleanup scenario removed subscriber", "user_id", s.harness.userID)
	return nil
}

func (s *WatchIdleCleanupScenario) Verify(context.Context, *DatabaseVerifier) error { return nil }

func (s *WatchIdleCleanupScenario) Cleanup(ctx context.Context) error {
	return s.harness.cleanup(ctx)
}

type WatchManyClientsConvergeScenario struct {
	*BaseScenario
	userID       string
	writerApp    *MobileApp
	watcherApps  []*MobileApp
	watcherStops []context.CancelFunc
	rowID        string
}

func NewWatchManyClientsConvergeScenario(simulator *Simulator) Scenario {
	return &WatchManyClientsConvergeScenario{
		BaseScenario: NewBaseScenario(simulator, "watch-many-clients-converge"),
	}
}

func (s *WatchManyClientsConvergeScenario) Name() string { return "watch-many-clients-converge" }

func (s *WatchManyClientsConvergeScenario) Description() string {
	return "Many watch-enabled clients for one scope converge after a peer push"
}

func (s *WatchManyClientsConvergeScenario) SetUserConfig(userID, deviceID string) {
	s.BaseScenario.SetUserConfig(userID, deviceID)
}

func (s *WatchManyClientsConvergeScenario) Setup(ctx context.Context) error {
	s.userID = watchScenarioUserID(s.config, "watch-many")
	const watcherCount = 8
	var err error
	s.writerApp, err = s.createManyClientApp("writer", false)
	if err != nil {
		return err
	}
	if err := s.writerApp.onLaunch(ctx); err != nil {
		return fmt.Errorf("launch writer app: %w", err)
	}
	if err := signInWithoutSimulatorSync(ctx, s.writerApp, s.userID); err != nil {
		return fmt.Errorf("sign in writer app: %w", err)
	}

	for i := 0; i < watcherCount; i++ {
		app, err := s.createManyClientApp(fmt.Sprintf("watcher-%02d", i+1), true)
		if err != nil {
			return err
		}
		if err := app.onLaunch(ctx); err != nil {
			return fmt.Errorf("launch watcher %d: %w", i+1, err)
		}
		if err := signInWithoutSimulatorSync(ctx, app, s.userID); err != nil {
			return fmt.Errorf("sign in watcher %d: %w", i+1, err)
		}
		s.watcherApps = append(s.watcherApps, app)
		if i == 0 {
			s.app = app
			s.simulator.currentApp = app
		}
	}
	return nil
}

func (s *WatchManyClientsConvergeScenario) createManyClientApp(deviceSuffix string, watch bool) (*MobileApp, error) {
	deviceID := "watch-many-" + deviceSuffix
	if s.config != nil && s.config.DeviceID != "" && s.config.DeviceID != "device-unknown-001" {
		deviceID = s.config.DeviceID + "-" + deviceSuffix
	}
	return s.simulator.newScenarioMobileApp(scenarioMobileAppOptions{
		UserID:           s.userID,
		DeviceID:         deviceID,
		DeviceName:       "Watch Many " + deviceSuffix,
		DBNamePrefix:     deviceID,
		OversqliteConfig: watchScenarioOversqliteConfig(watch),
	})
}

func (s *WatchManyClientsConvergeScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Starting watch many-clients convergence scenario", "watchers", len(s.watcherApps))
	for i, app := range s.watcherApps {
		watchCtx, cancel := context.WithCancel(ctx)
		s.watcherStops = append(s.watcherStops, cancel)
		if err := app.client.Start(watchCtx); err != nil {
			return fmt.Errorf("start watcher %d: %w", i+1, err)
		}
	}
	if err := waitForRemoteWatchSubscriberCount(ctx, s.simulator.config.ServerURL, s.userID, len(s.watcherApps), 5*time.Second); err != nil {
		return err
	}

	s.rowID = uuid.NewString()
	if err := s.writerApp.createUserWithContext(ctx, s.rowID, "Watch Many Ada", "watch.many@example.com"); err != nil {
		return fmt.Errorf("create many-client row: %w", err)
	}
	report, err := s.writerApp.client.Sync(ctx)
	if err != nil {
		return fmt.Errorf("writer sync: %w", err)
	}
	for i, app := range s.watcherApps {
		if err := waitForLocalUserState(ctx, app.db, s.rowID, "Watch Many Ada", report.Status.LastBundleSeqSeen, 5*time.Second); err != nil {
			return fmt.Errorf("watcher %d did not converge: %w", i+1, err)
		}
	}
	logger.Info("✅ Watch many-clients scenario converged", "watchers", len(s.watcherApps), "row_id", s.rowID)
	return nil
}

func (s *WatchManyClientsConvergeScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	return verifier.VerifyBusinessRecordsExist(ctx, "business.users", s.userID, []string{s.rowID})
}

func (s *WatchManyClientsConvergeScenario) Cleanup(ctx context.Context) error {
	for _, cancel := range s.watcherStops {
		cancel()
	}
	s.watcherStops = nil
	var firstErr error
	for i, app := range s.watcherApps {
		if err := app.close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close watcher %d: %w", i+1, err)
		}
	}
	s.watcherApps = nil
	if s.writerApp != nil {
		if err := s.writerApp.close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close writer app: %w", err)
		}
		s.writerApp = nil
	}
	if err := waitForRemoteWatchSubscriberCount(ctx, s.simulator.config.ServerURL, s.userID, 0, 5*time.Second); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func watchScenarioUserID(cfg *config.ScenarioConfig, fallbackPrefix string) string {
	if cfg != nil && cfg.UserID != "" && cfg.UserID != "user-unknown" {
		return cfg.UserID
	}
	return fallbackPrefix + "-" + uuid.NewString()
}

func watchScenarioOversqliteConfig(watch bool) *oversqlite.Config {
	cfg := oversqlite.DefaultConfig("business", managedSyncTables())
	cfg.UploadLimit = 100
	cfg.DownloadLimit = 1000
	cfg.BackoffMin = 25 * time.Millisecond
	cfg.BackoffMax = 250 * time.Millisecond
	cfg.WatchFallbackInterval = watchScenarioFallbackInterval
	if watch {
		cfg.BundleChangeWatchMode = oversqlite.BundleChangeWatchAuto
	}
	return cfg
}

func signInWithoutSimulatorSync(ctx context.Context, app *MobileApp, userID string) error {
	if app == nil {
		return fmt.Errorf("app is nil")
	}
	if err := app.session.SignIn(userID); err != nil {
		return fmt.Errorf("sign in session: %w", err)
	}
	if err := app.client.Open(ctx); err != nil {
		return fmt.Errorf("open oversqlite lifecycle: %w", err)
	}
	if err := app.connectLifecycle(ctx, userID); err != nil {
		return fmt.Errorf("connect oversqlite lifecycle: %w", err)
	}
	app.ui.SetBanner("Syncing...")
	app.ui.SetPendingBadge(app.getPendingCount())
	return nil
}

func waitForLocalUserState(ctx context.Context, db *sql.DB, rowID, wantName string, minBundleSeq int64, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var gotName string
		err := db.QueryRowContext(waitCtx, `SELECT name FROM users WHERE id = ?`, rowID).Scan(&gotName)
		if err == nil {
			if gotName != wantName {
				return fmt.Errorf("local user %s name = %q, want %q", rowID, gotName, wantName)
			}
			lastSeq, err := localLastBundleSeqSeen(waitCtx, db)
			if err != nil {
				return err
			}
			if lastSeq >= minBundleSeq {
				return nil
			}
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("query local user %s: %w", rowID, err)
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timed out waiting for local user %s through watch: %w", rowID, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func localLastBundleSeqSeen(ctx context.Context, db *sql.DB) (int64, error) {
	var lastSeq int64
	if err := db.QueryRowContext(ctx, `
		SELECT last_bundle_seq_seen
		FROM _sync_attachment_state
		WHERE singleton_key = 1
	`).Scan(&lastSeq); err != nil {
		return 0, fmt.Errorf("query local last_bundle_seq_seen: %w", err)
	}
	return lastSeq, nil
}

type watchScenarioProxy struct {
	targetURL              string
	server                 *httptest.Server
	advertiseWatchOverride *bool
	watchRequests          atomic.Int64
}

func newWatchScenarioProxy(targetURL string, advertiseWatchOverride *bool) (*watchScenarioProxy, error) {
	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("parse target server URL: %w", err)
	}
	p := &watchScenarioProxy{
		targetURL:              targetURL,
		advertiseWatchOverride: advertiseWatchOverride,
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(resp *http.Response) error {
		if p.advertiseWatchOverride == nil || resp == nil || resp.Request == nil || resp.Request.URL.Path != "/sync/capabilities" {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		var caps oversync.CapabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			return err
		}
		if caps.Features == nil {
			caps.Features = make(map[string]bool)
		}
		caps.Features["bundle_change_watch"] = *p.advertiseWatchOverride
		raw, err := json.Marshal(caps)
		if err != nil {
			return err
		}
		resp.Body = io.NopCloser(strings.NewReader(string(raw)))
		resp.ContentLength = int64(len(raw))
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(raw)))
		return nil
	}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sync/watch" {
			p.watchRequests.Add(1)
		}
		proxy.ServeHTTP(w, r)
	}))
	return p, nil
}

func (p *watchScenarioProxy) URL() string {
	if p == nil || p.server == nil {
		return ""
	}
	return p.server.URL
}

func (p *watchScenarioProxy) WatchRequests() int64 {
	if p == nil {
		return 0
	}
	return p.watchRequests.Load()
}

func (p *watchScenarioProxy) Close() {
	if p != nil && p.server != nil {
		p.server.Close()
	}
}

func remoteWatchSubscriberCount(ctx context.Context, serverURL, userID string) (int, error) {
	endpoint := strings.TrimRight(serverURL, "/") + "/test/watch-subscribers?user_id=" + url.QueryEscape(userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("watch subscriber count failed with %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		SubscriberCount int `json:"subscriber_count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}
	return payload.SubscriberCount, nil
}

func waitForRemoteWatchSubscriberCount(ctx context.Context, serverURL, userID string, want int, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var lastCount int
	var lastErr error
	for {
		lastCount, lastErr = remoteWatchSubscriberCount(waitCtx, serverURL, userID)
		if lastErr == nil && lastCount == want {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for watch subscriber count %d for %s: last error: %w", want, userID, lastErr)
			}
			return fmt.Errorf("timed out waiting for watch subscriber count %d for %s, got %d", want, userID, lastCount)
		case <-ticker.C:
		}
	}
}

func newWatchScenarioWriteService(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*oversync.SyncService, *pgxpool.Pool, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open write-service PostgreSQL pool: %w", err)
	}
	service, err := oversync.NewRuntimeService(pool, &oversync.ServiceConfig{
		MaxSupportedSchemaVersion: 1,
		AppName:                   "mobile-flow-watch-write-service",
		RegisteredTables:          serverpkg.RegisteredTablesForBusinessSchema("business"),
		BundleChangeWatch: oversync.BundleChangeWatchConfig{
			Enabled: true,
		},
	}, logger)
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("create write-side sync service: %w", err)
	}
	if err := service.Bootstrap(ctx); err != nil {
		_ = service.Close(context.Background())
		pool.Close()
		return nil, nil, fmt.Errorf("bootstrap write-side sync service: %w", err)
	}
	return service, pool, nil
}
