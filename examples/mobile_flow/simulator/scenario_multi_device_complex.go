package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// MultiDeviceComplexScenario performs a long, mixed sequence of ops across two devices for the SAME user
// and verifies both devices converge to exactly the same records (no duplicates, no misses).
//
// Flow (users table only):
// - Sign in as same user on Device 1 and Device 2; hydrate both
// - D1 adds A1, A2, A3  -> full sync D1 (upload + download loop)
// - D2 adds B1, B2      -> full sync D2
// - D1 updates A2       -> full sync D1
// - D2 deletes A1       -> full sync D2
// - D1 adds B3          -> full sync D1
// - D2 updates B2       -> full sync D2
// - D1 deletes A3 then re-adds A3 and updates it -> full sync D1
// - Final convergence download loops on both devices
// - Verify final set equals expected, on both devices and on server
// Expected final present IDs: {A2, A3(re-added), B1, B2(updated), B3}; Deleted: {A1}
// Expected count: 5
//
// Notes: After each upload, we run a download-until-empty loop to mirror mobile app behavior.

type MultiDeviceComplexScenario struct {
	*BaseScenario
	device2App *MobileApp
	device1ID  string
	device2ID  string
	userID     string

	A1 string
	A2 string
	A3 string
	B1 string
	B2 string
	B3 string
}

func NewMultiDeviceComplexScenario(sim *Simulator) Scenario {
	return &MultiDeviceComplexScenario{BaseScenario: NewBaseScenario(sim, "multi-device-complex")}
}

func (s *MultiDeviceComplexScenario) Name() string { return "multi-device-complex" }
func (s *MultiDeviceComplexScenario) Description() string {
	return "Complex two-device session (same user): adds, updates, deletes, re-adds; verify convergence"
}

func (s *MultiDeviceComplexScenario) Setup(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🔧 Setting up complex multi-device scenario for SAME user")

	s.userID = "user-" + uuid.New().String()[:8]
	s.device1ID = "d1-" + s.userID
	s.device2ID = "d2-" + s.userID

	cfg1 := &config.ScenarioConfig{UserID: s.userID}
	cfg2 := &config.ScenarioConfig{UserID: s.userID}

	app1, err := s.createDeviceApp(cfg1, s.device1ID, "Device 1 (Complex)")
	if err != nil {
		return err
	}
	app2, err := s.createDeviceApp(cfg2, s.device2ID, "Device 2 (Complex)")
	if err != nil {
		return err
	}
	s.app = app1
	s.device2App = app2

	if err := s.app.OnLaunch(ctx); err != nil {
		return err
	}
	if err := s.device2App.OnLaunch(ctx); err != nil {
		return err
	}
	return nil
}

func (s *MultiDeviceComplexScenario) createDeviceApp(scenarioConfig *config.ScenarioConfig, deviceID, deviceName string) (*MobileApp, error) {
	simCfg := s.simulator.GetConfig()

	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", deviceID, scenarioConfig.UserID, time.Now().UnixNano()))
	overs := &oversqlite.Config{
		Schema: "business",
		Tables: []oversqlite.SyncTable{
			{TableName: "users"},
			{TableName: "posts"},
			{TableName: "files"},
			{TableName: "file_reviews"},
		},
		UploadLimit:   100,
		DownloadLimit: 100,
	}
	appCfg := &MobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        simCfg.ServerURL,
		UserID:           scenarioConfig.UserID,
		SourceID:         deviceID,
		DeviceName:       deviceName,
		JWTSecret:        simCfg.JWTSecret,
		OversqliteConfig: overs,
		Logger:           s.simulator.GetLogger(),
	}
	return NewMobileApp(appCfg)
}

func (s *MultiDeviceComplexScenario) Execute(ctx context.Context) error {
	log := s.simulator.GetLogger()
	log.Info("🎯 Starting Multi-Device Complex Scenario (SAME user)")

	// Sign in both + hydrate
	if err := s.app.SignIn(ctx, s.userID); err != nil {
		return err
	}
	if err := s.device2App.SignIn(ctx, s.userID); err != nil {
		return err
	}
	if err := s.app.PerformHydration(ctx); err != nil {
		return err
	}
	if err := s.device2App.PerformHydration(ctx); err != nil {
		return err
	}

	// D1 adds A1,A2,A3
	s.A1, s.A2, s.A3 = uuid.New().String(), uuid.New().String(), uuid.New().String()
	if err := s.app.CreateUserWithContext(ctx, s.A1, "A1", "a1@example.com"); err != nil {
		return err
	}
	if err := s.app.CreateUserWithContext(ctx, s.A2, "A2", "a2@example.com"); err != nil {
		return err
	}
	if err := s.app.CreateUserWithContext(ctx, s.A3, "A3", "a3@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.app); err != nil {
		return err
	}

	// D2 adds B1,B2
	s.B1, s.B2 = uuid.New().String(), uuid.New().String()
	if err := s.device2App.CreateUserWithContext(ctx, s.B1, "B1", "b1@example.com"); err != nil {
		return err
	}
	if err := s.device2App.CreateUserWithContext(ctx, s.B2, "B2", "b2@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.device2App); err != nil {
		return err
	}

	// D1 updates A2
	if err := s.app.UpdateUser(s.A2, "A2-upd", "a2u@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.app); err != nil {
		return err
	}

	// D2 deletes A1
	if err := s.device2App.DeleteUserWithContext(ctx, s.A1); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.device2App); err != nil {
		return err
	}

	// D1 adds B3
	s.B3 = uuid.New().String()
	if err := s.app.CreateUserWithContext(ctx, s.B3, "B3", "b3@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.app); err != nil {
		return err
	}

	// D2 updates B2
	if err := s.device2App.UpdateUser(s.B2, "B2-upd", "b2u@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.device2App); err != nil {
		return err
	}

	// D1 deletes A3
	if err := s.app.DeleteUserWithContext(ctx, s.A3); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.app); err != nil {
		return err
	}

	// D1 re-adds A3
	if err := s.app.CreateUserWithContext(ctx, s.A3, "A3-re", "a3re@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.app); err != nil {
		return err
	}

	// D1 updates A3
	if err := s.app.UpdateUser(s.A3, "A3-re-upd", "a3reu@example.com"); err != nil {
		return err
	}
	if err := s.fullSyncUploadThenDownload(ctx, s.app); err != nil {
		return err
	}

	// Final convergence downloads for both devices
	if _, _, err := s.downloadUntilEmpty(ctx, s.app, 500); err != nil {
		return err
	}
	if _, _, err := s.downloadUntilEmpty(ctx, s.device2App, 500); err != nil {
		return err
	}

	// Debug dump right before verify if mismatch
	if cnt1, _ := s.app.GetUserCount(ctx); cnt1 != 5 {
		debugDumpUsers(s.app, "Device 1", log)
	}
	if cnt2, _ := s.device2App.GetUserCount(ctx); cnt2 != 5 {
		debugDumpUsers(s.device2App, "Device 2", log)
	}

	// Verify
	if err := s.verifyFinalState(ctx, log); err != nil {
		return err
	}
	log.Info("🎉 Complex Multi-Device Scenario completed successfully")
	return nil
}

func (s *MultiDeviceComplexScenario) fullSyncUploadThenDownload(ctx context.Context, app *MobileApp) error {
	if err := app.PerformSyncUpload(ctx); err != nil {
		return err
	}
	_, _, err := s.downloadUntilEmpty(ctx, app, 500)
	return err
}

func (s *MultiDeviceComplexScenario) downloadUntilEmpty(ctx context.Context, app *MobileApp, limit int) (total int, last int64, err error) {
	for {
		applied, next, err := app.PerformSyncDownload(ctx, limit)
		if err != nil {
			return total, last, err
		}
		total += applied
		last = next
		if applied < limit {
			return total, last, nil
		}
	}
}

func (s *MultiDeviceComplexScenario) verifyFinalState(ctx context.Context, logger *slog.Logger) error {
	// Expected present: A2, A3, B1, B2, B3
	expectedPresent := []string{s.A2, s.A3, s.B1, s.B2, s.B3}
	expectedMissing := []string{s.A1}

	// Device 1 checks
	if err := s.checkCountsAndPresence(ctx, s.app, expectedPresent, expectedMissing, 5); err != nil {
		return fmt.Errorf("device1: %w", err)
	}
	// Device 2 checks
	if err := s.checkCountsAndPresence(ctx, s.device2App, expectedPresent, expectedMissing, 5); err != nil {
		return fmt.Errorf("device2: %w", err)
	}

	// Server verification
	if s.simulator.verifier != nil {
		cnt, err := s.simulator.verifier.CountActualBusinessRecords(ctx, "business.users", s.userID)
		if err != nil {
			return err
		}
		if cnt != 5 {
			return fmt.Errorf("server expected 5 users, got %d", cnt)
		}
	}
	return nil
}

func (s *MultiDeviceComplexScenario) checkCountsAndPresence(ctx context.Context, app *MobileApp, present, missing []string, expectedCount int) error {
	cnt, err := app.GetUserCount(ctx)
	if err != nil {
		return err
	}
	if cnt != expectedCount {
		return fmt.Errorf("expected %d users, got %d", expectedCount, cnt)
	}
	for _, id := range present {
		ok, err := app.HasUser(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("expected user %s to exist", id)
		}
	}
	for _, id := range missing {
		ok, err := app.HasUser(ctx, id)
		if err != nil {
			return err
		}
		if ok {
			return fmt.Errorf("expected user %s to be deleted", id)
		}
	}
	return nil
}

func (s *MultiDeviceComplexScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	// Already verified via verifyFinalState; optionally add deeper checks here
	return nil
}

// debugDumpUsers prints current users for an app
func debugDumpUsers(app *MobileApp, label string, logger *slog.Logger) {
	if app == nil || app.db == nil {
		return
	}
	rows, err := app.db.Query(`SELECT id, name FROM users ORDER BY id`)
	if err != nil {
		logger.Warn("debug dump users failed", "label", label, "err", err)
		return
	}
	defer rows.Close()
	logger.Info("🔎 Users on " + label)
	for rows.Next() {
		var id, name string
		_ = rows.Scan(&id, &name)
		logger.Info("  •", "id", id, "name", name)
	}
}

func (s *MultiDeviceComplexScenario) Cleanup(ctx context.Context) error {
	if s.app != nil {
		_ = s.app.Close()
	}
	if s.device2App != nil {
		_ = s.device2App.Close()
	}
	return nil
}
