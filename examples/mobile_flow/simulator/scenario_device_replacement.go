package simulator

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// DeviceReplacementScenario simulates a user moving to a new device identity.
type DeviceReplacementScenario struct {
	*BaseScenario
	replacementApp *MobileApp
	originalRowID  string
	replacementID  string
}

// NewDeviceReplacementScenario creates a new device replacement scenario.
func NewDeviceReplacementScenario(simulator *Simulator) Scenario {
	return &DeviceReplacementScenario{
		BaseScenario: NewBaseScenario(simulator, "device-replacement"),
	}
}

func (s *DeviceReplacementScenario) Name() string {
	return s.config.Name
}

func (s *DeviceReplacementScenario) Description() string {
	return s.config.Description
}

func (s *DeviceReplacementScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Executing Device Replacement Scenario")

	if err := s.app.signIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in original device: %w", err)
	}

	s.originalRowID = uuid.NewString()
	if err := s.app.createUserWithID(ctx, s.originalRowID, "Original Device User", "original-device@example.com"); err != nil {
		return fmt.Errorf("failed to create original-device user: %w", err)
	}
	if err := s.app.pushPending(ctx); err != nil {
		return fmt.Errorf("failed to push original-device seed row: %w", err)
	}
	if _, _, err := s.app.pullToStable(ctx); err != nil {
		return fmt.Errorf("failed to stabilize original device after seed push: %w", err)
	}

	originalDeviceID := s.config.DeviceID
	replacementDeviceID := fmt.Sprintf("%s-replacement", originalDeviceID)
	logger.Info(
		"Preparing replacement device",
		"original_device_id", originalDeviceID,
		"replacement_device_id", replacementDeviceID,
	)
	replacementApp, err := s.createReplacementApp(replacementDeviceID)
	if err != nil {
		return err
	}
	s.replacementApp = replacementApp

	if err := s.replacementApp.signIn(ctx, s.config.UserID); err != nil {
		return fmt.Errorf("failed to sign in replacement device: %w", err)
	}
	if err := s.replacementApp.rebuildKeepSource(ctx); err != nil {
		return fmt.Errorf("failed to hydrate replacement device: %w", err)
	}

	hasOriginal, err := s.replacementApp.hasUser(ctx, s.originalRowID)
	if err != nil {
		return fmt.Errorf("failed to verify replacement-device restore of original row: %w", err)
	}
	if !hasOriginal {
		return fmt.Errorf("replacement device is missing original row %s", s.originalRowID)
	}

	s.replacementID = uuid.NewString()
	if err := s.replacementApp.createUserWithID(ctx, s.replacementID, "Replacement Device User", "replacement-device@example.com"); err != nil {
		return fmt.Errorf("failed to create replacement-device user: %w", err)
	}
	if err := s.replacementApp.pushPending(ctx); err != nil {
		return fmt.Errorf("failed to push replacement-device row: %w", err)
	}
	if _, _, err := s.replacementApp.pullToStable(ctx); err != nil {
		return fmt.Errorf("failed to stabilize replacement device after push: %w", err)
	}

	if _, _, err := s.app.pullToStable(ctx); err != nil {
		return fmt.Errorf("failed to pull replacement-device row on original device: %w", err)
	}

	for _, rowID := range []string{s.originalRowID, s.replacementID} {
		ok, err := s.app.hasUser(ctx, rowID)
		if err != nil {
			return fmt.Errorf("failed to verify original device row %s after replacement sync: %w", rowID, err)
		}
		if !ok {
			return fmt.Errorf("original device is missing expected row %s after replacement sync", rowID)
		}
		ok, err = s.replacementApp.hasUser(ctx, rowID)
		if err != nil {
			return fmt.Errorf("failed to verify replacement device row %s after replacement sync: %w", rowID, err)
		}
		if !ok {
			return fmt.Errorf("replacement device is missing expected row %s after replacement sync", rowID)
		}
	}

	logger.Info("✅ Device Replacement Scenario completed successfully", "rows", 2)
	return nil
}

func (s *DeviceReplacementScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	serverCount, err := verifier.CountUserRecords(ctx, "business.users", s.config.UserID)
	if err != nil {
		return fmt.Errorf("failed to verify device-replacement server state: %w", err)
	}
	if serverCount != 2 {
		return fmt.Errorf("expected 2 server users after device replacement flow, got %d", serverCount)
	}
	return nil
}

func (s *DeviceReplacementScenario) Cleanup(ctx context.Context) error {
	if s.replacementApp != nil {
		if err := s.replacementApp.close(); err != nil {
			return fmt.Errorf("failed to close replacement app: %w", err)
		}
	}
	return s.BaseScenario.Cleanup(ctx)
}

func (s *DeviceReplacementScenario) createReplacementApp(deviceID string) (*MobileApp, error) {
	simCfg := s.simulator.GetConfig()
	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", deviceID, s.config.UserID, time.Now().UnixNano()))
	appConfig := &mobileAppConfig{
		DatabaseFile: dbFile,
		ServerURL:    simCfg.ServerURL,
		UserID:       s.config.UserID,
		DeviceID:     deviceID,
		DeviceName:   "Replacement Device",
		JWTSecret:    simCfg.JWTSecret,
		OversqliteConfig: &oversqlite.Config{
			Schema:        "business",
			Tables:        managedSyncTables(),
			UploadLimit:   100,
			DownloadLimit: 100,
		},
		PreserveDB: simCfg.PreserveDB,
		Logger:     s.simulator.GetLogger(),
	}
	app, err := newMobileApp(appConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create replacement device app: %w", err)
	}
	if err := app.onLaunch(context.Background()); err != nil {
		app.close()
		return nil, fmt.Errorf("failed to launch replacement device app: %w", err)
	}
	return app, nil
}
