package simulator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/examples/mobile_flow/config"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

const conflictScenarioSiblingCount = 6

type conflictCase struct {
	name                string
	targetID            string
	siblingIDs          []string
	resolver            oversqlite.Resolver
	serverName          string
	localName           string
	expectedName        string
	expectedEmail       string
	serverUpdatedAt     time.Time
	localUpdatedAt      time.Time
	expectTargetVersion string
}

type conflictUserSnapshot struct {
	Name      string
	Email     string
	UpdatedAt string
}

// ConflictsScenario exercises structured conflict recovery under moderate multi-chunk load.
type ConflictsScenario struct {
	*BaseScenario
	device2App *MobileApp
	device1ID  string
	device2ID  string
	userID     string
	cases      []conflictCase
}

func NewConflictsScenario(simulator *Simulator) Scenario {
	return &ConflictsScenario{
		BaseScenario: NewBaseScenario(simulator, "conflicts"),
	}
}

func (s *ConflictsScenario) Name() string {
	return s.config.Name
}

func (s *ConflictsScenario) Description() string {
	return s.config.Description
}

func (s *ConflictsScenario) Setup(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🔧 Setting up structured conflict scenario")

	if s.config != nil && s.config.UserID != "" && s.config.UserID != "user-unknown" {
		s.userID = s.config.UserID
	} else {
		s.userID = "user-conflicts-" + uuid.NewString()[:8]
	}

	sourceBase := fmt.Sprintf("device-%s", s.userID)
	if s.config != nil && s.config.SourceID != "" && s.config.SourceID != "device-unknown-001" {
		sourceBase = s.config.SourceID
	}
	s.device1ID = sourceBase + "-d1"
	s.device2ID = sourceBase + "-d2"

	device1Config := &config.ScenarioConfig{UserID: s.userID}
	device1App, err := s.createDeviceApp(device1Config, s.device1ID, "Conflict Device 1")
	if err != nil {
		return fmt.Errorf("failed to create device 1 app: %w", err)
	}
	s.app = device1App
	s.simulator.currentApp = s.app

	device2Config := &config.ScenarioConfig{UserID: s.userID}
	device2App, err := s.createDeviceApp(device2Config, s.device2ID, "Conflict Device 2")
	if err != nil {
		return fmt.Errorf("failed to create device 2 app: %w", err)
	}
	s.device2App = device2App

	if err := s.app.OnLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch device 1 app: %w", err)
	}
	if err := s.device2App.OnLaunch(ctx); err != nil {
		return fmt.Errorf("failed to launch device 2 app: %w", err)
	}

	s.seedConflictCaseIDs()
	logger.Info("✅ Structured conflict scenario setup complete", "user_id", s.userID)
	return nil
}

func (s *ConflictsScenario) createDeviceApp(scenarioConfig *config.ScenarioConfig, deviceID, deviceName string) (*MobileApp, error) {
	simCfg := s.simulator.GetConfig()
	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", deviceID, scenarioConfig.UserID, time.Now().UnixNano()))

	oversqliteConfig := &oversqlite.Config{
		Schema:        "business",
		Tables:        managedSyncTables(),
		UploadLimit:   3,
		DownloadLimit: 100,
		BackoffMin:    100 * time.Millisecond,
		BackoffMax:    2 * time.Second,
	}

	appConfig := &MobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        simCfg.ServerURL,
		UserID:           scenarioConfig.UserID,
		SourceID:         deviceID,
		DeviceName:       deviceName,
		JWTSecret:        simCfg.JWTSecret,
		OversqliteConfig: oversqliteConfig,
		PreserveDB:       simCfg.PreserveDB,
		Logger:           s.simulator.GetLogger(),
	}
	return NewMobileApp(appConfig)
}

func (s *ConflictsScenario) seedConflictCaseIDs() {
	makeSiblings := func() []string {
		ids := make([]string, 0, conflictScenarioSiblingCount)
		for i := 0; i < conflictScenarioSiblingCount; i++ {
			ids = append(ids, uuid.NewString())
		}
		return ids
	}

	baseTime := time.Date(2026, 3, 24, 18, 0, 0, 0, time.UTC)
	s.cases = []conflictCase{
		{
			name:                "server-wins",
			targetID:            uuid.NewString(),
			siblingIDs:          makeSiblings(),
			resolver:            &oversqlite.ServerWinsResolver{},
			serverName:          "Server Wins Authoritative",
			localName:           "Server Wins Local",
			expectedName:        "Server Wins Authoritative",
			expectedEmail:       "server-wins-authoritative@example.com",
			serverUpdatedAt:     baseTime.Add(1 * time.Minute),
			localUpdatedAt:      baseTime.Add(2 * time.Minute),
			expectTargetVersion: "server",
		},
		{
			name:                "client-wins",
			targetID:            uuid.NewString(),
			siblingIDs:          makeSiblings(),
			resolver:            &oversqlite.ClientWinsResolver{},
			serverName:          "Client Wins Server",
			localName:           "Client Wins Local",
			expectedName:        "Client Wins Local",
			expectedEmail:       "client-wins-local@example.com",
			serverUpdatedAt:     baseTime.Add(3 * time.Minute),
			localUpdatedAt:      baseTime.Add(4 * time.Minute),
			expectTargetVersion: "local",
		},
		{
			name:       "latest-updated-at-merge",
			targetID:   uuid.NewString(),
			siblingIDs: makeSiblings(),
			resolver: &latestUpdatedAtResolver{
				logger: s.simulator.GetLogger(),
			},
			serverName:          "Merge Server Later",
			localName:           "Merge Local Earlier",
			expectedName:        "Merge Server Later",
			expectedEmail:       "merge-server-later@example.com",
			serverUpdatedAt:     baseTime.Add(6 * time.Minute),
			localUpdatedAt:      baseTime.Add(5 * time.Minute),
			expectTargetVersion: "server",
		},
	}
}

func (s *ConflictsScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Executing structured conflict scenario", "cases", len(s.cases), "sibling_count", conflictScenarioSiblingCount)

	if err := s.signInAndPauseSync(ctx); err != nil {
		return err
	}
	if err := s.seedBaselineDataset(ctx, logger); err != nil {
		return err
	}

	for _, tc := range s.cases {
		if err := s.runConflictCase(ctx, logger, tc); err != nil {
			return fmt.Errorf("conflict case %s failed: %w", tc.name, err)
		}
	}

	logger.Info("🎉 Structured conflict scenario completed successfully")
	return nil
}

func (s *ConflictsScenario) signInAndPauseSync(ctx context.Context) error {
	if err := s.app.SignIn(ctx, s.userID); err != nil {
		return fmt.Errorf("failed to sign in device 1: %w", err)
	}
	if err := s.device2App.SignIn(ctx, s.userID); err != nil {
		return fmt.Errorf("failed to sign in device 2: %w", err)
	}
	s.app.StopSync()
	s.device2App.StopSync()
	if err := s.app.Hydrate(ctx); err != nil {
		return fmt.Errorf("failed to hydrate device 1: %w", err)
	}
	if err := s.device2App.Hydrate(ctx); err != nil {
		return fmt.Errorf("failed to hydrate device 2: %w", err)
	}
	return nil
}

func (s *ConflictsScenario) seedBaselineDataset(ctx context.Context, logger *slog.Logger) error {
	logger.Info("🌱 Seeding baseline conflict dataset")
	for _, tc := range s.cases {
		if err := s.app.CreateUserWithContext(ctx, tc.targetID, baselineName(tc.name), baselineEmail(tc.name)); err != nil {
			return fmt.Errorf("failed to create baseline target for %s: %w", tc.name, err)
		}
		for i, siblingID := range tc.siblingIDs {
			if err := s.app.CreateUserWithContext(ctx, siblingID, siblingName(tc.name, i), siblingEmail(tc.name, i)); err != nil {
				return fmt.Errorf("failed to create baseline sibling %d for %s: %w", i, tc.name, err)
			}
		}
	}
	s.app.ResetPushTransferDiagnostics()
	if err := s.app.PushPending(ctx); err != nil {
		return fmt.Errorf("failed to upload baseline dataset: %w", err)
	}
	if _, _, err := s.app.PullToStable(ctx); err != nil {
		return fmt.Errorf("failed to stabilize device 1 after baseline upload: %w", err)
	}
	if err := s.device2App.Hydrate(ctx); err != nil {
		return fmt.Errorf("failed to hydrate baseline dataset on device 2: %w", err)
	}
	return s.verifyConvergedState(ctx, logger, "baseline")
}

func (s *ConflictsScenario) runConflictCase(ctx context.Context, logger *slog.Logger, tc conflictCase) error {
	logger.Info("⚔️ Running conflict case", "case", tc.name, "resolver", fmt.Sprintf("%T", tc.resolver))

	s.device2App.GetClient().Resolver = tc.resolver

	serverEmail := emailForName(tc.serverName)
	localEmail := emailForName(tc.localName)
	if err := updateUserWithTimestamp(ctx, s.app.db, tc.targetID, tc.serverName, serverEmail, tc.serverUpdatedAt); err != nil {
		return fmt.Errorf("failed to update authoritative row on device 1: %w", err)
	}
	if err := s.app.PushPending(ctx); err != nil {
		return fmt.Errorf("failed to push authoritative update for %s: %w", tc.name, err)
	}
	if _, _, err := s.app.PullToStable(ctx); err != nil {
		return fmt.Errorf("failed to stabilize device 1 after authoritative update: %w", err)
	}

	if err := updateUserWithTimestamp(ctx, s.device2App.db, tc.targetID, tc.localName, localEmail, tc.localUpdatedAt); err != nil {
		return fmt.Errorf("failed to update local conflicting row for %s: %w", tc.name, err)
	}
	siblingExpectations := make(map[string]conflictUserSnapshot, len(tc.siblingIDs))
	for i, siblingID := range tc.siblingIDs {
		name := fmt.Sprintf("%s sibling local %02d", tc.name, i+1)
		email := emailForName(name)
		updatedAt := tc.localUpdatedAt.Add(time.Duration(i+1) * time.Second)
		if err := updateUserWithTimestamp(ctx, s.device2App.db, siblingID, name, email, updatedAt); err != nil {
			return fmt.Errorf("failed to update sibling %d for %s: %w", i, tc.name, err)
		}
		siblingExpectations[siblingID] = conflictUserSnapshot{
			Name:      name,
			Email:     email,
			UpdatedAt: updatedAt.UTC().Format(time.RFC3339Nano),
		}
	}

	s.device2App.ResetPushTransferDiagnostics()
	if err := s.device2App.PushPending(ctx); err != nil {
		return fmt.Errorf("failed to push conflicting case %s: %w", tc.name, err)
	}
	if _, _, err := s.device2App.PullToStable(ctx); err != nil {
		return fmt.Errorf("failed to stabilize device 2 after conflict recovery for %s: %w", tc.name, err)
	}
	if _, _, err := s.app.PullToStable(ctx); err != nil {
		return fmt.Errorf("failed to pull recovered results to device 1 for %s: %w", tc.name, err)
	}

	pushStats := s.device2App.PushTransferDiagnostics()
	if pushStats.SessionsCreated != 2 {
		return fmt.Errorf("expected 2 push sessions for %s, got %d", tc.name, pushStats.SessionsCreated)
	}
	if pushStats.ChunksUploaded < 4 {
		return fmt.Errorf("expected multi-chunk conflict upload for %s, got %d chunks", tc.name, pushStats.ChunksUploaded)
	}

	targetExpectation := conflictUserSnapshot{
		Name:      tc.expectedName,
		Email:     tc.expectedEmail,
		UpdatedAt: expectedUpdatedAt(tc).UTC().Format(time.RFC3339Nano),
	}
	if err := assertUserSnapshot(ctx, s.app.db, tc.targetID, targetExpectation); err != nil {
		return fmt.Errorf("device 1 target mismatch for %s: %w", tc.name, err)
	}
	if err := assertUserSnapshot(ctx, s.device2App.db, tc.targetID, targetExpectation); err != nil {
		return fmt.Errorf("device 2 target mismatch for %s: %w", tc.name, err)
	}
	for siblingID, expected := range siblingExpectations {
		if err := assertUserSnapshot(ctx, s.app.db, siblingID, expected); err != nil {
			return fmt.Errorf("device 1 sibling mismatch for %s: %w", tc.name, err)
		}
		if err := assertUserSnapshot(ctx, s.device2App.db, siblingID, expected); err != nil {
			return fmt.Errorf("device 2 sibling mismatch for %s: %w", tc.name, err)
		}
	}

	if err := assertNoPendingPushRecoveryState(ctx, s.device2App.db); err != nil {
		return fmt.Errorf("device 2 left stale push recovery state for %s: %w", tc.name, err)
	}
	return s.verifyConvergedState(ctx, logger, tc.name)
}

func (s *ConflictsScenario) verifyConvergedState(ctx context.Context, logger *slog.Logger, label string) error {
	count1, err := s.app.GetUserCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to count users on device 1 for %s: %w", label, err)
	}
	count2, err := s.device2App.GetUserCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to count users on device 2 for %s: %w", label, err)
	}
	if count1 != count2 {
		return fmt.Errorf("user count mismatch after %s: device1=%d device2=%d", label, count1, count2)
	}

	lastSeq1, err := s.app.GetLastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read device 1 last bundle seq for %s: %w", label, err)
	}
	lastSeq2, err := s.device2App.GetLastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read device 2 last bundle seq for %s: %w", label, err)
	}
	if lastSeq1 != lastSeq2 {
		return fmt.Errorf("bundle checkpoint mismatch after %s: device1=%d device2=%d", label, lastSeq1, lastSeq2)
	}

	logger.Info("✅ Devices converged", "case", label, "users", count1, "last_bundle_seq_seen", lastSeq1)
	return nil
}

func (s *ConflictsScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}
	expectedCount := len(s.cases) * (1 + conflictScenarioSiblingCount)
	actualCount, err := verifier.CountActualBusinessRecords(ctx, "business.users", s.userID)
	if err != nil {
		return fmt.Errorf("failed to count server users for %s: %w", s.userID, err)
	}
	if actualCount != expectedCount {
		return fmt.Errorf("expected %d users on server for %s, found %d", expectedCount, s.userID, actualCount)
	}
	return nil
}

func (s *ConflictsScenario) Cleanup(ctx context.Context) error {
	var firstErr error
	if s.device2App != nil {
		if err := s.device2App.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close device 2 app: %w", err)
		}
		s.device2App = nil
	}
	if err := s.BaseScenario.Cleanup(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

type latestUpdatedAtResolver struct {
	logger *slog.Logger
}

func (r *latestUpdatedAtResolver) Resolve(conflict oversqlite.ConflictContext) oversqlite.MergeResult {
	if conflict.LocalOp != "UPDATE" || len(conflict.LocalPayload) == 0 || len(conflict.ServerRowPayload) == 0 {
		return oversqlite.AcceptServer{}
	}

	localPayload, localUpdatedAt, err := parseConflictPayload(conflict.LocalPayload)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("latestUpdatedAtResolver failed to parse local payload", "error", err.Error())
		}
		return oversqlite.AcceptServer{}
	}
	serverPayload, serverUpdatedAt, err := parseConflictPayload(conflict.ServerRowPayload)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("latestUpdatedAtResolver failed to parse server payload", "error", err.Error())
		}
		return oversqlite.AcceptServer{}
	}

	chosen := serverPayload
	chosenUpdatedAt := serverUpdatedAt
	if localUpdatedAt.After(serverUpdatedAt) || localUpdatedAt.Equal(serverUpdatedAt) {
		chosen = localPayload
		chosenUpdatedAt = localUpdatedAt
	}
	mergedPayload, err := json.Marshal(chosen)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("latestUpdatedAtResolver failed to marshal merged payload", "error", err.Error())
		}
		return oversqlite.AcceptServer{}
	}
	if r.logger != nil {
		r.logger.Info("🧩 latest-updated-at merge resolved conflict",
			"table", conflict.Table,
			"key", conflict.Key,
			"chosen_updated_at", chosenUpdatedAt.Format(time.RFC3339Nano))
	}
	return oversqlite.KeepMerged{MergedPayload: mergedPayload}
}

func parseConflictPayload(raw json.RawMessage) (map[string]any, time.Time, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to decode payload: %w", err)
	}
	updatedAtRaw, ok := payload["updated_at"].(string)
	if !ok {
		return nil, time.Time{}, fmt.Errorf("payload missing updated_at string")
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtRaw)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to parse updated_at %q: %w", updatedAtRaw, err)
	}
	return payload, updatedAt, nil
}

func updateUserWithTimestamp(ctx context.Context, db *sql.DB, userID, name, email string, updatedAt time.Time) error {
	_, err := db.ExecContext(ctx, `
		UPDATE users
		SET name = ?, email = ?, updated_at = ?
		WHERE id = ?
	`, name, email, updatedAt.UTC().Format(time.RFC3339Nano), userID)
	if err != nil {
		return fmt.Errorf("failed to update user %s: %w", userID, err)
	}
	return nil
}

func assertUserSnapshot(ctx context.Context, db *sql.DB, userID string, expected conflictUserSnapshot) error {
	var actual conflictUserSnapshot
	err := db.QueryRowContext(ctx, `
		SELECT name, email, updated_at
		FROM users
		WHERE id = ?
	`, userID).Scan(&actual.Name, &actual.Email, &actual.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to load user %s: %w", userID, err)
	}
	if actual.Name != expected.Name || actual.Email != expected.Email {
		return fmt.Errorf("expected name/email %+v, got %+v", expected, actual)
	}
	expectedTime, err := time.Parse(time.RFC3339Nano, expected.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to parse expected updated_at %q: %w", expected.UpdatedAt, err)
	}
	actualTime, err := time.Parse(time.RFC3339Nano, actual.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to parse actual updated_at %q: %w", actual.UpdatedAt, err)
	}
	if !actualTime.Equal(expectedTime) {
		return fmt.Errorf("expected updated_at %s, got %s", expectedTime.Format(time.RFC3339Nano), actualTime.Format(time.RFC3339Nano))
	}
	return nil
}

func assertNoPendingPushRecoveryState(ctx context.Context, db *sql.DB) error {
	var dirtyCount, outboundCount, stagedCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirtyCount); err != nil {
		return fmt.Errorf("failed to count dirty rows: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_push_outbound`).Scan(&outboundCount); err != nil {
		return fmt.Errorf("failed to count outbound rows: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_push_stage`).Scan(&stagedCount); err != nil {
		return fmt.Errorf("failed to count staged rows: %w", err)
	}
	if dirtyCount != 0 || outboundCount != 0 || stagedCount != 0 {
		return fmt.Errorf("expected clean local sync state, got dirty=%d outbound=%d staged=%d", dirtyCount, outboundCount, stagedCount)
	}
	return nil
}

func baselineName(caseName string) string {
	return fmt.Sprintf("%s baseline", caseName)
}

func baselineEmail(caseName string) string {
	return fmt.Sprintf("%s@example.com", caseName)
}

func siblingName(caseName string, index int) string {
	return fmt.Sprintf("%s sibling baseline %02d", caseName, index+1)
}

func siblingEmail(caseName string, index int) string {
	return fmt.Sprintf("%s-sibling-%02d@example.com", caseName, index+1)
}

func emailForName(name string) string {
	return fmt.Sprintf("%s@example.com", slugify(name))
}

func slugify(value string) string {
	out := make([]rune, 0, len(value))
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
			out = append(out, r+'a'-'A')
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

func expectedUpdatedAt(tc conflictCase) time.Time {
	switch tc.expectTargetVersion {
	case "local":
		return tc.localUpdatedAt
	default:
		return tc.serverUpdatedAt
	}
}
