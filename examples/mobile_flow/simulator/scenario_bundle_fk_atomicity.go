package simulator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversqlite"
)

// BundleFKAtomicityScenario verifies bundle checkpoint correctness across supported FK shapes.
type BundleFKAtomicityScenario struct {
	*BaseScenario
	device2App *MobileApp
	device3App *MobileApp

	userID    string
	device1ID string
	device2ID string
	device3ID string

	userRowID       string
	postRowID       string
	categoryRootID  string
	categoryChildID string
	teamID          string
	teamMemberID    string

	seedBundleSeq   int64
	deleteBundleSeq int64
}

// NewBundleFKAtomicityScenario creates a new bundle/FK atomicity scenario.
func NewBundleFKAtomicityScenario(simulator *Simulator) Scenario {
	return &BundleFKAtomicityScenario{
		BaseScenario: NewBaseScenario(simulator, "bundle-fk-atomicity"),
	}
}

func (s *BundleFKAtomicityScenario) Name() string {
	return "bundle-fk-atomicity"
}

func (s *BundleFKAtomicityScenario) Description() string {
	return "Verify bundle checkpoint correctness, dirty-pull rejection, and self-ref/cycle/cascade FK atomicity across devices"
}

func (s *BundleFKAtomicityScenario) Setup(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🔧 Setting up bundle/FK atomicity scenario")

	if s.config != nil && s.config.UserID != "" && s.config.UserID != "user-unknown" {
		s.userID = s.config.UserID
	} else {
		s.userID = "bundle-fk-user-" + uuid.NewString()[:8]
	}

	sourceBase := fmt.Sprintf("bundle-fk-%s", s.userID)
	if s.config != nil && s.config.SourceID != "" && s.config.SourceID != "device-unknown-001" {
		sourceBase = s.config.SourceID
	}
	s.device1ID = sourceBase + "-d1"
	s.device2ID = sourceBase + "-d2"
	s.device3ID = sourceBase + "-d3"

	device1App, err := s.createDeviceApp(s.device1ID, "Bundle Device 1")
	if err != nil {
		return fmt.Errorf("failed to create device 1 app: %w", err)
	}
	s.app = device1App
	s.simulator.currentApp = s.app

	device2App, err := s.createDeviceApp(s.device2ID, "Bundle Device 2")
	if err != nil {
		return fmt.Errorf("failed to create device 2 app: %w", err)
	}
	s.device2App = device2App

	device3App, err := s.createDeviceApp(s.device3ID, "Bundle Device 3")
	if err != nil {
		return fmt.Errorf("failed to create device 3 app: %w", err)
	}
	s.device3App = device3App

	for idx, app := range []*MobileApp{s.app, s.device2App, s.device3App} {
		if err := app.onLaunch(ctx); err != nil {
			return fmt.Errorf("failed to launch device %d app: %w", idx+1, err)
		}
	}

	return nil
}

func (s *BundleFKAtomicityScenario) createDeviceApp(deviceID, deviceName string) (*MobileApp, error) {
	simCfg := s.simulator.GetConfig()
	dbFile := filepath.Join("/tmp", fmt.Sprintf("mobile_flow_%s_%s_%d.db", deviceID, s.userID, time.Now().UnixNano()))

	oversqliteConfig := &oversqlite.Config{
		Schema:        "business",
		Tables:        managedSyncTables(),
		UploadLimit:   100,
		DownloadLimit: 100,
	}

	app, err := newMobileApp(&mobileAppConfig{
		DatabaseFile:     dbFile,
		ServerURL:        simCfg.ServerURL,
		UserID:           s.userID,
		SourceID:         deviceID,
		DeviceName:       deviceName,
		JWTSecret:        simCfg.JWTSecret,
		OversqliteConfig: oversqliteConfig,
		PreserveDB:       simCfg.PreserveDB,
		Logger:           s.simulator.GetLogger(),
	})
	if err != nil {
		return nil, err
	}
	return app, nil
}

func (s *BundleFKAtomicityScenario) Execute(ctx context.Context) error {
	logger := s.simulator.GetLogger()
	logger.Info("🎯 Executing bundle/FK atomicity scenario")

	for idx, app := range []*MobileApp{s.app, s.device2App, s.device3App} {
		if err := app.signIn(ctx, s.userID); err != nil {
			return fmt.Errorf("failed to sign in device %d: %w", idx+1, err)
		}
		app.sync.Stop()
	}

	s.userRowID = uuid.NewString()
	s.postRowID = uuid.NewString()
	s.categoryRootID = uuid.NewString()
	s.categoryChildID = uuid.NewString()
	s.teamID = uuid.NewString()
	s.teamMemberID = uuid.NewString()

	if err := s.seedSupportedFKGraph(ctx); err != nil {
		return err
	}

	pendingBeforePush, err := s.app.pendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to read pending count before seed push: %w", err)
	}
	if pendingBeforePush < 6 {
		return fmt.Errorf("expected at least 6 pending rows before seed push, got %d", pendingBeforePush)
	}

	if err := s.app.pushPending(ctx); err != nil {
		return fmt.Errorf("failed to push seed bundle: %w", err)
	}

	pendingAfterPush, err := s.app.pendingChangesCount(ctx)
	if err != nil {
		return fmt.Errorf("failed to read pending count after seed push: %w", err)
	}
	if pendingAfterPush != 0 {
		return fmt.Errorf("seed push left %d pending rows", pendingAfterPush)
	}

	s.seedBundleSeq, err = s.app.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read seed bundle seq: %w", err)
	}
	if s.seedBundleSeq <= 0 {
		return fmt.Errorf("expected positive seed bundle seq, got %d", s.seedBundleSeq)
	}

	if err := s.device2App.rebuildKeepSource(ctx); err != nil {
		return fmt.Errorf("failed to hydrate device 2: %w", err)
	}
	if err := s.device3App.rebuildKeepSource(ctx); err != nil {
		return fmt.Errorf("failed to hydrate device 3: %w", err)
	}

	if err := s.assertGraphCounts(ctx, s.device2App, 1, 1, 2, 1, 1); err != nil {
		return fmt.Errorf("device 2 seed state mismatch: %w", err)
	}
	if err := s.assertGraphCounts(ctx, s.device3App, 1, 1, 2, 1, 1); err != nil {
		return fmt.Errorf("device 3 seed state mismatch: %w", err)
	}

	device2SeqBeforeDirtyPull, err := s.device2App.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read device 2 checkpoint before dirty pull: %w", err)
	}
	if _, err := s.device2App.db.ExecContext(ctx, `
		UPDATE categories
		SET name = ?
		WHERE id = ?
	`, "Child Draft", s.categoryChildID); err != nil {
		return fmt.Errorf("failed to create local dirty category update on device 2: %w", err)
	}

	_, pullErr := s.device2App.currentClient().PullToStable(ctx)
	var dirtyErr *oversqlite.DirtyStateRejectedError
	if pullErr == nil || !errors.As(pullErr, &dirtyErr) {
		return fmt.Errorf("expected dirty-state rejection on device 2 pull, got: %v", pullErr)
	}

	device2SeqAfterDirtyPull, err := s.device2App.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read device 2 checkpoint after dirty pull: %w", err)
	}
	if device2SeqAfterDirtyPull != device2SeqBeforeDirtyPull {
		return fmt.Errorf("device 2 checkpoint advanced despite dirty pull rejection: before=%d after=%d", device2SeqBeforeDirtyPull, device2SeqAfterDirtyPull)
	}

	if err := s.deleteSupportedFKGraph(ctx); err != nil {
		return err
	}
	if err := s.app.pushPending(ctx); err != nil {
		return fmt.Errorf("failed to push supported FK graph delete bundle: %w", err)
	}

	s.deleteBundleSeq, err = s.app.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read delete bundle seq: %w", err)
	}
	if s.deleteBundleSeq <= s.seedBundleSeq {
		return fmt.Errorf("expected delete bundle seq > seed bundle seq, got seed=%d delete=%d", s.seedBundleSeq, s.deleteBundleSeq)
	}

	applied, nextAfter, err := s.device3App.pullToStable(ctx)
	if err != nil {
		return fmt.Errorf("failed to pull cascade delete bundle on device 3: %w", err)
	}
	if applied != 1 {
		return fmt.Errorf("expected exactly 1 applied bundle for cascade delete, got %d", applied)
	}
	if nextAfter != s.deleteBundleSeq {
		return fmt.Errorf("device 3 checkpoint mismatch after cascade delete pull: got %d want %d", nextAfter, s.deleteBundleSeq)
	}

	if err := s.assertGraphCounts(ctx, s.device3App, 0, 0, 0, 0, 0); err != nil {
		return fmt.Errorf("device 3 delete state mismatch: %w", err)
	}

	device3SeqAfterDelete, err := s.device3App.lastServerSeqSeen(ctx)
	if err != nil {
		return fmt.Errorf("failed to read device 3 checkpoint after cascade delete: %w", err)
	}
	if device3SeqAfterDelete != s.deleteBundleSeq {
		return fmt.Errorf("device 3 durable checkpoint mismatch: got %d want %d", device3SeqAfterDelete, s.deleteBundleSeq)
	}

	logger.Info("✅ Bundle/FK atomicity scenario completed",
		"seed_bundle_seq", s.seedBundleSeq,
		"delete_bundle_seq", s.deleteBundleSeq,
		"device2_dirty_pull_rejected", true)
	return nil
}

func (s *BundleFKAtomicityScenario) Verify(ctx context.Context, verifier *DatabaseVerifier) error {
	if verifier == nil {
		return nil
	}

	userID := strings.ReplaceAll(s.userID, "'", "''")
	schema := "business"
	liveUsersQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_id = '%s'
		  AND schema_name = '%s'
		  AND table_name = 'users'
		  AND deleted = FALSE
	`, userID, schema)
	livePostsQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_id = '%s'
		  AND schema_name = '%s'
		  AND table_name = 'posts'
		  AND deleted = FALSE
	`, userID, schema)
	liveCategoriesQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_id = '%s'
		  AND schema_name = '%s'
		  AND table_name = 'categories'
		  AND deleted = FALSE
	`, userID, schema)
	liveTeamsQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_id = '%s'
		  AND schema_name = '%s'
		  AND table_name = 'teams'
		  AND deleted = FALSE
	`, userID, schema)
	liveTeamMembersQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM sync.row_state
		WHERE user_id = '%s'
		  AND schema_name = '%s'
		  AND table_name = 'team_members'
		  AND deleted = FALSE
	`, userID, schema)
	bundleCountQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM sync.bundle_log
		WHERE user_id = '%s'
	`, userID)

	liveUsers, err := verifier.CountRecordsWithQuery(ctx, liveUsersQuery)
	if err != nil {
		return fmt.Errorf("failed to verify live user row_state count: %w", err)
	}
	livePosts, err := verifier.CountRecordsWithQuery(ctx, livePostsQuery)
	if err != nil {
		return fmt.Errorf("failed to verify live post row_state count: %w", err)
	}
	liveCategories, err := verifier.CountRecordsWithQuery(ctx, liveCategoriesQuery)
	if err != nil {
		return fmt.Errorf("failed to verify live category row_state count: %w", err)
	}
	liveTeams, err := verifier.CountRecordsWithQuery(ctx, liveTeamsQuery)
	if err != nil {
		return fmt.Errorf("failed to verify live team row_state count: %w", err)
	}
	liveTeamMembers, err := verifier.CountRecordsWithQuery(ctx, liveTeamMembersQuery)
	if err != nil {
		return fmt.Errorf("failed to verify live team_member row_state count: %w", err)
	}
	bundleCount, err := verifier.CountRecordsWithQuery(ctx, bundleCountQuery)
	if err != nil {
		return fmt.Errorf("failed to verify bundle log count: %w", err)
	}

	if liveUsers != 0 || livePosts != 0 || liveCategories != 0 || liveTeams != 0 || liveTeamMembers != 0 {
		return fmt.Errorf(
			"expected no live rows after delete bundle, got users=%d posts=%d categories=%d teams=%d team_members=%d",
			liveUsers, livePosts, liveCategories, liveTeams, liveTeamMembers,
		)
	}
	if bundleCount < 2 {
		return fmt.Errorf("expected at least 2 committed bundles, got %d", bundleCount)
	}
	return nil
}

func (s *BundleFKAtomicityScenario) Cleanup(ctx context.Context) error {
	var errs []error
	for _, app := range []*MobileApp{s.app, s.device2App, s.device3App} {
		if app == nil {
			continue
		}
		if err := app.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (s *BundleFKAtomicityScenario) seedSupportedFKGraph(ctx context.Context) error {
	if err := s.app.createUserWithID(ctx, s.userRowID, "Ada", "ada@example.com"); err != nil {
		return fmt.Errorf("failed to create seed user: %w", err)
	}
	if err := s.app.createPostWithID(ctx, s.postRowID, s.userRowID, "Hello", "Seed post"); err != nil {
		return fmt.Errorf("failed to create seed post: %w", err)
	}
	if _, err := s.app.db.ExecContext(ctx, `
		INSERT INTO categories (id, name, parent_id) VALUES (?, ?, NULL)
	`, s.categoryRootID, "Root Category"); err != nil {
		return fmt.Errorf("failed to create root category: %w", err)
	}
	if _, err := s.app.db.ExecContext(ctx, `
		INSERT INTO categories (id, name, parent_id) VALUES (?, ?, ?)
	`, s.categoryChildID, "Child Category", s.categoryRootID); err != nil {
		return fmt.Errorf("failed to create child category: %w", err)
	}

	tx, err := s.app.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin team cycle transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for team cycle seed: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO teams (id, name, captain_member_id) VALUES (?, ?, ?)
	`, s.teamID, "Alpha Team", s.teamMemberID); err != nil {
		return fmt.Errorf("failed to create team cycle parent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO team_members (id, name, team_id) VALUES (?, ?, ?)
	`, s.teamMemberID, "Captain Ada", s.teamID); err != nil {
		return fmt.Errorf("failed to create team cycle member: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit team cycle seed transaction: %w", err)
	}
	return nil
}

func (s *BundleFKAtomicityScenario) deleteSupportedFKGraph(ctx context.Context) error {
	tx, err := s.app.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin delete graph transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return fmt.Errorf("failed to defer foreign keys for delete graph transaction: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM categories WHERE id = ?`, s.categoryRootID); err != nil {
		return fmt.Errorf("failed to delete root category: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM teams WHERE id = ?`, s.teamID); err != nil {
		return fmt.Errorf("failed to delete team: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM users WHERE id = ?`, s.userRowID); err != nil {
		return fmt.Errorf("failed to delete seed user: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit delete graph transaction: %w", err)
	}
	return nil
}

func (s *BundleFKAtomicityScenario) assertGraphCounts(ctx context.Context, app *MobileApp, expectedUsers, expectedPosts, expectedCategories, expectedTeams, expectedTeamMembers int) error {
	var userCount int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&userCount); err != nil {
		return fmt.Errorf("failed to count users: %w", err)
	}
	var postCount int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM posts`).Scan(&postCount); err != nil {
		return fmt.Errorf("failed to count posts: %w", err)
	}
	var categoryCount int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM categories`).Scan(&categoryCount); err != nil {
		return fmt.Errorf("failed to count categories: %w", err)
	}
	var teamCount int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM teams`).Scan(&teamCount); err != nil {
		return fmt.Errorf("failed to count teams: %w", err)
	}
	var teamMemberCount int
	if err := app.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM team_members`).Scan(&teamMemberCount); err != nil {
		return fmt.Errorf("failed to count team_members: %w", err)
	}
	if userCount != expectedUsers || postCount != expectedPosts || categoryCount != expectedCategories || teamCount != expectedTeams || teamMemberCount != expectedTeamMembers {
		return fmt.Errorf(
			"expected users=%d posts=%d categories=%d teams=%d team_members=%d, got users=%d posts=%d categories=%d teams=%d team_members=%d",
			expectedUsers, expectedPosts, expectedCategories, expectedTeams, expectedTeamMembers,
			userCount, postCount, categoryCount, teamCount, teamMemberCount,
		)
	}
	return nil
}
