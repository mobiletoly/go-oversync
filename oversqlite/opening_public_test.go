package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type publicOpeningMigrator struct{}

func (publicOpeningMigrator) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (publicOpeningMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
	if _, err := tx.ExecContext(ctx, `CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `PRAGMA user_version=1`)
	return err
}

func publicOpeningConfig(path string) (*OpeningConfig, *Config) {
	target := DefaultConfig(1, []SyncTable{{TableName: "items", SyncKeyColumns: []string{"id"}}})
	target.DatabaseID = "public-opening-db"
	target.ServiceID = "public-opening-service"
	target.LocalVersion = 1
	plan := &UpgradePlan{
		PlanID: "public-opening-fresh-v1", Mode: UpgradeModeFresh,
		DatabaseID: target.DatabaseID, ServiceID: target.ServiceID,
		TargetGenerationID: 1, TargetVersion: 1, Migrator: publicOpeningMigrator{},
	}
	return &OpeningConfig{DatabasePath: path, Plan: plan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target}, target
}

type guardedPublicMigrator struct {
	installedCalls *atomic.Int64
	allowedCalls   int64
	migrate        func(context.Context, LocalMigrationTx, LocalMigrationStep) error
}

func (m guardedPublicMigrator) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	if m.installedCalls != nil && m.installedCalls.Add(1) > m.allowedCalls {
		return 0, errors.New("installed version must not be read after the ready commit")
	}
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m guardedPublicMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, step LocalMigrationStep) error {
	return m.migrate(ctx, tx, step)
}

func publicOpeningConfigForPlan(path string, plan UpgradePlan) *OpeningConfig {
	target := DefaultConfig(plan.TargetGenerationID, cloneSyncTables(plan.testTarget.Tables))
	target.DatabaseID = plan.DatabaseID
	target.ServiceID = plan.ServiceID
	target.LocalVersion = plan.TargetVersion
	target.LocalOnlyTables = append([]string(nil), plan.LocalOnlyTables...)
	return &OpeningConfig{
		DatabasePath: path, Plan: &plan, BaseURL: "http://opening.test", Token: tokenProviderForTests,
		TargetClientConfig: target,
	}
}

func TestNewOpening_ClaimsOwnershipBeforeSQLiteConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned.sqlite")
	config, _ := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)

	_, err = NewOpening(config)
	var owned *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &owned)

	symlink := filepath.Join(filepath.Dir(path), "owned-link.sqlite")
	require.NoError(t, os.Symlink(path, symlink))
	aliasConfig, _ := publicOpeningConfig(symlink)
	_, err = NewOpening(aliasConfig)
	require.ErrorAs(t, err, &owned)

	hardlink := filepath.Join(filepath.Dir(path), "owned-hard.sqlite")
	require.NoError(t, os.Link(path, hardlink))
	hardConfig, _ := publicOpeningConfig(hardlink)
	_, err = NewOpening(hardConfig)
	require.ErrorAs(t, err, &owned)
	require.NoError(t, opening.Close())
}

func TestNewOpening_DeepClonesPlanAndTargetConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloned.sqlite")
	config, target := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)

	config.Plan.PlanID = "mutated"
	config.Plan.LocalOnlyTables = append(config.Plan.LocalOnlyTables, "late_table")
	target.Tables[0].TableName = "late_table"
	target.Tables[0].SyncKeyColumns[0] = "late_key"
	target.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 99}

	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, "public-opening-fresh-v1", result.Status.PlanID)
	require.Equal(t, "items", result.Ready.config.Tables[0].TableName)
	require.Equal(t, []string{"id"}, result.Ready.config.Tables[0].SyncKeyColumns)
	require.NoError(t, result.Ready.Close())
}

func TestOpening_AlreadyReadyReturnsOwnedClientWithoutNetwork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.sqlite")
	config, target := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	require.NoError(t, result.Ready.Close())

	var requests atomic.Int64
	reopened, err := NewOpening(&OpeningConfig{
		DatabasePath: path, BaseURL: "http://opening.test",
		Token: func(context.Context) (string, error) { t.Fatal("ready reopen requested a token"); return "", nil },
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return nil, errors.New("unexpected network request")
		})},
		TargetClientConfig: target,
	})
	require.NoError(t, err)
	result, err = reopened.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	require.NotNil(t, result.Ready.Database())
	require.Zero(t, requests.Load())
	require.NoError(t, result.Ready.Close())
}

func TestOpening_ReadyCommitTransfersExistingOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transfer.sqlite")
	config, _ := publicOpeningConfig(path)
	reached := make(chan struct{})
	release := make(chan struct{})
	config.testBoundary = func(name string) {
		if name == "before_ready_transfer" {
			close(reached)
			<-release
		}
	}
	opening, err := NewOpening(config)
	require.NoError(t, err)
	type advanceResult struct {
		result OpeningResult
		err    error
	}
	done := make(chan advanceResult, 1)
	go func() { result, err := opening.Advance(context.Background()); done <- advanceResult{result, err} }()
	<-reached
	competitorConfig, _ := publicOpeningConfig(path)
	_, err = NewOpening(competitorConfig)
	var owned *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &owned)
	close(release)
	advanced := <-done
	require.NoError(t, advanced.err)
	require.NotNil(t, advanced.result.Ready)
	require.NoError(t, advanced.result.Ready.Close())
}

func TestOpening_LateCancellationAfterReadyCommitReturnsReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "late-cancel.sqlite")
	config, _ := publicOpeningConfig(path)
	ctx, cancel := context.WithCancel(context.Background())
	config.testBoundary = func(name string) {
		if name == "after_U4_commit" {
			cancel()
		}
	}
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NotNil(t, result.Ready)
	require.NoError(t, result.Ready.Close())
}

func TestOpening_CommittedReadyPublicationDoesNotRevalidate(t *testing.T) {
	t.Run("U4", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ready-u4.sqlite")
		var calls atomic.Int64
		target := DefaultConfig(1, []SyncTable{{TableName: "items", SyncKeyColumns: []string{"id"}}})
		target.DatabaseID, target.ServiceID, target.LocalVersion = "ready-u4-db", "ready-service", 1
		plan := UpgradePlan{
			PlanID: "ready-u4", Mode: UpgradeModeFresh, DatabaseID: target.DatabaseID, ServiceID: target.ServiceID,
			TargetGenerationID: target.GenerationID, TargetVersion: target.LocalVersion,
			Migrator: guardedPublicMigrator{installedCalls: &calls, allowedCalls: 2, migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
				_, err := tx.ExecContext(ctx, `CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL); PRAGMA user_version=1`)
				return err
			}},
		}
		var readyProgress atomic.Int64
		opening, err := NewOpening(&OpeningConfig{
			DatabasePath: path, Plan: &plan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target,
			Progress: func(status OpeningStatus) {
				if status.Outcome == UpgradeOutcomeReady {
					readyProgress.Add(1)
				}
			},
		})
		require.NoError(t, err)
		result, err := opening.Advance(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result.Ready)
		require.EqualValues(t, 2, calls.Load())
		require.Zero(t, readyProgress.Load())
		require.NoError(t, result.Ready.Close())
	})

	t.Run("U8", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ready-u8.sqlite")
		plan := prepareU8ProcessTestDatabase(t, path)
		var calls atomic.Int64
		plan.Migrator = guardedPublicMigrator{installedCalls: &calls, allowedCalls: 1, migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error {
			return errors.New("completed local migration must not run")
		}}
		config := publicOpeningConfigForPlan(path, plan)
		config.BaseURL = "http://process.invalid"
		config.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/process-snapshot":
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
			default:
				return nil, errors.New("unexpected ready U8 request")
			}
		})}
		var readyProgress atomic.Int64
		config.Progress = func(status OpeningStatus) {
			if status.Outcome == UpgradeOutcomeReady {
				readyProgress.Add(1)
			}
		}
		opening, err := NewOpening(config)
		require.NoError(t, err)
		result, err := opening.Advance(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result.Ready)
		require.EqualValues(t, 1, calls.Load())
		require.Zero(t, readyProgress.Load())
		require.NoError(t, result.Ready.Close())
	})
}

func TestOpening_StatusConcurrentWithReadyTransfer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status-transfer.sqlite")
	config, _ := publicOpeningConfig(path)
	reached := make(chan struct{})
	release := make(chan struct{})
	config.testBoundary = func(name string) {
		if name == "before_ready_transfer" {
			close(reached)
			<-release
		}
	}
	opening, err := NewOpening(config)
	require.NoError(t, err)
	type advanceResult struct {
		result OpeningResult
		err    error
	}
	advanced := make(chan advanceResult, 1)
	go func() {
		result, advanceErr := opening.Advance(context.Background())
		advanced <- advanceResult{result: result, err: advanceErr}
	}()
	<-reached
	statusDone := make(chan error, 1)
	go func() {
		for index := 0; index < 100; index++ {
			_, statusErr := opening.Status(context.Background())
			var closed *ClientClosedError
			if statusErr != nil && !errors.As(statusErr, &closed) {
				statusDone <- statusErr
				return
			}
		}
		statusDone <- nil
	}()
	close(release)
	got := <-advanced
	require.NoError(t, got.err)
	require.NoError(t, <-statusDone)
	require.NoError(t, got.result.Ready.Close())
}

func TestOpening_ReadyReopenValidatesCaptureTriggers(t *testing.T) {
	for _, scenario := range []string{"reject missing", "reject altered"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ready-trigger.sqlite")
			config, target := publicOpeningConfig(path)
			opening, err := NewOpening(config)
			require.NoError(t, err)
			result, err := opening.Advance(context.Background())
			require.NoError(t, err)
			require.NoError(t, result.Ready.Close())
			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			_, err = db.Exec(`DROP TRIGGER trg_items_ai`)
			require.NoError(t, err)
			if scenario == "reject altered" {
				_, err = db.Exec(`CREATE TRIGGER trg_items_ai AFTER INSERT ON items BEGIN DELETE FROM _sync_dirty_rows; END`)
				require.NoError(t, err)
			}
			require.NoError(t, db.Close())
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			var callbacks atomic.Int64
			config.Plan.Migrator = testLocalMigrator{
				installed: func(context.Context, LocalMigrationReader) (int64, error) { callbacks.Add(1); return 1, nil },
				migrate:   func(context.Context, LocalMigrationTx, LocalMigrationStep) error { callbacks.Add(1); return nil },
			}
			config.TargetClientConfig = target
			config.Token = func(context.Context) (string, error) { callbacks.Add(1); return "token", nil }
			reopened, err := NewOpening(config)
			require.ErrorContains(t, err, "trigger")
			require.Nil(t, reopened)
			require.Zero(t, callbacks.Load())
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "all retained DDL, data and journal bytes must remain unchanged")
			_, err = os.Stat(path + "-wal")
			require.True(t, os.IsNotExist(err), "rejection must not configure WAL")
		})
	}
}

func TestOpening_PlanFreeReadyReopenSupportsApplicationVersionLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "application-ledger.sqlite")
	target := DefaultConfig(1, []SyncTable{{TableName: "items", SyncKeyColumns: []string{"id"}}})
	target.DatabaseID, target.ServiceID, target.LocalVersion = "ledger-db", "ledger-service", 7
	target.LocalOnlyTables = []string{"app_version"}
	migrator := testLocalMigrator{
		installed: func(ctx context.Context, reader LocalMigrationReader) (int64, error) {
			var version int64
			err := reader.QueryRowContext(ctx, `SELECT version FROM app_version`).Scan(&version)
			if err != nil {
				return 0, nil
			}
			return version, nil
		},
		migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL); CREATE TABLE app_version(version INTEGER NOT NULL); INSERT INTO app_version VALUES(7)`)
			return err
		},
	}
	plan := UpgradePlan{PlanID: "application-ledger", Mode: UpgradeModeFresh, DatabaseID: target.DatabaseID, ServiceID: target.ServiceID, TargetGenerationID: 1, TargetVersion: 7, LocalOnlyTables: []string{"app_version"}, Migrator: migrator}
	opening, err := NewOpening(&OpeningConfig{DatabasePath: path, Plan: &plan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target})
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, result.Ready.Close())

	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	var userVersion int64
	require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&userVersion))
	require.Zero(t, userVersion)
	require.NoError(t, db.Close())

	reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target})
	require.NoError(t, err)
	result, err = reopened.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready.Database())
	require.NoError(t, result.Ready.Close())
}

func TestOpening_DescriptorFreeLocalOnlyMigrationUsesPublicPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "descriptor-free-local.sqlite")
	tables := []SyncTable{{TableName: "items", SyncKeyColumns: []string{"id"}}}
	initial := DefaultConfig(1, tables)
	initial.DatabaseID, initial.ServiceID, initial.LocalVersion = "local-db", "local-service", 1
	initial.LocalOnlyTables = []string{"local_notes"}
	freshPlan := UpgradePlan{
		PlanID: "descriptor-free-fresh", Mode: UpgradeModeFresh, DatabaseID: initial.DatabaseID, ServiceID: initial.ServiceID,
		TargetGenerationID: 1, TargetVersion: 1, LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL,quantity INTEGER NOT NULL,payload BLOB NOT NULL); CREATE TABLE local_notes(id TEXT PRIMARY KEY NOT NULL,note TEXT NOT NULL); INSERT INTO local_notes VALUES('note','keep'); PRAGMA user_version=1`)
			return err
		}},
	}
	opening, err := NewOpening(&OpeningConfig{DatabasePath: path, Plan: &freshPlan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: initial})
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	_, err = result.Ready.Database().Exec(`INSERT INTO items VALUES('pending','preserve',42,x'0001ff')`)
	require.NoError(t, err)
	require.NoError(t, result.Ready.Close())

	target := DefaultConfig(1, tables)
	target.DatabaseID, target.ServiceID, target.LocalVersion = initial.DatabaseID, initial.ServiceID, 2
	target.LocalOnlyTables = []string{"local_notes"}
	localPlan := UpgradePlan{
		PlanID: "descriptor-free-local-v2", Mode: UpgradeModeLocalOnly, DatabaseID: target.DatabaseID, ServiceID: target.ServiceID,
		InstalledGenerationID: 1, TargetGenerationID: 1, InstalledVersion: 1, TargetVersion: 2,
		LocalOnlyTables: []string{"local_notes"}, Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `PRAGMA user_version=2`)
			return err
		}},
	}
	reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, Plan: &localPlan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target})
	require.NoError(t, err)
	result, err = reopened.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Ready)
	var value, note, payloadType string
	var quantity int
	var payload []byte
	require.NoError(t, result.Ready.Database().QueryRow(`SELECT value,quantity,payload,typeof(payload) FROM items WHERE id='pending'`).Scan(&value, &quantity, &payload, &payloadType))
	require.Equal(t, "preserve", value)
	require.Equal(t, 42, quantity)
	require.Equal(t, []byte{0x00, 0x01, 0xff}, payload)
	require.Equal(t, "blob", payloadType)
	require.NoError(t, result.Ready.Database().QueryRow(`SELECT note FROM local_notes WHERE id='note'`).Scan(&note))
	require.Equal(t, "keep", note)
	var pending, descriptors int
	require.NoError(t, result.Ready.Database().QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name='items'`).Scan(&pending))
	require.Equal(t, 1, pending)
	require.NoError(t, result.Ready.Database().QueryRow(`SELECT COUNT(*) FROM _sync_admitted_contracts`).Scan(&descriptors))
	require.Zero(t, descriptors)
	require.NoError(t, result.Ready.Close())
}

func TestNewOpening_RejectsIncompleteOrMismatchedTargetDeclaration(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "blank database", mutate: func(config *Config) { config.DatabaseID = "" }},
		{name: "blank service", mutate: func(config *Config) { config.ServiceID = "" }},
		{name: "different database", mutate: func(config *Config) { config.DatabaseID = "different" }},
		{name: "different service", mutate: func(config *Config) { config.ServiceID = "different" }},
		{name: "different version", mutate: func(config *Config) { config.LocalVersion = 2 }},
		{name: "different local-only set", mutate: func(config *Config) { config.LocalOnlyTables = []string{"other"} }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid-target.sqlite")
			config, target := publicOpeningConfig(path)
			scenario.mutate(target)
			_, err := NewOpening(config)
			require.Error(t, err)
			db, openErr := sql.Open("sqlite3", path)
			require.NoError(t, openErr)
			var managed int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '_sync_%'`).Scan(&managed))
			require.Zero(t, managed)
			require.NoError(t, db.Close())
		})
	}
}

func TestOpening_NoApplicationCapabilityBeforeReady(t *testing.T) {
	t.Run("fresh", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fresh.sqlite")
		config, _ := publicOpeningConfig(path)
		ctx, cancel := context.WithCancel(context.Background())
		config.testBoundary = func(name string) {
			if name == "before_U4_commit" {
				cancel()
			}
		}
		opening, err := NewOpening(config)
		require.NoError(t, err)
		result, err := opening.Advance(ctx)
		require.Error(t, err)
		require.Nil(t, result.Ready)
		require.NoError(t, opening.Close())
	})

	t.Run("local-only", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "local.sqlite")
		config, target := publicOpeningConfig(path)
		opening, err := NewOpening(config)
		require.NoError(t, err)
		result, err := opening.Advance(context.Background())
		require.NoError(t, err)
		require.NoError(t, result.Ready.Close())
		localTarget := cloneCoordinatorClientConfig(target)
		localTarget.LocalVersion = 2
		plan := UpgradePlan{PlanID: "nonready-local", Mode: UpgradeModeLocalOnly, DatabaseID: target.DatabaseID, ServiceID: target.ServiceID, InstalledGenerationID: 1, TargetGenerationID: 1, InstalledVersion: 1, TargetVersion: 2, Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, migrateErr := tx.ExecContext(ctx, `PRAGMA user_version=2`)
			return migrateErr
		}}}
		ctx, cancel := context.WithCancel(context.Background())
		reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, Plan: &plan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: localTarget, testBoundary: func(name string) {
			if name == "before_U4_commit" {
				cancel()
			}
		}})
		require.NoError(t, err)
		result, err = reopened.Advance(ctx)
		require.Error(t, err)
		require.Nil(t, result.Ready)
		require.NoError(t, reopened.Close())
	})

	t.Run("generation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "generation.sqlite")
		plan := prepareU8ProcessTestDatabase(t, path)
		config := publicOpeningConfigForPlan(path, plan)
		ctx, cancel := context.WithCancel(context.Background())
		config.testBoundary = func(name string) {
			if name == "before_U8_commit" {
				cancel()
			}
		}
		config.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			}
			return nil, errors.New("unexpected non-ready generation request")
		})}
		opening, err := NewOpening(config)
		require.NoError(t, err)
		result, err := opening.Advance(ctx)
		require.Error(t, err)
		require.Nil(t, result.Ready)
		require.NoError(t, opening.Close())
	})
}

func TestOpening_ActionableStopsRetainOwnershipWithoutDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cancelled.sqlite")
	config, _ := publicOpeningConfig(path)
	ctx, cancel := context.WithCancel(context.Background())
	config.testBoundary = func(name string) {
		if name == "before_U4_commit" {
			cancel()
		}
	}
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(ctx)
	require.Error(t, err)
	require.Nil(t, result.Ready)
	require.Equal(t, UpgradeOutcomeCancelled, result.Status.Outcome)

	competitorConfig, _ := publicOpeningConfig(path)
	_, err = NewOpening(competitorConfig)
	var owned *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &owned)
	require.NoError(t, opening.Close())
}

func TestOpening_ProgressCallbackCanReadStatusAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "callback-close.sqlite")
	config, _ := publicOpeningConfig(path)
	var opening *Opening
	var callbackStatus OpeningStatus
	config.Progress = func(OpeningStatus) {
		status, err := opening.Status(context.Background())
		require.NoError(t, err)
		callbackStatus = status
		require.NoError(t, opening.Close())
	}
	var err error
	opening, err = NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.Error(t, err)
	require.Nil(t, result.Ready)
	require.Equal(t, UpgradeOutcomeCancelled, result.Status.Outcome)
	require.Equal(t, UpgradePhaseMigratingLocal, callbackStatus.Phase)
}

func TestOpening_CloseRetainsOwnershipUntilActiveWorkStopsAndClosesPool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-active.sqlite")
	config, _ := publicOpeningConfig(path)
	reached := make(chan struct{})
	release := make(chan struct{})
	config.testBoundary = func(name string) {
		if name == "before_U4_commit" {
			close(reached)
			<-release
		}
	}
	opening, err := NewOpening(config)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, err := opening.Advance(context.Background()); done <- err }()
	<-reached
	require.NoError(t, opening.Close())
	competitorConfig, _ := publicOpeningConfig(path)
	_, err = NewOpening(competitorConfig)
	var owned *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &owned)
	close(release)
	require.Error(t, <-done)
	require.Eventually(t, func() bool {
		next, openErr := NewOpening(competitorConfig)
		if openErr != nil {
			return false
		}
		_ = next.Close()
		return true
	}, time.Second, 10*time.Millisecond)
}

func TestClient_CloseClosesOwnedDatabaseAndReleasesOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client-close.sqlite")
	config, target := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	client := result.Ready
	pool := client.Database()
	require.NoError(t, client.Start(context.Background()))
	require.NoError(t, client.Stop(context.Background()))
	require.NoError(t, client.Close())
	require.Nil(t, client.Database())
	require.Error(t, pool.Ping())
	reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target})
	require.NoError(t, err)
	reopenedResult, err := reopened.Advance(context.Background())
	require.NoError(t, err)
	require.NotNil(t, reopenedResult.Ready)
	require.NoError(t, reopenedResult.Ready.Close())
}

func TestClient_StartRejectsSecondOwnedLoopPair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client-start.sqlite")
	config, _ := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	client := result.Ready
	require.NoError(t, client.Start(context.Background()))
	require.ErrorContains(t, client.Start(context.Background()), "already running")
	require.NoError(t, client.Stop(context.Background()))
	require.NoError(t, client.Close())
}

func TestClient_StopJoinsAndIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client-stop.sqlite")
	config, _ := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	client := result.Ready
	loopCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, client.Start(loopCtx))
	cancel()
	require.NoError(t, client.Stop(context.Background()))
	require.NoError(t, client.Stop(context.Background()))
	require.NoError(t, client.Close())
}

func TestClient_StartCannotOutliveTerminalShutdown(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		terminal func(*Client) error
	}{
		{name: "close", terminal: func(client *Client) error { return client.Close() }},
		{name: "uninstall", terminal: func(client *Client) error { return client.UninstallSync(context.Background()) }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "terminal-start.sqlite")
			config, _ := publicOpeningConfig(path)
			opening, err := NewOpening(config)
			require.NoError(t, err)
			result, err := opening.Advance(context.Background())
			require.NoError(t, err)
			client := result.Ready
			pool := client.Database()

			reached := make(chan struct{})
			release := make(chan struct{})
			client.beforeStartAdmissionHook = func() {
				close(reached)
				<-release
			}
			startResult := make(chan error, 1)
			go func() { startResult <- client.Start(context.Background()) }()
			<-reached

			terminalResult := make(chan error, 1)
			go func() { terminalResult <- scenario.terminal(client) }()
			require.NoError(t, <-terminalResult)
			close(release)

			var closedErr *ClientClosedError
			require.ErrorAs(t, <-startResult, &closedErr)
			client.loopMu.Lock()
			require.Nil(t, client.loopCancel)
			require.Nil(t, client.loopDone)
			client.loopMu.Unlock()
			require.Error(t, pool.Ping())
			require.NoError(t, client.Close())
		})
	}
}

func TestUninstallSync_RollbackKeepsClientLive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uninstall-rollback.sqlite")
	config, _ := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	client := result.Ready
	_, err = client.Database().Exec(`INSERT INTO items(id,value) VALUES('pending','keep')`)
	require.NoError(t, err)
	require.Error(t, client.UninstallSync(context.Background()))
	require.NotNil(t, client.Database())
	require.NoError(t, client.Database().Ping())
	require.NoError(t, client.Start(context.Background()))
	require.NoError(t, client.Stop(context.Background()))
	require.NoError(t, client.Close())
}

func TestUninstallSync_SuccessIsTerminalAndRejectsSameFileReinstall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uninstall-terminal.sqlite")
	config, _ := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	client := result.Ready
	pool := client.Database()
	require.NoError(t, client.UninstallSync(context.Background()))
	require.Nil(t, client.Database())
	require.Error(t, pool.Ping())
	require.NoError(t, client.Close())

	diagnostic, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	var itemsTable int
	require.NoError(t, diagnostic.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='items'`).Scan(&itemsTable))
	require.Equal(t, 1, itemsTable)
	require.NoError(t, diagnostic.Close())

	reinstallConfig, _ := publicOpeningConfig(path)
	reinstall, err := NewOpening(reinstallConfig)
	require.NoError(t, err)
	reinstallResult, err := reinstall.Advance(context.Background())
	require.Error(t, err)
	require.Nil(t, reinstallResult.Ready)
	require.NoError(t, reinstall.Close())
}

func TestOpening_NoExportedLegacySurface(t *testing.T) {
	openingConfigFields := map[string]reflect.Type{
		"DatabasePath":       reflect.TypeOf(""),
		"Plan":               reflect.TypeOf((*UpgradePlan)(nil)),
		"BaseURL":            reflect.TypeOf(""),
		"Token":              reflect.TypeOf((func(context.Context) (string, error))(nil)),
		"HTTP":               reflect.TypeOf((*http.Client)(nil)),
		"TargetClientConfig": reflect.TypeOf((*Config)(nil)),
		"Progress":           reflect.TypeOf((func(OpeningStatus))(nil)),
	}
	openingConfigType := reflect.TypeOf(OpeningConfig{})
	exportedFields := 0
	for index := 0; index < openingConfigType.NumField(); index++ {
		field := openingConfigType.Field(index)
		if !field.IsExported() {
			continue
		}
		exportedFields++
		want, accepted := openingConfigFields[field.Name]
		require.Truef(t, accepted, "unexpected exported OpeningConfig field %s", field.Name)
		require.Equal(t, want, field.Type, "OpeningConfig.%s changed type", field.Name)
	}
	require.Equal(t, len(openingConfigFields), exportedFields)
	clientType := reflect.TypeOf(Client{})
	_, hasDBField := clientType.FieldByName("DB")
	require.False(t, hasDBField, "Client.DB must not be exported")
	_, hasOpenMethod := reflect.TypeOf((*Client)(nil)).MethodByName("Open")
	require.False(t, hasOpenMethod, "Client.Open must not be exported")

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	forbidden := map[string]bool{
		"NewClient": true, "NewUpgradeCoordinator": true, "UpgradeCoordinator": true,
		"UpgradeConfig": true, "UpgradeStatus": true, "UpgradeResult": true,
		"UpgradeInProgressError": true,
	}
	set := token.NewFileSet()
	for _, path := range files {
		if filepath.Ext(path) != ".go" || filepath.Base(path) == "opening_public_test.go" {
			continue
		}
		parsed, parseErr := parser.ParseFile(set, path, nil, 0)
		require.NoError(t, parseErr)
		for name := range parsed.Scope.Objects {
			require.Falsef(t, forbidden[name], "legacy export %s remains in %s", name, path)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok {
				return true
			}
			for _, name := range field.Names {
				require.NotEqual(t, "SynchronizedReadsAvailable", name.Name)
				require.NotEqual(t, "SynchronizedWritesAvailable", name.Name)
			}
			return true
		})
	}
}
