package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type cleanupErrorMigrator struct {
	installed func(context.Context, LocalMigrationReader) (int64, error)
	migrate   func(context.Context, LocalMigrationTx, LocalMigrationStep) error
}

func (m cleanupErrorMigrator) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	return m.installed(ctx, reader)
}

func (m cleanupErrorMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, step LocalMigrationStep) error {
	return m.migrate(ctx, tx, step)
}

func registerMigrationCleanupError(t *testing.T, capability any, cleanupErr error) {
	t.Helper()
	restricted, ok := capability.(*restrictedMigrationCapability)
	require.True(t, ok)
	require.NoError(t, restricted.trackResult(func() error { return cleanupErr }))
}

func cleanupReadyContractWithForeignKey(t *testing.T) LocalGenerationContract {
	t.Helper()
	registered := []oversync.RegisteredTable{
		{Schema: "main", Table: "parents", SyncKeyColumns: []string{"id"}},
		{Schema: "main", Table: "children", SyncKeyColumns: []string{"id"}},
	}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{
		{Schema: "main", Table: "parents", Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		}},
		{Schema: "main", Table: "children", Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
			{Name: "parent_id", WireType: oversync.GenerationWireTypeUUID},
		}, ForeignKeys: []oversync.GenerationForeignKey{{
			Columns: []string{"parent_id"}, ReferencedSchema: "main", ReferencedTable: "parents", ReferencedColumns: []string{"id"},
			OnDelete: oversync.GenerationForeignKeyNoAction, OnUpdate: oversync.GenerationForeignKeyNoAction,
		}}},
	}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	compiled, ok := registry.Generation(1)
	require.True(t, ok)
	return LocalGenerationContract{
		Proof: oversync.SchemaGenerationProof{ID: 1, Fingerprint: compiled.Fingerprint()}, RegisteredTables: registered, Generation: generation,
		Tables: []SyncTable{
			{TableName: "parents", SyncKeyColumns: []string{"id"}},
			{TableName: "children", SyncKeyColumns: []string{"id"}},
		},
		Columns: []LocalTableColumns{
			{Schema: "main", Table: "parents", Columns: []LocalColumnContract{{Name: "id", Codec: LocalCodecUUIDText}}},
			{Schema: "main", Table: "children", Columns: []LocalColumnContract{
				{Name: "id", Codec: LocalCodecUUIDText},
				{Name: "parent_id", Codec: LocalCodecUUIDText},
			}},
		},
	}
}

func TestUpgradeRegression_U2DrainWitnessResumesWithoutOldGenerationIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drained.sqlite")
	plan := prepareGenerationDrainedProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()

	oldCalls := 0
	targetCalls := 0
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://drained.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet || request.URL.Path != "/sync/generations/1" {
				return nil, fmt.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
			}
			if request.Header.Get(oversync.SchemaGenerationIDHeader) == fmt.Sprint(plan.testInstalled.Proof.ID) {
				oldCalls++
				return nil, fmt.Errorf("old-generation endpoint is no longer available")
			}
			targetCalls++
			return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
		})},
	})
	require.NoError(t, err)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.True(t, rec.DrainComplete)

	require.NoError(t, coordinator.resolveOldIntent(context.Background(), rec))
	require.Equal(t, UpgradePhaseMigratingLocal, rec.Phase)
	require.Zero(t, oldCalls)
	require.Zero(t, targetCalls)
	require.NoError(t, coordinator.Close())
}

func TestUpgradeRegression_MigrationCleanupErrorsPreservePrimaryCause(t *testing.T) {
	primaryErr := errors.New("primary migration failure")
	cleanupErr := errors.New("migration result cleanup failure")

	t.Run("installed-version", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		migrator := cleanupErrorMigrator{
			installed: func(_ context.Context, reader LocalMigrationReader) (int64, error) {
				registerMigrationCleanupError(t, reader, cleanupErr)
				return 0, primaryErr
			},
			migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error { return nil },
		}
		_, err := readInstalledVersionRestricted(context.Background(), db, migrator)
		require.ErrorIs(t, err, primaryErr)
		require.ErrorIs(t, err, cleanupErr)
	})

	t.Run("installed-version-panic", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		migrator := cleanupErrorMigrator{
			installed: func(_ context.Context, reader LocalMigrationReader) (int64, error) {
				registerMigrationCleanupError(t, reader, cleanupErr)
				panic(primaryErr)
			},
			migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error { return nil },
		}
		_, err := readInstalledVersionRestricted(context.Background(), db, migrator)
		require.ErrorIs(t, err, primaryErr)
		require.ErrorIs(t, err, cleanupErr)
	})

	t.Run("construction-releases-ownership-after-installed-version-panic", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "pending.sqlite")
		plan := prepareGenerationDrainedProcessTestDatabase(t, path)
		workingMigrator := plan.Migrator
		plan.Migrator = cleanupErrorMigrator{
			installed: func(context.Context, LocalMigrationReader) (int64, error) { panic(primaryErr) },
			migrate:   func(context.Context, LocalMigrationTx, LocalMigrationStep) error { return nil },
		}
		db, err := sql.Open("sqlite3", path)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()

		_, err = newTestOpening(db, &openingRuntimeConfig{Plan: plan})
		require.ErrorIs(t, err, primaryErr)

		plan.Migrator = workingMigrator
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
		require.NoError(t, err)
		require.NoError(t, coordinator.Close())
	})

	for _, test := range []struct {
		name    string
		primary error
		migrate func(LocalMigrationTx)
	}{
		{name: "callback-error", primary: primaryErr, migrate: func(LocalMigrationTx) {}},
		{name: "cancellation", primary: context.Canceled, migrate: func(LocalMigrationTx) {}},
		{name: "panic", primary: primaryErr, migrate: func(LocalMigrationTx) { panic(primaryErr) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			tx, err := beginImmediateTx(context.Background(), db)
			require.NoError(t, err)
			defer tx.Rollback()
			migrator := cleanupErrorMigrator{
				installed: func(context.Context, LocalMigrationReader) (int64, error) { return 1, nil },
				migrate: func(_ context.Context, capability LocalMigrationTx, _ LocalMigrationStep) error {
					registerMigrationCleanupError(t, capability, cleanupErr)
					test.migrate(capability)
					return test.primary
				},
			}
			_, err = invokeMigratorInTx(context.Background(), tx, migrator, LocalMigrationStep{})
			require.ErrorIs(t, err, test.primary)
			require.ErrorIs(t, err, cleanupErr)
		})
	}
}

func TestUpgradeRegression_NewOpeningDeepClonesReadyConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.sqlite")
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()

	contract := cleanupReadyContractWithForeignKey(t)
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "cleanup-ready-fresh", Mode: UpgradeModeFresh, DatabaseID: "process-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE parents (id TEXT NOT NULL PRIMARY KEY);
				CREATE TABLE children (
					id TEXT NOT NULL PRIMARY KEY,
					parent_id TEXT NOT NULL,
					FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE NO ACTION ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED
				);
				CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	result, err := fresh.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)

	local := cloneLocalGenerationContract(contract)
	config := admittedFixtureConfig("main", cloneSyncTables(contract.Tables), contract.Proof)
	config.DatabaseID = "process-db"
	config.ServiceID = "service"
	config.LocalVersion = 1
	config.LocalOnlyTables = []string{"local_notes"}
	config.localContract = &local
	config.RetryPolicy = &RetryPolicy{Enabled: true, MaxAttempts: 3, InitialBackoff: time.Second}
	config.SnapshotCapacityRetryPolicy = &SnapshotCapacityRetryPolicy{Enabled: true, MaxWait: 2 * time.Second}
	config.PushCapacityRetryPolicy = &PushCapacityRetryPolicy{Enabled: true, MaxWait: 3 * time.Second}

	client, err := newAdmittedFixtureClient(db, "http://ready.invalid", tokenProviderForTests, config)
	require.NoError(t, err)

	config.Tables[0].TableName = "changed"
	config.Tables[0].SyncKeyColumns[0] = "changed"
	config.LocalOnlyTables[0] = "changed"
	config.localContract.Proof.ID = 99
	config.localContract.RegisteredTables[0].SyncKeyColumns[0] = "changed"
	config.localContract.Generation.Tables[0].Columns[0].Name = "changed"
	config.localContract.Generation.Tables[1].ForeignKeys[0].Columns[0] = "changed"
	config.localContract.Generation.Tables[1].ForeignKeys[0].ReferencedTable = "changed"
	config.localContract.Generation.Tables[1].ForeignKeys[0].ReferencedColumns[0] = "changed"
	config.localContract.Tables[0].SyncKeyColumns[0] = "changed"
	config.localContract.Columns[0].Columns[0].Name = "changed"
	config.RetryPolicy.MaxAttempts = 99
	config.SnapshotCapacityRetryPolicy.MaxWait = 99 * time.Second
	config.PushCapacityRetryPolicy.MaxWait = 99 * time.Second

	require.Equal(t, "parents", client.config.Tables[0].TableName)
	require.Equal(t, []string{"id"}, client.config.Tables[0].SyncKeyColumns)
	require.Equal(t, []string{"local_notes"}, client.config.LocalOnlyTables)
	require.Equal(t, contract.Proof, client.config.localContract.Proof)
	require.Equal(t, []string{"id"}, client.config.localContract.RegisteredTables[0].SyncKeyColumns)
	require.Equal(t, "id", client.config.localContract.Generation.Tables[0].Columns[0].Name)
	require.Equal(t, []string{"parent_id"}, client.config.localContract.Generation.Tables[1].ForeignKeys[0].Columns)
	require.Equal(t, "parents", client.config.localContract.Generation.Tables[1].ForeignKeys[0].ReferencedTable)
	require.Equal(t, []string{"id"}, client.config.localContract.Generation.Tables[1].ForeignKeys[0].ReferencedColumns)
	require.Equal(t, []string{"id"}, client.config.localContract.Tables[0].SyncKeyColumns)
	require.Equal(t, "id", client.config.localContract.Columns[0].Columns[0].Name)
	require.Equal(t, 3, client.config.RetryPolicy.MaxAttempts)
	require.Equal(t, 2*time.Second, client.config.SnapshotCapacityRetryPolicy.MaxWait)
	require.Equal(t, 3*time.Second, client.config.PushCapacityRetryPolicy.MaxWait)
	require.NoError(t, client.Close())
}

// A generation migrator may drop an omitted old table. Its capture inventory
// must change atomically with DDL so strict retained admission can resume.
func TestUpgradeRegression_DroppedTableCaptureInventoryCommitsAndRollsBack(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint("commit=", commit), func(t *testing.T) {
			ctx := context.Background()
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE legacy_notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL); CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); CREATE TABLE table_local_notes(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL); INSERT INTO table_local_notes VALUES('sentinel','preserve')`)
			require.NoError(t, err)
			old := tableVectorLocal(tableEvolutionVector(t), 0)
			target := tableVectorLocal(tableEvolutionVector(t), 1)
			config := admittedFixtureConfig("main", old.Tables, old.Proof)
			config.localContract = &old
			config.LocalOnlyTables = []string{"table_local_notes"}
			client, err := newAdmittedFixtureClient(db, "http://inventory.invalid", tokenProviderForTests, config)
			require.NoError(t, err)
			require.NoError(t, client.Close())
			tx, err := beginImmediateTx(ctx, db)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, suspendInstalledCaptureTriggersInTx(ctx, tx, old))
			_, err = tx.ExecContext(ctx, `DROP TABLE legacy_notes; CREATE TABLE projects(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); ALTER TABLE people ADD COLUMN project_id TEXT REFERENCES projects(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED; CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,person_id TEXT REFERENCES people(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,body TEXT NOT NULL)`)
			require.NoError(t, err)
			require.NoError(t, refreshTargetTriggersInTx(ctx, tx, target))
			require.NoError(t, validateRetainedCaptureTriggers(ctx, tx))
			if commit {
				require.NoError(t, tx.Commit(ctx))
			} else {
				require.NoError(t, tx.Rollback())
			}
			require.NoError(t, validateCompleteRetainedRuntime(ctx, db))
			var inventory string
			require.NoError(t, db.QueryRow(`SELECT group_concat(schema_name||'.'||table_name,',') FROM (SELECT schema_name,table_name FROM _sync_managed_tables ORDER BY schema_name,table_name)`).Scan(&inventory))
			expected := "main.legacy_notes,main.people"
			if commit {
				expected = "main.attachments,main.people,main.projects"
			}
			require.Equal(t, expected, inventory)
			var sentinel string
			require.NoError(t, db.QueryRow(`SELECT note FROM table_local_notes WHERE id='sentinel'`).Scan(&sentinel))
			require.Equal(t, "preserve", sentinel)
		})
	}
}
