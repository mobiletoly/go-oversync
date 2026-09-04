package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func prepareRound7LocalOnlyDatabase(t *testing.T, path string) (UpgradePlan, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	contract := processTestContract(t)
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "safety-fresh", Mode: UpgradeModeFresh, DatabaseID: "safety-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
				CREATE TABLE local_notes(note TEXT NOT NULL);
				INSERT INTO local_notes(rowid,note) VALUES (1,'before');
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	plan := UpgradePlan{
		PlanID: "safety-local", Mode: UpgradeModeLocalOnly, DatabaseID: "safety-db", ServiceID: "service",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
		LocalOnlyTables: []string{"local_notes"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `UPDATE local_notes SET note='after'; PRAGMA user_version=2`)
			return err
		}},
	}
	return plan, db
}

func TestUpgradeRegression_PreU0GuardCollisionLeavesReadyStateUntouched(t *testing.T) {
	plan, db := prepareRound7LocalOnlyDatabase(t, filepath.Join(t.TempDir(), "guard.sqlite"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err := db.Exec(`CREATE TRIGGER _sync_upgrade_guard_local_notes_insert BEFORE INSERT ON local_notes BEGIN SELECT RAISE(ABORT,'application'); END`)
	require.NoError(t, err)
	beforeJournal, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	beforeIdentity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	before := owningRemediationState(t, db, "people", "local_notes")

	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.ErrorContains(t, err, "local-only upgrade guard")
	require.Nil(t, coordinator, "forged guard rejects construction before U0")
	afterJournal, loadErr := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, loadErr)
	afterIdentity, loadErr := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, loadErr)
	require.Equal(t, beforeJournal, afterJournal)
	require.Equal(t, beforeIdentity, afterIdentity)
	require.True(t, afterIdentity.Ready)
	require.Equal(t, before, owningRemediationState(t, db, "people", "local_notes"))
}

func TestUpgradeRegression_U0RechecksAdmissionAfterPreflightRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "u0-race.sqlite")
	prepareProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	plan := processTestGenerationPlan(t)
	beforeJournal, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	beforeIdentity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: plan, BaseURL: "http://u0-race.invalid", Token: tokenProviderForTests,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
				return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
			}
			return nil, fmt.Errorf("unexpected U0 race request %s %s", request.Method, request.URL.Path)
		})},
		testBoundary: func(boundary string) {
			if boundary == "before_U0_recheck" {
				_, updateErr := db.Exec(`UPDATE _sync_attachment_state SET last_bundle_seq_seen=last_bundle_seq_seen+1 WHERE singleton_key=1`)
				require.NoError(t, updateErr)
			}
		},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.ErrorContains(t, err, "attachment state changed before U0")
	require.NotEqual(t, UpgradeOutcomeReady, result.Status.Outcome)
	afterJournal, loadErr := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, loadErr)
	afterIdentity, loadErr := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, loadErr)
	require.Equal(t, beforeJournal, afterJournal)
	require.Equal(t, beforeIdentity, afterIdentity)
	var guards int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE '_sync_upgrade_guard_%'`).Scan(&guards))
	require.Zero(t, guards)
	require.NoError(t, coordinator.Close())
}

func TestUpgradeRegression_LocalOnlyGuardsResumeAndPreserveApplicationObjects(t *testing.T) {
	plan, db := prepareRound7LocalOnlyDatabase(t, filepath.Join(t.TempDir(), "guards.sqlite"))
	_, err := db.Exec(`CREATE TRIGGER app_local_notes_update AFTER UPDATE ON local_notes BEGIN SELECT NEW.note; END`)
	require.NoError(t, err)

	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	var guards int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE '_sync_upgrade_guard_%'`).Scan(&guards))
	require.Zero(t, guards)
	var appTrigger int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='app_local_notes_update'`).Scan(&appTrigger))
	require.Equal(t, 1, appTrigger)
	var note string
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes`).Scan(&note))
	require.Equal(t, "after", note)
	require.NoError(t, coordinator.Close())
}

func TestUpgradeRegression_AlteredOrMissingGuardRefusesCompletion(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter string
	}{
		{name: "missing", alter: `DROP TRIGGER _sync_upgrade_guard_local_notes_insert`},
		{name: "altered", alter: `DROP TRIGGER _sync_upgrade_guard_local_notes_insert; CREATE TRIGGER _sync_upgrade_guard_local_notes_insert BEFORE INSERT ON local_notes BEGIN SELECT RAISE(ABORT,'altered'); END`},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, db := prepareRound7LocalOnlyDatabase(t, filepath.Join(t.TempDir(), "guards.sqlite"))
			ctx, cancel := context.WithCancel(context.Background())
			coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan, Progress: func(status OpeningStatus) {
				if status.Phase == UpgradePhaseMigratingLocal {
					cancel()
				}
			}})
			require.NoError(t, err)
			result, err := coordinator.Advance(ctx)
			require.Error(t, err)
			require.Equal(t, UpgradeOutcomeCancelled, result.Status.Outcome)
			require.NoError(t, coordinator.Close())
			_, err = db.Exec(test.alter)
			require.NoError(t, err)
			before := owningRemediationState(t, db, "people", "local_notes")

			resumed, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
			if test.name == "missing" {
				require.NoError(t, err)
				result, err = resumed.Advance(context.Background())
				require.NotEqual(t, UpgradeOutcomeReady, result.Status.Outcome)
				require.NoError(t, resumed.Close())
			} else {
				require.Nil(t, resumed, "altered guard rejects construction before capability or configuration")
			}
			require.ErrorContains(t, err, "local-only upgrade guard")
			after := owningRemediationState(t, db, "people", "local_notes")
			if test.name == "missing" {
				// Advance records its integrity-checked failure outcome/revision;
				// every other business, intent, replay and retained owner is unchanged.
				journal, loadErr := loadUpgradeJournal(context.Background(), db)
				require.NoError(t, loadErr)
				require.Equal(t, UpgradeOutcomeLocalPreservationFailed, journal.Outcome)
				for _, vector := range []qualificationVector{before, after} {
					for _, row := range vector["_sync_upgrade_journal"] {
						for _, column := range []string{"outcome_code", "failure_code", "failure_detail", "revision", "integrity_digest"} {
							delete(row, column)
						}
					}
				}
			}
			require.Equal(t, before, after)
			var version int
			require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
			require.Equal(t, 1, version)
		})
	}
}

func TestUpgradeRegression_RowIDDetectionUsesTableListMetadata(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`
		CREATE TABLE ordinary(note TEXT DEFAULT 'WITHOUT ROWID');
		INSERT INTO ordinary(rowid,note) VALUES (1,'keep');
		CREATE TABLE actual_without_rowid(id TEXT PRIMARY KEY,note TEXT) WITHOUT ROWID;
		INSERT INTO actual_without_rowid VALUES ('id','keep');
	`)
	require.NoError(t, err)
	before, err := localOnlyDigest(context.Background(), db, []string{"ordinary"})
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE ordinary SET rowid=2`)
	require.NoError(t, err)
	after, err := localOnlyDigest(context.Background(), db, []string{"ordinary"})
	require.NoError(t, err)
	require.NotEqual(t, before, after)

	withoutRowID, err := sqliteTableWithoutRowID(context.Background(), db, "actual_without_rowid")
	require.NoError(t, err)
	require.True(t, withoutRowID)
	require.Empty(t, observableSQLiteRowID([]string{"id", "note"}, withoutRowID))
}

type pragmaAttackMigrator struct {
	afterMigration bool
	migrateCalled  bool
}

type emptyUserVersionAssignmentMigrator struct {
	attackOnCall  int
	calls         int
	migrateCalled bool
}

func (m *emptyUserVersionAssignmentMigrator) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	m.calls++
	var version int64
	if err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return 0, err
	}
	if m.calls != m.attackOnCall {
		return version, nil
	}
	rows, err := reader.QueryContext(ctx, `PRAGMA user_version=''`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return version, rows.Err()
}

func (m *emptyUserVersionAssignmentMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
	m.migrateCalled = true
	_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); PRAGMA user_version=1`)
	return err
}

func (m *pragmaAttackMigrator) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	if m.afterMigration {
		rows, err := reader.QueryContext(ctx, `PRAGMA incremental_vacuum`)
		if err != nil {
			return 0, err
		}
		_ = rows.Close()
	}
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m *pragmaAttackMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
	m.migrateCalled = true
	m.afterMigration = true
	_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); PRAGMA user_version=1`)
	return err
}

func TestUpgradeRegression_ReadOnlyVersionReaderRejectsNoArgumentMutatingPragmas(t *testing.T) {
	contract := processTestContract(t)
	t.Run("pre-migration", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		migrator := &pragmaAttackMigrator{afterMigration: true}
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
			PlanID: "pragma-pre", Mode: UpgradeModeFresh, DatabaseID: "pragma-db", ServiceID: "service",
			TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: migrator,
		}})
		require.NoError(t, err)
		result, err := coordinator.Advance(context.Background())
		require.Error(t, err)
		require.Equal(t, UpgradeOutcomeLocalMigrationFailed, result.Status.Outcome)
		require.False(t, migrator.migrateCalled)
		require.NoError(t, coordinator.Close())
	})
	t.Run("post-migration", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		migrator := &pragmaAttackMigrator{}
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
			PlanID: "pragma-post", Mode: UpgradeModeFresh, DatabaseID: "pragma-db", ServiceID: "service",
			TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: migrator,
		}})
		require.NoError(t, err)
		result, err := coordinator.Advance(context.Background())
		require.Error(t, err)
		require.Equal(t, UpgradeOutcomeLocalMigrationFailed, result.Status.Outcome)
		require.True(t, migrator.migrateCalled)
		var people int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='people'`).Scan(&people))
		require.Zero(t, people)
		require.NoError(t, coordinator.Close())
	})
}

func TestUpgradeRegression_ReadOnlyVersionReaderRejectsEmptyUserVersionAssignment(t *testing.T) {
	contract := processTestContract(t)
	plan := func(id string, migrator LocalMigrator) UpgradePlan {
		return UpgradePlan{
			PlanID: id, Mode: UpgradeModeFresh, DatabaseID: "empty-pragma-db", ServiceID: "service",
			TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: migrator,
		}
	}
	assertVersion := func(t *testing.T, db *sql.DB, want int64) {
		t.Helper()
		var got int64
		require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&got))
		require.Equal(t, want, got)
	}

	t.Run("pre-migration", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		migrator := &emptyUserVersionAssignmentMigrator{attackOnCall: 1}
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan("empty-pragma-pre", migrator)})
		require.NoError(t, err)
		result, runErr := coordinator.Advance(context.Background())
		require.Error(t, runErr)
		require.Equal(t, UpgradeOutcomeLocalMigrationFailed, result.Status.Outcome)
		require.False(t, migrator.migrateCalled)
		assertVersion(t, db, 0)
		require.NoError(t, coordinator.Close())
	})

	t.Run("post-migration", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		migrator := &emptyUserVersionAssignmentMigrator{attackOnCall: 2}
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan("empty-pragma-post", migrator)})
		require.NoError(t, err)
		result, runErr := coordinator.Advance(context.Background())
		require.Error(t, runErr)
		require.Equal(t, UpgradeOutcomeLocalMigrationFailed, result.Status.Outcome)
		require.True(t, migrator.migrateCalled)
		assertVersion(t, db, 0)
		var people int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='people'`).Scan(&people))
		require.Zero(t, people)
		require.NoError(t, coordinator.Close())
	})

	t.Run("completed-plan", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		initial := &emptyUserVersionAssignmentMigrator{}
		completedPlan := plan("empty-pragma-completed", initial)
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: completedPlan})
		require.NoError(t, err)
		result, runErr := coordinator.Advance(context.Background())
		require.NoError(t, runErr)
		require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
		require.NoError(t, coordinator.Close())

		attack := &emptyUserVersionAssignmentMigrator{attackOnCall: 1}
		completedPlan.Migrator = attack
		reopened, err := newTestOpening(db, &openingRuntimeConfig{Plan: completedPlan})
		require.NoError(t, err)
		result, err = reopened.Advance(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result.Ready)
		require.Zero(t, attack.calls, "an already-ready database must not invoke the historical migrator")
		require.NoError(t, result.Ready.Close())
		assertVersion(t, db, 1)
		identity, err := loadUpgradeIdentity(context.Background(), db)
		require.NoError(t, err)
		require.NotNil(t, identity)
		require.True(t, identity.Ready)
		require.Equal(t, int64(1), identity.LocalVersion)
	})
}

func TestUpgradeRegression_GenerationErrorsAreTypedAtEveryReader(t *testing.T) {
	contract := processTestContract(t)
	for _, generationCase := range []struct {
		name   string
		status int
		code   oversync.SchemaGenerationRequestErrorCode
	}{
		{name: "unknown", status: http.StatusConflict, code: oversync.SchemaGenerationUnknown},
		{name: "mismatch", status: http.StatusConflict, code: oversync.SchemaGenerationFingerprintMismatch},
		{name: "unsupported", status: http.StatusGone, code: oversync.SchemaGenerationUnsupported},
	} {
		for _, endpoint := range []struct {
			name string
			call func(*Client) error
		}{
			{name: "pull", call: func(c *Client) error { _, err := c.sendPullRequest(context.Background(), 0, 1, 0); return err }},
			{name: "snapshot-create", call: func(c *Client) error {
				_, err := c.createSnapshotSession(context.Background(), &oversync.SnapshotSessionCreateRequest{})
				return err
			}},
			{name: "snapshot-chunk", call: func(c *Client) error {
				_, _, err := c.fetchSnapshotChunkAsSource(context.Background(), "snapshot", 1, 0, 1, 1024, "source-old")
				return err
			}},
			{name: "snapshot-delete", call: func(c *Client) error {
				return c.deleteSnapshotSessionAsSource(context.Background(), "snapshot", "source-old")
			}},
		} {
			t.Run(generationCase.name+"/"+endpoint.name, func(t *testing.T) {
				discovery := &oversync.GenerationDiscoveryResponse{
					ProtocolVersion: oversync.SyncProtocolVersion,
					Limits:          testWireLimits(),
				}
				client := &Client{
					BaseURL: "http://generation.invalid", Token: tokenProviderForTests,
					config: admittedFixtureConfig("main", contract.Tables, contract.Proof), schemaGenerationProof: contract.Proof,
					discovery: discovery,
					sourceID:  "source-old", HTTP: &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
						return errorJSONResponse(generationCase.status, map[string]any{"error": generationCase.code}), nil
					})},
				}
				err := endpoint.call(client)
				var generationErr *oversync.SchemaGenerationRequestError
				require.ErrorAs(t, err, &generationErr)
				require.Equal(t, generationCase.code, generationErr.Code)
				require.Equal(t, UpgradeOutcomeOldGenerationUnavailable, classifyUpgradeCause(err, false))
			})
		}
	}
}

func TestUpgradeRegression_OldGenerationLossRetainsFrozenIntentAndActiveSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-generation.sqlite")
	plan := preparePushProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	base := newMockPushServer(t)
	httpClient := &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
		if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1" {
			if generation == fmt.Sprint(plan.testInstalled.Proof.ID) {
				return jsonResponse(upgradeCapabilities(t, plan.testInstalled)), nil
			}
			return jsonResponse(upgradeCapabilities(t, plan.testTarget)), nil
		}
		if request.Method == http.MethodPost && request.URL.Path == "/sync/push" {
			return errorJSONResponse(http.StatusGone, map[string]any{"error": oversync.SchemaGenerationUnsupported}), nil
		}
		return base.RoundTrip(request)
	})}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan, BaseURL: "http://old.invalid", Token: tokenProviderForTests, HTTP: httpClient})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeOldGenerationUnavailable, result.Status.Outcome)
	require.Equal(t, UpgradePhaseResolvingOldIntent, result.Status.Phase)
	outbox, loadErr := loadOutboxBundle(context.Background(), db)
	require.NoError(t, loadErr)
	require.Equal(t, outboxStatePrepared, outbox.State)
	require.NotEmpty(t, outbox.CanonicalRequestHash)
	var dirty, staged int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_outbox_rows`).Scan(&staged))
	require.Zero(t, dirty)
	require.Equal(t, 1, staged)
	require.NoError(t, coordinator.Close())
	require.NoError(t, db.Close())
}

func TestUpgradeRegression_FreshRejectsLiteralSqlitePrefixLookalikeBeforeBootstrap(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE sqlitexnotes(note TEXT)`)
	require.NoError(t, err)
	contract := processTestContract(t)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "fresh-lookalike", Mode: UpgradeModeFresh, DatabaseID: "fresh-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: testLocalMigrator{migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error { return nil }},
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.ErrorContains(t, err, "sqlitexnotes")
	require.Zero(t, result)
	var upgradeErr *UpgradeError
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, UpgradeOutcomePreconditionFailed, upgradeErr.Outcome)
	for _, name := range []string{"_sync_upgrade_journal", "_sync_upgrade_identity", "_sync_dirty_rows"} {
		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count))
		require.Zero(t, count)
	}
	require.NoError(t, coordinator.Close())
}

func TestUpgradeRegression_GenerationErrorStatusMappingMustBeExact(t *testing.T) {
	proof := processTestContract(t).Proof
	body, err := json.Marshal(map[string]any{"error": oversync.SchemaGenerationUnsupported})
	require.NoError(t, err)
	require.Nil(t, decodeSchemaGenerationRequestError(http.StatusConflict, body, proof))
	body, err = json.Marshal(map[string]any{"error": oversync.SchemaGenerationUnknown})
	require.NoError(t, err)
	require.Nil(t, decodeSchemaGenerationRequestError(http.StatusGone, body, proof))
}

func TestUpgradeRegression_ReadyClientRejectsTablesOutsideLocalContractBeforeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready-client-table-scope.sqlite")
	prepareProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()

	contract := processTestContract(t)
	config := admittedFixtureConfig("main", append(append([]SyncTable(nil), contract.Tables...), SyncTable{
		TableName: "local_notes", SyncKeyColumnName: "id",
	}), contract.Proof)
	config.localContract = &contract
	config.DatabaseID = "process-db"
	config.ServiceID = "service"
	config.LocalVersion = 1
	config.LocalOnlyTables = []string{"local_notes"}

	var beforeSchemaVersion, beforeManaged, beforeTriggers int64
	require.NoError(t, db.QueryRow(`PRAGMA schema_version`).Scan(&beforeSchemaVersion))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_managed_tables WHERE table_name='local_notes'`).Scan(&beforeManaged))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_local_notes_%'`).Scan(&beforeTriggers))
	var beforeNote string
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&beforeNote))

	client, err := newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
	require.Nil(t, client)
	var upgradeErr *UpgradeError
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, UpgradeOutcomePlanMismatch, upgradeErr.Outcome)

	var afterSchemaVersion, afterManaged, afterTriggers int64
	require.NoError(t, db.QueryRow(`PRAGMA schema_version`).Scan(&afterSchemaVersion))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_managed_tables WHERE table_name='local_notes'`).Scan(&afterManaged))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_local_notes_%'`).Scan(&afterTriggers))
	var afterNote string
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&afterNote))
	require.Equal(t, beforeSchemaVersion, afterSchemaVersion)
	require.Equal(t, beforeManaged, afterManaged)
	require.Equal(t, beforeTriggers, afterTriggers)
	require.Equal(t, beforeNote, afterNote)

	valid := admittedFixtureConfig("main", contract.Tables, contract.Proof)
	valid.localContract = &contract
	valid.DatabaseID = "process-db"
	valid.ServiceID = "service"
	valid.LocalVersion = 1
	valid.LocalOnlyTables = []string{"local_notes"}
	client, err = newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, valid)
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestUpgradeRegression_ClientTableScopeMustExactlyMatchLocalContract(t *testing.T) {
	contract := processTestContract(t)
	compiled, err := compileLocalContract(contract)
	require.NoError(t, err)
	valid := admittedFixtureConfig("main", contract.Tables, contract.Proof)
	require.NoError(t, validateClientTablesMatchLocalContract(valid, compiled))

	for _, test := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing-table", mutate: func(config *Config) { config.Tables = nil }},
		{name: "extra-table", mutate: func(config *Config) {
			config.Tables = append(config.Tables, SyncTable{TableName: "local_notes", SyncKeyColumnName: "id"})
		}},
		{name: "local-only-overlap", mutate: func(config *Config) { config.LocalOnlyTables = []string{"people"} }},
		{name: "ordered-key", mutate: func(config *Config) {
			config.Tables = []SyncTable{{TableName: "people", SyncKeyColumnName: "name"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := *valid
			config.Tables = append([]SyncTable(nil), valid.Tables...)
			test.mutate(&config)
			require.Error(t, validateClientTablesMatchLocalContract(&config, compiled))
		})
	}
}

func TestUpgradeRegression_UpgradePlansRejectLocalOnlySynchronizedOverlap(t *testing.T) {
	contract := processTestContract(t)
	migrator := testLocalMigrator{migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error { return nil }}
	for _, plan := range []UpgradePlan{
		{
			PlanID: "fresh-overlap", Mode: UpgradeModeFresh, DatabaseID: "device", ServiceID: "service",
			TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"people"}, Migrator: migrator,
		},
		{
			PlanID: "local-overlap", Mode: UpgradeModeLocalOnly, DatabaseID: "device", ServiceID: "service",
			InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
			LocalOnlyTables: []string{"people"}, Migrator: migrator,
		},
	} {
		db := openUpgradeTestDB(t)
		coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
		require.Nil(t, coordinator)
		require.ErrorContains(t, err, "also a synchronized table")
		require.NoError(t, db.Close())
	}

	generation := processTestGenerationPlan(t)
	generation.LocalOnlyTables = []string{"people"}
	db := openUpgradeTestDB(t)
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: generation})
	require.Nil(t, coordinator)
	require.ErrorContains(t, err, "also a synchronized table")
	require.NoError(t, db.Close())

	db = openUpgradeTestDB(t)
	overlap := UpgradePlan{
		PlanID: "fresh-overlap-construction", Mode: UpgradeModeFresh, DatabaseID: "device", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"people"}, Migrator: migrator,
	}
	coordinator, err = newTestOpening(db, &openingRuntimeConfig{Plan: overlap})
	require.Nil(t, coordinator)
	var upgradeErr *UpgradeError
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, UpgradeOutcomePlanMismatch, upgradeErr.Outcome)

	overlap.LocalOnlyTables = nil
	coordinator, err = newTestOpening(db, &openingRuntimeConfig{Plan: overlap})
	require.NoError(t, err)
	require.NoError(t, coordinator.Close())
}

func TestUpgradeRegression_JournalCorruptionFailsBeforeRemoteDispatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		update string
	}{
		{name: "old-source", update: `UPDATE _sync_upgrade_journal SET old_source_id='source-substituted' WHERE singleton_key=1`},
		{name: "new-source", update: `UPDATE _sync_upgrade_journal SET new_source_id='source-substituted' WHERE singleton_key=1`},
		{name: "reason", update: `UPDATE _sync_upgrade_journal SET replacement_reason='other-reason' WHERE singleton_key=1`},
		{name: "target-generation", update: `UPDATE _sync_upgrade_journal SET target_generation_id=target_generation_id+1 WHERE singleton_key=1`},
		{name: "target-fingerprint", update: `UPDATE _sync_upgrade_journal SET target_generation_fingerprint='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' WHERE singleton_key=1`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt-journal.sqlite")
			plan := prepareU6ProcessTestDatabase(t, path)
			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			defer db.Close()
			_, err = db.Exec(test.update)
			require.NoError(t, err)

			remoteCalls := 0
			coordinator, err := newTestOpening(db, &openingRuntimeConfig{
				Plan: plan, BaseURL: "http://must-not-dispatch.invalid", Token: tokenProviderForTests,
				HTTP: &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
					remoteCalls++
					return nil, fmt.Errorf("unexpected remote dispatch")
				})},
			})
			require.Nil(t, coordinator)
			var upgradeErr *UpgradeError
			require.ErrorAs(t, err, &upgradeErr)
			require.Equal(t, UpgradeOutcomeIntegrityFailed, upgradeErr.Outcome)
			require.Zero(t, remoteCalls)
		})
	}
}

func TestUpgradeRegression_ResignedJournalCannotContradictImmutablePlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contradictory-journal.sqlite")
	plan := prepareU6ProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	rec.TargetVersion++
	require.NoError(t, persistUpgradeJournal(context.Background(), db, rec))

	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.Nil(t, coordinator)
	var upgradeErr *UpgradeError
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, UpgradeOutcomeIntegrityFailed, upgradeErr.Outcome)
}

func TestOpening_ReadyAdmissionRejectsCrossRecordIdentityMismatch(t *testing.T) {
	fingerprint := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	for _, scenario := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB, *upgradeJournalRecord)
	}{
		{name: "journal database", mutate: func(_ *testing.T, _ *sql.DB, rec *upgradeJournalRecord) {
			rec.DatabaseID = "copied-database"
		}},
		{name: "journal service", mutate: func(_ *testing.T, _ *sql.DB, rec *upgradeJournalRecord) {
			rec.ServiceID = "copied-service"
		}},
		{name: "journal fingerprint", mutate: func(_ *testing.T, _ *sql.DB, rec *upgradeJournalRecord) {
			rec.TargetGenerationFingerprint = fingerprint
			rec.ValidatedTargetFingerprint = fingerprint
		}},
		{name: "installed versus retained fingerprint", mutate: func(t *testing.T, db *sql.DB, _ *upgradeJournalRecord) {
			_, err := db.Exec(`UPDATE _sync_upgrade_identity SET generation_fingerprint=? WHERE singleton_key=1`, fingerprint)
			require.NoError(t, err)
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ready-cross-record.sqlite")
			config, target := publicOpeningConfig(path)
			opening, err := NewOpening(config)
			require.NoError(t, err)
			result, err := opening.Advance(context.Background())
			require.NoError(t, err)
			require.NoError(t, result.Ready.Close())

			db, err := sql.Open("sqlite3", path)
			require.NoError(t, err)
			rec, err := loadUpgradeJournal(context.Background(), db)
			require.NoError(t, err)
			scenario.mutate(t, db, rec)
			require.NoError(t, persistUpgradeJournal(context.Background(), db, rec))
			require.NoError(t, db.Close())

			reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target})
			require.Nil(t, reopened)
			var upgradeErr *UpgradeError
			require.ErrorAs(t, err, &upgradeErr)
			require.Equal(t, UpgradeOutcomeIntegrityFailed, upgradeErr.Outcome)
		})
	}
}

func TestOpening_StatusCountFailureReturnsZeroStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status-count-failure.sqlite")
	config, target := publicOpeningConfig(path)
	opening, err := NewOpening(config)
	require.NoError(t, err)
	result, err := opening.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, result.Ready.Close())

	ctx, cancel := context.WithCancel(context.Background())
	reopened, err := NewOpening(&OpeningConfig{
		DatabasePath: path, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target,
		testBoundary: func(name string) {
			if name == "before_status_counts" {
				cancel()
			}
		},
	})
	require.NoError(t, err)
	status, err := reopened.Status(ctx)
	require.Equal(t, OpeningStatus{}, status)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, reopened.Close())
}

func TestOpening_PlanFreeReadyReopenRejectsLocalOnlyMismatch(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		localOnly []string
		wantError bool
	}{
		{name: "exact", localOnly: []string{"local_notes"}},
		{name: "omitted", localOnly: nil, wantError: true},
		{name: "different", localOnly: []string{"other"}, wantError: true},
		{name: "nonexistent", localOnly: []string{"local_notes", "other"}, wantError: true},
		{name: "synchronized overlap", localOnly: []string{"items", "local_notes"}, wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ready-local-only.sqlite")
			target := DefaultConfig(1, []SyncTable{{TableName: "items", SyncKeyColumns: []string{"id"}}})
			target.DatabaseID, target.ServiceID, target.LocalVersion = "ready-local-db", "ready-local-service", 1
			target.LocalOnlyTables = []string{"local_notes"}
			plan := UpgradePlan{
				PlanID: "ready-local-fresh", Mode: UpgradeModeFresh, DatabaseID: target.DatabaseID, ServiceID: target.ServiceID,
				TargetGenerationID: 1, TargetVersion: 1, LocalOnlyTables: []string{"local_notes"},
				Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
					_, err := tx.ExecContext(ctx, `CREATE TABLE items(id TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL); CREATE TABLE local_notes(id TEXT PRIMARY KEY NOT NULL,note TEXT NOT NULL); INSERT INTO local_notes VALUES('note','keep'); PRAGMA user_version=1`)
					return err
				}},
			}
			opening, err := NewOpening(&OpeningConfig{DatabasePath: path, Plan: &plan, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: target})
			require.NoError(t, err)
			result, err := opening.Advance(context.Background())
			require.NoError(t, err)
			require.NoError(t, result.Ready.Close())

			requested := cloneCoordinatorClientConfig(target)
			requested.LocalOnlyTables = append([]string(nil), scenario.localOnly...)
			reopened, err := NewOpening(&OpeningConfig{DatabasePath: path, BaseURL: "http://opening.test", Token: tokenProviderForTests, TargetClientConfig: requested})
			if scenario.wantError {
				require.Nil(t, reopened)
				var upgradeErr *UpgradeError
				require.ErrorAs(t, err, &upgradeErr)
				require.Equal(t, UpgradeOutcomeLocalContractMismatch, upgradeErr.Outcome)
				return
			}
			require.NoError(t, err)
			result, err = reopened.Advance(context.Background())
			require.NoError(t, err)
			require.NotNil(t, result.Ready)
			require.NoError(t, result.Ready.Close())
		})
	}
}
