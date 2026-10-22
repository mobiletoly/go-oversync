package oversqlite

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestUpgradeRegression_AuthoritativePullRejectsApplicationTriggerFKViolationBeforeAcknowledgement(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	_, err := db.Exec(`
		CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE local_parents(id TEXT NOT NULL PRIMARY KEY);
		CREATE TABLE local_children(
			id TEXT NOT NULL PRIMARY KEY,
			parent_id TEXT NOT NULL REFERENCES local_parents(id) DEFERRABLE INITIALLY DEFERRED
		);
		INSERT INTO local_parents VALUES ('parent');
		INSERT INTO local_children VALUES ('child', 'parent');
	`)
	require.NoError(t, err)
	config := admittedFixtureConfig("main", contract.Tables, contract.Proof)
	config.localContract = &contract
	config.LocalOnlyTables = []string{"local_parents", "local_children"}
	client, err := newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	_, err = db.Exec(`
		UPDATE _sync_apply_state SET apply_mode=1;
		INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'before');
		UPDATE _sync_apply_state SET apply_mode=0;
		CREATE TRIGGER app_break_local_fk AFTER UPDATE ON people BEGIN
			DELETE FROM local_parents WHERE id='parent';
		END;
		INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES ('source-old',1);
		UPDATE _sync_attachment_state
		SET current_source_id='source-old',binding_state='attached',attached_user_id='user-1',schema_name='main'
		WHERE singleton_key=1;
		PRAGMA foreign_keys=OFF;
	`)
	require.NoError(t, err)
	client.UserID = "user-1"
	client.sourceID = "source-old"

	err = client.applyPulledBundleLocked(context.Background(), &oversync.Bundle{
		BundleSeq: 1,
		Rows: []oversync.BundleRow{{
			Schema: "main", Table: "people",
			Key: map[string]any{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
			Op:  oversync.OpUpdate, RowVersion: 1,
			Payload: []byte(`{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","name":"after"}`),
		}},
	})
	require.ErrorContains(t, err, "foreign key")
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM people`).Scan(&name))
	require.Equal(t, "before", name)
	var parents, checkpoint int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM local_parents`).Scan(&parents))
	require.Equal(t, int64(1), parents)
	require.NoError(t, db.QueryRow(`SELECT last_bundle_seq_seen FROM _sync_attachment_state`).Scan(&checkpoint))
	require.Zero(t, checkpoint)
}

func TestUpgradeRegression_CoordinatorPhysicalClientLogsCheckpointRecovery(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)
	seedConfig := admittedFixtureConfig("main", contract.Tables, contract.Proof)
	seedConfig.localContract = &contract
	seed, err := newAdmittedFixtureClient(db, "http://recovery.invalid", tokenProviderForTests, seedConfig)
	require.NoError(t, err)
	require.NoError(t, seed.Close())
	_, err = db.Exec(`
		INSERT INTO _sync_source_state(source_id,next_source_bundle_id) VALUES ('source-old',1);
		UPDATE _sync_attachment_state
		SET current_source_id='source-old',binding_state='attached',attached_user_id='user-1',schema_name='main'
		WHERE singleton_key=1
	`)
	require.NoError(t, err)
	httpClient := &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
			return errorJSONResponse(http.StatusConflict, map[string]any{"error": "history_pruned"}), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
			return snapshotFixtureResponseForGeneration(t, upgradeCapabilities(t, contract).SchemaGeneration, request, "checkpoint-recovery", 4, nil), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/checkpoint-recovery":
			return jsonResponse(oversync.SnapshotChunkResponse{
				SnapshotID: "checkpoint-recovery", SnapshotBundleSeq: 4,
				Rows: []oversync.SnapshotRow{}, NextRowOrdinal: 0, HasMore: false, ByteCount: 0,
			}), nil
		default:
			return nil, fmt.Errorf("unexpected recovery request %s %s", request.Method, request.URL.Path)
		}
	})}
	coordinator := &Opening{db: db, config: openingRuntimeConfig{
		BaseURL: "http://recovery.invalid", Token: tokenProviderForTests, HTTP: httpClient,
	}}
	client, err := coordinator.newPhysicalClient(contract, nil)
	require.NoError(t, err)
	discovery := upgradeCapabilities(t, contract)
	client.discovery = &discovery
	require.NotNil(t, client.logger)
	client.UserID = "user-1"
	client.sourceID = "source-old"
	client.sessionConnected = true
	report, err := client.pullToStableLocked(context.Background())
	require.NoError(t, err)
	require.Equal(t, RemoteSyncOutcomeAppliedSnapshot, report.Outcome)
}

func TestUpgradeRegression_ManagedReadyClientRequiresCompleteContractAndIdentity(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "managed-ready", Mode: UpgradeModeFresh, DatabaseID: "managed-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)

	_, err = newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, admittedFixtureConfig("main", contract.Tables, contract.Proof))
	require.ErrorContains(t, err, "config.localContract is required")

	config := admittedFixtureConfig("main", contract.Tables, contract.Proof)
	config.localContract = &contract
	config.DatabaseID = "wrong-db"
	config.ServiceID = "service"
	config.LocalVersion = 1
	_, err = newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
	var upgradeErr *UpgradeError
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, UpgradeOutcomePlanMismatch, upgradeErr.Outcome)

	config.DatabaseID = "managed-db"
	client, err := newAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	require.NotNil(t, client.localGeneration)
	require.NoError(t, client.Close())
}

func TestUpgradeRegression_ApplicationTriggerCollisionRollsBackFreshMigration(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "trigger-collision", Mode: UpgradeModeFresh, DatabaseID: "trigger-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"app_audit"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
				CREATE TABLE app_audit(message TEXT NOT NULL);
				CREATE TRIGGER trg_people_ai AFTER INSERT ON people BEGIN
					INSERT INTO app_audit VALUES ('inserted');
				END;
				PRAGMA user_version=1;
			`)
			return err
		}},
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeLocalContractMismatch, result.Status.Outcome)
	var people int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='people'`).Scan(&people))
	require.Zero(t, people)
}

func TestUpgradeRegression_LocalOnlyDigestIncludesImplicitRowIdentity(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE local_notes(note TEXT); INSERT INTO local_notes(rowid,note) VALUES (1,'keep')`)
	require.NoError(t, err)
	before, err := localOnlyDigest(context.Background(), db, []string{"local_notes"})
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE local_notes SET rowid=rowid+1`)
	require.NoError(t, err)
	after, err := localOnlyDigest(context.Background(), db, []string{"local_notes"})
	require.NoError(t, err)
	require.NotEqual(t, before, after)
}

func TestUpgradeRegression_PhysicalContractRejectsGeneratedAndAsyncTables(t *testing.T) {
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	t.Run("generated column", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,extra TEXT GENERATED ALWAYS AS (name) STORED)`)
		require.NoError(t, err)
		err = validatePhysicalLocalContract(context.Background(), db, contract, nil)
		require.ErrorContains(t, err, "non-generation column surface")
	})
	t.Run("async table", func(t *testing.T) {
		db := openUpgradeTestDB(t)
		_, err := db.Exec(`CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL); CREATE TABLE async_notes(note TEXT)`)
		require.NoError(t, err)
		err = validatePhysicalLocalContract(context.Background(), db, contract, nil)
		require.ErrorContains(t, err, `undeclared application table "async_notes"`)
	})
}

func TestUpgradeRegression_DelayedGenerationLossRemainsTyped(t *testing.T) {
	contract := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	client := &Client{
		BaseURL: "http://generation.invalid", Token: tokenProviderForTests,
		config: admittedFixtureConfig("main", contract.Tables, contract.Proof), schemaGenerationProof: contract.Proof,
		sourceID: "source-new",
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return errorJSONResponse(http.StatusGone, map[string]any{"error": "unsupported_schema_generation"}), nil
		})},
	}
	_, err := client.createSnapshotSession(context.Background(), &oversync.SnapshotSessionCreateRequest{})
	var generationErr *oversync.SchemaGenerationRequestError
	require.ErrorAs(t, err, &generationErr)
	require.Equal(t, UpgradeOutcomeTargetGenerationUnavailable, classifyUpgradeCause(err, true))
	_, _, err = client.fetchSnapshotChunkAsSource(context.Background(), "snapshot", 1, 0, 10, 1024, "source-new")
	require.ErrorAs(t, err, &generationErr)
	require.Equal(t, UpgradeOutcomeTargetGenerationUnavailable, classifyUpgradeCause(err, true))
}

func TestUpgradeRegression_TargetSelectionMustCoverDiscoveredInventory(t *testing.T) {
	columns := []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}
	registered := []oversync.RegisteredTable{
		{Schema: "main", Table: "people", SyncKeyColumns: []string{"id"}},
		{Schema: "main", Table: "projects", SyncKeyColumns: []string{"id"}},
	}
	generation := oversync.SchemaGeneration{ID: 2, Tables: []oversync.GenerationTable{
		{Schema: "main", Table: "people", Columns: columns},
		{Schema: "main", Table: "projects", Columns: columns},
	}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	contract, ok := registry.Generation(2)
	require.True(t, ok)

	scenarios := []struct {
		name      string
		tables    []SyncTable
		wantError string
	}{
		{
			name:      "partial selection cannot freeze",
			tables:    []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
			wantError: "cover every discovered generation table",
		},
		{
			name: "corrected retry remains possible",
			tables: []SyncTable{
				{TableName: "people", SyncKeyColumns: []string{"id"}},
				{TableName: "projects", SyncKeyColumns: []string{"id"}},
			},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			err := validateConfiguredGenerationSelection(scenario.tables, contract)
			if scenario.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, scenario.wantError)
		})
	}
}
