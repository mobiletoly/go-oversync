package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type testLocalMigrator struct {
	installed func(context.Context, LocalMigrationReader) (int64, error)
	migrate   func(context.Context, LocalMigrationTx, LocalMigrationStep) error
}

type postMigrationWritingVersionReader struct {
	migrated bool
}

func (m *postMigrationWritingVersionReader) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	if m.migrated {
		var version int64
		err := reader.QueryRowContext(ctx, `UPDATE app_version SET version=2 RETURNING version`).Scan(&version)
		return version, err
	}
	var version int64
	err := reader.QueryRowContext(ctx, `SELECT version FROM app_version`).Scan(&version)
	return version, err
}

func (m *postMigrationWritingVersionReader) Migrate(context.Context, LocalMigrationTx, LocalMigrationStep) error {
	m.migrated = true
	return nil
}

func (m testLocalMigrator) InstalledVersion(ctx context.Context, reader LocalMigrationReader) (int64, error) {
	if m.installed != nil {
		return m.installed(ctx, reader)
	}
	var version int64
	if err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (m testLocalMigrator) Migrate(ctx context.Context, tx LocalMigrationTx, step LocalMigrationStep) error {
	return m.migrate(ctx, tx, step)
}

func testUpgradeContract(t *testing.T, generationID oversync.SchemaGenerationID, columns []oversync.GenerationColumn) LocalGenerationContract {
	t.Helper()
	registered := []oversync.RegisteredTable{{Schema: "main", Table: "people", SyncKeyColumns: []string{"id"}}}
	generation := oversync.SchemaGeneration{ID: generationID, Tables: []oversync.GenerationTable{{Schema: "main", Table: "people", Columns: columns}}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	compiled, ok := registry.Generation(generationID)
	require.True(t, ok)
	codecs := make([]LocalColumnContract, 0, len(columns))
	for _, column := range columns {
		codec := LocalCodecText
		if column.WireType == oversync.GenerationWireTypeUUID {
			codec = LocalCodecUUIDText
		}
		codecs = append(codecs, LocalColumnContract{Name: column.Name, Codec: codec})
	}
	return LocalGenerationContract{
		Proof:            oversync.SchemaGenerationProof{ID: generationID, Fingerprint: compiled.Fingerprint()},
		RegisteredTables: registered, Generation: generation,
		Tables:  []SyncTable{{TableName: "people", SyncKeyColumns: []string{"id"}}},
		Columns: []LocalTableColumns{{Schema: "main", Table: "people", Columns: codecs}},
	}
}

func newTestOpening(db *sql.DB, config *openingRuntimeConfig) (*Opening, error) {
	if config == nil {
		return newOpeningFromDB(db, nil)
	}
	copyConfig := *config
	copyConfig.Plan = cloneUpgradePlan(config.Plan)
	if len(copyConfig.Plan.testInstalled.Tables) != 0 {
		registerMockGeneration(upgradeCapabilitiesValue(copyConfig.Plan.testInstalled).SchemaGeneration)
	}
	if len(copyConfig.Plan.testTarget.Tables) != 0 {
		registerMockGeneration(upgradeCapabilitiesValue(copyConfig.Plan.testTarget).SchemaGeneration)
	}
	if copyConfig.Token == nil {
		copyConfig.Token = tokenProviderForTests
	}
	if copyConfig.BaseURL == "" {
		copyConfig.BaseURL = "http://upgrade-test.invalid"
	}
	if copyConfig.OldClientConfig == nil && len(copyConfig.Plan.testInstalled.Tables) != 0 {
		copyConfig.OldClientConfig = DefaultConfig(copyConfig.Plan.InstalledGenerationID, cloneSyncTables(copyConfig.Plan.testInstalled.Tables))
		copyConfig.OldClientConfig.DatabaseID = copyConfig.Plan.DatabaseID
		copyConfig.OldClientConfig.ServiceID = copyConfig.Plan.ServiceID
		copyConfig.OldClientConfig.LocalVersion = copyConfig.Plan.InstalledVersion
		copyConfig.OldClientConfig.LocalOnlyTables = append([]string(nil), copyConfig.Plan.LocalOnlyTables...)
	}
	if copyConfig.TargetClientConfig == nil && len(copyConfig.Plan.testTarget.Tables) != 0 {
		copyConfig.TargetClientConfig = DefaultConfig(copyConfig.Plan.TargetGenerationID, cloneSyncTables(copyConfig.Plan.testTarget.Tables))
		copyConfig.TargetClientConfig.DatabaseID = copyConfig.Plan.DatabaseID
		copyConfig.TargetClientConfig.ServiceID = copyConfig.Plan.ServiceID
		copyConfig.TargetClientConfig.LocalVersion = copyConfig.Plan.TargetVersion
		copyConfig.TargetClientConfig.LocalOnlyTables = append([]string(nil), copyConfig.Plan.LocalOnlyTables...)
	}
	if copyConfig.HTTP != nil && copyConfig.Plan.Mode == UpgradeModeGeneration && len(copyConfig.Plan.testTarget.Generation.Tables) != 0 {
		httpCopy := *copyConfig.HTTP
		delegate := httpCopy.Transport
		if delegate == nil {
			delegate = http.DefaultTransport
		}
		target := cloneLocalGenerationContract(copyConfig.Plan.testTarget)
		httpCopy.Transport = rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/sync/generations/"+fmt.Sprint(target.Proof.ID) {
				body, err := json.Marshal(oversync.GenerationDiscoveryResponse{
					ProtocolVersion:  requiredProtocolVersion,
					SchemaGeneration: upgradeCapabilitiesValue(target).SchemaGeneration,
					Limits:           testWireLimits(),
				})
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			}
			return delegate.RoundTrip(request)
		})
		copyConfig.HTTP = &httpCopy
	}
	return newOpeningFromDB(db, &copyConfig)
}

func admitTestCoordinatorTarget(t *testing.T, coordinator *Opening) {
	t.Helper()
	require.NotNil(t, coordinator)
	target := coordinator.config.Plan.testTarget
	require.NotEmpty(t, target.Tables)
	config := DefaultConfig(coordinator.config.Plan.TargetGenerationID, target.Tables)
	config.DatabaseID = coordinator.config.Plan.DatabaseID
	config.ServiceID = coordinator.config.Plan.ServiceID
	config.LocalVersion = coordinator.config.Plan.TargetVersion
	config.LocalOnlyTables = append([]string(nil), coordinator.config.Plan.LocalOnlyTables...)
	client, err := newBorrowedClientFromDB(coordinator.db, "http://upgrade-admission.invalid", tokenProviderForTests, config)
	require.NoError(t, err)
	client.sourceIDGenerator = func() string { return "source-old" }
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		capabilities := upgradeCapabilitiesValue(target)
		switch request.URL.Path {
		case "/sync/generations/" + fmt.Sprint(target.Proof.ID):
			return jsonResponse(oversync.GenerationDiscoveryResponse{ProtocolVersion: requiredProtocolVersion, SchemaGeneration: capabilities.SchemaGeneration}), nil
		case "/sync/generations/1":
			return jsonResponse(capabilities), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return nil, fmt.Errorf("unexpected ordinary-admission request %s %s", request.Method, request.URL.Path)
		}
	})}
	err = client.open(context.Background())
	require.NoError(t, err)
	_, err = client.Attach(context.Background(), "user-1")
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func retainTestDiscoveredGenerationDescriptor(t *testing.T, db *sql.DB, local LocalGenerationContract) {
	t.Helper()
	capabilities := upgradeCapabilitiesValue(local)
	raw, err := json.Marshal(oversync.GenerationDiscoveryResponse{
		ProtocolVersion:  requiredProtocolVersion,
		SchemaGeneration: capabilities.SchemaGeneration,
		Limits:           testWireLimits(),
	})
	require.NoError(t, err)
	canonical, err := jcs.Canonicalize(raw)
	require.NoError(t, err)
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	require.NoError(t, retainDiscoveredGenerationDescriptor(
		context.Background(), tx, string(canonical), local.Proof,
	))
	require.NoError(t, tx.Commit(context.Background()))
}

func TestCompileLocalContract_SelectsSchemaQualifiedGenerationSubset(t *testing.T) {
	registered := []oversync.RegisteredTable{
		{Schema: "alpha", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "beta", Table: "users", SyncKeyColumns: []string{"id"}},
	}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{
		{Schema: "alpha", Table: "users", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}},
		{Schema: "beta", Table: "users", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}},
	}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	contract, ok := registry.Generation(1)
	require.True(t, ok)

	local := LocalGenerationContract{
		Proof:            oversync.SchemaGenerationProof{ID: 1, Fingerprint: contract.Fingerprint()},
		RegisteredTables: registered,
		Generation:       generation,
		Tables:           []SyncTable{{TableName: "users", SyncKeyColumns: []string{"id"}}},
		Columns: []LocalTableColumns{{Schema: "alpha", Table: "users", Columns: []LocalColumnContract{
			{Name: "id", Codec: LocalCodecUUIDText},
		}}},
	}
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	require.Equal(t, "alpha", compiled.localTables[0].Schema)
}

func TestValidateLocalOnlyTableScope_AllowsOmittedGenerationTable(t *testing.T) {
	registered := []oversync.RegisteredTable{
		{Schema: "main", Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: "main", Table: "audit", SyncKeyColumns: []string{"id"}},
	}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{
		{Schema: "main", Table: "users", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}},
		{Schema: "main", Table: "audit", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}},
	}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	require.NoError(t, err)
	contract, ok := registry.Generation(1)
	require.True(t, ok)
	local := LocalGenerationContract{
		Proof:            oversync.SchemaGenerationProof{ID: 1, Fingerprint: contract.Fingerprint()},
		RegisteredTables: registered,
		Generation:       generation,
		Tables:           []SyncTable{{TableName: "users", SyncKeyColumns: []string{"id"}}},
		Columns: []LocalTableColumns{{Schema: "main", Table: "users", Columns: []LocalColumnContract{
			{Name: "id", Codec: LocalCodecUUIDText},
		}}},
	}
	compiled, err := compileLocalContract(local)
	require.NoError(t, err)
	localOnly, err := validateLocalOnlyTableScope([]string{"audit"}, compiled)
	require.NoError(t, err)
	require.Equal(t, []string{"audit"}, localOnly)
}

func upgradeCapabilities(t *testing.T, local LocalGenerationContract) oversync.GenerationDiscoveryResponse {
	t.Helper()
	return upgradeCapabilitiesValue(local)
}

func upgradeCapabilitiesValue(local LocalGenerationContract) oversync.GenerationDiscoveryResponse {
	compiled, err := compileLocalContract(local)
	if err != nil {
		panic(err)
	}
	tables := make([]oversync.SchemaGenerationTableCapabilities, 0, len(compiled.contract.Tables()))
	for _, table := range compiled.contract.Tables() {
		out := oversync.SchemaGenerationTableCapabilities{
			Schema: table.Schema, Table: table.Table,
			SyncKeyColumns: append([]string(nil), table.SyncKeyColumns...),
			ForeignKeys:    []oversync.SchemaGenerationForeignKeyCapabilities{},
		}
		for _, column := range table.Columns {
			out.Columns = append(out.Columns, oversync.SchemaGenerationColumnCapabilities{Name: column.Name, WireType: column.WireType, Nullable: column.Nullable})
		}
		for _, foreignKey := range table.ForeignKeys {
			out.ForeignKeys = append(out.ForeignKeys, oversync.SchemaGenerationForeignKeyCapabilities{
				Columns: append([]string(nil), foreignKey.Columns...), ReferencedSchema: foreignKey.ReferencedSchema,
				ReferencedTable: foreignKey.ReferencedTable, ReferencedColumns: append([]string(nil), foreignKey.ReferencedColumns...),
				OnDelete: foreignKey.OnDelete, OnUpdate: foreignKey.OnUpdate,
			})
		}
		tables = append(tables, out)
	}
	return oversync.GenerationDiscoveryResponse{
		ProtocolVersion: requiredProtocolVersion,
		SchemaGeneration: oversync.SchemaGenerationCapabilities{
			ID: fmt.Sprint(local.Proof.ID), Fingerprint: local.Proof.Fingerprint, Tables: tables,
		},
		Limits: testWireLimits(),
	}
}

func openUpgradeTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "client.sqlite"))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestOpening_FreshCreationPublishesReadyAtomically(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	migrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, step LocalMigrationStep) error {
		require.True(t, step.Fresh)
		_, err := tx.ExecContext(ctx, `
			CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
			CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
			PRAGMA user_version=1;
		`)
		return err
	}}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "fresh-v1", Mode: UpgradeModeFresh, DatabaseID: "device-db", ServiceID: "example-service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"local_notes"}, Migrator: migrator,
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.Equal(t, UpgradePhaseReady, result.Status.Phase)
	require.NotNil(t, result.Ready)
	require.NoError(t, coordinator.Close())
	require.Equal(t, db, result.Ready.Database())
	require.NoError(t, result.Ready.Close())
}

func TestBorrowedClient_I01I02ClaimsOwnershipBeforeMutableReadyValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.sqlite")
	prepareProcessTestDatabase(t, path)
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	contract := processTestContract(t)
	config := admittedFixtureConfig("main", contract.Tables, contract.Proof)
	config.localContract = &contract
	config.DatabaseID = "process-db"
	config.ServiceID = "service"
	config.LocalVersion = 1
	config.LocalOnlyTables = []string{"local_notes"}
	owned := make(chan struct{})
	release := make(chan struct{})
	config.testAfterOwnership = func() {
		close(owned)
		<-release
	}
	type constructionResult struct {
		client *Client
		err    error
	}
	result := make(chan constructionResult, 1)
	go func() {
		client, err := newOwnedAdmittedFixtureClient(db, "http://example.invalid", tokenProviderForTests, config)
		result <- constructionResult{client: client, err: err}
	}()
	select {
	case <-owned:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not acquire ownership")
	}

	competitorDB, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	competitorDB.SetMaxOpenConns(1)
	_, err = newTestOpening(competitorDB, &openingRuntimeConfig{Plan: processTestLocalPlan(t)})
	var ownedErr *DatabaseAlreadyOwnedError
	require.ErrorAs(t, err, &ownedErr)
	require.NoError(t, competitorDB.Close())

	// Simulate mutable ready identity changing at the historical pre-ownership
	// race boundary. Validation after the ownership claim must still observe it.
	_, err = db.Exec(`UPDATE _sync_upgrade_identity SET ready=0 WHERE singleton_key=1`)
	require.NoError(t, err)
	close(release)
	constructed := <-result
	require.Nil(t, constructed.client)
	var upgradeErr *UpgradeError
	require.ErrorAs(t, constructed.err, &upgradeErr)
	require.Equal(t, UpgradeOutcomePlanMismatch, upgradeErr.Outcome)
}

func TestInitializeDatabase_I01RejectsPrototypeOutboxLayoutWithoutMigration(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE _sync_outbox_bundle (singleton_key INTEGER PRIMARY KEY)`)
	require.NoError(t, err)
	err = initializeDatabase(db)
	require.ErrorContains(t, err, "unsupported sync control schema")

	rows, err := db.Query(`PRAGMA table_info(_sync_outbox_bundle)`)
	require.NoError(t, err)
	defer rows.Close()
	foundActivePushID := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, declaredType string
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &declaredType, &notNull, &defaultValue, &pk))
		foundActivePushID = foundActivePushID || name == "active_push_id"
	}
	require.NoError(t, rows.Err())
	require.False(t, foundActivePushID, "unsupported prototype state must not be altered")
}

func TestOpening_I07ValidatesForeignKeysWithoutDeclarationTiming(t *testing.T) {
	table := oversync.GenerationTableContract{
		Schema: "main", Table: "children", SyncKeyColumns: []string{"id"},
		Columns: []oversync.GenerationColumn{
			{Name: "id", WireType: oversync.GenerationWireTypeUUID},
			{Name: "parent_a", WireType: oversync.GenerationWireTypeUUID},
			{Name: "parent_b", WireType: oversync.GenerationWireTypeUUID},
		},
		ForeignKeys: []oversync.GenerationForeignKey{
			{Columns: []string{"parent_a"}, ReferencedSchema: "main", ReferencedTable: "parents_a", ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeyNoAction, OnUpdate: oversync.GenerationForeignKeyNoAction},
			{Columns: []string{"parent_b"}, ReferencedSchema: "main", ReferencedTable: "parents_b", ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeyNoAction, OnUpdate: oversync.GenerationForeignKeyNoAction},
		},
	}
	for _, clause := range []string{"", "NOT DEFERRABLE", "DEFERRABLE INITIALLY IMMEDIATE", "DEFERRABLE INITIALLY DEFERRED"} {
		t.Run("timing="+clause, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`
				PRAGMA foreign_keys=ON;
				CREATE TABLE parents_a (id TEXT NOT NULL PRIMARY KEY);
				CREATE TABLE parents_b (id TEXT NOT NULL PRIMARY KEY);
				CREATE TABLE children (
					id TEXT NOT NULL PRIMARY KEY,
					parent_a TEXT NOT NULL REFERENCES parents_a(id) DEFERRABLE INITIALLY DEFERRED,
					parent_b TEXT NOT NULL REFERENCES parents_b(id) ` + clause + `
				)`)
			require.NoError(t, err)
			require.NoError(t, validatePhysicalForeignKeys(context.Background(), db, table))
			wrongAction := table
			wrongAction.ForeignKeys = append([]oversync.GenerationForeignKey(nil), table.ForeignKeys...)
			wrongAction.ForeignKeys[0].OnDelete = oversync.GenerationForeignKeyCascade
			require.ErrorContains(t, validatePhysicalForeignKeys(context.Background(), db, wrongAction), "foreign-key contract mismatch")
		})
	}
}

func TestOpening_I07RejectsForeignKeyCodecRepresentationMismatch(t *testing.T) {
	registered := []oversync.RegisteredTable{
		{Schema: "main", Table: "parents", SyncKeyColumns: []string{"id"}},
		{Schema: "main", Table: "children", SyncKeyColumns: []string{"id"}},
	}
	generation := oversync.SchemaGeneration{ID: 1, Tables: []oversync.GenerationTable{
		{Schema: "main", Table: "parents", Columns: []oversync.GenerationColumn{{Name: "id", WireType: oversync.GenerationWireTypeUUID}}},
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
	local := LocalGenerationContract{
		Proof: oversync.SchemaGenerationProof{ID: 1, Fingerprint: compiled.Fingerprint()}, RegisteredTables: registered, Generation: generation,
		Tables: []SyncTable{
			{TableName: "parents", SyncKeyColumns: []string{"id"}},
			{TableName: "children", SyncKeyColumns: []string{"id"}},
		},
		Columns: []LocalTableColumns{
			{Schema: "main", Table: "parents", Columns: []LocalColumnContract{{Name: "id", Codec: LocalCodecUUIDBlob}}},
			{Schema: "main", Table: "children", Columns: []LocalColumnContract{
				{Name: "id", Codec: LocalCodecUUIDText},
				{Name: "parent_id", Codec: LocalCodecUUIDText},
			}},
		},
	}
	_, err = compileLocalContract(local)
	require.ErrorContains(t, err, "foreign-key representation")
}

func TestOpening_FreshMigrationFailureRollsBackBusinessState(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	migrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
			return err
		}
		return fmt.Errorf("injected migration failure")
	}}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "fresh-fail", Mode: UpgradeModeFresh, DatabaseID: "device-db", ServiceID: "example-service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: migrator,
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeLocalMigrationFailed, result.Status.Outcome)
	var exists int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='people'`).Scan(&exists))
	require.Zero(t, exists)
	var version int
	require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
	require.Zero(t, version)
}

func TestOpening_I05AtomicMigrationFailureMatrix(t *testing.T) {
	for _, test := range []struct {
		name    string
		migrate func(context.Context, LocalMigrationTx, LocalMigrationStep) error
	}{
		{name: "false-version", migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)`)
			return err
		}},
		{name: "panic", migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			if _, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1;`); err != nil {
				return err
			}
			panic("application migration panic")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
				{Name: "id", WireType: oversync.GenerationWireTypeUUID},
				{Name: "name", WireType: oversync.GenerationWireTypeText},
			})
			coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
				PlanID: "atomic-" + test.name, Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
				TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: testLocalMigrator{migrate: test.migrate},
			}})
			require.NoError(t, err)
			result, err := coordinator.Advance(context.Background())
			require.Error(t, err)
			require.Equal(t, UpgradeOutcomeLocalMigrationFailed, result.Status.Outcome)
			var tableCount, version int
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='people'`).Scan(&tableCount))
			require.Zero(t, tableCount)
			require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
			require.Zero(t, version)
		})
	}
}

func TestOpening_I06PostMigrationVersionReaderIsReadOnly(t *testing.T) {
	db := openUpgradeTestDB(t)
	_, err := db.Exec(`CREATE TABLE app_version(version INTEGER NOT NULL); INSERT INTO app_version VALUES (1)`)
	require.NoError(t, err)
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	defer tx.Rollback()

	migrator := &postMigrationWritingVersionReader{}
	_, err = invokeMigratorInTx(context.Background(), tx, migrator, LocalMigrationStep{FromVersion: 1, ToVersion: 2, TargetVersion: 2})
	require.Error(t, err)
	var version int64
	require.NoError(t, tx.QueryRowContext(context.Background(), `SELECT version FROM app_version`).Scan(&version))
	require.Equal(t, int64(1), version)
}

func TestUpgradeMigrationCapabilityRejectsEscapesAndExpires(t *testing.T) {
	db := openUpgradeTestDB(t)
	db.SetMaxOpenConns(2)
	require.NoError(t, initializeDatabase(db))
	tx, err := beginImmediateTx(context.Background(), db)
	require.NoError(t, err)
	defer tx.Rollback()
	var retained LocalMigrationTx
	var retainedRows *sql.Rows
	var retainedRow *sql.Row
	migrator := testLocalMigrator{migrate: func(ctx context.Context, capability LocalMigrationTx, _ LocalMigrationStep) error {
		retained = capability
		_, err := capability.ExecContext(ctx, `COMMIT`)
		require.Error(t, err)
		_, err = capability.ExecContext(ctx, `UPDATE _sync_apply_state SET apply_mode=0`)
		require.Error(t, err)
		alternateCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err = db.ExecContext(alternateCtx, `CREATE TABLE alternate_connection_escape (id INTEGER)`)
		require.Error(t, err, "the coordinator-owned writer must exclude an alternate migration connection")
		retainedRows, err = capability.QueryContext(ctx, `SELECT 1 UNION ALL SELECT 2`)
		require.NoError(t, err)
		retainedRow = capability.QueryRowContext(ctx, `SELECT 1`)
		return nil
	}}
	_, err = invokeMigratorInTx(context.Background(), tx, migrator, LocalMigrationStep{})
	require.NoError(t, err)
	_, err = retained.ExecContext(context.Background(), `CREATE TABLE escaped (id INTEGER)`)
	require.Error(t, err)
	require.False(t, retainedRows.Next(), "retained rows must be closed when the migration callback returns")
	var retainedValue int
	require.Error(t, retainedRow.Scan(&retainedValue), "retained row must expire with the migration callback")
}

func TestOpening_LocalOnlyPreservesSyncStateAndAdvancesVersion(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	freshMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
			CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
			CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
			INSERT INTO local_notes VALUES ('sentinel', 'before');
			PRAGMA user_version=1;
		`)
		return err
	}}
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "fresh-for-local", Mode: UpgradeModeFresh, DatabaseID: "device-db", ServiceID: "example-service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"local_notes"}, Migrator: freshMigrator,
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	_, err = db.Exec(`INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'Alice')`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE _sync_source_state SET next_source_bundle_id=7 WHERE source_id='source-old'`)
	require.NoError(t, err)
	localMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `UPDATE local_notes SET note='after' WHERE id='sentinel'; PRAGMA user_version=2;`)
		return err
	}}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "local-v2", Mode: UpgradeModeLocalOnly, DatabaseID: "device-db", ServiceID: "example-service", UserID: "user-1",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
		LocalOnlyTables: []string{"local_notes"}, Migrator: localMigrator,
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	var name, note string
	require.NoError(t, db.QueryRow(`SELECT name FROM people`).Scan(&name))
	require.Equal(t, "Alice", name)
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
	require.Equal(t, "after", note)
	var dirty, next int64
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Equal(t, int64(1), dirty)
	require.NoError(t, db.QueryRow(`SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='source-old'`).Scan(&next))
	require.Equal(t, int64(7), next)
}

func TestOpening_I11AnonymousDatabaseAllowsOfflineLocalOnlyMigration(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "anonymous-fresh-v1", Mode: UpgradeModeFresh, DatabaseID: "anonymous-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())

	local, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "anonymous-local-v2", Mode: UpgradeModeLocalOnly, DatabaseID: "anonymous-db", ServiceID: "service",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `PRAGMA user_version=2`)
			return err
		}},
	}})
	require.NoError(t, err)
	result, err := local.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.Empty(t, result.Status.OldSourceID)
	var binding, userID string
	require.NoError(t, db.QueryRow(`SELECT binding_state,attached_user_id FROM _sync_attachment_state WHERE singleton_key=1`).Scan(&binding, &userID))
	require.Equal(t, attachmentBindingAnonymous, binding)
	require.Empty(t, userID)
	require.NoError(t, local.Close())
}

func TestOpening_I07ReopenedWriterEnforcesForeignKeysBeforeReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign-keys.sqlite")
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	freshMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
			CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL);
			CREATE TABLE local_parents(id TEXT NOT NULL PRIMARY KEY);
			CREATE TABLE local_children(
				id TEXT NOT NULL PRIMARY KEY,
				parent_id TEXT NOT NULL REFERENCES local_parents(id) DEFERRABLE INITIALLY DEFERRED
			);
			INSERT INTO local_parents VALUES ('parent');
			INSERT INTO local_children VALUES ('child', 'parent');
			PRAGMA user_version=1;
		`)
		return err
	}}
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "foreign-fresh", Mode: UpgradeModeFresh, DatabaseID: "foreign-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, LocalOnlyTables: []string{"local_children", "local_parents"}, Migrator: freshMigrator,
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	require.NoError(t, db.Close())

	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	local, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "foreign-local-v2", Mode: UpgradeModeLocalOnly, DatabaseID: "foreign-db", ServiceID: "service",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
		LocalOnlyTables: []string{"local_children", "local_parents"},
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `DELETE FROM local_parents WHERE id='parent'; PRAGMA user_version=2`)
			return err
		}},
	}})
	require.NoError(t, err)
	result, err := local.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeLocalContractMismatch, result.Status.Outcome)
	var parents, version int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM local_parents`).Scan(&parents))
	require.Equal(t, 1, parents)
	require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
	require.Equal(t, 1, version)
}

func TestOpening_I06MigrationCanTransformExistingSynchronizedRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := openUpgradeTestDB(t)
	oldContract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	targetContract := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "capture-fresh", Mode: UpgradeModeFresh, DatabaseID: "capture-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: oldContract.Proof.ID, testTarget: oldContract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(ctx)
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	_, err = db.Exec(`UPDATE _sync_apply_state SET apply_mode=1; INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8','Ada'); UPDATE _sync_apply_state SET apply_mode=0`)
	require.NoError(t, err)

	httpClient := &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
			if generation == fmt.Sprint(oldContract.Proof.ID) {
				return jsonResponse(upgradeCapabilities(t, oldContract)), nil
			}
			return jsonResponse(upgradeCapabilities(t, targetContract)), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
			return jsonResponse(oversync.PullResponse{StableBundleSeq: 0}), nil
		default:
			return nil, fmt.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	})}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: UpgradePlan{
			PlanID: "capture-generation", Mode: UpgradeModeGeneration, DatabaseID: "capture-db", ServiceID: "service", UserID: "user-1",
			InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: oldContract.Proof.ID, TargetGenerationID: targetContract.Proof.ID, testInstalled: oldContract, testTarget: targetContract,
			Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
				_, err := tx.ExecContext(ctx, `UPDATE people SET name=upper(name); PRAGMA user_version=2`)
				return err
			}},
		},
		BaseURL: "http://capture.invalid", Token: tokenProviderForTests, HTTP: httpClient,
		testBoundary: func(boundary string) {
			if boundary == "after_U4_commit" {
				cancel()
			}
		},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(ctx)
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeCancelled, result.Status.Outcome)
	var name string
	require.NoError(t, db.QueryRow(`SELECT name FROM people`).Scan(&name))
	require.Equal(t, "ADA", name)
	var dirty int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Zero(t, dirty)
	rec, err := loadUpgradeJournal(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, UpgradePhaseLocalMigrated, rec.Phase)
}

func TestOpening_I10CompletedAnonymousPlanReopensAfterNormalAttachment(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	plan := UpgradePlan{
		PlanID: "anonymous-attach-reopen", Mode: UpgradeModeFresh, DatabaseID: "anonymous-attach-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())

	clientConfig := DefaultConfig(contract.Proof.ID, contract.Tables)
	clientConfig.DatabaseID = "anonymous-attach-db"
	clientConfig.ServiceID = "service"
	clientConfig.LocalVersion = 1
	client, err := newClientFromDB(db, "http://attach.invalid", tokenProviderForTests, clientConfig)
	require.NoError(t, err)
	client.sourceIDGenerator = func() string { return "attached-source" }
	require.NoError(t, client.open(context.Background()))
	client.HTTP = &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/sync/generations/" + fmt.Sprint(contract.Proof.ID):
			capabilities := upgradeCapabilities(t, contract)
			return jsonResponse(oversync.GenerationDiscoveryResponse{
				ProtocolVersion: requiredProtocolVersion, SchemaGeneration: capabilities.SchemaGeneration,
			}), nil
		case "/sync/generations/1":
			return jsonResponse(upgradeCapabilities(t, contract)), nil
		case "/sync/connect":
			return jsonResponse(oversync.ConnectResponse{Resolution: "initialize_empty"}), nil
		default:
			return nil, fmt.Errorf("unexpected attach request %s %s", request.Method, request.URL.Path)
		}
	})}
	attach, err := client.Attach(context.Background(), "user-after-fresh")
	require.NoError(t, err)
	require.Equal(t, AttachStatusConnected, attach.Status)
	require.NoError(t, client.Close())

	reopened, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	result, err := reopened.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.NoError(t, reopened.Close())
}

func TestOpening_FailureLeavesDurableWriteFreeze(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	freshMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1;`)
		return err
	}}
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "fresh-freeze", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: freshMigrator,
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	_, err = db.Exec(`UPDATE _sync_upgrade_identity SET user_id='user-1'`)
	require.NoError(t, err)
	failing := testLocalMigrator{migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error {
		return fmt.Errorf("stop")
	}}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "local-fail", Mode: UpgradeModeLocalOnly, DatabaseID: "db", ServiceID: "service", UserID: "user-1",
		InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract, Migrator: failing,
	}})
	require.NoError(t, err)
	_, err = coordinator.Advance(context.Background())
	require.Error(t, err)
	_, err = db.Exec(`INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'blocked')`)
	require.ErrorContains(t, err, transitionPendingTriggerError)
}

func TestOpening_I01ExactIdentityPlanContract(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	_, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: " bad ", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: testLocalMigrator{},
	}})
	require.Error(t, err)
	var tables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tables))
	require.Zero(t, tables, "invalid construction must not mutate the database")
}

func TestOpening_I01PlanIsImmutableAfterConstruction(t *testing.T) {
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	db := openUpgradeTestDB(t)
	plan := UpgradePlan{
		PlanID: "immutable-plan", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	plan.PlanID = "mutated"
	plan.LocalOnlyTables = append(plan.LocalOnlyTables, "late_table")
	plan.testTarget.Tables[0].TableName = "late_table"
	plan.testTarget.RegisteredTables[0].SyncKeyColumns[0] = "late_key"
	plan.testTarget.Generation.Tables[0].Columns[0].Name = "late_column"
	plan.testTarget.Columns[0].Columns[0].Name = "late_column"

	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.Equal(t, "immutable-plan", result.Status.PlanID)
}

func TestOpening_I02DurableExclusiveFreeze(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	migrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1;`)
		return err
	}}
	config := &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "exclusive", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: migrator,
	}}
	first, err := newTestOpening(db, config)
	require.NoError(t, err)
	_, err = newTestOpening(db, config)
	var busy *UpgradeError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, UpgradeOutcomeBusy, busy.Outcome)
	require.NoError(t, first.Close())
}

func TestOpening_I02CloseRetainsOwnershipUntilActiveMigrationStops(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-active.sqlite")
	db1, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db1.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db1.Close()) })
	db2, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	db2.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db2.Close()) })
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	entered := make(chan struct{})
	release := make(chan struct{})
	config := &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "close-active", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			close(entered)
			<-release
			_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}}
	coordinator, err := newTestOpening(db1, config)
	require.NoError(t, err)
	runDone := make(chan error, 1)
	go func() {
		_, runErr := coordinator.Advance(context.Background())
		runDone <- runErr
	}()
	<-entered
	require.NoError(t, coordinator.Close())

	_, err = newTestOpening(db2, config)
	var busy *UpgradeError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, UpgradeOutcomeBusy, busy.Outcome)
	close(release)
	require.Error(t, <-runDone)

	retry, err := newTestOpening(db2, config)
	require.NoError(t, err)
	require.NoError(t, retry.Close())
}

func TestOpening_I02ProgressCallbackCanCloseAtSafeBoundary(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	plan := UpgradePlan{
		PlanID: "progress-close", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract,
		Migrator: testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1`)
			return err
		}},
	}
	var coordinator *Opening
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan, Progress: func(status OpeningStatus) {
		if status.Phase == UpgradePhaseMigratingLocal {
			require.NoError(t, coordinator.Close())
		}
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeCancelled, result.Status.Outcome)

	retry, err := newTestOpening(db, &openingRuntimeConfig{Plan: plan})
	require.NoError(t, err)
	require.NoError(t, retry.Close())
}

func TestOpening_I02CancellationKeepsDurableFreeze(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	freshMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL); PRAGMA user_version=1;`)
		return err
	}}
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "cancel-fresh", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: freshMigrator,
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(context.Background())
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	_, err = db.Exec(`UPDATE _sync_upgrade_identity SET user_id='user-1'`)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: UpgradePlan{
			PlanID: "cancel-local", Mode: UpgradeModeLocalOnly, DatabaseID: "db", ServiceID: "service", UserID: "user-1",
			InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: contract.Proof.ID, TargetGenerationID: contract.Proof.ID, testInstalled: contract, testTarget: contract,
			Migrator: testLocalMigrator{migrate: func(context.Context, LocalMigrationTx, LocalMigrationStep) error { return nil }},
		},
		Progress: func(status OpeningStatus) {
			if status.Phase == UpgradePhaseMigratingLocal {
				cancel()
			}
		},
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(ctx)
	require.Error(t, err)
	require.Equal(t, UpgradeOutcomeCancelled, result.Status.Outcome)
	require.Equal(t, UpgradePhaseMigratingLocal, result.Status.Phase)
	_, err = db.Exec(`INSERT INTO people VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8','blocked')`)
	require.ErrorContains(t, err, transitionPendingTriggerError)
}

func TestOpening_I07ContractAndPreservation(t *testing.T) {
	db := openUpgradeTestDB(t)
	contract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
	})
	migrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name BLOB NOT NULL); PRAGMA user_version=1;`)
		return err
	}}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "bad-affinity", Mode: UpgradeModeFresh, DatabaseID: "db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: contract.Proof.ID, testTarget: contract, Migrator: migrator,
	}})
	require.NoError(t, err)
	result, err := coordinator.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	var people int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='people'`).Scan(&people))
	require.Equal(t, 1, people)
	identity, err := loadUpgradeIdentity(context.Background(), db)
	require.NoError(t, err)
	require.Empty(t, identity.GenerationFingerprint, "fresh offline creation must not invent an admitted server descriptor")
}

func TestOpening_I09GenerationRebuildReady(t *testing.T) {
	ctx := context.Background()
	db := openUpgradeTestDB(t)
	oldContract := testUpgradeContract(t, 1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "legacy_name", WireType: oversync.GenerationWireTypeText},
	})
	targetContract := testUpgradeContract(t, 3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
		{Name: "new_label", WireType: oversync.GenerationWireTypeText},
	})
	freshMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
			CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, legacy_name TEXT NOT NULL);
			CREATE TABLE local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
			INSERT INTO local_notes VALUES ('sentinel', 'keep');
			PRAGMA user_version=1;
		`)
		return err
	}}
	fresh, err := newTestOpening(db, &openingRuntimeConfig{Plan: UpgradePlan{
		PlanID: "old-install", Mode: UpgradeModeFresh, DatabaseID: "device-db", ServiceID: "service",
		TargetVersion: 1, TargetGenerationID: oldContract.Proof.ID, testTarget: oldContract, LocalOnlyTables: []string{"local_notes"}, Migrator: freshMigrator,
	}})
	require.NoError(t, err)
	_, err = fresh.Advance(ctx)
	require.NoError(t, err)
	require.NoError(t, fresh.Close())
	admitTestCoordinatorTarget(t, fresh)
	_, err = db.Exec(`CREATE TRIGGER app_local_notes_update AFTER UPDATE ON local_notes BEGIN SELECT NEW.note; END`)
	require.NoError(t, err)
	personID := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	_, err = db.Exec(`INSERT INTO people VALUES (?, 'offline old edit')`, personID)
	require.NoError(t, err)

	server := newMockPushServer(t)
	targetRows := []oversync.SnapshotRow{{
		Schema: "main", Table: "people", Key: oversync.SyncKey{"id": personID}, RowVersion: 2,
		Payload: json.RawMessage(`{"id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","name":"server name","new_label":"hydrated"}`),
	}}
	targetBytes := testSnapshotRowsByteCount(targetRows)
	replacementRequests := 0
	requestCount := 0
	var replacementSourceID string
	httpClient := &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		generation := request.Header.Get(oversync.SchemaGenerationIDHeader)
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/sync/generations/1":
			if generation == fmt.Sprint(oldContract.Proof.ID) {
				return jsonResponse(upgradeCapabilities(t, oldContract)), nil
			}
			require.Equal(t, fmt.Sprint(targetContract.Proof.ID), generation)
			return jsonResponse(upgradeCapabilities(t, targetContract)), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/pull":
			require.Equal(t, fmt.Sprint(oldContract.Proof.ID), generation)
			return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
		case request.Method == http.MethodPost && request.URL.Path == "/sync/snapshot-sessions":
			require.Equal(t, fmt.Sprint(targetContract.Proof.ID), generation)
			var create oversync.SnapshotSessionCreateRequest
			require.NoError(t, json.NewDecoder(request.Body).Decode(&create))
			require.NotNil(t, create.SourceReplacement)
			require.Equal(t, "source-old", create.SourceReplacement.PreviousSourceID)
			require.NotEqual(t, "source-old", create.SourceReplacement.NewSourceID)
			require.Equal(t, "schema_generation_upgrade", create.SourceReplacement.Reason)
			replacementRequests++
			if replacementSourceID == "" {
				replacementSourceID = create.SourceReplacement.NewSourceID
			} else {
				require.Equal(t, replacementSourceID, create.SourceReplacement.NewSourceID)
			}
			if replacementRequests == 1 {
				return nil, io.ErrUnexpectedEOF
			}
			return jsonResponse(oversync.SnapshotSession{SnapshotID: "upgrade-snapshot", SnapshotBundleSeq: 2, RowCount: 1, ByteCount: targetBytes, ExpiresAt: "2030-01-01T00:00:00Z"}), nil
		case request.Method == http.MethodGet && request.URL.Path == "/sync/snapshot-sessions/upgrade-snapshot":
			return jsonResponse(oversync.SnapshotChunkResponse{SnapshotID: "upgrade-snapshot", SnapshotBundleSeq: 2, Rows: targetRows, NextRowOrdinal: 1, HasMore: false}), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/sync/snapshot-sessions/upgrade-snapshot":
			return jsonResponse(map[string]string{"status": "deleted"}), nil
		default:
			return server.RoundTrip(request)
		}
	})}
	targetMigrator := testLocalMigrator{migrate: func(ctx context.Context, tx LocalMigrationTx, _ LocalMigrationStep) error {
		before := requestCount
		_, err := tx.ExecContext(ctx, `
			ALTER TABLE people RENAME TO people_old;
			CREATE TABLE people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, new_label TEXT NOT NULL);
			INSERT INTO people(id, name, new_label) SELECT id, legacy_name, 'local default' FROM people_old;
			DROP TABLE people_old;
			PRAGMA user_version=2;
		`)
		require.Equal(t, before, requestCount, "the coordinator must perform no remote I/O inside migration")
		return err
	}}
	coordinator, err := newTestOpening(db, &openingRuntimeConfig{
		Plan: UpgradePlan{
			PlanID: "generation-1-to-3", Mode: UpgradeModeGeneration, DatabaseID: "device-db", ServiceID: "service", UserID: "user-1",
			InstalledVersion: 1, TargetVersion: 2, InstalledGenerationID: oldContract.Proof.ID, TargetGenerationID: targetContract.Proof.ID, testInstalled: oldContract, testTarget: targetContract,
			LocalOnlyTables: []string{"local_notes"}, Migrator: targetMigrator,
		},
		BaseURL: "http://upgrade.invalid", Token: tokenProviderForTests, HTTP: httpClient,
	})
	require.NoError(t, err)
	result, err := coordinator.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, UpgradeOutcomeReady, result.Status.Outcome)
	require.Equal(t, UpgradePhaseReady, result.Status.Phase)
	require.Equal(t, 2, replacementRequests, "reply loss must replay the exact durable replacement tuple")
	require.NotEqual(t, result.Status.OldSourceID, result.Status.NewSourceID)
	require.Len(t, server.pushRequests, 1, "the old offline edit must be committed before migration")
	var name, label, note string
	require.NoError(t, db.QueryRow(`SELECT name, new_label FROM people WHERE id=?`, personID).Scan(&name, &label))
	require.Equal(t, "server name", name)
	require.Equal(t, "hydrated", label)
	require.NoError(t, db.QueryRow(`SELECT note FROM local_notes WHERE id='sentinel'`).Scan(&note))
	require.Equal(t, "keep", note)
	var appTrigger int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='app_local_notes_update'`).Scan(&appTrigger))
	require.Equal(t, 1, appTrigger)
	var dirty int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM _sync_dirty_rows`).Scan(&dirty))
	require.Zero(t, dirty)
	journal, err := loadUpgradeJournal(ctx, db)
	require.NoError(t, err)
	require.Equal(t, journal.SnapshotRowCount, journal.SnapshotRowsStaged)
	require.Equal(t, journal.SnapshotByteCount, journal.SnapshotBytesStaged)
	vector := qualificationState(t, db, "people")
	require.Len(t, vector["_sync_row_state"], 1)
	authoritative := qualificationRow(t, vector, "_sync_row_state", personID)
	require.JSONEq(t, `{"id":{"storage":"text","value":"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},"name":{"storage":"text","value":"server name"},"new_label":{"storage":"text","value":"hydrated"}}`, qualificationText(authoritative, "server_payload").(string))
	require.Equal(t, "2", qualificationText(authoritative, "row_version"))
	require.Equal(t, "live", qualificationText(authoritative, "server_kind"))
	provenance := qualificationPayload(t, authoritative, "server_provenance").(map[string]any)
	require.Equal(t, float64(3), provenance["generation_id"])
	require.Equal(t, targetContract.Proof.Fingerprint, provenance["generation_fingerprint"])
	require.Equal(t, "device-db", provenance["database_id"])
	require.Equal(t, "service", provenance["service_id"])
	require.Equal(t, "snapshot", provenance["origin"])
	require.Equal(t, float64(2), provenance["bundle_sequence"])
	require.Equal(t, float64(2), provenance["authoritative_version"])
	require.Equal(t, float64(2), provenance["local_version"])
	require.Equal(t, "admitted", provenance["admission"])
	require.Equal(t, map[string]any{"binding": "attached", "account_id": "user-1"}, provenance["account_scope"])
	require.NotEmpty(t, provenance["origin_source_id"])
	require.NotEmpty(t, provenance["physical_shape"])
	for _, table := range []string{"_sync_dirty_rows", "_sync_outbox_rows", "_sync_relationships"} {
		require.Empty(t, vector[table], table)
	}
	require.Equal(t, result.Status.NewSourceID, qualificationText(vector["_sync_attachment_state"][0], "current_source_id"))
	require.Equal(t, "2", qualificationText(vector["_sync_attachment_state"][0], "last_bundle_seq_seen"))
	require.Equal(t, "1", baselineText(t, db, `SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=?`, result.Status.NewSourceID))
	t.Log("baseline-qualification: old intent drained and complete target baseline installed before Ready")
}

func TestOpening_I09ProductionSnapshotNotFoundIsRestartable(t *testing.T) {
	proof := oversync.SchemaGenerationProof{ID: 3, Fingerprint: strings.Repeat("a", 64)}
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "people", SyncKeyColumnName: "id"}}, proof)
	client := &Client{
		BaseURL: "http://snapshot.invalid", Token: tokenProviderForTests, config: config,
		sourceID: "source-new", schemaGenerationProof: proof,
		HTTP: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			require.Equal(t, "/sync/snapshot-sessions/expired", request.URL.Path)
			return errorJSONResponse(http.StatusNotFound, oversync.ErrorResponse{
				Error: "snapshot_session_not_found", Message: "cleaned up",
			}), nil
		})},
	}
	_, _, err := client.fetchSnapshotChunkAsSource(context.Background(), "expired", 7, 0, 1, 1024, "source-new")
	require.Error(t, err)
	require.True(t, isRestartableSnapshotSessionError(err))
}
