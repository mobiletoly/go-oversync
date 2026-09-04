package simulator

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

// versionedApplicationDefinition is the mobile harness's application-owned
// description. The target statement factory is deliberately invoked only
// after the coordinator has published ready for this exact definition.
type versionedApplicationDefinition struct {
	Name            string
	LocalVersion    int64
	Contract        oversqlite.LocalGenerationContract
	LocalOnlyTables []string
	Migrator        oversqlite.LocalMigrator
	Prepare         func(context.Context, *sql.DB) (map[string]*sql.Stmt, error)
}

type versionedApplication struct {
	Path       string
	Definition versionedApplicationDefinition
	DB         *sql.DB
	Client     *oversqlite.Client
	Statements map[string]*sql.Stmt
}

func (a *versionedApplication) Close() error {
	for _, statement := range a.Statements {
		_ = statement.Close()
	}
	if a.Client != nil {
		return a.Client.Close()
	}
	return nil
}

func openVersionedApplication(ctx context.Context, path, baseURL string, token func(context.Context) (string, error), httpClient *http.Client, plan oversqlite.UpgradePlan, definition versionedApplicationDefinition) (*versionedApplication, oversqlite.OpeningResult, error) {
	clientConfig := versionedTargetConfig(plan, definition)
	opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
		DatabasePath: path, Plan: &plan, BaseURL: baseURL, Token: token, HTTP: httpClient, TargetClientConfig: clientConfig,
	})
	if err != nil {
		return nil, oversqlite.OpeningResult{}, err
	}
	result, err := opening.Advance(ctx)
	if err != nil {
		_ = opening.Close()
		return nil, result, err
	}
	client := result.Ready
	if client == nil {
		return nil, result, fmt.Errorf("%s opening completed without a ready client", definition.Name)
	}
	db := client.Database()
	statements, err := definition.Prepare(ctx, db)
	if err != nil {
		_ = client.Close()
		return nil, result, fmt.Errorf("prepare %s statements after upgrade: %w", definition.Name, err)
	}
	return &versionedApplication{Path: path, Definition: definition, DB: db, Client: client, Statements: statements}, result, nil
}

func versionedTargetConfig(plan oversqlite.UpgradePlan, definition versionedApplicationDefinition) *oversqlite.Config {
	config := oversqlite.DefaultConfig(definition.Contract.Proof.ID, definition.Contract.Tables)
	config.DatabaseID = plan.DatabaseID
	config.ServiceID = plan.ServiceID
	config.LocalVersion = definition.LocalVersion
	config.LocalOnlyTables = append([]string(nil), definition.LocalOnlyTables...)
	return config
}

type mobileUpgradeMigrator struct {
	MigrateFunc func(context.Context, oversqlite.LocalMigrationTx, oversqlite.LocalMigrationStep) error
}

func (mobileUpgradeMigrator) InstalledVersion(ctx context.Context, reader oversqlite.LocalMigrationReader) (int64, error) {
	var version int64
	err := reader.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
	return version, err
}

func (m mobileUpgradeMigrator) Migrate(ctx context.Context, tx oversqlite.LocalMigrationTx, step oversqlite.LocalMigrationStep) error {
	return m.MigrateFunc(ctx, tx, step)
}

func mobileUpgradeContract(generationID oversync.SchemaGenerationID, columns []oversync.GenerationColumn) (oversqlite.LocalGenerationContract, error) {
	registered := []oversync.RegisteredTable{{Schema: "main", Table: "upgrade_people", SyncKeyColumns: []string{"id"}}}
	generation := oversync.SchemaGeneration{ID: generationID, Tables: []oversync.GenerationTable{{Schema: "main", Table: "upgrade_people", Columns: columns}}}
	registry, err := oversync.CompileSchemaGenerationRegistry(registered, []oversync.SchemaGeneration{generation})
	if err != nil {
		return oversqlite.LocalGenerationContract{}, err
	}
	compiled, ok := registry.Generation(generationID)
	if !ok {
		return oversqlite.LocalGenerationContract{}, fmt.Errorf("generation %d not compiled", generationID)
	}
	codecs := make([]oversqlite.LocalColumnContract, 0, len(columns))
	for _, column := range columns {
		codec := oversqlite.LocalCodecText
		if column.WireType == oversync.GenerationWireTypeUUID {
			codec = oversqlite.LocalCodecUUIDText
		}
		codecs = append(codecs, oversqlite.LocalColumnContract{Name: column.Name, Codec: codec})
	}
	return oversqlite.LocalGenerationContract{
		Proof:            oversync.SchemaGenerationProof{ID: generationID, Fingerprint: compiled.Fingerprint()},
		RegisteredTables: registered, Generation: generation,
		Tables:  []oversqlite.SyncTable{{TableName: "upgrade_people", SyncKeyColumns: []string{"id"}}},
		Columns: []oversqlite.LocalTableColumns{{Schema: "main", Table: "upgrade_people", Columns: codecs}},
	}, nil
}

func mobileUpgradeDefinitions() (versionedApplicationDefinition, versionedApplicationDefinition, error) {
	oldContract, err := mobileUpgradeContract(1, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "legacy_name", WireType: oversync.GenerationWireTypeText},
	})
	if err != nil {
		return versionedApplicationDefinition{}, versionedApplicationDefinition{}, err
	}
	targetContract, err := mobileUpgradeContract(3, []oversync.GenerationColumn{
		{Name: "id", WireType: oversync.GenerationWireTypeUUID},
		{Name: "name", WireType: oversync.GenerationWireTypeText},
		{Name: "new_label", WireType: oversync.GenerationWireTypeText},
	})
	if err != nil {
		return versionedApplicationDefinition{}, versionedApplicationDefinition{}, err
	}
	prepareOld := func(ctx context.Context, db *sql.DB) (map[string]*sql.Stmt, error) {
		statement, err := db.PrepareContext(ctx, `INSERT INTO upgrade_people(id,legacy_name) VALUES (?,?)`)
		return map[string]*sql.Stmt{"insert": statement}, err
	}
	prepareTarget := func(ctx context.Context, db *sql.DB) (map[string]*sql.Stmt, error) {
		statement, err := db.PrepareContext(ctx, `INSERT INTO upgrade_people(id,name,new_label) VALUES (?,?,?)`)
		return map[string]*sql.Stmt{"insert": statement}, err
	}
	old := versionedApplicationDefinition{
		Name: "old-v1", LocalVersion: 1, Contract: oldContract, LocalOnlyTables: []string{"upgrade_local_notes"},
		Migrator: mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				CREATE TABLE upgrade_people (id TEXT NOT NULL PRIMARY KEY, legacy_name TEXT NOT NULL);
				CREATE TABLE upgrade_local_notes (id TEXT NOT NULL PRIMARY KEY, note TEXT NOT NULL);
				PRAGMA user_version=1;
			`)
			return err
		}},
		Prepare: prepareOld,
	}
	target := versionedApplicationDefinition{
		Name: "new-v2", LocalVersion: 2, Contract: targetContract, LocalOnlyTables: []string{"upgrade_local_notes"},
		Migrator: mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
			_, err := tx.ExecContext(ctx, `
				ALTER TABLE upgrade_people RENAME TO upgrade_people_old;
				CREATE TABLE upgrade_people (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, new_label TEXT NOT NULL);
				INSERT INTO upgrade_people(id,name,new_label) SELECT id,legacy_name,'preloaded-default' FROM upgrade_people_old;
				DROP TABLE upgrade_people_old;
				PRAGMA user_version=2;
			`)
			return err
		}},
		Prepare: prepareTarget,
	}
	return old, target, nil
}
