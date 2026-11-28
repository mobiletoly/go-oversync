package simulator

import (
	"context"
	"database/sql"

	evolution "github.com/mobiletoly/go-oversync/integration/schema_evolution"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
)

// tableUpgradeDefinitions is application-owned DDL. Capability omission never
// authorizes the SDK to drop a business table on its own.
func tableUpgradeDefinitions() (versionedApplicationDefinition, versionedApplicationDefinition, error) {
	cfg, err := evolution.TableEvolutionConfiguration(evolution.TablesServing)
	if err != nil {
		return versionedApplicationDefinition{}, versionedApplicationDefinition{}, err
	}
	registry, err := oversync.CompileSchemaGenerationRegistry(cfg.RegisteredTables, cfg.SchemaGenerations)
	if err != nil {
		return versionedApplicationDefinition{}, versionedApplicationDefinition{}, err
	}
	definitions := make([]versionedApplicationDefinition, 2)
	for i, g := range cfg.SchemaGenerations {
		compiled, _ := registry.Generation(g.ID)
		contract := oversqlite.LocalGenerationContract{Proof: oversync.SchemaGenerationProof{ID: g.ID, Fingerprint: compiled.Fingerprint()}, RegisteredTables: cfg.RegisteredTables, Generation: g}
		for _, table := range g.Tables {
			contract.Tables = append(contract.Tables, oversqlite.SyncTable{TableName: table.Table, SyncKeyColumns: []string{"id"}})
			columns := oversqlite.LocalTableColumns{Schema: table.Schema, Table: table.Table}
			for _, column := range table.Columns {
				codec := oversqlite.LocalCodecText
				if column.WireType == oversync.GenerationWireTypeUUID {
					codec = oversqlite.LocalCodecUUIDText
				}
				columns.Columns = append(columns.Columns, oversqlite.LocalColumnContract{Name: column.Name, Codec: codec})
			}
			contract.Columns = append(contract.Columns, columns)
		}
		definitions[i] = versionedApplicationDefinition{Name: []string{"tables-old", "tables-target"}[i], LocalVersion: int64(i + 1), Contract: contract, LocalOnlyTables: []string{"table_local_notes"}, Prepare: func(ctx context.Context, db *sql.DB) (map[string]*sql.Stmt, error) {
			stmt, err := db.PrepareContext(ctx, `INSERT INTO people(id,name) VALUES(?,?)`)
			return map[string]*sql.Stmt{"insert": stmt}, err
		}}
	}
	definitions[0].Migrator = mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
 CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL);
 CREATE TABLE legacy_notes(id TEXT NOT NULL PRIMARY KEY,body TEXT NOT NULL);
 CREATE TABLE table_local_notes(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL);
 PRAGMA user_version=1;`)
		return err
	}}
	definitions[1].Migrator = mobileUpgradeMigrator{MigrateFunc: func(ctx context.Context, tx oversqlite.LocalMigrationTx, _ oversqlite.LocalMigrationStep) error {
		_, err := tx.ExecContext(ctx, `
 CREATE TABLE projects(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL);
 ALTER TABLE people RENAME TO people_old;
 CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,project_id TEXT REFERENCES projects(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED);
 INSERT INTO people(id,name) SELECT id,name FROM people_old;
 DROP TABLE people_old;
 CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,person_id TEXT REFERENCES people(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,body TEXT NOT NULL);
 DROP TABLE legacy_notes;
 PRAGMA user_version=2;`)
		return err
	}}
	return definitions[0], definitions[1], nil
}
