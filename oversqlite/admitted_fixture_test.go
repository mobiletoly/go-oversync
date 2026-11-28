package oversqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

// Existing data-plane and upgrade regressions seed an admitted engine directly.
// Applications have only NewOpening, whose admission is covered by the discovery
// suite and real-server journeys. No manual-contract production path exists.
func newAdmittedFixtureClient(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config) (client *Client, err error) {
	return newAdmittedFixtureClientWithOwnership(db, baseURL, tok, config, false)
}

func newOwnedAdmittedFixtureClient(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config) (client *Client, err error) {
	return newAdmittedFixtureClientWithOwnership(db, baseURL, tok, config, true)
}

func newAdmittedFixtureClientWithOwnership(db *sql.DB, baseURL string, tok func(ctx context.Context) (string, error), config *Config, claimOwnership bool) (client *Client, err error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}
	config, err = prepareAdmittedFixtureConfiguration(config)
	if err != nil {
		return nil, err
	}
	tableInfoProvider := NewTableInfoProvider()
	validatedTables, err := validateSyncTables(db, tableInfoProvider, config.Tables)
	if err != nil {
		return nil, err
	}
	var identity clientDBIdentity
	if claimOwnership {
		identity, err = claimClientDBOwnership(db)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err != nil {
				releaseClientDBOwnership(identity.Key)
			}
		}()
	}

	if config.testAfterOwnership != nil {
		config.testAfterOwnership()
	}

	if config.localContract == nil {
		return nil, fmt.Errorf("config.localContract is required")
	}
	upgradeIdentityExists, err := sqliteNamedTableExistsNoCase(context.Background(), db, "_sync_upgrade_identity")
	if err != nil {
		return nil, err
	}
	if upgradeIdentityExists {
		if _, err := db.ExecContext(context.Background(), `
			UPDATE _sync_upgrade_identity
			SET generation_fingerprint=?
			WHERE singleton_key=1 AND generation_id=? AND generation_fingerprint='' AND ready=1
		`, config.generationProof.Fingerprint, config.generationProof.ID); err != nil {
			return nil, err
		}
	}
	proof := config.generationProof
	managedReady, err := inspectUpgradeConstructionGate(context.Background(), db, proof)
	if err != nil {
		return nil, err
	}
	localGeneration, err := compileClientContract(config)
	if err != nil {
		if managedReady {
			return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
		}
		return nil, err
	}
	if managedReady {
		if err := validateReadyClientIdentity(context.Background(), db, config); err != nil {
			return nil, err
		}
	}
	if config.GenerationID == 0 {
		if err := initializeDatabase(db); err != nil {
			return nil, err
		}
		if err := validatePhysicalLocalContract(context.Background(), db, *config.localContract, config.LocalOnlyTables); err != nil {
			return nil, err
		}
	}

	if err := seedAdmittedFixtureIdentity(db, config); err != nil {
		return nil, err
	}

	client = assembleClientRuntime(db, baseURL, tok, config, tableInfoProvider, validatedTables, identity.Key, localGeneration)
	client.ownsDB = claimOwnership
	if err := installClientRuntime(context.Background(), db, client, validatedTables); err != nil {
		return nil, err
	}
	registerMockGeneration(upgradeCapabilitiesValue(*config.localContract).SchemaGeneration)

	return client, nil
}

func prepareAdmittedFixtureConfiguration(config *Config) (*Config, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	clone := cloneCoordinatorClientConfig(config)
	if clone.DatabaseID == "" {
		clone.DatabaseID = "fixture"
	}
	if clone.ServiceID == "" {
		clone.ServiceID = "fixture"
	}
	if clone.generationProof.ID < 1 || !validSchemaGenerationFingerprint(clone.generationProof.Fingerprint) {
		return nil, fmt.Errorf("fixture generation proof is invalid")
	}
	// Reuse production transport defaults and then restore the fixture admission.
	clone.GenerationID = clone.generationProof.ID
	prepared, err := prepareClientConfiguration(clone)
	if err != nil {
		return nil, err
	}
	prepared.GenerationID = 0
	prepared.schema, prepared.generationProof, prepared.localContract = clone.schema, clone.generationProof, clone.localContract
	if clone.localContract != nil {
		discovery := upgradeCapabilitiesValue(*clone.localContract)
		prepared.discovery = &discovery
	}
	return prepared, nil
}

// Direct engine fixtures bypass network admission, but must retain the same
// concrete identity and descriptor that production admission supplies to B.
func seedAdmittedFixtureIdentity(db *sql.DB, config *Config) error {
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM _sync_contract_identity)`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	shape, err := managedPhysicalShape(context.Background(), db, config.Tables)
	if err != nil {
		return err
	}
	descriptor, err := jcs.Marshal(config.discovery)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO _sync_contract_identity(singleton_key,database_id,service_id,generation_id,local_version,physical_shape,fingerprint) VALUES(1,?,?,?,?,?,?)`, config.DatabaseID, config.ServiceID, config.generationProof.ID, config.LocalVersion, shape, config.generationProof.Fingerprint)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO _sync_admitted_contracts(generation_id,fingerprint,descriptor,descriptor_hash) VALUES(?,?,?,?)`, config.generationProof.ID, config.generationProof.Fingerprint, string(descriptor), contractBytesHash(string(descriptor)))
	return err
}
