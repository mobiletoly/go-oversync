package oversqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

// retainedUpgradeContractIdentity is the ordinary Client identity observed at
// an upgrade commit boundary. The upgrade journal remains the publication
// guard; this record lets the ordinary runtime admit the exact same file after
// the coordinator publishes ready.
type retainedUpgradeContractIdentity struct {
	databaseID    string
	serviceID     string
	generation    int64
	localVersion  int64
	physicalShape string
	fingerprint   string
}

func loadRetainedUpgradeContractIdentity(ctx context.Context, q upgradeQueryer) (*retainedUpgradeContractIdentity, error) {
	var identity retainedUpgradeContractIdentity
	err := q.QueryRowContext(ctx, `
		SELECT database_id,service_id,generation_id,local_version,physical_shape,fingerprint
		FROM _sync_contract_identity WHERE singleton_key=1
	`).Scan(
		&identity.databaseID,
		&identity.serviceID,
		&identity.generation,
		&identity.localVersion,
		&identity.physicalShape,
		&identity.fingerprint,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ordinary retained contract identity: %w", err)
	}
	return &identity, nil
}

func normalizeUpgradeClientConfig(plan UpgradePlan, supplied *Config, selected oversync.SchemaGenerationID, version int64, label string) (*Config, error) {
	if supplied == nil {
		return nil, fmt.Errorf("%s ordinary client configuration is required", label)
	}
	config := cloneCoordinatorClientConfig(supplied)
	if config.GenerationID != selected {
		return nil, fmt.Errorf("%s ordinary client generation does not match the selected generation", label)
	}
	if len(config.Tables) == 0 {
		return nil, fmt.Errorf("%s ordinary client table selection is empty", label)
	}
	if config.DatabaseID != plan.DatabaseID {
		return nil, fmt.Errorf("%s ordinary client database identity differs from the upgrade edge", label)
	}
	if config.ServiceID != plan.ServiceID {
		return nil, fmt.Errorf("%s ordinary client service identity differs from the upgrade edge", label)
	}
	if config.LocalVersion != version {
		return nil, fmt.Errorf("%s ordinary client local version differs from the upgrade edge", label)
	}
	declaredLocalOnly, err := validateLocalOnlyTableScope(config.LocalOnlyTables)
	if err != nil {
		return nil, fmt.Errorf("%s ordinary client local-only declaration: %w", label, err)
	}
	plannedLocalOnly, err := validateLocalOnlyTableScope(plan.LocalOnlyTables)
	if err != nil {
		return nil, fmt.Errorf("upgrade edge local-only declaration: %w", err)
	}
	if !stringSlicesEqual(declaredLocalOnly, plannedLocalOnly) {
		return nil, fmt.Errorf("%s ordinary client local-only declaration differs from the upgrade edge", label)
	}
	config.generationProof = oversync.SchemaGenerationProof{}
	config.localContract = nil
	return config, nil
}

func installedUpgradeClientConfig(ctx context.Context, db upgradeQueryer, plan UpgradePlan, supplied *Config, target *Config) (*Config, error) {
	if supplied != nil {
		return normalizeUpgradeClientConfig(plan, supplied, plan.InstalledGenerationID, plan.InstalledVersion, "installed")
	}
	_, response, err := loadAdmittedGenerationDescriptor(ctx, db, plan.InstalledGenerationID)
	if err == sql.ErrNoRows && plan.Mode == UpgradeModeLocalOnly && target != nil {
		config := cloneCoordinatorClientConfig(target)
		config.GenerationID = plan.InstalledGenerationID
		config.LocalVersion = plan.InstalledVersion
		return normalizeUpgradeClientConfig(plan, config, plan.InstalledGenerationID, plan.InstalledVersion, "installed")
	}
	if err != nil {
		return nil, err
	}
	tables := make([]SyncTable, 0, len(response.SchemaGeneration.Tables))
	for _, table := range response.SchemaGeneration.Tables {
		tables = append(tables, SyncTable{TableName: table.Table, SyncKeyColumns: append([]string(nil), table.SyncKeyColumns...)})
	}
	config := DefaultConfig(plan.InstalledGenerationID, tables)
	config.DatabaseID = plan.DatabaseID
	config.ServiceID = plan.ServiceID
	config.LocalVersion = plan.InstalledVersion
	config.LocalOnlyTables = append([]string(nil), plan.LocalOnlyTables...)
	return normalizeUpgradeClientConfig(plan, config, plan.InstalledGenerationID, plan.InstalledVersion, "installed")
}

func loadInstalledOrdinaryContract(ctx context.Context, db upgradeQueryer, plan UpgradePlan, supplied *Config, target *Config) (*Config, *LocalGenerationContract, error) {
	config, err := installedUpgradeClientConfig(ctx, db, plan, supplied, target)
	if err != nil {
		return nil, nil, err
	}
	shape, err := managedPhysicalShape(ctx, db, config.Tables)
	if err != nil {
		return nil, nil, err
	}
	if err := readRetainedContract(ctx, db, config, shape); err != nil {
		return nil, nil, err
	}
	if config.localContract == nil {
		return config, nil, nil
	}
	local := cloneLocalGenerationContract(*config.localContract)
	return config, &local, nil
}

func loadAdmittedGenerationDescriptor(ctx context.Context, q upgradeQueryer, selected oversync.SchemaGenerationID) (string, *oversync.GenerationDiscoveryResponse, error) {
	var descriptor, digest, fingerprint string
	if err := q.QueryRowContext(ctx, `
		SELECT descriptor,descriptor_hash,fingerprint
		FROM _sync_admitted_contracts
		WHERE generation_id=? AND length(CAST(descriptor AS BLOB))<=65536
	`, selected).Scan(&descriptor, &digest, &fingerprint); err != nil {
		return "", nil, err
	}
	if contractBytesHash(descriptor) != digest {
		return "", nil, fmt.Errorf("admitted generation descriptor hash is corrupt")
	}
	response, err := decodeDiscoveredContract([]byte(descriptor), selected)
	if err != nil {
		return "", nil, err
	}
	if response.SchemaGeneration.Fingerprint != fingerprint {
		return "", nil, fmt.Errorf("admitted generation descriptor fingerprint is corrupt")
	}
	canonical, err := jcs.Canonicalize([]byte(descriptor))
	if err != nil || string(canonical) != descriptor {
		return "", nil, fmt.Errorf("admitted generation descriptor is not canonical")
	}
	return descriptor, response, nil
}

func retainDiscoveredGenerationDescriptor(ctx context.Context, tx *immediateTx, descriptor string, expected oversync.SchemaGenerationProof) error {
	response, err := decodeDiscoveredContract([]byte(descriptor), expected.ID)
	if err != nil {
		return err
	}
	if response.SchemaGeneration.Fingerprint != expected.Fingerprint {
		return fmt.Errorf("target discovery proof changed before retention")
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO _sync_admitted_contracts(generation_id,fingerprint,descriptor,descriptor_hash)
		VALUES(?,?,?,?) ON CONFLICT(generation_id) DO NOTHING
	`, expected.ID, expected.Fingerprint, descriptor, contractBytesHash(descriptor)); err != nil {
		return err
	}
	retained, _, err := loadAdmittedGenerationDescriptor(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if retained != descriptor {
		return fmt.Errorf("retained target descriptor differs from discovery")
	}
	return nil
}

func bindOrdinaryContractInTx(
	ctx context.Context,
	tx *immediateTx,
	plan UpgradePlan,
	installed LocalGenerationContract,
	target LocalGenerationContract,
	targetDescriptor string,
	targetVersion int64,
	expectExisting bool,
) error {
	shape, err := managedPhysicalShape(ctx, tx, target.Tables)
	if err != nil {
		return err
	}
	existing, err := loadRetainedUpgradeContractIdentity(ctx, tx)
	if err != nil {
		return err
	}
	if expectExisting {
		if existing == nil || existing.databaseID != plan.DatabaseID || existing.serviceID != plan.ServiceID ||
			existing.generation != int64(installed.Proof.ID) || existing.localVersion != plan.InstalledVersion ||
			existing.fingerprint != installed.Proof.Fingerprint {
			return fmt.Errorf("ordinary retained identity changed before upgrade publication")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE _sync_contract_identity
			SET generation_id=?,local_version=?,physical_shape=?,fingerprint=?
			WHERE singleton_key=1 AND database_id=? AND service_id=?
				AND generation_id=? AND local_version=? AND fingerprint=?
		`, target.Proof.ID, targetVersion, shape, target.Proof.Fingerprint,
			plan.DatabaseID, plan.ServiceID, installed.Proof.ID, plan.InstalledVersion, installed.Proof.Fingerprint)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("ordinary retained identity changed during upgrade publication")
		}
	} else {
		if existing != nil {
			return fmt.Errorf("fresh coordinator found an ordinary retained identity")
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO _sync_contract_identity(
				singleton_key,database_id,service_id,generation_id,local_version,physical_shape,fingerprint
			) VALUES(1,?,?,?,?,?,?)
		`, plan.DatabaseID, plan.ServiceID, target.Proof.ID, targetVersion, shape, target.Proof.Fingerprint); err != nil {
			return err
		}
	}
	if target.Proof.Fingerprint != "" {
		if targetDescriptor == "" {
			return fmt.Errorf("confirmed target contract is missing its discovered descriptor")
		}
		if err := retainDiscoveredGenerationDescriptor(ctx, tx, targetDescriptor, target.Proof); err != nil {
			return err
		}
	}
	return nil
}

func verifyOrdinaryTargetContractInTx(ctx context.Context, tx *immediateTx, plan UpgradePlan, target LocalGenerationContract) error {
	config := DefaultConfig(target.Proof.ID, cloneSyncTables(target.Tables))
	config.DatabaseID = plan.DatabaseID
	config.ServiceID = plan.ServiceID
	config.LocalVersion = plan.TargetVersion
	shape, err := managedPhysicalShape(ctx, tx, config.Tables)
	if err != nil {
		return err
	}
	return readRetainedContract(ctx, tx, config, shape)
}
