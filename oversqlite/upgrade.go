package oversqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

// Opening owns one SQLite file while starting or resuming a local
// upgrade. It never exposes an ordinary ready Client before the ready commit.
type Opening struct {
	db                   *sql.DB
	config               openingRuntimeConfig
	installedContract    LocalGenerationContract
	targetContract       LocalGenerationContract
	targetDescriptor     string
	planDigest           string
	identityKey          string
	mutator              chan struct{}
	stateMu              sync.Mutex
	activeCancel         context.CancelFunc
	activeMutation       bool
	ownershipReleased    bool
	consumed             bool
	readyAdmission       bool
	preparedClient       *Client
	preparedStatus       *OpeningStatus
	testBorrowedDB       bool
	closed               atomic.Bool
	localMigrationActive atomic.Bool
}

// NewOpening claims a filesystem database before SQLite initialization and
// constructs a package-owned coordinated opening. Construction performs no
// network request or application migration.
func NewOpening(config *OpeningConfig) (*Opening, error) {
	if config == nil {
		return nil, fmt.Errorf("opening config cannot be nil")
	}
	if config.TargetClientConfig == nil {
		return nil, fmt.Errorf("target client configuration is required")
	}
	identity, canonicalPath, err := claimOpeningDBOwnership(config.DatabasePath)
	if err != nil {
		return nil, &UpgradeError{Outcome: classifyUpgradeCause(err, false), Cause: err}
	}
	release := true
	defer func() {
		if release {
			releaseClientDBOwnership(identity.Key)
		}
	}()

	dsnURL := &url.URL{Scheme: "file", Path: canonicalPath}
	query := dsnURL.Query()
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "WAL")
	query.Set("_busy_timeout", "5000")
	query.Set("_txlock", "immediate")
	dsnURL.RawQuery = query.Encode()
	db, err := sql.Open("sqlite3", dsnURL.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	closePool := true
	defer func() {
		if closePool {
			_ = db.Close()
		}
	}()
	if err := bindOpeningDBOwnership(identity.Key, db); err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("open package-owned SQLite database: %w", err)
	}

	runtimeConfig := openingRuntimeConfig{
		BaseURL: config.BaseURL, Token: config.Token, HTTP: config.HTTP,
		TargetClientConfig: cloneCoordinatorClientConfig(config.TargetClientConfig),
		Progress:           config.Progress, testBoundary: config.testBoundary,
	}
	if config.Plan != nil {
		runtimeConfig.Plan = cloneUpgradePlan(*config.Plan)
		runtimeConfig.PlanProvided = true
	}
	opening, err := newOpeningFromOwnedDB(db, &runtimeConfig, identity.Key)
	if err != nil {
		return nil, err
	}
	release = false
	closePool = false
	return opening, nil
}

// newOpeningFromDB is retained only for package-internal lower-level tests.
func newOpeningFromDB(db *sql.DB, config *openingRuntimeConfig) (*Opening, error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}
	if config == nil {
		return nil, fmt.Errorf("upgrade config cannot be nil")
	}
	identity, err := detectClientDBIdentity(db)
	if err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomeBusy, Cause: err}
	}
	if err := claimClientDBIdentity(identity, db, "opening"); err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomeBusy, Cause: err}
	}
	opening, err := newOpeningFromOwnedDB(db, config, identity.Key)
	if err != nil {
		releaseClientDBOwnership(identity.Key)
		return nil, err
	}
	opening.testBorrowedDB = true
	return opening, nil
}

func newOpeningFromOwnedDB(db *sql.DB, config *openingRuntimeConfig, identityKey string) (*Opening, error) {
	// Both callers have just acquired the existing physical lifetime lease.
	// Construction must finish admission before altering derived reservations.
	ctx := context.WithValue(context.Background(), freshLifetimeAdmissionKey{}, true)
	opening, err := inspectOpeningFromOwnedDB(ctx, db, config, identityKey)
	if err != nil {
		return nil, err
	}
	tx, err := beginImmediateTx(context.Background(), db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	census, err := inspectRetainedRuntime(context.Background(), tx)
	if err != nil {
		return nil, err
	}
	if census == retainedRuntimeCurrent {
		prepared, err := prepareClientConfiguration(opening.config.TargetClientConfig)
		if err != nil {
			return nil, err
		}
		if err = reclaimWorkspaceReservations(context.Background(), tx, prepared); err != nil {
			return nil, err
		}
		if err = checkRetainedApplyWorkspace(context.Background(), tx, prepared); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(context.Background()); err != nil {
		return nil, err
	}
	return opening, nil
}

func inspectOpeningFromOwnedDB(ctx context.Context, db *sql.DB, config *openingRuntimeConfig, identityKey string) (*Opening, error) {
	copyConfig := *config
	if config.PlanProvided || config.Plan.PlanID != "" {
		copyConfig.Plan = cloneUpgradePlan(config.Plan)
		copyConfig.PlanProvided = true
	}
	copyConfig.OldClientConfig = cloneCoordinatorClientConfig(config.OldClientConfig)
	copyConfig.TargetClientConfig = cloneCoordinatorClientConfig(config.TargetClientConfig)
	opening := &Opening{
		db: db, config: copyConfig, identityKey: identityKey,
		mutator: make(chan struct{}, 1),
	}
	opening.mutator <- struct{}{}
	ready, err := opening.hasReadyJournal(ctx)
	if err != nil {
		return nil, err
	}
	if ready {
		transitionRequested := false
		if copyConfig.PlanProvided {
			identity, identityErr := loadUpgradeIdentity(ctx, db)
			target, targetErr := prepareClientConfiguration(copyConfig.TargetClientConfig)
			if identityErr == nil && targetErr == nil && identity != nil {
				transitionRequested = identity.DatabaseID != target.DatabaseID || identity.ServiceID != target.ServiceID ||
					identity.GenerationID != int64(target.GenerationID) || identity.LocalVersion != target.LocalVersion
			}
		}
		if err := opening.loadReadyAdmissionState(ctx); err == nil {
			return opening, nil
		} else if !transitionRequested {
			return nil, err
		}
	}
	if !copyConfig.PlanProvided {
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("an exact plan is required for new or pending opening work")}
	}
	digest, err := validateUpgradePlan(copyConfig.Plan)
	if err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	opening.planDigest = digest
	if err := opening.loadCoordinatorContractState(ctx); err != nil {
		var upgradeErr *UpgradeError
		if errors.As(err, &upgradeErr) {
			return nil, upgradeErr
		}
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	if err := opening.inspectConstructionIdentity(ctx); err != nil {
		return nil, err
	}
	return opening, nil
}

func (c *Opening) hasReadyJournal(ctx context.Context) (bool, error) {
	exists, err := sqliteNamedTableExists(ctx, c.db, "_sync_upgrade_journal")
	if err != nil || !exists {
		return false, err
	}
	rec, err := loadUpgradeJournal(ctx, c.db)
	if err != nil {
		return false, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	return rec != nil && rec.Phase == UpgradePhaseReady, nil
}

func (c *Opening) loadReadyAdmissionState(ctx context.Context) error {
	target, err := prepareClientConfiguration(c.config.TargetClientConfig)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	if target.DatabaseID == "" || target.ServiceID == "" || target.LocalVersion < 0 || len(target.Tables) == 0 {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("target client configuration is incomplete")}
	}
	runtimeState, err := inspectRetainedRuntime(ctx, c.db)
	if err != nil || runtimeState != retainedRuntimeCurrent {
		if err == nil {
			err = fmt.Errorf("retained sync runtime is missing")
		}
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	rec, err := loadUpgradeJournal(ctx, c.db)
	if err != nil || rec == nil || rec.Phase != UpgradePhaseReady || rec.Outcome != UpgradeOutcomeReady || !rec.MigrationComplete {
		if err == nil {
			err = fmt.Errorf("durable ready journal is missing or incomplete")
		}
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	identity, err := loadUpgradeIdentity(ctx, c.db)
	if err != nil || identity == nil || !identity.Ready {
		if err == nil {
			err = fmt.Errorf("durable ready identity is missing")
		}
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if err := c.validateReadyAdmissionIdentity(target, rec, identity); err != nil {
		return err
	}
	if err := validatePhysicalApplicationTableScope(ctx, c.db, target.Tables, target.LocalOnlyTables); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	shape, err := managedPhysicalShape(ctx, c.db, target.Tables)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	if err := readRetainedContract(ctx, c.db, target, shape); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	if target.generationProof.Fingerprint != identity.GenerationFingerprint {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("retained contract fingerprint contradicts installed identity")}
	}
	local := LocalGenerationContract{
		Proof:  oversync.SchemaGenerationProof{ID: target.GenerationID, Fingerprint: identity.GenerationFingerprint},
		Tables: cloneSyncTables(target.Tables),
	}
	if target.localContract != nil {
		local = cloneLocalGenerationContract(*target.localContract)
		compiled, compileErr := compileLocalContract(local)
		if compileErr != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: compileErr}
		}
		if len(compiled.contract.Tables()) == 0 {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: fmt.Errorf("ready target contract contains no tables")}
		}
		target.schema = compiled.contract.Tables()[0].Schema
		target.generationProof = local.Proof
		if err := validatePhysicalLocalContract(ctx, c.db, local, target.LocalOnlyTables); err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
	} else {
		target.schema = unconfirmedLocalSchema
		target.generationProof = local.Proof
	}
	if _, err := validateSyncTables(c.db, NewTableInfoProvider(), target.Tables); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	c.config.TargetClientConfig = target
	c.targetContract = local
	c.planDigest = rec.PlanDigest
	c.readyAdmission = true
	return nil
}

func (c *Opening) validateReadyAdmissionIdentity(target *Config, rec *upgradeJournalRecord, identity *upgradeIdentityRecord) error {
	if identity.DatabaseID != target.DatabaseID || identity.ServiceID != target.ServiceID ||
		identity.GenerationID != int64(target.GenerationID) || identity.LocalVersion != target.LocalVersion {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("requested ready target does not match installed identity")}
	}
	if rec.DatabaseID != identity.DatabaseID || rec.ServiceID != identity.ServiceID ||
		rec.TargetGenerationID != identity.GenerationID || rec.TargetVersion != identity.LocalVersion ||
		rec.TargetGenerationFingerprint != "" && rec.TargetGenerationFingerprint != identity.GenerationFingerprint ||
		rec.ValidatedTargetFingerprint != "" && rec.ValidatedTargetFingerprint != identity.GenerationFingerprint {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("ready journal contradicts installed identity")}
	}
	if !c.config.PlanProvided {
		return nil
	}
	if _, err := validateUpgradePlan(c.config.Plan); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	plan := c.config.Plan
	if plan.TargetGenerationID != target.GenerationID || plan.TargetVersion != target.LocalVersion ||
		plan.DatabaseID != target.DatabaseID || plan.ServiceID != target.ServiceID ||
		!stringSlicesEqual(plan.LocalOnlyTables, target.LocalOnlyTables) {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("supplied plan target differs from requested ready target")}
	}
	return nil
}

func (c *Opening) loadCoordinatorContractState(ctx context.Context) error {
	plan := c.config.Plan
	targetConfig, err := normalizeUpgradeClientConfig(plan, c.config.TargetClientConfig, plan.TargetGenerationID, plan.TargetVersion, "target")
	if err != nil {
		return err
	}
	c.config.TargetClientConfig = targetConfig
	if err := validateUpgradeLocalOnlySelection(plan.LocalOnlyTables, targetConfig.Tables); err != nil {
		return err
	}
	c.targetContract = LocalGenerationContract{
		Proof:  oversync.SchemaGenerationProof{ID: plan.TargetGenerationID},
		Tables: cloneSyncTables(targetConfig.Tables),
	}
	if plan.Mode == UpgradeModeFresh {
		return c.refreshPlanDigest()
	}

	oldConfig, err := installedUpgradeClientConfig(ctx, c.db, plan, c.config.OldClientConfig, targetConfig)
	if err != nil {
		return err
	}
	c.config.OldClientConfig = oldConfig
	journalExists, err := sqliteNamedTableExists(ctx, c.db, "_sync_upgrade_journal")
	if err != nil {
		return err
	}
	var record *upgradeJournalRecord
	if journalExists {
		record, err = loadUpgradeJournal(ctx, c.db)
		if err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
		}
		if record != nil && record.PlanID != plan.PlanID {
			record = nil
		}
	}
	postMigration := record != nil && record.MigrationComplete &&
		(record.Phase == UpgradePhaseLocalMigrated || record.Phase == UpgradePhaseReattachPending ||
			record.Phase == UpgradePhaseRebuildRunning || record.Phase == UpgradePhaseReady)
	if postMigration {
		c.installedContract = LocalGenerationContract{
			Proof:  upgradeProof(record.OldGenerationID, record.OldGenerationFingerprint),
			Tables: cloneSyncTables(oldConfig.Tables),
		}
		if c.installedContract.Proof.Fingerprint != "" {
			_, oldResponse, err := loadAdmittedGenerationDescriptor(ctx, c.db, plan.InstalledGenerationID)
			if err != nil {
				return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
			}
			c.installedContract = localContractFromDescriptor(plan.InstalledGenerationID, oldResponse.SchemaGeneration)
			c.installedContract.Tables = cloneSyncTables(oldConfig.Tables)
			if plan.Mode == UpgradeModeLocalOnly {
				c.installedContract, err = deriveLocalContract(ctx, c.db, plan.InstalledGenerationID, oldConfig.Tables, oldResponse.SchemaGeneration)
				if err != nil {
					return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
				}
			}
		}
	} else {
		loadedConfig, installed, err := loadInstalledOrdinaryContract(ctx, c.db, plan, oldConfig, targetConfig)
		if err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
		c.config.OldClientConfig = loadedConfig
		if installed == nil {
			if plan.Mode == UpgradeModeGeneration {
				return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: fmt.Errorf("installed generation is not backed by an admitted descriptor")}
			}
			c.installedContract = LocalGenerationContract{
				Proof:  oversync.SchemaGenerationProof{ID: plan.InstalledGenerationID},
				Tables: cloneSyncTables(oldConfig.Tables),
			}
		} else {
			c.installedContract = cloneLocalGenerationContract(*installed)
		}
	}
	if plan.Mode == UpgradeModeLocalOnly && !syncTablesEqual(oldConfig.Tables, targetConfig.Tables) {
		return fmt.Errorf("local-only migration must preserve the ordinary managed table selection")
	}
	if plan.Mode == UpgradeModeLocalOnly {
		c.targetContract = cloneLocalGenerationContract(c.installedContract)
		if c.targetContract.Proof.Fingerprint != "" {
			descriptor, _, err := loadAdmittedGenerationDescriptor(ctx, c.db, plan.TargetGenerationID)
			if err != nil {
				return err
			}
			c.targetDescriptor = descriptor
		}
	}
	if record != nil && record.TargetGenerationFingerprint != "" {
		descriptor, response, err := loadAdmittedGenerationDescriptor(ctx, c.db, plan.TargetGenerationID)
		if err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
		}
		if response.SchemaGeneration.Fingerprint != record.TargetGenerationFingerprint {
			return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("durable target descriptor differs from the upgrade journal")}
		}
		c.targetDescriptor = descriptor
		c.targetContract = localContractFromDescriptor(plan.TargetGenerationID, response.SchemaGeneration)
		c.targetContract.Tables = cloneSyncTables(targetConfig.Tables)
		if plan.Mode == UpgradeModeLocalOnly || record.Phase == UpgradePhaseLocalMigrated || record.Phase == UpgradePhaseReattachPending ||
			record.Phase == UpgradePhaseRebuildRunning || record.Phase == UpgradePhaseReady {
			derived, err := deriveLocalContract(ctx, c.db, plan.TargetGenerationID, targetConfig.Tables, response.SchemaGeneration)
			if err != nil {
				return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
			}
			c.targetContract = derived
		}
	}
	return c.refreshPlanDigest()
}

func validateUpgradeLocalOnlySelection(localOnly []string, managed []SyncTable) error {
	managedNames := make(map[string]struct{}, len(managed))
	for _, table := range managed {
		managedNames[table.TableName] = struct{}{}
	}
	for _, table := range localOnly {
		if _, overlaps := managedNames[table]; overlaps {
			return fmt.Errorf("local-only table %q is also a synchronized table", table)
		}
	}
	return nil
}

func (c *Opening) refreshPlanDigest() error {
	var oldProof, targetProof *oversync.SchemaGenerationProof
	var oldTables []SyncTable
	if c.installedContract.Proof.Fingerprint != "" {
		proof := c.installedContract.Proof
		oldProof = &proof
	}
	if c.targetContract.Proof.Fingerprint != "" {
		proof := c.targetContract.Proof
		targetProof = &proof
	}
	if c.config.OldClientConfig != nil {
		oldTables = c.config.OldClientConfig.Tables
	}
	digest, err := digestUpgradePlan(
		c.config.Plan,
		oldProof,
		targetProof,
		oldTables,
		c.config.TargetClientConfig.Tables,
	)
	if err != nil {
		return err
	}
	c.planDigest = digest
	return nil
}

func syncTablesEqual(left, right []SyncTable) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftKeys, leftErr := normalizedSyncKeyColumns(left[index])
		rightKeys, rightErr := normalizedSyncKeyColumns(right[index])
		if leftErr != nil || rightErr != nil || left[index].TableName != right[index].TableName || !stringSlicesEqual(leftKeys, rightKeys) {
			return false
		}
	}
	return true
}

// Close requests cancellation of active work or closes the pre-ready pool.
func (c *Opening) Close() error {
	if c == nil || c.closed.Swap(true) {
		return nil
	}
	c.stateMu.Lock()
	if c.consumed {
		c.stateMu.Unlock()
		return nil
	}
	if c.activeCancel != nil {
		c.activeCancel()
	}
	if !c.activeMutation {
		_ = c.releaseResourcesLocked()
	}
	c.stateMu.Unlock()
	return nil
}

func (c *Opening) releaseResourcesLocked() error {
	if c.ownershipReleased {
		return nil
	}
	var closeErr error
	if !c.testBorrowedDB {
		closeErr = c.db.Close()
	}
	releaseClientDBOwnership(c.identityKey)
	c.identityKey = ""
	c.ownershipReleased = true
	return closeErr
}

func (c *Opening) beginMutation(ctx context.Context) (context.Context, func() error, error) {
	if c == nil {
		return nil, nil, &ClientClosedError{}
	}
	select {
	case <-ctx.Done():
		return nil, nil, &UpgradeError{Outcome: UpgradeOutcomeCancelled, Cause: ctx.Err()}
	case <-c.mutator:
	default:
		return nil, nil, &UpgradeError{Outcome: UpgradeOutcomeBusy, Cause: fmt.Errorf("another opening mutation is active")}
	}
	c.stateMu.Lock()
	if c.closed.Load() || c.consumed {
		c.stateMu.Unlock()
		c.mutator <- struct{}{}
		return nil, nil, &ClientClosedError{}
	}
	operationCtx, cancel := context.WithCancel(ctx)
	c.activeMutation = true
	c.activeCancel = cancel
	c.stateMu.Unlock()
	finish := func() error {
		cancel()
		c.stateMu.Lock()
		c.activeMutation = false
		c.activeCancel = nil
		var cleanupErr error
		if c.closed.Load() && !c.consumed {
			cleanupErr = c.releaseResourcesLocked()
		}
		c.stateMu.Unlock()
		c.mutator <- struct{}{}
		return cleanupErr
	}
	return operationCtx, finish, nil
}

func (c *Opening) inspectConstructionIdentity(ctx context.Context) error {
	journalExists, err := sqliteNamedTableExists(ctx, c.db, "_sync_upgrade_journal")
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if !journalExists {
		if c.config.Plan.Mode != UpgradeModeFresh {
			return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("installed upgrade identity is missing")}
		}
		return nil
	}
	journal, err := loadUpgradeJournal(ctx, c.db)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if journal != nil && journal.Phase != UpgradePhaseReady {
		if journal.PlanID != c.config.Plan.PlanID || journal.PlanDigest != c.planDigest || journal.Mode != c.config.Plan.Mode {
			return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("durable upgrade plan differs from supplied plan")}
		}
		if err := c.validateJournalMatchesPlan(journal); err != nil {
			return err
		}
	}
	identity, err := loadUpgradeIdentity(ctx, c.db)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if journal == nil && identity == nil && c.config.Plan.Mode != UpgradeModeFresh {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("installed upgrade identity is missing")}
	}
	if identity != nil && (identity.DatabaseID != c.config.Plan.DatabaseID || identity.ServiceID != c.config.Plan.ServiceID) {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("database or service identity differs")}
	}
	if journal != nil && journal.Phase != UpgradePhaseReady {
		if err := c.validateResumeLocalState(ctx, journal, identity); err != nil {
			return err
		}
	} else if journal != nil && journal.Phase == UpgradePhaseReady && journal.PlanID == c.config.Plan.PlanID && journal.PlanDigest == c.planDigest && journal.Mode == c.config.Plan.Mode {
		if err := c.validateJournalMatchesPlan(journal); err != nil {
			return err
		}
		if err := c.validateCompletedPlanState(ctx, journal, identity); err != nil {
			return err
		}
	}
	return nil
}

func (c *Opening) validateJournalMatchesPlan(journal *upgradeJournalRecord) error {
	if c == nil || journal == nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("upgrade journal or coordinator is missing")}
	}
	plan := c.config.Plan
	completedAnonymousFresh := journal.Phase == UpgradePhaseReady && plan.Mode == UpgradeModeFresh &&
		plan.UserID == "" && journal.TargetGenerationFingerprint == ""
	userMatches := journal.UserID == plan.UserID || completedAnonymousFresh
	targetFingerprintMatches := journal.TargetGenerationFingerprint == c.targetContract.Proof.Fingerprint ||
		(completedAnonymousFresh && (journal.TargetGenerationFingerprint == "" || validSchemaGenerationFingerprint(journal.TargetGenerationFingerprint)))
	oldGenerationMatches := journal.OldGenerationID == int64(c.installedContract.Proof.ID) &&
		journal.OldGenerationFingerprint == c.installedContract.Proof.Fingerprint
	if completedAnonymousFresh {
		// Fresh plans have no installed generation. Ordinary admission after U8 may populate the
		// coordinator's in-memory installed contract on reopen, but it cannot retroactively change
		// the journal's intentionally empty old-generation edge.
		oldGenerationMatches = journal.OldGenerationID == 0 && journal.OldGenerationFingerprint == ""
	}
	if journal.PlanID != plan.PlanID || journal.PlanDigest != c.planDigest || journal.Mode != plan.Mode ||
		journal.DatabaseID != plan.DatabaseID || journal.ServiceID != plan.ServiceID || !userMatches ||
		!oldGenerationMatches ||
		journal.OldVersion != plan.InstalledVersion || journal.TargetGenerationID != int64(plan.TargetGenerationID) ||
		!targetFingerprintMatches || journal.TargetVersion != plan.TargetVersion {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf(
			"durable upgrade journal contradicts its immutable plan (plan=%q/%q digest=%q/%q mode=%q/%q database=%q/%q service=%q/%q phase=%q user=%q old=%d/%q target=%d/%q version=%d)",
			journal.PlanID, plan.PlanID, journal.PlanDigest, c.planDigest, journal.Mode, plan.Mode,
			journal.DatabaseID, plan.DatabaseID, journal.ServiceID, plan.ServiceID,
			journal.Phase, journal.UserID,
			journal.OldGenerationID, journal.OldGenerationFingerprint,
			journal.TargetGenerationID, journal.TargetGenerationFingerprint, journal.TargetVersion,
		)}
	}
	return nil
}

func (c *Opening) validateCompletedPlanState(ctx context.Context, journal *upgradeJournalRecord, identity *upgradeIdentityRecord) error {
	plan := c.config.Plan
	userMatches, userErr := c.completedPlanUserMatches(ctx, journal, identity)
	identityFingerprintMatches := identity != nil && identity.GenerationFingerprint == c.targetContract.Proof.Fingerprint
	anonymousFresh := plan.Mode == UpgradeModeFresh && journal != nil &&
		journal.TargetGenerationFingerprint == "" && identity != nil
	if anonymousFresh {
		identityFingerprintMatches = identity.GenerationFingerprint == "" || validSchemaGenerationFingerprint(identity.GenerationFingerprint)
	}
	validatedFingerprintMatches := journal != nil && journal.ValidatedTargetFingerprint == c.targetContract.Proof.Fingerprint
	if anonymousFresh {
		validatedFingerprintMatches = journal.ValidatedTargetFingerprint == "" ||
			journal.ValidatedTargetFingerprint == identity.GenerationFingerprint
	}
	if journal == nil || identity == nil || journal.Phase != UpgradePhaseReady || !journal.MigrationComplete ||
		!validatedFingerprintMatches || journal.TargetVersion != plan.TargetVersion ||
		!identity.Ready || identity.DatabaseID != plan.DatabaseID || identity.ServiceID != plan.ServiceID || !userMatches ||
		identity.GenerationID != int64(plan.TargetGenerationID) || !identityFingerprintMatches || identity.LocalVersion != plan.TargetVersion {
		if userErr != nil {
			return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: userErr}
		}
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf(
			"completed upgrade witness does not match the directed target (phase=%q migration=%t validated=%q expected=%q identity_ready=%t identity=%q/%q/%d/%q/%d user_matches=%t)",
			journal.Phase, journal.MigrationComplete, journal.ValidatedTargetFingerprint,
			c.targetContract.Proof.Fingerprint, identity.Ready, identity.DatabaseID, identity.ServiceID, identity.GenerationID,
			identity.GenerationFingerprint, identity.LocalVersion, userMatches,
		)}
	}
	if plan.Mode == UpgradeModeGeneration && (!journal.DrainComplete || !journal.ReplacementConfirmed || journal.NewSourceID == "" ||
		journal.SnapshotRowsStaged != journal.SnapshotRowCount || journal.SnapshotBytesStaged != journal.SnapshotByteCount) {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("completed generation upgrade is missing durable replacement evidence")}
	}
	observedVersion, err := readInstalledVersionRestricted(ctx, c.db, plan.Migrator)
	if err != nil || observedVersion != plan.TargetVersion {
		if err == nil {
			err = fmt.Errorf("application version %d does not match completed target version %d", observedVersion, plan.TargetVersion)
		}
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	if c.targetContract.Proof.Fingerprint != "" {
		if err := validatePhysicalLocalContract(ctx, c.db, c.targetContract, plan.LocalOnlyTables); err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
	} else {
		retained, err := loadRetainedUpgradeContractIdentity(ctx, c.db)
		if err != nil || retained == nil {
			if err == nil {
				err = fmt.Errorf("ordinary retained target identity is missing")
			}
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
		shape, err := managedPhysicalShape(ctx, c.db, c.config.TargetClientConfig.Tables)
		if err != nil || retained.databaseID != plan.DatabaseID || retained.serviceID != plan.ServiceID ||
			retained.generation != int64(plan.TargetGenerationID) || retained.localVersion != plan.TargetVersion ||
			retained.physicalShape != shape {
			if err == nil {
				err = fmt.Errorf("ordinary retained target identity or physical shape changed")
			}
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
		if retained.fingerprint != "" {
			descriptor, response, loadErr := loadAdmittedGenerationDescriptor(ctx, c.db, plan.TargetGenerationID)
			if loadErr != nil || response.SchemaGeneration.Fingerprint != retained.fingerprint {
				if loadErr == nil {
					loadErr = fmt.Errorf("ordinary retained target authority changed")
				}
				return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: loadErr}
			}
			derived, deriveErr := deriveLocalContract(
				ctx, c.db, plan.TargetGenerationID, c.config.TargetClientConfig.Tables, response.SchemaGeneration,
			)
			if deriveErr != nil {
				return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: deriveErr}
			}
			c.targetDescriptor = descriptor
			c.targetContract = derived
		}
	}
	return nil
}

func (c *Opening) completedPlanUserMatches(ctx context.Context, journal *upgradeJournalRecord, identity *upgradeIdentityRecord) (bool, error) {
	if journal == nil || identity == nil {
		return false, nil
	}
	if identity.UserID == c.config.Plan.UserID {
		return true, nil
	}
	if c.config.Plan.UserID != "" || journal.UserID != "" ||
		(c.config.Plan.Mode != UpgradeModeFresh && c.config.Plan.Mode != UpgradeModeLocalOnly) || identity.UserID == "" {
		return false, nil
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return false, err
	}
	if attachment.BindingState != attachmentBindingAttached || attachment.AttachedUserID != identity.UserID || attachment.CurrentSourceID == "" {
		return false, fmt.Errorf("completed anonymous plan has inconsistent later attachment identity")
	}
	return true, nil
}

func (c *Opening) validateResumeLocalState(ctx context.Context, journal *upgradeJournalRecord, identity *upgradeIdentityRecord) error {
	if journal == nil || identity == nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("pending upgrade is missing its installed identity")}
	}
	plan := c.config.Plan
	if identity.DatabaseID != plan.DatabaseID || identity.ServiceID != plan.ServiceID || identity.UserID != plan.UserID || identity.Ready {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("pending installed identity differs from the directed plan")}
	}
	expectedProof := c.installedContract.Proof
	expectedVersion := plan.InstalledVersion
	contract := &c.installedContract
	if plan.Mode == UpgradeModeFresh {
		expectedProof = c.targetContract.Proof
		expectedVersion = 0
		contract = nil
	} else if journal.Phase == UpgradePhaseLocalMigrated || journal.Phase == UpgradePhaseReattachPending || journal.Phase == UpgradePhaseRebuildRunning {
		expectedVersion = plan.TargetVersion
		contract = &c.targetContract
	}
	if identity.GenerationID != int64(expectedProof.ID) || identity.GenerationFingerprint != expectedProof.Fingerprint || identity.LocalVersion != expectedVersion {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("pending installed proof or local version differs from the durable phase")}
	}
	observedVersion, err := readInstalledVersionRestricted(ctx, c.db, plan.Migrator)
	if err != nil || observedVersion != expectedVersion {
		if err == nil {
			err = fmt.Errorf("application version %d does not match durable phase version %d", observedVersion, expectedVersion)
		}
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	if contract != nil {
		if contract.Proof.Fingerprint != "" {
			if err := validatePhysicalLocalContract(ctx, c.db, *contract, plan.LocalOnlyTables); err != nil {
				return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
			}
		} else if _, err := managedPhysicalShape(ctx, c.db, contract.Tables); err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
	}
	return nil
}

// Status returns a semantic snapshot of the durable upgrade state.
func (c *Opening) Status(ctx context.Context) (OpeningStatus, error) {
	if c == nil {
		return OpeningStatus{}, &ClientClosedError{}
	}
	c.stateMu.Lock()
	closed := c.closed.Load() || c.consumed
	c.stateMu.Unlock()
	if closed {
		return OpeningStatus{}, &ClientClosedError{}
	}
	exists, err := sqliteNamedTableExists(ctx, c.db, "_sync_upgrade_journal")
	if err != nil {
		return OpeningStatus{}, err
	}
	if !exists {
		return c.preStartStatus(), nil
	}
	rec, err := loadUpgradeJournal(ctx, c.db)
	if err != nil {
		return OpeningStatus{}, err
	}
	if rec == nil {
		return c.preStartStatus(), nil
	}
	c.reachTestBoundary("before_status_counts")
	return c.statusFromRecord(ctx, rec)
}

func (c *Opening) preStartStatus() OpeningStatus {
	status := OpeningStatus{NextAction: "advance"}
	if c != nil && c.config.PlanProvided {
		status.Mode = c.config.Plan.Mode
		status.PlanID = c.config.Plan.PlanID
		status.PlanDigest = c.planDigest
		status.InstalledVersion = c.config.Plan.InstalledVersion
		status.TargetVersion = c.config.Plan.TargetVersion
	}
	return status
}

func (c *Opening) statusFromRecord(ctx context.Context, rec *upgradeJournalRecord) (OpeningStatus, error) {
	return c.statusFromRecordUsing(ctx, c.db, rec)
}

func (c *Opening) statusFromRecordUsing(ctx context.Context, q queryRower, rec *upgradeJournalRecord) (OpeningStatus, error) {
	dirty, err := countRowsIfTableExists(ctx, q, "_sync_dirty_rows")
	if err != nil {
		return OpeningStatus{}, fmt.Errorf("count pending dirty rows: %w", err)
	}
	outbox, err := countRowsIfTableExists(ctx, q, "_sync_outbox_rows")
	if err != nil {
		return OpeningStatus{}, fmt.Errorf("count pending outbox rows: %w", err)
	}
	staged, err := countRowsForSnapshot(ctx, q, rec.SnapshotID)
	if err != nil {
		return OpeningStatus{}, fmt.Errorf("count staged snapshot rows: %w", err)
	}
	status := OpeningStatus{
		Mode: rec.Mode, Phase: rec.Phase, Revision: rec.Revision, PlanID: rec.PlanID, PlanDigest: rec.PlanDigest,
		OldProof:         upgradeProof(rec.OldGenerationID, rec.OldGenerationFingerprint),
		TargetProof:      upgradeProof(rec.TargetGenerationID, rec.TargetGenerationFingerprint),
		InstalledVersion: rec.OldVersion, TargetVersion: rec.TargetVersion,
		PendingDirtyRows: dirty, PendingOutboxRows: outbox, DrainComplete: rec.DrainComplete,
		LocalMigrationComplete: rec.MigrationComplete, OldSourceID: rec.OldSourceID, NewSourceID: rec.NewSourceID,
		ReplacementConfirmed: rec.ReplacementConfirmed, SnapshotID: rec.SnapshotID,
		SnapshotRowCount: rec.SnapshotRowCount, SnapshotRowsStaged: staged, SnapshotBundleSeq: rec.SnapshotBundleSeq,
		Outcome:            rec.Outcome,
		PendingRequestHash: rec.PendingRequestHash,
	}
	switch status.Outcome {
	case UpgradeOutcomeNeedsResolution:
		status.NextAction = "resolve_pending_intent"
	case UpgradeOutcomeReady:
		status.NextAction = ""
	default:
		status.NextAction = "advance"
	}
	return status, nil
}

// Advance starts or resumes the durable plan and continues until ready or until a
// stable application-visible outcome requires a later retry or decision.
func (c *Opening) Advance(ctx context.Context) (result OpeningResult, resultErr error) {
	operationCtx, finish, err := c.beginMutation(ctx)
	if err != nil {
		return OpeningResult{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, finish()) }()
	return c.runOwned(operationCtx)
}

func (c *Opening) runOwned(ctx context.Context) (OpeningResult, error) {
	if c.readyAdmission {
		rec, err := loadUpgradeJournal(context.Background(), c.db)
		if err != nil {
			return OpeningResult{}, err
		}
		return c.publishReadyClient(context.Background(), rec)
	}
	for {
		if err := ctx.Err(); err != nil {
			if ready, readyErr := c.readyResultIfCommitted(context.Background()); readyErr == nil && ready.Ready != nil {
				return ready, nil
			}
			return c.resultForError(context.Background(), nil, err, false)
		}
		rec, err := c.loadOrStart(ctx)
		if err != nil {
			return c.resultForError(ctx, nil, err, false)
		}
		if rec.Phase == UpgradePhaseReady {
			return c.publishReadyClient(ctx, rec)
		}
		retryRemote, err := c.advancePendingPhase(ctx, rec)
		if err != nil {
			return c.resultForError(ctx, rec, err, retryRemote)
		}
		if rec.Phase == UpgradePhaseReady {
			result, publishErr := c.publishPreparedReady(rec)
			if publishErr == nil && result.Ready != nil && rec.Mode == UpgradeModeGeneration {
				result.Ready.deleteSnapshotSessionBestEffortAsSource(rec.SnapshotID, rec.NewSourceID)
			}
			return result, publishErr
		}
	}
}

func (c *Opening) advancePendingPhase(ctx context.Context, rec *upgradeJournalRecord) (bool, error) {
	if rec.Mode != UpgradeModeGeneration {
		if rec.Mode != UpgradeModeFresh && rec.Mode != UpgradeModeLocalOnly {
			return false, fmt.Errorf("unknown durable upgrade mode %q", rec.Mode)
		}
		if rec.Phase != UpgradePhaseMigratingLocal {
			return false, fmt.Errorf("local mode has invalid phase %q", rec.Phase)
		}
		return false, c.runLocalMigration(ctx, rec)
	}
	switch rec.Phase {
	case UpgradePhaseStarted:
		return false, c.transitionPhase(ctx, rec, UpgradePhaseResolvingOldIntent)
	case UpgradePhaseResolvingOldIntent:
		return false, c.resolveOldIntent(ctx, rec)
	case UpgradePhaseMigratingLocal:
		return false, c.runLocalMigration(ctx, rec)
	case UpgradePhaseLocalMigrated:
		return true, c.prepareReplacement(ctx, rec)
	case UpgradePhaseReattachPending:
		return true, c.confirmReplacement(ctx, rec)
	case UpgradePhaseRebuildRunning:
		return true, c.resumeTargetSnapshot(ctx, rec)
	default:
		return false, fmt.Errorf("unknown durable upgrade phase %q", rec.Phase)
	}
}

func (c *Opening) readyResultIfCommitted(ctx context.Context) (OpeningResult, error) {
	rec, err := loadUpgradeJournal(ctx, c.db)
	if err != nil || rec == nil || rec.Phase != UpgradePhaseReady || rec.Outcome != UpgradeOutcomeReady {
		return OpeningResult{}, err
	}
	return c.publishReadyClient(context.Background(), rec)
}

func (c *Opening) publishReadyClient(ctx context.Context, rec *upgradeJournalRecord) (OpeningResult, error) {
	if rec == nil || rec.Phase != UpgradePhaseReady || rec.Outcome != UpgradeOutcomeReady {
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("ready publication requires a durable ready journal")}
	}
	client := c.preparedClient
	preparedStatus := c.preparedStatus
	if client == nil {
		var err error
		client, preparedStatus, err = c.prepareReadyClient(ctx, rec)
		if err != nil {
			return OpeningResult{}, &UpgradeError{Outcome: classifyUpgradeCause(err, true), Cause: err}
		}
		c.preparedClient = client
		c.preparedStatus = preparedStatus
	}
	return c.publishPreparedReady(rec)
}

func (c *Opening) publishPreparedReady(rec *upgradeJournalRecord) (OpeningResult, error) {
	if rec == nil || rec.Phase != UpgradePhaseReady || rec.Outcome != UpgradeOutcomeReady {
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("ready publication requires a durable ready journal")}
	}
	client := c.preparedClient
	preparedStatus := c.preparedStatus
	if client == nil {
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("ready client was not prepared before publication")}
	}
	if preparedStatus == nil {
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("ready status was not prepared before publication")}
	}
	status := *preparedStatus
	status.Outcome = UpgradeOutcomeReady
	c.reachTestBoundary("before_ready_transfer")
	c.stateMu.Lock()
	if c.consumed {
		c.stateMu.Unlock()
		return OpeningResult{}, &ClientClosedError{}
	}
	if c.testBorrowedDB {
		releaseClientDBOwnership(c.identityKey)
		client.ownsDB = false
	} else {
		if err := transferOpeningDBOwnership(c.identityKey, c.db); err != nil {
			c.stateMu.Unlock()
			return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
		}
		client.dbOwnershipKey = c.identityKey
	}
	c.identityKey = ""
	c.ownershipReleased = true
	c.consumed = true
	c.closed.Store(true)
	c.stateMu.Unlock()
	c.reachTestBoundary("after_ready_transfer")
	return OpeningResult{Status: status, Ready: client}, nil
}

func (c *Opening) prepareReadyClient(ctx context.Context, rec *upgradeJournalRecord) (*Client, *OpeningStatus, error) {
	if c.testBorrowedDB {
		client, err := c.buildReadyClient(c.db)
		if err != nil {
			return nil, nil, err
		}
		status, err := c.statusFromRecordUsing(ctx, c.db, rec)
		return client, &status, err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	client, err := c.prepareReadyClientInTx(ctx, tx)
	if err != nil {
		return nil, nil, err
	}
	status, err := c.statusFromRecordUsing(ctx, tx, rec)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit ready-client preparation: %w", err)
	}
	return client, &status, nil
}

func (c *Opening) prepareReadyClientInTx(ctx context.Context, tx sqliteTransaction) (*Client, error) {
	client, err := c.buildReadyClient(tx)
	if err != nil {
		return nil, err
	}
	if c.testBorrowedDB {
		return client, nil
	}
	triggerDB, ok := tx.(triggerSQLDB)
	if !ok {
		return nil, fmt.Errorf("ready-client transaction cannot manage capture triggers")
	}
	validated, err := validateSyncTables(tx, client.tableInfo, client.config.Tables)
	if err != nil {
		return nil, err
	}
	if err := installClientRuntime(ctx, triggerDB, client, validated); err != nil {
		return nil, err
	}
	state, err := client.restoreLifecycleStateInTx(ctx, tx)
	var pending *RemoteReplacePendingError
	if err != nil && !errors.As(err, &pending) {
		return nil, err
	}
	client.applyOpenedLifecycleState(state)
	return client, nil
}

func (c *Opening) buildReadyClient(q tableInfoQueryer) (*Client, error) {
	var client *Client
	if c.targetContract.Proof.Fingerprint != "" {
		prepared, err := c.newPhysicalClientUsing(c.targetContract, c.config.TargetClientConfig, q)
		if err != nil {
			return nil, err
		}
		client = prepared
		client.ownsDB = true
		if c.targetDescriptor != "" {
			discovery, err := decodeDiscoveredContract([]byte(c.targetDescriptor), c.targetContract.Proof.ID)
			if err != nil {
				return nil, err
			}
			client.discovery = discovery
			client.config.discovery = discovery
		}
	} else {
		config, err := prepareClientConfiguration(c.config.TargetClientConfig)
		if err != nil {
			return nil, err
		}
		config.schema = unconfirmedLocalSchema
		config.generationProof = oversync.SchemaGenerationProof{ID: config.GenerationID}
		provider := NewTableInfoProvider()
		validated, err := validateSyncTables(q, provider, config.Tables)
		if err != nil {
			return nil, err
		}
		client = assembleClientRuntime(c.db, c.config.BaseURL, c.config.Token, config, provider, validated, "", nil)
	}
	client.HTTP = c.config.HTTP
	if client.HTTP == nil {
		client.HTTP = &http.Client{Timeout: 120 * time.Second}
	}
	return client, nil
}

func (c *Opening) loadOrStart(ctx context.Context) (*upgradeJournalRecord, error) {
	exists, err := sqliteNamedTableExists(ctx, c.db, "_sync_upgrade_journal")
	if err != nil {
		return nil, err
	}
	if exists {
		rec, err := loadUpgradeJournal(ctx, c.db)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			if rec.PlanID == c.config.Plan.PlanID && rec.PlanDigest == c.planDigest && rec.Mode == c.config.Plan.Mode {
				if err := c.validateJournalMatchesPlan(rec); err != nil {
					return nil, err
				}
				if rec.Phase == UpgradePhaseReady {
					identity, loadErr := loadUpgradeIdentity(ctx, c.db)
					if loadErr != nil {
						return nil, loadErr
					}
					if err := c.validateCompletedPlanState(ctx, rec, identity); err != nil {
						return nil, err
					}
				}
				return rec, nil
			}
			if rec.Phase != UpgradePhaseReady {
				return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("durable plan mismatch")}
			}
		}
	}
	if c.config.Plan.Mode == UpgradeModeFresh {
		return c.startFresh(ctx)
	}
	return c.startInstalled(ctx)
}

func (c *Opening) startFresh(ctx context.Context) (*upgradeJournalRecord, error) {
	if err := rejectNonemptyFreshDatabase(ctx, c.db); err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomePreconditionFailed, Cause: err}
	}
	if err := configureSQLiteConnection(c.db); err != nil {
		return nil, err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := initializeDatabaseInTx(ctx, tx); err != nil {
		return nil, err
	}
	rec := c.newJournalRecord(UpgradePhaseMigratingLocal)
	if err := persistUpgradeJournal(ctx, tx, rec); err != nil {
		return nil, err
	}
	if err := persistUpgradeIdentity(ctx, tx, upgradeIdentityRecord{
		DatabaseID: c.config.Plan.DatabaseID, ServiceID: c.config.Plan.ServiceID,
		GenerationID: int64(c.config.Plan.TargetGenerationID), GenerationFingerprint: c.targetContract.Proof.Fingerprint,
		LocalVersion: 0, Ready: false,
	}); err != nil {
		return nil, err
	}
	c.reachTestBoundary("before_fresh_bootstrap_commit")
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	c.reachTestBoundary("after_fresh_bootstrap_commit")
	if err := c.notifyProgress(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (c *Opening) startInstalled(ctx context.Context) (*upgradeJournalRecord, error) {
	identity, err := loadUpgradeIdentity(ctx, c.db)
	if err != nil {
		return nil, err
	}
	plan := c.config.Plan
	if identity == nil || !identity.Ready || identity.DatabaseID != plan.DatabaseID || identity.ServiceID != plan.ServiceID ||
		identity.UserID != plan.UserID || identity.GenerationID != int64(c.installedContract.Proof.ID) ||
		identity.GenerationFingerprint != c.installedContract.Proof.Fingerprint || identity.LocalVersion != plan.InstalledVersion {
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("installed identity does not match directed plan")}
	}
	observedVersion, err := readInstalledVersionRestricted(ctx, c.db, plan.Migrator)
	if err != nil || observedVersion != plan.InstalledVersion {
		if err == nil {
			err = fmt.Errorf("application version %d does not match installed version %d", observedVersion, plan.InstalledVersion)
		}
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	if c.installedContract.Proof.Fingerprint != "" {
		if err := validatePhysicalLocalContract(ctx, c.db, c.installedContract, plan.LocalOnlyTables); err != nil {
			return nil, &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
	} else if _, err := managedPhysicalShape(ctx, c.db, c.config.OldClientConfig.Tables); err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	_, admittedInstalled, err := loadInstalledOrdinaryContract(ctx, c.db, plan, c.config.OldClientConfig, c.config.TargetClientConfig)
	if err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	if plan.Mode == UpgradeModeGeneration && (admittedInstalled == nil || admittedInstalled.Proof != c.installedContract.Proof) {
		return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("installed ordinary contract is not admitted for the directed generation")}
	}
	var expectedAttachment *attachmentStateRecord
	var expectedOperation *operationStateRecord
	if plan.Mode == UpgradeModeGeneration {
		if err := c.previewTarget(ctx); err != nil {
			return nil, err
		}
		installedDeclared, _, err := compileDeclaredGeneration(c.installedContract)
		if err != nil {
			return nil, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
		}
		targetDeclared, _, err := compileDeclaredGeneration(c.targetContract)
		if err != nil {
			return nil, &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
		}
		if err := validateGenerationEvolution(installedDeclared, targetDeclared); err != nil {
			return nil, &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
		}
		attachment, err := loadAttachmentState(ctx, c.db)
		if err != nil {
			return nil, err
		}
		operation, err := loadOperationState(ctx, c.db)
		if err != nil {
			return nil, err
		}
		if attachment.BindingState != attachmentBindingAttached || attachment.AttachedUserID != plan.UserID ||
			attachment.CurrentSourceID == "" || attachment.PendingInitializationID != "" || operation.Kind == operationKindRemoteReplace {
			return nil, &UpgradeError{Outcome: UpgradeOutcomePreconditionFailed, Cause: fmt.Errorf("installed attach or initialization lifecycle is incomplete")}
		}
		expectedAttachment = attachment
		expectedOperation = operation
	}

	c.reachTestBoundary("before_U0_recheck")
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := c.recheckInstalledAdmissionAtU0(ctx, tx, identity, expectedAttachment, expectedOperation); err != nil {
		return nil, err
	}
	if err := installLocalOnlyUpgradeGuards(ctx, tx, plan.LocalOnlyTables); err != nil {
		return nil, err
	}
	if plan.Mode == UpgradeModeGeneration {
		if err := retainDiscoveredGenerationDescriptor(ctx, tx, c.targetDescriptor, c.targetContract.Proof); err != nil {
			return nil, err
		}
	}
	phase := UpgradePhaseMigratingLocal
	if plan.Mode == UpgradeModeGeneration {
		phase = UpgradePhaseStarted
	}
	rec := c.newJournalRecord(phase)
	preservation, err := localOnlyDigestWithUpgradeGuards(ctx, tx, plan.LocalOnlyTables)
	if err != nil {
		return nil, &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: err}
	}
	rec.LocalOnlyDigest = preservation
	if plan.Mode == UpgradeModeGeneration {
		attachment, err := loadAttachmentState(ctx, tx)
		if err != nil {
			return nil, err
		}
		rec.OldSourceID = attachment.CurrentSourceID
	}
	if err := persistUpgradeJournal(ctx, tx, rec); err != nil {
		return nil, err
	}
	identity.Ready = false
	if err := persistUpgradeIdentity(ctx, tx, *identity); err != nil {
		return nil, err
	}
	c.reachTestBoundary("before_U0_commit")
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	c.reachTestBoundary("after_U0_commit")
	if err := c.notifyProgress(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (c *Opening) recheckInstalledAdmissionAtU0(
	ctx context.Context,
	tx *immediateTx,
	expectedIdentity *upgradeIdentityRecord,
	expectedAttachment *attachmentStateRecord,
	expectedOperation *operationStateRecord,
) error {
	plan := c.config.Plan
	identity, err := loadUpgradeIdentity(ctx, tx)
	if err != nil || identity == nil || expectedIdentity == nil || *identity != *expectedIdentity || !identity.Ready {
		return fmt.Errorf("installed identity changed before upgrade start")
	}
	state, err := inspectRetainedRuntime(ctx, tx)
	if err != nil || state != retainedRuntimeCurrent {
		if err == nil {
			err = fmt.Errorf("retained runtime is no longer complete")
		}
		return err
	}
	observedVersion, err := readInstalledVersionRestrictedInTx(ctx, tx, plan.Migrator)
	if err != nil || observedVersion != plan.InstalledVersion {
		if err == nil {
			err = fmt.Errorf("application version %d does not match installed version %d", observedVersion, plan.InstalledVersion)
		}
		return err
	}
	if c.installedContract.Proof.Fingerprint != "" {
		if err := validatePhysicalLocalContract(ctx, tx, c.installedContract, plan.LocalOnlyTables); err != nil {
			return err
		}
	} else if _, err := managedPhysicalShape(ctx, tx, c.config.OldClientConfig.Tables); err != nil {
		return err
	}
	_, admitted, err := loadInstalledOrdinaryContract(ctx, tx, plan, c.config.OldClientConfig, c.config.TargetClientConfig)
	if err != nil {
		return err
	}
	if plan.Mode == UpgradeModeGeneration && (admitted == nil || admitted.Proof != c.installedContract.Proof) {
		return fmt.Errorf("installed ordinary contract admission changed before U0")
	}
	if plan.Mode != UpgradeModeGeneration {
		return nil
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil || expectedAttachment == nil || *attachment != *expectedAttachment {
		return fmt.Errorf("attachment state changed before U0")
	}
	operation, err := loadOperationState(ctx, tx)
	if err != nil || expectedOperation == nil || *operation != *expectedOperation || operation.Kind != operationKindNone {
		return fmt.Errorf("operation state changed before U0")
	}
	response, err := decodeDiscoveredContract([]byte(c.targetDescriptor), plan.TargetGenerationID)
	if err != nil || response.SchemaGeneration.Fingerprint != c.targetContract.Proof.Fingerprint {
		return fmt.Errorf("target discovery descriptor changed before U0")
	}
	target := localContractFromDescriptor(plan.TargetGenerationID, response.SchemaGeneration)
	declared, _, err := compileDeclaredGeneration(target)
	if err != nil {
		return err
	}
	if err := validateConfiguredGenerationSelection(c.config.TargetClientConfig.Tables, declared); err != nil {
		return err
	}
	return nil
}

func (c *Opening) newJournalRecord(phase UpgradePhase) *upgradeJournalRecord {
	plan := c.config.Plan
	return &upgradeJournalRecord{
		Mode: plan.Mode, Phase: phase, Revision: 1, PlanID: plan.PlanID, PlanDigest: c.planDigest,
		DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID, UserID: plan.UserID,
		OldGenerationID: int64(c.installedContract.Proof.ID), OldGenerationFingerprint: c.installedContract.Proof.Fingerprint,
		OldVersion: plan.InstalledVersion, TargetGenerationID: int64(plan.TargetGenerationID),
		TargetGenerationFingerprint: c.targetContract.Proof.Fingerprint, TargetVersion: plan.TargetVersion,
		ReplacementReason: "schema_generation_upgrade", Outcome: "",
	}
}

func (c *Opening) transitionPhase(ctx context.Context, rec *upgradeJournalRecord, phase UpgradePhase) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Revision != rec.Revision || current.Phase != rec.Phase {
		return fmt.Errorf("upgrade journal changed during transition")
	}
	current.Phase = phase
	current.Revision++
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	boundary := "before_phase_commit"
	if rec.Phase == UpgradePhaseStarted && phase == UpgradePhaseResolvingOldIntent {
		boundary = "before_U1_commit"
	}
	c.reachTestBoundary(boundary)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if boundary == "before_U1_commit" {
		c.reachTestBoundary("after_U1_commit")
	}
	*rec = *current
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func (c *Opening) notifyProgress(ctx context.Context, rec *upgradeJournalRecord) (err error) {
	if c.config.Progress == nil || rec == nil || rec.Phase == UpgradePhaseReady {
		return nil
	}
	status, err := c.statusFromRecord(ctx, rec)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &UpgradeError{Outcome: UpgradeOutcomePreconditionFailed, Cause: fmt.Errorf("opening progress callback panicked: %v", recovered)}
		}
	}()
	c.config.Progress(status)
	return nil
}

func (c *Opening) reachTestBoundary(name string) {
	if c != nil && c.config.testBoundary != nil {
		c.config.testBoundary(name)
	}
}

func (c *Opening) resultForError(ctx context.Context, rec *upgradeJournalRecord, cause error, target bool) (OpeningResult, error) {
	outcome := classifyUpgradeCause(cause, target)
	var typed *UpgradeError
	if errors.As(cause, &typed) && !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		outcome = typed.Outcome
	}
	if rec == nil && c != nil && c.db != nil {
		if readable, loadErr := loadUpgradeJournal(context.Background(), c.db); loadErr == nil && readable != nil {
			if status, statusErr := c.statusFromRecord(context.Background(), readable); statusErr == nil {
				status.Outcome = outcome
				return OpeningResult{Status: status}, &UpgradeError{Outcome: outcome, Cause: cause}
			}
		}
	}
	if rec != nil {
		_ = c.recordOutcome(context.Background(), outcome, cause)
		fresh, loadErr := loadUpgradeJournal(context.Background(), c.db)
		if loadErr == nil && fresh != nil {
			rec = fresh
		}
		status, statusErr := c.statusFromRecord(context.Background(), rec)
		if statusErr == nil {
			status.Outcome = outcome
			return OpeningResult{Status: status}, &UpgradeError{Outcome: outcome, Cause: cause}
		}
	}
	return OpeningResult{}, &UpgradeError{Outcome: outcome, Cause: cause}
}

func (c *Opening) recordOutcome(ctx context.Context, outcome UpgradeOutcome, cause error) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rec, err := loadUpgradeJournal(ctx, tx)
	if err != nil || rec == nil {
		return err
	}
	rec.Outcome = outcome
	rec.FailureCode = string(outcome)
	if cause != nil {
		rec.FailureDetail = boundedFailureDetail(cause.Error())
	}
	if outbox, loadErr := loadOutboxBundle(ctx, tx); loadErr == nil && outbox != nil {
		rec.PendingRequestHash = outbox.CanonicalRequestHash
		if rec.ResolutionRequestHash != "" && rec.ResolutionRequestHash != outbox.CanonicalRequestHash {
			rec.ResolutionAction = ""
			rec.ResolutionRequestHash = ""
			rec.ResolutionPayload = ""
		}
	}
	rec.Revision++
	if err := persistUpgradeJournal(ctx, tx, rec); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func boundedFailureDetail(value string) string {
	const limit = 1024
	if len(value) > limit {
		return value[:limit]
	}
	return value
}

func sqliteNamedTableExists(ctx context.Context, q queryRower, name string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func sqliteNamedTableExistsNoCase(ctx context.Context, q queryRower, name string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE`, name).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func countRowsIfTableExists(ctx context.Context, q queryRower, table string) (int64, error) {
	exists, err := sqliteNamedTableExists(ctx, q, table)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	var count int64
	if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(table))).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func countRowsForSnapshot(ctx context.Context, q queryRower, snapshotID string) (int64, error) {
	if snapshotID == "" {
		return 0, nil
	}
	exists, err := sqliteNamedTableExists(ctx, q, "_sync_snapshot_stage")
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	var count int64
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id=?`, snapshotID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func rejectNonemptyFreshDatabase(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master
		WHERE type IN ('table','view','trigger')
		AND lower(substr(name, 1, 7)) <> 'sqlite_'
		AND name NOT IN ('_sync_upgrade_identity','_sync_upgrade_journal')
		LIMIT 1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		return fmt.Errorf("fresh database contains unrecognized state %q", name)
	}
	return rows.Err()
}

type localOnlyUpgradeGuard struct {
	Name string
	SQL  string
}

func localOnlyUpgradeGuards(tables []string) []localOnlyUpgradeGuard {
	guards := make([]localOnlyUpgradeGuard, 0, len(tables)*3)
	for _, table := range tables {
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			name := fmt.Sprintf("_sync_upgrade_guard_%s_%s", table, strings.ToLower(operation))
			statement := fmt.Sprintf(
				`CREATE TRIGGER %s BEFORE %s ON %s WHEN COALESCE((SELECT phase FROM _sync_upgrade_journal WHERE singleton_key=1), 'ready') != 'ready' AND NOT (COALESCE((SELECT mode FROM _sync_upgrade_journal WHERE singleton_key=1), '') = 'local_only' AND COALESCE((SELECT phase FROM _sync_upgrade_journal WHERE singleton_key=1), '') = 'migrating_local' AND COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1), 0) = 1) BEGIN SELECT RAISE(ABORT, '%s'); END`,
				quoteIdent(name), operation, quoteIdent(table), transitionPendingTriggerError,
			)
			guards = append(guards, localOnlyUpgradeGuard{Name: name, SQL: statement})
		}
	}
	return guards
}

func installLocalOnlyUpgradeGuards(ctx context.Context, tx *immediateTx, tables []string) error {
	for _, table := range tables {
		exists, err := sqliteNamedTableExists(ctx, tx, table)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("declared local-only table %q is missing", table)
		}
	}
	guards := localOnlyUpgradeGuards(tables)
	for _, guard := range guards {
		var kind string
		err := tx.QueryRowContext(ctx, `SELECT type FROM sqlite_master WHERE name=?`, guard.Name).Scan(&kind)
		if err == nil {
			return fmt.Errorf("local-only upgrade guard name %q is already owned by an application object of type %s", guard.Name, kind)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	for _, guard := range guards {
		if _, err := tx.ExecContext(ctx, guard.SQL); err != nil {
			return fmt.Errorf("install local-only upgrade guard %s: %w", guard.Name, err)
		}
	}
	return nil
}

func dropLocalOnlyUpgradeGuards(ctx context.Context, tx *immediateTx, tables []string) error {
	guards := localOnlyUpgradeGuards(tables)
	if err := verifyLocalOnlyUpgradeGuards(ctx, tx, guards); err != nil {
		return err
	}
	for _, guard := range guards {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DROP TRIGGER %s`, quoteIdent(guard.Name))); err != nil {
			return err
		}
	}
	return nil
}

func verifyLocalOnlyUpgradeGuards(ctx context.Context, q upgradeQueryer, guards []localOnlyUpgradeGuard) error {
	for _, guard := range guards {
		var kind, statement string
		err := q.QueryRowContext(ctx, `SELECT type, COALESCE(sql, '') FROM sqlite_master WHERE name=?`, guard.Name).Scan(&kind, &statement)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("local-only upgrade guard %q is missing", guard.Name)
		}
		if err != nil {
			return err
		}
		if kind != "trigger" || statement != guard.SQL {
			return fmt.Errorf("local-only upgrade guard %q does not match its canonical definition", guard.Name)
		}
	}
	return nil
}

func (c *Opening) previewTarget(ctx context.Context) error {
	if c.targetDescriptor != "" {
		return c.confirmRetainedTarget(ctx)
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeOldGenerationUnavailable, Cause: err}
	}
	config := cloneCoordinatorClientConfig(c.config.TargetClientConfig)
	if config == nil {
		config = DefaultConfig(c.config.Plan.TargetGenerationID, cloneSyncTables(c.targetContract.Tables))
	}
	if config.GenerationID != c.config.Plan.TargetGenerationID {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("target client generation does not match the selected target generation")}
	}
	client := &Client{
		db: c.db, BaseURL: c.config.BaseURL, Token: c.config.Token, HTTP: c.config.HTTP,
		config: config, logger: slog.Default(), sourceID: attachment.CurrentSourceID,
		sourceIDGenerator: func() string { return uuid.NewString() },
	}
	if client.HTTP == nil {
		client.HTTP = &http.Client{Timeout: 120 * time.Second}
	}
	if client.Token == nil {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("upgrade token provider is required")}
	}
	raw, err := client.fetchDiscoveredContract(ctx)
	if err != nil {
		return &UpgradeError{Outcome: classifyUpgradeCause(err, true), Cause: err}
	}
	discovery, err := decodeDiscoveredContract(raw, c.config.Plan.TargetGenerationID)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
	}
	discovered := localContractFromDescriptor(c.config.Plan.TargetGenerationID, discovery.SchemaGeneration)
	discovered.Tables = cloneSyncTables(config.Tables)
	declared, _, err := compileDeclaredGeneration(discovered)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
	}
	if err := validateConfiguredGenerationSelection(config.Tables, declared); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
	}
	c.targetDescriptor = string(canonical)
	c.targetContract = discovered
	if err := c.confirmRetainedTarget(ctx); err != nil {
		c.targetDescriptor = ""
		return err
	}
	if err := c.refreshPlanDigest(); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: err}
	}
	return nil
}

func (c *Opening) confirmRetainedTarget(ctx context.Context) error {
	if c.targetDescriptor == "" {
		return &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: fmt.Errorf("target discovery descriptor is unavailable")}
	}
	response, err := decodeDiscoveredContract([]byte(c.targetDescriptor), c.config.Plan.TargetGenerationID)
	if err != nil || response.SchemaGeneration.Fingerprint != c.targetContract.Proof.Fingerprint {
		if err == nil {
			err = fmt.Errorf("retained target descriptor proof changed")
		}
		return &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeOldGenerationUnavailable, Cause: err}
	}
	client := &Client{
		db: c.db, BaseURL: c.config.BaseURL, Token: c.config.Token, HTTP: c.config.HTTP,
		config: cloneCoordinatorClientConfig(c.config.TargetClientConfig), logger: slog.Default(),
		sourceID: attachment.CurrentSourceID, schemaGenerationProof: c.targetContract.Proof, discovery: response,
	}
	if client.HTTP == nil {
		client.HTTP = &http.Client{Timeout: 120 * time.Second}
	}
	if client.Token == nil {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("upgrade token provider is required")}
	}
	if err := validateDiscoveryContract(client.discovery, c.targetContract); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeTargetGenerationUnavailable, Cause: err}
	}
	return nil
}

func (c *Opening) newWireClient(local LocalGenerationContract, supplied *Config) (*Client, error) {
	config, err := normalizedCoordinatorClientConfig(local, supplied)
	if err != nil {
		return nil, err
	}
	compiled, err := compileLocalContract(local)
	if err != nil {
		return nil, err
	}
	client := &Client{
		db: c.db, BaseURL: c.config.BaseURL, Token: c.config.Token, HTTP: c.config.HTTP,
		config: config, logger: slog.Default(), schemaGenerationProof: local.Proof,
		tableInfo: NewTableInfoProvider(), localGeneration: compiled, sourceIDGenerator: func() string { return uuid.NewString() },
	}
	if c.targetDescriptor != "" && local.Proof.ID == c.config.Plan.TargetGenerationID {
		discovery, err := decodeDiscoveredContract([]byte(c.targetDescriptor), c.config.Plan.TargetGenerationID)
		if err != nil {
			return nil, err
		}
		if discovery.SchemaGeneration.Fingerprint == local.Proof.Fingerprint {
			client.discovery = discovery
			client.config.discovery = discovery
		}
	}
	if client.discovery == nil {
		_, discovery, err := loadAdmittedGenerationDescriptor(context.Background(), c.db, local.Proof.ID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if err == nil && discovery.SchemaGeneration.Fingerprint == local.Proof.Fingerprint {
			client.discovery = discovery
			client.config.discovery = discovery
		}
	}
	if client.HTTP == nil {
		client.HTTP = &http.Client{Timeout: 120 * time.Second}
	}
	if client.Token == nil {
		return nil, fmt.Errorf("upgrade token provider is required")
	}
	return client, nil
}

func (c *Opening) newPhysicalClient(local LocalGenerationContract, supplied *Config) (*Client, error) {
	return c.newPhysicalClientUsing(local, supplied, c.db)
}

func (c *Opening) newPhysicalClientUsing(local LocalGenerationContract, supplied *Config, q tableInfoQueryer) (*Client, error) {
	client, err := c.newWireClient(local, supplied)
	if err != nil {
		return nil, err
	}
	validated, err := validateSyncTables(q, client.tableInfo, client.config.Tables)
	if err != nil {
		return nil, err
	}
	client.pkByTable = validated.pkByTable
	client.keyByTable = validated.keyByTable
	client.tableOrder = validated.tableOrder
	return client, nil
}

func normalizedCoordinatorClientConfig(local LocalGenerationContract, supplied *Config) (*Config, error) {
	contract, err := compileLocalContract(local)
	if err != nil {
		return nil, err
	}
	tables := contract.contract.Tables()
	if len(tables) == 0 {
		return nil, fmt.Errorf("local generation has no tables")
	}
	schema := tables[0].Schema
	var config *Config
	if supplied == nil {
		config = DefaultConfig(local.Proof.ID, local.Tables)
		config.schema, config.generationProof = schema, local.Proof
	} else {
		copyValue := *supplied
		config = &copyValue
	}
	if config.GenerationID != local.Proof.ID {
		return nil, fmt.Errorf("client config generation does not match local contract")
	}
	if !syncTablesEqual(config.Tables, local.Tables) {
		return nil, fmt.Errorf("client config table selection does not match local contract")
	}
	config.schema, config.generationProof = schema, local.Proof
	localCopy := cloneLocalGenerationContract(local)
	config.localContract = &localCopy
	// The plan, not a mutable transport-tuning Config, owns the complete table
	// declaration used by old-intent drain and target rebuild.
	config.Tables = cloneSyncTables(local.Tables)
	if config.SnapshotChunkRows == 0 {
		config.SnapshotChunkRows = 1000
	}
	if config.SnapshotChunkBytes == 0 {
		config.SnapshotChunkBytes = 4 << 20
	}
	return config, nil
}

func cloneCoordinatorClientConfig(config *Config) *Config {
	if config == nil {
		return nil
	}
	clone := *config
	clone.Tables = cloneSyncTables(config.Tables)
	clone.LocalOnlyTables = append([]string(nil), config.LocalOnlyTables...)
	if config.localContract != nil {
		local := cloneLocalGenerationContract(*config.localContract)
		clone.localContract = &local
	}
	if config.discovery != nil {
		discovery := *config.discovery
		clone.discovery = &discovery
	}
	if config.RetryPolicy != nil {
		policy := *config.RetryPolicy
		clone.RetryPolicy = &policy
	}
	if config.SnapshotCapacityRetryPolicy != nil {
		policy := *config.SnapshotCapacityRetryPolicy
		clone.SnapshotCapacityRetryPolicy = &policy
	}
	if config.PushCapacityRetryPolicy != nil {
		policy := *config.PushCapacityRetryPolicy
		clone.PushCapacityRetryPolicy = &policy
	}
	return &clone
}

func cloneSyncTables(tables []SyncTable) []SyncTable {
	clone := append([]SyncTable(nil), tables...)
	for idx := range clone {
		clone[idx].SyncKeyColumns = append([]string(nil), tables[idx].SyncKeyColumns...)
	}
	return clone
}

func validateDiscoveryContract(discovery *oversync.GenerationDiscoveryResponse, local LocalGenerationContract) error {
	if discovery == nil || discovery.SchemaGeneration.ID != fmt.Sprint(local.Proof.ID) || discovery.SchemaGeneration.Fingerprint != local.Proof.Fingerprint {
		return fmt.Errorf("discovery generation proof mismatch")
	}
	compiled, _, err := compileDeclaredGeneration(local)
	if err != nil {
		return err
	}
	expected := compiled.Tables()
	actual := discovery.SchemaGeneration.Tables
	if len(actual) != len(expected) {
		return fmt.Errorf("discovery table count mismatch")
	}
	for idx := range expected {
		if actual[idx].Schema != expected[idx].Schema || actual[idx].Table != expected[idx].Table || !stringSlicesEqual(actual[idx].SyncKeyColumns, expected[idx].SyncKeyColumns) || len(actual[idx].Columns) != len(expected[idx].Columns) || len(actual[idx].ForeignKeys) != len(expected[idx].ForeignKeys) {
			return fmt.Errorf("discovery contract mismatch for %s.%s", expected[idx].Schema, expected[idx].Table)
		}
		for columnIdx := range expected[idx].Columns {
			want, got := expected[idx].Columns[columnIdx], actual[idx].Columns[columnIdx]
			if want.Name != got.Name || want.WireType != got.WireType || want.Nullable != got.Nullable {
				return fmt.Errorf("discovery column contract mismatch")
			}
		}
		for fkIdx := range expected[idx].ForeignKeys {
			want, got := expected[idx].ForeignKeys[fkIdx], actual[idx].ForeignKeys[fkIdx]
			if want.ReferencedSchema != got.ReferencedSchema || want.ReferencedTable != got.ReferencedTable || want.OnDelete != got.OnDelete || want.OnUpdate != got.OnUpdate || !stringSlicesEqual(want.Columns, got.Columns) || !stringSlicesEqual(want.ReferencedColumns, got.ReferencedColumns) {
				return fmt.Errorf("discovery foreign-key contract mismatch")
			}
		}
	}
	return nil
}

func sortStringsCopy(values []string) []string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return copyValues
}
