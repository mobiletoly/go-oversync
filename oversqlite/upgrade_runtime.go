package oversqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

type oneShotUpgradeResolver struct {
	mu       sync.Mutex
	result   MergeResult
	consumed bool
}

func (r *oneShotUpgradeResolver) Resolve(ConflictContext) MergeResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consumed {
		return deferUpgradeConflictResolution{}
	}
	r.consumed = true
	return r.result
}

type deferUpgradeResolver struct{}

func (deferUpgradeResolver) Resolve(ConflictContext) MergeResult {
	return deferUpgradeConflictResolution{}
}

func (c *Opening) resolveOldIntent(ctx context.Context, rec *upgradeJournalRecord) error {
	if rec.DrainComplete {
		return c.enterLocalMigration(ctx, rec)
	}

	client, err := c.newPhysicalClient(c.installedContract, c.config.OldClientConfig)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return err
	}
	client.sourceID = attachment.CurrentSourceID
	client.UserID = c.config.Plan.UserID
	client.sessionConnected = true
	client.Resolver = deferUpgradeResolver{}
	if err := c.restoreOldIntentDecision(ctx, client, rec); err != nil {
		return err
	}

	syncErr := c.syncOldIntent(ctx, client)
	if syncErr != nil {
		var sourceRecovery *SourceRecoveryRequiredError
		if errors.As(syncErr, &sourceRecovery) && sourceRecovery.Code != SourceRecoveryRetired {
			if err := c.recoverOldSourceForUpgrade(ctx, client, rec); err != nil {
				return err
			}
			syncErr = c.syncOldIntent(ctx, client)
		}
	}
	if syncErr != nil {
		var retired *oversync.SchemaGenerationRequestError
		if errors.As(syncErr, &retired) && retired.Code == oversync.SchemaGenerationRetired {
			drained, _, err := inspectOldDrain(ctx, c.db)
			if err != nil {
				return err
			}
			if drained {
				return c.recordOldDrain(ctx, rec)
			}
		}
		var httpErr *retryHTTPError
		if errors.As(syncErr, &httpErr) && strings.HasPrefix(httpErr.Operation, "push_") &&
			httpErr.StatusCode >= http.StatusBadRequest && httpErr.StatusCode < http.StatusInternalServerError &&
			httpErr.StatusCode != http.StatusRequestTimeout && httpErr.StatusCode != http.StatusTooManyRequests {
			return &UpgradeError{Outcome: UpgradeOutcomeNeedsResolution, Cause: syncErr}
		}
		return syncErr
	}

	return c.recordOldDrain(ctx, rec)
}

func (c *Opening) recordOldDrain(ctx context.Context, rec *upgradeJournalRecord) error {
	attachment, err := loadAttachmentState(ctx, c.db)
	if err != nil {
		return err
	}
	drained, reason, err := inspectOldDrain(ctx, c.db)
	if err != nil {
		return err
	}
	if !drained {
		return &UpgradeError{Outcome: UpgradeOutcomeNeedsResolution, Cause: fmt.Errorf("old-generation drain is incomplete: %s", reason)}
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Phase != UpgradePhaseResolvingOldIntent || current.DrainComplete {
		return fmt.Errorf("upgrade journal changed before drain completion")
	}
	drained, reason, err = inspectOldDrain(ctx, tx)
	if err != nil {
		return err
	}
	if !drained {
		return fmt.Errorf("old-generation drain changed under writer: %s", reason)
	}
	attachment, err = loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	current.OldSourceID = attachment.CurrentSourceID
	current.DrainComplete = true
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	current.PendingRequestHash = ""
	current.ResolutionAction = ""
	current.ResolutionRequestHash = ""
	current.ResolutionPayload = ""
	current.Revision++
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	c.reachTestBoundary("before_U2_commit")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.reachTestBoundary("after_U2_commit")
	*rec = *current
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func (c *Opening) restoreOldIntentDecision(ctx context.Context, client *Client, rec *upgradeJournalRecord) error {
	if rec.ResolutionAction != "" {
		outbox, err := loadOutboxBundle(ctx, c.db)
		if err != nil {
			return err
		}
		if outbox.State == outboxStatePrepared {
			if rec.ResolutionRequestHash == "" || outbox.CanonicalRequestHash != rec.ResolutionRequestHash {
				return &UpgradeError{Outcome: UpgradeOutcomeNeedsResolution, Cause: fmt.Errorf("durable resolution no longer matches the frozen request")}
			}
			committed, err := c.settleDefinitivePreparedPush(ctx, client, outbox)
			if err != nil {
				return err
			}
			if committed {
				client.Resolver = deferUpgradeResolver{}
			} else {
				resolver, err := resolverForDurableDecision(rec)
				if err != nil {
					return err
				}
				client.Resolver = resolver
			}
		}
	}

	return nil
}

// A frozen request can commit while later edits survive its replay. Keep draining
// that finite, writer-frozen intent before pull or any local table removal. Only
// a durable source watermark advance permits another pass; conflicts and other
// errors retain the ordinary explicit-resolution path.
func (c *Opening) syncOldIntent(ctx context.Context, client *Client) error {
	for {
		before, err := loadSourceState(ctx, c.db, client.sourceID)
		if err != nil {
			return err
		}
		_, err = client.Sync(ctx)
		var dirty *DirtyStateRejectedError
		if !errors.As(err, &dirty) {
			return err
		}
		after, readErr := loadSourceState(ctx, c.db, client.sourceID)
		if readErr != nil {
			return readErr
		}
		if before == nil || after == nil || before.SourceID != after.SourceID || after.NextSourceBundleID <= before.NextSourceBundleID {
			return &UpgradeError{Outcome: UpgradeOutcomeNeedsResolution, Cause: err}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (c *Opening) enterLocalMigration(ctx context.Context, rec *upgradeJournalRecord) error {
	if rec == nil || rec.Phase != UpgradePhaseResolvingOldIntent || !rec.DrainComplete {
		return fmt.Errorf("target migration gate requires a durable old-generation drain witness")
	}
	c.reachTestBoundary("before_U3_preview")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.previewTarget(ctx); err != nil {
		return err
	}
	c.reachTestBoundary("after_U3_preview")
	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Phase != UpgradePhaseResolvingOldIntent || !current.DrainComplete {
		return fmt.Errorf("upgrade journal changed before entering local migration")
	}
	drained, reason, err := inspectOldDrain(ctx, tx)
	if err != nil {
		return err
	}
	if !drained {
		return fmt.Errorf("old-generation drain changed under writer: %s", reason)
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	if attachment.CurrentSourceID != current.OldSourceID {
		return fmt.Errorf("old source changed after drain completion")
	}
	current.Phase = UpgradePhaseMigratingLocal
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	current.Revision++
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	c.reachTestBoundary("before_U3_commit")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.reachTestBoundary("after_U3_commit")
	*rec = *current
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func (c *Opening) recoverOldSourceForUpgrade(ctx context.Context, client *Client, rec *upgradeJournalRecord) error {
	if client == nil || rec == nil {
		return fmt.Errorf("old-source recovery requires coordinator state")
	}
	var committed *upgradeJournalRecord
	_, err := client.rebuildSourceRecoveryWithFinalizerLocked(ctx, func(finalCtx context.Context, tx *immediateTx, newSourceID string) error {
		current, err := loadUpgradeJournal(finalCtx, tx)
		if err != nil {
			return err
		}
		if current == nil || current.Phase != UpgradePhaseResolvingOldIntent || current.PlanID != rec.PlanID || current.PlanDigest != rec.PlanDigest {
			return fmt.Errorf("upgrade journal changed during old-source recovery")
		}
		if current.OldSourceID != rec.OldSourceID {
			return fmt.Errorf("old source changed during same-generation recovery")
		}
		current.OldSourceID = newSourceID
		current.Outcome = ""
		current.FailureCode = ""
		current.FailureDetail = ""
		current.Revision++
		if err := persistUpgradeJournal(finalCtx, tx, current); err != nil {
			return err
		}
		committed = current
		return nil
	})
	if err != nil {
		return err
	}
	if committed == nil {
		return fmt.Errorf("old-source recovery did not publish its upgrade witness")
	}
	*rec = *committed
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func (c *Opening) settleDefinitivePreparedPush(ctx context.Context, client *Client, outbox *outboxBundleRecord) (bool, error) {
	if client == nil || outbox == nil || outbox.State != outboxStatePrepared {
		return false, fmt.Errorf("prepared push settlement requires exact outbox evidence")
	}
	client.pendingInitializationID = outbox.InitializationID
	snapshot, err := client.loadPushOutboundSnapshot(ctx)
	if err != nil {
		return false, err
	}
	if snapshot.SourceBundleID != outbox.SourceBundleID || int64(len(snapshot.Rows)) != outbox.RowCount {
		return false, fmt.Errorf("prepared push snapshot no longer matches exact outbox evidence")
	}
	if _, err := client.commitPushOutboundSnapshot(ctx, snapshot); err != nil {
		var conflict *PushConflictError
		if errors.As(err, &conflict) || pushApplicationRejection(err) != nil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func resolverForDurableDecision(rec *upgradeJournalRecord) (Resolver, error) {
	if rec == nil {
		return nil, fmt.Errorf("durable resolution record is missing")
	}
	var result MergeResult
	switch rec.ResolutionAction {
	case PendingIntentAcceptServer:
		result = AcceptServer{}
	case PendingIntentKeepLocal:
		result = KeepLocal{}
	case PendingIntentKeepMerged:
		if rec.ResolutionPayload == "" {
			return nil, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("durable merged resolution payload is missing")}
		}
		result = KeepMerged{MergedPayload: json.RawMessage(rec.ResolutionPayload)}
	default:
		return nil, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("unsupported durable resolution action %q", rec.ResolutionAction)}
	}
	return &oneShotUpgradeResolver{result: result}, nil
}

func inspectOldDrain(ctx context.Context, q queryRower) (bool, string, error) {
	checks := []struct {
		table string
		name  string
	}{
		{"_sync_dirty_rows", "dirty synchronized rows"},
		{"_sync_outbox_rows", "frozen outbox rows"},
		{"_sync_push_stage", "staged push results"},
		{"_sync_snapshot_stage", "staged snapshot rows"},
	}
	for _, check := range checks {
		var count int64
		if err := q.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(check.table))).Scan(&count); err != nil {
			return false, "", err
		}
		if count != 0 {
			return false, check.name, nil
		}
	}
	outbox, err := loadOutboxBundle(ctx, q)
	if err != nil {
		return false, "", err
	}
	if outbox.State != outboxStateNone {
		return false, "outbox receipt or request", nil
	}
	attachment, err := loadAttachmentState(ctx, q)
	if err != nil {
		return false, "", err
	}
	if attachment.BindingState != attachmentBindingAttached || attachment.CurrentSourceID == "" || attachment.PendingInitializationID != "" || attachment.RebuildRequired {
		return false, "attachment, initialization, or recovery state", nil
	}
	operation, err := loadOperationState(ctx, q)
	if err != nil {
		return false, "", err
	}
	if operation.Kind != operationKindNone {
		return false, "pending lifecycle operation", nil
	}
	var applyMode int
	if err := q.QueryRowContext(ctx, `SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1`).Scan(&applyMode); err != nil {
		return false, "", err
	}
	if applyMode != 0 {
		return false, "committed internal write bypass", nil
	}
	return true, "", nil
}

func (c *Opening) runLocalMigration(ctx context.Context, rec *upgradeJournalRecord) error {
	plan := c.config.Plan
	observed, err := readInstalledVersionRestricted(ctx, c.db, plan.Migrator)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalMigrationFailed, Cause: err}
	}
	if observed != plan.InstalledVersion {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("durable application version %d does not match original version %d", observed, plan.InstalledVersion)}
	}
	c.localMigrationActive.Store(true)
	defer c.localMigrationActive.Store(false)
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Phase != UpgradePhaseMigratingLocal || current.PlanDigest != c.planDigest {
		return fmt.Errorf("upgrade journal changed before local migration")
	}
	var syncDigestBefore string
	if plan.Mode == UpgradeModeLocalOnly {
		syncDigestBefore, err = localOnlyDigest(ctx, tx, syncPreservationTableNames(c.installedContract))
		if err != nil {
			return err
		}
	}
	if err := setApplyMode(ctx, tx, true); err != nil {
		return err
	}
	if err := suspendInstalledCaptureTriggersInTx(ctx, tx, c.installedContract); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalMigrationFailed, Cause: err}
	}
	step := LocalMigrationStep{
		PlanID: plan.PlanID, OriginalVersion: plan.InstalledVersion, FromVersion: plan.InstalledVersion,
		ToVersion: plan.TargetVersion, TargetVersion: plan.TargetVersion, Fresh: plan.Mode == UpgradeModeFresh,
	}
	actualVersion, err := invokeMigratorInTx(ctx, tx, plan.Migrator, step)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalMigrationFailed, Cause: err}
	}
	if actualVersion != plan.TargetVersion {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalMigrationFailed, Cause: fmt.Errorf("migrator reported durable version %d; target is %d", actualVersion, plan.TargetVersion)}
	}
	if plan.Mode == UpgradeModeGeneration {
		_, response, err := loadAdmittedGenerationDescriptor(ctx, tx, plan.TargetGenerationID)
		if err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
		derived, err := deriveLocalContract(ctx, tx, plan.TargetGenerationID, c.config.TargetClientConfig.Tables, response.SchemaGeneration)
		if err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
		c.targetContract = derived
	}
	if c.targetContract.Proof.Fingerprint != "" {
		if err := validatePhysicalLocalContract(ctx, tx, c.targetContract, plan.LocalOnlyTables); err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
	} else if _, err := managedPhysicalShape(ctx, tx, c.config.TargetClientConfig.Tables); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	if err := validateForeignKeyIntegrity(ctx, tx); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	if c.targetContract.Proof.Fingerprint != "" {
		if err := refreshTargetTriggersInTx(ctx, tx, c.targetContract); err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
	} else if err := refreshUnconfirmedTargetTriggersInTx(ctx, tx, c.config.TargetClientConfig.Tables); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	if plan.Mode == UpgradeModeLocalOnly {
		if err := setApplyMode(ctx, tx, false); err != nil {
			return err
		}
		after, err := localOnlyDigest(ctx, tx, syncPreservationTableNames(c.targetContract))
		if err != nil {
			return err
		}
		if after != syncDigestBefore {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: fmt.Errorf("same-proof migration changed synchronized rows or control state")}
		}
	}
	var preservation string
	if plan.Mode == UpgradeModeFresh {
		preservation, err = localOnlyDigest(ctx, tx, plan.LocalOnlyTables)
	} else {
		preservation, err = localOnlyDigestWithUpgradeGuards(ctx, tx, plan.LocalOnlyTables)
	}
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: err}
	}
	if plan.Mode == UpgradeModeGeneration && current.LocalOnlyDigest != "" && preservation != current.LocalOnlyDigest {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: fmt.Errorf("generation migration changed local-only data")}
	}
	if err := bindOrdinaryContractInTx(
		ctx,
		tx,
		plan,
		c.installedContract,
		c.targetContract,
		c.targetDescriptor,
		plan.TargetVersion,
		plan.Mode != UpgradeModeFresh,
	); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	current.MigrationComplete = true
	current.ValidatedTargetFingerprint = c.targetContract.Proof.Fingerprint
	current.LocalOnlyDigest = preservation
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	current.Revision++
	identity := upgradeIdentityRecord{
		DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID, UserID: plan.UserID,
		GenerationID: int64(plan.InstalledGenerationID), GenerationFingerprint: c.installedContract.Proof.Fingerprint,
		LocalVersion: plan.TargetVersion, Ready: false,
	}
	var (
		readyCandidate *Client
		readyStatus    OpeningStatus
	)
	if plan.Mode == UpgradeModeGeneration {
		current.Phase = UpgradePhaseLocalMigrated
	} else {
		current.Phase = UpgradePhaseReady
		current.Outcome = UpgradeOutcomeReady
		identity.GenerationID = int64(plan.TargetGenerationID)
		identity.GenerationFingerprint = c.targetContract.Proof.Fingerprint
		identity.Ready = true
		if plan.Mode == UpgradeModeLocalOnly {
			if err := dropLocalOnlyUpgradeGuards(ctx, tx, plan.LocalOnlyTables); err != nil {
				return err
			}
			afterGuardRemoval, err := localOnlyDigest(ctx, tx, plan.LocalOnlyTables)
			if err != nil {
				return err
			}
			if afterGuardRemoval != preservation {
				return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: fmt.Errorf("removing local-only upgrade guards changed application state")}
			}
		}
	}
	if err := persistUpgradeIdentity(ctx, tx, identity); err != nil {
		return err
	}
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	if err := setApplyMode(ctx, tx, false); err != nil {
		return err
	}
	if current.Phase == UpgradePhaseReady {
		readyCandidate, err = c.prepareReadyClientInTx(ctx, tx)
		if err != nil {
			return &UpgradeError{Outcome: classifyUpgradeCause(err, true), Cause: err}
		}
		readyStatus, err = c.statusFromRecordUsing(ctx, tx, current)
		if err != nil {
			return err
		}
	}
	c.reachTestBoundary("before_U4_commit")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if readyCandidate != nil {
		c.preparedClient = readyCandidate
		c.preparedStatus = &readyStatus
	}
	c.reachTestBoundary("after_U4_commit")
	*rec = *current
	if rec.Phase != UpgradePhaseReady {
		if err := c.notifyProgress(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

func refreshUnconfirmedTargetTriggersInTx(ctx context.Context, tx *immediateTx, tables []SyncTable) error {
	provider := NewTableInfoProvider()
	for _, table := range tables {
		info, err := provider.Get(tx, table.TableName)
		if err != nil {
			return err
		}
		if err := createTriggersForTableWithInfo(tx, unconfirmedLocalSchema, table, info); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO _sync_managed_tables(schema_name,table_name) VALUES(?,?) ON CONFLICT DO NOTHING`, unconfirmedLocalSchema, strings.ToLower(table.TableName)); err != nil {
			return err
		}
	}
	return nil
}

func refreshTargetTriggersInTx(ctx context.Context, tx *immediateTx, local LocalGenerationContract) error {
	provider := NewTableInfoProvider()
	compiled, err := compileLocalContract(local)
	if err != nil {
		return err
	}
	byName := make(map[string]SyncTable, len(local.Tables))
	for _, table := range local.Tables {
		byName[table.TableName] = table
	}
	for _, table := range compiled.contract.Tables() {
		info, err := provider.Get(tx, table.Table)
		if err != nil {
			return err
		}
		if err := createTriggersForTableWithInfo(tx, table.Schema, byName[table.Table], info); err != nil {
			return err
		}
	}
	return nil
}

func syncPreservationTableNames(local LocalGenerationContract) []string {
	set := make(map[string]struct{})
	for _, table := range local.Tables {
		set[table.TableName] = struct{}{}
	}
	for _, table := range syncMetadataTableNames {
		if table == "_sync_upgrade_journal" || table == "_sync_upgrade_identity" ||
			table == "_sync_contract_identity" || table == "_sync_admitted_contracts" {
			continue
		}
		set[table] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for table := range set {
		result = append(result, table)
	}
	return result
}

func (c *Opening) prepareReplacement(ctx context.Context, rec *upgradeJournalRecord) error {
	plan := c.config.Plan
	if err := validatePhysicalLocalContract(ctx, c.db, c.targetContract, plan.LocalOnlyTables); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	targetClient, err := c.newPhysicalClient(c.targetContract, c.config.TargetClientConfig)
	if err != nil {
		return err
	}
	if err := targetClient.clearPushPlanForLifecycle(ctx, false); err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Phase != UpgradePhaseLocalMigrated || !current.DrainComplete || !current.MigrationComplete {
		return fmt.Errorf("upgrade journal is not eligible for source replacement")
	}
	if err := advanceAttachmentEpoch(ctx, tx); err != nil {
		return err
	}
	if err := setApplyMode(ctx, tx, true); err != nil {
		return err
	}
	if err := targetClient.clearManagedTablesInTxWithOptions(ctx, tx, clearManagedTablesOptions{}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_source_state`); err != nil {
		return err
	}
	preservation, err := localOnlyDigestWithUpgradeGuards(ctx, tx, plan.LocalOnlyTables)
	if err != nil {
		return err
	}
	if preservation != current.LocalOnlyDigest {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: fmt.Errorf("clearing synchronized state changed local-only data")}
	}
	newSource := uuid.NewString()
	if newSource == current.OldSourceID {
		return fmt.Errorf("generated replacement source is not distinct")
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.CurrentSourceID = current.OldSourceID
	attachment.BindingState = attachmentBindingAttached
	attachment.AttachedUserID = plan.UserID
	attachment.SchemaName = targetClient.config.schema
	attachment.LastBundleSeqSeen = 0
	attachment.RebuildRequired = true
	attachment.PendingInitializationID = ""
	if err := persistAttachmentState(ctx, tx, attachment, true); err != nil {
		return err
	}
	if err := persistOperationState(ctx, tx, &operationStateRecord{Kind: operationKindNone}); err != nil {
		return err
	}
	current.NewSourceID = newSource
	current.Phase = UpgradePhaseReattachPending
	current.Revision++
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	if err := setApplyMode(ctx, tx, false); err != nil {
		return err
	}
	c.reachTestBoundary("before_U5_commit")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.reachTestBoundary("after_U5_commit")
	*rec = *current
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func (c *Opening) confirmReplacement(ctx context.Context, rec *upgradeJournalRecord) error {
	if err := c.previewTarget(ctx); err != nil {
		return err
	}
	client, err := c.newPhysicalClient(c.targetContract, c.config.TargetClientConfig)
	if err != nil {
		return err
	}
	client.sourceID = rec.OldSourceID
	client.UserID = c.config.Plan.UserID
	client.sessionConnected = true
	req, err := snapshotSessionCreateRequestFromOptions(rec.OldSourceID, snapshotApplyOptions{
		RotateSource: true, NewSourceID: rec.NewSourceID, ReplacementReason: rec.ReplacementReason,
	})
	if err != nil {
		return err
	}
	session, err := client.createSnapshotSession(ctx, req)
	if err != nil {
		return err
	}
	c.reachTestBoundary("after_U6_remote_reply_before_local_receipt")
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Phase != UpgradePhaseReattachPending ||
		current.OldSourceID != rec.OldSourceID || current.NewSourceID != rec.NewSourceID || current.ReplacementReason != rec.ReplacementReason {
		return fmt.Errorf("replacement tuple changed before confirmation")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return err
	}
	current.ReplacementConfirmed = true
	current.SnapshotID = session.SnapshotID
	current.SnapshotBundleSeq = session.SnapshotBundleSeq
	current.SnapshotRowCount = session.RowCount
	current.SnapshotByteCount = session.ByteCount
	current.SnapshotRowsStaged = 0
	current.SnapshotBytesStaged = 0
	current.Phase = UpgradePhaseRebuildRunning
	current.Revision++
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	c.reachTestBoundary("before_U6_commit")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.reachTestBoundary("after_U6_commit")
	*rec = *current
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func (c *Opening) resumeTargetSnapshot(ctx context.Context, rec *upgradeJournalRecord) error {
	plan := c.config.Plan
	if err := validatePhysicalLocalContract(ctx, c.db, c.targetContract, plan.LocalOnlyTables); err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
	}
	client, err := c.newPhysicalClient(c.targetContract, c.config.TargetClientConfig)
	if err != nil {
		return err
	}
	client.sourceID = rec.NewSourceID
	client.UserID = plan.UserID
	client.sessionConnected = true
	limits, err := client.negotiateSnapshotLimits(ctx)
	if err != nil {
		return err
	}
	rows, bytes, err := client.stagedSnapshotTotals(ctx, c.db, rec.SnapshotID)
	if err != nil {
		return err
	}
	if rows != rec.SnapshotRowsStaged || bytes != rec.SnapshotBytesStaged ||
		rows > rec.SnapshotRowCount || bytes > rec.SnapshotByteCount {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("staged snapshot progress does not match durable journal")}
	}
	for rows < rec.SnapshotRowCount {
		chunk, _, fetchErr := client.fetchSnapshotChunkAsSource(ctx, rec.SnapshotID, rec.SnapshotBundleSeq, rows, limits.maxRows, limits.maxBytes, rec.NewSourceID)
		if fetchErr != nil {
			if isRestartableSnapshotSessionError(fetchErr) {
				return c.replaceExpiredTargetSnapshot(ctx, rec, client)
			}
			return fetchErr
		}
		if err := c.stageTargetSnapshotChunk(ctx, client, rec, chunk, rows); err != nil {
			return err
		}
		rows, bytes = rec.SnapshotRowsStaged, rec.SnapshotBytesStaged
		if !chunk.HasMore {
			break
		}
	}
	if rows != rec.SnapshotRowCount || bytes != rec.SnapshotByteCount {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("staged snapshot totals do not match descriptor")}
	}
	session := &oversync.SnapshotSession{
		SnapshotID: rec.SnapshotID, SnapshotBundleSeq: rec.SnapshotBundleSeq,
		RowCount: rec.SnapshotRowCount, ByteCount: rec.SnapshotByteCount,
	}
	options := snapshotApplyOptions{
		RotateSource: true, NewSourceID: rec.NewSourceID, ReplacementReason: rec.ReplacementReason,
		RequireFreshRotatedSource: true,
	}
	var (
		readyCandidate *Client
		readyStatus    OpeningStatus
		readyRecord    *upgradeJournalRecord
	)
	options.UpgradeFinalizer = func(finalCtx context.Context, tx *immediateTx) error {
		current, err := loadUpgradeJournal(finalCtx, tx)
		if err != nil || current == nil || current.Phase != UpgradePhaseRebuildRunning || !current.ReplacementConfirmed ||
			current.SnapshotID != session.SnapshotID || current.NewSourceID != rec.NewSourceID {
			return fmt.Errorf("upgrade journal changed before ready publication")
		}
		if err := validatePhysicalLocalContract(finalCtx, tx, c.targetContract, plan.LocalOnlyTables); err != nil {
			return err
		}
		if err := validateForeignKeyIntegrity(finalCtx, tx); err != nil {
			return err
		}
		preservation, err := localOnlyDigestWithUpgradeGuards(finalCtx, tx, plan.LocalOnlyTables)
		if err != nil {
			return err
		}
		if preservation != current.LocalOnlyDigest {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: fmt.Errorf("snapshot apply changed local-only data")}
		}
		current.Phase = UpgradePhaseReady
		current.Outcome = UpgradeOutcomeReady
		current.FailureCode = ""
		current.FailureDetail = ""
		current.Revision++
		current.SnapshotRowsStaged = current.SnapshotRowCount
		current.SnapshotBytesStaged = current.SnapshotByteCount
		if err := persistUpgradeIdentity(finalCtx, tx, upgradeIdentityRecord{
			DatabaseID: plan.DatabaseID, ServiceID: plan.ServiceID, UserID: plan.UserID,
			GenerationID: int64(c.targetContract.Proof.ID), GenerationFingerprint: c.targetContract.Proof.Fingerprint,
			LocalVersion: plan.TargetVersion, Ready: true,
		}); err != nil {
			return err
		}
		if err := persistUpgradeJournal(finalCtx, tx, current); err != nil {
			return err
		}
		if err := verifyOrdinaryTargetContractInTx(finalCtx, tx, plan, c.targetContract); err != nil {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalContractMismatch, Cause: err}
		}
		if err := dropLocalOnlyUpgradeGuards(finalCtx, tx, plan.LocalOnlyTables); err != nil {
			return err
		}
		afterGuardRemoval, err := localOnlyDigest(finalCtx, tx, plan.LocalOnlyTables)
		if err != nil {
			return err
		}
		if afterGuardRemoval != current.LocalOnlyDigest {
			return &UpgradeError{Outcome: UpgradeOutcomeLocalPreservationFailed, Cause: fmt.Errorf("removing local-only upgrade guards changed application state")}
		}
		readyCandidate, err = c.prepareReadyClientInTx(finalCtx, tx)
		if err != nil {
			return &UpgradeError{Outcome: classifyUpgradeCause(err, true), Cause: err}
		}
		readyStatus, err = c.statusFromRecordUsing(finalCtx, tx, current)
		if err != nil {
			return err
		}
		c.reachTestBoundary("before_U8_commit")
		if err := finalCtx.Err(); err != nil {
			return err
		}
		committed := *current
		readyRecord = &committed
		return nil
	}
	if err := client.applyStagedSnapshotLocked(ctx, session, options); err != nil {
		return err
	}
	c.preparedClient = readyCandidate
	c.preparedStatus = &readyStatus
	c.reachTestBoundary("after_U8_commit")
	if readyRecord == nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: fmt.Errorf("ready journal was not retained at the final commit")}
	}
	*rec = *readyRecord
	return nil
}

type upgradeTableQueryer interface {
	upgradeQueryer
	tableInfoQueryer
}

const upgradeSnapshotAccountingPageRows = 128

func (c *Client) stagedSnapshotTotals(ctx context.Context, q upgradeTableQueryer, snapshotID string) (int64, int64, error) {
	// Populate bounded per-table metadata before opening a staged-row cursor.
	// decodeDirtyKeyForPush then remains a cache-only operation while rows are
	// streamed, including when the database pool has a single connection.
	for _, table := range c.config.Tables {
		if _, err := c.getTableInfoTx(q, strings.ToLower(table.TableName)); err != nil {
			return 0, 0, err
		}
	}

	var rowCount, byteCount, afterOrdinal int64
	for {
		rows, err := q.QueryContext(ctx, `
			SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
			FROM _sync_snapshot_stage
			WHERE snapshot_id=? AND row_ordinal>=?
			ORDER BY row_ordinal
			LIMIT ?
		`, snapshotID, afterOrdinal, upgradeSnapshotAccountingPageRows)
		if err != nil {
			return 0, 0, err
		}
		pageRows := 0
		for rows.Next() {
			var ordinal, rowVersion int64
			var schemaName, tableName, keyJSON, payload string
			if err := rows.Scan(&ordinal, &schemaName, &tableName, &keyJSON, &rowVersion, &payload); err != nil {
				_ = rows.Close()
				return 0, 0, err
			}
			_, key, err := c.decodeDirtyKeyForPush(q, tableName, keyJSON)
			if err != nil {
				_ = rows.Close()
				return 0, 0, err
			}
			if c.discovery == nil {
				_ = rows.Close()
				return 0, 0, fmt.Errorf("staged snapshot accounting requires an admitted discovery contract")
			}
			_, wireBytes, err := oversync.CompactGroupsForSnapshotRowsAt(c.discovery.SchemaGeneration, []oversync.SnapshotRow{{
				Schema: schemaName, Table: tableName, Key: key,
				RowVersion: rowVersion, Payload: []byte(payload),
			}}, ordinal)
			if err != nil {
				_ = rows.Close()
				return 0, 0, err
			}
			byteCount, err = checkedAddInt64(byteCount, wireBytes)
			if err != nil {
				_ = rows.Close()
				return 0, 0, err
			}
			rowCount++
			pageRows++
			afterOrdinal = ordinal + 1
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		if err := rows.Close(); err != nil {
			return 0, 0, err
		}
		if pageRows < upgradeSnapshotAccountingPageRows {
			return rowCount, byteCount, nil
		}
	}
}

func snapshotChunkEncodedByteCount(chunk *oversync.SnapshotChunkResponse) (int64, error) {
	if chunk == nil {
		return 0, fmt.Errorf("snapshot chunk is missing")
	}
	var byteCount int64
	for idx := range chunk.Rows {
		wireBytes, err := snapshotRowEncodedByteCount(&chunk.Rows[idx])
		if err != nil {
			return 0, err
		}
		byteCount, err = checkedAddInt64(byteCount, wireBytes)
		if err != nil {
			return 0, err
		}
	}
	return byteCount, nil
}

func decodeSyncKeyJSON(raw string) (oversync.SyncKey, error) {
	var key oversync.SyncKey
	if err := jsonUnmarshalExact([]byte(raw), &key); err != nil {
		return nil, err
	}
	return key, nil
}

func jsonUnmarshalExact(raw []byte, target any) error {
	return json.Unmarshal(raw, target)
}

func (c *Opening) stageTargetSnapshotChunk(
	ctx context.Context,
	client *Client,
	rec *upgradeJournalRecord,
	chunk *oversync.SnapshotChunkResponse,
	afterRowOrdinal int64,
) error {
	chunkBytes := chunk.ByteCount
	var committed *upgradeJournalRecord
	err := client.stageSnapshotChunkWithFinalizer(ctx, chunk, afterRowOrdinal, func(finalCtx context.Context, tx *immediateTx) error {
		current, err := loadUpgradeJournal(finalCtx, tx)
		if err != nil || current == nil || current.Phase != UpgradePhaseRebuildRunning || current.SnapshotID != rec.SnapshotID {
			return fmt.Errorf("snapshot progress journal changed")
		}
		if current.SnapshotRowsStaged != afterRowOrdinal {
			return fmt.Errorf("snapshot staged row progress changed")
		}
		rows, err := checkedAddInt64(current.SnapshotRowsStaged, int64(len(chunk.Rows)))
		if err != nil {
			return err
		}
		bytes, err := checkedAddInt64(current.SnapshotBytesStaged, chunkBytes)
		if err != nil {
			return err
		}
		if rows > current.SnapshotRowCount || bytes > current.SnapshotByteCount {
			return fmt.Errorf("snapshot staged progress is outside the durable descriptor")
		}
		current.SnapshotRowsStaged = rows
		current.SnapshotBytesStaged = bytes
		current.Revision++
		if err := persistUpgradeJournal(finalCtx, tx, current); err != nil {
			return err
		}
		committed = current
		c.reachTestBoundary("before_U7_commit")
		if err := finalCtx.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.reachTestBoundary("after_U7_commit")
	*rec = *committed
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

func isRestartableSnapshotSessionError(err error) bool {
	var responseErr *snapshotSessionResponseError
	return errors.As(err, &responseErr) && (responseErr.Code == "snapshot_session_expired" || responseErr.Code == "snapshot_session_not_found")
}

func (c *Opening) replaceExpiredTargetSnapshot(ctx context.Context, rec *upgradeJournalRecord, client *Client) error {
	session, err := client.createSnapshotSession(ctx, nil)
	if err != nil {
		return err
	}
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Phase != UpgradePhaseRebuildRunning || current.NewSourceID != rec.NewSourceID {
		return fmt.Errorf("upgrade journal changed while replacing expired snapshot")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_snapshot_stage`); err != nil {
		return err
	}
	current.SnapshotID = session.SnapshotID
	current.SnapshotBundleSeq = session.SnapshotBundleSeq
	current.SnapshotRowCount = session.RowCount
	current.SnapshotByteCount = session.ByteCount
	current.SnapshotRowsStaged = 0
	current.SnapshotBytesStaged = 0
	current.Revision++
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	*rec = *current
	if err := c.notifyProgress(ctx, rec); err != nil {
		return err
	}
	return nil
}

// ResolvePendingIntent records and executes an explicit resolution against the
// exact request/revision reported by Status, then resumes Run.
func (c *Opening) ResolvePendingIntent(ctx context.Context, resolution PendingIntentResolution) (result OpeningResult, resultErr error) {
	operationCtx, finish, err := c.beginMutation(ctx)
	if err != nil {
		return OpeningResult{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, finish()) }()
	ctx = operationCtx
	rec, err := loadUpgradeJournal(ctx, c.db)
	if err != nil {
		return OpeningResult{}, err
	}
	if rec == nil || rec.Phase != UpgradePhaseResolvingOldIntent || rec.Outcome != UpgradeOutcomeNeedsResolution ||
		resolution.PlanID != rec.PlanID || resolution.Revision != rec.Revision || resolution.RequestHash == "" || resolution.RequestHash != rec.PendingRequestHash {
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("pending-intent resolution does not match durable request and revision")}
	}
	if rejection, loadErr := readPendingPushRejection(ctx, c.db); loadErr != nil {
		return OpeningResult{}, loadErr
	} else if rejection != nil && resolution.Action != PendingIntentRetry && resolution.Action != PendingIntentDiscard {
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("application rejection permits only retry or discard")}
	}
	switch resolution.Action {
	case PendingIntentRetry:
		if err := c.recordPendingIntentResolution(ctx, rec, "", nil); err != nil {
			return OpeningResult{}, err
		}
	case PendingIntentDiscard:
		if err := c.discardPendingIntent(ctx, rec); err != nil {
			return c.resultForError(ctx, rec, err, false)
		}
	case PendingIntentAcceptServer:
		if err := c.recordPendingIntentResolution(ctx, rec, resolution.Action, nil); err != nil {
			return OpeningResult{}, err
		}
	case PendingIntentKeepLocal:
		if err := c.recordPendingIntentResolution(ctx, rec, resolution.Action, nil); err != nil {
			return OpeningResult{}, err
		}
	case PendingIntentKeepMerged:
		if len(resolution.MergedPayload) == 0 {
			return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("merged resolution requires a payload")}
		}
		if err := c.recordPendingIntentResolution(ctx, rec, resolution.Action, resolution.MergedPayload); err != nil {
			return OpeningResult{}, err
		}
	default:
		return OpeningResult{}, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("unsupported pending-intent resolution action %q", resolution.Action)}
	}
	return c.runOwned(ctx)
}

func (c *Opening) recordPendingIntentResolution(ctx context.Context, rec *upgradeJournalRecord, action PendingIntentResolutionAction, payload json.RawMessage) error {
	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Revision != rec.Revision || current.PendingRequestHash != rec.PendingRequestHash {
		return fmt.Errorf("pending-intent journal changed")
	}
	if action == "" {
		if err := clearConflictForUpgradeDecision(ctx, tx, current.PendingRequestHash, "upgrade_retry"); err != nil {
			return err
		}
		if err := clearPushRejectionForUpgradeDecision(ctx, tx, current.PendingRequestHash, "upgrade_retry"); err != nil {
			return err
		}
	}
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	current.ResolutionAction = action
	current.ResolutionRequestHash = ""
	if action != "" {
		current.ResolutionRequestHash = current.PendingRequestHash
	}
	current.ResolutionPayload = string(payload)
	current.ResolutionRevision++
	current.Revision++
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	c.reachTestBoundary("before_resolution_commit")
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.reachTestBoundary("after_resolution_commit")
	*rec = *current
	return nil
}

func (c *Opening) discardPendingIntent(ctx context.Context, rec *upgradeJournalRecord) error {
	outbox, err := loadOutboxBundle(ctx, c.db)
	if err != nil {
		return err
	}
	if outbox.State != outboxStatePrepared || outbox.CanonicalRequestHash != rec.PendingRequestHash {
		return fmt.Errorf("discard is allowed only for the exact definitively uncommitted request")
	}
	client, err := c.newPhysicalClient(c.installedContract, c.config.OldClientConfig)
	if err != nil {
		return err
	}
	client.sourceID = rec.OldSourceID
	client.UserID = c.config.Plan.UserID
	client.sessionConnected = true
	rejection, err := readPendingPushRejection(ctx, c.db)
	if err != nil {
		return err
	}
	if rejection == nil {
		committed, err := c.settleDefinitivePreparedPush(ctx, client, outbox)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
	}

	tx, err := beginImmediateTx(ctx, c.db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	current, err := loadUpgradeJournal(ctx, tx)
	if err != nil || current == nil || current.Revision != rec.Revision || current.PendingRequestHash != rec.PendingRequestHash {
		return fmt.Errorf("pending-intent journal changed")
	}
	outbox, err = loadOutboxBundle(ctx, tx)
	if err != nil {
		return err
	}
	if outbox.State != outboxStatePrepared || outbox.CanonicalRequestHash != rec.PendingRequestHash {
		return fmt.Errorf("discard is allowed only for the exact definitively uncommitted request")
	}
	if err := clearConflictForUpgradeDecision(ctx, tx, current.PendingRequestHash, "upgrade_discard"); err != nil {
		return err
	}
	if err := clearPushRejectionForUpgradeDecision(ctx, tx, current.PendingRequestHash, "upgrade_discard"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_outbox_rows WHERE source_bundle_id=?`, outbox.SourceBundleID); err != nil {
		return err
	}
	if err := clearOutboxBundle(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _sync_push_stage`); err != nil {
		return err
	}
	attachment, err := loadAttachmentState(ctx, tx)
	if err != nil {
		return err
	}
	attachment.RebuildRequired = true
	if err := persistAttachmentState(ctx, tx, attachment); err != nil {
		return err
	}
	current.Outcome = ""
	current.FailureCode = ""
	current.FailureDetail = ""
	current.PendingRequestHash = ""
	current.ResolutionAction = ""
	current.ResolutionRequestHash = ""
	current.ResolutionPayload = ""
	current.ResolutionRevision++
	current.Revision++
	if err := persistUpgradeJournal(ctx, tx, current); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	*rec = *current
	client.sourceID = current.OldSourceID
	client.UserID = c.config.Plan.UserID
	client.sessionConnected = true
	_, err = client.Rebuild(ctx)
	return err
}
