package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

const upgradeJournalFormatVersion = 1

type upgradeIdentityRecord struct {
	DatabaseID            string
	ServiceID             string
	UserID                string
	GenerationID          int64
	GenerationFingerprint string
	LocalVersion          int64
	Ready                 bool
}

type upgradeJournalRecord struct {
	Mode                        UpgradeMode
	Phase                       UpgradePhase
	Revision                    int64
	PlanID                      string
	PlanDigest                  string
	DatabaseID                  string
	ServiceID                   string
	UserID                      string
	OldGenerationID             int64
	OldGenerationFingerprint    string
	OldVersion                  int64
	TargetGenerationID          int64
	TargetGenerationFingerprint string
	TargetVersion               int64
	OldSourceID                 string
	NewSourceID                 string
	ReplacementReason           string
	DrainComplete               bool
	MigrationComplete           bool
	ValidatedTargetFingerprint  string
	LocalOnlyDigest             string
	ReplacementConfirmed        bool
	SnapshotID                  string
	SnapshotBundleSeq           int64
	SnapshotRowCount            int64
	SnapshotByteCount           int64
	SnapshotRowsStaged          int64
	SnapshotBytesStaged         int64
	Outcome                     UpgradeOutcome
	FailureCode                 string
	FailureDetail               string
	PendingRequestHash          string
	ResolutionRevision          int64
	ResolutionAction            PendingIntentResolutionAction
	ResolutionRequestHash       string
	ResolutionPayload           string
	IntegrityDigest             string
}

func ensureUpgradeMetadata(ctx context.Context, e execer) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS _sync_upgrade_identity (
			singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
			format_version INTEGER NOT NULL CHECK (format_version = 1),
			database_id TEXT NOT NULL,
			service_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			generation_id INTEGER NOT NULL,
			generation_fingerprint TEXT NOT NULL,
			local_version INTEGER NOT NULL,
			ready INTEGER NOT NULL CHECK (ready IN (0, 1))
		)`,
		`CREATE TABLE IF NOT EXISTS _sync_upgrade_journal (
			singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
			format_version INTEGER NOT NULL CHECK (format_version = 1),
			mode TEXT NOT NULL CHECK (mode IN ('generation', 'fresh', 'local_only')),
			phase TEXT NOT NULL CHECK (phase IN ('ready', 'upgrade_started', 'resolving_old_intent', 'migrating_local', 'local_migrated', 'reattach_pending', 'rebuild_running')),
			revision INTEGER NOT NULL,
			plan_id TEXT NOT NULL,
			plan_digest TEXT NOT NULL,
			database_id TEXT NOT NULL,
			service_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			old_generation_id INTEGER NOT NULL,
			old_generation_fingerprint TEXT NOT NULL,
			old_version INTEGER NOT NULL,
			target_generation_id INTEGER NOT NULL,
			target_generation_fingerprint TEXT NOT NULL,
			target_version INTEGER NOT NULL,
			old_source_id TEXT NOT NULL,
			new_source_id TEXT NOT NULL,
			replacement_reason TEXT NOT NULL,
			drain_complete INTEGER NOT NULL CHECK (drain_complete IN (0, 1)),
			migration_complete INTEGER NOT NULL CHECK (migration_complete IN (0, 1)),
			validated_target_fingerprint TEXT NOT NULL,
			local_only_digest TEXT NOT NULL,
			replacement_confirmed INTEGER NOT NULL CHECK (replacement_confirmed IN (0, 1)),
			snapshot_id TEXT NOT NULL,
			snapshot_bundle_seq INTEGER NOT NULL,
			snapshot_row_count INTEGER NOT NULL,
			snapshot_byte_count INTEGER NOT NULL,
			snapshot_rows_staged INTEGER NOT NULL,
			snapshot_bytes_staged INTEGER NOT NULL,
			outcome_code TEXT NOT NULL,
			failure_code TEXT NOT NULL,
			failure_detail TEXT NOT NULL,
			pending_request_hash TEXT NOT NULL,
			resolution_revision INTEGER NOT NULL,
			resolution_action TEXT NOT NULL,
				resolution_request_hash TEXT NOT NULL,
				resolution_payload TEXT NOT NULL,
				integrity_digest TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := e.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("failed to initialize upgrade metadata: %w", err)
		}
	}
	return nil
}

func loadUpgradeIdentity(ctx context.Context, q queryRower) (*upgradeIdentityRecord, error) {
	var rec upgradeIdentityRecord
	var ready int
	err := q.QueryRowContext(ctx, `
		SELECT database_id, service_id, user_id, generation_id, generation_fingerprint, local_version, ready
		FROM _sync_upgrade_identity WHERE singleton_key = 1
	`).Scan(&rec.DatabaseID, &rec.ServiceID, &rec.UserID, &rec.GenerationID, &rec.GenerationFingerprint, &rec.LocalVersion, &ready)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load installed upgrade identity: %w", err)
	}
	rec.Ready = ready == 1
	if rec.GenerationID < 0 || rec.LocalVersion < 0 ||
		(rec.GenerationFingerprint != "" && !validSchemaGenerationFingerprint(rec.GenerationFingerprint)) {
		return nil, fmt.Errorf("installed upgrade identity is corrupt")
	}
	return &rec, nil
}

func persistUpgradeIdentity(ctx context.Context, e execer, rec upgradeIdentityRecord) error {
	ready := 0
	if rec.Ready {
		ready = 1
	}
	_, err := e.ExecContext(ctx, `
		INSERT INTO _sync_upgrade_identity (
			singleton_key, format_version, database_id, service_id, user_id,
			generation_id, generation_fingerprint, local_version, ready
		) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(singleton_key) DO UPDATE SET
			format_version=excluded.format_version,
			database_id=excluded.database_id,
			service_id=excluded.service_id,
			user_id=excluded.user_id,
			generation_id=excluded.generation_id,
			generation_fingerprint=excluded.generation_fingerprint,
			local_version=excluded.local_version,
			ready=excluded.ready
	`, upgradeJournalFormatVersion, rec.DatabaseID, rec.ServiceID, rec.UserID, rec.GenerationID, rec.GenerationFingerprint, rec.LocalVersion, ready)
	if err != nil {
		return fmt.Errorf("failed to persist installed upgrade identity: %w", err)
	}
	return nil
}

func loadUpgradeJournal(ctx context.Context, q queryRower) (*upgradeJournalRecord, error) {
	var rec upgradeJournalRecord
	var format, drain, migration, replacement int
	err := q.QueryRowContext(ctx, `
		SELECT format_version, mode, phase, revision, plan_id, plan_digest,
			database_id, service_id, user_id,
			old_generation_id, old_generation_fingerprint, old_version,
			target_generation_id, target_generation_fingerprint, target_version,
			old_source_id, new_source_id, replacement_reason,
			drain_complete, migration_complete, validated_target_fingerprint, local_only_digest,
			replacement_confirmed, snapshot_id, snapshot_bundle_seq,
			snapshot_row_count, snapshot_byte_count, snapshot_rows_staged, snapshot_bytes_staged,
			outcome_code, failure_code, failure_detail, pending_request_hash,
				resolution_revision, resolution_action, resolution_request_hash, resolution_payload,
				integrity_digest
		FROM _sync_upgrade_journal WHERE singleton_key = 1
	`).Scan(
		&format, &rec.Mode, &rec.Phase, &rec.Revision, &rec.PlanID, &rec.PlanDigest,
		&rec.DatabaseID, &rec.ServiceID, &rec.UserID,
		&rec.OldGenerationID, &rec.OldGenerationFingerprint, &rec.OldVersion,
		&rec.TargetGenerationID, &rec.TargetGenerationFingerprint, &rec.TargetVersion,
		&rec.OldSourceID, &rec.NewSourceID, &rec.ReplacementReason,
		&drain, &migration, &rec.ValidatedTargetFingerprint, &rec.LocalOnlyDigest,
		&replacement, &rec.SnapshotID, &rec.SnapshotBundleSeq,
		&rec.SnapshotRowCount, &rec.SnapshotByteCount, &rec.SnapshotRowsStaged, &rec.SnapshotBytesStaged,
		&rec.Outcome, &rec.FailureCode, &rec.FailureDetail, &rec.PendingRequestHash,
		&rec.ResolutionRevision, &rec.ResolutionAction, &rec.ResolutionRequestHash, &rec.ResolutionPayload,
		&rec.IntegrityDigest,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load upgrade journal: %w", err)
	}
	rec.DrainComplete = drain == 1
	rec.MigrationComplete = migration == 1
	rec.ReplacementConfirmed = replacement == 1
	if format != upgradeJournalFormatVersion {
		return nil, fmt.Errorf("upgrade journal is corrupt")
	}
	if err := validateUpgradeJournalRecord(&rec); err != nil {
		return nil, err
	}
	digest, err := upgradeJournalIntegrityDigest(&rec)
	if err != nil || rec.IntegrityDigest != digest {
		return nil, fmt.Errorf("upgrade journal integrity witness does not match")
	}
	return &rec, nil
}

func persistUpgradeJournal(ctx context.Context, e execer, rec *upgradeJournalRecord) error {
	if err := validateUpgradeJournalRecord(rec); err != nil {
		return err
	}
	integrityDigest, err := upgradeJournalIntegrityDigest(rec)
	if err != nil {
		return err
	}
	rec.IntegrityDigest = integrityDigest
	boolInt := func(value bool) int {
		if value {
			return 1
		}
		return 0
	}
	_, err = e.ExecContext(ctx, `
		INSERT INTO _sync_upgrade_journal (
			singleton_key, format_version, mode, phase, revision, plan_id, plan_digest,
			database_id, service_id, user_id,
			old_generation_id, old_generation_fingerprint, old_version,
			target_generation_id, target_generation_fingerprint, target_version,
			old_source_id, new_source_id, replacement_reason,
			drain_complete, migration_complete, validated_target_fingerprint, local_only_digest,
			replacement_confirmed, snapshot_id, snapshot_bundle_seq,
			snapshot_row_count, snapshot_byte_count, snapshot_rows_staged, snapshot_bytes_staged,
			outcome_code, failure_code, failure_detail, pending_request_hash,
				resolution_revision, resolution_action, resolution_request_hash, resolution_payload,
				integrity_digest
			) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(singleton_key) DO UPDATE SET
			format_version=excluded.format_version, mode=excluded.mode,
			phase=excluded.phase, revision=excluded.revision,
			plan_id=excluded.plan_id, plan_digest=excluded.plan_digest,
			database_id=excluded.database_id, service_id=excluded.service_id, user_id=excluded.user_id,
			old_generation_id=excluded.old_generation_id, old_generation_fingerprint=excluded.old_generation_fingerprint,
			old_version=excluded.old_version, target_generation_id=excluded.target_generation_id,
			target_generation_fingerprint=excluded.target_generation_fingerprint, target_version=excluded.target_version,
			old_source_id=excluded.old_source_id, new_source_id=excluded.new_source_id,
			replacement_reason=excluded.replacement_reason, drain_complete=excluded.drain_complete,
			migration_complete=excluded.migration_complete, validated_target_fingerprint=excluded.validated_target_fingerprint,
			local_only_digest=excluded.local_only_digest,
			replacement_confirmed=excluded.replacement_confirmed, snapshot_id=excluded.snapshot_id,
			snapshot_bundle_seq=excluded.snapshot_bundle_seq, snapshot_row_count=excluded.snapshot_row_count,
			snapshot_byte_count=excluded.snapshot_byte_count, snapshot_rows_staged=excluded.snapshot_rows_staged,
			snapshot_bytes_staged=excluded.snapshot_bytes_staged,
			outcome_code=excluded.outcome_code, failure_code=excluded.failure_code,
			failure_detail=excluded.failure_detail, pending_request_hash=excluded.pending_request_hash,
				resolution_revision=excluded.resolution_revision, resolution_action=excluded.resolution_action,
				resolution_request_hash=excluded.resolution_request_hash, resolution_payload=excluded.resolution_payload,
				integrity_digest=excluded.integrity_digest
	`, upgradeJournalFormatVersion, rec.Mode, rec.Phase, rec.Revision, rec.PlanID, rec.PlanDigest,
		rec.DatabaseID, rec.ServiceID, rec.UserID,
		rec.OldGenerationID, rec.OldGenerationFingerprint, rec.OldVersion,
		rec.TargetGenerationID, rec.TargetGenerationFingerprint, rec.TargetVersion,
		rec.OldSourceID, rec.NewSourceID, rec.ReplacementReason,
		boolInt(rec.DrainComplete), boolInt(rec.MigrationComplete), rec.ValidatedTargetFingerprint, rec.LocalOnlyDigest,
		boolInt(rec.ReplacementConfirmed), rec.SnapshotID, rec.SnapshotBundleSeq,
		rec.SnapshotRowCount, rec.SnapshotByteCount, rec.SnapshotRowsStaged, rec.SnapshotBytesStaged,
		rec.Outcome, rec.FailureCode, rec.FailureDetail, rec.PendingRequestHash,
		rec.ResolutionRevision, rec.ResolutionAction, rec.ResolutionRequestHash, rec.ResolutionPayload,
		rec.IntegrityDigest)
	if err != nil {
		return fmt.Errorf("failed to persist upgrade journal: %w", err)
	}
	return nil
}

func upgradeJournalIntegrityDigest(rec *upgradeJournalRecord) (string, error) {
	if rec == nil {
		return "", fmt.Errorf("upgrade journal record is missing")
	}
	canonical, err := jcs.Marshal([]any{
		"oversqlite.upgrade-journal.v1",
		string(rec.Mode), string(rec.Phase), fmt.Sprint(rec.Revision), rec.PlanID, rec.PlanDigest,
		rec.DatabaseID, rec.ServiceID, rec.UserID,
		fmt.Sprint(rec.OldGenerationID), rec.OldGenerationFingerprint, fmt.Sprint(rec.OldVersion),
		fmt.Sprint(rec.TargetGenerationID), rec.TargetGenerationFingerprint, fmt.Sprint(rec.TargetVersion),
		rec.OldSourceID, rec.NewSourceID, rec.ReplacementReason,
		rec.DrainComplete, rec.MigrationComplete, rec.ValidatedTargetFingerprint, rec.LocalOnlyDigest,
		rec.ReplacementConfirmed, rec.SnapshotID, fmt.Sprint(rec.SnapshotBundleSeq),
		fmt.Sprint(rec.SnapshotRowCount), fmt.Sprint(rec.SnapshotByteCount),
		fmt.Sprint(rec.SnapshotRowsStaged), fmt.Sprint(rec.SnapshotBytesStaged),
		string(rec.Outcome), rec.FailureCode, rec.FailureDetail, rec.PendingRequestHash,
		fmt.Sprint(rec.ResolutionRevision), string(rec.ResolutionAction), rec.ResolutionRequestHash, rec.ResolutionPayload,
	})
	if err != nil {
		return "", fmt.Errorf("canonicalize upgrade journal: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func validateUpgradeJournalRecord(rec *upgradeJournalRecord) error {
	if rec == nil || !validUpgradeMode(rec.Mode) || !validUpgradePhase(rec.Phase) || rec.Revision < 0 ||
		rec.PlanID == "" || rec.PlanID != strings.TrimSpace(rec.PlanID) || !validSchemaGenerationFingerprint(rec.PlanDigest) ||
		rec.DatabaseID == "" || rec.DatabaseID != strings.TrimSpace(rec.DatabaseID) ||
		rec.ServiceID == "" || rec.ServiceID != strings.TrimSpace(rec.ServiceID) ||
		rec.UserID != strings.TrimSpace(rec.UserID) || rec.OldVersion < 0 || rec.TargetVersion < 0 ||
		rec.TargetGenerationID <= 0 ||
		(rec.TargetGenerationFingerprint != "" && !validSchemaGenerationFingerprint(rec.TargetGenerationFingerprint)) ||
		rec.SnapshotBundleSeq < 0 || rec.SnapshotRowCount < 0 || rec.SnapshotByteCount < 0 ||
		rec.SnapshotRowsStaged < 0 || rec.SnapshotBytesStaged < 0 ||
		rec.SnapshotRowsStaged > rec.SnapshotRowCount || rec.SnapshotBytesStaged > rec.SnapshotByteCount ||
		rec.ResolutionRevision < 0 || !validUpgradeOutcome(rec.Outcome) {
		return fmt.Errorf("upgrade journal is corrupt")
	}
	if rec.Mode == UpgradeModeFresh {
		if rec.OldGenerationID != 0 || rec.OldGenerationFingerprint != "" || rec.OldVersion != 0 || rec.UserID != "" {
			return fmt.Errorf("fresh upgrade journal has contradictory installed identity")
		}
	} else if rec.OldGenerationID <= 0 ||
		(rec.OldGenerationFingerprint != "" && !validSchemaGenerationFingerprint(rec.OldGenerationFingerprint)) ||
		(rec.Mode == UpgradeModeGeneration && rec.OldGenerationFingerprint == "") {
		return fmt.Errorf("upgrade journal installed generation is corrupt")
	}
	if rec.Mode == UpgradeModeGeneration && rec.TargetGenerationFingerprint == "" {
		return fmt.Errorf("upgrade journal target generation is unconfirmed")
	}
	if rec.ValidatedTargetFingerprint != "" && rec.ValidatedTargetFingerprint != rec.TargetGenerationFingerprint {
		return fmt.Errorf("upgrade journal target validation witness is contradictory")
	}
	if rec.LocalOnlyDigest != "" && !validUpgradePreservationDigest(rec.LocalOnlyDigest) {
		return fmt.Errorf("upgrade journal local-only witness is corrupt")
	}
	if rec.FailureCode != "" && rec.FailureCode != string(rec.Outcome) {
		return fmt.Errorf("upgrade journal failure outcome is contradictory")
	}
	switch rec.ResolutionAction {
	case "":
		if rec.ResolutionRequestHash != "" || rec.ResolutionPayload != "" {
			return fmt.Errorf("upgrade journal has orphaned resolution evidence")
		}
	case PendingIntentAcceptServer, PendingIntentKeepLocal:
		if rec.Phase != UpgradePhaseResolvingOldIntent || rec.ResolutionRequestHash == "" || rec.ResolutionPayload != "" {
			return fmt.Errorf("upgrade journal resolution evidence is corrupt")
		}
	case PendingIntentKeepMerged:
		if rec.Phase != UpgradePhaseResolvingOldIntent || rec.ResolutionRequestHash == "" || rec.ResolutionPayload == "" {
			return fmt.Errorf("upgrade journal merged resolution evidence is corrupt")
		}
	default:
		return fmt.Errorf("upgrade journal resolution action is invalid")
	}
	if rec.Mode != UpgradeModeGeneration {
		if rec.Phase != UpgradePhaseMigratingLocal && rec.Phase != UpgradePhaseReady {
			return fmt.Errorf("local upgrade journal phase is contradictory")
		}
		if rec.OldSourceID != "" || rec.NewSourceID != "" || rec.DrainComplete || rec.ReplacementConfirmed || rec.SnapshotID != "" ||
			rec.SnapshotBundleSeq != 0 || rec.SnapshotRowCount != 0 || rec.SnapshotByteCount != 0 ||
			rec.SnapshotRowsStaged != 0 || rec.SnapshotBytesStaged != 0 || rec.MigrationComplete != (rec.Phase == UpgradePhaseReady) {
			return fmt.Errorf("local upgrade journal contains contradictory remote state")
		}
		return nil
	}
	if rec.UserID == "" || validateSourceID(rec.OldSourceID) != nil {
		return fmt.Errorf("generation upgrade journal source identity is corrupt")
	}
	switch rec.Phase {
	case UpgradePhaseStarted:
		if rec.DrainComplete || rec.MigrationComplete || rec.NewSourceID != "" || rec.ReplacementConfirmed || rec.SnapshotID != "" {
			return fmt.Errorf("generation upgrade journal pre-migration state is contradictory")
		}
	case UpgradePhaseResolvingOldIntent:
		if rec.MigrationComplete || rec.NewSourceID != "" || rec.ReplacementConfirmed || rec.SnapshotID != "" ||
			(rec.DrainComplete && (rec.PendingRequestHash != "" || rec.ResolutionAction != "")) {
			return fmt.Errorf("generation upgrade journal old-intent state is contradictory")
		}
	case UpgradePhaseMigratingLocal:
		if !rec.DrainComplete || rec.MigrationComplete || rec.NewSourceID != "" || rec.ReplacementConfirmed || rec.SnapshotID != "" {
			return fmt.Errorf("generation upgrade journal migration state is contradictory")
		}
	case UpgradePhaseLocalMigrated:
		if !upgradeJournalHasMigrationWitness(rec) || rec.NewSourceID != "" || rec.ReplacementConfirmed || rec.SnapshotID != "" {
			return fmt.Errorf("generation upgrade journal local-migrated state is contradictory")
		}
	case UpgradePhaseReattachPending:
		if !upgradeJournalHasMigrationWitness(rec) || !upgradeJournalHasReplacementTuple(rec) || rec.ReplacementConfirmed || rec.SnapshotID != "" {
			return fmt.Errorf("generation upgrade journal replacement-pending state is contradictory")
		}
	case UpgradePhaseRebuildRunning:
		if !upgradeJournalHasMigrationWitness(rec) || !upgradeJournalHasReplacementTuple(rec) || !rec.ReplacementConfirmed || rec.SnapshotID == "" {
			return fmt.Errorf("generation upgrade journal rebuild state is contradictory")
		}
	case UpgradePhaseReady:
		if !upgradeJournalHasMigrationWitness(rec) || !upgradeJournalHasReplacementTuple(rec) || !rec.ReplacementConfirmed || rec.SnapshotID == "" ||
			rec.SnapshotRowsStaged != rec.SnapshotRowCount || rec.SnapshotBytesStaged != rec.SnapshotByteCount {
			return fmt.Errorf("completed generation upgrade journal is contradictory")
		}
	default:
		return fmt.Errorf("generation upgrade journal phase is contradictory")
	}
	return nil
}

func validUpgradePreservationDigest(value string) bool {
	separator := strings.IndexByte(value, ':')
	if separator < 1 || separator == len(value)-1 {
		return false
	}
	if _, err := strconv.ParseUint(value[:separator], 10, 64); err != nil {
		return false
	}
	return validSchemaGenerationFingerprint(value[separator+1:])
}

func upgradeJournalHasMigrationWitness(rec *upgradeJournalRecord) bool {
	return rec != nil && rec.DrainComplete && rec.MigrationComplete &&
		rec.ValidatedTargetFingerprint == rec.TargetGenerationFingerprint && rec.LocalOnlyDigest != ""
}

func upgradeJournalHasReplacementTuple(rec *upgradeJournalRecord) bool {
	return rec != nil && validateSourceID(rec.NewSourceID) == nil && rec.NewSourceID != rec.OldSourceID &&
		rec.ReplacementReason == "schema_generation_upgrade"
}

func validUpgradeOutcome(outcome UpgradeOutcome) bool {
	switch outcome {
	case "", UpgradeOutcomeReady, UpgradeOutcomeWaitingForNetwork, UpgradeOutcomeNeedsResolution,
		UpgradeOutcomeCancelled, UpgradeOutcomeBusy, UpgradeOutcomePreconditionFailed, UpgradeOutcomePlanMismatch,
		UpgradeOutcomeOldGenerationUnavailable, UpgradeOutcomeTargetGenerationUnavailable,
		UpgradeOutcomeLocalMigrationFailed, UpgradeOutcomeLocalContractMismatch,
		UpgradeOutcomeLocalPreservationFailed, UpgradeOutcomeIntegrityFailed, UpgradeOutcomeReplacementDiverged:
		return true
	default:
		return false
	}
}

func validUpgradeMode(mode UpgradeMode) bool {
	switch mode {
	case UpgradeModeGeneration, UpgradeModeFresh, UpgradeModeLocalOnly:
		return true
	default:
		return false
	}
}

func validUpgradePhase(phase UpgradePhase) bool {
	switch phase {
	case UpgradePhaseReady, UpgradePhaseStarted, UpgradePhaseResolvingOldIntent,
		UpgradePhaseMigratingLocal, UpgradePhaseLocalMigrated,
		UpgradePhaseReattachPending, UpgradePhaseRebuildRunning:
		return true
	default:
		return false
	}
}

func upgradeProof(id int64, fingerprint string) oversync.SchemaGenerationProof {
	return oversync.SchemaGenerationProof{ID: oversync.SchemaGenerationID(id), Fingerprint: fingerprint}
}

func inspectUpgradeConstructionGate(ctx context.Context, db queryRower, proof oversync.SchemaGenerationProof) (bool, error) {
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_sync_upgrade_journal'`).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to inspect upgrade state: %w", err)
	}
	if exists == 0 {
		return false, nil
	}
	journal, err := loadUpgradeJournal(ctx, db)
	if err != nil {
		return true, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if journal != nil && journal.Phase != UpgradePhaseReady {
		return true, &UpgradeError{Outcome: UpgradeOutcomePreconditionFailed, Cause: fmt.Errorf("coordinated opening is pending in phase %q", journal.Phase)}
	}
	if journal != nil && (journal.TargetGenerationID != int64(proof.ID) ||
		journal.TargetGenerationFingerprint != "" && journal.TargetGenerationFingerprint != proof.Fingerprint) {
		return true, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("configured generation does not match installed ready identity")}
	}
	identity, err := loadUpgradeIdentity(ctx, db)
	if err != nil {
		return true, &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if identity != nil && (!identity.Ready || identity.GenerationID != int64(proof.ID) || identity.GenerationFingerprint != proof.Fingerprint) {
		return true, &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("configured generation does not match installed identity")}
	}
	return journal != nil || identity != nil, nil
}

func validateReadyClientIdentity(ctx context.Context, db queryRower, config *Config) error {
	if config == nil || config.DatabaseID == "" || config.ServiceID == "" || config.LocalVersion < 0 {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("migration-aware client identity fields are invalid")}
	}
	identity, err := loadUpgradeIdentity(ctx, db)
	if err != nil {
		return &UpgradeError{Outcome: UpgradeOutcomeIntegrityFailed, Cause: err}
	}
	if identity == nil || !identity.Ready || identity.DatabaseID != config.DatabaseID || identity.ServiceID != config.ServiceID ||
		identity.GenerationID != int64(config.generationProof.ID) || identity.GenerationFingerprint != config.generationProof.Fingerprint ||
		identity.LocalVersion != config.LocalVersion {
		return &UpgradeError{Outcome: UpgradeOutcomePlanMismatch, Cause: fmt.Errorf("migration-aware client configuration does not match durable installed identity")}
	}
	return nil
}

func upgradeJournalSummary(rec *upgradeJournalRecord) string {
	if rec == nil {
		return ""
	}
	return strings.Join([]string{string(rec.Mode), string(rec.Phase), rec.PlanID, rec.PlanDigest}, ":")
}
