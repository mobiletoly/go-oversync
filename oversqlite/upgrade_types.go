package oversqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/mobiletoly/go-oversync/oversync"
)

// LocalMigrationReader is the read-only view supplied to a local migrator.
// The coordinator enforces read-only SQL with SQLite's authorizer, not by
// relying on the method names alone.
type LocalMigrationReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// LocalMigrationTx is the transaction-scoped capability supplied to a local
// migrator. Transaction control and coordinator-owned state are unavailable.
type LocalMigrationTx interface {
	LocalMigrationReader
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// LocalMigrationStep describes the one atomic application-owned migration edge.
type LocalMigrationStep struct {
	PlanID          string
	OriginalVersion int64
	FromVersion     int64
	ToVersion       int64
	TargetVersion   int64
	Fresh           bool
}

// LocalMigrator owns application DDL, data changes, and its durable version
// ledger. Migrate executes inside the coordinator-owned SQLite transaction.
type LocalMigrator interface {
	InstalledVersion(context.Context, LocalMigrationReader) (int64, error)
	Migrate(context.Context, LocalMigrationTx, LocalMigrationStep) error
}

// LocalColumnCodec identifies the exact permitted SQLite storage/codec pairing.
type LocalColumnCodec string

const (
	LocalCodecBooleanInteger LocalColumnCodec = "boolean_integer"
	LocalCodecInt64Integer   LocalColumnCodec = "int64_integer"
	LocalCodecDecimalText    LocalColumnCodec = "decimal_text"
	LocalCodecFloat64Real    LocalColumnCodec = "float64_real"
	LocalCodecText           LocalColumnCodec = "text"
	LocalCodecBytesBlob      LocalColumnCodec = "bytes_blob"
	LocalCodecUUIDText       LocalColumnCodec = "uuid_text"
	LocalCodecUUIDBlob       LocalColumnCodec = "uuid_blob"
	LocalCodecJSONText       LocalColumnCodec = "json_text"
)

// LocalColumnContract selects the local codec for one generation-visible column.
type LocalColumnContract struct {
	Name  string           `json:"name"`
	Codec LocalColumnCodec `json:"codec"`
}

// LocalGenerationContract is the complete immutable declaration needed to
// compile and physically validate one local generation.
type LocalGenerationContract struct {
	Proof            oversync.SchemaGenerationProof `json:"proof"`
	RegisteredTables []oversync.RegisteredTable     `json:"registered_tables"`
	Generation       oversync.SchemaGeneration      `json:"generation"`
	Tables           []SyncTable                    `json:"tables"`
	Columns          []LocalTableColumns            `json:"columns"`
}

// LocalTableColumns contains codec choices for one local table.
type LocalTableColumns struct {
	Schema  string                `json:"schema"`
	Table   string                `json:"table"`
	Columns []LocalColumnContract `json:"columns"`
}

// UpgradeMode distinguishes generation replacement from the two local modes.
type UpgradeMode string

const (
	UpgradeModeGeneration UpgradeMode = "generation"
	UpgradeModeFresh      UpgradeMode = "fresh"
	UpgradeModeLocalOnly  UpgradeMode = "local_only"
)

// UpgradePhase is the durable semantic phase of an upgrade.
type UpgradePhase string

const (
	UpgradePhaseReady              UpgradePhase = "ready"
	UpgradePhaseStarted            UpgradePhase = "upgrade_started"
	UpgradePhaseResolvingOldIntent UpgradePhase = "resolving_old_intent"
	UpgradePhaseMigratingLocal     UpgradePhase = "migrating_local"
	UpgradePhaseLocalMigrated      UpgradePhase = "local_migrated"
	UpgradePhaseReattachPending    UpgradePhase = "reattach_pending"
	UpgradePhaseRebuildRunning     UpgradePhase = "rebuild_running"
)

// UpgradeOutcome is stable application-visible orchestration status.
type UpgradeOutcome string

const (
	UpgradeOutcomeReady                       UpgradeOutcome = "ready"
	UpgradeOutcomeWaitingForNetwork           UpgradeOutcome = "waiting_for_network"
	UpgradeOutcomeNeedsResolution             UpgradeOutcome = "needs_resolution"
	UpgradeOutcomeCancelled                   UpgradeOutcome = "cancelled"
	UpgradeOutcomeBusy                        UpgradeOutcome = "upgrade_busy"
	UpgradeOutcomePreconditionFailed          UpgradeOutcome = "upgrade_precondition_failed"
	UpgradeOutcomePlanMismatch                UpgradeOutcome = "upgrade_plan_mismatch"
	UpgradeOutcomeOldGenerationUnavailable    UpgradeOutcome = "old_generation_unavailable"
	UpgradeOutcomeTargetGenerationUnavailable UpgradeOutcome = "target_generation_unavailable"
	UpgradeOutcomeLocalMigrationFailed        UpgradeOutcome = "local_migration_failed"
	UpgradeOutcomeLocalContractMismatch       UpgradeOutcome = "local_contract_mismatch"
	UpgradeOutcomeLocalPreservationFailed     UpgradeOutcome = "local_preservation_failed"
	UpgradeOutcomeIntegrityFailed             UpgradeOutcome = "upgrade_integrity_failed"
	UpgradeOutcomeReplacementDiverged         UpgradeOutcome = "replacement_diverged"
)

// UpgradePlan is an exact directed application-owned migration edge.
// Executable migrator code is deliberately excluded from the digest; changing
// its behavior requires a new PlanID.
type UpgradePlan struct {
	PlanID                string                      `json:"plan_id"`
	Mode                  UpgradeMode                 `json:"mode"`
	DatabaseID            string                      `json:"database_id"`
	ServiceID             string                      `json:"service_id"`
	UserID                string                      `json:"user_id,omitempty"`
	InstalledGenerationID oversync.SchemaGenerationID `json:"installed_generation_id"`
	TargetGenerationID    oversync.SchemaGenerationID `json:"target_generation_id"`
	InstalledVersion      int64                       `json:"installed_version"`
	TargetVersion         int64                       `json:"target_version"`
	LocalOnlyTables       []string                    `json:"local_only_tables"`
	Migrator              LocalMigrator               `json:"-"`
	testInstalled         LocalGenerationContract
	testTarget            LocalGenerationContract
}

// OpeningConfig supplies the database location, desired ready-client declaration,
// and, when required, the immutable transition plan.
type OpeningConfig struct {
	DatabasePath       string
	Plan               *UpgradePlan
	BaseURL            string
	Token              func(context.Context) (string, error)
	HTTP               *http.Client
	TargetClientConfig *Config
	Progress           func(OpeningStatus)

	testBoundary func(string)
}

// openingRuntimeConfig is the private, deep-cloned configuration consumed by
// the retained opening state machine.
type openingRuntimeConfig struct {
	Plan               UpgradePlan
	PlanProvided       bool
	BaseURL            string
	Token              func(context.Context) (string, error)
	HTTP               *http.Client
	OldClientConfig    *Config
	TargetClientConfig *Config
	Progress           func(OpeningStatus)
	testBoundary       func(string)
}

// OpeningStatus is a semantic view of durable state; it does not expose the
// private SQLite journal layout.
type OpeningStatus struct {
	Mode                   UpgradeMode
	Phase                  UpgradePhase
	Revision               int64
	PlanID                 string
	PlanDigest             string
	OldProof               oversync.SchemaGenerationProof
	TargetProof            oversync.SchemaGenerationProof
	InstalledVersion       int64
	TargetVersion          int64
	PendingDirtyRows       int64
	PendingOutboxRows      int64
	DrainComplete          bool
	LocalMigrationComplete bool
	OldSourceID            string
	NewSourceID            string
	ReplacementConfirmed   bool
	SnapshotID             string
	SnapshotRowCount       int64
	SnapshotRowsStaged     int64
	SnapshotBundleSeq      int64
	Outcome                UpgradeOutcome
	PendingRequestHash     string
	NextAction             string
}

// OpeningResult is returned by Advance and explicit resolution calls.
type OpeningResult struct {
	Status OpeningStatus
	Ready  *Client
}

// UpgradeError preserves a diagnostic cause while exposing a stable outcome.
type UpgradeError struct {
	Outcome UpgradeOutcome
	Cause   error
}

func (e *UpgradeError) Error() string {
	if e == nil {
		return "oversqlite upgrade failed"
	}
	if e.Cause == nil {
		return string(e.Outcome)
	}
	return string(e.Outcome) + ": " + e.Cause.Error()
}

func (e *UpgradeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// PendingIntentResolutionAction is an explicit old-intent decision.
type PendingIntentResolutionAction string

const (
	PendingIntentRetry        PendingIntentResolutionAction = "retry"
	PendingIntentDiscard      PendingIntentResolutionAction = "discard"
	PendingIntentAcceptServer PendingIntentResolutionAction = "accept_server"
	PendingIntentKeepLocal    PendingIntentResolutionAction = "keep_local"
	PendingIntentKeepMerged   PendingIntentResolutionAction = "keep_merged"
)

// PendingIntentResolution binds an application decision to the exact durable
// request and journal revision observed in Status.
type PendingIntentResolution struct {
	Action        PendingIntentResolutionAction
	PlanID        string
	Revision      int64
	RequestHash   string
	MergedPayload json.RawMessage
}

type deferUpgradeConflictResolution struct{}

func (deferUpgradeConflictResolution) isMergeResult() {}

func classifyUpgradeCause(err error, target bool) UpgradeOutcome {
	if err == nil {
		return UpgradeOutcomeReady
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return UpgradeOutcomeCancelled
	}
	var busy *DatabaseAlreadyOwnedError
	if errors.As(err, &busy) {
		return UpgradeOutcomeBusy
	}
	var diverged *SourceReplacementDivergedError
	if errors.As(err, &diverged) {
		return UpgradeOutcomeReplacementDiverged
	}
	var conflict *PushConflictError
	var coverage *PushEffectCoverageError
	var rejection *PushRejectionNeedsResolutionError
	var invalidResolution *InvalidConflictResolutionError
	var conflictExhausted *PushConflictRetryExhaustedError
	if errors.As(err, &conflict) || errors.As(err, &coverage) || errors.As(err, &rejection) || errors.As(err, &invalidResolution) || errors.As(err, &conflictExhausted) {
		return UpgradeOutcomeNeedsResolution
	}
	var exhausted *RetryExhaustedError
	if errors.As(err, &exhausted) {
		return UpgradeOutcomeWaitingForNetwork
	}
	if isRetryableOperationError(context.Background(), err) {
		return UpgradeOutcomeWaitingForNetwork
	}
	var protocolMismatch *ProtocolVersionMismatchError
	var lifecycleUnsupported *AttachLifecycleUnsupportedError
	var generationRequest *oversync.SchemaGenerationRequestError
	var httpFailure *retryHTTPError
	if errors.As(err, &protocolMismatch) || errors.As(err, &lifecycleUnsupported) || errors.As(err, &generationRequest) || errors.As(err, &httpFailure) {
		if target {
			return UpgradeOutcomeTargetGenerationUnavailable
		}
		return UpgradeOutcomeOldGenerationUnavailable
	}
	return UpgradeOutcomeIntegrityFailed
}
