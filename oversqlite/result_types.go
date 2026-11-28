package oversqlite

import "github.com/mobiletoly/go-oversync/oversync"

// DetachOutcome describes the lifecycle outcome of Detach.
type DetachOutcome string

const (
	// DetachOutcomeDetached means Detach completed successfully.
	DetachOutcomeDetached DetachOutcome = "detached"
	// DetachOutcomeBlockedUnsyncedData means Detach was refused because pending attached sync state still exists.
	DetachOutcomeBlockedUnsyncedData DetachOutcome = "blocked_unsynced_data"
)

// DetachResult reports the outcome of Detach.
type DetachResult struct {
	Outcome         DetachOutcome
	PendingRowCount int64
}

// AuthorityStatus describes the authoritative materialization state for the currently connected scope.
type AuthorityStatus string

const (
	// AuthorityStatusLocalOnly means local data has not been attached to a server user.
	AuthorityStatusLocalOnly AuthorityStatus = "local_only"
	// AuthorityStatusPendingLocalSeed means an attached scope still has a pending initialization seed upload.
	AuthorityStatusPendingLocalSeed AuthorityStatus = "pending_local_seed"
	// AuthorityStatusAuthoritativeEmpty means the connected scope has no live authoritative rows materialized locally.
	AuthorityStatusAuthoritativeEmpty AuthorityStatus = "authoritative_empty"
	// AuthorityStatusAuthoritativeMaterialized means the connected scope has authoritative rows materialized locally.
	AuthorityStatusAuthoritativeMaterialized AuthorityStatus = "authoritative_materialized"
)

// SyncStatus reports durable local and attachment state without network access.
type SyncStatus struct {
	ContractConfirmed bool
	NeedsResolution   bool
	Authority         AuthorityStatus
	Pending           PendingSyncStatus
	LastBundleSeqSeen int64
	ActionRequired    ApplicationActionRequired
}

// ApplicationAction identifies a durable condition that automatic sync cannot
// resolve without an application-owned workflow.
type ApplicationAction string

const (
	// ApplicationActionNone means automatic sync has not recorded an action gate
	// for the client's current schema generation.
	ApplicationActionNone ApplicationAction = ""
	// ApplicationActionUpgrade means the current generation was retired and the
	// application must run its preserving upgrade workflow.
	ApplicationActionUpgrade ApplicationAction = "upgrade_application"
)

// ApplicationActionRequired is local durable state. It can be inspected even
// when connecting to the server is impossible or automatic loops have stopped.
type ApplicationActionRequired struct {
	Action     ApplicationAction
	Generation oversync.SchemaGenerationProof
}

// Required reports whether an application-owned action is pending.
func (s ApplicationActionRequired) Required() bool { return s.Action != ApplicationActionNone }

// RestoreSummary summarizes a snapshot-based restore applied locally.
type RestoreSummary struct {
	BundleSeq int64
	RowCount  int64
}

// PushOutcome describes the public outcome of PushPending.
type PushOutcome string

const (
	// PushOutcomeNoChange means no pushable work was committed.
	PushOutcomeNoChange PushOutcome = "no_change"
	// PushOutcomeCommitted means a committed push bundle was uploaded and replayed locally.
	PushOutcomeCommitted PushOutcome = "committed"
	// PushOutcomeSkippedPaused means uploads are paused and no push work was attempted after precondition validation.
	PushOutcomeSkippedPaused PushOutcome = "skipped_paused"
)

// PushReport reports the public result of PushPending.
type PushReport struct {
	Outcome PushOutcome
	Status  SyncStatus
}

// RemoteSyncOutcome describes the public outcome of PullToStable and Rebuild.
type RemoteSyncOutcome string

const (
	// RemoteSyncOutcomeAlreadyAtTarget means no newer remote state needed to be applied.
	RemoteSyncOutcomeAlreadyAtTarget RemoteSyncOutcome = "already_at_target"
	// RemoteSyncOutcomeAppliedIncremental means committed bundles were applied incrementally.
	RemoteSyncOutcomeAppliedIncremental RemoteSyncOutcome = "applied_incremental"
	// RemoteSyncOutcomeAppliedSnapshot means a snapshot restore was applied.
	RemoteSyncOutcomeAppliedSnapshot RemoteSyncOutcome = "applied_snapshot"
	// RemoteSyncOutcomeSkippedPaused means downloads are paused and no remote-sync work was attempted after precondition validation.
	RemoteSyncOutcomeSkippedPaused RemoteSyncOutcome = "skipped_paused"
)

// RemoteSyncReport reports the public result of PullToStable and Rebuild.
type RemoteSyncReport struct {
	Outcome RemoteSyncOutcome
	Status  SyncStatus
	Restore *RestoreSummary
}

// SyncReport reports the public result of Sync.
type SyncReport struct {
	PushOutcome   PushOutcome
	RemoteOutcome RemoteSyncOutcome
	Status        SyncStatus
	Restore       *RestoreSummary
}

// SyncThenDetachResult reports the public result of SyncThenDetach.
type SyncThenDetachResult struct {
	// LastSync is absent when no round completed a full remote sync.
	LastSync                 *SyncReport
	Detach                   DetachResult
	SyncRounds               int
	RemainingPendingRowCount int64
	BlockedReason            DetachBlockedReason
}

type DetachBlockedReason string

const (
	DetachBlockedPushAttemptLimit DetachBlockedReason = "push_attempt_limit"
	DetachBlockedNoProgress       DetachBlockedReason = "no_progress"
	DetachBlockedUploadsPaused    DetachBlockedReason = "uploads_paused"
	DetachBlockedDownloadsPaused  DetachBlockedReason = "downloads_paused"
	DetachBlockedSyncRoundLimit   DetachBlockedReason = "sync_round_limit"
)

// IsSuccess reports whether SyncThenDetach finished with a successful detach.
func (r SyncThenDetachResult) IsSuccess() bool {
	return r.Detach.Outcome == DetachOutcomeDetached
}

// SourceInfo exposes debug-only source diagnostics for the current local runtime state.
// CurrentSourceID is opaque and must not be treated as a host-controlled lifecycle surface.
type SourceInfo struct {
	CurrentSourceID        string
	RebuildRequired        bool
	SourceRecoveryRequired bool
	SourceRecoveryReason   SourceRecoveryCode
}
