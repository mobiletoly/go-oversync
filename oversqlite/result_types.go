package oversqlite

// OpenState describes the lifecycle state restored by Open.
type OpenState string

const (
	// OpenStateReadyAnonymous means the client is open with no attached user scope.
	OpenStateReadyAnonymous OpenState = "ready_anonymous"
	// OpenStateReadyAttached means durable attached local state exists, but Attach(userID) must still reconnect the session.
	OpenStateReadyAttached OpenState = "ready_attached"
	// OpenStateAttachRecoveryRequired means a pending remote-authoritative replace must be finalized by Attach(targetUserID).
	OpenStateAttachRecoveryRequired OpenState = "attach_recovery_required"
)

// OpenResult reports the lifecycle state restored by Open.
type OpenResult struct {
	State          OpenState
	AttachedUserID string
	TargetUserID   string
}

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
	// AuthorityStatusPendingLocalSeed means an attached scope still has a pending initialization seed upload.
	AuthorityStatusPendingLocalSeed AuthorityStatus = "pending_local_seed"
	// AuthorityStatusAuthoritativeEmpty means the connected scope has no live authoritative rows materialized locally.
	AuthorityStatusAuthoritativeEmpty AuthorityStatus = "authoritative_empty"
	// AuthorityStatusAuthoritativeMaterialized means the connected scope has authoritative rows materialized locally.
	AuthorityStatusAuthoritativeMaterialized AuthorityStatus = "authoritative_materialized"
)

// SyncStatus reports the canonical status for the currently connected scope.
type SyncStatus struct {
	Authority         AuthorityStatus
	Pending           PendingSyncStatus
	LastBundleSeqSeen int64
}

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
	LastSync                 SyncReport
	Detach                   DetachResult
	SyncRounds               int
	RemainingPendingRowCount int64
}

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
