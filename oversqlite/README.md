# oversqlite

`oversqlite` is the SQLite client runtime for go-oversync's bundle-based sync protocol.

The client captures local writes with triggers, sends bounded uploads, settles and acknowledges
their outcomes, and pulls committed remote bundles. Clean recovery can install a sealed
snapshot when incremental apply cannot continue.

## Managed Source Identity

`source_id` is first-class on the wire. Authenticated sync requests sent by `oversqlite` carry
`Oversync-Source-ID`. The application selects a generation ID plus stable database and service
identities. Call `NewOpening` and `Advance` to create or open the database. Exact ready reopen and
fresh opening can run offline. During first `Attach`, the client discovers the configured generation,
verifies its descriptor
against the local SQLite schema, and retains the confirmed fingerprint. Subsequent data-plane requests
carry the immutable `Oversync-Schema-Generation-ID` and
`Oversync-Schema-Generation-Fingerprint` proof. The host-facing contract is:

- `oversqlite` generates and persists source identity internally
- applications do not rotate source identity manually
- applications do not provide replacement source ids during rebuild
- applications configure a generation ID, database ID, and service ID rather than constructing a proof

The durable source/runtime state lives in:

- `_sync_source_state`
- `_sync_attachment_state.current_source_id`
- `_sync_operation_state`

## Public API

Database opening:

- `NewOpening(*OpeningConfig) (*Opening, error)`
- `Opening.Advance(ctx) (OpeningResult, error)`
- `OpeningResult.Ready` gives the application its `Client` and borrowed `Database()` pool.

Ready-client lifecycle:

- `Close() error`
- `Attach(ctx, userID) (AttachResult, error)`
- `Detach(ctx) (DetachResult, error)`
- `PendingSyncStatus(ctx) (PendingSyncStatus, error)`
- `SyncStatus(ctx) (SyncStatus, error)`
- `SyncThenDetach(ctx) (SyncThenDetachResult, error)`
- `UninstallSync(ctx) error`

Sync:

- `PushPending(ctx) (PushReport, error)`
- `PullToStable(ctx) (RemoteSyncReport, error)`
- `Sync(ctx) (SyncReport, error)`
- `Rebuild(ctx) (RemoteSyncReport, error)`
- `LastBundleSeqSeen(ctx) (int64, error)`

The `MaxSyncWorkUnitsPerInvocation` name is retained for source compatibility. Its default
1,000,000 value is an internal work-slice target. Ordinary operations continue their
checkpointed local phase under the same caller context; customers do not catch work-yield
errors or reset budgets. Cancellation/deadlines, genuine no-progress and hard memory/wire
limits remain actionable. No default SDK operation timeout is added.

`Sync(ctx)` drains successive bounded push bundles and then pulls. Its 64-attempt allowance
includes prepared-outbox replay, conflict reentry, and checkpoint-recovery pushes. A late local
write resumes the drain. `SyncIncompleteError` reports `push_attempt_limit`, `no_progress`, or
`uploads_paused` with dirty/outbound counts, uniquely confirmed bundles, and attempts used.
Confirmed effects and remaining local intent stay durable. `PushPending(ctx)` still handles one
bundle, and `PullToStable(ctx)` remains fail-closed over pending local work.

`SyncThenDetach(ctx)` shares that allowance across at most three rounds. Its `LastSync` pointer is
nil if no round completed, and blocked results expose a typed `BlockedReason` without detaching.
Direct `PushPending(ctx)` skips while uploads are paused. Direct pull/rebuild skip while downloads
are paused after local safety checks. `Sync(ctx)` reports `uploads_paused` if pending work remains;
with downloads paused it reports a skipped remote phase. `SyncThenDetach(ctx)` blocks rather than
detaching after either paused or incomplete phase. Background `Start(ctx)` follows its own pause
policy and waits without spinning.

Conflict decisions:

- `PendingConflict(ctx) (*PendingConflict, error)`
- `ResolveConflict(ctx, selected PendingConflict, result MergeResult) error`
- `RetryConflict(ctx, selected PendingConflict) error`

v0.5.0 changes the default from server-wins to durable defer. To resolve automatically, set
`ServerWinsResolver`, `ClientWinsResolver`, or an application resolver.

The SDK records the rejection and acknowledges it as a terminal upload outcome. A
`ConflictNeedsResolutionError` leaves the rejected request and local intent available for
inspection. Pass the current `PendingConflict` to `ResolveConflict` or `RetryConflict`. A
valid decision updates local state and preserves newer edits in one transaction.
`RetryConflict` requeues remaining intent for a successor upload. Call `PushPending` after
the local decision. Stale decisions leave pending work intact.

Definitive application-level push rejections:

- `PendingPushRejection(ctx) (*PendingPushRejection, error)`
- `RetryPushRejection(ctx, selected PendingPushRejection) error`
- `DiscardPushRejection(ctx, selected PendingPushRejection) error`

A terminal application rejection preserves its policy evidence and returns
`PushRejectionNeedsResolutionError`. Retry requeues unsatisfied intent for a successor
sequence. Discard abandons the rejected request in a guarded local transaction, preserving
newer dirty rows. The successor upload waits for both the policy decision and acknowledgment.

Diagnostics:

- `SourceInfo(ctx) (SourceInfo, error)`
- `ApplicationActionRequired(ctx) (ApplicationActionRequired, error)`

`SourceInfo` is debug-only. `CurrentSourceID` is opaque and must not be treated as a host-managed
lifecycle surface.

`ApplicationActionRequired` is local-only durable state. Automatic HTTP 426 retirement handling
records `upgrade_application` before stopping upload/download/watch loops, so the application can
prompt for its preserving upgrade workflow even after a process restart.

## Optional Watch Wakeups

`Config.BundleChangeWatchMode` defaults to `BundleChangeWatchOff`. Set
`BundleChangeWatchAuto` to let `Start(ctx)` use `/sync/watch` only when the admitted generation
discovery response sets `watch_enabled=true`.

Watch events are wake-up hints only. `oversqlite` still applies data through `Sync(ctx)`,
`PullToStable(ctx)`, and snapshot recovery; the watch stream never mutates SQLite directly.

## Preconditions

After `Advance(ctx)` returns a ready client:

- `Attach(ctx, userID)`
- `PendingSyncStatus(ctx)`
- `SourceInfo(ctx)`
- `UninstallSync(ctx)`

With durable attached local state:

- `Detach(ctx)`

After successful `Attach(ctx, userID)`:

- `PushPending(ctx)`
- `PullToStable(ctx)`
- `Sync(ctx)`
- `Rebuild(ctx)`
- `SyncStatus(ctx)`
- `LastBundleSeqSeen(ctx)`
- `SyncThenDetach(ctx)`

## Recovery

- `Sync` and `PullToStable` can install a sealed snapshot for eligible history failures when
  automatic recovery is enabled and the scope is clean. The client advances its checkpoint
  only when atomic snapshot apply commits.
- After an uncertain upload, the SDK resolves the original request, applies its outcome locally,
  and acknowledges it. After a lost freeze it can fetch the outcome by source sequence.
  See [Durable upload outcomes](../docs/documentation/durable-upload-outcomes.md).
- Use explicit `Rebuild(ctx)` when the source requires replacement. The client first settles
  any recoverable upload under the original source. An outstanding outcome blocks server
  replacement with `push_outcome_pending`. Eligible remaining intent carries to the new source.
  See [Snapshot recovery](../docs/documentation/snapshot-recovery.md).

## Upload limits and FK diagnostics

`DefaultConfig` uses soft upload targets of 200 rows and 1 MiB. A complete dependency group
can cross a soft target up to the discovery limits. Keep related edits together.

- `FKReconciliationRequiredError` identifies a local dependency that needs correction.
  Inspect its bounded key sample and reconcile the relationships before retrying.
- `PushRowExceedsAdmittedLimitError` and `PushDependencyGroupExceedsAdmittedLimitError`
  leave the blocked work dirty. For a group, use its `DiagnosticID` with
  `ReadPushDependencyDiagnostic(ctx, id, cursor, pageSize)`. Start with an empty cursor
  and follow `NextCursor`. Call `ReleasePushDependencyDiagnostic(ctx, id)` when finished.
  Releasing the diagnostic leaves local intent in place.

For snapshot workspace limits and pull paging, see the [Performance guide](../docs/documentation/performance.md).

## Generation wire values and database compatibility

Every client supplies a generation ID plus ordinary table/key metadata. On attachment, the client
discovers the server-owned generation contract and derives the local conversions internally. It
checks the physical SQLite affinity before admitting that contract; applications do not build
wire contracts or select codecs.

Exact generation tokens use JSON Booleans, bounded non-exponent decimal strings, canonical
`float64` strings, canonical padded standard base64, lowercase dashed UUIDs, Unicode scalar
text except U+0000, and inner JCS bytes without U+0000 carried in an outer JSON string. Outer JSON
`null` means SQL NULL; outer string `"null"` means a non-null JSON document. Invalid values fail
before server hashing, staging, or mutation.
SQLite `REAL` values are local binary floating-point storage, not the wire representation.
PostgreSQL `float4` and `float8` columns both use the `float64` token; committed `float4` values may
reflect PostgreSQL storage normalization. Decimal values remain exact text throughout.
The client hashes the frozen request separately from the authoritative
committed bundle. Its request hash includes the fixed generation ID and fingerprint and covers the
ordered rows supplied by the client, while the server's canonical effects and `row_count` may
differ. The server retains the supplied positive request count separately as
`bundle_log.planned_row_count` for ambiguity-safe replay.

Committed effects are not required to equal frozen intent: PostgreSQL triggers may normalize
values or operations and may add other visible effects. Oversqlite verifies the complete projected
descriptor and requires exactly one final effect for every uploaded key before acknowledging the
outbox. It applies authoritative effects atomically while retaining and rebasing any local edit
made after the outbox was frozen, including a same-valued edit.

Empty projected pull bundles still advance the durable checkpoint. Upload resolution uses the
retained terminal result independently of bundle history. The SDK commits local disposition
before sending acknowledgment under the original binding.

The client derives operation-specific decoded-body ceilings for push, pull, snapshots, rows, and
structured conflicts from admitted generation discovery and enforces them after gzip
decompression. These are fixed-generation data-plane guarantees. `Opening` adds the durable
upgrade generation-change and same-proof
local migration paths. See [`docs/client-upgrades.md`](../docs/client-upgrades.md) for plan identity,
blocked states, explicit intent resolution, migrator ownership, and restart behavior.

The opening drains old outboxes before application DDL and rotates the source only after the
local migration commits. Separate devices may keep syncing on different server-supported
generations. The opening supports compatible table additions and omissions and qualifying
nullable additive foreign keys. Shared identity and foreign-key semantics stay stable.
Database ownership follows the SQLite file's device/inode identity, so a hard-link alias cannot
overlap a ready client or opening, including when processes have different temporary-directory
settings. Ready-client construction takes that ownership before inspecting mutable installed state.
Opening `Close` does not release ownership until active upgrade work exits. A frozen whole-request
tuple stays durable until the client resolves and disposes of the outcome. Generation changes
wait for outstanding acknowledgment and old-generation intent settlement.

Generation-upgrade snapshot totals are recomputed with bounded pages on restart. Normal chunk
commits advance durable row and encoded-byte progress incrementally, so accounting does not retain
or repeatedly rescan the complete staged snapshot. v0.5.0 requires a fresh local database.
See the [release notes](../docs/releases/v0.5.0.md).

## Quick Start

```go
cfg := oversqlite.DefaultConfig(1, []oversqlite.SyncTable{{
	TableName: "people", SyncKeyColumns: []string{"id"},
}})
cfg.DatabaseID = "business"
cfg.ServiceID = "samplesync"
cfg.LocalVersion = 1

tokenProvider := func(ctx context.Context) (string, error) {
	return "<jwt>", nil
}

opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
	DatabasePath:       "app.db",
	Plan:               freshPlan, // exact first-install or upgrade edge
	BaseURL:            "http://localhost:8080",
	Token:              tokenProvider,
	TargetClientConfig: cfg,
})
if err != nil {
	log.Fatal(err)
}
defer opening.Close()

opened, err := opening.Advance(context.Background())
if err != nil {
	log.Fatal(err)
}
client := opened.Ready
if client == nil {
	log.Fatalf("database is not ready: %s", opened.Status.Outcome)
}
defer client.Close()

db := client.Database() // borrowed; quiesce application SQL before terminal operations

attachResult, err := client.Attach(context.Background(), "user-123")
if err != nil {
	log.Fatal(err)
}
if attachResult.Status == oversqlite.AttachStatusRetryLater {
	log.Printf("attach pending; retry after %s", attachResult.RetryAfter)
	return
}

syncReport, err := client.Sync(context.Background())
if err != nil {
	log.Fatal(err)
}
log.Printf("sync outcomes: push=%s remote=%s", syncReport.PushOutcome, syncReport.RemoteOutcome)

if _, err := client.Rebuild(context.Background()); err != nil {
	log.Printf("rebuild failed: %v", err)
}
```

## Invariants

- `_sync_outbox_*` is durable and restart-resumable
- Clean recovery waits for pending uploads, acknowledgment, and local edits to settle
- Explicit source-recovery rebuild can carry eligible intent across source replacement
- `last_bundle_seq_seen` advances only after durable local apply
- exactly one `oversqlite.Client` should own one SQLite database at a time
- `Close()` closes the SQLite pool and releases file ownership. Finish application queries
  and transactions before calling it
- `UninstallSync()` is more destructive than `Detach()` because it removes the installed sync
  metadata and triggers from the database

Each push is one complete atomic `POST /sync/push`. The client freezes the request bytes
and source tuple across retryable failures. For an uncertain dispatch the SDK resolves the
frozen operation. A custom client can retry the exact complete request while its outcome remains
unacknowledged.
There is no server upload session, acknowledged ordinal, staged
prefix, or `_sync_push_progress` state.

### Fresh-database qualification

The work-continuation candidate is qualified on newly created server and client
databases. It adds no database-format change today and does not implement
old-format migrations or dual readers. Current-format close/reopen, crash recovery
and replay remain required. Historical upgrade/retirement suites retain their
recorded status and are outside this scoped qualification.

For a clean adoption environment, create a new PostgreSQL database and point the
server's DATABASE_URL at it, then initialize the application's current business
schema. Close the client and database owners before creating a new SQLite database
with the current application schema and attaching to that authority. This task
recreates only isolated test databases; application data transfer and deployment
are separate work.
