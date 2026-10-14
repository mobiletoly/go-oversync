# oversqlite

`oversqlite` is the SQLite client runtime for go-oversync's bundle-based sync protocol.

The client captures local writes with triggers, freezes one outbound bundle at a time, sends that
bundle in one bounded `POST /sync/push` request, applies authoritative response rows locally, pulls committed remote
bundles, and rebuilds from chunked snapshots when incremental history is unavailable or when a
durable rebuild gate is active.

## Managed Source Identity

`source_id` is first-class on the wire. Authenticated sync requests sent by `oversqlite` carry
`Oversync-Source-ID`. The application selects a generation ID plus stable database and service
identities. `Open` is local-first: it validates and retains the local runtime without contacting the
server. During `Attach`, the client discovers the configured generation, verifies its descriptor
against the local SQLite schema, and retains the confirmed fingerprint. Subsequent data-plane requests
carry the immutable `Oversync-Schema-Generation-ID` and
`Oversync-Schema-Generation-Fingerprint` proof. The host-facing contract is:

- `oversqlite` generates and persists source identity internally
- applications do not call `Open(ctx, sourceID)`
- applications do not rotate source identity manually
- applications do not provide replacement source ids during rebuild
- applications configure a generation ID, database ID, and service ID rather than constructing a proof

The durable source/runtime state lives in:

- `_sync_source_state`
- `_sync_attachment_state.current_source_id`
- `_sync_operation_state`

## Public API

Lifecycle:

- `Open(ctx) error`
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

The default resolver durably defers every conflict. `ServerWinsResolver` and
`ClientWinsResolver` are explicit opt-in policies.
A `ConflictNeedsResolutionError` preserves the frozen request and local intent.
Inspect `PendingConflict`, retain its identity and revision for an explicit decision,
and pass that value to `ResolveConflict`. Stale or invalid decisions preserve the
request. Retry exhaustion also preserves it. `RetryConflict` clears only the
observation so a later `PushPending` can retry the exact request. Resolving a conflict
performs no remote write; call `PushPending` afterward. Newer local edits remain protected.
Malformed or mismatched server evidence never authorizes a decision.
Missing `server_row` is invalid and is distinct from an explicit JSON null. A valid
decision rewrites state and clears the outbox and conflict observation in one transaction;
failure or cancellation rolls the complete decision back.

Definitive application-level push rejections:

- `PendingPushRejection(ctx) (*PendingPushRejection, error)`
- `RetryPushRejection(ctx, selected PendingPushRejection) error`
- `DiscardPushRejection(ctx, selected PendingPushRejection) error`

HTTP 422 preserves the exact frozen request and returns
`PushRejectionNeedsResolutionError`. Retry keeps that request unchanged. Discard first replays
the selected whole frozen request to determine whether it committed, then abandons it
while retaining newer dirty rows. A stale or ambiguous decision changes nothing.

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

Require no prior lifecycle step:

- `Open(ctx)`

Require successful `Open(ctx)`:

- `Attach(ctx, userID)`
- `PendingSyncStatus(ctx)`
- `SourceInfo(ctx)`
- `UninstallSync(ctx)`

Require successful `Open(ctx)` plus durable attached local state:

- `Detach(ctx)`

Require successful `Open(ctx)` plus successful `Attach(ctx, userID)`:

- `PushPending(ctx)`
- `PullToStable(ctx)`
- `Sync(ctx)`
- `Rebuild(ctx)`
- `SyncStatus(ctx)`
- `LastBundleSeqSeen(ctx)`
- `SyncThenDetach(ctx)`

## Recovery

- `PullToStable()` falls back to authoritative snapshot rebuild for pull-side `history_pruned` and
  `checkpoint_ahead`
- checkpoint recovery durably records `rebuild_required` before the snapshot and normal `Sync()` or
  `PullToStable()` automatically resumes it after interruption
- pending local work is frozen into the durable outbox and uploaded first; direct recovery can
  return `CheckpointRecoveryBlockedError`, while interactive `Sync(ctx)` drains it through the
  shared allowance and retains terminal error precedence
- the durable checkpoint changes only when authoritative snapshot apply completes atomically
- an ambiguous replay pruned below the retained floor uses keep-source rebuild behavior and only
  clears the frozen outbox after recovery can establish the committed tuple
- stale/pruned, out-of-order, changed, and retired source cases still enter durable fail-closed
  source-recovery mode
- `PushPending()`, `PullToStable()`, and `Sync()` fail closed while source recovery is required;
  checkpoint recovery is automatically resumed by pull and sync
- `Rebuild(ctx)` remains explicit and attached/authenticated
- when source recovery is active, `Rebuild(ctx)` internally generates a fresh replacement source and
  preserves frozen outbox intent across the rotated rebuild

Source recovery reasons map to server errors:

- `history_pruned`
- `source_sequence_out_of_order`
- `source_sequence_changed`
- `source_retired`

Applications should not clear this state manually. Call `Rebuild(ctx)` after authentication and
let the client preserve pending outbox intent, rotate source identity when required, and resume
upload under the fresh source after snapshot recovery completes.

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

Empty projected pull bundles remain real envelopes and advance the durable checkpoint. A push reply
lost after server commit is resolved only by replaying the exact complete request or by a complete
same-generation snapshot with unchanged proof/source, no newer local intent, and final transactional
guards. Pruning alone never acknowledges a push.

The client derives operation-specific decoded-body ceilings for push, pull, snapshots, rows, and
structured conflicts from admitted generation discovery and enforces them after gzip
decompression. These are fixed-generation data-plane guarantees. `Opening` adds the durable
upgrade generation-change and same-proof
local migration paths. See [`docs/client-upgrades.md`](../docs/client-upgrades.md) for plan identity,
blocked states, explicit intent resolution, migrator ownership, and restart behavior.

The opening drains old outboxes before application DDL and rotates the source only after the
local migration commits. Separate devices may keep syncing on different server-supported
generations. Changes to synchronized table identity, keys, or foreign keys remain outside upgrade.
Database ownership follows the SQLite file's device/inode identity, so a hard-link alias cannot
overlap a ready client or opening, including when processes have different temporary-directory
settings. Ready-client construction takes that ownership before inspecting mutable installed state.
Opening `Close` does not release ownership until active upgrade work exits. A frozen whole-request
tuple remains durable until its exact replay establishes commit or a definitive rejection permits
the explicit drain; a transient failure cannot be mistaken for a completed drain.

Generation-upgrade snapshot totals are recomputed with bounded pages on restart. Normal chunk
commits advance durable row and encoded-byte progress incrementally, so accounting does not retain
or repeatedly rescan the complete staged snapshot. Unsupported prototype control tables are not
altered during initialization; a pre-`active_push_id` outbox layout fails closed.

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
- `PullToStable()` and `Rebuild(ctx)` fail closed while `_sync_outbox_*` exists
- `Sync()` fails closed while `_sync_attachment_state.rebuild_required = 1`
- `last_bundle_seq_seen` advances only after durable local apply
- exactly one `oversqlite.Client` should own one SQLite database at a time
- `Close()` releases client ownership of the SQLite database; it does not close the underlying
  `*sql.DB`
- `UninstallSync()` is more destructive than `Detach()` because it removes the installed sync
  metadata and triggers from the database

Each push is one complete atomic `POST /sync/push`. The client freezes the request bytes
and source tuple across retryable failures. If a response is uncertain or lost, retry the exact complete request.
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
