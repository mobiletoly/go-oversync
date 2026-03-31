# oversqlite

`oversqlite` is the SQLite client SDK for go-oversync’s bundle-based sync protocol.

It tracks local dirty rows with triggers, freezes one outbound snapshot at a time, uploads that
snapshot through push sessions, replays the authoritative committed bundle locally, pulls complete
committed bundles, and rebuilds through chunked snapshot sessions when a client needs a full local
refresh.

## Lifecycle model

- Business tables are your application tables.
- `_sync_dirty_rows` stores one coalesced dirty row per structured key.
- `_sync_outbox_bundle` is a singleton state machine for the one durable frozen push snapshot.
- `_sync_outbox_rows` stores the rows belonging to that frozen snapshot.
- `_sync_push_stage` stores authoritative committed rows before final local replay.
- `_sync_row_state` stores the authoritative replicated row version seen by the client.
- `_sync_managed_tables` stores the set of tables for which `oversqlite` has installed local
  change-capture triggers.
- `_sync_source_state` stores per-source durable sequencing state:
  - `source_id`
  - `next_source_bundle_id`
  - `replaced_by_source_id`
- `_sync_attachment_state` stores:
  - `current_source_id`
  - `schema_name`
  - `last_bundle_seq_seen`
  - `rebuild_required`
  - anonymous vs attached binding state
  - `pending_initialization_id`
- `_sync_operation_state` stores local-only lifecycle recovery state for pending `remote_replace`.
- `_sync_apply_state` stores trigger suppression state during authoritative local apply.

Public lifecycle surface:

- `Open(ctx, sourceID) (OpenResult, error)` is local-only and runs on every app launch.
- the first anonymous bootstrap capture happens only when `current_source_id` was previously blank;
  reopening after `Detach(ctx)` does not rerun a full preexisting scan
- `Attach(ctx, userID) (AttachResult, error)` resolves account attachment through `POST /sync/connect`.
- `Detach(ctx) (DetachResult, error)` reports blocked-vs-detached as a normal result and restores
  trigger capture before returning.
- `SyncStatus(ctx) (SyncStatus, error)` reports authority state, pending state, and last seen
  bundle checkpoint for the currently connected scope.
- `SyncThenDetach(ctx) (SyncThenDetachResult, error)` is a bounded best-effort helper over
  `Sync(ctx)` plus `Detach(ctx)`.
- `PendingSyncStatus(ctx)` distinguishes anonymous pending rows from detach-blocking attached rows.
- `RotateSource(ctx, sourceID) (SourceRotationResult, error)` is explicit caller-owned source
  rotation. It preserves plain dirty rows, but it is blocked by active outbox state,
  initialization, or pending durable lifecycle recovery.
- `UninstallSync(ctx)` is the explicit full local teardown path for removing `oversqlite` metadata
  tables and triggers from the SQLite database.

`AttachStatusRetryLater` is a normal retriable attachment outcome, not an auth failure. It means
the server has not finished resolving first-connect authority yet.

## Built-in transient retry

`oversqlite` owns bounded retries for transient transport and temporary server-availability
failures on sync I/O. If `Config.RetryPolicy` is left unset, the client uses a conservative
built-in default:

- `MaxAttempts: 3`
- `InitialBackoff: 100ms`
- `MaxBackoff: 1s`
- `JitterFraction: 0.2`

Internal retry covers:

- `PushPending(ctx)`
- `PullToStable(ctx)`
- snapshot session creation and snapshot chunk fetches during
  `Rebuild(ctx, RebuildKeepSource, "")` / `Rebuild(ctx, RebuildRotateSource, newSourceID)`
- transport-level reachability failures while `Attach(ctx, userID)` is calling `/sync/connect`

Internal retry does not hide caller-visible lifecycle or protocol outcomes:

- server `AttachStatusRetryLater`
- `401` / `403`
- capability mismatch / old-server incompatibility
- conflict / validation failures
- `Detach(ctx)`, `RotateSource(ctx, sourceID)`, `UninstallSync(ctx)`

Set `Config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}` to disable the built-in retry
layer explicitly.

## One-shot operations

- `PushPending(ctx) (PushReport, error)` freezes `_sync_dirty_rows` into `_sync_outbox_*`, uploads through
  `/sync/push-sessions`, fetches the committed authoritative rows, and replays them locally.
- `_sync_outbox_bundle.state` is one of `none`, `prepared`, or `committed_remote`.
- the client durably keeps at most one frozen outbox bundle at a time.
- if the process dies after freezing or after remote commit but before local replay, the next
  `PushPending(ctx)` resumes from `_sync_outbox_*` instead of recollecting a fresh local bundle.
- `PullToStable(ctx) (RemoteSyncReport, error)` pulls complete committed bundles until the frozen
  stable ceiling is reached. If the local checkpoint falls behind the retained bundle floor, it
  rebuilds from chunked snapshot sessions.
- `Sync(ctx) (SyncReport, error)` runs `PushPending(ctx)` and then `PullToStable(ctx)`.
- `Rebuild(ctx, RebuildKeepSource, "") (RemoteSyncReport, error)` rebuilds from
  `POST /sync/snapshot-sessions` plus chunk fetches without rotating source identity.
- `Rebuild(ctx, RebuildRotateSource, newSourceID) (RemoteSyncReport, error)` rebuilds from chunked
  snapshot sessions and atomically rotates to a caller-provided new source ID.

## Lifecycle Preconditions

Require only `Open(ctx, sourceID)`:

- `Open(ctx, sourceID)`
- `PendingSyncStatus(ctx)`
- `RotateSource(ctx, sourceID)`
- `UninstallSync(ctx)`

Require successful `Open(ctx, sourceID)` and successful `Attach(ctx, userID)`:

- `PushPending(ctx)`
- `PullToStable(ctx)`
- `Sync(ctx)`
- `Rebuild(ctx, RebuildKeepSource, "")`
- `Rebuild(ctx, RebuildRotateSource, newSourceID)`
- `LastBundleSeqSeen(ctx)`
- `Detach(ctx)`
- `SyncThenDetach(ctx)`

Typed precondition errors:

- `OpenRequiredError`: returned when an operation needs `Open(ctx, sourceID)` first
- `AttachRequiredError`: returned when attached-state sync operations run before successful
  `Attach(ctx, userID)`
- `DestructiveTransitionInProgressError`: returned while destructive lifecycle transitions block
  safe sync execution

Expected lifecycle/sync branches such as retry-later attach, blocked detach, paused push/pull, and
already-at-target pull are reported as `error == nil` with structured result values instead of typed
control-flow errors.

## Quick start

```go
db, err := sql.Open("sqlite3", "app.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
    {TableName: "users", SyncKeyColumnName: "id"},
    {TableName: "posts", SyncKeyColumnName: "id"},
})

tokenProvider := func(ctx context.Context) (string, error) {
    return "<jwt>", nil
}

client, err := oversqlite.NewClient(db, "http://localhost:8080", tokenProvider, cfg)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

openResult, err := client.Open(context.Background(), "device-abc")
if err != nil {
    log.Fatal(err)
}
log.Printf("open state: %s", openResult.State)

connectResult, err := client.Attach(context.Background(), "user-123")
if err != nil {
    log.Fatal(err)
}
if connectResult.Status == oversqlite.AttachStatusRetryLater {
    log.Printf("connect pending, retry after %s", connectResult.RetryAfter)
    return
}

pushReport, err := client.PushPending(context.Background())
if err != nil {
    log.Printf("push failed: %v", err)
}
log.Printf("push outcome: %s", pushReport.Outcome)

pullReport, err := client.PullToStable(context.Background())
if err != nil {
    log.Printf("pull failed: %v", err)
}
log.Printf("pull outcome: %s", pullReport.Outcome)

detachResult, err := client.Detach(context.Background())
if err != nil {
    log.Printf("detach failed: %v", err)
} else if detachResult.Outcome == oversqlite.DetachOutcomeBlockedUnsyncedData {
    log.Printf("detach blocked by %d pending rows", detachResult.PendingRowCount)
}

// Full local teardown when you want to remove oversqlite from this database entirely.
if err := client.UninstallSync(context.Background()); err != nil {
    log.Printf("uninstall failed: %v", err)
}
``` 

`UninstallSync(ctx)` is intentionally more destructive than `Detach(ctx)` or
`RotateSource(ctx, sourceID)`. It drops sync-owned `_sync_*` metadata tables and the
change-capture triggers installed on managed tables, but it does not delete your business tables.
After a successful uninstall, create a new client if you want to install sync again on the same
SQLite database.

Go intentionally does not expose KMP-style reactive progress streams. The public API returns rich
final results for lifecycle and sync operations; any future Go progress visibility should remain
opt-in and callback/log-oriented rather than a first-class reactive stream.

## Important invariants

- Payloads are full-row after-images for `INSERT` and `UPDATE`.
- Push is all-or-nothing at bundle level.
- Syncable application timestamps should be stored as SQLite `TEXT` in UTC RFC3339/RFC3339Nano
  form such as `2026-03-24T18:02:00Z` or `2026-03-24T18:02:00.123456789Z`.
- Structured `push_conflict` responses are resolved from machine-readable conflict details, not
  human-readable error strings.
- `PushPending(ctx)` auto-recovers valid structured conflict outcomes:
  - `AcceptServer`
  - `KeepLocal`
  - `KeepMerged(mergedPayload)`
- `KeepLocal` and `KeepMerged` retry from a fresh outbound snapshot while preserving the same
  logical `source_bundle_id`.
- Structured conflict retries are bounded. Invalid resolutions and retry exhaustion fail closed by
  clearing `_sync_outbox_*` and restoring replayable intents to `_sync_dirty_rows`.
- Pull applies complete bundles only and advances `last_bundle_seq_seen` only after durable commit.
- `last_bundle_seq_seen` is the highest contiguous durably applied bundle, not the highest seen seq.
- `PullToStable` and `Rebuild(...)` fail closed while `_sync_outbox_*` exists.
- startup and restart resume from `_sync_outbox_bundle.state = prepared` or `committed_remote`
  instead of recollecting a fresh local bundle.
- `Sync` fails closed while `_sync_attachment_state.rebuild_required = 1`; only `Rebuild(ctx, RebuildKeepSource, "")`
  or `Rebuild(ctx, RebuildRotateSource, newSourceID)`
  may clear that flag.
- `oversqlite` supports exactly one configured remote schema per local SQLite database in the
  current runtime. Startup fails if existing sync metadata belongs to a different schema or if a
  table registration is schema-qualified.
- Run exactly one `oversqlite.Client` against one SQLite database at a time. Cross-client
  coordination is not supported. Call `client.Close()` before replacing a client against the same
  SQLite file or handle.
- Local FK cascades and local trigger-generated writes on managed tables are captured into `_sync_dirty_rows`.
- direct managed-table writes fail closed with `SYNC_TRANSITION_PENDING` while
  `_sync_operation_state.kind != 'none'` unless `oversqlite` is in bounded internal apply mode
- local writes made after `Detach(ctx)` are captured immediately as anonymous dirty rows; they do
  not rely on a later reopen-time recovery scan
- Primary-key changes are represented as delete-plus-upsert on the client dirty set.
- Each managed table must declare exactly one visible sync key column.
- The visible sync key column must also be the local SQLite `PRIMARY KEY` in the current runtime.
- Supported local visible sync key shapes are `TEXT PRIMARY KEY` and UUID-backed `BLOB PRIMARY KEY`.
- Integer-like local sync key primary keys are rejected during client startup.

## Timestamp fields

`oversqlite` does not impose a special SQLite datetime type for your business rows. Sync-visible
absolute application timestamp columns should use:

- SQLite column type: `TEXT`
- value format: UTC RFC3339 or RFC3339Nano
- resolver behavior: parse timestamps as time values; do not compare raw strings unless your format
  is fixed and normalized

The client runtime's own metadata tables also use UTC text timestamps. Older ad hoc formats such as
`yyyy:mm:dd hh:mm:ss` are not the recommended sync format.

## Rebuild

If the server returns `history_pruned`, the client must rebuild from chunked snapshot sessions.

- `Rebuild(ctx, RebuildKeepSource, "")` performs a snapshot rebuild without rotating `current_source_id`.
- `Rebuild(ctx, RebuildRotateSource, newSourceID)` performs a destructive rebuild and rotates
  `current_source_id` only to the caller-provided `newSourceID`.

## Conflict Resolution

Resolvers now receive structured conflict context:

- `schema`
- `table`
- `key`
- `local op`
- `local payload`
- `base row version`
- `server row version`
- `server row deleted`
- `server row payload`

Built-in resolvers:

- `&oversqlite.ServerWinsResolver{}`
- `&oversqlite.ClientWinsResolver{}`

Example:

```go
client.Resolver = &oversqlite.ClientWinsResolver{}
```

Custom resolvers implement:

```go
type Resolver interface {
    Resolve(conflict oversqlite.ConflictContext) oversqlite.MergeResult
}
```

Possible results:

- `oversqlite.AcceptServer{}`
- `oversqlite.KeepLocal{}`
- `oversqlite.KeepMerged{MergedPayload: payload}`

Typed errors:

- `*oversqlite.PushConflictError`: structured conflict payload decoded from transport
- `*oversqlite.InvalidConflictResolutionError`: resolver returned an invalid outcome for the conflict shape
- `*oversqlite.PushConflictRetryExhaustedError`: automatic structured conflict retry hit the retry budget
- `*oversqlite.RetryExhaustedError`: bounded transient retry was exhausted for a retry-covered sync operation

## Server requirements

The server must expose:

- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`
- `DELETE /sync/push-sessions/{push_id}`
- `GET /sync/committed-bundles/{bundle_seq}/rows`
- `GET /sync/pull`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/capabilities`
- `POST /sync/connect`

The server must advertise `connect_lifecycle` in `GET /sync/capabilities`. If `/sync/connect` or
that capability is missing, `Attach(ctx, userID)` fails closed with a typed incompatibility error.

## Testing

```bash
go test ./oversqlite
```
