---
layout: default
title: Client
permalink: /documentation/client/
---

# Client

`oversqlite` is the SQLite client SDK for the current bundle-based contract.

`NewClient(...)` is lifecycle-neutral. It installs the runtime and local sync metadata, but it does
not attach a user and it does not validate or persist a caller-owned source ID. Call
`Open(ctx, sourceID)` on launch and `Attach(ctx, userID)` after sign-in.

Authenticated sync requests sent by `oversqlite` carry the current `sourceID` through the
`Oversync-Source-ID` header. The auth token remains host-owned and does not need to encode source
identity.

## SQLite State

- `_sync_dirty_rows`: one coalesced local dirty row per structured key
- `_sync_snapshot_stage`: snapshot rows staged during rebuild
- `_sync_outbox_bundle`: singleton outbox state for the one frozen in-flight push snapshot
- `_sync_outbox_rows`: the rows belonging to that frozen snapshot
- `_sync_push_stage`: authoritative committed rows staged before final replay
- `_sync_row_state`: authoritative replicated row state for managed tables
- `_sync_managed_tables`: registry of managed tables whose change-capture triggers were installed by
  `oversqlite`
- `_sync_source_state`: per-source durable state including `next_source_bundle_id`
- `_sync_attachment_state`: attachment-scoped durable state including `current_source_id`,
  `last_bundle_seq_seen`, `schema_name`, anonymous vs attached binding state,
  `pending_initialization_id`, and `rebuild_required`
- `_sync_operation_state`: lifecycle recovery state for pending `remote_replace`
- `_sync_apply_state`: trigger suppression state during authoritative local apply

## Operations

- `Open(ctx, sourceID) (OpenResult, error)`: local-only startup step; validates/persists the
  caller-owned `current_source_id`
  and captures pre-existing anonymous rows only on the first bootstrap open for a blank
  `current_source_id`
- `Attach(ctx, userID) (AttachResult, error)`: resolves first-account attachment through `POST /sync/connect`
- `Detach(ctx) (DetachResult, error)`: reports blocked-vs-detached as a normal result and otherwise
  clears synced local state while restoring dirty-row capture before commit
- `SyncStatus(ctx) (SyncStatus, error)`: reports authority state, pending state, and the last seen
  bundle checkpoint for the currently connected scope
- `SyncThenDetach(ctx) (SyncThenDetachResult, error)`: bounded best-effort sync-first detach
  convenience path
- `PendingSyncStatus(ctx)`: reports whether pending sync rows exist and whether they block detach
- `RotateSource(ctx, sourceID)`: explicit caller-owned source rotation; preserves dirty rows but is
  blocked by active outbox, initialization, or durable lifecycle recovery state
- `UninstallSync(ctx)`: explicit full local teardown that removes sync-owned metadata tables and
  triggers without deleting business tables
- `PushPending() (PushReport, error)`: freezes `_sync_dirty_rows` into `_sync_outbox_*`, uploads through push
  sessions, resolves valid structured `push_conflict` responses automatically, fetches
  authoritative committed rows into `_sync_push_stage`, and replays them locally
- `PullToStable() (RemoteSyncReport, error)`: pulls complete committed bundles until the frozen `stable_bundle_seq` is
  reached; if the server replies with `history_pruned`, the client rebuilds through snapshot
  sessions
- `Sync() (SyncReport, error)`: pushes, then pulls to a stable ceiling
- `Rebuild(ctx, RebuildKeepSource, "") (RemoteSyncReport, error)`: rebuilds managed tables through
  chunked snapshot sessions
- `Rebuild(ctx, RebuildRotateSource, newSourceID) (RemoteSyncReport, error)`: destructive rebuild
  through chunked snapshot sessions, then rotates to the caller-provided `newSourceID`

## Lifecycle Preconditions

Operations that require only `Open(ctx, sourceID)`:

- `Open(ctx, sourceID)` itself
- `PendingSyncStatus(ctx)`
- `RotateSource(ctx, sourceID)`
- `UninstallSync(ctx)`

Operations that require successful `Open(ctx, sourceID)` plus successful `Attach(ctx, userID)`:

- `PushPending()`
- `PullToStable()`
- `Sync()`
- `Rebuild(ctx, RebuildKeepSource, "")`
- `Rebuild(ctx, RebuildRotateSource, newSourceID)`
- `LastBundleSeqSeen(ctx)`
- `Detach(ctx)`
- `SyncThenDetach(ctx)`

Typed lifecycle/state-precondition errors:

- `OpenRequiredError`: returned when an operation requires `Open(ctx, sourceID)` first, including
  `Attach(ctx, userID)` before open
- `AttachRequiredError`: returned when attached-state operations are called before successful
  `Attach(ctx, userID)`
- `DestructiveTransitionInProgressError`: returned while durable lifecycle transition state
  prevents sync operations from running safely
- expected branches such as retry-later attach, blocked detach, paused push/pull, and
  already-at-target pull are returned as `error == nil` with structured results

## Built-In Transient Retry

If `Config.RetryPolicy` is unset, `oversqlite` enables a conservative built-in retry policy for
transient transport and temporary server-availability failures:

- `MaxAttempts: 3`
- `InitialBackoff: 100ms`
- `MaxBackoff: 1s`
- `JitterFraction: 0.2`

Covered internally:

- `PushPending()`
- `PullToStable()`
- snapshot session creation and snapshot chunk fetches used by `Rebuild(KEEP_SOURCE)` / `Rebuild(ROTATE_SOURCE)`
- transport-level `/sync/connect` failures while `Attach(ctx, userID)` is reaching the server

Not covered internally:

- semantic `AttachStatusRetryLater`
- `401` / `403`
- capability mismatch / old-server incompatibility
- conflict / validation failures
- `Detach()`
- `RotateSource(ctx, sourceID)`
- `UninstallSync()`

Set `Config.RetryPolicy = &oversqlite.RetryPolicy{Enabled: false}` to disable built-in retry
explicitly. When the retry budget is exhausted, the client returns `RetryExhaustedError`.

## Invariants

- `AttachStatusRetryLater` is a normal lifecycle result, not an auth failure
- Go does not expose KMP-style reactive progress streams; the supported public surface is
  result-oriented rather than stream-oriented
- the client never applies part of a bundle
- `last_bundle_seq_seen` advances only after durable bundle commit and represents the highest
  contiguous durably applied bundle
- the client durably keeps at most one frozen outbox bundle at a time
- process restart resumes from `_sync_outbox_bundle.state = prepared` or `committed_remote`
  instead of recollecting a fresh local bundle
- pull and rebuild fail closed while `_sync_outbox_*` exists
- normal sync fails closed while `rebuild_required = 1`; only `Rebuild(ctx, RebuildKeepSource, "")`
  and `Rebuild(ctx, RebuildRotateSource, newSourceID)` may
  clear that state
- local FK cascades and trigger-driven writes on managed tables are captured into
  `_sync_dirty_rows`
- direct managed-table writes fail closed with `SYNC_TRANSITION_PENDING` while
  `_sync_operation_state.kind != 'none'` unless the runtime is in internal apply mode
- local writes after `Detach(ctx)` are captured directly as anonymous pending rows and are not
  recovered later through a second bootstrap scan
- non-key binary payload fields use standard base64 on the wire
- UUID-valued keys and UUID-valued key columns use dashed UUID text on the wire
- visible sync keys on the wire are always JSON strings
- the current client runtime supports exactly one configured remote schema per local SQLite
  database
- run exactly one client runtime against one SQLite database at a time
- `Close()` releases client ownership of the SQLite database; it does not close the underlying
  `*sql.DB`
- `UninstallSync()` is more destructive than `Detach()` and `RotateSource()`: it removes the
  installed `oversqlite` footprint from the database and requires a new client instance before sync
  can be reinstalled

## Timestamp Fields

For sync-visible absolute application timestamps, use SQLite `TEXT` values encoded in RFC3339 or
RFC3339Nano with an explicit zone, for example `2026-03-24T18:02:00Z`.

- use parsed time comparison for merge logic such as `updated_at` conflict resolution
- avoid legacy ad hoc formats such as `yyyy:mm:dd hh:mm:ss`
- the client runtime's own metadata timestamps are also stored as UTC text values

## Structured Conflict Resolution

Structured `409 push_conflict` responses are part of the supported client contract.

- the client decodes the machine-readable conflict payload rather than parsing human-readable
  error strings
- resolvers receive `ConflictContext` with schema, table, key, local op, local payload,
  base row version, server row version, server row deleted, and server row payload
- valid resolver outcomes are:
  - `AcceptServer{}`
  - `KeepLocal{}`
  - `KeepMerged{MergedPayload: ...}`

Built-in resolvers:

- `ServerWinsResolver`
- `ClientWinsResolver`

Automatic recovery behavior:

- `AcceptServer` applies authoritative server state locally and drops the conflicting local intent
- `KeepLocal` rebases the local intent onto the authoritative row version when that is valid
- `KeepMerged` retries an explicit merged full-row payload when that is valid
- non-conflicting sibling rows from the rejected outbound snapshot are preserved and requeued
- retries rebuild a fresh outbound snapshot from `_sync_dirty_rows` while preserving the same
  logical `source_bundle_id`
- structured conflict auto-retry is bounded to at most `2` retries inside one `PushPending()`

Typed errors:

- `PushConflictError`: structured transport/decode-layer push-conflict error
- `InvalidConflictResolutionError`: resolver returned an outcome that is invalid for the conflict shape
- `PushConflictRetryExhaustedError`: retry budget was exhausted and replayable dirty state remains
- `RetryExhaustedError`: bounded transient retry was exhausted for a retry-covered sync operation

Fail-closed behavior:

- invalid resolution clears `_sync_outbox_*` and restores replayable intents to `_sync_dirty_rows`
- retry exhaustion also clears `_sync_outbox_*` and leaves unresolved intents replayable
- generic non-conflict commit/replay failures still use the existing fail-closed recovery path

## Local Schema Rules

- every managed table must declare its sync key explicitly
- every managed table must declare exactly one visible sync key column
- the visible sync key column must also be the local SQLite `PRIMARY KEY` in the current runtime
- supported local visible sync key shapes are:
  - `TEXT PRIMARY KEY`
  - UUID-backed `BLOB PRIMARY KEY`
- managed tables must be FK-closed
- composite sync keys are rejected by the current client runtime
- integer-like sync key primary keys are rejected during startup
- schema-qualified local table registrations are rejected
- unsupported local schema shapes fail during startup instead of degrading at runtime
