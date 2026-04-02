---
layout: default
title: Client
permalink: /documentation/client/
---

# Client

`oversqlite` is the SQLite client SDK for the current bundle-based contract.

`NewClient(...)` installs the runtime and metadata tables, but it does not attach a user. The
startup flow is:

1. `Open(ctx)`
2. `Attach(ctx, userID)` after sign-in
3. normal sync operations

`oversqlite` manages source identity internally. Host application code does not provide source ids
to `Open()`, does not rotate them manually, and does not provide replacement source ids to
`Rebuild()`.

Authenticated sync requests still carry the runtime's current source through
`Oversync-Source-ID`. Auth remains separately host-owned.

## SQLite State

- `_sync_dirty_rows`: one coalesced local dirty row per structured key
- `_sync_snapshot_stage`: rows staged during rebuild
- `_sync_outbox_bundle`: singleton frozen push snapshot state
- `_sync_outbox_rows`: rows belonging to that frozen snapshot
- `_sync_push_stage`: authoritative committed rows staged before final replay
- `_sync_row_state`: authoritative replicated row state for managed tables
- `_sync_managed_tables`: registry of managed tables whose triggers were installed by `oversqlite`
- `_sync_source_state`: per-source sequencing state and replacement lineage
- `_sync_attachment_state`: current source, binding state, bundle checkpoint, and rebuild gate
- `_sync_operation_state`: lifecycle and source-recovery state
- `_sync_apply_state`: trigger suppression state during authoritative local apply

## Public Operations

- `Open(ctx) error`
- `Attach(ctx, userID) (AttachResult, error)`
- `Detach(ctx) (DetachResult, error)`
- `PendingSyncStatus(ctx) (PendingSyncStatus, error)`
- `SyncStatus(ctx) (SyncStatus, error)`
- `SyncThenDetach(ctx) (SyncThenDetachResult, error)`
- `UninstallSync(ctx) error`
- `PushPending(ctx) (PushReport, error)`
- `PullToStable(ctx) (RemoteSyncReport, error)`
- `Sync(ctx) (SyncReport, error)`
- `Rebuild(ctx) (RemoteSyncReport, error)`
- `LastBundleSeqSeen(ctx) (int64, error)`
- `SourceInfo(ctx) (SourceInfo, error)`

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

- `PullToStable()` may rebuild from snapshots while keeping the current source
- committed-remote replay pruned below the retained floor also uses keep-source rebuild behavior
- stale/out-of-order upload cases enter durable source-recovery-required mode
- `PushPending()`, `PullToStable()`, and `Sync()` fail closed while source recovery is active
- `Rebuild(ctx)` is the explicit recovery entry point
- when source recovery is active, `Rebuild(ctx)` internally chooses rebuild-plus-rotate and
  preserves frozen outbox intent across that recovery

## Diagnostic Surface

`SourceInfo(ctx)` is debug-only. It exposes:

- `CurrentSourceID`
- `RebuildRequired`
- `SourceRecoveryRequired`
- `SourceRecoveryReason`

`CurrentSourceID` is opaque. Callers must not persist it externally or treat its format as part of
the host API contract.

## Typed Preconditions And Results

- `OpenRequiredError`
- `AttachRequiredError`
- `DestructiveTransitionInProgressError`

Normal lifecycle and sync branches such as retry-later attach, blocked detach, paused push/pull,
and already-at-target pull are reported as structured result values with `error == nil`.
