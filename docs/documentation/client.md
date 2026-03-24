---
layout: default
title: Client
permalink: /documentation/client/
---

# Client

`oversqlite` is the SQLite client SDK for the current bundle-based contract.

## SQLite State

- `_sync_dirty_rows`: one coalesced local dirty row per structured key
- `_sync_snapshot_stage`: snapshot rows staged during hydration or recovery
- `_sync_push_outbound`: the frozen in-flight push snapshot
- `_sync_push_stage`: authoritative committed rows staged before final replay
- `_sync_row_state`: authoritative replicated row state for managed tables
- `_sync_client_state`: client-scoped state including `source_id`, `next_source_bundle_id`,
  `last_bundle_seq_seen`, `schema_name`, `apply_mode`, and `rebuild_required`

## Operations

- `Bootstrap(ctx, performHydration)`: ensures client metadata exists and optionally hydrates
- `PushPending()`: freezes `_sync_dirty_rows` into `_sync_push_outbound`, uploads through push
  sessions, resolves valid structured `push_conflict` responses automatically, fetches
  authoritative committed rows into `_sync_push_stage`, and replays them locally
- `PullToStable()`: pulls complete committed bundles until the frozen `stable_bundle_seq` is
  reached; if the server replies with `history_pruned`, the client rebuilds through snapshot
  sessions
- `Sync()`: pushes, then pulls to a stable ceiling
- `Hydrate()`: rebuilds managed tables through chunked snapshot sessions
- `Recover()`: destructive rebuild through chunked snapshot sessions, then rotates `source_id`

## Invariants

- the client never applies part of a bundle
- `last_bundle_seq_seen` advances only after durable bundle commit and represents the highest
  contiguous durably applied bundle
- pull, hydrate, and recover fail closed while `_sync_push_outbound` exists
- normal sync fails closed while `rebuild_required = 1`; only `Hydrate()` and `Recover()` may
  clear that state
- local FK cascades and trigger-driven writes on managed tables are captured into
  `_sync_dirty_rows`
- non-key binary payload fields use standard base64 on the wire
- UUID-valued keys and UUID-valued key columns use dashed UUID text on the wire
- the current client runtime supports exactly one configured remote schema per local SQLite
  database
- run exactly one client runtime against one SQLite database at a time
- `Close()` releases client ownership of the SQLite database; it does not close the underlying
  `*sql.DB`

## Timestamp Fields

For syncable application rows, use timestamp columns as SQLite `TEXT` values encoded in UTC
RFC3339 or RFC3339Nano, for example `2026-03-24T18:02:00Z`.

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

Fail-closed behavior:

- invalid resolution clears `_sync_push_outbound` and restores replayable intents to `_sync_dirty_rows`
- retry exhaustion also clears `_sync_push_outbound` and leaves unresolved intents replayable
- generic non-conflict commit/replay failures still use the existing fail-closed recovery path

## Local Schema Rules

- every managed table must declare its sync key explicitly
- managed tables must be FK-closed
- composite sync keys are rejected by the current client runtime
- schema-qualified local table registrations are rejected
- unsupported local schema shapes fail during startup instead of degrading at runtime
