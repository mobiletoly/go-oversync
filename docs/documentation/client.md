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
  sessions, fetches authoritative committed rows into `_sync_push_stage`, and replays them locally
- `PullToStable()`: pulls complete committed bundles until the frozen `stable_bundle_seq` is
  reached; if the server replies with `history_pruned`, the client rebuilds through snapshot
  sessions
- `Sync()`: pushes, then pulls to a stable ceiling
- `Hydrate()`: rebuilds managed tables through chunked snapshot sessions
- `Recover()`: destructive rebuild through chunked snapshot sessions, then rotates `source_id`

## Invariants

- the client never applies part of a bundle
- `last_bundle_seq_seen` advances only after durable bundle commit
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

## Local Schema Rules

- every managed table must declare its sync key explicitly
- managed tables must be FK-closed
- composite sync keys are rejected by the current client runtime
- schema-qualified local table registrations are rejected
- unsupported local schema shapes fail during startup instead of degrading at runtime
