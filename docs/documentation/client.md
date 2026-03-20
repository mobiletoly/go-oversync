---
layout: default
title: Client
---

# Client

The supported client contract is bundle-based.

## SQLite state

- `_sync_dirty_rows`: one coalesced local dirty row per structured key.
- `_sync_push_outbound`: the frozen in-flight push snapshot.
- `_sync_push_stage`: the authoritative committed rows staged before final replay.
- `_sync_row_state`: authoritative replicated row state for managed tables.
- `_sync_client_state`: client-scoped state including `source_id`, `next_source_bundle_id`,
  `last_bundle_seq_seen`, `schema_name`, `apply_mode`, and `rebuild_required`.

## Operations

- `PushPending()`: freeze `_sync_dirty_rows` into `_sync_push_outbound`, upload through push
  sessions, fetch authoritative committed rows into `_sync_push_stage`, and replay them locally.
- `PullToStable()`: pull complete committed bundles until the frozen `stable_bundle_seq` is
  reached.
- `Sync()`: push, then pull to a stable ceiling.
- `Hydrate()`: rebuild managed tables through chunked snapshot sessions.
- `Recover()`: destructive rebuild through chunked snapshot sessions, then rotate `source_id`.

## Invariants

- The client never applies part of a bundle.
- `last_bundle_seq_seen` advances only after durable bundle commit.
- Pull, hydrate, and recover fail closed while `_sync_push_outbound` exists.
- Normal sync fails closed while `rebuild_required = 1`; only `Hydrate()` and `Recover()` may
  clear that state.
- Local FK cascades and trigger-driven writes on managed tables are captured into
  `_sync_dirty_rows`.
- Local SQLite dirty-row capture may use implementation-specific encodings internally.
  The supported wire contract is narrower:
  - non-key binary payload fields use standard base64
  - UUID-valued keys and UUID-valued key columns use dashed UUID text
- The current client runtime supports exactly one configured remote schema per local SQLite
  database. Startup fails closed if existing sync metadata belongs to a different schema or if a
  table registration is schema-qualified.
- Run exactly one client runtime against one SQLite database at a time. Cross-client coordination
  is not supported. Call `Close()` before replacing a client against the same SQLite file or
  handle.
- Unsupported local schema shapes fail during startup instead of degrading at runtime.
