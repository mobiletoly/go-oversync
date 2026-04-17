---
layout: default
title: Advanced Concepts
permalink: /documentation/advanced-concepts/
---

# Advanced Concepts

This page describes the supported bundle-based sync model.

## Authoritative state

Registered business tables are the source of truth. Sync metadata is derived from committed
business-table effects rather than projected afterward from a separate authoritative log.

## Bundles

A bundle is the smallest durable sync unit.

- One committed business-table transaction becomes one committed sync bundle.
- `sync.bundle_log` records committed bundle metadata.
- `sync.bundle_rows` records normalized row effects for that bundle.
- Clients apply complete bundles only.

## Structured keys

Rows are identified by structured sync keys, not by an implicit single `pk` field on the wire.
In the current supported envelope, each registered table still exposes exactly one visible sync
key column, but that column may be `uuid` or `text`. Visible sync key values are strings on the
wire, and UUID-valued keys must use canonical dashed lowercase UUID text.

## Row state

`sync.row_state` stores the authoritative per-row replicated state for conflict detection,
idempotent replay, and snapshot rebuilds.

- the server stores the current version as `bundle_seq`
- `row_version` is the wire name clients see for that authoritative `bundle_seq`
- `deleted` distinguishes live rows from tombstoned rows.
- the internal key is `(user_pk, table_id, key_bytes)`
- visible `schema`, `table`, and structured `key` fields are reconstructed from `sync.table_catalog`
  when the server emits API responses

## Timestamp encoding

For syncable business rows, timestamp columns should be stored as SQLite `TEXT` using UTC
RFC3339 or RFC3339Nano values. This keeps payloads portable across SQLite, JSON transport, and Go
resolver code.

- use parsed time comparison in custom merge logic such as `updated_at` conflict policies
- do not treat legacy formats such as `yyyy:mm:dd hh:mm:ss` as the canonical synced form

## Push

Push is all-or-nothing at bundle level.

- The client sends one logical dirty set with `source_id`, `source_bundle_id`, and `base_bundle_seq`.
- The server validates the whole request and either rejects it or commits one bundle.
- Retrying the same accepted `(user_id, source_id, source_bundle_id)` returns the same committed
  bundle.
- The server stores accepted-push replay metadata in `sync.bundle_log`.
- `sync.source_state` keeps a monotonic committed source-bundle watermark so stale or duplicate
  source bundle ids cannot be accepted again after older bundle history is pruned.
- Source bundle ids must be contiguous per source; stale/pruned, out-of-order, changed, or retired
  source failures require source recovery instead of generic retry.

### Structured conflict recovery

The supported client/runtime contract includes structured `push_conflict` recovery.

- Only decoded machine-readable `push_conflict` payloads participate in resolver-based recovery
- Valid resolver outcomes are:
  - accept server state
  - keep local intent
  - keep merged full-row payload
- Automatic structured recovery rewrites local row state, requeues surviving dirty intents,
  clears `_sync_outbox_*`, and retries from a fresh outbound snapshot
- Structured retries preserve the same logical `source_bundle_id`; `next_source_bundle_id` advances
  only after a successful committed replay
- The retry budget is bounded to `2` automatic retries inside one `PushPending()`

## Pull

Pull is frozen to a stable ceiling.

- The first `GET /sync/pull` response returns `stable_bundle_seq`.
- Follow-up requests must keep using that ceiling until it is reached.
- Clients advance `last_bundle_seq_seen` only after durable local bundle apply.
- Bundle history is retained only above the user's `retained_bundle_floor`.
- Pull or accepted-push replay at or below the retained floor returns `history_pruned`; snapshot
  rebuild is the recovery path.

## Snapshot rebuilds

Chunked snapshot sessions return the full current after-image at one exact frozen bundle sequence.

Use it for:

- first hydration
- destructive recovery
- rebuild after `history_pruned`

When rebuild requires source rotation:

- the client reserves one durable `replacement_source_id` locally and reuses it across restart or
  retry
- the rotated `POST /sync/snapshot-sessions` request tells the server to retire the old source and
  reserve the replacement source atomically
- the client keeps the old local `current_source_id` until authoritative snapshot apply succeeds
- normal source-sequenced sync stays fail-closed while recovery is pending

## Fail-closed contract

The supported envelope is intentionally strict.

- bootstrap fails for unsupported FK/key shapes
- bootstrap fails when the managed or registered table set is not FK-closed
- bootstrap fails when required FK deferrability is missing
- pull/hydrate/recover fail while local dirty rows exist
- malformed server responses are rejected without advancing durable checkpoints
- invalid structured conflict resolutions clear `_sync_outbox_*` and restore replayable intents
  to `_sync_dirty_rows`
- structured conflict retry exhaustion also clears `_sync_outbox_*` and leaves unresolved
  intents replayable
- generic non-conflict commit/replay failures still use the existing fail-closed recovery path

## Supported envelope

The current contract is designed to be reliable for:

- exactly one visible sync key column per registered/managed table
- scope-bound registered PostgreSQL identity with `_sync_scope_id`
- scope-inclusive FKs between registered PostgreSQL tables
- self-references
- multi-table cycles
- `ON DELETE CASCADE`
- `ON UPDATE CASCADE`

Unsupported shapes must fail at bootstrap rather than degrade into partial runtime behavior.
