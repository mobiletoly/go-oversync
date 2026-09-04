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

The stored bundle is canonical and generation-independent. At each request the server validates
that canonical framing, filters it through the pinned immutable generation, preserves surviving
order, and computes a new visible count, byte count, and hash. Hidden-only bundles remain empty
pull envelopes. No durable projection copies, transformation callbacks, aliases, or projection
cache participate in authority.

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

## Canonical JSON and numeric values

Generation-bound protocol bytes use RFC 8785 JCS plus the exact declared wire token. Integers are
range-checked canonical strings. Decimals preserve bounded non-exponent text. `float64` uses the
shortest finite spelling for both PostgreSQL `float4` and `float8`; committed `float4` values may
reflect storage normalization. Boolean values are
JSON literals, never `"0"` or `"1"` strings. Bytes, UUIDs, text, and inner JSON are likewise
validated without trimming, normalization, or repair. Text and inner-JSON strings preserve every
Unicode scalar except PostgreSQL-infeasible U+0000. SQL NULL is outer JSON `null`; a JSON document
whose value is null is the outer string `"null"`.

SQLite affinity cannot select between several of those tokens. generation provisions a fixed proof but
does not add the per-column local codec metadata owned by upgrade, so the current `oversqlite`
affinity bridge is not a generation-token fallback.

Invalid integer, decimal, floating-point, Boolean, base64, UUID, text, or inner-JSON wire values are rejected before durable staging,
business mutation, or SQLite mutation. Native JSON/JSONB numbers remain inside the ordinary JCS
binary64 domain; exact nested values must be modeled as strings by the application schema.
`bundle_hash` authenticates server-committed rows. The separately stored `canonical_request_hash`
proves that an exact direct-push replay belongs to the client's frozen original request even when
legitimate server mutation changed committed payloads.

## Push

Push is all-or-nothing at bundle level.

- The client sends one compact logical dirty set in a bounded `POST /sync/push`; `source_id` is the
  authenticated header identity and `source_bundle_id` is in the request envelope.
- The server validates the whole request and either rejects it or commits one bundle.
- Retrying the same accepted `(user_id, source_id, source_bundle_id)` returns the same committed
  bundle.
- The server stores accepted-push replay metadata in `sync.bundle_log`.
- Every uploaded key must have exactly one final captured effect visible to the pushing generation.
  Trigger normalization and extra effects are valid; incomplete coverage returns HTTP 422 and
  rolls back business, audit/cascade, bundle, and source progress together.
- `sync.source_state` keeps a monotonic committed source-bundle watermark so stale or duplicate
  source bundle ids cannot be accepted again after older bundle history is pruned.
- Source bundle ids must be contiguous per source; stale/pruned, out-of-order, changed, or retired
  source failures require source recovery instead of generic retry.

### Structured conflict recovery

The supported client/runtime contract includes structured `push_conflict` recovery.

- The default resolver is durable defer: `PushPending()` and `Sync()` return
  `ConflictNeedsResolutionError` without acknowledging the request or clearing its outbox.
- Inspect the exact retained `PendingConflict`, then call `ResolveConflict(selected, result)` or
  `RetryConflict(selected)`. Retry clears only the observation and preserves the frozen request.
- `ServerWinsResolver` and `ClientWinsResolver` are explicit opt-in policies; only those policies
  perform automatic conflict decisions.
- Only presence-aware, schema-validated machine-readable `push_conflict` payloads participate in
  resolver-based recovery. Missing `server_row` differs from explicit JSON null.
- Valid resolver outcomes are:
  - accept server state
  - keep local intent
  - keep merged full-row payload
- A selected decision validates the retained revision, rewrites local/row state, requeues surviving
  intent, and clears the outbox and conflict observation in one transaction. Failure or cancellation
  rolls the complete decision back.
- Opt-in automatic decisions preserve the logical `source_bundle_id`; the bounded retry budget is
  `2` inside one `PushPending()`.

## Administrative reset and TRUNCATE protection

Oversync does not provide an in-place migration, canonicalization fallback, hash-version
negotiation, compatibility alias, or mixed-version mode for incompatible database state. To reset
a deployment, stop all server and client processes, recreate PostgreSQL and every Oversqlite client
database, deploy compatible versions together, and reinitialize from authoritative business data.
This procedure discards bundle history, staged sessions, outboxes, checkpoints, hashes, retry
state, and offline work.

`TRUNCATE` fails closed on registered tables and current partitions. A rejection uses SQLSTATE
`55000` and preserves business rows, row state, bundles, hashes, source watermarks, pull, and
snapshots. Normal runtime has no reset endpoint. Privileged trigger disabling and later
managed-table DDL remain unsupported administrative risks.

The `sync` schema is reserved for Oversync, and startup validates its complete semantic layout
instead of trusting marker rows alone. A healthy restart performs no managed DDL. Drift returns a
typed `managed sync layout mismatch` and is never repaired silently. Keep all server instances
stopped while restoring the exact object, then retry; if exact repair is unsafe, use the coordinated
server/client recreation procedure above. Do not mix versions during diagnosis or repair. These
startup checks do not change wire messages, checkpoints, snapshots, bundles, or client local state.

## Pull

Pull is frozen to a stable ceiling.

- The first `GET /sync/pull` response returns `stable_bundle_seq`.
- Follow-up requests must keep using that ceiling until it is reached.
- Clients advance `last_bundle_seq_seen` only after durable local bundle apply.
- `retained_bundle_floor` is the highest discarded bundle. Pull from the floor is valid and returns
  only bundles strictly above it; lower checkpoints return `history_pruned`.
- Accepted-push replay remains stricter: replay at or below the floor returns `history_pruned`
  because the committed bundle itself is no longer available.
- A pull checkpoint or positive target above current committed history returns
  `checkpoint_ahead`, never a clamped stable checkpoint.

## Snapshot rebuilds

Chunked snapshot sessions return the full current after-image at one exact frozen bundle sequence.

Use it for:

- first hydration
- destructive recovery
- rebuild after `history_pruned`
- rebuild after `checkpoint_ahead`

Clients durably set the rebuild gate before authoritative checkpoint recovery. Normal sync resumes
that recovery automatically after restart. Pending local work is frozen and uploaded first when it
can be reconciled safely; otherwise recovery returns an actionable blocker and preserves the app
row, outbox, checkpoint, and rebuild gate. The checkpoint changes only in the final atomic snapshot
apply. `Rebuild` remains an optional explicit control, not a required resume step.

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
- invalid automatic conflict decisions atomically restore missing dirty intent, clear the invalid
  observation, and retire the frozen `_sync_outbox_*` request without overwriting newer dirty edits;
  an explicit invalid decision retains the pending conflict and frozen request
- inspect `PendingConflict`, then use `ResolveConflict` with the inspected decision identity;
  stale decisions fail without discarding intent
- `RetryConflict` clears only the conflict observation; the next `PushPending` retries the exact
  frozen request. It does not acknowledge, rebase, or discard local edits
- application HTTP 422 responses create a durable `PendingPushRejection`; explicit retry preserves
  the frozen request, while explicit discard abandons that whole request only after server staging
  is definitively retired and preserves every newer dirty edit
- other non-conflict commit/replay failures still use the existing fail-closed recovery path

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
