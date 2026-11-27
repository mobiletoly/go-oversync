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

The client sends one compact dependency-safe bundle in a bounded `POST /sync/push`. The server
commits the business change and its terminal outcome together, or retains a safe rejection
after rolling back the business write. Each uploaded key must have exactly one final captured
effect visible to the pushing generation. Incomplete coverage is a nonterminal HTTP 422 and
rolls back the whole business transaction.

An uncertain upload resolves under its original binding. Terminal outcomes survive bundle pruning
and consume the source operation sequence. After durable local disposition, the client sends ack.
Rejection and cancellation can requeue intent for a successor. See
[Durable upload outcomes]({{ site.baseurl }}/documentation/durable-upload-outcomes/).

### Structured conflict recovery

The default policy durably defers a conflict. The Go SDK records the rejection and acknowledges the
terminal outcome. Inspect the retained `PendingConflict` and pass that decision identity to
`ResolveConflict` or `RetryConflict`. A resolution can accept server state, keep local intent,
or keep a merged full-row payload. Retry requeues surviving intent under a successor sequence.

Applications can opt in to `ServerWinsResolver`, `ClientWinsResolver`, or a custom resolver.
The SDK calls an automatic resolver after acknowledgment. Decisions preserve newer local edits
and apply in one transaction. The automatic conflict retry budget is two inside a `PushPending` call.

## Administrative reset and TRUNCATE protection

Use a fresh sync layout and client databases for v0.5.0. See the
[v0.5.0 release notes]({{ site.baseurl }}/releases/v0.5.0/).

`TRUNCATE` fails closed on registered tables and current partitions. A rejection uses SQLSTATE
`55000` and preserves business rows, row state, bundles, hashes, source watermarks, pull, and
snapshots. Normal runtime has no reset endpoint. Privileged trigger disabling and later
managed-table DDL remain unsupported administrative risks.

The `sync` schema is reserved for Oversync, and startup validates its complete semantic layout
instead of trusting marker rows alone. A healthy restart performs no managed DDL. Drift returns a
typed `managed sync layout mismatch` and is never repaired silently. Keep all server instances
stopped while restoring from a reviewed compatible backup or following a separately reviewed
recovery procedure.

## Pull

Pull is frozen to a stable ceiling.

- The first `GET /sync/pull` response returns `stable_bundle_seq`.
- Follow-up requests must keep using that ceiling until it is reached.
- Clients advance `last_bundle_seq_seen` only after durable local bundle apply.
- `retained_bundle_floor` is the highest discarded bundle. Pull from the floor is valid and returns
  only bundles strictly above it; lower checkpoints return `history_pruned`.
- A pull checkpoint or positive target above current committed history returns
  `checkpoint_ahead`, never a clamped stable checkpoint.

## Snapshot rebuilds

Chunked snapshot sessions return the full current after-image at one exact frozen bundle sequence.

Use it for:

- first hydration
- destructive recovery
- rebuild after `history_pruned`
- rebuild after `checkpoint_ahead`

Automatic recovery can install a sealed snapshot after an eligible pull failure when the scope is
clean. Resume uses the durable recovery state. The final apply transaction publishes the new
checkpoint. Explicit `Rebuild` uses the same installer. See
[Snapshot recovery]({{ site.baseurl }}/documentation/snapshot-recovery/).

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
- clean snapshot recovery requires local intent, upload outcomes, and acknowledgment to be settled
- malformed responses leave durable checkpoints intact
- stale or invalid explicit decisions preserve the pending conflict and newer edits
- retry requeues intent for a successor sequence. Discarding a terminal application rejection
  preserves newer dirty rows. See the [Go SDK reference]({{ site.baseurl }}/documentation/client/).

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
