---
layout: doc
title: Server
permalink: /documentation/server/
parent: Architecture
---

This page is the PostgreSQL/server runtime overview. It explains what the server owns, which sync
flows it serves, and which contracts host applications must satisfy. Detailed guidance for
server-originated writes lives on the separate
[Server-Originated Writes]({{ site.baseurl }}/documentation/server-originated-writes/) page.

## What The Server Owns

The server treats registered PostgreSQL business tables as authoritative state.

`oversync.SyncService` is the low-level PostgreSQL sync runtime. It owns:

- schema bootstrap and validation for the supported registered-table envelope
- first-connect lifecycle state
- staged push-session creation, chunk upload, and commit
- committed bundle capture from business-table transactions
- pull and snapshot serving
- source sequencing, retirement, and retained-history enforcement

## Runtime Tables

Authoritative replication state:

- `sync.user_state`: per-user bundle sequencing and retained-floor tracking
- `sync.row_state`: authoritative per-row row-version and tombstone state
- `sync.bundle_log`: one row per committed sync bundle
- `sync.bundle_rows`: normalized row effects for each committed bundle
- `sync.applied_pushes`: accepted-push replay table keyed by `(user_id, source_id, source_bundle_id)`

Transport and lifecycle state:

- `sync.scope_state`: durable first-connect authority state (`UNINITIALIZED`, `INITIALIZING`,
  `INITIALIZED`)
- `sync.push_sessions`: active staged push sessions
- `sync.push_session_rows`: staged rows for each push session
- `sync.snapshot_sessions`: active frozen snapshot session metadata
- `sync.snapshot_session_rows`: materialized rows for each snapshot session

## Main Flows

### First Connect

Clients call `POST /sync/connect` to resolve first authority for one scope.

Possible outcomes:

- `remote_authoritative`: the scope was already initialized, even if it is currently empty
- `initialize_local`: this source won the initialization lease and may seed server state from local
  pending rows
- `initialize_empty`: the scope was uninitialized and the server established authoritative empty
  state
- `retry_later`: another initializer currently owns the lease, or the server is applying a bounded
  empty-first deferral optimization

`retry_later` is a normal lifecycle result, not an auth failure.

### Push

The normal client write flow is:

- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`

Accepted-push recovery fetches authoritative bundle rows through
`GET /sync/committed-bundles/{bundle_seq}/rows`.

### Pull And Snapshot

- `GET /sync/pull` returns complete committed bundles only
- `POST /sync/snapshot-sessions` materializes one frozen current after-image inside PostgreSQL
- `GET /sync/snapshot-sessions/{snapshot_id}` returns deterministic chunks from that frozen
  snapshot
- if a client checkpoint falls behind the retained bundle floor, the server returns
  `history_pruned`

Pull and snapshot creation fail closed before the scope reaches `INITIALIZED`.

## Server-Originated Writes

If your application writes registered PostgreSQL tables outside client push handling, use:

- `ScopeManager.ExecWrite(...)` in the common case
- `WithinSyncBundle(...)` only when your application already manages exact
  `(user_id, source_id, source_bundle_id)` tuples directly

That topic has enough runtime detail to deserve its own page. See
[Server-Originated Writes]({{ site.baseurl }}/documentation/server-originated-writes/).

## Registered Table Requirements

Registered PostgreSQL tables must satisfy these rules before bootstrap:

- exactly one visible sync key column per registered table
- visible sync key type must be `uuid` or `text`
- every registered table must define `_sync_scope_id TEXT NOT NULL`
- `(_sync_scope_id, sync_key)` must be unique
- every unique constraint or unique index on a registered table must include `_sync_scope_id`
- registered table sets must be FK-closed
- registered-to-registered foreign keys must be scope-inclusive and `DEFERRABLE`
- supported `ON DELETE` / `ON UPDATE` actions are `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`,
  and `SET DEFAULT`
- supported `MATCH` options are empty, `NONE`, or `SIMPLE`
- `DEFERRABLE INITIALLY DEFERRED` is recommended
- `DEFERRABLE INITIALLY IMMEDIATE` is accepted
- partial, predicate, and expression unique indexes are unsupported on registered tables

Bootstrap fails closed with `UnsupportedSchemaError` when the registered schema is outside the
supported envelope.

## Auth Contract

The handlers expect the host application to authenticate first and place
`oversync.Actor{UserID, SourceID}` into request context.

The built-in transport helper is `oversync.ActorMiddleware(...)`, which reads
`Oversync-Source-ID` after the host auth layer has already established trusted `user_id` in
request context.

`_sync_scope_id` is derived from `Actor.UserID`, enforced only on the authoritative PostgreSQL
side, and excluded from client-visible payloads, conflicts, pulls, and snapshots.

## Related Guides

- [Core Concepts]({{ site.baseurl }}/documentation/core-concepts/)
- [Server-Originated Writes]({{ site.baseurl }}/documentation/server-originated-writes/)
- [HTTP API]({{ site.baseurl }}/documentation/api/)
- [Performance]({{ site.baseurl }}/documentation/performance/)
