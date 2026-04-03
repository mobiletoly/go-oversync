---
layout: default
title: Server
permalink: /documentation/server/
---

# Server

The server treats registered PostgreSQL business tables as authoritative state.

## Runtime Tables

Authoritative replication state:

- `sync.user_state`: per-user bundle sequencing and retained-floor tracking
- `sync.row_state`: authoritative per-row row-version and tombstone state
- `sync.bundle_log`: one row per committed sync bundle
- `sync.bundle_rows`: normalized row effects for each committed bundle
- `sync.applied_pushes`: accepted-push replay table keyed by `(user_id, source_id, source_bundle_id)`

Transport/session state:

- `sync.scope_state`: durable first-connect authority state (`UNINITIALIZED`, `INITIALIZING`,
  `INITIALIZED`)
- `sync.push_sessions`: active staged push sessions
- `sync.push_session_rows`: staged rows for each push session
- `sync.snapshot_sessions`: active frozen snapshot session metadata
- `sync.snapshot_session_rows`: materialized rows for each snapshot session

## Write Path

- clients resolve account attachment and first-connect authority with `POST /sync/connect`
- clients create staged push sessions with `POST /sync/push-sessions`
- clients upload ordered chunks with `POST /sync/push-sessions/{push_id}/chunks`
- clients commit the staged push with `POST /sync/push-sessions/{push_id}/commit`
- accepted-push recovery fetches authoritative rows through
  `GET /sync/committed-bundles/{bundle_seq}/rows`
- server-originated registered-table writes should usually run through `ScopeManager.ExecWrite(...)`
- `ScopeManager.ExecWrite(...)` is the convenience API for one scope-aware server-originated write:
  it auto-initializes an `UNINITIALIZED` scope as remote-authoritative empty, allocates the next
  expected per-scope writer sequence, and commits one bundle on success
- that auto-initialization is a first-authority decision for the scope: after it happens, a later
  first device can no longer win the `initialize_local` path for that scope
- `WithinSyncBundle(...)` remains the strict low-level primitive for advanced callers that already
  manage scope lifecycle and exact `(user_id, source_id, source_bundle_id)` tuples themselves
- registered-table writes are captured by bundle triggers and finalized atomically with the
  business-table transaction
- `WithinSyncBundle(...)` does not auto-retry callback code on serialization/deadlock/lock-timeout
  failures; if your application wants retry, it must wrap the call explicitly and keep the
  callback transaction-safe

One push session, one committed bundle, and one `WithinSyncBundle(...)` transaction belong to
exactly one `user_id`. If application logic needs to affect multiple users, split that work into
separate per-user transactions or bundles.

### `ScopeManager` callback rules

`ScopeManager.ExecWrite(...)` runs direct PostgreSQL SQL inside one captured transaction.

Important rules:

- arbitrary SQL shape is allowed: multiple statements, joins, CTEs, trigger-driven writes, and
  writes across multiple tables
- ownership is enforced at row-execution time by the registered-table owner-guard triggers, not by
  static inspection of SQL text
- for `INSERT` into registered tables, omit `_sync_scope_id` unless you are setting it explicitly
  to the target scope
- for `UPDATE` and `DELETE` against registered tables, explicitly constrain affected rows to the
  target scope; `_sync_scope_id = ...` is the recommended default
- callbacks that produce no visible registered-table effects, including unregistered-only
  transactions, are rejected by `ScopeManager.ExecWrite(...)`

`ScopeManager.ExecWrite(...)` is meant for writes that must later become visible through sync. It is
not a generic helper for arbitrary unregistered business transactions.

Safe pattern examples:

```go
_, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
	WriterID: "admin-panel",
}, func(tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO business.users (id, name, email)
		VALUES ($1, $2, $3)
	`, userID, "Ada", "ada@example.com")
	return err
})
```

```go
_, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
	WriterID: "admin-panel",
}, func(tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
		UPDATE business.users
		SET name = $3
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, scopeID, userID, "Ada Updated")
	return err
})
```

### Writer IDs

`ScopeManager` takes a host-provided `WriterID`, which maps directly to the existing per-scope
`source_id` sequencing model.

Guidance:

- use one stable writer id per logical producer such as `admin-panel`, `billing-worker`, or
  `backoffice-tool`
- avoid collisions with client-managed source ids for the same scope
- prefixes such as `server:`, `worker:`, `tool:`, or `admin:` can help operational clarity, but
  the runtime does not require a specific format

### Business idempotency

`ScopeManager` owns sync-stream correctness, not business-command idempotency.

If the host app needs exactly-once semantics for domain operations such as:

- grant credit once
- append one audit event once
- apply one external command idempotently

that idempotency must be implemented by the application, not by `oversync`.

## First-connect lifecycle

`POST /sync/connect` resolves the client lifecycle without asking the app to choose between
"upload local" and "replace local":

- `remote_authoritative`: the scope was initialized previously, even if it is currently empty
- `initialize_local`: this source won the exclusive initialization lease and may seed from local
  pending rows
- `initialize_empty`: the scope was uninitialized and the server established authoritative empty
  state
- `retry_later`: another initializer currently owns the lease, or the server is applying a bounded
  empty-first deferral optimization

`retry_later` is a normal connect outcome, not an auth failure. Clients should retry after the
server-provided backoff.

Once a scope reaches `INITIALIZED`, remote remains authoritative for this feature set until some
future explicit reset feature exists.

## Registered Table DDL Requirements

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

Bootstrap validates these requirements and fails closed with an `UnsupportedSchemaError` if the
registered schema is outside the supported envelope.

## Read Path

- `GET /sync/pull` returns complete committed bundles only
- `POST /sync/snapshot-sessions` materializes one frozen current after-image inside PostgreSQL and
  returns a snapshot session id
- `GET /sync/snapshot-sessions/{snapshot_id}` returns deterministic chunks from that frozen
  snapshot without holding one long-lived database transaction across client round trips
- snapshot session creation may be bounded by optional row and byte caps in `ServiceConfig`
- rotated snapshot creation may carry explicit `source_replacement`; when present, the server
  reserves the replacement source in `sync.source_state` as `reserved` and retires the previous
  source with explicit lineage
- if a client checkpoint falls behind the retained bundle floor, the server returns
  `history_pruned`
- pull and snapshot creation fail closed before the scope reaches `INITIALIZED`

## Auth Contract

The handlers expect the caller to authenticate first and place
`oversync.Actor{UserID, SourceID}` into request context. The runtime does not require any specific
auth stack. The built-in transport helper is `oversync.ActorMiddleware(...)`, which reads
`Oversync-Source-ID` after host authentication has already established trusted `user_id` in
request context. `_sync_scope_id` is derived from `Actor.UserID`, enforced only on the
authoritative PostgreSQL side, and excluded from client-visible payloads, conflicts, pulls, and
snapshots.
