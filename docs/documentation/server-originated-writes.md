---
layout: doc
title: Server-Originated Writes
permalink: /documentation/server-originated-writes/
parent: Architecture
---

This page explains how to write registered PostgreSQL business rows on behalf of one scope so that
clients later receive those changes through the normal sync flow.

## When To Use This

Use server-originated writes when the host application needs to mutate registered business tables
outside client push handling, for example:

- admin or backoffice edits
- billing-worker adjustments
- workflow-triggered corrections
- server-side seeding of scope data

## Which API To Use

Use `ScopeManager.ExecWrite(...)` in the common case.

It is the convenience API for one scope-aware server write. On success it:

- auto-initializes an `UNINITIALIZED` scope as remote-authoritative empty
- allocates the next expected per-scope `source_bundle_id` for the chosen writer
- runs your callback inside one captured PostgreSQL transaction
- commits exactly one sync-visible bundle

Use `WithinSyncBundle(...)` only if your application already manages:

- exact `(user_id, source_id, source_bundle_id)` tuples
- scope lifecycle preconditions
- advanced retry/orchestration outside the convenience API

## Typical Workflow

1. Resolve the target `scopeID`.
2. Choose one stable `WriterID` for the logical producer, such as `admin-panel` or
   `billing-worker`.
3. Run `ScopeManager.ExecWrite(...)` with a callback that performs the business-table mutation.
4. Clients attached to that scope later receive the committed bundle through ordinary pull or
   snapshot flows.

Minimal shape:

```go
scopeMgr := oversync.NewScopeManager(syncService, oversync.ScopeManagerConfig{})

_, err := scopeMgr.ExecWrite(ctx, scopeID, oversync.ScopeWriteOptions{
	WriterID: "admin-panel",
}, func(tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
		UPDATE business.users
		SET name = $3
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, scopeID, userID, "Updated By Admin")
	return err
})
```

## Scope Initialization Behavior

`ExecWrite(...)` auto-initializes only `UNINITIALIZED` scopes.

That initialization is a first-authority decision:

- after `ExecWrite(...)` initializes the scope, a later first device can no longer win the
  `initialize_local` path for that scope

`ExecWrite(...)` does not hide `INITIALIZING` scopes. If another initializer currently holds the
lease, the call fails closed with the existing initialization error.

## Callback SQL Rules

`ScopeManager.ExecWrite(...)` runs your callback inside one captured PostgreSQL transaction.

Allowed:

- multiple SQL statements
- joins and subqueries
- CTEs
- writes across multiple registered tables for the same scope
- trigger-driven secondary writes

Important rules:

- `oversync` does not statically inspect SQL text
- row ownership is enforced at execution time by registered-table owner-guard triggers
- for `INSERT` into registered tables, omit `_sync_scope_id` unless you are setting it explicitly
  to the target scope
- for `UPDATE` and `DELETE` against registered tables, explicitly constrain affected rows to the
  target scope; `_sync_scope_id = ...` is the safest default
- callbacks that produce no visible registered-table effects, including unregistered-only
  transactions, are rejected with `ScopeWriteNoCapturedChangesError`

Safe `INSERT` example:

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

Safe scope-constrained `UPDATE` example:

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

## Writer IDs

`WriterID` maps directly to the existing per-scope `source_id` sequencing model.

Guidance:

- use one stable writer id per logical producer such as `admin-panel`, `billing-worker`, or
  `backoffice-tool`
- avoid collisions with client-managed `source_id` values for the same scope
- prefixes such as `server:`, `worker:`, `tool:`, or `admin:` can help operational clarity, but
  the runtime does not require a specific format

The same `WriterID` can be reused safely across different scopes because sequencing is per
`(scope_id, source_id)`, not global per writer id.

## Business Idempotency Boundary

`ScopeManager` owns sync-stream correctness, not business-command idempotency.

If the host application needs exactly-once behavior for domain operations such as:

- grant credit once
- append one audit event once
- apply one external command idempotently

that idempotency must be implemented by the application, not by `oversync`.

## Delivery Model

Server-originated writes are not delivered through any special admin-only path.

They become ordinary committed bundles, and clients receive them through the same mechanisms they
already use for synced data:

- `GET /sync/pull`
- snapshot rebuild after `history_pruned`

For executable end-to-end examples, see:

- [examples/nethttp_server/server/scope_manager_sync_test.go](/Users/pochkin/Projects/my/go-oversync/examples/nethttp_server/server/scope_manager_sync_test.go)
- [examples/nethttp_server/README.md](/Users/pochkin/Projects/my/go-oversync/examples/nethttp_server/README.md)
