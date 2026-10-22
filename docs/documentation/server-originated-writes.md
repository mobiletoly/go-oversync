---
layout: doc
title: Server-Originated Writes
permalink: /documentation/server-originated-writes/
parent: Architecture
---

Server-originated writes mutate registered PostgreSQL business tables for one scope and publish the
committed effects through the ordinary sync history. The callback contract is intentionally
retryable: application code must treat the callback as transaction-pure and may see it invoked up to
three times for one public call.

## Choose the API

Use `ScopeManager.ExecWrite(...)` for the common server-write case. It allocates the next writer
bundle ID, auto-initializes an `UNINITIALIZED` scope, and returns a durable `CommittedBundleRef`.

Use `WithinSyncBundle(...)` only when the caller already owns a stable exact
`(user_id, source_id, source_bundle_id)` tuple. It does not allocate a source bundle ID or replace
the caller's source-order policy.

Both APIs require a stable logical-operation hash and admission deadline. `ExecWrite` additionally
requires a stable, nonzero operation UUID. Generate and persist these values when the application
accepts the logical command, then reuse them across process restarts, ordinary retries, and an
unknown commit result. Do not generate a new identity inside a retry loop.

Persist the deadline in application-owned durable storage before the first public write attempt.
The Oversync receipt is not that storage: a definitely rolled-back attempt, or a lost response from
an attempt that later proves rolled back, leaves no receipt from which a restarted caller could
recover the original deadline. The integration server's `server_operation_deadlines` table demonstrates
the required pre-attempt persistence pattern.

## `ScopeManager.ExecWrite` example

This example assumes `command.ID`, `command.ValidUntil`, and the canonical command bytes are durable
application data. The hash must cover every value that can change callback SQL.

```go
operationHash := sha256.Sum256(command.CanonicalBytes())
scopeMgr := oversync.NewScopeManager(syncService, oversync.ScopeManagerConfig{})

result, err := scopeMgr.ExecWrite(ctx, command.ScopeID, oversync.ScopeWriteOptions{
	WriterID: "admin-panel",
	RetryableWriteOptions: oversync.RetryableWriteOptions{
		OperationID:         command.ID,
		OperationHash:       operationHash,
		OperationValidUntil: command.ValidUntil,
	},
	RequiredEffectTables: []oversync.RegisteredEffectTable{
		{Schema: "business", Table: "users"},
	},
}, func(callbackCtx context.Context, tx oversync.DatabaseWriteTx) error {
	_, err := tx.Exec(callbackCtx, `
		UPDATE business.users
		SET name = $3
		WHERE _sync_scope_id = $1
		  AND id = $2
	`, command.ScopeID, command.UserID, command.NewName)
	return err
})
if err != nil {
	// Handle the typed outcomes described below.
}
log.Printf("committed bundle %d with %d effects", result.Bundle.BundleSeq, result.Bundle.RowCount)
```

`OperationValidUntil` is checked against PostgreSQL time after operation-receipt ownership is
acquired. A first attempt must be in the future, no more than 90 days ahead, and have no
sub-microsecond precision. Crossing it after admission does not cancel an in-flight transaction.
Completed receipts remain replayable until their retention horizon.
After successful bootstrap, each Sync instance runs the bounded receipt cleanup lane once per minute
and once immediately after bootstrap. Each run executes at most four independent five-second,
500-row `READ COMMITTED` batches using ordered `FOR UPDATE SKIP LOCKED`; `Close` cancels and drains
the worker.

`RequiredEffectTables` and `ForbiddenEffectTables` are optional frozen registered-table sets. They
must be disjoint. Forbidden tables are checked against raw capture, so insert-then-delete does not
hide a forbidden effect. Required tables must have a normalized committed effect.

## Retryable callback rules

The callback may run three total times. Oversync retries only PostgreSQL SQLSTATE `40001`, `40P01`,
and `55P03` after definite rollback, waiting 25 ms and then 50 ms without jitter. Cancellation
interrupts either wait. Sequence allocation may leave gaps after a rolled-back attempt.

Inside the callback:

- use only the supplied `DatabaseWriteTx` and the supplied `callbackCtx`
- keep all business effects in that PostgreSQL transaction
- use deterministic in-memory calculation whose results are not retained between attempts
- do not mutate process-global state or call networks, filesystems, messaging services, email, object storage,
  telemetry publishers, or other external systems
- do not use a captured pool, connection, or outer context
- do not recursively call a transaction-owning Oversync API
- do not issue transaction control, `SET`, `RESET`, `SET CONSTRAINTS`, role/search-path changes,
  `set_config`, or trigger/constraint-altering settings
- audit invoked PostgreSQL functions and procedures for non-transactional side effects

Oversync lexes callback SQL before execution. It accepts one DML/read statement, rejects
transaction/session-control statement classes, rejects direct `set_config` calls including quoted
or comment-disguised spellings, and fails closed on Unicode-escaped identifiers. Invoked database
functions are still trusted-caller obligations because Oversync cannot sandbox their bodies.
Violating those remaining obligations can repeat a non-transactional effect and invalidates the
API's retry and atomicity guarantees.

The capability exposes only `Exec`, `Query`, and `QueryRow`. It has no commit, rollback, begin,
connection, batch, copy, or prepare escape hatch. `Query` still returns `pgx.Rows`, but its adapter's
`Conn()` method safely returns `nil`.

Registered-table owner guards enforce the exact active scope. Omit `_sync_scope_id` on insert unless
setting it to the exact target scope, and constrain update/delete statements by the exact scope.
New operations that produce no normalized registered-table effects fail closed.

## Replay and error handling

The durable operation receipt binds the operation hash, exact writer/source tuple, validity
deadline, and required/forbidden table sets. An exact completed replay returns the stored result
without callback execution or a new bundle. Reusing an identity with changed content returns
`OperationReplayChangedError`.

Handle these outcomes explicitly:

- `CommitOutcomeUnknownError`: never infer rollback. Repeat the same public operation with the
  exact same identity, hash, deadline, writer/source tuple, and effect-table contract.
- `OperationHistoryExpiredError`: no receipt exists and the admission deadline has passed. Do not
  reuse the identity for different work.
- `OperationHistoryUnavailableError`: an explicit source tuple is already committed but its receipt
  is absent. Recover through application policy; do not rerun the callback under a new identity as
  though rollback were proven.
- a typed PostgreSQL retryable error after the third attempt: the transaction definitely rolled
  back, but the bounded retry budget is exhausted.

This is bounded idempotency, not an exactly-once claim. Applications remain responsible for logical
command identity and for external-effect idempotency outside the callback.

`ScopeWriteResult.Bundle` is a durable `CommittedBundleRef`, not a materialized bundle. It includes
the sequence, source tuple, row count, bundle hash, and canonical request hash. It intentionally has
no `Bundle.Rows`; fetch retained payloads through ordinary pull/history APIs. Receipt replay returns
the same summary even after `sync.bundle_rows` and `sync.bundle_log` pruning.

## Source ownership

Use source IDs distinct from client Connect/upload sources within each scope for both trusted APIs. A client source remains client-owned after acknowledgment, binding cleanup, retirement, history pruning and restart. Both trusted APIs reject a client-owned source before receipt replay or callback execution; client Connect rejects a trusted-writer source with `source_generation_conflict` on `source_id`. Source strings in different scopes are independent. Distinct trusted writers retain ordinary receipt replay and committed sequence allocation. This owner-approved durable-recovery rule supersedes earlier shared-source examples; it does not change server-write receipt or reserved-ID guarantees.

## Reserved server writer IDs

Trusted server publishers may reserve exact visible-ASCII source IDs (1-256 bytes). Configure the
same frozen set on every Sync instance and at the HTTP actor boundary:

```go
reserved := []string{"server:billing"}

serviceConfig.ReservedServerSourceIDs = reserved
actorMiddleware := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
	UserIDFromContext:       userIDFromContext,
	ReservedServerSourceIDs: reserved,
})
```

Client actors and snapshot source replacement cannot use a reserved ID; trusted
`ScopeManager.ExecWrite` can. The first committed Scope write publishes `ever_used=true` in the same
transaction. That tombstone never returns to false and survives scope, source-state, and receipt
cleanup. Writer rotation retains every used reservation in the configured set.

Adding or removing a reservation is an offline layout-configuration operation under the bootstrap
advisory lock and the required table locks. An unused reservation can be removed only when no source
state or Scope receipt references it. A used reservation is never client-eligible again. The managed
reservation trigger acquires those locks, rejects additions that conflict with any client source
history, and rejects removal of referenced or used rows; operators must still stop all participating
processes and deploy one exact frozen set.


## Final-state capture and local application

Capture records which registered identities were touched. After deferred database
work completes, Oversync rereads their final rows in the same transaction and
compares them with prior authoritative row state. History contains the final
insert/update/delete for each identity in first-touch order. An absent-to-absent
insert/delete is omitted, while raw forbidden-table evidence remains effective.
Business data, history, row state, source advancement, notification and receipt
completion commit together. Final encoded images must pass the normal limits.

Established reserved writers hold compatible reservation locks until commit.
Different scopes can use one writer concurrently. First use may wait for another
first-use transaction; a rolled-back first use does not mark the reservation
used. A committed use leaves a permanent used tombstone and excludes offline
reservation removal.

Clients stage their complete intended images before applying a bundle. SQLite
orders unique-value transfers by their actual uniqueness dependencies. Safe
cycles temporarily vacate only their component and dependent closure inside the
atomic apply transaction. Wire order, hashes and identities do not change.

A client-side `FinalStateApplyBlockedError` (Go) or
`FinalStateApplyBlockedException` (Kotlin) leaves business data, pending intent
and the checkpoint intact. Its bounded `reason`, operation, table, index,
trigger and key fields identify the correction:

- `conflicting_desired_state`: resolve the competing records through supported
  application edits or conflict resolution. If the authoritative server itself
  contains duplicate values forbidden by the client schema, correct the records
  on the server or align the application's schema contract.
- `unsafe_trigger_or_dependency`: revise the named application trigger or
  dependency through the application's schema-upgrade path so temporary deletion
  has no unknown effects.
- `unsupported_uniqueness`: revise the named partial/expression unique index or
  obtain SDK support for its semantics through a supported application upgrade.

Retry ordinary sync after a local correction. Schema/intent guard changes
invalidate only derived preparation. A later server write cannot rewrite an
invalid retained bundle: after correcting authoritative state, use the supported
`Rebuild`/`rebuild` snapshot API to install current state while retaining pending
intent. Rebuild retains its dirty/outbox/conflict admission checks: resolve the
pending operation through supported APIs or fix its blocking schema first; never
clear a frozen operation to force recovery. Do not alter `_sync_*` tables, skip a checkpoint, or
remove the database to bypass a blocker. A valid supported final state that
blocks is an SDK defect; retain the diagnostics for a focused regression.

This repair targets fresh server and client databases. No migration or repair of
history produced by earlier capture behavior is included. Provision a separate
empty application database with the current application schema, bootstrap the
current server, and create new SDK databases using the current generation
contract. Recreating existing databases, deployment and publication require their
own authorization; this change does not perform them.
