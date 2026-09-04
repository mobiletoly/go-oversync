---
layout: default
title: Go SQLite client upgrades
---

# Go SQLite client upgrades

`NewOpening` is the single public path for fresh creation, exact ready reopen, same-proof local
migration, and generation replacement. It owns the SQLite file and pool before initialization and
does not publish ordinary application access until the durable ready commit succeeds. A ready
result contains the owned `Client`; every non-ready or actionable result has `Ready == nil`.

Supply the desired target `Config` on every open. An exact already-ready target needs no historical
plan and performs no network request. Fresh, pending, or target-changing work requires the exact
`UpgradePlan`. Do not construct codec declarations or modify package metadata to bypass admission.
Prototype sync databases are not migrated in place.

## Migrator ownership

Implement `LocalMigrator.InstalledVersion` from an application-owned durable version source.
`Migrate` receives a capability tied to the opening transaction. It can change application
tables and the application version in that transaction. SQLite transaction control, connection
attachment, and writes to `_sync_*` tables fail through a SQLite authorizer. The capability expires
when the callback returns.

The callback must not perform HTTP, token-provider, conflict-resolver, progress-observer, or other
remote work. The opening admits generation discovery before the transaction and performs source
replacement after the migration commit.

## Runtime states

Generation changes use these durable phases:

```text
ready -> upgrade_started -> resolving_old_intent -> migrating_local
      -> local_migrated -> reattach_pending -> rebuild_running -> ready
```

No application database capability is published in any non-ready phase. U3 durably records
`migrating_local` before the opening enters the U4 migration transaction, but the application still
has only the `Opening` control surface. Guard triggers reject synchronized and declared local-only
writes after the first freeze commit. Opening-owned settlement and snapshot work use a
transaction-scoped bypass. The ready client is prepared inside the final transaction and ownership
is transferred in memory after commit; a late cancellation cannot turn committed ready into a
non-ready result.

`needs_resolution` preserves the frozen old-generation request. Call
`ResolvePendingIntent` with an explicit retry, discard, or merge decision. The opening increments
the resolution revision and resumes under the old source and proof. Use the exact
`OpeningStatus.PendingRequestHash` returned with that revision when constructing the resolution;
the binding prevents a decision for one frozen request from being applied to another. A target
snapshot cannot clear old dirty rows or an unresolved receipt.

Cancellation stops at the next safe boundary and leaves the journal resumable. A network error
returns `waiting_for_network`. Restart with the same plan. The opening reuses the persisted
replacement tuple after reply loss and resumes a validated snapshot prefix. The U5 source rotation
and U8 ready publication are irreversible after their transactions commit.

Ownership is keyed by the SQLite file's filesystem identity, not only its pathname. Symlinks and
hard links therefore cannot acquire separate in-process or cross-process owners, even when
processes use different temporary-directory settings. `Close` cancels active opening work but
retains ownership until that work reaches a safe boundary and exits. A frozen old request is durable
outbox evidence: a transient replay failure blocks the U2 drain witness until exact replay proves
commit or a definitive rejection permits the drain.

## Fresh and same-proof modes

`UpgradeModeFresh` accepts an anonymous empty database, creates the final application schema through
the migrator, installs guarded sync triggers, and publishes the initial identity. The initial
`migrating_local` journal and all opening metadata are established in one transaction: death
before that commit leaves an empty business database, while death after it resumes the same plan.
Fresh mode performs no remote request.

`UpgradeModeLocalOnly` requires the same generation proof. It can run offline and retains source
sequence, attachment, checkpoints, dirty rows, outbox, and snapshot state. The opening
hashes synchronized state around the migration and rejects a changed digest. Application changes to
declared local-only tables remain allowed inside the migration transaction.

## Boundaries

`float64` remains a canonical decimal string on the protocol wire. SQLite stores its local
representation as `REAL`. PostgreSQL `float4` and `float8` columns both use this token, and committed
`float4` values may reflect PostgreSQL storage normalization. Incoming wire values are validated
strictly. Use the decimal codec when the value must be exact rather than IEEE 754 floating point.

Table evolution extends the opening to add synchronized tables and omit tables from a new
generation through an application-supplied migrator. Shared sync keys, column types/nullability
and visible FK endpoint/action semantics stay unchanged. A new FK on an existing table must use
newly visible, nullable child columns. All added relationships, including those on added
tables, require nullable child columns regardless of their delete action. Omitting a child
table or FK column removes that relationship from
the local view. Each target contract must still be FK-closed and match real SQLite constraints.

All installed-generation intent is settled before local table removal. This includes dirty deletes,
conflicts, frozen requests with lost replies and newer edits that survive replay. The opening
continues draining newer intent only after a durable source-sequence advance; an unresolved
conflict blocks migration. Source rotation and the full target snapshot retain the opening's durable
boundaries. Local-only tables survive unless the application migration explicitly changes them.
Server table omission retains its physical table, active catalog identity and old-generation history.

The [worked table/FK controller](../integration/schema_evolution/table_evolution/README.md) closes
admission, expires and cleans staging with a private capability, appends empty tables before the
old-child/new-parent FK is installed, then populates through captured writes. PostgreSQL transition
writes/readiness remain under transition support. SQLiteNow runtime and generator work are separate
from this Go implementation.

Target snapshot accounting is bounded: staged rows are verified in fixed-size pages on resume, and
each chunk transaction durably advances both row and encoded-byte progress without rescanning or
materializing the complete staged prefix. Database files created by an older unreleased prototype
control layout are not migrated in place. In particular, an `_sync_outbox_bundle` without durable
`active_push_id` evidence fails closed and must not be opened as if no session existed.

Run the portable fixture verifier with:

```text
env GOTOOLCHAIN=go1.25.12 go run ./cmd/client-upgrade-conformance -root conformance/client-upgrade/v1
```

The default runner validates the manifest and JSON Schema, then discovers and executes every Go
test named by the portable scenario and invariant mappings. Use `-go-adapters=false` only for the
standalone artifact/schema check; it is not complete Go conformance evidence.

## Retired generations

An authenticated request with an exact retired generation proof returns HTTP 426,
`retired_schema_generation`, `retryable: false`, and remediation `upgrade_application`.
This applies before source or session lookup, including a source that has never connected.
Malformed proofs and wrong fingerprints retain their distinct validation errors.

Go clients expose the typed schema-generation error and stop unchanged-generation sync
and watch retries. Retirement does not clear business rows, pending uploads, conflicts,
checkpoints, or source identity. A committed upload whose reply was lost is still unresolved
local work; retirement does not authorize replaying it under a new source or generation.
Automatic loops also persist `upgrade_application` in the SQLite control schema.
`ApplicationActionRequired(ctx)` reads that state without network access, including after the
client is closed and reopened on the same file. `SyncStatus.ActionRequired` carries the same
state whenever ordinary connected status is available. A completed upgrade to a different
generation proof is not gated by an older proof's retained action record.

The normal opening can migrate an empty retired client to a supported
application generation, rotate its source and rebuild. Unresolved old work keeps the
opening blocked before destructive phases. Applications must preserve that evidence
and request the appropriate application action; there is no automatic discard or retired
upload recovery route.

See the [rollout operator guide](../integration/schema_evolution/ROLLOUT.md) and
[Go qualification coverage](../integration/schema_evolution/QUALIFICATION.md) for the
server maintenance, permanent identity, retention and recovery requirements.
