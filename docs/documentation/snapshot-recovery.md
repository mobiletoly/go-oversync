---
layout: documentation
title: Clean recovery and safe snapshots
permalink: /documentation/snapshot-recovery/
---

Ordinary `Sync` in Go and Kotlin defaults to `automatic_clean`. When historical
pull state cannot be replayed safely, a completely clean client can install a
sealed authoritative snapshot and continue with ordinary pull after its boundary.
Explicit `Rebuild` uses the same guarded delta and clean-state checks. The
[r0001 contract](../../specs/r0001-oversqlite-durable-recovery.md) governs both.
C06/C07 platform, Swift, broader resource and final protocol qualification remain
separate gates; local/JVM evidence does not qualify those runtimes.

## Modes, status and retry

Go exposes `SetRecoveryMode(ctx, RecoveryAutomaticClean | RecoveryDisabled)`,
`RecoveryStatus(ctx)` and `RetryRecovery(ctx)`. Kotlin exposes
`setRecoveryMode(RecoveryMode.AUTOMATIC_CLEAN | RecoveryMode.DISABLED)`,
`recoveryStatus()` and `retryRecovery()` through the existing client result API.
Config sets the initial mode only for a fresh coordinator; subsequent mode changes
are durable. `disabled` preserves ordinary sync and exact upload outcome
resolution/ack while preventing automatic baseline jumps. Applications that need
every historical event should disable automatic recovery: a snapshot cannot
reconstruct skipped events.

Status is one current bounded record: phase, reason, owner, observed boundary,
object identities, at most 64 exact diagnostic keys, truncation and next action.
It contains no business payloads. An identity that exceeds the diagnostic byte
ceiling is omitted and truncation is set; keys are never shortened or trimmed.
The durable phases are captured, stage_baseline, prepare_transition, ready and
completed, or blocked. Ready and completion belong to the existing final atomic
writer; they do not create a second business transaction or application engine.

`RetryRecovery` re-arms one parked attempt. Duplicate calls coalesce. It cannot
clear dirty intent, replace a frozen request, settle a committed result, ignore a
mode change or bypass the clean gate. A changed relevant schema/dependency/mode
or an explicit retry may re-evaluate a blocker. Unchanged infeasibility stays
parked, including across reopening; unrelated later server sequence hints do not
cause another download. Transient transport/capacity responses retain the existing
bounded retry policy. Malformed snapshot evidence parks with a precise integrity
or limit reason.

The observation floor includes the blocked authoritative unit, checkpoint and all
installed newer observations. Successful installation records exactly snapshot T;
it does not change upload source sequencing, settle/requeue an upload or acknowledge
an outcome. Post-T writes arrive through ordinary pull. Mode and authority pins
are checked at resumed boundaries, actual dispatch and under the final writer.

One public Sync permits an initial plan and three relevant replans, one replacement
of an expired incomplete session, and 64 actual recovery dispatches shared with
preceding outcome resolution/ack. Retries, fallback and cleanup consume the same
parent budget. The existing 64 logical upload attempts and transport limits remain.
An essential ack that cannot dispatch remains ack_pending for the next invocation.
Useful large snapshots retain their sealed session/cursor across invocations.

## Admission and sealed transfer

The complete scope must be clean before recovery network traffic and at the
final writer: no synchronized dirty rows, frozen upload, unsettled committed
result, retained rejection or acknowledgment obligation. A committed upload that
cannot settle safely stays blocked. Rebuild cannot bypass it. A later local
synchronized write survives and prevents snapshot installation.

The server seals source, generation, minimum requested observation, snapshot T,
row/byte totals and a streaming hash over the immutable projection. Recovery
creation rejects an outstanding unacknowledged server outcome and a floor above
the current committed sequence. History pruning does not remove the current
projection. Writes after T do not change the sealed session.

The existing operation state retains the complete seal and staging cursor. Every
actual create/page dispatch, including transport/capacity retries and Kotlin's
one-row fallback, revalidates its pinned authority and clean state. Main/TEMP
schema, service/attachment/generation/registration, local revisions and negotiated
limits participate in that authority. TEMP definitions are fingerprinted because
connection-local TEMP schema counters do not survive reopening.

An incomplete expired session may be replaced once in an invocation. A confirmed
source replacement is never replayed to rotate again; refresh uses its already
established source. Complete verified local staging remains usable after server
expiry, after revalidating its seal and local context. HTTP cleanup retains its
separate deadline and the existing invocation's recovery exchange budget.

## Supported boundary and validation timing

Existing registration/open/upgrade admission checks govern keys, declared codecs,
managed columns, logical FKs and coordinated schema generations. They are not
expanded into a general ban on ordinary application triggers. Static uniqueness
capability checks run before automatic snapshot requests; changing main/TEMP
facts and affected trigger/dependency closure are checked again during preparation
and under the final writer. Trigger-owned *affected* mutations and unsafe local
cascades are recovery blockers, even when ordinary captured writes remain supported.

| Area | Supported contract | Excluded or blocked |
| --- | --- | --- |
| Application writes | Ordinary INSERT/UPDATE/DELETE and transactions through existing supported connections/change capture. | Bypassed capture, direct `_sync_*` edits or disabled required constraints. No new SDK-only write API. |
| Record identity | One visible primary/sync key per managed table, with existing text/UUID logical and SQLite storage mappings. | Composite sync keys or arbitrary identity mappings. |
| Values | Existing declared text, UUID, boolean, int64, decimal, float64, bytes and JSON codecs, including their exact range/nullability/representation rules. | Unmapped SQLite values and generated/hidden columns on managed tables. |
| Relationships | Existing admitted single-column logical FKs between registered tables, including self-references and cycles, with existing action/scope/deferrability rules. | Composite managed FKs and outgoing managed references to unregistered parents. No blanket promise for arbitrary FK actions/topologies. |
| Unique constraints | Plain-column uniqueness, including multi-column keys such as `UNIQUE(parent_id, name)`, under validated SQLite comparison semantics. | Partial or expression-based unique indexes, for example an index on `lower(email)` or one with a WHERE predicate. This restriction concerns uniqueness planning, not all nonunique indexes. |
| Local-only state | Unrelated local tables, admitted local-only columns, and exactly identified dependent rows whose full values can be preserved. | Incidental cascading loss; unknown dependency membership or an impossible preservation closure blocks recovery. |
| Application triggers | Existing ordinary-sync supported behavior remains. Recovery may leave an unchanged table with a trigger untouched. | The new snapshot installer cannot mutate an application-trigger-owned table, including TEMP triggers. This is not a blanket ordinary-sync trigger ban. |
| Schema changes | Existing coordinated schema/generation upgrade contract. | Uncoordinated managed DDL under an old admitted contract; recovery does not repair arbitrary schema drift. |
| Automatic baseline recovery | Clean scope with no dirty intent, F, unsettled C/rejection or ack_pending; full delta is provably safe. | Dirty snapshot merging, choosing winners or using S to bypass unsafe committed settlement. |
| Constraints and scale | Existing CHECK/NOT NULL/FK/uniqueness enforcement and explicit atomic/workspace limits. | Disabling constraints, invented temporary values, or promising unbounded rows/cycles to force completion. |

Managed physical/generation columns must still match exactly. Local-only columns
are conditional preservation obligations, not a new projection feature. Partial
or expression uniqueness receives its existing precise planner/capability blocker;
unrelated nonunique indexes and unrelated local tables are not blanket exclusions.

## Installation and supported dependencies

The installer computes a key-based delta and sends it through the existing shared
final-state application engine. It includes current unique-value holders and
managed FK closure, preserves unchanged rows and local columns, and records
observable SQLite rowids for snapshot rows that need temporary vacating. Ordinary
upload settlement retains its established newer-edit behavior.

Application triggers on affected main/TEMP business tables are blockers before
DML. SDK trigger definitions must match exactly. Triggers on untouched tables do
not block unrelated work. Partial/expression unique indexes retain precise
unsupported diagnostics; static restrictions are inspected before download where
possible.

An unmanaged incoming FK set must be proved empty, or meet the supported deferred
NO ACTION case with unchanged typed referenced values and writer-owned membership
proof. A nonempty local-only CASCADE set that would lose rows blocks before DML.
Supported clean managed descendants survive; a dirty descendant fails admission.
No trigger suppression, FK disabling or clear-and-reinsert fallback is used.

Preparation, graph edges, payload/rowid copies, statement storage, schema catalog
scans, unindexed membership scans, hashing and final atomic work are admitted
through the existing workspace/work owners. Cancellation, late children, changed
schema or mutation failure rolls back the final business transaction. Snapshot
absence is recorded as absence at T, not as an invented tombstone version.

Generation opening keeps migrated business rows, preserves the maximum installed
observation across U5, binds the U6 receipt to the full seal, and uses the same
installer at U7/U8. It does not fabricate source counters before replacement is
confirmed. Existing opening ownership blocks synchronized writes during that
transition.

## Ownership and replacement tests

| Superseded path | Retained owner and replacement evidence |
| --- | --- |
| Snapshot clear/reinsert and prepared-outbox overlay/retarget | Shared Go apply plan/unique/FK/upsert/workspace and Kotlin ApplyLifecycle/ApplySurvivors/LocalStore; clean-admission, full-vector delta and immutable-pending source tests. Explicit authorized local reset remains a separate lifecycle action. |
| Snapshot skip-current-holder/skip-FK-walk assumptions | Existing final-state graph and current unique holders; shared unique fixtures, cycles/transfers and managed descendant tests. |
| Orphan Kotlin SnapshotUpsertPlan/factory | Shared LocalStore upsert; ReplaySql typed-codec/cache tests, snapshot delta and physical restart proof. |
| Connection-local TEMP counters as durable identity | Actual TEMP definition fingerprint; authority/reopen and between-page/refresh tests. |
| Scratch false-audit/local-CASCADE loss witnesses | Logical audit/local-only closure, exact empty/deferred membership and rollback regressions before mutation. |

The only public additions are mode, current status and retry controls. No public
recovery decision, merge model, intent epoch, journal family, quota, report or preview
API was added. The existing operation owns snapshot
progress; the existing application engine owns final DML.

Synthetic activity fixtures preserve the pinned ten-file SQLite reference,
fourteen managed tables, local settings, CHECK/enums, self-referencing foreign keys, multi-column uniqueness,
nullable unique values and explicit Boolean/numeric/date/JSON wire mappings. The
runtime fixtures live in these implementation repositories and never open the
reference application checkout. DT01–DT06 local and actual HTTP evidence, failed
attempts, review corrections, native counts and cleanup are recorded in the
[C04 ledger](../../.artifacts/durable-recovery-snapshot-20261023/ledger.md).
Compilation or structural inspection does not qualify platform runtime behavior.

## Complexity review

New snapshot transfer, preservation, seal and request-admission owners have
cognitive complexity below 30. Existing transactional orchestration retains these
explicit C04 exceptions: server CreateSnapshotSessionWithRequest (61 to 80),
Opening.resumeTargetSnapshot (49 to 52), final snapshot-state publication (53 to
48), the final atomic apply slice (29 to 31 in C04; 33 with C05 ready/completion publication), and the create-snapshot HTTP boundary
(30 to 31). They keep fence/materialization/hash/commit, opening publication,
transaction cleanup and error precedence together. Splitting individual branches
only to lower a score would obscure those transaction responsibilities. Larger
existing materialization/migration/Sync hotspots remain incremental cleanup work.
The before/after owner comparison, gocognit, gocyclo and funlen diagnostics are in
the C04 evidence. Unrelated default line/statement limits are diagnostics, not
qualification gates. No reliability claim follows from fewer source lines.

## Automatic coordinator ownership and evidence

The existing operation singleton owns bounded current recovery metadata. Existing
upload journal/outbox owners retain F/C/rejection/ack; existing snapshot transfer
owns S; the shared final-state apply engine owns DML. The coordinator chooses when
those existing owners may run. It replaces the unconditional checkpoint-recovery
snapshot entry with typed historical eligibility and a durable parked witness.

Incoming local-only membership uses a streaming typed multiset digest. Catalog
names are traversed once and sorted only within an admitted host owner; column/FK
SQL visitation, sort comparisons, generated statement bytes, row copies and held
catalog descriptors remain charged together. Final ready/completed metadata is
admitted before first business DML. No cleanup copy is free before release.

Public automatic, disabled, dirty/pending, mode/schema/TEMP/comparison/child races,
large-session dispatch resume, physical process death, file/WAL cleanup and DT07
are recorded in the [C05 ledger](../../.artifacts/durable-recovery-automatic-20261023/ledger.md).
Its independent review dispositions and frozen handoff distinguish fresh runtime
proof, reused C04 evidence, failures and remaining qualification gates. Historical
source5862 SQL/accounting comparisons remain immutable historical input; current
v1 outcome-resolution tests retain functional preservation and handle bounds and
claim no performance equivalence.
