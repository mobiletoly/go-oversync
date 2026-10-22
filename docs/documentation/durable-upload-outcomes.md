---
layout: documentation
title: Durable upload outcomes
permalink: /documentation/durable-upload-outcomes/
---

The fresh server format retains one complete terminal upload result per
`(user_pk, source_id)` independently of bundle-history retention. The sole wire
marker is `v1`; old databases and old clients require recreation/replacement at
adoption. Go and Kotlin clients now retain exact uploads, settle terminal results
durably and acknowledge under the original binding. Explicit clean snapshot
installation now uses the [guarded delta installer](../snapshot-recovery/).
Automatic clean recovery remains a later phase. The governing contract is
[`r0001`](../../specs/r0001-oversqlite-durable-recovery.md).

## Durable ownership and bounds

`sync.push_protocol_store` has one `singleton=true` row. Its format and
`max_outcome_bytes` are immutable. `MaxPushOutcomeDecodedBytes=0` chooses
`MaxBytesPerBundle+8192`; an explicit value must be between that minimum and
`67117056` bytes. Every service bootstrap compares its normalized configuration
with the stored ceiling. SQL rejects updates/deletion and oversized inserts.
There is no migration, prepaid slot, global quota or global admission counter.

`sync.push_source_progress` is keyed by `(user_pk, source_id)` and references the
existing source row. It retains a nonnegative signed-64-bit acknowledged sequence.
At zero the acknowledgment fields are all null. At a positive sequence it stores
one original positive signed-32-bit generation ID, its 64-byte lowercase-hex
fingerprint, and the 64-byte request and outcome hashes. Source IDs retain the
existing exact grammar and 256-byte bound. Thus acknowledgment metadata does not
grow with operation count. It deliberately has no generation foreign key: a lost
ack response can be retried after the original generation is retired or the
source binding changes. A normal source deletion cannot erase a positive fence.

`sync.push_outcomes` shares that primary key and references progress. Its bounded
metadata is a positive signed-64-bit operation sequence, positive signed-32-bit
generation ID, three exact 64-byte hashes/fingerprint, nullable UUID initialization
ID and one timestamp. The single `BYTEA` envelope contains the complete compact
committed effect or rejection/cancellation evidence, including the outcome hash.
No other copy or payload archive is retained. SQL verifies that an insert is
exactly acknowledged+1, checks the persisted ceiling, and rejects updates. The
whole serialized envelope is bounded before insertion and business commit; a
small upload whose server-expanded effect exceeds that ceiling rolls back with
nonterminal `413 push_outcome_too_large`.

No TTL evicts an unacknowledged outcome. Acknowledgment atomically copies its
original binding and hashes into the compact latest fence and deletes the bulky
record. Exact latest duplicate acknowledgment returns 204; mismatched latest or
future acknowledgment fails, and an older acknowledged operation returns 410.
Cancelled/rejected operations consume sequence just as commits do. Only business
commits advance `max_committed_source_bundle_id` and global bundle history, so
operation sequence and committed bundle sequence can have different gaps.

## Transaction and lifecycle order

The established per-user upload gate and PostgreSQL session advisory lock are
shared by push, resolve and ack. Inside the owned transaction, application scope
admission precedes the existing user/scope authority lock, source state,
generation binding and upload-progress locks. Reserved server-source reservation
rows retain their existing key-share concurrency; this store introduces no global
exclusive reservation lock. Unrelated scopes can progress while one source waits.

Push first classifies its immutable tuple against progress and retained evidence.
Both fresh requests and exact replays validate immutable row shape, codec,
canonical hash and request bounds. Replays return the stored bytes without
consulting mutable business rows or running triggers again. Resolve uses the same
serialization: it returns the retained result or permanently cancels exactly the
next operation. A delayed push cannot commit a cancelled tuple, even if the process
that returned cancellation dies.

A business savepoint owns initialization changes, capture context, business and
deferred-trigger effects, row-state/history publication, committed-source progress
and initialization completion. A declared application SQLSTATE or real row
conflict rolls back that entire savepoint before the outer transaction retains a
safe rejection. Unknown SQL, storage failures and uncertain transaction errors
remain nonterminal. PostgreSQL savepoint rollback also restores transaction-local
capture context and initialization state. The complete projected result and all
successful business/history effects commit in the same outer transaction.

Source retirement, binding destruction and startup generation removal cannot
destroy an outstanding result. They acquire existing lifecycle locks and fail
while the outcome remains. Latest duplicate ack matches its original retained
binding before mutable source/generation admission. Explicit administrative scope
deletion retains the existing scope exclusion contract and may cascade its
run-owned state; it does not become a normal recovery shortcut.

## Operational evidence

The trusted Go `PushOutcomeStorage` method reports outstanding count, serialized
payload bytes, oldest age and the store tables' heap/index/TOAST allocation via
PostgreSQL relation sizes. Physical allocation can remain after logical ack
cleanup. WAL and total database size remain separate operational measurements.
These measurements do not establish a global storage cap and add no HTTP report
or decision API.

## Replacement and shared owners

`PushOneShot` replaces the old prunable-history replay lookup. The unused
`loadCommittedPushMetadataBySourceTuple` and `PushReplayMismatchError` are removed;
there is no fallback lookup. `TestDirectPush_PrunedCommittedTupleRetainsExactOutcomeWithoutMutation`
and `TestPushOutcome_HistoryPruning` replace the old history-pruned result test.
Persisted source-ID validation now exercises the retained-outcome loader in
`TestPhase3PersistedAndNotificationSourceIDsFailClosed`.

The implementation continues to use the existing generation projection and
compact codec, business apply/conflict engine, deferred-effect capture,
final-state coverage, retryable owned transactions, scope admission, history
retention and lifecycle admission. It does not introduce a parallel application
engine. Nine `TestPushOutcome_` families exercise atomic publication, rejection
rollback, both race orders, sequence/ack fences, pruning/restart, lifecycle
binding, bounded retention/storage failure, killed independent service processes
and unrelated-scope progress on PostgreSQL 16 and 17.10. Native reports and the
current C02 ledger determine which lanes actually ran; test existence alone does
not establish qualification. Snapshot installation, mapped synthetic activity runtime
acceptance and the full platform/final matrix remain later phases.

C02 server outcomes and the accepted distinct-owner rule have passed scoped
independent review, the nine required families on both PostgreSQL versions and
the targeted race checks. C03 completes Milestone A's scoped client settlement
and acknowledgment proof. Full protocol qualification remains incomplete. See the
[C02 handoff](../../.artifacts/durable-recovery-server-20261022/handoff.md).

## Client journal and settlement ownership

The fresh SQLite format adds one `_sync_upload_journal` row beside the existing
outbox. It retains the exact encoded F with its byte digest, original scope,
service/source/generation, existing attachment epoch and initialization token,
then the complete validated C with its outcome hash. Original row bases and
capture guards remain in `_sync_outbox_rows`. Binding metadata is bounded to 4 KiB;
encoded F is bounded to 64 MiB and C to 64 MiB plus 8192 bytes, within existing
admitted limits and aggregate workspace accounting. Kotlin retains the original
admitted generation contract and its digest with its existing contract owner so
reopening does not require mutable discovery before resolving old work.

A new upload dispatches the stored F. An uncertain resumed F uses resolve under
that exact binding. A stored C is validated again and applied locally without
another business request. C commits before derived planning or application. The
existing whole-row final-state engine still owns normalization, effects outside
F, newer-edit and local-only preservation. One guarded writer transaction records
local disposition, consumes the source operation sequence and marks `ack_pending`.
Unsafe committed application rolls back and retains F/C and the full local state;
it sends no ack and cannot enter snapshot installation.

Rejected operations retain actual row/application policy evidence and original
captures under the outbox's `rejected` state. Protocol disposition precedes ack,
and automatic resolver invocation follows ack. Explicit guarded policy actions
may complete while ack remains pending; a successor freeze waits for both ack
and policy completion. Retry requeues unsatisfied current intent for a successor
identity. Cancellation also requeues through the existing capture owner. Neither
status invents dirty revisions or repeats sequence consumption.

Ack uses the original proof even after server generation retirement or rebinding.
After a lost response, reopening retries the same ack without reapplication or a
new freeze. Existing Detach/Uninstall guards include this durable obligation.
Generation opening completes only the old journal before mutable target admission;
normal migration, old-intent policy and snapshot owners then retain their ordering.
Initialization lease renewal is explicit Attach after protocol/policy release; ack
never depends on Connect. Recovery HTTP dispatches, including actual transport
retries, share the existing invocation's 64-exchange ceiling. Ordinary upload adds
one ack; no-change Sync adds no recovery probe.

The old response replay/standalone conflict409 decoders and frozen-source
retarget/discard paths are replaced. Historical conflict parsing is test-only.
`TestRecoveryJournal_*`, matching Kotlin common tests, independent Go/JVM process
kill loops and real HTTP upload tests own replacement proof. Existing replay,
unique dirty-closure and A1 tests now assert stored-C settlement, original binding
and no snapshot bypass. This is ownership consolidation around the shared apply,
outbox, conflict, transaction and workspace engines; line count is not evidence
of reliability. Exact executed bodies and retained failures are in the
[C03 handoff](../../.artifacts/durable-recovery-client-20261022/handoff.md).

New/materially rewritten Go journal/lifecycle owners remain below 30 cognitive
complexity. One intentional adjacent exception is `Opening.startInstalled`
47→48: the old-upload completion guard must precede mutable U0 admission at the
existing boundary. Its larger migration/admission orchestration is retained to
preserve error and transaction ordering. `connectLocked` is 25 (entry 53);
unchanged older hotspots remain separate cleanup candidates. Gocyclo and funlen
diagnostics are recorded separately; their default size thresholds are not
acceptance gates.

The narrow adjacent guards leave two pre-existing orchestration hotspots above
the cognitive-complexity target: `Bootstrap` changes from 255 to 259, and
`applySnapshotSourceReplacementInTx` from 44 to 48. These are intentional local
exceptions for C02: the additional checks fail closed at their existing locked
transaction boundary. Refactoring their full registry/bootstrap and snapshot
lifecycle flows in this slice would broaden ownership and disturb SQL/cleanup
ordering. New outcome orchestration and materially rewritten push functions are
below 30; unchanged snapshot materialization and other older hotspots remain
separate cleanup candidates. Exact diagnostic outputs are retained in the C02
ledger.
