# o0001 — Retryable Oversync Transaction Foundation

Status: implemented
Parent: [o0000 — Overqueue Distributed Queue](o0000-overqueue-distributed-queue.md)
Created: 2026-08-26
Implementation phase: Phase 1
Implementation gate: complete

## Child authority and dependencies

This child owns the breaking, bounded-retry, transaction-pure callback contract for
`oversync.SyncService.WithinSyncBundle` and `oversync.ScopeManager.ExecWrite`, including
stable operation identity, durable replay receipts, commit ambiguity, exact token handling,
the breaking replacement of the exact `server_postgres_sync_v1` managed layout, its administrative
reset boundary, and the repository-wide caller audit.

It depends only on the shared product, identity, and rollout rules in
[o0000](o0000-overqueue-distributed-queue.md). Queue storage, execution, synchronized completion,
and remote-protocol children consume this contract and must not redefine it. A contradiction
between this child and another member of the spec set is a specification defect requiring an
owner-approved revision; it is not resolved by silently choosing one file.

## Retryable Oversync transaction contract


The existing Oversync APIs keep their names but gain an explicitly retryable callback type, a
stable operation identity, and a bounded idempotency horizon:

```go
type DatabaseWriteTx interface {
    Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
    Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
    QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type RetryableDatabaseWrite func(ctx context.Context, tx DatabaseWriteTx) error

type RetryableWriteOptions struct {
    OperationID         uuid.UUID
    OperationHash       [32]byte
    OperationValidUntil time.Time
}

type RetryableBundleWriteOptions struct {
    OperationHash       [32]byte
    OperationValidUntil time.Time
}

type RegisteredEffectTable struct {
    Schema string
    Table  string
}

type ScopeWriteOptions struct {
    WriterID string
    RetryableWriteOptions
    RequiredEffectTables  []RegisteredEffectTable
    ForbiddenEffectTables []RegisteredEffectTable
}

type CommittedBundleRef struct {
    BundleSeq            int64
    SourceID             string
    SourceBundleID       int64
    RowCount             int64
    BundleHash           string
    CanonicalRequestHash string
}

type ScopeWriteResult struct {
    AutoInitialized bool
    Bundle          CommittedBundleRef
}

func (s *SyncService) WithinSyncBundle(
    ctx context.Context,
    actor Actor,
    source BundleSource,
    opts RetryableBundleWriteOptions,
    fn RetryableDatabaseWrite,
) error

func (m *ScopeManager) ExecWrite(
    ctx context.Context,
    scopeID string,
    opts ScopeWriteOptions,
    fn RetryableDatabaseWrite,
) (*ScopeWriteResult, error)
```

`ScopeWriteOptions` retains its required exact `WriterID` and embeds the required `OperationID`,
`OperationHash`, and `OperationValidUntil` fields. On first submission, the deadline must be later
than the post-lock database-time validation sample and no more than 90 days after it. Both
effect-table sets are canonicalized,
deduplicated, required to be disjoint, and validated against the frozen registered-table set before
any transaction. Its operation IDs are exact, never reused for different logical work, and remain
stable across process restarts and all retries.
`WithinSyncBundle` instead derives its internal operation UUID in literal UUIDv5 namespace
`61f1b2cd-8dd4-5a91-a937-4d06f4ac2f97` over `frame_v1` fields
`("oversync.within-sync-bundle-operation-id.v1", scope_id, source_id, source_bundle_id)`; callers
cannot supply a second identity for that API. `OperationHash` is the caller's opaque 32-byte SHA-256
digest of the complete logical database request, including every application value that can affect
callback SQL; Oversync cannot infer or recompute intent from a Go function. The receipt separately
and exactly binds scope, stable writer/source identity, source bundle identity when present, validity
deadline, and required/forbidden-effect contract. Reusing an operation ID while concealing a changed
logical request behind the same caller hash is a contract violation. An empty writer ID, zero
operation UUID, all-zero hash, invalid source bundle tuple, unknown required or forbidden table,
overlapping effect sets, sub-microsecond validity deadline, or already expired first attempt fails
before the callback.
`BundleSource.CanonicalRequestHash` is either absent/empty for an internal server write or exactly 64
lowercase hexadecimal SHA-256 characters; it is preserved in the durable bundle reference without
trimming or case normalization.

`OperationValidUntil` is the admission deadline for inserting a new operation receipt. Once a
transaction has inserted and owns that receipt after a pre-deadline post-lock sample, crossing the
deadline does not revoke the in-flight database transaction or force a late commit to roll back. If
that owner rolls back, a waiter that can insert only after the deadline returns
`OperationHistoryExpired` without invoking the callback. A committed receipt remains replayable while
retained regardless of whether its deadline has since passed.

Oversync owns a durable `sync.scope_write_receipts` authority as part of its independently validated
schema:

```sql
CREATE TABLE sync.scope_write_receipts (
    user_pk bigint NOT NULL
        REFERENCES sync.user_state(user_pk) ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    operation_hash bytea NOT NULL,
    required_effect_tables_hash bytea NOT NULL,
    forbidden_effect_tables_hash bytea NOT NULL,
    writer_id text COLLATE "C" NOT NULL,
    explicit_source_bundle_id bigint,
    operation_valid_until timestamptz NOT NULL,
    receipt_state text COLLATE "C" NOT NULL,
    committed_source_bundle_id bigint,
    committed_bundle_seq bigint,
    committed_row_count bigint,
    committed_bundle_hash bytea,
    committed_canonical_request_hash text COLLATE "C",
    auto_initialized boolean,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    completed_at timestamptz,
    receipt_expires_at timestamptz NOT NULL,
    PRIMARY KEY (user_pk, operation_id),
    CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    CHECK (writer_id <> ''),
    CHECK (writer_id ~ '^[!-~]+$'),
    CHECK (octet_length(operation_hash) = 32),
    CHECK (octet_length(required_effect_tables_hash) = 32),
    CHECK (octet_length(forbidden_effect_tables_hash) = 32),
    CHECK (explicit_source_bundle_id IS NULL OR explicit_source_bundle_id > 0),
    CHECK (isfinite(operation_valid_until) AND isfinite(receipt_expires_at)),
    CHECK (operation_valid_until >= '0001-01-01 00:00:00+00'::timestamptz
           AND receipt_expires_at < '10000-01-01 00:00:00+00'::timestamptz),
    CHECK (receipt_state IN ('in_progress','completed')),
    CHECK (
        (receipt_state = 'in_progress'
         AND committed_source_bundle_id IS NULL
         AND committed_bundle_seq IS NULL
         AND committed_row_count IS NULL
         AND committed_bundle_hash IS NULL
         AND committed_canonical_request_hash IS NULL
         AND auto_initialized IS NULL
         AND completed_at IS NULL
         AND receipt_expires_at = operation_valid_until + interval '24 hours')
        OR
        (receipt_state = 'completed'
         AND committed_source_bundle_id IS NOT NULL
         AND committed_source_bundle_id > 0
         AND committed_bundle_seq IS NOT NULL
         AND committed_bundle_seq > 0
         AND committed_row_count IS NOT NULL
         AND committed_row_count > 0
         AND committed_bundle_hash IS NOT NULL
         AND octet_length(committed_bundle_hash) = 32
         AND committed_canonical_request_hash IS NOT NULL
         AND auto_initialized IS NOT NULL
         AND completed_at IS NOT NULL
         AND receipt_expires_at =
             GREATEST(operation_valid_until, completed_at) + interval '24 hours')
    ),
    CHECK (receipt_state <> 'completed'
           OR explicit_source_bundle_id IS NULL
           OR committed_source_bundle_id = explicit_source_bundle_id),
    CHECK (committed_canonical_request_hash IS NULL
           OR committed_canonical_request_hash = ''
           OR committed_canonical_request_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX scope_write_receipts_cleanup_idx
ON sync.scope_write_receipts (receipt_expires_at, user_pk, operation_id)
WHERE receipt_state = 'completed';

CREATE TABLE sync.server_source_reservations (
    source_id text COLLATE "C" PRIMARY KEY,
    ever_used boolean NOT NULL DEFAULT false,
    CHECK (octet_length(source_id) BETWEEN 1 AND 256),
    CHECK (source_id ~ '^[!-~]+$')
);
```

`WithinSyncBundle` sets `explicit_source_bundle_id` and completes `auto_initialized=false`;
`ScopeManager.ExecWrite` leaves the explicit source field null, allocates the next writer bundle ID
under the existing scope/source locks, and stores whether that committed transaction initialized the
scope. A completed receipt stores the complete `CommittedBundleRef` and can return the exact
`ScopeWriteResult` without depending on prunable `sync.bundle_log` or `sync.bundle_rows`. `SourceID`
is the exact retained `writer_id`, and `BundleHash` is the lowercase hexadecimal rendering of
`committed_bundle_hash`. A deferred
constraint-trigger finalizer rereads the authoritative receipt by `(user_pk, operation_id)` and
prevents it from committing in `in_progress`; it does not reject based on the insert event's stale
`NEW.receipt_state` after the same transaction has updated the row to `completed`. Receipt cleanup
cannot remove it before the later of `OperationValidUntil` or `completed_at`, plus the fixed 24-hour
receipt-retention margin.

A newly installed server-source reservation starts with `ever_used=false`. When
`ScopeManager.ExecWrite` uses a reserved `WriterID`, it locks that reservation row and changes
`ever_used` to true in the same transaction that owns the Scope receipt, registered effects, and
bundle. A rollback leaves an unused reservation false. Managed writes permit only the false-to-true
transition. Scope deletion and source-state or receipt cleanup do not change this global marker.
Offline reservation removal requires `ever_used=false`, no matching source-state or Scope receipt,
and no Queue catalog or configuration reference. It rejects a true marker even after cascading scope
deletion has removed the per-scope evidence. Writer rotation retains every true reservation in each
Sync instance's exact configured set.

Before a new `WithinSyncBundle` callback, the transaction also locks existing source state. If the
explicit source bundle is already at or below the committed source watermark but its operation
receipt is absent, the API returns `OperationHistoryUnavailable` without the callback; this covers
operations committed under the v1 database layout and source history whose receipt was pruned.
`OperationHistoryExpired` remains the deadline-based result for an absent new-operation receipt. Only
the exact next source bundle may create a receipt and invoke the callback. A future gap retains the
existing source-order error.

This addition retains managed Sync layout name `server_postgres_sync_v1` and sync wire protocol label
`v1`, but replaces the exact managed-layout facts associated with that name. It provides no in-place
upgrade or adoption path from the pre-o0001 layout. New code rejects a pre-o0001
`server_postgres_sync_v1` manifest, and old code rejects the revised manifest even though both use the
same layout name. Upgrade requires an administrative reset: stop all Sync/Queue processes and
producers, take a backup, recreate the Sync layout under the new binary, reset every client Sync
database or retained client state that can refer to the discarded server history, and resume only
after fresh bootstrap and readiness succeed. Fresh bootstrap creates and self-validates the receipt,
reservation, identity-collation, managed owner/capture-function, and registered-relation facts in one
transaction. It publishes `sync.meta.layout_name='server_postgres_sync_v1'` only as part of that
successful transaction. Any failure rolls back fresh creation. Rollback restores the backup and old
binary; no deployment may mix pre-o0001 and revised-v1 server or client state.

Each transaction attempts to insert the in-progress operation receipt before invoking the callback.
A managed insert path validates the disjoint registered sets and stores SHA-256 of the JCS array
`["oversync.effect-table-set.v1", sorted_schema_table_pairs]` for each set; arbitrary caller hashes
cannot become receipt authority.
A concurrent attempt with the same identity blocks on that unique row until the first transaction
commits or rolls back. If a committed receipt exists, exact content returns its stored result without
the callback; changed operation hash, writer/source identity, validity deadline, or canonical
required/forbidden table-set hash fails. If the first transaction rolled back, the waiting attempt may
insert the receipt and run. After the callback and before bundle finalization, the successful attempt
rejects any raw captured event in the forbidden set, including an update or insert-delete sequence
that normalization would erase. It then finalizes the sync bundle. If the required table list is
non-empty, it requires at least one normalized `sync.bundle_rows` effect in that exact set; raw
trigger capture and an effect later normalized away do not count. Only after those validations does
it mark the receipt complete in the same transaction.

Both APIs use one Sync lock order: per-scope advisory/session serialization, ensure and read
`user_pk`, insert or lock the operation receipt, then lock scope/source state. No path locks source
state and later requests an operation receipt.

An exact completed-receipt replay exits before capture setup and the callback, returns the stored
`ScopeWriteResult`, and is exempt from the new-write "at least one registered effect" check. It does
not create a no-change bundle. A genuinely new operation still must capture at least one registered
effect.

`RetryableDatabaseWrite` is an explicit transaction-pure contract. Its function may be invoked more
than once only after PostgreSQL proves the prior transaction rolled back. V1 automatic retries are
limited to SQLSTATE `40001` serialization failure, `40P01` deadlock, and `55P03`
lock-not-available, including configured lock timeout, when the driver confirms rollback. The
callback may use only the supplied transaction and
deterministic in-memory computation. It must not mutate process-global state or cause network,
filesystem, message, email, object-storage, or other external effects. It also must not issue
transaction control, change isolation, change role or search path, or force managed constraints
with `SET CONSTRAINTS`. It must not change `session_replication_role` or another setting that alters
managed trigger/constraint execution. Oversync alone controls bundle finalization and deferred
projection timing. These SQL restrictions are trusted-caller obligations: Oversync does not parse
callback SQL or sandbox invoked database functions. Violating them can end or alter the managed
transaction, bypass capture, or repeat a non-transactional effect, and invalidates the retry and
atomicity guarantees.

Both retryable APIs mark their callback context and transaction. A callback may not call an API that
starts or owns another transaction, recursively enter either retryable API, or use another pool or
connection; matching Sync and Queue entry points reject the callback-context marker. Generic Scope
writes may call only database helpers such as `EnqueueTx` that use the supplied `DatabaseWriteTx`.
When the callback marker is present, `EnqueueTx` also requires the matching transaction-local marker
and exact active scope on that capability, rejecting an unrelated transaction or a cross-scope
request.
The callback must use its supplied context rather than a captured outer or replacement context for
database calls and nested helpers. Completion adds the stricter guard in o0003 section 12.4 and rejects
`EnqueueTx` as well.

Database defaults and transactional database functions may produce different values on a definitely
rolled-back attempt when every business effect remains inside that transaction and the callback
retains none of it in process state. PostgreSQL sequence allocation is the one permitted DB-local
non-rollback artifact: retries may leave gaps, no contract may require gapless or attempt-stable
sequence values, and only the committed row value becomes business state. This allowance does not
permit a database function with network, filesystem, messaging, or another non-transactional side
effect.

The v1 automatic transaction policy is fixed at three total attempts. Before attempts two and
three it waits respectively 25ms and 50ms, with no jitter; context cancellation interrupts either
wait and prevents another callback. This exact policy applies to the two retryable write APIs and to
each service-owned mutation that this specification says automatically retries: `Enqueue`,
`CreatePushSession`, `UploadPushChunk`, `CommitPushSession`, `Fail`, `Cancel`, `ManualRetry`, and
`AcknowledgeCancel`. `EnqueueTx` and a caller-owned transaction never retry or commit themselves.
Claim commit ambiguity and maintenance's later-tick retry retain their separate rules. A non-listed
error, an error for which rollback is not proven, or exhaustion returns immediately under the error
rules below. Changing these attempt or delay values is a future compatibility revision, not a
per-call option.

The callback receives an Oversync wrapper implementing only `DatabaseWriteTx`; it has no `Commit`,
`Rollback`, `Begin`, raw `Conn`, `CopyFrom`, `SendBatch`, or prepare escape hatch. `Query` retains its
`pgx.Rows` return type. The retryable wrapper returns an adapter rather than the driver's concrete
rows: it delegates the ordinary `pgx.Rows` methods, and `Conn()` returns nil without panicking. This
keeps pgx row helpers usable without exposing the underlying connection through the supplied
capability. A concrete `pgx.Tx` also implements `DatabaseWriteTx` for caller-owned `EnqueueTx` use,
but the retryable APIs do not pass that concrete value through. The narrower interface does not make
arbitrary SQL or external behavior safe by itself, and the wrapper deliberately does not inspect SQL
text. Using a captured pool or another connection violates the transaction-pure callback contract
and remains subject to caller audit. Managed receipt/effect invariants fail closed where Oversync
owns the operation, and the audit must inspect SQL transaction/session control and invoked database
functions and procedures as well as Go statements.

A `COMMIT` transport error, connection loss during commit, or any driver result that cannot prove
rollback returns `CommitOutcomeUnknownError` immediately and is never automatically retried. The
caller resolves it by repeating the public operation with the same operation ID, hash, and validity
deadline. Receipt uniqueness waits for an older backend still finishing: a committed first attempt
returns its result, while a rolled-back attempt permits a new callback. If no receipt is present and
database time has passed `OperationValidUntil`, the API returns `OperationHistoryExpired` without
invoking the callback. This is bounded idempotency, not an exactly-once claim.

For Sync protocol v1 push-session create, chunk, and commit operations, the HTTP boundary maps
`CommitOutcomeUnknownError` to retryable `503 commit_outcome_unknown`. `oversqlite.Client` retains
the frozen source bundle ID and canonical request hash and retries from push-session creation. It
does not retry only the ambiguous request or infer rollback from missing staging state; an
`already_committed` create response completes replay through the authoritative committed bundle.

This is an intentional breaking semantic change to both existing methods. Implementation must
replace `TestWithinSyncBundle_DoesNotRetryCallbackOnRetryableTransactionError` with proof that a
retryable database-only callback reruns and commits exactly one transaction. Every repository call
site, example, and public document must be audited for transaction purity and a stable logical
operation identity. Every callback must adopt the new callback-scoped `context.Context` argument and
use it for all database calls. `ScopeManager.ExecWrite` also stops trimming `scopeID` and `WriterID`:
scope identity remains exact, while writer identity must pass the existing exact visible-ASCII
SourceID grammar. Callers that relied on normalization must fail validation or migrate explicitly.
`ScopeWriteResult.Bundle` becomes the durable `CommittedBundleRef`; it no longer returns
`Bundle.Rows`. First execution and receipt replay therefore have the same result even after normal
bundle pruning. A caller needing row payloads uses ordinary pull/history APIs while that history is
retained. This result-shape change and every caller that reads `Bundle.Rows` are part of the required
breaking-change audit. `WithinSyncBundle` binds its receipt to the caller-stable source ID,
source-bundle ID, and canonical request hash. The retry count/backoff policy is bounded,
context-cancellable, and documented; definite rollback exhaustion returns the last typed PostgreSQL
error, while ambiguous commit uses the distinct error above.

The Go contracts change in place; this proposal introduces no `ScopeWriteResultV2`, v2 Go or wire
API, or v2 PostgreSQL layout name. The revised exact `server_postgres_sync_v1` manifest contains the
new receipt and reservation relations. Reusing the layout name does not grant compatibility with the
pre-o0001 manifest.


## Child acceptance gate

This child is complete only when Phase 1 in o0000 and the following evidence are accepted:

- revised `server_postgres_sync_v1` fresh creation is exact-layout validated and atomic; new binaries
  reject the pre-o0001 manifest, old binaries reject the revised manifest, and reset, backup, recovery,
  and client-state invalidation instructions document the absence of an in-place migration;
- retry tests cover every permitted SQLSTATE, all three attempts, both fixed backoffs, context
  cancellation, definite rollback exhaustion, concurrent receipt ownership, and callback purity;
- ambiguous commit is never retried automatically and exact replay resolves committed, rolled-back,
  expired, and unavailable-history outcomes without fabricating rollback;
- retained receipts reproduce the durable `CommittedBundleRef` after bundle row/log pruning;
- reservation tests prove false-to-true `ever_used` publication in the same committed Scope
  transaction, rollback preservation of false, survival across scope deletion and receipt cleanup,
  and rejection of removal after first use;
- public API comments, documentation, and caller audits state the trusted transaction-pure SQL
  restrictions without claiming that Oversync parses or sandboxes callback SQL; the supplied
  transaction omits connection-owning methods, each returned `pgx.Rows` adapter reports `Conn()==nil`
  without a panic, and callback-context guards reject recursive transaction-owning APIs, mismatched
  transaction capabilities, and cross-scope enqueue;
- every repository and example caller is audited for stable identity, callback-scoped context,
  external effects, database procedures with non-transactional effects, token normalization,
  normalization-dependent behavior, and reads of the removed `Bundle.Rows` result shape;
- public documentation and release notes call this an intentional in-place breaking Go API and
  behavior change; no `ScopeWriteResultV2`, Go API v2, or sync wire-protocol v2 is introduced.


## Server-write reliability amendment (2026-10-22)

The owner authorizes a paired correction for final-state capture, uniqueness-safe local application, and reserved-writer concurrency. Wire shape, canonical first-touch ordering, operation identities and receipt guarantees remain unchanged. Deployments use fresh server and client databases; no migration or historical repair is provided.

Raw PostgreSQL capture is a touched-identity set and forbidden-effect witness, not authority for the final after-image. After deferred effects finish, finalization reads actual final rows in the same transaction and uses prior authoritative row_state to classify insert/update/delete or absent-to-absent. Preliminary effect detection and finalization use identical rules and existing exact physical/wire codecs and limits.

Both clients durably stage desired full local images and metadata decisions, then schedule plain-column uniqueness dependencies using SQLite affinity/collation/NULL semantics. Acyclic transfers release a value before claiming it. Cycles stage and preflight their complete dependent closure before temporary vacating and restoration; application-owned triggers or unknown dependencies that make temporary deletion unsafe block the entire apply. Unsupported partial/expression uniqueness and conflicting final desired values fail explicitly. No REPLACE, disabled constraints, invented business values, unbounded host graph, or canonical history rewrite is permitted. Preparation may yield with guarded staging only; complete business application and metadata/checkpoints commit atomically after final-state, protected-intent and FK verification. All extra persistent/host/statement/graph work is charged under the existing limits; no new numeric defaults. The shared apply owner covers pull, settlement/replay, conflict and snapshots.

Go exposes FinalStateApplyBlockedError and Kotlin FinalStateApplyBlockedException with bounded reasons conflicting_desired_state, unsafe_trigger_or_dependency and unsupported_uniqueness. They are local failures; they do not auto-resolve conflicts or advance synchronization state. Matching fixtures cover values/existence/versions, local values, dirty/outbox protection, failure/cancellation/reopen, closure and capacity limits, and runtime parity.

Established reserved writers hold compatible FOR KEY SHARE reservation locks; conditional false-to-true first-use publication remains in the business/history/receipt transaction. Concurrent first use may wait; established different scopes must overlap. Removal, permanent use tombstones, missing reservations and rollback remain fail-closed.

The correction qualification profile requires paired fast-contract checks, Go owning and focused race/live suites, Kotlin common metadata plus affected JVM/platform/live and Swift cancellation leaves, and measured ordinary-path SQL/workspace behavior. Follow the paired validation runbook and retain failed attempts; no missing/skipped/cached lane is a fresh pass. Delivery and actual database recreation remain separate gates.
