# Durable final-state recovery for Oversqlite — r0001

Status: C00–C05 are owner-accepted, committed paired prerequisites. C06 was explicitly owner-accepted and its sealed scoped packet remains historical. C07 implementation, all twelve mandatory initial-protocol qualification gates, independent review and evidence closure are complete under the exact owner-approved C07 execution amendments. See the [current C07 handoff](../.artifacts/durable-recovery-final-20261024/handoff.md) for current source, commands, actual-body/skip/worker limits, retained failures and cleanup. C07 owner acceptance remains pending. Changes remain unstaged and uncommitted; staging, commit, push, publication, deployment and real-data reset remain unauthorized. Qualification does not imply release or deployment approval.

## 1. Authority, scope and decisions

This document governs the new recovery work in Go Oversync, Go Oversqlite and Kotlin Oversqlite. The [Kotlin companion](../../sqlitenow-kmp/specs/oversqlite-durable-recovery.md), [active OpenAPI](../swagger/two_way_sync.yaml), [checklist](r0001-oversqlite-durable-recovery-checklist.md), [validation contract](r0001-oversqlite-durable-recovery-validation.md), and [fresh-session handoff](r0001-oversqlite-durable-recovery-handoff.md) form one candidate. C01 activates `swagger/two_way_sync.yaml` as the single wire authority. Current runtime qualification is recorded in the C07 packet; contract activation alone is not runtime qualification.

For this work, r0001 supersedes the old lifecycle/envelope rules in p0000, the wire-preserving recovery proposal, p0001's retry-only blocker boundary, and prepared-source retargeting where it conflicts with immutable unresolved v1 operations. It does not silently implement p0001 Stage B targeted reconciliation, general group policy callbacks or reports. Existing generation, scope-deletion fencing, canonical row codecs, FK preservation, server-write receipts, reservation concurrency and resource contracts apply unless expressly changed below. Server-write receipts from o0001 remain separate from the new client-upload outcomes.

The first delivery has three sequential milestones: **A — exact upload outcomes and crash-safe local settlement/acknowledgment; B — safe snapshot installation; C — automatic snapshot recovery only from a clean local state.** Close each milestone's scoped review and evidence before starting the next. Full protocol qualification remains a separate final gate.

The product result is: ordinary Sync resolves uncertain uploads without inventing outcomes, preserves existing later-edit behavior, and can move a clean client past an unusable historical state to a usable authoritative baseline. Dirty state or an unsettled committed result remains a durable blocker when ordinary settlement cannot safely handle it. This first delivery does not promise recovery of every currently blocked client.

Ordinary explicit `Rebuild` and automatic recovery share the strict clean-state admission in section 6.2 and the corrected delta installer. Sync invokes the shared guarded workflow; it does not catch arbitrary errors and invoke Rebuild. Explicit source replacement requires no dirty intent, frozen operation, ack obligation or active recovery on the client, and no unacknowledged outcome on the server.

Decisions fixed by this candidate:

| ID | Decision |
| --- | --- |
| R01 | Sole wire revision `v1`; mandatory protocol header; no negotiation/fallback/legacy support; fresh local/server formats. |
| R02 | One outstanding terminal upload outcome per source; exact outcomes survive history pruning and have no age-based expiry before acknowledgment. |
| R03 | Resolve-or-cancel serializes with upload: it returns the exact outcome or durably fences a never-committed operation. Absence alone is never returned as proof. |
| R04 | Committed, rejected and cancelled operations consume their source sequence. An explicit acknowledgment follows durable local disposition before the next operation. |
| R05 | Recovery snapshots bind a minimum sequence and a sealed streaming hash. They cannot settle an outstanding upload. Installation is a guarded delta, never clear-and-reinsert. |
| R06 | Preserve existing ordinary settlement and newer-edit guards. Snapshot recovery requires no dirty rows, frozen upload, unsettled outcome or ack obligation. No new merge or decision mechanism. |
| R07 | Default mode `automatic_clean` may resynchronize clean state; mode `disabled` prevents automatic checkpoint jumps. Snapshots cannot recreate missing history events. |
| R08 | No trigger suppression/rewriting, general expression-index parser, target-value search, or arbitrary policy callback framework in this scope. Conservative blockers remain explicit capabilities. |
| R09 | One durable recovery owner, aggregate resource accounting, bounded network/replan work, and no unchanged-state redownload loop. |
| R10 | All protocol/persistence changes require paired corpus, actual Go/Kotlin runtime/live evidence, interruption tests and the initial-protocol qualification profile. |

Implementation may choose internal types, SQL table layout and cohesive helpers. It must not change these decisions, public semantics, hash domains or limits without first amending this candidate and its checklist/OpenAPI. Do not delegate unresolved correctness decisions to a smaller implementation model.

Owner amendment during C00/C01 (2026-10-22): retain the sole literal `v1` rather than introduce a `v2` label. The new mandatory protocol header, terminal outcome/ack lifecycle, fresh formats and new hash domains still apply. Old envelopes and persisted formats are rejected; `v1` is not a compatibility promise or a negotiation mechanism. The new domains are `oversqlite.push-outcome.v1` and `oversqlite.snapshot.v1`; existing request/bundle/logical-row domains remain unchanged.

### 1.1 Complexity and ordinary-path cost

The goal is a simpler correctness model, not fewer endpoints or a claim of smaller implementation. This design adds resolve and acknowledgment, a durable outcome union and minimal guarded recovery state. It adds no prepaid storage slots, decision pages or recovery reports. A normal settled upload adds one ack request. Those costs must be measured and reported.

The implementation must replace the current need to recover an upload result from prunable history, invocation-only confirmed-result bookkeeping as recovery authority, rejection paths that reuse an old operation identity, and snapshot clear-all installation. All final-state mutations still use one application engine; all same-scope recovery uses one durable owner. Do not retain old paths as fallback implementations. Explicit clean-source replacement, normal row-conflict policy, generation management and server-write receipts remain distinct features with their real contracts; this change does not claim to remove them.

Before C07, record added/removed public concepts, source owners, request counts and retained state, and explain any parallel lifecycle that remains. Reduced ambiguity is a design objective; reduced code size or latency requires measurements and must not be asserted in advance.

The owner chose this narrower contract to reduce overlapping states and paths while retaining useful mobile-app schemas. Many schema restrictions already exist; documenting them alone is not evidence of simpler code. Reliability means preserving data, reaching one durable disposition and exposing an actionable blocker when proof is unavailable; it does not mean automatically completing every recovery.

The C07 simplification review must map each superseded mechanism named above to its removed or reused implementation owner and the tests covering its replacement. Record added/removed public concepts and durable phases, remaining ownership boundaries, materially changed function complexity and measured ordinary request/storage costs. Shared final-state application must remain shared; no dirty-merge, trigger interpreter or permissive fallback may be introduced to make a blocked case pass. A lower line count is not an acceptance target, and extra lines needed for explicit outcome/ack correctness are not by themselves a failure. Any claim of improved reliability or performance must name the evidence and its limits.

### 1.2 Deliberately limited SQLite support

Support a documented subset of SQLite that the SDK can inspect and prove safe. This is not a promise to synchronize arbitrary SQLite schemas, triggers or DDL. Validate restrictions at registration/open/upgrade where possible, and revalidate connection-local or changing facts under the final writer. Diagnostics must name the unsupported object and supported remedy; valid but unsupported application schema is not corruption.

Apply restrictions to registered tables and dependencies that recovery can affect. Unrelated local tables remain outside this contract. Retain the current supported ordinary write/capture contract; this revision does not introduce a mandatory SDK-only write API. Sections 2 and 7 fix the actual restrictions for this delivery. Broader prohibitions on application triggers during ordinary sync, new type/key restrictions, or removal of existing relationship support require a separately reviewed contract change. They are not implied by this design principle.

### 1.2.1 Concrete support boundary for the first delivery

This table summarizes retained admission rules and the new recovery restrictions. It does not expand the existing codecs, relationship actions or platform capabilities. Supported operations still require the declared schema/generation, preservation proof and existing resource bounds; C00 reviews this table against both SDK admission owners.

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

References to local-only columns are conditional preservation obligations, not a new managed-column projection feature. Current Go/Kotlin admission requires the exact managed physical/generation column set. This delivery does not relax it. Unrelated local tables and admitted dependent-row preservation are separate from managed-column admission.

The implementation must publish this boundary in application-facing documentation during C05. Early admission checks must distinguish database-wide unsupported schema from a recovery-only limitation. In particular, do not reject an ordinary supported trigger application merely to avoid implementing a precise recovery blocker. Additional simplifications, such as an ordinary-sync trigger ban or requiring all application writes through the SDK, need a separate explicit product decision.

Product suitability is not implied by protocol qualification. Before owner acceptance, assess representative target-app schemas and ordinary offline workflows using authorized source/schema information or synthetic equivalents. Specifically review trigger-based audit/search maintenance, case-insensitive uniqueness, join-table identity, and a recovery blocker with pending user edits. Record application changes or remaining blockers; do not claim drop-in compatibility with arbitrary existing mobile databases. This assessment requires no real user data or deployment.

### 1.2.2 Synthetic activity reference schema and required preservation cases

The maintained [synthetic SQLite activity schema](../oversqlite/testdata/activity-recovery.sql), [PostgreSQL representation](../oversqlite/testdata/activity-postgresql.sql) and [generation model](../oversqlite/testdata/activity.go) bind the representative recovery fixture. It contains 10 source schema shapes, 14 sync-enabled tables and one unrelated local table (`app_settings`). DDL and in-memory SQLite metadata inspection found single-column text keys, `WITHOUT ROWID`, ordinary TEXT/INTEGER/REAL storage, single-column registered FKs, plain-column uniqueness and CHECK constraints; no application triggers, generated/hidden columns, partial/expression unique indexes or local-only incoming FK dependencies were found in those shapes. This establishes structural fit only, not SDK/platform qualification, real-data behavior or universal recovery.

Keep support for these existing schema shapes. Do not simplify the engine by banning the activity schema’s cascades, self-references, text natural keys, `WITHOUT ROWID`, CHECK expressions or multi-column uniqueness. Composite uniqueness is not a composite synchronization key. SQL expressions in CHECK constraints are not expression-based unique indexes. No application schema rewrite is selected by this specification.

| Case | Representative shape | Required assertion |
| --- | --- | --- |
| DT01 — schema admission and values | All 14 synchronized tables, `profile.name`/`value_registry.key`, TEXT keys, `WITHOUT ROWID`, INTEGER booleans, REAL numeric values, dates/JSON encoded as declared text, and CHECK rules. | Both SDKs admit the equivalent supported schema and round-trip valid declared values without changing key, NULL, number or text semantics. Runtime annotations do not replace verification of the wire/server mapping. |
| DT02 — local-only state | Unsynchronized integer-key `app_settings`, with no FKs. | Explicit and automatic clean snapshot installation leave its full values and identity unchanged; sync-key restrictions do not leak into unrelated local tables. |
| DT03 — unique transfers | Three nullable `activity.sched_write_ref_*` unique indexes and `UNIQUE(program_item_id, input_key)` / `UNIQUE(goal_id, revision)`. | Valid value transfers/swaps and multiple NULLs retain SQLite semantics. A duplicate final desired value blocks atomically; no invented temporary business value or dropped constraint. |
| DT04 — dependent closure | `activity` → `program_item` → input goal → goal revision, plus activity/program-item state and bundle-state CASCADE relationships. | Surviving descendants retain complete values/metadata during any temporary vacating. Genuine authoritative deletion follows admitted relationships and cannot leave orphans; a remaining NO ACTION reference cannot be bypassed. |
| DT05 — self-reference | `daily_log.parent_daily_log_id`, plus its activity/program-item references. | Apply valid parent/child changes and preserve the complete surviving graph; do not reject the schema merely because it refers to itself. |
| DT06 — CHECK and failed application | Conditional schedule checks, enum/boolean checks and date-shape checks using `length`/`substr`. | Valid full rows apply under enforced constraints. An invalid target or failed physical transition preserves the complete old state; recovery does not rewrite business values to force success. |
| DT07 — pending offline work | A dirty daily log/activity, an immutable pending upload, a validated but unsafe committed outcome, or ack_pending. | Ordinary supported sync remains available under its own contract. Each unmet clean-state condition prevents snapshot recovery before its network request; preserve edits/evidence and expose the blocker. A new edit during preparation survives and prevents installation. |

Use synthetic data and maintained representative fixtures in the authorized Go/Kotlin repositories; CI must not depend on an external application checkout or live application database. Record schema-source provenance when deriving fixtures, including any necessary server representation mapping. Verify the pinned reference before derivation; on drift, reassess/update this evidence deliberately rather than treating an old fit assessment as current. No external application runtime/source edit or full-app rollout is authorized by these cases.

DT01–DT06 are implemented in C04 and receive its scoped V04/narrow V08 proof; affected platform/live qualification follows in C06 through V07/V08. DT07 uses C03 outcome evidence and closes through C05/C06 coordinator/live paths. The validation contract maps these cases to planned test owners. The reference cases retain the existing limits and deadlines and do not replace the general trigger/index/unknown-dependency refusal tests. Required positive cases must execute successfully within those limits; a blanket capability blocker is not a passing result.

### 1.3 Evidence and exclusions

The [investigation](../.artifacts/final-state-recovery-investigation-20261022/proposal.md) verified current Go `de557dc123854c590451eb1f4c00501532c0b0c9` and Kotlin `e28d96c2ad2bf33c9fabe2dfae26445bd5e3709a`. Scratch tests demonstrated false DELETE audit events and loss of declared local-only CASCADE children during successful rebuild; a valid committed result conflicting with a later local edit; conservative index/trigger/dependency blockers; and SQLite transition counterexamples. Kotlin's new counterexamples were source-inspected, not freshly executed. Prior qualification is incomplete.

No migration, dual reader/writer, legacy persisted-state repair, pre-fix stale/ghost history repair, automatic source replacement, arbitrary row winner, REPLACE, invented temporary business value, disabled FK/unique enforcement, trigger removal, manual `_sync_*` edits, checkpoint skipping or database deletion is a recovery mechanism. Fresh-format authorization does not authorize resetting a real database. A runtime opening an old persisted format returns an explicit incompatible-format error and leaves it intact.

## 2. Invariants and supported boundary

1. **Identity:** bind every operation to authenticated scope, server authority, source ID, generation ID/fingerprint, positive source sequence, canonical request hash and exact initialization-token presence/value. Never trim, normalize or retarget these tokens.
2. **Evidence:** only a validated terminal outcome proves commit or noncommit. Transport errors, HTTP errors outside that envelope, cancellation, a snapshot match or a sequence hint are insufficient.
3. **Preservation:** preserve original bases, full local values, frozen request bytes, newer edits, clean dependent rows and local-only data until an atomic, guarded disposition covers them.
4. **Atomicity:** server business effects/history/outcome/source advancement commit together; client upload settlement commits business state/baselines/dirty-outbox disposition and its ack obligation together; clean snapshot installation separately commits business state/baselines/checkpoint and recovery completion together. A snapshot never acknowledges an upload. Auxiliary staging may commit earlier under guards.
5. **No hidden effects:** validate the physical transition, not only the final managed rows. A clean FK check does not prove that a cascaded local-only child survived.
6. **Boundedness:** admit every simultaneous owner and the complete atomic application before its first business mutation. Work slices yield preparation; they do not split the final atomic transition.
7. **Parity:** Go and Kotlin expose equivalent outcomes, clean-state admission, errors and cancellation ownership. Swift uses the same Kotlin owner and cancellation joins it.

Automatic baseline recovery requires the strict clean-state gate in section 6.2 and supports a complete same-scope snapshot and a constraint-safe delta with ordinary/composite unique keys, supported codecs/collations, complete relationship coverage, and preserved local values. It must not download a snapshot to fix a static unsupported index or trigger capability.

For the first implementation, an application-owned main/TEMP trigger on any table whose rows the proposed recovery would mutate is a recovery capability blocker. Unchanged tables do not block merely by existing. Ordinary replay retains its established supported boundary; cycle housekeeping remains blocked when such triggers are involved. Known SDK trigger ownership requires matching definitions, not matching names. This conservative rule deliberately leaves legitimate audit-trigger applications needing an application design change or a future explicit trigger contract. No internal apply-mode flag is exposed as a workaround.

Partial/expression indexes keep a precise capability blocker; unsupported schemas must be diagnosed at open/upgrade inspection where possible, without calling valid application schema corrupt. Improving this capability is a separate future scope. Empty incoming dependency sets and provably safe deferred NO ACTION references are included in this work, subject to final writer-owned membership checks. CASCADE/SET NULL/SET DEFAULT/RESTRICT effects require a complete preserved closure or an explicit block. An application-visible rowid is a preservation obligation too.

## 3. Wire revision and canonical identities

All `/sync/*` requests require `Oversync-Protocol-Version: v1`, including discovery. Missing, duplicate, malformed or another value returns HTTP 426 `protocol_version_mismatch` before any mutation. Authentication precedes protocol/header diagnostics; existing source/generation authorization and body-boundary validation follow. `/syncx/health` and `/syncx/status` stay operational endpoints and are not recovery evidence. Discovery says exactly `protocol_version: "v1"`; no version negotiation, feature flags or legacy branches.

Existing source/generation headers, compact row tuples, request-row canonicalization, generation-pinned `canonical_request_hash` and committed `bundle_hash` algorithms remain unchanged. This limits the breaking change to lifecycle/envelopes rather than unnecessarily rewriting row identity. Freeze the exact encoded request body and binding headers locally; retries use the same request. Encoding/compression must not alter logical identities. An explicit source sequence increment after rejected/cancelled disposition is a new operation, never a rewrite of the old one.

Discovery adds `limits.max_push_outcome_decoded_bytes`. It bounds every complete push/resolve terminal envelope, including rejection details. This supersedes p0000 using `max_push_decoded_bytes` for successful push responses; that existing field now bounds push requests, while the new field bounds terminal responses. Existing decoded/encoded request limits, compact-codec rules, strict duplicate-field rejection, unknown-field rejection for closed objects, number ranges, UUID/token grammar and gzip rules apply to new endpoints. All int64 fields must use the existing exact-integer codec on JS/Wasm as well as Go/JVM/native; JSON numbers must not silently round.

Resolve and acknowledgment request bodies have a fixed 4,096-byte encoded and decoded ceiling. Header limits retain their existing separate bounds. Outcomes use the discovered complete-outcome ceiling, not this small request ceiling.

### 3.1 Terminal outcome envelope

`POST /sync/push` keeps its request shape and returns HTTP 200 with exactly one terminal envelope:

```json
{
  "source_id": "device-source",
  "schema_generation_id": "1",
  "schema_generation_fingerprint": "<64 lowercase hex>",
  "source_bundle_id": 7,
  "canonical_request_hash": "<64 lowercase hex>",
  "initialization_id": null,
  "status": "committed",
  "outcome_hash": "<64 lowercase hex>",
  "committed": {
    "bundle_seq": 42,
    "bundle_hash": "<64 lowercase hex>",
    "row_count": 2,
    "rows": "<existing compact groups>"
  }
}
```

This is explanatory notation; executable valid examples live in the active OpenAPI/corpus. `initialization_id` is always present, null or the exact canonical token. The union is closed:

- `committed`: required `committed`; no `rejection`; full authoritative effect, including server-created/changed/deleted rows, validated coverage and hash. No pagination or history lookup is needed to read it.
- `rejected`: required `rejection`; no `committed`. `rejection.code` is `push_conflict` or a bounded deployment-declared application rejection category; `conflict` is required only for `push_conflict` and has the existing `PushConflictDetails` schema. No partial business effect committed.
- `cancelled`: neither `committed` nor `rejection`. Resolve-or-cancel won the race and permanently prevented this operation from committing.

Only validated, operation-bound conflict or declared application rejection becomes `rejected`. Malformed input, auth/generation/initialization admission failures, capacity errors, unexpected SQL errors and transient infrastructure errors use the ordinary non-200 error envelope and do not claim a terminal outcome. The client resolves uncertainty before replacing such a frozen request. A transport-level HTTP 200 is not accepted until identity, union, counts, hashes and complete effect coverage validate.

The hash is SHA-256 over UTF-8 JCS of this logical array:

`["oversqlite.push-outcome.v1", source_id, generation_id, generation_fingerprint, source_bundle_id, canonical_request_hash, initialization_id_or_null, status, committed_or_null, rejection_or_null]`.

For the hash only, `committed_or_null` is `[bundle_seq, bundle_hash, row_count, logical_rows]`. Each logical row is `[schema, table, key_object, op, row_version, full_payload_or_null]` in the authoritative bundle's exact canonical order; `op` is uppercase `I`, `U`, or `D`, with null payload for D. Resolve compact groups using the pinned discovery before hashing. `rejection_or_null` is `[code, conflict_or_null]`; `conflict_or_null` is the existing complete conflict object with lowercase wire `op`. No human-readable message, compact grouping choice, compression or timestamp participates. Null is explicit.

For both new v1 hash domains, project every protocol integral position to its exact base-10 string before JCS: no plus sign, whitespace or leading zero except `"0"`. This includes source_bundle_id, bundle_seq, row_count, table_index, row_version, snapshot boundary T, logical_byte_count, recovery.minimum_bundle_seq and the base_row_version/server_row_version members inside a rejection conflict. Int64 values never pass through binary64. Keys and full payloads retain the existing typed codec/JCS representations; do not stringify arbitrary payload values. Wire integers remain JSON numbers. Existing request/bundle hashes and existing snapshot logical-byte accounting are unchanged. Add cross-language golden vectors including adjacent values above 2^53 and int64 boundaries before implementing consumers.

### 3.2 Resolve-or-cancel

`POST /sync/push-outcomes/resolve` takes exactly `source_bundle_id`, `canonical_request_hash`, and required nullable `initialization_id`; source/generation identity comes from the required headers. It is a mutation, despite usually reading a retained result.

Under the same source-operation serialization as push:

1. If this exact unacknowledged terminal outcome exists, return it unchanged.
2. If this is the next admitted sequence and no terminal outcome exists, persist `cancelled`, consume the sequence, and return that outcome. A delayed push for this tuple now returns cancelled and cannot run business DML.
3. If a record at that sequence has a different hash/token/binding, return 409 `push_outcome_mismatch`; never reveal or overwrite another operation's result.
4. A future sequence returns 409 `source_sequence_out_of_order`. A later sequence while an earlier terminal result is unacknowledged returns 409 `push_outcome_pending`.
5. An already acknowledged sequence returns 410 `push_outcome_acknowledged`; the client must already have its local disposition. It cannot reinterpret this as noncommit or resend under a new tuple. Missing local evidence is an explicit lifecycle/integrity blocker.

There is no `not_found`/`not_committed` observation that can race with a later commit. The source lock is the linearization boundary. If upload owns it first, resolve waits and then reads its committed terminal result, or cancels after its rollback. If resolve owns it first, upload observes the cancellation. Lock waits are cancellable and bounded by existing server request/transaction deadlines; a timeout gives no terminal proof.

Resolve does not require the initialization lease to remain valid: it must be able to resolve an upload whose lease expired after dispatch. It does require the original authorized scope/source/generation binding. It cannot initialize a new source, alter generation, replace a source, or cross scope ownership.

### 3.3 Acknowledgment and sequencing

`POST /sync/push-outcomes/ack` takes exactly `source_bundle_id`, `canonical_request_hash`, and `outcome_hash`, using the same source/generation headers. HTTP 204 means the acknowledgment is durable. It is sent only after the client's local disposition transaction commits.

The server persists `last_acknowledged_sequence` and the last acknowledged request/outcome hashes. Matching duplicate acknowledgment for that latest sequence returns 204. Mismatch returns 409; an older sequence returns 410; a future/missing terminal result returns 409. It atomically removes the bulky retained result and releases its charged storage. A delayed push/resolve at or below acknowledged sequence never executes again. The compact sequence fence survives normal cleanup. Retain its original authenticated scope/source/generation binding as well as sequence/request/outcome hashes. After authentication/scope authorization, match an exact latest duplicate ack against this fence before mutable writable-generation, lease, source-retirement or rebinding admission. It needs no retired outcome decoder. Normal lifecycle cleanup must preserve this compact evidence so a lost ack response remains retryable after retirement/rebinding; explicit destructive scope/account deletion retains section 4's exclusion. A different binding never receives duplicate-ack success.

There is at most one unacknowledged terminal outcome, at `last_acknowledged_sequence + 1`. Committed, rejected and cancelled all consume this number. The next operation cannot be admitted until acknowledgment; rejection/cancellation can therefore create gaps in the source IDs seen in committed history. Global bundle sequence advances only for committed business bundles, under its existing contract. Separate terminal-operation progress from committed-history progress in storage and tests.

The client has a durable `ack_pending` obligation. Lost ack response retries the same ack; reopening after local settlement never repeats business application or freezes another upload until ack succeeds. Pull may continue only if it does not alter the completed disposition or violate lifecycle guards. Ordinary successful one-bundle upload now requires one additional ack HTTP request. This explicitly replaces the old request-count expectation; no-change Sync adds none, and recovery must not add discovery/probe requests on every ordinary row. A future piggyback optimization is outside this scope.

### 3.4 Distinct source owners

Owner decision accepted 2026-10-22: within one scope, client-upload sources and trusted server-write sources have distinct identities. `Connect`, client push, resolve and ack own client-upload sequences; `WithinSyncBundle` and `ScopeManager.ExecWrite` retain their separate receipt/committed-history contracts and must use different source IDs. This expressly supersedes any earlier allowance to share a client source with a trusted writer; the remaining o0001 receipt, retry, exact caller-owned tuple and reservation guarantees are unchanged. The same source string in another scope is independent.

Establish client ownership atomically when Connect or explicit source replacement creates its generation binding: retain the existing upload-progress row even at acknowledged sequence zero. A generation binding or that durable progress row identifies a client source. An existing source-state row without either client marker identifies a trusted writer. A successful trusted write retains its existing source-state/committed watermark as ownership evidence independently of receipt or history pruning. No second ownership registry, intent epoch or wire endpoint is introduced.

Reject cross-owner use before receipt replay, callback invocation, sequence allocation or business mutation, under the existing per-scope upload serialization and lifecycle locks. Client Connect against a trusted-writer source returns the existing409 `source_generation_conflict` with field `source_id`; the trusted writer APIs use the same typed conflict when given a client source. This is an ownership conflict, not a generation change or permission to retry the callback. An original client ack remains matched against its compact original-binding fence as specified above.

Client progress/ownership and a used trusted-writer source-state fence survive normal binding cleanup, ack, source retirement, receipt/history pruning and restart. Normal cleanup cannot delete these fences or convert the source to another owner. Missing/corrupt required client binding/progress fails closed. Explicit destructive scope/account deletion remains the existing exclusion; it may cascade ownership with the rest of that scope. Existing reserved-writer key-share concurrency, source admission and distinct-writer receipt replay remain unchanged. Tests must prove both ownership directions and zero callbacks/receipt/business/history/progress mutation, including cancellation/rejection before and after acknowledgment, binding cleanup and restart.

## 4. Server transaction, retention and capacity contract

Reuse the existing scope fence and source-generation owners. Establish and document one lock order compatible with scope deletion, initialization, source retirement and generation changes. Do not turn an established reserved-server-writer key-share reservation into a service-wide exclusive lock. Source serialization must cover push, resolve, ack and lifecycle operations; it must not make unrelated scopes wait on one long business transaction.

Inside the source-owned transaction, validate request binding, apply business work, run deferred effects, finalize authoritative capture, validate full effect coverage, serialize/hash the exact projected outcome, store it independently of ordinary history, and advance terminal source state. Commit them together. If the complete result cannot fit its advertised bound or cannot be stored, roll back everything; never commit and then discover that the only recovery evidence cannot be stored or encoded.

A typed conflict/application rejection rolls back the business savepoint, restores capture/transaction context, then stores the bounded rejection and terminal source state in the enclosing transaction. Tests must prove that deferred effects, history, initialization state and user callbacks cannot leak across that rollback. If error classification or rollback integrity is uncertain, roll back the whole transaction and return a nonterminal error. No generic SQL exception is silently converted to a business rejection.

Store the original generation-projected result, not a pointer into prunable history. Exact push replay and resolve read it without re-running mutable business admission or reprojecting through a changed generation. Authentication and authorization still apply. Pin the generation contract needed to decode it. Normal source retirement/replacement, binding upgrade and generation retirement must refuse to destroy unresolved outcome evidence (`push_outcome_pending`). Explicit destructive scope/account deletion remains a separate authorized lifecycle operation, outside the recovery guarantee; the client preserves its local state and reports that lifecycle loss.

Unacknowledged outcomes have **no TTL**. A client that never returns can retain one bounded outcome indefinitely; storage pressure does not authorize eviction. Retain `MaxClientSourcesPerUser` (default 64) and its existing permitted range/lifecycle fencing. The default complete decoded outcome ceiling is `MaxBytesPerBundle + 8 KiB` (currently 16 MiB + 8 KiB). Allow a configured complete-outcome ceiling from `MaxBytesPerBundle + 8 KiB` through `64 MiB + 8 KiB`. Persist the chosen value when the fresh v1 protocol store is initialized; it is immutable for that store, and every service instance must match it at startup. A mismatch fails startup before serving traffic. Changing this bound requires a separately designed lifecycle transition; it never authorizes automatic database reset. Enforce it on committed, rejected and cancelled envelopes before publication. A legal request whose expanded result cannot fit returns 413 `push_outcome_too_large` after complete rollback. This nonterminal error does not prove an earlier attempt did not commit.

Store at most one complete unacknowledged outcome per source, with bounded identity/ack metadata. Avoid redundant full representations. Logical retained payload per scope is bounded by the admitted source count times the complete-outcome ceiling; SQL indexes/row overhead/WAL are measured separately. Total deployment storage also depends on scope count and is **not** globally bounded by this specification. Do not introduce prepaid slots, service/scope byte-quota rows, credit reservations or background reclamation. Existing source admission and server-write reservations retain their own contracts.

A storage write failure rolls back the entire outcome/business transaction and returns the existing nonterminal infrastructure error; a client retains F and resolves uncertainty. No capacity promise is inferred from connect. Resolve/read/ack have no new quota-admission gate, but still require functioning database storage. Report outstanding outcome count, retained payload bytes and oldest age without payloads. The immutable advertised ceiling and pinned source/generation contract keep retained evidence decodable until ack. Do not rewrite or evict evidence to satisfy a different configuration.

C02 must document exact SQL owners, bounded metadata and lock ordering, and prove single-outcome/source enforcement across processes, transaction rollback under storage failure, history-pruning survival and acknowledgment cleanup. Global storage quotas are a separate future product decision.

## 5. Recovery snapshot contract

`POST /sync/snapshot-sessions` gains optional `recovery`:

```json
{"recovery":{"minimum_bundle_seq":42}}
```

`minimum_bundle_seq` is required inside the closed `recovery` object and is nonnegative; there is no outcome reference. `source_replacement` and `recovery` are mutually exclusive. Client admission requires the clean-state gate in section 6.2. No snapshot path retargets or consumes a frozen operation.

Within the existing coherent snapshot transaction/fence, serialize recovery admission with the source lifecycle and require no unacknowledged terminal outcome; otherwise return 409 `push_outcome_pending`. Always require snapshot boundary T at least the requested minimum. If the floor is ahead of current scope authority, return 409 `recovery_floor_ahead` rather than waiting indefinitely or lowering it. A recovery snapshot bypasses old-history availability checks only where full current-state materialization does not need that history. It never bypasses generation projection integrity or scope access checks. Normal source replacement must also reject an unacknowledged outcome.

The response adds required `snapshot_hash` and optional `recovery` echo. Store the exact echo, source/generation binding, T, complete row count/bytes and hash with the immutable session. Pages retain their current ordinal/byte/count semantics and refer to this sealed view. A different request/session may not be spliced into the same local stage. A response that omits/mismatches a requested echo is invalid.

For all v1 snapshots, `snapshot_hash` is SHA-256 over a streaming frame sequence. Each frame is unsigned 64-bit big-endian byte length followed by UTF-8 JCS bytes. First frame: `["oversqlite.snapshot.v1", source_id, generation_id, generation_fingerprint, snapshot_id, T, recovery_or_null]` (with section 3.1's exact hash-only integral projection). Each following frame is `["i", table_index, key_values, row_version, full_payload]` in exact snapshot ordinal order. Final frame is `["end", row_count, logical_byte_count]`. Existing logical-byte accounting is unchanged. `recovery_or_null` is the closed recovery object exactly as echoed; key order is JCS canonical. Test empty snapshots, alternate chunking and all supported codecs. Server and client compute incrementally; no all-rows host buffer.

Creation retains existing bounded admission/session TTL. It is not made magically idempotent by this change. A lost create response can leave an orphan until TTL and consumes the retry budget. A fully downloaded and hash-verified local view remains usable after session expiry; incomplete expired views require a new session within restart limits. Remote writes after T do not invalidate the sealed view. Commit only T, then ordinary pull consumes T+1 onward.

## 6. Local settlement and clean-state admission

### 6.1 Resolve frozen work through ordinary settlement

Use existing original bases and dirty captures, immutable frozen request F, current local state and its existing capture guards. Add a durable complete terminal outcome C and an ack obligation. Do not add a new per-column intent model or existence-epoch journal for this delivery. Preserve the current whole-row newer-intent semantics; canonical typed payload comparison and SQLite uniqueness comparison retain their separate existing roles.

Prepared F is conservatively possibly dispatched. On timeout, ambiguous response or reopen without validated C, resolve-or-cancel using exactly F before replacing it. Ordinary safe transport retries may replay F exactly within existing policy. Persist the complete validated C and binding atomically before settlement; incomplete staging is never a certificate. Once C is durable, retry local settlement from C without refetching or resolving it on every invocation.

- **Committed:** use the existing final-state settlement engine, including guarded handling of newer local rows, deletions, server normalization, effects outside F and local-only values. Consume only the captured intent the existing guard proves settled. A later edit may remain dirty. If the final state or transition is unsafe, preserve F/C, original bases, all local rows and metadata; report a durable blocker and send no ack. Do not invent a column merge or use a snapshot to bypass this blocker.
- **Rejected:** the terminal record proves no business effect from F. Preserve original bases and current intended state, including changes since freeze. In one guarded local transaction, release the old frozen-operation ownership, retain/requeue unsatisfied intent and record its disposition plus ack_pending. The existing row conflict resolver may subsequently act on the retained real rejection evidence; no successor upload may be frozen until ack and the required existing rejection disposition are complete. Resolver cancellation/failure must preserve unsatisfied intent.
- **Cancelled:** use the same guarded requeue/disposition and ack order, without fabricating a conflict or invoking rejection policy. The old operation is permanently fenced, but its intended edit is not discarded.

If capture ownership cannot prove a safe settlement/requeue, remain blocked with F/C intact. Implementers may change storage layout to make the existing guards durable; they may not silently weaken preservation or introduce a new merge policy. Regression tests must cover same-value writes, deletion/recreation and later edits as well as the normal case.

Atomic local disposition records source progress, existing baseline/checkpoint effects and ack_pending together. Ack never depends on a later snapshot. After ack is durably confirmed, remove obsolete evidence in bounded owned cleanup; any rejection evidence still needed by the existing resolver remains owned until its disposition. No new upload is frozen while the predecessor is unsettled or ack_pending.

### 6.2 Strict clean-state gate

Both automatic recovery and explicit Rebuild require, for the entire attached synchronized scope:

1. No dirty managed rows or other pending local synchronized intent.
2. No frozen request/outbox operation, including a prepared request not known to have been dispatched.
3. No terminal outcome awaiting local settlement and no retained rejection awaiting local disposition.
4. No ack_pending obligation; the client has completed the server acknowledgment handshake.
5. The existing same-source/generation/authority/attachment lifecycle and schema/capability gates are valid.

Check before any recovery snapshot network request, throughout preparation at resumable boundaries, and again under the final writer before business DML. No apparent noncompetition exemption applies: even one unrelated dirty synchronized row blocks recovery. Local-only values do not themselves count as synchronized intent, but their complete preservation remains mandatory.

A new application write during preparation invalidates the clean gate. Preserve the write, discard or park only derived work, and return an actionable blocker; do not merge it into S. The next eligible attempt requires ordinary authorized sync/application work to resolve all intent and re-establish the full gate. Recovery may not delete/rebase metadata, synthesize acknowledgments or clear dirty state to make itself eligible. A committed-but-locally-blocked upload remains blocked in this delivery.

### 6.3 Minimal controls and diagnostics

Default mode is `automatic_clean`. Ordinary Sync may recover an eligible historical blocker by installing a clean authoritative baseline. Mode `disabled` prevents this automatic checkpoint jump; it does not disable exact outcome resolution/ack or ordinary sync. Explicit Rebuild remains subject to the same safety/clean admission. Applications requiring every history event must disable automatic baseline recovery; a snapshot cannot reproduce skipped events.

Expose idiomatic Go/Kotlin equivalents of `RecoveryStatus` and `RetryRecovery`. Status contains a bounded phase/reason, recovery ID, observed boundary, blocking table/index/trigger/dependency, at most 64 exact diagnostic keys with truncation, and allowed next action. Do not expose business payloads by default. `RetryRecovery` re-arms one parked attempt after explicit application request; it does not override the gate or create another owner. There is no preview, decision-page, choice/seal/submit or disposition-report API.

Persist only the current phase/blocker, guards and completion information needed for restart safety. A mode change invalidates automatic eligibility and must be rechecked under the final writer. A bounded latest completion/status is sufficient; no report history or per-row outcome catalog is required.

## 7. Durable clean snapshot workflow and delta

Outcome resolution/settlement/ack is the separate preceding ordinary upload lifecycle from sections 3 and 6.1. The snapshot workflow never owns or consumes a pending outcome.

| Phase | Durable transition / invariant |
| --- | --- |
| `captured` | Pass the clean gate. Bind blocker/bundle/hash/checkpoint, source/authority/generation/attachment, schema/capability fingerprint and current mode. Business state stays unchanged. |
| `stage_baseline` | Recheck eligibility. Compute floor=max(blocked authoritative unit, current checkpoint, every installed newer observation). Acquire/verify complete S, boundary T and exact echo/hash. |
| `prepare_transition` | Build key-based delta to S, complete preserved dependency closure, local-only capture/guards and uniqueness schedule. Publish only guarded auxiliary state. |
| `ready` | Under the final SQLite writer, recheck the entire clean gate, attachment/source/generation/schema/TEMP schema/semantic versions, S identity and dependency membership including new children. Admit all final work before DML. Drift invalidates derived work, never application edits. |
| `completed` | One transaction applies the delta, validates survivors/constraints/full local values, installs S baselines/absence observations, advances checkpoint exactly T and records completion. No upload sequence change, requeue or ack is performed. Release obsolete staging in bounded resumable chunks. |

Parked `blocked`/lifecycle states keep a reason and durable retry eligibility. A crash during preparation resumes or invalidates only derived work. A crash within final application leaves either the complete old state or complete installed state. Cancellation joins the actual owner; after-commit cancellation must report/recover completion, not claim rollback. An `applying` marker must never commit separately ahead of business changes. Remote session release is best effort under the budget/TTL.

Snapshot delta rules:

- Match by exact managed sync key. Keep unchanged rows in place. Preserve local-only columns of survivors. Update changed survivors, insert genuinely new identities with normal admitted local defaults, and delete only identities actually absent from S.
- Feed the complete delta and preservation closure to the same final-state engine used by pull/push/conflict paths. Include current unique holders; remove the old assumption that managed tables were already cleared. Do not retain the old clear-and-reinsert path as a fallback.
- Clean managed descendants needed during temporary vacating survive with full values/metadata. A dirty synchronized descendant fails the clean gate. Local-only dependent tables require exact ownership/membership and full value guards; incidental cascading loss is forbidden. Empty-set checks repeat under the final writer. Deferred NO ACTION references can be preserved only when exact referenced identities survive.
- An application-owned main/TEMP trigger on any table the recovery would mutate blocks recovery before business DML. Unchanged tables do not block merely by existing. Do not use trigger suppression, a trial transaction/clone or a single UPDATE of a unique swap as a general correctness proof.
- Cross-row desired collisions remain typed `conflicting_desired_state`. Unsupported uniqueness, unsafe trigger/dependency effects, corruption and intrinsic capacity failures remain precise blockers. A valid S need not be physically installable in every supported application state.

Remote writes after T are consumed by ordinary pull. Installed version-skipped observations set the minimum floor; a lower snapshot cannot overwrite them. Snapshot absence is absence-at-T evidence, not a fabricated tombstone version. Normal application writes remain possible while preparation yields; local-only tables need existing explicit revision tracking or bounded writer-owned membership/value revalidation because managed dirty counts do not cover them.

## 8. Budgets, retry eligibility and cleanup

Retain default client limits: 131,072 aggregate workspace entries, 256 MiB logical workspace and 1,000,000 soft work units per slice. Charge original captures when copied, full outcome certificates, snapshot rows, minimal recovery metadata, graph keys/edges/reachability, guards, prepared statements, decoded host copies and simultaneous superseded candidates. Referencing an owned immutable row can avoid a duplicate charge; copying it cannot. Durable primary outbox/dirty storage does not make additional recovery copies free. Snapshot/server wire ceilings also apply.

Keep existing 64 logical upload-attempt and transport retry limits. Add at most 64 recovery HTTP dispatches per public Sync invocation, including resolve, snapshot create/page/retry and ack/cleanup calls in these workflows. Upload settlement and snapshot preparation share this parent budget even though they run sequentially. Count before dispatch, never reset a parent budget from a nested helper. Exhaustion returns typed incomplete progress with resumable state. Reserve one available exchange for an essential ack or persist ack_pending for the next call. No new session is necessary merely because useful paging exceeded one invocation.

Allow at most three relevant local/schema replans and one replacement of an expired incomplete snapshot per invocation. Cancellation/deadline takes precedence. Persist failure signature across calls: original unit/hash, failing capability/dependency, schema/algorithm version, relevant local capture versions and authority observations. One eligible baseline attempt per unchanged historical-failure signature; if it fails with the same infeasibility witness, park. Unrelated later server sequence hints do not automatically redownload the scope. Relevant correction or explicit `RetryRecovery` can re-arm one attempt; it cannot suppress safety checks or multiply an in-flight attempt.

Static capability failures use normal local retry after a relevant schema/intent change, with no snapshot fetch. Intrinsic entry/byte/unit failure names its dimension and required remedy; it does not request a larger snapshot. Avoid quadratic desired-pair work and SCC closure blow-up where feasible, but no unrelated optimization campaign is required. Measure actual database work/temporary space; do not count one recursive SQL statement as one unit. The current 200-vertex cycle admission exceeds default entries despite the 200-row chain passing, so do not promise arbitrary cycle sizes.

Only delete owned obsolete auxiliary rows; never delete dirty/outbox/intent/evidence still needed for disposition or acknowledgment. Reopen must finish cleanup exactly once. Include logical table census and closed-file/WAL storage in resource evidence. No VACUUM, manual table clearing or database recreation may conceal accumulation.

## 9. Required contract and persistence work

Server fresh schema needs source terminal/ack fences, one retained exact outcome with generation binding, and snapshot floor/hash metadata. Client fresh schemas need durable exact F/C ownership, existing capture/base guards, ack obligations and a minimal clean-snapshot phase/session/hash/floor record. Reuse current owners where their invariants fit. Do not introduce new intent epochs, decision/report tables, quota credits or placeholder future public APIs.

C01 must promote the settled OpenAPI as the active contract, reconcile explicit superseded p0000/p0001/Kotlin statements, update both SDK protocol gates and fixture classifications, and pin new spec/OpenAPI/corpus hashes together. Legacy envelope examples may survive only in explicitly historical or rejection fixtures. Both consumers fail closed on absent/mismatched corpus and execute all claimed families. C01 activates paired wire models, protocol gates and executable contract consumers. C02/C03 implement outcome persistence and durable client settlement/acknowledgment with scoped proof. Safe snapshots, clean automatic recovery and final platform/runtime qualification remain pending in C04–C07; the intermediate checkout is not a deployable protocol release.

This candidate chooses the initial-protocol qualification profile. The exact requirements and command obligations are in the [validation contract](r0001-oversqlite-durable-recovery-validation.md). Milestone A (C00–C03), B (C04) and C (C05–C07) have scoped exits in the checklist. Their intermediate proof does not replace final initial-protocol acceptance. Do not expose an unfinished automatic coordinator between milestones. Missing launcher authority is an explicit first prerequisite, addressed through reviewed authority replacement rather than invented files or bypassed checks.

Implementation continues only in separately authorized phases using the [checklist](r0001-oversqlite-durable-recovery-checklist.md). This specification does not assert that the protocol, runtime, API or storage described here already exists.

## 10. Explicitly deferred

The following are outside every implementation phase in this candidate: per-column dirty-state merging; a new existence-epoch intent model; paged previews/decisions; application winner-choice APIs; disposition reports; using a later snapshot to settle an unsafe committed upload; general trigger semantics or SQL-expression parsing; global/per-scope prepaid outcome quotas; piggyback acknowledgment. No scaffolding or speculative extension points for these features are required.

A future proposal must identify a demonstrated application need, its added public/storage states, preservation tests and independent acceptance. The superseded broader draft is retained only as a historical preimage in the scope-revision evidence directory; it is not an additional implementation authority.

## Approved future qualification profile — 2026-10-26

Section9 qualification now permits the owner-approved future composition in [validation section9](r0001-oversqlite-durable-recovery-validation.md) for explicitly bound candidates. Its manifest preserves R01–R10 behavior, source/runtime/body distinctions and fresh-body/reuse controls. The approved amendment uses normal entries at most300s, four source-proven overlap removals and a1/5/10/25-user load ladder with25concurrency maximum; former50/100 load claims are retired, not subsumed. Other scale/duration/resource/byte obligations remain. This prospectively changes qualification composition, caps and the named load maximum; C05/C06/C07 evidence, acceptance/delivery history and section10 exclusions retain their original meaning. E41 authority adoption does not establish E42 execution or release readiness.

### Approved E42 repair amendment — 2026-10-27

The owner approved the [combined repair amendment](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/owner-approval-and-plan.json). The current version2
profile binds its [successor review packet](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/reviewed-profile/proposal.md):508 declarations,
499 normal entries and9 orchestration controllers. The original80-method local
Node entry is partitioned into32/48 whole-class groups, retaining all methods and
300-second wall /270-second work /30-second cleanup caps. Other entry membership,
source workload sizes, body deadlines and the25-user maximum remain unchanged.

The three identified stale fixture files use causal cascade-observation phases
and exclusive-lifetime migration ownership. The worker client immediately owns
its internal readiness rejection while preserving caller-visible errors and
bounded cleanup; only its client asset size/hash changes in the manifest.
These are approved fixture/ownership repairs, with no wire/schema/provider or
SDK-default change. Original coverage columns remain frozen baseline evidence;
additive current fixture bindings identify the four repaired method bodies.

Historical receipts and predecessor seals retain their original identities.
Reuse requires reviewed dependency nonreach; affected web Worker/Fetch/Browser
isolation evidence must be refreshed after the client repair. Linuxx64/ARM64
retain97selected methods each and remain owner-blocked. Approval does not grant
native qualification, E43 acceptance, commit, push, release or deployment.
