# p0001: Oversqlite FK-safe synchronization and automatic conflict resolution

> Current durable-recovery authority: [r0001](r0001-oversqlite-durable-recovery.md) and the active `swagger/two_way_sync.yaml` supersede conflicting lifecycle/envelope, request-count, source-sequence and pending-source-retargeting statements below. The sole marker remains `v1`; every sync request now requires its exact protocol header. All terminal outcomes consume source sequence, durable local disposition precedes ack, and ack precedes the successor. Old acceptance/evidence records below retain their historical status. Existing typed codecs, request/bundle hashes and unrelated contracts remain in force. Runtime phases after C01 remain pending.

Status: **REVISION 3 STAGED-IMPLEMENTATION AMENDMENT; READY FOR INDEPENDENT SPECIFICATION REVIEW;
REVISION 2 ACCEPTED; REVISION 3 NOT YET REVIEWED OR ACCEPTED**

Created: 2026-09-25

Revision 3: 2026-09-25. Separate ordinary FK correctness from targeted and automatic resolution,
settle contracts per active slice, and require a complete behavior before expanding implementation.
Sections 1.3 and 5 define the proposed staging changes. Sections 2 through 4 retain the accepted
end-state requirements and regression obligations without alteration.

Revision 3 refinement: 2026-09-26. Section 1.4 adds simplification and replacement requirements
to the existing slice gates. The accepted behavior and qualification matrix remain unchanged.

Revision 2 subsequently passed independent specification review and received owner acceptance in
[owner acceptance](../.artifacts/p0001-fk-safe-sync/owner-acceptance.md). The review-status statements
below and in sections 4.1–4.2 describe their historical review rounds. They do not undo that acceptance
or establish a review result for revision 3. The present request authorizes specification amendment
only; implementation waits for agreement on the reviewed approach under the existing authorization.

Revision: 2026-09-25, addressing all five findings against manifest SHA-256
`94bd5f7a7fbaf0e3a18ac9a399b48679375462dbd0a651cf2f5b54d35fd6055b`.
Review input: `/Users/pochkin/.codex/attachments/c7418d42-30c4-446f-82a8-5a6b516515d1/Pasted text.txt`,
SHA-256 `31dfd14b6c1d45412aaef7b7915be5676fcbf1aefe1686b983b1e71469df1979`.
Finding disposition and new acceptance cases are in section 4.1. Addressed in draft is not a review pass.

Revision 2: 2026-09-25, addressing the remaining fallback-dependency blocker against manifest
`b3a38f47afde03a7524f4d0b705c5e7166aa056ed259290c4153f2e62d9e09f9`.
Rereview input: `/Users/pochkin/.codex/attachments/48c3fcf3-fc7d-4f88-878b-296a5b4bb4ed/Pasted text.txt`,
SHA-256 `4cf9c50aadd64fb9160a4352a7b594d4394742a34798f88a58a8e3c74dec7cb3`.
That review resolved four original findings at specification level and partially resolved R3;
it remained BLOCKED on application-validation dependency coverage. Section 4.2 records this revision.

Authority repository: `/Users/pochkin/Projects/my/go-oversync`

Paired repository: `/Users/pochkin/Projects/my/sqlitenow-kmp`

Companion: [SQLiteNow FK-safe sync](../../sqlitenow-kmp/specs/oversqlite-fk-safe-sync-and-automatic-resolution.md).
Historical accepted revision 2 inputs: [baseline manifest](p0001-oversqlite-fk-safe-sync-baseline.json).
Revision 3 identity: [candidate manifest](../.artifacts/p0001-fk-safe-sync/revision-3-manifest.json).
Review request: [revision 3 read-only review](../.artifacts/p0001-fk-safe-sync/revision-3-review.md).

## 1. Authority, objective, and gates

This is the Go-owned proposed behavior contract for the Go server, direct Go client, Kotlin
Multiplatform client, Swift-facing products, shared contract corpus, documentation, and tests.
The owner authorized implementation of revision 2 in both repositories, then requested replanning
after rolling back the first attempt. That authorization remains recorded. This amendment does not
start runtime work, accept a candidate, or authorize delivery.

Oversqlite must synchronize supported foreign-key-linked records without silently losing surviving
records, freezing an otherwise sendable backlog, or requiring applications to implement dependency
traversal and recovery. Coverage includes insert and attach, updates and reparenting, key changes,
deletes and cascades, self-references and cycles, bounded push selection, replay and settlement,
server validation, incremental pull, conflicts, snapshots, recovery, and concurrent local writes.

### 1.1 Proposed precedence and intentional amendments

Once accepted for implementation, p0001 governs the FK dependency, final-state application,
automatic conflict policy, targeted reconciliation, and resolution reporting subjects below.
[p0000](p0000-oversqlite-protocol-simplification-and-efficiency.md) continues to govern retained
wire encoding, hashes, source/generation identity, bundle atomicity, and transport behavior.
[Convergent sync](../../sqlitenow-kmp/specs/oversqlite-convergent-sync.md) continues to govern
interactive drain, pause behavior, cancellation, recovery precedence, and the existing push budget.
The [validation runbook](../../sqlitenow-kmp/docs-engineering/oversqlite-validation.md) governs
evidence selection and invalidation, subject to expressly mandatory qualification commands.

The following changes are explicit, not implied by an implementation:

| Prior subject | p0001 amendment |
| --- | --- |
| Per-row/deferred conflict default | Group-aware automatic `ServerWins` becomes the default; `ClientPreferred` is opt-in. |
| No endpoint needed for the earlier convergence work | That observation applied to the earlier scope; this work adds `POST /sync/reconcile`. |
| First-unsent typed payload pair and conservative legacy grouping | Durable indexed server-visible relationships become the dependency authority. Unknown is distinct from known-empty and requires reconciliation. |
| Preserve pending intent during settlement | Retained. Only an explicit conflict-policy decision may discard affected intent, with an atomic durable report. |
| Go explicit resolution is local-only | Resolution may perform bounded network reads and candidate pushes. |
| FK validity proves successful application | Surviving business rows and payloads must also be verified. |

Existing status headers, recorded acceptance hashes, and historical review records are historical
evidence; this amendment does not reaccept or rewrite them. In particular, the older Kotlin p0000
companion pins a different p0000 candidate. Its earlier authorization is not authorization for this
package. Before runtime work, record acceptance of the exact reconciled authority set, not just a
new filename or the old acceptance record. A conflict on a subject not expressly amended here must
be resolved in the specifications before implementing its consumer.

### 1.2 Breaking-change posture and supported topology

- Retain protocol identity `v1` and existing hash-domain identities. No `v2`, negotiation,
  compatibility aliases, dual readers/writers, or mixed old/new deployment requirement.
- Cut over server, Go/Kotlin clients, Swift-facing products, fixtures, examples, and docs together.
- Detect incompatible internal sync layout before modifying the database. Leave both business and
  sync data untouched and return the existing fail-closed format error or a typed equivalent.
  Do not migrate incompatible prerelease sync state or automatically reset it. Development reset
  or recovery requires a separately explicit action. New-layout restart/replay remains mandatory.
- Preserve currently admitted managed tables with one visible sync key and admitted single-column
  logical FKs to that key, including self-references and cycles. Preserve action, representation,
  nullability, scope, and server-deferrability requirements. Do not admit arbitrary composite client
  FKs or unmanaged dependencies, or reject currently supported schemas to avoid fixing a defect.

Schema-evolution migrations, arbitrary application-schema redesign, unrelated refactoring, undo
archives, delivery, staging, commits, pushes, publication, and deployment are outside this work.

### 1.3 Revision 3 milestone boundary

Implement and review Stage A, ordinary FK correctness, before entering Stage B, targeted and automatic
resolution. Section 5.3 defines the slices. This changes implementation order, intermediate behavior
and qualification triggers; it does not remove requirements from the final p0001 product.

Stage A retains the current deferred conflict default and existing explicit resolver APIs. It adds
no reconcile endpoint, automatic group policy, ClientWins rename, or new resolution-report API.
It must preserve ordinary pull/settlement intent and protect clean survivors. Existing conflict
paths reached by changed code must use the same safe application rules. Where completing a decision
needs unavailable dependency evidence or historical repair, return typed local reason
`fk_reconciliation_required` with bounded affected-identity detail; preserve intent, outbox/replay
evidence and checkpoint, and leave the decision pending. Do not run a busy automatic retry loop,
guess relationships, declare the schema unsupported, or install a partial authoritative result.
Settle its public shape before its consumer. This is an explicit Stage A limitation, not fulfillment
of unknown-baseline reconciliation, FK-04/FK-05 or historical repair. Fresh supported Stage A flows
must maintain trustworthy baselines; this result cannot excuse missing baseline maintenance.

Capture the complete base B when introducing the new baseline layout, even though field merging
comes later. An incompatible previous layout still fails before mutation under section 1.2. Do not
create a migration, compatibility reader or implicit reset to bridge implementation slices.

Apply section 2.5's entries/bytes/work and cancellation rules to staging and traversal introduced by
each slice. Keep the existing FK-06 backlog-preservation deficit assigned to A3; an A1 development
pass does not claim to fix it. Stage A's qualified candidate must account for all Stage A working
state under one aggregate budget. Later-only sessions, floors and reports need no placeholder
implementation; once introduced they join their specified budgets. Do not introduce unbounded
working state to defer its limits to a cleanup pass. Survivor verification applies from A1.

Stage B retains all accepted targeted reads, historical repair, default ServerWins column/existence
merging, ClientPreferred, rejection evidence, journal lifecycle and Swift obligations. Introduce
targeted business-state installation together with historical repair and revision guards. Introduce
report-requiring decisions together with reservations, replay and acknowledgment. Full p0001
acceptance requires both stages and section 5.2's final matrix. A milestone review is not release
authorization; the coordinated breaking cutover and prohibition on mixed-version support remain.

### 1.4 Simplification and replacement requirements

A2-03 owner clarification (2026-09-27): attachment epoch is a distinct durable positive int64
incarnation revision in the existing attachment owner. Its lifecycle and exact guard semantics are
defined in the tracked engineering contract's "Attachment incarnation guard" and paired logical
persisted-format inventory. This accepted clarification adds no HTTP fields, hash-domain changes,
migration or historical B retargeting; it preserves frozen outbox replay priority.

Implement each slice by changing the existing owning mechanisms and removing what it supersedes.
Use section 1.2's coordinated breaking cutover: replace in-scope database layouts and wire contracts
without migration or compatibility layers. The owner will recreate incompatible databases explicitly;
retain rejection before mutation, supported-layout restart/replay, v1 and existing hash domains.
Database recreation is not a substitute for crash recovery within the new supported layout.

Track S01..S05 per slice in the existing ledger, separately for Go and Kotlin. These are part of the
existing entry/exit items and review stops, not a new foundation phase or a separate cleanup project.

- [ ] **S01: Describe the replacement before coding.** Record the current and intended call flow,
  owning functions/state, and what the slice retains, changes, replaces and removes. Identify where
  transaction, replay, cancellation and local-write guards remain. Keep this description limited to
  affected paths; if the behavior is new, explain why no existing mechanism can own it.
- [ ] **S02: Justify additions.** For each new abstraction or persistent structure, name its current
  requirement, owner and callers, and why extending existing facilities is insufficient. Explain the
  purpose/lifecycle of additional state copies and identify authoritative versus derived state.
  Do not introduce speculative coordinators, generic engines, wrapper chains or extension points
  for later slices. Required baseline B, paged staging, bounds and restart evidence remain mandatory.
- [ ] **S03: Complete the replacement.** Switch affected callers and remove superseded production
  branches, heuristics and unused schema objects in the owning slice. Keep regression assertions
  while adapting their setup to the replacement. Do not keep an old implementation as a fallback or
  compatibility path. Code still serving an unreplaced path is not obsolete: name its callers and
  owning later slice, without duplicating authority for a path already converted. Replan at the
  existing stop if removal would require expanding the agreed slice.
- [ ] **S04: Review the resulting complexity.** At the slice exit show the resulting call flow,
  removed mechanisms, remaining state owners, and justified additions. Report production and test
  additions/deletions separately; follow the repository's complexity guidance for affected code.
  Require a coherent implementation with clear ownership and no redundant mechanism for the same
  responsibility. Passing tests alone does not close this item. There is no net-negative line quota;
  fewer lines or helper extraction alone does not demonstrate simplification.
- [ ] **S05: Preserve correctness and scope.** Verify the replacement retains the slice's complete
  behavior, supported topologies, transaction/error precedence, bounds, cancellation and replay.
  Keep section 5.2's qualification obligations and section 1.3's explicit Stage A boundaries.
  Simplification does not authorize unrelated cleanup or weakening behavior. Stop and seek a
  specification amendment if a simpler design needs a behavioral change.

Stage A has these concrete replacement targets:

| Slice | Required direction |
| --- | --- |
| A1 | Extend existing application/transaction facilities and consolidate overlapping survivor handling on the reached pull and settlement/replay paths. Preserve operation-specific guards; do not add a parallel synchronization coordinator. |
| A2 | Replace existing dependency-grouping heuristics with the authoritative directed planner and remove superseded selection branches. Retain dirty/base captures needed for other specified semantics; do not maintain two competing dependency authorities. |
| A3 | Replace whole-backlog host materialization with bounded paged preservation in the owning workflow, and complete shared safe application on affected lifecycle paths. Remove superseded copying paths while retaining their safety assertions. |

## 2. Client correctness and bounded execution

### 2.1 Server-visible relationship baseline

Maintain indexed durable FK baseline information independently of the latest local payload. For
each relevant record distinguish known server-visible relationships, known absence of relationships,
and unknown baseline requiring reconciliation. Null capture payloads and tables without outgoing
FKs must not mean that the record could reference every dirty record in another table.

Update that baseline correctly at initial attachment/publication, settlement, replay, conflict
resolution, pull, snapshot hydration, deletion, and recovery. Local edits must not replace the
relationship still visible to the server. Dirty capture, requeue, and no-op pruning must preserve
this distinction; a typed payload pair alone is not the new baseline authority.

Dependency decisions consider both authoritative baseline and desired local relationships. If
server C references P but local C now references Q, deleting P must still account for C's old
relationship. Unknown baseline must be resolved before selecting a dependent destructive action.
Do not introduce a compatibility reader for incompatible prerelease metadata to manufacture it.

For field-level resolution also retain the complete typed authoritative base payload B and its
identity/version from which the pending edit began, independently of desired payload L and latest
observed server payload S. A never-published insertion has an explicit absent base. Preserve B
through subsequent local edits; after an atomic rebase, use S as the base of remaining intent.
Current reconciliation cannot reconstruct a lost historical B. An existing row with missing or
untrustworthy B returns `resolution_base_unavailable` and preserves intent; never infer a change
mask from S or silently downgrade to whole-row replacement. Fresh-layout capture and every
authoritative installation must make B available before allowing subsequent local writes.

### 2.2 Bounded push planning

Build directed prerequisites from operations and relationships:

- A child requiring a new parent depends on that parent becoming available.
- Parent deletion depends on deletion or reparenting of server-visible referencing children.
- Key changes account for the new identity, dependent reparenting, and old-identity removal.

Discover prerequisites through indexed durable state beyond the initial dirty page. Capture order
must not hide a required child update. Collapse strongly connected prerequisites into indivisible
components. Keep independent components independently sendable; table-level uncertainty must not
merge unrelated parent/child pairs. Represent the graph/frontier in durable staging with paged host
materialization, rather than loading the whole connected closure into host memory.

Respect hard row and decoded-request-byte limits. A complete component may cross the soft upload
target only if it fits both hard limits. A genuinely indivisible component that exceeds either hard
limit produces a typed error with the exceeded dimension and a bounded summary plus paged access
to component identities. Preserve intent and do not freeze an unsendable outbox or consume its
source-bundle ID. An acyclic backlog larger than one batch must still drain through valid groups.

### 2.3 Final-state application

Within each client, share one application mechanism across authoritative pull, committed-push
settlement/replay, conflict resolution, and snapshot installation. It must:

1. Validate original wire identities, versions, ordering, representation, and hashes.
2. Compute intended final business state separately from canonical wire order.
3. Durably stage affected survivors, including clean descendants vulnerable to CASCADE or SET NULL.
4. Apply action-aware SQL under existing transaction and FK-enforcement requirements. Do not disable
   enforcement or assume deferred checking alone prevents destructive action side effects.
5. Verify surviving rows/payloads, protected pending intent, and final FK integrity.
6. Atomically commit business state, metadata, dirty-state transitions, applicable checkpoints, and
   any resolution report entries for that transition.

Do not rewrite canonical history or its order/hashes to compensate for local SQL ordering.
`foreign_key_check` alone cannot detect a cascaded-away valid survivor. Version-skipped records
remain protected: applying an older bundle must not cascade-delete newer targeted-reconciliation
state. Apply only complete validated units; failed verification rolls back their business and
metadata changes. Earlier successfully committed bundles need not be rolled back.

### 2.3.1 Historical dependency repair after targeted reads

Persist each targeted installed record's observation floor `(source, generation, scope_bundle_seq,
disposition)` as well as any authoritative row version. An absent observation is not a synthetic
tombstone/version; its floor records only that this identity was absent at that scope sequence.
Validate every incoming bundle's original hash/order/proofs, including rows whose effects are
already superseded. A floor can suppress an effect only for that exact identity and a bundle at or
below that floor, never for an unknown child or the whole scope. Retain floors until the ordinary
checkpoint has passed them; their storage counts against the shared workspace quota in section 2.5.

If planning a validated historical bundle would introduce a dependency inconsistent with already
installed newer authoritative state, do not repeatedly attempt the same invalid SQL. Roll back or
stop before application, retain that bundle and checkpoint, and enter bounded historical repair:

1. Identify the offending records and concrete newer-observation witness (for example P's deletion
   at 12 versus incoming X->P at 11). Include surviving dependents that its replacement could disturb.
2. Fetch those roots and the required closure through a fresh stable reconciliation session with a
   minimum observation sequence at least the incoming bundle and all relevant installed floors.
   The server selects a current coherent view at or above that minimum; it need not reproduce an
   arbitrary historical snapshot. Missing retained history or source/generation changes follow
   existing recovery precedence, not synthetic proof or checkpoint skipping.
3. Plan authoritative replacement for the repair closure. Newly discovered offending records must
   have explicit live/deleted/absent evidence; do not infer their deletion from a parent's absence.
   Recursively extend repair for a newly exposed dependency mismatch, within shared resource and
   exchange bounds, using a new sealed session if additional roots are needed.
4. Under the ordinary dirty/outbox gate and revision guard, atomically install that closure/floors
   and apply every non-superseded effect of the original bundle. Verify all survivors and FKs. Mark
   every original effect applied or covered by identity-specific newer authoritative evidence.
   Only then advance the ordinary checkpoint to this bundle's sequence, never the session sequence.

A local write during repair returns to the high-level drain/recovery path; direct pull still fails
closed. Failed/cancelled/exhausted repair keeps the original bundle/checkpoint durable and resumable.
An FK failure without a proven newer observation is an integrity error, not permission to fetch
away malformed history. A coherent repair closure still violating FKs is also an integrity error.

Required result for checkpoint 10, P deleted at observation 12, unseen X->P inserted in bundle 11
then deleted in 12: fetch explicit X-at-12-or-later evidence, keep P and X absent, cover X's older
insertion by X's own observation floor, advance to 11 only after its complete validated apply, then
process 12 normally. No full-scope snapshot, fabricated tombstone, or jump from checkpoint 10 to 12.

### 2.4 Concurrent writes, settlement, and replay

Before remote reconciliation, durably capture desired local intent and revisions covering affected
records and dependency membership. At final application, reserve the writer and revalidate those
revisions, including new dependent insertions. If relevant local writes raced, preserve them and
replan. Never clear a newer dirty capture using an older observation. Do not hold database
transactions across network calls or application callbacks.

Replace Go's complete-backlog in-memory settlement preservation with indexed paged preservation and
comparison. Apply the same memory bound to equivalent Kotlin paths, graph traversal, reconciliation,
and report reads. Avoid repeated complete-backlog payload copying on every bounded settlement.
Bounded host memory is distinct from total work: account for durable staging and database query work
as well, and measure the paths that still scale with affected records.

Replay an ambiguous outbox request exactly before resolution may replace/discard it. Timeout,
cancellation, or unknown commit cannot authorize a new source sequence or automatic fallback.
Previously committed bundles remain committed. Multi-bundle resolution is a resumable sequence of
valid atomic server bundles, not one unlimited transaction; its reports distinguish committed local
intent from later fallback decisions.

### 2.5 Shared storage and computation limits

Expose the same positive local configuration in Go/Kotlin/Swift, with these defaults:

| Limit | Default | Accounting |
| --- | --- | --- |
| `max_sync_workspace_entries` | 131,072 | Every staged row copy, graph node, edge, frontier/SCC frame, revision/read-set entry, observation floor and reservation descriptor. |
| `max_sync_workspace_bytes` | 256 MiB | Sum of canonical encoded keys, payloads and metadata for every entry/copy above; no deduplication credit unless storage is actually shared. |
| `max_sync_work_units_per_invocation` | 1,000,000 soft target | Approximate library-controlled indexed row/edge visits, stage writes, graph/SCC steps, comparisons and repeated visits during replanning. |

These are aggregate limits per local database across ordinary push preparation, survivor preservation,
pending historical repair, reconciliation, snapshot working copies and restartable plans, not a
fresh allowance for each phase or component. Existing live business tables, the primary dirty/outbox
store, and ordinary per-record baseline storage are not temporary workspace; copying any of them
is charged. Reports use their separate quota in section 3.4.1, so total auxiliary retention is bounded
by both budgets, not merely one displayed page. Also enforce existing snapshot total limits and all
wire limits where applicable; the strictest applicable bound wins.

Charge before allocating/inserting or expanding a page, including simultaneous old/new copies and
replacement plans. Deduplicate keys before expansion; charge the lookup/work even on duplicates.
Check cancellation and work allowance at each page boundary and at most every 256 work units.
Do not embed an unbounded recursive query, table scan, sort, or graph algorithm inside one nominal
unit. Use indexed keyset iteration and paged durable graph/SCC progress; metadata/index maintenance
has the same bounded operation and cancellation obligations.

At the internal soft work target, persist safe traversal progress, finish slice cleanup and
cooperatively resume the saved frontier inside the same public operation if its revisions still match.
`sync_work_limit` is an internal slice signal, not an ordinary public-operation failure.
Discarding a stale plan does not refund already spent work in the current slice. Required opening
validation completes above target with bounded memory and cancellation checks. An otherwise admissible
first ready atomic unit runs even when it crosses the target; crossing alone neither rejects nor rolls
back the operation. Return success after semantic completion; before another unit, continue through a new internal slice at a safe boundary.
Do not split a bundle merely to fit the target. Saturate approximate work counters on overflow;
hard storage overflow remains a failure.

At entry/byte capacity exhaustion return `sync_workspace_limit` with dimension, configured limit,
used/reserved counts, bounded offending-unit identity and resumability. Stop before outbox freeze or
destructive apply. Keep original intent, replay evidence and checkpoint; retain only useful charged
continuation state. Drop obsolete plans transactionally and release charges exactly once. A unit
that intrinsically exceeds capacity requires a raised explicit limit or application action; repeated
calls must return the same diagnosable capacity result, not silently discard records or spin. Existing
reservations must remain usable after restart; reject a configuration reduction below retained use.

Server reconciliation applies these finite default entry/byte/work limits per build as well, capped
by advertised snapshot session bounds. Add a 30-second build deadline (configurable positive value),
bounded statement/lock timeouts no longer than the remaining deadline, and existing bounded build/
chunk admission. Incoming dependents must come from a transactionally maintained server edge index
keyed by scope, generation, referenced identity and referencing identity. Maintain edge membership
revisions atomically with all captured server writes. Use indexed keyset pages; never scan every
registered child table per root. Snapshot build rollback releases partial rows/permits. Capacity,
deadline and cancellation return distinct retryable/incomplete or intrinsic-limit results; none
authorizes policy fallback. Readiness validates the internal index; it does not narrow user schema
admission. Server admission reserves a finite retained-session capacity, default 8 sessions per
source, 64 per scope and 1,024 per service database, shared across staging/sealed sessions. Each
reserves its configured maximum entry/byte budget before accepting roots. Busy results preserve
existing sessions; explicit close or TTL cleanup releases the reservation. Thus inactive sessions
cannot grow retained storage without bound merely because build concurrency is limited.

## 3. Targeted reconciliation and policies

### 3.1 Read-only `POST /sync/reconcile`

Use existing authentication, source binding, generation proof, key/payload encoding, and compression
rules. Read-only means application-data read-only: bounded ephemeral control/session rows may be
written. Do not consume a source-bundle sequence, mutate business records or advance a checkpoint.
Replace scope-sequence restart-on-every-page with an immutable materialized targeted session. Reuse
the existing snapshot session lifecycle/capacity techniques, but materialize only the requested
closure, not the full scope. The contract consists of:

| Request | Meaning |
| --- | --- |
| Session action and client operation ID | Create, append root page, seal/build, read page or close; idempotent retry of each exact action. |
| Bounded root pages and closure selection | Desired/local-base dependency seeds; per-root outgoing-parent and affected incoming-descendant selection. |
| Optional minimum scope sequence | Lower bound for a new build, needed by historical repair; not a continuation freshness check. |
| Session ID, root-page ordinal/hash and cursor as applicable | Bind ordered root uploads, final root digest and immutable result pages. |

| Response | Meaning |
| --- | --- |
| Session identity, expiry and observed scope bundle sequence | One fixed coherent authoritative view for all sealed pages. |
| Authoritative record results | Explicit live/deleted/absent results, with applicable versions and payloads. |
| Complete selected closure and dependency read-set token | Outgoing ancestors and selected incoming descendants, plus row/absence and incoming-membership revisions. |
| Optional continuation cursor | More results remain for this request. |

Absent must not conceal authorization failure, source mismatch, or generation mismatch. Deleted
records retain authoritative tombstone semantics; missing data is not a fabricated tombstone.
Clients request identities of relevant local descendants explicitly even when those descendants
no longer exist among server-visible incoming dependents.

Bound each uploaded root page by `max_push_rows`, pages by snapshot chunk row/decoded-byte limits, and individual
rows by `max_row_decoded_bytes`. Request-body enforcement and exact JSON/schema/error shapes are
contract-settlement deliverables in section 5, before any runtime consumer. Continuation cursors
bind authenticated scope, source, generation, session ID, sealed root digest, traversal mode,
page position and observed sequence;
reject tampering or reuse in a different context.

Discover and stage required local roots under membership/revision guards before sealing; roots
larger than one request upload in idempotent ordered pages. The server seals only the complete
root digest, then captures the complete selected transitive closure and its read set in one bounded
consistent build transaction. Resolve newly found parents/incoming descendants during that build,
not through later live frontier reads. Preserve the database visibility/bundle-sequence fence used
by ordinary snapshots so rows and observation sequence refer to the same committed view. Build
success atomically publishes immutable rows and token; rollback publishes no partial view. Retrying
an acknowledged or ambiguous seal must return the same completed session, not silently build a
different observation. Unsealed sessions may not expose authoritative pages.

Subsequent pages read only sealed rows. Unrelated or related later business writes do not invalidate
these pages. Each read is independently authorized and source/generation-bound. Use the existing
snapshot TTL (currently default 15 minutes), expiring from completed build for sealed sessions and
creation for unsealed roots; do not renew indefinitely on reads. Expiry or explicit close releases
server storage. Client durable stages survive calls; finish downloaded complete views without a
live session, while an expired incomplete view restarts in a new session within budgets. New local
roots after sealing require a new guarded plan/session, not mixing live rows into the old view.

The dependency read set records versions/absence observations and incoming-edge membership
revisions for the selected closure. Candidate pushes bind their exact request and read-set token.
This is FK dependency evidence only, not a claim to cover arbitrary application-validator reads.
The server checks those dependencies under its mutation transaction/fence before applying changes.
A changed read set yields `reconcile_dependencies_changed`, causing refresh and a new candidate
attempt, never fallback. Unrelated scope changes alone must not reject a candidate or restart a
view. Keep ordinary canonical row hashes unchanged: freeze and replay the additional guard fields
as part of the exact request identity, including durable server replay comparison.

Deduplicate every frontier and enforce section 2.5 plus snapshot row/logical-byte totals. A stationary
affected group must complete despite unrelated writes between every request, provided finite work
fits configured capacity, the caller continues, and pages complete within TTL. Repeated relevant
changes, expiration, capacity, or unavailable server resources may return typed incomplete progress;
they do not grant an unconditional convergence guarantee. Checkpoint and source/generation recovery
retain precedence. Targeted installation never advances the global checkpoint; section 2.3.1 governs
historical mismatches encountered when ordinary pull resumes.

### 3.2 Durable resolution state machine

Persist `captured -> reconciling -> planned -> applying/retrying -> completed`, together with
source/generation/request identity, captured intent, discovered view, revision guards, policy,
progress, and report identities sufficient for restart. Failure preserves the durable phase and
pending work. Do not infer completion from missing transient memory.

Before policy evaluation settle ambiguous outbox work, fetch the coherent closure required by the
conflict and prospective SQL effects, and preserve desired local state separately from server rows.

`ServerWins` is the default and resolves competing changes at persisted-column granularity,
with authoritative row existence taking precedence. Use the precise rules in section 3.2.1, not
an implementer-selected interpretation of "compatible". An authoritative parent deletion may
discard an edited child when retaining it would contradict those rules. Report every lost pending
field/existence intent in the same local transaction. Ordinary pull/settlement must not invoke this
permission to bypass their local-work gates.

`ClientPreferred` is opt-in. Construct candidates from available complete local desired values and
authoritative dependencies; restore required ancestors only when permitted by the server. Never
invent required application fields, select an arbitrary parent, or bypass ordinary server
validation. Send candidates through ordinary bounded push groups; refresh/replan stale observations.

Fallback to the current authoritative affected group is allowed only after a definitive eligible
rejection or a positively established inability to construct the required valid local result from
available application values. Incomplete discovery, corrupt metadata, missing protocol evidence,
and capacity exhaustion are errors, not absence-of-values evidence. If earlier candidate bundles
committed, fallback uses the server state containing those commits and does not claim to undo them.

### 3.2.1 Membership, merge granularity, and deterministic dispositions

The merge table below defines ordinary ServerWins selection. ClientPreferred instead attempts its
captured local desired candidate. On an eligible ClientPreferred fallback, take S for the remaining
intents in the failed candidate's disposition set and report them; do not feed the rejected local
values back through the ServerWins preservation table and recreate the same rejected candidate.
Keep completed prefix effects and pending intent outside that set. Any expansion of the fallback
set requires the same explicit membership, revision, evidence and reservation rules.

Keep two explicit sets. The evidence set E contains every fetched/staged record, ancestor and
incoming dependent needed to prove the candidate. The disposition set D contains the conflicting
root intent and only pending local intents whose own state or FK validity would otherwise be
changed by applying that root result. Expand D to a fixed point over actual record effects, not
table connectivity. E membership alone never authorizes discarding or rewriting pending intent.
Clean E-only records may be hydrated authoritatively; dirty E-only records keep their desired
values/capture and participate in revision guards. Include incoming members in E where a proposed
delete/rekey/action could affect them. If the result would change a dirty E-only record, classify
it into D explicitly and reserve/report its disposition before proceeding.

For each D record compare typed complete base B, captured local L and coherent server S. Equality
uses canonical typed column equality; JSON-valued columns are atomic values, not nested merges.
Keys identify records and are not merged as editable fields: key changes retain delete/insert and
dependent-operation semantics. Generated/non-writable columns follow the schema's authoritative
rules and are never synthesized from a dirty mask.

For a record present in B, L and S, select each writable column as follows:

| Observation | Selected value and pending intent |
| --- | --- |
| L equals B | Take S; there was no local edit to this column. |
| L differs from B, S equals B | Keep L as pending local intent. |
| L and S differ from B but equal one another | Take S; the local intent is already satisfied, not discarded. |
| L and S both differ from B and each other | Take S; record the local column intent as discarded. |

Apply the same comparison to each admitted FK column, then validate the entire chosen graph. If a
kept local FK value requires a parent forbidden by ServerWins, replace that FK with S's value only
when S supplies a valid authoritative reference/null. Report the lost local FK intent; preserve
other compatible scalar edits. Do not set NULL solely because a column is nullable, invent a
parent, or resurrect an authoritative deletion. Iterate affected records under the same budgets;
an authoritative closed graph that still cannot be installed is an integrity error, not permission
to discard additional arbitrary fields.

Existence rules override column merging:

- B existed and S is deleted/absent: take authoritative absence. A desired local row is discarded
  and reported; a local delete is already satisfied. No row resurrection under ServerWins.
- B and S exist, L is deleted: retain the local delete only if S equals B and its effects do not
  delete/change an E-only authoritative record. Otherwise restore S and report the local deletion
  as discarded. Pending deletes/reparents already in D may jointly make the retained delete valid.
- B is absent and S exists: this is an identity collision; take S as a whole row and report any
  differing local insertion as discarded. An identical insertion is already satisfied.
- B and S are absent and L exists: preserve the never-published insertion if the chosen graph can
  support its exact values. If required ancestors are authoritatively unavailable and no valid
  local pending ancestor insertion exists, discard/report that insertion; do not synthesize a
  partial row or choose NULL without authoritative field evidence.

After merging M, atomically install M, update authoritative baseline to S, and retain dirty intent
exactly when M differs from S (or retains a valid local deletion/insertion). A subsequent ordinary
push publishes it under normal validation. Capacity/auth/transient failure never changes M to S
silently. A definitive rejection of a preserved merged candidate keeps its intent pending for
explicit resolution unless ClientPreferred's selected fallback rules apply; ServerWins must not
invent additional destructive domain-specific field repairs.

Journal record dispositions are deterministic: `satisfied` if all local intent already equals S;
otherwise `retained` if no intent is discarded, `merged` if some is retained/already satisfied and
some discarded, and `discarded` if every edit is discarded. Include sorted retained, already
satisfied and discarded column names or the existence marker,
not old payload values. Later confirmed pushes add `committed` segments without rewriting earlier
loss records. Clean evidence-only members have no loss disposition; optional diagnostic membership
must remain paged and charged.

Required example: B=(parent=P,name=old), L=(P,local), S=(NULL,old) after server SET NULL becomes
M=(NULL,local), baseline S, dirty name=local, disposition retained. If L.parent=Q and S.parent=R both
differ from B.parent=P, select R, discard/report the local parent edit and keep a noncompeting local
name edit (merged). Merely fetching Q or R does not authorize replacing their unrelated dirty rows.

### 3.3 Definitive rejection evidence

Retain the existing server application-rejection classification and separately classify automatic
fallback eligibility under section 3.3.1. A safe public application category is not dependency
coverage evidence. Do not interpret arbitrary HTTP status codes or parse human-readable messages
to decide that local intent should be discarded.
An eligible rejection identifies the exact frozen request (source, source-bundle identity,
canonical candidate hash and frozen dependency guard), certifies confirmed transaction rollback,
provides a safe machine-readable core-FK category, and identifies both the validation
scope sequence and validated session/read-set token. Preserve the existing protection against
classifying commit-unknown or rollback-unconfirmed outcomes as ordinary application rejection.

A changed dependency read set requires reconciliation and a new attempt of the refreshed candidate
before any fallback. Unrelated scope-sequence changes with a still-valid read set do not invalidate
the evidence. A rejection for one candidate/view may never justify discarding a different local
revision or refreshed candidate; bind fallback to the validated failed candidate and then install
the authoritative fallback closure through a fresh coherent session/revision gate. Changes to
affected dependencies in that fresh view require a new candidate attempt, not reuse of the old
rejection. Authentication/authorization failures, malformed protocol data, unknown commit,
timeouts, transient storage errors, and local capacity failures are ineligible fallback signals.
Do not expose raw SQL or internal constraint details. Exact public categories and error precedence
must be recorded in OpenAPI and paired fixtures at the contract-settlement gate.

Replay lookup precedes fresh dependency/session validation. An already committed exact request
returns its durable receipt even if its session expired or later dependencies changed. For a
not-yet-committed request whose guard expired, return a definitive non-commit result under the
existing source-sequence fence; retain local intent and refresh/replan through the ordinary proven
non-commit path. Never substitute a new guard into an ambiguous frozen request. After a confirmed
candidate prefix changes its own read set, obtain a new guarded view for the next candidate;
do not treat those self-caused changes as rejection evidence or silently reuse a stale token.

### 3.3.1 Closed eligibility boundary for application-validation dependencies

This version does not instrument application validators, accept application-declared dependency
coverage, or infer their reads from SQL text, FK topology, function volatility or a public category.
Choose fail-closed eligibility instead of a new application dependency-capture extension surface.
The existing `ApplicationWriteRejection{SQLState, Category}` declaration remains supported for safe
error classification; it never grants automatic fallback authority. No new developer annotation or
callback can opt an opaque validator into automatic fallback under p0001.

This restriction also applies to custom merges configured for automatic fallback. Do not relabel
an opaque validation rejection as inability to construct local intent: that separate fallback
requires actual missing required application values/ancestors established by complete evidence,
not a validator's refusal of an otherwise complete candidate.

Only a rejection produced by the core-owned, pre-application FK candidate validator may carry
automatic-fallback evidence. That validator must reason solely from the immutable admitted
generation contract, the exact frozen candidate and a complete guarded FK read set, before running
application DML/triggers/validators. Every consulted parent presence/absence and incoming-membership
predicate, including references introduced by the desired candidate, must be represented by the
guard. A missing dependency yields `reconcile_dependencies_changed`/refresh, not an eligible
rejection. Generation/authority change retains its existing error precedence. A row-local or
deterministic-looking domain rule is not part of this core FK validator merely because it reads no
obvious external table.

Do not change accepted write semantics by rejecting before an application mechanism that could
normalize or repair the candidate. Enable rejecting pre-application validation only for a
deployment whose pinned readiness evidence declares no application-controlled write/validation
mechanisms; otherwise run the ordinary write path and keep its rejections ineligible. Unknown
mechanism coverage is also ineligible. Internal capture mechanisms are core-owned, not an
application opt-in. This restricts fallback evidence, not schema or application-mechanism support.

| Rejection origin | Automatic fallback |
| --- | --- |
| Core pre-application FK validator; all consulted rows/absence/membership predicates guarded; exact candidate bound; rollback confirmed | Eligible subject to section 3.3 freshness/retry requirements. |
| Deployment-declared application SQLSTATE/category, including a trigger reading U or an EXISTS/COUNT/absence predicate | Ineligible: `application_validation_dependencies_untracked`. |
| PostgreSQL constraint/DML failure, including FK/unique/CHECK or a trigger-raised familiar SQLSTATE | Ineligible unless it is the independently constructed core pre-application evidence above; SQLSTATE/constraint name alone never establishes origin or coverage. |
| External services, clock/time, application functions, session configuration or other opaque validation inputs | Ineligible: `application_validation_dependencies_untracked`. |
| Missing/unknown coverage or malformed evidence | Never eligible; return the typed coverage failure or protocol error as appropriate. |

Return an explicit evidence discriminator for core-owned FK validation, with the exact request,
generation and complete FK read-set binding plus confirmed rollback. Application errors have no
such evidence. A matching error-category string cannot promote an application error to core origin.
Freeze this discriminated error shape and negative fixtures before consumers; clients must require
all evidence, not trust a bare `automatic_fallback_allowed` boolean or infer it from category names.

For an otherwise valid classified rejection without coverage, preserve the captured local intent,
baseline, unresolved resolution and any committed prefix. Return public reason
`automatic_fallback_ineligible`, detail `application_validation_dependencies_untracked`, the safe
application category, exact rejected request identity and revision-bound pending-resolution handle.
Do not install an authoritative fallback, clear/rebase the rejected intent, emit a discarded-field
disposition or report successful resolution. Retain/release reservations under the existing proven
non-commit rules; never release an ambiguous request's reservation. This is a blocked resolution,
not a transport retry or a busy automatic retry loop.

The handle exposes two explicit actions: retry the preserved desired intent through normal bounded
planning/validation, or `AcceptServer` for this exact pending disposition set/revision. AcceptServer
means explicit whole-set authoritative acceptance: take S without re-running the field-preservation
table or merely switching the configured default policy. The latter is an application decision,
not automatic inference from the untracked rejection, and
still requires current coherent evidence, revision/membership guards and report reservation. An
ineligible automatic fallback is not terminal corruption: retry may succeed once validator state
changes. Authorization/cancellation/commit-unknown handling keeps its higher precedence.

Required C/U result: an application validator rejects C while unrelated-to-FK root U is locked.
Even if U later unlocks and C's FK read set is identical, the original rejection is ineligible.
C's local name remains pending, with no discarded-name report. Explicit retry runs the candidate
again and may commit; explicit AcceptServer may instead discard it with a report. The library must
not assert that U is irrelevant, or widen every FK token to scope-wide freshness to guess coverage.
The stationary-FK-closure liveness guarantee concerns core FK reconciliation; it does not promise
automatic resolution of arbitrary application-domain rejection.

### 3.4 Public clients and reports

Provide equivalent Kotlin and Go behavior: configurable `ConflictPolicy` with `ServerWins` default,
opt-in `ClientPreferred`, explicit deferred/custom resolution, revision-bound conflict handles,
resolution summaries, paged per-record outcomes, and explicit report acknowledgment. Replace the
misleading `ClientWins` convenience name without a compatibility alias; update Swift and examples.

Existing explicit merge decisions provide candidate intent to group-aware machinery. Rejected
custom merges remain pending unless the caller explicitly selected automatic fallback. Callbacks
must not require unbounded in-memory group context and must run outside database transactions.
Public resolution contracts must disclose bounded network reads/retries and candidate pushes;
remove the obsolete Go local-only guarantee.

Reports identify resolution, policy, completed outcome, fallback reason where applicable, and the
exact record dispositions in section 3.2.1 plus committed segments. Persist entries atomically
with their corresponding local transitions, retain them until explicit acknowledgment, and page
reads. They are not an undo archive and must not imply all multi-bundle work committed atomically.

### 3.4.1 Journal capacity, reservations, and lifecycle

Expose finite positive `max_resolution_report_entries` (default 10,000) and
`max_resolution_report_bytes` (default 16 MiB) per local database. Count published entries, active
headers, sealed segments and durable reserved slots/bytes together across all sources and scopes.
Bytes are canonical encoded identity, enum, column-name and summary metadata; reports contain no
business payload copies. Enforce both dimensions; paging does not waive retention capacity.

Before any candidate push or local transition requiring a report, durably reserve its bounded
worst-case report entries/bytes plus final summary capacity. Reserve for exact known D identities
and other pending candidate keys whose publication will be reported (without granting discard rights),
all possible disposition labels and column masks; no report text may grow from arbitrary server
messages. Couple reservation identity to the frozen request/plan/revision. New affected intent
discovered before apply requires additional reservation and revalidation before it can be discarded.
Extra server effects outside reserved D are preserved/reconciled under normal safety rules, not
silently added to the loss set. Every potentially remote-committed request keeps its reservation
until replay/settlement resolves it; restart and a full journal cannot revoke that capacity.

If reservation fails, return `resolution_report_capacity_exceeded` with dimension, used/reserved,
required/limit, resolution identity and confirmed committed progress. Do not send that candidate,
discard local intent or advance that transition. A committed prefix remains committed; its reserved
settlement and publication must finish even if no capacity remains for the next candidate. Keep
the remaining phase resumable. Caller acknowledgment or a larger explicit limit permits progress;
do not auto-delete, auto-ack, overwrite, or silently switch policy. Physical disk I/O failure still
uses durable outbox replay; a logical reservation is not a promise against storage-device failure.

Publish immutable report segments per atomic local transition, keyed by resolution/segment and
revision. Acknowledge a sealed segment explicitly and idempotently, even while its overall resolution
is incomplete, so an application can free committed-prefix capacity and resume. Never acknowledge
an unpublished reservation; keep the bounded active summary until completion/explicit acknowledgment.
Acknowledgment releases entries/bytes transactionally without clearing dirty/outbox/recovery state.
Reject lowering configured limits below retained/reserved use; keep read/ack available when full.

Store reports in database-lifetime diagnostic state outside attachment/source-reset cleanup.
Detach and source replacement preserve unacknowledged segments, original account/scope/source/
generation identity and reservations. Default listing uses the attached account/scope; detached or
retired-source read/ack requires explicit account/scope selection through the local diagnostic API,
not a live server request or a current source binding. Never expose another account's entries by
implicit fallback. No automatic cleanup may treat reports as reconstructible sync cache.

### 3.5 Invocation bounds and recovery

Preserve the convergent-sync 64-push-attempt allowance. Add at most 64 reconciliation exchanges per
public synchronization/resolution invocation, counting session actions, pages, retries and restarts
together. Share that allowance through wrappers, recursive conflict handling, recovery, and all
rounds of one `syncThenDetach` invocation; nested helpers cannot reset it. Check before scheduling
exchange 65. Existing cancellation/deadline precedence remains intact.

Exhaustion returns typed incomplete progress identifying the reconciliation allowance and durable
continuation state. Do not report convergence, detach, or discard intent. A later invocation resumes
or refreshes the saved phase. Stable no-progress detection must remain bounded and must distinguish
changed durable intent from mere count equality. Retain explicit pull/local-work gates and the
recovery-authorized drain path when recovery is first discovered during ordinary pull.

## 4. Requirement-to-validation mapping

All new results are **pending**, not inherited from supplied passing tests. Each scenario must
assert business rows and payloads, dirty state, baseline relationships, outbox/request identity,
source progress, applicable checkpoints, and server state, including the state after restart.

| ID | Required result | Owning evidence |
| --- | --- | --- |
| FK-01 | Server updates P, reparents C from P to Q, then deletes P; D remains under C. Both clients retain Q, C->Q, D->C despite normalized delete-first wire order. | Go server normalization/projection; Go and KMP apply/replay; paired live test. |
| FK-02 | Required surviving-child reparent lies beyond the initial dirty page. Discovery finds it; valid bounded pushes converge without a bad frozen request. | Go/KMP selection tests and one high-level-call live scenario. |
| FK-03 | Many independent deleted parent/child pairs drain separately despite collectively exceeding one batch. Truly oversized indivisible components fail before freeze. | Both clients; row/byte boundary and cyclic live tests. |
| FK-04 | Accepting C->Q with Q absent locally fetches the needed closure and succeeds without full-scope snapshot or global checkpoint advance. | Reconcile endpoint; both conflict clients; paired live and restart. |
| FK-05 | Remote parent deletion conflicts with a different locally edited child. ServerWins reports affected discarded intent and preserves unrelated dirty rows. ClientPreferred restores when valid and falls back only under eligible conditions. | Both policies, server rejection classification, concurrency barriers, reports, live parity. |
| FK-06 | Small settlements against large FK dirty backlogs keep bounded host materialization and avoid repeated complete-backlog payload copying. | Deterministic materialization/allocation counters, owning stress lanes, representative memory evidence. |
| LC-01 | Insert/attach with existing/new parents, self-reference, and admitted cycles; reparent to old/new parents and null where admitted. | Both client lifecycle suites and live server. |
| LC-02 | Key changes with dependents, admitted cascades, delete chains, each admitted action, clean/dirty descendants; unsupported schemas still fail admission. | Contract admission, both apply/selection suites, live topology. |
| BD-01 | Rows/bytes/entries/work immediately below/at/above aggregate limits; wide/deep graphs, SCCs, combined copies, duplicates, cancellation, resume, cursor mismatch and session capacity/expiry. | Server endpoint, both planners, contract corpus, memory/work counters and indexed query plans. |
| RP-01 | Lost response and exact replay; crash at every durable resolution phase; committed partial sequence followed by fallback. | Both restart/replay suites and HTTP-backed tests. |
| CW-01 | Racing edit, reparent, inserted dependent, parent edit; writers through a second connection; no transaction held over HTTP/callback. | Deterministic barriers, Go race instrumentation, affected platform/live lanes. |
| CW-02 | Repeated unrelated writes between every root/page request and across calls do not restart a stable affected view. Relevant membership/row changes refresh and retry the candidate, not trigger fallback. | PostgreSQL and paired live conflict tests. |
| ER-01 | Domain/constraint rejection versus auth failure, malformed proof, rollback-unconfirmed and commit-unknown. | Server transaction/HTTP precedence and both client fallback tests. |
| ER-02 | Application validator reads non-FK U or an absence/range predicate; category alone never grants fallback. Core FK evidence covers every consulted dependency and cannot be spoofed by SQLSTATE/category. | Server classification/prevalidation and Go/KMP policy tests, live C/U race and explicit retry/AcceptServer. |
| RC-01 | Targeted state ahead of checkpoint survives older pull, including an unseen historical child repaired with explicit newer evidence; snapshot hydration, recovery first discovered in pull and one-call multi-batch convergence. | Both coordinator/recovery suites, paired live tests. |
| API-01 | Exact column/existence merge outcomes, evidence-only members, opt-ins/merge/defer, handles, Swift reconciliation cancellation, journal reservations/exhaustion/partial-prefix ack, detach/source replacement. | Public API tests and platform/Swift lanes. |
| PF-01 | Incompatible old layout fails before any business or sync mutation; new layout resumes all durable phases. | Both opening/persistence suites and platform drivers. |

For FK-01 retain a clean grandchild assertion: FK validity alone misses its loss. For FK-02 put the
reparent beyond the actual initial hard-limit lookahead, not just beyond the soft upload target.
For FK-03 include parents without outgoing FKs and children with newly captured delete payloads,
not only legacy data. For FK-06 assert materialized payload counts as well as representative RSS;
there is no current runtime memory measurement for the reviewed defect.

The recent typed dirty-capture, cyclic-delete grouping, and detach-write-reservation fixes remain
regression inputs. Their passing tests are not broader topology, policy, or qualification evidence.

### 4.1 Blocking review disposition and exact new acceptance cases

The first revision addressed all five findings. Its independent rereview resolved R1, R2, R4 and
R5 at specification level and partially resolved R3, finding the application-dependency gap in
section 4.2. Preserve those review dispositions; this second revision has not been rereviewed.

| Review finding | Normative revision | Required distinguishing evidence |
| --- | --- | --- |
| R1 / P1 historical unseen child blocks pull | Section 2.3.1: per-identity observation floors and bounded historical dependency repair. | Start at 10; unseen X->P in 11; P/X deleted in 12; resolve P at 12; pull 11 via explicit X evidence, then 12. Assert hashes, per-bundle checkpoints, no X resurrection and no full snapshot. Include later X reparent to Q, absent/tombstone results, multi-level mismatch, cancellation/restart, resource exhaustion and a local write at repair apply. |
| R2 / P1 undefined compatible intent | Sections 2.1 and 3.2.1: complete B/L/S, column/existence rules and E/D membership. | SET NULL + local name retains name with S baseline and dirty name; competing reparents choose S FK while keeping a noncompeting local name; same-value edits are satisfied; evidence-only dirty parents remain unchanged; missing B errors without discarding; row deletion and insert collision have exact dispositions. |
| R3 / P2 unrelated-write starvation | Section 3.1: sealed targeted materialization, paged seeds/results and dependency-specific candidate guards. | Write unrelated U between every request, including across 64-exchange boundaries; stationary C/Q completes without new sessions. Relevant new incoming membership invalidates guarded candidates. Include root-upload replay, ambiguous seal, expiry, session capacity and committed replay after expiry. |
| R4 / P2 total work/staging undefined | Section 2.5: shared entries/bytes/work, indexed incoming lookup, server session caps/deadlines. | Dirty chain/cycle and survivor copies at each limit boundary; combined concurrent stages count together; repeated replans consume work; cancellation and durable resume; intrinsic atomic unit returns capacity instead of endless retries; no repeated full child-table scans; server build timeout rolls back partial materialization. |
| R5 / P2 report retention unbounded | Section 3.4.1: finite quotas, pre-action reservation, sealed-segment acknowledgment and database-lifetime retention. | Repeat conflicts without ack until deterministic backpressure; no next candidate is sent. Crash/replay reserved candidate while full; settle committed prefix, ack its segment and resume; capacity after local race; detach/source replacement retains reports; detached explicit-scope read/ack; no automatic loss or wrong-account listing. |

Add Swift caller-cancellation cases during root upload, result download, historical repair and
explicit network resolution; the existing second-push cancellation case is not equivalent.
Beyond-page grouping must preserve a surviving child reparent, not substitute a child deletion.
All original FK-01..06, topology, supported-platform and live-server obligations remain pending.
These are new assertions to implement later; specification authoring has executed none of them.

### 4.2 Second review disposition and remaining-risk assertions

The remaining P1 fallback-dependency finding is **addressed in draft, pending independent rereview**.
Section 3.3.1 closes eligibility to proven core pre-application FK validation. Application/domain
rejections remain supported classified errors but cannot trigger automatic data-discard fallback.
No application dependency-registration mechanism is added in this scope.

Required ER-02 cases include a trigger reading root U, EXISTS/COUNT/range/absence predicates,
U changing after rejection, unknown/missing coverage, a trigger raising a familiar constraint
SQLSTATE or core-looking public category, and a genuinely eligible core FK rejection.
Also cover an admitted application trigger that repairs/normalizes an initially invalid FK: preserve
the ordinary accepted write behavior rather than introducing a premature core rejection. Assert exact
business values, B/L/S, dirty capture, pending handle/revision, outbox/non-commit evidence, reserved
report capacity and no discarded-field report on ineligibility. Retry after unlocking U must attempt
the preserved candidate anew; explicit AcceptServer must report the separately authorized loss.
Retain the unrelated-U-between-pages liveness test: do not replace dependency guards with global
scope-sequence invalidation. Include relevant FK membership changes requiring a new candidate.

Keep these precise remaining-risk assertions in qualification, without claiming runtime fixes:

- Merge failures: valid B/L/S producing a local CHECK/unique failure rolls back the apply and leaves
  original business values, baseline and dirty intent unchanged, with no completed/loss report.
  A subsequent opaque server-domain rejection follows section 3.3.1, not destructive field repair.
- Shared-workspace pressure: retained observation floors plus replacement plans and pending
  settlement must either release obsolete staging and resume or return a stable actionable capacity
  result. Never retain unusable duplicate plans and repeatedly report nominal progress.
- Historical repair: X reparented to Q, tombstone versus absence, multiple dependency levels,
  interrupted repair and a second-connection write at the final gate retain exact state assertions.
- Full journal: lose a reserved candidate response, restart, settle its prefix, acknowledge one
  segment and resume; repeat through source replacement and explicit detached-account access.
- Clean survivors, FK-01..06, surviving-child reparent beyond the initial page, and Swift cancellation
  during reconciliation/repair/explicit resolution retain their existing platform/live obligations.

The previous review's remaining risks are not newly claimed implementation defects. All runtime
evidence remains pending, and unchanged OpenAPI/corpus bytes remain intentional at this gate.

## 5. Contract impact, qualification, and implementation handoff

### 5.1 Contract settlement for the active slice

This amendment leaves the active OpenAPI and corpus bytes unchanged. They describe the current
implementation. Before each slice, identify its changed persistence, public and wire surfaces and
settle those contracts, fixtures/classifications, manifest and paired pins before their consumers.
Record a no-impact finding for unchanged surfaces. Stage A does not need Stage B's endpoint, policy
or report schemas settled in advance. This replaces revision 2's whole-program upfront settlement;
it does not permit implementing a changed contract before settling it.

The checklist below assigns each contract item to its first consumer in section 5.3. Go owns server
wire behavior; Go and Kotlin own their local/public contracts and shared fixture consumers. An item
with several owners closes only after all listed portions have evidence. Track its per-phase portions
in the ledger; future portions remain pending. A no-impact finding closes only the named phase's
portion, not the whole program item. Do not settle unrelated later surfaces to finish this list early.

- [ ] **C01 — B1a sessions; B1d guarded installation/repair.** Add reconcile session actions,
  exact request/response schemas, root/body limits, seed sealing,
  immutable result paging, expiry/capacity/close, authorization/source/generation precedence, cursor
  binding, dependency read-set guards and historical-repair observation floors/tombstone rules
  to `swagger/two_way_sync.yaml`; retain existing v1 encoding/compression and hash domains.
- [ ] **C02 — B1d guarded request/replay/non-commit fields; B3 eligible-rejection fields.** Define
  exact request/read-set-bound rollback-confirmed rejection fields and public eligible
  categories; preserve replay-before-expired-guard and commit-unknown precedence. Freeze safe
  existing application-error classification and the refreshed-candidate-before-fallback rule.
- [ ] **C03 — B1d ineligible application-rejection results and explicit actions; B3 core eligibility.**
  Encode the closed core-FK evidence discriminator and complete consulted-dependency binding;
  application declarations never imply coverage. Add `automatic_fallback_ineligible` plus safe
  detail/category/handle and explicit retry/AcceptServer semantics in both clients. Do not add an
  application coverage opt-in, infer eligibility from SQLSTATE, or leave the eligible family open.
- [ ] **C04 — A1 apply/staging bounds; A2 baseline/planner; A3 aggregate/pending results;
  B1b capture/download; B1c reports; B1d resolution handles; B2 merge/default; B3 ClientPreferred.**
  Record matching Go/Kotlin public types for B/L/S policies, E/D membership, shared resource
  limits/errors, incomplete progress, journal reservation, paged sealed reports and acknowledgment;
  align explicit merge, callback, network, detached diagnostic access, cancellation and Swift contracts.
- [ ] **C05 — each phase owns fixtures for its C01..C04 portions.** Add shared fixtures for stable
  sessions, historical repair, absence/deletion/paging, malformed/
  cross-context cursor, changed dependencies, all aggregate limits, rejection eligibility, exact
  column/existence merges, report reservations/ack/lifecycle and persisted format.
  Classify fixture families explicitly; update the sorted corpus manifest and pinned hashes together.
- [ ] **C06 — paired consumers in the same phase as each C05 change.** Update every claimed fixture
  consumer to execute its families and fail closed on absent root or
  manifest mismatch. No silent fallback corpus root or unclassified legacy payloads.
- [ ] **C07 — A1 baseline evidence; each later changed request path; B3 final confirmation.** Preserve
  ordinary no-change and one-row request-count criteria; reconciliation exchanges are
  exceptional conflict/dependency/recovery work, not an extra request on every admitted sync.
- [ ] **C08 — entry to A1, A2, A3, B1a, B1b, B1c, B1d, B2 and B3.** Review and freeze exact
  OpenAPI/specification/corpus identities for that phase's settled surfaces before implementing consumers.

### 5.2 Qualification profile and ledger

This changes wire behavior, persistence, default policy, and safety/boundedness contracts. It is an
initial-protocol qualification profile, not routine maintenance. The complete matrix below governs
full p0001 qualification/release, not every development slice. Section 5.2.1 governs Stage A's scoped
candidate. Specification review must confirm both profiles. Run focused regressions during development
and the fast paired-contract gate before expensive database suites. Freeze production changes before
the final candidate campaign. All commands below are future obligations; none ran for this draft.

The ledger records section/requirement, exact command/selector, toolchain, platform, database image
and durability, deadlines, start/end, exit status, nonzero matching tests, raw evidence, candidate
identity and fresh/reused/subsumed/not-applicable/blocked disposition. Retain failed/superseded and
rerun-required entries. No cache hit, old pass, or successful simulator drain loop substitutes for
fresh execution of the required transition. Database-backed families have independent budgets and
must not run concurrently against shared resources without proved isolation. Diagnose a timeout
before retrying; do not repeat the same aggregate just to seek a pass.

Mandatory Go lane authority is p0000 section 17, retained here for shared server/client/wire changes:

- Go 1.25.12; focused owning cases with `-count=1`, and focused `-race -count=1` for changed concurrency.
- `scripts/test-lane.sh package`, `scripts/test-lane.sh audit`, `scripts/test-lane.sh full`, and
  `scripts/test-lane.sh mobile`; root `./...` does not cover the nested mobile-flow module.
- `scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 go vet ./...`.
- Owning PostgreSQL 16 and 17.10 coverage, naming exact images and tests.
- Every corpus-consuming Go lane sets
  `OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts`.
- Existing p0000 request-count, wire-byte, legacy-absence, layout and formatting obligations remain
  subject to the ledger; this new profile does not waive the mandatory final aggregate.

Mandatory paired/KMP authority is the Kotlin p0000 companion section 11 and the validation runbook:

- Focused common/JVM regressions and paired contract/HTTP boundary checks first.
- `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance`.
- `./gradlew oversqliteComprehensive --rerun-tasks`.
- `./gradlew oversqlitePlatformAll --rerun-tasks` and `./gradlew swiftHybridSupportGate --rerun-tasks`.
- `./gradlew oversqliteRealserverAll --rerun-tasks`,
  `./gradlew swiftHybridSupportRealserverGate --rerun-tasks`, and
  `./gradlew oversqliteRealserverAllHeavy --rerun-tasks` against a pinned fresh Go server/database.
- Retain affected Android, JVM, iOS simulator, macOS, JS Node/browser and Wasm browser evidence.
  Host results do not replace platform/live evidence. Generator/compiler/plugin work is not planned;
  if it becomes necessary, add its owning validation, separate compiler/plugin runs and generated
  output verification before changing it.

After a change, refreeze identity and invalidate affected evidence plus mandatory aggregates under
the governing rules. A documentation-only specification candidate needs document/link/identity
inspection, not runtime tests; this does not assert its new behavior is implemented or qualified.

### 5.2.1 Development stops and Stage A qualification

Development slice review uses the named regression and adjacent invariants, followed by a narrow
paired live scenario. Record the exact public call and state assertions; helper-only tests cannot
close an end-to-end slice. Run changed contract consumers first. Record later qualification lanes
as pending; a focused pass does not qualify the product or replace platform/Swift/live evidence.

Stage A has its own scoped candidate profile because it changes persistence and safety contracts.
Before its implementation, record the selected commands, nonzero test selectors, deadlines and
requirement ownership in a small ledger. At its stable candidate, require:

- Affected Go owning packages with Go 1.25.12 and uncached focused race instrumentation for changed
  concurrency; affected server/database families on PostgreSQL 16 and 17.10 when those paths change.
- Paired corpus consumers and public-boundary cases for Stage A's changed persistence/errors.
- Common/JVM tests and affected scenarios on Android, JVM, iOS simulator, macOS, JS Node/browser
  and Wasm browser; live Go and KMP coverage for FK-01..03 and affected lifecycle/recovery paths.
- A public one-call multi-batch drain, exact replay/restart, second-connection races, cancellation,
  format rejection, and FK-06/BD-01 materialization, resource-bound and representative memory evidence.
- Swift caller-owned cancellation on affected operations and the existing live cancellation
  regression when shared operation/recovery code changes. Later reconciliation/repair/explicit
  network-resolution cancellation remains pending for Stage B and cannot reuse second-push proof.

Use the validation runbook's dependency-based fresh/reused/subsumed/not-applicable/blocked rules.
Name any independently mandatory aggregate by section, command and trigger. The full-program final
aggregates in section 5.2 are not Stage A development or scoped-candidate triggers. Run that full
matrix after both stages are complete and stable. No intermediate review or scoped acceptance may
claim full p0001 completion, authorize release, or retire a pending Stage B requirement.

### 5.2.2 Validation execution rules

Use these rules throughout A1..B3. They control scheduling and evidence reuse; they do not reduce
the assertions, supported runtimes or final qualification obligations in sections 4 and 5.2.

- Before a broad package, full JVM, platform, live-server or cross-repository run, record its
  checkpoint, exact command/selector and the changed dependency coverage or unresolved failure
  that requires it. For an independently mandatory aggregate, name its section and trigger.
  A small edit alone is not a trigger. Record this decision in the existing ledger; it does not
  require a new owner approval for each test command.
- During development, run the focused regression and adjacent invariants for one coherent change.
  Group related selectors into one invocation per compatible package/runtime/fixture where practical.
  Keep separate lanes where instrumentation, isolation or mandatory commands require it. Batching
  must retain per-test outcomes, cancellation, cleanup and the prescribed family deadlines; it
  does not justify concurrent execution against shared database or build resources.
- Reuse unchanged compiled/generated outputs and prepared test binaries when their dependency and
  environment identities match. Rebuild invalidated inputs, then execute affected tests afresh.
  Do not default to cleaning or forcing the entire build graph to rerun during development.
  Cached/skipped/zero-match tests are not fresh evidence. Preserve section 5.2's exact mandatory
  commands, including their rerun flags, at full qualification.
- After a failure, isolate the failing case and cause before another broad run. Rerun that case
  and coverage invalidated by the correction, plus any independently mandatory final aggregate.
  An environment retry needs a recorded cause and changed conditions. Retain failed attempts and
  do not repeat an unchanged broad suite merely to seek a pass or extend its prescribed deadline.
- Record preparation/build, execution and cleanup time separately where available, along with
  actual test counts, skips and outcomes. Mark unavailable timing as unknown. Invocation counts
  are not pass counts; overlapping durations cannot be summed into wall-clock time. Keep unaffected
  evidence reused/subsumed only with dependency justification and invalidated broad lanes pending.
  Use the existing ledger/reporting tools; this amendment does not introduce a test scheduler or
  require new validation infrastructure. Report accumulated validation time and remaining work at
  the existing 45–60 minute overrun checkpoint.

### 5.3 Slice order and actionable checklist

The current task stops at **SPECIFICATION READY FOR REVIEW**. Obtain independent specification review
and owner agreement on revision 3 before runtime work. The prior authorization for in-scope work in
both repositories remains; agreement on this amendment does not imply delivery authorization.

Dependency order: **A1 -> A2 -> A3 -> B1a -> B1b -> B1c -> B1d -> B2 -> B3**.
B1a–B1c are component review checkpoints inside B1; B1 becomes a complete targeted-resolution
behavior only at B1d. They do not authorize targeted business-state installation, policy changes or
deployment. Keep them limited to the dependencies of the explicit B1d scenario.
Component review may use production components through focused harnesses; do not introduce temporary
public APIs, feature flags or throwaway infrastructure just to demonstrate an intermediate stop.

Use these stable IDs in the existing implementation/validation ledger. All boxes start pending.
For each active item record affected files/consumers, exact public behavior, owning test selectors,
deadlines and section 4 assertions. Close it with implementation and evidence links, not a prose
claim or an invocation count. Track Go and Kotlin portions separately; one client's pass cannot close
the other's portion. Record completion in the ledger without continually editing frozen authority.
Each phase's first item settles its assigned C01..C08 portions before consumers; future portions stay
pending. The final item is its review stop, not permission to begin the next phase automatically.
Each phase's entry also records S01/S02 before coding; its exit closes the applicable S01..S05
portions with both repositories' replacement and complexity evidence. Record still-required code
and its later owner explicitly; do not treat deferred removal as completed simplification.

Reuse existing apply transactions, outbox/replay and snapshot facilities where they satisfy the
contract. Choose internal APIs and test ownership from current code. Do not build a general framework
or implement later surfaces in anticipation of future slices. Safety, bounds, cancellation and
restart apply from a path's first use; the ownership below does not postpone an earlier prerequisite.
If a supposedly later contract is needed earlier, settle that portion first and record the dependency.
A phase cannot close with an introduced path violating sections 2–4, subject only to section 1.3's
express Stage A boundaries.

#### 5.3.1 A1 — Preserve survivors in ordinary application

Prerequisite: reviewed amendment and owner agreement. Owners: Go and Kotlin clients; Go server owns
the normalized-effects witness. Complete behavior: FK-01 through public pull and settlement/replay.

- [ ] **A1-01** Settle C04's apply/staging bounds and C05..C08 for this slice; record ordinary no-change
  and one-row request-count baselines and focused FK-01 selectors before changing consumers.
- [ ] **A1-02** Establish the real server effect sequence: update P, reparent C to Q, delete P, with
  clean D under C. Implement survivor-safe final-state application in both clients' pull and committed
  settlement/replay paths; validate original wire history independently of SQL order.
- [ ] **A1-03** Preserve clean/dirty and version-skipped survivors, payloads, local captures and atomic
  metadata/checkpoint transitions. Bound new staging and retain FK enforcement; keep the pre-existing
  complete-backlog preservation deficit explicitly pending for A3 under section 1.3.
- [ ] **A1-04** Run focused Go/common-JVM FK-01, survivor, rollback and lost-response/reopen cases,
  asserting section 4's full state vector; then prove the public-call behavior against the Go server.
- [ ] **A1-05** Present paired diff, focused and narrow live evidence, request counts and remaining
  platform/resource obligations. Stop for owner review before baseline/planner expansion.

#### 5.3.2 A2 — Select valid bounded pushes

Prerequisite: reviewed A1. Owners: Go and Kotlin clients and paired contract consumers.
Complete behavior: FK-02/FK-03, including a surviving reparent beyond actual hard-limit lookahead.

- [x] **A2-01** Settle C04's persisted baseline/full-B layout, planner limits/errors and C05..C08.
  Name reachable lifecycle owners and incompatible-layout rejection checks before implementing them.
- [ ] **A2-02** Maintain indexed authoritative relationships and complete typed B across attach,
  publication, pull, settlement/replay, requeue, no-op pruning, snapshots/recovery and reached conflict
  paths. Distinguish known-empty from unknown; local edits cannot overwrite server-visible baselines.
- [ ] **A2-03** Discover directed prerequisites beyond the initial dirty page using bounded durable
  traversal. Keep SCCs indivisible and independent groups separate; enforce row/byte limits before
  freezing requests and return bounded oversized-component diagnostics.
- [ ] **A2-04** Run focused surviving-reparent, independent-pair, insert/attach, self-reference/cycle,
  key-change, row/byte overflow and baseline restart cases. Assert no bad frozen request or consumed
  request identity on preflight failure, and no mutation on incompatible-format rejection.
- [ ] **A2-05** Prove a public one-call multi-batch drain live in both clients; present baseline
  lifecycle coverage and focused evidence. Stop before expanding backlog preservation.

#### 5.3.3 A3 — Close ordinary correctness

Prerequisite: reviewed A2. Owners: Go and Kotlin clients, affected platforms and Swift consumers.
Complete behavior: bounded ordinary synchronization with safe pending outcomes where Stage B is needed.

- [ ] **A3-01** Settle C04's aggregate workspace and `fk_reconciliation_required` public results plus
  C05..C08. Assign Stage A assertions from LC-01/02, BD-01, RP-01, CW-01, RC-01 and PF-01 in the ledger.
- [ ] **A3-02** Complete FK-06 with paged preservation and aggregate entry/byte/work accounting across
  all Stage A working state. Avoid repeated whole-backlog payload copying; implement safe
  exhaustion, useful continuation, cleanup and restart without nominal-progress loops.
- [ ] **A3-03** Extend shared safe application through affected snapshot/recovery and existing conflict
  paths. Return section 1.3's pending result when required evidence/repair is unavailable, preserving
  intent, replay state and checkpoint. Complete the affected supported-topology lifecycle assertions.
- [ ] **A3-04** Run focused small-settlement/large-backlog counters, boundary limits, second-connection
  races, cancellation/resume, replay, recovery-first-found-in-pull and format cases. Include protected
  clean/dirty payloads and representative memory evidence; retain exact assertion-level dispositions.
- [ ] **A3-05** Freeze the Stage A candidate and execute section 5.2.1's scoped package, platform,
  live-server and Swift qualification. Present evidence and remaining Stage B obligations; stop for
  scoped candidate review. Do not claim full p0001 qualification or begin B1 on focused proof alone.

#### 5.3.4 B1a — Build coherent server sessions

Prerequisite: reviewed Stage A. Owners: Go server; Go/Kotlin shared wire fixture consumers.
Component result: an application-data-read-only session supplies one complete immutable selected view.

- [ ] **B1a-01** Settle C01's session actions, closure selection, token/paging, capacity and error
  contracts plus C05..C08. Define the read-set token here; candidate-push use remains B1d.
- [ ] **B1a-02** Maintain the server incoming-edge index and membership revisions atomically with
  captured writes. Implement idempotent create/root append/seal, consistent complete closure build
  and immutable live/deleted/absent pages with the existing visibility/sequence fence.
- [ ] **B1a-03** Enforce root/body/row, build and retained-session budgets, deadlines, admission,
  authorization/source/generation binding, cursor checks, exact ambiguous-seal retry, TTL and cleanup.
- [ ] **B1a-04** Run focused server/HTTP and paired fixture checks for multi-page roots/results,
  unrelated writes between every action, closure consistency, malformed contexts, partial-build
  rollback and capacity/expiry. Verify no business mutation, source sequence or checkpoint advance.
- [ ] **B1a-05** Present the bounded session API and focused live HTTP evidence. Stop for component
  review before client orchestration; FK-04 and guarded installation remain pending.

#### 5.3.5 B1b — Capture intent and download a restartable view

Prerequisite: reviewed B1a. Owners: Go and Kotlin clients, including Swift cancellation boundaries.
Component result: captured intent and a complete coherent view survive cancellation, expiry and restart.

- [ ] **B1b-01** Settle C04's captured phase, revision/membership guards, bounded continuation and
  incomplete/cancellation results plus C05..C08 before implementing the client session consumer.
- [ ] **B1b-02** Persist desired intent, roots, revisions and downloaded evidence in charged paged
  state. Upload roots and consume immutable pages without a SQLite transaction across HTTP/callbacks;
  refresh incomplete expired views without mixing observations or losing the captured intent.
- [ ] **B1b-03** Apply the shared 64 reconciliation-exchange and retained 64 push allowances across
  all reached wrappers/recovery/`syncThenDetach` rounds. Check cancellation per page and within
  256 work units; preserve a durable useful phase and prevent detach on incomplete progress.
- [ ] **B1b-04** Run focused restart/expiry, local/member race, budget and unrelated-write liveness
  cases against B1a, plus Swift cancellation during root upload and result download. Assert staged
  evidence and original business/dirty/outbox/checkpoint state through cancellation and reopen.
- [ ] **B1b-05** Present public-operation/HTTP evidence for capture/download and stop for component
  review. No targeted business install, intent discard or resolution candidate push is complete yet.

#### 5.3.6 B1c — Make resolution reporting durable and bounded

Prerequisite: reviewed B1b. Owners: Go/Kotlin clients and Swift-facing diagnostic APIs.
Component result: reservations, immutable segments and explicit acknowledgment survive lifecycle changes.

- [ ] **B1c-01** Settle C04's reservation, report paging/acknowledgment, detached account access and
  capacity contracts plus C05..C08, retaining section 3.4.1's exact accounting and defaults.
- [ ] **B1c-02** Implement durable pre-action reservations, atomic segment publication and idempotent
  sealed-prefix acknowledgment. Charge retained and reserved state together, keep read/ack available
  when full and reject limit reductions below use; do not auto-ack or discard unresolved reservations.
- [ ] **B1c-03** Preserve reports/reservations through crash, detach and source replacement, with
  explicit account/scope selection for detached access and no implicit cross-account fallback.
- [ ] **B1c-04** Run focused reserve/reopen/publish/ack transactions, entry/byte boundaries, incomplete
  prefix acknowledgment, full-journal and account-lifecycle cases. Candidate-response loss and remote
  replay integration remain pending for B1d; component proof cannot stand in for them.
- [ ] **B1c-05** Present the journal lifecycle and both public diagnostic APIs for component review.
  Stop before connecting report-requiring business decisions or candidate pushes.

#### 5.3.7 B1d — Complete guarded explicit resolution and historical repair

Prerequisite: reviewed B1a–B1c components. Owners: Go server, both clients and Swift consumers.
Complete behavior: FK-04 through explicit resolution, with restart and later ordinary pull remaining safe.

- [ ] **B1d-01** Settle C01's guard/repair/floor use, C02's exact guarded request/replay/non-commit
  fields, C03's ineligible-rejection handles and explicit actions, C04's resolution contracts and
  C05..C08. Positive core-rejection eligibility remains B3; absent coverage never permits fallback.
- [ ] **B1d-02** Integrate captured intent, E/D membership, writer/member revision checks and journal
  reservations with explicit resolution. Preserve newer captures and dirty evidence-only members;
  publish dispositions atomically. Do not enable the automatic default.
- [ ] **B1d-03** Bind guarded candidate pushes and replay to exact request/read-set identity. Replay
  committed requests before expired-guard checks; refresh changed dependencies without global
  scope-sequence invalidation. Preserve pending application rejections with bounded handles and
  explicit retry/whole-D AcceptServer under the same reservation and revision rules.
- [ ] **B1d-04** Add observation floors and historical repair with targeted installation: checkpoint
  10, unseen X->P at 11, then targeted deletion at 12 requires explicit newer X evidence. Process
  every original validated bundle/checkpoint in order; no invented tombstone, blanket skip or
  full-scope snapshot. Include reparent-to-Q, absent/tombstone and multilevel variants.
- [ ] **B1d-05** Run focused FK-04 and repair cases, final-gate second-connection races, workspace
  pressure, expired-session replay and full-journal lost-response/reopen/prefix-ack/resume. Exercise
  source replacement and detached access; prove Swift cancellation during repair and explicit
  network resolution, preserving section 4's complete state assertions.
- [ ] **B1d-06** Prove missing-parent explicit resolution, guarded retry and subsequent historical
  pull through both public clients against the Go server. Present the complete B1 diff/evidence,
  report/replay integration and remaining obligations. Stop before changing the default policy.

#### 5.3.8 B2 — Enable automatic ServerWins

Prerequisite: reviewed complete B1. Owners: Go/Kotlin clients, public/Swift APIs and paired fixtures.
Complete behavior: automatic resolution uses the exact field/existence table and reports every lost intent.

- [ ] **B2-01** Settle C04's ServerWins default, merge/defer and disposition contracts plus C05..C08.
  Preserve section 3.2.1's exact B/L/S table and E/D membership; do not reinterpret compatible edits.
- [ ] **B2-02** Implement typed column/existence selection, atomic JSON values, generated-column and
  key-change rules. Keep unrelated/dirty E-only intent; missing B returns
  `resolution_base_unavailable` without reconstructing it from S.
- [ ] **B2-03** Apply the selected result with baseline S and exact remaining dirty intent; reserve
  and publish column/existence losses atomically. Local CHECK/unique failure rolls back fully.
  An opaque server rejection cannot authorize destructive repair or completed resolution.
- [ ] **B2-04** Run focused SET NULL plus local name, competing reparent, equal edits, identity
  collision/deletion, dirty evidence-only parent, local constraint rollback and report/reopen cases.
  Assert every selected value, dirty/base transition, disposition and preserved unrelated record.
- [ ] **B2-05** Prove the default and explicit merge/defer public behavior live in both clients,
  including FK-05's ServerWins portion. Present parity and journal evidence; stop before ClientPreferred.

#### 5.3.9 B3 — Complete ClientPreferred and qualify the program

Prerequisite: reviewed B2. Owners: Go server, both clients, Swift/public callers, examples and docs.
Complete behavior: all final p0001 policies, safety contracts and section 4 assertions are implemented.

- [ ] **B3-01** Settle C02/C03's positive core-FK eligibility and C04's ClientPreferred rename plus
  C05..C08. Remove ClientWins without aliases and align public callers/docs, including network work
  and cancellation in explicit resolution.
- [ ] **B3-02** Implement closed core-owned pre-application FK evidence covering the complete candidate
  and every consulted dependency, including cycles. Preserve application-trigger normalization;
  early rejection requires section 3.3.1's readiness conditions. Category/SQLSTATE/application
  declarations cannot establish eligibility.
- [ ] **B3-03** Construct ClientPreferred candidates from available complete values and permitted
  ancestors. Implement eligible rejection fallback and positively proved inability-to-construct
  fallback, excluding missing evidence/capacity. Refresh relevant dependencies first; retain exact
  replay, committed prefixes, report reservations and shared invocation budgets through all paths.
- [ ] **B3-04** Run focused eligible-core and application C/U rejection/retry/AcceptServer cases,
  predicate/absence dependencies, spoofed categories, auth/protocol/unknown-commit exclusions,
  normalization, relevant changes, partial-prefix fallback and budget exhaustion. Complete FK-05,
  ER-01/02, API-01 and all remaining section 4/4.1/4.2 assertions in both clients.
- [ ] **B3-05** Close C01..C08 and the assertion-level requirement ledger with paired live/public
  parity. Review the complete implementation for missing requirements, freeze identities, then
  run section 5.2's mandatory full qualification. Deliver the final evidence package and paste-ready
  independent implementation review request; stop before acceptance or any delivery action.

### 5.4 Review-stop and handoff rules

At each stop present the behavior/component, paired diff, focused evidence, remaining obligations
and scope growth, including section 1.4's replacement and complexity evidence; wait for owner
agreement to continue. Unjustified layering or retained superseded mechanisms leave the slice open
even when its focused tests pass. If the phase cannot reach its promised result
without pulling in a later subsystem, explain the dependency and replan before expanding. Follow the
Go AGENTS 45–60 minute overrun checkpoint; diagnose the first timeout before retrying. Do not repeat
broad campaigns while central behavior is unfinished.

Mark each section 4 requirement as pending, partially evidenced or complete for its actual assertions.
A stage boundary is not a feature flag, compatibility mode or separate deployment. B1a–B1c component
evidence does not close B1, and no intermediate stop closes the full program. Sections 5.2.1–5.2.2
govern development/scoped qualification; section 5.2's final command matrix remains mandatory.

The future implementer must read the accepted documents, verify both complete identities and
authority hashes, inspect full relevant lifecycles rather than only the diff, and preserve unrelated
work. Stop on external authority/source/inventory drift, ambiguous overlap, or a contract ambiguity
that cannot be resolved under accepted authority. Implementation deliverables include code, OpenAPI,
classified fixtures/manifest/pins, regression ownership, validation ledger, final identities, and a
complete paste-ready independent review request. Qualification, owner acceptance, staging, commits,
pushes, publication, deployment, and release are not implied by this handoff.


## A1 local contract settlement — 2026-09-26


This elaborates revision 3 without changing its requirements. No HTTP field, v1 encoding, canonical
wire ordering or hash domain changes. The frozen review identity is retained as historical evidence.

Public configuration: Go `MaxSyncWorkspaceEntries`, `MaxSyncWorkspaceBytes`,
`MaxSyncWorkUnitsPerInvocation`; Kotlin/Swift `maxSyncWorkspaceEntries`, `maxSyncWorkspaceBytes`,
`maxSyncWorkUnitsPerInvocation`. Current defaults are 131072, 268435456, 1000000. Values must be positive.
Go's zero-valued omitted options use these defaults, consistent with its existing configuration.

Compatibility/internal-slice results: Go `SyncWorkLimitError` / Kotlin `SyncWorkLimitException` have `unit`, `limit`, `used`
and reason `sync_work_limit`; ordinary public operations continue this progress internally, without a transport retry. Public `SyncWorkNoProgressError` / `SyncWorkNoProgressException` identify a stalled local phase. Go
`SyncWorkspaceLimitError` / Kotlin `SyncWorkspaceLimitException` additionally have `dimension`,
`reserved` and `resumable`, with reason `sync_workspace_limit`. Dimensions are `entries`, `bytes`
only. The former `atomic_apply_work` dimension is historical and is not emitted by the current contract. `unit` is a bounded hash of the validated bundle identity and operation.
No source sequence or checkpoint is consumed by either incomplete result.

Persistence: `_sync_apply_plan` owns one validated-input/revision guard, root/closure cursors and
charged totals. `_sync_apply_rows` stores deduplicated intended rows plus an ordinal frontier.
The existing dirty revision, generation/source identity and ordinary checkpoint guard saved progress.
Resume only an exact match under the SQLite writer lock; page obsolete-row cleanup before reuse.
Pull refetches and validates the original complete bundle; settlement retains exact frozen replay.
Staged business copies are derived; existing business/dirty/outbox records remain authoritative.
New metadata belongs to the existing complete-layout census; there is no migration or compatibility reader.

Accounting: count every stored row and the plan descriptor, plus transient encoded copies before
loading them. Reserve encoded key/payload lengths and metadata bounds, including the saved cursor.
Use a bounded indexed primary-key step for discovery, charging visited nonmatching rows as well as
matches and duplicate lookups. No recursive SQL or hidden complete-table scan is one nominal unit.
Save progress at work exhaustion; stale-plan cleanup is also paged and charged. Reserve a bounded
final apply/restore/verification allowance before business writes; distinguish insufficient remaining
invocation allowance from an intrinsically oversized atomic apply. Cancellation rolls back the current
transaction; previously committed staging and earlier completed bundles remain usable.

The shared `local-apply/a1.json` family records defaults, local results and the FK-01 witness. Both
Go and JVM consumers must execute it and require the complete manifest. This is local state contract
coverage, not a new wire endpoint. Exact hashes are frozen in the A1 ledger before runtime consumers.
Final A1 review must include restart, stale-revision, limits, local-layout rejection and request counts.


## A1 first review remediation settlement — 2026-09-26

This replaces the earlier A1 discovery implementation detail; revision 3 behavior and limits remain
unchanged. Saved positional cursors bind the exact registered-table sequence and admitted FK traversal
shape. Reordered registration invalidates the old plan under the writer lock and uses charged cleanup
before restart. Every staged identity expands its current incoming descendants, including UPDATE,
locally preserved and version-skipped roots; expansion is not conditional on a live DELETE edge.

`_sync_apply_edges` is a derived, per-apply-unit incoming lookup containing only parent/child table
and key identities, an ordinal and encoded charge. Build it with bounded keyset visits once over the
registered FK tables for the guarded current view, persisting progress in the existing plan. Each
relationship is visited once during the build, independently of the number of affected parents;
subsequent closure visits use its parent/child index. No per-parent full child-table pass remains.
This is temporary charged workspace, not authoritative B or a persistent relationship authority.
It may contain unrelated current relationships; their storage and build work are fully charged.

The plan adds `edge_entries` and `edge_bytes`, sharing the existing aggregate entry/byte allowance
with row staging and simultaneous transient copies. Phase `index` lies between `roots` and `walk`.
Obsolete edges and rows are removed by bounded charged steps; final edge cleanup is preflighted and
commits atomically with apply. The additional table/columns belong to complete-layout admission;
recreate old development databases rather than migrate them. No HTTP or public option changes.

Pull seeds reserve the incoming payload's encoded size before allocating a string copy. Go uses
the received byte length; Kotlin measures JSON encoding without constructing its output, including
escaping, Unicode and nested JSON values, checking cancellation and temporary recursion capacity.
Only admitted payloads are serialized for the survivor stage.

## A2-01 contract settlement — 2026-09-26

Status: **AUTHOR CONTRACT CANDIDATE; independent C08 review and owner decision pending.**
The complete local contract now lives in the paired tracked engineering document
[Oversqlite authoritative state and dependency planning](../../sqlitenow-kmp/docs-engineering/oversqlite-dependency-planning.md).
This specification adopts its authority, persistence, lifecycle, directed selection, resource bounds,
restart, public-result and validation sections, plus the linked logical persisted format. The move
removes temporary phase vocabulary from durable files without changing settled behavior.

Phase/requirement mapping belongs here and in review evidence:
- A2.1–A2.4 correspond to the document's Authority and representation, Persistence and atomic ownership,
  Directed discovery and selection, and Bounds, restart and local public results sections.
- S01/S02 correspond to Current and intended call flows and Replacement ownership; S03–S05 remain
  implementation/removal/correctness/complexity obligations, not completed by this author candidate.
- C04–C06 use the durable state/lifecycle/public contract and local-planning/dependency-planning.json.
  Fixture runtime owners baseline-lifecycle, dependency-planning and public-sync-live map respectively
  to A2-02, A2-04 and A2-05. All 28 runtime scenarios remain unexecuted.
- C07 fresh pre-consumer request counts and original logs remain in the A2-01 ledger. The naming
  amendment ledger maps renamed selectors and records focused revalidation; the A1 exception is unused.
- A3's pending-result and B2's base-error prerequisites are settled in the durable local public shapes.
  A3 survivor/paged-backlog qualification and Stage B merging/reconciliation/report work remain later.
- C08 independent review and owner acceptance are still pending. Stop before A2-02 runtime work.

OpenAPI, protocol v1, hash domains, admitted topology and all earlier accepted specification text are
unchanged. Historical candidate 9271fb4a696f6d40133a5ae68287ecee0e290ada856dc1a36510b297d6f3aa9a
is superseded for current review by the naming amendment's new frozen candidate; its artifacts remain
unchanged under .artifacts/p0001-fk-safe-sync/a2-01/. The refreshed entry point is
.artifacts/p0001-fk-safe-sync/dependency-planning-naming/review-request.md.

## Rekey ordering review amendment — 2026-09-26

Status: **proposed contract correction; independent rereview and owner acceptance pending.**
The A2-01 review found that the extra old-delete -> new-insert prerequisite lacked durable capture
state. This amendment explicitly removes that synthetic prerequisite; it does not add pairing state
or infer pairs from ordinals, payload similarity, transaction adjacency or update history.

Clarify the Go specification's section 2.2 key-change/dependent-operation requirement: each identity retains its own
operation, complete historical B and desired state. Actual authoritative/desired FK relationships
supply prerequisites. A local key update atomically captures delete-old and upsert-new, but does not
promise an atomic remote rename or unconditional insertion-before-deletion order. With no FK
prerequisite and max_push_rows=1, earlier delete-old may commit first. If the new insertion then
fails, preserve its intent and report the committed prefix; do not recreate the old identity or
claim rollback. This is an explicit weakening of the unaccepted author's extra ordering guarantee,
not merely an implementation detail or a claim that the reviewed wording was satisfiable.

The durable engineering contract now defines independent coalescing, chained rekeys, restart,
freeze/requeue, pruning and cleanup under existing per-identity owners. No new persistent object,
field, FK projection kind, wire payload or runtime consumer is authorized. Runtime lifecycle and
planner validation must include no-dependent rekeys, unrelated inserts with identical persisted
facts, chained rekeys, restart and partial remote failure. The existing child-reparent case remains.

The shared corpus adds six bounded pure relationship/order witnesses and five pending runtime
scenarios. There are now 33 explicitly unexecuted runtime scenarios, superseding the prior count
of 28. New assertions check only declared persisted facts and one-row acyclic scheduling; they do
not implement or qualify SQLite capture, crash recovery, SCCs or live behavior. C04/A2-01 is corrected
at author-candidate level only; C08 requires rereview. Preserve the original C07 evidence, Stage A
platform/live/Swift obligations and all later gates. Stop before A2-02 runtime implementation.

## Contract documentation separation — 2026-09-26

Status: **author amendment; independent rereview and owner acceptance pending.**
This is a documentation-only separation with no change to contract behavior or validation obligations.
The durable `docs-engineering/oversqlite-dependency-planning.md` reference retains the state model,
fixed ownership, ordering, rekey semantics, bounds, public results and complete lifecycle invariants.
Its descriptive name and content remain useful after this specification's delivery work is complete.

The [temporary implementation and validation plan](../.artifacts/p0001-fk-safe-sync/contract-document-separation/implementation-plan.md) now owns current-versus-target call flows,
concrete replacement/removal instructions, remaining integration work, future test assignments,
command budgets and author review attention. Earlier references to those planning sections in the
engineering document now resolve to this plan. The lifecycle matrix itself remains in the engineering
reference; its contract witnesses remain explicitly distinct from executed runtime evidence.

No runtime/source/fixture requirements changed. Reuse unchanged focused contract evidence; refresh
corpus manifest integrity and frozen review identity. All 33 runtime scenarios and later platform,
live-server and Swift qualification remain pending. The rekey ordering amendment remains proposed,
including its owner-facing partial-progress tradeoff. This separation is not review, acceptance or
authorization to begin A2-02. Historical specifications and review records are preserved.

## Accepted baseline contract and lifecycle implementation — 2026-09-26

The owner accepted the independently reviewed contract candidate, including the rekey tradeoff,
and explicitly authorized A2-02 in both repositories with dependency-planner implementation excluded.
The accepted candidate SHA-256 is
`10b6078853de25aa4502fec354bca8cb9251ad63593a351115088b27130ef6e5`.
This acceptance supersedes the pending-acceptance wording in the historical amendments above;
those frozen artifacts remain historical evidence, not current worktree baselines.

The current author slice installs full authoritative A, per-intent historical B, server/desired
relationship projections and checked revisions under the existing owners. It covers capture,
freeze/requeue, reached conflict resolution, settlement, pull, snapshot/recovery, retained-format
rejection and cleanup. The old pair-valued capture readers and version-only baseline writers are
removed. Existing push selection is adapted only to read explicit fields; directed traversal,
SCCs, planner workspaces/bounds/diagnostics and the public live multi-batch proof remain A2-03..05.
The three planner tables in the settled logical inventory are intentionally not installed in this
baseline-only slice. This is not a complete planner format or qualification claim.

Runtime implementation remains an author candidate requiring independent review and owner
acceptance. Its focused validation ledger maps fresh and reused evidence without promoting the
33 shared contract scenario declarations to executed runtime traces. Package, platform, live-server,
Swift and later Stage A obligations remain pending. No A3 or Stage B work is authorized by this record.

Author evidence: [baseline lifecycle ledger](../.artifacts/p0001-fk-safe-sync/baseline-lifecycle/implementation-ledger.md).


## A3-01 aggregate workspace and ordinary pending contract — 2026-09-28

Status: **AUTHOR CONTRACT READY FOR INDEPENDENT REVIEW; no A3 runtime authorization or qualification.**
Entry is the owner-accepted, independently reviewed A2-05 checkpoint: Go
`047a5c3f7c5b02c82c690ef0cd39dca2877bd888`, Kotlin
`964f42f30241533e15f08aba6120afb7cd24b06c`, both `ft-migration-support` and initially clean.
The read-only checkpoint verifier passed against manifest
`8ed729df0ff559d861f60a9536f79cf071cce343448b4be064de734428efbf49` before documentation edits.
These entry identities remain historical after the authorized authority amendment; no frozen packet
or acceptance record is rewritten. The [A3 entry packet](../.artifacts/p0001-fk-safe-sync/a3-01-contract/README.md) owns the new identity overlay,
call-flow/replacement plan, assertion ledger, future selectors/budgets and contract-review request.

Adopt the [aggregate ordinary-operation workspace and safe pending contract](../../sqlitenow-kmp/docs-engineering/oversqlite-dependency-planning.md#aggregate-ordinary-operation-workspace-and-safe-pending-outcomes).
It settles C04's A3 portion: one database storage budget and outer-invocation work meter; all temporary
Stage A copies, including committed-effect/snapshot stages and direct conflict/pull buffers; bounded
paged preservation without a second full backlog copy; charged necessary backlog verification;
matching Go/Kotlin/Swift pending results and error precedence; safe restart, cleanup, cancellation,
complete local intent/historical B, clean survivors, exact replay and atomic checkpoints.

C05/C06 are a concrete future fixture/consumer activation plan in this packet. The active corpus,
logical-format declarations, sorted manifest, consumer pins and all 33 `executed:false` runtime
scenario declarations remain unchanged in A3-01. Activate the reviewed local contract fixtures,
classifications, logical field/index inventory and paired pins together before runtime consumers in
A3-02/03. C01–C03 and OpenAPI have no A3 impact; no HTTP fields or hash domains change. C07 retains
ordinary request-count requirements and assigns fresh affected-path proof; no runtime suite ran here.
C08 remains pending independent review and owner decision. No checklist item is promoted to an
independent PASS by this author preparation.

The packet assigns FK-06, LC-01/02, BD-01, RP-01, CW-01, RC-01 and PF-01 assertions to bounded
A3-02/03 implementation, A3-04 focused evidence and A3-05 qualification after candidate freeze.
Use p0001 sections 5.2.1–5.2.2, not a maintenance exemption or the later full-program aggregate.
Node remains transient. Historical repair, sessions, automatic policies, report APIs, Stage B and
full-program qualification remain later obligations. Stop for independent contract review.


## A3 supported ownership clarification — 2026-09-28

Status: **AUTHOR AMENDMENT READY FOR INDEPENDENT CONTRACT REVIEW.** The owner agreed to prepare
this bounded clarification in the receiving A3-02 task. Independent amendment review and owner
acceptance of the frozen candidate remain pending; runtime implementation remains stopped.

Adopt the engineering contract's [Supported database identity and reservation lifetime](../../sqlitenow-kmp/docs-engineering/oversqlite-dependency-planning.md#supported-database-identity-and-reservation-lifetime)
section. A3 retains SE03's existing same-runtime lifecycle boundary: hard-link aliases, mixed-runtime
ownership and hostile-filesystem behavior remain outside the completion contract. Within the
supported database identity, all admitted handles and retained/reserved derived state share one
aggregate storage budget; it is not a separate allowance per Client. Preserve stronger existing Go
physical-file exclusion behavior and the persistent browser worker's lifetime Web Lock. Node stays
transient.

Use short writer transactions for reservation admission, transfer and release in existing owners.
Reclaim an orphan transient reservation only after reacquiring the existing exclusive lifetime
lease proves that the preceding owner ended; retain the lease through cancellation/rollback/cleanup.
Internal clients sharing a live coordinated opening do not establish fresh abandonment proof.
Retained stage/input charges, complete A/B/L and historical bases, clean survivors, frozen replay
identity and checkpoints remain protected. No TTL reclamation, transaction across HTTP/callback,
new persistence owner, ownership protocol or Stage B work is introduced.

The [amendment packet](../.artifacts/p0001-fk-safe-sync-a3-02-ownership-amendment/README.md) preserves A3-01 acceptance and the A3-02 preflight stop as
historical evidence and records this new authority overlay, paired diff and review identity.
Before A3-02 production changes, settle the exact physical inventory and activate matching local
fixtures/consumers/pins. Focused second-handle, supported-alias, live-buffer cancellation and physical
restart/reacquisition witnesses remain required. No runtime suite or fixture activation occurred
while preparing this amendment; all 33 original runtime declarations remain `executed:false`.
Stop for independent contract review and owner acceptance of this candidate before resuming A3-02.


## Library-owned work slices — 2026-10-13

This owner-authorized amendment supersedes public work-yield behavior in earlier sections. Ordinary push, pull, sync, rebuild, attachment/recovery, conflict resolution and local lifecycle operations complete their existing semantic unit inside the library. Push handles one logical bundle; sync drains successive bundles before pulling. The existing work-limit field and its 1,000,000 default now select an internal computation slice, not a customer-visible completion cap. No new SDK operation timeout is introduced: caller cancellation/deadlines and existing convergence guards remain authoritative. Entry/byte, wire, snapshot and dependency-group hard limits remain unchanged.

Continuation resumes the current local phase after a useful safe checkpoint and successful cleanup, under the same outer operation owner. It preserves exact frozen request, snapshot/replacement, pull ceiling, retry budgets, attempt/round allowances and accumulated uniquely confirmed effects. Slice accounting and the first-ready-atomic allowance may reset only at a safe boundary. Every positive target must allow a bounded useful step or admissible ready atomic unit; setup must not starve progress. A slice that yields without actual progress fails explicitly rather than spinning. Mixed work/cleanup failures, cancellation and actionable errors retain precedence. Scheduling yields do not escape ordinary public APIs or enter transport backoff. Historical work-error types remain available; internal bounded-step tests may observe them. No local format or HTTP change is authorized by this amendment.

The scoped qualification profile is the paired work-slice completion program, not a routine-maintenance exemption or full protocol/release acceptance. Fresh evidence requires paired corpus/boundary consumers; Go owning-client and nested-module tests with Go 1.25.12 and focused race instrumentation; KMP common/JVM comprehensive coverage; affected Android/JVM/iOS/macOS/JS Node/browser/Wasm and Swift cancellation/mapping; isolated live multi-batch, replay/restart, snapshot and cancellation; for future approved-profile candidates, the default-budget1/5/10/25-user complex scenario ladder with matching concurrency and verification (former50/100 requirements retired with no equivalent credit); and current 1,000/10,000-row resource evidence. Add a synthetic 14-table/3,399-row witness for the maintained synthetic activity shape. Do not schedule retired 50,000-row replay campaigns. Existing per-command deadlines, failure diagnosis, candidate freezes and evidence invalidation rules remain.

Implementation ledger: Go `.artifacts/work-slice-completion-20261013/ledger.txt`, with immutable paired baselines/preimages and a current execution ledger. Specification and corpus changes alone do not assert runtime qualification. Delivery remains separately authorized.


### Seed-stage unpublished-delete pruning

During initial root seeding, a delete already validated as unpublished/absent/deleted may be pruned without discarding unchanged roots seeded earlier in that same guarded traversal. Charge its validation and the additional guard refresh. Under the same writer transaction, advance the seed cursor and rebind the existing guard after only this SDK-owned deletion; dependency/edge traversal has not begun and the deleted key was never inserted as a root. Per-node intent revisions remain their capture revisions. Any external relevant revision change still takes the normal bounded invalidation/cleanup path. No new persistent field, phase discriminator or wire behavior is introduced. Required witnesses compare seed visits for 125 live roots plus 10 unpublished deletes, retain malformed-base refusal and second-writer invalidation, and execute the mixed-CRUD complex workload.

Bounded planning transaction refinement (2026-10-13): public push preparation may run at most 32 existing planning steps in one writer transaction. Work admission, guards, capacity checks, cursor persistence and cancellation checks remain per step. A normal work refusal after successful steps commits their already saved private frontier only after cleanup succeeds; other failures roll back the entire group. Tiny targets still admit one useful step. Transaction-owned schema metadata may be reused within that writer snapshot and is released before continuation. Raw bounded-kernel tests keep one-step transaction entry points. No configured target, accounting weight, hard quota, public operation unit or stored representation changes.

Writer-scoped planning admission snapshots (2026-10-13): while a bounded push-planning writer transaction owns the database, operation/reservation/snapshot and apply-owner scalar charges are immutable. They may be read once and reused for that transaction. Every planning allocation still adds its current push-plan charge and requested temporary charge and applies the unchanged hard limits; no retained push workspace is discounted. The finite scalar snapshot is charged as 2 entries and 256 bytes, released before the writer is released, and never reused across a transaction or work slice. No payloads or live SQL handles are retained. Guard checks and per-step work charges remain unchanged. Raw kernels and non-planning operations retain uncached admission. Required structural witnesses compare metadata SQL reads, steps, charged work, hard refusals and cleanup, separately from timing.

Writer-scoped planning guard snapshots (2026-10-14): a guard hash, source-bundle ordinal, initialization identity and their previously admitted context-size bounds may be reused only inside the same bounded planning writer transaction. Every step still checks its plan guard and current workspace against those bounds and receives the unchanged full work charge. The cache is charged as 4 entries/8192 bytes in addition to the scalar-owner snapshot; initialization identities over 1024 UTF8 bytes or insufficient optional cache capacity use fresh reads instead. SDK-owned no-op deletion invalidates the cached guard before rebinding. Other guard inputs cannot change under this writer; external changes are read at the next writer and final freeze gate. Release before continuation; no persistence or wire change. Required witnesses retain identical planning steps/work and demonstrate fewer guard reconstructions plus external-write, no-op, hard-limit and cancellation safety.

Decision-copy reservation continuation correction (2026-10-14): capture the operation-owned buffer reservation base once before replay continuation. Each slice admits its complete simultaneous decision-copy workspace relative to that same base, never relative to a previous slice's temporary charge. The decision allowance may conservatively retain its maximum for the one outer operation; it is reused, not accumulated, and the existing once-only outer release remains mandatory. Statement pools remain slice-owned and close/release before progress commit. Retained apply inputs stay fully charged. A paired125-row/8-table/max-push40/default-and-target1 fixture must complete without growing reservation entries as slice count increases. No weights, quotas, public signatures or stored layout change.


### Fresh-database completion profile — 2026-10-14

The owner authorizes completion on freshly created server and client databases. Backward compatibility with older database formats is not required. A measured optimization may change the local format or indexes after its paired contract is amended; create the new format directly, without old-format migration, dual readers or compatibility shims. Current method signatures, configuration defaults and HTTP shape remain unless a demonstrated problem requires an explicit amendment. No format change is currently selected.

Reopen, process restart, replay and recovery of databases created by the new implementation remain mandatory, including frozen identity, confirmed effects, checkpoints, cancellation, resource ownership and hard limits. Old-version upgrade and table-retirement compatibility journeys are outside this scoped completion profile; preserve their recorded baseline failures and historical requirements. Their exclusion never claims the broader historical suite passed.

For future candidates bound to the approved short-entry profile, obtain phase-separated structural evidence and verified1/1,5/5,10/10,25/25-user/concurrency phases against one freshly isolated authority. First three caps are240s and final300s, with default client budgets, verification enabled, all25/25 users completing and zero leaked scheduling failures. Section9 of the paired r0001 validation contract defines shared preparation, prerequisite and cleanup bounds. The former50-user rehearsal and100-user/50-concurrency acceptance are retired prospectively; historical results retain their original meaning. Keep resource measurements and comprehensive suites isolated from heavy load. No increased work target, reduced accounting weights, caller continuation loop, blanket retry, verification weakening or runner extension qualifies.

The latest KMP decision-reservation correction invalidates dependent replay/ownership public-call evidence. Freeze after focused checks and source review, then refresh exactly the affected platform, Swift, live and resource lanes plus independently mandatory aggregates. Current 1,000/10,000-row replay workloads use name widths 16/1,024; no retired 50,000-row replay campaign is reintroduced. Deliver paired patches, quantified findings, an audited qualification ledger and explicit database-recreation guidance; acceptance and delivery remain separate.


### Bounded planning statements and cleanup — 2026-10-14

Go may opt private planning reads and scalar/null mutations into the existing fully charged writer-owned statement reservation. Reuse remains limited by its admitted handle/SQL capacity, at most 16 arguments and a 64KiB encoded argument envelope; oversized arguments or optional capacity refusal retain one-shot execution. Original planning work charges remain; added argument/lease visits are charged. Close all handles and release the pool before writer commit or continuation. A per-step savepoint protects the prior saved frontier if added lease accounting yields inside a step. Prepay six boundary work units before the savepoint; rollback/release cannot themselves yield, and their failures retain precedence. Raw kernels keep their original single-step ownership. No quota, default, wire or database-layout change.

Both clients may group at most 32 existing push-plan cleanup records in one writer transaction. Preserve fixed cleanup identity, per-record work and charge release, cancellation checks and saved state after every record. Only a pure computation refusal after useful steps may publish the saved prefix after successful cleanup. Other failures roll back the writer. Kotlin's existing step accounting refuses before mutation; Go's leased mutations retain the savepoint boundary. Required witnesses compare prepares/closes, cleanup commits/descriptor reads, complete primary state, actual progress, charged work, hard fallback and tiny-target behavior separately from ordinary latency.


### Writer-owned planning descriptor reuse — 2026-10-14

Inside one bounded planning writer, retain the same admitted mutable push descriptor and the validated NONE outbox state. Do not copy payloads or retain the descriptor across writers, slices or bundles. The existing 2-entry/256-byte scalar holder covers only the added reference and flags; the descriptor remains fully charged by the existing current push-state admission on every step. Hydration and its temporary decode admission happen once per writer. Every original planning step, guard, current-state capacity check, saved cursor and cancellation check still runs. Clear the reference when the plan owner is deleted, including an all-no-op frontier. Discard the entire holder after cleanup before publication/continuation, including a rolled-back partial step; the next writer reloads durable state. Raw kernels keep fresh reads. Required witnesses compare descriptor/outbox reads and unchanged semantic progress/accounting coverage, plus external writes, cleanup, no-ops, hard refusals and tiny targets. No work weight, quota, format or wire change.


### Go bounded baseline reads and ready atomic statements — 2026-10-14

Go may reuse the existing fully charged writer-owned statement pool for scalar baseline reads and typed payload projections. Projections with more than 16 arguments retain the original one-shot route; oversized encoded bindings also fall back. Reuse holds statements, never read results, and preserves current identity/provenance, typed values, all validation, query ordering and existing accounting. Added bounded-argument and lease visits are charged. Planning calls remain inside their existing per-step savepoints; apply preparation retains its preadmitted checkpoint and durable-prefix rules.

After a ready apply unit has prepaid its existing atomic statement credits, Go may also reuse admitted mutation statements. Original write visits remain charged, additional argument visits debit those same conservative prepaid credits, and no work refusal is introduced inside the ready unit. Bounds remain 16 arguments and a 64KiB encoded envelope. Unsupported binding types, oversized bindings or optional pool-capacity refusal use the original one-shot driver path. Close/release every retained handle before publication or continuation; preserve failure/cancellation precedence and hard workspace limits. No process-wide SQLite/runtime configuration, work weight, public default, wire or layout change is selected. Required witnesses include typed and wide projections, trigger visibility, optional fallback, cleanup failures, public tiny-target completion, fixed-fixture accounting, native prepares/closes and repeatable ordinary complete-scenario timing. Kotlin retains its own charged statement machinery; representation identity is not asserted.


### Device-load process ownership amendment — 2026-10-14

The owner authorizes one operating-system process per simulated user for device-load qualification. Each child executes the complete existing scenario, including mixed CRUD, laptop recovery, both crash hooks, replay/restart and final verification, with unchanged SDK defaults and transport policies. The parent runs at most `--concurrency` children, requires a successful child result and independent per-user final verification, and owns cancellation, termination and reaping. Child verification uses a bounded single-connection PostgreSQL pool; credentials travel through private process input rather than command arguments or reports.

Parallel mobile_flow uses process workers by default; `--parallel-mode=goroutine` retains the original shared-process SDK stress mode. Previously failed shared-process attempts remain failed stress evidence and are not relabeled as device-load passes. The library completion/cancellation/resource contract applies in both modes. For future approved-profile candidates, device-load qualification requires four verified1/1,5/5,10/10,25/25-user/concurrency phases under the shared short-entry load-sequence contract:240/240/240/300s, same fresh authority, unchanged per-user scenario/default budgets/verification. The former50/100 current requirements are retired, with no equivalent credit; historical evidence is unchanged. Parent cancellation stops queued work and joins every owned child; incomplete, malformed, missing or failing child results fail the run. No global SQLite configuration or SDK throttle is introduced.


### History-projection retry timing correction (2026-10-14)

A validated HTTP 503 `history_projection_capacity_exceeded` response with a positive delta-seconds `Retry-After` must postpone the next transport attempt by at least that delay. Both clients retain their existing generic attempt limit, backoff configuration and numeric defaults; this correction neither introduces another retry budget nor restarts a public operation. Invalid, absent, overflowing or nonpositive headers retain existing backoff. Caller cancellation interrupts the wait, and terminal/malformed responses retain error precedence. Local work slices never create transport delay. Server history-projection permits, workspace limits and wire shapes remain unchanged. Qualify header propagation, unchanged request identities/attempt count, cancellation, terminal errors and the existing public-call platform/live gates.


### Writer-owned replay reservation binding reuse (2026-10-14)

Kotlin may reuse the reservation context binding only while building replay decisions inside one apply writer. During that phase, reads and reservation-ledger updates do not modify any input of `contextGuard`; actual authoritative/business mutations begin after the decision map returns. The binding must match the database, runtime state, token, unit and writer-owned statement-cache identity. Recompute it in every resumed writer, and retain all ledger validation, per-row size measurement, row work charging and admission checks. A bounded binding holder is charged as four entries and512bytes against the existing stable outer reservation base; repeated slices must not add preceding slices' temporary charge. If this optional holder cannot fit, discard it and use the existing uncached path without concealing cleanup, cancellation or integrity failures. The holder does not escape the decision-building phase.

No work weight, public configuration default, wire field, database layout, atomic-dispatch allowance or slice quantum changes. Qualification requires a structural before/after binding-reconstruction witness, unchanged per-row accounting and committed results, ordinary timings separate from reservation observation, and fresh affected cleanup/workspace/replay/native/live lanes. Go implementation is unchanged by this Kotlin optimization; paired authority/corpus consumers must refresh, while intact Go runtime/load evidence may be reused by dependency.


### Guarded replay decisions across local slices (2026-10-14)

A Kotlin push may retain its already constructed replay-decision map under the same outer reservation while private apply preparation publishes prefixes. Before reuse in each new writer, reconstruct the complete existing reservation context guard, validate the ledger and exact operation owner, and compare the guard. Registered external business writes advance the guarded dirty revision; baseline/attachment/source/outbox/configuration inputs remain in the guard. A mismatch discards decisions before reconstructing them. No decision reuse is permitted after authoritative business mutation begins. Cancellation and failures clear the holder before outer release.

All decision copies remain fully charged against the stable original base between slices. This combined holder supersedes the earlier writer-only four-entry/512-byte holder; the two charges are not additive. Charge a bounded eight-entry/1,024-byte combined binding/continuation holder; if optional retention cannot fit, use the uncached construction path. Per-record16-work-unit,32-copy-entry and64-byte-scaling charges remain unchanged when a decision is actually reconstructed. Reuse charges guard bookkeeping and retains the already admitted copies; it does not charge a traversal that no longer occurs. Planning slice quanta and all hard limits/defaults remain unchanged. Qualify stale/external-write invalidation, frozen identities, cleanup precedence, constant reservation peaks, once-only outer release and separate ordinary timing. This does not change Go runtime or the wire.

### Empty dependency graph traversal

Kotlin apply preparation may retire one pending root in one existing bounded walk step when the completed dependency index records zero edges and a current indexed absence query confirms the edge table is empty. The pending root must exist before retirement, including when a nonzero visit cursor is loaded from durable state. A contradictory retained counter or missing root is an integrity failure. Skip per-table child traversal only under this proof; nonempty graphs retain the original walk. Preserve fresh writer guards, the16-unit step charge for each actual step, pending-row/cursor publication, hard workspace and dependency limits, and the unchanged slice quantum/defaults. This removes redundant empty traversal and writer commits; it does not enlarge a slice. Qualify empty/nonempty graphs, stale guards, cancellation/reopen, charged workspace, and browser tiny-target completion under the existing deadlines. Go runtime and wire remain unchanged.

### Independent push components

When completed push discovery records zero edges and a current indexed absence query confirms that result, Kotlin public planning may assign one pristine unvisited node directly to its singleton component. Preserve DFS/index/stack counters, component identity, blocked reason, ready ordering, encoded charge and guarded writer publication. A nonpristine or partially visited node uses the original DFS path. After packing a singleton in this proven empty graph, advance directly to component selection without traversing nonexistent dependency edges. Each transition remains bounded to one node and indexed scalar queries; original per-step work weights,32-step writer quantum, low-level kernel behavior and hard limits/defaults remain unchanged. Qualify exact wire order, independent/blocked/cyclic graphs, external writes, cancellation/reopen, workspace refusal and tiny browser/Wasm completion; no wire/layout change is introduced.


## Server-write reliability amendment (2026-10-22)

The owner authorizes a paired correction for final-state capture, uniqueness-safe local application, and reserved-writer concurrency. Wire shape, canonical first-touch ordering, operation identities and receipt guarantees remain unchanged. Deployments use fresh server and client databases; no migration or historical repair is provided.

Raw PostgreSQL capture is a touched-identity set and forbidden-effect witness, not authority for the final after-image. After deferred effects finish, finalization reads actual final rows in the same transaction and uses prior authoritative row_state to classify insert/update/delete or absent-to-absent. Preliminary effect detection and finalization use identical rules and existing exact physical/wire codecs and limits.

Both clients durably stage desired full local images and metadata decisions, then schedule plain-column uniqueness dependencies using SQLite affinity/collation/NULL semantics. Acyclic transfers release a value before claiming it. Cycles stage and preflight their complete dependent closure before temporary vacating and restoration; application-owned triggers or unknown dependencies that make temporary deletion unsafe block the entire apply. Unsupported partial/expression uniqueness and conflicting final desired values fail explicitly. No REPLACE, disabled constraints, invented business values, unbounded host graph, or canonical history rewrite is permitted. Preparation may yield with guarded staging only; complete business application and metadata/checkpoints commit atomically after final-state, protected-intent and FK verification. All extra persistent/host/statement/graph work is charged under the existing limits; no new numeric defaults. The shared apply owner covers pull, settlement/replay, conflict and snapshots.

Go exposes FinalStateApplyBlockedError and Kotlin FinalStateApplyBlockedException with bounded reasons conflicting_desired_state, unsafe_trigger_or_dependency and unsupported_uniqueness. They are local failures; they do not auto-resolve conflicts or advance synchronization state. Matching fixtures cover values/existence/versions, local values, dirty/outbox protection, failure/cancellation/reopen, closure and capacity limits, and runtime parity.

Established reserved writers hold compatible FOR KEY SHARE reservation locks; conditional false-to-true first-use publication remains in the business/history/receipt transaction. Concurrent first use may wait; established different scopes must overlap. Removal, permanent use tombstones, missing reservations and rollback remain fail-closed.

The correction qualification profile requires paired fast-contract checks, Go owning and focused race/live suites, Kotlin common metadata plus affected JVM/platform/live and Swift cancellation leaves, and measured ordinary-path SQL/workspace behavior. Follow the paired validation runbook and retain failed attempts; no missing/skipped/cached lane is a fresh pass. Delivery and actual database recreation remain separate gates.

Blocker recovery contract: errors identify operation, exact table/index or application trigger, and bounded exact affected keys plus truncation status, without payloads. Supported valid transfers/cycles must execute automatically; an unexpected blocker remains an SDK defect. Application corrections use ordinary edit/conflict or coordinated schema-upgrade APIs. After a local schema/trigger or pending-intent correction, ordinary Sync invalidates only derived staging through the current guard and retries. Later server writes do not change a retained invalid bundle: correct the authoritative final state, then use the supported Rebuild/rebuild snapshot API (preserving pending intent) to recover from that history. Rebuild retains its dirty/outbox/conflict admission checks; a frozen pending operation is never discarded to force recovery, and may require schema correction or supported conflict resolution first. Never require direct sync-metadata edits, manual checkpoint skipping, or database deletion. Repeated unchanged attempts must preserve the same pending intent.

## Prospective sections5.2/5.2.2 qualification amendment — 2026-10-26

The owner-approved [r0001 validation section9](r0001-oversqlite-durable-recovery-validation.md) and bound profile replace only the listed future package/full and comprehensive/Swift repetition and named envelopes. Audit/nested mobile/conformance, distinct Swift leaves, platform/live, ordinary/heavy, resource/load/byte, source guards and full typed vectors remain required. The single load-sequence root owns four same-authority1/5/10/25-user phases with matching concurrency. Normal entries are at most300s; former50/100 requirements are prospectively retired with zero current credit, preserving historical receipts. This does not activate deferred Stage B, alter preservation/bounds/safety, waive affected-owning maintenance proof or reinterpret historical C05/C06/C07.

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
