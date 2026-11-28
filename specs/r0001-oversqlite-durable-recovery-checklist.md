# r0001 implementation checklist

Status: C00–C05 are accepted/committed prerequisites, C06 is owner-accepted and uncommitted, and C07 implementation/qualification/review/evidence closure is complete. All twelve mandatory V10 gates have independently reviewed evidence under the exact C07 owner-approved execution amendments. Failed attempts, skipped/guarded bodies and worker persistence limits remain explicit in the [current C07 handoff](../.artifacts/durable-recovery-final-20261024/handoff.md) and [ledger](../.artifacts/durable-recovery-final-20261024/ledger.md). C07 owner acceptance, staging, commit, push, publication, deployment and real-data actions remain separate and unauthorized.

The first delivery has three milestones: **A (C00–C03): exact upload outcomes and durable settlement/ack; B (C04): safe snapshot installation; C (C05–C07): clean automatic recovery and final qualification.** Close each milestone's scoped review and evidence before beginning the next. No intermediate milestone is protocol release acceptance. Deferred features in specification section 10 are excluded from every phase; do not add their tables, APIs or extension points.

For each phase: inspect owning code/callers; write an exact paired path allowlist and requirement/test map; choose cohesive internal boundaries; implement; run focused tests; review the complete diff; close the phase or report its blocker. Preserve immutable baselines and unrelated work. Stop dependent production work when its required proof route is unreachable. Diagnose timeouts before retrying. No session/token limit is set; provide a checkpoint at 45–60 minutes.

## C00 — Reproduce authority and make qualification reachable

**Scope:** documents and qualification prerequisites. **Depends on:** separate instruction to begin implementation. **Maps:** R01–R10. No production runtime edits.

- [x] C00.1 Verify both HEADs/branches, complete tracked/untracked inventories, original Kotlin pyc hash and every [handoff binding](r0001-recovery-handoff.json), including the current revision. This candidate is deliberately uncommitted. Stop on unexplained source/index drift or ambiguous overlap.
- [x] C00.2 Create new ignored implementation evidence and complete paired baselines. Record spec/OpenAPI/corpus/toolchain identities, start/budget, exact phase allowlist and validation ledger. Preserve earlier evidence without relabeling its qualification.
- [x] C00.3 Review the complete narrowed candidate for outcome races, ack crash windows, lifecycle binding, strict clean admission, the section 1.2.1 support table against both admission owners, and snapshot side effects. Obtain independent specification review before C02 production work; resolve findings in the contract first. Do not reintroduce deferred merging/quotas to resolve an implementation convenience.
- [x] C00.4 Replace the qualification launcher's three missing authority inputs through validation section 2. Edit its owner and tests in a dedicated prerequisite batch; hash the real authorities and active OpenAPI/corpus, and fail closed on missing/tampered inputs. Never fabricate old filenames or bypass preflight.
- [x] C00.5 Bind later gates to exact commands, nonzero selectors, owned synthetic fixtures, normal durability, storage and deadlines. Classify them pending/blocked without running the campaign now. Keep prior Android/server/JVM failures and resource/live/Swift gaps visible. Verify the Activity source binding from section 1.2.2 before deriving synthetic fixtures; retain its inspected-only status until runtime evidence exists.

**Exit:** reproducible authority, recorded review, passing launcher prerequisite tests and reachable selected proof routes. Missing review permits a reviewable contract candidate only.

## C01 — Settle and activate v1 contract artifacts

**Owners:** active OpenAPI, `oversync/rest_models.go`, protocol/discovery owners, paired specs, `oversqlite-contracts/` and Go/JVM consumers. **Maps:** R01–R05, R10. **Depends on:** C00.

- [x] C01.1 Activate the settled OpenAPI to active; reconcile explicit v1-only, request-count and source-sequence statements in existing authorities. Keep historical acceptance records. At final handoff designate one wire authority; do not maintain divergent live/design contracts.
- [x] C01.2 Add strict header/error/discovery fixtures: stale clients, duplicates, unknown fields, null/absent initialization tokens, integer limits, gzip/Fetch differences and cross-scope/source/generation misuse. Classify legacy payloads as historical/rejection input only.
- [x] C01.3 Add independently computed outcome/hash vectors for all statuses, effect coverage, compact regrouping, binding mismatch, required/forbidden union members and rejected/cancelled sequence gaps. Both consumers calculate hashes rather than only parsing fixtures.
- [x] C01.4 Add snapshot floor/hash vectors: empty/multipage, alternate chunking, changed rows/order/count/bytes/floor/echo and null framing. Reject an `outcome` field inside recovery. Add resolve/ack/reopen workflow vectors; recovery snapshot creation must reject outstanding outcomes.
- [x] C01.5 Update sorted manifest, fixture classifications and paired pins together. Fail closed on missing/mismatched roots and undeclared/unevaluated families. Preserve all 12 existing uniqueness cases.
- [x] C01.6 Implement sole-v1 gates/wire models without compatibility adapters. Run V01 paired contract/body/hash checks and metadata compilation before DB-heavy work. Record temporary runtime gaps honestly.

**Exit:** settled wire artifacts and executable paired vectors. Only floor/hash snapshot wire fields are defined here; runtime snapshot work belongs to B.

## C02 — Server exact outcomes and sequence fences

**Owners:** `oversync/push.go`, source/generation/state/admission owners, schema/bootstrap/configuration, HTTP handlers and bounded writers. Cohesive new outcome-owner files are appropriate. **Maps:** R02–R04, R09. **Depends on:** C01 and independent specification review.

- [x] C02.1 Document exact keys/constraints/metadata bounds and lifecycle lock order. Implement fresh source terminal/ack fences and one retained complete result per source. No prepaid slots, global quota rows or long global lock.
- [x] C02.2 Commit business/history/capture/source progress/exact projected outcome atomically. Enforce complete-envelope bounds before commit, including expanded server effects. Exact replay returns retained evidence without re-running mutable business admission.
- [x] C02.3 Implement typed rejection rollback and retained rejection. Prove no business/deferred/history/capture/initialization/context leakage; uncertain SQL/infrastructure errors remain nonterminal.
- [x] C02.4 Implement resolve-or-cancel with barriers for push-first, resolve-first, rollback, process death and delayed retry. No absence observation may permit a later same-tuple commit.
- [x] C02.5 Implement ack latest-duplicate idempotency, mismatch/future/older cases, lost-ack retry after generation retirement/rebinding using the preserved original binding, and next-operation admission. All terminal statuses consume operation sequence; only business commits advance committed history.
- [x] C02.6 Preserve exact outcomes across history pruning and restart. Refuse source/generation retirement/rebinding that would destroy them; retain scope-deletion fencing and reserved-server-writer concurrency. Enforce section3.4 distinct source owners before both trusted callbacks and receipt replay; ownership survives ack, binding/history cleanup and restart, with zero cross-owner effects.
- [x] C02.7 Enforce one complete bounded outcome per source across service processes. Test oversized envelopes, storage failure rollback, mismatched configuration across service instances, ack cleanup and no TTL eviction. Report retained count/bytes/age and physical storage; do not claim a global database cap.
- [x] C02.8 Run V02 separately on PostgreSQL 16 and 17.10, then required stable owning/race checks. Record nonzero bodies, unrelated-scope progress and owned cleanup.

**Required test families:** `TestPushOutcome_AtomicCommit`, `TestPushOutcome_RejectionRollback`, `TestPushOutcome_ResolveSerializesWithPush`, `TestPushOutcome_AckAndSequenceFence`, `TestPushOutcome_HistoryPruning`, `TestPushOutcome_LifecycleBinding`, `TestPushOutcome_BoundedRetention`, `TestPushOutcome_CrossProcessCrash`, `TestPushOutcome_UnrelatedScopesProgress`.

## C03 — Client durable settlement and acknowledgment

**Owners:** Go push HTTP/replay/settlement/state/storage/sync; Kotlin push workflow/replay/local store/operation controller. **Maps:** R03–R04, R06, R09. **Depends on:** C02 boundary and C01 vectors.

- [x] C03.1 Persist exact frozen F, original bases/existing capture guards, full validated C and ack obligations. Reject old formats without mutation/migration. Do not add new intent epochs or column masks.
- [x] C03.2 Validate/persist C atomically before local settlement. Test death before response, during decode/partial stage, validation failure, local evidence commit and reopen. Never promote unbound rows into a certificate.
- [x] C03.3 Resolve ambiguous F using its unchanged tuple. Distinguish committed/rejected/cancelled. Rejection/cancellation requeues preserved intent and durably records disposition before ack and a successor sequence. Retain real rejection evidence until the existing resolver has disposed of it.
- [x] C03.4 Make ack_pending crash-safe. Cover death after local disposition, after server ack before response, after response before local cleanup, generation retirement/rebinding before duplicate ack, and delayed old requests. No duplicate business application or successor freeze before ack.
- [x] C03.5 Preserve current ordinary whole-row newer-edit settlement, server normalization, effects outside F and local-only values. Test same-value writes, delete/recreate and newer dirty closure. An unsafe committed settlement retains F/C and all local state, sends no ack and requests no snapshot.
- [x] C03.6 Update exact ordinary request counts: one additional ack per terminal upload, no recovery probes for no-change Sync. Run V03, reached V01/V02 and focused actual HTTP upload cases from V08; document remaining platform/final gates.

**Required tests:** `TestRecoveryJournal_OutcomeCaptureCrashMatrix`, `TestRecoveryJournal_AckCrashMatrix`, `TestRecoveryJournal_CancelledRequestPreservesIntent`, `TestRecoveryJournal_RejectedRequestPreservesIntent`, `TestRecoveryJournal_SourceSequenceGaps`, `TestRecoveryJournal_FrozenBytesUnchanged`, `TestRecoveryJournal_BlockedSettlementRetainsOutcome`; matching Kotlin `RecoveryJournalTest` methods and separate JVM process-death coverage.

**Milestone A exit:** reviewed server/client implementation and focused contract/DB/crash/race/real-HTTP proof for all three outcomes, pruning, later edits and lost ack. No claim that snapshot recovery or full platform qualification is complete. Record the exact remaining gates before B; no deferred merge/decision behavior is a prerequisite.

C03 scoped proof: Go seven required journal families plus eight actual process-kill boundaries, Kotlin matching common tests and a separate JVM eight-kill loop, exact request counts, original-generation opening/policy/ack guards, unsafe whole-vector preservation, and focused actual HTTP upload cases. Required adjacent dirty-closure replay assertions now require stored-C-only settlement and one ack. The adjacent late-write snapshot case and three positive Go remote-attach snapshot cases, plus Kotlin two-client clean snapshot live case, remain blocked by C04 snapshot decoding and are retained as failures, not C03 passes. JS/Wasm/common metadata compilation supplies no platform runtime qualification. DT01–DT07 mapped runtime, full V07/V08 and final initial-protocol aggregates remain pending. See the C03 report for native body counts, ownership replacements, failures, scope and cleanup.

## C04 — Safe explicit snapshot installation

**Owners:** server snapshot/session/hash; Go pull/recovery/snapshot gate/final-state apply; Kotlin download/apply/local store. **Maps:** R05–R06, R08. **Depends on:** A exit.

- [x] C04.1 Implement sealed floor/hash snapshot metadata. Require no unacknowledged server outcome on recovery creation; reject future floor and cross-context reuse. Test pruning, post-T writes, complete/incomplete expiry and streaming hash. Do not add outcome references.
- [x] C04.2 Apply the complete clean gate to explicit Rebuild before network and under the final writer. Cover dirty rows anywhere, prepared F, unsettled C, retained rejection and ack_pending. A new local synchronized write during preparation prevents installation and survives intact.
- [x] C04.3 Replace clear-all installation with a key-based delta including current unique holders. Preserve unchanged rows, full local columns, observable rowids and clean dependent closure. Dirty descendants fail admission.
- [x] C04.4 Convert scratch loss witnesses to regression expectations: no false delete audit commits; local-only CASCADE notes survive if their parent survives, or recovery blocks before mutation. Assert full values/identities, not just FK integrity or managed-row counts.
- [x] C04.5 Check affected application main/TEMP triggers. Implement exact empty incoming-set and safe deferred NO ACTION cases with writer membership guards. Retain precise unsupported index/dependency blockers; diagnose static restrictions at inspection where possible.
- [x] C04.6 Admit the complete atomic delta and preservation work before DML. Test mutation failure, cancellation, schema changes and late children. Run V04, paired vectors and narrow real HTTP snapshot cases, including DT01–DT06 from section 1.2.2. Keep fixtures in the implementation repositories and require successful supported cases. Remove all reachable clear-and-reinsert fallbacks.

**Required tests:** `TestRecoverySnapshot_BoundaryAndPendingOutcome`, `TestRecoverySnapshot_StreamingHash`, `TestFinalStateRecovery_CleanAdmission`, `TestFinalStateRecovery_SnapshotPreservesLogicalAudit`, `TestFinalStateRecovery_SnapshotPreservesLocalOnlyClosure`, `TestFinalStateRecovery_DependencyMembership`, `TestFinalStateRecovery_SnapshotDeltaRollback`; matching Kotlin `FinalStateRecoveryTest` methods.

**Milestone B exit:** reviewed safe explicit installation with clean-gate/race/rollback/preservation proof and actual HTTP snapshot evidence. No automatic trigger yet. Unsafe application states remain precise blockers.

C04 scoped exit: Go server PG16/17 floor/hash/pruning and SDK full-vector delta/dependency/trigger/unique/late-write proof; Go and JVM eight actual kill/reopen boundaries; actual retry/fallback admission; U5 floor and U6/U7 generation callers; DT01–DT06 in both SDKs and narrow actual HTTP (Go both PG versions, Kotlin owned PG16). The three Go remote-attach, late-write and Kotlin A1 two-client decoder failures retained from C03 now have passing constituent proof. Paired contracts and hash/status pins are current. Native counts and failures are in C04 evidence. Historical Kotlin performance-oracle tests remain retained failures with no performance credit and require deliberate adaptation/reclassification before full owning qualification. No full V07/V08/V09/V10, platform or release claim.

## C05 — Clean automatic recovery coordinator

**Owners:** current Go/Kotlin sync/download/recovery controllers and minimal public status/retry controls. No parallel sync/apply engine. **Maps:** R05–R09. **Depends on:** B exit.

- [x] C05.1 Implement `automatic_clean` (default) and `disabled`. Only eligible historical blockers with the complete clean gate may request a baseline. Static capability/corrupt-evidence/intrinsic-capacity failures never cause snapshot fetches.
- [x] C05.2 Implement the minimal durable phases, one owner, full observation floor and sealed-session resume. Snapshot completion installs T without upload sequence changes, outcome settlement or ack. Post-T writes are pulled normally.
- [x] C05.3 Add bounded `RecoveryStatus` and `RetryRecovery`; no preview/choice/report APIs. Test mode changes, duplicate retry, dirty admission, new dirty writes during preparation and explicit retry that cannot override safety. Publish the section 1.2.1 support table with early-admission versus recovery-only diagnostics. Close DT07, including dirty activity-schema rows, immutable F, unsafe C and ack_pending, without changing normal offline-write support.
- [x] C05.4 Enforce unchanged-failure parking, three replans, one expired-session replacement and 64 recovery dispatches per Sync. Nested helpers cannot reset budgets. Resume large valid snapshots across invocations.
- [x] C05.5 Charge all actual simultaneous journal/certificate/snapshot/graph/statement/host-copy owners. The gate excludes active F/C from snapshot preparation; obsolete cleanup copies still count until released. Test boundaries/tiny work targets and complete atomic admission.
- [x] C05.6 Test schema/TEMP/collation/local-only child changes, cancellation and process death at every reached phase. Before-commit failure preserves the old state vector; after-commit cancellation preserves completion. Prove bounded owned cleanup and SQLite/WAL growth.
- [x] C05.7 Run V05/V06 and affected stable owning/race checks. Review changed Go functions for cognitive complexity below 30 or a justified exception; preserve transaction/error ordering over metric cosmetics.

**Required tests:** `TestFinalStateRecovery_PublicSyncHistoricalRecovery`, `TestFinalStateRecovery_DirtyStateNeverDownloads`, `TestFinalStateRecovery_DisabledMode`, `TestRecoveryJournal_GuardedPreparationRaces`, `TestRecoveryJournal_CancelAndReopen`, `TestRecoveryJournal_UnchangedFailureParks`, `TestRecoveryJournal_ExchangeBudget`, `TestRecoveryJournal_AggregateBoundsAndCleanup`.

## C06 — Actual cross-authority and platform proof

**Maps:** R10. **Depends on:** stable C01–C05. Use V07–V09 isolation, selectors and deadlines.

- [x] C06.1 Run both clients through actual HTTP lost-response/resolve-fence/ack-reopen/history-pruning/clean-later-snapshot scenarios. Also prove later-edit conflict remains safely blocked with C retained, no ack and no snapshot. Include the section 1.2.2 Activity case mapping, not only generic helper tests.
- [x] C06.2 Execute affected common bodies on JVM, Android, macOS, iOS simulator, Node, browser and Wasm. Audit actual native bodies and integer/hash/gzip differences; cached/opted-out tests do not qualify.
- [x] C06.3 Extend owned Go/Swift launcher selections and prove cancellation at resolve, snapshot download, final apply and ack. Retain second-push regression. Swift cancellation joins KMP ownership and preserves intent.
- [x] C06.4 Complete both PostgreSQL versions and unrelated-scope progress. Resource evidence covers active 1,000/10,000-row workloads, widths 16/1,024, bounded cycle failures and repeated outcome/snapshot cleanup; no quota-slot subsystem.
- [x] C06.5 Record failures/skips/timeouts, reuse dependencies, changed retry conditions and cleanup. Inherited failures remain visible; focused passes do not erase aggregate failures.

## C07 — Freeze and qualify the complete first delivery

- [x] C07.1 Review the full implementation against R01–R10, all checkboxes and the deferred-scope exclusion. Record the section 1.1 mapping from each superseded mechanism to removed/reused owners and replacement tests, added/removed concepts and phases, changed-function complexity, and measured ack/storage costs. Fewer lines alone do not prove simplification or reliability. Freeze paired source/spec/OpenAPI/corpus and the ledger before final expensive lanes.
- [x] C07.2 Run independently mandatory V10 initial-protocol aggregates under the original limits or exact explicitly owner-approved C07 execution/body amendments recorded in V10. Reuse only where authority permits; mandatory final lanes stay mandatory. Diagnose first timeout before any retry.
- [x] C07.3 Obtain independent implementation/evidence review. Resolve findings with explicit invalidation/rerun accounting and refreeze. Record target-app fit against section 1.2.1 using authorized schemas or synthetic equivalents, including the required Activity DT01–DT07 results and the broader trigger/search, normalized uniqueness, join identity and pending-edit blocker handling. Acceptance remains an owner decision; do not equate review or passing tests with deployment authorization.
- [x] C07.4 Produce exact source/hash/command/scope/cleanup evidence and remaining limitations. State that dirty recovery, unsafe committed-result snapshot settlement and general SQLite support remain excluded. No commit/push/release/reset unless separately requested.

**Milestone C exit:** complete implementation and qualification evidence, with outstanding acceptance/delivery gates stated precisely. An earlier request to release A or B still requires the applicable full initial-protocol qualification; intermediate scoped exits never waive it.

## Requirement-to-proof map

| Requirement | Implementation | Decisive proof |
| --- | --- | --- |
| R01 sole v1/fresh format | C01–C03 | Paired wire/format rejection and no compatibility branches. |
| R02 retained outcomes | C02–C03 | Exact response after pruning/restart, one bounded result/source. |
| R03 resolve fence | C02–C03 | Both race orders and delayed push cannot commit after cancellation. |
| R04 disposition/ack sequence | C02–C03 | Atomic local disposition, lost ack/reopen, rejected/cancelled gaps. |
| R05 clean sealed delta | C04–C06 | Floor/hash, pending-outcome rejection, no audit/local-only loss. |
| R06 preserve existing intent | C03–C06 | Ordinary newer edits survive; dirty/F/C/ack block snapshot before network. |
| R07 automatic_clean/disabled | C05–C06 | Eligible completion, disabled no-jump, no history-event promise. |
| R08 limited SQLite contract | C04–C06 | Early precise restrictions plus writer-owned revalidation. |
| R09 bounded durable owners | C02–C06 | Retention/cleanup, crash/cancel, attempt/replan/work bounds. |
| R10 qualification/parity | C00–C07 | Reachable launchers, actual platforms/live/Swift, final review/ledger. |

For a smaller implementation model, assign one phase whose entry criteria have passed. Internal structure is its choice; a contradiction in wire, retention, clean admission or preservation must be reported and resolved in the specification before continuing. Do not ask it to implement the whole program autonomously in one pass.

## Approved future profile binding — 2026-10-26

For future candidates explicitly bound to the approved manifest, C06/C07/R10 qualification uses [validation section9](r0001-oversqlite-durable-recovery-validation.md) and its exact old-obligation disposition map: normal entries at most300s, approved overlaps and load maximum25users/25concurrency. The old50/100 rows are retired with zero current credit; the new25-user proof remains required. Earlier milestone checkboxes, C07-only execution amendments, failed roots and acceptance/delivery records remain historical. E40 is independently reviewed and owner-approved; E41 binds paired authority and fast checks. E42 native coverage/isolation/cleanup and E43 review/owner acceptance remain separate open gates. No R01–R09 behavior or evidence claim changes.

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
