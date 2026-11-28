# Oversqlite qualification efficiency and test portfolio

Status: **Completed and owner-accepted on 2026-10-27, with Linux x64/ARM64 explicitly deferred by the owner.** See [completion and acceptance](#completion-and-owner-acceptance--2026-10-27) for the evidence, accepted scope and deferred work.

Created: 2026-10-25. The original specification/review-only authorization was followed by the stage approvals and amendments recorded below. The owner has now accepted the implemented and reviewed work and approved postponing Linux. Earlier stage boundaries and historical receipts retain their original meaning; acceptance does not authorize commit or delivery.

## 1. Objective and authority

Reduce the cost of obtaining trustworthy Oversqlite evidence in Go and SQLiteNow. Remove verification whose result is discarded, repeat equivalent work less often, separate small semantic regressions from explicitly required scale proofs, and prepare executable artifacts once while executing required test bodies freshly.

The first implementation stage preserves current workloads, assertions, runtime coverage and qualification commands. Later stages propose explicit changes to the test portfolio and execution contract. A faster run is acceptable only when its required observations remain complete and its qualification claim is accurately scoped.

Canonical repositories:

- Go: `/Users/pochkin/Projects/my/go-oversync`.
- Kotlin/Swift/platform consumers: `/Users/pochkin/Projects/my/sqlitenow-kmp`.

This document is the single prospective design authority for this work. Existing authorities remain binding until an explicit approved amendment identifies what it supersedes:

- [Go repository instructions](../AGENTS.md) and [SQLiteNow instructions](../../sqlitenow-kmp/AGENTS.md).
- [Paired validation runbook](../../sqlitenow-kmp/docs-engineering/oversqlite-validation.md), including the reservation-remediation and current replay-workload sections.
- [r0001](r0001-oversqlite-durable-recovery.md), its [validation contract](r0001-oversqlite-durable-recovery-validation.md), especially V09/V10, and the [Kotlin companion](../../sqlitenow-kmp/specs/oversqlite-durable-recovery.md).
- [p0000](p0000-oversqlite-protocol-simplification-and-efficiency.md), [p0001](p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md), and [paired dependency-planning requirements](../../sqlitenow-kmp/docs-engineering/oversqlite-dependency-planning.md).
- [Go replay measurement work](oversqlite-replay-memory-allocation.md) and [Kotlin replay efficiency](../../sqlitenow-kmp/specs/oversqlite-replay-efficiency.md), where their measurement and instrumentation distinctions apply.

**Current C07 commands, approved envelopes and acceptance gates are unchanged.** This work adds no C07 gate and must not modify a candidate being qualified by another session. Before later implementation, establish a stable candidate or an explicitly coordinated isolated checkout. Do not fold an efficiency experiment into a running qualification campaign.

## 2. Evidence and limits

The [independent investigation](../.artifacts/durable-recovery-final-20261024/qualification-radical-efficiency-investigation.md) and [timing sample](../.artifacts/durable-recovery-final-20261024/qualification-radical-efficiency-measured-timings.json) establish campaign-level duplication. The follow-up inspection read source and retained XML/Go JSON without running tests. These observations motivate work; they are not a fresh performance baseline or a complete minimality proof for the suite.

| Observation | Evidence | Interpretation |
| --- | --- | --- |
| Five executions contain the same 706 comprehensive case identities. | Standalone066, Swift068/087/090 and live Swift095 in the timing sample; 1,861.020 cumulative testcase seconds. | Framework identity is not execution equivalence. The local census includes 669 executed passing bodies, 13 explicit skips and 24 guarded returns. About 24m52s above the first observed execution is testcase time, not established removable campaign wall time. |
| Seven cases account for 231.242 of 369.057 comprehensive testcase seconds, or 62.7%. | [Comprehensive066](../.artifacts/durable-recovery-final-20261024/kmp-comprehensive-066/), excluding archived prior outputs. | Optimize measured concentrations before pursuing a smaller test count. Crash boundaries, socket waiting and continuation work have different purposes. |
| Four replay cases account for 217.818 of 320.224 testcase seconds on macOS. | [macOS101 native results](../.artifacts/durable-recovery-final-20261024/kmp-live-macos-focus-101-native/). | 68% of this selected live sample; not 68% of a full matrix. The 10K/1024 case takes 136.525s versus 6.386s in [JVM100](../.artifacts/durable-recovery-final-20261024/kmp-live-jvm-focus-100-native/). Phase attribution remains unmeasured here. |
| Replay performs repeated complete fingerprints, including results subsequently discarded. | [OrdinarySafetyLiveTest](../../sqlitenow-kmp/platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/realserver/OrdinarySafetyLiveTest.kt), `runCommittedReplay` and its independent-readback helper; [DatabaseStateAssertions](../../sqlitenow-kmp/platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/DatabaseStateAssertions.kt). | With one business table the helper enumerates **22 tables**. Source tracing gives approximately seven passes over three large backlog tables on the native reopen path: about 210K row hashes for a 10K case, plus smaller rows. This is a static estimate, not an instrumented measurement. |
| Go reservation completion takes 80.77s, of which targets 1/8/17 consume 76.14s. | [Go full108 log](../.artifacts/durable-recovery-final-20261024/go-full-108.log), `TestSettlementReservation_PublicCompletion`; client package elapsed 211.134s. | A concentrated fixture/continuation cost, about 38% of package elapsed in this invocation. The exact 125-row and 1,000-row fixtures are already prescribed. |
| Kotlin automatic/explicit 2,000-row continuation cases take 19.664/22.148s. | Comprehensive066; [ConflictReservationContinuationJvmTest](../../sqlitenow-kmp/library-oversqlite/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/ConflictReservationContinuationJvmTest.kt). | Exact 2,000-row fixtures are prescribed by the reservation-remediation runbook. Smaller replacements need an amendment. |
| Preparation-checkpoint verification executes 6 phases × 2 reuse modes × 20 budgets = 240 subcases. | [replay_statement_cleanup_test.go](../oversqlite/replay_statement_cleanup_test.go), `TestReplayStatementCleanup_PreparationCheckpoint`; full108 elapsed 11.01s. | Repeated coarse phase/error classifications suggest a boundary-focused replacement. They do not prove equivalent cursors, committed mutations, cleanup or resumed behavior. |
| The memory subclass inherits an unchanged trigger-restoration test. | [SettlementMemoryTest](../../sqlitenow-kmp/library-oversqlite/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/SettlementMemoryTest.kt) and [SettlementTest](../../sqlitenow-kmp/library-oversqlite/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/SettlementTest.kt). | `triggerRestoration` bypasses the overridden measurement method and adds no instrumentation; sampled duplicate cost 0.041s. `backlogBound` adds memory sampling and is distinct evidence, costing 6.395s. |
| A socket regression deliberately waits 40s. | [BundleChangeWatchSocketTimeoutTest](../../sqlitenow-kmp/library-oversqlite/src/jvmAndroidTest/kotlin/dev/goquick/sqlitenow/oversqlite/BundleChangeWatchSocketTimeoutTest.kt), two 20s sleeps; comprehensive066 elapsed 40.034s. | A real elapsed-time witness, not slow row verification. Shortening its waits changes the proof unless an accepted replacement preserves it. |

The earlier conversational estimate of 21 fingerprint tables is corrected to 22. Ten matching live scenario names in the direct-library and platform suites are not ten proven duplicates: direct construction and coordinated opening/storage exercise different boundaries. Ordinary/heavy, uninstrumented/memory-observed, Go/Kotlin, and different drivers or transports must not be collapsed on name similarity alone.

At authoring, Go HEAD was `4bb0629bdc40a353164fd0aa10156cdbd1490e71` and Kotlin HEAD was `c228cb3453917c59d355d6c756bb01a52b408d43`; both worktrees contained existing changes. These are orientation identities, not frozen implementation inputs. Later work must capture complete tracked/untracked baselines and relevant ignored authority. Historical evidence must use its own receipt-bound source; do not attribute old timings to current source merely because a test name matches.

## 3. Scope and exclusions

Proposed scope is test/support verification, fixture design, test selection, build/test separation and qualification tooling in the two repositories. Initial targets are the named owners above, their actual consumers, and existing qualification tools such as [oversqlite_smoke.py](../../sqlitenow-kmp/tools/qualification/oversqlite_smoke.py). Reuse existing ownership, cleanup and native-result adapters before introducing another runner.

Excluded: product algorithm optimization, wire/schema/API/default changes, disabling durability, changing provider or build type to improve a result, random record sampling, deleting assertions to satisfy a deadline, real accounts/data, application database resets, hosted execution that evades existing guards, retired 50K replay workloads, automatic evidence acceptance, and changes to historical receipts. Commit, push, release and deployment remain separate gates.

No target reduction in test count is prescribed. No 10×, one-hour or other end-to-end performance promise is established. Campaign time, native command time, testcase time and measured public-operation time are separate quantities.

## 4. Verification equivalence requirements

### Q01 — Explicit observations before optimization

For each selected expensive test, map the requirement to: setup; action; exact expected state or error; observation point; rows/columns/storage types; process/connection/storage boundary; required instrumentation; and cleanup. Include intermediate committed progress, refusal precedence, unchanged owners, source/checkpoint, frozen upload/outcome/ack, cancellation and reopen when applicable.

Classify proposed work as one of:

1. **Same proof, less execution:** avoid computing unused values or repeat an observation only once within a demonstrably unchanged state.
2. **Equivalent portfolio replacement:** replace tests/parameters with reviewed witnesses covering every old obligation.
3. **Coverage or qualification-policy change:** change required workload, runtime/profile, command, deadline, repetition or temporal witness.

The latter two require an explicit reviewed disposition before old evidence is removed from the selected profile. Approval can cover a coherent stage; do not create a permission cycle for every mechanical helper edit within that approved scope.

### Q02 — Avoid unused scans without narrowing the assertions

Allow callers to select the exact tables they consume before SQL projection, sorting, row transfer or hashing. Preserve the current all-table behavior for callers requesting a complete vector. Unknown/missing selected tables, required columns, query/scan failures and close errors must fail the verification. A result for a selected subset must not be described as complete database preservation. The existing helper discovers columns with `PRAGMA table_info`; it does not by itself prove a fixed schema. Preserve that distinction and use the owning schema assertion when an exact schema is required.

The new selection contract rejects an empty selection, duplicate names and names outside the available business/internal table roster. Table names remain exact tokens. Preserve each selected table's existing column projection, row ordering, predicates and charge/staging options. Return exactly the requested table keys; a required key cannot be omitted merely because its table is empty. Existing full-vector callers retain their behavior. Keep the API private to test support; choose an additive helper or parameter without a product API change.

For `runCommittedReplay`, eliminate the large-table scan performed solely to produce `settled` values when the consuming assertions use only identity/staging tables. Preserve the later complete before/after independent-readback proof. Similarly, apply table selection before `filterKeys` discards data. The initial conversions are exactly the two unrelated-intent vector calls and the final identity/staging-only `settled` call. Keep the complete `before`, reopen comparison and independent-readback vectors. Change no other caller in P1 unless source inspection demonstrates that these three sites no longer have the stated roles; in that case stop and revise the observation map before editing.

The exact selected sets and retained full-vector boundaries are:

| Converted call | Selected tables | Full-vector boundary covering omitted tables |
| --- | --- | --- |
| Initial unrelated-intent capture | `categories`, `_sync_row_state`, `_sync_dirty_rows`, `_sync_relationships`; preserve their existing predicates. | The complete `before` vector immediately preceding this call. |
| Post-replay unrelated-intent comparison | The same four tables and predicates. | The complete original-database vector captured at the start of `verifyIndependentReadback(localSettled = true)`, before peer operations. |
| Final `settled` capture | Identity: `_sync_opening_state`, `_sync_contract_identity`, `_sync_admitted_contracts`, `_sync_managed_tables`. Staging: `_sync_snapshot_stage`, `_sync_apply_plan`, `_sync_apply_rows`, `_sync_apply_edges`, `_sync_push_plan`, `_sync_push_nodes`, `_sync_push_edges`. | The same complete original-database vector at the start of independent readback. |

E01 must map incidental table-existence, query, scan and close-failure checks as well as visible equality/assertion consumers. For each omitted table, verify that the named full boundary covers its original check and that no intervening original-database mutation, client operation, reopen or concurrent actor can invalidate that coverage. The current intervening assertions and queries are read-only; P0 must verify this against its frozen source and actor ownership. Preserve full scans wherever that proof fails, rather than silently dropping the obligation. E11 must include a small missing-owner/query-failure control or an explicit equivalent-coverage argument tied to these retained boundaries. Do not add a second expensive oracle by default.

Share a previously captured observation only when no relevant write, client operation, callback, concurrent actor, transaction transition, close/reopen, cancellation or recovery boundary can have changed the asserted state. Do not cache the actual side of a before/after preservation assertion. Prefer local variables with explicit lifetimes to a global fingerprint cache or invalidation framework.

Keep full required rows and typed values. Preserve NULL, BLOB bytes, text/Unicode, integer/real storage distinctions, counts/multiplicity, schema/owner identity and deterministic ordering. Do not replace a complete comparison with counts, selected IDs or a sample. Preserve current predicate semantics and declared exclusions such as derived charges; do not silently expand exclusions.

A helper redesign must retain streaming/bounded payload handling. Do not materialize the full backlog merely to share two scans. Any selected-table API or combined-pass representation is an implementation choice subject to these invariants.

### Q03 — Verify the verifier

Compare the old and proposed verifiers on small controlled fixtures, then run the affected real workloads with the replacement. Retain the old oracle as test-only reference where needed; do not double-run both expensive oracles permanently in every qualification case.

Required negative controls include row insertion/deletion, a changed value outside any convenient prefix/sample, type-only changes, NULL/empty/BLOB distinctions, Unicode, multiplicity, missing owner/schema, and relevant row ordering. Match old behavior for enabled predicates and exclusions. Add deliberate drift between observation points and across physical reopen to show that reuse cannot hide it. Use independent expected values as well as differential equality, so two identically wrong implementations cannot pass by agreeing.

Measure selected tables, rows visited/hashed and bytes processed in a bounded test-only diagnostic. Prove discarded tables are not queried or hashed within each optimized subset call and every selected required row still is. Separately retained complete boundary vectors continue to scan their full roster. Keep diagnostic overhead separate from ordinary timing.

If phase measurements identify hashing as material, a platform-accelerated SHA-256 implementation may be proposed separately. It must produce identical digests for identical input bytes, preserve record framing/order and all platform semantics, and have independent known-answer tests. Algorithm replacement is not a prerequisite for removing unused scans.

## 5. Test portfolio and workload decisions

### Q04 — Replace numeric sweeps with witnessed boundaries

For Go `TestReplayStatementCleanup_PreparationCheckpoint`, characterize all six phases under reuse enabled/disabled. Map the current twenty budgets to actual internal transition, durable rows/edges/cursor/counters, pending/error/success outcome, statement cleanup, physical reopen and resumed completion. The existing positive `mutatingProgress` assertion must remain represented in every phase/reuse family.

Propose the smallest explicit parameter set retaining all distinct obligations, including threshold-minus-one/exact/plus-one where meaningful, no-progress refusal, useful committed pending progress and completion. Do not infer equivalence from the log's phase name and error class alone. Expected boundaries must come from the contract or an independently reviewed cost calculation; tests must not read a newly observed implementation threshold and automatically accept it.

Keep a broader sweep as an explicitly classified diagnostic only if it catches distinct failures or has justified exploration value. Its new schedule must be part of the accepted profile; moving it out of the aggregate is not automatically authorized.

### Q05 — Separate semantic fixtures from scale proofs

The proposal may add smaller development regressions for Go reservation completion and Kotlin automatic/explicit conflict continuation. They must cross the relevant useful-progress, transaction, reservation-release and cancellation/reopen boundaries and retain exact state assertions. Demonstrate detection of the historical accumulation/continuation failure through a controlled faulty variant or equivalent negative control.

Keep the full-size qualification fixtures until an explicit amendment replaces or reclassifies them. The current runbook names Go eight-table/125-row/upload-40 cases at targets 1/8/17/default plus default 1,000 rows, and Kotlin common 64-row/4,096-entry cases plus JVM 2,000-row self-FK automatic/explicit cases. These are not arbitrary constants available for routine cleanup.

Preserve 1K/10K × 16/1024 replay proofs, target 1/8/17/default obligations, default-limit cycle refusal, retained-database cycles, and the fourteen-table/3,399-row historical-shape witness wherever the governing selection requires them. Smaller fixtures may supplement these as fast regressions; replacing them needs a statement of which scaling/default-limit claim remains and where it is executed. The full Cartesian product need not be assumed necessary forever, but its replacement must be approved before changing coverage.

### Q06 — Remove genuine duplicates; retain distinct entry paths

Propose removing the inherited `SettlementMemoryTest.triggerRestoration` execution while keeping the ordinary trigger-restoration owner. Preserve the memory-instrumented backlog test and distinguish its evidence from ordinary timing. Update test discovery/census expectations rather than accepting an unexplained count reduction.

Shared scenario code may reduce maintenance duplication between direct-library and coordinated-platform tests, but both required entry paths must still execute. A shared helper cannot make two independent assertions agree by reusing the same wrong expected value. Preserve driver, HTTP, worker, lifecycle and storage differences in the coverage inventory.

### Q07 — Preserve the temporal socket proof

Investigate whether a short real-socket regression with deliberately small caller timeout, delayed heartbeats and an observed production watch-timeout override can detect the original timeout defect. Keep ordinary-request timeout preservation and prompt cancellation/connection closure tests. An in-memory transport or virtual clock alone cannot qualify the real socket engine.

Retain the existing two-real-heartbeat-duration test in its current required profile until the owner accepts either equivalent replacement or an explicit separate transport qualification schedule. Demonstrate that the proposed fast test fails when the watch override is removed. Do not change product timeout values or merely shorten sleeps and retain the old test's claim.

## 6. Execution efficiency after test-level improvements

### Q08 — Prepare immutable artifacts once; execute bodies freshly

Pilot one web target and one Apple target, after their prerequisites and source are stable. Record artifact, generated-input, toolchain, runtime, server and relevant environment dependencies. Reuse compilation only while these match. Native test execution must still occur against fresh owned state, with exact body start/terminal/assertion evidence and intact raw reports.

Inspect nested Gradle Exec wrappers: an outer rerun flag does not prove freshness of every child, and forcing every dependency rebuild is not necessary to make a leaf fresh. Do not replace mandated flags/commands before the corresponding profile is approved. A cached test result, stale XML, zero selector match, early guarded return or explicit skip earns no fresh-body credit.

Prefer existing task execution with correct inputs before inventing direct-artifact adapters. If direct execution is needed, prove it includes the same assets, initialization, generated consumers and runtime options as the original task.

### Q09 — Reviewed coverage union, not command-label counting

Prepare a prospective matrix mapping every old mandatory command to required test/scenario entries. An entry binds assertion obligations, runtime/provider/transport, direct/coordinated/generated caller, workload, configuration, instrumentation, relevant dependency identities, state/authority isolation and deadline. Matching class/method names alone never establish equivalence.

A proposed union may remove package/full or comprehensive/local-Swift/live-Swift repetition only when the old obligations are all present. Ordinary and heavy configurations remain separate where workload, branch, timing or browser gating differs. Keep audit build tags, nested mobile modules, Swift caller cancellation, actual process/worker death, supported database versions and resource/load proofs.

Coverage remains manually selected and reuse reviewer-confirmed. Retain failed attempts and explicitly resolve each failed required entry; do not combine intermittent passes into a clean result or count incompatible source revisions as one candidate. A failed root can contain useful reached evidence only where the accepted profile permits that reuse. The failed root remains failed.

The amendment packet must name exact old/new commands, selectors, budgets, trigger and entry-to-requirement mapping. Update affected V10/r0001/p0000/p0001, paired companion/runbook and authority/corpus pins together when adoption is approved. Recheck the fast paired contract gate before database-heavy work if those bindings change. This draft does not itself activate the union.

### Q10 — Consolidate the harness; parallelize isolated work

Extend existing fixture ownership, readiness, deadline, cleanup and result-reading code. Keep machine readiness on a private structured channel separate from redacted logs. One current ledger should derive summaries from immutable attempt records; parser-only fixes should replay saved raw output when sufficient, rather than rerun passing bodies.

Test absent/duplicate bodies, wrong selectors/native class prefixes, guarded returns, truncated readiness, stale outputs, changed inputs, partial results, failure/cancellation and cleanup leaks. Native process output and assertion witnesses remain the execution authority; a wrapper's exit status is insufficient.

Only then pilot two isolated workers. Separate mutable build/output directories, PostgreSQL databases/listeners/application authorities, storage roots, ports and devices. A unique account is insufficient if a reset endpoint affects the whole server. Serialize work that shares such resources. Benchmark contention and respect host memory/thermal constraints and supported heavy-execution guards. Expand concurrency only on measured benefit; two workers do not promise 2× wall-time reduction.

## 7. Stages, deliverables and authorization

All stages below are **completed for the owner-approved scope**, with the Linux exception recorded in the final acceptance section. P0/P1 delivered the verifier improvements, P2 delivered the reviewed proposal and accepted replacements, P3 delivered the preparation pilot, and P4 delivered profile adoption and the selected rehearsal. The table retains the stage authorization boundaries; implementation, independent review, owner acceptance and delivery remain separate. Evidence for each completed stage is linked in the final acceptance section.

| Stage | Deliverable and exit | Boundary |
| --- | --- | --- |
| P0 — Baseline and plan | Current affected-test inventory; observation/coverage map; historical source bindings; measured or eligible reused phase costs; exact focused command/deadline/runtime plan; explicit approved scope. | Read-only planning and separately authorized bounded calibration. No test or contract reduction. |
| P1 — Verification efficiency | Q01–Q03: selected-table/observation optimization, verifier controls, unchanged-workload affected-runtime evidence, before/after scan counts and timings. | Test/support implementation only. Preserve current commands, workload sizes, body obligations and ordinary/instrumented distinction. Stop at reviewable candidate. |
| P2 — Portfolio proposal and approved replacements | Q04–Q07: old/new obligation map, explicit candidate parameters and schedules, negative controls and required authority amendments. Implement only the reviewed replacements approved by the owner. | Exact fixture sizes, temporal claims, test removal and moving tests between profiles need explicit disposition. Approval of P1 is not approval of P2. |
| P3 — Build and runner pilot | Q08 and the bounded harness portion of Q10: one web and one Apple adapter, fresh native census, ownership/error controls and measured preparation savings. | Approved pilot commands only; no automatic replacement of full-profile gates. |
| P4 — Full-profile adoption | Q09 and isolated Q10 execution: approved union matrix, paired authority update, two-worker rehearsal, complete coverage/cleanup audit and measured critical path. | Requires prospective qualification-profile approval. No retroactive reinterpretation of C07 or automatic acceptance. |

Prefer accepting useful P1 results independently of P2–P4. Do not require a large scheduler, automatic impact inference or a campaign-wide rewrite to deliver the first improvement.

### 7.1 First-slice scope and artifacts

P0 reads both repositories. P1 changes Kotlin test/support source only; Go product/tests, both build configurations, shared hashing implementations and the active governing specifications remain read-only. The P1 source allowlist is:

- `platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/DatabaseStateAssertions.kt`.
- `platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/DatabaseStateVectorTest.kt` (extend the existing verifier tests and reference oracle).
- `platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/realserver/OrdinarySafetyLiveTest.kt` (only the three Q02 conversions and bounded test-only measurements).

Keep measurement support in these test owners where practical. A necessary extra test-support path must appear in the P0 plan before edits; a broader runtime/runner/build change belongs to P3 or a revised scope. Do not patch historical campaign controllers to make a P1 lane reachable.

Create one new ignored output directory per implementation campaign, outside the historical C07 directory. Keep one `ledger.md`, `plan.json`, `coverage.csv`, paired baseline inventories, immutable `attempts/` results and a final `handoff.md`; add small verifier/performance tables when needed. Reuse existing schemas/tools where compatible. `plan.json` must contain stage authority, scope, source/dependency hashes, native owner/selector inventory, opt-ins and non-secret environment identity, commands as argv arrays, cwd, nested deadlines, expected bodies/guards, resource ownership and cleanup, lane disposition, and measured or unknown costs. `coverage.csv` maps requirement → observation → old/new test/configuration → evidence. These are evidence formats, not a new public planner API.

Capture ignored authority/spec hashes as well as tracked/untracked contents. Copy baselines/preimages outside indexed source scopes. Pin this specification by SHA-256 in the plan. A new session validates that packet rather than reconstructing the old campaign. No historical HEAD in section 2 grants permission to overwrite current work.

### 7.2 Actionable work items

The checked items below reconcile the completed outputs and reviewed validation. Proposal completion does not imply every proposed replacement was adopted: E20 retained the original checkpoint sweep, and E42 closes the accepted selected rehearsal with Linux explicitly deferred. See the final acceptance section for those dispositions and evidence.

**P0: establish the runnable boundary.**

- [x] **E00 — Capture authority and ownership.** Record stage authorization, both current worktree/index identities and dirty provenance, ignored authority hashes, active campaign conflicts and the P1 allowlist. Exit: source overlap is resolved and the baseline can be reproduced without restoring another session's files.
- [x] **E01 — Discover consumers and observations.** Enumerate `databaseStateVector` callers from current source, expand each to actual tests/subcases/runtime guards, and map the three Q02 sites to every retained assertion and incidental failure check, including omitted-table coverage at the named full boundaries. Seed consumers include `DatabaseStateVectorTest`, `OrdinarySafetyPlatformTest`, `OrdinarySafetyLiveTest`, `RealServerHarnessTest`, `ProcessDeathRecoveryJvmTest`, `WorkerCrashReplayTest` and `MigratedOversqliteStateTest`. Exit: `coverage.csv` classifies each owner fresh/reused/not-applicable with a reason; grep hits alone are not a complete test inventory.
- [x] **E02 — Bind runnable lanes.** Fill `plan.json` with section 8.1's tasks, exact selectors/native names, all activation flags, authority/device/storage prerequisites, deadline tree and cleanup. Inspect retained runners and current task definitions; validate selector completeness against discovery. Exit: no unknown mandatory owner or unresolved runner capability; no placeholder command can launch.
- [x] **E03 — Freeze the comparison baseline.** Bind retained timing source when eligible, otherwise calibrate only the missing selected family within its existing ceiling after validation authorization. Record preparation/setup/public-call/verification/cleanup separately or as unknown. Exit: the selected serial plan fits its ceilings, or the dependent lane is blocked with a concrete amendment proposal.

**P1: remove discarded verification work while preserving coverage.**

- [x] **E10 — Add subset observation support.** Implement Q02's private selection contract without changing full-vector results, typed projection, existing predicates/options or resource cleanup. Exit: full and subset results match the existing oracle for their respective keys; invalid selection and missing tables fail.
- [x] **E11 — Strengthen verifier controls.** Extend the existing verifier test class with Q03 controls, selection/options cross-products, selected empty-table handling, unselected-table non-access within subset calls, error/close/cancellation cleanup and fresh observations after deliberate writes/reopen. Include Q02's omitted-table missing-owner/query-failure control or explicit equivalent-coverage argument. Exit: independent expected values and the reference oracle agree, and each injected corruption is detected.
- [x] **E12 — Convert the three callers.** Use the four unrelated-intent tables at both corresponding call sites and the final identity/staging table set at `settled`. Preserve all complete boundary comparisons and every original assertion. Exit: the observation map has no dropped obligation and structural diagnostics show zero row projection/hash work for discarded tables at these sites.
- [x] **E13 — Qualify affected consumers.** Execute the selected section 8.1 lanes, reconcile actual bodies and guards, and record reviewed reuse for unchanged paths. Exit: required runtime evidence, physical reopen or explicit unsupported classification, live authority/readback and cleanup are complete. A common helper regression blocks all dependent claims.
- [x] **E14 — Measure and hand off.** Run section 8.2's bounded matched experiment, record scan reduction and separate timing conclusions, freeze the candidate and prepare `handoff.md`. Exit: independent implementation review can reproduce the scope/evidence; performance may remain inconclusive without concealing it. Stop for review/acceptance before P2 runtime changes.

**P2: design and implement only accepted portfolio replacements.**

- [x] **E20 — Map checkpoint-sweep transitions.** Produce the six-phase/two-mode/twenty-budget characterization, including committed cursor/counter transitions and each `mutatingProgress` witness. Propose explicit replacement values and retained diagnostics under Q04. Exit: every removed combination has a supported equivalence or policy disposition.
- [x] **E21 — Specify continuation fixtures.** Propose concrete smaller fixtures, their threshold-crossing/negative controls, retained large-fixture schedule and exact runbook amendments for both languages. Exit: no unspecified row count or missing scaling/default-limit claim; full prescribed fixtures remain unchanged pending approval.
- [x] **E22 — Specify duplicate and socket changes.** Record the exact inherited trigger owner to remove, preserved memory observations and new discovery counts. For the socket alternative, specify real engine, caller/watch timeouts, delays, default-value assertions, original-defect negative control and full-duration disposition. Exit: JVM/Android and timing claims are explicit; a failed equivalence study retains the current long test.
- [x] **E23 — Obtain replacement approval, then implement.** Present one coherent old/new coverage and authority diff for E20–E22. Apply only accepted rows, update discovery/manifest expectations, and run owning/affected-runtime controls. Exit: independent review confirms the approved replacement proofs and recorded performance; rejected proposals remain documented without source removal.

**P3: reuse preparation and existing harness components.**

- [x] **E30 — Select pilot adapters.** Choose JS browser and macOS ARM64 as initial web/Apple targets, subject to current availability. Inventory `planner.py`, `campaign.py`, `oversqlite_smoke.py` and their tests; state what is reused and each exact gap. Exit: no parallel competing harness and no hidden dependence on historical script mutation. Another target needs a documented scope decision before its pilot.
- [x] **E31 — Bind artifact freshness.** Implement Q08's build/runtime input closure and native body verification using approved pilot commands. Exit: unchanged inputs reuse compilation, changed fixture/environment/dependency inputs invalidate the right work, and both invocations run fresh required bodies on owned state.
- [x] **E32 — Exercise runner failure controls.** Cover missing/extra/duplicate cases, guards/skips, stale output, truncated readiness, wrong identity, interruption and cleanup failure. Replay retained native samples where sufficient; use controlled processes for lifecycle controls. Exit: unsafe evidence cannot produce a pass and owned resources are reaped.
- [x] **E33 — Measure pilot savings.** Compare ordinary preparation/execution with the approved pilot using section 8.2. Exit: equivalent coverage and cleanup, timing split and limitations are reviewed; current mandatory profile commands remain unchanged.

**P4: adopt a reviewed future full profile.**

- [x] **E40 — Produce the union amendment.** Map all existing mandatory commands to Q09 entries, proposed commands and exact deadline/trigger changes; retain build/support checks as well as test bodies. Replay retained evidence without accepting it for a new source. Exit: no uncovered entry and no implicit ordinary/heavy or instrumented/ordinary subsumption.
- [x] **E41 — Approve and bind authority.** Obtain owner approval of the precise future profile; update only its listed paired specs/runbook/manifests/pins and pass applicable fast contract checks. Exit: the new profile has a reproducible identity and does not reinterpret historical C07.
- [x] **E42 — Pilot isolation and rehearse.** Start with two workers, demonstrate authority/build/device/storage separation and cancellation cleanup, then run the approved matrix. Exit: complete native coverage, preserved failures, cleanup, critical-path timing and contention observations. Do not expand worker count on an unmeasured assumption.
- [x] **E43 — Review and close the adopted profile.** Freeze source/tooling/authority and give an independent reviewer the exact union and attempt ledger. Exit: review findings are resolved, owner acceptance is separate and delivery remains unperformed unless explicitly authorized.

## 8. Validation and performance reporting

### 8.1 Required validation selection and command binding

Before implementation, enumerate the complete affected helper/test consumers, including retained-state fixtures, workers/providers, cancellation/reconnect and process-death owners. Select focused development and maintenance lanes under the runbook. A helper change invalidates every consuming assertion path reached by the change; it does not automatically invalidate unrelated server database suites.

For P1, start with small verifier controls and the cheapest owning runtime. Then cover each affected supported runtime/provider using the required current replay configurations and meaningful ordinary/heavy modes. Preserve actual reopen and independent-server readback where required. For Go portfolio changes, use Go 1.25.12, owning focused/package evidence and affected race instrumentation. For socket changes, exercise both JVM and Android consumers. A host pass is not platform qualification.

P0 must supply exact runnable commands and existing governing body/package/outer/cleanup ceilings before expensive validation. Assess aggregate feasibility from constituent timings and stop on a known mismatch; do not invent a larger deadline or start an inevitably timing-out root. Retain separately mandatory aggregates unless an approved amendment explicitly replaces them. P0 command binding instantiates current environment/runner details; it cannot change this specification's required observations or silently accept a different profile.

The P1 runtime roster is JVM, Android, macOS ARM64, iOS simulator ARM64, JS Node, JS browser and Wasm browser. Each executes the updated verifier controls and its applicable changed replay cases. Preserve existing heavy opt-ins and browser gating; the four 1K/10K × 16/1024 configurations remain mandatory on each supported replay runtime where the accepted profile requires them. Do not claim physical persistence on transient Node. Existing body limits, including the replay body's 900s limit, remain intact. Missing runtime access blocks that runtime's qualification rather than reducing the roster.

| Lane | Task/selector seed and activation | Acceptance |
| --- | --- | --- |
| Verifier controls | From Kotlin, `:platform-oversqlite-test:composeApp:jvmTest`, `macosArm64Test`, `iosSimulatorArm64Test`, `jsNodeTest`, `jsBrowserTest`, `wasmJsBrowserTest`, or `connectedAndroidDeviceTest`; select `dev.goquick.sqlitenow.oversqlite.platform.DatabaseStateVectorTest`. | Exact discovered methods and expected per-runtime guards, plus new negative controls; local mock fixture only. |
| Changed live replay | Same runtime tasks; select all four `dev.goquick.sqlitenow.oversqlite.realserver.OrdinarySafetyLiveTest.committedReplayWith*` methods. | Exact native identities, original assertions, required sizes/widths, owned categories authority, complete before/after/reopen/readback evidence and cleanup. |
| Existing full-vector consumers | E01's discovered methods in the seven seeded consumer classes, on applicable runtimes. | Execute paths affected by implementation changes; reviewer-confirmed reuse is allowed only for unchanged dependencies/behavior, not simply because an API default stayed the same. |
| Later Go portfolio changes | Go 1.25.12; exact affected owners `TestReplayStatementCleanup_PreparationCheckpoint`, `TestSettlementReservation_PublicCompletion` and adjacent cleanup/cancellation; then selected owning package evidence. | Fresh native subtests and required failure/continuation/reopen assertions; apply the accepted stage's package/outer budgets, not C07-only exceptions. |
| Later memory/socket changes | `SettlementTest`, `SettlementMemoryTest`, `ConflictReservationContinuationJvmTest` and `BundleChangeWatchSocketTimeoutTest` in their owning library tasks, as affected. | New discovery census, retained memory evidence, exact continuation witnesses and real JVM/Android socket behavior. |

Only the JVM/macOS/iOS task argument forms are assumed to support the existing `--tests` selector route; verify the actual registered task before using it. Android uses instrumentation class/method arguments and runtime properties; web uses its current verified filter/environment forwarding. Bind those adapters from current source/native inventories, not guessed command syntax or a package selector presented as one method. Include `OVERSQLITE_PLATFORM_TESTS` for verifier controls; live replay needs `OVERSQLITE_REALSERVER_TESTS`, the required heavy flag and explicit `OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL` (plus other fixture URLs actually reached). Forward simulator values through `SIMCTL_CHILD_`, Android values through instrumentation properties, and web values through its worker/environment adapter. Require activation witnesses so omitted flags cannot yield silent passes.

Before any lane launches, its plan row must name a validated ownership/cleanup runner, native command, preparation/readiness/body/outer/cleanup deadlines with governing source, expected method set, authority/server artifact, isolated database/storage and device identity, and how raw outputs are saved before another run replaces them. Existing C07 focus runners are source examples, not automatically reusable commands or transferable approvals. If the available runner cannot meet these requirements without code changes outside P1, stop that lane and prepare the bounded P3 prerequisite; do not invent an ad hoc controller during qualification. Complete unaffected P1 work and report the blocked evidence without claiming P1 qualified.

### 8.2 Bounded performance experiments

For P1, select exactly one paired replay experiment: macOS ARM64 at 10K/1024, where the retained sample shows high verification cost. Run one initial correctness/preflight case at 1K/16 before that experiment if equivalent completed evidence is unavailable. The full correctness roster above is separate from the timing sample. Structural scan accounting uses a small controlled fixture on JVM and an affected native/web adapter as needed; do not run three timing pairs on every runtime or every test. If macOS is unavailable, propose another evidenced runtime before timing; do not claim the macOS result from JVM. An expensive baseline which cannot fit its inherited body/outer ceilings blocks the comparison pending a documented decision.

For that selected experiment, use three matched baseline/candidate pairs, alternating order A→B, B→A, A→B, within a predeclared experiment budget. Freeze inputs and keep workload, build type, hardware, process startup, fixture storage/durability, server and instrumentation policy equal. Use isolated copies if another campaign owns the working checkout. Report cold preparation and warm execution separately. Reuse compilation where eligible, never test results. Do not measure speed with both expensive comparison oracles enabled. Reuse a qualifying experiment invocation as a correctness leaf when its dependencies/selector/observations match and the governing profile allows it; do not repeat it merely to fill two labels. P2–P4 select similarly bounded representative comparisons in their approved plans, not automatic three-pair full-matrix repetitions.

Report setup, public operation, verification, cleanup and wrapper elapsed separately; phase instrumentation may use a separate diagnostic run. Record scanned tables/rows/bytes, actual cases/subcases, required witnesses, skips and failures. The structural success criterion for P1 is removal of the identified discarded scans with identical retained observations. Report a timing improvement only if at least two of three matched pairs and the median improve; otherwise classify it as mixed/inconclusive without changing workloads to manufacture a gain. This is directional evidence, not statistical significance or an SLA.

For P2, show the old and replacement coverage maps plus deliberate failures detected, and explain any change to scale or temporal claims. For P3/P4, record fresh-body equivalence and full critical-path elapsed including setup/cleanup, not the sum of overlapping commands. Historical correction/approval gaps are not baseline execution time. Keep performance attribution bounded to measured components.

Stop a failed aggregate, diagnose its owning family, and preflight unreached families before an eligible retry. Require a diagnosed cause and corrective change or changed environment. Preserve failed/superseded receipts. At 45–60 minutes give completed criteria, remaining mandatory/untimed work, blockers and a measured revised estimate. Do not add repeated review packets for mechanically unchanged controls.

### 8.3 Evidence invalidation and review

Changing projection, ordering, hash bytes or shared full-vector execution invalidates all reached consumers, including process/worker/reopen paths. A new isolated subset helper with an unchanged full-vector implementation may retain unrelated consumer results only after dependency review. Changing only the three live caller selections invalidates their live bodies and verifier controls; it does not invalidate unrelated server database or Go product results. A runner-only collector correction may replay immutable raw outputs, while changed execution/cleanup behavior needs fresh affected adapter evidence. Authority-only edits refresh bindings and contract checks; rerun product suites only when their requirements or behavior changed.

Preserve one immutable raw attempt per invocation. Each checked checklist item points to that evidence or its reviewed reuse rationale in the ledger. Independent implementation/evidence review is required at stage exit, with one coherent correction batch per set of findings; individual assertion edits do not each require a new review campaign.

## 9. Completion and handoff checklist

- [x] Owner-approved stage scope, exact validation profile and paired baseline are recorded.
- [x] Each implemented change maps to Q01–Q10 and a concrete observation or execution saving.
- [x] Retained assertions, workloads, runtime paths and independent instrumentation are accounted for; replacements have explicit approval.
- [x] Verifier/runner negative controls fail for the intended faults and pass after correction.
- [x] Actual execution, scan reduction, timing, cleanup and limitations have current evidence.
- [x] Required authority/corpus updates and affected consumer checks are complete for any adopted profile change.
- [x] One ledger lists fresh, reused, subsumed, not-applicable, blocked, superseded and rerun-required lanes with dependency reasons.
- [x] Independent review findings are resolved or retained as blockers; owner acceptance is recorded separately.
- [x] Handoff names source/spec identities, exact commands/receipts, remaining work and stop conditions. Commit/delivery status is explicit.

Stop dependent work on unresolved source overlap, missing authority, incomplete native body evidence, unproved replacement coverage, infeasible deadlines, or resource-ownership/cleanup failure. Leave unrelated work and historical campaigns intact. Documentation-only work on this draft requires document/link/identity checks, not database suites.


## E40/E41 owner disposition — 2026-10-26

The owner approved the corrected E40 packet `qualification-efficiency-p4-proposal-20261026T075558-review-fixes-v2` after its final
independent AI rereview found no blocking findings. Original proposal/review
inputs and correction preimages remain frozen. This activates E41 across both
repositories: the exact listed paired authority/runbook/manifest/pin changes
and fast Go, common metadata and focused JVM corpus checks. The approved profile
is `tools/qualification/profiles/oversqlite-full.json` in SQLiteNow, governed by
[r0001 validation section9](r0001-oversqlite-durable-recovery-validation.md).

E41 evidence and approval are in [the adoption packet](../.artifacts/qualification-efficiency-p4-adoption-20261026T082040/owner-approval.json).
The originally pinned specification preimage remains in that packet. P0/P1,
the approved P2 batch and P3 delivery checkpoints remain completed despite the
old authoring checkboxes. E40 is complete. E41 authority/identity and fast-check outputs are complete; its adoption ledger
records22 top-level Go tests,30 JVM fixture methods and successful common metadata.
The Kotlin checks used300s operational wrappers within the1800s maxima.

The owner approved the exact future union, scopes and envelope/framework policies,
including the single load-sequence owner and the new capture-support caps.
Approval does not supply implementation, current-source preparation, runtime
access, measured feasibility, native bodies, cleanup or isolation. This E41
slice implements no new runner/fixture source and runs no native full matrix,
database/load/Swift campaign, device, calibration or two-worker pilot. E42/E43,
commit/push/publication/release/deployment/reset remain separate gates.


### Follow-up entry-size steering

The owner subsequently asked for much shorter work units, including elimination
of overlapping qualifications. Normal-entry working target is120–300seconds.
The adopted1800s ceilings are not scheduling targets. Resolve the long-entry
schedule and explicit overlap/exception map before E42 execution; the current
profile remains execution-gated. The adoption packet records concrete source
candidates and the remaining amendment work in `short-entry-schedule.md`. No
assertion, scale/duration claim, supported-runtime path or mandatory proof was
removed by this steering record.

## Approved short-entry amendment — 2026-10-26

The owner approved the independently reviewed [short-entry proposal](../.artifacts/qualification-efficiency-short-entries-20261026T095519+0200/proposal.md).
The [adoption packet](../.artifacts/qualification-efficiency-short-adoption-20261026T104300+0200/owner-approval.json) records exact approval and preimages.
The version2 full-profile manifest now binds that frozen schedule and review:499
normal entries at most300s,9 conditional phase controllers, exact12-root coverage
and four source-proven overlap removals. The load maximum is25users/25concurrency,
with1/5/10/25 phases; old50/100 current obligations are retired with zero coverage
credit. Complete per-user scenarios, other unchanged scale/duration/source body
ceilings, typed vectors and runtime/caller/instrumentation distinctions remain.
The prior follow-up steering and original E41 profile are historical inputs.

This activates paired authority/profile/schema/pin/manifest adoption and fast
Go/all-family JVM corpus plus common-metadata checks only. E42 implementation,
measured constituent/root feasibility, two-worker isolation/cancellation and
native matrix execution remain separately gated. No speed improvement or
current25-user completion is claimed; no normal long-entry exception is granted.

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


## Completion and owner acceptance — 2026-10-27

The owner accepted the work and approved postponing Linux: “ok, I accept this work. let's mark this spec completed if nothing remained left (other than Linux work, but I approve postponing this work)”. This closes P0–P4 and E43 for the approved implementation, adoption and selected rehearsal scope. No other mandatory work remains in that accepted scope.

| Completed stage | Evidence and disposition |
| --- | --- |
| P0/P1 — E00–E14 | [Completed qualification handoff](../.artifacts/qualification-efficiency-runner-20261025/handoff.md): seven verifier runtimes, all 28 replay configurations, structural scan proof, matched macOS timing and independent final review. Earlier blocked P1 ledgers are superseded by this completed packet. |
| P2 — E20–E23 | [Checkpoint/fixture proposal ledger](../.artifacts/qualification-efficiency-p2-20261025/ledger.md) and [accepted-batch implementation ledger](../.artifacts/qualification-efficiency-p2-implementation-20261025/ledger.md): all 240 checkpoint cases were characterized; the approved disposition retains the original sweep. The smaller continuation regressions, duplicate removal and socket regression were implemented and reviewed while retaining required scale/duration proofs. The proposed checkpoint reduction was not adopted and is not outstanding implementation work. |
| P3 — E30–E33 | [Preparation-pilot ledger](../.artifacts/qualification-efficiency-p3-20261026/ledger.md): fresh-body equivalence, immutable preparation bindings, failure controls and three matched browser timing pairs. |
| P4 — E40/E41 | The approved union, short-entry and repair amendments above bind the paired authority/profile changes, fast checks, five-minute normal-entry caps and 25-user load maximum. |
| P4 — E42 | [Final rehearsal ledger](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/ledger.md): all 49 selected standard obligations covered by 35 fresh native entries, one fresh static support entry and 13 reviewed reused entries; all nine special contracts and two isolation obligations covered. |
| P4 — E43 | [Consolidated independent review](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/final-evidence-independent-review.md) found no remaining blockers in the executable selection. The owner's acceptance is recorded in the [closeout record](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/owner-acceptance-20261027T083949Z/acceptance.json). |

**Deferred Linux work:** matching-host execution/support for Linux x64 and ARM64, 97 selected methods per target. Neither target receives native pass credit. The owner-approved deferral removes this work from the current completion gate; later Linux work must establish its supported execution route and fresh runtime evidence. No Linux bridge was implemented.

This is completion of the accepted efficiency specification, not certification that every declaration in the 508-entry catalog or every future release trigger has executed. Untriggered release roots remain subject to the adopted profile when triggered. The optional three-pair concurrency timing study was not selected; isolation evidence does not establish a whole-profile speedup. The approved 25-user maximum carries no 50/100-user claim.

The earlier [execution closure seal](../.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/closure-seal.json), review, ledgers and raw attempts remain immutable historical evidence. Their “whole E42 incomplete” and “E43 not accepted” statements describe the state before this acceptance and Linux deferral. This documentation-only closeout updates this specification and its current profile authority hash; it changes no schedule, selector, assertion, workload, runtime code, execution gate or corpus payload and requires no native suite rerun.

Delivery status: no staging, commit, push, publication, release or deployment was authorized or performed by this closeout. Drymint remained prohibited and was not used.
