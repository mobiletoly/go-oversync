# r0001 validation contract

**Prospective profile amendment (2026-10-26):** Section9 below adopts the owner-approved future profile for explicitly bound candidates. Earlier C05/C06/C07 command text and evidence retain their historical scope. Authority adoption is separate from runnable tooling and qualification.

This is the initial-protocol qualification profile for [r0001](r0001-oversqlite-durable-recovery.md), selected because wire lifecycle, storage, default recovery and both SDKs change. It is not routine maintenance. The [paired runbook](../../sqlitenow-kmp/docs-engineering/oversqlite-validation.md) governs evidence classification, isolation, deadlines and reuse. The authoring sessions did not execute runtime commands. The authorized C00/C01 session subsequently completed the launcher prerequisite and V01 contract/metadata checks; its [ledger](../.artifacts/durable-recovery-contract-20261022/ledger.md) records exact results. C02 completed scoped V02 server proof; C03 completed scoped V03 journal/settlement/ack and focused V08 upload proof. C04 subsequently completed scoped V04 explicit snapshot and narrow V08 DT01–DT06 proof; C05 coordination is implemented; its required stable owning/race and narrow HTTP lanes are recorded in the [C05 ledger](../.artifacts/durable-recovery-automatic-20261023/ledger.md). C06 actual HTTP/Swift/PostgreSQL/resource/load and six affected runtime routes now have scoped passing evidence in the [C06 ledger](../.artifacts/durable-recovery-qualification-20261024/ledger.md). C06.2 subsequently passes the complete Node160-method route under the separately owner-approved one-body120s amendment below. C06 scoped qualification is complete and explicitly owner-accepted. C07 implementation and all twelve mandatory V10 gates have independently reviewed qualification evidence in the [current C07 packet](../.artifacts/durable-recovery-final-20261024/handoff.md); C07 owner acceptance and delivery remain pending. The [C04 ledger](../.artifacts/durable-recovery-snapshot-20261023/ledger.md) records current counts, reuse, retained failures and cleanup. The [C03 ledger](../.artifacts/durable-recovery-client-20261022/ledger.md) distinguishes passing native bodies from retained failures and compile-only checks. Milestone A closes scoped upload proof, B closes explicit snapshot proof, and C closes clean automation plus final qualification. Scoped exits do not waive the initial-protocol profile for any release claim.

## 1. Before every implementation phase

Capture both complete worktree inventories with `tools/check_change_scope.py`, exact allowlists and immutable baselines. Record original HEADs plus the intended specification delta, source/authority/corpus hashes, tests, fixture schema/state/counters, runtime route, command deadlines and available storage. Run focused leaves during development; freeze production before owning/final campaigns. Required status values are fresh-required, fresh-pass/fail, reused, subsumed, not-applicable, blocked and rerun-required, with the precise dependency reason.

At initial authoring, V01 recovery selectors were executable while V02 onward recovery selectors were planned tests to create. Their implemented owners and actual outcomes are now recorded in the current C07 packet; the original phase contracts below remain the governing requirements. Audit nonzero matching bodies. A selected test returning early for unavailable opt-in infrastructure is blocked/skipped, not passed. Retain native logs and all failed attempts. No timeout extension or unchanged retry to seek a pass.

Use Go 1.25.12 (`GOTOOLCHAIN=go1.25.12`), synthetic fresh run-owned databases, SQLite FK ON/WAL/synchronous FULL where supported, and normal PostgreSQL durability. macOS managed PostgreSQL uses Docker volumes by default; do not override with faster unsafe durability. Own distinct database/listener/application/scope/source identities. Run DB families serially unless full isolation and both wrapper/package deadlines are proved. Cleanup is part of success.

## 2. Resolve the missing launcher authority without bypass

The incoming `tools/qualification/oversqlite_smoke.py:contract_hashes` named three absent Kotlin files. C00 implemented the replacement mapping below with fail-closed preflight tests; no historical documents were fabricated. This specification deliberately replaces those required filenames for **r0001 qualification only**, as follows:

| Absent historical input | Replacement authority that must be hashed and checked |
| --- | --- |
| `specs/oversqlite-protocol-simplification-and-efficiency.md` | Go `specs/p0000-oversqlite-protocol-simplification-and-efficiency.md`, active `swagger/two_way_sync.yaml`, canonical r0001 and Kotlin `specs/oversqlite-durable-recovery.md`. r0001 expressly supersedes version/lifecycle/request-count conflicts; p0000 retains other codecs/transport rules. |
| `specs/oversqlite-convergent-sync.md` | Go and Kotlin `specs/*fk-safe-sync-and-automatic-resolution.md`, canonical r0001 sections 6–8 and this validation contract. This does not claim p0001 Stage B has been implemented. |
| `specs/oversqlite-platform-gzip-implementation.md` | Go p0000 section 9.1 and its raw-transport versus browser-Fetch compression/limits rules, active OpenAPI and transport fixture families in the manifest-pinned shared corpus. These requirements apply equally to Go and every Kotlin platform. |

Continue hashing both AGENTS.md files, the paired runbook and sorted corpus manifest. Add the Kotlin companion, r0001, checklist and this validation contract to the required input list; do not drop a missing replacement or downgrade strict verification to an optional lookup. The source-lifecycle/cancellation assertion sets and all launcher deadlines remain intact. No postponed historical `oversqlite-wire-schema-v2.md` document is substituted as authority.

C00 changes the launcher and its isolated controlled-process tests in a dedicated prerequisite batch. Add tests deleting/tampering with each required replacement and confirming preflight fails before child startup, plus a complete-input test. The new authority mapping is explicit here, so this is a reviewed replacement of the owning contract, not a bypass of it. Until that batch passes, dependent live/Swift execution is blocked.

From the Kotlin root, selected prerequisite command, outer 180s:

```sh
../go-oversync/scripts/run-with-timeout.pl 180 -- env PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tools.qualification.tests.test_oversqlite_smoke
```

Use the existing cleanup-aware outer runner to enforce 180s. These tests launch only their controlled fixtures. Preserve the original unrelated pyc. Independent specification review remains required before C02 production work.

## 3. Focused lanes

Run Go commands from `/Users/pochkin/Projects/my/go-oversync`; Kotlin/Gradle commands from `/Users/pochkin/Projects/my/sqlitenow-kmp`. Export `OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts` for both Go and JVM corpus consumers; neither consumer guesses a local root. Capture JSON/native reports to a new ignored output; never overwrite historical logs. The following exact selectors define the new required test prefixes and must be backed by real tests.

### V01 — Fast paired contract before DB-heavy work

```sh
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts go test ./oversync ./oversqlite -run '^(TestContractCorpus_|TestSharedFinalStateUniqueFixtures$|TestSharedRecoveryProtocolFixtures$|TestRecoveryProtocol_|TestCommittedEffectCoverage_|TestValidatePullResponse_)' -count=1 -timeout=3m30s -json
```

The `TestRecoveryProtocol_` family is wire/HTTP boundary validation without starting DB fixtures. It covers the new header/outcome/resolve/ack/snapshot errors, complete hash coverage and request counts. Existing compact/gzip corpus consumers remain selected. Kotlin metadata first, then focused JVM consumers:

```sh
./gradlew :library-oversqlite:compileCommonMainKotlinMetadata --no-daemon
OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew :library-oversqlite:jvmTest --tests '*SharedContractFixtureManifestTest' --tests '*SharedFinalStateUniqueFixtureTest' --tests '*SharedRecoveryProtocolFixtureTest' --tests '*RecoveryProtocolTest' --rerun --no-daemon
```

Retain 1800s existing lane ceilings, stricter registered inner deadlines and actual body counts. Metadata compilation is not runtime evidence. Contract/corpus changes invalidate both consumers.

### V02 — Server outcomes, race ordering and retention

Implement each `TestPushOutcome_` family with subtests named `pg16` and `pg17_10`, using the existing `startManagedIntegrationPostgres` helper with exact images `postgres:16` and `postgres:17.10`. Both families are independently budgeted; do not invent an image environment variable the fixture does not read. Record resolved image digest/version.

```sh
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSYNC_TEST_DOCKER_VOLUME=1 scripts/go-test-with-cleanup.sh ./oversync -run '^TestPushOutcome_/pg16$' -count=1 -timeout=3m30s -json
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSYNC_TEST_DOCKER_VOLUME=1 scripts/go-test-with-cleanup.sh ./oversync -run '^TestPushOutcome_/pg17_10$' -count=1 -timeout=3m30s -json
```

Go's slash selector permits any top-level `TestPushOutcome_...` prefix followed by the named image subtest. Audit the expected top-level names from C02, not just one body per database. Add focused `-race` execution for `TestPushOutcome_ResolveSerializesWithPush` and `TestPushOutcome_UnrelatedScopesProgress` under the same 240s/3m30s ceilings after stable source. Process-death tests use owned child processes and verify durable state from a new process.

The V02 LifecycleBinding family also proves section3.4 distinct client/trusted-writer ownership in both directions before callbacks or receipt replay, including cancelled/rejected acknowledged operations, initial zero-progress ownership, binding cleanup and restart. Use different writer IDs in existing trusted-write fixtures; preserve their original callback/receipt/capture assertions. This is runtime proof, not a pure reducer substitute.

The decisive assertions are actual DB rows/history/outcome/source fences, exact immutable result hashes, both race orders, lost-ack retry after generation retirement/rebinding with the exact original binding, no post-cancel commit, no replayed side effects, preserved initialization, bounded one-outcome/source retention, storage-failure rollback, unrelated-scope progress, cleanup and correct failure disposition. A mocked resolver or HTTP status alone is insufficient.

### V03–V06 — Client lifecycle, clean snapshot and coordinator

```sh
scripts/run-with-timeout.pl 180 -- env GOTOOLCHAIN=go1.25.12 go test ./oversqlite -run '^(TestRecoveryJournal_|TestFinalStateRecovery_|TestFinalStateUniqueApply_)' -count=1 -timeout=120s -json
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSYNC_TEST_DOCKER_VOLUME=1 scripts/go-test-with-cleanup.sh ./oversync -run '^TestRecoverySnapshot_/pg16$' -count=1 -timeout=3m30s -json
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSYNC_TEST_DOCKER_VOLUME=1 scripts/go-test-with-cleanup.sh ./oversync -run '^TestRecoverySnapshot_/pg17_10$' -count=1 -timeout=3m30s -json
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 go test -race ./oversqlite -run '^(TestRecoveryJournal_GuardedPreparationRaces|TestRecoveryJournal_CancelAndReopen|TestRecoveryJournal_AckCrashMatrix|TestA3Concurrency_|TestImmediateTransaction_)' -count=1 -timeout=3m30s -json
```

V03 covers C03 upload lifecycle, V04 covers C04 snapshot installation, V05 covers C05 clean eligibility/mode and V06 covers C05 concurrency/bounds/cleanup. Select reached exact tests and adjacent owners during development; do not repeat the whole set per phase. Run the full focused command once after stable C05 integration. Server snapshot tests use the same explicit `pg16`/`pg17_10` subtest names and exact fixture images as V02, with separate invocations/budgets as shown.

C04 selected the reached snapshot owners and I09 callers under the existing budgets; planned C05 GuardedPreparationRaces/CancelAndReopen names above remain unimplemented obligations, not zero-execution passes:

```sh
scripts/run-with-timeout.pl 180 -- env GOTOOLCHAIN=go1.25.12 go test ./oversqlite -run '^(TestFinalStateRecovery_|TestFinalStateUniqueApply_|TestOpening_I09|TestSnapshotFinalGate_LocalWriteDuringDownloadPreservesEverything$|TestA3Replay_PhysicalRestart$)' -count=1 -timeout=120s -json
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 go test -race ./oversqlite -run '^(TestA3Concurrency_|TestImmediateTransaction_|TestFinalStateRecovery_(DependencyMembership|SnapshotDeltaRollback|ContextChangePreventsDispatch|RetryAdmission)$|TestOpening_I09|TestSnapshotFinalGate_LocalWriteDuringDownloadPreservesEverything$)' -count=1 -timeout=3m30s -json
```

Its final U6 descriptor accounting correction refreshes focused I09 admission/callers; unchanged snapshot/core/server/process evidence is explicitly reused by independent review. Actual create/page retry dispatch must retain pinned admission, including Ktor plugin retries and the one-row fallback; cleanup retains independent bounds. No C05 coordinator is implied. The C04 command/evidence map records exact attempts and nonzero native execution. Unchanged historical ReplaySqlStructuralTest assumptions failed before their frozen source SQL oracle was reached; preserve those failures and no performance credit. Later full JVM/qualification must adapt or reclassify them deliberately; existing cancellation/rollback/reopen obligations are not waived.

Kotlin focused command, 1800s ceiling:

```sh
./gradlew :library-oversqlite:jvmTest --tests '*RecoveryJournalTest' --tests '*FinalStateRecoveryTest' --tests '*FinalStateUniqueApplyTest' --rerun --no-daemon
```

Add actual JVM process-death tests to their named owner and selected lane; close/reopen in one process is not a substitute. Retain adjacent current `TestSnapshotFinalGate_LocalWriteDuringDownloadPreservesEverything`, `TestA3Replay_PhysicalRestart`, `TestFinalStateUniqueApply_CommittedReplayPreservesDirtyClosure`, original local-apply corpus, and source lifecycle tests. The old prepared-source-rebind test must be deliberately reclassified/replaced under the new immutable pending-operation contract; do not quietly delete its preservation assertion.

Full state-vector assertions include synchronized rows, local-only values, clean descendants, original bases/captures, frozen bytes/hash/source/token, current local state/guards, exact C, snapshot T/hash, row baselines/absence observations, dirty/outbox, source terminal/ack progress, checkpoint, minimal recovery status, resource owners and cleanup. Every failure and crash boundary asserts both what committed and what remained unchanged. Explicitly assert zero recovery network requests for each dirty/F/C/rejection/ack_pending admission failure, and zero ack for unsafe committed settlement. New writes during snapshot preparation survive and prevent installation; no merge is expected.

### Activity-schema reference-case ownership (DT01–DT07)

Specification section 1.2.2 makes these required representative cases, with synthetic fixtures derived from the pinned schema inventory. The prior in-memory DDL inspection is provenance/structural evidence only; all SDK/server/platform evidence remains fresh-required. Keep fixtures within the authorized implementation repositories, with original source hashes and explicit server-wire mapping; no developer checkout or real data is needed to execute them.

- Go local cases: `TestFinalStateRecovery_ActivitySchema` with subtests `DT01` through `DT07`, selected by the existing `TestFinalStateRecovery_` prefix. During C04 select DT01–DT06; DT07 closes after C05. Reuse C03's required outcome tests for F/C/ack-specific crash assertions rather than duplicating the upload engine.
- Kotlin local cases: `testActivityDT01` through `testActivityDT07` in the existing planned `FinalStateRecoveryTest` class, selected on all affected runtimes through V07. The existing `RecoveryJournalTest` still owns outcome/ack crash boundaries.
- Actual HTTP: `TestServerWriteRecovery_ActivitySchema` and corresponding cases in Kotlin `FinalStateRecoveryLiveTest`, selected by V08's existing prefixes/classes. Prove cascade/self-reference/unique transfers and clean snapshot recovery against a correctly mapped synthetic server generation, and preservation/no snapshot for pending intent or unsafe settlement. A local-only helper pass cannot substitute for these wire/runtime paths.

At milestone exits, record the assertion and owning test for every DT case, actual nonzero body execution and any intentionally reused unchanged evidence. A positive supported case returning a capability blocker is failure, not a preservation pass. An intentionally invalid final state must block atomically and retain the full state vector. Do not add a redundant expensive standalone aggregate or extend existing deadlines for these cases; use focused selectors during development and the existing V04/V07/V08/V10 obligations at their triggers.

## 4. V07 — Owning and platform lanes

At stable C05/C06 candidate, run owning Go server/client separately with independent 240s wrappers and 3m30s package ceilings; run affected server database families on both versions. Root package tests do not cover the nested mobile module.

```sh
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts scripts/go-test-with-cleanup.sh ./oversync -count=1 -timeout=3m30s -json
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts go test ./oversqlite -count=1 -timeout=3m30s -json
./gradlew :library-oversqlite:jvmTest --rerun --no-daemon
```

For this C05 candidate, the owner explicitly approved replacing the single client package command with two audited, serial invocations, each retaining the240s wrapper/210s package ceiling. Their combined allowance is420s package/480s wrapper time. The server command remains unchanged. This is a qualification execution amendment; it changes no workload, assertion, body deadline, source behavior or skip policy. The reviewed [manifest](../.artifacts/durable-recovery-automatic-20261023/client-owning-batches.json) enumerates all691current Test/Example/Fuzz owners: one reservation owner and690remaining owners, with complete union and empty intersection. The [owner approval](../.artifacts/durable-recovery-automatic-20261023/client-owning-batches-owner-approval.json) binds the proposal. Before each invocation, list current owners with Go1.25.12 and fail closed on any manifest mismatch. Use each exact anchored manifest selector with the client command above plus `-run`; retain native output and audit every owner, subtest failure and existing skip by exact owner/reason.099/107 remain failed/inconclusive historical evidence. That historical C05 approval does not authorize another phase.

C06-only owner-approved execution amendments (2026-10-24):

- The [reviewed Go proposal](../.artifacts/durable-recovery-qualification-20261024/client-owning-proposal.md) and [owner approval](../.artifacts/durable-recovery-qualification-20261024/client-owning-owner-approval.json) authorize two audited serial client batches for this C06 candidate: each240s wrapper/210s package, combined480s/420s. Fresh discovery must exactly match all691owners, with one reservation owner and690remaining owners, disjoint complete union and unchanged skip policy. The original server command is unchanged. Current080/089 actual package times70.677s/117.659s satisfy these limits. This is separate C06 authorization, not reuse of the historical C05 approval.
- The [reviewed single-body web proposal](../.artifacts/durable-recovery-qualification-20261024/web-single-deadline-proposal.md) and [owner approval](../.artifacts/durable-recovery-qualification-20261024/web-single-deadline-owner-approval.json) change only `AutomaticRecoveryTest.uploadAndSnapshotShareDispatchBudget`: Node Mocha30→60s with workflow60s unchanged, and Browser/Wasm workflow60→120s. All other body deadlines,64-dispatch workload, assertions, SDK limits and1800s wrappers remain unchanged. The earlier three-body Node proposal is superseded and unexecuted. Node100 remains a failed60s attempt; the subsequent separately approved Node-only120s amendment below supersedes that execution ceiling for the named body only. Browser115/Wasm116 each pass165 exact native methods.
- The [reviewed Node-only proposal](../.artifacts/durable-recovery-qualification-20261024/node120-proposal.md) and [explicit owner approval](../.artifacts/durable-recovery-qualification-20261024/node120-owner-approval.json) subsequently change only that same method on Node: Mocha60→120s and coroutine60→120s. Browser/Wasm remain120s; native/JVM/Android remain60s, other Node Mocha bodies30s, other common workflows60s and worker bodies180s.64dispatches/maxPushRows1/allassertions/SDKlimits/1800s wrappers stay intact. The dedicated reviewed v8 Node-only route permits exactly focused1body or full160(83common+77worker), verifies all original approved controls, and requires same-task successful focus before the full route.120focus completes in54.337s;121full160 completes in250.149s, with20applicable journal witnesses/21wire cases/three explicit transient physical-restart exclusions/77complete decoded worker receipts/DT03negative1 and no missing, failed or skipped body. Node100 and the older aggregates remain failed, not retrospectively qualified.
- Current [common selection](../.artifacts/durable-recovery-qualification-20261024/platform-selection-dt03.json) explicitly includes83 exact common methods (including22 automatic-coordinator and4 common wire-parity methods),22 journal witnesses and21 wire cases. The [reviewed worker extension](../.artifacts/durable-recovery-qualification-20261024/worker-selection-evidence-corrective.json) adds82 methods for Browser/Wasm and77 for transient Node; named durable-restart exclusions have no execution credit. Reviewed `run_platform_lane_v7.py` preserves the original tasks and controls, requires same-task actual preflight and exact native/witness/full decoded worker receipts. The Karma hook adds only missing line terminators to stock test-report records. The added DT03 negative assertion uses the existing common owner and unchanged body deadline. Native macOS/iOS and Android reuse only independently verified unchanged owners, with fresh affected DT03 execution; no skipped, early-return or compile-only result is runtime proof.

These amendments qualify execution of this C06 candidate only. They grant no C07 or release/delivery approval and do not amend SDK semantics, workloads, defaults, protocol or future final-delivery commands.

For each KMP task below, use `./gradlew TASK --tests '*RecoveryJournalTest*' --tests '*FinalStateRecoveryTest*' --rerun --no-daemon`, under its existing registered ceiling (maximum 1800s):

| Runtime | Exact TASK | Required distinction |
| --- | --- | --- |
| macOS | `:library-oversqlite:macosArm64Test` | Native SQLite statements, persistence and cancellation. |
| iOS simulator | `:library-oversqlite:iosSimulatorArm64Test` | Actual simulator bodies/driver; macOS is not a substitute. |
| Node | `:library-oversqlite:jsNodeTest` | Worker ownership, exact int64/typed values; retain Mocha 30s body timeout. |
| Browser | `:library-oversqlite:jsBrowserTest` | Actual worker persistence, Fetch bounds and cancellation. |
| Wasm browser | `:library-oversqlite:wasmJsBrowserTest` | Wasm codec/hash/persistence and reached worker cancellation. |

Android runs each class separately on one owned device/APK:

```sh
./gradlew :library-oversqlite:connectedAndroidDeviceTest -Pandroid.testInstrumentationRunnerArguments.class=dev.goquick.sqlitenow.oversqlite.FinalStateRecoveryTest --rerun --no-daemon
```

Repeat separately with `RecoveryJournalTest`. Preserve existing per-body deadlines (current workflow bodies 60s) and native XML/body evidence. Do not use previously unreliable comma-separated method selectors. Any new test ownership/filter wiring must prove nonzero actual bodies before the campaign.

If generator/compiler/plugin code becomes necessary, declare that extra ownership and run compiler and plugin tests separately, including `:sqlitenow-gradle-plugin:test`. Do not change generated output by hand. These changes are not presumed by this plan.

## 5. V08 — Real HTTP, public drain and Swift

Go integration family with a run-owned PostgreSQL 16 fixture, outer 180s/inner 150s, cleanup 30s:

```sh
scripts/run-with-timeout.pl 180 -- env GOTOOLCHAIN=go1.25.12 scripts/go-test-with-cleanup.sh ./integration/nethttp_server/server -run '^(TestServerWriteRecovery_|TestServerWriteReliability_)' -count=1 -timeout=150s -json
```

The outer runner/fixture must be selected and recorded before launch. Use a fresh isolated service/database/listener/application and preserve normal durability. Add corresponding 17.10 evidence to the affected server families; do not assume an arbitrary external HTTP server is owned.

Kotlin against that pinned owned server, 1800s lane ceiling:

```sh
OVERSQLITE_REALSERVER_TESTS=true OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=<owned-fixture-url> ./gradlew :library-oversqlite:jvmTest --tests '*FinalStateRecoveryLiveTest' --tests '*A1LiveSurvivorsTest' --rerun --no-daemon
```

The angle-bracket URL is an explicitly supplied fixture value, not literal shell syntax. Audit real body start/pass; unavailable server returns/skips do not qualify. Both clients must execute: lost response then resolve, resolve-before-delayed-push, history pruned before reconnect, ack response lost, clean later snapshot past a bad historical state, unsafe committed result versus later local unique conflict (F/C retained, no ack, no snapshot), dirty admission refusal, reopen and post-T write. Include actual transmitted identities and full state vectors.

After C00 authority correction, use the owned launcher from Kotlin:

```sh
python3 tools/qualification/oversqlite_smoke.py go-direct-sync --go-repo /Users/pochkin/Projects/my/go-oversync --output <new-ignored-output>
python3 tools/qualification/oversqlite_smoke.py swift-cancellation --go-repo /Users/pochkin/Projects/my/go-oversync --output <new-ignored-output>
python3 tools/qualification/oversqlite_smoke.py swift-cancellation-state --go-repo /Users/pochkin/Projects/my/go-oversync --output <new-ignored-output>
```

Supply `OVERSYNC_EXAMPLE_TEST_DATABASE_URL` through the environment with a service authorized to create/drop isolated run-owned databases. Never log credentials. C06 must extend the owned exact selector set with `TestMobileFlow_DirectSyncRecoveryPreservesIntent` and Swift `SyncCancellationStateTests.testFinalStateRecoveryCancellationPreservesOriginalIntent`, while retaining the existing direct-drain and second-push tests. Required reached cancellation boundaries are resolve, snapshot download, final apply and ack; phase-barrier tests must distinguish them. The launcher reports each required body's native evidence, not a generic suite pass.

Keep existing deadlines: Go fixture preparation+test 240s total, Go test≤3m30s and remaining outer budget; Swift preparation 15m, readiness 60s, selected Swift test 240s, cleanup 30s. Do not parallelize shared Swift build products. If extending the selector set cannot fit the existing per-invocation ceiling, add separately invocable boundary selections with the same ceilings and explicit contract review; never silently extend one aggregate deadline.

## 6. V09 — Bounds, storage and load

Prove entry/byte/work boundaries for every new owner, including the full upload certificate in its lifecycle and snapshot/old-new plans in the clean recovery lifecycle. The clean gate prevents active F/C plus snapshot preparation, but obsolete staging/cleanup copies count until released. Test both mutual exclusion and aggregate accounting; no decision/report stores are required. Include work targets 1/8/17/default, small acyclic/cyclic graphs, the existing default-limit 200-cycle refusal, active replay sizes 1,000/10,000 and payload widths 16/1,024 where affected. Do not reintroduce retired 50K workloads. Measure SQL visit/temp-space work and retained files/WAL as well as host allocation.

Retained audit includes ten update/delete/sync/reinsert cycles, recovery rebuild, failure/cancel/reopen and ack lifecycle. Census every `_sync_*`/outcome owner, inspect closed copies, reconcile metadata and integrity/FKs, and show no unchanged-state downloads or accumulating evidence. No manual cleanup/VACUUM to improve the result. Server accounting proves at most one bounded outcome per source, metadata/ack fences and safe multiple-service-process behavior; byte counters alone are not proof of physical disk usage.

**Historical V09 load gate (superseded prospectively by section9 for approved-profile candidates):** Retain the runbook's load gates: structural correctness first, three smaller verified complete-scenario timings, 50-user/50-concurrency rehearsal≤600s, then 100-user/50-concurrency final≤1200s. Current explicitly bound candidates instead require all four1/5/10/25-user phases with matching concurrency and240/240/240/300s caps; no50/100 current credit. Record newly added ack request/byte/latency cost and peak retained-outcome storage before launch. Do not silently increase defaults or budgets to pass load; configuration may be explicitly chosen and recorded within the existing source-count and new per-outcome bounds. Do not introduce quota credits or claim a deployment-wide storage ceiling. Reuse unaffected evidence only when all dependencies match; v1 transport/ack lifecycle invalidates old request-count and affected load evidence.

## 7. V10 — Mandatory final initial-protocol aggregates

**Trigger:** C07 stable full first-delivery candidate, or any earlier request to claim protocol acceptance/release readiness for a milestone. Governing authority is r0001 section 9, p0001 section 5.2, p0000 section 17 and the paired runbook's initial-protocol profile. These are not per-development-slice commands.

```sh
scripts/test-lane.sh package
scripts/test-lane.sh audit
scripts/test-lane.sh full
scripts/test-lane.sh mobile
scripts/run-with-timeout.pl 240 -- env GOTOOLCHAIN=go1.25.12 go vet ./...
```

Wrappers pin Go 1.25.12 and currently use outer 240s/inner 3m30s plus owned cleanup. Set the required corpus environment. Root `./...` does not replace mobile. Preserve separately required lanes even when broader execution covers unchanged focused tests. An aggregate timeout is inconclusive until isolated; constituent evidence with another budget is allowed only after an explicit governing-contract amendment permits it, not to evade the existing wrapper.

### C07-only owner-approved execution amendment (2026-10-24)

The [reviewed proposal](../.artifacts/durable-recovery-final-20261024/execution-amendment-proposal.md), [independent review](../.artifacts/durable-recovery-final-20261024/execution-amendment-review-1.md) and [explicit owner approval](../.artifacts/durable-recovery-final-20261024/execution-amendment-owner-approval.json) authorize these exact equivalent envelopes for this C07 candidate. The package and full lanes remain independently mandatory, with their complete original package and owner selections, now serial via `-p=1`,600-second outer and420-second per-package limits. Standalone comprehensive retains the original task/class filter with a900-second outer limit. No assertion, workload, body deadline, SDK behavior or skip rule changes. Audit/mobile remain240/210 and vet240. Other KMP aggregates retain1800 except for the later explicitly approved C07 overlays below; every other inner/body limit remains unchanged. No future-release-profile or acceptance/delivery approval is implied.

```sh
scripts/run-with-timeout.pl 600 -- env GOTOOLCHAIN=go1.25.12 OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts scripts/go-test-with-cleanup.sh ./oversync ./oversqlite -p=1 -count=1 -timeout=7m -json
scripts/run-with-timeout.pl 600 -- env GOTOOLCHAIN=go1.25.12 OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts scripts/go-test-with-cleanup.sh ./... -p=1 -count=1 -timeout=7m -json
```

From Kotlin:

```sh
/Users/pochkin/Projects/my/go-oversync/scripts/run-with-timeout.pl 900 -- env GOTOOLCHAIN=go1.25.12 JAVA_HOME=/Users/pochkin/Library/Java/JavaVirtualMachines/jbr-21.0.11/Contents/Home GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync OVERSQLITE_CONTRACTS_ROOT=/Users/pochkin/Projects/my/sqlitenow-kmp/oversqlite-contracts ./gradlew oversqliteComprehensive --rerun-tasks --no-daemon --console=plain
```

Retain all prior failures and fail closed on source, index, contract, owner inventory or owned-resource drift. These three amended envelopes supersede only the corresponding original commands for this C07 qualification. The original scripts remain unchanged.

### Additional C07-only owner-approved overlays (2026-10-24)

The [PlatformAll proposal](../.artifacts/durable-recovery-final-20261024/platform-execution-amendment-proposal.json), [review](../.artifacts/durable-recovery-final-20261024/platform-execution-amendment-review-1.md) and [approval](../.artifacts/durable-recovery-final-20261024/platform-execution-amendment-owner-approval.json) define two audited1800s batches,3600s combined: only the four reviewed positive Android/iOS/Browser/Node families from failed079 are reused as batchA, and all JVM/macOS/Wasm bodies execute freshly in batchB.079 remains failed; both parts and independent union/body/cleanup review are required. This does not authorize another matrix or a future release profile.

The separately [reviewed shared-stress proposal](../.artifacts/durable-recovery-final-20261024/ios-shared-stress-deadline-proposal/proposal-v3.json), [review](../.artifacts/durable-recovery-final-20261024/c07-ios-shared-stress-deadline-proposal-review-2.md) and [approval](../.artifacts/durable-recovery-final-20261024/ios-shared-stress-deadline-owner-approval.json) change only the common shared-connection stress owner's outer coroutine deadline60→150s. Its ordinary45s/heavy120s active-work limits, workloads, assertions and1800s wrappers remain intact. Failed iOS102 remains failed; the exact corrective104 body and later mandatory execution retain their distinct evidence. No other body deadline changes.

The [final live matrix proposal](../.artifacts/durable-recovery-final-20261024/final-live-execution-amendment-proposal.json), [review](../.artifacts/durable-recovery-final-20261024/c07-final-live-execution-amendment-review-1.md) and [approval](../.artifacts/durable-recovery-final-20261024/final-live-execution-amendment-owner-approval.json) permit two fresh serial batches for each separate ordinary/heavy root. Each batch retains1800s wrapper and30s shared cleanup; combined wrappers are3600s per root. The exact disjoint registered task union, original leaf flags/filters, workloads, assertions, skip rules and body limits stay unchanged. Both complete batches and independent actual-body/profile/cleanup review are necessary to qualify a root. Focused prerequisites and failed prefixes do not replace either batch. The following commands run through the reviewed owned three-profile/private-device controller and recorded environment:

```sh
# realserver-native
/Users/pochkin/Projects/my/go-oversync/scripts/run-with-timeout.pl 1800 -- ./gradlew oversqliteRealserverJvm oversqliteRealserverAndroid oversqliteRealserverJvmHarness oversqliteRealserverIosSimulatorArm64 oversqliteRealserverMacosArm64 --rerun-tasks --no-daemon --console=plain
# realserver-web
/Users/pochkin/Projects/my/go-oversync/scripts/run-with-timeout.pl 1800 -- ./gradlew oversqliteRealserverJsNode oversqliteRealserverJsBrowser oversqliteRealserverWasmBrowser --rerun-tasks --no-daemon --console=plain
# heavy-native
/Users/pochkin/Projects/my/go-oversync/scripts/run-with-timeout.pl 1800 -- ./gradlew oversqliteRealserverJvm oversqliteRealserverJvmHeavy oversqliteRealserverAndroidHeavy oversqliteRealserverJvmHarnessHeavy oversqliteRealserverIosSimulatorArm64Heavy oversqliteRealserverMacosArm64Heavy --rerun-tasks --no-daemon --console=plain
# heavy-web
/Users/pochkin/Projects/my/go-oversync/scripts/run-with-timeout.pl 1800 -- ./gradlew oversqliteRealserverJsNodeHeavy oversqliteRealserverWasmBrowserHeavy --rerun-tasks --no-daemon --console=plain
```

Original approved source52/proposal/runner identities remain immutable. The [narrow source53 fixture correction](../.artifacts/durable-recovery-final-20261024/c07-f1-live-suite-correction-review-1.md) and [control successor review](../.artifacts/durable-recovery-final-20261024/c07-f1-final-live-controls-review-1.md) bridge only the required F1 test helper's existing platform-or-realserver admission, preserving every body/skip/task/budget and original approval provenance. Failed116 remains failed. No arbitrary later source rebind, new budget, full-gate waiver, owner acceptance or delivery authority is inferred.

Paired full-profile commands from Kotlin:

```sh
GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance
./gradlew oversqliteComprehensive --rerun-tasks
./gradlew oversqlitePlatformAll --rerun-tasks
./gradlew swiftHybridSupportGate --rerun-tasks
./gradlew oversqliteRealserverAll --rerun-tasks
./gradlew swiftHybridSupportRealserverGate --rerun-tasks
./gradlew oversqliteRealserverAllHeavy --rerun-tasks
```

Bind their actual registered inner/outer deadlines and fixture/runtime paths at C00; existing ceilings are not waived by this document. If a full gate is currently unreachable, record it and stop the dependent qualification. Do not report full completion from focused slices. Independent implementation/evidence review and owner acceptance remain separate, with all inherited/current failures and limits retained.

## 8. Invalidation and evidence handoff

Contract/hash/fixture changes invalidate both consumers and affected wire runtimes; server outcome/retention changes invalidate server DB/race/live/load paths; local journal/apply/clean-admission changes invalidate owning clients and reached platform/live/Swift/resource paths. Documentation-only clarification invalidates identity/review checks, not unchanged DB suites. A late production change requires refreeze and directly affected leaves plus independently mandatory final aggregates. Do not rerun an expensive unchanged lane already covered by a later successful broader one unless independently mandated.

Final evidence includes exact source/spec/OpenAPI/corpus identities, complete scope deltas, command/environment/deadline records without secrets, actual body counts, failures/retries, request/byte/storage measurements, cleanup proof, limitations, the DT01–DT07 assertion/evidence map, the section 1.1 simplification review and review outcome. It must distinguish implementation, qualification, acceptance and release. No command here authorizes a real application database reset.

## 9. Approved short-entry full profile — 2026-10-26

The owner approved the independently reviewed short-entry amendment and25-user
maximum on2026-10-26. The [version2 profile](../../sqlitenow-kmp/tools/qualification/profiles/oversqlite-full.json)
and its schema adopt the frozen schedule, exact9,158-row disposition map and
review outcome. Approval is recorded in [the amendment adoption packet](../.artifacts/qualification-efficiency-short-adoption-20261026T104300+0200/owner-approval.json).
The previous E41 profile and authority preimages remain immutable there.

### Authority, trigger and representation

For future initial-protocol/release candidates explicitly bound to this profile,
this section supersedes the preceding E41 section9 composition/envelopes and
named repeated roots in V09/V10, p0000 section17, p0001 sections5.2/5.2.2, the paired
companions and work-slice runbook. Development/maintenance keep manually selected
affected owners and reviewer-confirmed reuse. Historical C05/C06/C07 commands,
failed attempts, native evidence and acceptance retain their original meanings.
The completed unrestricted V07 JVM gate is historical and has no claimed filtered
union equivalence; reactivation requires its original current fixture binding.

The manifest's `schedule` is the exact approved proposal object. Its embedded
proposal-status/pre-adoption-preservation fields describe the frozen review
snapshot; top-level adoption status and execution gate govern current use.
`schedule.entries` supplies508 literal argv/cwd/environment/deadline records;
`schedule.logical_roots` and `schedule.command_aliases` give exact membership.
The frozen coverage retains every original column, adds retirement/subsumption
fields, and records no50/100-user current credit. Missing or mismatched approved
inputs block interpretation. All native execution flags remain false: E42
implementation, controls, calibration, isolation and matrix execution are separate.

### Bounded normal entries and cumulative roots

Every normal entry is at most300seconds, normally270 for its native/build process
plus30 cleanup. Tighter inherited caps remain. The499 normal entries and9 explicitly
conditional phase controllers form a catalog, not a command to run every owner on
every change. Orchestration-only parents describe accounting/ordering and cannot
run as opaque long normal jobs. There are no long normal-entry exceptions.
Source body ceilings/workloads are unchanged; operational caps are deliberately
stricter. An indivisible owner that cannot fit blocks for reviewed decomposition
or a new explicit amendment, never an automatic1800-second retry or workload cut.

The27 root Go packages are partitioned exactly once into oversync, oversqlite and
the25-package remainder including8 build-only owners. Use Go1.25.12 and the existing
cleanup-aware runner, `-p=1 -count=1 -timeout=4m -json`,270s native plus30s cleanup.
The owning-package root needs its two jobs; full adds the remainder. Audit tags,
nested mobile, vet, race, PostgreSQL16/17.10 and HTTP boundaries remain distinct.
Compatible Kotlin native invocations select at most96 exact methods; Android uses
one class or one method. Heavy replay/settlement/live owners retain their separate
fixtures. Swift products/local/live leaves remain distinct except the exact
live-class filter below. Linux x64/arm64 require matching-host discovery/bodies;
macOS compilation/linking cannot qualify them.

All12 logical roots retain their inherited cumulative envelope trees: Go owning
and full600s, audit/mobile/vet240s, Kotlin cross/comprehensive/Swift1800s, platform/
live/heavy3600s. These are cumulative limits, not scheduling targets or extra
per-member credits. Charge actual reached preparation, framework startup, native
work, observation/report and cleanup. A complete approved member union replaces
its repeated opaque aggregate. Shared physical work counts once in campaign
elapsed and its full reached cost in every dependent root's feasibility analysis.

### Approved overlap and preparation rules

One identical ordinary direct-JVM HTTP Test witness may cover its graph edge in
both ordinary and heavy roots, with HEAVY=false and identical owned leases. Heavy
JVM stress and coordinated heavy contexts remain separate. The nine local/mock
DatabaseStateVectorTest methods run once per runtime under canonical platform
admission; preserve exact worker/reopen capability exclusions. Compatible126 direct
ordinary JVM owners may use the same-candidate comprehensive classpath/JUnit/neutral
witness. Live/resource/instrumented or different caller/runtime paths are distinct.
Generated Swift keeps its complete local Exec and selects only the two actual HTTP
SyncFixtureRealserverTests on live Exec. The specifically approved future init file
`tools/qualification/profiles/short-swift-filter.init.gradle` must preserve that
Exec's dependencies/cwd/environment and fresh bodies. Other conditional Swift
pilots retain their own guard dispositions.

Preparation is separately bounded and source/context/output-bound, not universal
task-name reuse. Exclude native Test/Android instrumentation/nested Gradle drivers
from preparation, including the late-bound crossRepo Exec. Canonicalize included
compiler Copy/Jar dependency edges. Reuse/exclude only verified completed nonbody
predecessors with matching inputs/toolchain/output/cleanup; missing proof blocks.
Changed flags/URLs invalidate reached webpack/generated preparation. Preserve the
removed drivers' Swagger/corpus/hash and explicit live/device URL preflights.
Allowlist the environment and clear ambient activation/audit/sampler/row overrides.
P3 record and reuse remain separate fresh native invocations when selected.

Retain the original named120s Node AutomaticRecovery dispatch-budget policy and
480s web Settlement backlog framework alignment as source ceilings, not new normal
wall allowances. Preserve other source defaults,77 applicable Node worker methods
and5 unsupported durable-reopen exclusions, all82 Browser/Wasm worker methods,
four1000/10000-by16/1024 replay workloads, Go125/1000 and Kotlin64/2000 witnesses,
checkpoint/process/worker death, actual sockets, generated callers, packaging,
full typed vectors and F/C/ack accounting. Guards/skips/build outputs earn no body credit.

### Resource, load and byte proofs

Retained Go/JVM default/one audits keep125keys/ten cycles and all physical copy,
rebuild/cancel/reopen/lost-ack/schema/state/integrity/FK assertions. Proposed
lifecycle caps are authority180, readiness60, native210 (Go package200), read-only
inspection30 and cleanup30 within540s. Require the full270s native+inspection+
cleanup remainder. JVM cold compilation is a prior bounded preparation entry.
Normal/observed replay binaries and instrumentation remain separate; keep240s
build and150s native+30s cleanup, exact five diagnostics including1/16 control,
observer/modfile/tag and10ms sampler. Sampled sizes remain lower bounds.

The sole public load root is load-sequence with four internal phases:
1user/1concurrency →5/5 →10/10 →25/25. The first three phase caps are240s, final300s;
package caps are phase-minus10 and contexts phase-minus20. Authority180 and client240
builds occur before a shared1200s monotonic fixture lease. Readiness60 + phase
maxima1020 + shared cleanup30 =1110, leaving90s coordination headroom; overall
logical containment1620 includes preparation. Reserve the full next phase plus
cleanup before dispatch. Keep one fresh authority/database/application/reset-domain
lease across all four phases; no reset, public load-N acceptance or receipt stitching.
Only all four fresh members plus joined terminal cleanup qualify the sequence.

The former50-user rehearsal and100-user/50-concurrency proof are prospectively
retired, with zero current credit and no25-user equivalence claim. Historical
receipts keep their original claims. New maximum proof is25users/25concurrent
process clients. Retain the complete complex-multi-batch scenario per user,
verify=true, preserved databases, FULL/WAL/FK, both crash hooks, original final
checks and ack/storage observations, history projection32/product default4,
unchanged wait/bounds and structural/client-owning/race/retained/resource/ack
prerequisites. There is no current25-user timing/completion claim.

Raw and Fetch capture retain all7 p0000 section9.2 shapes and exact byte/storage/
transport assertions, separate normal/observed boundaries and existing cumulative
resource trees. Individual capture/build/native entries are at most300s. The opaque
4096-byte compression-selector fixture makes no protocol-semantic claim. Encoded
network bytes must come from the actual raw/server-wire capture, joined with actual
decoded browser receipts. Existing malformed/overflow proofs remain independent.

### Adoption checks and later execution

Authority adoption requires strict profile/schema/input/pin/manifest/link checks,
fast Go corpus/wire boundaries under240s with210s package timeout, common metadata
under270s, and the exact all-family nine-class30-method JVM union under270s, each
with separate cleanup up to30s. Metadata is compilation evidence only. Both corpus
consumers must execute fresh after pin changes; no database suite is needed here.

Before E42 matrix launch, implement the existing-coordinator adapters/controls,
measure missing constituent costs once within approved caps, and prove each reached
root fits including cold/context-specific preparation and framework startup. Unknown
or infeasible roots block launch. Two-worker separation/cancellation/cleanup proof
precedes selected concurrent execution; the existing7200s three-pair study is a
conditional accounting parent, never a long normal unit. Heavy/load/memory remain
host-exclusive. No speedup, current native matrix or resource feasibility is claimed
by authority adoption. E43 review/owner acceptance and delivery remain separate.

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
