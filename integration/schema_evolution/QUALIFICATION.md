# rollout Go qualification coverage

This ledger maps the accepted rollout revision 2 and parent baseline contract §17 to executable
Go evidence. Run results, source hashes, exact deadlines and every failed attempt are
retained in `.artifacts/rollout/revision1/validation-ledger.md`. A test name below is
an ownership mapping; only a successful recorded lane qualifies its execution.
Independent review and owner closure remain separate gates.

## Child matrix

| Contract | Owning evidence |
| --- | --- |
| G1, G2 permanent generation identities | `oversync/generation_lifecycle_test.go` checks malformed/incomplete/capacity/support-count evidence and immutable proofs; `TestRolloutLedger_DurableAllocationAndGuards` checks real SQL allocation, abandonment, archive validation, enabled trigger/function identity and public ACL guards; table and mobile journeys activate generation 3 without resetting the authority. |
| F1 fleet agreement | `TestRolloutFleet_ThreeProcessesFenceAndReopen` launches two serving processes and one writer with distinct roles, executable hashes and fresh starts; missing/mismatched membership blocks reopen. |
| F2 admission and writer fences | Fleet test exercises authenticated sync/watch and writer DML, credential disablement, backend termination and process death. Existing `TestReadiness_WatchRegistrationRacesDrain`, `TestReadiness_GuardedWatchDrainsWithoutClientCancellation`, `TestEvolution_AdmissionLeaseDrainAndWorkerTakeover`, `TestEvolution_WorkerTakeoverFencesLivePredecessor`, `TestEvolution_WorkerMetadataSerializesWithTakeover` retain admission/ownership races. |
| F3 crash, ambiguity and limits | `TestRolloutController_DeathBeforeAndAfterCommit` kills a real controller inside a SQL latch, then loses a committed reply and reconciles exact status/resume; altered inputs reject. `TestRolloutPolicy_EligibilityAndFinitePause` checks attempts, deadlines and evidence bounds. The controller test executes the actual CLI. |
| P1 explicit product policy | `TestRolloutPolicy_EligibilityAndFinitePause` checks missing/channel/delayed/window/notice/offline/hold/admission failures; both mobile journeys keep one device old while the other upgrades. |
| R1 sessions | `TestRolloutCleanup_NaturalExpiryBlocksUntilAuditedDeletion` creates real push and snapshot staging, waits the original TTL, proves expired-but-present remains blocked, then uses bounded audited deletion and verifies authoritative state. |
| R2 retired wire/direct precedence | `TestGenerationLifecycle_RetiredProofEveryDirectOperation` covers 13 direct calls before database access. Both mobile journeys consume the 12-route HTTP matrix with authentication, no prior binding, malformed/unknown/mismatch competitors and no source side effect. OpenAPI locked contract asserts all generation endpoints expose 426. |
| R3 preserved local state | `oversqlite/retirement*_test.go` consumes loop, boundary and conflict/uncertain fixtures with real SQLite state and verifies the durable public `ApplicationActionRequired` result across client reopen. Mobile E1/E2 reject frozen old pending work, reopen the same file in a subprocess, block destructive upgrade, and upgrade an empty retired file normally. |
| T1/T2 catalog retirement | `TestCatalogRetirement_MultipleTablesPartitionsAndAtomicFailure` checks two retired tables, partition guards, active FK rejection, SQL fault rollback and exact revision/next ID. The rollout journey inventories a serving-role dependency and verifies its DML privileges are atomically revoked before managed capture guards disappear. Existing registered-catalog and managed-layout tests retain config omission and inventory guards. |
| H1 retained bundle integrity | `TestRolloutTables_RetirementContractAndLostResponse` creates real mixed-table bundles, rejects premature prune, explicitly expires whole prefixes, compares surviving canonical descriptors/hashes/counts and the complete source-state JSON before/after prune. |
| H2 bounded prune | The table journey resumes each batch through a fresh authority/private runtime, reconciles committed outcomes, asserts maximum row count, distinct scope ownership, unchanged source state and permanent catalog IDs; scale repeats for 100 scopes. Capacity and takeover tests retain closed-state failure coverage. |
| C1 read-only reports | `TestRolloutReport_FenceAndDurableIntent` checks no mutation, omitted writer roles, stable predicate/source coverage, target/policy/backup/limits evidence, stale reports, incomplete-evidence tampering and immutable reservations. Dependency inventory is bounded and execution repeats it under the authority transaction. Policy test checks backup digest, deadline and capacity failure. CLI uses the same Go gate. |
| C2 physical contraction | `TestRolloutColumn_RetirementAndRestrictedContraction` and table journey execute exact transactional DROP RESTRICT migrations and fresh post-contract Bootstrap; mobile journeys sync supported clients against the contracted schema. |
| B1 forward recovery | Existing authority crash/index reconciliation, worker partial-progress/process-death and mobile local-upgrade restart tests cover expand/transition/backfill/coexistence. New controller/table/column tests cover retirement and contraction reconciliation without unretirement. |
| B2 terminal recovery cut | `TestRolloutRecovery_FencedArchiveAndReturningReceipts` takes real earlier and terminal pg_dump archives with embedded recovery gates, restores both, proves admission remains closed before verification, rejects missing commits despite unchanged deployment/ledger and latest archive labels, and binds the durable verification result before exact lost-response replay and checkpoint continuity. Returning identities are direct Go protocol callers; no source/reset substitutes are used. |
| O1 observability | `TestGenerationObservation_BoundedLabelsAndUnavailable` checks disabled/overflow/zero, concurrent fixed rejection buckets and absence of client labels. Cleanup/table tests assert complete bounded durable telemetry, audit counts, durations and unavailable process observations. |
| E1/E2/E3 real clients | `TestMobileFlow_RolloutColumnRetirementSameFiles` and `TestMobileFlow_RolloutTableRetirementSameFiles` extend the transition/table-evolution journeys on the same PostgreSQL authority and SQLite files; table journey activates a later generation and upgrades the existing supported file. |

## Parent §17 ownership

| Item | Go ownership | Later runtime ownership |
| --- | --- | --- |
| 1 fresh generation 1/2 | `TestOpening_FreshCreationPublishesReadyAtomically`; `TestNewOpening_ClaimsOwnershipBeforeSQLiteConfiguration`; upgrade/transition/table-evolution mobile journeys | other runtime implementation; cross-runtime parity run |
| 2 old outbox and rebuild | `oversqlite/upgrade_process_test.go`, `upgrade_test.go`, retirement intent/boundary tests; E1/E2 | other runtime parity |
| 3 application migrator/version/rollback/local-only | `TestOpening_I05AtomicMigrationFailureMatrix`, `TestOpening_LocalOnlyPreservesSyncStateAndAdvancesVersion`, `TestOpening_I10EveryLocalBoundaryProcessDeath`, contract validation tests | SQLiteNow existing migration tests plus other runtime parity |
| 4 same user, old/new concurrent devices | transition/table-evolution mobile journeys and production Go upgrade E2E | other runtime parity |
| 5 offline old write after expansion | `TestMobileFlow_TransitionPostgreSQLTransitionAndUpgrade`, `TestMobileFlow_TableEvolutionAddRemoveFKSameFile` | other runtime parity |
| 6 server writes in coexistence | transition capture and table-evolution table population tests; mobile journeys | Server contract shared; client parity other runtime parity |
| 7 interrupted DDL/backfill/local migration/rebuild | `TestEvolution_AuthorityCrashAndAmbiguousIndex`, worker recovery tests, upgrade process death matrix | Server contract shared; client parity other runtime parity |
| 8 coordinated deployment/drain/mismatch | readiness/admission tests, F1/F2/F3 | Server contract shared; client error behavior other runtime parity |
| 9 push boundaries | generation protocol/direct-push tests, retirement boundary corpus, full mobile HTTP matrix | other runtime parity |
| 10 canonical retrieval/verification | `TestGenerationVisibleDataPlane_DifferingViewsPushReplayPullSnapshotAndCoverage`, projection integrity tests | other runtime parity |
| 11 hidden history | `TestProjectCommittedBundle_FiltersDeterministicallyAndPreservesVisibleEffects`, checkpoint-only projection tests, H1 | other runtime parity |
| 12 generation snapshots/limits | generation data-plane integration plus existing snapshot memory/limit/guard tests | other runtime parity; existing memory gates unchanged |
| 13 hidden watch wakeups | watch client tests, data-plane projection, readiness watch races and real fleet watch | other runtime parity |
| 14 conflicts and versions | maintenance-conflicts v2 six scenarios, `oversqlite/conflict_*test.go`, transition mobile journey | other runtime parity public resolution APIs |
| 15 add/retire catalog | registered catalog/adoption tests, table-evolution additive table tests, T1/T2/H2 | Server contract shared; table clients other runtime parity |
| 16 nullable-to-not-null | transition constraint/certificate/backfill tests and direct mobile column journey | other runtime parity |
| 17 FK rules | schema generation FK shape/action/set tests, managed layout, table-evolution table/mobile FK tests, T2 | other runtime parity |
| 18 retirement/open sessions/offline | R1/R2/R3 and E1/E2 | other runtime parity |
| 19 rollback/forward recovery | B1/B2; no inverse retirement or nonzero-RPO same-authority reopening | Runtime recovery parity other runtime parity |
| 20 runtime lifecycle parity | Direct Go evidence only | KMP, Dart/Flutter and Swift remain unqualified here |
| 21 API/OpenAPI drift | package exported-surface guard, locked OpenAPI/server response tests, four portable guards | Generated APIs/runtime-specific drift checks other runtime parity |
| 22 no aliases/transforms/default injection | `TestSchemaGenerationDeclarationSurface_IsStrictlyDeclarative`, typed configuration/closed wire vocabulary tests | other runtime parity compiler/runtime bindings |
| 23 exact stored values/transition-before-capture | wire domain and projection tests, transition capture, table-evolution population tests | Shared server contract; runtime codecs other runtime parity |
| 24 preserving conflict/deployment scenarios | maintenance-conflicts v2 guard and its six owning Go consumers | non-Go public/generated APIs; all-runtime parity |

## Portable families and limits

Retain client-upgrade v1, maintenance-conflicts v2 and table-evolution v1. The new
rollout-retirement v1 corpus adds 28 input cases: four loop, twelve HTTP, two intent,
eight maintenance/retirement boundary and two same-file journey cases. The independent
Python guard validates schema, duplicate keys, exact hashes, bounded case structure and
expected preservation semantics, then requires PASS events from actual Go consumers.

Client-upgrade manifest revision 15 records the ready-only opening ownership change and the
authorized CU-C01 post-U8 ready correction. No other portable event or wire expectation changed.
The retired-proof lookup remains the explicit accepted narrow generation amendment.
Kotlin, Dart and Swift implementations do not become qualified from Go fixture execution.

The reference uses local child processes and PostgreSQL privileges, a fixed application
migration allowlist and complete fenced-cut custom-format PostgreSQL archives. It supplies
no cloud orchestrator, arbitrary migration engine, WAL replay adapter, retired-intent upload
route, or automatic production policy. Scale results are fixture measurements, not a
production latency or memory SLA.

Run the nine expensive rollout application scenarios explicitly with
`ROLLOUT_QUALIFY=1 go test ./integration/schema_evolution -run '^TestRollout(Ledger_|Report_|Policy_|Cleanup_|Controller_|Fleet_|Column_|Tables_|Recovery_)' -v -count=1 -timeout 570s`
under the 600-second wrapper and `GOTOOLCHAIN=go1.25.12`. Set `ROLLOUT_PG_MAJOR=17`
for the declared equivalence subset. Ordinary aggregates report these cases as skipped;
the qualification lane must show PASS for all nine, with zero skipped selected cases.
The separate scale selector is `ROLLOUT_SCALE=1` and keeps its own 600-second budget.

The core PostgreSQL fixture now defaults to the image's durable Docker volume on
macOS, including `scripts/test-lane.sh package` and `scripts/test-lane.sh full`.
Set `OVERSYNC_TEST_DOCKER_VOLUME=0` to require a host bind mount or `=1` to require
a Docker volume on any platform. Other platforms retain the bind-mount default.
Normal fsync and SQL settings remain unchanged, and container removal cleans up the
volume. The original rollout final macOS lanes explicitly selected `=1`; the later
default change is recorded separately in `.artifacts/rollout/storage-default-followup/`.
This avoids filesystem sharing
latency without changing the accepted 240-second outer or 210-second Go deadlines.
The existing 29-case managed-layout drift test retains every assertion and limits
its independently provisioned databases to four concurrent fixtures.
