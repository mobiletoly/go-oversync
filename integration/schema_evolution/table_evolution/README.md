# Additive tables and foreign keys

This is the table-evolution worked application, using the same authority lock, admission fence,
origin attestation and scope writer as the transition column example. Its three SQL files
are a separate migration manifest for a fresh application database. Do not apply the
column and table manifests to the same application ledger.

The baseline registers only `people` and `legacy_notes`. The final generation adds
`projects`, `attachments`, and nullable `people.project_id`; it omits `legacy_notes`.
That omission leaves the server table, active catalog identity, capture triggers,
row state and old-generation history intact.

## Controller sequence

1. On a fresh database, `OpenFreshAuthority`, `ApplyTableEvolution(TablesBaseline)`
   and `Publish(TableEvolutionConfiguration(TablesBaseline))`. Close authority,
   Bootstrap a fresh runtime, and `ReopenTableServing`. Register every application
   scope through `CreateScope` before writing its business rows.
2. Install the preserving-client runtime policy, then `Drain` every serving process
   and application writer. Open authority with the returned proof. Record the client
   policy using `RequirePreservingClients` and freeze a nonzero plan UUID, `TableBounds`
   and capacity measurements using `ActivateTableEvolution`. Publish `TablesCleanup`,
   close authority, and Bootstrap its private old-registration runtime.
3. Run `TableWorker.Cleanup`. This uses a dedicated capability, with background workers
   disabled and ordinary APIs closed. It waits only until the immutable deadline for
   disconnected staging to expire. Each transaction locks expired headers, deletes at
   most the approved number of child rows, deletes only empty headers, and commits
   application audit and accounting atomically. Unexpired/locked sessions, capture
   residue, missing capacity or exhausted limits leave admission closed.
4. Stop the cleanup runtime. Under authority apply and publish `TablesAppend`. The new
   tables are still empty and **the people-to-projects FK is absent**. Close authority
   and Bootstrap the provisioning runtime with target catalog revision 2. This appends
   `attachments` and `projects` without renumbering `legacy_notes=1` or `people=2`.
   Stop this runtime; it cannot write business data or open public admission.
5. Under authority apply `TablesBackfill`: install and validate both physical FKs.
   Publish that phase, close authority, and freshly Bootstrap its private runtime.
   `TableWorker.Populate` inserts the deterministic project per scope and assigns
   existing people through captured `ScopeManager.ExecWrite` transactions. The all-zero
   person UUID is a valid business key. `attachments` deliberately remains empty.
6. Stop the worker/runtime, publish `TablesServing`, then close authority and Bootstrap
   each serving process. `ReopenTableServing` checks the final catalog, application
   mechanisms, certificate, coverage and preserving-client policy under the admission
   transaction. The generic reopen helper also rejects intermediate table phases.

Use `NewReadinessPoolPair` for each runtime and pass its execution pool to
`NewRuntimeService`; close the service before closing the pair. Provisioning owns DDL
and catalog/trigger installation. `GrantTableRuntime` grants a separate role attach
and DML privileges after provisioning and rejects reachable ownership, trigger,
replication, role-management and ledger-writing privileges. Catalog tables `sync.meta`
and `sync.table_catalog` are SELECT-only: prior direct write grants are revoked, and
remaining column, PUBLIC or reachable role write privileges reject the grant transaction.
Exact Bootstrap attachment uses the existing exclusive bootstrap advisory lock without
requiring catalog UPDATE permission; provisioning append retains its metadata row lock.
Serving cannot append or repair catalog metadata.
The worker needs a separate trusted controller credential for its application ledger.

Old inserts omit `project_id` and leave it NULL; old updates preserve it. New explicit
NULL clears it. Existing physical endpoint, action, type and nullability semantics
cannot change while supported generations expose them. Enum, native timestamp,
precision/scale changes and generic uniqueness evolution gain no new support here.

## Recovery and bounds

The plan freezes the scope watermark, source/SQL digests, total deadline, per-query
limit, row/byte limits, cumulative history/evidence/bundle capacities, cleanup limits,
lag/retention headroom and attempt/rate limits. Capacity is checked again while work
runs. Scope pages and row selections are bounded. Durable pacing survives process
restart. Every attempt reserves the full bundle byte budget; conservative reservations
remain charged even for no-ops or repairs. Evidence includes pending-outcome headroom.

Captured writes and their checkpoints commit together. A no-capture rollback takes a
separate fenced transaction that revalidates the exact selected rows before checkpoint.
Final certification checks source/target coverage, current captured row state and
retained history, no orphan relationships and the deliberately empty child policy.
Publication, fresh Bootstrap and the first reopen repeat those full completion checks.
The first reopen changes the durable run phase from `populated` to `serving` in the same
transaction that opens admission; a failed reopen cannot relax validation.
After that committed transition, readiness permits ordinary NULL/unassigned rows and parent deletes;
it rechecks certificate identity and referential integrity rather than requiring that
all future writers reproduce the one-time seed state.

After a crash, reacquire worker ownership and resume the same phase and frozen plan.
Repeated completed cleanup/population calls revalidate completion. An uncertain commit
keeps the descriptor unresolved; never relabel a still-running transaction rolled back.
Takeover's exclusive admission lock joins all predecessor transactions before the next
owner can write evidence. A known rollback is recorded as `apply_rolled_back`, without
copying SQL, payloads or secrets into durable diagnostics.

`RepairTableBatch(operationID)` is explicit authorization to retry the identical
transformation after an operational defect is corrected. It fences predecessors,
requires no retained receipt/applied checkpoint for the failed operation, preserves the
old descriptor/evidence and original deadline, and reserves another bounded operation.
It neither skips invalid rows nor permits a changed conversion or widened plan. Such a
change requires a separate reviewed application plan. Expired global budgets remain closed.

`SyncService.CleanupExpiredStaging(ctx, tx)` is the reusable core entry point. The caller
owns a real PostgreSQL transaction, the application capability/admission locks, and audit
commit. Roll back the entire transaction on any error. Finish audit/commit before stopping
that private runtime. Success is provisional until commit. No route exposes this method.
The method cannot mutate source bindings/watermarks, committed results, write receipts,
row state, canonical history or capture residue. Staging deletion is never an acknowledgment.

## Go upgrade and qualification

The mobile example's `tableUpgradeDefinitions` supplies local SQLite DDL. The existing
upgrade coordinator freezes writers and settles the installed-generation outbox, dirty
rows and explicit conflicts before it invokes that DDL. Newer edits surviving a lost-reply
replay drain only after a proven durable source-sequence advance. The migrator creates the
new tables/FKs and drops `legacy_notes`; the coordinator rotates source and installs a full
target snapshot before ready. Unchanged `table_local_notes` survives. Foreign keys stay on.

Run focused application tests from the root:

```sh
GOTOOLCHAIN=go1.25.12 scripts/go-test-with-cleanup.sh ./integration/schema_evolution -run '^TestTableEvolution_' -count=1
GOTOOLCHAIN=go1.25.12 go test ./oversqlite -run '^TestTableEvolution_' -count=1
```

The nested `integration/mobile_flow` module's `TestMobileFlow_TableEvolutionAddRemoveFKSameFile`
uses a real PostgreSQL 16 instance selected by `TEST_DATABASE_URL`, isolated databases,
HTTP, two Go clients, disconnected staging, conflict resolution, and child processes
killed during local migration and snapshot staging. It also covers a dirty delete and
newer intent on the removed table. The portable cases live in
`conformance/table-evolution/v1`; SQLiteNow execution awaits other runtimes and parity cross-runtime.

Implementation qualification records are in `.artifacts/table_evolution/implementation/validation-ledger.md`.
Specification acceptance does not close the implementation review or authorize publication.

Population certification uses bounded keyset validation pages over scope inventory,
recorded scopes, batch outcomes and each relevant business table. History checks
join only the materialized page. Each successful page commits a fixed-size
`validation_cursor` in `table_run` under the existing worker capability and fence.
Cancellation or a failed page does not advance it. Resume retains the plan, epoch,
source identity, original deadline and capacity bounds; it does not repeat population
or reset validation progress. Completion and certificate publication occur only after
all stages finish. Validation format is `table-population-v2`.

Publication, fresh Bootstrap and first reopen independently recheck live invariants
from the beginning using bounded read-only pages. They do not reuse a historical
worker checkpoint, so post-certification restore damage still closes admission.
These readiness calls remain subject to their caller's overall startup deadline;
only the fenced worker owns durable validation progress. Ordinary serving
coexistence checks remain relaxed after the atomic durable serving transition.
