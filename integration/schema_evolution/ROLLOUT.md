# Rollout operations and generation retirement

The integration authority adds closed-maintenance retirement to the column and table
evolution fixtures. Runtime services do not migrate themselves. Production policy,
fleet routing and backup custody belong to the application operator.

## Authority and phases

`PrepareGenerationLedger` installs permanent canonical generation history under the
drained migration authority. `AllocateGeneration` reserves an ID above every prior ID;
`AbandonGeneration` permanently closes an unused reservation. Allocation does not enable
the generation. Runtime roles cannot edit history. Startup checks archived declarations,
their fingerprints and exact agreement between supported entries and the compiled registry.
The ledger holds at most 4096 entries, with a 1 MiB per-declaration and 16 MiB total archive
read bound. Capacity exhaustion fails closed; tombstones are never evicted.

Each `RolloutPlan` identifies one phase, the current epoch/deployment/catalog/ledger,
the exact compiled target digest, immutable policy and finite execution budgets.
Policy requires positive support and notice windows, replacement channel evidence,
independent preserving-client admission, backup evidence, accountable approval and the
explicit disposition `preserve_unresolved_no_retired_upload`. A rollback hold blocks work.
`BackupEvidence` is the lowercase SHA-256 identity of the externally retained evidence,
not a free-form success label.
There is no default support duration or automatic retirement based on inactivity.

The table targets are ledger (6), generation retirement (7), registration retirement
(8) and physical contraction (9). Column targets are ledger/certification (5),
generation retirement (6) and physical contraction (7). `RolloutSuccessorConfiguration`
supports a later generation with the unchanged surviving wire schema; a schema change
requires its own application migration and qualification.

The operation actions are `prepare`, `cleanup_staging`, `retire_generation`,
`enable_generation`, `retire_tables`, `expire_history`, `prune_table_state` and
`contract`. Each phase has its own reviewed report. Reopen and archive recovery use
the concrete process/archive drivers described below, rather than a supplied boolean.

## Operator command

Build `integration/schema_evolution/cmd/rollout`. Supply the authority connection through
`DATABASE_URL`; keep credentials and client allowlists outside version control.
An input is a JSON `RolloutExecution` containing its plan and compiled table or column
target selection, target ledger and explicit qualification digest.

```sh
GOTOOLCHAIN=go1.25.12 go build -o /tmp/rollout ./integration/schema_evolution/cmd/rollout
/tmp/rollout report --plan proposed.json --format json > reviewed.json
/tmp/rollout apply --plan proposed.json --report reviewed.json
/tmp/rollout status --operation-id OPERATION_ID
/tmp/rollout resume --operation-id OPERATION_ID
```

`report` is a repeatable-read, read-only transaction. It records sorted predicate results
and evidence sources, target operations, database/deployment/catalog/ledger identity,
policy and backup/rollback status, writer and dependency inventories, session/capture and
retention counts, transition invariants, and bounded estimates/limits. Incomplete scans or
byte/time exhaustion return an error. A complete report lists deterministic blockers; it does not
authorize execution. `apply` rechecks the report under the exclusive closed authority.
Intent and original limits commit before the phase; mutation, batch audit and result
commit together. Lost replies reconcile the original result before constructing an
obsolete private runtime. Resume never extends deadlines or resets attempts, batches
or aggregate audit capacity. The command stores target selection in the immutable plan; resume loads it by operation ID.
Report exit codes are 0 for complete and clear, 2 for blocked, and 1 for evidence or
execution failure. `--format text` summarizes the same report.

Inputs are bounded to 1 MiB. Plans require a positive query timeout no greater than
30 seconds, an absolute deadline, row bound no greater than 4096, and explicit attempt,
batch and evidence capacities. A command is bounded to 60 seconds. Exhaustion leaves
maintenance closed and the operation inspectable. A new plan is not an automatic waiver
of an exhausted or ambiguous predecessor.

## Fleet fencing and reopen

The reference `cmd/rollout-member` is a real child process with its own database role,
Bootstrap, process-start ID and pinned PostgreSQL backend identity. It serves authenticated
sync routes and a controlled writer. `StartLocalFleetProcess` verifies the executable
digest and reads that process's handshake; `ReopenLocalFleet` verifies fresh live endpoints,
backend identities, independent admission digest, full deployment digest and exact intended
membership. Use separate nonprivileged runtime credentials for each serving/writer process.
The authority credential must never be a serving credential.

Use `Drain` with `FenceLocalFleet` to request maintenance, disable every declared runtime
role, terminate its PostgreSQL sessions and stop each process. The authoritative role
inventory also discovers omitted writers, including inherited/SET ROLE privileges,
column writes, sequence privileges and executable security-definer functions. Missing,
stale or mismatched members and any unfenced writer block reopening. Routing or process
disappearance alone is insufficient. Never execute DDL while an old binary retains
database write access. Fresh Bootstrap and a separate reopen plan follow contraction.

## Sessions, history and physical contraction

Unexpired old push/snapshot sessions block retirement. Natural expiry still leaves
durable staging and therefore still blocks it. `cleanup_staging` uses the existing
private cleanup capability with bounded sessions/rows and atomic audit; it does not
shorten TTLs or infer whether a push committed. Source watermarks and receipts remain
authoritative.

`retire_tables` checks live catalog/layout/FK dependencies and global staging/capture
emptiness, locks registered roots and descendants, changes all selected catalog rows
and one catalog revision, requires the plan declaration
`TableWritePolicy: deny_and_revoke_atomically`, revokes effective ordinary DML privileges on every selected
table, and only then removes managed root/partition triggers atomically. Runtime role
provisioning first clears old broad `main` grants and grants DML only on active catalog tables.
The report inventories partitions, triggers, views, functions, policies, foreign keys and
writer roles with their encoded disposition; an external FK is unresolved and blocks.
Catalog IDs, names and next ID remain permanent. Configuration omission is not retirement.

Retained bundle rows referring to a retired table block its row-state pruning, including
mixed-table bundles. Optional explicit `expire_history` applies the existing whole-prefix
retention policy to one scope; it never deletes selected rows from a bundle. A prefix
larger than the declared row budget pauses. `prune_table_state` removes at most the
declared rows in one locked scope incarnation, preserving source state and catalog history.

`contract` admits only the exact checksummed reference migration and declared affected
objects. Both DROP paths use RESTRICT. Post-contract certification validates the surviving
representation and replaces obsolete transition/backfill prerequisites before publishing
the target. A successful physical DROP does not reopen service by itself.

## Recovery and client preservation

Retirement is irreversible in the live authority. Resume the original phase or make a
qualified forward correction. Do not redeploy an incompatible old binary or unretire an ID.

`CaptureRecoveryCut` first commits a `required` recovery gate under the terminal writer fence;
that gate is part of the exported snapshot while `DockerPostgresArchive` streams a real
custom-format pg_dump. The live predecessor removes its copy only after capture completes.
Retain the archive,
cut, independently held cut digest, current lifecycle journal and matching binaries outside
the recovered database. The cut binds database/authority/epoch, WAL position, complete
application/sync/control row and sequence state, archive hash and finite byte budget.

Restore only into the isolated, unrouted, fenced original database identity. Use
`RestoreRecoveryArchive`, apply the narrowly qualified catalog-expression normalization with
`PrepareRecoveredCatalog`, then call `VerifyRecoveredCut` against the independently retained
terminal cut and lifecycle history. Matching generation IDs alone do not establish
continuity. An older archive missing a later upload or observed checkpoint cannot reopen
the same authority, even if the owner accepts an RPO. This adapter qualifies a terminal-cut
archive; it does not implement WAL replay to repair an older archive.

`VerifyRecoveredCut` changes the archived gate from `required` to `verified` and returns a
durable verification digest. A recovery `reopen` plan binds both the external cut digest and
that result digest. Generic serving reopen also refuses a restored database whose embedded gate
is still `required`, so omission and controller restart stay closed. Catalog preparation accepts
only the three known equivalent restore forms and is not a bypass of layout validation.

HTTP 426 `retired_schema_generation` stops automatic Go client upload/download/watch.
Keep the SQLite file, business rows, local-only rows, checkpoint, source identity,
outbox hash/sequence and uncertain receipts. Pending retired work blocks destructive
upgrade. Only a completely drained old file can use the normal supported-generation
migration, source replacement and snapshot rebuild. No retired upload recovery exists.
The stop is recorded in `_sync_action_required`; applications inspect it through
`ApplicationActionRequired(ctx)` without contacting the retired server.

## Measurements and qualification

`ReadRolloutTelemetry` returns bounded aggregate deployment, ledger, history, session,
binding, phase, attempt, prune and evidence measurements. Nullable values and `Complete`
distinguish unavailable from zero. `GenerationObservations` exposes active local work,
configured generation labels, proof request activity and aggregate unknown/retired buckets.
An explicitly configured observation window tracks bounded recent distinct sources;
overflow makes that count unavailable until the affected window expires. It is process-local
proof activity, not proof of completed writes or absent offline devices. Durable binding
counts are separate. Identifiers and fingerprints are not metric labels.

Qualification commands, failures, retries and frozen identities belong in
`.artifacts/rollout/revision1/validation-ledger.md`. Portable consumers live in
`conformance/rollout-retirement/v1`. The scale lane uses `ROLLOUT_SCALE=1`, 100,000 retired
table rows across 100 scopes, bounded history/prune pages, two servers and a writer.
Its timings/RSS are fixture evidence, not a production SLA. Go qualification, independent
implementation review, owner closure, Git actions and later runtime ports remain separate.
