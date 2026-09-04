# Application PostgreSQL evolution

This package implements the mobile integration fixture's transition revision-4
migration authority. Core Oversync verifies typed readiness evidence and captures
ordinary transactional writes; the application owns DDL, conversion, maintenance,
census, progress, certificates, capacity and deployment membership.

The controller first installs an independently authenticated runtime allowlist.
Every client admitted after migration must preserve ordinary conflicts before
the server migration begins. A generation fingerprint does not establish this
behavior. `ClientRuntimeGuard` supplies the fixture's application-owned 426
response. The mobile fixture allowlists an issued signed bearer token; production
applications need their own authenticated deployment policy. Preserve local files
and outboxes while an unsupported runtime is updated in place.

Use one continuous maintenance window:

1. Disable restarts of stale application processes and inventory all writers.
   `Drain` durably closes admission before invoking its required external-writer
   callback, withdraws every supplied service, closes streams and drains admitted
   operations. Its 60-second deadline is absolute. Failure returns no drain proof
   and leaves admission closed. Controller membership truth remains external.
2. `OpenAuthority` requires completed drain. Apply exact migration files through
   0003, publish the old-only maintaining configuration, record the independent
   client prerequisite with `RequirePreservingClients`, then `Activate` the job
   with fixed bounds and its finite scope/physical-key census. Resolve initializing
   scopes before census completion; an initialization lease is never skipped.
3. Close authority before Bootstrap. Privately Bootstrap the migration runtime
   against that exact old-only configuration. Do not reopen serving. Run `Worker`.
   Only its unexported epoch/owner capability enters ordinary `ScopeManager.ExecWrite`
   while the fence is closed. It uses migration credentials to issue a transaction-
   bound SQL permit; no client header or user-set PostgreSQL setting grants one.
4. Stop the worker. Drain its private runtime, open authority, and `Certify(3)`.
   Apply 0004 to switch authority and validate constraints. `Certify(4)` must issue
   a fresh certificate for the post-switch manifest and transition. Publish the
   two-generation configuration only after that certificate succeeds.
5. Close authority, Bootstrap all intended fresh serving members, and call
   `ReopenServing`. It requires completed drain, matching startup reports, a stopped
   worker, unchanged census, post-switch certificate and the client prerequisite.
   Stale deployments are rejected by the durable admission adapter. Only then
   resume ordinary external writers. Failed validation or abandonment stays closed.

`SyncService.ReadinessGuard` wraps all sync routes before authentication and proof
parsing. Direct Go operations and background cleanup also consult admission.
Every admitted operation holds shared transaction advisory lock `(1869833075,9)`
through completion; ordinary business SQL takes the same lock in its statement
trigger. Drain and worker takeover wait for the exclusive lock. This accounts for
in-flight writes across multiple requests without pretending the whole upload is
one transaction. Staging and exact committed receipts remain governed by existing
protocol retention/recovery rules. `MaintenanceStatus` reports the durable epoch,
drain state and `maintenance_in_progress`; a private Bootstrap report alone does
not authorize serving.

The dedicated authority session lock is `(1869833075,6)`, with a ten-second
acquisition budget and sixty-second migration statement budget. Bootstrap backs
off outside a transaction during contention, retaining no waiting snapshot that
could block concurrent-index completion. Worker ownership uses `(1869833075,7)`.
After owner loss, a successor drains predecessor transactions before changing the
owner token. Connections with uncertain cleanup are discarded before pool return.

The four exact-byte SQL files replay on fresh databases. 0002 adds nullable fields
and `convert_person`; both backfill and the BEFORE trigger call that real SQL
function. The domain is exactly `name|new_label`, with nonempty ASCII alphanumeric
or underscore components. The trigger uses null-safe actual-value changes, accepts
the documented partial UPDATE combinations, rejects incomplete replacement INSERTs,
and never trims invalid input. There is no Go conversion or remote preparation.

Create scope inventory through `CreateScope` before initialization or import.
`DeleteScope` records the incarnation tombstone with captured deletion. Maintenance
blocks both operations. Each lifecycle transaction verifies the returned inventory
identity and the exact expected trigger/rule contract before commit, so suppression,
row diversion, or an added trigger effect rolls back business and inventory state
together. The immutable census includes uninitialized physical rows and empty/deleted
inventory entries. Unexpected keys, scopes or incarnations fail closed; they are not
ordinary online races. Final validation covers the complete physical population and
compares it with the census.

The SQL worker defaults to 100 rows, 1 MiB, one attempt per second, thirty seconds
per query/transaction, three bounded outer retries, 64 MiB pending descriptor storage,
one coordinator and one in-flight descriptor. Capacity must include measured
history rates, consumer lag and retention headroom, including offline backlog.
Unavailable/exceeded capacity pauses the run. It cannot force pruning or reopening.

Persist v3 descriptor bytes before execution. They bind epoch, census, application
and SQL/function identities, bounds, sorted keys, source evidence, UUID, writer,
effect sets and a seven-day immutable deadline. A conditional scoped SQL UPDATE
changes only rows violating the target invariant. Changed rows use normal row
versions, capture, bundle history and receipts. Missing or changed selected rows
invalidate the attempt. Never reinterpret the retained v1/v2 encoding vectors as v3. The revision-5
capacity-contract decision authorized this new format; old descriptors fail closed.

`ExecWrite` rolls back no-effect callbacks, including their checkpoint writes.
A separate application transaction rechecks ownership, scope, rows and invariant
before committing a no-op checkpoint. It emits no fake history and claims no
rolled-back core initialization. Retry uncertain outcomes with the exact immutable
identity. After expiry, reconcile terminal outcomes and current invariants before
retiring the old descriptor and selecting proven remaining work under a new UUID.

Runtime credentials must not own application functions, DDL, maintenance, census,
job plans, permits or certificates. `GrantRuntime` grants ordinary business access,
control reads and scope lifecycle access; worker/coordinator credentials remain
separate. Bootstrap needs core-managed DDL privileges, provisioned while all
ordinary writers are fenced. Restrict/revoke migration ownership before serving;
never give untrusted clients database credentials. All non-SDK writers must obey
the same fence, and old binaries that lack it must be excluded by the controller.

Ordinary Go clients now expose `SyncStatus.NeedsResolution`, `PendingConflict`,
`ResolveConflict` and `RetryConflict`. The default resolver preserves the complete
frozen request. Explicit decisions bind its identity and current local revision.
Accepting an older server result preserves newer dirty edits; keep/merge refuses
to overwrite newer edits and requires a fresh decision. Resolver failures, stale
choices, trigger side effects, cancellation and failed commits preserve intent.
Rebuild, pull and uninstall cannot bypass an unresolved conflict. The upgrade
coordinator retains its own bound decision journal during a generation upgrade.

The versioned `conformance/maintenance-conflicts/v2` supplement is shared with
future KMP, Dart/Flutter and Swift implementations; their execution remains pending
other runtimes. Existing upgrade fixture meanings are unchanged. Restore business data,
control/census/progress, `sync.*`, and matching binary/registry artifacts together;
keep admission closed until reconciled and validated. Full fleet orchestration,
retirement, contraction and large-table operational qualification remain rollout+.


Revision 5 requires explicit total limits in `Bounds`: `Deadline`, `MaxEvidenceBytes`,
`MaxHistoryBytes`, `MaxBundles` and `MinFreeDiskBytes`. DefaultBounds supplies only
per-batch/rate limits and is deliberately insufficient for activation. Supply a
fresh Capacity measurement to Activate; Worker.Capacity must keep reporting free
disk, lag and retention. Fixture values are not production recommendations.

Evidence accounting sums retained PostgreSQL row values, including census, both
descriptor representations, outcomes, row failures and reserved diagnostic space.
It is distinct from physical free disk (including indexes/WAL/overhead), which the
application must measure. Worst-case history reservations precede descriptor
insertion; uncertain outcomes retain them, while completed metrics replace them
idempotently with canonical byte counts. Restart retains the absolute deadline.

The serving ServiceConfig.Readiness.AdmissionPool is mandatory for this adapter.
Create a separate bounded pgxpool with the same database, restricted credentials,
primary and fallback routes, TLS identity, protocol bounds and RuntimeParams. Use
pgx's default connection transport; notice, error and tracing callbacks remain
permitted, but connection/session/acquisition hooks do not. Both pools are caller-owned:
close the service before closing either pool. Include both pools in deployment
connection capacity. Initial bootstrap and catalog updates use provisioning credentials;
serving Bootstrap attaches to an existing validated layout. GrantRuntime revokes
TRIGGER on the business table and independently rejects protected schema/table/function
ownership, administrative attributes and reachable provisioning roles. Membership
checks conservatively include transitive roles even when INHERIT/SET options currently
deny their use. Remove those memberships before serving. Grant/revoke validation is
atomic. PostgreSQL ownership cannot be neutralized through ordinary privilege revocation:
https://www.postgresql.org/docs/16/ddl-priv.html and
https://www.postgresql.org/docs/16/role-membership.html.

Watches release database admission after registration and terminate when drain begins.
Each service permits one running listener and owns one additional connection outside
execution pool capacity. Acquisition uses the execution pool's connection/session hooks,
then transfers ownership with Hijack. Budget this extra connection per service; draining
closes it before returning. Listener reconnect/catch-up leaves a one-connection execution
pool available for ordinary sync operations.

ScopeWriteOptions.MaxBytesPerBundle tightens canonical finalization for each batch;
include its value in the application's frozen operation identity. The reference
worker binds it through BoundsDigest. Overflow rolls back the entire transaction.
WriteRollbackUnconfirmedError prohibits failure checkpointing until cleanup is
reconciled; it does not prove a successful commit or rollback.

Retained certificate rows count toward MaxEvidenceBytes. Certification reserves space
and checks the final retained total before committing either the certificate or validated
job state. Application source identity includes this accounting policy: an updated worker
cannot resume a previous frozen job under different code or limits.

### Serving-role qualification and retained evidence

GrantRuntime rejects reachable SET privileges on session_replication_role and
column-level INSERT/UPDATE privileges on application control tables, including
inheritance and SET ROLE paths. Scope inventory permits INSERT and UPDATE(deleted),
but not updates to its identity columns, DELETE, TRUNCATE, or trigger replacement.
An unsafe role is rejected atomically; provisioning must remove unsafe grants before
attachment. PostgreSQL's effective privilege functions include PUBLIC grants.
See [parameter privileges](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-SESSION-REPLICATION-ROLE)
and [column privilege inspection](https://www.postgresql.org/docs/16/functions-info.html#FUNCTIONS-INFO-ACCESS-TABLE).

Worker evidence mutations check the persisted total limit before commit, including
conversion diagnostics, outcomes, metrics, and job metadata. Reservations remain
conservatively counted after completion. New qualification freezes bounds at activation
and covers actual committed-but-unobserved COMMIT replies, replay, and exhaustion.

Worker metadata transactions take the shared admission lock and validate their original
capability before mutation. Takeover takes the exclusive lock, so it waits for already
admitted transactions and fences later predecessor writes. Pause and duration cleanup
retain the capability with cancellation removed and a finite timeout. A lost advisory
lock connection cannot leave a still-running predecessor able to pause its successor.

Census activation persists the immutable bounds before copying any population.
Each transaction copies a stable keyset page and commits its cursor and SHA-256
state. Calling `Activate` again with the exact bounds resumes those pages; it
cannot extend the deadline. Capacity exhaustion preserves completed pages and
keeps admission closed. Publication performs a completeness comparison under the fence.

For a confirmed `invalid_historical_input` failure, call
`Worker.Repair(ctx, failedOperationID, rowKey, approvedLegacyName)`. The replacement
must pass the same SQL conversion before approval is frozen. This creates a fresh
operation with the `app-backfill-repair-v1` extension to the v3 descriptor, linked
to the unchanged failed descriptor. A captured SQL UPDATE, final invariant check,
and repair outcome commit atomically. The old failure evidence survives resolution.
Repeating the same approval reconciles the operation; a different approval fails
closed. Restart `Run` to reselect the original interval and finish validation.
Neither the run deadline nor cumulative capacity is renewed by a repair. Policy or
algorithm changes require a new explicitly qualified job contract.

Serving and admission pools must use the same declarative connection parameters.
Arbitrary BeforeConnect, AfterConnect, BeforeAcquire, PrepareConn and AfterRelease
hooks are rejected: Go function pointers cannot prove equivalent captured state.
Use identical `RuntimeParams` for session initialization. Actual acquired execution
and admission sessions must also report `session_replication_role=origin`; applicable
role/database defaults outside origin fail runtime-role provisioning checks.

The separate [table-evolution additive table/FK harness](table_evolution/README.md) starts from a
real old registration set and adds private expiry cleanup, an FK-absent catalog append,
captured population, and a same-file add/remove upgrade. Its migration manifest is for a
separate application database; do not mix it with this column-evolution manifest.

The rollout [rollout operations guide](ROLLOUT.md) covers permanent generation history,
private cleanup/retirement, bounded pruning, physical contraction, real fleet fencing
and recovery-cut verification.

The [qualification coverage ledger](QUALIFICATION.md) maps rollout tests to the parent
contract and identifies the later SQLiteNow and cross-runtime gates.
