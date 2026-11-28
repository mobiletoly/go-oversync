# Oversync Assurance Remediation Status

This register records the durable disposition of findings from the 2026-07-10
Oversync assurance audit. It replaces the chronological remediation ledger.

## Status semantics

- **Closed:** the original reproducer has a permanent green regression and the
  accepted contract is implemented within the stated compatibility envelope.
- **Open:** the original finding has not received a complete, explicitly
  accepted closeout. Later work may provide partial coverage without closing the
  broader finding.
- **Investigation:** the original evidence was a hypothesis and still requires a
  deterministic reproducer before remediation is authorized.

These states describe the assurance program, not whether a nearby line of code
has changed. Reclassifying a finding requires focused source inspection,
expected-red or characterization evidence where applicable, permanent green
regressions, compatibility review, and current Go 1.25.12 validation.

## Finding register

| Slice | Finding | Severity | Status | Durable disposition |
| --- | --- | --- | --- | --- |
| C1 | `OS-AUD-001` exact-value corruption | Critical | Closed | RFC 8785 JCS plus `jcs_uniform_numeric_strings_v1`; exact schema-typed numeric strings; independent request and committed-bundle hashes; invalid mappings reject before mutation. |
| C2 | `OS-AUD-018` unlogged registered tables | Critical | Closed | Bootstrap accepts only permanent logged relations and rejects unsupported persistence before managed mutation; crash/restart coverage preserves authoritative and sync state. |
| C3 | `OS-AUD-002` uncaptured `TRUNCATE` | Critical | Closed | Unconditional statement-level guards reject registered-table `TRUNCATE`, including explicit transactions, bundle context, partitions, multi-table statements, and `CASCADE`. |
| H1 | `OS-AUD-003` retention boundaries | High | Closed | The retained floor is the highest discarded bundle; equality is valid, lower checkpoints fail, future checkpoints/targets fail, and recovery advances the checkpoint only with final atomic snapshot apply. |
| H2 | `OS-AUD-005` populated-table adoption | High | Closed | Fresh managed-layout creation atomically adopts authoritative rows under locks and validates final equivalence. Existing marked layouts now use the separately reviewed trusted-attachment contract and do not repeat business/history scans. |
| H3 | `OS-AUD-006` nullable identity | High | Closed | Catalog preflight and managed triggers require non-null scope owner and visible sync key identities; client runtimes reject missing visible keys before managed local-state mutation. |
| H4 | `OS-AUD-007` managed-layout drift | High | Closed | Bootstrap compares the complete `server_postgres_sync_v1` semantic manifest and fails closed with bounded deterministic differences; coherent restart attachment performs no repair. |
| H5 | `OS-AUD-008` token normalization | High | Open | Exact-token helpers and later source/session work overlap this area, but the complete middleware, ScopeManager, replacement-source, grammar, size-bound, and compatibility matrix has not been formally closed as one finding. |
| H6 | `OS-AUD-004` unenforced bundle/request bounds | High | Open | Snapshot-specific bounds now exist, but planned, cumulative staged, committed-bundle, and shared HTTP-body admission require a complete finding-level closeout. |
| H7 | `OS-AUD-009` inconsistent and unbounded parsing | High | Open | Snapshot parsing is stricter, but all request bodies still need one bounded single-document policy with stable duplicate, unknown, case-collision, media-type, path, and 400/413/415 behavior. |
| H8 | `OS-AUD-011` snapshot/pull/watch amplification | High | Open, partial coverage | Snapshot materialization, transfer, client staging/apply, concurrency, and cleanup are now bounded. The finding also owns pull amplification, snapshot-create replay identity, active-session policy, watch subscriber quotas/write deadlines, and listener resource policy; those remaining dimensions prevent closure. |
| H9 | `OS-AUD-019` advisory-lock pool starvation | High | Open | Same-scope lock waiters still require a documented bounded try-lock/backoff and fairness contract that preserves unrelated-scope pool progress. |
| M1 | `OS-AUD-017` generated-column replay | Medium | Open | Generated columns must either be excluded from client-applied assignments while retained in authoritative after-images or be rejected as unsupported registered schema. |
| M2 | `OS-AUD-015` post-close lifecycle mapping | Medium | Open | Shutting-down and closed operations need one typed unavailable result mapped consistently to HTTP 503 across handlers. |
| M3 | `OS-AUD-013` status history scans | Medium | Open | Operational status needs bounded counters or aggregates with a documented freshness and coherence contract instead of work proportional to retained bundle history. |
| M4 | `OS-AUD-014` public contract drift | Medium | Open | OpenAPI and maintained docs have received later updates, but the original mismatch set needs a focused runtime/spec golden review before formal closure. |
| L1 | `OS-AUD-016` unused declarations | Low | Open | Remove only proven-dead code or promote intended extension points to exercised documented surfaces; close against current staticcheck output, not the historical count. |
| I1 | `OS-AUD-020` mixed application/database clocks | Medium hypothesis | Investigation | Reproduce skewed service clocks against one PostgreSQL clock before classifying or authorizing remediation. |

`OS-AUD-010` and `OS-AUD-012` were never assigned and remain intentionally
unused.

## Closed contracts that remain operationally important

### Numeric and hash cutover

The numeric/hash correction is a breaking v0 contract. Historical values and
hashes rounded by an older implementation are not reconstructable. Deployments
that contain incompatible synchronized state must stop old writers, recreate
the affected server and client databases, and deploy compatible components as
one release set. Mixed legacy and corrected writers, rolling compatibility, and
automatic repair are unsupported.

SQLite `REAL` remains approximate by contract. Exact decimal values remain
ordinary `TEXT`; PostgreSQL integer, decimal, and floating values use canonical
numeric strings on the wire. Boolean output remains JSON Boolean.

### Registered PostgreSQL storage

Registered roots and current descendants must be permanent logged relations,
must expose exactly one supported visible sync key, and must declare both the
visible key and `_sync_scope_id` as `NOT NULL`. Correct nullable or unsupported
storage according to application policy before Bootstrap. Oversync does not
silently migrate those schemas.

Registered-table `TRUNCATE` is never an application reset mechanism. Supported
administrative reset is stopped-process, whole-database recreation followed by
a reviewed fresh bootstrap. Privileged trigger bypass, replication-role bypass,
direct managed-row edits, and unreviewed partition/managed-trigger DDL are
outside the supported trust boundary.

### Fresh adoption versus trusted restart

When no managed layout exists, Bootstrap locks registered writes and creates,
adopts, and validates the complete managed state in one transaction before
readiness. It does not alter or delete authoritative business rows.

When a coherent marked layout exists, Bootstrap validates registered
declarations, marker/catalog identity, and the exact managed semantic manifest.
It deliberately does not scan business rows, inspect operational sync data,
take explicit data locks, run adoption, canonicalize payloads, hash retained
history, perform cleanup, execute DML/DDL, or repair the database. This path
trusts PostgreSQL durability after the original atomic creation/adoption.

If the marked layout or registered declaration is unsupported, stop affected
instances and restore a reviewed coherent backup or follow a separately
reviewed operator procedure. Automatic repair and rolling mixed-version
migration are not part of this contract.

### Retention and snapshot recovery

Pull is valid from `retained_bundle_floor` and returns bundles strictly above
it. Lower checkpoints return `history_pruned`; future checkpoints and positive
targets return `checkpoint_ahead`; target zero captures the current committed
ceiling. A positive target below the checkpoint is invalid.

Clients reconcile durable pending work before destructive recovery, stage the
complete replacement snapshot durably, recheck the final gate inside the write
transaction, replace managed rows atomically, and publish lifecycle/checkpoint
state only after commit. Failures preserve retryable durable state or roll back
the replacement; they do not expose a partially applied snapshot.

## Remaining-work rules

1. Handle one open finding or one explicitly bounded corrective at a time.
2. Do not infer closure from green default tests or from later feature overlap.
3. Promote the original finding-level reproducer or replace it with an equally
   strong permanent regression before changing status.
4. Review wire, persisted-state, generated-client, and operator compatibility
   explicitly for every closeout.
5. Use Go 1.25.12 for normative Go validation, and PostgreSQL 17.11 for every
   database-backed assurance lane.
6. Keep heavy memory, scale, concurrency, and real-server matrices local-only;
   keep deterministic fail-closed guards in normal CI.
7. Update this register only after the focused implementation, validation,
   independent review, and user acceptance boundary for the finding is complete.

For current runtime details, see [Oversync Assurance](README.md) and the public
[server](../../docs/documentation/server.md),
[API](../../docs/documentation/api.md), and
[performance](../../docs/documentation/performance.md) documentation.
