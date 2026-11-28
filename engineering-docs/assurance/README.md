# Oversync Assurance

This directory records the durable assurance posture of the current Oversync
implementation. It replaces the dated 2026-07-10 audit journal and append-only
remediation log as the maintained engineering reference.

Use this document for the current invariants, validation policy, and evidence
ownership. Use [remediation-status.md](remediation-status.md) for the disposition
of the original `OS-AUD-*` findings.

## Sources of truth

The assurance documents summarize behavior; they do not replace executable or
public contracts. When a summary conflicts with a source below, fix the summary
and preserve the source-of-truth contract.

1. Production code and permanent tests under `oversync/`, `oversqlite/`, and
   `internal/` own executable behavior.
2. `swagger/two_way_sync.yaml` owns the HTTP wire schema.
3. [Server](../../docs/documentation/server.md),
   [API](../../docs/documentation/api.md),
   [architecture](../../docs/architecture.md), and
   [performance](../../docs/documentation/performance.md) documentation own the
   maintained operator and consumer guidance.
4. This directory owns the assurance register and remaining-risk summary.
5. Git history owns command transcripts, local timings, temporary infrastructure
   details, and superseded implementation journals.

Locally ignored `specs/` files are execution contracts for individual work
programs. They are not durable repository documentation and are intentionally
not required to understand the maintained assurance posture.

## Scope and interpretation

The original assurance audit inspected `oversync/` at tag `v0.2.1`, commit
`1f1118f1749490daaa8f1f76a7d8516f5303a873`. It was a local correctness,
durability, protocol, resource-safety, and performance audit. It explicitly did
not assess vulnerabilities, exploitability, authentication, authorization,
database privileges, deployment hardening, or overall security posture.

Later remediation and feature work changed the implementation substantially.
The original finding text is therefore historical evidence, not a statement
that every defect still exists in the current checkout. Conversely, later code
that overlaps a finding does not close it without a focused regression,
compatibility review, and explicit update to the maintained register.

This repository currently requires:

- language and consumer minimum: Go 1.25.0;
- reproducible development, validation, benchmark, and memory-evidence
  toolchain: Go 1.25.12;
- PostgreSQL 17.11 for every database-backed assurance lane; and
- Docker for testcontainers and restart/crash tests.

Go 1.26 results in the historical audit remain historical only. They are not a
substitute for the normative Go 1.25.12 lane.

The public server requires PostgreSQL 17. All database-backed assurance lanes,
including managed-layout validation, are pinned to PostgreSQL 17.11.

## Durable invariants

The implementation and future changes must preserve these target invariants.
Open entries in the remediation register identify known or not-yet-revalidated
exceptions; listing an invariant here does not conceal those gaps.

- **Exact identity:** user, source, session, hash, key, and other protocol tokens
  are exact values. Presence checks do not trim or normalize them.
- **Atomic commit:** business changes, captured effects, bundle metadata, source
  watermarks, and staged-session retirement commit or roll back together.
- **Idempotent replay:** a committed source tuple resolves to its one durable
  result and never applies the same logical bundle twice.
- **Row-state equivalence:** every live authoritative row has one matching live
  row-state entry; tombstones have no live business row.
- **Bundle integrity:** row count, ordinals, payloads, keys, operations, row
  versions, canonical byte count, and hash describe the complete committed
  bundle deterministically.
- **Retention safety:** `retained_bundle_floor` is the highest discarded bundle.
  Pull from the floor is valid; a lower checkpoint fails with
  `history_pruned`; a checkpoint or positive target above committed history
  fails with `checkpoint_ahead`.
- **Snapshot completeness:** a frozen snapshot contains every live row exactly
  once, excludes tombstones, uses deterministic ordering, and is served and
  applied with bounded row and byte work.
- **Bootstrap safety:** a fresh layout is created or populated data is adopted
  atomically. An existing marked layout attaches only after registered
  declarations and the exact managed definition validate.
- **Fail-closed schema support:** unsupported persistence, nullable identities,
  managed-layout drift, and unsupported registered-table shapes reject before
  readiness or partial managed mutation.
- **Bounded cleanup:** cancellation, expiry, explicit deletion, shutdown, and
  connection failure release owned transactions, permits, listeners, sessions,
  staging rows, and temporary resources within configured bounds.

## Current implemented safeguards

| Area | Maintained contract | Executable owners |
| --- | --- | --- |
| Numeric values and hashes | `jcs_uniform_numeric_strings_v1`; canonical numeric strings, independent request and committed-bundle hashes, fail-closed type conversion | `internal/jcs`, `internal/protocolhash`, `internal/wirevalue`, numeric wire tests |
| Registered storage | Only permanent logged PostgreSQL relations are accepted | schema bootstrap and PostgreSQL restart tests |
| Destructive operations | Registered-table `TRUNCATE` is rejected, including bundle and `CASCADE` paths | managed trigger and bootstrap tests |
| Retention and recovery | Exact floor/ahead semantics; history-pruned clients rebuild through snapshots before advancing durable checkpoints | pull, retention, recovery, and lifecycle tests |
| Fresh populated adoption | Registered writes are locked; one transaction installs managed state, adopts rows, and validates exact final equivalence before readiness | populated-table adoption and failure-injection tests |
| Identity nullability | Registered owner and visible-key columns must be explicitly `NOT NULL`; managed triggers also reject NULL identities | schema contract, trigger, and client visible-key tests |
| Managed layout | `server_postgres_sync_v1` is compared semantically and deterministically; drift fails closed without repair | managed-layout and restart tests |
| Trusted restart | A coherent marked layout validates declarations and managed definitions without scanning or rewriting business or operational data | bootstrap attachment query-shape and restart tests |
| Snapshot resource bounds | Server materialization uses keyset pages and bounded COPY batches; session, row, byte, chunk, concurrency, and cleanup limits are enforced; the Go client stages durably and applies atomically in bounded pages | `snapshot_*` server/client tests and capability/OpenAPI regressions |

The exact public and operator details remain in the maintained docs linked under
Sources of truth. This table is an assurance index, not a second API contract.

## Validation policy

The normal local validation floor is:

```bash
GOTOOLCHAIN=go1.25.12 go test ./oversync ./oversqlite
GOTOOLCHAIN=go1.25.12 go test ./...
GOTOOLCHAIN=go1.25.12 go vet ./...
```

Run relevant focused race, PostgreSQL restart, real-server, OpenAPI, client, and
cross-runtime lanes when their owners change. The `oversync_audit` build tag
contains both promoted green regressions and intentional expected-red coverage
for findings that remain open, so use explicit selectors and document their
expected disposition rather than treating the whole tagged package as one green
gate.

The 100,000-row, one-million-row, measured-memory, high-concurrency, and heavy
real-server matrices are local-only. They must use test-owned databases,
`GITHUB_ACTIONS` unset, and the repository's fail-closed guard wrappers. Normal
CI should run deterministic correctness and structural-boundedness coverage,
not the heavy acceptance workloads.

## Historical evidence

The removed journals remain recoverable from Git without carrying stale status,
worktree paths, wall times, ports, database names, or ephemeral review IDs in
the active documentation:

```bash
git show e08879263c44934c1470084a8792c301f17bb0df:engineering-docs/audit/2026-07-10-oversync-assurance.md
git show 6489a82d3b0d7557bcc915a097d5b71407e5c9d2:engineering-docs/remediation/2026-07-10-oversync-assurance.md
```

The historical Phase 6 CPU, heap, and block profiles remain under
`engineering-docs/audit/artifacts/2026-07-10-phase6/`. They are diagnostic
evidence, not current performance baselines or production SLOs.
