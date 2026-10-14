---
layout: default
title: Performance
permalink: /documentation/performance/
---

# Performance

The current sync path is bundle-based. Performance tuning should focus on bundle capture,
bundle replay, and snapshot rebuilds.

## Server hotspots

- writes to registered business tables inside `ScopeManager.ExecWrite(...)` or
  `WithinSyncBundle(...)`
- trigger capture into `sync.bundle_capture_stage`
- bundle finalization into `sync.bundle_log` and `sync.bundle_rows`
- bounded on-demand generation projection for push replay, committed pages, conflicts, and pull
- `GET /sync/pull` pagination by bundle count and projected-byte budget
- optional `GET /sync/watch` heartbeat interval and subscriber count when bundle-change watch is
  enabled
- chunked snapshot-session rebuild size and retained-floor policy

The server storage layout keeps hot row-bearing tables compact: registered tables are represented
by `table_id`, row keys by `key_bytes`, users by `user_pk`, and operations by `op_code`. Tune the
runtime around those access paths rather than repeated wire-facing schema/table/key JSON strings.

## Client hotspots

- trigger writes into `_sync_dirty_rows`
- durable replay of accepted push bundles
- durable replay of pulled bundles
- snapshot rebuild time for hydrate/recover
- optional bundle-change watch streams as wake-up hints; authoritative download cost still belongs
  to `PullToStable()` / `Sync()`

## Device-load simulation

Parallel `mobile_flow` runs use one process per simulated user by default. The
`--concurrency` setting bounds active child processes. Each child runs the entire
scenario with the existing SDK settings and owns one PostgreSQL verification
connection; the parent verifies each user's final state and collects the report.

For example, start `nethttp_server`, then run
`mobile_flow --scenario=complex-multi-batch --parallel=100 --concurrency=50` with
verification enabled. Use `--parallel-mode=goroutine` for the retained SDK stress
mode, where all users share one process and its SQLite allocator. These modes
measure different deployment shapes; keep their timings and results separate.
Caller cancellation stops further child launches and joins owned workers.

## Sizing snapshot rebuilds

Snapshot limits are application configuration, not fixed framework capacity.
On the server, `ServiceConfig` exposes session, row, chunk, materialization,
concurrency, and cleanup bounds including `MaxRowsPerSnapshotSession`,
`MaxBytesPerSnapshotSession`, `MaxBytesPerSnapshotRow`, the default/maximum
row and byte limits per chunk, and the materialization and cleanup batch
limits. The server publishes the effective transfer limits through generation
discovery.

Clients request both a row limit and a byte limit for every chunk and then
clamp those requests to the advertised server limits. Go applications tune
`Config.SnapshotChunkRows` and `Config.SnapshotChunkBytes`; KMP and Dart expose
the equivalent snapshot chunk and apply-page settings. Keep the row and byte
controls together: a row-only limit does not bound a single large row or a
compressed response after decoding.

Use measured application data to choose limits. The project validation matrix
uses these roles:

- 1,000 rows for small correctness and fixed overhead;
- 10,000 rows as a representative offline-first mobile profile;
- 100,000 rows as a large profile for repeated local measurement; and
- 1,000,000 rows as a single-process extreme resource-safety proof.

The matrix also uses approximately 256-byte and 1 KiB encoded rows. These are
test shapes, not protocol requirements or universal application ceilings. The
one-million-row case proves bounded memory, atomic rollback, cleanup, and
resource safety on a recorded reference host; it is not a mobile latency SLO.

The 100,000-row, one-million-row, measured-memory, high-concurrency, and heavy
real-server lanes are local-only. They require explicit opt-in, fresh
test-owned PostgreSQL and SQLite databases, and `GITHUB_ACTIONS` unset. Normal
CI should retain deterministic correctness, fixture, analyzer, and structural
boundedness tests without enabling these workloads.

## Guardrails

- configure finite `MaxRowsPerBundle`, `MaxBytesPerBundle`, `MaxBytesPerPullResponse`, and
  `MaxConcurrentHistoryProjections`; zero selects bounded defaults and hard maxima fail startup
- keep bundles reasonably bounded; avoid very large dirty sets in one push
- monitor fixed-cardinality projection row/byte, failure-code, active-permit, and high-water metrics;
  never label them with users, sources, keys, or bundle sequences
- projection retains one bounded canonical bundle plus admitted visible output and is recomputed on
  demand; there is no projection cache or memory growth with retained-history length
- keep managed table sets FK-closed
- monitor prune fallback frequency, accepted-push replay hits, and rejected out-of-bundle writes
- treat malformed server responses as fail-closed conditions, not retry-forever conditions
- treat `history_pruned` as an expected retained-history signal and size the retention policy
  around acceptable snapshot-rebuild frequency
- recreate only test-owned or explicitly disposable databases when adopting an incompatible
  snapshot protocol or local metadata layout; runtime code never deletes an application database
  automatically
