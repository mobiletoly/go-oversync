<!-- moved to specs -->
# Production Readiness Checklist (Server + Client)

This document tracks the remaining work to reach full production readiness with high performance and 100% durability guarantees for the go-oversync stack.

## Durability & Correctness

- Statement and transaction timeouts (PostgreSQL)
  - Set `statement_timeout`, `idle_in_transaction_session_timeout`; keep `lock_timeout` local as today
  - Validate long-running upload batches do not trip timeouts under expected SLOs
- Disaster recovery
  - Backups and PITR testing; recovery drills; check sidecar integrity verification after restore

## Performance & Scalability

- Index coverage & maintenance
  - Monitor bloat; autovacuum thresholds; index-only scans for download hot path
- Paging optimization
  - Benchmark alternative: streaming rows to app vs `json_agg` in DB for very large pages
- Idempotency gate cost
  - Measure cost of early insert to change_log; compare to prior EXISTS; confirm SAVEPOINT rollback cost is minimal under failure paths
- Pool sizing & connection settings
  - Tune pgxpool: `MaxConns`, `MinConns`, lifetimes; right-size for expected parallel uploads and downloads
- Partitioning & retention (optional)
  - Partition `server_change_log` by time or user for very large datasets; define retention policies and compaction if needed

## Observability & Operations

- Metrics: counters, histograms
  - applied / conflict / invalid / materialize_error counts
  - upload and download latency p50/p90/p99; page sizes; has_more ratio
  - FK precheck miss rates; idempotency duplicate rates
- Structured logging
  - Trace `server_id` windows, `next_after`, `until`, `limit`, `has_more` per page
- Health / readiness endpoints
  - DB connectivity check; minimal query; good to expose `/health` and `/ready`
- Runbooks
  - FK migration guidance (deferrable migration)
  - Sidecar schema evolution procedures
  - Incident playbooks for materializer backlog and conflict spikes

## Security & Compliance

- Authentication
  - JWT key rotation; audience/issuer validation; clock skew tolerances
- Authorization
  - Optional ACLs for allowed schemas/tables per user or device
- Data protection
  - TLS everywhere; secrets management for DB/JWT keys
- Audit
  - Who/what/when on materializer retries and admin overrides

## API Contract & Versioning

- API versioning strategy
  - Backward-compatible changes; deprecation policy
- Swagger/OpenAPI kept in sync with code (handlers + models)

## Client SDK (oversqlite) Hardening

- Automatic sync coordination
  - Ensure background sync auto-pauses during hydrate/import and other bulk local changes
  - Guard `apply_mode` to never leak from server-apply paths; self-heal to `apply_mode=0` on startup
- Single-writer discipline
  - WAL + busy_timeout; serialize writes; optionally expose a write mutex
- Stress tests
  - Fuzz payloads; long-running sync with intermittent network; crash/restart during apply

## Testing & Validation

- Concurrency and chaos
  - High parallel upload/download tests; simulate network errors/timeouts; forced conflicts; server restarts during batch
- Large-scale
  - Multi-million change_log rows; long page walks; memory profiling; CPU hotspots
- FK heavy schemas
  - Dense FK graphs, composite FKs; ensure prechecks + deferrable enforcement behave under heavy write load
- Recovery validation
  - Restore from backup; verify sidecar integrity; replay tests

## Launch Criteria

- All “Must” items above implemented and verified
- Load test SLOs met (p95 latency targets for upload/download; page throughput)
- Observability in place (metrics + logs + alerts)
- Runbooks finalized; on-call ready
