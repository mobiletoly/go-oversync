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
- `GET /sync/pull` pagination by bundle count
- chunked snapshot-session rebuild size and retained-floor policy

The server storage layout keeps hot row-bearing tables compact: registered tables are represented
by `table_id`, row keys by `key_bytes`, users by `user_pk`, and operations by `op_code`. Tune the
runtime around those access paths rather than repeated wire-facing schema/table/key JSON strings.

## Client hotspots

- trigger writes into `_sync_dirty_rows`
- durable replay of accepted push bundles
- durable replay of pulled bundles
- snapshot rebuild time for hydrate/recover

## Guardrails

- keep bundles reasonably bounded; avoid very large dirty sets in one push
- keep managed table sets FK-closed
- monitor prune fallback frequency, accepted-push replay hits, and rejected out-of-bundle writes
- treat malformed server responses as fail-closed conditions, not retry-forever conditions
- treat `history_pruned` as an expected retained-history signal and size the retention policy
  around acceptable snapshot-rebuild frequency
