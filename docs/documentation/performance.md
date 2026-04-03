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
