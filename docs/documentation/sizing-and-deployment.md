---
layout: doc
title: Sizing and Deployment
permalink: /documentation/sizing-and-deployment/
parent: Architecture
---

This page tells a developer, or an AI agent working for one, how to size an Oversync server for
the machines it runs on and for the PostgreSQL database behind it. Follow the steps in order. Each
step names the API to call and the check that must pass. The
[Deployment and Operations]({{ site.baseurl }}/documentation/server-deployment-and-operations/)
guide covers the rest of the host lifecycle.

Oversync is a library. It never reads environment variables and never changes process-wide
runtime settings such as `GOMEMLIMIT` or `GOMAXPROCS`. Your host program decides those values;
Oversync computes recommendations, checks them at startup, and logs what it runs with.

Memory and CPU figures on this page were measured on a laptop (Apple M2 Max) and are estimates for
cloud vCPUs. Confirm them with a load test on your target machine.

## What you need before you start

| Input | How to find it |
| --- | --- |
| CPUs per instance | The container CPU limit, or `nproc` on a VM. Go 1.25 already uses the container CPU limit for `GOMAXPROCS`. |
| Memory per instance | The container memory limit (`cat /sys/fs/cgroup/memory.max`), or the VM memory. |
| Number of instances | Every process that runs `SyncService` against the same database, including during a rolling deploy. |
| PostgreSQL `max_connections` | `SHOW max_connections;` |
| PostgreSQL `superuser_reserved_connections` | `SHOW superuser_reserved_connections;` (default 3) |
| Other database users | Connections used by migrations, administration, and other services. Assume 10 if you do not know. |

## Step 1: Describe the machine

```go
machine := oversync.Machine{CPUs: 2, MemoryBytes: 2 << 30} // 2 vCPU, 2 GiB
```

`MachineSmall` (2 vCPU / 2 GiB), `MachineMedium` (4 vCPU / 8 GiB), and `MachineLarge`
(8 vCPU / 16 GiB) are ready-made values. They go through the same rule as any other machine, so
use your real numbers when they differ. `oversync.RuntimeMachine()` reads `GOMAXPROCS` and the
current Go memory limit without changing either; its `MemoryBytes` is 0 when no Go memory limit is
set, and then you must fill it in.

## Step 2: Size the service

```go
serviceConfig := &oversync.ServiceConfig{ /* tables, generations, … */ }
sizing, err := serviceConfig.ApplySizing(machine)
if err != nil {
    return err // the machine cannot admit one maximum-size push
}
```

`ApplySizing` fills the three admission permits where they are zero and keeps any value you set.
It reads, but never changes, the bundle and chunk byte limits. The rule:

- snapshot builds = CPUs (at most 16);
- snapshot chunk requests = 2 × CPUs (2 to 32);
- history projections (pushes, pull projections, push-outcome reads) = the smaller of 2 × CPUs and
  the number of maximum-size pushes that fit in 40% of the recommended Go memory limit (1 to 32).
  One admitted push needs about six times `MaxBytesPerBundle` (about 96 MiB at the default 16 MiB).

| Machine | History projections | Snapshot builds | Chunk requests | Go memory limit | Estimated peak heap | Pool budget | Connections per instance (watch on) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 vCPU / 2 GiB | 4 | 2 | 4 | 1536 MiB | 0.66 GiB | 16 | 17 |
| 4 vCPU / 8 GiB | 8 | 4 | 8 | 6 GiB | 1.25 GiB | 26 | 27 |
| 8 vCPU / 16 GiB | 16 | 8 | 16 | 12 GiB | 2.4 GiB | 46 | 47 |
| No sizing (zero values) | 4 | 4 | 4 | — | 0.7 GiB | 18 | 19 |

You can still set any permit by hand, before or after `ApplySizing`. Lower snapshot builds first
when PostgreSQL CPU is the bottleneck during restores.

## Step 3: Size the PostgreSQL pool

```go
budget := serviceConfig.ConnectionBudget()
poolConfig.MaxConns = budget.PoolMaxConns
poolConfig.MinConns = 2
```

`ConnectionBudget` adds the connections one instance can use at the same time:

| Part | Connections | Why |
| --- | ---: | --- |
| History projections | one per permit | An admitted push, pull projection, or push-outcome read holds one connection. A push pins its connection for the whole request. |
| Snapshot builds | one per permit | One transaction per build. |
| Snapshot chunk requests | one per permit | One read transaction per chunk. |
| Background workers | 2 | Snapshot cleanup and scope-receipt cleanup, each one batch at a time. |
| Short requests | 4 | Requests that take a connection without a permit: pull planning (a pull takes its permit only after planning), push-outcome acknowledgment, source binding, snapshot deletion, watch subscription, and status. |
| **`PoolMaxConns`** | sum | The minimum `pgxpool` `MaxConns`. |
| Listener | 1 when `BundleChangeWatch.Enabled` | The watch takes one connection out of the pool for `LISTEN`, so PostgreSQL sees it in addition to `MaxConns`. |
| **`PerInstance`** | `PoolMaxConns` + listener | PostgreSQL connections per instance. Add the admission pool size when you use `NewReadinessPoolPair` (2 in the reference server). |

`NewRuntimeService` logs a warning when the pool's `MaxConns` is below `PoolMaxConns`. It does not
refuse to start: a smaller pool works, but admitted requests then wait for connections. A larger
pool is allowed when PostgreSQL has room. Pool waits show up as latency, not as errors, because
Oversync does not time out pool acquisition; requests wait until their context ends.

Use PostgreSQL directly or a pooler in **session mode only**. Two features need one server
connection for the lifetime of a client connection:

- a push holds a session-level advisory lock for its user across several transactions
  (`pg_advisory_lock` in `oversync/upload_support.go`), and
- the bundle change watch uses `LISTEN` on its own connection
  (`oversync/bundle_change_watch.go`).

Transaction pooling (for example PgBouncer `pool_mode = transaction`) can release the advisory lock
on a different connection and loses notifications. Do not use it. With a few instances, connect
directly: `pgxpool` already keeps its connections open, so a session-mode pooler adds little.

## Step 4: Check the fleet against PostgreSQL

All instances share one database, so their budgets add up:

```text
instances × PerInstance + other database users ≤ max_connections − superuser_reserved_connections
```

Count the instances that run during a rolling deploy, not only the steady state.

Example: four 2 vCPU / 2 GiB instances with the watch enabled use 4 × 17 = 68 connections. With 10
for migrations and administration that is 78, which fits the PostgreSQL default of 100 − 3 = 97.
Three 4 vCPU / 8 GiB instances need 3 × 27 + 10 = 91, which also fits but leaves little room for a
rolling deploy; raise `max_connections` or lower the snapshot builds.

If the check fails, lower the permits on every instance (snapshot builds first, then chunk
requests, then history projections), recompute the budget, and check again. Do not lower the pool
below the budget instead: that keeps the same demand and only moves the wait into the pool.

## Step 5: Set the Go memory limit

`sizing.GoMemoryLimitBytes` is 75% of the machine memory. Set it before serving traffic, either with
the environment variable `GOMEMLIMIT` (for example `GOMEMLIMIT=1536MiB` on a 2 GiB container) or in
code:

```go
if os.Getenv("GOMEMLIMIT") == "" {
    debug.SetMemoryLimit(sizing.GoMemoryLimitBytes)
}
```

Without a limit, Go collects garbage only when the heap doubles, so a container limit can kill the
process before the collector works harder. `NewRuntimeService` warns once when no Go memory limit
is set, and warns when the estimated peak heap of the configured permits exceeds 80% of the limit.
Keep `GOGC` at its default.

## Step 6: Keep shared settings identical

Clients read the wire limits from one instance and may send the next request to another, and
several settings act on shared database state. Use the same values on every instance for:

| Setting | Why it must match |
| --- | --- |
| `MaxRowsPerBundle`, `MaxBytesPerBundle`, `MaxBytesPerPullResponse` | Advertised to clients. |
| `MaxRowsPerSnapshotChunk`, `MaxBytesPerSnapshotChunk` | Advertised to clients. |
| `MaxRowsPerSnapshotSession`, `MaxBytesPerSnapshotSession` | Advertised; an instance with a lower cap refuses sessions another accepts. |
| `MaxPushOutcomeDecodedBytes` | Stored once in the database; leave it zero so every instance adopts the stored value. |
| `SnapshotSessionTTL` | Written into each session's expiry; any instance may clean it up. |
| `RetainedBundlesPerUser` | Pruning runs inside every push. |
| `InitializationLeaseTTL`, `MaxClientSourcesPerUser` | Enforced against shared rows. |
| `BundleChangeWatch.Enabled`, `BundleChangeWatch.NotifyChannel` | Advertised; every listener must use the same channel. |
| Registered tables, schema generations, catalog revision, reserved source IDs | See the deployment checklist. |

Permits (`MaxConcurrent…`), the pool size, and the Go memory limit are per instance and may differ
between machine sizes.

`SyncService.EffectiveSettings().SharedSettingsFingerprint` is a SHA-256 over the advertised limits,
TTLs, retention, source limit, and watch channel. Bootstrap logs it in the "Oversync effective
settings" line. Instances that should match must log the same fingerprint.

## Step 7: Start and verify

1. Start one instance and wait for `Bootstrap` to finish. Check `GET /syncx/health` returns 200.
2. Read the "Oversync effective settings" log line: permits, `pool_max_conns`,
   `connection_budget_pool`, `connection_budget_per_instance`, `go_memory_limit_bytes`,
   `estimated_peak_bytes`, the advertised limits, and `shared_settings_fingerprint`.
3. Confirm there is no warning about the pool, the Go memory limit, or the estimated peak.
4. Start the other instances and compare their fingerprints.
5. Under load, count connections per instance with
   `SELECT count(*) FROM pg_stat_activity WHERE application_name = '<AppName>';` and compare with
   `PerInstance`.

## Step 8: Adjust from measurements

Change one setting at a time, on every instance of the same size, and record the result.

| Signal | Action |
| --- | --- |
| Sustained 503 `history_projection_capacity_exceeded`, memory below 50% of the limit, CPU below 70% | Raise `MaxConcurrentHistoryProjections` by 2 and recompute the budget. |
| Process memory above 80% of the limit | Lower `MaxConcurrentHistoryProjections`, or lower `MaxBytesPerBundle` (see the outcome ceiling rule below). |
| Sustained 429 `snapshot_build_capacity`, PostgreSQL CPU below 60% | Raise `MaxConcurrentSnapshotBuilds` by 1. |
| PostgreSQL CPU above 80% during restores | Lower `MaxConcurrentSnapshotBuilds`. |
| Request latency grows without CPU load | The pool is waiting: raise `MaxConns` within the Step 4 check. |
| `sync.snapshot_session_rows` keeps growing | Check cleanup logs; shorten `SnapshotSessionTTL`. |

## Burst behaviour

Oversync does not queue excess work. Over capacity it refuses immediately and the clients retry:

- pushes, pulls, and push-outcome reads: HTTP 503 `history_projection_capacity_exceeded` with
  `Retry-After: 1`;
- snapshot session creation: HTTP 429 `snapshot_build_capacity` with `Retry-After: 1`;
- snapshot chunk reads: HTTP 429 `snapshot_chunk_capacity` with `Retry-After: 1`.

The Go client retries these within an elapsed-time budget (30 seconds by default); give other
clients an equivalent retry budget. Nothing partial is applied when a request is refused.

Fleet-wide restore bursts need attention. A new schema generation rolled out to all devices, or a
database restored from a backup, makes many clients build snapshot sessions at once. Each session
stages the user's whole account in `sync.snapshot_session_rows` (about 1.5 KB per row including
indexes, measured). The per-instance build permits bound concurrent builds, but nothing bounds the
total staged rows across the fleet. Clients delete their session when they finish; cleanup then
removes its rows promptly in batches of 5,000 rows, one transaction each, and keeps going until the
expired work is gone. A session a client abandons stays until `SnapshotSessionTTL` (15 minutes by
default) expires. Budget disk and WAL for the peak: concurrent restoring users × rows per account ×
1.5 KB, and watch `pg_total_relation_size('sync.snapshot_session_rows')`.

## Every setting and what it bounds

Zero selects the default for every field.

| Field | Default | Bounds | Scope |
| --- | --- | --- | --- |
| `MaxConcurrentHistoryProjections` | 4 (1–32) | Pushes, pull projections and push-outcome reads in flight; also the push-body decode budget. Memory and one connection each. | Per instance |
| `MaxConcurrentSnapshotBuilds` | 4 | Snapshot session builds in flight; CPU on both sides and one connection each. | Per instance |
| `MaxConcurrentSnapshotChunkRequests` | 4 | Snapshot chunk reads in flight; one connection and up to one chunk of memory each. | Per instance |
| `MaxRowsPerBundle` | 10,000 (max 100,000) | Rows per push. | Shared, advertised |
| `MaxBytesPerBundle` | 16 MiB (max 64 MiB) | Decoded push body; push memory is about six times this. | Shared, advertised |
| `MaxPushOutcomeDecodedBytes` | stored value | One retained push outcome. A new store records `MaxBytesPerBundle` + 8 KiB; zero adopts the stored value afterwards. | Shared, stored |
| `MaxBytesPerPullResponse` | 32 MiB (max 128 MiB) | One pull page and its memory. | Shared, advertised |
| `MaxRowsPerSnapshotChunk` | 5,000 | Rows per chunk response. | Shared, advertised |
| `MaxBytesPerSnapshotChunk` | 16 MiB | Bytes per chunk response and its memory. | Shared, advertised |
| `MaxRowsPerSnapshotSession`, `MaxBytesPerSnapshotSession` | unbounded | Staged rows and bytes of one session. An account above the cap can never rebuild, so set it only as a product decision. | Shared, advertised |
| `SnapshotSessionTTL` | 15 minutes | How long an unfinished session and its staged rows live. | Shared |
| `RetainedBundlesPerUser` | 10,000 | History kept per user; a client further behind rebuilds from a snapshot. | Shared |
| `InitializationLeaseTTL` | 15 minutes | Scope initialization lease. | Shared |
| `MaxClientSourcesPerUser` | 64 (1–4096) | Devices per account. | Shared |
| `UploadLockTimeout` | none | PostgreSQL `lock_timeout` inside push transactions; zero leaves lock waits to the request context. | Per instance |
| `BundleChangeWatch` | off | Metadata-only change notifications; one extra connection per instance. | Shared channel |
| `StageMetrics`, `LogStageTimings`, `GenerationObservation` | off | Diagnostics. | Per instance |

Batch sizes for snapshot materialization, cleanup, and history pruning are internal; they do not
change with machine size.

### The push outcome ceiling

The first `Bootstrap` against a new database stores the push outcome ceiling and it never changes.
With `MaxPushOutcomeDecodedBytes` left at zero, later starts adopt the stored value, so you can lower
`MaxBytesPerBundle` freely. Raising `MaxBytesPerBundle` above the stored ceiling minus 8 KiB stops
`Bootstrap` with an error that names both values: keep `MaxBytesPerBundle` at or below that value,
or recreate the sync store. There is no in-place change. Because the advertised ceiling comes from
the database, `GET /sync/generations/{generation_id}` answers 503 `service_not_ready` until
`Bootstrap` has succeeded.

## Reference integrations

The repository's example servers apply these steps from environment variables, as a host program
would:

| Variable | Effect |
| --- | --- |
| `OVERSYNC_MACHINE` | `small`, `medium`, `large`, or `auto` (`RuntimeMachine`); applies `ApplySizing`. |
| `OVERSYNC_CPUS`, `OVERSYNC_MEMORY` | Override the machine's CPUs and memory (for example `2GiB`); both together describe a custom machine. |
| `OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS`, `OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS`, `OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS` | Explicit permits; they win over the machine sizing. |
| `OVERSYNC_DB_POOL_MAX_CONNS`, `OVERSYNC_DB_POOL_MIN_CONNS` | Pool size. Default: `pool_max_conns` from `DATABASE_URL`, else the connection budget. |
| `OVERSYNC_LOG_LEVEL` | `debug`, `info` (default), `warn`, or `error`. |
| `OVERSYNC_LOG_STAGE_TIMINGS` | `1`, `true`, or `yes`. |
| `GOMEMLIMIT` | Left alone when set; otherwise the servers set the recommended limit when a machine is configured. |
| `PORT` | Listen port (default 8080). |

See `internal/serverenv` and the `SetupServer` functions of
[`integration/nethttp_server`](https://github.com/mobiletoly/go-oversync/tree/main/integration/nethttp_server)
and
[`examples/samplesync_server`](https://github.com/mobiletoly/go-oversync/tree/main/examples/samplesync_server).
