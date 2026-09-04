---
layout: doc
title: Server Deployment and Operations
permalink: /documentation/server-deployment-and-operations/
parent: Architecture
---

Use this guide after you have a working server from [Getting Started]({{ site.baseurl }}/getting-started.html).
It covers the production boundary around `oversync.SyncService`: database connections, snapshot
capacity, HTTP lifecycle, monitoring, upgrades, and recovery. The
repository-owned
[`nethttp_server`](https://github.com/mobiletoly/go-oversync/tree/main/integration/nethttp_server)
integration harness provides runnable operational reference code. It is not application starter
code. Treat its authentication, CORS policy, secrets, and resource limits as test-environment
choices that your application must replace or review.

## Production Ownership

Your application owns:

- TLS termination, reverse-proxy policy, request size limits, and network access
- authentication, secret storage, and the trusted mapping from a credential to `user_id`
- PostgreSQL sizing, connection budgets, backups, deployment order, and alerts
- HTTP access logs and metrics for status codes and request latency

Oversync owns:

- validation and bootstrap of the reserved `sync` schema
- sync transactions, frozen snapshot sessions, and snapshot admission control
- push, pull, snapshot, retention, and client recovery protocol behavior
- rejection of registered-table writes that bypass an Oversync bundle context

Give every server instance that shares a database the same registered-table catalog, schema
version, reserved server source IDs, and layout-compatible configuration. Keep per-instance
capacity settings aligned unless your load balancer and monitoring account for different limits.
Do not place application objects in the `sync` schema or change Oversync-managed objects.

## Build The Host Server

The following skeleton shows the lifecycle that a production host must provide. Replace
`authenticate`, `trustedUserIDFromContext`, and `recordStageTiming` with your application code.
Replace the numeric pool and capacity values with results from your load tests. Add each route your
clients use; the example registers the complete current sync surface.

```go
package main

import (
    "context"
    "errors"
    "log"
    "log/slog"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/mobiletoly/go-oversync/oversync"
)

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}

func run() error {
    logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

    poolConfig, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
    if err != nil {
        return err
    }
    poolConfig.MaxConns = 40 // Reserve room under the PostgreSQL connection limit.
    poolConfig.MinConns = 4
    poolConfig.MaxConnLifetime = time.Hour
    poolConfig.MaxConnIdleTime = 30 * time.Minute

    appCtx, stopApp := signal.NotifyContext(
        context.Background(), syscall.SIGINT, syscall.SIGTERM,
    )
    defer stopApp()

    pool, err := pgxpool.NewWithConfig(appCtx, poolConfig)
    if err != nil {
        return err
    }
    defer pool.Close()

    serviceConfig := &oversync.ServiceConfig{
        SchemaGenerations: schemaGenerations, // one or two complete declarations
        AppName:          "my-sync-api",
        RegisteredTables: []oversync.RegisteredTable{
            {Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
            {Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
        },
        MaxRowsPerBundle:                    10_000,
        MaxBytesPerBundle:                   16 << 20,
        MaxBytesPerPullResponse:             32 << 20,
        MaxConcurrentHistoryProjections:     4,
        MaxConcurrentSnapshotBuilds:         8,
        MaxConcurrentSnapshotChunkRequests: 4,
        StageMetrics: oversync.StageMetricsRecorderFunc(
            func(ctx context.Context, timing oversync.StageTiming) {
                recordStageTiming(timing) // Keep this callback fast and concurrency-safe.
            },
        ),
        BundleChangeWatch: oversync.BundleChangeWatchConfig{Enabled: true},
    }

    syncService, err := oversync.NewRuntimeService(pool, serviceConfig, logger)
    if err != nil {
        return err
    }
    if err := syncService.Bootstrap(appCtx); err != nil {
        return err
    }

    go func() {
        err := syncService.RunBundleChangeListener(appCtx)
        if err != nil && !errors.Is(err, context.Canceled) {
            logger.Error("bundle change listener stopped", "error", err)
        }
    }()

    handlers := oversync.NewHTTPSyncHandlers(syncService, logger)
    actorMiddleware := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
        UserIDFromContext: trustedUserIDFromContext,
    })
    withSyncAdmission := func(next http.Handler) http.Handler {
        return authenticate(actorMiddleware(next))
    }
    withOperationalReadiness := func(next http.Handler) http.Handler {
        return syncService.ReadinessGuard(next)
    }

    mux := http.NewServeMux()
    syncHandler := registerSyncRoutes(mux, handlers, withSyncAdmission, withOperationalReadiness)
    mux.HandleFunc("GET /syncx/health", handlers.HandleHealth)
    mux.HandleFunc("GET /syncx/status", handlers.HandleStatus)

    server := &http.Server{
        Addr:         ":8080",
        Handler:      syncHandler,
        ReadTimeout:  120 * time.Second,
        WriteTimeout: 120 * time.Second,
        IdleTimeout:  60 * time.Second,
    }

    serveErr := make(chan error, 1)
    go func() { serveErr <- server.ListenAndServe() }()

    var serveFailure error
    select {
    case <-appCtx.Done():
    case err := <-serveErr:
        if err != nil && !errors.Is(err, http.ErrServerClosed) {
            serveFailure = err
        }
    }

    httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(
        context.Background(), 30*time.Second,
    )
    defer cancelHTTPShutdown()
    httpShutdownDone := make(chan error, 1)
    go func() { httpShutdownDone <- server.Shutdown(httpShutdownCtx) }()

    serviceCloseCtx, cancelServiceClose := context.WithTimeout(
        context.Background(), 30*time.Second,
    )
    defer cancelServiceClose()
    serviceCloseErr := syncService.Close(serviceCloseCtx)

    httpShutdownErr := <-httpShutdownDone
    var forceCloseErr error
    if httpShutdownErr != nil {
        forceCloseErr = server.Close()
    }
    return errors.Join(serveFailure, serviceCloseErr, httpShutdownErr, forceCloseErr)
}

func registerSyncRoutes(
    mux *http.ServeMux,
    handlers *oversync.HTTPSyncHandlers,
    withSyncAdmission func(http.Handler) http.Handler,
    withOperationalReadiness func(http.Handler) http.Handler,
) http.Handler {
    handle := func(pattern string, fn http.HandlerFunc) { mux.HandleFunc(pattern, fn) }

    handle("POST /sync/connect", handlers.HandleConnect)
    handle("POST /sync/push", handlers.HandlePush)
    handle("GET /sync/pull", handlers.HandlePull)
    handle("GET /sync/watch", handlers.HandleWatch)
    handle("POST /sync/snapshot-sessions", handlers.HandleCreateSnapshotSession)
    handle("GET /sync/snapshot-sessions/{snapshot_id}", handlers.HandleGetSnapshotChunk)
    handle("DELETE /sync/snapshot-sessions/{snapshot_id}", handlers.HandleDeleteSnapshotSession)
    discovery := http.HandlerFunc(handlers.HandleGeneration)
    mux.Handle("GET /sync/generations/{generation_id}", discovery)
    mux.Handle("GET /sync/generations/", discovery)
    isGenerationDiscovery := func(r *http.Request) bool {
        return r.Method == http.MethodGet &&
            (r.URL.Path == "/sync/generations" || strings.HasPrefix(r.URL.Path, "/sync/generations/"))
    }
    admissionTarget := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if isGenerationDiscovery(r) {
            discovery.ServeHTTP(w, r)
            return
        }
        mux.ServeHTTP(w, r)
    })
    admittedSync := withSyncAdmission(admissionTarget)
    operational := withOperationalReadiness(admittedSync)
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if isGenerationDiscovery(r) {
            admittedSync.ServeHTTP(w, r)
            return
        }
        if strings.HasPrefix(r.URL.Path, "/sync/") {
            operational.ServeHTTP(w, r)
            return
        }
        mux.ServeHTTP(w, r)
    })
}
```

Generation discovery bypasses only application operational readiness so a compatible client can
discover its selected generation during rollout. It remains behind authentication, actor/source
validation, deployment client policy, shutdown admission, cancellation, and operation tracking.
The outer dispatcher reaches the authenticated handler before `http.ServeMux` can clean or redirect
the request. Leading-zero, overflow, encoded, missing, repeated-slash, dot-segment, and suffixed paths
therefore receive the canonical HTTP 400 envelope. Every other `/sync/` request, including non-GET
discovery requests, uses operational readiness admission.

History projection admission is non-blocking: saturation returns retryable HTTP 503
`history_projection_capacity_exceeded`. Row-bearing push, pull, snapshot, and conflict bodies use
the decoded ceilings advertised by generation discovery, and clients enforce those ceilings after
gzip decompression. Monitor projection active/high-water counts and stable failure codes without
user/source/key labels. Projection results are not cached, so pruning canonical history cannot be
bypassed by process memory.

`authenticate` must verify the caller before `ActorMiddleware` runs.
`trustedUserIDFromContext` must return the exact authenticated `user_id` from the request context.
`ActorMiddleware` reads `Oversync-Source-ID`, validates it, and creates
`oversync.Actor{UserID, SourceID}`. If you reserve source IDs for trusted server writers, pass the
same exact set to `ServiceConfig.ReservedServerSourceIDs` and
`ActorMiddlewareConfig.ReservedServerSourceIDs`.

Call `Bootstrap()` before you advertise readiness or serve sync traffic. During shutdown, remove
the instance from client traffic and start `http.Server.Shutdown` so the server stops accepting
connections. Call `SyncService.Close` while HTTP shutdown waits. Service closure rejects new sync
operations, drains accepted work, and ends long-lived `/sync/watch` subscriptions. Wait for both
steps before you close the pool. Give each step a deadline. A process exit after either deadline
can interrupt requests, so clients must retain their normal retry and replay behavior.

## Set Database And HTTP Limits

Set a connection budget for the whole application. Oversync uses the supplied `pgxpool.Pool` for
request transactions, bootstrap, cleanup, and the optional bundle-change listener. Your own SQL
also consumes the same pool if you share it.

Choose `MaxConns` below the PostgreSQL connection limit after subtracting connections used by
administration, migrations, and other services. Watch pool acquisition duration and the counts
returned by `pgxpool.Stat()`. A larger pool cannot repair slow queries or an undersized database.

The reference `nethttp_server` uses these development defaults:

| Owner | Setting | Reference value |
| --- | --- | ---: |
| `nethttp_server` | PostgreSQL maximum connections | 50 |
| `nethttp_server` | PostgreSQL minimum connections | 5 |
| `nethttp_server` | Read timeout | 120 seconds |
| `nethttp_server` | Write timeout | 120 seconds |
| `nethttp_server` | Idle timeout | 60 seconds |
| `nethttp_server` | Graceful HTTP shutdown deadline | 30 seconds |

The integration server accepts `OVERSYNC_DB_POOL_MAX_CONNS` and `OVERSYNC_DB_POOL_MIN_CONNS`. It also sets a
one-hour maximum connection lifetime and a 30-minute maximum idle time. Check these values against
your request sizes, proxy timeouts, database limits, and shutdown budget before you copy them.

The integration server also accepts `OVERSYNC_MAX_CONCURRENT_HISTORY_PROJECTIONS`. When unset, the service's
bounded default of four applies. Projection permits can remain held until the HTTP response is
written, so raise this only from measurements and size it with the PostgreSQL pool, request
duration, and server memory. Admission overload returns retryable HTTP `503` with `Retry-After: 1`.
The Go client's `PushCapacityRetryPolicy` handles this signal independently of the generic
three-attempt transport retry budget; a nil policy uses a 30-second elapsed-time budget.

Place TLS and public request controls in your application or trusted proxy. Keep
`/syncx/health` and `/syncx/status` on an internal network or protect them with an access policy.
The integration server exposes them without authentication for local development.

## Control Snapshot Capacity

Snapshot rebuilds have two independent admission limits:

- `MaxConcurrentSnapshotBuilds` bounds frozen snapshot materialization.
- `MaxConcurrentSnapshotChunkRequests` bounds reads from materialized snapshot sessions.

Zero selects the Oversync defaults: eight active builds and four active chunk requests per server
process. The integration server accepts positive overrides through
`OVERSYNC_MAX_CONCURRENT_SNAPSHOT_BUILDS` and
`OVERSYNC_MAX_CONCURRENT_SNAPSHOT_CHUNK_REQUESTS`.

Oversync does not queue excess snapshot work in the server. It returns HTTP `429` with
`Retry-After: 1` and one of these error codes:

- `snapshot_build_capacity` for snapshot-session creation
- `snapshot_chunk_capacity` for chunk fetch

The service rejects the request before it opens the snapshot transaction. A client that receives
one of these responses has no partial result to apply. Frozen snapshot sessions keep their
transaction-consistent contents, and the Go client changes its durable checkpoint after it commits
the local snapshot apply.

The Go client handles these two error codes through `SnapshotCapacityRetryPolicy`, separate from
the generic transport `RetryPolicy`. A nil policy enables an elapsed-time budget of 30 seconds,
uses the server's `Retry-After` header, falls back to one second when the header is absent, and adds
jitter. If capacity remains unavailable, the client returns
`SnapshotCapacityRetryExhaustedError`. The operation has failed at that point. Report the failure
to your caller or schedule a later sync according to your product policy.

Capacity rejection protects PostgreSQL and server memory. It does not corrupt the PostgreSQL
store or return an inconsistent snapshot. Repeated retry exhaustion means the offered load stayed
above the admitted capacity for longer than the client budget.

Choose limits from measurements:

1. Measure build duration, chunk duration, pool acquisition time, database CPU, and server memory.
2. Start with a small active-client limit and raise it in steps.
3. Count capacity responses, retries, exhausted operations, and request failures at each step.
4. Stop raising concurrency when latency, pool waits, memory, or failure rates leave your service
   budget.

Use the [Performance]({{ site.baseurl }}/documentation/performance/) guide to size snapshot row,
byte, session, materialization, and cleanup bounds from your application data.

Reducing client concurrency is the first control when a test creates a burst that production
traffic would spread across time. Raise server admission or pool limits when measurements show
headroom. Keep build and chunk limits separate because their database and memory costs differ.

Authenticated clients can read effective transfer and concurrency limits from
`GET /sync/generations/{generation_id}` during client admission. The request must carry the client's fixed generation proof, and the
response confirms only that requested generation. It does not list other generation IDs, the
registry fingerprint, catalog IDs, or omitted tables. Treat that response as advisory for the
instance that served it; every later request validates both proof headers again.

## Reproduce A 500-User Run

The mobile-flow simulator separates total work from active work:

- `--parallel=500` creates 500 user simulations for the run.
- `--concurrency=30` permits at most 30 whole-user lifecycles at once.

Start the integration test server, then run:

```bash
cd integration/mobile_flow
GOTOOLCHAIN=go1.25.12 go run . \
  --scenario=complex-multi-batch \
  --parallel=500 \
  --concurrency=30
```

This command is a load-test recipe. It does not claim that a machine, database, or deployment can
serve 500 concurrent users. The run contains 500 total users and no more than 30 active simulator
lifecycles.

Repeat the run with clean, isolated test databases. Record the server configuration, PostgreSQL
configuration, machine resources, dataset shape, wall-clock duration, 429 counts, client capacity
retries, failures, and peak memory. Increase `--concurrency` in controlled steps such as 10, 20,
and 30. Use values that expose the knee in your own deployment rather than treating those steps as
presets.

A small number of 429 responses followed by successful retries shows that admission control
absorbed a short burst. Errors containing
`oversqlite snapshot capacity retry exhausted for snapshot_chunk_fetch` or
`oversqlite snapshot capacity retry exhausted for snapshot_session_create` show that capacity
stayed unavailable past the retry budget. Reduce active simulator concurrency first. If production
traffic has the same arrival pattern, measure database and server headroom before changing the
server limits.

## Monitor The Running Service

Use these surfaces for distinct jobs:

- `GET /syncx/health` returns readiness derived from service status. It returns `503` when the
  service reports `unhealthy`.
- `GET /syncx/status` reports lifecycle, in-flight work, retention windows, history-pruned count,
  accepted-push replay count, rejected registered writes, and committed-bundle totals.
- `GET /sync/generations/{generation_id}` supplies the immutable descriptor and effective limits
  during client admission.

Add HTTP instrumentation around the handlers. Count `snapshot_build_capacity` and
`snapshot_chunk_capacity` responses, group latency by route and status, and alert on sustained 5xx
responses. On Go clients, `SnapshotTransferDiagnostics()` reports capacity responses, capacity
retries, capacity wait time, sessions, chunks, and transfer high-water values. Count returned
`SnapshotCapacityRetryExhaustedError` values at the application boundary.

Set `ServiceConfig.StageMetrics` when you need stage timing for sync hot paths. Oversync calls the
recorder in the request path, so make the implementation concurrency-safe and keep it fast. Send
records to a buffered metrics path or update bounded in-memory instruments. Do not perform network
I/O in the callback.

`LogStageTimings` writes stage timing at DEBUG level. Use it for a bounded investigation and keep
it disabled during normal production service. The snapshot and scope-receipt cleanup workers log
batch failures and completion details. Alert on cleanup failures and check whether expired session
rows continue to grow.

Monitor PostgreSQL pool acquisition, acquired and idle connections, database CPU, lock waits, and
storage growth for `sync.bundle_log`, `sync.bundle_rows`, and snapshot-session tables. Track
`history_pruned` frequency because it causes snapshot recovery. A rising frequency can indicate a
retention window that is too short for your offline clients.

## Deploy, Upgrade, And Recover

Back up PostgreSQL before first adoption of populated tables and before an upgrade that can change
the managed layout. Test the restore procedure against a separate database.

For first adoption or a layout-affecting upgrade:

1. Stop every old Oversync instance and every writer to registered tables.
2. Back up PostgreSQL.
3. Start one compatible server and let `Bootstrap()` finish.
4. Check `/syncx/health` and inspect `/syncx/status`.
5. Start the remaining compatible instances, then restore client traffic.

Do not run mixed versions during adoption or a managed-layout change. Oversync validates the
managed layout and fails closed on definition drift, but an older binary can still violate a newer
contract before the new process starts.

Direct edits to `sync.*`, privileged trigger bypass, and changes to managed triggers fall outside
the runtime contract. `Bootstrap()` does not audit or repair business and managed row values after
such an intervention. Stop all Oversync processes and restore a known coherent backup or follow an
operator procedure reviewed for that recovery. Do not repair, truncate, or recreate part of the
reserved `sync` schema while any server or client uses the database.

## Pre-Production Checklist

- [ ] PostgreSQL runs a supported 16.x or 17.x release and has a tested backup and restore path.
- [ ] Every registered table passes bootstrap validation on a staging copy of the production
  schema.
- [ ] All instances use the same registered tables, one-or-two-generation registry, catalog
  revision, source-admission limit, and reserved source IDs.
- [ ] Every client has a fixed accepted generation proof and sends both required proof headers.
- [ ] Authentication supplies an exact trusted `user_id`; clients supply a valid
  `Oversync-Source-ID`.
- [ ] TLS, proxy limits, health-endpoint access, and secrets match your infrastructure policy.
- [ ] PostgreSQL pool, HTTP timeouts, snapshot limits, and client retry budgets come from measured
  load tests.
- [ ] Dashboards cover readiness, 429 and 5xx responses, client retry exhaustion, pool pressure,
  cleanup failures, and history-pruned recovery.
- [ ] Shutdown drains HTTP requests and `SyncService` work before the process closes the pool.
- [ ] Upgrade runbooks stop incompatible versions before bootstrap or adoption.

## Related Guides

- [Getting Started]({{ site.baseurl }}/getting-started.html)
- [Server]({{ site.baseurl }}/documentation/server/)
- [Performance]({{ site.baseurl }}/documentation/performance/)
- [Advanced Concepts]({{ site.baseurl }}/documentation/advanced-concepts/)
- [HTTP API]({{ site.baseurl }}/documentation/api/)
