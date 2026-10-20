# go-oversync

Sync local SQLite clients with a PostgreSQL-backed Go server.

go-oversync is a Go library suite for client-server database sync. It gives you the server
runtime, HTTP handlers, and SQLite client SDK pieces needed to replicate application data between
local SQLite databases and authoritative PostgreSQL business tables.

Use it when your app needs local database writes, reconnect-safe uploads, incremental downloads,
conflict handling, and snapshot recovery without hand-rolling the replication protocol.

## Why It Is Useful

go-oversync handles the sync machinery that is easy to underestimate:

- trigger-based dirty-row capture on SQLite clients
- one bounded whole-bundle push request with durable, exact replay
- authoritative PostgreSQL commits and replay back to clients
- retry-safe server-originated writes recorded in normal sync history
- complete committed-bundle pulls with stable checkpoints
- conflict reporting and client-side resolution hooks
- frozen snapshot rebuilds for fresh installs and retained-history recovery
- fail-closed schema validation so unsupported table shapes do not sync silently

Your application still owns its domain schema, auth, business logic, and HTTP server. go-oversync
owns the replication runtime around those pieces.

## What You Can Build

- Mobile apps that keep a local SQLite database and sync with server-side PostgreSQL.
- Desktop apps or local caches that replicate to a central Go backend.
- Multi-device account sync across phones, tablets, and laptops.
- Field tools that capture durable local writes and reconcile them when connectivity returns.

## Typical Architecture

```text
SQLite client database
  -> oversqlite client runtime (or your own over-HTTP implementation)
  -> your authenticated HTTP API
  -> oversync HTTP handlers
  -> PostgreSQL business tables plus sync metadata
```

PostgreSQL business tables are authoritative. Clients push local dirty rows, replay the committed
authoritative result, and pull complete committed bundles from the server. Fresh installs and
history-pruned clients rebuild from frozen server snapshots.

The server stores one immutable canonical history of normalized PostgreSQL effects. Each client is
pinned to one immutable schema generation, and push replay, pull, conflicts, and snapshots expose a
deterministic projection containing only that generation's tables and columns. Projection is
bounded and computed on demand; there is no durable per-generation history copy or projection
cache. A canonical bundle that has no visible effects is still returned as an empty envelope so a
client can durably advance its checkpoint.

A push succeeds only when every uploaded key has a final captured effect visible to the pushing
generation. Trigger normalization and additional captured effects are allowed, but suppressed or
uncovered uploaded keys reject the whole transaction with HTTP 422
`push_effect_coverage_incomplete`. The fixed-generation Go client verifies projected counts,
bytes, hashes, and Q39, preserves newer local edits while applying authoritative effects, and can
use a guarded same-generation snapshot after a confirmed result has been pruned. Generation
switching and local schema migration remain later work.

## Client Options

- Go clients can use `oversqlite` directly.
- Kotlin Multiplatform clients can use SQLiteNow KMP's optional OverSqlite sync component:
  <https://github.com/mobiletoly/sqlitenow-kmp>

SQLiteNow KMP is the mobile companion for Kotlin Multiplatform apps. This repository remains the
Go/PostgreSQL sync server foundation and includes a Go SQLite client SDK for Go-based clients and
simulators.

## Numeric Wire Contract

Generation-bound rows use the exact generation tokens. Integers are canonical range-checked decimal
strings; exact decimals are bounded strings with no exponent notation; `float64` values use a
shortest finite spelling, including PostgreSQL `float4` values after storage normalization; and
Boolean values are JSON `true` or `false` only.
Bytes use canonical padded standard base64, UUIDs use lowercase dashed text, and JSON values are
inner RFC 8785 JCS bytes carried in an outer JSON string. Text and inner-JSON strings preserve every
Unicode scalar except U+0000, which PostgreSQL text/JSON storage cannot represent. Outer JSON
`null` means SQL NULL; outer string `"null"` means a non-null JSON document. Invalid tokens are
rejected before request hashing, staging, or mutation.

The fixed generation proof supplies wire identity. Ordinary clients discover the server-owned
column contract and derive SQLite conversions internally; applications do not declare per-column
wire types or codecs. The separately gated upgrade machinery retains explicit historical local
codec metadata for coordinated generation changes, but ordinary open, attach, push, pull, and
rebuild do not depend on it.

## Quick Start

Install the module:

```bash
go get github.com/mobiletoly/go-oversync
```

Run the fast checks:

```bash
go test ./oversync ./oversqlite
```

Start the maintained sample server:

```bash
DATABASE_URL="postgres://postgres:postgres@localhost:5432/samplesync?sslmode=disable" \
JWT_SECRET="dev-secret" \
go run ./examples/samplesync_server
```

See [`examples/samplesync_server`](examples/samplesync_server/) for its PostgreSQL setup and the
matching `samplesync-kmp` client application.

## Server Integration

Register the PostgreSQL tables that participate in sync, bootstrap the runtime, and mount the
HTTP handlers behind your existing authentication middleware.

```go
schemaGenerations := []oversync.SchemaGeneration{{
    ID: 1,
    Tables: []oversync.GenerationTable{
        {Schema: "business", Table: "users", Columns: []oversync.GenerationColumn{
            {Name: "email", WireType: oversync.GenerationWireTypeText},
            {Name: "id", WireType: oversync.GenerationWireTypeUUID},
            {Name: "name", WireType: oversync.GenerationWireTypeText},
        }},
        {Schema: "business", Table: "posts", Columns: []oversync.GenerationColumn{
            {Name: "author_id", WireType: oversync.GenerationWireTypeUUID, Nullable: true},
            {Name: "content", WireType: oversync.GenerationWireTypeText},
            {Name: "id", WireType: oversync.GenerationWireTypeUUID},
            {Name: "title", WireType: oversync.GenerationWireTypeText},
        }, ForeignKeys: []oversync.GenerationForeignKey{{
            Columns: []string{"author_id"}, ReferencedSchema: "business", ReferencedTable: "users",
            ReferencedColumns: []string{"id"}, OnDelete: oversync.GenerationForeignKeyNoAction,
            OnUpdate: oversync.GenerationForeignKeyNoAction,
        }}},
    },
}}
cfg := &oversync.ServiceConfig{
    SchemaGenerations: schemaGenerations,
    AppName:          "my-sync-app",
    RegisteredTables: []oversync.RegisteredTable{
        {Schema: "business", Table: "users", SyncKeyColumns: []string{"id"}},
        {Schema: "business", Table: "posts", SyncKeyColumns: []string{"id"}},
    },
}

svc, err := oversync.NewRuntimeService(pool, cfg, logger)
if err != nil {
    log.Fatal(err)
}
if err := svc.Bootstrap(ctx); err != nil {
    log.Fatal(err)
}

handlers := oversync.NewHTTPSyncHandlers(svc, logger)

syncActorMiddleware := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
    UserIDFromContext: func(ctx context.Context) (string, error) {
        return yourUserIDFromContext(ctx)
    },
})

// Wrap your auth middleware around the sync actor middleware, which is required
// to identify the sync scope for each request.
withSyncActor := func(next http.Handler) http.Handler {
    return yourAuthMiddleware(syncActorMiddleware(next))
}
withSyncAdmission := func(next http.Handler) http.Handler {
    // Discovery bypasses rollout readiness, but retains authentication,
    // actor/source validation, client policy, cancellation, and tracking.
    return yourSyncClientPolicy(withSyncActor(next))
}

mux := http.NewServeMux()
// Register raw sync handlers. The outer dispatcher below owns admission.
mux.HandleFunc("POST /sync/connect", handlers.HandleConnect)
mux.HandleFunc("POST /sync/push", handlers.HandlePush)
mux.HandleFunc("GET /sync/pull", handlers.HandlePull)
mux.HandleFunc("POST /sync/snapshot-sessions", handlers.HandleCreateSnapshotSession)
mux.HandleFunc("GET /sync/snapshot-sessions/{snapshot_id}", handlers.HandleGetSnapshotChunk)
mux.HandleFunc("DELETE /sync/snapshot-sessions/{snapshot_id}", handlers.HandleDeleteSnapshotSession)
generationDiscovery := http.HandlerFunc(handlers.HandleGeneration)
mux.Handle("GET /sync/generations/{generation_id}", generationDiscovery)
mux.Handle("GET /sync/generations/", generationDiscovery)
mux.HandleFunc("GET /syncx/health", handlers.HandleHealth)
mux.HandleFunc("GET /syncx/status", handlers.HandleStatus)

isGenerationDiscovery := func(r *http.Request) bool {
    return r.Method == http.MethodGet &&
        (r.URL.Path == "/sync/generations" || strings.HasPrefix(r.URL.Path, "/sync/generations/"))
}
admissionTarget := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if isGenerationDiscovery(r) {
        generationDiscovery.ServeHTTP(w, r)
        return
    }
    mux.ServeHTTP(w, r)
})
admittedSync := withSyncAdmission(admissionTarget)
operationalSync := svc.ReadinessGuard(admittedSync)
syncHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if isGenerationDiscovery(r) {
        admittedSync.ServeHTTP(w, r)
        return
    }
    if strings.HasPrefix(r.URL.Path, "/sync/") {
        operationalSync.ServeHTTP(w, r)
        return
    }
    mux.ServeHTTP(w, r)
})
// Use syncHandler as the root http.Server Handler.
```

`yourAuthMiddleware` must authenticate the request and expose trusted `user_id` in request context.
`oversync.ActorMiddleware(...)` reads `Oversync-Source-ID` from the client request and combines it
with that trusted user identity. The runtime derives user sync scope id from `Actor.UserID`.

Clients select a generation ID and discover its server-owned descriptor through
`GET /sync/generations/{generation_id}`. This authenticated request includes `Oversync-Source-ID`
and omits both generation proof headers. After validating discovery, the SDK sends the exact
`Oversync-Schema-Generation-ID` and `Oversync-Schema-Generation-Fingerprint` on every subsequent
`/sync` request. Applications do not provision a fingerprint or local codecs.
Discovery bypasses only operational readiness. The root dispatcher runs before `http.ServeMux`, so
malformed discovery paths remain authenticated and receive the protocol HTTP 400 response without
a cleaning redirect. Non-GET discovery and all other `/sync/` requests remain behind operational
readiness.

## SQLite Client

Go clients can use `oversqlite` to install local metadata tables, capture local dirty rows, attach
an authenticated user, and run push/pull sync.

Opening and local writes work offline. `Attach` discovers and confirms the selected generation,
validates local storage, and saves the contract before connecting. Later opens reuse that contract.
The `ContractConfirmed` field returned by `SyncStatus(ctx)` distinguishes unconfirmed local state. Register every table in the
selected generation; schema names and conversions come from the server. Generation or managed-shape
changes require the explicit preserving upgrade path described in [the upgrade design](docs/client-upgrades.md).

```go
cfg := &oversqlite.Config{
    GenerationID: 1,
    DatabaseID: "app-db",
    ServiceID: "my-service",
    LocalVersion: 1,
    Tables: []oversqlite.SyncTable{
        {TableName: "users", SyncKeyColumnName: "id"},
        {TableName: "posts", SyncKeyColumnName: "id"},
    },
    UploadLimit: 1000,
    DownloadLimit: 1000,
}

opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
    DatabasePath:       "app.db",
    Plan:               freshPlan, // exact first-install or upgrade edge
    BaseURL:            "http://localhost:8080",
    Token:              tokenProvider,
    TargetClientConfig: cfg,
})
if err != nil {
    log.Fatal(err)
}
defer opening.Close()

opened, err := opening.Advance(ctx)
if err != nil {
    log.Fatal(err)
}
client := opened.Ready
if client == nil {
    log.Fatalf("database is not ready: %s", opened.Status.Outcome)
}
defer client.Close()

db := client.Database() // borrowed; stop application SQL before Close or UninstallSync

attachResult, err := client.Attach(ctx, "user-123")
if err != nil {
    log.Fatal(err)
}
if attachResult.Status == oversqlite.AttachStatusRetryLater {
    log.Printf("connect pending, retry after %s", attachResult.RetryAfter)
    return
}

syncReport, err := client.Sync(ctx)
if err != nil {
    log.Fatal(err)
}
log.Printf("sync outcomes: push=%s remote=%s", syncReport.PushOutcome, syncReport.RemoteOutcome)

detachResult, err := client.Detach(ctx)
if err != nil {
    log.Fatal(err)
}
if detachResult.Outcome == oversqlite.DetachOutcomeBlockedUnsyncedData {
    log.Printf("detach blocked by %d pending rows", detachResult.PendingRowCount)
}
```

Kotlin Multiplatform clients should use SQLiteNow KMP's OverSqlite client instead of this Go SDK.
Both clients target the same go-oversync server protocol.

## How The Sync Model Works

The current contract is bundle-based:

- PostgreSQL business tables are authoritative.
- clients push one logical dirty set at a time through `POST /sync/push`
- accepted pushes apply the authoritative response locally, atomically
- clients pull complete committed bundles only
- fresh installs and prune recovery rebuild from frozen snapshot sessions

The goal is durable visibility at clear boundaries: a push either commits as one authoritative
bundle or fails, and a pull advances a local checkpoint only after complete committed bundles are
applied.

## Current Supported Envelope

Server-side registered tables are intentionally constrained:

- one sync key column per registered table
- visible sync key type must be `uuid` or `text` and the column must declare `NOT NULL`
- registered relations must be permanent logged PostgreSQL tables (`pg_class.relpersistence = 'p'`);
  `UNLOGGED` and temporary relations are unsupported
- registered PostgreSQL roots and every current partition/inheritance descendant must include
  `_sync_scope_id TEXT NOT NULL` and a non-null visible sync key
- registered PostgreSQL row identity must be scope-bound through `(_sync_scope_id, sync_key)`
- registered tables must be FK-closed
- registered-to-registered foreign keys must be scope-inclusive and `DEFERRABLE`
- unsupported key shapes or FK shapes fail during bootstrap

The SQLite client is likewise fail-closed:

- one configured remote schema per SQLite database
- one `oversqlite.Client` process owner per SQLite database
- managed local tables must be FK-closed
- the configured visible `TEXT`/`BLOB` primary-key column must declare `NOT NULL` explicitly

See `docs/getting-started.md` and `docs/documentation/server.md` for the full table requirements.

`Opening` supports directed column-surface generation changes whose synchronized table,
key, and foreign-key sets stay unchanged. Separate devices may remain on different supported
generations with independent source identities. Deployments outside this envelope need a reviewed
operator procedure or a compatible database backup.

Nullable identity declarations use a data-preserving operator migration rather than the destructive
reset above. Stop all writers and mixed-version instances, resolve existing NULL owner/key values
according to application policy, add explicit `NOT NULL` to every reported PostgreSQL
root/descendant or SQLite visible key, then retry bootstrap/initialization. Corrected binaries do not
change the wire, checkpoint, or snapshot contract, but older binaries do not enforce this invariant.

Registered-table `TRUNCATE` is always rejected with PostgreSQL SQLSTATE `55000`, including through
`CASCADE`, direct current-partition targets, and `WithinSyncBundle`. Ordinary direct
`INSERT`/`UPDATE`/`DELETE` without an Oversync bundle context is also rejected. Oversync exposes no
runtime reset endpoint or bundle-context escape hatch.

Supported populated registered tables are adopted only while creating a fresh managed layout,
without rewriting their business rows. Bootstrap locks registered writes and commits trigger
installation plus one replayable baseline bundle per populated scope atomically. Runtime operations
remain unavailable until bootstrap succeeds. Existing populated deployments require a coordinated
stop, backup, upgraded bootstrap, and readiness check for that one-time adoption.

The PostgreSQL `sync` namespace is reserved for Oversync. After the fresh transaction has committed,
later `Bootstrap()` calls use a trusted-database attachment path: they revalidate registered-table
declarations, the marker/catalog, and the complete `server_postgres_sync_v1` semantic definition,
then publish readiness without reading business rows or managed operational state. Attachment takes
no explicit data locks and performs no DML, DDL, adoption, canonicalization, hashing, cleanup, or
repair. Missing, extra, disabled, invalid, or altered managed definitions return a typed
`UnsupportedSchemaError` with expected/actual fingerprints and bounded field differences.

This fast path trusts PostgreSQL's committed durable data. It does not audit or repair business or
managed row values after privileged trigger bypass or direct managed-state edits. Such intervention
is unsupported; restore a known coherent backup or use a separately reviewed operator procedure.

For a managed-layout rejection, stop every server instance and investigate the reported definition
delta. Oversync does not mutate the rejected database or prescribe an in-place repair. Do not run a
different binary against it while the discrepancy is unresolved. This contract changes no
HTTP/wire shape, checkpoint, snapshot, bundle, or Go/KMP/Dart client durable state.

## Packages

- `oversync/`: PostgreSQL adapter, schema validation, bundle capture, HTTP handlers
- `oversqlite/`: SQLite client SDK with trigger-based dirty capture and sync loops
- `examples/samplesync_server/`: sample server for the KMP sample app
- `integration/`: repository-owned server, simulator, schema-evolution, conformance, load, and
  recovery harnesses
- `docs/`: Jekyll site content
- `swagger/two_way_sync.yaml`: OpenAPI description of the HTTP surface

## Documentation

- docs site: <https://mobiletoly.github.io/go-oversync/>
- getting started: `docs/getting-started.md`
- server reference: `docs/documentation/server.md`
- server deployment and operations: `docs/documentation/server-deployment-and-operations.md`
- server-originated writes: `docs/documentation/server-originated-writes.md`
- client reference: `docs/documentation/client.md`
- HTTP API reference: `docs/documentation/api.md`

## Examples

- `examples/samplesync_server/`: sample server used by the Kotlin sample app

## Integration Harnesses

- `integration/nethttp_server/`: repository test server with JWT auth and fault/test helpers
- `integration/mobile_flow/`: end-to-end simulator and load/recovery scenarios
- `integration/schema_evolution/`: migration, rollout, and recovery qualification authority

## License

Apache 2.0. See `LICENSE` if present in your distribution or the source headers in this repository.

### Transaction admission for application-owned scope lifecycle

`ServiceConfig.ScopeTransactionAdmission` optionally supplies an application policy implementing
`AdmitScopeTransaction(context.Context, DatabaseWriteTx, ScopeTransactionOperation) error`.
Oversync calls it inside each actual scoped mutation transaction, before scope creation or receipt
replay, and calls it again on every transaction retry. A nil capability preserves SDK behavior.
The supplied SQL-only transaction has the same context, SQL and nested SDK operation restrictions
as retryable write callbacks. The application owns its policy tables and lock namespace.

`ScopeTransactionOperation` carries exact `ScopeID` and `SourceID` values and an SDK-derived `Kind`:

| Entry point | Kind | Source |
| --- | --- | --- |
| `Connect` | `ScopeTransactionConnect` | authenticated client source |
| `PushOneShot` and `WithinSyncBundle` | `ScopeTransactionClientPush` | validated non-reserved source |
| `ScopeManager.ExecWrite` | `ScopeTransactionServerWrite` | trusted in-process writer |
| Populated-table baseline adoption | `ScopeTransactionServerWrite` | SDK `oversync-adoption:` namespace |
| Snapshot creation and source replacement | `ScopeTransactionSnapshotCreate` | authenticated old/current source |
| Explicit snapshot retirement | `ScopeTransactionSnapshotRetire` | authenticated client source |

Client sources cannot impersonate configured reserved server writers. Authorize privileged writers
using both the kind and exact source ID. A policy can acquire a shared transaction advisory lock,
then read application lifecycle state in a **separate statement** under Read Committed; a retiring
application takes the matching exclusive lock and durably records retirement. The lock stays held
until commit or rollback. Scope reads, watches and erasure-only metadata maintenance do not invoke
this capability or hold its lifecycle lock.

Return `&oversync.ScopeAccessDeniedError{}` to reject with safe HTTP 403 `scope_access_denied`.
Other policy errors fail closed as `ScopeAdmissionUnavailableError`, with safe HTTP 503
`scope_admission_unavailable`; the cause is available to trusted Go callers through `Unwrap`.
The SDK implements no account-deletion policy and creates no lifecycle table.

`ScopeWriteOptions.RequireExistingScope` makes trusted server writes reject absent or uninitialized
scopes with `ScopeUninitializedError`, after transaction admission and without creating metadata.
It supports repeated application erasure without accidentally initializing an empty scope. Include
this option in the application's frozen `OperationHash`, just as application-selected byte limits.
