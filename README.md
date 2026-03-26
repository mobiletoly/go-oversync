# go-oversync

go-oversync is a Go library suite for two-way sync between local SQLite clients and PostgreSQL
servers.

The current contract is bundle-based:

- PostgreSQL business tables are authoritative.
- clients push one logical dirty set at a time through staged push sessions
- clients pull complete committed bundles only
- fresh installs and prune recovery rebuild from frozen snapshot sessions

## Packages

- `oversync/`: PostgreSQL adapter, schema validation, bundle capture, HTTP handlers
- `oversqlite/`: SQLite client SDK with trigger-based dirty capture and sync loops
- `examples/nethttp_server/`: reference `net/http` server
- `examples/mobile_flow/`: end-to-end simulator for the current client/server contract
- `examples/samplesync_server/`: sample server for the KMP sample app
- `docs/`: Jekyll site content
- `swagger/two_way_sync.yaml`: OpenAPI description of the HTTP surface

## Current Supported Envelope

Server-side registered tables are intentionally constrained:

- one sync key column per registered table
- visible sync key type must be `uuid` or `text`
- registered PostgreSQL tables must include `_sync_scope_id TEXT NOT NULL`
- registered PostgreSQL row identity must be scope-bound through `(_sync_scope_id, sync_key)`
- registered tables must be FK-closed
- registered-to-registered foreign keys must be scope-inclusive and `DEFERRABLE`
- unsupported key shapes or FK shapes fail during bootstrap

The SQLite client is likewise fail-closed:

- one configured remote schema per SQLite database
- one `oversqlite.Client` process owner per SQLite database
- managed local tables must be FK-closed

## Quick Start

Install the module:

```bash
go get github.com/mobiletoly/go-oversync
```

Run the fast checks:

```bash
go test ./oversync ./oversqlite
```

Start the reference server:

```bash
DATABASE_URL="postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable" \
JWT_SECRET="dev-secret" \
go run ./examples/nethttp_server
```

Run an implemented simulator scenario:

```bash
cd examples/mobile_flow
go run . --scenario=fresh-install --cleanup=false
```

## Server Integration

```go
cfg := &oversync.ServiceConfig{
    MaxSupportedSchemaVersion: 1,
    AppName:                   "my-sync-app",
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

mux := http.NewServeMux()
mux.Handle("POST /sync/push-sessions", auth(http.HandlerFunc(handlers.HandleCreatePushSession)))
mux.Handle("POST /sync/push-sessions/{push_id}/chunks", auth(http.HandlerFunc(handlers.HandlePushSessionChunk)))
mux.Handle("POST /sync/push-sessions/{push_id}/commit", auth(http.HandlerFunc(handlers.HandleCommitPushSession)))
mux.Handle("DELETE /sync/push-sessions/{push_id}", auth(http.HandlerFunc(handlers.HandleDeletePushSession)))
mux.Handle("GET /sync/committed-bundles/{bundle_seq}/rows", auth(http.HandlerFunc(handlers.HandleGetCommittedBundleRows)))
mux.Handle("GET /sync/pull", auth(http.HandlerFunc(handlers.HandlePull)))
mux.Handle("POST /sync/snapshot-sessions", auth(http.HandlerFunc(handlers.HandleCreateSnapshotSession)))
mux.Handle("GET /sync/snapshot-sessions/{snapshot_id}", auth(http.HandlerFunc(handlers.HandleGetSnapshotChunk)))
mux.Handle("DELETE /sync/snapshot-sessions/{snapshot_id}", auth(http.HandlerFunc(handlers.HandleDeleteSnapshotSession)))
mux.Handle("GET /sync/capabilities", auth(http.HandlerFunc(handlers.HandleCapabilities)))
mux.HandleFunc("GET /health", handlers.HandleHealth)
mux.HandleFunc("GET /status", handlers.HandleStatus)
```

Your auth middleware must authenticate the request and inject
`oversync.Actor{UserID, SourceID}` into request context before calling the sync handlers.
The runtime derives `_sync_scope_id` from `Actor.UserID`; clients never send or receive
`_sync_scope_id` in visible sync payloads.

## SQLite Client

```go
cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
    {TableName: "users", SyncKeyColumnName: "id"},
    {TableName: "posts", SyncKeyColumnName: "id"},
})

client, err := oversqlite.NewClient(db, "http://localhost:8080", "user-123", "device-abc", tokenProvider, cfg)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

if err := client.Bootstrap(ctx, true); err != nil {
    log.Fatal(err)
}

if err := client.Sync(ctx); err != nil {
    log.Fatal(err)
}
```

## Documentation

- docs site: <https://mobiletoly.github.io/go-oversync/>
- getting started: `docs/getting-started.md`
- server reference: `docs/documentation/server.md`
- client reference: `docs/documentation/client.md`
- HTTP API reference: `docs/documentation/api.md`

## Examples

- `examples/nethttp_server/`: reference server with JWT auth and test helpers
- `examples/mobile_flow/`: simulator for implemented and scaffolded sync scenarios
- `examples/samplesync_server/`: sample server used by the Kotlin sample app

## License

Apache 2.0. See `LICENSE` if present in your distribution or the source headers in this repository.
