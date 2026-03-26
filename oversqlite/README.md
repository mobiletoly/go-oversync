# oversqlite

`oversqlite` is the SQLite client SDK for go-oversync’s bundle-based sync protocol.

It tracks local dirty rows with triggers, freezes one outbound snapshot at a time, uploads that
snapshot through push sessions, replays the authoritative committed bundle locally, pulls complete
committed bundles, and rebuilds through chunked snapshot sessions for hydration or recovery.

## Current model

- Business tables are your application tables.
- `_sync_dirty_rows` stores one coalesced dirty row per structured key.
- `_sync_push_outbound` stores the frozen in-flight push snapshot.
- `_sync_push_stage` stores authoritative committed rows before final local replay.
- `_sync_row_state` stores the authoritative replicated row version seen by the client.
- `_sync_client_state` stores:
  - `source_id`
  - `schema_name`
  - `next_source_bundle_id`
  - `last_bundle_seq_seen`
  - `apply_mode`
  - `rebuild_required`

## One-shot operations

- `PushPending(ctx)` freezes `_sync_dirty_rows` into `_sync_push_outbound`, uploads through
  `/sync/push-sessions`, fetches the committed authoritative rows, and replays them locally.
- `PullToStable(ctx)` pulls complete committed bundles until the frozen stable ceiling is reached.
  If the local checkpoint falls behind the retained bundle floor, it rebuilds from chunked
  snapshot sessions.
- `Sync(ctx)` runs `PushPending(ctx)` and then `PullToStable(ctx)`.
- `Hydrate(ctx)` rebuilds from `POST /sync/snapshot-sessions` plus chunk fetches.
- `Recover(ctx)` resets managed state, rebuilds from chunked snapshot sessions, and rotates `source_id`.

## Quick start

```go
db, err := sql.Open("sqlite3", "app.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

cfg := oversqlite.DefaultConfig("business", []oversqlite.SyncTable{
    {TableName: "users", SyncKeyColumnName: "id"},
    {TableName: "posts", SyncKeyColumnName: "id"},
})

tokenProvider := func(ctx context.Context) (string, error) {
    return "<jwt>", nil
}

client, err := oversqlite.NewClient(
    db,
    "http://localhost:8080",
    "user-123",
    "device-abc",
    tokenProvider,
    cfg,
)
if err != nil {
    log.Fatal(err)
}

if err := client.PushPending(context.Background()); err != nil {
    log.Printf("push failed: %v", err)
}
if err := client.PullToStable(context.Background()); err != nil {
    log.Printf("pull failed: %v", err)
}
```

## Important invariants

- Payloads are full-row after-images for `INSERT` and `UPDATE`.
- Push is all-or-nothing at bundle level.
- Syncable application timestamps should be stored as SQLite `TEXT` in UTC RFC3339/RFC3339Nano
  form such as `2026-03-24T18:02:00Z` or `2026-03-24T18:02:00.123456789Z`.
- Structured `push_conflict` responses are resolved from machine-readable conflict details, not
  human-readable error strings.
- `PushPending(ctx)` auto-recovers valid structured conflict outcomes:
  - `AcceptServer`
  - `KeepLocal`
  - `KeepMerged(mergedPayload)`
- `KeepLocal` and `KeepMerged` retry from a fresh outbound snapshot while preserving the same
  logical `source_bundle_id`.
- Structured conflict retries are bounded. Invalid resolutions and retry exhaustion fail closed by
  clearing `_sync_push_outbound` and restoring replayable intents to `_sync_dirty_rows`.
- Pull applies complete bundles only and advances `last_bundle_seq_seen` only after durable commit.
- `last_bundle_seq_seen` is the highest contiguous durably applied bundle, not the highest seen seq.
- `PullToStable`, `Hydrate`, and `Recover` fail closed while `_sync_push_outbound` exists.
- `Sync` fails closed while `_sync_client_state.rebuild_required = 1`; only `Hydrate` or `Recover`
  may clear that flag.
- `oversqlite` supports exactly one configured remote schema per local SQLite database in the
  current runtime. Startup fails if existing sync metadata belongs to a different schema or if a
  table registration is schema-qualified.
- Run exactly one `oversqlite.Client` against one SQLite database at a time. Cross-client
  coordination is not supported. Call `client.Close()` before replacing a client against the same
  SQLite file or handle.
- Local FK cascades and local trigger-generated writes on managed tables are captured into `_sync_dirty_rows`.
- Primary-key changes are represented as delete-plus-upsert on the client dirty set.
- Each managed table must declare exactly one visible sync key column.
- The visible sync key column must also be the local SQLite `PRIMARY KEY` in the current runtime.
- Supported local visible sync key shapes are `TEXT PRIMARY KEY` and UUID-backed `BLOB PRIMARY KEY`.
- Integer-like local sync key primary keys are rejected during client startup.

## Timestamp fields

`oversqlite` does not impose a special SQLite datetime type for your business rows. Syncable
application timestamp columns should use:

- SQLite column type: `TEXT`
- value format: UTC RFC3339 or RFC3339Nano
- resolver behavior: parse timestamps as time values; do not compare raw strings unless your format
  is fixed and normalized

The client runtime's own metadata tables also use UTC text timestamps. Older ad hoc formats such as
`yyyy:mm:dd hh:mm:ss` are not the recommended sync format.

## Recovery

If the server returns `history_pruned`, the client must rebuild from chunked snapshot sessions.

- `Hydrate(ctx)` performs a snapshot rebuild without rotating `source_id`.
- `Recover(ctx)` performs a destructive rebuild and rotates `source_id` before new writes resume.

## Conflict Resolution

Resolvers now receive structured conflict context:

- `schema`
- `table`
- `key`
- `local op`
- `local payload`
- `base row version`
- `server row version`
- `server row deleted`
- `server row payload`

Built-in resolvers:

- `&oversqlite.ServerWinsResolver{}`
- `&oversqlite.ClientWinsResolver{}`

Example:

```go
client.Resolver = &oversqlite.ClientWinsResolver{}
```

Custom resolvers implement:

```go
type Resolver interface {
    Resolve(conflict oversqlite.ConflictContext) oversqlite.MergeResult
}
```

Possible results:

- `oversqlite.AcceptServer{}`
- `oversqlite.KeepLocal{}`
- `oversqlite.KeepMerged{MergedPayload: payload}`

Typed errors:

- `*oversqlite.PushConflictError`: structured conflict payload decoded from transport
- `*oversqlite.InvalidConflictResolutionError`: resolver returned an invalid outcome for the conflict shape
- `*oversqlite.PushConflictRetryExhaustedError`: automatic structured conflict retry hit the retry budget

## Server requirements

The server must expose:

- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{push_id}/chunks`
- `POST /sync/push-sessions/{push_id}/commit`
- `DELETE /sync/push-sessions/{push_id}`
- `GET /sync/committed-bundles/{bundle_seq}/rows`
- `GET /sync/pull`
- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshot_id}`
- `DELETE /sync/snapshot-sessions/{snapshot_id}`
- `GET /sync/capabilities`

## Testing

```bash
go test ./oversqlite
```
