---
layout: default
title: Client
permalink: /documentation/client/
---

# Client

`oversqlite` is the SQLite client SDK for the bundle-based sync contract. Applications open a
database through `NewOpening`. The opening owns the file and its SQLite pool while it creates,
upgrades, or resumes the database. Application SQL access is published only when
`OpeningResult.Ready` is non-nil and the durable phase is `ready`.

The startup flow is:

1. Build the desired target `Config`.
2. Call `NewOpening` with the database path and, when work is pending, the exact `UpgradePlan`.
3. Call `Advance` until it returns a ready client or an actionable status.
4. Call `Attach(ctx, userID)` after sign-in, then use the normal sync operations.

An exact already-ready target can be reopened without its historical plan. Fresh databases,
pending work, and target changes require the exact plan. Fresh and local-only migration can run
offline. Generation changes use authenticated descriptor discovery and the retained U0-U8 recovery
state machine.

```go
opening, err := oversqlite.NewOpening(&oversqlite.OpeningConfig{
    DatabasePath:       "app.db",
    Plan:               plan, // nil only for an exact already-ready target
    BaseURL:            "https://sync.example.com",
    Token:              tokenProvider,
    TargetClientConfig: targetConfig,
})
if err != nil {
    return err
}
defer opening.Close()

result, err := opening.Advance(ctx)
if err != nil {
    // result.Status.Outcome identifies the stable failure class.
    return err
}
if result.Ready == nil {
    return fmt.Errorf("database is not ready: %s", result.Status.Outcome)
}

client := result.Ready
defer client.Close()
db := client.Database()
```

`Database()` returns a borrowed pool. Stop application queries and release rows, transactions, and
statements before `Client.Close` or successful `UninstallSync`. Those terminal operations stop and
join package-owned background loops, close the pool, and release file ownership. A successful
uninstall preserves business tables but intentionally prevents same-file fresh reinstall; adopting
that file requires a separately designed provenance path.

`oversqlite` manages source identity internally. Host code does not provide, persist, or rotate
source IDs. Auth remains host-owned.

## Configuration

Configure `GenerationID`, the complete managed table/key list, and stable `DatabaseID`, `ServiceID`,
and `LocalVersion` values. `DefaultConfig(generationID, tables)` supplies transport defaults.
`NewOpening` deep-copies the plan and target configuration.

The SDK derives the initial local physical declaration from SQLite metadata. During attachment it
discovers the selected server generation, recomputes its fingerprint, validates local storage, and
retains the admitted descriptor. Later exact ready reopens are offline. A changed identity or shape
requires a supported plan; it is never selected silently.

## Public lifecycle

Before ready, `Opening` exposes only control-plane methods:

- `Advance(ctx) (OpeningResult, error)`
- `Status(ctx) (OpeningStatus, error)`
- `ResolvePendingIntent(ctx, resolution) (OpeningResult, error)`
- `Close() error`

All non-ready results have `Ready == nil`. Progress callbacks occur only at non-ready durable
boundaries. They may call `Status` or `Close`; there is no final ready progress callback.

A ready `Client` provides:

- `Database() *sql.DB`
- `Attach`, `Detach`, `PendingSyncStatus`, and `SyncThenDetach`
- `PushPending`, `PullToStable`, `Sync`, and `Rebuild`
- `Start` and `Stop` for one client-owned uploader/downloader pair
- `SyncStatus`, `SourceInfo`, and `ApplicationActionRequired`
- `UninstallSync` and `Close`

`Attach` discovers and confirms the selected generation before connected sync. Before attachment,
local writes remain pending and `SyncStatus.ContractConfirmed` is false. Watch mode is optional:
set `Config.BundleChangeWatchMode` to `BundleChangeWatchAuto`; watch events only wake normal sync
logic and never mutate SQLite directly.

## Durable state and recovery

The client persists admitted contracts, dirty rows, frozen outboxes, authoritative replay rows,
snapshot staging, row state, source lineage, attachment state, lifecycle operations, and apply mode
in `_sync_*` tables. These tables are package-owned.

The client retains the exact frozen whole-request tuple until its authoritative response is applied.
If an ambiguous replay is later pruned, only a complete same-generation snapshot can settle it after
all source, proof, checkpoint, newer-intent, and transaction guards pass. Source recovery is explicit:
`PushPending`, `PullToStable`, and `Sync` fail closed while recovery is required, and `Rebuild`
performs the supported repair while preserving frozen intent.

`SourceInfo` is diagnostic only. Its source ID is opaque and must not be persisted as an application
identity.

See [Go SQLite client upgrades](/client-upgrades/) for plan, migration, resolution, and restart
behavior.
