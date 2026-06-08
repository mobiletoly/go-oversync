# Mobile Flow Simulator

`mobile_flow` is the end-to-end simulator for the current bundle-based contract. It runs realistic
SQLite clients against the `nethttp_server` example.

Most scenario names are fully implemented regression flows. A smaller remainder is still present in
the CLI as partial scaffolds. This README distinguishes the two.

## What It Proves

- implemented scenarios exercise `PushPending`, `PullToStable`, and `Rebuild(ctx)`
- the simulator consumes rich `oversqlite` results instead of treating lifecycle/sync operations as
  error-only calls
- dirty local state blocks pull instead of being silently rebased
- bundle checkpoints advance only after durable local apply
- multi-chunk push upload and post-commit recovery paths work for implemented scenarios
- supported FK graphs in the example schema converge across devices
- canonical binary wire payloads survive end-to-end sync for `files` and `file_reviews`
- server-originated writes and cascades are visible as committed bundle outcomes
- watch-mode regression tests prove default clients do not open `/sync/watch`, opt-in clients can
  use it as a wake-up stream, and a nethttp-backed peer push wakes a watching client through normal
  pull
- after `Detach()`, offline local writes are still captured immediately and show up in the pending
  badge before the next sign-in

## What It Does Not Prove

- unsupported composite sync keys or composite foreign keys
- arbitrary schemas outside the example registered-table set
- every possible crash timing or production workload shape

## Prerequisites

1. Start the example server:

```bash
go run ./examples/nethttp_server
```

2. Ensure PostgreSQL is reachable with the same database used by the example server.

## Usage

Interactive mode:

```bash
cd examples/mobile_flow
go run .
```

Command-line mode:

```bash
cd examples/mobile_flow

go run . --scenario=fresh-install
go run . --scenario=multi-device-sync
go run . --scenario=files-sync
go run . --scenario=typed-rows
go run . --scenario=watch-all
go run . --scenario=watch-peer-push
go run . --scenario=watch-server-originated
go run . --scenario=watch-reconnect-catch-up
go run . --scenario=watch-default-off
go run . --scenario=watch-server-unsupported-fallback
go run . --scenario=watch-idle-cleanup
go run . --scenario=watch-many-clients-converge
go run . --scenario=bundle-fk-atomicity
go run . --scenario=complex-multi-batch --cleanup=true --verbose
go run . --scenario=all
```

Useful flags:

- `--server=http://127.0.0.1:8080`
- `--db=postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable`
- `--parallel=10`
- `--cleanup=true`
- `--verbose`

## Implemented Scenarios

- `fresh-install`
  - bootstrap, first sign-in, and initial snapshot hydration
- `normal-usage`
  - routine push/pull behavior on an established client
- `bundle-fk-atomicity`
  - self-reference, cycle, and cascade visibility under committed bundle semantics
- `reinstall`
  - reinstall-style rebuild with internally managed source identity
- `device-replacement`
  - replacement-device rebuild on a fresh app install for the same user
- `complex-multi-batch`
  - forces multi-chunk push upload with a low per-chunk `UploadLimit`, then checks pre-commit and
    post-commit restart recovery across scalar and BLOB-bearing tables
- `multi-device-sync`
  - two devices syncing through the same authoritative server state
- `multi-device-complex`
  - longer mixed-operation convergence flow across two devices
- `conflicts`
  - structured conflict recovery under moderate multi-chunk load, covering built-in server-wins,
    built-in client-wins, and a custom `updated_at`-based merge resolver using UTC RFC3339Nano
    timestamp strings in SQLite `TEXT` columns
- `files-sync`
  - narrow BLOB-focused scenario for quick debugging of `files` / `file_reviews`
- `typed-rows`
  - narrow scalar/nullability scenario for quick debugging of `typed_rows`
- `watch-peer-push`
  - starts a watch-enabled client, pushes from a peer client, and asserts the watcher converges
    through normal pull before the low-frequency fallback poll interval
- `watch-server-originated`
  - commits a `ScopeManager.ExecWrite(...)` bundle from the simulator process and asserts the
    launched `nethttp_server` watch endpoint wakes the client through PostgreSQL `NOTIFY`
- `watch-reconnect-catch-up`
  - disconnects a watcher, pushes from a peer, reconnects with `after_bundle_seq`, and asserts
    immediate durable catch-up through normal pull
- `watch-default-off`
  - runs through a counting proxy to prove default clients never open `/sync/watch` while still
    converging through polling
- `watch-server-unsupported-fallback`
  - rewrites `/sync/capabilities` through a local proxy to advertise watch as unsupported and
    proves watch-auto clients keep syncing through polling without opening `/sync/watch`
- `watch-idle-cleanup`
  - opens an idle watch stream, cancels it without a data event, and verifies the server-side
    process-local subscriber count returns to zero
- `watch-many-clients-converge`
  - starts many watch-enabled clients for one scope, commits a peer push, and verifies every
    watcher reaches the expected row state and `last_bundle_seq_seen`
- `watch-all`
  - runs all implemented real watch scenarios against the configured server

## Partially Scaffolded Scenario Names

These names are accepted by the simulator, but the implementations are not complete yet:

- `offline-online`
- `user-switch`

## Simulator Architecture

- `MobileApp`
  - wraps the local SQLite database, auth session, and `oversqlite.Client`
  - branches intentionally on rich `Open`, `Attach`, `Detach`, `PushPending`, `PullToStable`, and
    `Rebuild` results
- `SyncManager`
  - runs periodic push and pull loops using the supported bundle APIs
- `DatabaseVerifier`
  - checks PostgreSQL business tables and sync metadata
- `ReportGenerator`
  - emits per-scenario reports for debugging and regression tracking

## Supported Local Metadata

The simulator assumes the supported `oversqlite` metadata model:

- `_sync_attachment_state`
- `_sync_row_state`
- `_sync_dirty_rows`
- `_sync_outbox_*`
- `_sync_push_stage`

## Troubleshooting

- If pull fails while local dirty rows exist, that is expected fail-closed behavior.
- If detach returns a blocked result, that is expected when attached pending sync state still
  exists; the simulator keeps the session attached in that case.
- If the server returns `history_pruned`, the client should rebuild through chunked snapshot sessions.
- If a scenario fails, inspect the JSON report when you ran with `--output`, or the generated
  `test_reports/` file for parallel runs, before digging into PostgreSQL bundle metadata.

More detail:

- [Complex Multi-Batch Scenario](docs/complex-multi-batch-scenario.md)
- [Troubleshooting Bundle Push Completion](docs/troubleshooting-upload-completion.md)
