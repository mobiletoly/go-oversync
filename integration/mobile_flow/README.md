# Mobile Flow Simulator

`mobile_flow` is the repository's end-to-end simulator for the current bundle-based contract. It
runs realistic SQLite clients against the `nethttp_server` integration test server. It is
qualification infrastructure, not a consumer-facing example.

Most scenario names are fully implemented regression flows. A smaller remainder is still present in
the CLI as partial scaffolds. This README distinguishes the two.

## What It Proves

- implemented scenarios exercise `PushPending`, `PullToStable`, and `Rebuild(ctx)`
- the simulator consumes rich `oversqlite` results instead of treating lifecycle/sync operations as
  error-only calls
- dirty local state blocks pull instead of being silently rebased
- bundle checkpoints advance only after durable local apply
- every client is provisioned with one immutable schema-generation proof retained after discovery
  and used for requests, retries, and watch reconnects
- fixed-generation clients verify generation-visible counts, bytes, hashes, and pages derived from
  the server's single canonical history; empty visible envelopes still advance their checkpoints
- normalized authoritative push effects settle without effect-versus-intent equality, while newer
  local edits remain dirty and rebased; uncovered uploaded keys fail atomically with HTTP 422
- whole-request push upload, response-loss replay, and post-commit recovery paths work for
  implemented scenarios
- supported FK graphs in the integration fixture schema converge across devices
- canonical binary wire payloads survive end-to-end sync for `files` and `file_reviews`
- server-originated writes and cascades are visible as committed bundle outcomes
- watch-mode regression tests prove default clients do not open `/sync/watch`, opt-in clients can
  use it as a wake-up stream, and a nethttp-backed peer push wakes a watching client through normal
  pull
- after `Detach()`, offline local writes are still captured immediately and show up in the pending
  badge before the next sign-in
- `TestMobileFlow_UpgradeSameFileUpgradeRestartJourney` reopens one SQLite file under an explicit
  newer application definition, drains an old offline write, retries one lost replacement reply,
  preserves a local-only sentinel, hydrates a new-only column, and resumes target push/pull
- the same journey keeps a second device on the old generation and syncs it before and after the
  first device rotates its source

## What It Does Not Prove

- unsupported composite sync keys or composite foreign keys
- arbitrary schemas outside the integration fixture's registered-table set
- PostgreSQL transition-write and rollout orchestration from transition-rollout
- projection metadata caches or per-generation durable history copies
- every possible crash timing or production workload shape

The repository package tests own the exhaustive U-boundary and subprocess-death matrix. Run the
nested same-file integration journey with:

```bash
env GOTOOLCHAIN=go1.25.12 go test ./simulator -run TestMobileFlow_UpgradeSameFileUpgradeRestartJourney -v
```

## Prerequisites

1. Start the integration test server:

```bash
go run ./integration/nethttp_server
```

2. Ensure PostgreSQL is reachable with the same database used by the integration test server.

## Usage

Interactive mode:

```bash
cd integration/mobile_flow
go run .
```

Command-line mode:

```bash
cd integration/mobile_flow

go run . --scenario=fresh-install
go run . --scenario=multi-device-sync
go run . --scenario=files-sync
# Legacy affinity-only negative fixture; migration-aware clients use declared upgrade codecs:
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
go run . --scenario=complex-multi-batch --verbose
go run . --scenario=complex-multi-batch --parallel=500 --concurrency=30
go run . --scenario=all
```

`--scenario=all` runs the positive scenarios and excludes `typed-rows`, whose expected outcome is
the explicit validation failure described below.

Useful flags:

- `--server=http://127.0.0.1:8080`
- `--db=postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable`
- `--parallel=500` sets the total number of simulated users
- `--concurrency=30` limits simultaneously active user scenarios (`0` or omitted uses `--parallel`)
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
  - exercises bounded multi-row whole-request push and restart recovery across scalar and
    BLOB-bearing tables
- `multi-device-sync`
  - two devices syncing through the same authoritative server state
- `multi-device-complex`
  - longer mixed-operation convergence flow across two devices
- `conflicts`
  - structured conflict recovery under moderate multi-row load, covering built-in server-wins,
    built-in client-wins, and a custom `updated_at`-based merge resolver using UTC RFC3339Nano
    timestamp strings in SQLite `TEXT` columns
- `files-sync`
  - narrow BLOB-focused scenario for quick debugging of `files` / `file_reviews`
- `typed-rows`
  - negative contract-boundary scenario: inserts legacy SQLite affinity-derived Boolean and numeric
    values, then exits with the exact-generation upload failure expected under generation
  - its executable regression confirms HTTP 400, zero authoritative `typed_rows`, two unchanged
    local rows, and two rows retained in the durable outbox
  - migration-aware upgrade clients use explicit per-column local codecs; this legacy fixture stays
    deliberately affinity-only so it continues to verify the old fail-closed boundary
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
  - exercises a discovery response without watch support and
    proves watch-auto clients keep syncing through polling without opening `/sync/watch`
- `watch-idle-cleanup`
  - opens an idle watch stream, cancels it without a data event, and verifies the server-side
    process-local subscriber count returns to zero
- `watch-many-clients-converge`
  - starts many watch-enabled clients for one scope, commits a peer push, and verifies every
    watcher reaches the expected row state and `last_bundle_seq_seen`
- `watch-all`
  - runs all implemented real watch scenarios against the configured server

## Versioned upgrade upgrade harness

The nested module owns three deterministic, pre-provisioned upgrade journeys:

- `TestMobileFlow_UpgradeSameFileUpgradeRestartJourney` reopens one SQLite file under the newer
  application definition, drains an offline old-generation write, rotates exactly one source,
  hydrates target-only columns, preserves a local-only sentinel, resumes target push/pull, and
  keeps an independent second device syncing on the old generation.
- `TestMobileFlow_UpgradeControlledFailureRestartJourney` reopens the same file after an outbox/network
  pause, migration rollback, committed migration, exhausted replacement reply loss, and a staged
  snapshot prefix, then proves final ready recovery.
- `TestMobileFlow_UpgradeProductionServerSameFileProcessDeathJourney` runs the same-file upgrade
  through the production HTTP handlers and PostgreSQL runtime. The client subprocess dies after the
  server durably commits source replacement but before SQLite records the reply; the parent verifies
  the retired/reserved source tuple, resumes the file, preserves local-only data, hydrates the target
  generation, and keeps a second old-generation device operational.

Run all journeys directly with:

```bash
cd integration/mobile_flow
env GOTOOLCHAIN=go1.25.12 go test ./simulator -run '^TestMobileFlow_Upgrade' -v
```

The harness prepares target statements only after the coordinator publishes ready. It uses no
transition-write/backfill behavior and does not claim SQLiteNow runtime delivery.

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
