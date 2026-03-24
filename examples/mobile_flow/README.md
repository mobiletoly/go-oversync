# Mobile Flow Simulator

`mobile_flow` is the end-to-end simulator for the current bundle-based contract. It runs realistic
SQLite clients against the `nethttp_server` example.

Some scenario names are fully implemented regression flows. Others are present in the CLI but are
still scaffolds that log `not yet implemented`. This README distinguishes the two.

## What It Proves

- implemented scenarios exercise `PushPending`, `PullToStable`, `Hydrate`, and `Recover`
- dirty local state blocks pull instead of being silently rebased
- bundle checkpoints advance only after durable local apply
- multi-chunk push upload and post-commit recovery paths work for implemented scenarios
- supported FK graphs in the example schema converge across devices
- canonical binary wire payloads survive end-to-end sync for `files` and `file_reviews`
- server-originated writes and cascades are visible as committed bundle outcomes

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

## Scaffolded Scenario Names

These names are accepted by the simulator today but are not implemented yet:

- `reinstall`
- `device-replacement`
- `offline-online`
- `user-switch`

## Simulator Architecture

- `MobileApp`
  - wraps the local SQLite database, auth session, and `oversqlite.Client`
- `SyncManager`
  - runs periodic push and pull loops using the supported bundle APIs
- `DatabaseVerifier`
  - checks PostgreSQL business tables and sync metadata
- `ReportGenerator`
  - emits per-scenario reports for debugging and regression tracking

## Supported Local Metadata

The simulator assumes the supported `oversqlite` metadata model:

- `_sync_client_state`
- `_sync_row_state`
- `_sync_dirty_rows`
- `_sync_push_outbound`
- `_sync_push_stage`

## Troubleshooting

- If pull fails while local dirty rows exist, that is expected fail-closed behavior.
- If the server returns `history_pruned`, the client should rebuild through chunked snapshot sessions.
- If a scenario fails, inspect the JSON report when you ran with `--output`, or the generated
  `test_reports/` file for parallel runs, before digging into PostgreSQL bundle metadata.

More detail:

- [Complex Multi-Batch Scenario](docs/complex-multi-batch-scenario.md)
- [Troubleshooting Bundle Push Completion](docs/troubleshooting-upload-completion.md)
