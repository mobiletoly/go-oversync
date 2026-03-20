# Mobile Flow Simulator

`mobile_flow` is the bundle-era end-to-end simulator for go-oversync. It runs realistic SQLite
clients against the `nethttp_server` example and verifies the supported sync contract under
multi-device, offline, conflict, cascade, self-reference, FK-cycle, and BLOB-bearing-table
scenarios.

## What It Proves

- clients use `PushPending`, `PullToStable`, `Hydrate`, and `Recover`
- dirty local state blocks pull instead of being silently rebased
- bundle checkpoints advance only after durable local apply
- large offline dirty sets can exceed one push chunk and still converge through chunked push
  sessions
- restart recovery works both before commit and after commit but before final local replay
- supported FK graphs in the example schema converge across devices
- canonical binary wire payloads survive real end-to-end sync for `files` and `file_reviews`
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

## Key Scenarios

- `fresh-install`
  - bootstrap, first sign-in, and initial snapshot hydration
- `normal-usage`
  - routine push/pull behavior on an established client
- `reinstall`
  - destructive rebuild using `Recover`
- `device-replacement`
  - same user on a new device/source id
- `multi-device-conflicts`
  - whole-bundle conflict handling across peers
- `complex-multi-batch`
  - forces multi-chunk push upload with a low per-chunk `UploadLimit`, then checks pre-commit and
    post-commit restart recovery across scalar and BLOB-bearing tables
- `files-sync`
  - narrow BLOB-focused scenario for quick debugging of `files` / `file_reviews`
- `bundle-fk-atomicity`
  - self-reference, cycle, and cascade visibility under committed bundle semantics

## Simulator Architecture

- `MobileApp`
  - wraps the local SQLite database, auth session, and `oversqlite.Client`
- `SyncManager`
  - runs periodic push and pull loops using the supported bundle APIs
- `DatabaseVerifier`
  - checks PostgreSQL business tables and bundle-era sync metadata
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
- If a scenario fails, inspect the JSON report and PostgreSQL bundle metadata before looking at
  managed business tables and bundle metadata.

More detail:

- [Complex Multi-Batch Scenario](docs/complex-multi-batch-scenario.md)
- [Troubleshooting Bundle Push Completion](docs/troubleshooting-upload-completion.md)
