# Snapshot capacity server

This example runs the real Oversync HTTP server against a disposable PostgreSQL database. It registers 14 `business.records_*` tables with three foreign keys on the final table, then seeds 50,000 synthetic rows through normal bundle writes. It is used by SQLiteNow's large replacement qualification; building or testing the Go package does not start a server or create data.

Use an isolated local PostgreSQL instance and set `OVERSYNC_EXAMPLE_TEST_DATABASE_URL` to its administrative database URL. Set `OVERSYNC_SMOKE_DATABASE_NAME` to a unique `oversync_smoke_*` name owned by the run. Start with `go run ./examples/snapshot_capacity_server`. The `READY` line reports the authority URL, a loopback control URL, the generation fingerprint, and the initial bundle sequence. `POST /replace` on the control URL changes all 50,000 rows through normal bundle writes and returns the final sequence. The control listener binds only to `127.0.0.1`.

Stop the example with SIGINT. Run `go run ./examples/snapshot_capacity_server --cleanup` with the same database environment to drop its owned database. The SQLiteNow qualification runner owns the PostgreSQL container, volume, process, and cleanup when it invokes this example.
