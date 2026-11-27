# Mixed-schema sync tests

This suite checks two current Go SDK clients with different application schemas
sharing one server. Each client has its own SQLite database and source identity.
The server starts with both schema generations registered against the final
PostgreSQL schema.

The [manifest](v1/manifest.json) lists 34 required cases covering CRUD, hidden-field
preservation, defaults, nullable relationships, conflicts, lost replies, retries,
client/server restarts, and interrupted snapshots. Tests check explicit expected
rows, schema visibility, checkpoints, and settled pending state. They do not cover
historical SDKs, application migrations, or other client runtimes.

## Run

From the repository root, with Docker, Python 3, and Go 1.25.12 available:

```sh
python3 -B scripts/mixed-schema-fixture.py 17 coexistence
python3 -B -m unittest discover -s scripts -p test_mixed_schema.py
python3 -B -m unittest discover -s scripts -p test_root_partition.py
```

The wrapper provisions an isolated PostgreSQL container and durable volume,
preserves normal durability settings, and removes its own resources afterward.
The coexistence lane runs uncached with a nine-minute Go timeout and a ten-minute
outer limit. The ordinary root test run skips these opt-in cases.

Other validation lanes use the same wrapper:

```sh
python3 -B scripts/mixed-schema-fixture.py 17 package
python3 -B scripts/mixed-schema-fixture.py 17 root-evolution
python3 -B scripts/mixed-schema-fixture.py 17 root-rest
python3 -B scripts/mixed-schema-fixture.py 17 mobile
python3 -B scripts/mixed-schema-fixture.py 17 vet
```

The [partition](../../scripts/root-partition.py) assigns every root-module
package to exactly one lane: `package` owns `oversync` and `oversqlite`,
`root-evolution` owns `integration/schema_evolution/...`, and `root-rest` owns
every other package, including new ones. Each lane keeps a 240-second outer
limit. The `package`, `root-evolution`, `root-rest`, `mobile` and `full` lanes
record package evidence in `fixture.json`: every expected package must start
once and pass, or skip when it has no test files. Runs with extra Go arguments
are development runs and never count as package evidence.

`python3 -B scripts/mixed-schema-fixture.py 17 full` remains a local aggregate
of the three root lanes. To build a lane's test binaries without running them,
as CI does before the timed lane, use `scripts/test-lane.sh precompile <lane>`.

The runner prints the report location. Reports contain redacted output, test
counts, commands, runtime versions, and input fingerprints. The checker rejects
missing, skipped, duplicate, unknown, failed, or incomplete required results:

```sh
python3 -B scripts/mixed-schema.py manifest
python3 -B scripts/mixed-schema.py check /absolute/path/to/test.jsonl
```

The [data tests](../../oversqlite_e2e/mixed_schema_test.go) and
[process recovery tests](../../oversqlite_e2e/mixed_schema_process_test.go) own the
scenarios. The [runner](../../scripts/mixed-schema.py) enforces completeness.

## CI

The [workflow](../../.github/workflows/mixed-schema.yml) runs the coexistence lane,
the `package`, `root-evolution`, `root-rest`, `mobile` and `vet` regression lanes
on PostgreSQL 17.11, and the guards on pull requests and main pushes. Regression
lanes build their test binaries in a separate step of at most four minutes
before the timed lane. The guards check the partition against `go list ./...`
and publish it as an artifact. Diagnostic artifacts are kept for 14 days. For a
run without Go caches, such as the release-gate run, dispatch the workflow on
the exact commit with `go-cache` unchecked.

Configure branch protection to require **mixed-schema-required**. This aggregate
check succeeds only when all prerequisite jobs succeed and every lane artifact
is present and complete: clean fixture results, the full coexistence manifest,
and package evidence matching the published partition. Verify that a missing or
failing check blocks merging; the workflow alone does not configure branch protection.
