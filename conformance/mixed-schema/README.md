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
python3 -B scripts/mixed-schema-fixture.py 17 package-oversync
python3 -B scripts/mixed-schema-fixture.py 17 package-oversqlite-1
python3 -B scripts/mixed-schema-fixture.py 17 package-oversqlite-2
python3 -B scripts/mixed-schema-fixture.py 17 root-evolution
python3 -B scripts/mixed-schema-fixture.py 17 root-rest
python3 -B scripts/mixed-schema-fixture.py 17 mobile
python3 -B scripts/mixed-schema-fixture.py 17 vet
```

The [partition](../../scripts/root-partition.py) assigns every root-module
package to exactly one lane: `package-oversync` owns `oversync` and runs the
manifest's supporting test, `package-oversqlite-1` and `package-oversqlite-2`
split the top-level tests of `oversqlite`, `root-evolution` owns
`integration/schema_evolution/...`, and `root-rest` owns every other package,
including new ones. Each lane keeps a 240-second outer limit. The root lanes,
`mobile`, and the local `package` and `full` aggregates record package evidence
in `fixture.json`: every expected package must start once and pass, or skip when
it has no test files.

The `oversqlite` shards take their test list from `go test -list .`. A short
pin list keeps the longest tests apart; every other test goes to a shard by a
hash of its name, so new tests are assigned automatically and existing ones do
not move. Each shard also records test evidence: every assigned test must run
once and pass or skip, and no other top-level test may run. Runs with extra Go
arguments are development runs and never count as package or test evidence.

`package` (both L2 packages) and `full` (every root package) remain local
aggregates; hosted CI runs the separate lanes. To build a lane's test binaries
without running them, as CI does before the timed lane, use
`scripts/test-lane.sh precompile <lane>`.

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
the `package-oversync`, `package-oversqlite-1`, `package-oversqlite-2`,
`root-evolution`, `root-rest`, `mobile` and `vet` regression lanes
on PostgreSQL 17.11, and the guards on pull requests and main pushes. Regression
lanes build their test binaries in a separate step of at most four minutes
before the timed lane. The guards check the partition against `go list ./...`
and `go test -list`, and publish it, with both shards' test lists, as an
artifact. Diagnostic artifacts are kept for 14 days. For a
run without Go caches, such as the release-gate run, dispatch the workflow on
the exact commit with `go-cache` unchecked.

Configure branch protection to require **mixed-schema-required**. This aggregate
check succeeds only when all prerequisite jobs succeed and every lane artifact
is present and complete: clean fixture results, the full coexistence manifest,
package evidence matching the published partition, and shard test evidence
covering every listed `oversqlite` test exactly once. Verify that a missing or
failing check blocks merging; the workflow alone does not configure branch protection.
