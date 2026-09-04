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
python3 -B scripts/mixed-schema-fixture.py 16 coexistence
python3 -B scripts/mixed-schema-fixture.py 17 coexistence
python3 -B -m unittest discover -s scripts -p test_mixed_schema.py
```

The wrapper provisions an isolated PostgreSQL container and durable volume,
preserves normal durability settings, and removes its own resources afterward.
The coexistence lane runs uncached with a nine-minute Go timeout and a ten-minute
outer limit. The ordinary root test run skips these opt-in cases.

Other validation lanes use the same wrapper:

```sh
python3 -B scripts/mixed-schema-fixture.py 16 package
python3 -B scripts/mixed-schema-fixture.py 16 full
python3 -B scripts/mixed-schema-fixture.py 16 mobile
python3 -B scripts/mixed-schema-fixture.py 16 vet
```

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

The [workflow](../../.github/workflows/mixed-schema.yml) runs PostgreSQL 16 and 17,
the separate regression lanes, and checker tests on pull requests and main pushes.
It retains diagnostic artifacts for 14 days.

Configure branch protection to require **mixed-schema-required**. This aggregate
check succeeds only when all prerequisite jobs succeed. Verify that a missing or
failing check blocks merging; the workflow alone does not configure branch protection.
