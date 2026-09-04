# Table evolution conformance v1

`contract.json` contains exact old/target declarations and compiled generation proofs.
`cases.json` uses those proof indices and fixed scope/business UUIDs. Case IDs are stable.
The baseline registers only people and legacy_notes; the target registration list in the
contract file describes the post-append catalog, not the starting database.

The protocol sequence starts with one captured Person insert (bundle 1), then captured
project population and assignment (bundle 2). Snapshots run before further writes.
`old-update-omission` uses base version 2 and commits bundle 3; `explicit-null` uses
base version 3 and commits bundle 4. Their exact generation headers, compact-push
envelopes and canonical request hashes are inputs checked by Go tests.

For each upload, POST the case's complete compact `push_request`, including its row, to
`/sync/push` using the case's headers plus the actor's `Oversync-Source-ID` and normal
authentication. These fixture requests commit atomically. Source identifiers and HTTP
authorization tokens are bound runtime references, never business identities or golden
values. Exact snapshots compare payloads by table; current row versions must come from
the captured row state. Snapshot creation supplies
`{snapshot_id}`; chunk GET includes `after_row_ordinal=0`, `max_rows=100`, and a positive
`max_bytes` within the advertised limit.

The hidden-cascade case first inserts one attachment referencing Person, producing an
empty old-generation bundle. A captured Person delete removes both canonical rows while
the old view contains only the Person delete. Both retain the real checkpoint.
The population test also rejects target pull from pre-expansion history with no new FK
field; it cannot invent a NULL value or consult a current row to rewrite history.

The local cases require the application migrator, old-intent resolution gate and full
snapshot rebuild. The mobile test consumes the person ID, pending-intent classes and
process-death boundaries. The physical-FK test consumes the mismatch outcome; it also
checks matching, missing, wrong-action and nondeferrable SQLite constraints. The real
cleanup tests preserve source/history/receipt evidence across expiry, rollback and replay.

`manifest.json` maps every case to named executable Go tests and binds the artifacts.
Run `python3 scripts/check-table-evolution-conformance.py` to check the manifest; add
`--execute` to run each mapped test once. Execution uses Go 1.25.12 and requires Docker
for the application lane and an isolated PostgreSQL 16 `TEST_DATABASE_URL` for mobile.
This corpus defines runtime-neutral outcomes; SQLiteNow execution is pending,
and cross-runtime parity qualification is pending.

The independent `captured-set-null` journey starts from fresh baseline/population (bundles 1/2), then submits the exact target project DELETE at base version 2. Bundle 3 captures the referencing Person UPDATE and project DELETE; `_sync_scope_id` and another scope with the same business keys remain intact. Ordered expected rows and the post-delete snapshot are provided for both views. The old view omits project_id and the project DELETE but retains the Person version. Tests verify projected hashes/counts/bytes, paginated committed results and replay for the owning target source, both snapshots, and metadata-only watch wakeups. An old source cannot read the target source's committed-result endpoint; it receives its own view through pull.
